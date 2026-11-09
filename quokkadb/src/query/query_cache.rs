use crate::error::Result;
use crate::obs::metrics::{self, Counter, DerivedGauge, HitRatio, MetricRegistry};
use crate::query::SizeEstimate;
use crate::query::logical_plan::LogicalPlan;
use crate::query::physical_plan::PhysicalPlan;
use moka::sync::{Cache, CacheBuilder};
use std::mem::size_of;
use std::sync::Arc;
use tracing::trace_span;

/// A cache of optimized physical plans keyed by collection, schema version, and
/// the hash of a parameterized logical plan.
pub struct QueryCache {
    metrics: Metrics,
    cache: Cache<(u32, u32, u64), Arc<CachedPhysicalPlan>>,
}

#[derive(Debug)]
struct CachedPhysicalPlan {
    plan: Arc<PhysicalPlan>,
    estimated_size: u32,
}

impl QueryCache {
    /// Creates a new query cache with a maximum weighted size in bytes.
    pub fn new(metric_registry: &mut MetricRegistry, max_size_bytes: u64) -> Self {
        let cache = CacheBuilder::new(max_size_bytes)
            .weigher(|_key: &(u32, u32, u64), cached: &Arc<CachedPhysicalPlan>| {
                cached.estimated_size
            })
            .build();
        let metrics = Metrics::new(cache.clone());
        metrics.register_to(metric_registry);
        tracing::debug!(max_size_bytes, "QueryCache initialized");

        Self { metrics, cache }
    }

    /// Retrieves or inserts a physical plan, leaving failed optimizations uncached.
    pub fn get_or_insert_with<F>(
        &self,
        collection: u32,
        collection_version: u32,
        logical_plan: Arc<LogicalPlan>,
        build: F,
    ) -> Result<Arc<PhysicalPlan>>
    where
        F: FnOnce() -> Result<Arc<PhysicalPlan>>,
    {
        let key = (collection, collection_version, logical_plan.compute_hash());
        let _span = trace_span!(
            "query_cache.get",
            hash = key.2,
            collection,
            collection_version
        )
        .entered();

        if let Some(cached_plan) = self.cache.get(&key) {
            self.metrics.hits.inc();
            tracing::trace!(
                hash = key.2,
                collection,
                collection_version,
                "query cache hit"
            );
            return Ok(cached_plan.plan.clone());
        }

        self.metrics.misses.inc();
        tracing::trace!(
            hash = key.2,
            collection,
            collection_version,
            "query cache miss"
        );

        let plan = build()?;
        let cached_plan = Arc::new(CachedPhysicalPlan {
            estimated_size: estimate_cached_plan_size(plan.as_ref()),
            plan,
        });
        Ok(self.cache.get_with(key, || cached_plan).plan.clone())
    }
}

struct Metrics {
    size: Arc<DerivedGauge>,
    hits: Arc<Counter>,
    misses: Arc<Counter>,
    hit_ratio: Arc<HitRatio>,
}

impl Metrics {
    fn new(cache: Cache<(u32, u32, u64), Arc<CachedPhysicalPlan>>) -> Self {
        let hits = Counter::new();
        let misses = Counter::new();

        Self {
            size: DerivedGauge::new(Arc::new(move || {
                cache.run_pending_tasks();
                cache.weighted_size()
            })),
            hits: hits.clone(),
            misses: misses.clone(),
            hit_ratio: HitRatio::new(hits, misses),
        }
    }

    fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_gauge(metrics::names::query_cache::SIZE, self.size.clone())
            .register_counter(metrics::names::query_cache::HITS, self.hits.clone())
            .register_counter(metrics::names::query_cache::MISSES, self.misses.clone())
            .register_computed(
                metrics::names::query_cache::HIT_RATIO,
                self.hit_ratio.clone(),
            );
    }
}

fn estimate_cached_plan_size(plan: &PhysicalPlan) -> u32 {
    (size_of::<CachedPhysicalPlan>() + plan.estimated_heap_size()).min(u32::MAX as usize) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::query::expr_fn::{eq, field, field_filters, include, lit, proj_field, proj_fields};
    use crate::query::logical_plan::LogicalPlanBuilder;
    use crate::query::optimizer::optimizer::Optimizer;
    use crate::query::physical_plan::PhysicalPlan;
    use crate::storage::Direction;
    use crate::util::interval::Interval;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn new_cache() -> (QueryCache, MetricRegistry) {
        let mut metric_registry = MetricRegistry::new();
        let cache = QueryCache::new(&mut metric_registry, 1024 * 1024);
        (cache, metric_registry)
    }

    fn assert_hits(metric_registry: &MetricRegistry, expected_hits: u64) {
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::HITS),
            expected_hits
        );
    }

    fn assert_misses(metric_registry: &MetricRegistry, expected_misses: u64) {
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::MISSES),
            expected_misses
        );
    }

    #[test]
    fn get_or_insert_records_hit_and_miss_metrics() {
        let (cache, metric_registry) = new_cache();
        let logical_plan = Arc::new(LogicalPlanBuilder::scan(7, None).build());
        let build_count = AtomicUsize::new(0);

        let build_plan = || {
            build_count.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(PhysicalPlan::NoOp))
        };

        let first = cache
            .get_or_insert_with(7, 1, logical_plan.clone(), build_plan)
            .unwrap();
        let cached_size = get_cached_size(&metric_registry);
        let second = cache
            .get_or_insert_with(7, 1, logical_plan, build_plan)
            .unwrap();

        assert_eq!(*first, PhysicalPlan::NoOp);
        assert_eq!(*second, PhysicalPlan::NoOp);
        assert_eq!(build_count.load(Ordering::Relaxed), 1);
        assert!(cached_size > 0);
        assert_eq!(get_cached_size(&metric_registry), cached_size);
        assert_hits(&metric_registry, 1);
        assert_misses(&metric_registry, 1);
        assert_eq!(
            metric_registry.computed_value(metrics::names::query_cache::HIT_RATIO),
            0.5
        );
    }

    #[test]
    fn failed_plan_builds_are_not_cached() {
        let (cache, metric_registry) = new_cache();
        let logical_plan = Arc::new(LogicalPlanBuilder::scan(7, None).build());

        let error = cache
            .get_or_insert_with(7, 1, logical_plan.clone(), || {
                Err(crate::error::Error::InvalidRequest(
                    "unusable hinted index".to_string(),
                ))
            })
            .unwrap_err();
        assert!(matches!(error, crate::error::Error::InvalidRequest(_)));

        let plan = cache
            .get_or_insert_with(7, 1, logical_plan, || Ok(Arc::new(PhysicalPlan::NoOp)))
            .unwrap();

        assert_eq!(*plan, PhysicalPlan::NoOp);
        assert_hits(&metric_registry, 0);
        assert_misses(&metric_registry, 2);
    }

    #[test]
    fn different_scan_hints_use_distinct_cache_entries() {
        let (cache, metric_registry) = new_cache();
        let hints = [None, Some(0), Some(17)];

        for (expected_misses, expected_hits) in [(3, 0), (3, 3)] {
            for hint in hints {
                let logical_plan = Arc::new(LogicalPlanBuilder::scan(7, hint).build());
                cache
                    .get_or_insert_with(7, 1, logical_plan, || Ok(Arc::new(PhysicalPlan::NoOp)))
                    .unwrap();
            }

            assert_misses(&metric_registry, expected_misses);
            assert_hits(&metric_registry, expected_hits);
        }
    }

    #[test]
    fn equivalent_parameterized_logical_plans_share_a_cache_entry() {
        let (cache, metric_registry) = new_cache();
        let optimizer = Optimizer::new(&Options::default());
        let build_count = AtomicUsize::new(0);

        let first_logical_plan = Arc::new(
            LogicalPlanBuilder::scan(7, None)
                .filter(field_filters(field(["status"]), [eq(lit("A"))]))
                .build(),
        );
        let second_logical_plan = Arc::new(
            LogicalPlanBuilder::scan(7, None)
                .filter(field_filters(field(["status"]), [eq(lit("B"))]))
                .build(),
        );

        let (first_parameterized_plan, _) =
            optimizer.parametrize(optimizer.normalize(first_logical_plan));
        let (second_parameterized_plan, _) =
            optimizer.parametrize(optimizer.normalize(second_logical_plan));

        let build_plan = || {
            build_count.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(PhysicalPlan::NoOp))
        };

        cache
            .get_or_insert_with(7, 1, first_parameterized_plan, build_plan)
            .unwrap();
        assert_misses(&metric_registry, 1);
        assert_hits(&metric_registry, 0);

        let size_after_first_insert = get_cached_size(&metric_registry);
        assert!(size_after_first_insert > 0);

        cache
            .get_or_insert_with(7, 1, second_parameterized_plan, build_plan)
            .unwrap();

        assert_eq!(build_count.load(Ordering::Relaxed), 1);
        assert_misses(&metric_registry, 1);
        assert_hits(&metric_registry, 1);
        assert_eq!(get_cached_size(&metric_registry), size_after_first_insert);
    }

    fn get_cached_size(metric_registry: &MetricRegistry) -> u64 {
        metric_registry.gauge_value(metrics::names::query_cache::SIZE)
    }

    #[test]
    fn schema_versions_use_distinct_cache_entries() {
        let (cache, metric_registry) = new_cache();
        let logical_plan = Arc::new(LogicalPlanBuilder::scan(1, None).build());
        let build_count = AtomicUsize::new(0);
        let build_plan = || {
            build_count.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(PhysicalPlan::NoOp))
        };

        cache
            .get_or_insert_with(1, 1, logical_plan.clone(), build_plan)
            .unwrap();
        cache
            .get_or_insert_with(1, 1, logical_plan.clone(), build_plan)
            .unwrap();

        cache
            .get_or_insert_with(1, 2, logical_plan, build_plan)
            .unwrap();

        assert_eq!(build_count.load(Ordering::Relaxed), 2);
        assert_hits(&metric_registry, 1);
        assert_misses(&metric_registry, 2);
        assert!(get_cached_size(&metric_registry) > 0);
    }

    #[test]
    fn size_metric_reports_weighted_bytes() {
        let (cache, metric_registry) = new_cache();
        let logical_plan = Arc::new(LogicalPlanBuilder::scan(4, None).build());

        cache
            .get_or_insert_with(4, 1, logical_plan, || {
                Ok(Arc::new(PhysicalPlan::Projection {
                    input: Arc::new(PhysicalPlan::CollectionScan {
                        collection: 4,
                        range: Interval::all(),
                        direction: Direction::Forward,
                        filter: Some(field(["name"])),
                        projection: None,
                    }),
                    projection: include(proj_fields([("name", proj_field())])),
                }))
            })
            .unwrap();

        assert!(get_cached_size(&metric_registry) > 0);
    }
}
