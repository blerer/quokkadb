use crate::obs::metrics::{self, Counter, DerivedGauge, HitRatio, MetricRegistry};
use crate::query::logical_plan::LogicalPlan;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::SizeEstimate;
use moka::sync::{Cache, CacheBuilder};
use std::mem::size_of;
use std::sync::Arc;
use tracing::trace_span;

/// A cache of optimized physical plans keyed by the hash of a parameterized logical plan.
pub struct QueryCache {
    metrics: Metrics,
    cache: Cache<u64, Arc<CachedPhysicalPlan>>,
}

#[derive(Debug)]
struct CachedPhysicalPlan {
    collection: u32,
    plan: Arc<PhysicalPlan>,
    estimated_size: u32,
}

impl QueryCache {
    /// Creates a new query cache with a maximum weighted size in bytes.
    pub fn new(metric_registry: &mut MetricRegistry, max_size_bytes: u64) -> Self {
        let cache = CacheBuilder::new(max_size_bytes)
            .weigher(|_key: &u64, cached: &Arc<CachedPhysicalPlan>| cached.estimated_size)
            .build();
        let metrics = Metrics::new(cache.clone());
        metrics.register_to(metric_registry);
        tracing::debug!(max_size_bytes, "QueryCache initialized");

        Self { metrics, cache }
    }

    /// Retrieves or inserts a physical plan for the given parameterized logical plan.
    pub fn get_or_insert_with<F>(
        &self,
        collection: u32,
        logical_plan: Arc<LogicalPlan>,
        build: F,
    ) -> Arc<PhysicalPlan>
    where
        F: FnOnce() -> Arc<PhysicalPlan>,
    {
        let key = logical_plan.compute_hash();
        let _span = trace_span!("query_cache.get", hash = key).entered();

        if let Some(cached_plan) = self.cache.get(&key) {
            self.metrics.hits.inc();
            tracing::trace!(hash = key, collection, "query cache hit");
            return cached_plan.plan.clone();
        }

        self.metrics.misses.inc();
        tracing::trace!(hash = key, collection, "query cache miss");

        self.cache
            .get_with(key, || {
                let plan = build();
                Arc::new(CachedPhysicalPlan {
                    collection,
                    estimated_size: estimate_cached_plan_size(plan.as_ref()),
                    plan,
                })
            })
            .plan
            .clone()
    }

    /// Invalidates all cached plans for the given collection.
    pub fn invalidate_collection(&self, collection: u32) {
        let keys_to_invalidate: Vec<u64> = self
            .cache
            .iter()
            .filter_map(|(key, cached)| (cached.collection == collection).then_some(*key))
            .collect();

        for key in keys_to_invalidate {
            self.cache.invalidate(&key);
        }
        self.cache.run_pending_tasks();

        tracing::trace!(collection, "query cache invalidate collection");
    }
}

struct Metrics {
    size: Arc<DerivedGauge>,
    hits: Arc<Counter>,
    misses: Arc<Counter>,
    hit_ratio: Arc<HitRatio>,
}

impl Metrics {
    fn new(cache: Cache<u64, Arc<CachedPhysicalPlan>>) -> Self {
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

    #[test]
    fn get_or_insert_records_hit_and_miss_metrics() {
        let (cache, metric_registry) = new_cache();
        let logical_plan = LogicalPlanBuilder::scan(7).build();
        let build_count = AtomicUsize::new(0);

        let build_plan = || {
            build_count.fetch_add(1, Ordering::Relaxed);
            Arc::new(PhysicalPlan::NoOp)
        };

        let first = cache.get_or_insert_with(7, logical_plan.clone(), build_plan);
        let cached_size = metric_registry.gauge_value(metrics::names::query_cache::SIZE);
        let second = cache.get_or_insert_with(7, logical_plan, build_plan);

        assert_eq!(*first, PhysicalPlan::NoOp);
        assert_eq!(*second, PhysicalPlan::NoOp);
        assert_eq!(build_count.load(Ordering::Relaxed), 1);
        assert!(cached_size > 0);
        assert_eq!(
            metric_registry.gauge_value(metrics::names::query_cache::SIZE),
            cached_size
        );
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::HITS),
            1
        );
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::MISSES),
            1
        );
        assert_eq!(
            metric_registry.computed_value(metrics::names::query_cache::HIT_RATIO),
            0.5
        );
    }

    #[test]
    fn equivalent_parameterized_logical_plans_share_a_cache_entry() {
        let (cache, metric_registry) = new_cache();
        let optimizer = Optimizer::new();
        let build_count = AtomicUsize::new(0);

        let first_logical_plan = LogicalPlanBuilder::scan(7)
            .filter(field_filters(field(["status"]), [eq(lit("A"))]))
            .build();
        let second_logical_plan = LogicalPlanBuilder::scan(7)
            .filter(field_filters(field(["status"]), [eq(lit("B"))]))
            .build();

        let (first_parameterized_plan, _) =
            optimizer.parametrize(optimizer.normalize(first_logical_plan));
        let (second_parameterized_plan, _) =
            optimizer.parametrize(optimizer.normalize(second_logical_plan));

        let build_plan = || {
            build_count.fetch_add(1, Ordering::Relaxed);
            Arc::new(PhysicalPlan::NoOp)
        };

        cache.get_or_insert_with(7, first_parameterized_plan, build_plan);
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::MISSES),
            1
        );
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::HITS),
            0
        );

        let size_after_first_insert =
            metric_registry.gauge_value(metrics::names::query_cache::SIZE);
        assert!(size_after_first_insert > 0);

        cache.get_or_insert_with(7, second_parameterized_plan, build_plan);

        assert_eq!(build_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::MISSES),
            1
        );
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::HITS),
            1
        );
        assert_eq!(
            metric_registry.gauge_value(metrics::names::query_cache::SIZE),
            size_after_first_insert
        );
    }

    #[test]
    fn invalidate_collection_only_removes_matching_entries() {
        let (cache, metric_registry) = new_cache();
        let collection_one_plan = LogicalPlanBuilder::scan(1).build();
        let collection_two_plan = LogicalPlanBuilder::scan(2).build();

        cache.get_or_insert_with(1, collection_one_plan.clone(), || {
            Arc::new(PhysicalPlan::NoOp)
        });
        cache.get_or_insert_with(2, collection_two_plan.clone(), || {
            Arc::new(PhysicalPlan::NoOp)
        });

        let size_before_invalidation =
            metric_registry.gauge_value(metrics::names::query_cache::SIZE);
        assert!(size_before_invalidation > 0);

        cache.invalidate_collection(1);

        let size_after_invalidation =
            metric_registry.gauge_value(metrics::names::query_cache::SIZE);
        assert!(size_after_invalidation < size_before_invalidation);

        cache.get_or_insert_with(1, collection_one_plan, || Arc::new(PhysicalPlan::NoOp));
        cache.get_or_insert_with(2, collection_two_plan, || Arc::new(PhysicalPlan::NoOp));

        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::HITS),
            1
        );
        assert_eq!(
            metric_registry.counter_value(metrics::names::query_cache::MISSES),
            3
        );
    }

    #[test]
    fn size_metric_reports_weighted_bytes() {
        let (cache, metric_registry) = new_cache();
        let logical_plan = LogicalPlanBuilder::scan(4).build();

        cache.get_or_insert_with(4, logical_plan, || {
            Arc::new(PhysicalPlan::Projection {
                input: Arc::new(PhysicalPlan::CollectionScan {
                    collection: 4,
                    range: Interval::all(),
                    direction: Direction::Forward,
                    filter: Some(field(["name"])),
                    projection: None,
                }),
                projection: include(proj_fields([("name", proj_field())])),
            })
        });

        assert!(metric_registry.gauge_value(metrics::names::query_cache::SIZE) > 0);
    }
}
