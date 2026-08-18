use crate::obs::metrics::{self, Counter, DerivedGauge, HitRatio, MetricRegistry};
use crate::options::options::Options;
use crate::storage::sstable::block_cache::BlockCache;
use crate::storage::sstable::sstable_reader::SSTableReader;
use moka::sync::Cache;
use std::io::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::trace_span;

/// SSTable reader cache (an LRU cache).
pub struct SSTableCache {
    metrics: Metrics,
    block_cache: Arc<BlockCache>,
    cache: Cache<String, Result<Arc<SSTableReader>, Arc<Error>>>, // Key = file_path
}

impl SSTableCache {
    /// Create a new SSTableCache
    pub fn new(metric_registry: &mut MetricRegistry, options: &Options) -> Self {
        let cache_size = options.max_open_files() as u64;
        let cache = Cache::new(cache_size);
        let metrics = Metrics::new(cache.clone());
        metrics.register_to(metric_registry);
        tracing::info!(capacity = cache_size, "SSTableCache initialized");

        let block_cache = BlockCache::new(metric_registry, options);

        Self {
            metrics,
            block_cache,
            cache,
        }
    }

    /// Retrieve the specified SSTableReader, creating it by reading the file on disk if necessary
    pub fn get(&self, file: &Path) -> Result<Arc<SSTableReader>, Error> {
        let key = file.to_string_lossy().into_owned();
        let _span = trace_span!("sstable_cache.get", file = %key).entered();

        let mut miss = false;

        // Fetch or insert the sstable reader in a thread-safe manner
        let sstable_reader = self.cache.get_with(key.clone(), || {
            let start = Instant::now();

            let reader = SSTableReader::open(self.block_cache.clone(), &file);

            let duration = start.elapsed().as_millis();
            miss = true;

            if let Err(e) = reader {
                tracing::error!(file = %key, error = %e, "Failed to load SSTable");
                return Err(Arc::new(e));
            }
            tracing::trace!(file = %key, duration_ms = duration, "sstable load done");
            Ok(Arc::new(reader?))
        });

        if miss {
            self.metrics.misses.inc();
        } else {
            tracing::trace!(file = %key, "sstable cache hit");
            self.metrics.hits.inc();
        }

        sstable_reader.map_err(|arc_err| Error::new(arc_err.kind(), arc_err.to_string()))
        // Clone so each thread gets its own Result<Arc<Vec<u8>>>
    }

    pub fn evict(&self, file: &Path) {
        let key = file.to_string_lossy().into_owned();
        self.cache.invalidate(&key);
        tracing::trace!(file = %key, "sstable cache evict");
    }
}

struct Metrics {
    /// The number of open sstables (stored in the SSTableCache)
    sstables_open_count: Arc<DerivedGauge>,

    /// Tracks the number of sstable cache hit
    hits: Arc<Counter>,

    /// Tracks the number of sstable cache miss
    misses: Arc<Counter>,

    /// The ratio of SSTable cache hits to the total number of lookups (hits + misses)
    hit_ratio: Arc<HitRatio>,
}

impl Metrics {
    fn new(cache: Cache<String, Result<Arc<SSTableReader>, Arc<Error>>>) -> Metrics {
        let hits = Counter::new();
        let misses = Counter::new();

        Self {
            sstables_open_count: DerivedGauge::new(Arc::new(move || cache.entry_count())),
            hits: hits.clone(),
            misses: misses.clone(),
            hit_ratio: HitRatio::new(hits, misses),
        }
    }

    fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_gauge(
                metrics::names::sstable_cache::OPEN_COUNT,
                self.sstables_open_count.clone(),
            )
            .register_counter(metrics::names::sstable_cache::HITS, self.hits.clone())
            .register_counter(metrics::names::sstable_cache::MISSES, self.misses.clone())
            .register_computed(
                metrics::names::sstable_cache::HIT_RATIO,
                self.hit_ratio.clone(),
            );
    }
}
