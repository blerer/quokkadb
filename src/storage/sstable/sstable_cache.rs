use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::obs::metrics::{Counter, DerivedGaugeU64, HitRatioGauge, MetricRegistry};
use crate::options::options::DatabaseOptions;
use crate::storage::sstable::block_cache::BlockCache;
use crate::storage::sstable::sstable_reader::SSTableReader;
use moka::sync::Cache;
use std::io::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use crate::{error, event, info};

/// SSTable reader cache (an LRU cache).
pub struct SSTableCache {
    logger: Arc<dyn LoggerAndTracer>,
    metrics: Metrics,
    block_cache: Arc<BlockCache>,
    cache: Cache<String, Result<Arc<SSTableReader>, Arc<Error>>>, // Key = file_path
}

impl SSTableCache {
    /// Create a new SSTableCache
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: &DatabaseOptions,
    ) -> Self {
        let cache_size = options.max_open_files as u64;
        let cache = Cache::new(cache_size);
        let metrics = Metrics::new(cache.clone());
        metrics.register_to(metric_registry);
        info!(logger, "SSTableCache initialized with capacity={}", cache_size);

        let block_cache = BlockCache::new(logger.clone(), metric_registry, options);

        Self {
            logger,
            metrics,
            block_cache,
            cache,
        }
    }

    /// Retrieve the specified SSTableReader, creating it by reading the file on disk if necessary
    pub fn get(&self, file: &Path) -> Result<Arc<SSTableReader>, Error> {

        let key = file.to_string_lossy().into_owned();

        event!(self.logger, "get start file={}", &key);

        let mut miss = false;

        // Fetch or insert the sstable reader in a thread-safe manner
        let sstable_reader = self.cache.get_with(key.clone(), || {
            event!(self.logger, "load start file={}", &key);

            let start = Instant::now();

            let reader = SSTableReader::open(self.logger.clone(), self.block_cache.clone(), &file);

            let duration = start.elapsed().as_millis();
             miss = true;

            if let Err(e) = reader {
                error!(self.logger, "Failed to load SSTable from file={}: {}", &key, e);
                event!(self.logger, "load error file={} error={}", &key, e);
                return Err(Arc::new(e));
            }
            event!(self.logger, "load finish file={} duration={}ms", &key, duration);
            Ok(Arc::new(reader?))
        });

        if miss {
            self.metrics.misses.inc();
        } else {
            event!(self.logger, "hit, file={}", &key);
            self.metrics.hits.inc();
        }

        sstable_reader.map_err(|arc_err| Error::new(arc_err.kind(), arc_err.to_string()))
        // Clone so each thread gets its own Result<Arc<Vec<u8>>>
    }
}

struct Metrics {
    /// The number of open sstables (stored in the SSTableCache)
    sstables_open_count: Arc<DerivedGaugeU64>,

    /// Tracks the number of sstable cache hit
    hits: Arc<Counter>,

    /// Tracks the number of sstable cache miss
    misses: Arc<Counter>,

    /// The ratio of SSTable cache hits to the total number of lookups (hits + misses)
    hit_ratio: Arc<HitRatioGauge>,
}

impl Metrics {
    fn new(cache: Cache<String, Result<Arc<SSTableReader>, Arc<Error>>>) -> Metrics {
        let hits = Counter::new();
        let misses = Counter::new();

        Self {
            sstables_open_count: DerivedGaugeU64::new(Arc::new(move || cache.entry_count())),
            hits: hits.clone(),
            misses: misses.clone(),
            hit_ratio: HitRatioGauge::new(hits, misses),
        }
    }

    fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_derived_gauge("sstables_open_count", &self.sstables_open_count)
            .register_counter("sstable_cache_hit", &self.hits)
            .register_counter("sstable_cache_miss", &self.misses)
            .register_hit_ratio_gauge("sstable_cache_hit_ratio", &self.hit_ratio);
    }
}
