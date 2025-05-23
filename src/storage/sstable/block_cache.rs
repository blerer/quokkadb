use crate::io::checksum::ChecksumStrategy;
use crate::io::compressor::Compressor;
use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::obs::metrics::{Counter, DerivedGaugeU64, HitRatioGauge, MetricRegistry};
use crate::options::options::DatabaseOptions;
use crate::storage::sstable::sstable_reader::SharedFile;
use crate::storage::sstable::BlockHandle;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use std::io::Error;
use std::sync::Arc;
use std::time::Instant;
use crate::{error, event, info};

/// Block Cache (LRU, retrieves blocks using the shared file provided as a get parameter)
pub struct BlockCache {
    logger: Arc<dyn LoggerAndTracer>,
    metrics: Metrics,
    cache: Cache<(String, u64), Result<Arc<Vec<u8>>, Arc<Error>>>, // Key = (file_path, block_handle)
}

impl BlockCache {
    /// Create a new Block Cache
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: &DatabaseOptions,
    ) -> Arc<Self> {
        let cache_size = options.block_cache_size.to_bytes() as u64;
        let evictions_counter = Counter::new();
        let cache = Cache::builder()
            .max_capacity(cache_size)
            .weigher(
                |_: &(String, u64), block: &Result<Arc<Vec<u8>>, Arc<Error>>| {
                    match block {
                        Ok(block) => block.len() as u32,
                        Err(_) => 1, // Errors take minimal space
                    }
                },
            )
            .eviction_listener(Self::eviction_listener(
                logger.clone(),
                evictions_counter.clone(),
            ))
            .build();

        let cache_ptr = cache.clone();
        let size_gauge = DerivedGaugeU64::new(Arc::new(move || cache_ptr.weighted_size()));

        let metrics = Metrics::new(size_gauge, evictions_counter);
        metrics.register_to(metric_registry);

        info!(logger, "BlockCache initialized with {} bytes", cache_size);

        Arc::new(Self {
            logger,
            metrics,
            cache,
        })
    }

    /// Retrieve the specified block, loading it from disk if necessary
    pub fn get(
        &self,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        file: &SharedFile,
        block_handle: &BlockHandle,
    ) -> Result<Arc<Vec<u8>>, Error> {
        let key = (
            file.path.to_string_lossy().into_owned(),
            block_handle.offset,
        );

        // Fetch or insert the block in a thread-safe manner
        let block = self.cache.get_with(key.clone(), || {
            let path = file.path.to_string_lossy();
            let start = Instant::now();
            event!(self.logger, "block_load start file={} offset={} size={}", path, block_handle.offset, block_handle.size);

            // Read, decompress, and validate the block
            let uncompressed_block =
                self.read_and_decompress(compressor, checksum_strategy, file, block_handle);

            if let Err(e) = uncompressed_block {
                error!(self.logger, "Error loading block from {} at offset {}: {}", path, block_handle.offset, e);
                return Err(Arc::new(e));
            }

            let uncompressed_block = uncompressed_block?;
            let duration = start.elapsed();
            event!(self.logger,
                "block_load end, size={}, duration={}ms",
                uncompressed_block.len(),
                duration.as_millis()
            );

            Ok(Arc::new(uncompressed_block))
        });

        block.map_err(|arc_err| Error::new(arc_err.kind(), arc_err.to_string()))
        // Clone so each thread gets its own Result<Arc<Vec<u8>>>
    }

    fn read_and_decompress(
        &self,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        file: &SharedFile,
        block_handle: &BlockHandle,
    ) -> Result<Vec<u8>, Error> {
        let compressed_block = file.read_block(block_handle.offset, block_handle.size as usize)?;
        let mut uncompressed_block = compressor.decompress(&compressed_block)?;
        let checksum_offset = uncompressed_block.len() - checksum_strategy.checksum_size();
        checksum_strategy.verify_checksum(
            &uncompressed_block[..checksum_offset],
            &uncompressed_block[checksum_offset..],
        )?;
        uncompressed_block.truncate(checksum_offset);
        Ok(uncompressed_block)
    }

    fn eviction_listener(
        logger: Arc<dyn LoggerAndTracer>,
        evictions_counter: Arc<Counter>,
    ) -> impl Fn(Arc<(String, u64)>, Result<Arc<Vec<u8>>, Arc<Error>>, RemovalCause) {
        move |key, _value, cause| {
            event!(logger, "evict_block file={} offset={} cause={:?}", key.0, key.1, cause);
            evictions_counter.inc();
        }
    }
}

/// The Block cache metrics
struct Metrics {
    /// The current size of the block cache in bytes.
    size: Arc<DerivedGaugeU64>,

    /// Tracks the number of block cache hit
    hits: Arc<Counter>,

    /// Tracks the number of block cache miss
    misses: Arc<Counter>,

    /// The ratio of Block cache hits to the total number of lookups (hits + misses)
    hit_ratio: Arc<HitRatioGauge>,

    /// Number of blocks evicted from the cache.
    evictions: Arc<Counter>,
}

impl Metrics {
    fn new(size: Arc<DerivedGaugeU64>, evictions: Arc<Counter>) -> Metrics {
        let hits = Counter::new();
        let misses = Counter::new();

        Self {
            size,
            hits: hits.clone(),
            misses: misses.clone(),
            hit_ratio: HitRatioGauge::new(hits, misses),
            evictions,
        }
    }

    fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_derived_gauge("block_cache_size", &self.size)
            .register_counter("block_cache_hit", &self.hits)
            .register_counter("block_cache_miss", &self.misses)
            .register_hit_ratio_gauge("block_cache_hit_ratio", &self.hit_ratio)
            .register_counter("block_cache_evictions", &self.evictions);
    }
}
