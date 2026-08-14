//! Typed access to QuokkaDB runtime metrics.
//!
//! Use [`crate::QuokkaDB::metrics`] to obtain a facade over the metrics
//! currently registered by the database:
//!
//! ```ignore
//! let metrics = db.metrics();
//! let hits = metrics.block_cache().hits();
//! let misses = metrics.block_cache().misses();
//! let wal_files = metrics.wal().files();
//! ```
//!
//! Metrics are grouped by source component. Scalar metrics return plain values.
//! Histogram metrics return [`HistogramMetrics`], which exposes derived values
//! such as count, mean, quantiles, and buckets.
//!
//! The accessors below document the semantics of each metric directly on the
//! public API so callers do not need to inspect internal storage components.

use crate::obs::metrics::{self as obs_metrics, MetricRegistry};
use std::sync::Arc;

/// Root typed facade for runtime metrics exposed by a [`crate::QuokkaDB`].
pub struct Metrics<'a> {
    pub(crate) registry: &'a MetricRegistry,
}

/// Metrics for the block cache.
pub struct BlockCacheMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Metrics for the SSTable reader cache.
pub struct SSTableCacheMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Metrics for the write-ahead log.
pub struct WalMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Metrics for background flushes.
pub struct FlushMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Metrics for the manifest.
pub struct ManifestMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Storage-engine level metrics.
pub struct StorageMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Metrics for compaction picking and activity.
pub struct CompactionMetrics<'a> {
    registry: &'a MetricRegistry,
}

/// Read-only view over a histogram metric.
pub struct HistogramMetrics {
    histogram: Arc<obs_metrics::Histogram>,
}

impl<'a> Metrics<'a> {
    /// Returns block-cache metrics.
    pub fn block_cache(&self) -> BlockCacheMetrics<'a> {
        BlockCacheMetrics {
            registry: self.registry,
        }
    }

    /// Returns SSTable-cache metrics.
    pub fn sstable_cache(&self) -> SSTableCacheMetrics<'a> {
        SSTableCacheMetrics {
            registry: self.registry,
        }
    }

    /// Returns write-ahead-log metrics.
    pub fn wal(&self) -> WalMetrics<'a> {
        WalMetrics {
            registry: self.registry,
        }
    }

    /// Returns flush metrics.
    pub fn flush(&self) -> FlushMetrics<'a> {
        FlushMetrics {
            registry: self.registry,
        }
    }

    /// Returns manifest metrics.
    pub fn manifest(&self) -> ManifestMetrics<'a> {
        ManifestMetrics {
            registry: self.registry,
        }
    }

    /// Returns storage-engine metrics.
    pub fn storage(&self) -> StorageMetrics<'a> {
        StorageMetrics {
            registry: self.registry,
        }
    }

    /// Returns compaction metrics.
    pub fn compaction(&self) -> CompactionMetrics<'a> {
        CompactionMetrics {
            registry: self.registry,
        }
    }
}

impl<'a> BlockCacheMetrics<'a> {
    /// Returns the current block-cache size in bytes.
    pub fn size(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::block_cache::SIZE)
    }

    /// Returns the number of block-cache hits since the database was opened.
    pub fn hits(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::block_cache::HITS)
    }

    /// Returns the number of block-cache misses since the database was opened.
    pub fn misses(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::block_cache::MISSES)
    }

    /// Returns the block-cache hit ratio in the range `[0.0, 1.0]`.
    pub fn hit_ratio(&self) -> f64 {
        self.registry
            .computed_value(obs_metrics::names::block_cache::HIT_RATIO)
    }

    /// Returns the number of block-cache evictions since the database was opened.
    pub fn evictions(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::block_cache::EVICTIONS)
    }
}

impl<'a> SSTableCacheMetrics<'a> {
    /// Returns the number of SSTable readers currently kept open in the cache.
    pub fn open_count(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::sstable_cache::OPEN_COUNT)
    }

    /// Returns the number of SSTable-cache hits since the database was opened.
    pub fn hits(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::sstable_cache::HITS)
    }

    /// Returns the number of SSTable-cache misses since the database was opened.
    pub fn misses(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::sstable_cache::MISSES)
    }

    /// Returns the SSTable-cache hit ratio in the range `[0.0, 1.0]`.
    pub fn hit_ratio(&self) -> f64 {
        self.registry
            .computed_value(obs_metrics::names::sstable_cache::HIT_RATIO)
    }
}

impl<'a> WalMetrics<'a> {
    /// Returns the number of WAL files currently tracked, including rotated files
    /// that are still needed for recovery.
    pub fn files(&self) -> u64 {
        self.registry.gauge_value(obs_metrics::names::wal::FILES)
    }

    /// Returns the total size in bytes of all tracked WAL files.
    pub fn total_bytes(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::wal::TOTAL_BYTES)
    }

    /// Returns the number of WAL sync operations performed since the database was opened.
    pub fn syncs(&self) -> u64 {
        self.registry.counter_value(obs_metrics::names::wal::SYNCS)
    }

    /// Returns the number of bytes currently buffered in the WAL but not yet synced.
    pub fn bytes_buffered(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::wal::BYTES_BUFFERED)
    }

    /// Returns the total number of bytes written to WAL file descriptors.
    ///
    /// This measures bytes handed to the operating system, not guaranteed durable bytes.
    pub fn bytes_written(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::wal::BYTES_WRITTEN)
    }
}

impl<'a> FlushMetrics<'a> {
    /// Returns the number of memtable flushes completed since the database was opened.
    pub fn count(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::flush::COUNT)
    }

    /// Returns a histogram of flush durations measured in microseconds.
    pub fn duration(&self) -> HistogramMetrics {
        HistogramMetrics::new(self.registry.histogram(obs_metrics::names::flush::DURATION))
    }

    /// Returns a histogram of flush write throughput measured in bytes per second.
    pub fn write_throughput(&self) -> HistogramMetrics {
        HistogramMetrics::new(
            self.registry
                .histogram(obs_metrics::names::flush::WRITE_THROUGHPUT),
        )
    }

    /// Returns a histogram of flushed memtable sizes measured in bytes.
    pub fn memtable_size(&self) -> HistogramMetrics {
        HistogramMetrics::new(
            self.registry
                .histogram(obs_metrics::names::flush::MEMTABLE_SIZE),
        )
    }
}

impl<'a> ManifestMetrics<'a> {
    /// Returns the number of manifest rewrites triggered since the database was opened.
    pub fn rewrite(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::manifest::REWRITE)
    }

    /// Returns the number of manifest append operations performed since the database was opened.
    pub fn writes(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::manifest::WRITES)
    }

    /// Returns the current manifest size in bytes.
    pub fn size(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::manifest::SIZE)
    }

    /// Returns the total number of bytes written to manifest files since the database was opened.
    pub fn bytes_written(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::manifest::BYTES_WRITTEN)
    }
}

impl<'a> StorageMetrics<'a> {
    /// Returns the total number of SSTables currently present across all levels.
    pub fn sstable_count(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::storage::SSTABLE_COUNT)
    }

    /// Returns the number of SSTables currently present in the given level.
    pub fn sstable_count_at_level(&self, level: usize) -> u64 {
        self.registry
            .gauge_value(&obs_metrics::names::storage::sstable_count_level(level))
    }

    /// Returns the total size in bytes of all SSTables across all levels.
    pub fn total_sstable_size(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::storage::TOTAL_SSTABLE_SIZE)
    }

    /// Returns the total size in bytes of SSTables currently present in the given level.
    pub fn sstable_size_at_level(&self, level: usize) -> u64 {
        self.registry
            .gauge_value(&obs_metrics::names::storage::sstable_size_level(level))
    }

    /// Returns the current size in bytes of the active mutable memtable.
    pub fn memtable_size(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::storage::MEMTABLE_SIZE)
    }

    /// Returns the total size in bytes of the active memtable plus immutable memtables awaiting flush.
    pub fn memtable_total_size(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::storage::MEMTABLE_TOTAL_SIZE)
    }

    /// Returns the number of memtables currently resident in memory.
    ///
    /// This includes the active mutable memtable and any immutable memtables awaiting flush.
    pub fn memtable_count(&self) -> u64 {
        self.registry
            .gauge_value(obs_metrics::names::storage::MEMTABLE_COUNT)
    }
}

impl<'a> CompactionMetrics<'a> {
    /// Returns the number of compaction jobs selected since the database was opened.
    pub fn jobs_picked(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::compaction::JOBS_PICKED)
    }

    /// Returns the number of full compaction jobs selected since the database was opened.
    pub fn jobs_picked_full(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::compaction::JOBS_PICKED_FULL)
    }

    /// Returns the number of partial compaction jobs selected since the database was opened.
    pub fn jobs_picked_partial(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::compaction::JOBS_PICKED_PARTIAL)
    }

    /// Returns the number of compaction candidates skipped because a level was already compacting.
    pub fn jobs_skipped_level_compacting(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::compaction::JOBS_SKIPPED_LEVEL_COMPACTING)
    }

    /// Returns the number of compaction candidates skipped because of range overlap.
    pub fn jobs_skipped_range_overlap(&self) -> u64 {
        self.registry
            .counter_value(obs_metrics::names::compaction::JOBS_SKIPPED_RANGE_OVERLAP)
    }

    /// Returns a histogram of the number of input files selected per compaction job.
    pub fn input_files_count(&self) -> HistogramMetrics {
        HistogramMetrics::new(
            self.registry
                .histogram(obs_metrics::names::compaction::INPUT_FILES_COUNT),
        )
    }

    /// Returns the number of compaction jobs picked from the given input level.
    pub fn picked_at_level(&self, level: usize) -> u64 {
        self.registry
            .counter_value(&obs_metrics::names::compaction::picked_level(level))
    }

    /// Returns the current compaction score for the given level multiplied by 100.
    ///
    /// For example, a returned value of `135` corresponds to a score of `1.35`.
    pub fn score_at_level(&self, level: usize) -> u64 {
        self.registry
            .gauge_value(&obs_metrics::names::compaction::score_level(level))
    }

    /// Returns the number of active compactions currently involving the given level.
    pub fn active_at_level(&self, level: usize) -> u64 {
        self.registry
            .gauge_value(&obs_metrics::names::compaction::active_level(level))
    }
}

impl HistogramMetrics {
    pub(crate) fn new(histogram: Arc<obs_metrics::Histogram>) -> Self {
        Self { histogram }
    }

    /// Returns the number of recorded samples.
    pub fn count(&self) -> u64 {
        self.histogram.snapshot().count
    }

    /// Returns the sum of all recorded samples.
    pub fn sum(&self) -> u64 {
        self.histogram.snapshot().sum
    }

    /// Returns the minimum recorded sample.
    pub fn min(&self) -> u64 {
        self.histogram.snapshot().min
    }

    /// Returns the maximum recorded sample.
    pub fn max(&self) -> u64 {
        self.histogram.snapshot().max
    }

    /// Returns the arithmetic mean of recorded samples.
    pub fn mean(&self) -> f64 {
        self.histogram.snapshot().mean
    }

    /// Returns the standard deviation of recorded samples.
    pub fn stddev(&self) -> f64 {
        self.histogram.snapshot().stddev
    }

    /// Returns the estimated value for the requested quantile.
    ///
    /// The returned estimate is the upper bound of the first bucket that satisfies
    /// the requested quantile.
    pub fn quantile(&self, quantile: f64) -> Option<u64> {
        self.histogram.estimate_quantile(quantile)
    }

    /// Returns bucket counts as `(upper_bound, count)` pairs.
    pub fn buckets(&self) -> Vec<(u64, u64)> {
        self.histogram.snapshot().buckets
    }
}

#[cfg(test)]
mod tests {
    use crate::QuokkaDB;
    use tempfile::tempdir;

    #[test]
    fn metrics_api_exposes_typed_component_accessors() {
        let dir = tempdir().unwrap();
        let db = QuokkaDB::open(dir.path()).unwrap();

        let metrics = db.metrics();
        assert_eq!(metrics.block_cache().hits(), 0);
        assert_eq!(metrics.block_cache().misses(), 0);
        assert_eq!(metrics.flush().count(), 0);
        assert_eq!(metrics.flush().duration().count(), 0);
        assert_eq!(metrics.wal().files(), 1);
        assert_eq!(metrics.storage().memtable_count(), 1);
    }
}
