use crate::io::compressor::CompressorType;
use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
use std::fmt;

/// Top-level configuration struct containing database and SSTable tuning options.
#[derive(Clone, Default)]
pub struct Options {
    pub db: DatabaseOptions,
    pub sst: SSTableOptions,
}

impl Options {
    /// Lightweight profile: small memory footprint, minimal syncs. Similar to SQLite default config.
    pub fn lightweight() -> Self {
        Self::default()
    }

    /// Optimized profile: better caching and throughput. Comparable to tuned SQLite with PRAGMAs.
    pub fn optimized() -> Self {
        Self {
            db: DatabaseOptions {
                file_write_buffer_size: StorageQuantity::new(4, StorageUnit::Mebibytes),
                max_open_files: 512,
                block_cache_size: StorageQuantity::new(128, StorageUnit::Mebibytes),
                wal_bytes_per_sync: StorageQuantity::new(1, StorageUnit::Mebibytes),
                max_manifest_file_size: StorageQuantity::new(1, StorageUnit::Mebibytes),
            },
            sst: SSTableOptions {
                block_size: StorageQuantity::new(8, StorageUnit::Kibibytes),
                restart_interval: 16,
                bloom_filter_false_positive: 0.005,
                compressor_type: CompressorType::LZ4,
            },
        }
    }

    /// High query load profile: aggressive caching and frequent WAL syncs.
    pub fn high_query_load() -> Self {
        Self {
            db: DatabaseOptions {
                file_write_buffer_size: StorageQuantity::new(8, StorageUnit::Mebibytes),
                max_open_files: 1024,
                block_cache_size: StorageQuantity::new(512, StorageUnit::Mebibytes),
                wal_bytes_per_sync: StorageQuantity::new(512, StorageUnit::Kibibytes),
                max_manifest_file_size: StorageQuantity::new(2, StorageUnit::Mebibytes),
            },
            sst: SSTableOptions {
                block_size: StorageQuantity::new(16, StorageUnit::Kibibytes),
                restart_interval: 32,
                bloom_filter_false_positive: 0.001,
                compressor_type: CompressorType::LZ4,
            },
        }
    }
}

impl fmt::Display for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Database Options:\n{}", self.db)?;
        writeln!(f, "SSTable Options:\n{}", self.sst)
    }
}

/// Database-level configuration settings.
#[derive(Clone)]
pub struct DatabaseOptions {
    pub file_write_buffer_size: StorageQuantity,

    /// The maximum number of file descriptors kept in the field descriptor cache.
    /// A negative number means not limit.
    pub max_open_files: u32,

    /// The block cache size.
    pub block_cache_size: StorageQuantity,

    /// Number of bytes written before the wal will issue a fsync.
    pub wal_bytes_per_sync: StorageQuantity,

    /// The size of the manifest file triggering a rotation.
    pub max_manifest_file_size: StorageQuantity,
}

impl DatabaseOptions {
    /// Override the block cache size.
    pub fn with_block_cache_size(mut self, size: StorageQuantity) -> Self {
        self.block_cache_size = size;
        self
    }

    /// Override the max number of open files.
    pub fn with_max_open_files(mut self, count: u32) -> Self {
        self.max_open_files = count;
        self
    }

    /// Override the file write buffer size.
    pub fn with_file_write_buffer_size(mut self, size: StorageQuantity) -> Self {
        self.file_write_buffer_size = size;
        self
    }

    /// Override WAL bytes per sync.
    pub fn with_wal_bytes_per_sync(mut self, size: StorageQuantity) -> Self {
        self.wal_bytes_per_sync = size;
        self
    }

    /// Override manifest file size limit.
    pub fn with_max_manifest_file_size(mut self, size: StorageQuantity) -> Self {
        self.max_manifest_file_size = size;
        self
    }
}

impl fmt::Display for DatabaseOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "  File Write Buffer Size: {:?}",
            self.file_write_buffer_size
        )?;
        writeln!(f, "  Max Open Files: {:?}", self.max_open_files)?;
        writeln!(f, "  Block Cache Size: {:?}", self.block_cache_size)?;
        writeln!(f, "  WAL Bytes Per Sync: {:?}", self.wal_bytes_per_sync)?;
        writeln!(
            f,
            "  Max Manifest File Size: {:?}",
            self.max_manifest_file_size
        )
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        DatabaseOptions {
            file_write_buffer_size: StorageQuantity::new(1, StorageUnit::Mebibytes),
            max_open_files: 200,
            block_cache_size: StorageQuantity::new(4, StorageUnit::Mebibytes),
            wal_bytes_per_sync: StorageQuantity::new(256, StorageUnit::Kibibytes),
            max_manifest_file_size: StorageQuantity::new(256, StorageUnit::Kibibytes),
        }
    }
}

/// SSTable-specific configuration settings.
#[derive(Clone)]
pub struct SSTableOptions {
    /// SSTable block size
    pub block_size: StorageQuantity,

    /// Number of keys between restart points in SSTable blocks
    pub restart_interval: usize,

    /// Desired false positive rate for the bloom filter.
    pub bloom_filter_false_positive: f64,

    /// Compression strategy for SSTable bloc
    pub compressor_type: CompressorType,
}

impl SSTableOptions {
    /// Override SSTable block size.
    pub fn with_block_size(mut self, size: StorageQuantity) -> Self {
        self.block_size = size;
        self
    }

    /// Override the number of keys between restart points in SSTable blocks
    pub fn with_restart_interval(mut self, interval: usize) -> Self {
        self.restart_interval = interval;
        self
    }

    /// Override bloom filter false positive rate.
    pub fn with_bloom_fpr(mut self, fpr: f64) -> Self {
        self.bloom_filter_false_positive = fpr;
        self
    }

    /// Override compressor type.
    pub fn with_compressor(mut self, comp: CompressorType) -> Self {
        self.compressor_type = comp;
        self
    }
}

impl Default for SSTableOptions {
    fn default() -> Self {
        SSTableOptions {
            block_size: StorageQuantity::new(4, StorageUnit::Kibibytes),
            restart_interval: 8,
            bloom_filter_false_positive: 0.01,
            compressor_type: CompressorType::Snappy,
        }
    }
}

impl fmt::Display for SSTableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "  Block Size: {:?}", self.block_size)?;
        writeln!(f, "  Restart Interval: {:?}", self.restart_interval)?;
        writeln!(f, "  Bloom FPR: {:?}", self.bloom_filter_false_positive)?;
        writeln!(f, "  Compressor: {:?}", self.compressor_type)
    }
}
