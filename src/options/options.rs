use crate::error::{Error, Result};
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
                max_levels: 4,
                level0_file_num_compaction_trigger: 4,
                max_bytes_for_level_base: StorageQuantity::new(256, StorageUnit::Mebibytes),
                max_bytes_for_level_multiplier: 10.0,
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
                max_levels: 5,
                level0_file_num_compaction_trigger: 8,
                max_bytes_for_level_base: StorageQuantity::new(512, StorageUnit::Mebibytes),
                max_bytes_for_level_multiplier: 10.0,
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

impl Options {
    /// Validates all options and returns an error if any are invalid.
    pub fn validate(&self) -> Result<()> {
        self.db.validate()?;
        self.sst.validate()?;
        Ok(())
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

    /// The maximum number of levels in the LSM tree.
    pub max_levels: u8,

    /// Number of files in L0 that triggers compaction to L1.
    pub level0_file_num_compaction_trigger: usize,

    /// Target size for level 1 (base level). Higher levels derive their targets
    /// by multiplying this value by `max_bytes_for_level_multiplier` for each level.
    pub max_bytes_for_level_base: StorageQuantity,

    /// Multiplier used to compute target size for levels beyond L1.
    /// Target for level N = max_bytes_for_level_base * (multiplier ^ (N - 1)).
    pub max_bytes_for_level_multiplier: f64,
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

    /// Override the maximum number of levels in the LSM tree.
    pub fn with_max_levels(mut self, levels: u8) -> Self {
        self.max_levels = levels;
        self
    }

    /// Override the L0 file count compaction trigger.
    pub fn with_level0_file_num_compaction_trigger(mut self, count: usize) -> Self {
        self.level0_file_num_compaction_trigger = count;
        self
    }

    /// Override the target size for L1.
    pub fn with_max_bytes_for_level_base(mut self, size: StorageQuantity) -> Self {
        self.max_bytes_for_level_base = size;
        self
    }

    /// Override the level size multiplier.
    pub fn with_max_bytes_for_level_multiplier(mut self, multiplier: f64) -> Self {
        self.max_bytes_for_level_multiplier = multiplier;
        self
    }

    /// Validates all database options and returns an error if any are invalid.
    pub fn validate(&self) -> Result<()> {
        if self.file_write_buffer_size.to_bytes() == 0 {
            return Err(Error::InvalidOptions(
                "file_write_buffer_size must be greater than 0".into(),
            ));
        }
        if self.max_open_files == 0 {
            return Err(Error::InvalidOptions(
                "max_open_files must be greater than 0".into(),
            ));
        }
        if self.max_levels < 2 {
            return Err(Error::InvalidOptions(
                "max_levels must be at least 2 (L0 + at least one more level)".into(),
            ));
        }
        if self.level0_file_num_compaction_trigger == 0 {
            return Err(Error::InvalidOptions(
                "level0_file_num_compaction_trigger must be greater than 0".into(),
            ));
        }
        if self.max_bytes_for_level_base.to_bytes() == 0 {
            return Err(Error::InvalidOptions(
                "max_bytes_for_level_base must be greater than 0".into(),
            ));
        }
        if self.max_bytes_for_level_multiplier <= 1.0 {
            return Err(Error::InvalidOptions(
                "max_bytes_for_level_multiplier must be greater than 1.0".into(),
            ));
        }
        Ok(())
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
        writeln!(f, "  Max Manifest File Size: {:?}", self.max_manifest_file_size)?;
        writeln!(f, "  Max Levels: {:?}", self.max_levels)?;
        writeln!(
            f,
            "  L0 File Num Compaction Trigger: {:?}",
            self.level0_file_num_compaction_trigger
        )?;
        writeln!(
            f,
            "  Max Bytes for Level Base: {:?}",
            self.max_bytes_for_level_base
        )?;
        writeln!(
            f,
            "  Max Bytes for Level Multiplier: {:?}",
            self.max_bytes_for_level_multiplier
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
            max_levels: 3,
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: StorageQuantity::new(64, StorageUnit::Mebibytes),
            max_bytes_for_level_multiplier: 10.0,
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

    /// Validates all SSTable options and returns an error if any are invalid.
    pub fn validate(&self) -> Result<()> {
        if self.block_size.to_bytes() == 0 {
            return Err(Error::InvalidOptions(
                "block_size must be greater than 0".into(),
            ));
        }
        if self.restart_interval == 0 {
            return Err(Error::InvalidOptions(
                "restart_interval must be greater than 0".into(),
            ));
        }
        if self.bloom_filter_false_positive <= 0.0 || self.bloom_filter_false_positive >= 1.0 {
            return Err(Error::InvalidOptions(
                "bloom_filter_false_positive must be between 0.0 and 1.0 (exclusive)".into(),
            ));
        }
        Ok(())
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
