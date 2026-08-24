use crate::error::{Error, Result};
use crate::io::compressor::CompressorType;
use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
use std::fmt;

/// Controls how acknowledged WAL writes are propagated from QuokkaDB buffers to the OS and disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalDurability {
    /// Acknowledged writes are durable before the call returns.
    Durable,
    /// Acknowledged writes survive a process crash, but recent writes may still be lost on an OS
    /// crash or power loss until the next periodic sync.
    ProcessSafe,
    /// Acknowledged writes may still be sitting in QuokkaDB's userspace buffer and can be lost on
    /// a normal process crash.
    Buffered,
}

/// Top-level configuration struct containing all database tuning options.
///
/// Use one of the preset profiles (`lightweight()`, `optimized()`, `high_query_load()`)
/// and customize with builder methods as needed.
#[derive(Clone, Debug)]
pub struct Options {
    // I/O and resource options
    file_write_buffer_size: StorageQuantity,
    max_open_files: u32,
    block_cache_size: StorageQuantity,
    query_cache_size: StorageQuantity,
    wal_durability: WalDurability,
    wal_bytes_per_sync: StorageQuantity,
    max_manifest_file_size: StorageQuantity,

    // LSM compaction options
    max_levels: usize,
    level0_file_num_compaction_trigger: usize,
    max_bytes_for_level_base: StorageQuantity,
    max_bytes_for_level_multiplier: f64,
    compaction_threads: usize,
    max_target_file_size: StorageQuantity,

    // SSTable format options
    block_size: StorageQuantity,
    restart_interval: usize,
    bloom_filter_false_positive: f64,
    compressor_type: CompressorType,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            // I/O and resource defaults
            file_write_buffer_size: StorageQuantity::new(1, StorageUnit::Mebibytes),
            max_open_files: 200,
            block_cache_size: StorageQuantity::new(4, StorageUnit::Mebibytes),
            query_cache_size: StorageQuantity::new(4, StorageUnit::Mebibytes),
            wal_durability: WalDurability::Durable,
            wal_bytes_per_sync: StorageQuantity::new(256, StorageUnit::Kibibytes),
            max_manifest_file_size: StorageQuantity::new(256, StorageUnit::Kibibytes),

            // LSM compaction defaults
            max_levels: 3,
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: StorageQuantity::new(64, StorageUnit::Mebibytes),
            max_bytes_for_level_multiplier: 10.0,
            compaction_threads: 1,
            max_target_file_size: StorageQuantity::new(128, StorageUnit::Mebibytes),

            // SSTable format defaults
            block_size: StorageQuantity::new(4, StorageUnit::Kibibytes),
            restart_interval: 8,
            bloom_filter_false_positive: 0.01,
            compressor_type: CompressorType::LZ4,
        }
    }
}

// Preset profiles
impl Options {
    /// Lightweight profile: small memory footprint, minimal syncs. Similar to SQLite default config.
    pub fn lightweight() -> Self {
        Self::default()
    }

    /// Optimized profile: better caching and throughput. Comparable to tuned SQLite with PRAGMAs.
    pub fn optimized() -> Self {
        Self {
            file_write_buffer_size: StorageQuantity::new(4, StorageUnit::Mebibytes),
            max_open_files: 512,
            block_cache_size: StorageQuantity::new(128, StorageUnit::Mebibytes),
            query_cache_size: StorageQuantity::new(16, StorageUnit::Mebibytes),
            wal_durability: WalDurability::Durable,
            wal_bytes_per_sync: StorageQuantity::new(1, StorageUnit::Mebibytes),
            max_manifest_file_size: StorageQuantity::new(1, StorageUnit::Mebibytes),
            max_levels: 4,
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: StorageQuantity::new(256, StorageUnit::Mebibytes),
            max_bytes_for_level_multiplier: 10.0,
            compaction_threads: 2,
            block_size: StorageQuantity::new(8, StorageUnit::Kibibytes),
            restart_interval: 16,
            bloom_filter_false_positive: 0.005,
            compressor_type: CompressorType::LZ4,
            max_target_file_size: StorageQuantity::new(512, StorageUnit::Mebibytes),
        }
    }

    /// High query load profile: aggressive caching and frequent WAL syncs.
    pub fn high_query_load() -> Self {
        Self {
            file_write_buffer_size: StorageQuantity::new(8, StorageUnit::Mebibytes),
            max_open_files: 1024,
            block_cache_size: StorageQuantity::new(512, StorageUnit::Mebibytes),
            query_cache_size: StorageQuantity::new(64, StorageUnit::Mebibytes),
            wal_durability: WalDurability::ProcessSafe,
            wal_bytes_per_sync: StorageQuantity::new(512, StorageUnit::Kibibytes),
            max_manifest_file_size: StorageQuantity::new(2, StorageUnit::Mebibytes),
            max_levels: 5,
            level0_file_num_compaction_trigger: 8,
            max_bytes_for_level_base: StorageQuantity::new(512, StorageUnit::Mebibytes),
            max_bytes_for_level_multiplier: 10.0,
            compaction_threads: 2,
            block_size: StorageQuantity::new(16, StorageUnit::Kibibytes),
            restart_interval: 32,
            bloom_filter_false_positive: 0.001,
            compressor_type: CompressorType::LZ4,
            max_target_file_size: StorageQuantity::new(512, StorageUnit::Mebibytes),
        }
    }
}

// Builder methods
impl Options {
    /// Override the file write buffer size.
    pub fn with_file_write_buffer_size(mut self, size: StorageQuantity) -> Self {
        self.file_write_buffer_size = size;
        self
    }

    /// Override the max number of open files.
    pub fn with_max_open_files(mut self, count: u32) -> Self {
        self.max_open_files = count;
        self
    }

    /// Override the block cache size.
    pub fn with_block_cache_size(mut self, size: StorageQuantity) -> Self {
        self.block_cache_size = size;
        self
    }

    /// Override the query cache size.
    pub fn with_query_cache_size(mut self, size: StorageQuantity) -> Self {
        self.query_cache_size = size;
        self
    }

    /// Override WAL durability behavior.
    pub fn with_wal_durability(mut self, durability: WalDurability) -> Self {
        self.wal_durability = durability;
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
    pub fn with_max_levels(mut self, levels: usize) -> Self {
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

    /// Override the number of threads used for compaction.
    pub fn with_compaction_threads(mut self, threads: usize) -> Self {
        self.compaction_threads = threads;
        self
    }

    /// Override SSTable block size.
    pub fn with_block_size(mut self, size: StorageQuantity) -> Self {
        self.block_size = size;
        self
    }

    /// Override the number of keys between restart points in SSTable blocks.
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

    /// Override the maximum target file size for compactions. This is an upper bound on the
    /// size of SSTables produced by compactions, regardless of the level.
    /// It can be used to prevent excessively large SSTables in deeper levels when the level size
    /// multiplier is high.
    pub fn with_max_target_file_size(mut self, size: StorageQuantity) -> Self {
        self.max_target_file_size = size;
        self
    }
}

// Accessors
impl Options {
    pub fn file_write_buffer_size(&self) -> StorageQuantity {
        self.file_write_buffer_size
    }

    pub fn max_open_files(&self) -> u32 {
        self.max_open_files
    }

    pub fn block_cache_size(&self) -> StorageQuantity {
        self.block_cache_size
    }

    pub fn query_cache_size(&self) -> StorageQuantity {
        self.query_cache_size
    }

    pub fn wal_durability(&self) -> WalDurability {
        self.wal_durability
    }

    pub fn wal_bytes_per_sync(&self) -> StorageQuantity {
        self.wal_bytes_per_sync
    }

    pub fn max_manifest_file_size(&self) -> StorageQuantity {
        self.max_manifest_file_size
    }

    pub fn max_levels(&self) -> usize {
        self.max_levels
    }

    pub fn level0_file_num_compaction_trigger(&self) -> usize {
        self.level0_file_num_compaction_trigger
    }

    pub fn max_bytes_for_level_base(&self) -> StorageQuantity {
        self.max_bytes_for_level_base
    }

    pub fn max_bytes_for_level_multiplier(&self) -> f64 {
        self.max_bytes_for_level_multiplier
    }

    pub fn max_target_file_size(&self) -> StorageQuantity {
        self.max_target_file_size
    }

    /// Returns the target SSTable size, in bytes, for the given LSM level.
    ///
    /// The base size is derived from the existing compaction options instead of
    /// introducing another independent knob:
    ///
    /// ```text
    /// base_target_file_size = max_bytes_for_level_base / level0_file_num_compaction_trigger
    /// ```
    ///
    /// `max_bytes_for_level_base` is the target size of L1. Dividing it by the
    /// L0 compaction trigger gives the size of the files/partitions that should
    /// approximately fill L1 after an L0 -> L1 compaction. L0 and L1 therefore
    /// use the same base target size.
    ///
    /// For deeper levels, the target file size grows by the square root of the
    /// level-size multiplier:
    ///
    /// ```text
    /// file_size_multiplier = sqrt(max_bytes_for_level_multiplier)
    /// ```
    ///
    /// This is a compromise between two extremes:
    ///
    /// * If file sizes did not grow, deeper levels would contain many small
    ///   SSTables, increasing metadata, manifest, cache and file-descriptor
    ///   pressure.
    /// * If file sizes grew at the same rate as the level size, each level would
    ///   keep roughly the same number of SSTables, but deep compactions would
    ///   become very coarse and bursty.
    ///
    /// Using the square root lets deeper levels use larger SSTables while still
    /// allowing the number of SSTables per level to grow gradually. With the
    /// default level multiplier of 10, file sizes grow by about 3.16x per level
    /// and the number of SSTables grows by about 3.16x per level as well.
    ///
    /// Level numbering follows the rest of the LSM code:
    ///
    /// * `level == 0`: L0 target file size.
    /// * `level == 1`: L1 target file size.
    /// * `level >= 2`: deeper-level target file size.
    ///
    /// The returned value is always at least one block, so a very small
    /// configuration cannot produce SSTables smaller than the configured block
    /// size.
    pub(crate) fn target_file_size_for_level(&self, level: u8) -> u64 {
        assert_ne!(
            level, 0,
            "L0 file size is determined indirectly by file_write_buffer_size, not this method"
        );

        let base_target_file_size =
            self.max_bytes_for_level_base.to_bytes() / self.level0_file_num_compaction_trigger;

        let base_target_file_size = base_target_file_size.max(self.block_size.to_bytes());

        let file_size_multiplier = self.max_bytes_for_level_multiplier.sqrt();
        let exponent = (level - 1) as i32;
        let target = ((base_target_file_size as f64) * file_size_multiplier.powi(exponent)) as u64;

        target.min(self.max_target_file_size.to_bytes() as u64)
    }

    pub fn compaction_threads(&self) -> usize {
        self.compaction_threads
    }

    pub fn block_size(&self) -> StorageQuantity {
        self.block_size
    }

    pub fn restart_interval(&self) -> usize {
        self.restart_interval
    }

    pub fn bloom_filter_false_positive(&self) -> f64 {
        self.bloom_filter_false_positive
    }

    pub fn compressor_type(&self) -> CompressorType {
        self.compressor_type
    }
}

// Validation
impl Options {
    /// Validates all options and returns an error if any are invalid.
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
        if self.query_cache_size.to_bytes() == 0 {
            return Err(Error::InvalidOptions(
                "query_cache_size must be greater than 0".into(),
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
        if self.compaction_threads == 0 {
            return Err(Error::InvalidOptions(
                "compaction_threads must be greater than 0".into(),
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_options() -> Options {
        Options::default()
    }

    /// Returns the base target file size for the default Options.
    /// base = max_bytes_for_level_base / level0_file_num_compaction_trigger
    ///      = 64 MiB / 4 = 16 MiB
    /// That is larger than block_size (4 KiB), so the floor does not apply.
    fn default_base_bytes() -> u64 {
        let opts = base_options();
        (opts.max_bytes_for_level_base.to_bytes() / opts.level0_file_num_compaction_trigger) as u64
    }

    #[test]
    fn l1_returns_base_size() {
        let opts = base_options();
        let base = default_base_bytes();
        assert_eq!(opts.target_file_size_for_level(1), base);
    }

    #[test]
    fn query_cache_size_defaults_to_non_zero_value() {
        let opts = base_options();
        assert_eq!(
            opts.query_cache_size(),
            StorageQuantity::new(4, StorageUnit::Mebibytes)
        );
    }

    #[test]
    fn wal_durability_defaults_to_durable() {
        let opts = base_options();
        assert_eq!(opts.wal_durability(), WalDurability::Durable);
    }

    #[test]
    fn l2_scales_by_sqrt_of_multiplier() {
        let opts = base_options();
        let base = default_base_bytes();
        let expected = ((base as f64) * opts.max_bytes_for_level_multiplier.sqrt()) as u64;
        assert_eq!(opts.target_file_size_for_level(2), expected);
    }

    #[test]
    fn l3_scales_by_multiplier_itself() {
        // sqrt(m)^2 == m, so L3 = base * multiplier.
        // Raise max_target_file_size so the cap does not interfere:
        // base = 16 MiB, multiplier = 10, so L3 uncapped = ~160 MiB.
        let opts = base_options()
            .with_max_target_file_size(StorageQuantity::new(256, StorageUnit::Mebibytes));
        let base = default_base_bytes();
        let expected = ((base as f64) * opts.max_bytes_for_level_multiplier.sqrt().powi(2)) as u64;
        assert_eq!(opts.target_file_size_for_level(3), expected);
    }

    #[test]
    fn deeper_levels_are_monotonically_increasing() {
        let opts = base_options();
        let sizes: Vec<u64> = (1u8..=5)
            .map(|l| opts.target_file_size_for_level(l))
            .collect();
        for window in sizes.windows(2) {
            assert!(
                window[0] <= window[1],
                "sizes should be non-decreasing: {:?}",
                sizes
            );
        }
    }

    #[test]
    fn block_size_floor_is_applied() {
        // Make max_bytes_for_level_base tiny so base < block_size.
        let opts = Options::default()
            .with_max_bytes_for_level_base(StorageQuantity::new(1, StorageUnit::Kibibytes))
            .with_level0_file_num_compaction_trigger(4);
        // base = 1 KiB / 4 = 256 bytes, which is less than block_size (4 KiB).
        let block = opts.block_size().to_bytes() as u64;
        assert_eq!(opts.target_file_size_for_level(1), block);
    }

    #[test]
    fn max_target_file_size_caps_deep_levels() {
        // Set a very small cap so it is reached quickly.
        let cap = StorageQuantity::new(20, StorageUnit::Mebibytes);
        let opts = Options::default().with_max_target_file_size(cap);
        let cap_bytes = cap.to_bytes() as u64;
        // L3 with default multiplier = base * 10 = 160 MiB, well above 20 MiB.
        assert_eq!(opts.target_file_size_for_level(3), cap_bytes);
    }

    #[test]
    #[should_panic(expected = "L0 file size is determined indirectly by file_write_buffer_size")]
    fn panics_on_level_zero() {
        base_options().target_file_size_for_level(0);
    }

    #[test]
    fn validate_rejects_zero_query_cache_size() {
        let err = Options::default()
            .with_query_cache_size(StorageQuantity::new(0, StorageUnit::Bytes))
            .validate()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid options: query_cache_size must be greater than 0"
        );
    }
}

impl fmt::Display for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Database Options:")?;
        writeln!(
            f,
            "  File Write Buffer Size: {:?}",
            self.file_write_buffer_size
        )?;
        writeln!(f, "  Max Open Files: {:?}", self.max_open_files)?;
        writeln!(f, "  Block Cache Size: {:?}", self.block_cache_size)?;
        writeln!(f, "  Query Cache Size: {:?}", self.query_cache_size)?;
        writeln!(f, "  WAL Durability: {:?}", self.wal_durability)?;
        writeln!(f, "  WAL Bytes Per Sync: {:?}", self.wal_bytes_per_sync)?;
        writeln!(
            f,
            "  Max Manifest File Size: {:?}",
            self.max_manifest_file_size
        )?;
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
        )?;
        writeln!(f, "  Compaction Threads: {:?}", self.compaction_threads)?;
        writeln!(f, "SSTable Options:")?;
        writeln!(f, "  Block Size: {:?}", self.block_size)?;
        writeln!(f, "  Restart Interval: {:?}", self.restart_interval)?;
        writeln!(f, "  Bloom FPR: {:?}", self.bloom_filter_false_positive)?;
        writeln!(f, "  Compressor: {:?}", self.compressor_type)
    }
}
