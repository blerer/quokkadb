use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
use crate::io::compressor::CompressorType;

#[derive(Default)]
pub struct Options {
    database_options: Option<DatabaseOptions>,
    sstable_options: Option<SSTableOptions>,
}

impl Options {

    pub fn database_options(&self) -> &DatabaseOptions {
        static DEFAULT_DATABASE_OPTIONS: DatabaseOptions = DatabaseOptions {
            file_write_buffer_size: None,
            max_open_files: None,
            block_cache_size: None,
            wal_bytes_per_sync: None,
            max_manifest_file_size: None,
        };
        self.database_options.as_ref().unwrap_or(&DEFAULT_DATABASE_OPTIONS)
    }

    pub fn sstable_options(&self) -> &SSTableOptions {
        static DEFAULT_SSTABLE_OPTIONS: SSTableOptions = SSTableOptions {
            block_size: None,
            restart_interval: None,
            bloom_filter_false_positive: None,
            compressor_type: None,
        };
        self.sstable_options.as_ref().unwrap_or(&DEFAULT_SSTABLE_OPTIONS)
    }
}

#[derive(Default)]
pub struct DatabaseOptions {
    file_write_buffer_size: Option<StorageQuantity>,

    /// The maximum number of file descriptors kept in the field descriptor cache.
    /// A negative number means not limit.
    max_open_files: Option<i32>,

    /// The block cache size.
    block_cache_size: Option<StorageQuantity>,

    /// Number of bytes written before the wal will issue a fsync.
    wal_bytes_per_sync: Option<StorageQuantity>,

    /// The size of the manifest file triggering a rotation.
    max_manifest_file_size: Option<StorageQuantity>,
}

impl DatabaseOptions {
    pub fn file_write_buffer_size(&self) -> StorageQuantity {
        self.file_write_buffer_size.unwrap_or(StorageQuantity::new(2, StorageUnit::Mebibytes))
    }

    pub fn max_open_files(&self) -> i32 {
        self.max_open_files.unwrap_or(200)
    }

    pub fn block_cache_size(&self) -> StorageQuantity {
        self.block_cache_size.unwrap_or(StorageQuantity::new(512, StorageUnit::Mebibytes))
    }

    pub fn wal_bytes_per_sync(&self) -> StorageQuantity {
        self.wal_bytes_per_sync.unwrap_or(StorageQuantity::new(512, StorageUnit::Mebibytes))
    }

    pub fn max_manifest_file_size(&self) -> StorageQuantity {
        self.wal_bytes_per_sync.unwrap_or(StorageQuantity::new(256, StorageUnit::Kibibytes)) // General embedded use. 128 KB for ultra-light
    }
}

pub struct CollectionOptions {}

#[derive(Default)]
pub struct SSTableOptions {

    block_size: Option<StorageQuantity>,

    /// Number of keys between restart points in SSTable blocks
    restart_interval: Option<usize>,

    /// Desired false positive rate for the bloom filter.
    bloom_filter_false_positive: Option<f64>,

    /// Compression strategy for SSTable blocks.
    compressor_type: Option<CompressorType>
}

impl SSTableOptions {

    pub fn block_size(&self) -> StorageQuantity {
        self.block_size.unwrap_or(StorageQuantity::new(4, StorageUnit::Kibibytes))
    }

    pub fn restart_interval(&self) -> usize {
        self.restart_interval.unwrap_or(8)
    }

    pub fn bloom_filter_false_positive(&self) -> f64 {
        self.bloom_filter_false_positive.unwrap_or(0.01)
    }

    pub fn compressor_type(&self) -> CompressorType {
        self.compressor_type.unwrap_or(CompressorType::Snappy)
    }
}


