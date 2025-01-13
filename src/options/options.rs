use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
use crate::util::compressor::CompressorType;

#[derive(Default)]
pub struct Options {
    database_options: Option<DatabaseOptions>,
    sstable_options: Option<SSTableOptions>,
}

impl Options {

    pub fn database_options(&self) -> &DatabaseOptions {
        static DEFAULT_DATABASE_OPTIONS: DatabaseOptions = DatabaseOptions { file_write_buffer_size: None};
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
}

impl DatabaseOptions {
    pub fn file_write_buffer_size(&self) -> StorageQuantity {
        self.file_write_buffer_size.unwrap_or(StorageQuantity::new(5, StorageUnit::Mebibytes))
    }
}

#[derive(Default)]
pub struct SSTableOptions {
    block_size: Option<StorageQuantity>,
    restart_interval: Option<usize>, // Number of keys between restart points.
    bloom_filter_false_positive: Option<f64>, // Desired false positive rate for the bloom filter.
    compressor_type: Option<CompressorType> // Compression strategy for blocks.
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


