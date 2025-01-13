use std::io::{Error, ErrorKind};
use std::time::SystemTime;
use bson::{from_slice, to_vec};
use serde::{Deserialize, Serialize};
use crate::storage::compound_key::CompoundKey;

const BSON_MIN_KEY: &[u8] = &[0x00]; // Represent BSON MinKey with the smallest possible binary
const BSON_MAX_KEY: &[u8] = &[0xFF]; // Represent BSON MaxKey with the largest possible binary

/// Represents the properties of an SSTable.
///
/// This struct encapsulates metadata about an SSTable, including its creation time,
/// key range, compression type, and various size metrics.
///
/// The properties are used to manage and query the SSTable effectively, providing
/// details such as the smallest and largest keys, sequence number bounds, and the sizes
/// of different components like data, index, and filters.
///
/// # Key Features
/// - Tracks the minimum and maximum user keys (`min_key` and `max_key`) stored in the SSTable.
/// - Maintains bounds for sequence numbers (`min_sequence` and `max_sequence`).
/// - Stores size-related statistics for efficient query planning and resource management.
/// - Supports serialization and deserialization for persistence.
///
/// This struct is designed to be compatible with BSON for key storage and comparison
#[derive(Debug, Serialize, Deserialize)]
pub struct SSTableProperties {
    /// The time when the SSTable was created.
    pub creation_time: SystemTime,

    /// The version of the SSTable format, used for compatibility.
    pub sstable_version: String,

    /// The type of compression used for the SSTable, represented as an 8-bit integer.
    pub compression_type: u8,

    /// The smallest user key present in the SSTable, initialized to BSON MinKey.
    pub min_key: Vec<u8>,

    /// The largest user key present in the SSTable, initialized to BSON MaxKey.
    pub max_key: Vec<u8>,

    /// The smallest sequence number among the entries in the SSTable,
    /// initialized to `u64::MAX`.
    pub min_sequence: u64,

    /// The largest sequence number among the entries in the SSTable,
    /// initialized to `u64::MIN`.
    pub max_sequence: u64,

    /// The total number of entries in the SSTable.
    pub num_entries: usize,

    /// The total size of all user keys in the SSTable, in bytes.
    pub raw_key_size: usize,

    /// The total size of all values in the SSTable, in bytes.
    pub raw_value_size: usize,

    /// The size of the data blocks in the SSTable, in bytes.
    pub data_size: usize,

    /// The size of the index blocks in the SSTable, in bytes.
    pub index_size: usize,

    /// The size of the filter blocks in the SSTable, in bytes.
    pub filter_size: usize,
}

impl SSTableProperties {

    /// Calculates the compression ratio for the SSTable.
    ///
    /// The compression ratio is defined as the ratio of the raw (uncompressed) size
    /// of keys and values to the actual size of the data blocks.
    ///
    /// # Panics
    /// Panics if the `data_size` is zero, as the compression ratio cannot be calculated.
    ///
    /// # Returns
    /// - `f64`: The compression ratio.
    pub fn compression_ratio(&self) -> f64 {
        assert!(self.data_size > 0, "Data size must be greater than zero to calculate compression ratio.");
        let uncompressed_size = self.raw_key_size + self.raw_value_size;
        uncompressed_size as f64 / self.data_size as f64
    }

    pub fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        to_vec(self).map_err(|e| Error::new(ErrorKind::Other, e))
    }

    pub fn from_slice(slice: &[u8]) -> std::io::Result<Self> {
        from_slice(slice).map_err(|e| Error::new(ErrorKind::Other, e))
    }
}

pub struct SSTablePropertiesBuilder {
    creation_time: SystemTime,
    sstable_version: String,
    compression_type: u8,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    min_sequence: u64,
    max_sequence: u64,
    num_entries: usize,
    raw_key_size: usize,
    raw_value_size: usize,
    data_size: usize,
    index_size: usize,
    filter_size: usize,
}

impl SSTablePropertiesBuilder {
    /// Creates a new `SSTablePropertiesBuilder` with default values.
    pub fn new(sstable_version: String, compression_type: u8) -> Self {
        Self {
            creation_time: SystemTime::now(),
            sstable_version,
            compression_type,
            min_key: BSON_MAX_KEY.to_vec(),
            max_key: BSON_MIN_KEY.to_vec(),
            min_sequence: u64::MAX,
            max_sequence: u64::MIN,
            num_entries: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            data_size: 0,
            index_size: 0,
            filter_size: 0,
        }
    }

    /// Updates the entry properties with the provided key, key size, and value size.
    pub fn with_entry(&mut self, key: &CompoundKey, key_size: usize, value_size: usize) -> &mut Self {
        self.num_entries += 1;
        self.raw_key_size += key_size;
        self.raw_value_size += value_size;

        if key.user_key < self.min_key {
            self.min_key = key.user_key.clone();
        }

        if key.user_key > self.max_key {
            self.max_key = key.user_key.clone();
        }

        let sequence = key.extract_sequence_number();
        self.min_sequence = self.min_sequence.min(sequence);
        self.max_sequence = self.max_sequence.max(sequence);

        self
    }

    /// Updates the size of the data block.
    pub fn with_data_block(&mut self, block_size: usize) -> &mut Self {
        self.data_size += block_size;
        self
    }

    /// Updates the size of the index block.
    pub fn with_index_block(&mut self, block_size: usize) -> &mut Self {
        self.index_size += block_size;
        self
    }

    /// Updates the size of the filter block.
    pub fn with_filter_block(&mut self, block_size: usize) -> &mut Self {
        self.filter_size += block_size;
        self
    }

    /// Builds the immutable `SSTableProperties` instance.
    pub fn build(&self) -> SSTableProperties {
        SSTableProperties {
            creation_time: self.creation_time,
            sstable_version: self.sstable_version.clone(),
            compression_type: self.compression_type,
            min_key: self.min_key.clone(),
            max_key: self.max_key.clone(),
            min_sequence: self.min_sequence,
            max_sequence: self.max_sequence,
            num_entries: self.num_entries,
            raw_key_size: self.raw_key_size,
            raw_value_size: self.raw_value_size,
            data_size: self.data_size,
            index_size: self.index_size,
            filter_size: self.filter_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compound_key::CompoundKey;

    #[test]
    fn test_update_entry_properties_with_builder() {
        let key1 = CompoundKey::new(vec![10, 20, 30], 1, crate::storage::operation::OperationType::Put);
        let key2 = CompoundKey::new(vec![5, 15, 25], 5, crate::storage::operation::OperationType::Put);
        let key3 = CompoundKey::new(vec![50, 60, 70], 3, crate::storage::operation::OperationType::Put);

        let mut builder = SSTablePropertiesBuilder::new("1.0".to_string(), 0);

        builder.with_entry(&key1, 10, 50)
               .with_entry(&key2, 15, 40)
               .with_entry(&key3, 12, 25);

        let sstable_properties = builder.build();

        // Validate the properties
        assert_eq!(sstable_properties.min_key, vec![5, 15, 25], "Min key should be [5, 15, 25]");
        assert_eq!(sstable_properties.max_key, vec![50, 60, 70], "Max key should be [50, 60, 70]");
        assert_eq!(sstable_properties.min_sequence, 1, "Min sequence should be 1");
        assert_eq!(sstable_properties.max_sequence, 5, "Max sequence should be 5");
        assert_eq!(sstable_properties.num_entries, 3, "Number of entries should be 3");
        assert_eq!(sstable_properties.raw_key_size, 37, "Raw key size should be 37 (10 + 15 + 12)");
        assert_eq!(sstable_properties.raw_value_size, 115, "Raw value size should be 115 (50 + 40 + 25)");
    }


    #[test]
    fn test_compression_ratio_valid_data() {
        let properties = SSTableProperties {
            raw_key_size: 1000,
            raw_value_size: 4000,
            data_size: 2000,
            ..SSTablePropertiesBuilder::new("1.0".to_string(), 0).build()
        };

        let ratio = properties.compression_ratio();
        assert!((ratio - 2.5).abs() < f64::EPSILON, "Expected compression ratio to be 2.5, got {}", ratio);
    }

    #[test]
    fn test_compression_ratio_no_compression() {
        let properties = SSTableProperties {
            raw_key_size: 1000,
            raw_value_size: 1000,
            data_size: 2000,
            ..SSTablePropertiesBuilder::new("1.0".to_string(), 0).build()
        };

        let ratio = properties.compression_ratio();
        assert!((ratio - 1.0).abs() < f64::EPSILON, "Expected compression ratio to be 1.0, got {}", ratio);
    }
}