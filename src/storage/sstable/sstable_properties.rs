use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::invalid_data;
use crate::io::serializable::Serializable;
use crate::storage::count_stats::CountStats;
use crate::storage::internal_key::{extract_record_key, extract_sequence_number};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
#[derive(Debug, PartialEq, Eq)]
pub struct SSTableProperties {
    /// The time when the SSTable was created.
    pub creation_time: SystemTime,

    /// The version of the SSTable format, used for compatibility.
    pub sstable_version: u8,

    /// The type of compression used for the SSTable, represented as an 8-bit integer.
    pub compression_type: u8,

    /// The smallest record key present in the SSTable, initialized to BSON MinKey.
    pub min_key: Vec<u8>,

    /// The largest record key present in the SSTable, initialized to BSON MaxKey.
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
        compute_compression_ratio(self.raw_key_size, self.raw_value_size, self.data_size)
    }

    pub fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        let mut writer = ByteWriter::new();
        self.write_to(&mut writer);
        Ok(writer.take_buffer())
    }

    pub fn from_slice(slice: &[u8]) -> std::io::Result<Self> {
        let reader = ByteReader::new(slice);
        let properties = Self::read_from(&reader)?;
        if reader.has_remaining() {
            // Backward compatibility for the short-lived format that appended CountStats.
            let _ = CountStats::read_from(&reader)?;
        }
        if reader.has_remaining() {
            return Err(invalid_data("trailing bytes in SSTableProperties"));
        }
        Ok(properties)
    }
}

impl Serializable for SSTableProperties {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let creation_time = UNIX_EPOCH
            .checked_add(Duration::from_millis(reader.read_varint_u64()?))
            .ok_or_else(|| invalid_data("SSTableProperties creation_time overflow"))?;

        let sstable_version = reader.read_u8()?;
        let compression_type = reader.read_u8()?;
        let min_key = reader.read_length_prefixed_slice()?.to_vec();
        let max_key = reader.read_length_prefixed_slice()?.to_vec();
        let min_sequence = reader.read_varint_u64()?;
        let max_sequence = reader.read_varint_u64()?;
        let num_entries = reader.read_varint_u64()? as usize;
        let raw_key_size = reader.read_varint_u64()? as usize;
        let raw_value_size = reader.read_varint_u64()? as usize;
        let data_size = reader.read_varint_u64()? as usize;
        let index_size = reader.read_varint_u64()? as usize;
        let filter_size = reader.read_varint_u64()? as usize;
        Ok(SSTableProperties {
            creation_time,
            sstable_version,
            compression_type,
            min_key,
            max_key,
            min_sequence,
            max_sequence,
            num_entries,
            raw_key_size,
            raw_value_size,
            data_size,
            index_size,
            filter_size,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let creation_millis = self
            .creation_time
            .duration_since(UNIX_EPOCH)
            .expect("SSTableProperties creation_time must not be before UNIX_EPOCH")
            .as_millis()
            .try_into()
            .expect("SSTableProperties creation_time must fit in u64 milliseconds");

        writer
            .write_varint_u64(creation_millis)
            .write_u8(self.sstable_version)
            .write_u8(self.compression_type)
            .write_length_prefixed_slice(&self.min_key)
            .write_length_prefixed_slice(&self.max_key)
            .write_varint_u64(self.min_sequence)
            .write_varint_u64(self.max_sequence)
            .write_varint_u64(self.num_entries as u64)
            .write_varint_u64(self.raw_key_size as u64)
            .write_varint_u64(self.raw_value_size as u64)
            .write_varint_u64(self.data_size as u64)
            .write_varint_u64(self.index_size as u64)
            .write_varint_u64(self.filter_size as u64);
    }
}

pub struct SSTablePropertiesBuilder {
    creation_time: SystemTime,
    sstable_version: u8,
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
    pub fn new(sstable_version: u8, compression_type: u8) -> Self {
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
    pub fn with_entry(&mut self, key: &[u8], value_size: usize) -> &mut Self {
        self.num_entries += 1;
        self.raw_key_size += key.len() + 4 + 4 + 8; // user_key + collection + index + sequence (with op)
        self.raw_value_size += value_size;

        let record_key = extract_record_key(key);
        if record_key < &self.min_key {
            self.min_key = record_key.to_vec();
        }

        if record_key > &self.max_key {
            self.max_key = record_key.to_vec();
        }

        let sequence = extract_sequence_number(key);
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
        self.index_size = block_size;
        self
    }

    /// Updates the size of the filter block.
    pub fn with_filter_block(&mut self, block_size: usize) -> &mut Self {
        self.filter_size = block_size;
        self
    }

    pub fn estimated_compression_ratio(&self) -> f64 {
        if self.data_size == 0 {
            return 1.0; // Avoid division by zero, assume no compression
        }
        let ratio =
            compute_compression_ratio(self.raw_key_size, self.raw_value_size, self.data_size);
        if ratio.is_finite() && ratio > 0.0 {
            // Avoid extreme under-estimates that could lead to huge overshoots.
            ratio.clamp(0.05, 1.0)
        } else {
            1.0
        }
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

/// Calculates the compression ratio given the raw key size, raw value size, and data size.
/// The compression ratio is defined as the ratio of the uncompressed size (raw key size + raw value size)
/// to the compressed size (data size).
fn compute_compression_ratio(raw_key_size: usize, raw_value_size: usize, data_size: usize) -> f64 {
    assert!(
        data_size > 0,
        "Data size must be greater than zero to calculate compression ratio."
    );
    let uncompressed_size = raw_key_size + raw_value_size;
    uncompressed_size as f64 / data_size as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::{encode_internal_key, encode_record_key};
    use crate::storage::operation::OperationType;

    #[test]
    fn test_update_entry_properties_with_builder() {
        let collection = 1;
        let record_key_1 = encode_record_key(collection, 0, &vec![10, 20, 30]);
        let record_key_2 = encode_record_key(collection, 0, &vec![5, 15, 25, 35]);
        let record_key_3 = encode_record_key(collection, 0, &vec![50, 60, 70, 80, 90]);
        let key1 = encode_internal_key(&record_key_1, 1, OperationType::Put);
        let key2 = encode_internal_key(&record_key_2, 5, OperationType::Put);
        let key3 = encode_internal_key(&record_key_3, 3, OperationType::Put);

        let mut builder = SSTablePropertiesBuilder::new(1, 0);

        builder
            .with_entry(&key1, 50)
            .with_entry(&key2, 40)
            .with_entry(&key3, 25);

        let sstable_properties = builder.build();

        // Validate the properties
        assert_eq!(sstable_properties.min_key, record_key_2);
        assert_eq!(sstable_properties.max_key, record_key_3);
        assert_eq!(sstable_properties.min_sequence, 1);
        assert_eq!(sstable_properties.max_sequence, 5);
        assert_eq!(sstable_properties.num_entries, 3);
        assert_eq!(
            sstable_properties.raw_key_size, 90,
            "Raw key size should be 90 (29 + 30 + 31)"
        );
        assert_eq!(
            sstable_properties.raw_value_size, 115,
            "Raw value size should be 115 (50 + 40 + 25)"
        );
    }

    #[test]
    fn test_compression_ratio_valid_data() {
        let properties = SSTableProperties {
            raw_key_size: 1000,
            raw_value_size: 4000,
            data_size: 2000,
            ..SSTablePropertiesBuilder::new(1, 0).build()
        };

        let ratio = properties.compression_ratio();
        assert!(
            (ratio - 2.5).abs() < f64::EPSILON,
            "Expected compression ratio to be 2.5, got {}",
            ratio
        );
    }

    #[test]
    fn test_compression_ratio_no_compression() {
        let properties = SSTableProperties {
            raw_key_size: 1000,
            raw_value_size: 1000,
            data_size: 2000,
            ..SSTablePropertiesBuilder::new(1, 0).build()
        };

        let ratio = properties.compression_ratio();
        assert!(
            (ratio - 1.0).abs() < f64::EPSILON,
            "Expected compression ratio to be 1.0, got {}",
            ratio
        );
    }
}
