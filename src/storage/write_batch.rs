use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::storage::operation::Operation;
use crate::storage::count_stats::CountStats;
use std::collections::BTreeSet;
use std::io::Result;

#[derive(Debug, PartialEq)]
pub enum Precondition {
    VersionMatch {
        collection: u32,
        index: u32,
        user_key: Vec<u8>,
    },
}
#[derive(Debug, PartialEq)]
pub struct Preconditions {
    since: u64,
    conditions: Vec<Precondition>,
}

impl Preconditions {
    pub fn new(since: u64, conditions: Vec<Precondition>) -> Self {
        Preconditions { since, conditions }
    }

    pub fn since(&self) -> u64 {
        self.since
    }

    pub fn conditions(&self) -> &[Precondition] {
        &self.conditions
    }
}

#[derive(Default, Debug)]
pub struct WriteBatch {
    operations: Vec<Operation>,
    preconditions: Option<Preconditions>,
    count_stats: CountStats,
    precomputed_wal_record: Option<Vec<u8>>,
    required_collections: BTreeSet<(u32, u32)>,
}

impl WriteBatch {
    pub fn new(operations: Vec<Operation>, count_stats: CountStats) -> WriteBatch {
        let precomputed_wal_record =
            Some(Self::precompute_wal_record(&operations, &count_stats));
        let required_collections = Self::extract_required_collections(&operations);
        WriteBatch {
            operations,
            preconditions: None,
            count_stats,
            precomputed_wal_record,
            required_collections,
        }
    }

    pub fn new_with_preconditions(
        operations: Vec<Operation>,
        preconditions: Preconditions,
        count_stats: CountStats,
    ) -> Self {
        let precomputed_wal_record =
            Some(Self::precompute_wal_record(&operations, &count_stats));
        let required_collections = Self::extract_required_collections(&operations);
        WriteBatch {
            operations,
            preconditions: Some(preconditions),
            count_stats,
            precomputed_wal_record,
            required_collections,
        }
    }

    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    pub fn preconditions(&self) -> Option<&Preconditions> {
        self.preconditions.as_ref()
    }

    pub fn count_stats(&self) -> &CountStats {
        &self.count_stats
    }

    pub fn required_collections(&self) -> &BTreeSet<(u32, u32)> {
        &self.required_collections
    }

    pub fn to_wal_record(&self, seq: u64) -> Vec<u8> {
        let precomputed_wal_record = if self.precomputed_wal_record.is_none() {
            &Self::precompute_wal_record(&self.operations, &self.count_stats)
        } else {
            self.precomputed_wal_record.as_ref().unwrap()
        };
        let mut vec = Vec::with_capacity(8 + precomputed_wal_record.len());
        vec.extend_from_slice(&seq.to_be_bytes());
        vec.extend_from_slice(precomputed_wal_record);
        vec
    }

    fn extract_required_collections(operations: &[Operation]) -> BTreeSet<(u32, u32)> {
        operations
            .iter()
            .map(|op| (op.collection, op.index))
            .collect()
    }

    fn precompute_wal_record(operations: &[Operation], count_stats: &CountStats) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        writer.write_varint_u64(operations.len() as u64);
        for operation in operations {
            operation.write_to(&mut writer);
        }
        count_stats.write_to(&mut writer);
        writer.take_buffer()
    }

    pub fn from_wal_record(bytes: &[u8]) -> Result<Self> {
        let reader = ByteReader::new(bytes);
        let nbr_operations = reader.read_varint_u64()? as usize;
        let mut operations = Vec::with_capacity(nbr_operations);
        for _ in 0..nbr_operations {
            operations.push(Operation::read_from(&reader)?);
        }
        let count_stats = CountStats::read_from(&reader)?;
        Ok(WriteBatch {
            operations,
            preconditions: None,
            count_stats,
            precomputed_wal_record: None,
            required_collections: BTreeSet::new(),
        })
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }
}

impl PartialEq for WriteBatch {
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
            && self.preconditions == other.preconditions
            && self.count_stats == other.count_stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::operation::Operation;
    use crate::storage::count_stats::CountStatsKey;
    use std::collections::BTreeMap;

    #[test]
    fn test_write_batch_wal_round_trip() {
        let batch = WriteBatch::new(vec![
            Operation::new_put(10, 5, b"key1".to_vec(), b"value1".to_vec()),
            Operation::new_delete(20, 6, b"key2".to_vec()),
        ], CountStats::default());

        let seq = 12345;
        let wal = batch.to_wal_record(seq);

        // Decode skipping the first 8 bytes (sequence number)
        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }

    #[test]
    fn test_empty_write_batch_round_trip() {
        let batch = WriteBatch::new(vec![], CountStats::default());

        let seq = 0;
        let wal = batch.to_wal_record(seq);

        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }

    #[test]
    fn test_write_batch_wal_round_trip_preserves_count_stats() {
        let batch = WriteBatch::new(
            vec![Operation::new_put(10, 0, b"key".to_vec(), b"value".to_vec())],
            CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(10), 1),
                (
                    CountStatsKey::Index {
                        collection: 10,
                        index: 2,
                    },
                    1,
                ),
            ])),
        );

        let wal = batch.to_wal_record(99);
        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }
}
