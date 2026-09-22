use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::storage::count_stats::CountStats;
use crate::storage::operation::Operation;
use crate::storage::snapshot_manager::Snapshot;
use std::io::Result;

#[derive(Debug, PartialEq)]
pub enum Precondition {
    CollectionVersionMatch {
        collection: u32,
        version: u32,
    },
    VersionMatch {
        collection: u32,
        index: u32,
        user_key: Vec<u8>,
    },
}
#[derive(Debug)]
pub struct Preconditions {
    snapshot: Snapshot,
    collection_version_matches: Vec<Precondition>,
    version_matches: Vec<Precondition>,
}

impl Preconditions {
    pub fn new(snapshot: Snapshot) -> Self {
        Preconditions {
            snapshot,
            collection_version_matches: Vec::new(),
            version_matches: Vec::new(),
        }
    }

    pub fn add_collection_version_match(&mut self, collection: u32, version: u32) {
        self.collection_version_matches
            .push(Precondition::CollectionVersionMatch {
                collection,
                version,
            });
    }

    pub fn extend_version_matches(&mut self, conditions: impl IntoIterator<Item = Precondition>) {
        for condition in conditions {
            assert!(
                matches!(condition, Precondition::VersionMatch { .. }),
                "collection version preconditions must be added with add_collection_version_match"
            );
            self.version_matches.push(condition);
        }
    }

    pub fn since(&self) -> u64 {
        self.snapshot.sequence()
    }

    pub fn conditions(&self) -> impl Iterator<Item = &Precondition> {
        self.collection_version_matches
            .iter()
            .chain(self.version_matches.iter())
    }
}

#[derive(Default, Debug)]
pub struct WriteBatch {
    operations: Vec<Operation>,
    preconditions: Option<Preconditions>,
    count_stats: CountStats,
    precomputed_wal_record: Option<Vec<u8>>,
}

impl WriteBatch {
    #[cfg(test)]
    pub(crate) fn new_for_test(operations: Vec<Operation>, count_stats: CountStats) -> WriteBatch {
        let precomputed_wal_record = Some(Self::precompute_wal_record(&operations, &count_stats));
        WriteBatch {
            operations,
            preconditions: None,
            count_stats,
            precomputed_wal_record,
        }
    }

    pub fn new_with_preconditions(
        operations: Vec<Operation>,
        count_stats: CountStats,
        preconditions: Preconditions,
    ) -> Self {
        let precomputed_wal_record = Some(Self::precompute_wal_record(&operations, &count_stats));
        WriteBatch {
            operations,
            preconditions: Some(preconditions),
            count_stats,
            precomputed_wal_record,
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

impl PartialEq for Preconditions {
    fn eq(&self, other: &Self) -> bool {
        self.since() == other.since()
            && self.collection_version_matches == other.collection_version_matches
            && self.version_matches == other.version_matches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::count_stats::CountStatsKey;
    use crate::storage::operation::Operation;
    use crate::storage::snapshot_manager::SnapshotManager;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn test_collection_version_matches_precede_version_matches() {
        let snapshot = Arc::new(SnapshotManager::new()).acquire(1);
        let mut preconditions = Preconditions::new(snapshot);
        preconditions.extend_version_matches(vec![Precondition::VersionMatch {
            collection: 1,
            index: 2,
            user_key: b"key".to_vec(),
        }]);
        preconditions.add_collection_version_match(3, 4);
        preconditions.add_collection_version_match(5, 6);

        let conditions = preconditions.conditions().collect::<Vec<_>>();
        assert!(matches!(
            conditions.as_slice(),
            [
                Precondition::CollectionVersionMatch {
                    collection: 3,
                    version: 4,
                },
                Precondition::CollectionVersionMatch {
                    collection: 5,
                    version: 6,
                },
                Precondition::VersionMatch { .. },
            ]
        ));
    }

    #[test]
    fn test_write_batch_wal_round_trip_discards_preconditions() {
        let snapshot = Arc::new(SnapshotManager::new()).acquire(1);
        let mut preconditions = Preconditions::new(snapshot);
        preconditions.add_collection_version_match(10, 2);
        preconditions.extend_version_matches(vec![Precondition::VersionMatch {
            collection: 10,
            index: 0,
            user_key: b"key".to_vec(),
        }]);
        let batch = WriteBatch::new_with_preconditions(
            vec![Operation::new_put(
                10,
                0,
                b"key".to_vec(),
                b"value".to_vec(),
            )],
            CountStats::default(),
            preconditions,
        );

        let wal = batch.to_wal_record(12345);
        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(decoded.operations(), batch.operations());
        assert_eq!(decoded.count_stats(), batch.count_stats());
        assert!(decoded.preconditions().is_none());
    }

    #[test]
    fn test_write_batch_wal_round_trip() {
        let batch = WriteBatch::new_for_test(
            vec![
                Operation::new_put(10, 5, b"key1".to_vec(), b"value1".to_vec()),
                Operation::new_delete(20, 6, b"key2".to_vec()),
            ],
            CountStats::default(),
        );

        let seq = 12345;
        let wal = batch.to_wal_record(seq);

        // Decode skipping the first 8 bytes (sequence number)
        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }

    #[test]
    fn test_empty_write_batch_round_trip() {
        let batch = WriteBatch::new_for_test(vec![], CountStats::default());

        let seq = 0;
        let wal = batch.to_wal_record(seq);

        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }

    #[test]
    fn test_write_batch_wal_round_trip_preserves_count_stats() {
        let batch = WriteBatch::new_for_test(
            vec![Operation::new_put(
                10,
                0,
                b"key".to_vec(),
                b"value".to_vec(),
            )],
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
