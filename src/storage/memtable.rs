use std::ops::{Bound, RangeBounds};
use std::ops::Bound::Unbounded;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_skiplist::SkipMap;
use crate::storage::operation::OperationType;
use crate::storage::write_batch::WriteBatch;
use crate::util::interval::Interval;
use crate::statistics::CollectionStatistics;
use crate::storage::compound_key::CompoundKey;

pub struct Memtable {
    skiplist: SkipMap<CompoundKey, Vec<u8>>, // Binary values
    size: AtomicUsize,                       // Current size of the memtable
    stats: Arc<CollectionStatistics>,
}

impl Memtable {
    pub fn new(stats: Arc<CollectionStatistics>) -> Self {
        Memtable {
            skiplist: SkipMap::new(),
            size: AtomicUsize::new(0),
            stats,
        }
    }

    /// Applies all the WriteBatch operations to the Memtable
    pub fn write(&mut self, batch: WriteBatch) {

        for operation in batch.operations {
            let (operation_type, key, value) = operation.deconstruct();
            let key_size = key.len();
            let value_size = value.len();

            // Create a compound key with the operation type (PUT)
            let compound_key = CompoundKey::new(key, batch.sequence_number.unwrap(), operation_type);
            // Insert into the skip list
            self.skiplist.insert(compound_key, value);

            // Update the size
            self.size.fetch_add(key_size + value_size, Ordering::Relaxed);

            // Update stats
            self.stats.add_memtable_operation(operation_type);
        }
    }

    /// Read a value by user key and optional snapshot (sequence number)
    pub fn read(&self, key: &[u8], snapshot: Option<u64>) -> Option<Vec<u8>> {
        // Snapshot sequence number (if provided)
        let max_sequence = snapshot.unwrap_or(u64::MAX);

        // Create the range bounds for the search:
        // We want to retrieve all the entries for the specified key
        let start_key = CompoundKey::new(key.to_vec(), max_sequence, OperationType::MaxKey);
        let end_key = CompoundKey::new(key.to_vec(), u64::MIN, OperationType::MinKey);

        // Traverse the skip list
        let mut iter = self.skiplist.range(start_key..=end_key);

        while let Some(entry) = iter.next() {
            let compound_key = entry.key();

            if compound_key.extract_value_type() == OperationType::Put {
                self.stats.increment_mem_table_hit();
                return Some(entry.value().clone()) // Found a valid PUT
            } else {
                self.stats.increment_mem_table_miss();
                return None  // Deleted, stop searching
            }
        }
        self.stats.increment_mem_table_miss();
        None // No valid entry found
    }


    /// Perform a range scan for keys within the specified range.
    /// Supports both inclusive and exclusive bounds.
    ///
    ///  Note: Hits and misses statistics are not updated by range scans
    pub fn range_scan<R>(&self, range: &R, snapshot: Option<u64>) -> RangeScanIterator
    where
        R: RangeBounds<Vec<u8>>,
    {
        // Snapshot sequence number (if provided)
        let max_sequence = snapshot.unwrap_or(u64::MAX);

        // Convert the start and end bounds
        let start = match range.start_bound() {
            Bound::Included(key) => Bound::Included(CompoundKey::new(key.clone(), max_sequence, OperationType::MaxKey)),
            Bound::Excluded(key) => Bound::Excluded(CompoundKey::new(key.clone(), u64::MIN, OperationType::MinKey)),
            Unbounded => Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(key) => Bound::Included(CompoundKey::new(key.clone(), u64::MIN, OperationType::MinKey)),
            Bound::Excluded(key) => Bound::Excluded(CompoundKey::new(key.clone(), max_sequence, OperationType::MaxKey)),
            Unbounded => Unbounded,
        };

        let selected_range = Interval::new(start, end);

        let iter = self.skiplist.range(selected_range);

        RangeScanIterator {
            iter,
            snapshot,
            previous: None,
        }
    }
}

/// Iterator type for range scans.
pub struct RangeScanIterator<'a> {
    iter: crossbeam_skiplist::map::Range<'a, CompoundKey, Interval<CompoundKey>, CompoundKey, Vec<u8>>,
    snapshot: Option<u64>,
    previous: Option<Vec<u8>>,
}

impl<'a> Iterator for RangeScanIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // Keep the latest for each key
        // "latest" -> matching snapshot

        while let Some(entry) = self.iter.next() {
            let compound_key = entry.key();
            if  self.snapshot != None && compound_key.extract_sequence_number() > self.snapshot.unwrap() {
                continue
            }
            let user_key = Some(compound_key.user_key.clone());
            if self.previous == user_key {
                continue
            }
            self.previous = user_key;

            if compound_key.extract_value_type() == OperationType::Put {
                return Some((compound_key.user_key.clone(), entry.value().clone()));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::operation::Operation;
    use crate::storage::write_batch::WriteBatch;
    use super::*;

    #[test]
    fn test_write_put_operations() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        // Create a WriteBatch with PUT operations
        let batch = WriteBatch::new(1, vec![put(b"key1",  b"value1"), put(b"key2", b"value2")]);

        memtable.write(batch);

        // Verify data was inserted
        assert_eq!(memtable.read(b"key1", None), Some(b"value1".to_vec()));
        assert_eq!(memtable.read(b"key2", None), Some(b"value2".to_vec()));

        // Verify statistics
        assert_eq!(stats.get_memtable_total_inserts(), 2);
        assert_eq!(stats.get_memtable_total_deletes(), 0);
        assert_eq!(stats.get_mem_table_miss(), 0);
        assert_eq!(stats.get_mem_table_hit(), 2);
    }

    #[test]
    fn test_write_put_and_delete_operations() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        // Create a WriteBatch with PUT and DELETE operations
        let batch =
            WriteBatch::new(2, vec![put(b"key1", b"value1"), delete(b"key1")]);

        memtable.write(batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(b"key1", Some(2)), None);
        assert_eq!(memtable.read(b"key1", None), None);

        // Verify statistics
        assert_eq!(stats.get_memtable_total_inserts(), 1);
        assert_eq!(stats.get_memtable_total_deletes(), 1);
        assert_eq!(stats.get_mem_table_miss(), 2);
        assert_eq!(stats.get_mem_table_hit(), 0);
    }

    #[test]
    fn test_write_put_and_delete_operations_in_different_batches() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        // Create a WriteBatch with PUT and DELETE operations
        let batch =
            WriteBatch::new(1, vec![put(b"key1", b"value1")]);

        memtable.write(batch);

        let batch =
            WriteBatch::new(2, vec![delete(b"key1")]);

        memtable.write(batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(b"key1", None), None);

        // Verify statistics
        assert_eq!(stats.get_memtable_total_inserts(), 1);
        assert_eq!(stats.get_memtable_total_deletes(), 1);
        assert_eq!(stats.get_mem_table_miss(), 1);
        assert_eq!(stats.get_mem_table_hit(), 0);
    }

    #[test]
    fn test_read_with_snapshot() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        // Insert multiple versions of the same key
        let batch1 = WriteBatch::new(2, // Sequence number = 2
                                     vec![put(b"key1",  b"value1_v1")]);
        memtable.write(batch1);

        let batch2 = WriteBatch::new(3, // Sequence number = 3
                                      vec![put(b"key1",  b"value1_v2")]);
        memtable.write(batch2);

        // Read with snapshots
        assert_eq!(memtable.read(b"key1", Some(1)), None); // Snapshot 0
        assert_eq!(memtable.read(b"key1", Some(2)), Some(b"value1_v1".to_vec())); // Snapshot 1
        assert_eq!(memtable.read(b"key1", Some(3)), Some(b"value1_v2".to_vec())); // Snapshot 2
        assert_eq!(memtable.read(b"key1", None), Some(b"value1_v2".to_vec()));   // Latest

        // Verify statistics
        assert_eq!(stats.get_memtable_total_inserts(), 2);
        assert_eq!(stats.get_memtable_total_deletes(), 0);
        assert_eq!(stats.get_mem_table_miss(), 1);
        assert_eq!(stats.get_mem_table_hit(), 3);
    }

    #[test]
    fn test_read_non_existent_key() {
        let stats = CollectionStatistics::new();
        let memtable = Memtable::new(stats.clone());

        // Read a key that was never inserted
        assert_eq!(memtable.read(b"non_existent_key", None), None);
        assert_eq!(stats.get_mem_table_miss(), 1);
        assert_eq!(stats.get_mem_table_hit(), 0);
    }

    #[test]
    fn test_range_scan_with_no_matching_keys() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        let batch = WriteBatch::new(1, vec![
            put(b"key1", b"value1"),
            put(b"key4", b"value4"),
        ]);

        memtable.write(batch);

        // Range scan: key2 to key3 (no matching keys)
        let range = Interval::closed(b"key2".to_vec(), b"key3".to_vec());
        let result: Vec<_> = memtable.range_scan(&range, None).collect();
        assert!(result.is_empty());
    }

    #[test]
    fn test_range_scan_with_different_ranges() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        let batch1 = WriteBatch::new(1, vec![
            put(b"key1", b"value1_v1"),
            put(b"key2", b"value2_v1"),
            put(b"key3", b"value3_v1"),
        ]);
        let batch2 = WriteBatch::new(2, vec![
            delete(b"key1"),
            put(b"key2", b"value2_v2"),
            delete(b"key3"),
        ]);
        let batch3 = WriteBatch::new(3, vec![
            put(b"key4", b"value4_v1"),
            put(b"key5", b"value5_v1"),
            put(b"key6", b"value6_v1"),
        ]);

        memtable.write(batch1);
        memtable.write(batch2);
        memtable.write(batch3);

        let range = Interval::closed(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(&range, None);

        assert_eq!(range_iter.next(), Some((b"key2".to_vec(), b"value2_v2".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key4".to_vec(), b"value4_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key5".to_vec(), b"value5_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key6".to_vec(), b"value6_v1".to_vec())));
        assert_eq!(range_iter.next(), None);

        let range = Interval::closed_open(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(&range, None);

        assert_eq!(range_iter.next(), Some((b"key2".to_vec(), b"value2_v2".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key4".to_vec(), b"value4_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key5".to_vec(), b"value5_v1".to_vec())));
        assert_eq!(range_iter.next(), None);

        let range = Interval::open_closed(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(&range, None);

        assert_eq!(range_iter.next(), Some((b"key4".to_vec(), b"value4_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key5".to_vec(), b"value5_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key6".to_vec(), b"value6_v1".to_vec())));
        assert_eq!(range_iter.next(), None);

        let range = Interval::open(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(&range, None);

        assert_eq!(range_iter.next(), Some((b"key4".to_vec(), b"value4_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key5".to_vec(), b"value5_v1".to_vec())));
        assert_eq!(range_iter.next(), None);
    }


    #[test]
    fn test_range_scan_with_mixed_operations() {
        let stats = CollectionStatistics::new();
        let mut memtable = Memtable::new(stats.clone());

        let batch1 = WriteBatch::new(1, vec![
            put(b"key1", b"value1_v1"),
            put(b"key2", b"value2_v1"),
            put(b"key3", b"value3_v1"),
        ]);
        let batch2 = WriteBatch::new(2, vec![
            delete(b"key1"),
            put(b"key2", b"value2_v2"),
            delete(b"key3"),
        ]);

        memtable.write(batch1);
        memtable.write(batch2);

        // Range scan: key1 to key2
        let range = Interval::closed(b"key1".to_vec(), b"key3".to_vec());
        let mut range_iter = memtable.range_scan(&range, None);

        assert_eq!(range_iter.next(), Some((b"key2".to_vec(), b"value2_v2".to_vec())));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, Some(2));

        assert_eq!(range_iter.next(), Some((b"key2".to_vec(), b"value2_v2".to_vec())));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, Some(1));

        assert_eq!(range_iter.next(), Some((b"key1".to_vec(), b"value1_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key2".to_vec(), b"value2_v1".to_vec())));
        assert_eq!(range_iter.next(), Some((b"key3".to_vec(), b"value3_v1".to_vec())));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, Some(0));

        assert_eq!(range_iter.next(), None);
    }

    fn put(key: &[u8], value: &[u8]) -> Operation {
        Operation::new_put(key.to_vec(), value.to_vec())
    }

    fn delete(key: &[u8]) -> Operation {
        Operation::new_delete(key.to_vec())
    }
}