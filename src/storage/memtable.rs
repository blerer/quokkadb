use std::io::Result;
use std::ops::{Bound, RangeBounds};
use std::ops::Bound::Unbounded;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_skiplist::SkipMap;
use crate::io::fd_cache::FileDescriptorCache;
use crate::options::options::Options;
use crate::storage::operation::OperationType;
use crate::storage::write_batch::WriteBatch;
use crate::util::interval::Interval;
use crate::statistics::CollectionStatistics;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, extract_operation_type, extract_record_key, extract_sequence_number, extract_user_key, MAX_SEQUENCE_NUMBER};
use crate::storage::lsm_tree::SSTableMetadata;
use crate::storage::sstable::sstable_writer::SSTableWriter;

pub struct Memtable {
    skiplist: SkipMap<Vec<u8>, Vec<u8>>, // Binary values
    size: AtomicUsize,                       // Current size of the memtable
    stats: Arc<CollectionStatistics>,
    pub log_number: u64, // The number of the write-ahead log file associated to this memtable
}

impl Memtable {
    pub fn new(log_number: u64) -> Self {
        Memtable {
            skiplist: SkipMap::new(),
            size: AtomicUsize::new(0),
            stats: CollectionStatistics::new(),
            log_number,
        }
    }

    /// Applies all the WriteBatch operations to the Memtable
    pub fn write(&self, seq: u64, batch: &WriteBatch) {

        for operation in batch.operations() {
            let key = operation.compound_key(seq);
            let value = operation.value().to_vec();
            let key_size = key.len();
            let value_size = value.len();

            // Insert into the skip list
            self.skiplist.insert(key, value);

            // Update the size
            self.size.fetch_add(key_size + value_size, Ordering::Relaxed);

            // Update stats
            self.stats.add_memtable_operation(operation.operation_type());
        }
    }

    /// Read a collection value by user key and optional snapshot (sequence number)
    pub fn read(&self, collection: u32, key: &[u8], snapshot: u64) -> Option<Vec<u8>> {

        // Create the range bounds for the search:
        // We want to retrieve all the entries for the specified key
        let start_key = encode_internal_key(collection, 0, key, snapshot, OperationType::MaxKey);
        let end_key = encode_internal_key(collection, 0, key, u64::MIN, OperationType::MinKey);

        // Traverse the skip list
        let mut iter = self.skiplist.range(start_key..=end_key);

        while let Some(entry) = iter.next() {
            let compound_key = entry.key();

            if extract_operation_type(compound_key) == OperationType::Put {
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
    pub fn range_scan<R>(&self, collection: u32, range: &R, snapshot: Option<u64>) -> RangeScanIterator
    where
        R: RangeBounds<Vec<u8>>,
    {
        // Snapshot sequence number (if provided)
        let max_sequence = snapshot.unwrap_or(MAX_SEQUENCE_NUMBER);

        // Convert the start and end bounds
        let start = match range.start_bound() {
            Bound::Included(key) => Bound::Included(encode_internal_key(collection, 0, &key, max_sequence, OperationType::MaxKey)),
            Bound::Excluded(key) => Bound::Excluded(encode_internal_key(collection, 0, &key, u64::MIN, OperationType::MinKey)),
            Unbounded => Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(key) => Bound::Included(encode_internal_key(collection, 0, key, u64::MIN, OperationType::MinKey)),
            Bound::Excluded(key) => Bound::Excluded(encode_internal_key(collection, 0, key, max_sequence, OperationType::MaxKey)),
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

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn flush(&self,
                 fd_cache: Arc<FileDescriptorCache>,
                 directory: &Path,
                 db_file: &DbFile,
                 options: &Options
    ) -> Result<SSTableMetadata> {
        self.write_sstable(fd_cache.clone(), directory, db_file, options)
    }

    fn write_sstable(&self,
                     fd_cache: Arc<FileDescriptorCache>,
                     directory: &Path,
                     db_file: &DbFile,
                     options: &Options
    ) -> Result<SSTableMetadata> {
        let mut writer = SSTableWriter::new(fd_cache, directory, db_file, options, self.skiplist.len())?;
        for entry in self.skiplist.iter() {
            writer.add(entry.key(), entry.value())?;
        }
        writer.finish()
    }
}

/// Iterator type for range scans.
pub struct RangeScanIterator<'a> {
    iter: crossbeam_skiplist::map::Range<'a, Vec<u8>, Interval<Vec<u8>>, Vec<u8>, Vec<u8>>,
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
            if  self.snapshot != None && extract_sequence_number(compound_key) > self.snapshot.unwrap() {
                continue
            }
            let user_key = Some(extract_user_key(compound_key).to_vec());
            if self.previous == user_key {
                continue
            }
            self.previous = user_key;

            if extract_operation_type(compound_key) == OperationType::Put {
                return Some((extract_user_key(compound_key).to_vec(), entry.value().clone()));
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
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT operations
        let collection: u32 = 32;
        let batch = write_batch(vec![put(collection, b"key1",  b"value1"), put(collection,b"key2", b"value2")]);

        memtable.write(1, &batch);

        // Verify data was inserted
        assert_eq!(memtable.read(collection, b"key1", MAX_SEQUENCE_NUMBER), Some(Vec::from(b"value1")));
        assert_eq!(memtable.read(collection, b"key2", MAX_SEQUENCE_NUMBER), Some(Vec::from(b"value2")));

        // Verify statistics
        let stats = memtable.stats.clone();
        assert_eq!(stats.get_memtable_total_inserts(), 2);
        assert_eq!(stats.get_memtable_total_deletes(), 0);
        assert_eq!(stats.get_mem_table_miss(), 0);
        assert_eq!(stats.get_mem_table_hit(), 2);
    }

    #[test]
    fn test_write_put_and_delete_operations() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch =
            write_batch(vec![put(collection,b"key1", b"value1"), delete(collection, b"key1")]);

        memtable.write(1, &batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(collection, b"key1", 2), None);
        assert_eq!(memtable.read(collection, b"key1", MAX_SEQUENCE_NUMBER), None);

        // Verify statistics
        let stats = memtable.stats.clone();
        assert_eq!(stats.get_memtable_total_inserts(), 1);
        assert_eq!(stats.get_memtable_total_deletes(), 1);
        assert_eq!(stats.get_mem_table_miss(), 2);
        assert_eq!(stats.get_mem_table_hit(), 0);
    }

    #[test]
    fn test_write_put_and_delete_operations_in_different_batches() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch = write_batch(vec![put(collection, b"key1", b"value1")]);

        memtable.write(1, &batch);

        let batch = write_batch(vec![delete(collection, b"key1")]);

        memtable.write(2, &batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(collection, b"key1", MAX_SEQUENCE_NUMBER), None);

        // Verify statistics
        let stats = memtable.stats.clone();
        assert_eq!(stats.get_memtable_total_inserts(), 1);
        assert_eq!(stats.get_memtable_total_deletes(), 1);
        assert_eq!(stats.get_mem_table_miss(), 1);
        assert_eq!(stats.get_mem_table_hit(), 0);
    }

    #[test]
    fn test_read_with_snapshot() {
        let memtable = Memtable::new(2);

        let collection: u32 = 32;
        // Insert multiple versions of the same key
        let batch1 = write_batch(vec![put(collection,b"key1",  b"value1_v1")]);
        memtable.write(2, &batch1);

        let batch2 = write_batch(vec![put(collection,b"key1",  b"value1_v2")]);
        memtable.write(3, &batch2);

        // Read with snapshots
        assert_eq!(memtable.read(collection,b"key1", 1), None); // Snapshot 0
        assert_eq!(memtable.read(collection,b"key1", 2), Some(Vec::from(b"value1_v1"))); // Snapshot 1
        assert_eq!(memtable.read(collection, b"key1", 3), Some(Vec::from(b"value1_v2"))); // Snapshot 2
        assert_eq!(memtable.read(collection,b"key1", MAX_SEQUENCE_NUMBER), Some(Vec::from(b"value1_v2")));   // Latest

        // Verify statistics
        let stats = memtable.stats.clone();
        assert_eq!(stats.get_memtable_total_inserts(), 2);
        assert_eq!(stats.get_memtable_total_deletes(), 0);
        assert_eq!(stats.get_mem_table_miss(), 1);
        assert_eq!(stats.get_mem_table_hit(), 3);
    }

    #[test]
    fn test_read_non_existent_key() {
        let memtable = Memtable::new(2);
        let collection = 32;
        // Read a key that was never inserted
        assert_eq!(memtable.read(collection, b"non_existent_key", MAX_SEQUENCE_NUMBER), None);
        let stats = memtable.stats.clone();
        assert_eq!(stats.get_mem_table_miss(), 1);
        assert_eq!(stats.get_mem_table_hit(), 0);
    }

    #[test]
    fn test_range_scan_with_no_matching_keys() {
        let memtable = Memtable::new(2);

        let collection: u32 = 32;
        let batch = write_batch(vec![
            put(collection, b"key1", b"value1"),
            put(collection,b"key4", b"value4"),
        ]);

        memtable.write(1, &batch);

        // Range scan: key2 to key3 (no matching keys)
        let range = Interval::closed(b"key2".to_vec(), b"key3".to_vec());
        let result: Vec<_> = memtable.range_scan(collection, &range, None).collect();
        assert!(result.is_empty());
    }

    #[test]
    fn test_range_scan_with_different_ranges() {
        let memtable = Memtable::new(2);

        let collection: u32 = 32;
        let batch1 = write_batch(vec![
            put(collection,b"key1", b"value1_v1"),
            put(collection,b"key2", b"value2_v1"),
            put(collection,b"key3", b"value3_v1"),
        ]);
        let batch2 = write_batch(vec![
            delete(collection,b"key1"),
            put(collection,b"key2", b"value2_v2"),
            delete(collection,b"key3"),
        ]);
        let batch3 = write_batch(vec![
            put(collection,b"key4", b"value4_v1"),
            put(collection,b"key5", b"value5_v1"),
            put(collection,b"key6", b"value6_v1"),
        ]);

        memtable.write(1, &batch1);
        memtable.write(2, &batch2);
        memtable.write(3, &batch3);

        let range = Interval::closed(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(collection, &range, None);

        assert_eq!(range_iter.next(), Some((Vec::from(b"key2"), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key4"), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key5"), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key6"), Vec::from(b"value6_v1"))));
        assert_eq!(range_iter.next(), None);

        let range = Interval::closed_open(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(collection, &range, None);

        assert_eq!(range_iter.next(), Some((Vec::from(b"key2"), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key4"), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key5"), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), None);

        let range = Interval::open_closed(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(collection, &range, None);

        assert_eq!(range_iter.next(), Some((Vec::from(b"key4"), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key5"), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key6"), Vec::from(b"value6_v1"))));
        assert_eq!(range_iter.next(), None);

        let range = Interval::open(b"key2".to_vec(), b"key6".to_vec());
        let mut range_iter = memtable.range_scan(collection, &range, None);

        assert_eq!(range_iter.next(), Some((Vec::from(b"key4"), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key5"), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), None);
    }


    #[test]
    fn test_range_scan_with_mixed_operations() {
        let memtable = Memtable::new(2);

        let collection: u32 = 32;
        let batch1 = write_batch(vec![
            put(collection,b"key1", b"value1_v1"),
            put(collection, b"key2", b"value2_v1"),
            put(collection, b"key3", b"value3_v1"),
        ]);
        let batch2 = write_batch(vec![
            delete(collection,b"key1"),
            put(collection, b"key2", b"value2_v2"),
            delete(collection,b"key3"),
        ]);

        memtable.write(1, &batch1);
        memtable.write(2, &batch2);

        // Range scan: key1 to key2
        let range = Interval::closed(b"key1".to_vec(), b"key3".to_vec());
        let mut range_iter = memtable.range_scan(collection, &range, None);

        assert_eq!(range_iter.next(), Some((Vec::from(b"key2"), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(collection, &range, Some(2));

        assert_eq!(range_iter.next(), Some((Vec::from(b"key2"), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(collection, &range, Some(1));

        assert_eq!(range_iter.next(), Some((Vec::from(b"key1"), Vec::from(b"value1_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key2"), Vec::from(b"value2_v1"))));
        assert_eq!(range_iter.next(), Some((Vec::from(b"key3"), Vec::from(b"value3_v1"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(collection, &range, Some(0));

        assert_eq!(range_iter.next(), None);
    }

    fn put(collection: u32, key: &[u8], value: &[u8]) -> Operation {
        Operation::new_put(collection, 0, key.to_vec(), value.to_vec())
    }

    fn delete(collection: u32, key: &[u8]) -> Operation {
        Operation::new_delete(collection, 0, key.to_vec())
    }

    fn write_batch(operations: Vec<Operation>) -> WriteBatch {
        WriteBatch::new(operations)
    }
}