use crate::options::options::Options;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, encode_internal_key_range, encode_record_key, extract_sequence_number, extract_user_key};
use crate::storage::lsm_version::SSTableMetadata;
use crate::storage::operation::OperationType;
use crate::storage::sstable::sstable_writer::SSTableWriter;
use crate::storage::write_batch::WriteBatch;
use crate::util::interval::Interval;
use crossbeam_skiplist::SkipMap;
use std::io::Result;
use std::iter::Rev;
use std::ops::{RangeBounds};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_skiplist::map::Range;
use crate::storage::Direction;

pub struct Memtable {
    skiplist: SkipMap<Vec<u8>, Vec<u8>>, // Binary values
    size: AtomicUsize,                   // Current size of the memtable
    pub log_number: u64, // The number of the write-ahead log file associated to this memtable
}

impl Memtable {
    pub fn new(log_number: u64) -> Self {
        Memtable {
            skiplist: SkipMap::new(),
            size: AtomicUsize::new(0),
            log_number,
        }
    }

    /// Applies all the WriteBatch operations to the Memtable
    pub fn write(&self, seq: u64, batch: &WriteBatch) {
        for operation in batch.operations() {
            let key = operation.internal_key(seq);
            let value = operation.value().to_vec();
            let key_size = key.len();
            let value_size = value.len();

            // Insert into the skip list
            self.skiplist.insert(key, value);

            // Update the size
            self.size.fetch_add(key_size + value_size, Ordering::Relaxed);
        }
    }

    /// Read a collection value by user key and optional snapshot (sequence number)
    pub fn read(&self, record_key: &[u8], snapshot: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        // Create the range bounds for the search:
        // We want to retrieve all the entries for the specified key
        let start_key = encode_internal_key(record_key, snapshot, OperationType::MaxKey);
        let end_key = encode_internal_key(record_key, u64::MIN, OperationType::MinKey);

        // Traverse the skip list
        let mut iter = self.skiplist.range(start_key..=end_key);

        while let Some(entry) = iter.next() {
            let internal_key = entry.key();

            return Some((internal_key.clone(), entry.value().clone()));
        }
        None // No valid entry found
    }

    /// Perform a range scan for keys within the specified range.
    /// Supports both inclusive and exclusive bounds.
    pub fn range_scan<'a>(
        &'a self,
        range: &Interval<Vec<u8>>,
        snapshot: u64,
        direction: Direction,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>
    {
        let iter = self.skiplist.range(range.clone());

        if direction == Direction::Reverse {
            Box::new(ReverseRangeScanIterator {
                iter: iter.rev(),
                snapshot,
                previous: None,
            })
        } else {
            Box::new(RangeScanIterator {
                iter,
                snapshot,
                previous: None,
            })
        }
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn flush(
        &self,
        directory: &Path,
        sst_file: &DbFile,
        options: &Options,
    ) -> Result<SSTableMetadata> {
        let mut writer = SSTableWriter::new(directory, sst_file, options, self.skiplist.len())?;
        for entry in self.skiplist.iter() {
            writer.add(entry.key(), entry.value())?;
        }
        writer.finish()
    }
}

/// Iterator type for range scans in forward order.
pub struct RangeScanIterator<'a> {
    iter: Range<'a, Vec<u8>, Interval<Vec<u8>>, Vec<u8>, Vec<u8>>,
    snapshot: u64,
    previous: Option<Vec<u8>>,
}

impl<'a> Iterator for RangeScanIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {

        while let Some(entry) = self.iter.next() {
            let key = entry.key();

            let seq = extract_sequence_number(key);
            if seq > self.snapshot {
                continue;
            }

            let user_key = extract_user_key(key);
            if let Some(prev) = &self.previous {
                if prev == user_key {
                    continue;
                }
            }
            self.previous = Some(user_key.to_vec());

            return Some((key.to_vec(), entry.value().clone(),))
        }
        None
    }
}

/// Iterator type for range scans in reverse order.
pub struct ReverseRangeScanIterator<'a> {
    iter: Rev<Range<'a, Vec<u8>, Interval<Vec<u8>>, Vec<u8>, Vec<u8>>>,
    snapshot: u64,
    previous: Option<(Vec<u8>, Vec<u8>)>,
}

impl<'a> Iterator for ReverseRangeScanIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        while let Some(entry) = self.iter.next() {
            let key = entry.key();
            let seq = extract_sequence_number(key);
            if seq > self.snapshot {
                continue;
            }
            let user_key = extract_user_key(key);
            if let Some((ref prev_key, _)) = self.previous {
                if extract_user_key(prev_key) != user_key {
                    // User key changed, yield previous
                    return self.previous.replace((key.to_vec(), entry.value().clone()));
                }
            }
            self.previous = Some((key.to_vec(), entry.value().clone()));
        }
        // At the end, yield the last buffered entry if any
        self.previous.take()
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use tempfile::tempdir;
    use crate::storage::internal_key::MAX_SEQUENCE_NUMBER;
    use super::*;
    use crate::storage::operation::Operation;
    use crate::storage::write_batch::WriteBatch;
    use crate::util::bson_utils::as_key_value;

    #[test]
    fn write_put_operations() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT operations
        let collection: u32 = 32;
        let batch = write_batch(vec![
            put(collection, b"key1", b"value1"),
            put(collection, b"key2", b"value2"),
        ]);

        memtable.write(1, &batch);

        // Verify data was inserted
        assert_eq!(
            memtable.read(
                &encode_record_key(collection, 0, b"key1"),
                MAX_SEQUENCE_NUMBER,
            ),
            Some((
                put_key(collection, &Vec::from(b"key1"), 1),
                Vec::from(b"value1")
            ))
        );
        assert_eq!(
            memtable.read(
                &encode_record_key(collection, 0, b"key2"),
                MAX_SEQUENCE_NUMBER
            ),
            Some((
                put_key(collection, &Vec::from(b"key2"), 1),
                Vec::from(b"value2")
            ))
        );
    }

    #[test]
    fn write_put_and_delete_operations() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch = write_batch(vec![
            put(collection, b"key1", b"value1"),
            delete(collection, b"key1"),
        ]);

        memtable.write(1, &batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(&put_key(collection, b"key1", 1), 2), None);
        assert_eq!(
            memtable.read(&del_key(collection, b"key1", 1), MAX_SEQUENCE_NUMBER),
            None
        );
    }

    #[test]
    fn write_put_and_delete_operations_in_different_batches() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch = write_batch(vec![put(collection, b"key1", b"value1")]);

        memtable.write(1, &batch);

        let batch = write_batch(vec![delete(collection, b"key1")]);

        memtable.write(2, &batch);

        // Verify key1 was deleted
        assert_eq!(
            memtable.read(
                &encode_record_key(collection, 0, b"key1"),
                MAX_SEQUENCE_NUMBER
            ),
            Some((del_key(collection, &Vec::from(b"key1"), 2), vec![]))
        );
    }

    #[test]
    fn read_with_snapshot() {
        let memtable = Memtable::new(2);

        let collection: u32 = 32;
        // Insert multiple versions of the same key
        let batch1 = write_batch(vec![put(collection, b"key1", b"value1_v1")]);
        memtable.write(2, &batch1);

        let batch2 = write_batch(vec![put(collection, b"key1", b"value1_v2")]);
        memtable.write(3, &batch2);

        // Read with snapshots
        let record_key = encode_record_key(collection, 0, b"key1");
        assert_eq!(memtable.read(&record_key, 1), None); // Snapshot 0
        assert_eq!(
            memtable.read(&record_key, 2),
            Some((
                put_key(collection, &Vec::from(b"key1"), 2),
                Vec::from(b"value1_v1")
            ))
        ); // Snapshot 1
        assert_eq!(
            memtable.read(&record_key, 3),
            Some((
                put_key(collection, &Vec::from(b"key1"), 3),
                Vec::from(b"value1_v2")
            ))
        ); // Snapshot 2
        assert_eq!(
            memtable.read(&record_key, MAX_SEQUENCE_NUMBER),
            Some((
                put_key(collection, &Vec::from(b"key1"), 3),
                Vec::from(b"value1_v2")
            ))
        ); // Latest
    }

    #[test]
    fn read_non_existent_key() {
        let memtable = Memtable::new(2);
        let collection = 32;
        // Read a key that was never inserted
        assert_eq!(
            memtable.read(
                &encode_record_key(collection, 0, b"non_existent_key"),
                MAX_SEQUENCE_NUMBER
            ),
            None
        );
    }

    #[test]
    fn range_scan_with_no_matching_keys() {
        let memtable = Memtable::new(2);

        let collection: u32 = 32;
        let batch = write_batch(vec![
            put(collection, b"key1", b"value1"),
            put(collection, b"key4", b"value4"),
        ]);

        memtable.write(1, &batch);

        // Range scan: key2 to key3 (no matching keys)
        let range = to_key_range(collection, &Interval::closed(b"key2".to_vec(), b"key3".to_vec()), MAX_SEQUENCE_NUMBER);
        let result: Vec<_> = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Forward).collect();
        assert!(result.is_empty());

        let result: Vec<_> = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Reverse).collect();
        assert!(result.is_empty());
    }

    #[test]
    fn range_scan_with_different_ranges() {
        let memtable = Memtable::new(2);

        let col: u32 = 32;
        let batch1 = write_batch(vec![
            put(col, b"key1", b"value1_v1"),
            put(col, b"key2", b"value2_v1"),
            put(col, b"key3", b"value3_v1"),
        ]);
        let batch2 = write_batch(vec![
            delete(col, b"key1"),
            put(col, b"key2", b"value2_v2"),
            delete(col, b"key3"),
        ]);
        let batch3 = write_batch(vec![
            put(col, b"key4", b"value4_v1"),
            put(col, b"key5", b"value5_v1"),
            put(col, b"key6", b"value6_v1"),
        ]);

        memtable.write(1, &batch1);
        memtable.write(2, &batch2);
        memtable.write(3, &batch3);

        let range = to_key_range(col, &Interval::closed(b"key2".to_vec(), b"key6".to_vec()), MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Forward);

        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col,b"key6", 3), Vec::from(b"value6_v1"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((put_key(col,b"key6", 3), Vec::from(b"value6_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), None);

        let range = to_key_range(col, &Interval::closed_open(b"key2".to_vec(), b"key6".to_vec()), MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan( &range, MAX_SEQUENCE_NUMBER, Direction::Forward);

        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), None);

        let range = to_key_range(col, &Interval::open_closed(b"key2".to_vec(), b"key6".to_vec()), MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Forward);

        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col,b"key6", 3), Vec::from(b"value6_v1"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((put_key(col,b"key6", 3), Vec::from(b"value6_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), None);

        let range = to_key_range(col, &Interval::open(b"key2".to_vec(), b"key6".to_vec()), MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan( &range, MAX_SEQUENCE_NUMBER, Direction::Forward);

        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((put_key(col, b"key5", 3), Vec::from(b"value5_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key4", 3), Vec::from(b"value4_v1"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), None);
    }

    #[test]
    fn range_scan_with_mixed_operations() {
        let memtable = Memtable::new(2);

        let col: u32 = 32;
        let batch1 = write_batch(vec![
            put(col, b"key1", b"value1_v1"),
            put(col, b"key2", b"value2_v1"),
            put(col, b"key3", b"value3_v1"),
        ]);
        let batch2 = write_batch(vec![
            delete(col, b"key1"),
            put(col, b"key2", b"value2_v2"),
            delete(col, b"key3"),
        ]);

        memtable.write(1, &batch1);
        memtable.write(2, &batch2);

        // Range scan: key1 to key2
        let range = to_key_range(col, &Interval::closed(b"key1".to_vec(), b"key3".to_vec()), MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Forward);

        assert_eq!(range_iter.next(), Some((del_key(col, b"key1", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, MAX_SEQUENCE_NUMBER, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key1", 2), vec!())));
        assert_eq!(range_iter.next(), None);

        let range = to_key_range(col, &Interval::closed(b"key1".to_vec(), b"key3".to_vec()), 2);
        let mut range_iter = memtable.range_scan(&range, 2, Direction::Forward);

        assert_eq!(range_iter.next(), Some((del_key(col, b"key1", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, 2, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((del_key(col, b"key3", 2), vec!())));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 2), Vec::from(b"value2_v2"))));
        assert_eq!(range_iter.next(), Some((del_key(col, b"key1", 2), vec!())));
        assert_eq!(range_iter.next(), None);

        let range = to_key_range(col, &Interval::closed(b"key1".to_vec(), b"key3".to_vec()), 1);
        let mut range_iter = memtable.range_scan(&range, 1, Direction::Forward);

        assert_eq!(range_iter.next(), Some((put_key(col, b"key1", 1), Vec::from(b"value1_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 1), Vec::from(b"value2_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key3", 1), Vec::from(b"value3_v1"))));
        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, 1, Direction::Reverse);

        assert_eq!(range_iter.next(), Some((put_key(col, b"key3", 1), Vec::from(b"value3_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key2", 1), Vec::from(b"value2_v1"))));
        assert_eq!(range_iter.next(), Some((put_key(col, b"key1", 1), Vec::from(b"value1_v1"))));
        assert_eq!(range_iter.next(), None);

        let range = to_key_range(col, &Interval::closed(b"key1".to_vec(), b"key3".to_vec()), 0);
        let mut range_iter = memtable.range_scan(&range, 0, Direction::Forward);

        assert_eq!(range_iter.next(), None);

        let mut range_iter = memtable.range_scan(&range, 0, Direction::Reverse);

        assert_eq!(range_iter.next(), None);
    }

    #[test]
    fn test_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let memtable = Memtable::new(2);

        let inserts = vec![
            as_key_value(&doc! { "id": 1, "name": "Luke Skywalker", "role": "Jedi" }).unwrap(),
            as_key_value(&doc! { "id": 2, "name": "Darth Vader", "role": "Sith" }).unwrap(),
            as_key_value(&doc! { "id": 3, "name": "Leia Organa", "role": "Princess" }).unwrap(),
            as_key_value(&doc! { "id": 4, "name": "Han Solo", "role": "Smuggler" }).unwrap(),
        ];

        let mut seq = 15;
        for (user_key, value) in inserts.iter() {
            let _ = memtable.write(seq, &write_batch(vec!(put(10, user_key, value))));
            seq += 1;
        }

        let sst_file = DbFile::new_sst(3);
        let sst = memtable.flush(&path, &sst_file, &Options::lightweight()).unwrap();

        let expected_size = path.join(sst_file.filename()).metadata().unwrap().len();

        let expected = SSTableMetadata::new(
            3,
            0,
            &encode_record_key(10, 0, &inserts[0].0),
            &encode_record_key(10, 0, &inserts[3].0),
            15,
            18,
            expected_size,
        );
        assert_eq!(sst, expected);
    }

    fn put_key(collection: u32, key: &[u8], snapshot: u64) -> Vec<u8> {
        encode_internal_key(
            &encode_record_key(collection, 0, key),
            snapshot,
            OperationType::Put,
        )
    }

    fn del_key(collection: u32, key: &[u8], snapshot: u64) -> Vec<u8> {
        encode_internal_key(
            &encode_record_key(collection, 0, key),
            snapshot,
            OperationType::Delete,
        )
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

    fn to_key_range<R>(collection: u32, range: &R, snapshot: u64) -> Interval<Vec<u8>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        encode_internal_key_range(collection, 0, range, snapshot)
    }
}
