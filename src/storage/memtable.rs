use crate::options::options::Options;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, InternalKeyBound, InternalKeyRange};
use crate::storage::lsm_version::SSTableMetadata;
use crate::storage::operation::OperationType;
use crate::storage::sstable::sstable_writer::SSTableWriter;
use crate::storage::write_batch::WriteBatch;
use crate::util::interval::Interval;
use crossbeam_skiplist::SkipMap;
use std::io::Result;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ops::{Bound, Bound::Unbounded};
use std::rc::Rc;
use crate::storage::Direction;
use crate::storage::iterators::{ForwardIterator, ReverseIterator};

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
        range: Rc<InternalKeyRange>,
        snapshot: u64,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>> {

        let start = match range.start_bound() {
            InternalKeyBound::Unbounded => Unbounded,
            InternalKeyBound::Bounded(internal_key) => Bound::Included(internal_key.clone()),
        };

        let end = match range.end_bound() {
            InternalKeyBound::Unbounded => Unbounded,
            InternalKeyBound::Bounded(internal_key) => Bound::Included(internal_key.clone()),
        };

        let interval = Interval::new(start, end);
        let range = self.skiplist.range(interval);

        let iter: Box<dyn Iterator<Item = _>> = if direction == Direction::Reverse {
            Box::new(range.rev())
        } else {
            Box::new(range)
        };

        let iter = Box::new(RangeScanIterator { iter });

        Ok(if direction == Direction::Reverse {
            Box::new(ReverseIterator::new(iter, snapshot))
        } else {
            Box::new(ForwardIterator::new(iter, snapshot))
        })
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

pub struct RangeScanIterator<'a> {
    iter: Box<dyn Iterator<Item = crossbeam_skiplist::map::Entry<'a, Vec<u8>, Vec<u8>>> + 'a>,
}

impl <'a> Iterator for RangeScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|entry| Ok((entry.key().clone(), entry.value().clone())))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use crate::storage::internal_key::{encode_internal_key_range, MAX_SEQUENCE_NUMBER};
    use super::*;
    use crate::storage::operation::Operation;
    use crate::storage::test_utils::{assert_next_entry_eq, delete_op, delete_rec, put_op, put_rec, record_key, user_key};
    use crate::storage::write_batch::WriteBatch;

    #[test]
    fn write_put_operations() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT operations
        let collection: u32 = 32;
        let batch = write_batch(vec![
            put_op(collection, 1, 1),
            put_op(collection, 2, 1),
        ]);

        memtable.write(1, &batch);

        // Verify data was inserted
        assert_eq!(
            memtable.read(&record_key(collection, 1), MAX_SEQUENCE_NUMBER),
            Some(put_rec(collection, 1, 1, 1))
        );
        assert_eq!(
            memtable.read(&record_key(collection, 2), MAX_SEQUENCE_NUMBER),
            Some(put_rec(collection,2, 1, 1))
        );
    }

    #[test]
    fn write_put_and_delete_operations() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch = write_batch(vec![
            put_op(collection, 1, 1),
            delete_op(collection, 1),
        ]);

        memtable.write(1, &batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(&record_key(collection, 1), 2),
                   Some(delete_rec(collection, 1, 1)));
        assert_eq!(memtable.read(&record_key(collection, 1), MAX_SEQUENCE_NUMBER),
                   Some(delete_rec(collection, 1, 1)));
    }

    #[test]
    fn write_put_and_delete_operations_in_different_batches() {
        let memtable = Memtable::new(2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch = write_batch(vec![put_op(collection, 1, 1)]);

        memtable.write(1, &batch);

        let batch = write_batch(vec![delete_op(collection, 1)]);

        memtable.write(2, &batch);

        // Verify key1 was deleted
        assert_eq!(
            memtable.read(&record_key(collection, 1), MAX_SEQUENCE_NUMBER),
            Some(delete_rec(collection, 1, 2))
        );
    }

    #[test]
    fn read_with_snapshot() {
        let memtable = Memtable::new(2);

        let collection = 10;
        // Insert multiple versions of the same key
        let batch1 = write_batch(vec![put_op(collection, 1, 1)]);
        memtable.write(2, &batch1);

        let batch2 = write_batch(vec![put_op(collection, 1, 2)]);
        memtable.write(3, &batch2);

        // Read with snapshots
        let record_key = record_key(collection, 1);
        assert_eq!(memtable.read(&record_key, 1), None); // Snapshot 0
        assert_eq!(
            memtable.read(&record_key, 2),
            Some(put_rec(collection, 1, 1, 2))
        ); // Snapshot 1
        assert_eq!(
            memtable.read(&record_key, 3),
            Some(put_rec(collection, 1, 2, 3))
        ); // Snapshot 2
        assert_eq!(
            memtable.read(&record_key, MAX_SEQUENCE_NUMBER),
            Some(put_rec(collection, 1, 2, 3))
        ); // Latest
    }

    #[test]
    fn read_non_existent_key() {
        let memtable = Memtable::new(2);
        let collection = 32;
        // Read a key that was never inserted
        assert_eq!(
            memtable.read(&record_key(collection, -300), MAX_SEQUENCE_NUMBER),
            None
        );
    }

    #[test]
    fn range_scan_with_no_matching_keys() {
        let memtable = Memtable::new(2);

        let col: u32 = 32;
        let batch = write_batch(vec![
            put_op(col, 1, 1),
            put_op(col, 4, 1),
        ]);

        memtable.write(1, &batch);

        // Range scan: key2 to key3 (no matching keys)
        let range = Interval::closed(user_key(2), user_key(3));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();
        assert!(range_iter.next().is_none());
    }

    #[test]
    fn range_scan_with_different_ranges() {
        let memtable = Memtable::new(2);

        let col: u32 = 32;
        let batch1 = write_batch(vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
        ]);
        let batch2 = write_batch(vec![
            delete_op(col, 1),
            put_op(col, 2, 2),
            delete_op(col, 3),
        ]);
        let batch3 = write_batch(vec![
            put_op(col, 4, 1),
            put_op(col, 5, 1),
            put_op(col, 6, 1),
        ]);

        memtable.write(1, &batch1);
        memtable.write(2, &batch2);
        memtable.write(3, &batch3);

        let range = Interval::closed(user_key(2), user_key(6));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 6, 1, 3));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 6, 1, 3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert!(range_iter.next().is_none());

        let range = Interval::closed_open(user_key(2), user_key(6));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert!(range_iter.next().is_none());

        let range = Interval::open(user_key(2), user_key(6));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert!(range_iter.next().is_none());

        let range = Interval::open_closed(user_key(2), user_key(6));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 6, 1, 3));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 6, 1, 3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert!(range_iter.next().is_none());


        let range = Interval::at_least(user_key(2));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 6, 1, 3));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 6, 1, 3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 5, 1,3));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 4, 1,3));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert!(range_iter.next().is_none());
    }

    #[test]
    fn range_scan_with_mixed_operations() {
        let memtable = Memtable::new(2);

        let col: u32 = 32;
        let batch1 = write_batch(vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
        ]);
        let batch2 = write_batch(vec![
            delete_op(col, 1),
            put_op(col, 2, 2),
            delete_op(col, 3),
        ]);

        memtable.write(1, &batch1);
        memtable.write(2, &batch2);

        let range = Interval::closed(user_key(1), user_key(3));
        let internal_key_range = forward_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 1, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, MAX_SEQUENCE_NUMBER);
        let mut range_iter = memtable.range_scan(internal_key_range, MAX_SEQUENCE_NUMBER, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 1, 2));
        assert!(range_iter.next().is_none());

        let internal_key_range = forward_range(col, &range, 2);
        let mut range_iter = memtable.range_scan(internal_key_range, 2, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 1, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, 2);
        let mut range_iter = memtable.range_scan(internal_key_range, 2, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 3, 2));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 2, 2));
        assert_next_entry_eq(&mut range_iter, &delete_rec(col, 1, 2));
        assert!(range_iter.next().is_none());

        let internal_key_range = forward_range(col, &range, 1);
        let mut range_iter = memtable.range_scan(internal_key_range, 1, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 1, 1));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 3, 1, 1));
        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, 1);
        let mut range_iter = memtable.range_scan(internal_key_range, 1, Direction::Reverse).unwrap();

        assert_next_entry_eq(&mut range_iter, &put_rec(col, 3, 1, 1));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 2, 1, 1));
        assert_next_entry_eq(&mut range_iter, &put_rec(col, 1, 1, 1));
        assert!(range_iter.next().is_none());

        let internal_key_range = forward_range(col, &range, 0);
        let mut range_iter = memtable.range_scan(internal_key_range, 0, Direction::Forward).unwrap();

        assert!(range_iter.next().is_none());

        let internal_key_range = reverse_range(col, &range, 0);
        let mut range_iter = memtable.range_scan(internal_key_range, 0, Direction::Reverse).unwrap();

        assert!(range_iter.next().is_none());
    }

    #[test]
    fn test_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let memtable = Memtable::new(2);

        let collection = 10;
        let inserts = vec![
            put_op(collection, 1, 1),
            put_op(collection, 2, 1),
            put_op(collection, 3, 1),
            put_op(collection, 4, 1),
        ];

        let mut seq = 15;
        for insert in inserts {
            let _ = memtable.write(seq, &write_batch(vec!(insert)));
            seq += 1;
        }

        let sst_file = DbFile::new_sst(3);
        let sst = memtable.flush(&path, &sst_file, &Options::lightweight()).unwrap();

        let expected_size = path.join(sst_file.filename()).metadata().unwrap().len();

        let expected = SSTableMetadata::new(
            3,
            0,
            &record_key(collection, 1),
            &record_key(collection, 4),
            15,
            18,
            expected_size,
        );
        assert_eq!(sst, expected);
    }

    fn forward_range(collection: u32, range: &Interval<Vec<u8>>, seq: u64) -> Rc<InternalKeyRange> {
        encode_internal_key_range(collection, 0, range, seq, Direction::Forward)
    }

    fn reverse_range(collection: u32, range: &Interval<Vec<u8>>, seq: u64) -> Rc<InternalKeyRange> {
        encode_internal_key_range(collection, 0, range, seq, Direction::Reverse)
    }

    fn write_batch(operations: Vec<Operation>) -> WriteBatch {
        WriteBatch::new(operations)
    }
}
