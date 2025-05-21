use std::io::Result;
use std::ops::{Bound, RangeBounds};
use std::ops::Bound::Unbounded;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_skiplist::SkipMap;
use crate::options::options::Options;
use crate::storage::operation::OperationType;
use crate::storage::write_batch::WriteBatch;
use crate::util::interval::Interval;
use crate::obs::metrics::{Counter, HitRatioGauge, MetricRegistry};
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, extract_operation_type, extract_sequence_number, extract_user_key, MAX_SEQUENCE_NUMBER};
use crate::storage::lsm_version::SSTableMetadata;
use crate::storage::sstable::sstable_writer::SSTableWriter;

pub struct Memtable {
    metrics: Arc<MemtableMetrics>,
    skiplist: SkipMap<Vec<u8>, Vec<u8>>, // Binary values
    size: AtomicUsize, // Current size of the memtable
    pub log_number: u64, // The number of the write-ahead log file associated to this memtable
}

impl Memtable {
    pub fn new(metrics: Arc<MemtableMetrics>, log_number: u64) -> Self {

        Memtable {
            metrics,
            skiplist: SkipMap::new(),
            size: AtomicUsize::new(0),
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
            self.update_operation_counters(operation.operation_type());
        }
    }

    /// Update the memtable operation counters.
    ///
    /// # Arguments
    /// - `operation`: The type of the operation that has been performed
    pub fn update_operation_counters(&self, operation: OperationType) {
        match operation {
            OperationType::Put => { self.metrics.total_inserts.inc(); }
            OperationType::Delete => {self.metrics.total_deletes.inc(); }
            _ => panic!("Unexpected operation type {:?}", operation),
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

            return if extract_operation_type(compound_key) == OperationType::Put {
                self.metrics.hits.inc();
                Some(entry.value().clone()) // Found a valid PUT
            } else {
                self.metrics.misses.inc();
                None  // Deleted, stop searching
            }
        }
        self.metrics.misses.inc();
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
                 directory: &Path,
                 sst_file: &DbFile,
                 options: &Options
    ) -> Result<SSTableMetadata> {
        let mut writer = SSTableWriter::new(directory, sst_file, options, self.skiplist.len())?;
        for entry in self.skiplist.iter() {
            writer.add(entry.key(), entry.value())?;
        }
        writer.finish()
    }

    pub fn metrics(&self) -> Arc<MemtableMetrics> {
        self.metrics.clone()
    }
}

/// The Memtable exposed metrics
pub struct MemtableMetrics {
    /// Tracks the number of Memtable hits (reads satisfied by the MemTable).
    hits: Arc<Counter>,

    /// Tracks the number of Memtable misses (reads not found in the MemTable).
    misses: Arc<Counter>,

    /// Tracks the total number of inserts (PUT operations) into the MemTable.
    total_inserts: Arc<Counter>,

    /// Tracks the total number of delete operations applied to the MemTable.
    total_deletes: Arc<Counter>,

    /// The ratio of MemTable hits to the total number of lookups (hits + misses)
    hit_ratio: Arc<HitRatioGauge>,
}

impl MemtableMetrics {
    pub fn new() -> Arc<Self> {
        let memtable_hit = Counter::new();
        let memtable_miss = Counter::new();

        Arc::new(Self {
            hits: memtable_hit.clone(),
            misses: memtable_miss.clone(),
            total_inserts: Counter::new(),
            total_deletes: Counter::new(),
            hit_ratio: HitRatioGauge::new(memtable_hit, memtable_miss),
        })
    }

    pub fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry.register_counter("memtable_hit", &self.hits)
            .register_counter("memtable_miss", &self.misses)
            .register_counter("memtable_total_inserts", &self.total_inserts)
            .register_counter("memtable_total_deletes", &self.total_deletes)
            .register_hit_ratio_gauge("memtable_hit_ratio", &self.hit_ratio);
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
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

        // Create a WriteBatch with PUT operations
        let collection: u32 = 32;
        let batch = write_batch(vec![put(collection, b"key1",  b"value1"), put(collection,b"key2", b"value2")]);

        memtable.write(1, &batch);

        // Verify data was inserted
        assert_eq!(memtable.read(collection, b"key1", MAX_SEQUENCE_NUMBER), Some(Vec::from(b"value1")));
        assert_eq!(memtable.read(collection, b"key2", MAX_SEQUENCE_NUMBER), Some(Vec::from(b"value2")));

        // Verify metrics
        let metrics = memtable.metrics.clone();
        assert_eq!(metrics.total_inserts.get(), 2);
        assert_eq!(metrics.total_deletes.get(), 0);
        assert_eq!(metrics.misses.get(), 0);
        assert_eq!(metrics.hits.get(), 2);
    }

    #[test]
    fn test_write_put_and_delete_operations() {
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch =
            write_batch(vec![put(collection,b"key1", b"value1"), delete(collection, b"key1")]);

        memtable.write(1, &batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(collection, b"key1", 2), None);
        assert_eq!(memtable.read(collection, b"key1", MAX_SEQUENCE_NUMBER), None);

        // Verify statistics
        let stats = memtable.metrics.clone();
        assert_eq!(stats.total_inserts.get(), 1);
        assert_eq!(stats.total_deletes.get(), 1);
        assert_eq!(stats.misses.get(), 2);
        assert_eq!(stats.hits.get(), 0);
    }

    #[test]
    fn test_write_put_and_delete_operations_in_different_batches() {
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

        // Create a WriteBatch with PUT and DELETE operations
        let collection: u32 = 32;
        let batch = write_batch(vec![put(collection, b"key1", b"value1")]);

        memtable.write(1, &batch);

        let batch = write_batch(vec![delete(collection, b"key1")]);

        memtable.write(2, &batch);

        // Verify key1 was deleted
        assert_eq!(memtable.read(collection, b"key1", MAX_SEQUENCE_NUMBER), None);

        // Verify statistics
        let stats = memtable.metrics.clone();
        assert_eq!(stats.total_inserts.get(), 1);
        assert_eq!(stats.total_deletes.get(), 1);
        assert_eq!(stats.misses.get(), 1);
        assert_eq!(stats.hits.get(), 0);
    }

    #[test]
    fn test_read_with_snapshot() {
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

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
        let stats = memtable.metrics.clone();
        assert_eq!(stats.total_inserts.get(), 2);
        assert_eq!(stats.total_deletes.get(), 0);
        assert_eq!(stats.misses.get(), 1);
        assert_eq!(stats.hits.get(), 3);
    }

    #[test]
    fn test_read_non_existent_key() {
        let memtable = Memtable::new(MemtableMetrics::new(), 2);
        let collection = 32;
        // Read a key that was never inserted
        assert_eq!(memtable.read(collection, b"non_existent_key", MAX_SEQUENCE_NUMBER), None);
        let stats = memtable.metrics.clone();
        assert_eq!(stats.misses.get(), 1);
        assert_eq!(stats.hits.get(), 0);
    }

    #[test]
    fn test_range_scan_with_no_matching_keys() {
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

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
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

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
        let memtable = Memtable::new(MemtableMetrics::new(), 2);

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