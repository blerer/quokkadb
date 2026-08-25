use std::io::Result;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::files::DbFile;
use crate::storage::internal_key::{extract_record_key, extract_sequence_number, InternalKeyRange};
use crate::storage::iterators::MergeIterator;
use crate::storage::lsm_version::{DropMetadata, LevelItem, SSTableMetadata};
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::Direction;
use crate::util::interval::{Interval, IntervalPosition};

/// An iterator that merges records from multiple SSTables while applying drops to skip deleted
/// records during compaction.
pub struct CompactionIterator<'a> {
    record_iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    drop_iter: Box<dyn Iterator<Item = Arc<DropMetadata>>>,
    current_drop_interval: Option<Interval<Vec<u8>>>,
}

impl<'a> CompactionIterator<'a> {
    /// Creates a new `CompactionIterator` for the given compaction job. It initializes iterators
    /// for the input and output SSTables, applies drops to skip deleted records, and merges
    /// records from multiple SSTables if necessary.
    pub fn new(
        db_dir: &PathBuf,
        sst_cache: Arc<SSTableCache>,
        input_files: &[Arc<SSTableMetadata>],
        output_files: &[Arc<SSTableMetadata>],
        drops: &[Arc<DropMetadata>],
        oldest_active_snapshot: Option<u64>,
    ) -> Result<Box<Self>> {
        // drops should be sorted and should be non overlapping after compaction picker's merge/split logic
        let drops: Vec<Arc<DropMetadata>> = drops.iter().cloned().collect();

        let sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>> = input_files
            .into_iter()
            .chain(output_files.into_iter())
            .filter(|sst| !sst_skippable_due_to_drops(sst.as_ref(), &drops))
            .map(|sst| {
                let file_path = db_dir.join(DbFile::new_sst(sst.number).filename());
                let reader = sst_cache.get(&file_path)?;
                reader.range_scan(InternalKeyRange::all(), Direction::Forward)
            })
            .collect::<Result<Vec<_>>>()?;

        if sources.is_empty() {
            return Ok(Box::new(Self {
                record_iter: Box::new(std::iter::empty()),
                drop_iter: Box::new(std::iter::empty()),
                current_drop_interval: None,
            }));
        }

        let record_iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a> =
            if sources.len() == 1 {
                Box::new(sources.into_iter().next().unwrap())
            } else {
                Box::new(MergeIterator::new(sources, Direction::Forward)?)
            };

        let record_iter = Box::new(VersionRetentionIterator::new(
            record_iter,
            oldest_active_snapshot,
        ));

        let mut drop_iter: Box<dyn Iterator<Item = Arc<DropMetadata>>> =
            Box::new(drops.into_iter());
        let current_drop_interval = drop_iter
            .next()
            .map_or(None, |drop| Some(drop.record_key_range().clone()));

        Ok(Box::new(Self {
            record_iter,
            drop_iter,
            current_drop_interval,
        }))
    }
}

struct VersionRetentionIterator<'a> {
    iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    oldest_active_snapshot: Option<u64>,
    previous_record_key: Option<Vec<u8>>,
    emitted_floor_version: bool,
}

impl<'a> VersionRetentionIterator<'a> {
    fn new(
        iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
        oldest_active_snapshot: Option<u64>,
    ) -> Self {
        Self {
            iter,
            oldest_active_snapshot,
            previous_record_key: None,
            emitted_floor_version: false,
        }
    }
}

impl<'a> Iterator for VersionRetentionIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.iter.next() {
            match entry {
                Ok((key, value)) => {
                    let record_key = extract_record_key(&key);
                    let is_new_record = self
                        .previous_record_key
                        .as_ref()
                        .is_none_or(|previous| previous.as_slice() != record_key);

                    if is_new_record {
                        self.previous_record_key = Some(record_key.to_vec());
                        self.emitted_floor_version = false;
                    }

                    match self.oldest_active_snapshot {
                        None => {
                            if is_new_record {
                                return Some(Ok((key, value)));
                            }
                        }
                        Some(snapshot_floor) => {
                            let seq = extract_sequence_number(&key);
                            if seq > snapshot_floor {
                                return Some(Ok((key, value)));
                            }
                            if !self.emitted_floor_version {
                                self.emitted_floor_version = true;
                                return Some(Ok((key, value)));
                            }
                        }
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }

        None
    }
}

impl<'a> Iterator for CompactionIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.record_iter.next() {
            match entry {
                Ok((key, value)) => loop {
                    match &self.current_drop_interval {
                        Some(interval) => match interval.position_of(extract_record_key(&key)) {
                            IntervalPosition::Before => return Some(Ok((key, value))),
                            IntervalPosition::Contained => break,
                            IntervalPosition::After => {
                                self.current_drop_interval = self
                                    .drop_iter
                                    .next()
                                    .map_or(None, |drop| Some(drop.record_key_range()));
                            }
                        },
                        None => return Some(Ok((key, value))),
                    }
                },
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

/// Determines if an SSTable can be skipped during compaction because it is fully covered by one of the drops.
fn sst_skippable_due_to_drops(sst: &SSTableMetadata, drops: &[Arc<DropMetadata>]) -> bool {
    if drops.is_empty() {
        return false;
    }
    let sst_range = sst.record_key_range();
    drops
        .iter()
        .any(|drop| drop.key_range.contains_interval(&sst_range))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::storage::files::DbFile;
    use crate::storage::sstable::sstable_writer::SSTableWriter;
    use crate::storage::test_utils::{delete_rec, put_rec, record_key};
    use tempfile::tempdir;

    const COL: u32 = 10;

    fn write_sst(
        dir: &std::path::Path,
        sst_number: u64,
        entries: &[(Vec<u8>, Vec<u8>)],
        options: &Options,
    ) -> Arc<SSTableMetadata> {
        let sst_file = DbFile::new_sst(sst_number);
        let mut writer =
            SSTableWriter::new_with_expected_keys(dir, &sst_file, options, entries.len()).unwrap();

        for (key, value) in entries {
            writer.add(key, value).unwrap();
        }

        Arc::new(writer.finish().unwrap())
    }

    fn setup_cache() -> Arc<SSTableCache> {
        let options = Options::lightweight();
        let mut metric_registry = MetricRegistry::new();
        Arc::new(SSTableCache::new(&mut metric_registry, &options))
    }

    #[test]
    fn test_compaction_iterator_single_sst_no_drops() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let entries = vec![
            put_rec(COL, 1, 1, 10),
            put_rec(COL, 2, 1, 11),
            put_rec(COL, 3, 1, 12),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        let mut iter =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[], None)
                .unwrap();

        for expected in &entries {
            let actual = iter.next().unwrap().unwrap();
            assert_eq!(actual, *expected);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_merges_multiple_ssts() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        // SST 1: keys 1, 3, 5
        let entries1 = vec![
            put_rec(COL, 1, 1, 10),
            put_rec(COL, 3, 1, 12),
            put_rec(COL, 5, 1, 14),
        ];
        let sst1 = write_sst(path, 1, &entries1, &options);

        // SST 2: keys 2, 4, 6
        let entries2 = vec![
            put_rec(COL, 2, 1, 11),
            put_rec(COL, 4, 1, 13),
            put_rec(COL, 6, 1, 15),
        ];
        let sst2 = write_sst(path, 2, &entries2, &options);

        let mut iter =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst1], &[sst2], &[], None)
                .unwrap();

        // Should be merged in sorted order
        let expected_keys: Vec<i32> = vec![1, 2, 3, 4, 5, 6];
        for expected_key in expected_keys {
            let (key, _) = iter.next().unwrap().unwrap();
            let record_key_bytes = extract_record_key(&key);
            assert_eq!(record_key_bytes, record_key(COL, expected_key));
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_drops_single_collection() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let col_1 = COL;
        let col_2 = COL + 1;
        let col_3 = COL + 2;

        let entries = vec![
            put_rec(col_1, 1, 1, 9),
            put_rec(col_1, 2, 1, 10),
            put_rec(col_2, 1, 1, 11),
            put_rec(col_2, 2, 1, 12),
            put_rec(col_3, 1, 1, 13),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        // Create a drop that covers collection 1
        let drop = DropMetadata::new_collection_drop(col_1, 100);

        let mut iter =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[drop], None)
                .unwrap();

        let expected_entries = vec![
            put_rec(col_2, 1, 1, 11),
            put_rec(col_2, 2, 1, 12),
            put_rec(col_3, 1, 1, 13),
        ];

        for expected in &expected_entries {
            let actual = iter.next().unwrap().unwrap();
            assert_eq!(actual, *expected);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_multiple_drops() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let col_1 = COL;
        let col_2 = COL + 1;
        let col_3 = COL + 2;

        let entries = vec![
            put_rec(col_1, 1, 1, 9),
            put_rec(col_1, 2, 1, 10),
            put_rec(col_2, 1, 1, 11),
            put_rec(col_2, 2, 1, 12),
            put_rec(col_3, 1, 1, 13),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        // Create a drop that covers collection 1
        let drop_1 = DropMetadata::new_collection_drop(col_1, 100);
        // Create a drop that covers part of collection 3
        let drop_3 = DropMetadata::new_collection_drop(col_3, 101)
            .split_at(&record_key(col_3, 50))
            .expect_two()
            .0;

        let mut iter = CompactionIterator::new(
            &path.to_path_buf(),
            sst_cache,
            &[sst],
            &[],
            &[drop_1, drop_3],
            None,
        )
        .unwrap();

        let expected_entries = vec![put_rec(col_2, 1, 1, 11), put_rec(col_2, 2, 1, 12)];

        for expected in &expected_entries {
            let actual = iter.next().unwrap().unwrap();
            assert_eq!(actual, *expected);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_drop_all_records() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let entries = vec![
            put_rec(COL, 1, 1, 10),
            put_rec(COL, 2, 1, 11),
            put_rec(COL, 3, 1, 12),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        // Create a drop that covers all keys
        let drop = DropMetadata::new_collection_drop(COL, 100);

        let mut iter =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[drop], None)
                .unwrap();

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_retains_versions_needed_by_oldest_snapshot() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let entries = vec![
            put_rec(COL, 1, 4, 4),
            put_rec(COL, 1, 3, 3),
            put_rec(COL, 1, 2, 2),
            put_rec(COL, 1, 1, 1),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        let retained: Vec<_> =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[], Some(2))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

        assert_eq!(
            retained,
            vec![
                put_rec(COL, 1, 4, 4),
                put_rec(COL, 1, 3, 3),
                put_rec(COL, 1, 2, 2),
            ]
        );
    }

    #[test]
    fn test_compaction_iterator_retains_single_floor_version_when_all_versions_are_old() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let entries = vec![
            put_rec(COL, 1, 3, 3),
            put_rec(COL, 1, 2, 2),
            put_rec(COL, 1, 1, 1),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        let retained: Vec<_> =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[], Some(5))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

        assert_eq!(retained, vec![put_rec(COL, 1, 3, 3)]);
    }

    #[test]
    fn test_compaction_iterator_retains_exactly_one_version_at_snapshot_floor() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let entries = vec![
            put_rec(COL, 1, 5, 5),
            put_rec(COL, 1, 4, 4),
            put_rec(COL, 1, 3, 3),
            put_rec(COL, 1, 2, 2),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        let retained: Vec<_> =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[], Some(4))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

        assert_eq!(
            retained,
            vec![put_rec(COL, 1, 5, 5), put_rec(COL, 1, 4, 4),]
        );
    }

    #[test]
    fn test_compaction_iterator_resets_snapshot_floor_retention_per_record_key() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let entries = vec![
            put_rec(COL, 1, 5, 5),
            put_rec(COL, 1, 4, 4),
            put_rec(COL, 1, 3, 3),
            put_rec(COL, 2, 6, 6),
            put_rec(COL, 2, 4, 4),
            put_rec(COL, 2, 2, 2),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        let retained: Vec<_> =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[], Some(4))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

        assert_eq!(
            retained,
            vec![
                put_rec(COL, 1, 5, 5),
                put_rec(COL, 1, 4, 4),
                put_rec(COL, 2, 6, 6),
                put_rec(COL, 2, 4, 4),
            ]
        );
    }

    #[test]
    fn test_compaction_iterator_skips_sst_covered_by_drop() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        let col_1 = COL;
        let col_2 = COL + 1;

        // SST 1: keys 1-3
        let entries1 = vec![
            put_rec(col_1, 1, 1, 10),
            put_rec(col_1, 2, 1, 11),
            put_rec(col_1, 3, 1, 12),
        ];
        let sst1 = write_sst(path, 1, &entries1, &options);

        // SST 2: keys 10-12 (non-overlapping with drop)
        let entries2 = vec![
            put_rec(col_2, 10, 1, 20),
            put_rec(col_2, 11, 1, 21),
            put_rec(col_2, 12, 1, 22),
        ];
        let sst2 = write_sst(path, 2, &entries2, &options);

        // Create a drop that fully covers SST 1
        let drop = DropMetadata::new_collection_drop(col_1, 100);

        let mut iter = CompactionIterator::new(
            &path.to_path_buf(),
            sst_cache,
            &[sst1, sst2],
            &[],
            &[drop],
            None,
        )
        .unwrap();

        // SST 1 should be skipped entirely, only entries from SST 2 should be returned
        for expected in &entries2 {
            let actual = iter.next().unwrap().unwrap();
            assert_eq!(actual, *expected);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_handles_deletes() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        // Include both puts and deletes
        let entries = vec![
            put_rec(COL, 1, 1, 10),
            delete_rec(COL, 2, 11),
            put_rec(COL, 3, 1, 12),
        ];

        let sst = write_sst(path, 1, &entries, &options);

        let mut iter =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst], &[], &[], None)
                .unwrap();

        // All entries including deletes should be present
        for expected in &entries {
            let actual = iter.next().unwrap().unwrap();
            assert_eq!(actual, *expected);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_compaction_iterator_mvcc_deduplication() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::lightweight();
        let sst_cache = setup_cache();

        // Multiple versions of the same key across two SSTables
        // SST 1: older version of key 1
        let entries1 = vec![put_rec(COL, 1, 1, 10)];
        let sst1 = write_sst(path, 1, &entries1, &options);

        // SST 2: newer version of key 1
        let entries2 = vec![put_rec(COL, 1, 2, 20)];
        let sst2 = write_sst(path, 2, &entries2, &options);

        let mut iter =
            CompactionIterator::new(&path.to_path_buf(), sst_cache, &[sst1], &[sst2], &[], None)
                .unwrap();

        // ForwardIterator should deduplicate, keeping only the newest version (seq 20)
        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(extract_record_key(&key), record_key(COL, 1));
        assert_eq!((key, value), entries2[0]);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_sst_skippable_due_to_drops_fully_covered() {
        let sst = SSTableMetadata::new(
            1,
            0,
            &record_key(COL, 10),
            &record_key(COL, 20),
            100,
            200,
            1000,
        );

        // Drop that fully covers the SST
        let drop = DropMetadata::new_collection_drop(COL, 300);

        assert!(sst_skippable_due_to_drops(&sst, &[drop]));
    }

    #[test]
    fn test_sst_skippable_due_to_drops_partially_covered() {
        let sst = SSTableMetadata::new(
            1,
            0,
            &record_key(COL, 10),
            &record_key(COL, 20),
            100,
            200,
            1000,
        );

        // Drop that only partially covers the SST: take a full collection drop then split.
        let full = DropMetadata::new_collection_drop(COL, 300);
        let (left, _right) = full.split_at(&record_key(COL, 15)).expect_two();
        let (_discard, mid) = left.split_at(&record_key(COL, 5)).expect_two();
        let drop = mid;

        assert!(!sst_skippable_due_to_drops(&sst, &[drop]));
    }

    #[test]
    fn test_sst_skippable_due_to_drops_no_overlap() {
        let sst = SSTableMetadata::new(
            1,
            0,
            &record_key(COL, 10),
            &record_key(COL, 20),
            100,
            200,
            1000,
        );

        // Drop that doesn't overlap with the SST: take a full collection drop then split.
        let full = DropMetadata::new_collection_drop(COL, 300);
        let (left, _right) = full.split_at(&record_key(COL, 40)).expect_two();
        let (_discard, mid) = left.split_at(&record_key(COL, 30)).expect_two();
        let drop = mid;

        assert!(!sst_skippable_due_to_drops(&sst, &[drop]));
    }

    #[test]
    fn test_sst_skippable_due_to_drops_empty_drops() {
        let sst = SSTableMetadata::new(
            1,
            0,
            &record_key(COL, 10),
            &record_key(COL, 20),
            100,
            200,
            1000,
        );

        assert!(!sst_skippable_due_to_drops(&sst, &[]));
    }

    #[test]
    fn test_sst_skippable_due_to_drops_multiple_drops() {
        let sst = SSTableMetadata::new(
            1,
            0,
            &record_key(COL, 10),
            &record_key(COL, 20),
            100,
            200,
            1000,
        );

        // First drop doesn't cover SST: take a full collection drop then split.
        let full1 = DropMetadata::new_collection_drop(COL, 300);
        let (left, _right) = full1.split_at(&record_key(COL, 5)).expect_two();
        let (_discard, drop1) = left.split_at(&record_key(COL, 1)).expect_two();

        // Second drop fully covers SST
        let drop2 = DropMetadata::new_collection_drop(COL, 301);

        assert!(sst_skippable_due_to_drops(&sst, &[drop1, drop2]));
    }
}
