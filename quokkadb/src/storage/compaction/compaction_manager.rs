use crate::io::sync_dir;
use crate::obs::metrics::MetricRegistry;
use crate::options::options::Options;
use crate::storage::callback::Callback;
use crate::storage::compaction::compaction_iterator::CompactionIterator;
use crate::storage::compaction::compaction_picker::{CompactionJob, CompactionPicker};
use crate::storage::files::DbFile;
use crate::storage::internal_key::extract_record_key;
use crate::storage::lsm_version::{DropMetadata, LevelItem, Levels, SplitResults};
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::sstable::sstable_writer::SSTableWriter;
use crate::storage::storage_engine::SSTableOperation;
#[cfg(test)]
use crate::storage::test_utils::fs_sync::{SyncPoint, before_sync};
use crate::util::interval::{IntervalPosition, has_overlapping_intervals};
use std::collections::VecDeque;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use tracing::{Span, trace_span};

struct CompactionTask {
    compaction_job: CompactionJob,
    oldest_active_snapshot: Option<u64>,
    callback: Arc<Callback<Result<SSTableOperation>>>,
}

struct Shared {
    queue: VecDeque<CompactionTask>,
    shutdown: bool,
}

pub struct CompactionManager {
    instance_span: Option<Span>,
    compaction_picker: Mutex<CompactionPicker>,
    shared: Arc<(Mutex<Shared>, Condvar)>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl CompactionManager {
    pub fn new(
        metric_registry: &mut MetricRegistry,
        options: Arc<Options>,
        db_dir: &Path,
        sst_cache: Arc<SSTableCache>,
        next_file_number: Arc<AtomicU64>,
    ) -> Result<Self> {
        let instance_span = Some(Span::current());
        let shared = Arc::new((
            Mutex::new(Shared {
                queue: VecDeque::new(),
                shutdown: false,
            }),
            Condvar::new(),
        ));

        let compaction_picker = Mutex::new(CompactionPicker::new(metric_registry, &options));

        let mut workers = Vec::with_capacity(options.compaction_threads());

        for i in 0..options.compaction_threads() {
            let shared_clone = Arc::clone(&shared);
            let options = options.clone();
            let db_dir = db_dir.to_path_buf();
            let sst_cache = sst_cache.clone();
            let next_file_number = next_file_number.clone();
            let instance_span = instance_span.clone();

            let handle = thread::Builder::new()
                .name(format!("compaction_manager-{i}"))
                .spawn(move || {
                    worker_loop(
                        shared_clone,
                        &options,
                        &db_dir,
                        sst_cache,
                        &next_file_number,
                        instance_span,
                    )
                })?;

            workers.push(handle);
        }

        let workers = Mutex::new(workers);

        Ok(Self {
            instance_span,
            compaction_picker,
            shared,
            workers,
        })
    }

    pub fn schedule_compaction_if_needed(
        &self,
        levels: &Levels,
        oldest_active_snapshot: Option<u64>,
        callback: &Arc<Callback<Result<SSTableOperation>>>,
    ) {
        while let Some(job) = self
            .compaction_picker
            .lock()
            .unwrap()
            .pick_compaction(levels)
        {
            let _instance_span = self.instance_span.as_ref().map(|span| span.enter());
            tracing::debug!(
                job_id = job.id,
                input_level = job.input_level,
                output_level = job.output_level,
                input_files = job.input_files.len(),
                "compaction job picked"
            );
            self.enqueue_compaction_job(job, oldest_active_snapshot, callback);
        }
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub fn schedule_single_compaction(
        &self,
        levels: &Levels,
        oldest_active_snapshot: Option<u64>,
        callback: &Arc<Callback<Result<SSTableOperation>>>,
    ) -> bool {
        if let Some(job) = self
            .compaction_picker
            .lock()
            .unwrap()
            .pick_compaction(levels)
        {
            self.enqueue_compaction_job(job, oldest_active_snapshot, callback);
            true
        } else {
            false
        }
    }

    fn enqueue_compaction_job(
        &self,
        job: CompactionJob,
        oldest_active_snapshot: Option<u64>,
        callback: &Arc<Callback<Result<SSTableOperation>>>,
    ) {
        let task = CompactionTask {
            compaction_job: job,
            oldest_active_snapshot,
            callback: callback.clone(),
        };
        self.enqueue(task);
    }

    fn enqueue(&self, task: CompactionTask) {
        let (lock, cvar) = &*self.shared;
        let mut shared = lock.lock().unwrap();

        if shared.shutdown {
            panic!("cannot enqueue task after shutdown");
        }

        shared.queue.push_back(task);
        cvar.notify_one();
    }

    pub fn shutdown(&self) {
        let _instance_span = self.instance_span.as_ref().map(|span| span.enter());
        tracing::debug!("compaction manager shutdown started");
        let (lock, cvar) = &*self.shared;
        {
            let mut shared = lock.lock().unwrap();
            if shared.shutdown {
                panic!("shutdown has been called twice");
            }
            shared.shutdown = true;
        }
        cvar.notify_all();

        for worker in self.workers.lock().unwrap().drain(..) {
            let _ = worker.join();
        }
    }

    pub fn unmark_compacting(&self, compaction_job: &CompactionJob) {
        self.compaction_picker
            .lock()
            .unwrap()
            .unmark_compacting(compaction_job);
    }
}

fn worker_loop(
    shared: Arc<(Mutex<Shared>, Condvar)>,
    options: &Options,
    db_dir: &PathBuf,
    sst_cache: Arc<SSTableCache>,
    next_file_number: &AtomicU64,
    instance_span: Option<Span>,
) {
    let (lock, cvar) = &*shared;

    loop {
        let task = {
            let mut shared = lock.lock().unwrap();

            loop {
                if let Some(task) = shared.queue.pop_front() {
                    break Some(task);
                }

                if shared.shutdown {
                    break None;
                }

                shared = cvar.wait(shared).unwrap();
            }
        };

        match task {
            Some(task) => {
                let _instance_span = instance_span.as_ref().map(|span| span.enter());
                let CompactionTask {
                    compaction_job,
                    oldest_active_snapshot,
                    callback,
                } = task;
                let _span = trace_span!(
                    "compaction.run",
                    job_id = compaction_job.id,
                    input_level = compaction_job.input_level,
                    output_level = compaction_job.output_level
                )
                .entered();

                let rs = perform_compaction(
                    options,
                    db_dir,
                    sst_cache.clone(),
                    compaction_job,
                    oldest_active_snapshot,
                    next_file_number,
                );
                match rs {
                    Ok(op) => {
                        if let SSTableOperation::Compaction {
                            compaction_job,
                            added_sstables,
                            removed_sstables,
                            removed_drops,
                            added_drops,
                        } = &op
                        {
                            tracing::trace!(
                                job_id = compaction_job.id,
                                input_level = compaction_job.input_level,
                                output_level = compaction_job.output_level,
                                compaction_job.id,
                                added = added_sstables.len(),
                                removed = removed_sstables.len(),
                                removed_drops = removed_drops.len(),
                                added_drops = added_drops.len(),
                                "compaction done"
                            );
                        }
                        callback.call(Ok(op))
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "compaction failed");
                        callback.call(Err(e))
                    }
                }
            }
            None => break,
        }
    }
}

fn perform_compaction(
    options: &Options,
    db_dir: &PathBuf,
    sst_cache: Arc<SSTableCache>,
    job: CompactionJob,
    oldest_active_snapshot: Option<u64>,
    next_file_number: &AtomicU64,
) -> Result<SSTableOperation> {
    assert!(
        job.drops.iter().all(|drop| {
            job.input_key_range.contains_interval(&drop.key_range)
                && job.output_key_range.contains_interval(&drop.key_range)
        }),
        "Compaction must process the entire range of each selected drop"
    );
    let sst_max_size = options.target_file_size_for_level(job.output_level);
    let drops_to_apply = drops_safe_to_apply(&job.drops, oldest_active_snapshot);

    let compaction_iter = CompactionIterator::new(
        db_dir,
        sst_cache.clone(),
        &job.input_files,
        &job.output_files,
        &drops_to_apply,
        oldest_active_snapshot,
        &job.deeper_level_ranges,
    )?;

    let mut added = Vec::new();

    let mut sstable_writer = new_sstable_writer(options, db_dir, next_file_number)?;
    let mut boundary_idx: usize = 0;
    let mut previous_record_key = Vec::new();

    for entry in compaction_iter {
        let (internal_key, value) = entry?;
        let record_key = extract_record_key(&internal_key);
        let is_new_record = previous_record_key.as_slice() != record_key;
        let mut crossed_boundary = false;

        if let Some(boundaries) = &job.partitions_grid {
            while boundary_idx < boundaries.len()
                && record_key > boundaries[boundary_idx].as_slice()
            {
                crossed_boundary = true;
                boundary_idx += 1;
            }
        }

        // Keep all retained versions of a record together, even if they exceed the target size.
        let estimated_size = sstable_writer.estimated_size();
        if estimated_size > 0
            && is_new_record
            && (crossed_boundary || estimated_size >= sst_max_size)
        {
            added.push(Arc::new(sstable_writer.finish()?));
            sstable_writer = new_sstable_writer(options, db_dir, next_file_number)?;
        }

        sstable_writer.add(&internal_key, &value)?;
        if is_new_record {
            previous_record_key.clear();
            previous_record_key.extend_from_slice(record_key);
        }
    }

    if sstable_writer.estimated_size() > 0 {
        added.push(Arc::new(sstable_writer.finish()?));
    }
    if !added.is_empty() {
        // All output files are synced; persist their names together before publication.
        #[cfg(test)]
        before_sync(db_dir, SyncPoint::Directory)?;
        sync_dir(db_dir)?;
    }
    let removed = job
        .input_files
        .iter()
        .cloned()
        .chain(job.output_files.iter().cloned())
        .collect();

    // A marker must survive until its range was filtered and no covered data can
    // remain below the output level. SSTable bounds conservatively prove absence.
    let added_drops = split_drops_if_needed(&job)
        .into_iter()
        .filter(|drop| {
            oldest_active_snapshot.is_some_and(|snapshot| drop.drop_sequence_number > snapshot)
                || has_overlapping_intervals(
                    std::slice::from_ref(&drop.key_range),
                    &job.deeper_level_ranges,
                )
        })
        .collect();
    let removed_drops = job.drops.clone();

    Ok(SSTableOperation::Compaction {
        compaction_job: job,
        added_sstables: added,
        removed_sstables: removed,
        removed_drops,
        added_drops,
    })
}

fn drops_safe_to_apply(
    drops: &[Arc<DropMetadata>],
    oldest_active_snapshot: Option<u64>,
) -> Vec<Arc<DropMetadata>> {
    match oldest_active_snapshot {
        Some(snapshot) => drops
            .iter()
            .filter(|drop| drop.drop_sequence_number <= snapshot)
            .cloned()
            .collect(),
        None => drops.to_vec(),
    }
}

/// Splits the drops, so that they match the output partitions
fn split_drops_if_needed(job: &CompactionJob) -> Vec<Arc<DropMetadata>> {
    if job.drops.is_empty() {
        return Vec::new();
    }

    // Drop are sorted by min key, so we can iterate on them and partition at the same time.
    if let Some(boundaries) = &job.partitions_grid {
        if boundaries.is_empty() {
            return job.drops.clone();
        }

        let mut output_drops = Vec::new();
        let mut partition_idx = 0;
        let mut drop_iter = job.drops.iter();
        let mut current_drop = drop_iter.next().cloned();

        while current_drop.is_some() && partition_idx < boundaries.len() {
            let drop = current_drop.unwrap();
            match drop
                .record_key_range()
                .position_of(&boundaries[partition_idx])
            {
                IntervalPosition::Before => {
                    // drop is after the partition, we should move to the next partition
                    partition_idx += 1;
                    current_drop = Some(drop);
                }
                IntervalPosition::Contained => {
                    // drop contains the partition end, we need to split it and include only
                    // the part within the partition
                    match drop.split_at(&boundaries[partition_idx]) {
                        SplitResults::One(only) => {
                            output_drops.push(only);
                            partition_idx += 1;
                            current_drop = drop_iter.next().cloned();
                        }
                        SplitResults::Two(left, right) => {
                            output_drops.push(left);
                            partition_idx += 1;
                            current_drop = Some(right);
                        }
                    }
                }
                IntervalPosition::After => {
                    // drop is before the partition end, so it belongs to the current partition.
                    output_drops.push(drop);
                    current_drop = drop_iter.next().cloned();
                }
            }
        }

        while current_drop.is_some() {
            output_drops.push(current_drop.unwrap());
            current_drop = drop_iter.next().cloned();
        }

        output_drops
    } else {
        job.drops.clone()
    }
}

fn new_sstable_writer<'a>(
    options: &'a Options,
    db_dir: &'a PathBuf,
    next_file_number: &'a AtomicU64,
) -> Result<SSTableWriter<'a>> {
    let sst_file = DbFile::new_sst(next_file_number.fetch_add(1, Ordering::Relaxed));
    Ok(SSTableWriter::new(db_dir, &sst_file, options)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::storage::Direction;
    use crate::storage::files::DbFile;
    use crate::storage::internal_key::InternalKeyRange;
    use crate::storage::lsm_version::{DropMetadata, SSTableMetadata};
    use crate::storage::sstable::sstable_cache::SSTableCache;
    use crate::storage::sstable::sstable_writer::SSTableWriter;
    use crate::storage::test_utils::fs_sync::{SyncHook, SyncPoint};
    use crate::storage::test_utils::{delete_rec, put_rec, record_key};
    use crate::util::interval::Interval;
    use std::ops::{Bound, RangeBounds};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use tempfile::tempdir;

    fn setup_cache(options: &Options) -> Arc<SSTableCache> {
        let mut metric_registry = MetricRegistry::new();
        Arc::new(SSTableCache::new(&mut metric_registry, &options))
    }

    fn write_sst(
        dir: &Path,
        sst_number: u64,
        entries: &[(Vec<u8>, Vec<u8>)],
        options: &Options,
    ) -> Arc<SSTableMetadata> {
        let sst_file = DbFile::new_sst(sst_number);
        let mut writer =
            SSTableWriter::new_with_expected_keys(dir, &sst_file, options, entries.len().max(1))
                .unwrap();
        for (key, value) in entries {
            writer.add(key, value).unwrap();
        }
        Arc::new(writer.finish().unwrap())
    }

    fn next_file_number(start: u64) -> AtomicU64 {
        AtomicU64::new(start)
    }

    fn read_sst_entries(
        dir: &Path,
        cache: Arc<SSTableCache>,
        sst: &Arc<SSTableMetadata>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let reader = cache
            .get(&dir.join(DbFile::new_sst(sst.number).filename()))
            .unwrap();
        reader
            .range_scan(InternalKeyRange::all(), Direction::Forward, true)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // Helper: assert the SSTableOperation is a Compaction and unpack it.
    // -----------------------------------------------------------------------
    fn unwrap_compaction(
        op: SSTableOperation,
    ) -> (
        CompactionJob,
        Vec<Arc<SSTableMetadata>>,
        Vec<Arc<SSTableMetadata>>,
        Vec<Arc<DropMetadata>>,
    ) {
        match op {
            SSTableOperation::Compaction {
                compaction_job,
                added_sstables,
                removed_sstables,
                removed_drops,
                added_drops,
            } => {
                assert_eq!(removed_drops, compaction_job.drops);
                (
                    compaction_job,
                    added_sstables,
                    removed_sstables,
                    added_drops,
                )
            }
            _ => panic!("Expected SSTableOperation::Compaction"),
        }
    }

    fn assert_record_versions_stay_together(partitioned: bool, include_next_record: bool) {
        let dir = tempdir().unwrap();
        // Every nonempty writer exceeds this target, including within a record's history.
        let options = Options::lightweight()
            .with_max_target_file_size(StorageQuantity::new(1, StorageUnit::Bytes));
        let cache = setup_cache(&options);
        let counter = next_file_number(10);
        let col = 10;
        let newest = put_rec(col, 50, 3, 5);
        let deletion = delete_rec(col, 50, 4);
        let floor = put_rec(col, 50, 2, 3);
        let obsolete = put_rec(col, 50, 1, 2);
        let next_record = put_rec(col, 70, 1, 6);

        // L0 files represent successive memtable flushes with disjoint sequence ranges.
        let first = write_sst(dir.path(), 1, &[floor.clone(), obsolete], &options);
        let mut second_entries = vec![newest.clone(), deletion.clone()];
        if include_next_record {
            second_entries.push(next_record.clone());
        }
        let second = write_sst(dir.path(), 2, &second_entries, &options);
        assert!(first.max_sequence_number < second.min_sequence_number);
        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![first, second],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            // The next record crosses both boundaries; neither should create an empty file.
            partitions_grid: partitioned.then(|| vec![record_key(col, 50), record_key(col, 60)]),
        };
        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache.clone(),
            job,
            Some(3),
            &counter,
        )
        .unwrap();
        let (_, added, removed, _) = unwrap_compaction(op);

        assert_eq!(removed.len(), 2);
        assert_eq!(added.len(), if include_next_record { 2 } else { 1 });
        assert_eq!(
            read_sst_entries(dir.path(), cache.clone(), &added[0]),
            vec![newest, deletion, floor]
        );
        assert_eq!(added[0].min_key, record_key(col, 50));
        assert_eq!(added[0].max_key, record_key(col, 50));
        assert!(added[0].size > options.target_file_size_for_level(1));
        if include_next_record {
            assert_eq!(
                read_sst_entries(dir.path(), cache, &added[1]),
                vec![next_record]
            );
        }
        for pair in added.windows(2) {
            assert!(pair[0].max_key < pair[1].min_key);
        }
    }

    #[test]
    fn test_compaction_uses_deeper_ranges_to_reclaim_point_tombstones() {
        for overlap in [false, true] {
            let dir = tempdir().unwrap();
            let options = Options::lightweight();
            let cache = setup_cache(&options);
            let deletion = delete_rec(10, 1, 2);
            let input = write_sst(
                dir.path(),
                1,
                &[deletion.clone(), put_rec(10, 1, 1, 1)],
                &options,
            );
            let job = CompactionJob {
                id: 0,
                input_level: 0,
                output_level: 1,
                input_files: vec![input.clone()],
                output_files: vec![],
                drops: vec![],
                input_key_range: Interval::all(),
                output_key_range: Interval::all(),
                deeper_level_ranges: if overlap {
                    vec![Interval::closed(record_key(10, 1), record_key(10, 1))]
                } else {
                    vec![]
                },
                partitions_grid: None,
            };
            let op = perform_compaction(
                &options,
                &dir.path().to_path_buf(),
                cache.clone(),
                job,
                None,
                &next_file_number(10),
            )
            .unwrap();
            let (_, added, removed, _) = unwrap_compaction(op);
            assert_eq!(removed, vec![input]);
            if overlap {
                assert_eq!(added.len(), 1);
                assert_eq!(
                    read_sst_entries(dir.path(), cache, &added[0]),
                    vec![deletion]
                );
            } else {
                assert!(added.is_empty());
            }
        }
    }

    #[test]
    fn test_size_split_keeps_retained_record_versions_together() {
        assert_record_versions_stay_together(false, true);
    }

    #[test]
    fn test_size_and_partition_split_keep_boundary_record_versions_together() {
        assert_record_versions_stay_together(true, true);
    }

    #[test]
    fn test_final_oversized_record_versions_stay_in_one_file() {
        assert_record_versions_stay_together(false, false);
    }

    // -----------------------------------------------------------------------
    // Full compaction, no partitions_grid (L0 → L1)
    // -----------------------------------------------------------------------

    /// The simplest case: a single input SST, no output files, no drops.
    /// After compaction there should be exactly one added SST and one removed SST.
    #[test]
    fn test_full_no_partitions_single_input_file() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        // Write an input SST with a few entries.
        let entries = vec![
            put_rec(col, 10, 1, 1),
            put_rec(col, 20, 1, 2),
            put_rec(col, 30, 1, 3),
        ];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, removed, drops) = unwrap_compaction(op);

        assert_eq!(compaction_job, job);
        assert_eq!(added.len(), 1, "Should produce exactly one output SST");
        assert_eq!(added[0].number, 10);
        assert_eq!(removed.len(), 1, "Input SST should be removed");
        assert_eq!(removed[0].number, input_sst.number);
        assert!(drops.is_empty());
        assert_eq!(counter.load(Ordering::SeqCst), 11);
    }

    /// Two input SSTables (L0 can have overlapping files) and two output files (L1).
    /// All four files should appear in `removed_sstables`.
    #[test]
    fn test_full_no_partitions_input_and_output_files_removed() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        // input SST: keys 10, 20
        let input1 = write_sst(
            dir.path(),
            1,
            &[put_rec(col, 10, 1, 5), put_rec(col, 20, 1, 6)],
            &options,
        );
        // another input SST: key 30
        let input2 = write_sst(dir.path(), 2, &[put_rec(col, 30, 1, 7)], &options);
        // output SST that overlaps: keys 15, 25
        let output1 = write_sst(
            dir.path(),
            3,
            &[put_rec(col, 15, 1, 1), put_rec(col, 25, 1, 2)],
            &options,
        );

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![input1.clone(), input2.clone()],
            output_files: vec![output1.clone()],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(compaction_job, job);
        assert!(!added.is_empty());

        let removed_numbers: Vec<u64> = removed.iter().map(|s| s.number).collect();
        assert!(removed_numbers.contains(&input1.number));
        assert!(removed_numbers.contains(&input2.number));
        assert!(removed_numbers.contains(&output1.number));
        assert_eq!(removed.len(), 3);
    }

    /// Applied drops overlapping deeper data are passed through without a partitions_grid.
    #[test]
    fn test_full_no_partitions_drops_passed_through() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let input_sst = write_sst(dir.path(), 1, &[put_rec(col, 10, 1, 1)], &options);

        let drop = DropMetadata::new_collection_drop(col, 50);

        let job = CompactionJob {
            deeper_level_ranges: vec![DropMetadata::new_collection_drop(col, 2).key_range.clone()],
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let hook = SyncHook::new(dir.path(), Some(SyncPoint::Directory));
        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job,
            None,
            &counter,
        )
        .unwrap();
        let (_compaction_job, added, removed, drops) = unwrap_compaction(op);

        // Deeper data requires retaining the drop; no partitions_grid means no split.
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0], drop);

        assert_eq!(added.len(), 0);
        assert!(hook.events().is_empty());
        assert_eq!(removed.len(), 1);
    }

    #[test]
    fn test_drop_is_carried_but_not_applied_when_snapshot_is_older_than_drop() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;
        let entries = vec![put_rec(col, 10, 1, 1)];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);
        let drop = DropMetadata::new_collection_drop(col, 50);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache.clone(),
            job,
            Some(40),
            &counter,
        )
        .unwrap();
        let (_job, added, _removed, drops) = unwrap_compaction(op);

        assert_eq!(drops, vec![drop]);
        assert_eq!(added.len(), 1);
        assert_eq!(read_sst_entries(dir.path(), cache, &added[0]), entries);
    }

    #[test]
    fn test_drop_is_applied_when_snapshot_is_old_enough() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;
        let input_sst = write_sst(dir.path(), 1, &[put_rec(col, 10, 1, 1)], &options);
        let drop = DropMetadata::new_collection_drop(col, 50);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache.clone(),
            job,
            Some(50),
            &counter,
        )
        .unwrap();
        let (_job, added, _removed, drops) = unwrap_compaction(op);

        assert!(
            drops.is_empty(),
            "Applied drop has no remaining deeper data"
        );
        assert!(
            added.is_empty(),
            "applied drop should erase compacted history"
        );
    }

    #[test]
    fn test_mixed_drop_visibility_applies_only_drops_visible_to_oldest_snapshot() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col_1 = 10;
        let col_2 = 11;

        let entries = vec![put_rec(col_1, 10, 1, 1), put_rec(col_2, 10, 1, 2)];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let applied_drop = DropMetadata::new_collection_drop(col_1, 40);
        let retained_drop = DropMetadata::new_collection_drop(col_2, 60);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![applied_drop.clone(), retained_drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache.clone(),
            job,
            Some(50),
            &counter,
        )
        .unwrap();
        let (_job, added, _removed, drops) = unwrap_compaction(op);

        assert_eq!(drops, vec![retained_drop]);
        assert_eq!(added.len(), 1);
        assert_eq!(
            read_sst_entries(dir.path(), cache, &added[0]),
            vec![put_rec(col_2, 10, 1, 2)]
        );
    }

    // -----------------------------------------------------------------------
    // Full compaction with empty partitions_grid (L1 → L2, max level empty)
    // -----------------------------------------------------------------------

    /// L1→L2 full compaction with an empty boundary list.
    /// Splitting logic skips the boundary loop entirely, so the output should be
    /// a single SST (or multiple if size limit is hit).
    #[test]
    fn test_full_empty_partitions_grid_no_split() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let entries: Vec<_> = (1..=5).map(|k| put_rec(col, k * 10, 1, k as u64)).collect();
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            // Empty boundary list: output is a partitioned level but max level is empty
            partitions_grid: Some(vec![]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(compaction_job, job);
        assert!(!added.is_empty());
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].number, input_sst.number);
    }

    /// Drops overlapping deeper data with an empty partitions_grid are passed through unchanged
    /// (split_drops_if_needed returns early when boundaries is empty).
    #[test]
    fn test_full_empty_partitions_grid_drops_passed_through() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let input_sst = write_sst(dir.path(), 1, &[put_rec(col, 10, 1, 1)], &options);
        let drop = DropMetadata::new_collection_drop(col, 99);

        let job = CompactionJob {
            deeper_level_ranges: vec![DropMetadata::new_collection_drop(col, 2).key_range.clone()],
            id: 0,
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job,
            None,
            &counter,
        )
        .unwrap();
        let (_compaction_job, _added, _removed, drops) = unwrap_compaction(op);

        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0], drop);
    }

    // -----------------------------------------------------------------------
    // Full compaction with non-empty partitions_grid (L1 → L2)
    // -----------------------------------------------------------------------

    /// Entries on either side of a single boundary should be split into two output SSTables.
    #[test]
    fn test_full_with_partitions_grid_splits_at_boundary() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        // boundary at record_key(50): keys <= 50 go to SST 0, keys > 50 go to SST 1.
        let boundary = record_key(col, 50);

        // Write entries spanning the boundary.
        let entries = vec![
            put_rec(col, 10, 1, 1),
            put_rec(col, 30, 1, 2),
            put_rec(col, 70, 1, 3),
            put_rec(col, 90, 1, 4),
        ];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary.clone()]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(compaction_job, job);
        assert_eq!(
            added.len(),
            2,
            "One boundary should produce two output SSTables"
        );
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].number, input_sst.number);

        // First SST should cover keys up to the boundary; second SST should cover keys after it.
        assert!(
            added[0].max_key <= boundary,
            "First SST max_key should be <= boundary"
        );
        assert!(
            added[1].min_key > boundary,
            "Second SST min_key should be > boundary"
        );
    }

    /// Two boundaries → three output SSTables.
    #[test]
    fn test_full_with_partitions_grid_two_boundaries_three_ssts() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let b1 = record_key(col, 30);
        let b2 = record_key(col, 60);

        let entries = vec![
            put_rec(col, 10, 1, 1),
            put_rec(col, 20, 1, 2),
            put_rec(col, 40, 1, 3),
            put_rec(col, 50, 1, 4),
            put_rec(col, 70, 1, 5),
            put_rec(col, 80, 1, 6),
        ];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![b1.clone(), b2.clone()]),
        };

        let hook = SyncHook::new(dir.path(), None);
        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache.clone(),
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, _removed, _drops) = unwrap_compaction(op);

        assert_eq!(
            hook.events(),
            vec![
                SyncPoint::File,
                SyncPoint::File,
                SyncPoint::File,
                SyncPoint::Directory
            ]
        );
        let recovered_entries: Vec<_> = added
            .iter()
            .flat_map(|sst| read_sst_entries(dir.path(), cache.clone(), sst))
            .collect();
        assert_eq!(recovered_entries, entries);
        assert_eq!(compaction_job, job);
        assert_eq!(
            added.len(),
            3,
            "Two boundaries should produce three output SSTables"
        );
        assert!(added[0].max_key <= b1);
        assert!(added[1].min_key > b1);
        assert!(added[1].max_key <= b2);
        assert!(added[2].min_key > b2);
    }

    /// When all entries are on one side of the boundary, only one non-empty SST is produced
    #[test]
    fn test_full_with_partitions_grid_all_entries_before_boundary() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let boundary = record_key(col, 100); // all entries are below this

        let entries = vec![put_rec(col, 10, 1, 1), put_rec(col, 20, 1, 2)];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job,
            None,
            &counter,
        )
        .unwrap();
        let (_compaction_job, added, _removed, _drops) = unwrap_compaction(op);

        // Entries only before boundary: the boundary-crossing loop never fires, so
        // we get exactly one output SST from the trailing push.
        assert_eq!(added.len(), 1);
    }

    /// Drops spanning the boundary should be split by split_drops_if_needed.
    #[test]
    fn test_full_with_partitions_grid_drop_split_at_boundary() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let boundary = record_key(col, 50);

        let input_sst = write_sst(
            dir.path(),
            1,
            &vec![put_rec(col, 10, 1, 1), put_rec(col, 70, 1, 2)],
            &options,
        );

        // A full collection drop that spans the boundary.
        let full_drop = DropMetadata::new_collection_drop(col, 10);

        let job = CompactionJob {
            deeper_level_ranges: vec![DropMetadata::new_collection_drop(col, 2).key_range.clone()],
            id: 0,
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![full_drop],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary.clone()]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job,
            None,
            &counter,
        )
        .unwrap();
        let (_level, _added, _removed, drops) = unwrap_compaction(op);

        // The drop should have been split at the boundary.
        assert_eq!(drops.len(), 2, "the drop should have been split");
        // The left fragment's end should be exactly the boundary (Included).
        assert_eq!(
            drops[0].key_range.end_bound(),
            Bound::Included(&boundary.clone()),
        );
        assert_eq!(
            drops[1].key_range.start_bound(),
            Bound::Excluded(&boundary.clone()),
        );
    }

    // -----------------------------------------------------------------------
    // Partial compaction (L2 → L3) — always has partitions_grid: Some(...)
    // -----------------------------------------------------------------------
    /// Partial compaction where the partition range holds all entries in one partition.
    /// Only files within the partition are touched; output level is 3.
    #[test]
    fn test_partial_single_partition_all_entries() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let entries = vec![put_rec(col, 10, 1, 1), put_rec(col, 20, 1, 2)];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        // Single partition (no boundaries) — everything goes into one SST.
        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 2,
            output_level: 3,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(compaction_job, job);
        assert_eq!(added.len(), 1);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].number, input_sst.number);
    }

    /// Partial compaction with one boundary that splits entries into two output SSTables.
    #[test]
    fn test_partial_with_boundary_splits_output() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let boundary = record_key(col, 50);

        let input_entries = vec![
            put_rec(col, 10, 1, 2),
            put_rec(col, 30, 1, 3),
            put_rec(col, 70, 1, 4),
        ];
        let input_sst = write_sst(dir.path(), 1, &input_entries, &options);

        // Output SST from L3 that will be merged.
        let output_entries = vec![put_rec(col, 20, 1, 1)];
        let output_sst = write_sst(dir.path(), 2, &output_entries, &options);

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 2,
            output_level: 3,
            input_files: vec![input_sst.clone()],
            output_files: vec![output_sst.clone()],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary.clone()]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job.clone(),
            None,
            &counter,
        )
        .unwrap();
        let (compaction_job, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(compaction_job, job);
        assert_eq!(
            added.len(),
            2,
            "Boundary should split into two output SSTables"
        );
        assert_eq!(
            removed.len(),
            2,
            "Both input and output SSTables should be removed"
        );

        let removed_numbers: Vec<u64> = removed.iter().map(|s| s.number).collect();
        assert!(removed_numbers.contains(&input_sst.number));
        assert!(removed_numbers.contains(&output_sst.number));

        assert!(added[0].max_key <= boundary);
        assert!(added[1].min_key > boundary);
    }

    /// Partial compaction into the bottom level retires an applied drop fragment.
    #[test]
    fn test_partial_drop_in_partition_is_retired_at_bottom() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let input_sst = write_sst(dir.path(), 1, &[put_rec(col, 10, 1, 1)], &options);

        // A drop already scoped to partition 0 (no need to split).
        let (drop_p0, _drop_p1) = DropMetadata::new_collection_drop(col, 10)
            .split_at(&record_key(col, 50))
            .expect_two();

        let job = CompactionJob {
            deeper_level_ranges: Vec::new(),
            id: 0,
            input_level: 2,
            output_level: 3,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![drop_p0.clone()],
            input_key_range: Interval::new(Bound::Unbounded, Bound::Included(record_key(col, 50))),
            output_key_range: Interval::new(Bound::Unbounded, Bound::Included(record_key(col, 50))),
            // boundary at 50 — drop is entirely within the left partition
            partitions_grid: Some(vec![record_key(col, 50)]),
        };

        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache,
            job,
            None,
            &counter,
        )
        .unwrap();
        let (_compaction_job, _added, _removed, drops) = unwrap_compaction(op);

        // The fragment includes the boundary, and all its data was processed.
        // No replacement is needed at the bottom level.
        assert!(drops.is_empty());
    }

    #[test]
    fn test_drop_retirement_respects_snapshot_and_fragment_boundaries() {
        let col = 10;
        let col2 = 11;
        let boundary = record_key(col, 50);
        let drop = DropMetadata::new_collection_drop(col, 50);
        let (left, right) = drop.split_at(&boundary).expect_two();
        let cases = [
            (vec![], vec![]),
            // The split boundary belongs only to the left fragment.
            (
                vec![Interval::closed(boundary.clone(), boundary.clone())],
                vec![left.clone()],
            ),
            (
                vec![Interval::open_closed(boundary.clone(), record_key(col, 70))],
                vec![right.clone()],
            ),
            (
                vec![Interval::closed(record_key(col, 40), record_key(col, 60))],
                vec![left.clone(), right.clone()],
            ),
            (
                vec![Interval::closed(record_key(col2, 1), record_key(col2, 2))],
                vec![],
            ),
        ];

        for snapshot in [None, Some(49), Some(50), Some(51)] {
            for (deeper_level_ranges, retained) in &cases {
                let dir = tempdir().unwrap();
                let options = Options::lightweight();
                let cache = setup_cache(&options);
                let entries = vec![
                    put_rec(col, 50, 1, 1),
                    put_rec(col, 70, 1, 2),
                    put_rec(col2, 1, 1, 3),
                ];
                let input = write_sst(dir.path(), 1, &entries, &options);
                let job = CompactionJob {
                    id: 0,
                    input_level: 0,
                    output_level: 1,
                    input_files: vec![input],
                    output_files: vec![],
                    drops: vec![drop.clone()],
                    input_key_range: Interval::all(),
                    output_key_range: Interval::all(),
                    deeper_level_ranges: deeper_level_ranges.clone(),
                    partitions_grid: Some(vec![boundary.clone()]),
                };
                let op = perform_compaction(
                    &options,
                    &dir.path().to_path_buf(),
                    cache.clone(),
                    job,
                    snapshot,
                    &next_file_number(10),
                )
                .unwrap();
                let (_, added, _, added_drops) = unwrap_compaction(op);
                let blocked = snapshot == Some(49);
                let expected_drops = if blocked {
                    vec![left.clone(), right.clone()]
                } else {
                    retained.clone()
                };
                assert_eq!(added_drops, expected_drops);
                let actual: Vec<_> = added
                    .iter()
                    .flat_map(|sst| read_sst_entries(dir.path(), cache.clone(), sst))
                    .collect();
                assert_eq!(
                    actual,
                    if blocked {
                        entries
                    } else {
                        vec![entries[2].clone()]
                    }
                );
            }
        }
    }

    #[test]
    fn test_bottom_compaction_preserves_snapshot_blocked_fragment() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let entries = vec![put_rec(10, 1, 1, 1)];
        let input = write_sst(dir.path(), 1, &entries, &options);
        let drop = DropMetadata::new_collection_drop(10, 50);
        let cache = setup_cache(&options);
        let job = CompactionJob {
            id: 0,
            input_level: 2,
            output_level: 3,
            input_files: vec![input],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            deeper_level_ranges: vec![],
            partitions_grid: None,
        };
        let op = perform_compaction(
            &options,
            &dir.path().to_path_buf(),
            cache.clone(),
            job,
            Some(49),
            &next_file_number(10),
        )
        .unwrap();
        let (_, added, _, added_drops) = unwrap_compaction(op);
        assert_eq!(added_drops, vec![drop]);
        assert_eq!(read_sst_entries(dir.path(), cache, &added[0]), entries);
    }

    mod split_drops_tests {
        use super::*;
        use crate::storage::test_utils::record_key;
        use std::ops::{Bound, RangeBounds};

        fn job_with_drops(
            drops: Vec<Arc<DropMetadata>>,
            partitions_grid: Option<Vec<Vec<u8>>>,
        ) -> CompactionJob {
            CompactionJob {
                deeper_level_ranges: Vec::new(),
                id: 0,
                input_level: 0,
                output_level: 1,
                input_files: vec![],
                output_files: vec![],
                drops,
                input_key_range: Interval::all(),
                output_key_range: Interval::all(),
                partitions_grid,
            }
        }

        #[test]
        fn test_no_drops_returns_empty() {
            let job = job_with_drops(vec![], None);
            let result = split_drops_if_needed(&job);
            assert!(result.is_empty());
        }

        #[test]
        fn test_no_drops_with_boundaries_returns_empty() {
            let col = 10;
            let job = job_with_drops(vec![], Some(vec![record_key(col, 50)]));
            let result = split_drops_if_needed(&job);
            assert!(result.is_empty());
        }

        #[test]
        fn test_none_partitions_grid_returns_drops_unchanged() {
            let col1 = 10;
            let col2 = 11;
            let d1 = DropMetadata::new_collection_drop(col1, 10);
            let d2 = DropMetadata::new_collection_drop(col2, 11);
            let expected = vec![d1.clone(), d2.clone()];
            let job = job_with_drops(expected.clone(), None);

            let result = split_drops_if_needed(&job);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_empty_partitions_grid_returns_drops_unchanged() {
            let col1 = 10;
            let col2 = 11;
            let d1 = DropMetadata::new_collection_drop(col1, 10);
            let d2 = DropMetadata::new_collection_drop(col2, 11);
            let expected = vec![d1.clone(), d2.clone()];
            let job = job_with_drops(expected.clone(), Some(vec![]));

            let result = split_drops_if_needed(&job);

            assert_eq!(result, expected);
        }

        #[test]
        fn test_drop_entirely_before_boundary() {
            let col1 = 10;
            let col2 = 11;
            let drop = DropMetadata::new_collection_drop(col1, 10);
            let job = job_with_drops(vec![drop.clone()], Some(vec![record_key(col2, 50)]));

            let result = split_drops_if_needed(&job);

            assert_eq!(result.len(), 1);
            assert_eq!(result[0], drop);
        }

        #[test]
        fn test_drop_ending_exactly_at_boundary() {
            let col = 10;
            let (left, _) = DropMetadata::new_collection_drop(col, 10)
                .split_at(&record_key(col, 50))
                .expect_two();
            let job = job_with_drops(vec![left.clone()], Some(vec![record_key(col, 50)]));

            let result = split_drops_if_needed(&job);

            assert_eq!(result.len(), 1);
            assert_eq!(result[0], left);
        }

        #[test]
        fn test_drop_starting_exactly_at_boundary() {
            let col = 10;
            let (_, right) = DropMetadata::new_collection_drop(col, 10)
                .split_at(&record_key(col, 80))
                .expect_two();
            let job = job_with_drops(vec![right.clone()], Some(vec![record_key(col, 80)]));

            let result = split_drops_if_needed(&&job);

            assert_eq!(result.len(), 1);
            assert_eq!(result[0], right);
        }

        #[test]
        fn test_drop_entirely_after_boundary() {
            let col1 = 10;
            let col2 = 11;
            let drop = DropMetadata::new_collection_drop(col2, 10);
            let job = job_with_drops(vec![drop.clone()], Some(vec![record_key(col1, 50)]));

            let result = split_drops_if_needed(&&job);

            assert_eq!(result.len(), 1);
            assert_eq!(result[0], drop);
        }

        #[test]
        fn test_drop_spanning_single_boundary_is_split() {
            let col = 10;
            let job = job_with_drops(
                vec![DropMetadata::new_collection_drop(col, 10)],
                Some(vec![record_key(col, 50)]),
            );

            let result = split_drops_if_needed(&&job);

            assert_eq!(result.len(), 2);
            assert_eq!(
                result[0].key_range.end_bound(),
                Bound::Included(&record_key(col, 50))
            );
            assert_eq!(
                result[1].key_range.start_bound(),
                Bound::Excluded(&record_key(col, 50))
            );
        }

        #[test]
        fn test_drop_spanning_two_boundaries_is_split_twice() {
            let col = 10;
            let job = job_with_drops(
                vec![DropMetadata::new_collection_drop(col, 10)],
                Some(vec![record_key(col, 30), record_key(col, 70)]),
            );

            let result = split_drops_if_needed(&&job);

            assert_eq!(result.len(), 3);
            assert_eq!(
                result[0].key_range.end_bound(),
                Bound::Included(&record_key(col, 30))
            );
            assert_eq!(
                result[1].key_range.end_bound(),
                Bound::Included(&record_key(col, 70))
            );
            assert_eq!(
                result[2].key_range.start_bound(),
                Bound::Excluded(&record_key(col, 70))
            );
        }

        #[test]
        fn test_multiple_drops_some_spanning_boundary() {
            let col1 = 10;
            let col2 = 11;
            let drop = DropMetadata::new_collection_drop(col1, 10);
            let spanning = DropMetadata::new_collection_drop(col2, 20);

            let job = job_with_drops(
                vec![drop.clone(), spanning.clone()],
                Some(vec![record_key(col2, 50)]),
            );

            let result = split_drops_if_needed(&&job);

            assert_eq!(result.len(), 3);
            assert_eq!(result[0], drop);
            assert_eq!(
                result[1].key_range.end_bound(),
                Bound::Included(&record_key(col2, 50))
            );
            assert_eq!(
                result[2].key_range.start_bound(),
                Bound::Excluded(&record_key(col2, 50))
            );
        }
    }
}
