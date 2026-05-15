use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::{debug, event, info, warn};
use crate::options::options::Options;
use crate::storage::callback::Callback;
use crate::storage::compaction_picker::{CompactionJob, CompactionPicker};
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::storage_engine::SSTableOperation;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::io::Result;
use crate::obs::metrics::MetricRegistry;
use crate::storage::compaction_iterator::CompactionIterator;
use crate::storage::files::DbFile;
use crate::storage::internal_key::extract_record_key;
use crate::storage::lsm_version::{DropMetadata, LevelItem, Levels, SplitResults};
use crate::storage::sstable::sstable_writer::SSTableWriter;
use crate::util::interval::IntervalPosition;

struct CompactionTask {
    compaction_job: CompactionJob,
    callback: Arc<Callback<Result<SSTableOperation>>>,
}

struct Shared {
    queue: VecDeque<CompactionTask>,
    shutdown: bool,
}

pub struct CompactionManager {
    logger: Arc<dyn LoggerAndTracer>,
    compaction_picker: Arc<Mutex<CompactionPicker>>,
    shared: Arc<(Mutex<Shared>, Condvar)>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl CompactionManager {
    pub fn new(logger: Arc<dyn LoggerAndTracer>,
               metric_registry: &mut MetricRegistry,
               options: Arc<Options>,
               db_dir: &Path,
               sst_cache: Arc<SSTableCache>,
               next_file_number: Arc<AtomicU64>) -> Result<Self> {

        let shared = Arc::new((
            Mutex::new(Shared {
                queue: VecDeque::new(),
                shutdown: false,
            }),
            Condvar::new(),
        ));

        let compaction_picker = Arc::new(Mutex::new(CompactionPicker::new(logger.clone(), metric_registry, &options)));

        let mut workers = Vec::with_capacity(options.compaction_threads());
        let worker_logger = logger.clone();

        for i in 0..options.compaction_threads() {
            let shared_clone = Arc::clone(&shared);
            let options = options.clone();
            let db_dir = db_dir.to_path_buf();
            let sst_cache = sst_cache.clone();
            let next_file_number = next_file_number.clone();
            let compaction_picker = compaction_picker.clone();
            let logger = worker_logger.clone();

            let handle = thread::Builder::new()
                .name(format!("compaction_manager-{i}"))
                .spawn(move || worker_loop(shared_clone, &options, &db_dir, sst_cache, &next_file_number, compaction_picker, logger))?;

            workers.push(handle);
        }

        let workers = Mutex::new(workers);

        Ok(Self {
            logger,
            compaction_picker,
            shared,
            workers,
        })
    }

    pub fn schedule_compaction_if_needed(&self, levels: &Levels, callback: &Arc<Callback<Result<SSTableOperation>>>) {

        while let Some(job) = self.compaction_picker.lock().unwrap().pick_compaction(levels) {
            debug!(
                self.logger,
                "compaction job picked, input_level={}, output_level={}, input_files={}",
                job.input_level,
                job.output_level,
                job.input_files.len()
            );
            self.enqueue_compaction_job(job, callback);
        }
    }

    #[cfg(test)]
    pub fn schedule_single_compaction(&self,
                                      levels: &Levels,
                                      callback: &Arc<Callback<Result<SSTableOperation>>>
    ) -> bool
    {
        if let Some(job) = self.compaction_picker.lock().unwrap().pick_compaction(levels) {
            self.enqueue_compaction_job(job, callback);
            true
        } else {
            false
        }
    }

    fn enqueue_compaction_job(&self,
                              job: CompactionJob,
                              callback: &Arc<Callback<Result<SSTableOperation>>>
    ) {
        let task = CompactionTask {
            compaction_job: job,
            callback: callback.clone(),
        };
        self.enqueue(task);
    }

    fn enqueue(&self, task: CompactionTask)
    {
        let (lock, cvar) = &*self.shared;
        let mut shared = lock.lock().unwrap();

        if shared.shutdown {
            panic!("cannot enqueue task after shutdown");
        }

        shared.queue.push_back(task);
        cvar.notify_one();
    }

    pub fn shutdown(&self) {
        info!(self.logger, "compaction manager shutdown started");
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
}

fn worker_loop(shared: Arc<(Mutex<Shared>, Condvar)>,
               options: &Options,
               db_dir: &PathBuf,
               sst_cache: Arc<SSTableCache>,
               next_file_number: &AtomicU64,
               compaction_picker: Arc<Mutex<CompactionPicker>>,
               logger: Arc<dyn LoggerAndTracer>)
{
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

                let CompactionTask { compaction_job, callback}  =  task;
                event!(
                    logger,
                    "compaction start, input_level={}, output_level={}, input_files={}, output_files={}",
                    compaction_job.input_level,
                    compaction_job.output_level,
                    compaction_job.input_files.len(),
                    compaction_job.output_files.len()
                );

                let rs = perform_compaction(options, db_dir, sst_cache.clone(), &compaction_job, next_file_number);
                compaction_picker.lock().unwrap().unmark_compacting(&compaction_job);
                match rs {
                    Ok(op) => {
                        if let SSTableOperation::Compaction {
                            added_sstables,
                            removed_sstables,
                            ..
                        } = &op
                        {
                            event!(
                                logger,
                                "compaction done, input_level={}, output_level={}, added={}, removed={}",
                                compaction_job.input_level,
                                compaction_job.output_level,
                                added_sstables.len(),
                                removed_sstables.len()
                            );
                        }
                        drop(compaction_job); // We need to drop the compaction_job to avoid keeping a reference on the removed SSTableMetadata Arcs
                        callback.call(Ok(op))
                    }
                    Err(e) => {
                        warn!(logger, "compaction failed: {}", e);
                        callback.call(Err(e))
                    }
                }
            }
            None => break,
        }
    }
}

fn perform_compaction(options: &Options,
                      db_dir: &PathBuf,
                      sst_cache: Arc<SSTableCache>,
                      job: &CompactionJob,
                      next_file_number: &AtomicU64) -> Result<SSTableOperation>{

    let sst_max_size = compute_sst_max_size(options, job.output_level);

    let compaction_iter = CompactionIterator::new(
        db_dir,
        sst_cache.clone(),
        &job.input_files,
        &job.output_files,
        &job.drops)?;

    let mut added = Vec::new();

    let mut sstable_writer = new_sstable_writer(options, db_dir, next_file_number)?;
    let mut boundary_idx: usize = 0;

    for entry in compaction_iter {
        let (internal_key, value) = entry?;

        if let Some(boundaries) = &job.partitions_grid {
            let record_key = extract_record_key(&internal_key);
            while boundary_idx < boundaries.len() && record_key > boundaries[boundary_idx].as_slice() {
                if sstable_writer.estimated_size() > 0 {
                    added.push(Arc::new(sstable_writer.finish()?));
                    sstable_writer = new_sstable_writer(options, db_dir, next_file_number)?;
                }
                boundary_idx += 1;
            }
        }

        sstable_writer.add(&internal_key, &value)?;

        if sstable_writer.estimated_size() >= sst_max_size {
            added.push(Arc::new(sstable_writer.finish()?));
            sstable_writer = new_sstable_writer(options, db_dir, next_file_number)?;
        }
    }

    if sstable_writer.estimated_size() > 0 {
        added.push(Arc::new(sstable_writer.finish()?));
    }
    let removed = job.input_files.iter()
            .cloned()
            .chain(job.output_files.iter().cloned())
            .collect();

    Ok(SSTableOperation::Compaction {
        output_level: job.output_level as usize,
        added_sstables: added,
        removed_sstables: removed,
        drops: split_drops_if_needed(&job),
    })
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
            match drop.record_key_range().position_of(&boundaries[partition_idx]) {
                IntervalPosition::Before => {
                    // drop is after the partition, we should move to the next partition
                    partition_idx += 1;
                    current_drop = Some(drop);
                },
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
                },
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
    next_file_number: &'a AtomicU64
) -> Result<SSTableWriter<'a>> {
    let sst_file = DbFile::new_sst(next_file_number.fetch_add(1, Ordering::Relaxed));
    Ok(SSTableWriter::new(db_dir, &sst_file, options)?)
}

fn compute_sst_max_size(options: &Options, level: u8) -> usize {
    // L0 is special/overlapping, but compaction writes into L1+.
    // Still handle it defensively by treating it as L1 sizing.
    let effective_level = level.max(1);

    let base_bytes = options.max_bytes_for_level_base().to_bytes() as f64;
    let multiplier = options.max_bytes_for_level_multiplier();

    // L1 target is `base`, L2 is `base * multiplier`, etc.
    let target_level_bytes =
        base_bytes * multiplier.powi((effective_level.saturating_sub(1)) as i32);

    // Aim for ~10 SSTables per level at steady state.
    let mut per_sst_bytes = (target_level_bytes / 10.0) as usize;

    // Clamp to avoid pathological values (too small => too many files).
    let min_bytes = options.file_write_buffer_size().to_bytes().max(1 << 20); // at least 1 MiB
    let max_bytes = target_level_bytes.max(1.0) as usize;

    if per_sst_bytes < min_bytes {
        per_sst_bytes = min_bytes;
    }
    if per_sst_bytes > max_bytes {
        per_sst_bytes = max_bytes;
    }

    // Never return 0.
    per_sst_bytes.max(1)
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeBounds};
    use super::*;
    use crate::obs::logger::test_instance;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::storage::files::DbFile;
    use crate::storage::lsm_version::{DropMetadata, SSTableMetadata};
    use crate::storage::sstable::sstable_cache::SSTableCache;
    use crate::storage::sstable::sstable_writer::SSTableWriter;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tempfile::tempdir;
    use crate::storage::test_utils::{put_rec, record_key};
    use crate::util::interval::Interval;

    fn setup_cache(options: &Options) -> Arc<SSTableCache> {
        let mut metric_registry = MetricRegistry::new();
        Arc::new(SSTableCache::new(
            test_instance(),
            &mut metric_registry,
            &options,
        ))
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

    // -----------------------------------------------------------------------
    // Helper: assert the SSTableOperation is a Compaction and unpack it.
    // -----------------------------------------------------------------------
    fn unwrap_compaction(
        op: SSTableOperation,
    ) -> (usize, Vec<Arc<SSTableMetadata>>, Vec<Arc<SSTableMetadata>>, Vec<Arc<DropMetadata>>) {
        match op {
            SSTableOperation::Compaction {
                output_level,
                added_sstables,
                removed_sstables,
                drops,
            } => (output_level, added_sstables, removed_sstables, drops),
            _ => panic!("Expected SSTableOperation::Compaction"),
        }
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
            put_rec(col, 10, 1,1),
            put_rec(col, 20, 1,2),
            put_rec(col, 30, 1,3),
        ];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, removed, drops) = unwrap_compaction(op);

        assert_eq!(output_level, 1);
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
            &[
                put_rec(col, 10, 1, 5),
                put_rec(col, 20, 1, 6)
            ],
            &options,
        );
        // another input SST: key 30
        let input2 = write_sst(
            dir.path(),
            2,
            &[put_rec(col, 30, 1, 7)],
            &options,
        );
        // output SST that overlaps: keys 15, 25
        let output1 = write_sst(
            dir.path(),
            3,
            &[
                put_rec(col,15, 1, 1),
                put_rec(col,25, 1, 2)
            ],
            &options,
        );

        let job = CompactionJob {
            input_level: 0,
            output_level: 1,
            input_files: vec![input1.clone(), input2.clone()],
            output_files: vec![output1.clone()],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(output_level, 1);
        assert!(!added.is_empty());

        let removed_numbers: Vec<u64> = removed.iter().map(|s| s.number).collect();
        assert!(removed_numbers.contains(&input1.number));
        assert!(removed_numbers.contains(&input2.number));
        assert!(removed_numbers.contains(&output1.number));
        assert_eq!(removed.len(), 3);
    }

    /// Drops without a partitions_grid are passed through unchanged.
    #[test]
    fn test_full_no_partitions_drops_passed_through() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let input_sst = write_sst(
            dir.path(),
            1,
            &[put_rec(col, 10, 1, 1)],
            &options,
        );

        let drop = DropMetadata::new_collection_drop(col, 50);

        let job = CompactionJob {
            input_level: 0,
            output_level: 1,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: None,
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (_level, added, removed, drops) = unwrap_compaction(op);

        // No partitions_grid → drops returned as-is
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0], drop);

        assert_eq!(added.len(), 0);
        assert_eq!(removed.len(), 1);
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

        let entries: Vec<_> = (1..=5)
            .map(|k| put_rec(col, k * 10, 1, k as u64))
            .collect();
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
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

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(output_level, 2);
        assert!(!added.is_empty());
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].number, input_sst.number);
    }

    /// Drops with an empty partitions_grid are still passed through unchanged
    /// (split_drops_if_needed returns early when boundaries is empty).
    #[test]
    fn test_full_empty_partitions_grid_drops_passed_through() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let input_sst = write_sst(
            dir.path(),
            1,
            &[put_rec(col,10, 1,1)],
            &options,
        );
        let drop = DropMetadata::new_collection_drop(col, 99);

        let job = CompactionJob {
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![drop.clone()],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (_level, _added, _removed, drops) = unwrap_compaction(op);

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
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary.clone()]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(output_level, 2);
        assert_eq!(added.len(), 2, "One boundary should produce two output SSTables");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].number, input_sst.number);

        // First SST should cover keys up to the boundary; second SST should cover keys after it.
        assert!(added[0].max_key <= boundary, "First SST max_key should be <= boundary");
        assert!(added[1].min_key > boundary, "Second SST min_key should be > boundary");
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
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![b1.clone(), b2.clone()]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, _removed, _drops) = unwrap_compaction(op);

        assert_eq!(output_level, 2);
        assert_eq!(added.len(), 3, "Two boundaries should produce three output SSTables");
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

        let entries = vec![
            put_rec(col, 10, 1, 1),
            put_rec(col, 20, 1, 2),
        ];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        let job = CompactionJob {
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (_level, added, _removed, _drops) = unwrap_compaction(op);

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
            &vec![
                put_rec(col, 10, 1, 1),
                put_rec(col, 70, 1, 2),
            ],
            &options,
        );

        // A full collection drop that spans the boundary.
        let full_drop = DropMetadata::new_collection_drop(col, 10);

        let job = CompactionJob {
            input_level: 1,
            output_level: 2,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![full_drop],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary.clone()]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
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

        let entries = vec![
            put_rec(col, 10, 1,1),
            put_rec(col, 20, 1, 2),
        ];
        let input_sst = write_sst(dir.path(), 1, &entries, &options);

        // Single partition (no boundaries) — everything goes into one SST.
        let job = CompactionJob {
            input_level: 2,
            output_level: 3,
            input_files: vec![input_sst.clone()],
            output_files: vec![],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(output_level, 3);
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
        let output_entries = vec![put_rec(col, 20, 1, 1),];
        let output_sst = write_sst(dir.path(), 2, &output_entries, &options);

        let job = CompactionJob {
            input_level: 2,
            output_level: 3,
            input_files: vec![input_sst.clone()],
            output_files: vec![output_sst.clone()],
            drops: vec![],
            input_key_range: Interval::all(),
            output_key_range: Interval::all(),
            partitions_grid: Some(vec![boundary.clone()]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (output_level, added, removed, _drops) = unwrap_compaction(op);

        assert_eq!(output_level, 3);
        assert_eq!(added.len(), 2, "Boundary should split into two output SSTables");
        assert_eq!(removed.len(), 2, "Both input and output SSTables should be removed");

        let removed_numbers: Vec<u64> = removed.iter().map(|s| s.number).collect();
        assert!(removed_numbers.contains(&input_sst.number));
        assert!(removed_numbers.contains(&output_sst.number));

        assert!(added[0].max_key <= boundary);
        assert!(added[1].min_key > boundary);
    }

    /// Partial compaction: a drop in the same partition is included in the output drops.
    #[test]
    fn test_partial_drop_in_partition_included_in_output() {
        let dir = tempdir().unwrap();
        let options = Options::lightweight();
        let cache = setup_cache(&options);
        let counter = next_file_number(10);

        let col = 10;

        let input_sst = write_sst(
            dir.path(),
            1,
            &[put_rec(col, 10, 1, 1)],
            &options,
        );

        // A drop already scoped to partition 0 (no need to split).
        let (drop_p0, _drop_p1) = DropMetadata::new_collection_drop(col, 10)
            .split_at(&record_key(col, 50))
            .expect_two();

        let job = CompactionJob {
            input_level: 2,
            output_level: 3,
            input_files: vec![input_sst],
            output_files: vec![],
            drops: vec![drop_p0.clone()],
            input_key_range: Interval::new(
                Bound::Unbounded,
                Bound::Included(record_key(col, 50)),
            ),
            output_key_range: Interval::new(
                Bound::Unbounded,
                Bound::Included(record_key(col, 50)),
            ),
            // boundary at 50 — drop is entirely within the left partition
            partitions_grid: Some(vec![record_key(col, 50)]),
        };

        let op = perform_compaction(&options, &dir.path().to_path_buf(), cache, &job, &counter)
            .unwrap();
        let (_level, _added, _removed, drops) = unwrap_compaction(op);

        // The drop fragment ends at the boundary (Included(50)); it is the left fragment
        // so split_drops_if_needed should return it as-is (it ends at the boundary).
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0], drop_p0);
    }

    /// compute_sst_max_size is defensive about L0: treats it like L1.
    #[test]
    fn test_compute_sst_max_size_l0_treated_as_l1() {
        let options = Options::lightweight();
        let size_l0 = compute_sst_max_size(&options, 0);
        let size_l1 = compute_sst_max_size(&options, 1);
        assert_eq!(size_l0, size_l1, "L0 and L1 should produce the same sst_max_size");
    }

    /// Higher levels produce larger SST size targets.
    #[test]
    fn test_compute_sst_max_size_grows_with_level() {
        let options = Options::lightweight();
        let size_l1 = compute_sst_max_size(&options, 1);
        let size_l2 = compute_sst_max_size(&options, 2);
        let size_l3 = compute_sst_max_size(&options, 3);
        assert!(size_l2 >= size_l1, "L2 target should be >= L1");
        assert!(size_l3 >= size_l2, "L3 target should be >= L2");
    }

    mod split_drops_tests {
        use std::ops::{Bound, RangeBounds};
        use crate::storage::test_utils::record_key;
        use super::*;

        fn job_with_drops(
            drops: Vec<Arc<DropMetadata>>,
            partitions_grid: Option<Vec<Vec<u8>>>,
        ) -> CompactionJob {
            CompactionJob {
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
            assert_eq!(result[0].key_range.end_bound(), Bound::Included(&record_key(col, 50)));
            assert_eq!(result[1].key_range.start_bound(), Bound::Excluded(&record_key(col, 50)));
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
            assert_eq!(result[0].key_range.end_bound(), Bound::Included(&record_key(col, 30)));
            assert_eq!(result[1].key_range.end_bound(), Bound::Included(&record_key(col, 70)));
            assert_eq!(result[2].key_range.start_bound(), Bound::Excluded(&record_key(col, 70)));
        }

        #[test]
        fn test_multiple_drops_some_spanning_boundary() {
            let col1 = 10;
            let col2 = 11;
            let drop = DropMetadata::new_collection_drop(col1, 10);
            let spanning = DropMetadata::new_collection_drop(col2, 20);

            let job = job_with_drops(vec![drop.clone(), spanning.clone()], Some(vec![record_key(col2, 50)]));

            let result = split_drops_if_needed(&&job);

            assert_eq!(result.len(), 3);
            assert_eq!(result[0], drop);
            assert_eq!(result[1].key_range.end_bound(), Bound::Included(&record_key(col2, 50)));
            assert_eq!(result[2].key_range.start_bound(), Bound::Excluded(&record_key(col2, 50)));
        }
    }
}
