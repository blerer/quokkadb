use crate::obs::logger::LoggerAndTracer;
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
    workers: Vec<thread::JoinHandle<()>>,
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

        for i in 0..options.compaction_threads() {
            let shared_clone = Arc::clone(&shared);
            let options = options.clone();
            let db_dir = db_dir.to_path_buf();
            let sst_cache = sst_cache.clone();
            let next_file_number = next_file_number.clone();
            let compaction_picker = compaction_picker.clone();

            let handle = thread::Builder::new()
                .name(format!("compaction_manager-{i}"))
                .spawn(move || worker_loop(shared_clone, &options, &db_dir, sst_cache, &next_file_number, compaction_picker))?;

            workers.push(handle);
        }

        Ok(Self {
            logger,
            compaction_picker,
            shared,
            workers,
        })
    }

    pub fn schedule_compaction_if_needed(&self, levels: &Levels, callback: &Arc<Callback<Result<SSTableOperation>>>) {
        while let Some(job) = self.compaction_picker.lock().unwrap().pick_compaction(levels) {
            let task = CompactionTask {
                compaction_job: job,
                callback: callback.clone(),
            };
            self.enqueue(task);
        }
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

    pub fn shutdown(self) {
        let (lock, cvar) = &*self.shared;
        {
            let mut shared = lock.lock().unwrap();
            shared.shutdown = true;
        }
        cvar.notify_all();

        for worker in self.workers {
            let _ = worker.join();
        }
    }
}

fn worker_loop(shared: Arc<(Mutex<Shared>, Condvar)>,
               options: &Options,
               db_dir: &PathBuf,
               sst_cache: Arc<SSTableCache>,
               next_file_number: &AtomicU64,
               compaction_picker: Arc<Mutex<CompactionPicker>>)
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
                let rs = perform_compaction(options, db_dir, sst_cache.clone(), &task.compaction_job, next_file_number);
                compaction_picker.lock().unwrap().unmark_compacting(&task.compaction_job);
                match rs {
                    Ok(op) => task.callback.call(Ok(op)),
                    Err(e) => task.callback.call(Err(e)),
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
                if sstable_writer.estimated_size() != 0 {
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

   added.push(Arc::new(sstable_writer.finish()?));

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
fn split_drops_if_needed(job: &&CompactionJob) -> Vec<Arc<DropMetadata>> {
    // Drop are sorted by min key, so we can iterate on them and partition at the same time.
    if let Some(boundaries) = &job.partitions_grid {
        let mut output_drops = Vec::new();
        if boundaries.is_empty() || job.drops.is_empty() {
            output_drops
        } else {
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
            output_drops
        }
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