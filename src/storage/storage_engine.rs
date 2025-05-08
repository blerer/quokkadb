use std::collections::VecDeque;
use std::fs;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use arc_swap::ArcSwap;
use tracing::{error, warn};
use crate::io::{mark_file_as_corrupted, truncate_file};
use crate::options::options::Options;
use crate::storage::callback::{AsyncCallback, BlockingCallback, Callback};
use crate::storage::files::{DbFile, FileType};
use crate::storage::flush_scheduler::{FlushScheduler, FlushTask};
use crate::storage::log::LogReplayError;
use crate::storage::lsm_tree::{LsmTree, LsmTreeEdit};
use crate::storage::manifest::Manifest;
use crate::storage::memtable::Memtable;
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::sstable::sstable_reader::SSTableReader;
use crate::storage::wal::WriteAheadLog;
use crate::storage::write_batch::WriteBatch;

struct StorageEngine {
    db_dir: PathBuf,
    options: Arc<Options>,
    queue: Mutex<VecDeque<Arc<Writer>>>,
    manifest: Mutex<Manifest>,
    write_ahead_log: Mutex<WriteAheadLog>,
    lsm_tree: ArcSwap<LsmTree>,
    next_file_number: AtomicU64, // The counter used to create the file ids
    next_seq_number: AtomicU64, // The counter used to create sequence numbers
    last_visible_seq: AtomicU64,
    sst_cache: Arc<SSTableCache>,
    flush_scheduler: FlushScheduler,
    async_callback: OnceLock<Arc<AsyncCallback<Result<LsmTreeEdit>>>>,
    error_mode: AtomicBool,
}

impl StorageEngine {
    pub fn new(db_dir: &Path, options: Arc<Options>) -> Result<Arc<Self>> {

        let sst_cache = Arc::new(SSTableCache::new(options.database_options()));

        // Retrieve the latest manifest path.
        let manifest_path = Manifest::read_current_file(db_dir)?;

        // If the manifest exists we need to recreate the lsm tree and replay the wal records.
        // Otherwise, it is the first time that we start this database and need to create a
        // new manifest and wal.
        if let Some(manifest_path) = manifest_path {

            let mut lsm_tree = Manifest::rebuild_lsm_tree(&manifest_path)?;
            let mut last_seq_nbr = lsm_tree.last_sequence_number;

            let scan_results = scan_db_directory(db_dir, lsm_tree.oldest_log_number)?;

            let mut wal_files_iter = scan_results.wal_files.iter().peekable();

            let mut reusable_wal = None;

            while let Some((log_number, wal_path)) = wal_files_iter.next() {

                // We need to re-associate write-ahead log files and memtables
                if log_number != &lsm_tree.memtable.log_number {
                    lsm_tree = lsm_tree.apply(&LsmTreeEdit::WalRotation { log_number: *log_number })
                }

                let rs = WriteAheadLog::replay(wal_path, lsm_tree.last_sequence_number);
                let is_last_wal_file = wal_files_iter.peek().is_none();

                match rs {
                    Ok(iter) => {
                        for rs in iter {
                            match rs {
                                Err(e) => {
                                    if is_last_wal_file {
                                        match e {
                                            LogReplayError::Io(e) => return Err(e.into()),
                                            LogReplayError::Corruption { record_offset, reason } => {
                                                warn!("Corruption detected in the {} file for record at offset {}. Truncating the file at this offset. Cause: {}",
                                                    wal_path.to_string_lossy(), record_offset, reason);
                                                truncate_file(wal_path, record_offset)?;
                                                reusable_wal = Some(wal_path);
                                            }
                                        }
                                    } else {
                                        return Err(e.into())
                                    }
                                },
                                Ok((seq, batch)) => {
                                    assert_eq!(last_seq_nbr, seq + 1);
                                    lsm_tree.memtable.write(seq, &batch);
                                    last_seq_nbr = seq;
                                    if is_last_wal_file {
                                        reusable_wal = Some(wal_path)
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        // We are here because the file could not be read or its header is corrupted.
                        // If it is not the last wal file, we propagate the error and let the user deal with it.
                        // If it is the last wal file, and the error is a corruption, we mark the file as corrupted (e.g. "000023.log.corrupted")
                        // and will start with a brand new wal file.
                        // If it is an IO error we propagate it to the user.
                        if is_last_wal_file {
                            match e {
                                LogReplayError::Io(e) => {
                                    error!("{}", e);
                                    return Err(e.into())
                                },
                                LogReplayError::Corruption { record_offset: _, reason } => {
                                    mark_file_as_corrupted(wal_path)?;
                                    error!("Corruption detected in the {} file header. \
                                    Making the file has corrupted and starting from a new one. {}",
                                        wal_path.to_string_lossy(), reason);
                                }
                            }

                        } else {
                            error!("{}", e);
                            return Err(e.into())
                        }
                    }
                }
            }

            let mut manifest = Manifest::load_from(manifest_path, options.database_options())?;

            // If a file with a higher number that the next_file number has been detected we need to update
            // the Lsm tree in-memory and on-disk (MANIFEST file)
            let next_file_number = AtomicU64::new(if lsm_tree.next_file_number < scan_results.next_file_number {
                let edit = LsmTreeEdit::FilesDetectedOnRestart { next_file_number: scan_results.next_file_number };
                lsm_tree.apply(&edit);
                manifest.append_edit(&edit)?;
                scan_results.next_file_number
            } else {
                lsm_tree.next_file_number
            });

            // If the last wal file can be reused, either because it was fine or because it has been
            // corrected by truncation, we will reuse it. If not, it should have been marked as corrupted,
            // and we need to create a new one and update the Lsm tree.
            let wal = if let Some(wal_path) = reusable_wal {
                WriteAheadLog::load_from(wal_path.clone(), options.database_options())?
            } else {
                let log_number = next_file_number.fetch_add(1, Ordering::Relaxed);
                let wal = WriteAheadLog::new(db_dir, options.database_options(), log_number)?;
                let edit = LsmTreeEdit::WalRotation { log_number };
                lsm_tree = lsm_tree.apply(&edit);
                manifest.append_edit(&edit)?;
                wal
            };

            let flush_scheduler = FlushScheduler::new(db_dir, options.clone(), sst_cache.clone());

            Ok(Arc::new(StorageEngine {
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                manifest: Mutex::new(manifest),
                write_ahead_log: Mutex::new(wal),
                lsm_tree: ArcSwap::new(Arc::new(lsm_tree)),
                next_file_number,
                next_seq_number: AtomicU64::new(last_seq_nbr + 1),
                last_visible_seq: AtomicU64::new(last_seq_nbr),
                sst_cache,
                flush_scheduler,
                async_callback: OnceLock::new(),
                error_mode: AtomicBool::new(false),
            }))
        } else {
            let next_file_number = AtomicU64::new(1);
            let manifest_file_id = next_file_number.fetch_add(1, Ordering::Relaxed);
            let wal_file_id = next_file_number.fetch_add(1, Ordering::Relaxed);
            let lsm_tree = Arc::new(LsmTree::new(wal_file_id, next_file_number.load(Ordering::Relaxed)));
            let snapshot = LsmTreeEdit::Snapshot(lsm_tree.clone());
            let manifest = Manifest::new(db_dir, options.database_options(), manifest_file_id, &snapshot)?;
            let wal = WriteAheadLog::new(db_dir, options.database_options(), wal_file_id)?;
            let flush_scheduler = FlushScheduler::new(db_dir, options.clone(), sst_cache.clone());

            Ok(Arc::new(StorageEngine {
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                manifest: Mutex::new(manifest),
                write_ahead_log: Mutex::new(wal),
                lsm_tree: ArcSwap::new(lsm_tree),
                next_file_number,
                next_seq_number: AtomicU64::new(0),
                last_visible_seq: AtomicU64::new(0),
                sst_cache,
                flush_scheduler,
                async_callback: OnceLock::new(),
                error_mode: AtomicBool::new(false),
            }))
        }
    }

    pub fn write(self: Arc<Self>, batch: WriteBatch) -> Result<()> {
        if self.error_mode.load(Ordering::Relaxed) {
            return Err(Error::new(ErrorKind::Other, "The database is in error mode dues to a previous write error"));
        }

        let writer = Arc::new(Writer::new(batch));

        // Add the writer to the queue
        self.queue.lock().unwrap().push_back(writer.clone());

        // If no leader is active, this thread becomes leader
        if self.is_leader(&writer) {
            self.perform_writes();
            writer.result()
        } else {
            writer.wait()
        }
    }

    /// Checks if the specified writer is the leader (front of the queue) and should take care
    /// of performing the writes.
    fn is_leader(&self, writer: &Arc<Writer>) -> bool {
        self.queue
            .lock().unwrap()
            .front()
            .map_or(false, |front| std::ptr::eq(front, writer))
    }

    fn perform_writes(self: Arc<Self>) {

        // Only a single leader should reach that point at a given time as queue locking logic will
        // block other writers until the leader as empty the queue.
        self.perform_wal_and_memtable_rotation_if_needed();

        // We lock the queue to retrieve the pending writes. It prevents new incoming writes,
        // avoiding the issue of an infinite loop with the drain.
        let mut queue = self.queue.lock().unwrap();
        let mut writers = Vec::new();
        for writer in queue.drain(..) {
            writers.push(writer);
        }
        // We want to acquire the lock on the wal before we release the one on
        // the queue, to avoid a race condition where the next leader thread that just entered
        // the queue, on lock release, take the wal on them first.
        let mut wal = self.write_ahead_log.lock().unwrap();

        // Release the queue lock
        drop(queue);

        // Grab the sequence numbers for the set of batches
        let seq = self.next_seq_number.fetch_add(writers.len() as u64, Ordering::Relaxed);

        let res = Self::append_to_wal(&writers, &mut wal, seq);

        if let Err(error) = &res {
            self.handle_write_error(error, &writers);
            return
        }

        let with_sequence = res.unwrap();

        // We use the manifest lock when accessing the memtable to prevent a race with
        // a wal/memtable rotation and avoid races on writes between leaders.
        let memtable_write_lock = self.manifest.lock();

        drop(wal);

        let lsm_tree = self.lsm_tree.load().clone();

        let mut with_results = Vec::with_capacity(with_sequence.len());

        for (writer, seq) in with_sequence {
            lsm_tree.memtable.write(seq, writer.batch());
            with_results.push((writer, Ok(())));
            let compare = self.last_visible_seq.compare_exchange(seq - 1, seq, Ordering::Acquire, Ordering::Relaxed);
            if compare.is_err() {}
        }

        drop(memtable_write_lock);

        for (writer, result) in with_results {
            writer.done(result);
        }
    }

    pub fn read(&self, collection: u32, key: &[u8], snapshot: Option<u64>) -> Option<Arc<[u8]>> {
        // TODO: to implement
        None
    }

    fn append_to_wal(writers: &Vec<Arc<Writer>>, wal: &mut MutexGuard<WriteAheadLog>, mut seq: u64) -> Result<Vec<(Arc<Writer>, u64)>> {
        let mut with_sequence = Vec::with_capacity(writers.len());

        for writer in writers {
            let batch = writer.batch();
            wal.append(seq, batch)?;
            with_sequence.push((writer.clone(), seq));

            seq += 1;
        }
        Ok(with_sequence)
    }

    pub fn flush(self: &Arc<Self>) -> Result<()> {
        let engine = self.clone();
        let callback = Arc::new(BlockingCallback::new(move | result | {
            engine.update_lsm_tree_sstables(result)
        }));

        self.perform_wal_and_memtable_rotation(&callback.clone())?;
        callback.await_blocking()
    }

    fn perform_wal_and_memtable_rotation_if_needed(self: &Arc<Self>) {
        let write_buffer_size = self.options.database_options().file_write_buffer_size().to_bytes();
        let memtable_size = self.lsm_tree.load().memtable.size();
        if memtable_size >= write_buffer_size {

            match self.perform_wal_and_memtable_rotation(self.get_async_callback()) {
                Err(error) => {
                    error!("An error occurred during wal and memtable rotation: {}", error);
                }
                Ok(_) => (),
            }
        }
    }

    fn get_async_callback(self: &Arc<Self>) -> &Arc<AsyncCallback<Result<LsmTreeEdit>>> {
        self.async_callback.get_or_init(|| {
            let engine = self.clone();
            Arc::new(AsyncCallback::new(move |result| {
                engine.update_lsm_tree_sstables(result)
            }))
        })
    }

    fn perform_wal_and_memtable_rotation<C>(
        self: &Arc<Self>,
        callback: &Arc<C>,
    ) -> Result<()>
    where
        C: Callback<Result<LsmTreeEdit>> + 'static,
    {
       // Rotate the write-ahead log file and the memtable
       // (through applying a WalRotation edit to the LSM tree and replacing it atomically)
       let mut wal = self.write_ahead_log.lock().unwrap();

        // We want to perform the changes within the manifest lock to avoid concurrent updates to
        // the LSM tree. The order into which locks are acquired is important to avoid deadlocks
        // with the writes. The wal lock must always be acquired before the manifest one.
        let mut manifest = self.manifest.lock().unwrap();
        let new_log_number = self.next_file_number.fetch_add(1, Ordering::Relaxed);
        wal.rotate(new_log_number)?;

        drop(wal); // let release the write-ahead log as it is not needed

        let edit = LsmTreeEdit::WalRotation { log_number: new_log_number };
        let lsm_tree = self.append_edit(&mut manifest, &edit)?;
        let memtable = lsm_tree.imm_memtables.back().unwrap().clone(); // We just pushed this memtable to the back, within the manifest lock, therefore we can safely retrieve it.

        self.schedule_flush(memtable, callback)
    }

    fn schedule_flush<C>(self: &Arc<Self>,
                      memtable: Arc<Memtable>,
                      callback: &Arc<C>
    ) -> Result<()>
    where
        C: Callback<Result<LsmTreeEdit>> + 'static,
    {

        let sst_number = self.next_file_number.fetch_add(1, Ordering::Relaxed);

        let flush_task = FlushTask { db_file: DbFile::new_sst(sst_number), memtable, callback: Some(callback.clone()) };
        self.flush_scheduler.enqueue(flush_task)
    }


    fn update_lsm_tree_sstables(self: &Arc<Self>, edit: Result<LsmTreeEdit>) -> Result<()> {

        match edit {
            Ok(edit) => {
                // We want to perform the changes within the manifest lock to avoid concurrent updates to
                // the LSM tree
                let mut manifest = self.manifest.lock().unwrap();
                self.append_edit(&mut manifest, &edit)?;

                match edit {
                    LsmTreeEdit::Flush { log_number, sst : _sst} => {
                        // Delete write-ahead log files for persisted data
                    },
                    _ => {}
                }
                Ok(())
            }
            Err(error) => Err(error)
        }


    }

    fn append_edit(self: &Arc<Self>,
                   manifest:&mut MutexGuard<Manifest>,
                   edit: &LsmTreeEdit
    ) -> Result<Arc<LsmTree>> {

        let lsm_tree = self.lsm_tree.load();

        manifest.append_edit(&edit)?;
        let new_tree = Arc::new(lsm_tree.apply(&edit));
        self.lsm_tree.store(new_tree.clone());

        if manifest.should_rotate() {
            let new_manifest_id = self.next_file_number.fetch_add(1, Ordering::Relaxed);
            manifest.rotate(new_manifest_id, &LsmTreeEdit::Snapshot(new_tree.clone()))?;
        }
        Ok(new_tree)
    }

    fn handle_write_error(&self, error: &Error, writers: &Vec<Arc<Writer>>) {
        todo!()
    }
}

impl Callback<Result<LsmTreeEdit>> for AsyncCallback<Result<LsmTreeEdit>> {
    fn call(&self, value: Result<LsmTreeEdit>) {
        self.call(value);
    }
}

/// The result of scanning the database directory at startup.
///
/// Contains the list of WAL files that must be replayed,
/// and the next available file number to assign to new files.
#[derive(Debug)]
pub struct StartupScanResult {
    /// WAL files to be replayed, sorted by file ID in ascending order.
    /// Each entry is a tuple of (file_number, full_path).
    pub wal_files: Vec<(u64, PathBuf)>,

    /// The next unused file number. This is computed as one greater
    /// than the highest file number seen among MANIFEST, WAL, and SST files.
    pub next_file_number: u64,
}

/// Scans the given database directory to find WAL files that need replay,
/// and determines the next file number to use for new files.
///
/// This function performs a single pass over all directory entries and:
/// - Identifies WAL files (`*.log`) with IDs >= `oldest_log_number`
/// - Tracks the highest file ID across all known file types
///
/// # Arguments
///
/// * `dir` - The path to the database directory to scan
/// * `oldest_log_number` - The lowest WAL file number that may still contain unflushed data
///
/// # Returns
///
/// A `StartupScanResult` containing:
/// - The sorted list of WAL files to replay
/// - The next file number to use for future file creation
///
/// # Errors
///
/// Returns an error if the directory can't be read.
pub fn scan_db_directory(dir: &Path, oldest_log_number: u64) -> Result<StartupScanResult> {
    let mut wal_files = Vec::new();
    let mut max_file_num = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(db_file) = DbFile::new(&path) {
            max_file_num = max_file_num.max(db_file.id);

            if db_file.file_type == FileType::WriteAheadLog && db_file.id >= oldest_log_number {
                wal_files.push((db_file.id, path.clone()));
            }
        }
    }

    wal_files.sort_by_key(|(id, _)| *id);

    Ok(StartupScanResult {
        wal_files,
        next_file_number: max_file_num + 1,
    })
}

pub struct Writer {
    write_batch: WriteBatch,
    result: Mutex<Option<Result<()>>>,
    condvar: Condvar,
}

impl Writer {

    fn new(batch: WriteBatch) -> Writer {
        Writer {
            write_batch: batch,
            result: Mutex::new(None),
            condvar: Condvar::new(),
        }
    }

    pub fn batch(& self) -> &WriteBatch {
        &self.write_batch
    }

    fn wait(&self) -> Result<()> {
        let mut result = self.result.lock().unwrap();
        while result.is_none() {
            result = self.condvar.wait(result).unwrap();
        }
        Self::copy(result).unwrap()
    }

    fn result(&self) -> Result<()> {
        Self::copy(self.result.lock().unwrap()).unwrap_or_else(|| Err(Error::new(ErrorKind::Other, "No result available")))
    }
    fn done(&self, res: Result<()>) {
        let mut result = self.result.lock().unwrap();
        *result = Some(res);
        self.condvar.notify_one();
    }

    fn copy(result: MutexGuard<Option<Result<()>>>) -> Option<Result<()>> {
        match &*result {
            Some(Ok(())) => Some(Ok(())), // Return Ok if present
            Some(Err(e)) => Some(Err(Error::new(e.kind(), e.to_string()))), // Recreate the error
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    mod scan_tests {
        use super::*;
        use std::fs::File;
        use tempfile::tempdir;

        fn touch_file(path: &Path) {
            File::create(path).expect("failed to create file");
        }

        #[test]
        fn test_scan_db_directory_filters_and_orders() {
            let dir = tempdir().expect("create temp dir");
            let base = dir.path();

            // Create files
            touch_file(&base.join("MANIFEST-000009"));
            touch_file(&base.join("000002.log"));
            touch_file(&base.join("000005.log"));
            touch_file(&base.join("000004.sst"));
            touch_file(&base.join("ignore.me"));

            let result = scan_db_directory(base, 3).expect("scan should succeed");

            // Only logs ≥ 3 should be returned
            assert_eq!(result.wal_files.len(), 1);
            assert_eq!(result.wal_files[0].0, 5);

            // Next file number should be 10
            assert_eq!(result.next_file_number, 10);
        }

        #[test]
        fn test_wal_file_sorting_large_ids() {
            let dir = tempdir().unwrap();
            let base = dir.path();

            // Create log files with varying IDs
            touch_file(&base.join("000001.log"));
            touch_file(&base.join("000999.log"));
            touch_file(&base.join("001000.log")); // 6 digits
            touch_file(&base.join("1000000.log")); // 7 digits

            let result = scan_db_directory(base, 0).unwrap();

            let ids: Vec<u64> = result.wal_files.iter().map(|(id, _)| *id).collect();
            assert_eq!(ids, vec![1, 999, 1000, 1_000_000]);

            assert_eq!(result.next_file_number, 1_000_001);
        }
    }
}


