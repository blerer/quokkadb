use crate::io::{mark_file_as_corrupted, sync_dir, truncate_file};
use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::obs::metrics::{DerivedGauge, MetricRegistry};
use crate::options::options::Options;
use crate::storage::append_log::LogReplayError;
use crate::storage::callback::Callback;
use crate::storage::catalog::Catalog;
use crate::storage::files::{DbFile, FileType};
use crate::storage::flush_manager::{FlushManager, FlushTask};
use crate::storage::internal_key::encode_record_key;
use crate::storage::lsm_tree::LsmTree;
use crate::storage::lsm_version::SSTableMetadata;
use crate::storage::manifest::Manifest;
use crate::storage::manifest_state::ManifestEdit;
use crate::storage::memtable::Memtable;
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::wal::WriteAheadLog;
use crate::storage::write_batch::{Precondition, Preconditions, WriteBatch};
use crate::storage::Direction;
use crate::{debug, error, event, info, warn};
use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::{fmt, fs};
use std::fs::remove_file;
use std::io::{Result, Error};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

struct WalAndManifest {
    wal: WriteAheadLog,
    manifest: Manifest,
}

pub(crate) struct StorageEngine {
    logger: Arc<dyn LoggerAndTracer>,
    db_dir: PathBuf,
    options: Arc<Options>,
    queue: Mutex<VecDeque<Arc<Writer>>>,
    db_mutex: Mutex<WalAndManifest>,
    lsm_tree: Arc<ArcSwap<LsmTree>>,
    next_file_number: AtomicU64, // The counter used to create the file ids
    next_seq_number: AtomicU64,  // The counter used to create sequence numbers
    last_visible_seq: AtomicU64,
    sst_cache: Arc<SSTableCache>,
    flush_manager: FlushManager,
    async_callback: OnceLock<Arc<Callback<Result<SSTableOperation>>>>,
    error_mode: AtomicBool,
    #[cfg(test)]
    fail_next_precondition_checks: AtomicU8,
}

impl StorageEngine {
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: Arc<Options>,
        db_dir: &Path,
    ) -> StorageResult<Arc<Self>> {
        let sst_cache = Arc::new(SSTableCache::new(
            logger.clone(),
            metric_registry,
            &options.db,
        ));

        info!(logger, "Starting storage engine at {}", db_dir.to_string_lossy());

        // Retrieve the latest manifest path.
        let manifest_path = Manifest::read_current_file(db_dir)?;

        // If the manifest exists we need to recreate the lsm tree and replay the wal records.
        // Otherwise, it is the first time that we start this database and need to create a
        // new manifest and wal.
        if let Some(manifest_path) = manifest_path {

            let manifest_state = Manifest::rebuild_manifest_state(&manifest_path)?;
            let mut last_seq_nbr = manifest_state.lsm.last_sequence_number;

            let scan_results = scan_db_directory(db_dir, manifest_state.lsm.oldest_log_number)?;

            let mut wal_files_iter = scan_results.wal_files.iter().peekable();

            let mut reusable_wal = None;

            let original_current_log_number = manifest_state.lsm.current_log_number;

            let mut manifest =
                Manifest::load_from(logger.clone(), metric_registry, &options.db, manifest_path)?;

            let next_file_number = manifest_state.lsm.next_file_number;

            let mut lsm_tree = LsmTree::from(manifest_state);

            // If a file with a higher number that the next_file number has been detected we need to update
            // the Lsm tree in-memory and on-disk (MANIFEST file)
            let next_file_number =
                AtomicU64::new(if next_file_number < scan_results.next_file_number {

                    info!(logger,
                        "Files with higher numbers have been detected. Updating next_file_number to {}",
                        scan_results.next_file_number);

                    let edit = ManifestEdit::FilesDetectedOnRestart {
                        next_file_number: scan_results.next_file_number,
                    };
                    manifest.append_edit(&edit)?;
                    lsm_tree.apply(&edit);
                    scan_results.next_file_number
                } else {
                    next_file_number
                });

            // We will keep track of the rotated log files while replaying the wal files.
            let mut rotated_log_files =
                VecDeque::from_iter(scan_results.obsolete_wal_files.iter().rev().cloned());

            let mut previous = None;

            while let Some((log_number, wal_path)) = wal_files_iter.next() {

                info!(logger, "Replaying operations from {}", wal_path.to_string_lossy());

                // The initial memtable will be associated with the oldest_log_number. For
                // the following , we need to re-associate the wal log number and the memtable
                // one by doing a wal rotations.
                if log_number != &lsm_tree.memtable.log_number {
                    let edit = ManifestEdit::WalRotation {
                        log_number: *log_number,
                        next_seq: last_seq_nbr + 1,
                    };
                    rotated_log_files.push_back(previous.clone().unwrap());

                    lsm_tree = lsm_tree.apply(&edit);

                    // If the log number is higher than the original current log number,
                    // we need to update the manifest file to reflect that, as we are
                    // replaying wal files that were not recorded in the manifest.
                    if log_number > &original_current_log_number {
                        manifest.append_edit(&edit)?;
                    }

                    lsm_tree = Self::flush_replayed_data(
                        &logger,
                        &options,
                        &db_dir,
                        &mut manifest,
                        &mut lsm_tree,
                        &next_file_number
                    )?;
                }

                let rs = WriteAheadLog::replay(wal_path);
                let is_last_wal_file = wal_files_iter.peek().is_none();

                match rs {
                    Ok(iter) => {
                        let mut count = 0;
                        for rs in iter {
                            match rs {
                                Err(e) => {
                                    if is_last_wal_file {
                                        match e {
                                            LogReplayError::Io(e) => return Err(e.into()),
                                            LogReplayError::Corruption {
                                                record_offset,
                                                reason,
                                            } => {
                                                warn!(logger, "Corruption detected in the {} file for record at offset {}. Truncating the file at this offset. Cause: {}",
                                                    wal_path.to_string_lossy(), record_offset, reason);
                                                truncate_file(wal_path, record_offset)?;
                                                reusable_wal = Some(wal_path);
                                            }
                                        }
                                    } else {
                                        return Err(e.into());
                                    }
                                }
                                Ok((seq, batch)) => {
                                    count += 1;
                                    lsm_tree.memtable.write(seq, &batch);
                                    last_seq_nbr = seq;
                                    if is_last_wal_file {
                                        reusable_wal = Some(wal_path)
                                    }
                                }
                            }
                        }
                        previous = Some((*log_number, wal_path.clone()));
                        info!(logger, "{} operations replayed from {}", count, wal_path.to_string_lossy());
                    }
                    Err(e) => {
                        // We are here because the file could not be read or its header is corrupted.
                        // If it is not the last wal file, we propagate the error and let the user deal with it.
                        // If it is the last wal file, and the error is a corruption, we mark the file as corrupted (e.g. "000023.log.corrupted")
                        // and will start with a brand new wal file.
                        // If it is an IO error we propagate it to the user.
                        if is_last_wal_file {
                            match e {
                                LogReplayError::Io(e) => {
                                    error!(logger, "{}", e);
                                    return Err(e.into());
                                }
                                LogReplayError::Corruption {
                                    record_offset: _,
                                    reason,
                                } => {
                                    mark_file_as_corrupted(logger.clone(), wal_path)?;
                                    error!(logger,
                                        "Corruption detected in the {} file header. \
                                    Making the file has corrupted and starting from a new one. {}",
                                        wal_path.to_string_lossy(),
                                        reason
                                    );
                                }
                            }
                        } else {
                            error!(logger, "{}", e);
                            return Err(e.into());
                        }
                    }
                }
            }

            // If the last wal file can be reused, either because it was fine or because it has been
            // corrected by truncation, we will reuse it. If not, it should have been marked as corrupted,
            // and we need to create a new one and update the Lsm tree.
            let wal = if let Some(wal_path) = reusable_wal {
                WriteAheadLog::load_from(
                    logger.clone(),
                    metric_registry,
                    &options.db,
                    &wal_path,
                    rotated_log_files,
                )?
            } else {

                let log_number = next_file_number.fetch_add(1, Ordering::Relaxed);

                info!(logger, "Latest wal was corrupted. Starting from a clean wal file: {}", log_number);

                let wal = WriteAheadLog::new_after_corruption(
                    logger.clone(),
                    metric_registry,
                    &options.db,
                    db_dir,
                    log_number,
                    rotated_log_files,
                )?;
                let edit = ManifestEdit::WalRotation { log_number, next_seq: last_seq_nbr + 1 };
                lsm_tree = lsm_tree.apply(&edit);
                manifest.append_edit(&edit)?;

                // If the corrupted wal contained some data we need to flush them to disk otherwise
                // we can just drop the memtable.
                if lsm_tree.imm_memtables[0].size() > 0 {
                    lsm_tree = Self::flush_replayed_data(
                        &logger,
                        &options,
                        &db_dir,
                        &mut manifest,
                        &mut lsm_tree,
                        &next_file_number
                    )?;
                } else {

                    info!(logger, "Ignoring empty memtable: {}", lsm_tree.imm_memtables[0].log_number);

                    // Drop the empty memtable
                    let edit = ManifestEdit::IgnoringEmptyMemtable {
                        oldest_log_number: lsm_tree.imm_memtables[0].log_number,
                    };
                    lsm_tree = lsm_tree.apply(&edit);
                    manifest.append_edit(&edit)?;
                }
                wal
            };

            let flush_manager = FlushManager::new(
                logger.clone(),
                metric_registry,
                options.clone(),
                db_dir,
                sst_cache.clone(),
            )?;

            let lsm_tree = Arc::new(ArcSwap::new(Arc::new(lsm_tree)));
            Self::add_metrics(metric_registry, &options, lsm_tree.clone());

            info!(logger, "Storage engine started",);

            Ok(Arc::new(StorageEngine {
                logger,
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                db_mutex: Mutex::new(WalAndManifest {
                    wal,
                    manifest,
                }),
                lsm_tree,
                next_file_number,
                next_seq_number: AtomicU64::new(last_seq_nbr + 1),
                last_visible_seq: AtomicU64::new(last_seq_nbr),
                sst_cache,
                flush_manager,
                async_callback: OnceLock::new(),
                error_mode: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_precondition_checks: AtomicU8::new(0),
            }))
        } else {
            let next_file_number = AtomicU64::new(1);
            let next_seq_number = AtomicU64::new(1);
            let manifest_number = next_file_number.fetch_add(1, Ordering::Relaxed);
            let log_number = next_file_number.fetch_add(1, Ordering::Relaxed);
            let lsm_tree = Arc::new(LsmTree::new(
                log_number,
                next_file_number.load(Ordering::Relaxed),
                next_seq_number.load(Ordering::Relaxed),
            ));
            let snapshot = ManifestEdit::Snapshot(lsm_tree.manifest.clone());
            let manifest = Manifest::new(
                logger.clone(),
                metric_registry,
                &options.db,
                db_dir,
                manifest_number,
                &snapshot,
            )?;
            let wal = WriteAheadLog::new(
                logger.clone(),
                metric_registry,
                &options.db,
                db_dir,
                log_number,
            )?;
            let flush_manager = FlushManager::new(
                logger.clone(),
                metric_registry,
                options.clone(),
                db_dir,
                sst_cache.clone(),
            )?;

            let lsm_tree = Arc::new(ArcSwap::new(lsm_tree));
            Self::add_metrics(metric_registry, &options, lsm_tree.clone());

            info!(logger, "Storage engine started");

            Ok(Arc::new(StorageEngine {
                logger,
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                db_mutex: Mutex::new(WalAndManifest {
                    wal,
                    manifest,
                }),
                lsm_tree,
                next_file_number,
                next_seq_number,
                last_visible_seq: AtomicU64::new(0),
                sst_cache,
                flush_manager,
                async_callback: OnceLock::new(),
                error_mode: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_precondition_checks: AtomicU8::new(0),
            }))
        }
    }

    fn flush_replayed_data(
        logger: &Arc<dyn LoggerAndTracer>,
        options: &Arc<Options>,
        db_dir: &&Path,
        manifest: &mut Manifest,
        lsm_tree: &mut LsmTree,
        next_file_number: &AtomicU64
    ) -> StorageResult<LsmTree> {

        info!(logger, "Flushing data from {}", lsm_tree.imm_memtables[0].log_number);

        // Flush the current memtable to a sst file before processing the next
        // wal file.
        let sst_file = DbFile::new_sst(
            next_file_number.fetch_add(1, Ordering::Relaxed)
        );
        let sst = Arc::new(
            lsm_tree.imm_memtables[0].flush(&db_dir, &sst_file, &options)?);

        let edit = ManifestEdit::Flush {
            oldest_log_number: lsm_tree.imm_memtables[0].log_number,
            sst,
        };
        let lsm_tree = lsm_tree.apply(&edit);
        manifest.append_edit(&edit)?;
        Ok(lsm_tree)
    }

    pub fn create_collection_if_not_exists(self: &Arc<Self>, name: &str) -> StorageResult<u32> {
        if self.error_mode.load(Ordering::Relaxed) {
            return Err(StorageError::ErrorMode("The database is in error mode dues to a previous write error".into()));
        }
        let collection = self.catalog().get_collection_by_name(name);

        if let Some(collection) = collection {
            Ok(collection.id)
        } else {
            // The collection do not exist we need to create it and update the manifest
            let mut wal_and_manifest = self.db_mutex.lock().unwrap();

            // We need first to check that the collection has not been created concurrently
            let lsm_tree = self.lsm_tree.load();
            let catalogue = lsm_tree.catalogue();
            let collection = catalogue.get_collection_by_name(name);
            if collection.is_none() {
                let id = catalogue.next_collection_id;
                let edit = ManifestEdit::CreateCollection {
                    name: name.to_string(),
                    id,
                };
                let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
                Ok(id)
            } else {
                Ok(collection.unwrap().id)
            }
        }
    }

    pub fn last_visible_sequence(&self) -> u64 {
        self.last_visible_seq.load(Ordering::Relaxed)
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        let lsm_tree = self.lsm_tree.load();
        lsm_tree.catalogue().clone()
    }

    pub fn write(self: &Arc<Self>, batch: WriteBatch) -> StorageResult<()> {

        if self.error_mode.load(Ordering::Relaxed) {
            return Err(StorageError::ErrorMode(
                "The database is in error mode dues to a previous write error".to_string(),
            ));
        }

        let writer = Arc::new(Writer::new(batch));

        // Add the writer to the queue
        self.queue.lock().unwrap().push_back(writer.clone());

        // If no leader is active, this thread becomes leader
        if self.is_leader(&writer) {
            debug!(self.logger, "Thread {:?} is the leader and will perform the write",
                std::thread::current().id()
            );
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
            .lock()
            .unwrap()
            .front()
            .map_or(false, |front| std::ptr::eq(&**front, &**writer))
    }

    fn perform_writes(self: &Arc<Self>) {

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

        debug!(self.logger, "Thread {:?} will perform the write for {:?} writers",
                std::thread::current().id(), writers.len(),
            );
        event!(self.logger, "write start, writers_size={}", writers.len());

        // We want to acquire the db lock before we release the one on the queue,
        // to avoid a race condition where the next leader thread that just entered
        // the queue, on lock release, take the lock on them first.
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        // Release the queue lock
        drop(queue);

        // Check the preconditions for each writer
        let writers = self.check_preconditions(&mut writers);

        if writers.is_empty() {
            event!(self.logger, "write done (no-op due to preconditions)");
            return;
        }

        // Grab the sequence numbers for the set of batches
        let seq = self
            .next_seq_number
            .fetch_add(writers.len() as u64, Ordering::Relaxed);

        let res = Self::append_to_wal(&writers, &mut wal_and_manifest, seq);

        if let Err(error) = res {
            self.handle_write_error(&StorageError::Io(error), &writers);
            return;
        }

        let with_sequence = res.unwrap();

        let lsm_tree = self.lsm_tree.load().clone();

        let mut with_results = Vec::with_capacity(with_sequence.len());

        for (writer, seq) in with_sequence {
            event!(self.logger,
                "memtable_write start, seq={}, memtable={}, batch_size={}",
                seq, lsm_tree.memtable.log_number, writer.batch().len());

            lsm_tree.memtable.write(seq, writer.batch());
            with_results.push((writer, Ok(())));
            let compare = self.last_visible_seq.compare_exchange(
                seq - 1,
                seq,
                Ordering::Acquire,
                Ordering::Relaxed,
            );
            if compare.is_err() {
                panic!("Last visible sequence number out of order");
            }
            event!(self.logger,
                "memtable_write done, seq={}, memtable={}",
                seq, lsm_tree.memtable.log_number);
        }

        drop(wal_and_manifest); // release the lock as soon as possible

        for (writer, result) in with_results {
            writer.done(result);
        }
        event!(self.logger, "write done");
    }

    fn check_preconditions(self: &Arc<Self>, writers: &mut Vec<Arc<Writer>>) -> Vec<Arc<Writer>> {
        let seq = self.next_seq_number.load(Ordering::Relaxed);

        let mut successful_writers = Vec::with_capacity(writers.len());

        // Check preconditions before assigning sequence numbers
        for writer in writers {
            if let Some(preconditions) = writer.batch().preconditions() {
                let rs = self.check_writer_preconditions(seq, preconditions);
                if let Err(error) = rs {
                    writer.done(Err(error));
                    continue;
                }
            }
            successful_writers.push(writer.clone());
        }
        successful_writers
    }

    fn check_writer_preconditions(self: &Arc<Self>, seq: u64, preconditions: &Preconditions) -> StorageResult<()> {
        for precondition in preconditions.conditions() {
            match precondition {
                Precondition::MustNotExist {
                    collection,
                    index,
                    user_key,
                } => {
                    let rs = self.read_internal(
                        *collection,
                        *index,
                        user_key,
                        seq,
                        Some(preconditions.since()),
                    ).map_err(|e| StorageError::Io(e))?;

                    #[cfg(test)]
                    if self.fail_next_precondition_checks.load(Ordering::Relaxed) >= 1 {
                        self.fail_next_precondition_checks.fetch_sub(1, Ordering::Relaxed);
                        let error = Self::version_conflict_error(collection, index, user_key, preconditions.since());
                        return Err(error);
                    }

                    if let Some(_) = rs {
                        // Conflict detected
                        let error = Self::version_conflict_error(collection, index, user_key, preconditions.since());
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }

    fn version_conflict_error(
        collection: &u32,
        index: &u32,
        user_key: &Vec<u8>,
        since: u64
    ) -> StorageError {

        StorageError::VersionConflict {
            user_key: user_key.clone(),
            reason:
            format!(
                "Optimistic locking failed: key for collection {} index {} user_key {:x?} exists since snapshot {}",
                collection, index, user_key, since
            ),
        }
    }

    pub fn read(
        &self,
        collection: u32,
        index: u32,
        user_key: &[u8],
        snapshot: Option<u64>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let last_visible_sequence = self.last_visible_seq.load(Ordering::Relaxed);
        let snapshot = snapshot.map_or(last_visible_sequence, |s| s.min(last_visible_sequence));

        self.read_internal(collection, index, user_key, snapshot, None)
    }

    fn read_internal(
        &self,
        collection: u32,
        index: u32,
        user_key: &[u8],
        snapshot: u64,
        min_snapshot: Option<u64>
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {

        let lsm_tree = self.lsm_tree.load();
        lsm_tree.read(
            self.sst_cache.clone(),
            &self.db_dir,
            &encode_record_key(collection, index, user_key),
            snapshot,
            min_snapshot,
        ).into()
    }

    pub fn range_scan<R>(
        &self,
        collection: u32,
        index: u32,
        user_key_range: &R,
        snapshot: Option<u64>,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let last_visible_sequence = self.last_visible_seq.load(Ordering::Relaxed);
        let snapshot = snapshot.map_or(last_visible_sequence, |s| s.min(last_visible_sequence));

        let lsm_tree = self.lsm_tree.load_full();

        let iter_with_lifetime = lsm_tree.range_scan(
            self.sst_cache.clone(),
            &self.db_dir,
            collection,
            index,
            user_key_range,
            snapshot,
            direction,
        )?;

        // Here we are saying that the iterator can live for 'static.
        // This is safe because we are moving the lsm_tree Arc into the returned iterator struct,
        // so the LsmTree will live as long as the iterator.
        let static_iterator = unsafe {
            std::mem::transmute::<
                Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>,
                Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'static>,
            >(iter_with_lifetime)
        };

        let result_iterator = RangeScanIterator {
            _lsm_tree: lsm_tree,
            iterator: static_iterator,
        };

        Ok(Box::new(result_iterator))
    }

    fn append_to_wal(
        writers: &Vec<Arc<Writer>>,
        wal_and_manifest: &mut MutexGuard<WalAndManifest>,
        mut seq: u64,
    ) -> Result<Vec<(Arc<Writer>, u64)>> {
        let mut with_sequence = Vec::with_capacity(writers.len());

        for writer in writers {
            let batch = writer.batch();
            wal_and_manifest.wal.append(seq, batch)?;
            with_sequence.push((writer.clone(), seq));

            seq += 1;
        }
        Ok(with_sequence)
    }

    pub fn shutdown(self: &Arc<Self>) -> StorageResult<()> {
        info!(self.logger, "Shutting down storage engine");
        self.flush()?;
        info!(self.logger, "Storage engine flush completed successfully");
        Ok(())
    }

    pub fn flush(self: &Arc<Self>) -> StorageResult<()> {
        info!(self.logger, "Flush requested");
        event!(self.logger, "requested_flush start");

        if self.error_mode.load(Ordering::Relaxed) {
            return Err(StorageError::ErrorMode(
                "The database is in error mode dues to a previous write error".to_string(),
            ));
        }

        self.perform_wal_and_memtable_rotation(true)?;
        event!(self.logger, "requested_flush completed");
        Ok(())
    }

    fn wait_for_pending_flushes(self: &Arc<Self>) -> Result<()> {
        event!(self.logger, "flush_sync start");
        let callback = Callback::new_blocking(Box::new(|result| { result }));
        self.flush_manager.enqueue(FlushTask::Sync { callback: callback.clone() })?;
        callback.await_blocking()?;
        event!(self.logger, "flush_sync end");
        Ok(())
    }

    fn perform_wal_and_memtable_rotation_if_needed(self: &Arc<Self>) {
        let write_buffer_size = self.options.db.file_write_buffer_size.to_bytes();
        let memtable_size = self.lsm_tree.load().memtable.size();
        if memtable_size >= write_buffer_size {
            info!(self.logger, "Memtable size exceeded: size={}, limit={}", memtable_size, write_buffer_size);
            match self.perform_wal_and_memtable_rotation(false) {
                Err(error) => {
                    error!(self.logger, "An error occurred during wal and memtable rotation: {}", error);
                }
                Ok(_) => (),
            }
        }
    }

    fn get_async_callback(self: &Arc<Self>) -> &Arc<Callback<Result<SSTableOperation>>> {
        self.async_callback.get_or_init(|| {
            let engine = self.clone();
            Callback::new_async(self.logger.clone(), move |result| {
                engine.update_lsm_tree_sstables(result)
            })
        })
    }

    fn perform_wal_and_memtable_rotation(self: &Arc<Self>, force_flush: bool) -> Result<()>
    {
        // Rotate the write-ahead log file and the memtable
        // (through applying a WalRotation edit to the LSM tree and replacing it atomically)
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        let lsm_tree = self.lsm_tree.load();

        if !force_flush && lsm_tree.memtable.size() < self.options.db.file_write_buffer_size.to_bytes() {
            // No need to rotate
            return Ok(());
        }

        if force_flush && lsm_tree.memtable.size() == 0 {

            // No need to perform a flush as the memtable is empty, we can just wait
            // for pending flushes before continuing. At this point, we do not really care if
            // another race with the sync so we can release the locks before performing the sync
            drop(wal_and_manifest);

            self.wait_for_pending_flushes()

        } else {

            let new_log_number = self.next_file_number.fetch_add(1, Ordering::Relaxed);
            let rs = wal_and_manifest.wal.rotate(new_log_number);
            if rs .is_err() {
                error!(self.logger, "An error occurred during wal rotation: {}", rs.as_ref().err().unwrap());
                self.error_mode.store(true, Ordering::Relaxed);
                rs?;
            }

            let edit = ManifestEdit::WalRotation {
                log_number: new_log_number,
                next_seq: self.next_seq_number.load(Ordering::Relaxed),
            };

            let lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;

            // We want to put the task within the flush queue, while still holding the lock,
            // to ensure ordering with other flushes.

            // We just pushed the memtable to the back of the immutable queue.
            let memtable = lsm_tree.imm_memtables.back().unwrap().clone();

            let engine = self.clone();
            let callback = if force_flush {
                Callback::new_blocking(Box::new(move |result| {
                    let rs = engine.update_lsm_tree_sstables(result);
                    if rs.is_err() {
                        engine.error_mode.store(true, Ordering::Relaxed);
                    }
                    rs
                }))
            } else {
                self.get_async_callback().clone()
            };

            self.schedule_flush(memtable, callback.clone())?;

            // Once the flush task within the flush manager queue, we can release
            // the manifest lock to schedule the flush.
            drop(wal_and_manifest);

            if callback.is_blocking() {
                callback.await_blocking()
            } else {
                Ok(())
            }
        }
    }

    fn schedule_flush(self: &Arc<Self>,
                      memtable: Arc<Memtable>,
                      callback: Arc<Callback<Result<SSTableOperation>>>
    ) -> Result<()>
    {
        let sst_number = self.next_file_number.fetch_add(1, Ordering::Relaxed);

        let flush_task = FlushTask::Flush {
            sst_file: DbFile::new_sst(sst_number),
            memtable,
            callback: callback.clone(),
        };
        self.flush_manager.enqueue(flush_task)
    }

    fn update_lsm_tree_sstables(
        self: &Arc<Self>,
        operation: Result<SSTableOperation>,
    ) -> Result<()> {
        match operation {
            Ok(operation) => {
                match operation {
                    SSTableOperation::Flush {
                        log_number,
                        flushed,
                    } => {
                        event!(self.logger,
                            "manifest_update_after_flush started, log_number={}, sst={}",
                            log_number,
                            flushed,
                        );
                        // We want to perform the changes within the manifest lock to avoid concurrent updates to
                        // the LSM tree
                        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

                        let lsm_tree = self.lsm_tree.load();
                        let oldest_log_number = lsm_tree.next_log_number_after(log_number);

                        let edit = ManifestEdit::Flush {
                            oldest_log_number,
                            sst: flushed,
                        };
                        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;

                        let obsolete_log_files = wal_and_manifest.wal.drain_obsolete_logs(oldest_log_number)?;

                        drop(wal_and_manifest); // we do not need the manifest lock for deleting obsolete log files

                        self.delete_obsolete_log_files(obsolete_log_files)?;
                    }
                    SSTableOperation::Compaction { added: _, removed: _ } => {}
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn delete_obsolete_log_files(self: &Arc<Self>, obsolete_log_files: Vec<PathBuf>) -> Result<()> {
        for obsolete in obsolete_log_files {
            debug!(self.logger, "Deleting obsolete log file: {}", obsolete.to_string_lossy());
            remove_file(obsolete)?;
        }
        sync_dir(&self.db_dir)?;
        Ok(())
    }

    fn append_edit(
        self: &Arc<Self>,
        lsm_tree: &LsmTree,
        wal_and_manifest: &mut MutexGuard<WalAndManifest>,
        edit: &ManifestEdit,
    ) -> Result<Arc<LsmTree>> {
        let manifest = &mut wal_and_manifest.manifest;
        let rs = manifest.append_edit(&edit);

        if rs.is_err() {
            error!(self.logger, "An error occurred during manifest update: {}", rs.as_ref().err().unwrap());
            self.error_mode.store(true, Ordering::Relaxed);
            rs?;
        }

        let mut new_tree = Arc::new(lsm_tree.apply(&edit));

        if manifest.should_rotate() {
            let new_manifest_number = self.next_file_number.fetch_add(1, Ordering::Relaxed);
            let rs = manifest.rotate(
                new_manifest_number,
                &ManifestEdit::Snapshot(new_tree.manifest.clone()),
            );

            if rs.is_err() {
                error!(self.logger, "An error occurred during manifest rotation: {}", rs.as_ref().err().unwrap());
                self.error_mode.store(true, Ordering::Relaxed);
                rs?;
            }

            let edit = ManifestEdit::ManifestRotation {
                manifest_number: new_manifest_number,
            };
            new_tree = Arc::new(new_tree.apply(&edit));
        }

        self.lsm_tree.store(new_tree.clone());

        Ok(new_tree)
    }

    fn handle_write_error(&self, error: &StorageError, writers: &Vec<Arc<Writer>>) {
        self.error_mode.store(true, Ordering::Relaxed);
        error!(self.logger, "A write error occurred: {}", error);
        for writer in writers {
            writer.done(Err(error.clone()));
        }
        event!(self.logger, "write done");
    }

    fn add_metrics(metric_registry: &mut MetricRegistry, options: &Options, lsm_tree: Arc<ArcSwap<LsmTree>>) {

        for level in 0..options.db.max_levels {
            let level_name = format!("level_{}", level);

            let lsm = lsm_tree.clone();
            metric_registry.register_gauge(
                &format!("sstable_count_{}", level_name),
                DerivedGauge::new(Arc::new(move || {
                    let levels = lsm.load().levels();
                    let may_be_level = levels.level(level as usize);
                    may_be_level.map_or(0, |l| l.sst_count() as u64)
                })),
            );

            let lsm = lsm_tree.clone();
            metric_registry.register_gauge(
                &format!("sstable_size_{}", level_name),
                DerivedGauge::new(Arc::new(move || {
                    let levels = lsm.load().levels();
                    let may_be_level = levels.level(level as usize);
                    may_be_level.map_or(0, |l| l.total_bytes())
                })),
            );
        }

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge("sstable_count",
            DerivedGauge::new(Arc::new(move || { lsm.load().levels().sst_count() as u64 })),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge("stable_size",
            DerivedGauge::new(Arc::new(move || { lsm.load().levels().total_bytes() })),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge("memtable_size",
                                       DerivedGauge::new(Arc::new(move || { lsm.load().memtable.size() as u64 })),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge("memtable_total_size",
                                       DerivedGauge::new(Arc::new(move || {
                                           let lsm_tree = lsm.load();
                                           (lsm_tree.memtable.size()
                                               + lsm_tree.imm_memtables.iter().map(|m| m.size()).sum::<usize>())
                                           as u64
                                       })),
        );

        let lsm_tree = lsm_tree.clone();
        metric_registry.register_gauge("memtable_count",
                                       DerivedGauge::new(Arc::new(move || { (lsm_tree.load().imm_memtables.len() + 1) as u64 })),
        );
    }

    #[cfg(test)]
    pub fn wal_return_error_on_write(&self, value: bool) {
        self.db_mutex.lock().unwrap().wal.return_error_on_append(value);
    }

    #[cfg(test)]
    pub fn manifest_return_error_on_write(&self, value: bool) {
        self.db_mutex.lock().unwrap().manifest.return_error_on_append(value);
    }

    #[cfg(test)]
    pub fn wal_return_error_on_rotate(&self, value: bool) {
        self.db_mutex.lock().unwrap().wal.return_error_on_rotate(value);
    }

    #[cfg(test)]
    pub fn manifest_return_error_on_rotate(&self, value: bool) {
        self.db_mutex.lock().unwrap().manifest.return_error_on_rotate(value);
    }

    #[cfg(test)]
    pub fn lsm_tree(&self) -> Arc<LsmTree> {
        self.lsm_tree.load_full()
    }

    #[cfg(test)]
    pub fn fail_next_precondition_checks(&self, count: u8) {
        let _ = self.fail_next_precondition_checks.store(count, Ordering::Relaxed);
    }
}



/// The result of scanning the database directory at startup.
///
/// Contains the list of WAL files that must be replayed,
/// and the next available file number to assign to new files.
#[derive(Debug)]
struct StartupScanResult {
    /// WAL files to be replayed, sorted by file ID in ascending order.
    /// Each entry is a tuple of (file_number, full_path).
    wal_files: Vec<(u64, PathBuf)>,

    /// WAL files that are obsolete and can be deleted.
    obsolete_wal_files: Vec<(u64, PathBuf)>,

    /// The next unused file number. This is computed as one greater
    /// than the highest file number seen among MANIFEST, WAL, and SST files.
    next_file_number: u64,
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
fn scan_db_directory(dir: &Path, oldest_log_number: u64) -> StorageResult<StartupScanResult> {
    let mut wal_files = Vec::new();
    let mut obsolete_wal_files = Vec::new();
    let mut max_file_num = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(db_file) = DbFile::new(&path) {
            max_file_num = max_file_num.max(db_file.number);

            if db_file.file_type == FileType::WriteAheadLog {
                if db_file.number >= oldest_log_number {
                    wal_files.push((db_file.number, path.clone()));
                } else {
                    obsolete_wal_files.push((db_file.number, path.clone()));
                }
            }
        }
    }

    wal_files.sort_by_key(|(id, _)| *id);

    Ok(StartupScanResult {
        wal_files,
        obsolete_wal_files,
        next_file_number: max_file_num + 1,
    })
}

#[allow(dead_code)]
struct Writer {
    write_batch: WriteBatch,
    result: Mutex<Option<StorageResult<()>>>,
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

    pub fn batch(&self) -> &WriteBatch {
        &self.write_batch
    }

    fn wait(&self) -> StorageResult<()> {
        let mut result = self.result.lock().unwrap();
        while result.is_none() {
            result = self.condvar.wait(result).unwrap();
        }
        Self::copy(result).unwrap()
    }

    fn result(&self) -> StorageResult<()> {
        Self::copy(self.result.lock().unwrap())
            .unwrap_or_else(|| Err(StorageError::UnexpectedError("No result available".to_string())))
    }
    fn done(&self, res: StorageResult<()>) {
        let mut result = self.result.lock().unwrap();
        *result = Some(res);
        self.condvar.notify_one();
    }

    fn copy(result: MutexGuard<Option<StorageResult<()>>>) -> Option<StorageResult<()>> {
        match &*result {
            Some(Ok(())) => Some(Ok(())), // Return Ok if present
            Some(Err(e)) => Some(Err(e.clone())), // Recreate the error
            None => None,
        }
    }
}

pub enum SSTableOperation {
    Flush {
        log_number: u64,
        flushed: Arc<SSTableMetadata>,
    },
    Compaction {
        added: Vec<Arc<SSTableMetadata>>,
        removed: Vec<Arc<SSTableMetadata>>,
    },
}

struct RangeScanIterator {
    // This must be declared before the iterator to be dropped after.
    _lsm_tree: Arc<LsmTree>,
    iterator: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'static>,
}

impl Iterator for RangeScanIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

#[derive(Debug)]
pub enum StorageError {
    Io(Error),
    UnexpectedError(String),
    ErrorMode(String),
    VersionConflict{ user_key: Vec<u8>, reason: String },
    LogCorruption { record_offset: u64, reason: String },
}

impl StorageError {
    pub fn as_io_error(&self) -> Option<&Error> {
        match self {
            StorageError::Io(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<Error> for StorageError {
    fn from(err: Error) -> Self {
        StorageError::Io(err)
    }
}

impl From<LogReplayError> for StorageError {
    fn from(err: LogReplayError) -> Self {
        match err {
            LogReplayError::Io(e) => StorageError::Io(e),
            LogReplayError::Corruption { record_offset, reason } => {
                StorageError::LogCorruption { record_offset, reason }
            }
        }
    }
}

impl Clone for StorageError {
    fn clone(&self) -> Self {
        match self {
            StorageError::Io(e) => StorageError::Io(Error::new(e.kind(), e.to_string())),
            StorageError::UnexpectedError(msg) => StorageError::UnexpectedError(msg.clone()),
            StorageError::ErrorMode(msg) => StorageError::ErrorMode(msg.clone()),
            StorageError::VersionConflict { user_key, reason} =>
                StorageError::VersionConflict{ user_key: user_key.clone(), reason: reason.clone() },
            StorageError::LogCorruption { record_offset, reason } =>
                StorageError::LogCorruption {
                    record_offset: *record_offset,
                    reason: reason.clone()
                },
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "IO error: {}", e),
            StorageError::UnexpectedError(msg) => write!(f, "Unexpected error: {}", msg),
            StorageError::ErrorMode(msg) => write!(f, "Error mode: {}", msg),
            StorageError::VersionConflict { user_key, reason} =>
                write!(f, "Version conflict for user_key {:?} : {}", user_key, reason),
            StorageError::LogCorruption { record_offset, reason } => {
                write!(f, "Log corruption at offset {}: {}", record_offset, reason)
            }
        }
    }
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::logger::{test_instance, NoOpLogger};
    use crate::obs::metrics::{assert_counter_eq, assert_gauge_eq};
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::options::storage_quantity::StorageUnit::Mebibytes;
    use crate::storage::operation::Operation;
    use crate::storage::test_utils::{assert_next_entry_eq, delete_op, delete_rec, document, put_op, put_rec, user_key};
    use crate::storage::write_batch::{Precondition, Preconditions};
    use bson::{doc, to_vec};
    use std::fs::{self, OpenOptions};
    use std::io::{ErrorKind, Seek, SeekFrom, Write};
    use std::path::Path;
    use tempfile::tempdir;

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

            // Only logs ≥ 3 should be returned as wal_files
            assert_eq!(result.wal_files.len(), 1);
            assert_eq!(result.wal_files[0].0, 5);

            // Obsolete logs < 3 should be returned as obsolete_wal_files
            assert_eq!(result.obsolete_wal_files.len(), 1);
            assert_eq!(result.obsolete_wal_files[0].0, 2);

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

    #[test]
    fn test_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(Options::lightweight()),
            &path,
        )
        .unwrap();

        let col = 10;
        let idx = 0;

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(WriteBatch::new(vec![insert])).unwrap();
        }

        let snapshot = engine.last_visible_seq.load(Ordering::Relaxed);

        let _ = &engine.write(WriteBatch::new(vec![delete_op(col, 4)])).unwrap();

        assert_gauge_eq(registry, "sstable_count", 0);
        assert_gauge_eq(registry, "sstable_count_level_0", 0);
        assert_gauge_eq(registry, "sstable_count_level_1", 0);
        assert_counter_eq(registry, "flush_count", 0);

        for flush in [false, true] {
            if flush {
                let _ = &engine.flush().unwrap();

                assert_gauge_eq(registry, "sstable_count", 1);
                assert_gauge_eq(registry, "sstable_count_level_0", 1);
                assert_gauge_eq(registry, "sstable_count_level_1", 0);
                assert_counter_eq(registry, "flush_count", 1);

            }

            let actual = &engine.read(col, idx, &user_key(1), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 1, 1, 1));

            let actual = &engine.read(col, idx, &user_key(2), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 2, 1, 2));

            let actual = &engine.read(col, idx, &user_key(3), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 3, 1, 3));

            let actual = &engine.read(col, idx, &user_key(4), None).unwrap().unwrap();
            assert_eq!(actual, &delete_rec(col, 4, 5));

            assert!(&engine.read(col, idx, &user_key(5), None).unwrap().is_none());
        }

        let updates = vec![
            put_op(col, 2, 2),
            put_op(col, 3, 2),
            put_op(col, 4, 2),
        ];

        for update in updates {
            let _ = &engine.write(WriteBatch::new(vec![update])).unwrap();
        }

        for flush in [false, true] {
            if flush {
                let _ = &engine.flush().unwrap();

                assert_gauge_eq(registry, "sstable_count", 2);
                assert_gauge_eq(registry, "sstable_count_level_0", 2);
                assert_gauge_eq(registry, "sstable_count_level_1", 0);
                assert_counter_eq(registry, "flush_count", 2);
            }

            let actual = &engine.read(col, idx, &user_key(1), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 1, 1, 1));

            let actual = &engine.read(col, idx, &user_key(2), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 2, 2, 6));

            let actual = &engine.read(col, idx, &user_key(3), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 3, 2, 7));

            let actual = &engine.read(col, idx, &user_key(4), None).unwrap().unwrap();
            assert_eq!(actual, &put_rec(col, 4, 2, 8));
        }

        // Now we will test with a snapshot
        let actual = &engine.read(col, idx, &user_key(1), Some(snapshot)).unwrap().unwrap();
        assert_eq!(actual, &put_rec(col, 1, 1, 1));

        let actual = &engine.read(col, idx, &user_key(2), Some(snapshot)).unwrap().unwrap();
        assert_eq!(actual, &put_rec(col, 2, 1, 2));

        let actual = &engine.read(col, idx, &user_key(3), Some(snapshot)).unwrap().unwrap();
        assert_eq!(actual, &put_rec(col, 3, 1, 3));

        let actual = &engine.read(col, idx, &user_key(4), Some(snapshot)).unwrap().unwrap();
        assert_eq!(actual, &put_rec(col, 4, 1, 4));
    }

    #[test]
    fn test_range_scan() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(Options::lightweight()),
            &path,
        )
        .unwrap();

        let col = 42;
        let idx = 0;

        // Stage 1: All in memtable
        let inserts = vec![
            put_op(col, 1, 1), // seq 1
            put_op(col, 2, 1), // seq 2
            put_op(col, 3, 1), // seq 3
            put_op(col, 4, 1), // seq 4
            put_op(col, 5, 1), // seq 5
        ];
        for insert in inserts {
            engine.write(WriteBatch::new(vec![insert])).unwrap();
        }

        let snapshot1 = engine.last_visible_seq.load(Ordering::Relaxed);
        assert_eq!(snapshot1, 5);

        // update 2, delete 4
        let updates = vec![
            put_op(col, 2, 2), // seq 6
            delete_op(col, 4), // seq 7
        ];
        for update in updates {
            engine.write(WriteBatch::new(vec![update])).unwrap();
        }

        // --- Verification: memtable only ---
        let mut iter = engine
            .range_scan(col, idx, &(..), None, Direction::Forward)
            .unwrap();
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 2, 6));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 4, 7));
        assert_next_entry_eq(&mut iter, &put_rec(col, 5, 1, 5));
        assert!(iter.next().is_none());

        let mut iter = engine
            .range_scan(col, idx, &(..), Some(snapshot1), Direction::Forward)
            .unwrap();
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &put_rec(col, 5, 1, 5));
        assert!(iter.next().is_none());

        // Stage 2: One SSTable and memtable
        engine.flush().unwrap();
        let snapshot2 = engine.last_visible_seq.load(Ordering::Relaxed);
        assert_eq!(snapshot2, 7);

        let updates2 = vec![
            put_op(col, 6, 1), // seq 8
            put_op(col, 3, 2), // seq 9
            delete_op(col, 5), // seq 10
        ];
        for update in updates2 {
            engine.write(WriteBatch::new(vec![update])).unwrap();
        }

        // --- Verification: 1 SSTable + memtable ---
        let mut iter = engine
            .range_scan(col, idx, &(..), None, Direction::Forward)
            .unwrap();
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 2, 6));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 2, 9));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 4, 7));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 10));
        assert_next_entry_eq(&mut iter, &put_rec(col, 6, 1, 8));
        assert!(iter.next().is_none());

        let mut iter = engine
            .range_scan(col, idx, &(..), Some(snapshot2), Direction::Reverse)
            .unwrap();
        assert_next_entry_eq(&mut iter, &put_rec(col, 5, 1, 5));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 4, 7));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 2, 6));
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert!(iter.next().is_none());

        // Stage 3: Two SSTables and memtable
        engine.flush().unwrap();
        let snapshot3 = engine.last_visible_seq.load(Ordering::Relaxed);
        assert_eq!(snapshot3, 10);

        let updates3 = vec![
            put_op(col, 1, 2), // seq 11
            put_op(col, 7, 1), // seq 12
        ];
        for update in updates3 {
            engine.write(WriteBatch::new(vec![update])).unwrap();
        }

        // --- Verification: 2 SSTables + memtable ---
        let mut iter = engine
            .range_scan(col, idx, &(..), None, Direction::Forward)
            .unwrap();
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 2, 11));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 2, 6));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 2, 9));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 4, 7));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 10));
        assert_next_entry_eq(&mut iter, &put_rec(col, 6, 1, 8));
        assert_next_entry_eq(&mut iter, &put_rec(col, 7, 1, 12));
        assert!(iter.next().is_none());

        let mut iter = engine
            .range_scan(
                col,
                idx,
                &(user_key(2)..=user_key(6)),
                Some(snapshot3),
                Direction::Reverse,
            )
            .unwrap();
        assert_next_entry_eq(&mut iter, &put_rec(col, 6, 1, 8));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 10));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 4, 7));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 2, 9));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 2, 6));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_read_and_scan_with_immutable_memtables() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.file_write_buffer_size = StorageQuantity::new(4, Mebibytes);
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(options),
            &path,
        )
        .unwrap();

        let col = 10;
        let idx = 0;

        // Pause the flush manager to keep immutable memtables around.
        engine.flush_manager.pause();

        // Write enough data to trigger a memtable rotation.
        // We write four ~1MB values to fill up the 4MB memtable.
        let val_1mb_string = "a".repeat(1024 * 1024);
        let val_1mb = to_vec(&doc! { "v": val_1mb_string }).unwrap();
        for i in 1..=4 {
            engine
                .write(WriteBatch::new(vec![Operation::new_put(
                    col,
                    idx,
                    user_key(i),
                    val_1mb.clone(),
                )]))
                .unwrap();
        }

        // This fifth write will trigger rotation, creating an immutable memtable.
        let val_active = to_vec(&doc! { "v": "active" }).unwrap();
        engine
            .write(WriteBatch::new(vec![Operation::new_put(
                col,
                idx,
                user_key(5),
                val_active.clone(),
            )]))
            .unwrap();

        // Verify that one immutable memtable now exists.
        assert_eq!(engine.lsm_tree().imm_memtables.len(), 1);

        // --- Verification: Read from both active and immutable memtables ---
        // Read from what is now the immutable memtable.
        assert_eq!(
            engine
                .read(col, idx, &user_key(1), None)
                .unwrap()
                .unwrap()
                .1,
            val_1mb
        );
        // Read from the active memtable.
        assert_eq!(
            engine
                .read(col, idx, &user_key(5), None)
                .unwrap()
                .unwrap()
                .1,
            val_active
        );
        // Read a non-existent key.
        assert!(engine
            .read(col, idx, &user_key(6), None)
            .unwrap()
            .is_none());

        // --- Verification: Range scan over both memtables ---
        let results: Vec<_> = engine
            .range_scan(col, idx, &(..), None, Direction::Forward)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].1, val_1mb);
        assert_eq!(results[4].1, val_active);
        assert!(results[0]
            .0
            .starts_with(&encode_record_key(col, idx, &user_key(1))));
        assert!(results[4]
            .0
            .starts_with(&encode_record_key(col, idx, &user_key(5))));

        // --- Verification: Updates and snapshots ---
        let snapshot = engine.last_visible_seq.load(Ordering::Relaxed);
        assert_eq!(snapshot, 5);

        // Update a key that is in the immutable memtable. The update goes to the active memtable.
        let val_update = to_vec(&doc! { "v": "updated" }).unwrap();
        engine
            .write(WriteBatch::new(vec![Operation::new_put(
                col,
                idx,
                user_key(2),
                val_update.clone(),
            )]))
            .unwrap();

        // Read the updated key without a snapshot. Should see the new value.
        assert_eq!(
            engine
                .read(col, idx, &user_key(2), None)
                .unwrap()
                .unwrap()
                .1,
            val_update
        );
        // Read with the snapshot. Should see the old value.
        assert_eq!(
            engine
                .read(col, idx, &user_key(2), Some(snapshot))
                .unwrap()
                .unwrap()
                .1,
            val_1mb
        );

        // --- Cleanup and final verification ---
        // Resume the flush manager and wait for it to process the pending flush task.
        engine.flush_manager.resume();
        engine.wait_for_pending_flushes().unwrap();

        // The immutable memtable should now be flushed to an SSTable.
        assert_eq!(engine.lsm_tree().imm_memtables.len(), 0);
        assert_gauge_eq(registry, "sstable_count_level_0", 1);
        assert_counter_eq(registry, "flush_count", 1);

        // Data should still be readable from the new SSTable.
        assert_eq!(
            engine
                .read(col, idx, &user_key(2), None)
                .unwrap()
                .unwrap()
                .1,
            val_update
        );

        // Flush the active memtable as well.
        engine.flush().unwrap();
        assert_gauge_eq(registry, "sstable_count_level_0", 2);
    }

    #[test]
    fn test_replay_with_multiple_wals() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.file_write_buffer_size = StorageQuantity::new(4, Mebibytes);

        let col = 10;
        let idx = 0;

        let val_1mb_string = "a".repeat(1024 * 1024);
        let val_1mb = to_vec(&doc! { "v": val_1mb_string }).unwrap();

        {
            let old_engine = StorageEngine::new(
                test_instance(),
                registry,
                Arc::new(options.clone()),
                &path,
            ).unwrap();

            // Pause the flush manager to keep immutable memtables around.
            old_engine.flush_manager.pause();

            // Write enough data to trigger 2 memtable rotations.
            // We write four ~1MB values to fill up the 4MB memtable.
            for i in 1..=11 {
                old_engine
                    .write(WriteBatch::new(vec![Operation::new_put(
                        col,
                        idx,
                        user_key(i),
                        val_1mb.clone(),
                    )]))
                    .unwrap();
            }

            // Verify that two immutable memtables now exists.
            assert_eq!(old_engine.lsm_tree().imm_memtables.len(), 2);

            assert!(path.join("000002.log").exists());
            assert!(path.join("000003.log").exists());
            // The next WAL should be 000005.log as the flush will be blocked after the sstable number is assigned.
            assert!(path.join("000005.log").exists());
        }

        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(options),
            &path,
        ).unwrap();

        let new_wal_path = path.join("000006.sst");
        assert!(new_wal_path.exists());
        let new_sst_path = path.join("000007.sst");
        assert!(new_sst_path.exists());

        // --- Verification ---
        for i in 1..=11 {
            assert_eq!(
                engine
                    .read(col, idx, &user_key(i), None)
                    .unwrap()
                    .unwrap()
                    .1,
                val_1mb
            );
        }
    }

    #[test]
    fn test_manifest_rotation() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        // Each flush generates two edits (WalRotation, Flush). A new manifest starts with a 4KiB
        // block. We set the limit to 5KiB to ensure a rotation occurs within our test loop.
        options.db.max_manifest_file_size =
            StorageQuantity::new(5, crate::options::storage_quantity::StorageUnit::Kibibytes);

        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options.clone()), path).unwrap();

        let col = 1;
        let idx = 0;

        assert_counter_eq(registry, "manifest_rewrite", 0);

        let initial_manifest_path = Manifest::read_current_file(path).unwrap().unwrap();
        assert!(initial_manifest_path
            .to_string_lossy()
            .contains("MANIFEST-000001"));

        // Each flush generates two edits (WalRotation, Flush), consuming space in the manifest.
        // The initial manifest is ~4KiB. Each pair of edits for a flush is ~40 bytes.
        // We need to cross the 5KiB threshold, so we need ~30 flushes.
        for i in 0..30 {
            engine
                .write(WriteBatch::new(vec![put_op(col, i, i as u32)]))
                .unwrap();
            engine.flush().unwrap();
        }

        // A new manifest should have been created.
        assert_counter_eq(registry, "manifest_rewrite", 1);
        let current_manifest_path = Manifest::read_current_file(path).unwrap().unwrap();
        assert_ne!(current_manifest_path, initial_manifest_path);

        // Verify data is readable after rotation.
        for i in 0..30 {
            let (_key, val) = engine
                .read(col, idx, &user_key(i), None)
                .unwrap()
                .unwrap();
            let (_expected_key, expected_val) = put_rec(col, i, i as u32, (i + 1) as u64);
            assert_eq!(val, expected_val);
        }

        // Verify recovery after rotation.
        let db_path = path.to_path_buf();
        drop(engine);

        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            Arc::new(options),
            &db_path,
        )
        .unwrap();

        // Verify data is readable after restart.
        for i in 0..30 {
            let (_key, val) = engine_restarted
                .read(col, idx, &user_key(i), None)
                .unwrap()
                .unwrap();
            let (_expected_key, expected_val) = put_rec(col, i, i as u32, (i + 1) as u64);
            assert_eq!(val, expected_val);
        }
    }

    #[test]
    fn test_manifest_rotation_error() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        // Each flush generates two edits (WalRotation, Flush). A new manifest starts with a 4KiB
        // block. We set the limit to 5KiB to ensure a rotation occurs within our test loop.
        options.db.max_manifest_file_size =
            StorageQuantity::new(5, StorageUnit::Kibibytes);

        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options.clone()), path).unwrap();

        let col = 1;

        assert_counter_eq(registry, "manifest_rewrite", 0);

        let initial_manifest_path = Manifest::read_current_file(path).unwrap().unwrap();
        assert!(initial_manifest_path
            .to_string_lossy()
            .contains("MANIFEST-000001"));

        engine.manifest_return_error_on_rotate(true);

        // Each flush generates two edits (WalRotation, Flush), consuming space in the manifest.
        // The initial manifest is ~4KiB. Each pair of edits for a flush is ~40 bytes.
        // We need to cross the 5KiB threshold, so we need ~30 flushes.
        for i in 0..30 {
            engine
                .write(WriteBatch::new(vec![put_op(col, i, i as u32)]))
                .unwrap();
            let rs = engine.flush();
            if rs.is_err() {
                assert_eq!(
                    rs.err().unwrap().to_string(),
                    "IO error: Injected error on rotate",
                );
                break;
            }
        }

        check_error_mode(engine, col);
    }

    #[test]
    fn test_obsolete_wal_deletion() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let registry = &mut MetricRegistry::default();
        let options = Options::lightweight();

        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options.clone()), path).unwrap();

        let col = 10;
        let idx = 0;

        // The first WAL file should be 000002.log.
        let wal_path_1 = path.join("000002.log");
        assert!(wal_path_1.exists());

        // Write some data, which goes into the first WAL.
        engine
            .write(WriteBatch::new(vec![put_op(col, 1, 1)]))
            .unwrap();

        // Flush will rotate the WAL, flush the memtable, and then delete the old WAL.
        engine.flush().unwrap();

        // The old WAL (000002.log) should now be deleted.
        assert!(!wal_path_1.exists());

        // A new WAL should have been created (000003.log).
        let wal_path_2 = path.join("000003.log");
        assert!(wal_path_2.exists());

        // Write more data, which goes into the second WAL.
        engine
            .write(WriteBatch::new(vec![put_op(col, 2, 2)]))
            .unwrap();

        // Flush again.
        engine.flush().unwrap();

        // The second WAL (000003.log) should now be deleted.
        assert!(!wal_path_2.exists());

        // And a third one should exist (000005.log).
        let wal_path_3 = path.join("000005.log");
        assert!(wal_path_3.exists());

        // Data should still be readable from SSTables.
        let (_key, val1) = engine
            .read(col, idx, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_expected_key, expected_val1) = put_rec(col, 1, 1, 1);
        assert_eq!(val1, expected_val1);

        let (_key, val2) = engine
            .read(col, idx, &user_key(2), None)
            .unwrap()
            .unwrap();
        let (_expected_key, expected_val2) = put_rec(col, 2, 2, 2);
        assert_eq!(val2, expected_val2);
    }

    #[test]
    fn test_concurrent_writes_simple() {
        test_concurrent_writes(false);
    }

    #[test]
    fn test_concurrent_writes_with_concurrent_flushes() {
        test_concurrent_writes(true);
    }

    fn test_concurrent_writes(with_concurrent_flushes: bool) {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let engine = StorageEngine::new(
            Arc::new(NoOpLogger::default()), // Disabling traces as RustRover cannot handle this amount of logging when running the tests
            registry,
            Arc::new(Options::lightweight()),
            &path,
        )
            .unwrap();

        let num_threads = 5;
        let writes_per_thread = 200;
        let col = 10;
        let idx = 0;

        std::thread::scope(|s| {
            for i in 0..num_threads {
                let engine_clone = engine.clone();
                s.spawn(move || {
                    for j in 0..writes_per_thread {
                        let key = i * writes_per_thread + j;
                        let value = key as u32;
                        let op = put_op(col, key, value);
                        engine_clone.write(WriteBatch::new(vec![op])).unwrap();
                        if  with_concurrent_flushes && j == 100 {
                            // Occasionally flush to increase concurrency complexity
                            engine_clone.flush().unwrap();
                        }
                    }
                });
            }
        });

        for flush in [false, true] {

            if flush {
                engine.flush().unwrap();
            }

            // Verification
            for i in 0..num_threads {
                for j in 0..writes_per_thread {
                    let key = i * writes_per_thread + j;
                    let value = key as u32;
                    let (_record_key, record_value) =
                        engine.read(col, idx, &user_key(key), None).unwrap().unwrap();
                    let expected_value = to_vec(&document(key, value)).unwrap();
                    assert_eq!(record_value, expected_value, "Record value does not match expected value for key {}", key);
                }
            }
        }
    }

    #[test]
    fn test_shutdown_and_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.wal_bytes_per_sync = StorageQuantity::new(0, StorageUnit::Bytes); // force syncs for each write
        let options = Arc::new(options);

        let col = 10;
        let idx = 0;

        // --- First run ---
        {
            let engine = StorageEngine::new(
                test_instance(),
                registry,
                options.clone(),
                &db_path,
            )
            .unwrap();

            // Write some data and flush it to an SSTable.
            engine.write(WriteBatch::new(vec![put_op(col, 1, 1)])).unwrap();
            engine.write(WriteBatch::new(vec![put_op(col, 2, 1)])).unwrap();
            engine.flush().unwrap();

            // Write more data that will remain in the memtable.
            engine.write(WriteBatch::new(vec![put_op(col, 3, 1)])).unwrap();
            engine.write(WriteBatch::new(vec![put_op(col, 2, 2)])).unwrap(); // Update flushed key

            // Gracefully shut down the engine. This should flush the memtable.
            engine.shutdown().unwrap();
        }

        // --- Second run (restart) ---
        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        // Verify all data is present and correct after restart.
        let (_key1, val1) = engine_restarted.read(col, idx, &user_key(1), None).unwrap().unwrap();
        let (_, expected_val1) = put_rec(col, 1, 1, 1);
        assert_eq!(val1, expected_val1);

        let (_key2, val2) = engine_restarted.read(col, idx, &user_key(2), None).unwrap().unwrap();
        let (_, expected_val2) = put_rec(col, 2, 2, 4);
        assert_eq!(val2, expected_val2);

        let (_key3, val3) = engine_restarted.read(col, idx, &user_key(3), None).unwrap().unwrap();
        let (_, expected_val3) = put_rec(col, 3, 1, 3);
        assert_eq!(val3, expected_val3);

        assert!(engine_restarted.read(col, idx, &user_key(4), None).unwrap().is_none());
    }

    #[test]
    fn test_wal_replay_on_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.wal_bytes_per_sync = StorageQuantity::new(0, StorageUnit::Bytes); // force syncs for each write
        let options = Arc::new(options);

        let col = 10;
        let idx = 0;

        // --- First run (simulating a crash) ---
        {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();

            // Write some data and flush it to an SSTable.
            engine
                .write(WriteBatch::new(vec![put_op(col, 1, 1)]))
                .unwrap(); // seq 1
            engine
                .write(WriteBatch::new(vec![put_op(col, 2, 1)]))
                .unwrap(); // seq 2
            engine.flush().unwrap(); // Flushes memtable, rotates WAL.

            // Data in SSTable: {1:1, 2:1}

            // Write more data that will remain in the memtable and WAL.
            engine
                .write(WriteBatch::new(vec![put_op(col, 3, 1)]))
                .unwrap(); // seq 3
            engine
                .write(WriteBatch::new(vec![put_op(col, 2, 2)]))
                .unwrap(); // seq 4, updates a flushed key

            // Simulate a crash by just dropping the engine without calling shutdown.
            // The memtable content is lost, but the WAL records should persist.
            drop(engine);
        }

        // --- Second run (restart and replay) ---
        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        // The WAL should be replayed, restoring the memtable state.
        // Verify all data is present and correct after restart.

        // From SSTable
        let (_key1, val1) = engine_restarted
            .read(col, idx, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_, expected_val1) = put_rec(col, 1, 1, 1);
        assert_eq!(val1, expected_val1);

        // From WAL replay (update)
        let (_key2, val2) = engine_restarted
            .read(col, idx, &user_key(2), None)
            .unwrap()
            .unwrap();
        let (_, expected_val2) = put_rec(col, 2, 2, 4);
        assert_eq!(val2, expected_val2);

        // From WAL replay (new key)
        let (_key3, val3) = engine_restarted
            .read(col, idx, &user_key(3), None)
            .unwrap()
            .unwrap();
        let (_, expected_val3) = put_rec(col, 3, 1, 3);
        assert_eq!(val3, expected_val3);

        assert!(engine_restarted
        .read(col, idx, &user_key(4), None)
        .unwrap()
        .is_none());
        }

    #[test]
    fn test_wal_replay_with_last_log_partially_written() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.wal_bytes_per_sync = StorageQuantity::new(0, StorageUnit::Bytes);
        let options = Arc::new(options);

        let col = 10;
        let idx = 0;

        let wal_path;
        // --- First run (simulating a crash) ---
        {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();
            engine
                .write(WriteBatch::new(vec![put_op(col, 1, 1)]))
                .unwrap();
            engine
                .write(WriteBatch::new(vec![put_op(col, 2, 1)]))
                .unwrap();

            wal_path = db_path.join("000002.log");
            drop(engine);
        }

        // Corrupt the WAL by appending a partial record.
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        // Write a record size (4 bytes), but nothing else. This simulates a crash during write.
        file.write_all(&[0, 0, 1, 0]).unwrap(); // size = 256
        file.sync_all().unwrap();
        drop(file);

        // --- Second run (restart and replay) ---
        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
            .unwrap();

        // WAL replay should have truncated the file and recovered the valid records.
        let (_key1, val1) = engine_restarted
            .read(col, idx, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_, expected_val1) = put_rec(col, 1, 1, 1);
        assert_eq!(val1, expected_val1);

        let (_key2, val2) = engine_restarted
            .read(col, idx, &user_key(2), None)
            .unwrap()
            .unwrap();
        let (_, expected_val2) = put_rec(col, 2, 1, 2);
        assert_eq!(val2, expected_val2);

        // Key 3 should not exist because it was part of the corrupted, truncated segment.
        assert!(engine_restarted
            .read(col, idx, &user_key(3), None)
            .unwrap()
            .is_none());

        // Writing a new record should work.
        engine_restarted
            .write(WriteBatch::new(vec![put_op(col, 3, 1)]))
            .unwrap();
        let (_key3, val3) = engine_restarted
            .read(col, idx, &user_key(3), None)
            .unwrap()
            .unwrap();
        let (_, expected_val3) = put_rec(col, 3, 1, 3); // next seq is 3
        assert_eq!(val3, expected_val3);
    }

    #[test]
    fn test_wal_replay_with_header_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.wal_bytes_per_sync = StorageQuantity::new(0, StorageUnit::Bytes);
        let options = Arc::new(options);

        let col = 10;
        let idx = 0;

        let original_wal_path;
        // --- First run ---
        {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();
            engine
                .write(WriteBatch::new(vec![put_op(col, 1, 1)]))
                .unwrap();
            original_wal_path = db_path.join("000002.log");
            drop(engine);
        }

        // Corrupt the WAL header by overwriting the first few bytes.
        let mut file = OpenOptions::new()
            .write(true)
            .open(&original_wal_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xFF; 16]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // --- Second run (restart) ---
        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
            .unwrap();

        // The corrupted WAL should have been renamed.
        let corrupted_path = db_path.join(format!(
            "{}.corrupted",
            original_wal_path.file_name().unwrap().to_str().unwrap()
        ));
        assert!(corrupted_path.exists());
        assert!(!original_wal_path.exists());

        // A new WAL file should be created.
        let new_wal_path = db_path.join("000003.log");
        assert!(new_wal_path.exists());

        // The data should be lost.
        assert!(engine_restarted
            .read(col, idx, &user_key(1), None)
            .unwrap()
            .is_none());

        // The database should be usable.
        engine_restarted
            .write(WriteBatch::new(vec![put_op(col, 2, 1)]))
            .unwrap();
        assert!(engine_restarted
            .read(col, idx, &user_key(2), None)
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_restart_fails_with_corrupted_old_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        // Set a small buffer size to trigger memtable rotation easily.
        options.db.file_write_buffer_size = StorageQuantity::new(1, StorageUnit::Kibibytes);
        let options = Arc::new(options);

        let col = 10;
        let idx = 0;

        let old_wal_path;
        // --- First run ---
        {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();

            old_wal_path = db_path.join("000002.log");
            assert!(old_wal_path.exists());

            // Pause the flush manager to keep wal around.
            engine.flush_manager.pause();

            // Write enough data to trigger memtable rotation, which also rotates the WAL.
            let large_val = vec![0; 1024];
            engine
                .write(WriteBatch::new(vec![Operation::new_put(
                    col,
                    idx,
                    user_key(1),
                    large_val.clone(),
                )]))
                .unwrap();
            engine
                .write(WriteBatch::new(vec![Operation::new_put(
                    col,
                    idx,
                    user_key(2),
                    large_val.clone(),
                )]))
                .unwrap();

            // A new WAL should exist now.
            let new_wal_path = db_path.join("000003.log");
            assert!(new_wal_path.exists());

            // Simulate crash by dropping the engine.
            drop(engine);
        }

        // Corrupt the old WAL file (not the header, but a record in it).
        let mut file = OpenOptions::new().write(true).open(&old_wal_path).unwrap();
        file.seek(SeekFrom::Start(4096)).unwrap(); // After header block
        file.write_all(&[0xFF; 16]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // --- Second run (restart) ---
        let result = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        );

        // Restart should fail because an old (non-terminal) WAL is corrupted.
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(matches!(error, StorageError::LogCorruption { .. }));
    }

    #[test]
    fn test_restart_with_stale_files() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let options = Arc::new(Options::lightweight());

        let col = 10;

        // --- First run ---
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &db_path,
            )
            .unwrap();

            // After initialization: MANIFEST-000001, 000002.log are created. Next file is 3.
            let next_file_num_before = engine.next_file_number.load(Ordering::Relaxed);
            assert_eq!(next_file_num_before, 3);

            let inserts = vec![
                put_op(col, 1, 1),
                put_op(col, 2, 1),
                put_op(col, 3, 1),
                put_op(col, 4, 1),
            ];

            for insert in inserts {
                let _ = &engine.write(WriteBatch::new(vec![insert])).unwrap();
            }

            // Simulate crash
            drop(engine);
        }

        // --- Create stale files ---
        // Create files with numbers higher than what the manifest knows.
        let stale_sst_path = db_path.join("000010.sst");
        fs::File::create(&stale_sst_path).unwrap();

        let stale_log_path = db_path.join("000012.log");
        fs::File::create(&stale_log_path).unwrap();

        // --- Second run (restart) ---
        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        // The engine should have detected the "000012.log" file and marked it as corrupted.
        assert!(db_path.join("000012.log.corrupted").exists());
        assert!(!stale_log_path.exists());

        // The engine should have detected the stale files and updated its file number counter.
        // The highest number was set to 12, one sstable has been flushed (000013.st for memtable 2),
        // and a new wal has been created (000014.log), so the next file number will be 15.
        let next_file_num_after = engine_restarted.next_file_number.load(Ordering::Relaxed);
        assert_eq!(next_file_num_after, 15);

        // Verify that new files are created with the correct numbers.
        // A flush rotates the WAL, so a new WAL file should be created.
        engine_restarted
            .write(WriteBatch::new(vec![put_op(col, 1, 1)]))
            .unwrap();
        engine_restarted.flush().unwrap();

        // Flush rotates WAL to 15.log and creates sstable 16.sst. Next file number is 16.
        let new_wal_path = db_path.join("000015.log");
        assert!(new_wal_path.exists());
        let new_sst_path = db_path.join("000016.sst");
        assert!(new_sst_path.exists());

        assert_eq!(engine_restarted.next_file_number.load(Ordering::Relaxed), 17);
    }

    #[test]
    fn test_error_mode_activation_and_rejection() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let registry = &mut MetricRegistry::default();
        let options = Arc::new(Options::lightweight());

        let engine =
            StorageEngine::new(test_instance(), registry, options.clone(), path).unwrap();

        let col = 10;

        // 1. Inject an error into the WAL write path.
        engine.wal_return_error_on_write(true);

        // 2. Perform a write operation that is expected to fail.
        let write_result = engine.write(WriteBatch::new(vec![put_op(col, 1, 1)]));
        assert!(write_result.is_err());
        let error = write_result.err().unwrap();
        let io_error = error.as_io_error().unwrap();
        assert_eq!(io_error.kind(), ErrorKind::Other);
        assert!(io_error.to_string().contains("Injected error on append"));

        // 3. Disable error injection to ensure subsequent failures are due to error_mode.
        engine.wal_return_error_on_write(false);
        assert!(engine.error_mode.load(Ordering::Relaxed));

        // 4. Verify that subsequent operations are rejected.
        check_error_mode(engine.clone(), col);
    }

    #[test]
    fn test_wal_rotation_on_write_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let mut options = Options::lightweight();
        options.db.file_write_buffer_size = StorageQuantity::new(4, Mebibytes);
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(options),
            &path,
        )
            .unwrap();

        let col = 10;
        let idx = 0;

        engine.wal_return_error_on_rotate(true);

        // Write enough data to trigger a memtable rotation.
        // We write five ~1MB values to fill up the 4MB memtable.
        let val_1mb_string = "a".repeat(1024 * 1024);
        let val_1mb = to_vec(&doc! { "v": val_1mb_string }).unwrap();
        for i in 1..=5 {
            let rs = engine
                .write(WriteBatch::new(vec![Operation::new_put(
                    col,
                    idx,
                    user_key(i),
                    val_1mb.clone(),
                )]));

            if rs.is_err() {
                assert_eq!(
                    rs.err().unwrap().to_string(),
                    "IO error: Injected error on rotate",
                );
                break;
            }
        }

        engine.wal_return_error_on_rotate(false);
        assert!(engine.error_mode.load(Ordering::Relaxed));

        check_error_mode(engine.clone(), col);
    }

    #[test]
    fn test_wal_rotation_on_flush_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let options = Options::lightweight();
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(options),
            &path,
        )
            .unwrap();

        let col = 10;

        engine.wal_return_error_on_rotate(true);

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(WriteBatch::new(vec![insert])).unwrap();
        }

        let rs = engine.flush();
        if rs.is_err() {
            assert_eq!(
                    rs.err().unwrap().to_string(),
                    "IO error: Injected error on rotate",
                );
        }

        engine.wal_return_error_on_rotate(false);
        assert!(engine.error_mode.load(Ordering::Relaxed));

        check_error_mode(engine.clone(), col);
    }

    #[test]
    fn test_manifest_write_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let options = Options::lightweight();
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(options),
            &path,
        )
            .unwrap();

        let col = 10;

        engine.manifest_return_error_on_write(true);

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(WriteBatch::new(vec![insert])).unwrap();
        }

        let rs = engine.flush();
        if rs.is_err() {
            assert_eq!(
                    rs.err().unwrap().to_string(),
                    "IO error: Injected error on append",
                );
        }

        engine.manifest_return_error_on_write(false);
        assert!(engine.error_mode.load(Ordering::Relaxed));

        check_error_mode(engine.clone(), col);
    }

    #[test]
    fn test_memtable_flush_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let options = Options::lightweight();
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(options),
            &path,
        )
            .unwrap();

        let col = 10;

        engine.lsm_tree().memtable.return_error_on_flush(true);

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(WriteBatch::new(vec![insert])).unwrap();
        }

        let rs = engine.flush();
        if rs.is_err() {
            assert_eq!(
                    rs.err().unwrap().to_string(),
                    "IO error: Simulated memtable flush error",
                );
        }

        engine.manifest_return_error_on_write(false);
        assert!(engine.error_mode.load(Ordering::Relaxed));

        check_error_mode(engine.clone(), col);
    }

    fn check_error_mode(engine: Arc<StorageEngine>, col: u32) {
        let expected_error_msg = "Error mode: The database is in error mode dues to a previous write error";

        // Test write
        let write_result_after_error = engine.write(WriteBatch::new(vec![put_op(col, 2, 2)]));
        assert!(write_result_after_error.is_err());
        assert_eq!(
            write_result_after_error.err().unwrap().to_string(),
            expected_error_msg
        );

        // Test flush
        let flush_result = engine.flush();
        assert!(flush_result.is_err());
        assert_eq!(flush_result.err().unwrap().to_string(), expected_error_msg);

        // Test create_collection
        let create_coll_result = engine.create_collection_if_not_exists("new_collection");
        assert!(create_coll_result.is_err());
        assert_eq!(
            create_coll_result.err().unwrap().to_string(),
            expected_error_msg
        );
    }

    #[test]
    fn test_optimistic_locking_must_not_exist() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let engine = StorageEngine::new(
            test_instance(),
            registry,
            Arc::new(Options::lightweight()),
            &path,
        )
        .unwrap();

        let col = 10;
        let idx = 0;

        // 1. Write key1.
        engine
            .write(WriteBatch::new(vec![put_op(col, 1, 1)]))
            .unwrap();

        // 2. Take a snapshot after key1 is written.
        let snapshot1 = engine.last_visible_sequence();

        // 3. Write key2.
        engine
            .write(WriteBatch::new(vec![put_op(col, 2, 1)]))
            .unwrap();

        // 4. Try to write key2 again with a precondition based on the old snapshot.
        // This should fail because key2 was created *after* snapshot1.
        let precondition = Precondition::MustNotExist {
            collection: col,
            index: idx,
            user_key: user_key(2),
        };
        let preconditions = Preconditions::new(snapshot1, vec![precondition]);
        let batch = WriteBatch::new_with_preconditions(vec![put_op(col, 2, 2)], preconditions);
        let result = engine.write(batch);

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .to_string()
            .contains("Optimistic locking failed: key for collection 10 index 0 user_key"));

        // 5. Take a new snapshot and try to write a new key. This should succeed.
        let snapshot2 = engine.last_visible_sequence();
        let precondition_ok = Precondition::MustNotExist {
            collection: col,
            index: idx,
            user_key: user_key(3),
        };
        let preconditions_ok = Preconditions::new(snapshot2, vec![precondition_ok]);
        let batch_ok =
            WriteBatch::new_with_preconditions(vec![put_op(col, 3, 1)], preconditions_ok);
        engine.write(batch_ok).unwrap();

        // 6. Try to write an existing key (key1) again. This should fail because the key
        // already exists, and the `read_since` check will find it.
        let precondition_fail = Precondition::MustNotExist {
            collection: col,
            index: idx,
            user_key: user_key(1),
        };
        let preconditions_fail = Preconditions::new(0, vec![precondition_fail]);
        let batch_fail =
            WriteBatch::new_with_preconditions(vec![put_op(col, 1, 2)], preconditions_fail);
        let result_fail = engine.write(batch_fail);
        println!("Result fail: {:?}", result_fail);
        assert!(result_fail.is_err());
        let err_fail = result_fail.err().unwrap();
        assert!(err_fail
            .to_string()
            .contains("Optimistic locking failed: key for collection 10 index 0 user_key"));
    }
}
