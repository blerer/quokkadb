use crate::io::{mark_file_as_corrupted, sync_dir, truncate_file};
use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::obs::metrics::{DerivedGauge, MetricRegistry};
use crate::options::options::Options;
use crate::storage::append_log::LogReplayError;
use crate::storage::callback::Callback;
use crate::storage::catalog::{Catalog, CollectionOptions, IndexDefinition, IndexOptions};
use crate::storage::compaction::compaction_manager::CompactionManager;
use crate::storage::compaction::compaction_picker::CompactionJob;
use crate::storage::count_stats::{CountStatSource, CountStats, CountStatsKey};
use crate::storage::files::{DbFile, FileType};
use crate::storage::flush_manager::{FlushManager, FlushTask};
use crate::storage::lsm_tree::LsmTree;
use crate::storage::lsm_version::{DropMetadata, SSTableMetadata};
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
use std::fs::remove_file;
use std::io::{Error, Result};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::{fmt, fs};

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
    next_file_number: Arc<AtomicU64>, // The counter used to create the file ids
    next_seq_number: AtomicU64,       // The counter used to create sequence numbers
    last_visible_seq: AtomicU64,
    sst_cache: Arc<SSTableCache>,
    flush_manager: FlushManager,
    compaction_manager: CompactionManager,
    async_callback: OnceLock<Arc<Callback<Result<SSTableOperation>>>>,
    obsolete_sstables: Mutex<VecDeque<Arc<SSTableMetadata>>>,
    error_mode: AtomicBool,
    #[cfg(test)]
    fail_next_precondition_checks: AtomicU8,
    #[cfg(test)]
    disable_auto_compaction: AtomicBool,
}

impl StorageEngine {
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: Arc<Options>,
        db_dir: &Path,
    ) -> StorageResult<Arc<Self>> {
        let sst_cache = Arc::new(SSTableCache::new(logger.clone(), metric_registry, &options));

        info!(
            logger,
            "Starting storage engine at {}",
            db_dir.to_string_lossy()
        );

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
                Manifest::load_from(logger.clone(), metric_registry, &options, manifest_path)?;

            let next_file_number = manifest_state.lsm.next_file_number;

            let mut lsm_tree = LsmTree::from(manifest_state);

            // If a file with a higher number that the next_file number has been detected we need to update
            // the Lsm tree in-memory and on-disk (MANIFEST file)
            let next_file_number = Arc::new(AtomicU64::new(
                if next_file_number < scan_results.next_file_number {
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
                },
            ));

            // We will keep track of the rotated log files while replaying the wal files.
            let mut rotated_log_files =
                VecDeque::from_iter(scan_results.obsolete_wal_files.iter().rev().cloned());

            let mut previous = None;

            while let Some((log_number, wal_path)) = wal_files_iter.next() {
                info!(
                    logger,
                    "Replaying operations from {}",
                    wal_path.to_string_lossy()
                );

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
                        &next_file_number,
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
                        info!(
                            logger,
                            "{} operations replayed from {}",
                            count,
                            wal_path.to_string_lossy()
                        );
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
                                    error!(
                                        logger,
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
                    &options,
                    &wal_path,
                    rotated_log_files,
                )?
            } else {
                let log_number = next_file_number.fetch_add(1, Ordering::Relaxed);

                info!(
                    logger,
                    "Latest wal was corrupted. Starting from a clean wal file: {}", log_number
                );

                let wal = WriteAheadLog::new_after_corruption(
                    logger.clone(),
                    metric_registry,
                    &options,
                    db_dir,
                    log_number,
                    rotated_log_files,
                )?;
                let edit = ManifestEdit::WalRotation {
                    log_number,
                    next_seq: last_seq_nbr + 1,
                };
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
                        &next_file_number,
                    )?;
                } else {
                    info!(
                        logger,
                        "Ignoring empty memtable: {}", lsm_tree.imm_memtables[0].log_number
                    );

                    // Drop the empty memtable
                    let edit = ManifestEdit::IgnoringEmptyMemtable {
                        oldest_log_number: lsm_tree.imm_memtables[0].log_number,
                    };
                    lsm_tree = lsm_tree.apply(&edit);
                    manifest.append_edit(&edit)?;
                }
                wal
            };

            // Delete SST files that are on disk but not referenced by the manifest.
            // These are leftovers from a compaction that was interrupted before the
            // old input files could be deleted.
            let live_ssts = lsm_tree.levels().live_sst_numbers();
            let mut deleted_orphaned_ssts = Vec::new();
            for (number, path) in &scan_results.sst_files {
                if !live_ssts.contains(number) {
                    info!(
                        logger,
                        "Deleting orphaned SST file at startup: {}",
                        path.to_string_lossy()
                    );
                    fs::remove_file(path)?;
                    deleted_orphaned_ssts.push(path.clone());
                }
            }
            if !deleted_orphaned_ssts.is_empty() {
                sync_dir(db_dir)?;
            }

            let flush_manager = FlushManager::new(
                logger.clone(),
                metric_registry,
                options.clone(),
                db_dir,
                sst_cache.clone(),
            )?;

            let compaction_manager = CompactionManager::new(
                logger.clone(),
                metric_registry,
                options.clone(),
                &db_dir,
                sst_cache.clone(),
                next_file_number.clone(),
            )?;

            let lsm_tree = Arc::new(ArcSwap::new(Arc::new(lsm_tree)));
            Self::add_metrics(metric_registry, &options, lsm_tree.clone());

            let engine = Arc::new(StorageEngine {
                logger: logger.clone(),
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                db_mutex: Mutex::new(WalAndManifest { wal, manifest }),
                lsm_tree,
                next_file_number,
                next_seq_number: AtomicU64::new(last_seq_nbr + 1),
                last_visible_seq: AtomicU64::new(last_seq_nbr),
                sst_cache,
                flush_manager,
                compaction_manager,
                async_callback: OnceLock::new(),
                obsolete_sstables: Mutex::new(VecDeque::new()),
                error_mode: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_precondition_checks: AtomicU8::new(0),
                #[cfg(test)]
                disable_auto_compaction: AtomicBool::new(false),
            });

            info!(logger, "Storage engine started");

            engine.schedule_compaction_if_needed();

            Ok(engine)
        } else {
            let next_file_number = Arc::new(AtomicU64::new(1));
            let next_seq_number = AtomicU64::new(1);
            let manifest_number = next_file_number.fetch_add(1, Ordering::Relaxed);
            let log_number = next_file_number.fetch_add(1, Ordering::Relaxed);
            let lsm_tree = Arc::new(LsmTree::new(
                log_number,
                next_file_number.load(Ordering::Relaxed),
                next_seq_number.load(Ordering::Relaxed),
                options.max_levels(),
            ));
            let snapshot = ManifestEdit::Snapshot(lsm_tree.manifest.clone());
            let manifest = Manifest::new(
                logger.clone(),
                metric_registry,
                &options,
                db_dir,
                manifest_number,
                &snapshot,
            )?;
            let wal = WriteAheadLog::new(
                logger.clone(),
                metric_registry,
                &options,
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

            let compaction_manager = CompactionManager::new(
                logger.clone(),
                metric_registry,
                options.clone(),
                &db_dir,
                sst_cache.clone(),
                next_file_number.clone(),
            )?;

            let lsm_tree = Arc::new(ArcSwap::new(lsm_tree));
            Self::add_metrics(metric_registry, &options, lsm_tree.clone());

            info!(logger, "Storage engine started");

            Ok(Arc::new(StorageEngine {
                logger,
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                db_mutex: Mutex::new(WalAndManifest { wal, manifest }),
                lsm_tree,
                next_file_number,
                next_seq_number,
                last_visible_seq: AtomicU64::new(0),
                sst_cache,
                flush_manager,
                compaction_manager,
                async_callback: OnceLock::new(),
                obsolete_sstables: Mutex::new(VecDeque::new()),
                error_mode: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_precondition_checks: AtomicU8::new(0),
                #[cfg(test)]
                disable_auto_compaction: AtomicBool::new(false),
            }))
        }
    }

    fn flush_replayed_data(
        logger: &Arc<dyn LoggerAndTracer>,
        options: &Arc<Options>,
        db_dir: &&Path,
        manifest: &mut Manifest,
        lsm_tree: &mut LsmTree,
        next_file_number: &AtomicU64,
    ) -> StorageResult<LsmTree> {
        info!(
            logger,
            "Flushing data from {}", lsm_tree.imm_memtables[0].log_number
        );

        // Flush the current memtable to a sst file before processing the next
        // wal file.
        let sst_file = DbFile::new_sst(next_file_number.fetch_add(1, Ordering::Relaxed));

        let imm_memtable = lsm_tree.imm_memtables[0].clone();
        let count_stats = imm_memtable.count_stats_for_flush();

        let sst = Arc::new(imm_memtable.flush(&db_dir, &sst_file, &options)?);

        let edit = ManifestEdit::Flush {
            oldest_log_number: lsm_tree.imm_memtables[0].log_number,
            sst,
            count_stats,
        };
        let lsm_tree = lsm_tree.apply(&edit);
        manifest.append_edit(&edit)?;
        Ok(lsm_tree)
    }

    pub fn create_collection_if_not_exists(self: &Arc<Self>, name: &str) -> StorageResult<u32> {
        self.check_error_mode()?;
        if let Some(collection) = self.catalog().get_collection_by_name(name) {
            Ok(collection.id)
        } else {
            self.perform_create_collection(name, CollectionOptions::default(), true)
        }
    }

    fn check_error_mode(self: &Arc<Self>) -> StorageResult<()> {
        if self.error_mode.load(Ordering::Relaxed) {
            Err(StorageError::ErrorMode(
                "The database is in error mode dues to a previous write error".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn create_collection(
        self: &Arc<Self>,
        name: &str,
        options: CollectionOptions,
    ) -> StorageResult<u32> {
        self.check_error_mode()?;
        self.perform_create_collection(name, options, false)
    }

    fn perform_create_collection(
        self: &Arc<Self>,
        name: &str,
        options: CollectionOptions,
        if_exists: bool,
    ) -> StorageResult<u32> {
        // The collection do not exist we need to create it and update the manifest
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        // We need first to check that the collection has not been created concurrently
        let lsm_tree = self.lsm_tree.load();
        let catalogue = lsm_tree.catalog();
        let collection = catalogue.get_collection_by_name(name);
        if collection.is_none() {
            let id = catalogue.next_collection_id;
            let edit = ManifestEdit::CreateCollection {
                name: name.to_string(),
                id,
                created_at: self.next_seq_number.load(Ordering::Relaxed),
                options,
            };
            // We want to sync to ensure that all the previous sequence numbers are persisted.
            wal_and_manifest.wal.sync()?;
            info!(self.logger, "Creating collection '{}' with id {}", name, id);
            let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
            Ok(id)
        } else {
            if if_exists {
                Ok(collection.unwrap().id)
            } else {
                Err(StorageError::CollectionAlreadyExists(name.to_string()))
            }
        }
    }

    pub fn drop_collection(self: &Arc<Self>, name: &str) -> StorageResult<()> {
        self.check_error_mode()?;
        let id = self.catalog().get_collection_by_name(name).map(|c| c.id);

        if id.is_none() {
            // Collection does not exist, nothing to do
            return Ok(());
        }

        let id = id.unwrap();
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        let lsm_tree = self.lsm_tree.load();
        let edit = ManifestEdit::DropCollection {
            id,
            dropped_at: self.next_seq_number.load(Ordering::Relaxed),
        };
        // We want to sync to ensure that all the previous sequence numbers are persisted.
        wal_and_manifest.wal.sync()?;
        info!(self.logger, "Dropping collection '{}' with id {}", name, id);
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(())
    }

    pub fn rename_collection(
        self: &Arc<Self>,
        old_name: &str,
        new_name: &str,
    ) -> StorageResult<()> {
        self.check_error_mode()?;

        let catalog = self.catalog();
        let id = catalog
            .get_collection_by_name(old_name)
            .map(|c| c.id)
            .ok_or_else(|| StorageError::CollectionNotFound {
                name: old_name.to_string(),
                id: None,
            })?;

        // Check that new name is not already taken
        if catalog.get_collection_by_name(new_name).is_some() {
            return Err(StorageError::CollectionAlreadyExists(new_name.to_string()));
        }

        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        // Re-check under lock to avoid TOCTOU
        let lsm_tree = self.lsm_tree.load();
        let catalog = lsm_tree.catalog();

        if catalog.get_collection_by_name(old_name).is_none() {
            return Err(StorageError::CollectionNotFound {
                name: old_name.to_string(),
                id: None,
            });
        }
        if catalog.get_collection_by_name(new_name).is_some() {
            return Err(StorageError::CollectionAlreadyExists(new_name.to_string()));
        }

        let edit = ManifestEdit::RenameCollection {
            id,
            new_name: new_name.to_string(),
        };

        wal_and_manifest.wal.sync()?;
        info!(
            self.logger,
            "Renaming collection '{}' to '{}' (id {})", old_name, new_name, id
        );
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(())
    }

    pub fn create_index(
        self: &Arc<Self>,
        collection_id: u32,
        definition: IndexDefinition,
        options: IndexOptions,
    ) -> StorageResult<String> {
        self.check_error_mode()?;

        let snapshot = self.next_seq_number.load(Ordering::Relaxed);
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        let lsm_tree = self.lsm_tree.load();
        let catalog = lsm_tree.catalog();
        let collection = catalog
            .get_collection_at(collection_id, snapshot)
            .ok_or_else(|| StorageError::CollectionNotFound {
                name: catalog
                    .get_collection_by_id(&collection_id)
                    .map(|collection| collection.name.clone())
                    .unwrap_or_default(),
                id: Some(collection_id),
            })?;

        let resolved_name = options
            .name
            .clone()
            .unwrap_or_else(|| definition.as_string());

        if let Some(existing_index) = collection.get_index_by_name(&resolved_name) {
            if existing_index.is_equivalent_to(&definition, &options) {
                return Ok(existing_index.name());
            }

            return Err(StorageError::IndexOptionsConflict {
                collection_name: collection.name.clone(),
                index_name: resolved_name,
                reason: "An index with the same name already exists with a different definition or options".to_string(),
            });
        }

        if let Some(existing_index) = collection.find_index_equivalent_to(&definition, &options) {
            let index_name = existing_index.name();
            return Err(StorageError::IndexOptionsConflict {
                collection_name: collection.name.clone(),
                index_name: index_name.clone(),
                reason: format!(
                    "An equivalent index already exists under a different name: {}",
                    index_name
                ),
            });
        }

        let index_id = collection.next_index_id;
        let edit = ManifestEdit::CreateIndex {
            collection_id,
            index_id,
            definition,
            options,
            created_at: snapshot,
        };

        wal_and_manifest.wal.sync()?;
        info!(
            self.logger,
            "Creating index '{}.{}' with id {}", collection.name, resolved_name, index_id
        );
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(resolved_name)
    }

    pub fn drop_index(self: &Arc<Self>, collection_id: u32, index_id: u32) -> StorageResult<()> {
        self.check_error_mode()?;

        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        let lsm_tree = self.lsm_tree.load();
        let catalog = lsm_tree.catalog();
        let snapshot = self.next_seq_number.load(Ordering::Relaxed);
        let collection = match catalog.get_collection_at(collection_id, snapshot) {
            Some(collection) => collection,
            None => {
                if catalog.get_collection_by_id(&collection_id).is_some() {
                    // The collection has already been dropped, so the index is already gone. We can return Ok.
                    return Ok(());
                }

                return Err(StorageError::CollectionNotFound {
                    name: String::new(),
                    id: Some(collection_id),
                });
            }
        };

        let index = match collection.get_index_at(index_id, snapshot) {
            Some(collection) => collection,
            None => {
                if collection.get_index_by_id(index_id).is_some() {
                    // The index has already been dropped
                    return Ok(());
                }

                return Err(StorageError::IndexNotFound {
                    collection_name: collection.name.clone(),
                    index_name: collection
                        .get_index_by_id(index_id)
                        .map(|index| index.name())
                        .unwrap_or_default(),
                    id: Some(index_id),
                });
            }
        };

        let edit = ManifestEdit::DropIndex {
            collection_id,
            index_id,
            dropped_at: snapshot,
        };

        wal_and_manifest.wal.sync()?;
        info!(
            self.logger,
            "Dropping index '{}.{}' with id {}",
            collection.name,
            index.name(),
            index.id
        );
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(())
    }

    pub fn last_visible_sequence(&self) -> u64 {
        self.last_visible_seq.load(Ordering::Relaxed)
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        let lsm_tree = self.lsm_tree.load();
        lsm_tree.catalog().clone()
    }

    pub fn count_stat(&self, key: &CountStatsKey) -> Option<i64> {
        let lsm_tree = self.lsm_tree.load();
        lsm_tree.count_stat(key)
    }

    pub fn write(self: &Arc<Self>, batch: WriteBatch) -> StorageResult<()> {
        self.check_error_mode()?;

        let writer = Arc::new(Writer::new(batch));

        // Add the writer to the queue
        self.queue.lock().unwrap().push_back(writer.clone());

        // If no leader is active, this thread becomes leader
        if self.is_leader(&writer) {
            debug!(
                self.logger,
                "Thread {:?} is the leader and will perform the write",
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

        debug!(
            self.logger,
            "Thread {:?} will perform the write for {:?} writers",
            std::thread::current().id(),
            writers.len(),
        );
        event!(self.logger, "write start, writers_size={}", writers.len());

        // We want to acquire the db lock before we release the one on the queue,
        // to avoid a race condition where the next leader thread that just entered
        // the queue, on lock release, take the lock on them first.
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        // Release the queue lock
        drop(queue);

        let lsm_tree = self.lsm_tree.load().clone();

        // Check the preconditions for each writer
        let writers = self.check_preconditions(lsm_tree.catalog(), &mut writers);

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

        let mut with_results = Vec::with_capacity(with_sequence.len());

        for (writer, seq) in with_sequence {
            event!(
                self.logger,
                "memtable_write start, seq={}, memtable={}, batch_size={}",
                seq,
                lsm_tree.memtable.log_number,
                writer.batch().len()
            );

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
            event!(
                self.logger,
                "memtable_write done, seq={}, memtable={}",
                seq,
                lsm_tree.memtable.log_number
            );
        }

        drop(wal_and_manifest); // release the lock as soon as possible

        for (writer, result) in with_results {
            writer.done(result);
        }
        event!(self.logger, "write done");
    }

    fn check_preconditions(
        self: &Arc<Self>,
        catalog: Arc<Catalog>,
        writers: &mut Vec<Arc<Writer>>,
    ) -> Vec<Arc<Writer>> {
        let seq = self.next_seq_number.load(Ordering::Relaxed);

        let mut successful_writers = Vec::with_capacity(writers.len());

        for writer in writers {
            let rs = self.check_writer_collections_exist(&catalog, writer.batch(), seq);

            if let Err(error) = rs {
                writer.done(Err(error));
                continue;
            }

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

    fn check_writer_collections_exist(
        self: &Arc<Self>,
        catalog: &Catalog,
        batch: &WriteBatch,
        seq: u64,
    ) -> StorageResult<()> {
        for &(col, idx) in batch.required_collections() {
            let collection =
                catalog
                    .get_collection_at(col, seq)
                    .ok_or(StorageError::CollectionNotFound {
                        name: catalog
                            .get_collection_by_id(&col)
                            .map(|c| c.name.clone())
                            .unwrap(),
                        id: Some(col),
                    })?;

            if idx != 0 {
                collection
                    .get_index_at(idx, seq)
                    .ok_or(StorageError::IndexNotFound {
                        collection_name: collection.name.clone(),
                        index_name: collection.get_index_by_id(idx).map(|i| i.name()).unwrap(),
                        id: Some(idx),
                    })?;
            }
        }
        Ok(())
    }

    fn check_writer_preconditions(
        self: &Arc<Self>,
        seq: u64,
        preconditions: &Preconditions,
    ) -> StorageResult<()> {
        for precondition in preconditions.conditions() {
            match precondition {
                Precondition::VersionMatch {
                    collection,
                    index,
                    user_key,
                } => {
                    let rs = self
                        .read_internal(
                            *collection,
                            *index,
                            user_key,
                            seq,
                            Some(preconditions.since()),
                        )
                        .map_err(|e| StorageError::Io(e))?;

                    #[cfg(test)]
                    if self.fail_next_precondition_checks.load(Ordering::Relaxed) >= 1 {
                        self.fail_next_precondition_checks
                            .fetch_sub(1, Ordering::Relaxed);
                        let error = Self::version_conflict_error(
                            collection,
                            index,
                            user_key,
                            preconditions.since(),
                        );
                        return Err(error);
                    }

                    if let Some(_) = rs {
                        // Conflict detected
                        let error = Self::version_conflict_error(
                            collection,
                            index,
                            user_key,
                            preconditions.since(),
                        );
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
        since: u64,
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
        min_snapshot: Option<u64>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let lsm_tree = self.lsm_tree.load();

        lsm_tree
            .read(
                self.sst_cache.clone(),
                &self.db_dir,
                collection,
                index,
                user_key,
                snapshot,
                min_snapshot,
            )
            .into()
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
        self.compaction_manager.shutdown();
        Ok(())
    }

    pub fn flush(self: &Arc<Self>) -> StorageResult<()> {
        info!(self.logger, "Flush requested");
        event!(self.logger, "requested_flush start");

        self.check_error_mode()?;

        self.perform_wal_and_memtable_rotation(true)?;
        event!(self.logger, "requested_flush completed");
        Ok(())
    }

    fn wait_for_pending_flushes(self: &Arc<Self>) -> Result<()> {
        event!(self.logger, "flush_sync start");
        let callback = Callback::new_blocking(Box::new(|result| result));
        self.flush_manager.enqueue(FlushTask::Sync {
            callback: callback.clone(),
        })?;
        callback.await_blocking()?;
        event!(self.logger, "flush_sync end");
        Ok(())
    }

    fn perform_wal_and_memtable_rotation_if_needed(self: &Arc<Self>) {
        let write_buffer_size = self.options.file_write_buffer_size().to_bytes();
        let memtable_size = self.lsm_tree.load().memtable.size();
        if memtable_size >= write_buffer_size {
            info!(
                self.logger,
                "Memtable size exceeded: size={}, limit={}", memtable_size, write_buffer_size
            );
            match self.perform_wal_and_memtable_rotation(false) {
                Err(error) => {
                    error!(
                        self.logger,
                        "An error occurred during wal and memtable rotation: {}", error
                    );
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

    fn perform_wal_and_memtable_rotation(self: &Arc<Self>, force_flush: bool) -> Result<()> {
        // Rotate the write-ahead log file and the memtable
        // (through applying a WalRotation edit to the LSM tree and replacing it atomically)
        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

        let lsm_tree = self.lsm_tree.load();

        if !force_flush
            && lsm_tree.memtable.size() < self.options.file_write_buffer_size().to_bytes()
        {
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
            if rs.is_err() {
                error!(
                    self.logger,
                    "An error occurred during wal rotation: {}",
                    rs.as_ref().err().unwrap()
                );
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

            drop(wal_and_manifest);
            self.delete_obsolete_sst_files()?;

            if callback.is_blocking() {
                callback.await_blocking()
            } else {
                Ok(())
            }
        }
    }

    fn schedule_flush(
        self: &Arc<Self>,
        memtable: Arc<Memtable>,
        callback: Arc<Callback<Result<SSTableOperation>>>,
    ) -> Result<()> {
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
                let lsm_tree = match operation {
                    SSTableOperation::Flush {
                        log_number,
                        flushed,
                        count_stats,
                    } => {
                        event!(
                            self.logger,
                            "manifest_update_after_flush started, log_number={}, sst={}",
                            log_number,
                            flushed,
                        );
                        // We want to perform the changes within the manifest lock to avoid concurrent updates to
                        // the LSM tree
                        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

                        let lsm_tree = self.lsm_tree.load_full();
                        let oldest_log_number = lsm_tree.next_log_number_after(log_number);

                        let edit = ManifestEdit::Flush {
                            oldest_log_number,
                            sst: flushed,
                            count_stats,
                        };
                        let lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;

                        let obsolete_log_files = wal_and_manifest
                            .wal
                            .drain_obsolete_logs(oldest_log_number)?;

                        drop(wal_and_manifest); // we do not need the manifest lock for deleting obsolete log files

                        self.delete_obsolete_log_files(obsolete_log_files)?;
                        lsm_tree
                    }
                    SSTableOperation::Compaction {
                        compaction_job,
                        removed_sstables,
                        added_sstables,
                        drops,
                    } => {
                        // We want to perform the changes within the manifest lock to avoid concurrent updates to
                        // the LSM tree
                        let mut wal_and_manifest = self.db_mutex.lock().unwrap();

                        let lsm_tree = self.lsm_tree.load_full();

                        for sst in &removed_sstables {
                            let sst_path = self.db_dir.join(DbFile::new_sst(sst.number).filename());
                            self.sst_cache.evict(&sst_path);
                        }

                        {
                            event!(
                                self.logger,
                                "Marking sst tables {:?} as obsoletes",
                                removed_sstables
                                    .iter()
                                    .map(|sst| sst.number)
                                    .collect::<Vec<u64>>()
                            );

                            let mut obsolete = self.obsolete_sstables.lock().unwrap();
                            obsolete.extend(removed_sstables.iter().cloned());
                        }

                        let output_level = compaction_job.output_level as usize;
                        let edit = ManifestEdit::Compaction {
                            output_level,
                            removed_sstables,
                            added_sstables,
                            drops,
                        };

                        let new_lsm_tree =
                            self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
                        self.compaction_manager.unmark_compacting(&compaction_job);
                        new_lsm_tree
                    }
                };
                self.schedule_compaction_if_needed();
                drop(lsm_tree);
                self.delete_obsolete_sst_files()?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn schedule_compaction_if_needed(self: &Arc<Self>) {
        #[cfg(test)]
        if self.disable_auto_compaction.load(Ordering::Relaxed) {
            return;
        }

        let _wal_and_manifest = self.db_mutex.lock().unwrap();
        let levels = self.lsm_tree.load_full().levels();
        self.compaction_manager
            .schedule_compaction_if_needed(&levels, self.get_async_callback());
    }

    fn delete_obsolete_log_files(self: &Arc<Self>, obsolete_log_files: Vec<PathBuf>) -> Result<()> {
        for obsolete in obsolete_log_files {
            debug!(
                self.logger,
                "Deleting obsolete log file: {}",
                obsolete.to_string_lossy()
            );
            remove_file(obsolete)?;
        }
        sync_dir(&self.db_dir)?;
        Ok(())
    }

    fn delete_obsolete_sst_files(&self) -> Result<()> {
        let mut obsolete = self.obsolete_sstables.lock().unwrap();

        let mut to_delete = Vec::new();

        while matches!(obsolete.front(), Some(sst) if Arc::strong_count(sst) == 1) {
            to_delete.push(obsolete.pop_front().unwrap());
        }
        drop(obsolete);

        if to_delete.is_empty() {
            return Ok(());
        }

        for sst in to_delete {
            let path = self.db_dir.join(DbFile::new_sst(sst.number).filename());
            debug!(
                self.logger,
                "Deleting obsolete sst file: {}",
                path.to_string_lossy()
            );
            remove_file(path)?;
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
            error!(
                self.logger,
                "An error occurred during manifest update: {}",
                rs.as_ref().err().unwrap()
            );
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
                error!(
                    self.logger,
                    "An error occurred during manifest rotation: {}",
                    rs.as_ref().err().unwrap()
                );
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

    fn add_metrics(
        metric_registry: &mut MetricRegistry,
        options: &Options,
        lsm_tree: Arc<ArcSwap<LsmTree>>,
    ) {
        for level in 0..options.max_levels() {
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
        metric_registry.register_gauge(
            "sstable_count",
            DerivedGauge::new(Arc::new(move || lsm.load().levels().sst_count() as u64)),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            "stable_size",
            DerivedGauge::new(Arc::new(move || lsm.load().levels().total_bytes())),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            "memtable_size",
            DerivedGauge::new(Arc::new(move || lsm.load().memtable.size() as u64)),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            "memtable_total_size",
            DerivedGauge::new(Arc::new(move || {
                let lsm_tree = lsm.load();
                (lsm_tree.memtable.size()
                    + lsm_tree
                        .imm_memtables
                        .iter()
                        .map(|m| m.size())
                        .sum::<usize>()) as u64
            })),
        );

        let lsm_tree = lsm_tree.clone();
        metric_registry.register_gauge(
            "memtable_count",
            DerivedGauge::new(Arc::new(move || {
                (lsm_tree.load().imm_memtables.len() + 1) as u64
            })),
        );
    }
}

impl CountStatSource for StorageEngine {
    fn count_stat(&self, key: &CountStatsKey) -> Option<i64> {
        self.count_stat(key)
    }
}

#[cfg(test)]
impl StorageEngine {
    pub fn wal_return_error_on_write(&self, value: bool) {
        self.db_mutex
            .lock()
            .unwrap()
            .wal
            .return_error_on_append(value);
    }

    pub fn manifest_return_error_on_write(&self, value: bool) {
        self.db_mutex
            .lock()
            .unwrap()
            .manifest
            .return_error_on_append(value);
    }

    pub fn wal_return_error_on_rotate(&self, value: bool) {
        self.db_mutex
            .lock()
            .unwrap()
            .wal
            .return_error_on_rotate(value);
    }

    pub fn manifest_return_error_on_rotate(&self, value: bool) {
        self.db_mutex
            .lock()
            .unwrap()
            .manifest
            .return_error_on_rotate(value);
    }

    pub fn lsm_tree(&self) -> Arc<LsmTree> {
        self.lsm_tree.load_full()
    }

    pub fn fail_next_precondition_checks(&self, count: u8) {
        let _ = self
            .fail_next_precondition_checks
            .store(count, Ordering::Relaxed);
    }

    pub fn disable_auto_compaction(&self) {
        self.disable_auto_compaction.store(true, Ordering::Relaxed);
    }

    pub fn compact(self: &Arc<Self>) -> StorageResult<()> {
        let engine = self.clone();
        let callback = Callback::new_blocking(Box::new(move |result| {
            let rs = engine.update_lsm_tree_sstables(result);
            if rs.is_err() {
                engine.error_mode.store(true, Ordering::Relaxed);
            }
            rs
        }));

        let levels = self.lsm_tree.load().levels();
        let scheduled = self
            .compaction_manager
            .schedule_single_compaction(&levels, &callback);
        drop(levels); // We need to release levels to allow obsolete Arc<SSTableMetadata> to be dropped and the files to be deleted after compaction

        if scheduled {
            callback.await_blocking().map_err(StorageError::Io)
        } else {
            Ok(())
        }
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

    /// SST files found on disk (number, full path).
    sst_files: Vec<(u64, PathBuf)>,

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
    let mut sst_files = Vec::new();
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

            if db_file.file_type == FileType::SST {
                sst_files.push((db_file.number, path.clone()));
            }
        }
    }

    wal_files.sort_by_key(|(id, _)| *id);

    Ok(StartupScanResult {
        wal_files,
        obsolete_wal_files,
        sst_files,
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
        Self::copy(self.result.lock().unwrap()).unwrap_or_else(|| {
            Err(StorageError::UnexpectedError(
                "No result available".to_string(),
            ))
        })
    }
    fn done(&self, res: StorageResult<()>) {
        let mut result = self.result.lock().unwrap();
        *result = Some(res);
        self.condvar.notify_one();
    }

    fn copy(result: MutexGuard<Option<StorageResult<()>>>) -> Option<StorageResult<()>> {
        match &*result {
            Some(Ok(())) => Some(Ok(())),         // Return Ok if present
            Some(Err(e)) => Some(Err(e.clone())), // Recreate the error
            None => None,
        }
    }
}

/// Represents the type of operation that was performed on the SSTables, which is used to update the LSM tree accordingly.
/// This is the type of the result returned by the flush and compaction tasks through the callback, to update the LSM tree with the new SSTables.
pub enum SSTableOperation {
    Flush {
        log_number: u64,
        flushed: Arc<SSTableMetadata>,
        count_stats: CountStats,
    },
    Compaction {
        compaction_job: CompactionJob,
        removed_sstables: Vec<Arc<SSTableMetadata>>,
        added_sstables: Vec<Arc<SSTableMetadata>>,
        drops: Vec<Arc<DropMetadata>>,
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
    VersionConflict {
        user_key: Vec<u8>,
        reason: String,
    },
    LogCorruption {
        record_offset: u64,
        reason: String,
    },
    CollectionAlreadyExists(String),
    CollectionNotFound {
        name: String,
        id: Option<u32>,
    },
    IndexNotFound {
        collection_name: String,
        index_name: String,
        id: Option<u32>,
    },
    IndexOptionsConflict {
        collection_name: String,
        index_name: String,
        reason: String,
    },
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
            LogReplayError::Corruption {
                record_offset,
                reason,
            } => StorageError::LogCorruption {
                record_offset,
                reason,
            },
        }
    }
}

impl Clone for StorageError {
    fn clone(&self) -> Self {
        match self {
            StorageError::Io(e) => StorageError::Io(Error::new(e.kind(), e.to_string())),
            StorageError::UnexpectedError(msg) => StorageError::UnexpectedError(msg.clone()),
            StorageError::ErrorMode(msg) => StorageError::ErrorMode(msg.clone()),
            StorageError::VersionConflict { user_key, reason } => StorageError::VersionConflict {
                user_key: user_key.clone(),
                reason: reason.clone(),
            },
            StorageError::CollectionNotFound { name, id } => StorageError::CollectionNotFound {
                name: name.clone(),
                id: *id,
            },
            StorageError::IndexNotFound {
                collection_name,
                index_name,
                id,
            } => StorageError::IndexNotFound {
                collection_name: collection_name.clone(),
                index_name: index_name.clone(),
                id: *id,
            },
            StorageError::LogCorruption {
                record_offset,
                reason,
            } => StorageError::LogCorruption {
                record_offset: *record_offset,
                reason: reason.clone(),
            },
            StorageError::CollectionAlreadyExists(name) => {
                StorageError::CollectionAlreadyExists(name.clone())
            }
            StorageError::IndexOptionsConflict {
                collection_name,
                index_name,
                reason,
            } => StorageError::IndexOptionsConflict {
                collection_name: collection_name.clone(),
                index_name: index_name.clone(),
                reason: reason.clone(),
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
            StorageError::VersionConflict { user_key, reason } => write!(
                f,
                "Version conflict for user_key {:?} : {}",
                user_key, reason
            ),
            StorageError::CollectionNotFound { name, id } => {
                if let Some(col_id) = id {
                    write!(f, "Collection does not exist: {} (id: {})", name, col_id)
                } else {
                    write!(f, "Collection does not exist: {}", name)
                }
            }
            StorageError::IndexNotFound {
                collection_name,
                index_name,
                id,
            } => {
                if let Some(idx_id) = id {
                    write!(
                        f,
                        "Index does not exist: {}.{} (id: {})",
                        collection_name, index_name, idx_id
                    )
                } else {
                    write!(
                        f,
                        "Index does not exist: {}.{}",
                        collection_name, index_name
                    )
                }
            }
            StorageError::LogCorruption {
                record_offset,
                reason,
            } => {
                write!(f, "Log corruption at offset {}: {}", record_offset, reason)
            }
            StorageError::CollectionAlreadyExists(name) => {
                write!(f, "Collection already exists: {}", name)
            }
            StorageError::IndexOptionsConflict {
                collection_name,
                index_name,
                reason,
            } => {
                write!(
                    f,
                    "Index options conflict: {}.{}: {}",
                    collection_name, index_name, reason
                )
            }
        }
    }
}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use super::*;
    use crate::obs::logger::{test_instance, NoOpLogger};
    use crate::obs::metrics::{assert_counter_eq, assert_gauge_eq};
    use crate::options::storage_quantity::StorageUnit::Mebibytes;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::storage::catalog::{
        IndexDefinition, IndexDirection, IndexOptions, IndexPath, OrderedIndexField,
    };
    use crate::storage::internal_key::encode_record_key;
    use crate::storage::lsm_version::DropKind;
    use crate::storage::operation::Operation;
    use crate::storage::count_stats::{CountStats, CountStatsKey};
    use crate::storage::test_utils::{
        assert_next_entry_eq, delete_op, delete_rec, document, put_op, put_rec, user_key,
    };
    use crate::storage::write_batch::{Precondition, Preconditions};
    use bson::doc;
    use std::fs::{self, OpenOptions};
    use std::io::{ErrorKind, Seek, SeekFrom, Write};
    use std::path::Path;
    use tempfile::tempdir;

    fn write_batch(operations: Vec<Operation>) -> WriteBatch {
        WriteBatch::new(operations, CountStats::default())
    }

    fn write_batch_with_count_stats(
        operations: Vec<Operation>,
        count_stats: CountStats,
    ) -> WriteBatch {
        WriteBatch::new(operations, count_stats)
    }

    fn write_batch_with_preconditions(
        operations: Vec<Operation>,
        preconditions: Preconditions,
    ) -> WriteBatch {
        WriteBatch::new_with_preconditions(operations, preconditions, CountStats::default())
    }

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

        #[test]
        fn test_scan_db_directory_collects_sst_files() {
            let dir = tempdir().unwrap();
            let base = dir.path();

            touch_file(&base.join("000010.sst"));
            touch_file(&base.join("000020.sst"));
            touch_file(&base.join("000003.log"));

            let result = scan_db_directory(base, 0).unwrap();

            let mut sst_ids: Vec<u64> = result.sst_files.iter().map(|(id, _)| *id).collect();
            sst_ids.sort();
            assert_eq!(sst_ids, vec![10, 20]);
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

        let col = engine.create_collection_if_not_exists("test_read").unwrap();
        let idx = 0;

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(write_batch(vec![insert])).unwrap();
        }

        let snapshot = engine.last_visible_seq.load(Ordering::Relaxed);

        let _ = &engine
            .write(write_batch(vec![delete_op(col, 4)]))
            .unwrap();

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

        let updates = vec![put_op(col, 2, 2), put_op(col, 3, 2), put_op(col, 4, 2)];

        for update in updates {
            let _ = &engine.write(write_batch(vec![update])).unwrap();
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
        let actual = &engine
            .read(col, idx, &user_key(1), Some(snapshot))
            .unwrap()
            .unwrap();
        assert_eq!(actual, &put_rec(col, 1, 1, 1));

        let actual = &engine
            .read(col, idx, &user_key(2), Some(snapshot))
            .unwrap()
            .unwrap();
        assert_eq!(actual, &put_rec(col, 2, 1, 2));

        let actual = &engine
            .read(col, idx, &user_key(3), Some(snapshot))
            .unwrap()
            .unwrap();
        assert_eq!(actual, &put_rec(col, 3, 1, 3));

        let actual = &engine
            .read(col, idx, &user_key(4), Some(snapshot))
            .unwrap()
            .unwrap();
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

        let col = engine
            .create_collection_if_not_exists("test_range_scan")
            .unwrap();
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
            engine.write(write_batch(vec![insert])).unwrap();
        }

        let snapshot1 = engine.last_visible_seq.load(Ordering::Relaxed);
        assert_eq!(snapshot1, 5);

        // update 2, delete 4
        let updates = vec![
            put_op(col, 2, 2), // seq 6
            delete_op(col, 4), // seq 7
        ];
        for update in updates {
            engine.write(write_batch(vec![update])).unwrap();
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
            engine.write(write_batch(vec![update])).unwrap();
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
            engine.write(write_batch(vec![update])).unwrap();
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
        let options =
            Options::lightweight().with_file_write_buffer_size(StorageQuantity::new(4, Mebibytes));
        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options), &path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_immutable_memtables")
            .unwrap();
        let idx = 0;

        // Pause the flush manager to keep immutable memtables around.
        engine.flush_manager.pause();

        // Write enough data to trigger a memtable rotation.
        // We write four ~1MB values to fill up the 4MB memtable.
        let val_1mb_string = "a".repeat(1024 * 1024);
        let val_1mb = doc! { "v": val_1mb_string }.to_vec().unwrap();
        for i in 1..=4 {
            engine
                .write(write_batch(vec![Operation::new_put(
                    col,
                    idx,
                    user_key(i),
                    val_1mb.clone(),
                )]))
                .unwrap();
        }

        // This fifth write will trigger rotation, creating an immutable memtable.
        let val_active = doc! { "v": "active" }.to_vec().unwrap();
        engine
            .write(write_batch(vec![Operation::new_put(
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
        assert!(engine.read(col, idx, &user_key(6), None).unwrap().is_none());

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
        let val_update = doc! { "v": "updated" }.to_vec().unwrap();
        engine
            .write(write_batch(vec![Operation::new_put(
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
        let options = Arc::new(
            Options::lightweight().with_file_write_buffer_size(StorageQuantity::new(4, Mebibytes)),
        );

        let idx = 0;

        let val_1mb_string = "a".repeat(1024 * 1024);
        let val_1mb = doc! { "v": val_1mb_string }.to_vec().unwrap();

        let col = {
            let old_engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &path).unwrap();

            // Pause the flush manager to keep immutable memtables around.
            old_engine.flush_manager.pause();

            let col = old_engine
                .create_collection_if_not_exists("test_replay_with_multiple_wals")
                .unwrap();

            // Write enough data to trigger 2 memtable rotations.
            // We write four ~1MB values to fill up the 4MB memtable.
            for i in 1..=11 {
                old_engine
                    .write(write_batch(vec![Operation::new_put(
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

            col
        };

        let engine = StorageEngine::new(test_instance(), registry, options, &path).unwrap();

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
        // Each flush generates two edits (WalRotation, Flush). A new manifest starts with a 4KiB
        // block. We set the limit to 5KiB to ensure a rotation occurs within our test loop.
        let options = Arc::new(
            Options::lightweight()
                .with_max_manifest_file_size(StorageQuantity::new(5, StorageUnit::Kibibytes)),
        );

        let engine = StorageEngine::new(test_instance(), registry, options.clone(), path).unwrap();

        engine.disable_auto_compaction();

        assert_counter_eq(registry, "manifest_rewrite", 0);

        let col = engine
            .create_collection_if_not_exists("test_manifest_rotation")
            .unwrap();
        let idx = 0;

        let initial_manifest_path = Manifest::read_current_file(path).unwrap().unwrap();
        assert!(initial_manifest_path
            .to_string_lossy()
            .contains("MANIFEST-000001"));

        // Each flush generates two edits (WalRotation, Flush), consuming space in the manifest.
        // The initial manifest is ~4KiB. Each pair of edits for a flush
        // is ~40 bytes. We need to cross the 5KiB threshold, so we need ~25 flushes.
        for i in 0..25 {
            engine
                .write(write_batch(vec![put_op(col, i, i as u32)]))
                .unwrap();
            engine.flush().unwrap();
        }

        // A new manifest should have been created.
        assert_counter_eq(registry, "manifest_rewrite", 1);
        let current_manifest_path = Manifest::read_current_file(path).unwrap().unwrap();
        assert_ne!(current_manifest_path, initial_manifest_path);

        // Verify data is readable after rotation.
        for i in 0..25 {
            let (_key, val) = engine.read(col, idx, &user_key(i), None).unwrap().unwrap();
            let (_expected_key, expected_val) = put_rec(col, i, i as u32, (i + 1) as u64);
            assert_eq!(val, expected_val);
        }

        // Verify recovery after rotation.
        let db_path = path.to_path_buf();
        drop(engine);

        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        // Verify data is readable after restart.
        for i in 0..25 {
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
        // Each flush generates two edits (WalRotation, Flush). A new manifest starts with a 4KiB
        // block. We set the limit to 5KiB to ensure a rotation occurs within our test loop.
        let options = Arc::new(
            Options::lightweight()
                .with_max_manifest_file_size(StorageQuantity::new(5, StorageUnit::Kibibytes)),
        );

        let engine = StorageEngine::new(test_instance(), registry, options.clone(), path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_manifest_rotation_error")
            .unwrap();

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
                .write(write_batch(vec![put_op(col, i, i as u32)]))
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

        let col = engine
            .create_collection_if_not_exists("test_obsolete_wal_deletion")
            .unwrap();
        let idx = 0;

        // The first WAL file should be 000002.log.
        let wal_path_1 = path.join("000002.log");
        assert!(wal_path_1.exists());

        // Write some data, which goes into the first WAL.
        engine
            .write(write_batch(vec![put_op(col, 1, 1)]))
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
            .write(write_batch(vec![put_op(col, 2, 2)]))
            .unwrap();

        // Flush again.
        engine.flush().unwrap();

        // The second WAL (000003.log) should now be deleted.
        assert!(!wal_path_2.exists());

        // And a third one should exist (000005.log).
        let wal_path_3 = path.join("000005.log");
        assert!(wal_path_3.exists());

        // Data should still be readable from SSTables.
        let (_key, val1) = engine.read(col, idx, &user_key(1), None).unwrap().unwrap();
        let (_expected_key, expected_val1) = put_rec(col, 1, 1, 1);
        assert_eq!(val1, expected_val1);

        let (_key, val2) = engine.read(col, idx, &user_key(2), None).unwrap().unwrap();
        let (_expected_key, expected_val2) = put_rec(col, 2, 2, 2);
        assert_eq!(val2, expected_val2);
    }

    #[test]
    fn test_obsolete_sst_deletion_after_compaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let registry = &mut MetricRegistry::default();
        let options = Arc::new(
            Options::lightweight()
                .with_file_write_buffer_size(StorageQuantity::new(1, StorageUnit::Kibibytes))
                .with_level0_file_num_compaction_trigger(2),
        );

        let engine = StorageEngine::new(test_instance(), registry, options, &path).unwrap();
        engine.disable_auto_compaction();
        let col = engine
            .create_collection_if_not_exists("test_obsolete_sst_deletion_after_compaction")
            .unwrap();

        engine
            .write(write_batch(vec![put_op(col, 1, 1)]))
            .unwrap();
        engine.flush().unwrap();

        let l0_before = engine
            .lsm_tree()
            .levels()
            .level(0)
            .unwrap()
            .sstables()
            .iter()
            .map(|sst| sst.number)
            .collect::<Vec<_>>();
        assert_eq!(l0_before.len(), 1);
        let first_sst_path = path.join(DbFile::new_sst(l0_before[0]).filename());
        assert!(first_sst_path.exists());

        engine
            .write(write_batch(vec![put_op(col, 2, 1)]))
            .unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();

        assert!(
            !first_sst_path.exists(),
            "expected obsolete SSTable file to be deleted: {}",
            first_sst_path.to_string_lossy()
        );

        assert!(engine.read(col, 0, &user_key(1), None).unwrap().is_some());
        assert!(engine.read(col, 0, &user_key(2), None).unwrap().is_some());
    }

    #[test]
    fn test_orphaned_sst_cleanup_on_startup() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Arc::new(Options::lightweight());

        let (col, real_sst_paths) = {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            let col = engine
                .create_collection_if_not_exists("test_orphaned_sst_cleanup_on_startup")
                .unwrap();

            engine
                .write(write_batch(vec![put_op(col, 1, 1)]))
                .unwrap();
            engine.flush().unwrap();

            let live_sst_numbers = engine.lsm_tree().levels().live_sst_numbers();
            assert!(!live_sst_numbers.is_empty());

            let real_sst_paths = live_sst_numbers
                .iter()
                .map(|number| path.join(DbFile::new_sst(*number).filename()))
                .collect::<Vec<_>>();

            drop(engine);

            (col, real_sst_paths)
        };

        let orphan_1 = path.join(DbFile::new_sst(999_998).filename());
        let orphan_2 = path.join(DbFile::new_sst(999_999).filename());
        fs::File::create(&orphan_1).unwrap();
        fs::File::create(&orphan_2).unwrap();
        assert!(orphan_1.exists());
        assert!(orphan_2.exists());

        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &path,
        )
        .unwrap();

        assert!(!orphan_1.exists());
        assert!(!orphan_2.exists());

        for real_sst_path in &real_sst_paths {
            assert!(
                real_sst_path.exists(),
                "expected live SST to exist: {}",
                real_sst_path.to_string_lossy()
            );
        }

        let (_key, val) = engine_restarted
            .read(col, 0, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_, expected_val) = put_rec(col, 1, 1, 1);
        assert_eq!(val, expected_val);
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
        let col = engine
            .create_collection_if_not_exists("concurrent_writes")
            .unwrap();
        let idx = 0;

        std::thread::scope(|s| {
            for i in 0..num_threads {
                let engine_clone = engine.clone();
                s.spawn(move || {
                    for j in 0..writes_per_thread {
                        let key = i * writes_per_thread + j;
                        let value = key as u32;
                        let op = put_op(col, key, value);
                        engine_clone.write(write_batch(vec![op])).unwrap();
                        if with_concurrent_flushes && j == 100 {
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
                    let (_record_key, record_value) = engine
                        .read(col, idx, &user_key(key), None)
                        .unwrap()
                        .unwrap();
                    let expected_value = document(key, value).to_vec().unwrap();
                    assert_eq!(
                        record_value, expected_value,
                        "Record value does not match expected value for key {}",
                        key
                    );
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
        let options = Options::lightweight()
            .with_wal_bytes_per_sync(StorageQuantity::new(0, StorageUnit::Bytes)); // force syncs for each write
        let options = Arc::new(options);

        let idx = 0;

        // --- First run ---
        let col = {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();

            let col = engine
                .create_collection_if_not_exists("test_shutdown_and_restart")
                .unwrap();

            // Write some data and flush it to an SSTable.
            engine
                .write(write_batch(vec![put_op(col, 1, 1)]))
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col, 2, 1)]))
                .unwrap();
            engine.flush().unwrap();

            // Write more data that will remain in the memtable.
            engine
                .write(write_batch(vec![put_op(col, 3, 1)]))
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col, 2, 2)]))
                .unwrap(); // Update flushed key

            // Gracefully shut down the engine. This should flush the memtable.
            engine.shutdown().unwrap();

            col
        };

        // --- Second run (restart) ---
        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        // Verify all data is present and correct after restart.
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
        let (_, expected_val2) = put_rec(col, 2, 2, 4);
        assert_eq!(val2, expected_val2);

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
    fn test_wal_replay_on_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let options = Arc::new(
            Options::lightweight()
                .with_wal_bytes_per_sync(StorageQuantity::new(0, StorageUnit::Bytes)),
        ); // force syncs for each write

        let idx = 0;

        // --- First run (simulating a crash) ---
        let col = {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();

            let col = engine
                .create_collection_if_not_exists("test_wal_replay_on_restart")
                .unwrap();

            // Write some data and flush it to an SSTable.
            engine
                .write(write_batch(vec![put_op(col, 1, 1)]))
                .unwrap(); // seq 1
            engine
                .write(write_batch(vec![put_op(col, 2, 1)]))
                .unwrap(); // seq 2
            engine.flush().unwrap(); // Flushes memtable, rotates WAL.

            // Data in SSTable: {1:1, 2:1}

            // Write more data that will remain in the memtable and WAL.
            engine
                .write(write_batch(vec![put_op(col, 3, 1)]))
                .unwrap(); // seq 3
            engine
                .write(write_batch(vec![put_op(col, 2, 2)]))
                .unwrap(); // seq 4, updates a flushed key

            // Simulate a crash by just dropping the engine without calling shutdown.
            // The memtable content is lost, but the WAL records should persist.
            drop(engine);

            col
        };

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
    fn test_count_stats_persist_across_flush_and_wal_replay_on_restart() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().to_path_buf();
        let options = Arc::new(
            Options::lightweight()
                .with_wal_bytes_per_sync(StorageQuantity::new(0, StorageUnit::Bytes)),
        );

        let (collection_id, index_id) = {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &db_path,
            )
            .unwrap();

            let collection_id = engine
                .create_collection_if_not_exists("test_count_stats_restart")
                .unwrap();
            let index_name = engine
                .create_index(
                    collection_id,
                    simple_index_definition(),
                    IndexOptions {
                        name: Some("by_name".to_string()),
                    },
                )
                .unwrap();
            let index_id = engine
                .catalog()
                .get_collection_by_id(&collection_id)
                .unwrap()
                .get_index_by_name(&index_name)
                .unwrap()
                .id;

            engine
                .write(write_batch_with_count_stats(
                    vec![
                        Operation::new_put(collection_id, 0, user_key(1), document(1, 1).to_vec().unwrap()),
                        Operation::new_put(collection_id, index_id, b"alice".to_vec(), user_key(1)),
                    ],
                    CountStats::new(BTreeMap::from([
                        (CountStatsKey::Collection(collection_id), 1),
                        (
                            CountStatsKey::Index {
                                collection: collection_id,
                                index: index_id,
                            },
                            1,
                        ),
                    ])),
                ))
                .unwrap();
            engine.flush().unwrap();

            engine
                .write(write_batch_with_count_stats(
                    vec![
                        Operation::new_put(collection_id, 0, user_key(2), document(2, 1).to_vec().unwrap()),
                        Operation::new_put(collection_id, index_id, b"bob".to_vec(), user_key(2)),
                    ],
                    CountStats::new(BTreeMap::from([
                        (CountStatsKey::Collection(collection_id), 1),
                        (
                            CountStatsKey::Index {
                                collection: collection_id,
                                index: index_id,
                            },
                            1,
                        ),
                    ])),
                ))
                .unwrap();

            drop(engine);

            (collection_id, index_id)
        };

        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        assert_eq!(
            engine_restarted.count_stat(&CountStatsKey::Collection(collection_id)),
            Some(2)
        );
        assert_eq!(
            engine_restarted.count_stat(&CountStatsKey::Index {
                collection: collection_id,
                index: index_id,
            }),
            Some(2)
        );
    }

    #[test]
    fn test_count_stats_delete_delta_replayed_on_restart() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().to_path_buf();
        let options = Arc::new(
            Options::lightweight()
                .with_wal_bytes_per_sync(StorageQuantity::new(0, StorageUnit::Bytes)),
        );

        let (collection_id, index_id) = {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &db_path,
            )
            .unwrap();

            let collection_id = engine
                .create_collection_if_not_exists("test_count_stats_delete_restart")
                .unwrap();
            let index_name = engine
                .create_index(
                    collection_id,
                    simple_index_definition(),
                    IndexOptions {
                        name: Some("by_name".to_string()),
                    },
                )
                .unwrap();
            let index_id = engine
                .catalog()
                .get_collection_by_id(&collection_id)
                .unwrap()
                .get_index_by_name(&index_name)
                .unwrap()
                .id;

            engine
                .write(write_batch_with_count_stats(
                    vec![
                        Operation::new_put(collection_id, 0, user_key(1), document(1, 1).to_vec().unwrap()),
                        Operation::new_put(collection_id, index_id, b"alice".to_vec(), user_key(1)),
                    ],
                    CountStats::new(std::collections::BTreeMap::from([
                        (CountStatsKey::Collection(collection_id), 1),
                        (
                            CountStatsKey::Index {
                                collection: collection_id,
                                index: index_id,
                            },
                            1,
                        ),
                    ])),
                ))
                .unwrap();
            engine.flush().unwrap();

            engine
                .write(write_batch_with_count_stats(
                    vec![
                        Operation::new_delete(collection_id, 0, user_key(1)),
                        Operation::new_delete(collection_id, index_id, b"alice".to_vec()),
                    ],
                    CountStats::new(std::collections::BTreeMap::from([
                        (CountStatsKey::Collection(collection_id), -1),
                        (
                            CountStatsKey::Index {
                                collection: collection_id,
                                index: index_id,
                            },
                            -1,
                        ),
                    ])),
                ))
                .unwrap();

            drop(engine);

            (collection_id, index_id)
        };

        let engine_restarted = StorageEngine::new(
            test_instance(),
            &mut MetricRegistry::default(),
            options,
            &db_path,
        )
        .unwrap();

        assert_eq!(
            engine_restarted.count_stat(&CountStatsKey::Collection(collection_id)),
            None
        );
        assert_eq!(
            engine_restarted.count_stat(&CountStatsKey::Index {
                collection: collection_id,
                index: index_id,
            }),
            None
        );
    }

    #[test]
    fn test_wal_replay_with_last_log_partially_written() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let db_path = path.to_path_buf();
        let registry = &mut MetricRegistry::default();
        let options = Arc::new(
            Options::lightweight()
                .with_wal_bytes_per_sync(StorageQuantity::new(0, StorageUnit::Bytes)),
        ); // force syncs for each write

        let idx = 0;

        let wal_path;
        // --- First run (simulating a crash) ---
        let col = {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();

            let col = engine
                .create_collection_if_not_exists("test_wal_replay_with_partial_log")
                .unwrap();

            engine
                .write(write_batch(vec![put_op(col, 1, 1)]))
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col, 2, 1)]))
                .unwrap();

            wal_path = db_path.join("000002.log");
            drop(engine);
            col
        };

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
            .write(write_batch(vec![put_op(col, 3, 1)]))
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
        let options = Arc::new(
            Options::lightweight()
                .with_wal_bytes_per_sync(StorageQuantity::new(0, StorageUnit::Bytes)),
        );

        let idx = 0;

        let original_wal_path;
        // --- First run ---
        let col = {
            let engine =
                StorageEngine::new(test_instance(), registry, options.clone(), &db_path).unwrap();

            let col = engine
                .create_collection_if_not_exists("test_wal_replay_with_header_corruption")
                .unwrap();

            engine
                .write(write_batch(vec![put_op(col, 1, 1)]))
                .unwrap();
            original_wal_path = db_path.join("000002.log");
            drop(engine);
            col
        };

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
            .write(write_batch(vec![put_op(col, 2, 1)]))
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
        // Set a small buffer size to trigger memtable rotation easily.
        let options = Arc::new(
            Options::lightweight()
                .with_file_write_buffer_size(StorageQuantity::new(1, StorageUnit::Kibibytes)),
        );

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

            let col = engine
                .create_collection_if_not_exists("test_restart_fails_with_corrupted_old_wal")
                .unwrap();

            // Write enough data to trigger memtable rotation, which also rotates the WAL.
            let large_val = vec![0; 1024];
            engine
                .write(write_batch(vec![Operation::new_put(
                    col,
                    idx,
                    user_key(1),
                    large_val.clone(),
                )]))
                .unwrap();
            engine
                .write(write_batch(vec![Operation::new_put(
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

        // --- First run ---
        let col = {
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

            let col = engine
                .create_collection_if_not_exists("test_restart_with_stale_files")
                .unwrap();

            let inserts = vec![
                put_op(col, 1, 1),
                put_op(col, 2, 1),
                put_op(col, 3, 1),
                put_op(col, 4, 1),
            ];

            for insert in inserts {
                let _ = &engine.write(write_batch(vec![insert])).unwrap();
            }

            // Simulate crash
            drop(engine);

            col
        };

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
            .write(write_batch(vec![put_op(col, 1, 1)]))
            .unwrap();
        engine_restarted.flush().unwrap();

        // Flush rotates WAL to 15.log and creates sstable 16.sst. Next file number is 16.
        let new_wal_path = db_path.join("000015.log");
        assert!(new_wal_path.exists());
        let new_sst_path = db_path.join("000016.sst");
        assert!(new_sst_path.exists());

        assert_eq!(
            engine_restarted.next_file_number.load(Ordering::Relaxed),
            17
        );
    }

    #[test]
    fn test_error_mode_activation_and_rejection() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let registry = &mut MetricRegistry::default();
        let options = Arc::new(Options::lightweight());

        let engine = StorageEngine::new(test_instance(), registry, options.clone(), path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_error_mode_activation_and_rejection")
            .unwrap();

        // 1. Inject an error into the WAL write path.
        engine.wal_return_error_on_write(true);

        // 2. Perform a write operation that is expected to fail.
        let write_result = engine.write(write_batch(vec![put_op(col, 1, 1)]));
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
        let options = Arc::new(
            Options::lightweight().with_file_write_buffer_size(StorageQuantity::new(4, Mebibytes)),
        );
        let engine = StorageEngine::new(test_instance(), registry, options, &path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_wal_rotation_on_write_error")
            .unwrap();
        let idx = 0;

        engine.wal_return_error_on_rotate(true);

        // Write enough data to trigger a memtable rotation.
        // We write five ~1MB values to fill up the 4MB memtable.
        let val_1mb_string = "a".repeat(1024 * 1024);
        let val_1mb = doc! { "v": val_1mb_string }.to_vec().unwrap();
        for i in 1..=5 {
            let rs = engine.write(write_batch(vec![Operation::new_put(
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
        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options), &path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_wal_rotation_on_flush_error")
            .unwrap();

        engine.wal_return_error_on_rotate(true);

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(write_batch(vec![insert])).unwrap();
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
        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options), &path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_manifest_write_error")
            .unwrap();

        engine.manifest_return_error_on_write(true);

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(write_batch(vec![insert])).unwrap();
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
        let engine =
            StorageEngine::new(test_instance(), registry, Arc::new(options), &path).unwrap();

        let col = engine
            .create_collection_if_not_exists("test_memtable_flush_error")
            .unwrap();

        engine.lsm_tree().memtable.return_error_on_flush(true);

        let inserts = vec![
            put_op(col, 1, 1),
            put_op(col, 2, 1),
            put_op(col, 3, 1),
            put_op(col, 4, 1),
        ];

        for insert in inserts {
            let _ = &engine.write(write_batch(vec![insert])).unwrap();
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
        let expected_error_msg =
            "Error mode: The database is in error mode dues to a previous write error";

        // Test write
        let write_result_after_error = engine.write(write_batch(vec![put_op(col, 2, 2)]));
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
    fn test_create_collection() {
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

        // Create a new collection
        let col_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        assert_eq!(col_id, 10); // First user collection ID

        // Verify collection exists in catalog
        let catalog = engine.catalog();
        let collection = catalog.get_collection_by_name("test_collection");
        assert!(collection.is_some());
        assert_eq!(collection.unwrap().id, col_id);

        // Create another collection
        let col_id_2 = engine
            .create_collection("test_collection_2", CollectionOptions::default())
            .unwrap();
        assert_eq!(col_id_2, 11);

        // Verify both collections exist
        let catalog = engine.catalog();
        assert!(catalog.get_collection_by_name("test_collection").is_some());
        assert!(catalog
            .get_collection_by_name("test_collection_2")
            .is_some());
    }

    #[test]
    fn test_create_collection_already_exists() {
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

        // Create a collection
        engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();

        // Try to create the same collection again - should fail
        let result = engine.create_collection("test_collection", CollectionOptions::default());
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionAlreadyExists(_)));
        assert!(err.to_string().contains("test_collection"));
    }

    #[test]
    fn test_create_collection_if_not_exists() {
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

        // Create a collection
        let col_id_1 = engine
            .create_collection_if_not_exists("test_collection")
            .unwrap();
        assert_eq!(col_id_1, 10);

        // Call again - should return existing ID, not error
        let col_id_2 = engine
            .create_collection_if_not_exists("test_collection")
            .unwrap();
        assert_eq!(col_id_2, col_id_1);

        // Verify only one collection exists with that name
        let catalog = engine.catalog();
        assert_eq!(catalog.next_collection_id, 11);
    }

    fn simple_index_definition() -> IndexDefinition {
        IndexDefinition::Regular(vec![OrderedIndexField {
            path: IndexPath {
                components: vec!["name".to_string()],
            },
            direction: IndexDirection::Ascending,
        }])
    }

    #[test]
    fn test_create_index() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        let index_name = engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        assert_eq!(index_name, "by_name".to_string());

        let collection = engine
            .catalog()
            .get_collection_by_id(&collection_id)
            .unwrap();
        let index = collection.get_index_by_name("by_name").unwrap();
        assert_eq!(index.name(), index_name);
        assert_eq!(index.definition, simple_index_definition());
    }

    #[test]
    fn test_create_index_is_noop_when_same_name_and_equivalent_spec_exist() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();

        let index_id = engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        let second_index_id = engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        assert_eq!(second_index_id, index_id);

        let collection = engine
            .catalog()
            .get_collection_by_name("test_collection")
            .unwrap();
        assert_eq!(collection.active_indexes().len(), 1);
        assert_eq!(collection.next_index_id, 2);
    }

    #[test]
    fn test_create_index_rejects_equivalent_spec_under_different_name() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        let result = engine.create_index(
            collection_id,
            simple_index_definition(),
            IndexOptions {
                name: Some("also_by_name".to_string()),
            },
        );

        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::IndexOptionsConflict { .. }));
        assert!(err.to_string().contains("equivalent index already exists"));
    }

    #[test]
    fn test_create_index_rejects_same_name_with_different_definition() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        let different_definition = IndexDefinition::Regular(vec![OrderedIndexField {
            path: IndexPath {
                components: vec!["age".to_string()],
            },
            direction: IndexDirection::Ascending,
        }]);

        let result = engine.create_index(
            collection_id,
            different_definition,
            IndexOptions {
                name: Some("by_name".to_string()),
            },
        );

        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::IndexOptionsConflict { .. }));
        assert!(err.to_string().contains("different definition or options"));
    }

    #[test]
    fn test_drop_index() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        let index_name = engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        assert_eq!(index_name, "by_name");

        let collection = engine
            .catalog()
            .get_collection_by_id(&collection_id)
            .unwrap();
        let index_id = collection.get_index_by_name(&index_name).unwrap().id;

        assert!(engine
            .lsm_tree()
            .get_drops_before_or_at(u64::MAX)
            .is_empty());

        let drop_seq = engine.next_seq_number.load(Ordering::Relaxed);
        engine.drop_index(collection_id, index_id).unwrap();

        let collection = engine
            .catalog()
            .get_collection_by_id(&collection_id)
            .unwrap();
        assert!(collection.get_index_by_name(&index_name).is_none());

        let pending_drops = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
        assert_eq!(pending_drops.len(), 1);
        let drop_metadata = &pending_drops[0];
        assert_eq!(drop_metadata.collection, collection_id);
        assert_eq!(drop_metadata.kind, DropKind::Index(index_id));
        assert_eq!(drop_metadata.drop_sequence_number, drop_seq);
    }

    #[test]
    fn test_drop_index_not_found() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();

        let result = engine.drop_index(collection_id, 99);
        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::IndexNotFound { .. }));
        assert!(err.to_string().contains("test_collection"));
        assert!(err.to_string().contains("id: 99"));
    }

    #[test]
    fn test_drop_index_is_noop_when_index_already_dropped() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        let index_name = engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        assert_eq!(index_name, "by_name");

        let collection = engine
            .catalog()
            .get_collection_by_id(&collection_id)
            .unwrap();
        let index_id = collection.get_index_by_name(&index_name).unwrap().id;

        engine.drop_index(collection_id, index_id).unwrap();
        let drops_before = engine.lsm_tree().get_drops_before_or_at(u64::MAX);

        let result = engine.drop_index(collection_id, index_id);
        assert!(result.is_ok());

        let drops_after = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
        assert_eq!(drops_after, drops_before);
    }

    #[test]
    fn test_drop_index_is_noop_when_collection_already_dropped() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        let index_name = engine
            .create_index(
                collection_id,
                simple_index_definition(),
                IndexOptions {
                    name: Some("by_name".to_string()),
                },
            )
            .unwrap();

        assert_eq!(index_name, "by_name");

        let collection = engine
            .catalog()
            .get_collection_by_id(&collection_id)
            .unwrap();
        let index_id = collection.get_index_by_name(&index_name).unwrap().id;

        engine.drop_collection("test_collection").unwrap();
        let drops_before = engine.lsm_tree().get_drops_before_or_at(u64::MAX);

        let result = engine.drop_index(collection_id, index_id);
        assert!(result.is_ok());

        let drops_after = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
        assert_eq!(drops_after, drops_before);
    }

    #[test]
    fn test_drop_index_collection_not_found() {
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

        let result = engine.drop_index(999, 1);
        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionNotFound { .. }));
        assert!(err.to_string().contains("id: 999"));
    }

    #[test]
    fn test_create_index_collection_not_found() {
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

        let result = engine.create_index(
            999,
            simple_index_definition(),
            IndexOptions {
                name: Some("by_name".to_string()),
            },
        );

        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionNotFound { .. }));
        assert!(err.to_string().contains("id: 999"));
    }

    #[test]
    fn test_create_index_on_dropped_collection_returns_error() {
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

        let collection_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        engine.drop_collection("test_collection").unwrap();

        let result = engine.create_index(
            collection_id,
            simple_index_definition(),
            IndexOptions {
                name: Some("by_name".to_string()),
            },
        );

        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionNotFound { .. }));
        assert!(err.to_string().contains("test_collection"));
        assert!(err.to_string().contains("id: 10"));
    }

    #[test]
    fn test_drop_collection() {
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

        // Create a collection
        let col_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();

        // Write some data to it
        engine
            .write(write_batch(vec![put_op(col_id, 1, 1)]))
            .unwrap();

        // Verify data exists
        let result = engine.read(col_id, 0, &user_key(1), None).unwrap();
        assert!(result.is_some());

        // Verify no pending drops before dropping
        let lsm_tree = engine.lsm_tree();
        assert!(lsm_tree.get_drops_before_or_at(u64::MAX).is_empty());

        // Drop the collection
        let drop_seq = engine.next_seq_number.load(Ordering::Relaxed);
        engine.drop_collection("test_collection").unwrap();

        // Verify collection is no longer accessible by name
        let catalog = engine.catalog();
        assert!(catalog.get_collection_by_name("test_collection").is_none());

        // Verify DropMetadata is registered in pending_drops
        let lsm_tree = engine.lsm_tree();
        let pending_drops = lsm_tree.get_drops_before_or_at(u64::MAX);
        assert_eq!(pending_drops.len(), 1);
        let drop_metadata = &pending_drops[0];
        assert_eq!(drop_metadata.collection, col_id);
        assert_eq!(drop_metadata.kind, DropKind::Collection);
        assert_eq!(drop_metadata.drop_sequence_number, drop_seq);
    }

    #[test]
    fn test_drop_collection_not_found() {
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

        // Drop a non-existent collection - should succeed (no-op)
        let result = engine.drop_collection("non_existent");
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_to_non_existent_collection() {
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

        let name = "existing_collection";
        let col = engine.create_collection_if_not_exists(name).unwrap();
        engine
            .write(write_batch(vec![put_op(col, 1, 1)]))
            .unwrap();

        engine.drop_collection(name).unwrap();

        // Try to write to a collection that doesn't exist
        let result = engine.write(write_batch(vec![put_op(col, 1, 1)]));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionNotFound { .. }));
    }

    #[test]
    fn test_write_to_dropped_collection() {
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

        // Create and then drop a collection
        let col_id = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        engine.drop_collection("test_collection").unwrap();

        // Try to write to the dropped collection
        let result = engine.write(write_batch(vec![put_op(col_id, 1, 1)]));
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionNotFound { .. }));
    }

    #[test]
    fn test_collection_persistence_across_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Arc::new(Options::lightweight());

        // First run - create collections
        let (col_id_1, col_id_2) = {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            let col_id_1 = engine
                .create_collection("collection_1", CollectionOptions::default())
                .unwrap();
            let col_id_2 = engine
                .create_collection("collection_2", CollectionOptions::default())
                .unwrap();

            // Write data to both
            engine
                .write(write_batch(vec![put_op(col_id_1, 1, 1)]))
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col_id_2, 2, 2)]))
                .unwrap();

            engine.shutdown().unwrap();

            (col_id_1, col_id_2)
        };

        // Second run - verify collections persist
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            let catalog = engine.catalog();
            assert!(catalog.get_collection_by_name("collection_1").is_some());
            assert!(catalog.get_collection_by_name("collection_2").is_some());

            // Verify data
            let result_1 = engine.read(col_id_1, 0, &user_key(1), None).unwrap();
            assert!(result_1.is_some());

            let result_2 = engine.read(col_id_2, 0, &user_key(2), None).unwrap();
            assert!(result_2.is_some());

            // Creating a new collection should get the next ID
            let col_id_3 = engine
                .create_collection("collection_3", CollectionOptions::default())
                .unwrap();
            assert_eq!(col_id_3, 12);
        }
    }

    #[test]
    fn test_drop_collection_persistence_across_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Arc::new(Options::lightweight());

        // First run - create and drop a collection
        let (col_id, drop_seq) = {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            let col_id = engine
                .create_collection("to_drop", CollectionOptions::default())
                .unwrap();
            engine
                .create_collection("to_keep", CollectionOptions::default())
                .unwrap();

            let drop_seq = engine.next_seq_number.load(Ordering::Relaxed);
            engine.drop_collection("to_drop").unwrap();

            // Verify DropMetadata is registered before shutdown
            let pending_drops = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
            assert_eq!(pending_drops.len(), 1);
            assert_eq!(pending_drops[0].collection, col_id);
            assert_eq!(pending_drops[0].drop_sequence_number, drop_seq);

            engine.shutdown().unwrap();

            (col_id, drop_seq)
        };

        // Second run - verify drop persisted
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            let catalog = engine.catalog();
            assert!(catalog.get_collection_by_name("to_drop").is_none());
            assert!(catalog.get_collection_by_name("to_keep").is_some());

            // Verify DropMetadata persisted across restart
            let pending_drops = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
            assert_eq!(pending_drops.len(), 1);
            assert_eq!(pending_drops[0].collection, col_id);
            assert_eq!(pending_drops[0].drop_sequence_number, drop_seq);

            // Writing to dropped collection should fail
            let result = engine.write(write_batch(vec![put_op(col_id, 1, 1)]));
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_drop_and_recreate_collection_data_isolation() {
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

        // Create a collection and write data to it
        let col_id_1 = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        assert_eq!(col_id_1, 10);

        engine
            .write(write_batch(vec![put_op(col_id_1, 1, 100)]))
            .unwrap();
        engine
            .write(write_batch(vec![put_op(col_id_1, 2, 200)]))
            .unwrap();
        engine
            .write(write_batch(vec![put_op(col_id_1, 3, 300)]))
            .unwrap();

        // Verify data exists
        let (_, val1) = engine
            .read(col_id_1, 0, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_, expected_val1) = put_rec(col_id_1, 1, 100, 1);
        assert_eq!(val1, expected_val1);

        // Drop the collection
        engine.drop_collection("test_collection").unwrap();

        // Recreate the collection with the same name
        let col_id_2 = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();
        assert_eq!(col_id_2, 11); // Should get a new ID

        // The old data should NOT be visible when querying with the new collection ID
        assert!(engine
            .read(col_id_2, 0, &user_key(1), None)
            .unwrap()
            .is_none());
        assert!(engine
            .read(col_id_2, 0, &user_key(2), None)
            .unwrap()
            .is_none());
        assert!(engine
            .read(col_id_2, 0, &user_key(3), None)
            .unwrap()
            .is_none());

        // Write new data to the recreated collection
        engine
            .write(write_batch(vec![put_op(col_id_2, 1, 999)]))
            .unwrap();

        // The new data should be visible
        let (_, new_val) = engine
            .read(col_id_2, 0, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_, expected_new_val) = put_rec(col_id_2, 1, 999, 5); // seq 5 after create(1), 3 writes, drop(doesn't increment)
        assert_eq!(new_val, expected_new_val);

        // Range scan on new collection should only return the new data
        let results: Vec<_> = engine
            .range_scan(col_id_2, 0, &(..), None, Direction::Forward)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_drop_and_recreate_collection_with_flush() {
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

        // Create a collection and write data to it
        let col_id_1 = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();

        engine
            .write(write_batch(vec![put_op(col_id_1, 1, 100)]))
            .unwrap();
        engine
            .write(write_batch(vec![put_op(col_id_1, 2, 200)]))
            .unwrap();

        // Flush data to SSTable
        engine.flush().unwrap();

        // Write more data (in memtable)
        engine
            .write(write_batch(vec![put_op(col_id_1, 3, 300)]))
            .unwrap();

        // Verify all data exists
        assert!(engine
            .read(col_id_1, 0, &user_key(1), None)
            .unwrap()
            .is_some());
        assert!(engine
            .read(col_id_1, 0, &user_key(2), None)
            .unwrap()
            .is_some());
        assert!(engine
            .read(col_id_1, 0, &user_key(3), None)
            .unwrap()
            .is_some());

        // Verify no pending drops before dropping
        assert!(engine
            .lsm_tree()
            .get_drops_before_or_at(u64::MAX)
            .is_empty());

        // Drop the collection
        let drop_seq = engine.next_seq_number.load(Ordering::Relaxed);
        engine.drop_collection("test_collection").unwrap();

        // Verify DropMetadata is registered
        let pending_drops = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
        assert_eq!(pending_drops.len(), 1);
        assert_eq!(pending_drops[0].collection, col_id_1);
        assert_eq!(pending_drops[0].drop_sequence_number, drop_seq);

        // Recreate the collection
        let col_id_2 = engine
            .create_collection("test_collection", CollectionOptions::default())
            .unwrap();

        // Old data (both from SSTable and memtable) should NOT be visible with new collection ID
        assert!(engine
            .read(col_id_2, 0, &user_key(1), None)
            .unwrap()
            .is_none());
        assert!(engine
            .read(col_id_2, 0, &user_key(2), None)
            .unwrap()
            .is_none());
        assert!(engine
            .read(col_id_2, 0, &user_key(3), None)
            .unwrap()
            .is_none());

        // Write and flush new data
        engine
            .write(write_batch(vec![put_op(col_id_2, 1, 999)]))
            .unwrap();
        engine.flush().unwrap();

        // After flush, DropMetadata should be removed since the SSTable max_seq > drop_seq
        // The flush creates an SSTable with sequence numbers up to the current sequence,
        // which is after the drop_seq, so the drop should be cleared from pending_drops
        let pending_drops_after_flush = engine.lsm_tree().get_drops_before_or_at(u64::MAX);
        assert!(
            pending_drops_after_flush.is_empty(),
            "Expected pending_drops to be empty after flush, got {:?}",
            pending_drops_after_flush
        );

        // New data should be visible
        let (_, new_val) = engine
            .read(col_id_2, 0, &user_key(1), None)
            .unwrap()
            .unwrap();
        let (_, expected_new_val) = put_rec(col_id_2, 1, 999, 4);
        assert_eq!(new_val, expected_new_val);

        // Range scan should only return data from the new collection
        let results: Vec<_> = engine
            .range_scan(col_id_2, 0, &(..), None, Direction::Forward)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_drop_and_recreate_collection_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Arc::new(Options::lightweight());

        let col_id_2;
        // First run - create, populate, drop, recreate, repopulate
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            // Create first collection
            let col_id_1 = engine
                .create_collection("test_collection", CollectionOptions::default())
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col_id_1, 1, 100)]))
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col_id_1, 2, 200)]))
                .unwrap();
            engine.flush().unwrap();

            // Drop and recreate
            engine.drop_collection("test_collection").unwrap();
            col_id_2 = engine
                .create_collection("test_collection", CollectionOptions::default())
                .unwrap();

            // Write different data to recreated collection
            engine
                .write(write_batch(vec![put_op(col_id_2, 5, 500)]))
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col_id_2, 6, 600)]))
                .unwrap();

            engine.shutdown().unwrap();
        }

        // Second run - verify only new data is visible after restart
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            // Collection should exist with the new ID
            let catalog = engine.catalog();
            let collection = catalog.get_collection_by_name("test_collection").unwrap();
            assert_eq!(collection.id, col_id_2);

            // Old data (keys 1, 2) should NOT be visible
            assert!(engine
                .read(col_id_2, 0, &user_key(1), None)
                .unwrap()
                .is_none());
            assert!(engine
                .read(col_id_2, 0, &user_key(2), None)
                .unwrap()
                .is_none());

            // New data (keys 5, 6) should be visible
            assert!(engine
                .read(col_id_2, 0, &user_key(5), None)
                .unwrap()
                .is_some());
            assert!(engine
                .read(col_id_2, 0, &user_key(6), None)
                .unwrap()
                .is_some());

            // Range scan should only return the new data
            let results: Vec<_> = engine
                .range_scan(col_id_2, 0, &(..), None, Direction::Forward)
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(results.len(), 2);
        }
    }

    #[test]
    fn test_rename_collection() {
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

        // Create a collection and write data
        let col_id = engine
            .create_collection("original_name", CollectionOptions::default())
            .unwrap();
        engine
            .write(write_batch(vec![put_op(col_id, 1, 100)]))
            .unwrap();

        // Rename the collection
        engine
            .rename_collection("original_name", "new_name")
            .unwrap();

        // Verify old name no longer works
        let catalog = engine.catalog();
        assert!(catalog.get_collection_by_name("original_name").is_none());

        // Verify new name works and has the same ID
        let collection = catalog.get_collection_by_name("new_name").unwrap();
        assert_eq!(collection.id, col_id);

        // Verify data is still accessible with the same collection ID
        let (_, val) = engine.read(col_id, 0, &user_key(1), None).unwrap().unwrap();
        let (_, expected_val) = put_rec(col_id, 1, 100, 1);
        assert_eq!(val, expected_val);
    }

    #[test]
    fn test_rename_collection_not_found() {
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

        let result = engine.rename_collection("non_existent", "new_name");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(
            err,
            StorageError::CollectionNotFound { name: _, id: _ }
        ));
        assert!(err.to_string().contains("non_existent"));
    }

    #[test]
    fn test_rename_collection_target_exists() {
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

        // Create two collections
        engine
            .create_collection("collection_a", CollectionOptions::default())
            .unwrap();
        engine
            .create_collection("collection_b", CollectionOptions::default())
            .unwrap();

        // Try to rename collection_a to collection_b - should fail
        let result = engine.rename_collection("collection_a", "collection_b");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, StorageError::CollectionAlreadyExists(_)));
        assert!(err.to_string().contains("collection_b"));
    }

    #[test]
    fn test_rename_collection_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Arc::new(Options::lightweight());

        let col_id;
        // First run - create and rename
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            col_id = engine
                .create_collection("original", CollectionOptions::default())
                .unwrap();
            engine
                .write(write_batch(vec![put_op(col_id, 1, 100)]))
                .unwrap();
            engine.rename_collection("original", "renamed").unwrap();
            engine.shutdown().unwrap();
        }

        // Second run - verify rename persisted
        {
            let engine = StorageEngine::new(
                test_instance(),
                &mut MetricRegistry::default(),
                options.clone(),
                &path,
            )
            .unwrap();

            let catalog = engine.catalog();
            assert!(catalog.get_collection_by_name("original").is_none());
            assert!(catalog.get_collection_by_name("renamed").is_some());
            assert_eq!(
                catalog.get_collection_by_name("renamed").unwrap().id,
                col_id
            );

            // Data still accessible
            let (_, val) = engine.read(col_id, 0, &user_key(1), None).unwrap().unwrap();
            let (_, expected_val) = put_rec(col_id, 1, 100, 1);
            assert_eq!(val, expected_val);
        }
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

        let col = engine
            .create_collection_if_not_exists("test_optimistic_locking")
            .unwrap();
        let idx = 0;

        // 1. Write key1.
        engine
            .write(write_batch(vec![put_op(col, 1, 1)]))
            .unwrap();

        // 2. Take a snapshot after key1 is written.
        let snapshot1 = engine.last_visible_sequence();

        // 3. Write key2.
        engine
            .write(write_batch(vec![put_op(col, 2, 1)]))
            .unwrap();

        // 4. Try to write key2 again with a precondition based on the old snapshot.
        // This should fail because key2 was created *after* snapshot1.
        let precondition = Precondition::VersionMatch {
            collection: col,
            index: idx,
            user_key: user_key(2),
        };
        let preconditions = Preconditions::new(snapshot1, vec![precondition]);
        let batch = write_batch_with_preconditions(vec![put_op(col, 2, 2)], preconditions);
        let result = engine.write(batch);

        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err
            .to_string()
            .contains("Optimistic locking failed: key for collection 10 index 0 user_key"));

        // 5. Take a new snapshot and try to write a new key. This should succeed.
        let snapshot2 = engine.last_visible_sequence();
        let precondition_ok = Precondition::VersionMatch {
            collection: col,
            index: idx,
            user_key: user_key(3),
        };
        let preconditions_ok = Preconditions::new(snapshot2, vec![precondition_ok]);
        let batch_ok =
            write_batch_with_preconditions(vec![put_op(col, 3, 1)], preconditions_ok);
        engine.write(batch_ok).unwrap();

        // 6. Try to write an existing key (key1) again. This should fail because the key
        // already exists, and the `read_since` check will find it.
        let precondition_fail = Precondition::VersionMatch {
            collection: col,
            index: idx,
            user_key: user_key(1),
        };
        let preconditions_fail = Preconditions::new(0, vec![precondition_fail]);
        let batch_fail =
            write_batch_with_preconditions(vec![put_op(col, 1, 2)], preconditions_fail);
        let result_fail = engine.write(batch_fail);
        assert!(result_fail.is_err());
        let err_fail = result_fail.err().unwrap();
        assert!(err_fail
            .to_string()
            .contains("Optimistic locking failed: key for collection 10 index 0 user_key"));
    }
}
