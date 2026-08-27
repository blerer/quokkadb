use crate::io::{mark_file_as_corrupted, sync_dir, truncate_file};
use crate::obs::metrics::{self, DerivedGauge, MetricRegistry};
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
use crate::storage::snapshot_manager::{Snapshot, SnapshotManager};
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::wal::WriteAheadLog;
use crate::storage::write_batch::{Precondition, Preconditions, WriteBatch};
use crate::storage::Direction;
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
    db_dir: PathBuf,
    options: Arc<Options>,
    queue: Mutex<VecDeque<Arc<Writer>>>,
    db_mutex: Mutex<WalAndManifest>,
    lsm_tree: Arc<ArcSwap<LsmTree>>,
    next_file_number: Arc<AtomicU64>, // The counter used to create the file ids
    next_seq_number: AtomicU64,       // The counter used to create sequence numbers
    last_visible_seq: AtomicU64,
    snapshot_manager: Arc<SnapshotManager>,
    sst_cache: Arc<SSTableCache>,
    flush_manager: FlushManager,
    compaction_manager: CompactionManager,
    async_callback: OnceLock<Arc<Callback<Result<SSTableOperation>>>>,
    obsolete_sstables: Mutex<VecDeque<Arc<SSTableMetadata>>>,
    error_mode: AtomicBool,
    disable_auto_compaction: AtomicBool,
    #[cfg(test)]
    fail_next_precondition_checks: AtomicU8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedIndex {
    pub id: u32,
    pub name: String,
}

impl StorageEngine {
    pub fn new(
        metric_registry: &mut MetricRegistry,
        options: Arc<Options>,
        db_dir: &Path,
    ) -> StorageResult<Arc<Self>> {
        let sst_cache = Arc::new(SSTableCache::new(metric_registry, &options));

        tracing::debug!(path = %db_dir.display(), "starting storage engine");

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

            let mut manifest = Manifest::load_from(metric_registry, &options, manifest_path)?;

            let next_file_number = manifest_state.lsm.next_file_number;

            let mut lsm_tree = LsmTree::from(manifest_state);

            // If a file with a higher number that the next_file number has been detected we need to update
            // the Lsm tree in-memory and on-disk (MANIFEST file)
            let next_file_number = Arc::new(AtomicU64::new(
                if next_file_number < scan_results.next_file_number {
                    tracing::debug!(
                        next_file_number = scan_results.next_file_number,
                        "files with higher numbers detected during restart"
                    );

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
                tracing::debug!(path = %wal_path.display(), "replaying operations from WAL");

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
                                                tracing::warn!(
                                                    path = %wal_path.display(),
                                                    record_offset,
                                                    reason = %reason,
                                                    "corruption detected in WAL record; truncating file"
                                                );
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
                        tracing::debug!(
                            operation_count = count,
                            path = %wal_path.display(),
                            "replayed operations from WAL"
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
                                    tracing::error!(error = %e);
                                    return Err(e.into());
                                }
                                LogReplayError::Corruption {
                                    record_offset: _,
                                    reason,
                                } => {
                                    mark_file_as_corrupted(wal_path)?;
                                    tracing::error!(
                                        path = %wal_path.display(),
                                        reason = %reason,
                                        "corruption detected in WAL header; marked file as corrupted and starting a new WAL"
                                    );
                                }
                            }
                        } else {
                            tracing::error!(error = %e);
                            return Err(e.into());
                        }
                    }
                }
            }

            // If the last wal file can be reused, either because it was fine or because it has been
            // corrected by truncation, we will reuse it. If not, it should have been marked as corrupted,
            // and we need to create a new one and update the Lsm tree.
            let wal = if let Some(wal_path) = reusable_wal {
                WriteAheadLog::load_from(metric_registry, &options, &wal_path, rotated_log_files)?
            } else {
                let log_number = next_file_number.fetch_add(1, Ordering::Relaxed);

                tracing::debug!(
                    log_number,
                    "latest WAL was corrupted; starting from a clean WAL"
                );

                let wal = WriteAheadLog::new_after_corruption(
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
                        &options,
                        &db_dir,
                        &mut manifest,
                        &mut lsm_tree,
                        &next_file_number,
                    )?;
                } else {
                    tracing::debug!(
                        log_number = lsm_tree.imm_memtables[0].log_number,
                        "ignoring empty memtable"
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
                    tracing::debug!(path = %path.display(), "deleting orphaned SST file at startup");
                    fs::remove_file(path)?;
                    deleted_orphaned_ssts.push(path.clone());
                }
            }
            if !deleted_orphaned_ssts.is_empty() {
                sync_dir(db_dir)?;
            }

            let flush_manager =
                FlushManager::new(metric_registry, options.clone(), db_dir, sst_cache.clone())?;

            let compaction_manager = CompactionManager::new(
                metric_registry,
                options.clone(),
                &db_dir,
                sst_cache.clone(),
                next_file_number.clone(),
            )?;

            let lsm_tree = Arc::new(ArcSwap::new(Arc::new(lsm_tree)));
            Self::add_metrics(metric_registry, &options, lsm_tree.clone());

            let engine = Arc::new(StorageEngine {
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                db_mutex: Mutex::new(WalAndManifest { wal, manifest }),
                lsm_tree,
                next_file_number,
                next_seq_number: AtomicU64::new(last_seq_nbr + 1),
                last_visible_seq: AtomicU64::new(last_seq_nbr),
                sst_cache,
                snapshot_manager: Arc::new(SnapshotManager::new()),
                flush_manager,
                compaction_manager,
                async_callback: OnceLock::new(),
                obsolete_sstables: Mutex::new(VecDeque::new()),
                error_mode: AtomicBool::new(false),
                disable_auto_compaction: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_precondition_checks: AtomicU8::new(0),
            });

            tracing::debug!("storage engine started");

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
                metric_registry,
                &options,
                db_dir,
                manifest_number,
                &snapshot,
            )?;
            let wal = WriteAheadLog::new(metric_registry, &options, db_dir, log_number)?;
            let flush_manager =
                FlushManager::new(metric_registry, options.clone(), db_dir, sst_cache.clone())?;

            let compaction_manager = CompactionManager::new(
                metric_registry,
                options.clone(),
                &db_dir,
                sst_cache.clone(),
                next_file_number.clone(),
            )?;

            let lsm_tree = Arc::new(ArcSwap::new(lsm_tree));
            Self::add_metrics(metric_registry, &options, lsm_tree.clone());

            tracing::debug!("storage engine started");

            Ok(Arc::new(StorageEngine {
                db_dir: db_dir.to_path_buf(),
                options,
                queue: Mutex::new(VecDeque::new()), // TODO: limit unbounded queue
                db_mutex: Mutex::new(WalAndManifest { wal, manifest }),
                lsm_tree,
                next_file_number,
                next_seq_number,
                last_visible_seq: AtomicU64::new(0),
                sst_cache,
                snapshot_manager: Arc::new(SnapshotManager::new()),
                flush_manager,
                compaction_manager,
                async_callback: OnceLock::new(),
                obsolete_sstables: Mutex::new(VecDeque::new()),
                error_mode: AtomicBool::new(false),
                disable_auto_compaction: AtomicBool::new(false),
                #[cfg(test)]
                fail_next_precondition_checks: AtomicU8::new(0),
            }))
        }
    }

    fn flush_replayed_data(
        options: &Arc<Options>,
        db_dir: &&Path,
        manifest: &mut Manifest,
        lsm_tree: &mut LsmTree,
        next_file_number: &AtomicU64,
    ) -> StorageResult<LsmTree> {
        tracing::debug!(
            log_number = lsm_tree.imm_memtables[0].log_number,
            "flushing replayed data"
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
            tracing::debug!(collection = %name, id, "creating collection");
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

    pub fn drop_collection(self: &Arc<Self>, name: &str) -> StorageResult<Option<u32>> {
        self.check_error_mode()?;
        let id = self.catalog().get_collection_by_name(name).map(|c| c.id);

        if id.is_none() {
            // Collection does not exist, nothing to do
            return Ok(None);
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
        tracing::debug!(collection = %name, id, "dropping collection");
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(Some(id))
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
        tracing::debug!(
            old_name = %old_name,
            new_name = %new_name,
            id,
            "renaming collection"
        );
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(())
    }

    pub fn create_index(
        self: &Arc<Self>,
        collection_id: u32,
        definition: IndexDefinition,
        options: IndexOptions,
    ) -> StorageResult<CreatedIndex> {
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
                return Ok(CreatedIndex {
                    id: existing_index.id,
                    name: existing_index.name(),
                });
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
        tracing::debug!(
            collection = %collection.name,
            index = %resolved_name,
            id = index_id,
            "creating index"
        );
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(CreatedIndex {
            id: index_id,
            name: resolved_name,
        })
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
        tracing::debug!(
            collection = %collection.name,
            index = %index.name(),
            id = index.id,
            "dropping index"
        );
        let _lsm_tree = self.append_edit(&lsm_tree, &mut wal_and_manifest, &edit)?;
        Ok(())
    }

    /// Returns the latest sequence number that is visible to ordinary reads.
    ///
    /// This is a visibility fence, not a leased snapshot. Callers may use it to
    /// perform an internally consistent read operation, but the storage engine does
    /// not currently retain historical state indefinitely for arbitrary past
    /// sequence numbers. In particular, compaction may later reclaim older history
    /// that is no longer protected by a real snapshot lease.
    fn last_visible_sequence(&self) -> u64 {
        self.last_visible_seq.load(Ordering::Relaxed)
    }

    /// Acquires a real snapshot lease for the current visible sequence.
    ///
    /// The returned lease remains active until the last clone is dropped. Future
    /// compaction logic can use the oldest active lease to decide which history
    /// must be retained.
    pub fn acquire_snapshot(&self) -> Snapshot {
        self.snapshot_manager.acquire(self.last_visible_sequence())
    }

    fn clamp_snapshot_sequence(&self, sequence: u64) -> u64 {
        sequence.min(self.last_visible_sequence())
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        let lsm_tree = self.lsm_tree.load();
        lsm_tree.catalog().clone()
    }

    pub fn count_stat(&self, key: &CountStatsKey) -> Option<i64> {
        let lsm_tree = self.lsm_tree.load();
        let count = lsm_tree.count_stat(key);

        assert!(
            count.is_none_or(|count| count >= 0),
            "Count stat for key {:?} was negative: {:?}",
            key,
            count
        );

        count
    }

    pub fn write(self: &Arc<Self>, batch: WriteBatch, sync: bool) -> StorageResult<()> {
        self.check_error_mode()?;

        let writer = Arc::new(Writer::new(batch, sync));

        // Add the writer to the queue
        self.queue.lock().unwrap().push_back(writer.clone());

        // If no leader is active, this thread becomes leader
        if self.is_leader(&writer) {
            tracing::debug!(thread_id = ?std::thread::current().id(), "thread became write leader");
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

        tracing::debug!(
            thread_id = ?std::thread::current().id(),
            writer_count = writers.len(),
            "performing writes"
        );
        tracing::trace!(writer_count = writers.len(), "write started");

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
            tracing::trace!("write finished without applying batches due to preconditions");
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
            tracing::trace!(
                seq,
                memtable = lsm_tree.memtable.log_number,
                batch_size = writer.batch().len(),
                "memtable write started"
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
            tracing::trace!(
                seq,
                memtable = lsm_tree.memtable.log_number,
                "memtable write finished"
            );
        }

        drop(wal_and_manifest); // release the lock as soon as possible

        for (writer, result) in with_results {
            writer.done(result);
        }
        tracing::trace!("write finished");
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
        if preconditions.since() + 1 == seq {
            return Ok(());
        }

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

    /// Reads a single key using a real snapshot lease.
    pub fn read_at_snapshot(
        &self,
        collection: u32,
        index: u32,
        user_key: &[u8],
        snapshot: &Snapshot,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.read_internal(
            collection,
            index,
            user_key,
            self.clamp_snapshot_sequence(snapshot.sequence()),
            None,
        )
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

    /// Scans a key range using a real snapshot lease.
    pub fn range_scan_at_snapshot<R>(
        &self,
        collection: u32,
        index: u32,
        user_key_range: &R,
        snapshot: &Snapshot,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let snapshot_sequence = self.clamp_snapshot_sequence(snapshot.sequence());
        let lsm_tree = self.lsm_tree.load_full();

        let iter_with_lifetime = lsm_tree.range_scan(
            self.sst_cache.clone(),
            &self.db_dir,
            collection,
            index,
            user_key_range,
            snapshot_sequence,
            direction,
        )?;

        let static_iterator = unsafe {
            std::mem::transmute::<
                Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>,
                Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'static>,
            >(iter_with_lifetime)
        };

        let result_iterator = RangeScanIterator {
            _lsm_tree: lsm_tree,
            _snapshot: Some(snapshot.clone()),
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
        let mut should_sync = false;

        for writer in writers {
            let batch = writer.batch();
            wal_and_manifest.wal.append(seq, batch)?;
            with_sequence.push((writer.clone(), seq));
            should_sync |= writer.sync();

            seq += 1;
        }

        wal_and_manifest.wal.finish_append_group(should_sync)?;
        Ok(with_sequence)
    }

    pub fn shutdown(self: &Arc<Self>) -> StorageResult<()> {
        // This code should be only called once when the database instance is dropped.
        self.disable_auto_compaction.store(true, Ordering::Relaxed);
        tracing::debug!("shutting down storage engine");
        self.flush()?;
        self.compaction_manager.shutdown();
        Ok(())
    }

    pub fn flush(self: &Arc<Self>) -> StorageResult<()> {
        tracing::debug!("requested flush started");

        self.check_error_mode()?;

        self.perform_wal_and_memtable_rotation(true)?;
        tracing::debug!("requested flush completed");
        Ok(())
    }

    fn wait_for_pending_flushes(self: &Arc<Self>) -> Result<()> {
        tracing::trace!("flush sync started");
        let callback = Callback::new_blocking(Box::new(|result| result));
        self.flush_manager.enqueue(FlushTask::Sync {
            callback: callback.clone(),
        })?;
        callback.await_blocking()?;
        tracing::trace!("flush sync finished");
        Ok(())
    }

    fn perform_wal_and_memtable_rotation_if_needed(self: &Arc<Self>) {
        let write_buffer_size = self.options.file_write_buffer_size().to_bytes();
        let memtable_size = self.lsm_tree.load().memtable.size();
        if memtable_size >= write_buffer_size {
            tracing::debug!(
                memtable_size,
                write_buffer_size,
                "memtable size exceeded write buffer"
            );
            match self.perform_wal_and_memtable_rotation(false) {
                Err(error) => {
                    tracing::error!(error = %error, "WAL and memtable rotation failed");
                }
                Ok(_) => (),
            }
        }
    }

    fn get_async_callback(self: &Arc<Self>) -> &Arc<Callback<Result<SSTableOperation>>> {
        self.async_callback.get_or_init(|| {
            let engine = self.clone();
            Callback::new_async(move |result| engine.update_lsm_tree_sstables(result))
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
                tracing::error!(error = %rs.as_ref().err().unwrap(), "WAL rotation failed");
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
                            tracing::trace!(
                                removed_sstables = ?removed_sstables
                                    .iter()
                                    .map(|sst| sst.number)
                                    .collect::<Vec<u64>>(),
                                "marking SSTables as obsolete"
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
        if self.disable_auto_compaction.load(Ordering::Relaxed) {
            return;
        }

        let _wal_and_manifest = self.db_mutex.lock().unwrap();
        let levels = self.lsm_tree.load_full().levels();
        self.compaction_manager.schedule_compaction_if_needed(
            &levels,
            self.snapshot_manager.oldest_active_snapshot(),
            self.get_async_callback(),
        );
    }

    fn delete_obsolete_log_files(self: &Arc<Self>, obsolete_log_files: Vec<PathBuf>) -> Result<()> {
        for obsolete in obsolete_log_files {
            tracing::debug!(path = %obsolete.display(), "deleting obsolete log file");
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
            tracing::debug!(path = %path.display(), "deleting obsolete SST file");
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
            tracing::error!(error = %rs.as_ref().err().unwrap(), "manifest update failed");
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
                tracing::error!(error = %rs.as_ref().err().unwrap(), "manifest rotation failed");
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
        tracing::error!(error = %error, "write failed");
        for writer in writers {
            writer.done(Err(error.clone()));
        }
        tracing::trace!("write finished");
    }

    fn add_metrics(
        metric_registry: &mut MetricRegistry,
        options: &Options,
        lsm_tree: Arc<ArcSwap<LsmTree>>,
    ) {
        for level in 0..options.max_levels() {
            let lsm = lsm_tree.clone();
            metric_registry.register_gauge(
                metrics::names::storage::sstable_count_level(level),
                DerivedGauge::new(Arc::new(move || {
                    let levels = lsm.load().levels();
                    let may_be_level = levels.level(level as usize);
                    may_be_level.map_or(0, |l| l.sst_count() as u64)
                })),
            );

            let lsm = lsm_tree.clone();
            metric_registry.register_gauge(
                metrics::names::storage::sstable_size_level(level),
                DerivedGauge::new(Arc::new(move || {
                    let levels = lsm.load().levels();
                    let may_be_level = levels.level(level as usize);
                    may_be_level.map_or(0, |l| l.total_bytes())
                })),
            );
        }

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            metrics::names::storage::SSTABLE_COUNT,
            DerivedGauge::new(Arc::new(move || lsm.load().levels().sst_count() as u64)),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            metrics::names::storage::TOTAL_SSTABLE_SIZE,
            DerivedGauge::new(Arc::new(move || lsm.load().levels().total_bytes())),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            metrics::names::storage::MEMTABLE_SIZE,
            DerivedGauge::new(Arc::new(move || lsm.load().memtable.size() as u64)),
        );

        let lsm = lsm_tree.clone();
        metric_registry.register_gauge(
            metrics::names::storage::MEMTABLE_TOTAL_SIZE,
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
            metrics::names::storage::MEMTABLE_COUNT,
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
}

#[cfg(any(test, feature = "internal-testing"))]
impl StorageEngine {
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
        let oldest_active_snapshot = self.snapshot_manager.oldest_active_snapshot();
        let scheduled = self.compaction_manager.schedule_single_compaction(
            &levels,
            oldest_active_snapshot,
            &callback,
        );
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
    sync: bool,
    result: Mutex<Option<StorageResult<()>>>,
    condvar: Condvar,
}

impl Writer {
    fn new(batch: WriteBatch, sync: bool) -> Writer {
        Writer {
            write_batch: batch,
            sync,
            result: Mutex::new(None),
            condvar: Condvar::new(),
        }
    }

    pub fn batch(&self) -> &WriteBatch {
        &self.write_batch
    }

    pub fn sync(&self) -> bool {
        self.sync
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
    _snapshot: Option<Snapshot>,
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
mod tests;
