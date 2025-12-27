use crate::storage::catalog::Catalog;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_record_key, encode_record_key_range};
use crate::storage::manifest_state::{ManifestEdit, ManifestState};
use crate::storage::memtable::Memtable;
use crate::storage::sstable::sstable_cache::SSTableCache;

use crate::storage::internal_key::encode_internal_key_range;

use crate::storage::iterators::{ForwardIterator, MergeIterator, ReverseIterator};
use crate::storage::lsm_version::Levels;
use crate::storage::Direction;
use std::collections::VecDeque;
use std::io::Result;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

/// Represents the state of the LSM tree, including the manifest, active memtable,
/// and immutable memtables waiting to be flushed to disk.
pub struct LsmTree {
    pub manifest: Arc<ManifestState>,
    pub memtable: Arc<Memtable>,
    pub imm_memtables: Arc<VecDeque<Arc<Memtable>>>,
}

impl LsmTree {
    pub fn new(current_log_number: u64, next_file_number: u64, next_seq: u64) -> Self {
        LsmTree {
            manifest: Arc::new(ManifestState::new(current_log_number, next_file_number)),
            memtable: Arc::new(Memtable::new(current_log_number, next_seq)),
            imm_memtables: Arc::new(VecDeque::new()),
        }
    }

    pub fn from(manifest_state: ManifestState) -> Self {
        let oldest_log_number = manifest_state.lsm.oldest_log_number;
        let next_seq = manifest_state.lsm.last_sequence_number + 1;
        LsmTree {
            manifest: Arc::new(manifest_state),
            memtable: Arc::new(Memtable::new(oldest_log_number, next_seq)),
            imm_memtables: Arc::new(VecDeque::new()),
        }
    }

    pub fn apply(&self, edit: &ManifestEdit) -> Self {
        match edit {
            ManifestEdit::WalRotation { log_number, next_seq } => {
                // The log was rotated because the memtable was considered as full. The memtable
                // should be considered as immutable and placed in the queue waiting for being
                // flushed to disk. A new memtable should be created and associated to the new log.
                let mut imm_memtables: VecDeque<Arc<Memtable>> =
                    self.imm_memtables.iter().cloned().collect();
                imm_memtables.push_back(self.memtable.clone());

                LsmTree {
                    manifest: Arc::new(self.manifest.apply(edit)),
                    memtable: Arc::new(Memtable::new(*log_number, *next_seq)),
                    imm_memtables: Arc::new(imm_memtables),
                }
            }
            ManifestEdit::Flush {
                oldest_log_number: _oldest_log_number,
                sst: _sst,
            } => {
                let mut imm_memtables: VecDeque<Arc<Memtable>> =
                    self.imm_memtables.iter().cloned().collect();
                let _flushed = imm_memtables.pop_front();

                LsmTree {
                    manifest: Arc::new(self.manifest.apply(edit)),
                    memtable: self.memtable.clone(),
                    imm_memtables: Arc::new(imm_memtables),
                }
            }
            ManifestEdit::IgnoringEmptyMemtable {
                oldest_log_number: _oldest_log_number,
            } => {
                let mut imm_memtables: VecDeque<Arc<Memtable>> =
                    self.imm_memtables.iter().cloned().collect();
                let _ignored = imm_memtables.pop_front();

                LsmTree {
                    manifest: Arc::new(self.manifest.apply(edit)),
                    memtable: self.memtable.clone(),
                    imm_memtables: Arc::new(imm_memtables),
                }
            }
            _ => LsmTree {
                manifest: Arc::new(self.manifest.apply(edit)),
                memtable: self.memtable.clone(),
                imm_memtables: self.imm_memtables.clone(),
            },
        }
    }

    pub fn next_log_number_after(&self, log_number: u64) -> u64 {
        assert!(self.imm_memtables.len() > 0);
        assert_eq!(self.imm_memtables[0].log_number, log_number);

        if self.imm_memtables.len() > 1 {
            self.imm_memtables[1].log_number
        } else {
            self.memtable.log_number
        }
    }

    pub fn read(
        &self,
        sstable_cache: Arc<SSTableCache>,
        db_dir: &Path,
        collection: u32,
        index: u32,
        user_key: &[u8],
        snapshot: u64,
        min_snapshot: Option<u64>
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {

        if self.catalog().collection_or_index_exist_at(collection, index, snapshot) == false {
            return Ok(None);
        }

        let record_key = encode_record_key(collection, index, user_key);
        let rs = self.memtable.read(&record_key, snapshot, min_snapshot);
        if let Some((internal_key, value)) = rs {
            return Ok(Some((internal_key, value)))
        }

        if let Some(min_snapshot) = min_snapshot {
            if self.memtable.min_seq >= min_snapshot {
                return Ok(None);
            }
        }

        // Iterate from newest to oldest
        for imm_memtable in self.imm_memtables.iter().rev() {
            if let Some((internal_key, value)) = imm_memtable.read(
                &record_key,
                snapshot,
                min_snapshot
            ) {
                return Ok(Some((internal_key, value)))
            }

            if let Some(min_snapshot) = min_snapshot {
                if imm_memtable.min_seq >= min_snapshot {
                    return Ok(None);
                }
            }
        }

        for sst in self.manifest.find(&record_key, snapshot, min_snapshot) {
            let file = db_dir.join(DbFile::new_sst(sst.number).filename());
            let sst_reader = sstable_cache.get(&file)?;
            if let Some((internal_key, value)) = sst_reader.read(&record_key, snapshot, min_snapshot)? {
                return Ok(Some((internal_key, value)))
            }
        }

        Ok(None)
    }

    pub fn range_scan<'a, R>(
        &'a self,
        sstable_cache: Arc<SSTableCache>,
        db_dir: &Path,
        collection: u32,
        index: u32,
        user_key_range: &R,
        snapshot: u64,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        if self.catalog().collection_or_index_exist_at(collection, index, snapshot) == false {
            // Return an empty iterator if the collection or index does not exist
            let empty_iter = std::iter::empty();
            return Ok(Box::new(empty_iter));
        }

        let internal_key_range_for_scan = encode_internal_key_range(
            collection,
            index,
            user_key_range,
            snapshot,
            direction.clone(),
        );

        let mut iterators: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>> =
            Vec::new();

        // Active memtable iterator
        iterators.push(self.memtable.range_scan(
            internal_key_range_for_scan.clone(),
            snapshot,
            direction.clone(),
        )?);

        // Immutable memtables iterators (iterate from newest to oldest)
        for imm_memtable in self.imm_memtables.iter().rev() {
            iterators.push(imm_memtable.range_scan(
                internal_key_range_for_scan.clone(),
                snapshot,
                direction.clone(),
            )?);
        }

        let record_key_interval =
            encode_record_key_range(collection, index, user_key_range);

        // SSTable iterators
        for sst_meta in self.manifest.find_range(&record_key_interval, snapshot) {
            let file_path = db_dir.join(DbFile::new_sst(sst_meta.number).filename());
            let sst_reader = sstable_cache.get(&file_path)?;
            iterators.push(sst_reader.range_scan(
                internal_key_range_for_scan.clone(),
                snapshot,
                direction.clone(),
            )?);
        }

        let merge_iter = MergeIterator::new(iterators, direction.clone())?;

        let result_iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a> =
            match direction {
                Direction::Forward => Box::new(ForwardIterator::new(Box::new(merge_iter), snapshot)),
                Direction::Reverse => Box::new(ReverseIterator::new(Box::new(merge_iter), snapshot)),
            };

        Ok(result_iter)
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        let self1 = &self.manifest;
        self1.catalog.clone()
    }

    pub fn levels(&self) -> Arc<Levels> {
        self.manifest.lsm.sst_levels.clone()
    }
}
