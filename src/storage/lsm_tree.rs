use crate::obs::metrics::MetricRegistry;
use crate::storage::catalog::Catalog;
use crate::storage::files::DbFile;
use crate::storage::internal_key::extract_operation_type;
use crate::storage::manifest_state::{ManifestEdit, ManifestState};
use crate::storage::memtable::{Memtable, MemtableMetrics};
use crate::storage::operation::OperationType;
use crate::storage::sstable::sstable_cache::SSTableCache;
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::sync::Arc;

pub struct LsmTree {
    pub manifest: Arc<ManifestState>,
    pub memtable: Arc<Memtable>,
    pub imm_memtables: Arc<VecDeque<Arc<Memtable>>>,
}

impl LsmTree {
    pub fn new(
        metric_registry: &mut MetricRegistry,
        current_log_number: u64,
        next_file_number: u64,
    ) -> Self {
        let memtable_metrics = MemtableMetrics::new();
        memtable_metrics.register_to(metric_registry);
        LsmTree {
            manifest: Arc::new(ManifestState::new(current_log_number, next_file_number)),
            memtable: Arc::new(Memtable::new(memtable_metrics, current_log_number)),
            imm_memtables: Arc::new(VecDeque::new()),
        }
    }

    pub fn from(metric_registry: &mut MetricRegistry, manifest_state: ManifestState) -> Self {
        let memtable_metrics = MemtableMetrics::new();
        memtable_metrics.register_to(metric_registry);
        let oldest_log_number = manifest_state.lsm.oldest_log_number;
        LsmTree {
            manifest: Arc::new(manifest_state),
            memtable: Arc::new(Memtable::new(memtable_metrics, oldest_log_number)),
            imm_memtables: Arc::new(VecDeque::new()),
        }
    }

    pub fn apply(&self, edit: &ManifestEdit) -> Self {
        match edit {
            ManifestEdit::WalRotation { log_number } => {
                // The log was rotated because the memtable was considered as full. The memtable
                // should be considered as immutable and placed in the queue waiting for being
                // flushed to disk. A new memtable should be created and associated to the new log.
                let mut imm_memtables: VecDeque<Arc<Memtable>> =
                    self.imm_memtables.iter().cloned().collect();
                imm_memtables.push_back(self.memtable.clone());

                let metrics = self.memtable.metrics();

                LsmTree {
                    manifest: Arc::new(self.manifest.apply(edit)),
                    memtable: Arc::new(Memtable::new(metrics, *log_number)),
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
        record_key: &[u8],
        snapshot: u64,
    ) -> Result<Option<Vec<u8>>> {
        if let Some((internal_key, value)) = self.memtable.read(&record_key, snapshot) {
            return Self::resolve_value(&internal_key, value);
        }

        for imm_memtable in self.imm_memtables.iter() {
            if let Some((internal_key, value)) = imm_memtable.read(&record_key, snapshot) {
                return Self::resolve_value(&internal_key, value);
            }
        }

        for sst in self.manifest.find(&record_key, snapshot) {
            let file = db_dir.join(DbFile::new_sst(sst.number).filename());
            let sst_reader = sstable_cache.get(&file)?;
            if let Some((internal_key, value)) = sst_reader.read(&record_key, snapshot)? {
                return Self::resolve_value(&internal_key, value);
            }
        }

        Ok(None)
    }

    fn resolve_value(internal_key: &Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let op = extract_operation_type(&internal_key);
        match op {
            OperationType::Put => Ok(Some(value)),
            OperationType::Delete => Ok(None),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unexpected operation type: {:?}", op),
            )),
        }
    }

    pub fn catalogue(&self) -> &Arc<Catalog> {
        self.manifest.catalogue()
    }
}
