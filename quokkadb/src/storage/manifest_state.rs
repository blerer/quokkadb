use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::catalog::{Catalog, CollectionOptions, IndexDefinition, IndexOptions};
use crate::storage::count_stats::{CountStats, CountStatsKey};
use crate::storage::lsm_version::{DropMetadata, LsmVersion, SSTableMetadata};
use crate::util::interval::Interval;
use std::fmt::Debug;
use std::io::Result;
use std::sync::Arc;

pub(crate) const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Represents a full snapshot of the database's durable state at a point in time.
///
/// `ManifestState` includes both physical state (`LsmVersion`) and logical schema
/// (`Catalog`). It is used for manifest snapshots and to apply manifest edits
/// deterministically during recovery.
#[derive(Debug, PartialEq)]
pub struct ManifestState {
    /// The persisted state of the LSM tree and WALs (excluding memtables).
    pub lsm: Arc<LsmVersion>,
    /// The catalog of collections and indexes committed by a flush.
    pub catalog: Arc<Catalog>,
    /// Aggregated current logical totals for collections and indexes.
    pub count_stats: CountStats,
    /// Schema changes that are visible in memory but not yet committed by a flush.
    pending_catalog_edits: Arc<Vec<ManifestEdit>>,
}

impl ManifestState {
    pub fn new(current_log_number: u64, next_file_number: u64, max_levels: usize) -> Self {
        ManifestState {
            lsm: Arc::new(LsmVersion::new(
                current_log_number,
                next_file_number,
                max_levels,
            )),
            catalog: Arc::new(Catalog::new()),
            count_stats: CountStats::default(),
            pending_catalog_edits: Arc::new(Vec::new()),
        }
    }

    pub fn visible_catalog(&self) -> Arc<Catalog> {
        let mut catalog = self.catalog.clone();
        for edit in self.pending_catalog_edits.iter() {
            catalog = Arc::new(edit.apply_to_catalog(&catalog));
        }
        catalog
    }

    #[cfg(test)]
    pub fn has_pending_catalog_edits(&self) -> bool {
        !self.pending_catalog_edits.is_empty()
    }

    pub fn has_pending_catalog_edits_after(&self, sequence: u64) -> bool {
        self.pending_catalog_edits
            .last()
            .is_some_and(|edit| edit.catalog_edit_sequence().unwrap() > sequence)
    }

    fn queue_catalog_edit(&self, edit: &ManifestEdit) -> Self {
        let sequence = edit
            .catalog_edit_sequence()
            .expect("Only catalog edits can be queued");
        let mut pending_catalog_edits = (*self.pending_catalog_edits).clone();
        assert!(
            pending_catalog_edits
                .last()
                .is_none_or(|previous| previous.catalog_edit_sequence().unwrap() <= sequence),
            "Pending catalog edits must be ordered by sequence number"
        );
        pending_catalog_edits.push(edit.clone());
        ManifestState {
            lsm: self.lsm.clone(),
            catalog: self.catalog.clone(),
            count_stats: self.count_stats.clone(),
            pending_catalog_edits: Arc::new(pending_catalog_edits),
        }
    }

    fn commit_pending_catalog_edits_through(
        &self,
        oldest_log_number: u64,
        sst: &Arc<SSTableMetadata>,
        count_stats: &CountStats,
    ) -> Self {
        let split = self.pending_catalog_edits.partition_point(|edit| {
            edit.catalog_edit_sequence().unwrap() <= sst.max_sequence_number
        });
        let drops = self.pending_catalog_edits[..split]
            .iter()
            .filter_map(ManifestEdit::drop_metadata)
            .collect::<Vec<_>>();
        let mut state = ManifestState {
            lsm: Arc::new(self.lsm.with_flushed_sstable(oldest_log_number, sst, drops)),
            catalog: self.catalog.clone(),
            count_stats: apply_count_stats_delta(&self.count_stats, count_stats),
            pending_catalog_edits: Arc::new(self.pending_catalog_edits[split..].to_vec()),
        };

        for edit in &self.pending_catalog_edits[..split] {
            state.apply_committed_catalog_edit(edit);
        }
        state
    }

    fn discard_pending_catalog_edits_after(&self, sequence: u64) -> Self {
        let retained = self
            .pending_catalog_edits
            .iter()
            .take_while(|edit| edit.catalog_edit_sequence().unwrap() <= sequence)
            .cloned()
            .collect();
        ManifestState {
            lsm: self.lsm.clone(),
            catalog: self.catalog.clone(),
            count_stats: self.count_stats.clone(),
            pending_catalog_edits: Arc::new(retained),
        }
    }

    fn apply_committed_catalog_edit(&mut self, edit: &ManifestEdit) {
        match edit {
            ManifestEdit::CreateCollection {
                name,
                id,
                created_at,
                options,
            } => {
                self.catalog = Arc::new(self.catalog.add_collection_with_options(
                    name,
                    *id,
                    *created_at,
                    options.clone(),
                ));
            }
            ManifestEdit::DropCollection { id, dropped_at } => {
                self.catalog = Arc::new(self.catalog.drop_collection(*id, *dropped_at));
                self.count_stats = without_collection_count_stats(&self.count_stats, *id);
            }
            ManifestEdit::RenameCollection { id, new_name, .. } => {
                self.catalog = Arc::new(self.catalog.rename_collection(*id, new_name));
            }
            ManifestEdit::CreateIndex {
                collection_id,
                index_id,
                definition,
                options,
                created_at,
            } => {
                self.catalog = Arc::new(self.catalog.add_index_to_collection(
                    *collection_id,
                    *index_id,
                    definition,
                    options,
                    *created_at,
                ));
            }
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => {
                self.catalog = Arc::new(self.catalog.drop_index(
                    *collection_id,
                    *index_id,
                    *dropped_at,
                ));
                self.count_stats =
                    without_index_count_stats(&self.count_stats, *collection_id, *index_id);
            }
            _ => unreachable!("Only catalog edits can be committed"),
        }
    }

    pub fn apply(&self, edit: &ManifestEdit) -> Self {
        match edit {
            ManifestEdit::WalRotation {
                log_number,
                next_seq: _next_seq,
            } => ManifestState {
                lsm: Arc::new(self.lsm.with_new_log_file(*log_number)),
                catalog: self.catalog.clone(),
                count_stats: self.count_stats.clone(),
                pending_catalog_edits: self.pending_catalog_edits.clone(),
            },
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
                count_stats,
            } => self.commit_pending_catalog_edits_through(*oldest_log_number, sst, count_stats),
            edit @ (ManifestEdit::CreateCollection { .. }
            | ManifestEdit::DropCollection { .. }
            | ManifestEdit::RenameCollection { .. }
            | ManifestEdit::CreateIndex { .. }
            | ManifestEdit::DropIndex { .. }) => self.queue_catalog_edit(edit),
            ManifestEdit::FilesDetectedOnRestart { next_file_number } => ManifestState {
                lsm: Arc::new(self.lsm.adjust_file_number(*next_file_number)),
                catalog: self.catalog.clone(),
                count_stats: self.count_stats.clone(),
                pending_catalog_edits: self.pending_catalog_edits.clone(),
            },
            ManifestEdit::ManifestRotation { manifest_number } => ManifestState {
                lsm: Arc::new(self.lsm.manifest_rotation(*manifest_number)),
                catalog: self.catalog.clone(),
                count_stats: self.count_stats.clone(),
                pending_catalog_edits: self.pending_catalog_edits.clone(),
            },
            ManifestEdit::Snapshot(_snapshot) => {
                unreachable!("Snapshots should not be applied to an LSMTree")
            }
            ManifestEdit::IgnoringEmptyMemtable { oldest_log_number } => ManifestState {
                lsm: Arc::new(self.lsm.with_ignored_empty_memtable(*oldest_log_number)),
                catalog: self.catalog.clone(),
                count_stats: self.count_stats.clone(),
                pending_catalog_edits: self.pending_catalog_edits.clone(),
            },
            ManifestEdit::Compaction {
                output_level,
                removed_sstables,
                added_sstables,
                drops,
            } => ManifestState {
                lsm: Arc::new(self.lsm.with_compaction(
                    *output_level,
                    removed_sstables,
                    added_sstables,
                    drops,
                )),
                catalog: self.catalog.clone(),
                count_stats: self.count_stats.clone(),
                pending_catalog_edits: self.pending_catalog_edits.clone(),
            },
            ManifestEdit::DiscardPendingCatalogEditsAfter { sequence } => {
                self.discard_pending_catalog_edits_after(*sequence)
            }
        }
    }

    /// Returns the drops with a sequence number smaller or equal to the given sequence_number.
    #[cfg(test)]
    pub fn get_drops_before_or_at(&self, sequence_number: u64) -> Vec<Arc<DropMetadata>> {
        self.lsm.get_drops_before_or_at(sequence_number)
    }

    pub fn count_stat(&self, key: &CountStatsKey) -> Option<i64> {
        self.count_stats.count_stat(key)
    }

    pub fn find_sstables<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
        min_snapshot: Option<u64>,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.lsm.find_sstables(record_key, snapshot, min_snapshot)
    }

    pub fn find_sstables_in_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.lsm.find_sstables_in_range(record_key_range, snapshot)
    }
}

impl Serializable for ManifestState {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        Ok(ManifestState {
            lsm: Arc::new(LsmVersion::read_from(reader, version)?),
            catalog: Arc::new(Catalog::read_from(reader, version)?),
            count_stats: CountStats::read_from(reader, version)?,
            pending_catalog_edits: Arc::new(Vec::<ManifestEdit>::read_from(reader, version)?),
        })
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        self.lsm.write_to(writer, version);
        self.catalog.write_to(writer, version);
        self.count_stats.write_to(writer, version);
        self.pending_catalog_edits.write_to(writer, version);
    }
}

mod tags {
    pub const SNAPSHOT: u8 = 0;
    pub const CREATE_COLLECTION: u8 = 1;
    pub const DROP_COLLECTION: u8 = 2;
    pub const RENAME_COLLECTION: u8 = 3;
    pub const WAL_ROTATION: u8 = 4;
    pub const MANIFEST_ROTATION: u8 = 5;
    pub const FLUSH: u8 = 6;
    pub const FILES_DETECTED_ON_RESTART: u8 = 7;
    pub const IGNORING_EMPTY_MEMTABLE: u8 = 8;
    pub const COMPACTION: u8 = 9;
    pub const CREATE_INDEX: u8 = 10;
    pub const DROP_INDEX: u8 = 11;
    pub const DISCARD_PENDING_CATALOG_EDITS_AFTER: u8 = 12;
}

impl Serializable for ManifestEdit {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        if version != MANIFEST_FORMAT_VERSION {
            return Err(invalid_data(format!(
                "Unsupported manifest version {version}"
            )));
        }

        let edit = reader.read_u8()?;
        match edit {
            tags::SNAPSHOT => Ok(ManifestEdit::Snapshot(Arc::new(ManifestState::read_from(
                &reader, version,
            )?))),
            tags::CREATE_COLLECTION => {
                let name = reader.read_str()?.to_string();
                let id = reader.read_varint_u32()?;
                let created_at = reader.read_varint_u64()?;
                let options = CollectionOptions::read_from(&reader, version)?;
                Ok(ManifestEdit::CreateCollection {
                    name,
                    id,
                    created_at,
                    options,
                })
            }
            tags::DROP_COLLECTION => {
                let id = reader.read_varint_u32()?;
                let dropped_at = reader.read_varint_u64()?;
                Ok(ManifestEdit::DropCollection { id, dropped_at })
            }
            tags::RENAME_COLLECTION => {
                let id = reader.read_varint_u32()?;
                let new_name = reader.read_str()?.to_string();
                let renamed_at = reader.read_varint_u64()?;
                Ok(ManifestEdit::RenameCollection {
                    id,
                    new_name,
                    renamed_at,
                })
            }
            tags::WAL_ROTATION => {
                let log_number = reader.read_varint_u64()?;
                let next_seq = reader.read_varint_u64()?;
                Ok(ManifestEdit::WalRotation {
                    log_number,
                    next_seq,
                })
            }
            tags::MANIFEST_ROTATION => {
                let manifest_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::ManifestRotation { manifest_number })
            }
            tags::FLUSH => {
                let oldest_log_number = reader.read_varint_u64()?;
                let sst = Arc::new(SSTableMetadata::read_from(&reader, version)?);
                let count_stats = if reader.has_remaining() {
                    CountStats::read_from(&reader, version)?
                } else {
                    CountStats::default()
                };
                Ok(ManifestEdit::Flush {
                    oldest_log_number,
                    sst,
                    count_stats,
                })
            }
            tags::FILES_DETECTED_ON_RESTART => {
                let next_file_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::FilesDetectedOnRestart { next_file_number })
            }
            tags::IGNORING_EMPTY_MEMTABLE => {
                let oldest_log_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::IgnoringEmptyMemtable { oldest_log_number })
            }
            tags::COMPACTION => {
                let output_level = reader.read_u8()? as usize;
                let removed_sstables = Vec::<Arc<SSTableMetadata>>::read_from(&reader, version)?;
                let added_sstables = Vec::<Arc<SSTableMetadata>>::read_from(&reader, version)?;
                let drops = Vec::<Arc<DropMetadata>>::read_from(&reader, version)?;
                Ok(ManifestEdit::Compaction {
                    output_level,
                    removed_sstables,
                    added_sstables,
                    drops,
                })
            }
            tags::CREATE_INDEX => {
                let collection_id = reader.read_varint_u32()?;
                let index_id = reader.read_varint_u32()?;
                let definition = IndexDefinition::read_from(&reader, version)?;
                let options = IndexOptions::read_from(&reader, version)?;
                let created_at = reader.read_varint_u64()?;
                Ok(ManifestEdit::CreateIndex {
                    collection_id,
                    index_id,
                    definition,
                    options,
                    created_at,
                })
            }
            tags::DROP_INDEX => {
                let collection_id = reader.read_varint_u32()?;
                let index_id = reader.read_varint_u32()?;
                let dropped_at = reader.read_varint_u64()?;
                Ok(ManifestEdit::DropIndex {
                    collection_id,
                    index_id,
                    dropped_at,
                })
            }
            tags::DISCARD_PENDING_CATALOG_EDITS_AFTER => {
                Ok(ManifestEdit::DiscardPendingCatalogEditsAfter {
                    sequence: reader.read_varint_u64()?,
                })
            }
            _ => Err(invalid_data(format!("ManifestEdit: {}", edit))),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        if version != MANIFEST_FORMAT_VERSION {
            panic!("Unsupported manifest version {version}");
        }

        match self {
            ManifestEdit::Snapshot(tree) => {
                writer.write_u8(tags::SNAPSHOT);
                tree.write_to(writer, version);
            }
            ManifestEdit::CreateCollection {
                name,
                id,
                created_at,
                options,
            } => {
                writer
                    .write_u8(tags::CREATE_COLLECTION)
                    .write_str(&name)
                    .write_varint_u32(*id)
                    .write_varint_u64(*created_at);
                options.write_to(writer, version);
            }
            ManifestEdit::DropCollection {
                id,
                dropped_at: drop_at,
            } => {
                writer
                    .write_u8(tags::DROP_COLLECTION)
                    .write_varint_u32(*id)
                    .write_varint_u64(*drop_at);
            }
            ManifestEdit::RenameCollection {
                id,
                new_name,
                renamed_at,
            } => {
                writer
                    .write_u8(tags::RENAME_COLLECTION)
                    .write_varint_u32(*id)
                    .write_str(new_name)
                    .write_varint_u64(*renamed_at);
            }
            ManifestEdit::WalRotation {
                log_number,
                next_seq,
            } => {
                writer
                    .write_u8(tags::WAL_ROTATION)
                    .write_varint_u64(*log_number)
                    .write_varint_u64(*next_seq);
            }
            ManifestEdit::ManifestRotation { manifest_number } => {
                writer
                    .write_u8(tags::MANIFEST_ROTATION)
                    .write_varint_u64(*manifest_number);
            }
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
                count_stats,
            } => {
                writer
                    .write_u8(tags::FLUSH)
                    .write_varint_u64(*oldest_log_number);
                sst.write_to(writer, version);
                count_stats.write_to(writer, version);
            }
            ManifestEdit::FilesDetectedOnRestart { next_file_number } => {
                writer
                    .write_u8(tags::FILES_DETECTED_ON_RESTART)
                    .write_varint_u64(*next_file_number);
            }
            ManifestEdit::IgnoringEmptyMemtable { oldest_log_number } => {
                writer
                    .write_u8(tags::IGNORING_EMPTY_MEMTABLE)
                    .write_varint_u64(*oldest_log_number);
            }
            ManifestEdit::Compaction {
                output_level,
                removed_sstables,
                added_sstables,
                drops,
            } => {
                writer.write_u8(tags::COMPACTION);
                writer.write_u8(*output_level as u8);
                Vec::<Arc<SSTableMetadata>>::write_to(removed_sstables, writer, version);
                Vec::<Arc<SSTableMetadata>>::write_to(added_sstables, writer, version);
                Vec::<Arc<DropMetadata>>::write_to(drops, writer, version);
            }
            ManifestEdit::CreateIndex {
                collection_id,
                index_id,
                definition,
                options,
                created_at,
            } => {
                writer.write_u8(tags::CREATE_INDEX);
                writer.write_varint_u32(*collection_id);
                writer.write_varint_u32(*index_id);
                definition.write_to(writer, version);
                options.write_to(writer, version);
                writer.write_varint_u64(*created_at);
            }
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => {
                writer.write_u8(tags::DROP_INDEX);
                writer.write_varint_u32(*collection_id);
                writer.write_varint_u32(*index_id);
                writer.write_varint_u64(*dropped_at);
            }
            ManifestEdit::DiscardPendingCatalogEditsAfter { sequence } => {
                writer
                    .write_u8(tags::DISCARD_PENDING_CATALOG_EDITS_AFTER)
                    .write_varint_u64(*sequence);
            }
        }
    }
}

fn apply_count_stats_delta(current: &CountStats, delta: &CountStats) -> CountStats {
    let mut merged = current.deltas.clone();

    for (key, value) in &delta.deltas {
        let new_value = merged.get(key).copied().unwrap_or_default() + value;
        if new_value == 0 {
            merged.remove(key);
        } else {
            merged.insert(key.clone(), new_value);
        }
    }

    CountStats::new(merged)
}

fn without_collection_count_stats(current: &CountStats, collection: u32) -> CountStats {
    CountStats::new(
        current
            .deltas
            .iter()
            .filter(|(key, _)| {
                !matches!(key, CountStatsKey::Collection(id) if *id == collection)
                    && !matches!(
                        key,
                        CountStatsKey::Index {
                            collection: id,
                            ..
                        } if *id == collection
                    )
            })
            .map(|(key, delta)| (key.clone(), *delta))
            .collect(),
    )
}

fn without_index_count_stats(current: &CountStats, collection: u32, index: u32) -> CountStats {
    CountStats::new(
        current
            .deltas
            .iter()
            .filter(|(key, _)| {
                !matches!(
                    key,
                    CountStatsKey::Index {
                        collection: c,
                        index: i
                    } if *c == collection && *i == index
                )
            })
            .map(|(key, delta)| (key.clone(), *delta))
            .collect(),
    )
}

/// Represents a single atomic change to the manifest state.
///
/// This enum is logged in the manifest and replayed at startup to reconstruct
/// the full `ManifestState`.
#[derive(Debug, Clone, PartialEq)]
pub enum ManifestEdit {
    /// A full snapshot of the current manifest state.
    Snapshot(Arc<ManifestState>),

    /// Adds a new collection to the catalog.
    CreateCollection {
        name: String,
        id: u32,
        created_at: u64,
        options: CollectionOptions,
    },

    /// Removes a collection from the catalog.
    DropCollection { id: u32, dropped_at: u64 },

    /// Renames a collection in the catalog.
    RenameCollection {
        id: u32,
        new_name: String,
        renamed_at: u64,
    },

    /// Indicates a new WAL file has been created.
    WalRotation { log_number: u64, next_seq: u64 },

    /// Indicates a new manifest file has been created.
    ManifestRotation { manifest_number: u64 },

    /// Records a flush of a memtable into an SSTable.
    Flush {
        oldest_log_number: u64,
        sst: Arc<SSTableMetadata>,
        count_stats: CountStats,
    },

    /// Updates file number tracking based on files detected during recovery.
    FilesDetectedOnRestart { next_file_number: u64 },

    /// On replay if a WAL was corrupted and did not result in any update we need to skip it
    /// and drop the empty memtable.
    IgnoringEmptyMemtable { oldest_log_number: u64 },

    /// Records a compaction that has been performed, the SSTables removed and added, and any drops
    /// that were applied.
    Compaction {
        output_level: usize,
        removed_sstables: Vec<Arc<SSTableMetadata>>,
        added_sstables: Vec<Arc<SSTableMetadata>>,
        drops: Vec<Arc<DropMetadata>>,
    },

    /// Add a new index to a collection
    CreateIndex {
        collection_id: u32,
        index_id: u32,
        definition: IndexDefinition,
        options: IndexOptions,
        created_at: u64,
    },

    /// Marks an index as dropped in a collection.
    DropIndex {
        collection_id: u32,
        index_id: u32,
        dropped_at: u64,
    },

    /// Removes queued schema mutations after a WAL recovery boundary.
    DiscardPendingCatalogEditsAfter { sequence: u64 },
}

impl ManifestEdit {
    fn catalog_edit_sequence(&self) -> Option<u64> {
        match self {
            ManifestEdit::CreateCollection { created_at, .. }
            | ManifestEdit::CreateIndex { created_at, .. } => Some(*created_at),
            ManifestEdit::DropCollection { dropped_at, .. }
            | ManifestEdit::DropIndex { dropped_at, .. } => Some(*dropped_at),
            ManifestEdit::RenameCollection { renamed_at, .. } => Some(*renamed_at),
            _ => None,
        }
    }

    fn apply_to_catalog(&self, catalog: &Catalog) -> Catalog {
        match self {
            ManifestEdit::CreateCollection {
                name,
                id,
                created_at,
                options,
            } => catalog.add_collection_with_options(name, *id, *created_at, options.clone()),
            ManifestEdit::DropCollection { id, dropped_at } => {
                catalog.drop_collection(*id, *dropped_at)
            }
            ManifestEdit::RenameCollection { id, new_name, .. } => {
                catalog.rename_collection(*id, new_name)
            }
            ManifestEdit::CreateIndex {
                collection_id,
                index_id,
                definition,
                options,
                created_at,
            } => catalog.add_index_to_collection(
                *collection_id,
                *index_id,
                definition,
                options,
                *created_at,
            ),
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => catalog.drop_index(*collection_id, *index_id, *dropped_at),
            _ => unreachable!("Only catalog edits can be applied to the catalog"),
        }
    }

    fn drop_metadata(&self) -> Option<Arc<DropMetadata>> {
        match self {
            ManifestEdit::DropCollection { id, dropped_at } => {
                Some(DropMetadata::new_collection_drop(*id, *dropped_at))
            }
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => Some(DropMetadata::new_index_drop(
                *collection_id,
                *index_id,
                *dropped_at,
            )),
            _ => None,
        }
    }

    pub fn to_vec(&self, version: u32) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        self.write_to(&mut writer, version);
        writer.take_buffer()
    }

    pub fn try_from_vec(input: &[u8], version: u32) -> Result<ManifestEdit> {
        let reader = ByteReader::new(input);
        Self::read_from(&reader, version)
    }
}

use crate::io::invalid_data;
use crate::io::serializable::Serializable;
use std::fmt;

impl fmt::Display for ManifestEdit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestEdit::Snapshot(state) => write!(f, "Snapshot({:?})", state),
            ManifestEdit::CreateCollection {
                name,
                id,
                created_at,
                options,
            } => {
                write!(
                    f,
                    "CreateCollection {{ name: {}, id: {}, created_at: {}, options: {} }}",
                    name, id, created_at, options
                )
            }
            ManifestEdit::DropCollection { id, dropped_at } => {
                write!(
                    f,
                    "DropCollection {{ id: {}, dropped_at: {} }}",
                    id, dropped_at
                )
            }
            ManifestEdit::RenameCollection {
                id,
                new_name,
                renamed_at,
            } => {
                write!(
                    f,
                    "RenameCollection {{ id: {}, new_name: {}, renamed_at: {} }}",
                    id, new_name, renamed_at
                )
            }
            ManifestEdit::WalRotation {
                log_number,
                next_seq,
            } => {
                write!(
                    f,
                    "WalRotation {{ log_number: {}, next_seq: {} }}",
                    log_number, next_seq
                )
            }
            ManifestEdit::ManifestRotation { manifest_number } => write!(
                f,
                "ManifestRotation {{ manifest_number: {} }}",
                manifest_number
            ),
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
                count_stats,
            } => write!(
                f,
                "Flush {{ oldest_log_number: {}, sst: {:?}, count_stats: {:?} }}",
                oldest_log_number, sst, count_stats,
            ),
            ManifestEdit::FilesDetectedOnRestart { next_file_number } => write!(
                f,
                "FilesDetectedOnRestart {{ next_file_number: {} }}",
                next_file_number
            ),
            ManifestEdit::IgnoringEmptyMemtable { oldest_log_number } => write!(
                f,
                "IgnoringEmptyMemtable {{ oldest_log_number: {} }}",
                oldest_log_number
            ),
            ManifestEdit::Compaction {
                output_level,
                removed_sstables,
                added_sstables,
                drops,
            } => write!(
                f,
                "Compaction {{ output_level: {}, removed_sstables: {:?}, added_sstables: {:?}, drops: {:?} }}",
                output_level, removed_sstables, added_sstables, drops
            ),
            ManifestEdit::CreateIndex {
                collection_id,
                index_id,
                definition,
                options,
                created_at,
            } => write!(
                f,
                "CreateIndex {{ collection_id: {}, index_id: {}, definition: {}, options: {:?}, created_at: {} }}",
                collection_id, index_id, definition, options, created_at
            ),
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => write!(
                f,
                "DropIndex {{ collection_id: {}, index_id: {}, dropped_at: {} }}",
                collection_id, index_id, dropped_at
            ),
            ManifestEdit::DiscardPendingCatalogEditsAfter { sequence } => write!(
                f,
                "DiscardPendingCatalogEditsAfter {{ sequence: {} }}",
                sequence
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::catalog::{
        CollectionMetadata, CollectionOptions, IndexDirection, IndexPath, OrderedIndexField,
    };
    use crate::storage::internal_key::encode_record_key;
    use crate::util::bson_utils::BsonKey;
    use bson::Bson;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn test_create_and_drop_collection_serialization() {
        let edit = ManifestEdit::CreateCollection {
            name: "my_collection".to_string(),
            id: 42,
            created_at: 1627846261,
            options: CollectionOptions::default(),
        };
        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);

        let edit = ManifestEdit::DropCollection {
            id: 42,
            dropped_at: 1627846262,
        };
        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn test_rename_collection_serialization() {
        let edit = ManifestEdit::RenameCollection {
            id: 42,
            new_name: "new_name".to_string(),
            renamed_at: 1627846261,
        };
        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn test_discard_pending_catalog_edits_serialization() {
        check_edit_serialization_roundtrip(
            ManifestEdit::DiscardPendingCatalogEditsAfter { sequence: 42 },
            MANIFEST_FORMAT_VERSION,
        );
    }

    #[test]
    fn test_snapshot_serializes_pending_catalog_edits() {
        let state = ManifestState::new(1, 2, 3).apply(&ManifestEdit::CreateCollection {
            name: "docs".to_string(),
            id: 10,
            created_at: 1000,
            options: CollectionOptions::default(),
        });

        check_edit_serialization_roundtrip(
            ManifestEdit::Snapshot(Arc::new(state)),
            MANIFEST_FORMAT_VERSION,
        );
    }

    #[test]
    fn test_apply_rename_collection() {
        let tree = ManifestState::new(1, 2, 3);

        let tree = tree.apply(&ManifestEdit::CreateCollection {
            name: "old_name".to_string(),
            id: 10,
            created_at: 1000,
            options: CollectionOptions::default(),
        });
        let tree = flush_pending_catalog_edits(tree, 1000);

        assert!(tree.catalog.get_collection_by_name("old_name").is_some());
        assert!(tree.catalog.get_collection_by_name("new_name").is_none());

        let tree = tree.apply(&ManifestEdit::RenameCollection {
            id: 10,
            new_name: "new_name".to_string(),
            renamed_at: 1000,
        });
        let tree = flush_pending_catalog_edits(tree, 1000);

        assert!(tree.catalog.get_collection_by_name("old_name").is_none());
        assert!(tree.catalog.get_collection_by_name("new_name").is_some());
        assert_eq!(
            tree.catalog.get_collection_by_name("new_name").unwrap().id,
            10
        );
    }

    #[test]
    fn test_wal_and_manifest_rotation_serialization() {
        check_edit_serialization_roundtrip(
            ManifestEdit::WalRotation {
                log_number: 123,
                next_seq: 456,
            },
            MANIFEST_FORMAT_VERSION,
        );
        check_edit_serialization_roundtrip(
            ManifestEdit::ManifestRotation {
                manifest_number: 456,
            },
            MANIFEST_FORMAT_VERSION,
        );
    }

    #[test]
    fn test_manifest_edit_rejects_unsupported_version() {
        let edit = ManifestEdit::ManifestRotation {
            manifest_number: 456,
        };
        let bytes = edit.to_vec(MANIFEST_FORMAT_VERSION);

        assert!(ManifestEdit::try_from_vec(&bytes, 2).is_err());
    }

    #[test]
    fn test_files_detected_on_restart_serialization() {
        check_edit_serialization_roundtrip(
            ManifestEdit::FilesDetectedOnRestart {
                next_file_number: 789,
            },
            MANIFEST_FORMAT_VERSION,
        );
    }

    #[test]
    fn test_flush_serialization() {
        use crate::storage::count_stats::CountStats;

        let sst = Arc::new(SSTableMetadata::new(
            1,
            0,
            &record_key(1),
            &record_key(250),
            100,
            200,
            1024,
        ));

        let edit = ManifestEdit::Flush {
            oldest_log_number: 8,
            sst: sst.clone(),
            count_stats: CountStats::default(),
        };

        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn test_apply_flush_merges_count_stats_into_manifest_state() {
        let state = ManifestState::new(1, 10, 4);
        let sst1 = Arc::new(SSTableMetadata::new(
            1,
            0,
            &record_key(1),
            &record_key(250),
            100,
            200,
            1024,
        ));
        let delta = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(7), 3),
            (
                CountStatsKey::Index {
                    collection: 7,
                    index: 2,
                },
                5,
            ),
        ]));

        let state = state.apply(&ManifestEdit::Flush {
            oldest_log_number: 8,
            sst: sst1,
            count_stats: delta.clone(),
        });

        let sst2 = Arc::new(SSTableMetadata::new(
            2,
            0,
            &record_key(251),
            &record_key(500),
            101,
            201,
            2048,
        ));
        let delta2 = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(7), 2),
            (
                CountStatsKey::Index {
                    collection: 7,
                    index: 2,
                },
                -1,
            ),
            (
                CountStatsKey::Index {
                    collection: 7,
                    index: 3,
                },
                4,
            ),
        ]));

        let state = state.apply(&ManifestEdit::Flush {
            oldest_log_number: 9,
            sst: sst2,
            count_stats: delta2,
        });

        assert_eq!(
            state.count_stats,
            CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(7), 5),
                (
                    CountStatsKey::Index {
                        collection: 7,
                        index: 2,
                    },
                    4,
                ),
                (
                    CountStatsKey::Index {
                        collection: 7,
                        index: 3,
                    },
                    4,
                ),
            ]))
        );
    }

    #[test]
    fn test_apply_drop_collection_removes_collection_and_index_count_stats() {
        let state = ManifestState::new(1, 10, 4)
            .apply(&ManifestEdit::CreateCollection {
                name: "docs".to_string(),
                id: 10,
                created_at: 10,
                options: CollectionOptions::default(),
            })
            .apply(&ManifestEdit::CreateIndex {
                collection_id: 10,
                index_id: 1,
                definition: IndexDefinition::Regular(vec![OrderedIndexField {
                    path: "a".into(),
                    direction: IndexDirection::Ascending,
                }]),
                options: IndexOptions::default(),
                created_at: 11,
            });
        let state = flush_pending_catalog_edits(state, 11);
        let state = ManifestState {
            count_stats: CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(10), 5),
                (
                    CountStatsKey::Index {
                        collection: 10,
                        index: 1,
                    },
                    4,
                ),
                (
                    CountStatsKey::Index {
                        collection: 8,
                        index: 1,
                    },
                    3,
                ),
            ])),
            ..state
        };

        let state = state.apply(&ManifestEdit::DropCollection {
            id: 10,
            dropped_at: 100,
        });
        let state = flush_pending_catalog_edits(state, 100);

        assert_eq!(
            state.count_stats,
            CountStats::new(BTreeMap::from([(
                CountStatsKey::Index {
                    collection: 8,
                    index: 1,
                },
                3,
            )]))
        );
    }

    #[test]
    fn test_apply_drop_index_removes_only_target_index_count_stats() {
        let state = ManifestState::new(1, 10, 4)
            .apply(&ManifestEdit::CreateCollection {
                name: "docs".to_string(),
                id: 10,
                created_at: 10,
                options: CollectionOptions::default(),
            })
            .apply(&ManifestEdit::CreateIndex {
                collection_id: 10,
                index_id: 1,
                definition: IndexDefinition::Regular(vec![OrderedIndexField {
                    path: "a".into(),
                    direction: IndexDirection::Ascending,
                }]),
                options: IndexOptions::default(),
                created_at: 11,
            })
            .apply(&ManifestEdit::CreateIndex {
                collection_id: 10,
                index_id: 2,
                definition: IndexDefinition::Regular(vec![OrderedIndexField {
                    path: "b".into(),
                    direction: IndexDirection::Ascending,
                }]),
                options: IndexOptions::default(),
                created_at: 12,
            });
        let state = flush_pending_catalog_edits(state, 12);
        let state = ManifestState {
            count_stats: CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(10), 5),
                (
                    CountStatsKey::Index {
                        collection: 10,
                        index: 1,
                    },
                    4,
                ),
                (
                    CountStatsKey::Index {
                        collection: 10,
                        index: 2,
                    },
                    6,
                ),
            ])),
            ..state
        };

        let state = state.apply(&ManifestEdit::DropIndex {
            collection_id: 10,
            index_id: 1,
            dropped_at: 100,
        });
        let state = flush_pending_catalog_edits(state, 100);

        assert_eq!(
            state.count_stats,
            CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(10), 5),
                (
                    CountStatsKey::Index {
                        collection: 10,
                        index: 2,
                    },
                    6,
                ),
            ]))
        );
    }

    #[test]
    fn test_apply_flush_removes_count_stats_entry_when_total_reaches_zero() {
        let state = ManifestState {
            lsm: Arc::new(LsmVersion::new(1, 10, 4)),
            catalog: Arc::new(Catalog::new()),
            count_stats: CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(10), 5),
                (
                    CountStatsKey::Index {
                        collection: 10,
                        index: 1,
                    },
                    4,
                ),
            ])),
            pending_catalog_edits: Arc::new(Vec::new()),
        };
        let sst = Arc::new(SSTableMetadata::new(
            1,
            0,
            &record_key(1),
            &record_key(250),
            100,
            200,
            1024,
        ));

        let state = state.apply(&ManifestEdit::Flush {
            oldest_log_number: 8,
            sst,
            count_stats: CountStats::new(BTreeMap::from([(
                CountStatsKey::Index {
                    collection: 10,
                    index: 1,
                },
                -4,
            )])),
        });

        assert_eq!(
            state.count_stats,
            CountStats::new(BTreeMap::from([(CountStatsKey::Collection(10), 5,)]))
        );
    }

    #[test]
    fn test_compaction_serialization() {
        let sst1 = Arc::new(SSTableMetadata::new(
            1,
            0,
            &record_key(1),
            &record_key(250),
            100,
            200,
            1024,
        ));
        let sst2 = Arc::new(SSTableMetadata::new(
            2,
            0,
            &record_key(251),
            &record_key(500),
            101,
            201,
            2048,
        ));
        let drop1 = DropMetadata::new_collection_drop(10, 150);
        let drop2 = DropMetadata::new_index_drop(20, 1, 160);

        let edit = ManifestEdit::Compaction {
            output_level: 1,
            removed_sstables: vec![sst1.clone()],
            added_sstables: vec![sst2.clone()],
            drops: vec![drop1, drop2],
        };

        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn test_create_index_serialization() {
        let edit = ManifestEdit::CreateIndex {
            collection_id: 10,
            index_id: 2,
            definition: IndexDefinition::Regular(vec![
                OrderedIndexField {
                    path: IndexPath {
                        components: vec!["address".to_string(), "city".to_string()],
                    },
                    direction: IndexDirection::Ascending,
                },
                OrderedIndexField {
                    path: "score".into(),
                    direction: IndexDirection::Descending,
                },
            ]),
            options: IndexOptions {
                name: Some("by_address_and_score".to_string()),
            },
            created_at: 1627846261,
        };

        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn test_drop_index_serialization() {
        let edit = ManifestEdit::DropIndex {
            collection_id: 10,
            index_id: 2,
            dropped_at: 1627846262,
        };

        check_edit_serialization_roundtrip(edit, MANIFEST_FORMAT_VERSION);
    }

    #[test]
    fn test_apply_create_and_drop_collection() {
        let tree = ManifestState::new(1, 2, 3);

        let tree = tree.apply(&ManifestEdit::CreateCollection {
            name: "docs".to_string(),
            id: 10,
            created_at: 1000,
            options: CollectionOptions::default(),
        });
        let tree = flush_pending_catalog_edits(tree, 1000);

        assert_eq!(
            Some(Arc::new(CollectionMetadata::new(
                10,
                "docs",
                1000,
                CollectionOptions::default()
            ))),
            tree.catalog.get_collection_by_name(&"docs".to_string())
        );

        let tree = tree.apply(&ManifestEdit::DropCollection {
            id: 10,
            dropped_at: 2000,
        });
        let tree = flush_pending_catalog_edits(tree, 2000);
        assert_eq!(
            None,
            tree.catalog.get_collection_by_name(&"docs".to_string())
        );
    }

    #[test]
    fn test_apply_wal_and_manifest_rotation() {
        let tree = ManifestState::new(1, 2, 3);

        let tree = tree.apply(&ManifestEdit::WalRotation {
            log_number: 99,
            next_seq: 567,
        });
        assert_eq!(tree.lsm.current_log_number, 99);
        assert_eq!(tree.lsm.next_file_number, 100);

        let tree = tree.apply(&ManifestEdit::ManifestRotation {
            manifest_number: 150,
        });
        assert_eq!(tree.lsm.next_file_number, 151);
    }

    #[test]
    fn test_apply_files_detected_on_restart() {
        let tree = ManifestState::new(1, 2, 3);
        let tree = tree.apply(&ManifestEdit::FilesDetectedOnRestart {
            next_file_number: 200,
        });
        assert_eq!(tree.lsm.next_file_number, 200);
    }

    fn record_key(number: i32) -> Vec<u8> {
        let user_key = Bson::Int32(number).try_into_key().unwrap();
        encode_record_key(1, 0, &user_key)
    }

    fn flush_pending_catalog_edits(
        state: ManifestState,
        max_sequence_number: u64,
    ) -> ManifestState {
        state.apply(&ManifestEdit::Flush {
            oldest_log_number: state.lsm.oldest_log_number,
            sst: Arc::new(SSTableMetadata::new(
                1,
                0,
                &record_key(1),
                &record_key(1),
                max_sequence_number,
                max_sequence_number,
                1,
            )),
            count_stats: CountStats::default(),
        })
    }

    pub fn check_edit_serialization_roundtrip(edit: ManifestEdit, version: u32) {
        let bytes = edit.to_vec(version);
        let parsed =
            ManifestEdit::try_from_vec(&bytes, version).expect("Deserialization should succeed");
        assert_eq!(&edit, &parsed);
    }
}
