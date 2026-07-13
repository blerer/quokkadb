use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::catalog::{Catalog, CollectionOptions, IndexDefinition, IndexOptions};
use crate::storage::lsm_version::{DropMetadata, LsmVersion, SSTableMetadata};
use crate::util::interval::Interval;
use std::fmt::Debug;
use std::io::Result;
use std::sync::Arc;

/// Represents a full snapshot of the database's durable state at a point in time.
///
/// `ManifestState` includes both physical state (`LsmVersion`) and logical schema
/// (`Catalog`). It is used for manifest snapshots and to apply manifest edits
/// deterministically during recovery.
#[derive(Debug, PartialEq)]
pub struct ManifestState {
    /// The persisted state of the LSM tree and WALs (excluding memtables).
    pub lsm: Arc<LsmVersion>,
    /// The catalog of collections and indexes.
    pub catalog: Arc<Catalog>,
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
            },
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
            } => ManifestState {
                lsm: Arc::new(self.lsm.with_flushed_sstable(*oldest_log_number, sst)),
                catalog: self.catalog.clone(),
            },
            ManifestEdit::CreateCollection {
                name,
                id,
                created_at,
                options,
            } => ManifestState {
                lsm: self.lsm.clone(),
                catalog: Arc::new(self.catalog.add_collection_with_options(
                    name,
                    *id,
                    *created_at,
                    options.clone(),
                )),
            },
            ManifestEdit::DropCollection { id, dropped_at } => ManifestState {
                lsm: Arc::new(self.lsm.add_collection_drop(*id, *dropped_at)),
                catalog: Arc::new(self.catalog.drop_collection(*id, *dropped_at)),
            },
            ManifestEdit::RenameCollection { id, new_name } => ManifestState {
                lsm: self.lsm.clone(),
                catalog: Arc::new(self.catalog.rename_collection(*id, new_name)),
            },
            ManifestEdit::FilesDetectedOnRestart { next_file_number } => ManifestState {
                lsm: Arc::new(self.lsm.adjust_file_number(*next_file_number)),
                catalog: self.catalog.clone(),
            },
            ManifestEdit::ManifestRotation { manifest_number } => ManifestState {
                lsm: Arc::new(self.lsm.manifest_rotation(*manifest_number)),
                catalog: self.catalog.clone(),
            },
            ManifestEdit::Snapshot(_) => {
                unreachable!("Snapshots should not be applied to an LSMTree");
            }
            ManifestEdit::IgnoringEmptyMemtable { oldest_log_number } => ManifestState {
                lsm: Arc::new(self.lsm.with_ignored_empty_memtable(*oldest_log_number)),
                catalog: self.catalog.clone(),
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
            },
            ManifestEdit::CreateIndex {
                collection_id,
                index_id,
                definition,
                options,
                created_at,
            } => ManifestState {
                lsm: self.lsm.clone(),
                catalog: Arc::new(self.catalog.add_index_to_collection(
                    *collection_id,
                    *index_id,
                    definition,
                    options,
                    *created_at,
                )),
            },
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => ManifestState {
                lsm: Arc::new(
                    self.lsm
                        .add_index_drop(*collection_id, *index_id, *dropped_at),
                ),
                catalog: Arc::new(
                    self.catalog
                        .drop_index(*collection_id, *index_id, *dropped_at),
                ),
            },
        }
    }

    /// Returns the drops with a sequence number smaller or equal to the given sequence_number.
    pub fn get_drops_before_or_at(&self, sequence_number: u64) -> Vec<Arc<DropMetadata>> {
        self.lsm.get_drops_before_or_at(sequence_number)
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
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        Ok(ManifestState {
            lsm: Arc::new(LsmVersion::read_from(reader)?),
            catalog: Arc::new(Catalog::read_from(reader)?),
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.lsm.write_to(writer);
        self.catalog.write_to(writer);
    }
}

/// Represents a single atomic change to the manifest state.
///
/// This enum is logged in the manifest and replayed at startup to reconstruct
/// the full `ManifestState`.
#[derive(Debug, PartialEq)]
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
    RenameCollection { id: u32, new_name: String },

    /// Indicates a new WAL file has been created.
    WalRotation { log_number: u64, next_seq: u64 },

    /// Indicates a new manifest file has been created.
    ManifestRotation { manifest_number: u64 },

    /// Records a flush of a memtable into an SSTable.
    Flush {
        oldest_log_number: u64,
        sst: Arc<SSTableMetadata>,
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
}

impl ManifestEdit {
    pub fn to_vec(&self) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        match self {
            ManifestEdit::Snapshot(tree) => {
                writer.write_u8(tags::SNAPSHOT);
                tree.write_to(&mut writer);
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
                options.write_to(&mut writer);
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
            ManifestEdit::RenameCollection { id, new_name } => {
                writer
                    .write_u8(tags::RENAME_COLLECTION)
                    .write_varint_u32(*id)
                    .write_str(new_name);
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
            } => {
                writer
                    .write_u8(tags::FLUSH)
                    .write_varint_u64(*oldest_log_number);
                sst.write_to(&mut writer);
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
                Vec::<Arc<SSTableMetadata>>::write_to(removed_sstables, &mut writer);
                Vec::<Arc<SSTableMetadata>>::write_to(added_sstables, &mut writer);
                Vec::<Arc<DropMetadata>>::write_to(drops, &mut writer);
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
                definition.write_to(&mut writer);
                options.write_to(&mut writer);
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
        }
        writer.take_buffer()
    }

    pub fn try_from_vec(input: &[u8]) -> Result<ManifestEdit> {
        let reader = ByteReader::new(input);
        let edit = reader.read_u8()?;
        match edit {
            tags::SNAPSHOT => Ok(ManifestEdit::Snapshot(Arc::new(ManifestState::read_from(
                &reader,
            )?))),
            tags::CREATE_COLLECTION => {
                let name = reader.read_str()?.to_string();
                let id = reader.read_varint_u32()?;
                let created_at = reader.read_varint_u64()?;
                let options = CollectionOptions::read_from(&reader)?;
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
                Ok(ManifestEdit::RenameCollection { id, new_name })
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
                let sst = Arc::new(SSTableMetadata::read_from(&reader)?);
                Ok(ManifestEdit::Flush {
                    oldest_log_number,
                    sst,
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
                let removed_sstables = Vec::<Arc<SSTableMetadata>>::read_from(&reader)?;
                let added_sstables = Vec::<Arc<SSTableMetadata>>::read_from(&reader)?;
                let drops = Vec::<Arc<DropMetadata>>::read_from(&reader)?;
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
                let definition = IndexDefinition::read_from(&reader)?;
                let options = IndexOptions::read_from(&reader)?;
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
            _ => Err(invalid_data(format!("ManifestEdit: {}", edit))),
        }
    }
}

use crate::io::invalid_data;
use crate::io::serializable::Serializable;
use std::fmt;

impl fmt::Display for ManifestEdit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestEdit::Snapshot(state) => write!(f, "Snapshot({:?})", state),
            ManifestEdit::CreateCollection { name, id, created_at, options } => {
                write!(f, "CreateCollection {{ name: {}, id: {}, created_at: {}, options: {} }}", name, id, created_at, options)
            }
            ManifestEdit::DropCollection { id, dropped_at } => {
                write!(f, "DropCollection {{ id: {}, dropped_at: {} }}", id, dropped_at)
            }
            ManifestEdit::RenameCollection { id, new_name } => {
                write!(f, "RenameCollection {{ id: {}, new_name: {} }}", id, new_name)
            }
            ManifestEdit::WalRotation { log_number, next_seq } => {
                write!(f, "WalRotation {{ log_number: {}, next_seq: {} }}", log_number, next_seq)
            }
            ManifestEdit::ManifestRotation { manifest_number } => write!(
                f,
                "ManifestRotation {{ manifest_number: {} }}",
                manifest_number
            ),
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
            } => write!(
                f,
                "Flush {{ oldest_log_number: {}, sst: {:?}}}",
                oldest_log_number, sst,
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
                created_at
            } => write!(
                f,
                "CreateIndex {{ collection_id: {}, index_id: {}, definition: {}, options: {:?}, created_at: {} }}",
                collection_id,
                index_id,
                definition,
                options,
                created_at
            ),
            ManifestEdit::DropIndex {
                collection_id,
                index_id,
                dropped_at,
            } => write!(
                f,
                "DropIndex {{ collection_id: {}, index_id: {}, dropped_at: {} }}",
                collection_id,
                index_id,
                dropped_at
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
    use std::sync::Arc;

    #[test]
    fn test_create_and_drop_collection_serialization() {
        let edit = ManifestEdit::CreateCollection {
            name: "my_collection".to_string(),
            id: 42,
            created_at: 1627846261,
            options: CollectionOptions::default(),
        };
        check_edit_serialization_roundtrip(edit);

        let edit = ManifestEdit::DropCollection {
            id: 42,
            dropped_at: 1627846262,
        };
        check_edit_serialization_roundtrip(edit);
    }

    #[test]
    fn test_rename_collection_serialization() {
        let edit = ManifestEdit::RenameCollection {
            id: 42,
            new_name: "new_name".to_string(),
        };
        check_edit_serialization_roundtrip(edit);
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

        assert!(tree.catalog.get_collection_by_name("old_name").is_some());
        assert!(tree.catalog.get_collection_by_name("new_name").is_none());

        let tree = tree.apply(&ManifestEdit::RenameCollection {
            id: 10,
            new_name: "new_name".to_string(),
        });

        assert!(tree.catalog.get_collection_by_name("old_name").is_none());
        assert!(tree.catalog.get_collection_by_name("new_name").is_some());
        assert_eq!(
            tree.catalog.get_collection_by_name("new_name").unwrap().id,
            10
        );
    }

    #[test]
    fn test_wal_and_manifest_rotation_serialization() {
        check_edit_serialization_roundtrip(ManifestEdit::WalRotation {
            log_number: 123,
            next_seq: 456,
        });
        check_edit_serialization_roundtrip(ManifestEdit::ManifestRotation {
            manifest_number: 456,
        });
    }

    #[test]
    fn test_files_detected_on_restart_serialization() {
        check_edit_serialization_roundtrip(ManifestEdit::FilesDetectedOnRestart {
            next_file_number: 789,
        });
    }

    #[test]
    fn test_flush_serialization() {
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
        };

        check_edit_serialization_roundtrip(edit);
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

        check_edit_serialization_roundtrip(edit);
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

        check_edit_serialization_roundtrip(edit);
    }

    #[test]
    fn test_drop_index_serialization() {
        let edit = ManifestEdit::DropIndex {
            collection_id: 10,
            index_id: 2,
            dropped_at: 1627846262,
        };

        check_edit_serialization_roundtrip(edit);
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

    pub fn check_edit_serialization_roundtrip(edit: ManifestEdit) {
        let bytes = edit.to_vec();
        let parsed = ManifestEdit::try_from_vec(&bytes).expect("Deserialization should succeed");
        assert_eq!(&edit, &parsed);
    }
}
