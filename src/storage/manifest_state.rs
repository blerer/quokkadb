use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::catalog::{Catalog, CollectionOptions};
use crate::util::interval::Interval;
use crate::storage::lsm_version::{LsmVersion, SSTableMetadata};
use std::fmt::Debug;
use std::io::{Error, ErrorKind, Result};
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
    pub fn new(current_log_number: u64, next_file_number: u64) -> Self {
        ManifestState {
            lsm: Arc::new(LsmVersion::new(current_log_number, next_file_number)),
            catalog: Arc::new(Catalog::new()),
        }
    }

    pub fn apply(&self, edit: &ManifestEdit) -> Self {
        match edit {
            ManifestEdit::WalRotation { log_number, next_seq: _next_seq } => ManifestState {
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
            ManifestEdit::CreateCollection { name, id, created_at , options} => ManifestState {
                lsm: self.lsm.clone(),
                catalog: Arc::new(self.catalog.add_collection_with_options(name,
                                                                           *id,
                                                                           *created_at,
                                                                           options.clone())),
            },
            ManifestEdit::DropCollection { id, dropped_at } => ManifestState {
                lsm: self.lsm.clone(),
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
                panic!("Snapshots should not be applied to an LSMTree");
            },
            ManifestEdit::IgnoringEmptyMemtable { oldest_log_number} => ManifestState {
                lsm: Arc::new(self.lsm.with_ignored_empty_memtable(*oldest_log_number)),
                catalog: self.catalog.clone(),
            }
        }
    }

    pub fn find<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
        min_snapshot: Option<u64>,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.lsm.find(record_key, snapshot, min_snapshot)
    }

    pub fn find_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.lsm.find_range(record_key_range, snapshot)
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
    CreateCollection { name: String, id: u32, created_at: u64, options: CollectionOptions},

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
    IgnoringEmptyMemtable {
        oldest_log_number: u64,
    },
}

impl ManifestEdit {
    pub fn to_vec(&self) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        match self {
            ManifestEdit::Snapshot(tree) => {
                writer.write_u8(0);
                tree.write_to(&mut writer);
            }
            ManifestEdit::CreateCollection { name, id, created_at, options } => {
                writer
                    .write_u8(1)
                    .write_str(&name)
                    .write_varint_u32(*id)
                    .write_varint_u64(*created_at);
                options.write_to(&mut writer);
            }
            ManifestEdit::DropCollection { id, dropped_at: drop_at } => {
                writer.write_u8(2).write_varint_u32(*id).write_varint_u64(*drop_at);
            }
            ManifestEdit::RenameCollection { id, new_name } => {
                writer.write_u8(3).write_varint_u32(*id).write_str(new_name);
            }
            ManifestEdit::WalRotation { log_number, next_seq } => {
                writer.write_u8(4).write_varint_u64(*log_number).write_varint_u64(*next_seq);
            }
            ManifestEdit::ManifestRotation { manifest_number } => {
                writer.write_u8(5).write_varint_u64(*manifest_number);
            }
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
            } => {
                writer.write_u8(6).write_varint_u64(*oldest_log_number);
                sst.write_to(&mut writer);
            }
            ManifestEdit::FilesDetectedOnRestart { next_file_number } => {
                writer.write_u8(7).write_varint_u64(*next_file_number);
            }
            ManifestEdit::IgnoringEmptyMemtable { oldest_log_number } => {
                writer.write_u8(8).write_varint_u64(*oldest_log_number);
            }
        }
        writer.take_buffer()
    }

    pub fn try_from_vec(input: &[u8]) -> Result<ManifestEdit> {
        let reader = ByteReader::new(input);
        let edit = reader.read_u8()?;
        match edit {
            0 => Ok(ManifestEdit::Snapshot(Arc::new(ManifestState::read_from(
                &reader,
            )?))),
            1 => {
                let name = reader.read_str()?.to_string();
                let id = reader.read_varint_u32()?;
                let created_at = reader.read_varint_u64()?;
                let options = CollectionOptions::read_from(&reader)?;
                Ok(ManifestEdit::CreateCollection { name, id, created_at, options })
            }
            2 => {
                let id = reader.read_varint_u32()?;
                let dropped_at = reader.read_varint_u64()?;
                Ok(ManifestEdit::DropCollection { id, dropped_at })
            }
            3 => {
                let id = reader.read_varint_u32()?;
                let new_name = reader.read_str()?.to_string();
                Ok(ManifestEdit::RenameCollection { id, new_name })
            }
            4 => {
                let log_number = reader.read_varint_u64()?;
                let next_seq = reader.read_varint_u64()?;
                Ok(ManifestEdit::WalRotation { log_number, next_seq })
            }
            5 => {
                let manifest_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::ManifestRotation { manifest_number })
            }
            6 => {
                let oldest_log_number = reader.read_varint_u64()?;
                let sst = Arc::new(SSTableMetadata::read_from(&reader)?);
                Ok(ManifestEdit::Flush {
                    oldest_log_number,
                    sst,
                })
            }
            7 => {
                let next_file_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::FilesDetectedOnRestart { next_file_number })
            }
            8 => {
                let oldest_log_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::IgnoringEmptyMemtable { oldest_log_number })
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("ManifestEdit: {}", edit),
            )),
        }
    }
}

use std::fmt;
use crate::io::serializable::Serializable;

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
                "Flush {{ oldest_log_number: {}, sst: {:?} }}",
                oldest_log_number, sst
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::encode_record_key;
    use crate::util::bson_utils::BsonKey;
    use bson::Bson;
    use std::sync::Arc;
    use crate::storage::catalog::{CollectionMetadata, CollectionOptions};

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
        let tree = ManifestState::new(1, 2);

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
        assert_eq!(tree.catalog.get_collection_by_name("new_name").unwrap().id, 10);
    }

    #[test]
    fn test_wal_and_manifest_rotation_serialization() {
        check_edit_serialization_roundtrip(ManifestEdit::WalRotation { log_number: 123, next_seq: 456 });
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
    fn test_apply_create_and_drop_collection() {
        let tree = ManifestState::new(1, 2);

        let tree = tree.apply(&ManifestEdit::CreateCollection {
            name: "docs".to_string(),
            id: 10,
            created_at: 1000,
            options: CollectionOptions::default(),
        });

        assert_eq!(
            Some(Arc::new(CollectionMetadata::new(10, "docs", 1000, CollectionOptions::default()))),
            tree.catalog.get_collection_by_name(&"docs".to_string())
        );

        let tree = tree.apply(&ManifestEdit::DropCollection {
            id: 10,
            dropped_at: 2000,
        });
        assert_eq!(None, tree.catalog.get_collection_by_name(&"docs".to_string()));
    }

    #[test]
    fn test_apply_wal_and_manifest_rotation() {
        let tree = ManifestState::new(1, 2);

        let tree = tree.apply(&ManifestEdit::WalRotation { log_number: 99, next_seq: 567 });
        assert_eq!(tree.lsm.current_log_number, 99);
        assert_eq!(tree.lsm.next_file_number, 100);

        let tree = tree.apply(&ManifestEdit::ManifestRotation {
            manifest_number: 150,
        });
        assert_eq!(tree.lsm.next_file_number, 151);
    }

    #[test]
    fn test_apply_files_detected_on_restart() {
        let tree = ManifestState::new(1, 2);
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
