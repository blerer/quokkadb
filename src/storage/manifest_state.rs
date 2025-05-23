use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::catalog::{Catalog, CollectionMetadata};
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

    pub fn catalogue(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn apply(&self, edit: &ManifestEdit) -> Self {
        match edit {
            ManifestEdit::WalRotation { log_number } => ManifestState {
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
            ManifestEdit::CreateCollection { name, id } => ManifestState {
                lsm: self.lsm.clone(),
                catalog: Arc::new(self.catalog.add_collection(name, *id)),
            },
            ManifestEdit::DropCollection { name } => ManifestState {
                lsm: self.lsm.clone(),
                catalog: Arc::new(self.catalog.drop_collection(name)),
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
            }
        }
    }

    pub fn find<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.lsm.find(record_key, snapshot)
    }
}

impl SnapshotElement for ManifestState {
    fn read_from(reader: &ByteReader) -> Result<Self> {
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

/// Represent an element of the snapshots persisted at the beginning of the Manifest file.
/// A snapshot represent the LSM tree at a given point in time. It does not contain the Memtables
/// as their content is preserved by the write ahead log.
pub trait SnapshotElement {
    /// Deserialized the element from the specified ByteReader
    fn read_from(reader: &ByteReader) -> Result<Self>
    where
        Self: Sized;

    /// Serialize the element into the specified ByteWriter
    fn write_to(&self, writer: &mut ByteWriter);
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
    CreateCollection { name: String, id: u32 },

    /// Removes a collection from the catalog.
    DropCollection { name: String },

    /// Indicates a new WAL file has been created.
    WalRotation { log_number: u64 },

    /// Indicates a new manifest file has been created.
    ManifestRotation { manifest_number: u64 },

    /// Records a flush of a memtable into an SSTable.
    Flush {
        oldest_log_number: u64,
        sst: Arc<SSTableMetadata>,
    },

    /// Updates file number tracking based on files detected during recovery.
    FilesDetectedOnRestart { next_file_number: u64 },
}

impl ManifestEdit {
    pub fn to_vec(&self) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        match self {
            ManifestEdit::Snapshot(tree) => {
                writer.write_u8(0);
                tree.write_to(&mut writer);
            }
            ManifestEdit::CreateCollection { name, id } => {
                writer
                    .write_u8(1)
                    .write_str(&name)
                    .write_varint_u64(*id as u64);
            }
            ManifestEdit::DropCollection { name } => {
                writer.write_u8(2).write_str(&name);
            }
            ManifestEdit::WalRotation { log_number } => {
                writer.write_u8(3).write_varint_u64(*log_number);
            }
            ManifestEdit::ManifestRotation { manifest_number } => {
                writer.write_u8(4).write_varint_u64(*manifest_number);
            }
            ManifestEdit::Flush {
                oldest_log_number,
                sst,
            } => {
                writer.write_u8(5).write_varint_u64(*oldest_log_number);
                sst.write_to(&mut writer);
            }
            ManifestEdit::FilesDetectedOnRestart { next_file_number } => {
                writer.write_u8(6).write_varint_u64(*next_file_number);
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
                let id = reader.read_varint_u64()? as u32;
                Ok(ManifestEdit::CreateCollection { name, id })
            }
            2 => {
                let name = reader.read_str()?.to_string();
                Ok(ManifestEdit::DropCollection { name })
            }
            3 => {
                let log_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::WalRotation { log_number })
            }
            4 => {
                let manifest_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::ManifestRotation { manifest_number })
            }
            5 => {
                let oldest_log_number = reader.read_varint_u64()?;
                let sst = Arc::new(SSTableMetadata::read_from(&reader)?);
                Ok(ManifestEdit::Flush {
                    oldest_log_number,
                    sst,
                })
            }
            6 => {
                let next_file_number = reader.read_varint_u64()?;
                Ok(ManifestEdit::FilesDetectedOnRestart { next_file_number })
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("ManifestEdit: {}", edit),
            )),
        }
    }
}

use std::fmt;

impl fmt::Display for ManifestEdit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestEdit::Snapshot(state) => write!(f, "Snapshot({:?})", state),
            ManifestEdit::CreateCollection { name, id } => {
                write!(f, "CreateCollection {{ name: {}, id: {} }}", name, id)
            }
            ManifestEdit::DropCollection { name } => {
                write!(f, "DropCollection {{ name: {} }}", name)
            }
            ManifestEdit::WalRotation { log_number } => {
                write!(f, "WalRotation {{ log_number: {} }}", log_number)
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::encode_record_key;
    use crate::util::bson_utils::BsonKey;
    use crate::Collection;
    use bson::Bson;
    use std::sync::Arc;

    #[test]
    fn test_create_and_drop_collection_serialization() {
        let edit = ManifestEdit::CreateCollection {
            name: "my_collection".to_string(),
            id: 42,
        };
        check_edit_serialization_roundtrip(edit);

        let edit = ManifestEdit::DropCollection {
            name: "my_collection".to_string(),
        };
        check_edit_serialization_roundtrip(edit);
    }

    #[test]
    fn test_wal_and_manifest_rotation_serialization() {
        check_edit_serialization_roundtrip(ManifestEdit::WalRotation { log_number: 123 });
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
        });

        assert_eq!(
            Some(Arc::new(CollectionMetadata::new(10, "docs"))),
            tree.catalog.get_collection(&"docs".to_string())
        );

        let tree = tree.apply(&ManifestEdit::DropCollection {
            name: "docs".to_string(),
        });
        assert_eq!(None, tree.catalog.get_collection(&"docs".to_string()));
    }

    #[test]
    fn test_apply_wal_and_manifest_rotation() {
        let tree = ManifestState::new(1, 2);

        let tree = tree.apply(&ManifestEdit::WalRotation { log_number: 99 });
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

#[cfg(test)]
pub fn check_serialization_round_trip<T>(element: T)
where
    T: Debug + PartialEq + SnapshotElement,
{
    // Serialize the element.
    let mut writer = ByteWriter::new();
    element.write_to(&mut writer);
    let bytes = writer.take_buffer();

    // Deserialize the element from the serialized bytes.
    let reader = ByteReader::new(&bytes);
    let deserialized = SnapshotElement::read_from(&reader).expect("Deserialization should succeed");

    assert_eq!(element, deserialized);

    // Re-serialize the deserialized element.
    let mut writer2 = ByteWriter::new();
    deserialized.write_to(&mut writer2);
    let bytes2 = writer2.take_buffer();

    // Verify that the round-trip serialization is lossless.
    assert_eq!(bytes, bytes2);
}
