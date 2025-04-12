use std::fs;
use std::fs::{remove_file, File};
use std::path::{Path, PathBuf};
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::sync::Arc;
use crate::io::{file_name_as_str, sync_dir};
use crate::io::byte_reader::ByteReader;
use crate::options::options::DatabaseOptions;
use crate::storage::files::DbFile;
use crate::storage::log::{Log, LogFileCreator};
use crate::storage::lsm_tree::{LsmTree, LsmTreeEdit};

const MANIFEST_MAGIC_NUMBER: u32 = 0x516D616E; // "Qman" in ASCII

pub struct Manifest {
    log: Log,
    db_path: PathBuf,
    rotation_threshold: u64, // When reached, trigger rotation
}

impl Manifest {
    pub fn new(path: PathBuf, options: &DatabaseOptions, manifest_id: u64, snapshot: &LsmTreeEdit) -> Result<Self> {
        debug_assert!(matches!(snapshot, LsmTreeEdit::Snapshot(_)));
        let log = Log::new(path.clone(), DbFile::new_manifest(manifest_id), Arc::new(ManifestFileCreator{}))?;
        let log_filename = log.filename().ok_or_else(|| Error::new(ErrorKind::NotFound, "Could not retrieve the Manifest filename"))?;
        let mut manifest = Manifest {
            log,
            db_path: path.clone(),
            rotation_threshold: options.max_manifest_file_size().to_bytes() as u64,
        };
        manifest.append_edit(&snapshot)?;
        Self::update_current_file(&path, &log_filename)?;
        Ok(manifest)
    }

    pub fn load_from(manifest_path: PathBuf, options: &DatabaseOptions) -> Result<Self> {
        let file = DbFile::new(&manifest_path).ok_or(Error::new(ErrorKind::NotFound, "Invalid manifest path"))?;
        let db_path = manifest_path.parent().ok_or(Error::new(ErrorKind::NotFound, "Invalid manifest path"))?
                                                    .to_path_buf();

        let log = Log::new(db_path.clone(), file, Arc::new(ManifestFileCreator{}))?;
        Ok(Manifest {
            log,
            db_path,
            rotation_threshold: options.max_manifest_file_size().to_bytes() as u64,
        })
    }

    // Append a version edit to the manifest.
    pub fn append_edit(&mut self, edit: &LsmTreeEdit) -> Result<()> {
        self.log.append(&edit.to_vec())?;
        self.log.sync()?;
        Ok(())
    }

    pub fn should_rotate(&self) -> bool {
        self.log.log_file_size() > self.rotation_threshold
    }

    // Rotation logic: flush current file, create new manifest, and update CURRENT pointer.
    pub fn rotate(&mut self, new_manifest_id: u64, snapshot: &LsmTreeEdit) -> Result<()> {

        let (new_file, old_file) = self.log.rotate(DbFile::new_manifest(new_manifest_id))?;
        self.append_edit(snapshot)?;

        // Update the CURRENT pointer file to point to the new manifest.
        // If a crash occurs before the CURRENT file is updated, the new manifest will be ignored
        // on restart and will have to be removed manually.
        Self::update_current_file(&self.db_path, file_name_as_str(&new_file).unwrap())?;

        // If we reached that point we can safely delete the old manifest.
        remove_file(old_file)?;
        sync_dir(&self.db_path)?;

        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.log.sync()?;
        Ok(())
    }

    /// Updates the `CURRENT` file with the new manifest name using an atomic rename.
    fn update_current_file(db_path: &PathBuf, manifest_filename: &str) -> Result<()> {
        let current_path = db_path.join("CURRENT");
        let temp_path = db_path.join("CURRENT.tmp");

        // Step 1: Write the manifest filename to a temp file
        {
            let mut temp_file = File::create(&temp_path)?;
            writeln!(temp_file, "{}", manifest_filename)?;
            temp_file.sync_all()?;
        }

        // Step 2: Atomically replace CURRENT with the temp file
        fs::rename(&temp_path, &current_path)?;
        sync_dir(&db_path)?;

        Ok(())
    }

    /// Reads the CURRENT file to find the active MANIFEST filename.
    pub fn read_current_file(db_dir: &PathBuf) -> Result<Option<PathBuf>> {
        let current_path = db_dir.join("CURRENT");
        if !current_path.exists() {
            return Ok(None);
        }
        let mut file = File::open(&current_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let manifest_name = contents.trim();
        Ok(Some(db_dir.join(manifest_name)))
    }

    pub fn rebuild_lsm_tree(manifest_path: &Path) -> Result<LsmTree> {
        const HEADER_SIZE: usize = 4 + 4 + 8; // MAGIC (u32) + VERSION (u32) + ID (u64)

        let (header, iter) = Log::read_log_file(manifest_path.to_path_buf(), HEADER_SIZE)?;
        let reader = ByteReader::new(&header);

        let magic = reader.read_u32_be()?;
        if magic != MANIFEST_MAGIC_NUMBER {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid manifest magic number"));
        }

        let _version = reader.read_u32_be()?; // Could be needed one day.
        let _id = reader.read_u64_be()?;

        let mut tree: Option<LsmTree> = None;

        for bytes in iter {
            let edit = LsmTreeEdit::try_from_vec(&bytes?)?;
            match edit {
                LsmTreeEdit::Snapshot(snapshot) => {
                    tree = Some(snapshot);
                }
                _ => {
                    if let Some(t) = tree {
                        tree = Some(t.apply(&edit));
                    }
                }
            }
        }

        tree.ok_or(Error::new(ErrorKind::UnexpectedEof, format!("Invalid manifest file: {}", manifest_path.display())))
    }
}

struct ManifestFileCreator { }

impl LogFileCreator for ManifestFileCreator {

    fn header(&self, id: u64) -> Vec<u8> {

        /// The current version of the write-ahead log format.
        const MANIFEST_VERSION: u32 = 1;

        let mut vec = Vec::with_capacity(4 + 1 + 8);
        vec.extend_from_slice(&MANIFEST_MAGIC_NUMBER.to_be_bytes());
        vec.extend_from_slice(&MANIFEST_VERSION.to_be_bytes());
        vec.extend_from_slice(&id.to_be_bytes());
        vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::storage::lsm_tree::{LsmTreeEdit, LsmTree, SSTableMetadata};
    use std::sync::Arc;
    use bson::Bson;
    use crate::storage::internal_key::encode_record_key;
    use crate::util::bson_utils::BsonKey;

    #[test]
    fn test_rebuild_lsm_tree_from_valid_manifest() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let snapshot = LsmTreeEdit::Snapshot(LsmTree::new(1, 1));
        let mut manifest = Manifest::new(path.clone(), &DatabaseOptions::default(), 1, &snapshot).unwrap();

        let mut expected = match snapshot {
            LsmTreeEdit::Snapshot(snapshot) => snapshot,
            _ => unreachable!(),
        };

        let sst = Arc::new(SSTableMetadata::new(
            3,
            0,
            &record_key(1, 0),
            &record_key(1, 2567),
            0,
            2567
        ));

        let sst2 = Arc::new(SSTableMetadata::new(
            4,
            0,
            &record_key(2, 0),
            &record_key(1, 3800),
            2568,
            6489,
        ));

        let edits = vec![
            LsmTreeEdit::CreateCollection { id: 1, name: "my_coll".to_string() },
            LsmTreeEdit::CreateCollection {id: 2, name: "my_other_coll".to_string() },
            LsmTreeEdit::Flush {log_id:  1, sst },
            LsmTreeEdit::Flush {log_id:  2, sst: sst2 },];

        for edit in edits {
            manifest.append_edit(&edit).unwrap();
            expected = expected.apply(&edit);
        }
        manifest.sync().unwrap();
        drop(manifest);


        let manifest_path = path.join("MANIFEST-000001");

        let rebuilt = Manifest::rebuild_lsm_tree(&manifest_path.clone()).unwrap();
        assert_eq!(expected, rebuilt);
    }

    fn record_key(collection: u32, id: i32) -> Vec<u8> {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        encode_record_key(collection, 0, &user_key)
    }
}