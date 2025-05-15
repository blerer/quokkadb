use std::fs;
use std::fs::{remove_file, File};
use std::path::{Path, PathBuf};
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::sync::Arc;
use crate::io::{file_name_as_str, sync_dir};
use crate::io::byte_reader::ByteReader;
use crate::options::options::DatabaseOptions;
use crate::statistics::ServerStatistics;
use crate::storage::files::DbFile;
use crate::storage::log::{Log, LogFileCreator, LogObserver};
use crate::storage::manifest_state::{ManifestEdit, ManifestState};

const MANIFEST_MAGIC_NUMBER: u32 = 0x516D616E; // "Qman" in ASCII

pub struct Manifest {
    log: Log<ManifestFileCreator>,
    db_dir: PathBuf,
    rotation_threshold: u64, // When reached, trigger rotation
    /// The server statistics
    stats: Arc<ServerStatistics>
}

impl Manifest {
    pub fn new(db_dir: &Path, options: &DatabaseOptions, manifest_id: u64, snapshot: &ManifestEdit, stats: Arc<ServerStatistics>) -> Result<Self> {
        debug_assert!(matches!(snapshot, ManifestEdit::Snapshot(_)));
        let log = Log::new(db_dir, DbFile::new_manifest(manifest_id), ManifestFileCreator::new(), ManifestObserver::new(stats.clone()))?;
        let log_filename = log.filename().ok_or_else(|| Error::new(ErrorKind::NotFound, "Could not retrieve the Manifest filename"))?;
        let mut manifest = Manifest {
            log,
            db_dir: db_dir.to_path_buf(),
            rotation_threshold: options.max_manifest_file_size.to_bytes() as u64,
            stats
        };
        manifest.append_edit(&snapshot)?;
        Self::update_current_file(db_dir, &log_filename)?;
        Ok(manifest)
    }

    pub fn load_from(manifest_path: PathBuf, options: &DatabaseOptions, stats: Arc<ServerStatistics>) -> Result<Self> {
        let db_dir = manifest_path.parent().ok_or(Error::new(ErrorKind::NotFound, "Invalid manifest path"))?
                                                    .to_path_buf();

        let log = Log::load_from(&manifest_path, ManifestFileCreator::new(), ManifestObserver::new(stats.clone()))?;
        Ok(Manifest {
            log,
            db_dir,
            rotation_threshold: options.max_manifest_file_size.to_bytes() as u64,
            stats
        })
    }

    // Append a version edit to the manifest.
    pub fn append_edit(&mut self, edit: &ManifestEdit) -> Result<()> {
        self.log.append(&edit.to_vec())?;
        self.log.sync()?;
        self.stats.manifest_writes.inc(1);
        Ok(())
    }

    pub fn should_rotate(&self) -> bool {
        self.log.log_file_size() > self.rotation_threshold
    }

    // Rotation logic: flush current file, create new manifest, and update CURRENT pointer.
    pub fn rotate(&mut self, new_manifest_number: u64, snapshot: &ManifestEdit) -> Result<()> {

        let (new_file, old_file) = self.log.rotate(DbFile::new_manifest(new_manifest_number))?;
        self.append_edit(snapshot)?;

        // Update the CURRENT pointer file to point to the new manifest.
        // If a crash occurs before the CURRENT file is updated, the new manifest will be ignored
        // on restart and will have to be removed manually.
        Self::update_current_file(&self.db_dir, file_name_as_str(&new_file).unwrap())?;


        self.stats.manifest_size.dec(old_file.metadata()?.len());
        self.stats.manifest_rewrite.inc(1);

        // If we reached that point we can safely delete the old manifest.
        remove_file(old_file)?;
        sync_dir(&self.db_dir)?;


        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.log.sync()?;
        Ok(())
    }

    /// Updates the `CURRENT` file with the new manifest name using an atomic rename.
    fn update_current_file(db_dir: &Path, manifest_filename: &str) -> Result<()> {
        let current_path = db_dir.join("CURRENT");
        let temp_path = db_dir.join("CURRENT.tmp");

        // Step 1: Write the manifest filename to a temp file
        {
            let mut temp_file = File::create(&temp_path)?;
            writeln!(temp_file, "{}", manifest_filename)?;
            temp_file.sync_all()?;
        }

        // Step 2: Atomically replace CURRENT with the temp file
        fs::rename(&temp_path, &current_path)?;
        sync_dir(&db_dir)?;

        Ok(())
    }

    /// Reads the CURRENT file to find the active MANIFEST filename.
    pub fn read_current_file(db_dir: &Path) -> Result<Option<PathBuf>> {
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

    pub fn rebuild_manifest_state(manifest_path: &Path) -> Result<ManifestState> {
        const HEADER_SIZE: usize = 4 + 4 + 8; // MAGIC (u32) + VERSION (u32) + ID (u64)

        let (header, iter) = Log::<ManifestFileCreator>::read_log_file(manifest_path.to_path_buf(), HEADER_SIZE)?;
        let reader = ByteReader::new(&header);

        let magic = reader.read_u32_be()?;
        if magic != MANIFEST_MAGIC_NUMBER {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid manifest magic number"));
        }

        let _version = reader.read_u32_be()?; // Could be needed one day.
        let _id = reader.read_u64_be()?;

        let mut tree: Option<ManifestState> = None;

        for bytes in iter {
            let edit = ManifestEdit::try_from_vec(&bytes?)?;
            match edit {
                ManifestEdit::Snapshot(snapshot) => {
                    tree = Some(Arc::try_unwrap(snapshot).unwrap()); // We know that we are the only owner.
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

impl ManifestFileCreator {
    fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

impl LogFileCreator for ManifestFileCreator {
    type Observer = ManifestObserver;

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

struct ManifestObserver {
    stats: Arc<ServerStatistics>,
}

impl ManifestObserver {
    fn new(stats: Arc<ServerStatistics>) -> Arc<Self> {
        Arc::new(Self { stats })
    }
}

impl LogObserver for ManifestObserver {
    fn on_load(&self, bytes: u64) {
        self.stats.manifest_size.inc(bytes);
    }

    fn on_buffered(&self, _bytes: u64) {
        // Manifest flush and sync after each write, therefore, tracking buffered bytes does not make sense
    }

    fn on_bytes_written(&self, bytes: u64) {
        self.stats.manifest_bytes_written.inc(bytes);
    }

    fn on_sync(&self, bytes: u64) {
        self.stats.manifest_size.inc(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::sync::Arc;
    use bson::Bson;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::storage::internal_key::encode_record_key;
    use crate::storage::log::BUFFER_SIZE_IN_BYTES;
    use crate::storage::lsm_version::SSTableMetadata;
    use crate::util::bson_utils::BsonKey;

    #[test]
    fn test_rebuild_lsm_tree_from_valid_manifest() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let stats = ServerStatistics::new();
        let snapshot = ManifestEdit::Snapshot(Arc::new(ManifestState::new(1, 1)));
        let mut manifest = Manifest::new(path, &DatabaseOptions::default(), 1, &snapshot, stats).unwrap();

        let mut expected = match snapshot {
            ManifestEdit::Snapshot(snapshot) => Arc::try_unwrap(snapshot).unwrap(),
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
            ManifestEdit::CreateCollection { id: 1, name: "my_coll".to_string() },
            ManifestEdit::CreateCollection {id: 2, name: "my_other_coll".to_string() },
            ManifestEdit::Flush { oldest_log_number:  2, sst },
            ManifestEdit::Flush { oldest_log_number:  3, sst: sst2 },];

        for edit in edits {
            manifest.append_edit(&edit).unwrap();
            expected = expected.apply(&edit);
        }
        manifest.sync().unwrap();
        drop(manifest);

        let manifest_path = path.join("MANIFEST-000001");

        let rebuilt = Manifest::rebuild_manifest_state(&manifest_path.clone()).unwrap();
        assert_eq!(expected, rebuilt);
    }

    #[test]
    fn test_manifest_new_and_append_edit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("MANIFEST");

        let dir = tempdir().unwrap();
        let path = dir.path();
        let stats = ServerStatistics::new();
        let snapshot = ManifestEdit::Snapshot(Arc::new(ManifestState::new(1, 1)));

        let mut manifest = Manifest::new(path, &DatabaseOptions::default(), 1, &snapshot, stats).unwrap();

        let current_path = path.join("CURRENT");
        assert!(current_path.exists());
        assert_eq!(Manifest::read_current_file(&path).unwrap(), Some(path.join("MANIFEST-000001")));

        assert_eq!(manifest.stats.manifest_writes.get(), 1);
        let len = manifest.log.log_file_size();
        assert_eq!(manifest.stats.manifest_size.get(), len);
        assert_eq!(manifest.stats.manifest_bytes_written.get(), len);

        manifest.append_edit(&ManifestEdit::CreateCollection { id: 1, name: "my_coll".to_string() }).unwrap();

        assert_eq!(manifest.stats.manifest_writes.get(), 2);
        let len = manifest.log.log_file_size();
        assert_eq!(manifest.stats.manifest_size.get(), len);
        assert_eq!(manifest.stats.manifest_bytes_written.get(), len);
    }

    #[test]
    fn test_manifest_rotate() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let current_path = path.join("CURRENT");
        assert!(!current_path.exists());

        let stats = ServerStatistics::new();
        let snapshot = Arc::new(ManifestState::new(1, 1));
        let snapshot_edit = ManifestEdit::Snapshot(snapshot.clone());

        let options = DatabaseOptions::default().with_max_manifest_file_size(StorageQuantity::new(4120, StorageUnit::Bytes));
        let mut manifest = Manifest::new(path, &options, 1, &snapshot_edit, stats).unwrap();

        assert!(current_path.exists());
        assert_eq!(Manifest::read_current_file(&path).unwrap(), Some(path.join("MANIFEST-000001")));

        assert_eq!(manifest.should_rotate(), false);

        let edit = ManifestEdit::CreateCollection { id: 1, name: "my_coll".to_string() };
        manifest.append_edit(&edit).unwrap();

        assert_eq!(manifest.should_rotate(), true);

        assert_eq!(manifest.stats.manifest_writes.get(), 2);
        let len = manifest.log.log_file_size();

        assert_eq!(manifest.stats.manifest_size.get(), len);
        assert_eq!(manifest.stats.manifest_bytes_written.get(), len);

        let snapshot = Arc::new(snapshot.apply(&edit));

        let snapshot_edit = ManifestEdit::Snapshot(snapshot);

        manifest.rotate(5, &snapshot_edit).unwrap();

        let current_path = path.join("CURRENT");
        assert!(current_path.exists());
        assert_eq!(Manifest::read_current_file(&path).unwrap(), Some(path.join("MANIFEST-000005")));

        let old_manifest = path.join("MANIFEST-000001");
        assert!(!old_manifest.exists());

        assert_eq!(manifest.stats.manifest_writes.get(), 3);

        let len = manifest.log.log_file_size();
        let byte_written = len + (2 * BUFFER_SIZE_IN_BYTES as u64); // current file + old file (header, edit + padding)
        assert_eq!(manifest.stats.manifest_size.get(), len);
        assert_eq!(manifest.stats.manifest_bytes_written.get(), byte_written);
    }

    #[test]
    fn test_manifest_load() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let snapshot = Arc::new(ManifestState::new(1, 1));
        let snapshot_edit = ManifestEdit::Snapshot(snapshot.clone());

        let options = DatabaseOptions::default();
        let mut manifest = Manifest::new(path, &options, 1, &snapshot_edit, ServerStatistics::new()).unwrap();

        let edit = ManifestEdit::CreateCollection { id: 1, name: "my_coll".to_string() };
        manifest.append_edit(&edit).unwrap();
        drop(manifest);

        let manifest_path = path.join("MANIFEST-000001");
        let len = manifest_path.metadata().unwrap().len();

        let manifest = Manifest::load_from(manifest_path, &options, ServerStatistics::new()).unwrap();

        assert_eq!(manifest.stats.manifest_writes.get(), 0);
        assert_eq!(manifest.stats.manifest_size.get(), len);
        assert_eq!(manifest.stats.manifest_bytes_written.get(), 0);
    }

    fn record_key(collection: u32, id: i32) -> Vec<u8> {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        encode_record_key(collection, 0, &user_key)
    }
}