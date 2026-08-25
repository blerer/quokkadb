use bson::Document;
use quokkadb::options::options::Options;
use quokkadb::options::storage_quantity::{StorageQuantity, StorageUnit};
use quokkadb::QuokkaDB;
use std::path::Path;
use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageLayout {
    MemtableOnly,
    FlushedFirstHalfThenMemtable,
    TwoSstables,
    CompactedTwoSstables,
}

#[allow(dead_code)]
impl StorageLayout {
    pub const ALL: [StorageLayout; 4] = [
        StorageLayout::MemtableOnly,
        StorageLayout::FlushedFirstHalfThenMemtable,
        StorageLayout::TwoSstables,
        StorageLayout::CompactedTwoSstables,
    ];

    pub fn name(self) -> &'static str {
        match self {
            StorageLayout::MemtableOnly => "memtable_only",
            StorageLayout::FlushedFirstHalfThenMemtable => "flushed_first_half_then_memtable",
            StorageLayout::TwoSstables => "two_sstables",
            StorageLayout::CompactedTwoSstables => "compacted_two_sstables",
        }
    }
}

#[allow(dead_code)]
#[cfg(feature = "internal-testing")]
pub fn test_storage_layouts() -> &'static [StorageLayout] {
    &StorageLayout::ALL
}

#[allow(dead_code)]
#[cfg(not(feature = "internal-testing"))]
pub fn test_storage_layouts() -> &'static [StorageLayout] {
    // Storage-sensitive tests opt into the full matrix, but the default build keeps them on the
    // baseline memtable path so ordinary integration runs stay fast.
    &[StorageLayout::MemtableOnly]
}

pub fn init_tracing() {
    static TEST_TRACING: OnceLock<()> = OnceLock::new();

    TEST_TRACING.get_or_init(|| {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("quokkadb=trace"));

        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_test_writer()
            .try_init();
    });
}

pub fn open_db(path: &Path) -> QuokkaDB {
    init_tracing();
    QuokkaDB::open(path).unwrap()
}

#[allow(dead_code)]
pub fn open_db_for_layout(path: &Path, layout: StorageLayout) -> QuokkaDB {
    init_tracing();
    QuokkaDB::open_with_options(path, options_for_layout(layout)).unwrap()
}

#[allow(dead_code)]
pub fn open_db_with_seed_data(
    path: &Path,
    collection_name: &str,
    documents: &[Document],
    layout: StorageLayout,
) -> QuokkaDB {
    let db = open_db_for_layout(path, layout);
    seed_collection(&db, collection_name, documents, layout);
    db
}

#[allow(dead_code)]
pub fn seed_collection(
    db: &QuokkaDB,
    collection_name: &str,
    documents: &[Document],
    layout: StorageLayout,
) {
    let collection = db.collection(collection_name).create_if_missing();
    let midpoint = documents.len().saturating_add(1) / 2;

    match layout {
        StorageLayout::MemtableOnly => {
            insert_documents(&collection, documents);
        }
        StorageLayout::FlushedFirstHalfThenMemtable => {
            insert_documents(&collection, &documents[..midpoint]);
            flush_db(db);
            insert_documents(&collection, &documents[midpoint..]);
        }
        StorageLayout::TwoSstables => {
            insert_documents(&collection, &documents[..midpoint]);
            flush_db(db);
            insert_documents(&collection, &documents[midpoint..]);
            flush_db(db);
        }
        StorageLayout::CompactedTwoSstables => {
            disable_auto_compaction(db, layout);
            insert_documents(&collection, &documents[..midpoint]);
            flush_db(db);
            insert_documents(&collection, &documents[midpoint..]);
            flush_db(db);
            compact_db(db);
        }
    }
}

fn insert_documents(collection: &quokkadb::collection::Collection, documents: &[Document]) {
    if documents.is_empty() {
        return;
    }

    collection.insert_many(documents.to_vec()).unwrap();
}

fn options_for_layout(layout: StorageLayout) -> Options {
    match layout {
        StorageLayout::CompactedTwoSstables => Options::default()
            .with_level0_file_num_compaction_trigger(2)
            .with_max_bytes_for_level_base(StorageQuantity::new(1, StorageUnit::Kibibytes)),
        StorageLayout::MemtableOnly
        | StorageLayout::FlushedFirstHalfThenMemtable
        | StorageLayout::TwoSstables => Options::default(),
    }
}

#[cfg(feature = "internal-testing")]
fn flush_db(db: &QuokkaDB) {
    db.test_control().flush().unwrap();
}

#[cfg(not(feature = "internal-testing"))]
fn flush_db(_: &QuokkaDB) {
    panic!(
        "Storage layouts with flushed SSTables require building tests with `--features internal-testing`"
    );
}

#[cfg(feature = "internal-testing")]
fn compact_db(db: &QuokkaDB) {
    db.test_control().compact().unwrap();
}

#[cfg(not(feature = "internal-testing"))]
fn compact_db(_: &QuokkaDB) {
    panic!("Compacted storage layouts require building tests with `--features internal-testing`");
}

#[cfg(feature = "internal-testing")]
fn disable_auto_compaction(db: &QuokkaDB, _: StorageLayout) {
    db.test_control().disable_auto_compaction();
}

#[cfg(not(feature = "internal-testing"))]
fn disable_auto_compaction(_: &QuokkaDB, layout: StorageLayout) {
    panic!(
        "Storage layout `{}` requires building tests with `--features internal-testing`",
        layout.name()
    );
}
