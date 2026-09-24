mod append_log;
mod callback;
pub(crate) mod catalog;
mod compaction;
pub(crate) mod count_stats;
mod files;
mod flush_manager;
pub(crate) mod index_build_state;
pub(crate) mod internal_key;
mod iterators;
mod lsm_tree;
mod lsm_version;
mod manifest;
mod manifest_state;
mod memtable;
pub(crate) mod operation;
pub(crate) mod snapshot_manager;
mod sstable;
pub(crate) mod storage_engine;
#[cfg(test)]
pub(crate) mod test_utils;
mod wal;
pub(crate) mod write_batch;

/// Number of collection IDs reserved for QuokkaDB's internal collections.
///
/// User-created collections start at [`FIRST_USER_COLLECTION_ID`]. Internal
/// collections are stored directly by their reserved ID and are not catalog
/// entries exposed through the public collection APIs.
pub(crate) const RESERVED_COLLECTION_ID_COUNT: u32 = 10;

/// The first collection ID available to user-created collections.
pub(crate) const FIRST_USER_COLLECTION_ID: u32 = RESERVED_COLLECTION_ID_COUNT;

/// Reserved collection used for persisted index-build state.
pub(crate) const INTERNAL_INDEX_BUILD_COLLECTION_ID: u32 = 0;

#[derive(Clone, Debug, PartialEq)]
pub enum Direction {
    Forward,
    Reverse,
}
