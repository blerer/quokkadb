mod append_log;
mod callback;
pub(crate) mod catalog;
mod files;
mod flush_manager;
pub(crate) mod internal_key;
mod lsm_tree;
mod lsm_version;
mod manifest_state;
mod memtable;
pub(crate) mod operation;
mod sstable;
pub(crate) mod storage_engine;
mod wal;
pub(crate) mod write_batch;
mod compaction;
mod iterators;
mod manifest;
#[cfg(test)]
pub(crate) mod test_utils;

#[derive(Clone, Debug, PartialEq)]
pub enum Direction {
    Forward,
    Reverse,
}
