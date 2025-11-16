use crate::obs::metrics::MetricRegistry;
use crate::storage::storage_engine::{StorageEngine, StorageResult};
use std::fmt::Debug;
use std::io::Result;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use bson::{doc, to_vec, Bson, Document};
use crate::obs::logger::test_instance;
use crate::options::options::Options;
use crate::storage::internal_key::encode_record_key;
use crate::storage::operation::Operation;
use crate::util::bson_utils::BsonKey;

pub fn assert_next_entry_eq<K: Debug + PartialEq, V: Debug + PartialEq, I: Iterator<Item = Result<(K, V)>>>(
    iter: &mut I,
    expected: &(K, V),
) {
    match iter.next() {
        Some(Ok(ref actual)) => assert_eq!(actual, expected),
        Some(Err(e)) => panic!("Iterator returned error: {:?}", e),
        None => panic!("Expected {:?}, but iterator is exhausted", expected),
    }
}

pub fn record_key(collection: u32, user_key_val: i32) -> Vec<u8> {
    encode_record_key(collection, 0, &user_key(user_key_val))
}

pub fn user_key(user_key: i32) -> Vec<u8> {
    Bson::Int32(user_key).try_into_key().unwrap()
}

pub fn put_rec(collection: u32, user_key: i32, version: u32, sequence_num: u64) -> (Vec<u8>, Vec<u8>) {
    let op = put_op(collection, user_key, version);
    (op.internal_key(sequence_num), op.value().to_vec())
}
pub fn delete_rec(collection: u32, user_key: i32, sequence_num: u64) -> (Vec<u8>, Vec<u8>) {
    let op = delete_op(collection, user_key);
    (op.internal_key(sequence_num), op.value().to_vec())
}

pub fn put_op(collection: u32, user_key_val: i32, version: u32) -> Operation {
    let doc = document(user_key_val, version);
    Operation::new_put(collection, 0, user_key(user_key_val), to_vec(&doc).unwrap())
}

pub fn delete_op(collection: u32, user_key_val: i32) -> Operation {
    Operation::new_delete(collection, 0, user_key(user_key_val))
}

pub fn document(user_key: i32, version: u32) -> Document {
    doc! {
            "id": user_key,
            "version": version,
            "payload": format!("This is document {} version {}.", user_key, version)
        }
}

pub fn storage_engine() -> StorageResult<(Arc<StorageEngine>, TempDir)> {
    let dir = tempdir()?;
    let options = Arc::new(Options::lightweight());
    let mut metric_registry = MetricRegistry::new();
    let logger = test_instance();
    let storage_engine = StorageEngine::new(logger, &mut metric_registry, options, dir.path())?;
    Ok((storage_engine, dir))
}
