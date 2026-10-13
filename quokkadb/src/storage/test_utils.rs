use crate::storage::internal_key::encode_record_key;
use crate::storage::operation::Operation;
use crate::util::bson_utils::BsonKey;
use bson::{Bson, Document, doc};
use std::fmt::Debug;
use std::io::Result;

pub fn assert_next_entry_eq<
    K: Debug + PartialEq,
    V: Debug + PartialEq,
    I: Iterator<Item = Result<(K, V)>>,
>(
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

pub fn put_rec(
    collection: u32,
    user_key: i32,
    version: u32,
    sequence_num: u64,
) -> (Vec<u8>, Vec<u8>) {
    let op = put_op(collection, user_key, version);
    (op.internal_key(sequence_num), op.value().to_vec())
}
pub fn delete_rec(collection: u32, user_key: i32, sequence_num: u64) -> (Vec<u8>, Vec<u8>) {
    let op = delete_op(collection, user_key);
    (op.internal_key(sequence_num), op.value().to_vec())
}

pub fn put_op(collection: u32, user_key_val: i32, version: u32) -> Operation {
    let doc = document(user_key_val, version);
    Operation::new_put(collection, 0, user_key(user_key_val), doc.to_vec().unwrap())
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

pub mod fs_sync {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Error;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum SyncPoint {
        File,
        Directory,
    }

    struct State {
        fail_at: Option<SyncPoint>,
        events: Vec<SyncPoint>,
    }

    static HOOKS: Mutex<BTreeMap<PathBuf, State>> = Mutex::new(BTreeMap::new());

    pub(crate) struct SyncHook(PathBuf);

    impl SyncHook {
        pub(crate) fn new(directory: &Path, fail_at: Option<SyncPoint>) -> Self {
            let previous = HOOKS.lock().unwrap().insert(
                directory.to_path_buf(),
                State {
                    fail_at,
                    events: Vec::new(),
                },
            );
            assert!(previous.is_none(), "SST sync hook already registered");
            Self(directory.to_path_buf())
        }

        pub(crate) fn events(&self) -> Vec<SyncPoint> {
            HOOKS.lock().unwrap().get(&self.0).unwrap().events.clone()
        }
    }

    impl Drop for SyncHook {
        fn drop(&mut self) {
            HOOKS.lock().unwrap().remove(&self.0);
        }
    }

    pub(crate) fn before_sync(directory: &Path, point: SyncPoint) -> Result<()> {
        if let Some(state) = HOOKS.lock().unwrap().get_mut(directory) {
            state.events.push(point);
            if state.fail_at == Some(point) {
                return Err(Error::other(format!("Injected SST {point:?} sync error")));
            }
        }
        Ok(())
    }
}
