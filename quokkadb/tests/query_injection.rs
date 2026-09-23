mod common;

use bson::{Bson, Document, doc};
use quokkadb::error::Error;
use quokkadb::{QuokkaDB, QuokkaDocument};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

const OPERATOR_SHAPED_VALUE: &str = r#"{"$ne":"Alice"}"#;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    active: bool,
}

fn setup() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db.typed_collection::<User>("users").create_if_missing();

    collection
        .insert_many([
            User {
                id: 1,
                name: "Alice".to_string(),
                active: true,
            },
            User {
                id: 2,
                name: OPERATOR_SHAPED_VALUE.to_string(),
                active: true,
            },
            User {
                id: 3,
                name: "Bob".to_string(),
                active: true,
            },
        ])
        .unwrap();

    collection
        .create_index(|user| user.name.index_asc())
        .unwrap();
    (dir, db)
}

fn raw_ids(collection: &quokkadb::Collection, filter: Document) -> Vec<i64> {
    collection
        .find(filter)
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap()
        .into_iter()
        .map(|document| match document.get("_id").unwrap() {
            Bson::Int32(id) => i64::from(*id),
            Bson::Int64(id) => *id,
            value => panic!("unexpected _id value: {value:?}"),
        })
        .collect()
}

#[test]
fn typed_filters_treat_operator_shaped_values_as_literals() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let ids = collection
        .find(|user| user.name.eq(OPERATOR_SHAPED_VALUE))
        .sort(|user| user.id.asc())
        .select(|user| user.id)
        .execute_collect()
        .unwrap();

    assert_eq!(ids, vec![2]);
}

#[test]
fn document_filters_treat_operator_shaped_strings_as_literals() {
    let (_dir, db) = setup();
    let collection = db.collection("users");

    assert_eq!(
        raw_ids(&collection, doc! { "name": OPERATOR_SHAPED_VALUE }),
        vec![2]
    );
}

#[test]
fn typed_filters_cannot_broaden_updates_or_deletes() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let update = collection
        .update_many(
            |user| user.name.eq(OPERATOR_SHAPED_VALUE),
            |user| user.active.set(false),
        )
        .unwrap();
    assert_eq!(update.matched_count, 1);
    assert_eq!(update.modified_count, 1);
    assert!(
        collection
            .find_one(|user| user.id.eq(1_u64))
            .unwrap()
            .unwrap()
            .active
    );
    assert!(
        !collection
            .find_one(|user| user.id.eq(2_u64))
            .unwrap()
            .unwrap()
            .active
    );
    assert!(
        collection
            .find_one(|user| user.id.eq(3_u64))
            .unwrap()
            .unwrap()
            .active
    );

    let delete = collection
        .delete_many(|user| user.name.eq(OPERATOR_SHAPED_VALUE))
        .unwrap();
    assert_eq!(delete.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 2);
}

#[test]
fn document_filters_cannot_broaden_updates_or_deletes() {
    let (_dir, db) = setup();
    let collection = db.collection("users");

    let update = collection
        .update_many(
            doc! { "name": OPERATOR_SHAPED_VALUE },
            doc! { "$set": { "active": false } },
        )
        .unwrap();
    assert_eq!(update.matched_count, 1);
    assert_eq!(update.modified_count, 1);
    assert!(
        collection
            .find_one(doc! { "_id": 1 })
            .unwrap()
            .unwrap()
            .get_bool("active")
            .unwrap()
    );
    assert!(
        !collection
            .find_one(doc! { "_id": 2 })
            .unwrap()
            .unwrap()
            .get_bool("active")
            .unwrap()
    );
    assert!(
        collection
            .find_one(doc! { "_id": 3 })
            .unwrap()
            .unwrap()
            .get_bool("active")
            .unwrap()
    );

    let delete = collection
        .delete_many(doc! { "name": OPERATOR_SHAPED_VALUE })
        .unwrap();
    assert_eq!(delete.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 2);
}

#[test]
fn cached_filter_plans_bind_each_value_independently() {
    let (_dir, db) = setup();
    let collection = db.collection("users");

    assert_eq!(raw_ids(&collection, doc! { "name": "Alice" }), vec![1]);
    assert_eq!(
        raw_ids(&collection, doc! { "name": OPERATOR_SHAPED_VALUE }),
        vec![2]
    );

    let cache = db.metrics().query_cache();
    assert_eq!(cache.misses(), 1);
    assert_eq!(cache.hits(), 1);
}

#[test]
fn document_api_keeps_explicit_operators_and_rejects_unknown_ones() {
    let (_dir, db) = setup();
    let collection = db.collection("users");

    assert_eq!(
        raw_ids(&collection, doc! { "name": { "$ne": "Alice" } }),
        vec![2, 3]
    );

    let error = match collection
        .find(doc! { "name": { "$regex": "Alice" } })
        .execute()
    {
        Ok(_) => panic!("unsupported query operators must be rejected"),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::InvalidRequest(message) if message == "Unknown operator: $regex")
    );
}

#[test]
fn document_api_can_match_an_operator_shaped_document_with_eq() {
    let (_dir, db) = setup();
    let collection = db.collection("users");
    let literal = doc! { "$ne": "Alice" };

    collection
        .insert_one(doc! {
            "_id": 4,
            "metadata": literal.clone(),
        })
        .unwrap();

    assert_eq!(
        raw_ids(&collection, doc! { "metadata": { "$eq": literal } },),
        vec![4]
    );
}
