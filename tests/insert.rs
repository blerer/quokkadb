mod common;

use bson::{doc, Bson};
use quokkadb::collection::IdCreationStrategy;
use quokkadb::{error::Error, QuokkaDB};
use tempfile::tempdir;

fn setup() -> (tempfile::TempDir, QuokkaDB) {
    let dir = tempdir().unwrap();
    let db = common::open_db(dir.path());
    (dir, db)
}

// =============================================================================
// IdCreationStrategy::Mixed (default) tests
// =============================================================================

#[test]
fn mixed_strategy_generates_id_when_missing() {
    let (_dir, db) = setup();
    let collection = db.collection("test").create_if_missing();

    assert_eq!(collection.estimated_document_count().unwrap(), 0);

    // Insert without _id
    let doc = doc! {"x": 1};
    let result = collection.insert_one(doc).unwrap();

    assert_eq!(collection.estimated_document_count().unwrap(), 1);

    // Should have generated an _id
    let inserted_id = &result.inserted_id;
    assert!(matches!(inserted_id, Bson::Int64(_)));
    let generated_id = inserted_id.as_i64().unwrap();

    // Verify that the document can be retrieved using the generated _id
    let mut result = collection
        .find(doc! {"_id": generated_id})
        .limit(1)
        .execute()
        .unwrap();
    assert_eq!(
        result.next().unwrap().unwrap(),
        doc! {"_id": generated_id, "x": 1}
    );
}

#[test]
fn mixed_strategy_uses_provided_id() {
    let (_dir, db) = setup();
    let collection = db.collection("test").create_if_missing();

    // Insert with explicit _id
    let doc = doc! {"_id": 42, "x": 1};
    let result = collection.insert_one(doc).unwrap();

    // Should use the provided _id
    assert_eq!(result.inserted_id.as_i32().unwrap(), 42);
}

#[test]
fn mixed_strategy_insert_many_generates_ids_when_missing() {
    let (_dir, db) = setup();
    let collection = db.collection("test").create_if_missing();

    let docs = vec![doc! {"x": 1}, doc! {"_id": 100, "x": 2}, doc! {"x": 3}];
    let result = collection.insert_many(docs).unwrap();

    assert_eq!(collection.estimated_document_count().unwrap(), 3);

    let inserted_ids = &result.inserted_ids;
    assert_eq!(inserted_ids.len(), 3);

    // First and third should be generated (Int64), second should be provided (Int32)
    assert!(matches!(&inserted_ids[0], Bson::Int64(_)));
    assert_eq!(inserted_ids[1].as_i32().unwrap(), 100);
    assert!(matches!(&inserted_ids[2], Bson::Int64(_)));

    // Verify that the documents can be retrieved using the generated _id
    let first_id = inserted_ids[0].as_i64().unwrap();
    let last_id = inserted_ids[2].as_i64().unwrap();

    let mut result = collection
        .find(doc! {"_id": {"$in" : [first_id, 100, last_id]}})
        .sort(doc! { "x": 1 })
        .execute()
        .unwrap();

    assert_eq!(
        result.next().unwrap().unwrap(),
        doc! {"_id": first_id, "x": 1}
    );
    assert_eq!(result.next().unwrap().unwrap(), doc! {"_id": 100, "x": 2});
    assert_eq!(
        result.next().unwrap().unwrap(),
        doc! {"_id": last_id, "x": 3}
    );
}

// =============================================================================
// IdCreationStrategy::Generated tests
// =============================================================================

#[test]
fn generated_strategy_creates_id_automatically() {
    let (_dir, db) = setup();

    db.create_collection_with("generated_ids")
        .id_creation_strategy(IdCreationStrategy::Generated)
        .execute()
        .unwrap();

    let collection = db.collection("generated_ids");

    let doc = doc! {"x": 1};
    let result = collection.insert_one(doc).unwrap();

    // Should have generated an _id
    let inserted_id = &result.inserted_id;
    assert!(matches!(inserted_id, Bson::Int64(_)));

    // Verify that the document can be retrieved using the generated _id
    let generated_id = inserted_id.as_i64().unwrap();
    let mut result = collection
        .find(doc! {"_id": generated_id})
        .limit(1)
        .execute()
        .unwrap();
    assert_eq!(
        result.next().unwrap().unwrap(),
        doc! {"_id": generated_id, "x": 1}
    );
}

#[test]
fn generated_strategy_rejects_user_provided_id() {
    let (_dir, db) = setup();

    db.create_collection_with("generated_ids")
        .id_creation_strategy(IdCreationStrategy::Generated)
        .execute()
        .unwrap();

    let collection = db.collection("generated_ids");

    // Try to insert with explicit _id
    let doc = doc! {"_id": 42, "x": 1};
    let result = collection.insert_one(doc);

    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert!(
        format!("{}", error).contains("Cannot specify _id"),
        "Error message should mention cannot specify _id, got: {}",
        error
    );
}

#[test]
fn generated_strategy_insert_many_rejects_any_user_provided_id() {
    let (_dir, db) = setup();

    db.create_collection_with("generated_ids")
        .id_creation_strategy(IdCreationStrategy::Generated)
        .execute()
        .unwrap();

    let collection = db.collection("generated_ids");

    // Mix of docs with and without _id
    let docs = vec![
        doc! {"x": 1},
        doc! {"_id": 100, "x": 2}, // This should cause failure
        doc! {"x": 3},
    ];
    let result = collection.insert_many(docs);

    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert!(
        format!("{}", error).contains("Cannot specify _id"),
        "Error message should mention cannot specify _id, got: {}",
        error
    );
}

// =============================================================================
// IdCreationStrategy::Manual tests
// =============================================================================

#[test]
fn manual_strategy_requires_user_provided_id() {
    let (_dir, db) = setup();

    db.create_collection_with("manual_ids")
        .id_creation_strategy(IdCreationStrategy::Manual)
        .execute()
        .unwrap();

    let collection = db.collection("manual_ids");

    // Try to insert without _id
    let doc = doc! {"x": 1};
    let result = collection.insert_one(doc);

    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert!(
        format!("{}", error).contains("must contain an _id"),
        "Error message should mention _id requirement, got: {}",
        error
    );
}

#[test]
fn manual_strategy_accepts_user_provided_id() {
    let (_dir, db) = setup();

    db.create_collection_with("manual_ids")
        .id_creation_strategy(IdCreationStrategy::Manual)
        .execute()
        .unwrap();

    let collection = db.collection("manual_ids");

    let doc = doc! {"_id": "my-custom-id", "x": 1};
    let result = collection.insert_one(doc).unwrap();

    assert_eq!(result.inserted_id.as_str().unwrap(), "my-custom-id");
}

#[test]
fn manual_strategy_insert_many_rejects_missing_id() {
    let (_dir, db) = setup();

    db.create_collection_with("manual_ids")
        .id_creation_strategy(IdCreationStrategy::Manual)
        .execute()
        .unwrap();

    let collection = db.collection("manual_ids");

    let docs = vec![
        doc! {"_id": 1, "x": 1},
        doc! {"x": 2}, // Missing _id
        doc! {"_id": 3, "x": 3},
    ];
    let result = collection.insert_many(docs);

    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert!(
        format!("{}", error).contains("must contain an _id"),
        "Error message should mention _id requirement, got: {}",
        error
    );
}

#[test]
fn manual_strategy_insert_many_succeeds_with_all_ids() {
    let (_dir, db) = setup();

    db.create_collection_with("manual_ids")
        .id_creation_strategy(IdCreationStrategy::Manual)
        .execute()
        .unwrap();

    let collection = db.collection("manual_ids");

    let docs = vec![
        doc! {"_id": "a", "x": 1},
        doc! {"_id": "b", "x": 2},
        doc! {"_id": "c", "x": 3},
    ];
    let result = collection.insert_many(docs).unwrap();

    assert_eq!(collection.estimated_document_count().unwrap(), 3);

    let inserted_ids = &result.inserted_ids;
    assert_eq!(inserted_ids.len(), 3);
    assert_eq!(inserted_ids[0].as_str().unwrap(), "a");
    assert_eq!(inserted_ids[1].as_str().unwrap(), "b");
    assert_eq!(inserted_ids[2].as_str().unwrap(), "c");
}

#[test]
fn test_insert_many_manual_id_succeeds_after_delete_same_key() {
    let (_dir, db) = setup();

    db.create_collection_with("manual_ids")
        .id_creation_strategy(IdCreationStrategy::Manual)
        .execute()
        .unwrap();

    let collection = db.collection("manual_ids");

    collection.insert_one(doc! {"_id": 1, "x": 1}).unwrap();

    let delete_result = collection.delete_one(doc! {"_id": 1}).unwrap();
    assert_eq!(delete_result.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 0);

    let result = collection
        .insert_many(vec![doc! {"_id": 1, "x": 2}, doc! {"_id": 2, "x": 3}])
        .unwrap();

    assert_eq!(collection.estimated_document_count().unwrap(), 2);

    let inserted_ids = &result.inserted_ids;
    assert_eq!(inserted_ids.len(), 2);
    assert_eq!(inserted_ids[0].as_i32().unwrap(), 1);
    assert_eq!(inserted_ids[1].as_i32().unwrap(), 2);

    let docs: Vec<_> = collection
        .find(doc! {"_id": {"$in": [1, 2]}})
        .sort(doc! {"_id": 1})
        .execute()
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect();
    assert_eq!(docs, vec![doc! {"_id": 1, "x": 2}, doc! {"_id": 2, "x": 3}]);
}

#[test]
fn manual_strategy_detects_duplicate_ids() {
    let (_dir, db) = setup();

    db.create_collection_with("manual_ids")
        .id_creation_strategy(IdCreationStrategy::Manual)
        .execute()
        .unwrap();

    let collection = db.collection("manual_ids");

    // First insert succeeds
    collection.insert_one(doc! {"_id": 1, "x": 1}).unwrap();
    assert_eq!(collection.estimated_document_count().unwrap(), 1);

    // Second insert with same _id should fail
    let result = collection.insert_one(doc! {"_id": 1, "x": 2});
    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert!(
        format!("{}", error).contains("Duplicate key"),
        "Error message should mention duplicate key, got: {}",
        error
    );
    assert_eq!(collection.estimated_document_count().unwrap(), 1);
}

// =============================================================================
// Original duplicate key test
// =============================================================================

#[test]
fn insert_duplicate_id_is_version_conflict() {
    let (_dir, db) = setup();
    let collection = db.collection("test").create_if_missing();

    let doc = doc! {"_id": 365, "x": 1};

    // First insert should succeed.
    collection.insert_one(doc.clone()).unwrap();
    assert_eq!(collection.estimated_document_count().unwrap(), 1);

    // Second insert with the same _id should fail with VersionConflict.
    let result = collection.insert_one(doc);
    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert_eq!(
        format!("{}", error),
        "Duplicate key error. dup key: { _id: 365 }"
    );
    assert_eq!(collection.estimated_document_count().unwrap(), 1);
}

#[test]
fn insert_same_id_succeeds_after_delete() {
    let (_dir, db) = setup();
    let collection = db.collection("test").create_if_missing();

    collection.insert_one(doc! {"_id": 365, "x": 1}).unwrap();

    let delete_result = collection.delete_one(doc! {"_id": 365}).unwrap();
    assert_eq!(delete_result.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 0);

    let result = collection.insert_one(doc! {"_id": 365, "x": 2}).unwrap();
    assert_eq!(result.inserted_id.as_i32().unwrap(), 365);
    assert_eq!(collection.estimated_document_count().unwrap(), 1);

    let mut docs = collection.find(doc! {"_id": 365}).execute().unwrap();
    assert_eq!(docs.next().unwrap().unwrap(), doc! {"_id": 365, "x": 2});
    assert!(docs.next().is_none());
}

#[test]
fn insert_metrics_are_exposed_via_public_api() {
    let (_dir, db) = setup();
    let collection = db.collection("test").create_if_missing();

    collection.insert_one(doc! { "_id": 1, "x": 1 }).unwrap();
    collection
        .insert_many(vec![doc! { "_id": 2, "x": 2 }, doc! { "_id": 3, "x": 3 }])
        .unwrap();

    let metrics = db.metrics().executor();
    assert_eq!(metrics.write_queries(), 2);
    assert_eq!(metrics.documents_written(), 3);
    assert_eq!(metrics.write_query_duration().count(), 2);
    assert_eq!(metrics.read_queries(), 0);
    assert_eq!(metrics.rows_returned(), 0);
}
