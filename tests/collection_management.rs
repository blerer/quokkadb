use bson::doc;
use quokkadb::collection::{CreateCollectionOptions, IdCreationStrategy};
use quokkadb::error::Error;
use quokkadb::QuokkaDB;
use tempfile::tempdir;

#[test]
fn test_create_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].get_str("name").unwrap(), "users");
    assert_eq!(collections[0].get_str("type").unwrap(), "collection");
}

#[test]
fn test_create_collection_already_exists() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();

    let result = db.create_collection("users");
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::CollectionAlreadyExists(name) => {
            assert_eq!(name, "users");
        }
        e => panic!("Expected CollectionAlreadyExists error, got: {:?}", e),
    }
}

#[test]
fn test_create_collection_with_options() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let options = CreateCollectionOptions::builder()
        .id_creation_strategy(IdCreationStrategy::Generated)
        .build();
    db.create_collection_with_options("users", options).unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].get_str("name").unwrap(), "users");

    let options_doc = collections[0].get_document("options").unwrap();
    assert_eq!(options_doc.get_str("idCreationStrategy").unwrap(), "Generated");
}

#[test]
fn test_drop_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();
    assert_eq!(db.list_collections().len(), 1);

    db.drop_collection("users").unwrap();
    assert_eq!(db.list_collections().len(), 0);
}

#[test]
fn test_drop_collection_not_found() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let result = db.drop_collection("nonexistent");
    assert!(result.is_ok()); // Dropping a non-existent collection is a no-op
}

#[test]
fn test_rename_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("old_name").unwrap();
    db.rename_collection("old_name", "new_name").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].get_str("name").unwrap(), "new_name");
}

#[test]
fn test_rename_collection_preserves_data() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("old_name").unwrap();
    db.collection("old_name")
        .insert_one(doc! { "_id": 1, "name": "Alice" })
        .unwrap();
    db.collection("old_name")
        .insert_one(doc! { "_id": 2, "name": "Bob" })
        .unwrap();

    // Verify data exists under old name
    let results: Vec<_> = db
        .collection("old_name")
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 2);

    // Rename the collection
    db.rename_collection("old_name", "new_name").unwrap();

    // Data should NOT be found under old name
    let old_results: Vec<_> = db
        .collection("old_name")
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(old_results.is_empty());

    // Data SHOULD be found under new name
    let new_results: Vec<_> = db
        .collection("new_name")
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(new_results.len(), 2);

    let names: Vec<&str> = new_results
        .iter()
        .map(|d| d.get_str("name").unwrap())
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
}

#[test]
fn test_rename_collection_not_found() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let result = db.rename_collection("nonexistent", "new_name");
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::CollectionNotFound { name, .. } => {
            assert_eq!(name, "nonexistent");
        }
        e => panic!("Expected CollectionNotFound error, got: {:?}", e),
    }
}

#[test]
fn test_rename_collection_target_exists() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("source").unwrap();
    db.create_collection("target").unwrap();

    let result = db.rename_collection("source", "target");
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::CollectionAlreadyExists(name) => {
            assert_eq!(name, "target");
        }
        e => panic!("Expected CollectionAlreadyExists error, got: {:?}", e),
    }
}

#[test]
fn test_list_collections_empty() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let collections = db.list_collections();
    assert!(collections.is_empty());
}

#[test]
fn test_list_collections_multiple() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("alpha").unwrap();
    db.create_collection("beta").unwrap();
    db.create_collection("gamma").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 3);

    let names: Vec<&str> = collections
        .iter()
        .map(|c| c.get_str("name").unwrap())
        .collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
    assert!(names.contains(&"gamma"));
}

#[test]
fn test_list_collections_excludes_dropped() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("keep").unwrap();
    db.create_collection("drop_me").unwrap();
    db.drop_collection("drop_me").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].get_str("name").unwrap(), "keep");
}

#[test]
fn test_list_collections_metadata_format() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("test_collection").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);

    let col = &collections[0];
    assert_eq!(col.get_str("name").unwrap(), "test_collection");
    assert_eq!(col.get_str("type").unwrap(), "collection");

    let options = col.get_document("options").unwrap();
    assert!(options.get_str("idCreationStrategy").is_ok());

    let info = col.get_document("info").unwrap();
    assert!(info.get_i64("id").is_ok());
    assert!(info.get_i64("createdAt").is_ok());
}

#[test]
fn test_collection_data_isolated_after_drop_recreate() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    // Create collection and insert initial data
    db.create_collection("users").unwrap();
    db.collection("users")
        .insert_one(doc! { "_id": 1, "name": "Alice" })
        .unwrap();
    db.collection("users")
        .insert_one(doc! { "_id": 2, "name": "Bob" })
        .unwrap();

    let results: Vec<_> = db
        .collection("users")
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 2);

    // Drop and re-create the collection
    db.drop_collection("users").unwrap();
    db.create_collection("users").unwrap();

    // Old data should NOT be visible
    let results: Vec<_> = db
        .collection("users")
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(results.is_empty());

    // Insert new data after re-creation
    db.collection("users")
        .insert_one(doc! { "_id": 3, "name": "Charlie" })
        .unwrap();
    db.collection("users")
        .insert_one(doc! { "_id": 4, "name": "Diana" })
        .unwrap();

    // Only new data should be visible
    let results: Vec<_> = db
        .collection("users")
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 2);

    let names: Vec<&str> = results
        .iter()
        .map(|d| d.get_str("name").unwrap())
        .collect();
    assert!(names.contains(&"Charlie"));
    assert!(names.contains(&"Diana"));
    assert!(!names.contains(&"Alice"));
    assert!(!names.contains(&"Bob"));
}

#[test]
fn test_drop_recreate_same_id_allowed() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    // Create collection and insert document with specific _id
    db.create_collection("items").unwrap();
    db.collection("items")
        .insert_one(doc! { "_id": 100, "value": "original" })
        .unwrap();

    // Drop and re-create
    db.drop_collection("items").unwrap();
    db.create_collection("items").unwrap();

    // Should be able to insert a document with the same _id
    db.collection("items")
        .insert_one(doc! { "_id": 100, "value": "new" })
        .unwrap();

    let results: Vec<_> = db
        .collection("items")
        .find(doc! { "_id": 100 })
        .execute()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_str("value").unwrap(), "new");
}

#[test]
fn test_implicit_collection_creation_on_insert() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    assert!(db.list_collections().is_empty());

    db.collection("auto_created")
        .insert_one(doc! { "value": 42 })
        .unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].get_str("name").unwrap(), "auto_created");
}
