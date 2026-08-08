use bson::doc;
use quokkadb::collection::{IdCreationStrategy, IndexDirection};
use quokkadb::error::Error;
use quokkadb::{CollectionInfo, QuokkaDB};
use tempfile::tempdir;

#[test]
fn test_create_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "users");
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

    db.create_collection_with("users")
        .id_creation_strategy(IdCreationStrategy::Generated)
        .execute()
        .unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "users");
    assert_eq!(
        collections[0].id_creation_strategy,
        IdCreationStrategy::Generated
    );
}

#[test]
fn test_drop_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let collection = db.collection("users").create_if_missing();
    collection
        .insert_one(doc! { "_id": 1, "name": "Alice" })
        .unwrap();
    assert_eq!(db.list_collections().len(), 1);

    collection.drop_collection().unwrap();
    assert_eq!(db.list_collections().len(), 0);
}

#[test]
fn test_drop_collection_not_found() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let result = db.collection("nonexistent").drop_collection();
    assert!(result.is_ok()); // Dropping a non-existent collection is a no-op
}

#[test]
fn test_delete_one_create_if_missing_returns_zero_without_creating_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let result = db
        .collection("missing")
        .create_if_missing()
        .delete_one(doc! { "_id": 1 })
        .unwrap();

    assert_eq!(result.deleted_count, 0);
    assert!(db.list_collections().is_empty());
}

#[test]
fn test_rename_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("old_name").unwrap();
    let renamed = db.collection("old_name").rename("new_name").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "new_name");
    assert!(renamed.find(doc! {}).execute().is_ok());
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
    let renamed = db.collection("old_name").rename("new_name").unwrap();

    // The old handle is now strict by default, so querying the old name should error.
    match db.collection("old_name").find(doc! {}).execute() {
        Err(Error::CollectionNotFound { .. }) => {}
        Err(err) => panic!("Expected CollectionNotFound error, got: {:?}", err),
        Ok(_) => panic!("Expected CollectionNotFound error"),
    }

    // Data SHOULD be found under new name
    let new_results: Vec<_> = renamed
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

    match db.collection("nonexistent").rename("new_name") {
        Err(Error::CollectionNotFound { name, .. }) => {
            assert_eq!(name, "nonexistent");
        }
        Err(e) => panic!("Expected CollectionNotFound error, got: {:?}", e),
        Ok(_) => panic!("Expected CollectionNotFound error"),
    }
}

#[test]
fn test_rename_collection_target_exists() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("source").unwrap();
    db.create_collection("target").unwrap();

    match db.collection("source").rename("target") {
        Err(Error::CollectionAlreadyExists(name)) => {
            assert_eq!(name, "target");
        }
        Err(e) => panic!("Expected CollectionAlreadyExists error, got: {:?}", e),
        Ok(_) => panic!("Expected CollectionAlreadyExists error"),
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

    let names: Vec<&str> = collections.iter().map(|c| c.name.as_str()).collect();
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
    db.collection("drop_me").drop_collection().unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "keep");
}

#[test]
fn test_list_collections_metadata_shape() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("test_collection").unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);

    let col: &CollectionInfo = &collections[0];
    assert_eq!(col.name, "test_collection");
    assert_eq!(col.id_creation_strategy, IdCreationStrategy::Mixed);
    assert!(col.id > 0);
}

#[test]
fn test_get_indexes_returns_active_indexes() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();
    let collection = db.collection("users");

    let default_name = collection.create_index(doc! { "name": 1 }).unwrap();
    let custom_name = collection
        .create_index_with(doc! { "age": -1, "email": 1 })
        .name("by_age_email")
        .execute()
        .unwrap();

    let indexes = collection.list_indexes().unwrap();
    assert_eq!(indexes.len(), 2);

    let by_name = indexes
        .iter()
        .find(|index| index.name == default_name)
        .unwrap();
    assert_eq!(by_name.fields.len(), 1);
    assert_eq!(by_name.fields[0].path, "name");
    assert_eq!(by_name.fields[0].direction, IndexDirection::Ascending);

    let by_age_email = indexes
        .iter()
        .find(|index| index.name == custom_name)
        .unwrap();
    assert_eq!(by_age_email.fields.len(), 2);
    assert_eq!(by_age_email.fields[0].path, "age");
    assert_eq!(by_age_email.fields[0].direction, IndexDirection::Descending);
    assert_eq!(by_age_email.fields[1].path, "email");
    assert_eq!(by_age_email.fields[1].direction, IndexDirection::Ascending);
}

#[test]
fn test_get_indexes_is_strict_by_default() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let err = db.collection("missing").list_indexes().unwrap_err();
    match err {
        Error::CollectionNotFound { name, id } => {
            assert_eq!(name, "missing");
            assert_eq!(id, None);
        }
        other => panic!("Expected CollectionNotFound error, got: {:?}", other),
    }
}

#[test]
fn test_get_indexes_create_if_missing_returns_empty_for_missing_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let indexes = db
        .collection("missing")
        .create_if_missing()
        .list_indexes()
        .unwrap();
    assert!(indexes.is_empty());
}

#[test]
fn test_drop_index_removes_existing_index() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();
    let collection = db.collection("users");

    let index_name = collection.create_index(doc! { "name": 1 }).unwrap();

    assert_eq!(collection.list_indexes().unwrap().len(), 1);
    collection.drop_index(&index_name).unwrap();
    assert!(collection.list_indexes().unwrap().is_empty());

    let err = collection.drop_index(&index_name).unwrap_err();
    assert!(matches!(err, Error::IndexNotFound { .. }));
}

#[test]
fn test_drop_index_returns_collection_not_found_for_missing_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let err = db.collection("missing").drop_index("name_1").unwrap_err();
    match err {
        Error::CollectionNotFound { name, id } => {
            assert_eq!(name, "missing");
            assert_eq!(id, None);
        }
        other => panic!("Expected CollectionNotFound error, got: {:?}", other),
    }
}

#[test]
fn test_drop_index_returns_index_not_found_for_missing_index() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    db.create_collection("users").unwrap();

    let err = db
        .collection("users")
        .drop_index("missing_index")
        .unwrap_err();
    match err {
        Error::IndexNotFound {
            collection_name,
            index_name,
            id,
        } => {
            assert_eq!(collection_name, "users");
            assert_eq!(index_name, "missing_index");
            assert_eq!(id, None);
        }
        other => panic!("Expected IndexNotFound error, got: {:?}", other),
    }
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
    db.collection("users").drop_collection().unwrap();
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

    let names: Vec<&str> = results.iter().map(|d| d.get_str("name").unwrap()).collect();
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
    db.collection("items").drop_collection().unwrap();
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
fn test_collection_insert_is_strict_by_default() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    assert!(db.list_collections().is_empty());

    let err = db
        .collection("auto_created")
        .insert_one(doc! { "value": 42 })
        .unwrap_err();
    assert!(matches!(err, Error::CollectionNotFound { .. }));
    assert!(db.list_collections().is_empty());
}

#[test]
fn test_create_if_missing_allows_implicit_collection_creation_on_insert() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    assert!(db.list_collections().is_empty());

    db.collection("auto_created")
        .create_if_missing()
        .insert_one(doc! { "value": 42 })
        .unwrap();

    let collections = db.list_collections();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].name, "auto_created");
}

#[test]
fn test_create_if_missing_query_returns_empty_for_missing_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let results: Vec<_> = db
        .collection("missing")
        .create_if_missing()
        .find(doc! {})
        .execute()
        .unwrap()
        .collect();

    assert!(results.is_empty());
}

#[test]
fn test_estimated_document_count_returns_collection_count() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let collection = db.collection("users").create_if_missing();
    collection
        .insert_one(doc! { "_id": 1, "name": "Alice" })
        .unwrap();
    collection
        .insert_one(doc! { "_id": 2, "name": "Bob" })
        .unwrap();

    assert_eq!(collection.estimated_document_count().unwrap(), 2);
}

#[test]
fn test_estimated_document_count_is_strict_by_default() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let err = db
        .collection("missing")
        .estimated_document_count()
        .unwrap_err();
    assert!(matches!(err, Error::CollectionNotFound { .. }));
}

#[test]
fn test_estimated_document_count_create_if_missing_returns_zero_for_missing_collection() {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();

    let count = db
        .collection("missing")
        .create_if_missing()
        .estimated_document_count()
        .unwrap();

    assert_eq!(count, 0);
}
