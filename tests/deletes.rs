use bson::{doc, Document};
use quokkadb::error::Result;
use quokkadb::QuokkaDB;
use tempfile::TempDir;

fn get_sample_data() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "item": "journal", "status": "A" },
        doc! { "_id": 2, "item": "notebook", "status": "A" },
        doc! { "_id": 3, "item": "paper", "status": "D" },
    ]
}

fn setup_db_with_data() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();
    let collection = db.collection("test").create_if_missing();
    collection.insert_many(get_sample_data()).unwrap();
    (dir, db)
}

#[test]
fn test_delete_one_deletes_matching_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.delete_one(doc! { "_id": 1 }).unwrap();

    assert_eq!(result.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 2);

    let remaining: Vec<_> = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect();
    assert_eq!(
        remaining,
        vec![
            doc! { "_id": 2, "item": "notebook", "status": "A" },
            doc! { "_id": 3, "item": "paper", "status": "D" },
        ]
    );
}

#[test]
fn test_delete_one_returns_zero_when_no_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.delete_one(doc! { "_id": 99 }).unwrap();

    assert_eq!(result.deleted_count, 0);
    assert_eq!(collection.estimated_document_count().unwrap(), 3);

    let remaining: Vec<_> = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect();
    assert_eq!(remaining, get_sample_data());
}

#[test]
fn test_delete_one_with_sort_deletes_lowest_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection
        .delete_one_with(doc! { "status": "A" })
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap();

    assert_eq!(result.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 2);

    let remaining: Vec<_> = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect();
    assert_eq!(
        remaining,
        vec![
            doc! { "_id": 2, "item": "notebook", "status": "A" },
            doc! { "_id": 3, "item": "paper", "status": "D" },
        ]
    );
}

#[test]
fn test_delete_one_with_sort_deletes_highest_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection
        .delete_one_with(doc! { "status": "A" })
        .sort(doc! { "_id": -1 })
        .execute()
        .unwrap();

    assert_eq!(result.deleted_count, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 2);

    let remaining: Vec<_> = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect();
    assert_eq!(
        remaining,
        vec![
            doc! { "_id": 1, "item": "journal", "status": "A" },
            doc! { "_id": 3, "item": "paper", "status": "D" },
        ]
    );
}

#[test]
fn test_find_one_and_delete_returns_none_when_no_match() -> Result<()> {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.find_one_and_delete(doc! { "_id": 99 })?;

    assert_eq!(result, None);
    assert_eq!(
        collection.find_one(doc! { "_id": 1 })?,
        Some(doc! { "_id": 1, "item": "journal", "status": "A" })
    );

    Ok(())
}

#[test]
fn test_find_one_and_delete_applies_projection() -> Result<()> {
    let dir = TempDir::new()?;
    let db = QuokkaDB::open(dir.path())?;
    let collection = db.collection("test").create_if_missing();

    collection.insert_one(doc! { "_id": 1, "value": "one", "extra": true })?;

    let result = collection
        .find_one_and_delete_with(doc! { "_id": 1 })
        .projection(doc! { "value": 1 })
        .execute()?;

    assert_eq!(result, Some(doc! { "_id": 1, "value": "one" }));
    assert_eq!(collection.find_one(doc! { "_id": 1 })?, None);

    Ok(())
}

#[test]
fn test_find_one_and_delete_respects_sort() -> Result<()> {
    let dir = TempDir::new()?;
    let db = QuokkaDB::open(dir.path())?;
    let collection = db.collection("test").create_if_missing();

    collection.insert_one(doc! { "_id": 1, "priority": 1, "value": "low" })?;
    collection.insert_one(doc! { "_id": 2, "priority": 2, "value": "high" })?;

    let result = collection
        .find_one_and_delete_with(doc! {})
        .sort(doc! { "priority": -1 })
        .execute()?;

    assert_eq!(
        result,
        Some(doc! { "_id": 2, "priority": 2, "value": "high" })
    );
    assert_eq!(
        collection.find_one(doc! { "_id": 1 })?,
        Some(doc! { "_id": 1, "priority": 1, "value": "low" })
    );
    assert_eq!(collection.find_one(doc! { "_id": 2 })?, None);

    Ok(())
}

#[test]
fn test_find_one_and_delete_create_if_missing_returns_none_without_creating_collection(
) -> Result<()> {
    let dir = TempDir::new()?;
    let db = QuokkaDB::open(dir.path())?;

    let result = db
        .collection("missing")
        .create_if_missing()
        .find_one_and_delete(doc! { "_id": 1 })?;

    assert_eq!(result, None);
    assert!(db.list_collections().is_empty());

    Ok(())
}
