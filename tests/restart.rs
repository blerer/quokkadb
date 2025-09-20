use bson::{doc, Document};
use quokkadb::QuokkaDB;
use std::path::Path;
use tempfile::tempdir;

fn get_db(path: &Path) -> QuokkaDB {
    QuokkaDB::open(path).unwrap()
}

#[test]
fn test_restart_with_multiple_collections() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path();

    {
        let db = get_db(db_path);
        let collection1 = db.collection("collection1");
        let collection2 = db.collection("collection2");

        collection1
            .insert_many(vec![
                doc! { "name": "Alice", "age": 30 },
                doc! { "name": "Bob", "age": 25 },
            ])
            .unwrap();

        collection2
            .insert_many(vec![
                doc! { "city": "New York", "population": 8_400_000 },
                doc! { "city": "London", "population": 8_900_000 },
            ])
            .unwrap();
    } // db is dropped here, and data should be flushed to disk

    // Re-open the database
    let db = get_db(db_path);
    let collection1 = db.collection("collection1");
    let collection2 = db.collection("collection2");

    let mut results1 = collection1
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|d| d.unwrap())
        .collect::<Vec<Document>>();
    assert_eq!(results1.len(), 2);

    // Order is not guaranteed, so we sort before asserting
    results1.sort_by_key(|d| d["name"].as_str().unwrap().to_string());
    assert_eq!(results1[0]["name"], "Alice".into());
    assert_eq!(results1[0]["age"], 30.into());
    assert_eq!(results1[1]["name"], "Bob".into());
    assert_eq!(results1[1]["age"], 25.into());

    let mut results2 = collection2
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|d| d.unwrap())
        .collect::<Vec<Document>>();
    assert_eq!(results2.len(), 2);

    // Order is not guaranteed, so we sort before asserting
    results2.sort_by_key(|d| d["city"].as_str().unwrap().to_string());
    assert_eq!(results2[0]["city"], "London".into());
    assert_eq!(results2[0]["population"], 8_900_000.into());
    assert_eq!(results2[1]["city"], "New York".into());
    assert_eq!(results2[1]["population"], 8_400_000.into());
}
