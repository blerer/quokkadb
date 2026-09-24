mod common;

use bson::{Document, doc};
use tempfile::TempDir;

#[test]
fn create_index_backfills_before_return_and_maintains_writes_after_publication() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("users").create_if_missing();

    collection
        .insert_many([
            doc! { "_id": 1, "name": "Ada" },
            doc! { "_id": 2, "name": "Grace" },
        ])
        .unwrap();

    let index_name = collection.create_index(doc! { "name": 1 }).unwrap();
    let indexes = collection.list_indexes().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, index_name);

    let matches: Vec<Document> = collection
        .find(doc! { "name": "Grace" })
        .execute_collect()
        .unwrap();
    assert_eq!(matches, vec![doc! { "_id": 2, "name": "Grace" }]);

    collection
        .insert_one(doc! { "_id": 3, "name": "Lin" })
        .unwrap();

    let inserted_match: Vec<Document> = collection
        .find(doc! { "name": "Lin" })
        .execute_collect()
        .unwrap();
    assert_eq!(inserted_match, vec![doc! { "_id": 3, "name": "Lin" }]);

    let documents = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap();
    assert_eq!(
        documents,
        vec![
            doc! { "_id": 1, "name": "Ada" },
            doc! { "_id": 2, "name": "Grace" },
            doc! { "_id": 3, "name": "Lin" },
        ]
    );
}

#[test]
fn completed_index_build_survives_database_restart() {
    let directory = TempDir::new().unwrap();
    let index_name = {
        let db = common::open_db(directory.path());
        let collection = db.collection("users").create_if_missing();
        collection
            .insert_many([
                doc! { "_id": 1, "name": "Ada" },
                doc! { "_id": 2, "name": "Grace" },
            ])
            .unwrap();
        collection.create_index(doc! { "name": 1 }).unwrap()
    };

    let db = common::open_db(directory.path());
    let collection = db.collection("users");
    let indexes = collection.list_indexes().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, index_name);

    let matches: Vec<Document> = collection
        .find(doc! { "name": "Grace" })
        .execute_collect()
        .unwrap();
    assert_eq!(matches, vec![doc! { "_id": 2, "name": "Grace" }]);
}
