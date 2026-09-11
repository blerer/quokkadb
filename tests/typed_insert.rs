mod common;

use bson::doc;
use quokkadb::error::Error;
use quokkadb::{QuokkaDB, QuokkaDocument};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    age: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct Plant {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    needs_water: bool,
}

fn setup() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    (dir, db)
}

fn user(id: u64, name: &str, age: i32) -> User {
    User {
        id,
        name: name.to_string(),
        age,
    }
}

#[test]
fn typed_insert_one_returns_the_document_id_and_serializes_the_document() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users").create_if_missing();

    let result = collection.insert_one(user(1, "Alice", 30)).unwrap();

    assert_eq!(result.inserted_id, 1);
    assert_eq!(collection.estimated_document_count().unwrap(), 1);
    let document = db
        .collection("users")
        .find(doc! { "_id": 1_i64 })
        .execute()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        document,
        doc! { "_id": 1_i64, "name": "Alice", "age": 30_i32 }
    );
}

#[test]
fn typed_insert_many_returns_typed_ids_in_input_order() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users").create_if_missing();

    let result = collection
        .insert_many_with([
            user(1, "Alice", 30),
            user(2, "Bob", 40),
            user(3, "Cara", 20),
        ])
        .unwrap()
        .sync()
        .execute()
        .unwrap();

    assert_eq!(result.inserted_ids, vec![1, 2, 3]);
    assert_eq!(collection.estimated_document_count().unwrap(), 3);
}

#[test]
fn typed_insert_rejects_duplicate_ids() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users").create_if_missing();

    collection.insert_one(user(1, "Alice", 30)).unwrap();
    let error = collection.insert_one(user(1, "Alice", 31)).unwrap_err();

    assert!(matches!(error, Error::InvalidRequest(_)));
    assert_eq!(collection.estimated_document_count().unwrap(), 1);
}

#[test]
fn cloned_database_handle_can_insert_a_typed_document_from_a_worker_thread() {
    let (_dir, db) = setup();
    let plants = db.typed_collection::<Plant>("plants").create_if_missing();
    plants
        .insert_one(Plant {
            id: 1,
            name: "Monstera".to_string(),
            needs_water: true,
        })
        .unwrap();

    let worker_db = db.clone();
    let worker = std::thread::spawn(move || {
        let plants = worker_db.typed_collection::<Plant>("plants");
        plants.insert_one(Plant {
            id: 2,
            name: "Spider plant".to_string(),
            needs_water: false,
        })
    });

    let inserted = worker.join().unwrap().unwrap();
    assert_eq!(inserted.inserted_id, 2);

    let plant = plants
        .find_one(|plant| plant.id.eq(2_u64))
        .unwrap()
        .unwrap();
    assert_eq!(plant.name, "Spider plant");
}
