mod common;

use bson::doc;
use quokkadb::{QuokkaDB, QuokkaDocument};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    age: i32,
    active: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
struct UserSummary {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct UserName {
    name: String,
}

fn users() -> Vec<User> {
    vec![
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
            active: true,
        },
        User {
            id: 2,
            name: "Bob".to_string(),
            age: 40,
            active: true,
        },
        User {
            id: 3,
            name: "Cara".to_string(),
            age: 20,
            active: false,
        },
    ]
}

fn setup() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db.typed_collection::<User>("users").create_if_missing();
    collection.insert_many(users()).unwrap();
    (dir, db)
}

#[test]
fn typed_delete_one_uses_typed_filters_and_sorts() {
    for &layout in common::test_storage_layouts() {
        let dir = TempDir::new().unwrap();
        let documents = users();
        let db = common::open_db_with_seed_data(dir.path(), "users", &documents, layout);
        let collection = db.typed_collection::<User>("users");

        let result = collection
            .delete_one_with(|user| user.active.eq(true))
            .sort(|user| user.age.desc())
            .execute()
            .unwrap();

        assert_eq!(result.deleted_count, 1);
        assert_eq!(collection.find_one(|user| user.id.eq(2_u64)).unwrap(), None);
        assert_eq!(
            collection
                .find_one(|user| user.id.eq(1_u64))
                .unwrap()
                .unwrap()
                .name,
            "Alice"
        );
    }
}

#[test]
fn typed_delete_one_reports_no_match_without_deleting_documents() {
    for &layout in common::test_storage_layouts() {
        let dir = TempDir::new().unwrap();
        let documents = users();
        let db = common::open_db_with_seed_data(dir.path(), "users", &documents, layout);
        let collection = db.typed_collection::<User>("users");

        let result = collection.delete_one(|user| user.name.eq("Dora")).unwrap();

        assert_eq!(result.deleted_count, 0);
        assert_eq!(collection.estimated_document_count().unwrap(), 3);
        assert_eq!(
            collection.find_one(|user| user.id.eq(1_u64)).unwrap(),
            Some(documents[0].clone())
        );
    }
}

#[test]
fn typed_delete_many_deletes_all_matches_and_reports_no_match() {
    for &layout in common::test_storage_layouts() {
        let dir = TempDir::new().unwrap();
        let documents = users();
        let db = common::open_db_with_seed_data(dir.path(), "users", &documents, layout);
        let collection = db.typed_collection::<User>("users");

        let result = collection.delete_many(|user| user.active.eq(true)).unwrap();
        assert_eq!(result.deleted_count, 2);
        assert_eq!(collection.estimated_document_count().unwrap(), 1);
        assert_eq!(
            collection
                .delete_many_with(|user| user.name.eq("Dora"))
                .execute()
                .unwrap()
                .deleted_count,
            0
        );
    }
}

#[test]
fn typed_find_one_and_delete_returns_the_sorted_deleted_document() {
    for &layout in common::test_storage_layouts() {
        let dir = TempDir::new().unwrap();
        let documents = users();
        let db = common::open_db_with_seed_data(dir.path(), "users", &documents, layout);
        let collection = db.typed_collection::<User>("users");

        let deleted = collection
            .find_one_and_delete_with(|user| user.active.eq(true))
            .sort(|user| user.age.desc())
            .sync()
            .execute()
            .unwrap();

        assert_eq!(deleted.unwrap().name, "Bob");
        assert!(
            collection
                .find_one(|user| user.id.eq(2_u64))
                .unwrap()
                .is_none()
        );
        assert!(
            collection
                .find_one_and_delete(|user| user.name.eq("Dora"))
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn typed_find_one_and_delete_supports_typed_projections_and_selections() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let summary: UserSummary = collection
        .find_one_and_delete_with(|user| user.id.eq(1_u64))
        .include(|user| user.name)
        .execute()
        .unwrap()
        .unwrap();
    assert_eq!(
        summary,
        UserSummary {
            id: 1,
            name: "Alice".to_string(),
        }
    );

    let selected = collection
        .find_one_and_delete_with(|user| user.id.eq(2_u64))
        .select(|user| (user.name, user.age))
        .execute()
        .unwrap();
    assert_eq!(selected, Some(("Bob".to_string(), 40)));
    assert!(
        collection
            .find_one(|user| user.id.eq(1_u64))
            .unwrap()
            .is_none()
    );
    assert!(
        collection
            .find_one(|user| user.id.eq(2_u64))
            .unwrap()
            .is_none()
    );
}

#[test]
fn typed_find_one_and_delete_supports_other_projection_modes_and_sorting() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let name: UserName = collection
        .find_one_and_delete_with(|user| user.id.eq(1_u64))
        .include_without_id(|user| user.name)
        .execute()
        .unwrap()
        .unwrap();
    assert_eq!(
        name,
        UserName {
            name: "Alice".to_string()
        }
    );

    let summary: UserSummary = collection
        .find_one_and_delete_with(|user| user.active.eq(true))
        .exclude(|user| user.active)
        .sort(|user| user.age.desc())
        .execute()
        .unwrap()
        .unwrap();
    assert_eq!(
        summary,
        UserSummary {
            id: 2,
            name: "Bob".to_string(),
        }
    );
    assert!(
        collection
            .find_one(|user| user.id.eq(2_u64))
            .unwrap()
            .is_none()
    );
}

#[test]
fn typed_deletes_on_a_missing_create_if_missing_collection_do_not_create_it() {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());

    let deleted_count = db
        .typed_collection::<User>("missing")
        .create_if_missing()
        .delete_many(|user| user.id.eq(1_u64))
        .unwrap()
        .deleted_count;
    assert_eq!(deleted_count, 0);

    let deleted_count = db
        .typed_collection::<User>("missing")
        .create_if_missing()
        .delete_one(|user| user.id.eq(1_u64))
        .unwrap()
        .deleted_count;
    assert_eq!(deleted_count, 0);

    let deleted = db
        .typed_collection::<User>("missing")
        .create_if_missing()
        .find_one_and_delete(|user| user.id.eq(1_u64))
        .unwrap();
    assert_eq!(deleted, None);
    assert!(db.list_collections().is_empty());
}
