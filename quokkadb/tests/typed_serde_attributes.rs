mod common;

use bson::doc;
use quokkadb::{QuokkaDB, QuokkaDocument, QuokkaType};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
#[serde(rename_all = "camelCase")]
struct Profile {
    first_name: String,
    #[serde(rename = "postal")]
    postal_code: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
#[serde(rename_all = "camelCase")]
struct AttributeDocument {
    #[serde(rename = "_id")]
    identifier: u64,
    #[serde(rename = "display_name")]
    display_name: String,
    #[serde(default)]
    retry_count: i32,
    profile: Profile,
}

fn setup() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    (dir, db)
}

fn attribute_document(id: u64) -> AttributeDocument {
    AttributeDocument {
        identifier: id,
        display_name: "Alice".to_string(),
        retry_count: 7,
        profile: Profile {
            first_name: "Ada".to_string(),
            postal_code: "8000".to_string(),
        },
    }
}

#[test]
fn typed_serde_rename_and_rename_all_drive_storage_and_queries() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<AttributeDocument>("attributes")
        .create_if_missing();

    collection.insert_one(attribute_document(1)).unwrap();

    let stored = db
        .collection("attributes")
        .find_one(doc! { "_id": 1_i64 })
        .unwrap()
        .unwrap();
    assert_eq!(
        stored,
        doc! {
            "_id": 1_i64,
            "display_name": "Alice",
            "retryCount": 7_i32,
            "profile": {
                "firstName": "Ada",
                "postal": "8000",
            },
        }
    );

    let found = collection
        .find_one(|document| {
            document
                .display_name
                .eq("Alice")
                .and(document.profile.first_name.eq("Ada"))
        })
        .unwrap();
    assert_eq!(found, Some(attribute_document(1)));
}

#[test]
fn typed_serde_default_and_skip_apply_at_the_serde_boundary() {
    let (_dir, db) = setup();
    db.collection("attributes")
        .create_if_missing()
        .insert_one(doc! {
            "_id": 2_i64,
            "display_name": "Bob",
            "profile": {
                "firstName": "Bea",
                "postal": "1000",
            },
        })
        .unwrap();

    let document = db
        .typed_collection::<AttributeDocument>("attributes")
        .find_one(|document| document.identifier.eq(2_u64))
        .unwrap()
        .unwrap();

    assert_eq!(document.retry_count, 0);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct SkippedDocument {
    #[serde(rename = "_id")]
    id: u64,
    #[serde(skip)]
    internal: String,
    name: String,
}

#[test]
fn typed_serde_skip_omits_the_field_and_uses_its_default_on_read() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<SkippedDocument>("skipped")
        .create_if_missing();

    collection
        .insert_one(SkippedDocument {
            id: 1,
            internal: "private".to_string(),
            name: "Alice".to_string(),
        })
        .unwrap();

    let stored = db
        .collection("skipped")
        .find_one(doc! { "_id": 1_i64 })
        .unwrap()
        .unwrap();
    assert!(!stored.contains_key("internal"));

    let loaded = collection
        .find_one(|document| document.id.eq(1_u64))
        .unwrap()
        .unwrap();
    assert_eq!(loaded.internal, String::new());
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
#[serde(rename_all = "camelCase")]
struct ConditionalDocument {
    #[serde(rename = "_id")]
    id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    optional_note: Option<String>,
}

#[test]
fn typed_serde_skip_serializing_if_omits_only_empty_values() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<ConditionalDocument>("conditional")
        .create_if_missing();

    collection
        .insert_one(ConditionalDocument {
            id: 1,
            optional_note: None,
        })
        .unwrap();
    collection
        .insert_one(ConditionalDocument {
            id: 2,
            optional_note: Some("visible".to_string()),
        })
        .unwrap();

    let without_note = db
        .collection("conditional")
        .find_one(doc! { "_id": 1_i64 })
        .unwrap()
        .unwrap();
    assert!(!without_note.contains_key("optionalNote"));

    let with_note = db
        .collection("conditional")
        .find_one(doc! { "_id": 2_i64 })
        .unwrap()
        .unwrap();
    assert_eq!(with_note.get_str("optionalNote").unwrap(), "visible");

    let matching_ids = collection
        .find(|document| document.optional_note.eq("visible"))
        .select(|document| document.id)
        .execute_collect()
        .unwrap();
    assert_eq!(matching_ids, vec![2]);
}
