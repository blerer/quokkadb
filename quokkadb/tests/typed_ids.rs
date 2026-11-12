mod common;

use quokkadb::{Error, QuokkaDB, QuokkaDocument, QuokkaId};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct Document {
    #[serde(rename = "_id")]
    id: QuokkaId,
    related_id: QuokkaId,
    name: String,
}

fn id(value: u64) -> QuokkaId {
    QuokkaId::try_from(value).unwrap()
}

fn setup() -> (TempDir, QuokkaDB) {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    (directory, db)
}

#[test]
fn quokka_id_is_searchable_sortable_indexable_and_selectable() {
    let (_directory, db) = setup();
    let collection = db
        .typed_collection::<Document>("documents")
        .create_if_missing();
    let first = id(1);
    let second = id(2);

    collection
        .insert_many([
            Document {
                id: second,
                related_id: first,
                name: "second".to_string(),
            },
            Document {
                id: first,
                related_id: second,
                name: "first".to_string(),
            },
        ])
        .unwrap();

    collection
        .create_index(|document| document.id.index_asc())
        .unwrap();

    let ids: Vec<QuokkaId> = collection
        .find(|document| document.id.gte(first))
        .sort(|document| document.id.asc())
        .select(|document| document.id)
        .execute_collect()
        .unwrap();

    assert_eq!(ids, vec![first, second]);
}

#[test]
fn typed_find_one_by_id_returns_matching_document_or_none() {
    let (_directory, db) = setup();
    let collection = db
        .typed_collection::<Document>("documents")
        .create_if_missing();
    let document = Document {
        id: id(1),
        related_id: id(2),
        name: "document".to_string(),
    };

    collection.insert_one(&document).unwrap();

    assert_eq!(
        collection.find_one_by_id(document.id).unwrap(),
        Some(document)
    );
    assert_eq!(collection.find_one_by_id(id(2)).unwrap(), None);
}

#[test]
fn quokka_id_values_in_non_id_fields_remain_updatable() {
    let (_directory, db) = setup();
    let collection = db
        .typed_collection::<Document>("documents")
        .create_if_missing();
    let document_id = id(1);
    let old_related_id = id(2);
    let new_related_id = id(3);

    collection
        .insert_one(Document {
            id: document_id,
            related_id: old_related_id,
            name: "document".to_string(),
        })
        .unwrap();

    collection
        .update_one(
            |document| document.id.eq(document_id),
            |document| document.related_id.set(new_related_id),
        )
        .unwrap();

    assert_eq!(
        collection
            .find_one(|document| document.id.eq(document_id))
            .unwrap()
            .unwrap()
            .related_id,
        new_related_id
    );
}

#[test]
fn typed_upsert_results_preserve_quokka_id_for_follow_up_reads() {
    let (_directory, db) = setup();
    let collection = db
        .typed_collection::<Document>("documents")
        .create_if_missing();
    let document_id = id(1);

    let result = collection
        .update_one_with(
            |document| document.id.eq(document_id),
            |document| {
                document
                    .related_id
                    .set(document_id)
                    .and(document.name.set("upserted"))
            },
        )
        .upsert(true)
        .execute()
        .unwrap();

    assert_eq!(result.upserted_id, Some(document_id));
    assert_eq!(
        collection
            .find_one_by_id(result.upserted_id.unwrap())
            .unwrap(),
        Some(Document {
            id: document_id,
            related_id: document_id,
            name: "upserted".to_string(),
        })
    );
}

#[test]
fn typed_quokka_id_updates_are_rejected_at_execution() {
    let (_directory, db) = setup();
    let collection = db
        .typed_collection::<Document>("documents")
        .create_if_missing();
    let document_id = id(1);
    let replacement_id = id(2);

    collection
        .insert_one(Document {
            id: document_id,
            related_id: document_id,
            name: "document".to_string(),
        })
        .unwrap();

    let error = collection
        .update_one(
            |document| document.id.eq(document_id),
            |document| document.id.set(replacement_id),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidRequest(message) if message == "The _id field cannot be changed"
    ));
    assert!(
        collection
            .find_one(|document| document.id.eq(document_id))
            .unwrap()
            .is_some()
    );
    assert!(
        collection
            .find_one(|document| document.id.eq(replacement_id))
            .unwrap()
            .is_none()
    );
}
