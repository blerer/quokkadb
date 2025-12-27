use bson::{doc, Bson, Document};
use quokkadb::obs::logger::{LogLevel, StdoutLogger};
use quokkadb::QuokkaDB;
use std::collections::BTreeSet;
use tempfile::TempDir;
use quokkadb::collection::{Collection, UpdateOptions};

fn get_sample_data() -> Vec<Document> {
    vec![
        doc! { "_id": 1, "item": "journal", "qty": 25, "size": { "h": 14, "w": 21, "uom": "cm" }, "status": "A", "tags": ["blank", "red"], "dim_cm": [ 14, 21 ] },
        doc! { "_id": 2, "item": "notebook", "qty": 50, "size": { "h": 8.5, "w": 11, "uom": "in" }, "status": "A", "tags": ["red", "blank"], "dim_cm": [ 8.5, 11 ] },
        doc! { "_id": 3, "item": "paper", "qty": 100, "size": { "h": 8.5, "w": 11, "uom": "in" }, "status": "D", "tags": ["red", "blank", "plain"], "dim_cm": [ 8.5, 11 ] },
        doc! { "_id": 4, "item": "planner", "qty": 75, "size": { "h": 22.85, "w": 30, "uom": "cm" }, "status": "D", "tags": ["blank", "red"], "dim_cm": [ 22.85, 30 ] },
        doc! { "_id": 5, "item": "postcard", "qty": 45, "size": { "h": 10, "w": 15.25, "uom": "cm" }, "status": "A", "tags": ["blue"], "dim_cm": [ 10, 15.25 ] },
        doc! { "_id": 6, "item": "canvas", "qty": 30, "size": { "h": 20, "w": 30.5, "uom": "cm" }, "status": "B", "tags": [], "dim_cm": [ 20, 30.5 ], "ratings": [ { "user": "A", "score": 8 }, { "user": "B", "score": 7 } ] },
        doc! { "_id": 7, "item": "mat", "qty": 85, "size": { "h": 27.9, "w": 35.5, "uom": "cm" }, "status": "A", "tags": ["gray"], "dim_cm": [ 27.9, 35.5 ], "ratings": [ { "user": "C", "score": 9 }, { "user": "D", "score": 5 } ] },
        doc! { "_id": 8, "item": "poster", "qty": 20, "size": { "h": 50, "w": 70, "uom": "cm" }, "status": "B", "tags": ["art", "color"], "dim_cm": [ 50, 70 ], "ratings": [ { "user": "E", "score": 10 }, { "user": "F", "score": 9 } ] },
    ]
}

fn setup_db_with_data() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let db = QuokkaDB::open_with_logger(path, StdoutLogger::new(LogLevel::Debug, true)).unwrap();
    let collection = db.collection("test");
    collection.insert_many(get_sample_data()).unwrap();
    (dir, db)
}

fn find_one(collection: &Collection, filter: Document) -> Option<Document> {
    collection
        .find(filter)
        .execute()
        .unwrap()
        .next()
        .map(|d| d.unwrap())
}

#[test]
fn test_set_simple() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(doc! { "_id": 1 }, doc! { "$set": { "qty": 30 } }, UpdateOptions::default())
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 30);
    assert_eq!(doc.get_str("item").unwrap(), "journal"); // Check other fields remain
}

#[test]
fn test_set_nested_and_create() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$set": { "size.uom": "mm", "details.model": "T-800" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let size = doc.get_document("size").unwrap();
    assert_eq!(size.get_str("uom").unwrap(), "mm");
    let details = doc.get_document("details").unwrap();
    assert_eq!(details.get_str("model").unwrap(), "T-800");
}

#[test]
fn test_unset() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$unset": { "qty": "", "size.uom": "" } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert!(!doc.contains_key("qty"));
    let size = doc.get_document("size").unwrap();
    assert!(!size.contains_key("uom"));
    assert!(size.contains_key("h"));
}

#[test]
fn test_inc() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$inc": { "qty": 5, "views": 1 } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 30);
    assert_eq!(doc.get_i32("views").unwrap(), 1);
}

#[test]
fn test_mul() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$mul": { "qty": 2.0, "views": 10 } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_f64("qty").unwrap(), 50.0);
    assert_eq!(doc.get_i32("views").unwrap(), 0); // views doesn't exist, defaults to 0
}

#[test]
fn test_rename() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$rename": { "qty": "quantity", "size.h": "size.height" } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert!(!doc.contains_key("qty"));
    assert!(doc.contains_key("quantity"));
    assert_eq!(doc.get_i32("quantity").unwrap(), 25);
    let size = doc.get_document("size").unwrap();
    assert!(!size.contains_key("h"));
    assert!(size.contains_key("height"));
    assert_eq!(size.get_i32("height").unwrap(), 14);
}

#[test]
fn test_min_max() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$min": { "qty": 20 }, "$max": {"stock": 100} },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 20); // 20 is < 25
    assert_eq!(doc.get_i32("stock").unwrap(), 100); // stock doesn't exist

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$min": { "qty": 22 } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 20); // 22 is > 20, no-op

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$max": { "qty": 50 } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 50); // 50 is > 20
}

#[test]
fn test_current_date() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$currentDate": { "lastModified": true, "audit.timestamp": { "$type": "timestamp" } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert!(matches!(doc.get("lastModified"), Some(Bson::DateTime(_))));
    let audit = doc.get_document("audit").unwrap();
    assert!(matches!(audit.get("timestamp"), Some(Bson::Timestamp(_))));
}

#[test]
fn test_add_to_set() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$addToSet": { "tags": "new" } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 3);
    assert!(tags.contains(&Bson::String("new".to_string())));

    // Adding existing value is a no-op
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$addToSet": { "tags": "red" } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_array("tags").unwrap().len(), 3);
}

#[test]
fn test_push() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": "new" } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[2], Bson::String("new".to_string()));

    // Create array on new field
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "comments": "first" } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let comments = doc.get_array("comments").unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0], Bson::String("first".to_string()));
}

#[test]
fn test_pop() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Pop last
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$pop": { "tags": 1 } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags, &vec![Bson::String("blank".to_string())]);

    // Pop first
    collection
        .update_one(
            doc! { "_id": 2 },
            doc! { "$pop": { "tags": -1 } },
            UpdateOptions::default()
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 2 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags, &vec![Bson::String("blank".to_string())]);
}

#[test]
fn test_pull() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 3 },
            doc! { "$pull": { "tags": "blank" } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 3 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(
        tags.iter().map(|s| s.as_str().unwrap()).collect::<BTreeSet<_>>(),
        vec!["red", "plain"].into_iter().collect()
    );
}

#[test]
fn test_pull_all() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 3 },
            doc! { "$pullAll": { "tags": ["red", "plain"] } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 3 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags, &vec![Bson::String("blank".to_string())]);
}

#[test]
fn test_pull_from_array_of_documents() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$pull": { "ratings": { "score": 8 } } },
            UpdateOptions::default()
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(ratings.len(), 1);
    assert_eq!(
        ratings[0].as_document().unwrap().get_i32("score").unwrap(),
        7
    );
}

#[test]
fn test_bit() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // qty is 25 (0b11001)
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$bit": { "qty": { "and": 0b10101, "or": 0b00010 } } },
            UpdateOptions::default(),
        )
        .unwrap();
    // and: 0b11001 & 0b10101 = 0b10001
    // or:  0b10001 | 0b00010 = 0b10011 (19)
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 19);
}

#[test]
fn test_update_many() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Set status to 'C' and inc qty for all 'A' status documents
    collection
        .update_many(
            doc! { "status": "A" },
            doc! { "$set": { "status": "C" }, "$inc": { "qty": 10 } },
            UpdateOptions::default()
        )
        .unwrap();

    let results: Vec<Document> = collection
        .find(doc! { "status": "C" })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(results.len(), 4);

    let doc1 = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc1.get_i32("qty").unwrap(), 35);
    let doc2 = find_one(&collection, doc! { "_id": 2 }).unwrap();
    assert_eq!(doc2.get_i32("qty").unwrap(), 60);

    let count_a = collection
        .find(doc! { "status": "A" })
        .execute()
        .unwrap()
        .count();
    assert_eq!(count_a, 0);
}

#[test]
fn test_update_many_add_to_set() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_many(
            doc! { "tags": "red" },
            doc! { "$addToSet": { "tags": "warm-color" } },
            UpdateOptions::default(),
        )
        .unwrap();

    // Docs 1, 2, 3, 4 have "red" tag.
    let results: Vec<Document> = collection
        .find(doc! { "tags": "warm-color" })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(results.len(), 4);

    let doc1 = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags1 = doc1.get_array("tags").unwrap();
    assert!(tags1.contains(&Bson::String("warm-color".to_string())));
    assert_eq!(tags1.len(), 3);

    // Check a doc that was not updated
    let doc5 = find_one(&collection, doc! { "_id": 5 }).unwrap();
    let tags5 = doc5.get_array("tags").unwrap();
    assert!(!tags5.contains(&Bson::String("warm-color".to_string())));
}

#[test]
fn test_update_many_no_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let original_docs: Vec<Document> = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    collection
        .update_many(
            doc! { "status": "nonexistent" },
            doc! { "$set": { "should_not_exist": true } },
            UpdateOptions::default(),
        )
        .unwrap();

    let new_docs: Vec<Document> = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(original_docs, new_docs);
}

#[test]
fn test_push_with_each_and_slice() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["blank", "red"] -> push ["a", "b", "c"] -> ["blank", "red", "a", "b", "c"] -> slice -2 -> ["b", "c"]
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": { "$each": ["a", "b", "c"], "$slice": -2 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].as_str().unwrap(), "b");
    assert_eq!(tags[1].as_str().unwrap(), "c");
}

#[test]
fn test_add_to_set_with_each() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["blank", "red"] -> addToSet ["red", "green"] -> ["blank", "red", "green"]
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$addToSet": { "tags": { "$each": ["red", "green"] } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(
        tags.iter().map(|s| s.as_str().unwrap()).collect::<BTreeSet<_>>(),
        vec!["blank", "red", "green"].into_iter().collect()
    );
}

#[test]
fn test_pull_with_condition() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // ratings: [ { user: "A", score: 8 }, { user: "B", score: 7 } ] -> pull score >= 8
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$pull": { "ratings": { "score": { "$gte": 8 } } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(ratings.len(), 1);
    assert_eq!(
        ratings[0].as_document().unwrap(),
        &doc! { "user": "B", "score": 7 }
    );
}

#[test]
fn test_positional_all_elements_set_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$set": { "ratings.$[].approved": true } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(
        ratings,
        &vec![
            Bson::from(doc! { "user": "A", "score": 8, "approved": true }),
            Bson::from(doc! { "user": "B", "score": 7, "approved": true }),
        ]
    );
}

#[test]
fn test_positional_inc_on_scalar_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // dim_cm: [ 14, 21 ] -> inc by 1.5 -> [15.5, 22.5]
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$inc": { "dim_cm.$[]": 1.5 } },
            UpdateOptions::default(),
        )
        .unwrap();
    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let dim_cm = doc.get_array("dim_cm").unwrap();
    assert_eq!(dim_cm[0].as_f64().unwrap(), 15.5);
    assert_eq!(dim_cm[1].as_f64().unwrap(), 22.5);
}

#[test]
fn test_positional_unset_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$unset": { "ratings.$[].user": "" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(
        ratings,
        &vec![
            Bson::from(doc! { "score": 8 }),
            Bson::from(doc! { "score": 7 }),
        ]
    );
}

#[test]
fn test_positional_push_to_nested_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Set up nested arrays first
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$set": { "ratings.$[].comments": [] } },
            UpdateOptions::default(),
        )
        .unwrap();

    // Push to all nested arrays
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$push": { "ratings.$[].comments": "a comment" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(
        ratings,
        &vec![
            Bson::from(
                doc! { "user": "A", "score": 8, "comments": ["a comment"] }
            ),
            Bson::from(
                doc! { "user": "B", "score": 7, "comments": ["a comment"] }
            ),
        ]
    );
}

#[test]
fn test_nested_positional_operators_set() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    collection.insert_one(doc! {
        "_id": 100,
        "schools": [
            { "classes": [
                { "students": [ {"name": "A"}, {"name": "B"} ] },
                { "students": [ {"name": "C"} ] }
            ] },
            { "classes": [
                { "students": [ {"name": "D"} ] }
            ] }
        ]
    }).unwrap();

    collection.update_one(
        doc! { "_id": 100 },
        doc! { "$set": { "schools.$[].classes.$[].students.$[].passed": true } },
        UpdateOptions::default()
    ).unwrap();

    let doc = find_one(&collection, doc! { "_id": 100 }).unwrap();
    let expected = doc! {
        "_id": 100,
        "schools": [
            { "classes": [
                { "students": [ {"name": "A", "passed": true}, {"name": "B", "passed": true} ] },
                { "students": [ {"name": "C", "passed": true} ] }
            ] },
            { "classes": [
                { "students": [ {"name": "D", "passed": true} ] }
            ] }
        ]
    };
    assert_eq!(doc, expected);
}

#[test]
fn test_positional_on_empty_array_is_noop() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // doc 6 has tags: []
    let doc_before = find_one(&collection, doc! { "_id": 6 }).unwrap();
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$inc": { "tags.$[]": 1 } },
            UpdateOptions::default(),
        )
        .unwrap();
    let doc_after = find_one(&collection, doc! { "_id": 6 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_positional_on_missing_field_is_noop() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let doc_before = find_one(&collection, doc! { "_id": 1 }).unwrap();
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$set": { "non_existent.$[].field": 1 } },
            UpdateOptions::default(),
        )
        .unwrap();
    let doc_after = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_positional_on_non_array_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$set": { "item.$[].field": 1 } },
        UpdateOptions::default(),
    );
    assert!(result.is_err());
}

#[test]
fn test_first_positional_operator_rejected() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$set": { "tags.$": "new" } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "The positional operator '$' is not supported."
    );
}

// ============================================================================
// Filtered Positional Operator Tests ($[<identifier>])
// ============================================================================

#[test]
fn test_filtered_positional_set_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // ratings: [ { user: "A", score: 8 }, { user: "B", score: 7 } ]
    // Only update elements where score >= 8
    let options = UpdateOptions {
        array_filters: Some(vec![doc! { "elem.score": { "$gte": 8 } }]),
        ..Default::default()
    };
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$set": { "ratings.$[elem].reviewed": true } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(
        ratings,
        &vec![
            Bson::from(doc! { "user": "A", "score": 8, "reviewed": true }),
            Bson::from(doc! { "user": "B", "score": 7 }),
        ]
    );
}

#[test]
fn test_filtered_positional_inc() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Increment score by 1 for elements where score < 8
    let options = UpdateOptions {
        array_filters: Some(vec![doc! { "r.score": { "$lt": 8 } }]),
        ..Default::default()
    };
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$inc": { "ratings.$[r].score": 1 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(
        ratings[0].as_document().unwrap().get_i32("score").unwrap(),
        8
    );
    assert_eq!(
        ratings[1].as_document().unwrap().get_i32("score").unwrap(),
        8  // was 7, incremented by 1
    );
}

#[test]
fn test_filtered_positional_multiple_filters() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    collection.insert_one(doc! {
        "_id": 300,
        "grades": [
            { "subject": "math", "score": 80 },
            { "subject": "english", "score": 90 },
            { "subject": "math", "score": 70 },
        ]
    }).unwrap();

    // Update only math subjects with score >= 75
    let options = UpdateOptions {
        array_filters: Some(vec![
            doc! { "g.subject": "math", "g.score": { "$gte": 75 } }
        ]),
        ..Default::default()
    };
    collection
        .update_one(
            doc! { "_id": 300 },
            doc! { "$set": { "grades.$[g].passed": true } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 300 }).unwrap();
    assert_eq!(&doc,
        &doc! {
            "_id": 300,
            "grades": [
                { "subject": "math", "score": 80, "passed": true },
                { "subject": "english", "score": 90 },
                { "subject": "math", "score": 70 },
            ]
        }
    );
}

#[test]
fn test_filtered_positional_no_matching_elements() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let doc_before = find_one(&collection, doc! { "_id": 6 }).unwrap();

    // No elements have score > 100
    let options = UpdateOptions {
        array_filters: Some(vec![doc! { "elem.score": { "$gt": 100 } }]),
        ..Default::default()
    };
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$set": { "ratings.$[elem].flag": true } },
            options,
        )
        .unwrap();

    let doc_after = find_one(&collection, doc! { "_id": 6 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_filtered_positional_nested_arrays() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    collection.insert_one(doc! {
        "_id": 301,
        "departments": [
            {
                "name": "engineering",
                "employees": [
                    { "name": "Alice", "level": 3 },
                    { "name": "Bob", "level": 2 },
                ]
            },
            {
                "name": "sales",
                "employees": [
                    { "name": "Carol", "level": 4 },
                ]
            }
        ]
    }).unwrap();

    // Update all employees with level >= 3 across all departments
    let options = UpdateOptions {
        array_filters: Some(vec![doc! { "emp.level": { "$gte": 3 } }]),
        ..Default::default()
    };
    collection
        .update_one(
            doc! { "_id": 301 },
            doc! { "$set": { "departments.$[].employees.$[emp].senior": true } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 301 }).unwrap();
    let departments = doc.get_array("departments").unwrap();
    
    let eng_employees = departments[0].as_document().unwrap().get_array("employees").unwrap();
    assert!(eng_employees[0].as_document().unwrap().get_bool("senior").unwrap());
    assert!(!eng_employees[1].as_document().unwrap().contains_key("senior"));
    
    let sales_employees = departments[1].as_document().unwrap().get_array("employees").unwrap();
    assert!(sales_employees[0].as_document().unwrap().get_bool("senior").unwrap());
}

#[test]
fn test_filtered_positional_update_many() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Docs 6, 7, 8 have ratings arrays
    let options = UpdateOptions {
        array_filters: Some(vec![doc! { "r.score": { "$gte": 9 } }]),
        ..Default::default()
    };
    collection
        .update_many(
            doc! { "ratings": { "$exists": true } },
            doc! { "$set": { "ratings.$[r].excellent": true } },
            options,
        )
        .unwrap();

    // Doc 7: scores 9 and 5
    let doc7 = find_one(&collection, doc! { "_id": 7 }).unwrap();
    let ratings7 = doc7.get_array("ratings").unwrap();
    assert!(ratings7[0].as_document().unwrap().get_bool("excellent").unwrap());
    assert!(!ratings7[1].as_document().unwrap().contains_key("excellent"));

    // Doc 8: scores 10 and 9
    let doc8 = find_one(&collection, doc! { "_id": 8 }).unwrap();
    let ratings8 = doc8.get_array("ratings").unwrap();
    assert!(ratings8[0].as_document().unwrap().get_bool("excellent").unwrap());
    assert!(ratings8[1].as_document().unwrap().get_bool("excellent").unwrap());

    // Doc 6: scores 8 and 7 - neither >= 9
    let doc6 = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings6 = doc6.get_array("ratings").unwrap();
    assert!(!ratings6[0].as_document().unwrap().contains_key("excellent"));
    assert!(!ratings6[1].as_document().unwrap().contains_key("excellent"));
}

// ============================================================================
// $push Modifier Tests ($position, $sort, $slice combinations)
// ============================================================================

#[test]
fn test_push_with_position() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["blank", "red"] -> insert "first" at position 0
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": { "$each": ["first"], "$position": 0 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags[0].as_str().unwrap(), "first");
    assert_eq!(tags[1].as_str().unwrap(), "blank");
    assert_eq!(tags[2].as_str().unwrap(), "red");
}

#[test]
fn test_push_with_position_middle() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["blank", "red"] -> insert "middle" at position 1
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": { "$each": ["middle"], "$position": 1 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags[0].as_str().unwrap(), "blank");
    assert_eq!(tags[1].as_str().unwrap(), "middle");
    assert_eq!(tags[2].as_str().unwrap(), "red");
}

#[test]
fn test_push_with_negative_position() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["blank", "red"] -> insert "second_last" at position -1 (before last)
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": { "$each": ["second_last"], "$position": -1 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();

    println!("{:?}", tags);

    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].as_str().unwrap(), "blank");
    assert_eq!(tags[1].as_str().unwrap(), "second_last");
    assert_eq!(tags[2].as_str().unwrap(), "red");
}

#[test]
fn test_push_with_sort_ascending() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection.insert_one(doc! {
        "_id": 400,
        "scores": [5, 3, 8]
    }).unwrap();

    collection
        .update_one(
            doc! { "_id": 400 },
            doc! { "$push": { "scores": { "$each": [1, 9], "$sort": 1 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 400 }).unwrap();
    let scores = doc.get_array("scores").unwrap();
    let values: Vec<i32> = scores.iter().map(|b| b.as_i32().unwrap()).collect();
    assert_eq!(values, vec![1, 3, 5, 8, 9]);
}

#[test]
fn test_push_with_sort_descending() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection.insert_one(doc! {
        "_id": 401,
        "scores": [5, 3, 8]
    }).unwrap();

    collection
        .update_one(
            doc! { "_id": 401 },
            doc! { "$push": { "scores": { "$each": [1, 9], "$sort": -1 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 401 }).unwrap();
    let scores = doc.get_array("scores").unwrap();
    let values: Vec<i32> = scores.iter().map(|b| b.as_i32().unwrap()).collect();
    assert_eq!(values, vec![9, 8, 5, 3, 1]);
}

#[test]
fn test_push_with_sort_by_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection.insert_one(doc! {
        "_id": 402,
        "items": [
            { "name": "b", "price": 20 },
            { "name": "a", "price": 10 },
        ]
    }).unwrap();

    collection
        .update_one(
            doc! { "_id": 402 },
            doc! { "$push": { "items": { "$each": [{ "name": "c", "price": 15 }], "$sort": { "price": 1 } } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 402 }).unwrap();
    let items = doc.get_array("items").unwrap();
    assert_eq!(items[0].as_document().unwrap().get_i32("price").unwrap(), 10);
    assert_eq!(items[1].as_document().unwrap().get_i32("price").unwrap(), 15);
    assert_eq!(items[2].as_document().unwrap().get_i32("price").unwrap(), 20);
}

#[test]
fn test_push_with_positive_slice() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["blank", "red"] -> push ["a", "b", "c"] -> slice first 3
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": { "$each": ["a", "b", "c"], "$slice": 3 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 3);
    assert_eq!(tags[0].as_str().unwrap(), "blank");
    assert_eq!(tags[1].as_str().unwrap(), "red");
    assert_eq!(tags[2].as_str().unwrap(), "a");
}

#[test]
fn test_push_with_slice_zero() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // $slice: 0 should result in empty array
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "tags": { "$each": ["a"], "$slice": 0 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 0);
}

#[test]
fn test_push_with_sort_and_slice() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection.insert_one(doc! {
        "_id": 403,
        "top_scores": [50, 30]
    }).unwrap();

    // Add scores, sort descending, keep top 3
    collection
        .update_one(
            doc! { "_id": 403 },
            doc! { "$push": { "top_scores": { "$each": [80, 10, 60], "$sort": -1, "$slice": 3 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 403 }).unwrap();
    let scores = doc.get_array("top_scores").unwrap();
    let values: Vec<i32> = scores.iter().map(|b| b.as_i32().unwrap()).collect();
    assert_eq!(values, vec![80, 60, 50]);
}

#[test]
fn test_push_with_position_and_slice() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection.insert_one(doc! {
        "_id": 404,
        "queue": ["b", "c", "d"]
    }).unwrap();

    // Insert at position 0, then keep last 3
    collection
        .update_one(
            doc! { "_id": 404 },
            doc! { "$push": { "queue": { "$each": ["a"], "$position": 0, "$slice": -3 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 404 }).unwrap();
    let queue = doc.get_array("queue").unwrap();
    let values: Vec<&str> = queue.iter().map(|b| b.as_str().unwrap()).collect();
    assert_eq!(values, vec!["b", "c", "d"]);
}

// ============================================================================
// $bit xor Tests
// ============================================================================

#[test]
fn test_bit_xor() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // qty is 25 (0b11001), xor with 0b01010 = 0b10011 (19)
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$bit": { "qty": { "xor": 0b01010 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 19);
}

#[test]
fn test_bit_xor_toggle() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // XOR twice with same value should return original
    let original_qty = find_one(&collection, doc! { "_id": 1 }).unwrap().get_i32("qty").unwrap();

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$bit": { "qty": { "xor": 0b1111 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$bit": { "qty": { "xor": 0b1111 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), original_qty);
}

#[test]
fn test_bit_combined_and_or_xor() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection.insert_one(doc! { "_id": 500, "flags": 0b11110000_i32 }).unwrap();

    // and: 0b11110000 & 0b11111100 = 0b11110000
    // or:  0b11110000 | 0b00000011 = 0b11110011
    // xor: 0b11110011 ^ 0b00001111 = 0b11111100
    collection
        .update_one(
            doc! { "_id": 500 },
            doc! { "$bit": { "flags": { "and": 0b11111100, "or": 0b00000011, "xor": 0b00001111 } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 500 }).unwrap();
    assert_eq!(doc.get_i32("flags").unwrap(), 0b11111100);
}

// ============================================================================
// Error Cases and Edge Cases
// ============================================================================

#[test]
fn test_inc_on_non_numeric_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // item is a string
    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$inc": { "item": 1 } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_mul_on_non_numeric_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$mul": { "item": 2 } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_push_on_non_array_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$push": { "item": "value" } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_pop_on_non_array_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$pop": { "item": 1 } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_pull_on_non_array_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$pull": { "item": "value" } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_add_to_set_on_non_array_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$addToSet": { "item": "value" } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_bit_on_non_integer_field_errors() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let result = collection.update_one(
        doc! { "_id": 1 },
        doc! { "$bit": { "item": { "or": 1 } } },
        UpdateOptions::default(),
    );

    assert!(result.is_err());
}

#[test]
fn test_pop_on_empty_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // doc 6 has empty tags: []
    let doc_before = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let tags_before = doc_before.get_array("tags").unwrap();
    assert!(tags_before.is_empty());

    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$pop": { "tags": 1 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc_after = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let tags_after = doc_after.get_array("tags").unwrap();
    assert!(tags_after.is_empty());
}

#[test]
fn test_pull_on_empty_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let doc_before = find_one(&collection, doc! { "_id": 6 }).unwrap();

    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$pull": { "tags": "nonexistent" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc_after = find_one(&collection, doc! { "_id": 6 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_pullall_on_empty_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let doc_before = find_one(&collection, doc! { "_id": 6 }).unwrap();

    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$pullAll": { "tags": ["a", "b", "c"] } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc_after = find_one(&collection, doc! { "_id": 6 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_inc_creates_nested_path() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$inc": { "stats.views.daily": 1 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let stats = doc.get_document("stats").unwrap();
    let views = stats.get_document("views").unwrap();
    assert_eq!(views.get_i32("daily").unwrap(), 1);
}

#[test]
fn test_push_creates_nested_path() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$push": { "metadata.history": "event1" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let metadata = doc.get_document("metadata").unwrap();
    let history = metadata.get_array("history").unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].as_str().unwrap(), "event1");
}

#[test]
fn test_inc_with_float_on_integer_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // qty is i32
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$inc": { "qty": 1.5 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    // Result should be f64 due to float increment
    assert_eq!(doc.get_f64("qty").unwrap(), 26.5);
}

#[test]
fn test_mul_with_float_on_integer_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // qty is 25
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$mul": { "qty": 1.5 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_f64("qty").unwrap(), 37.5);
}

#[test]
fn test_min_max_with_different_numeric_types() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // qty is 25 (i32), compare with f64
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$min": { "qty": 24.9 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_f64("qty").unwrap(), 24.9);

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$max": { "qty": 25_i64 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    // 25 > 24.9, so should be updated
    assert_eq!(doc.get_i64("qty").unwrap(), 25);
}

#[test]
fn test_rename_to_existing_field_overwrites() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Rename qty to item (which already exists as string "journal")
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$rename": { "qty": "item" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert!(!doc.contains_key("qty"));
    // item should now be the old qty value (25)
    assert_eq!(doc.get_i32("item").unwrap(), 25);
}

#[test]
fn test_rename_nonexistent_field_is_noop() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let doc_before = find_one(&collection, doc! { "_id": 1 }).unwrap();

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$rename": { "nonexistent": "newname" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc_after = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_unset_nonexistent_field_is_noop() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let doc_before = find_one(&collection, doc! { "_id": 1 }).unwrap();

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$unset": { "nonexistent": "" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc_after = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc_before, doc_after);
}

#[test]
fn test_update_many_with_positional_all() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Update all elements in dim_cm for all documents with status A
    collection
        .update_many(
            doc! { "status": "A" },
            doc! { "$inc": { "dim_cm.$[]": 0.5 } },
            UpdateOptions::default(),
        )
        .unwrap();

    // Check doc 1 (status A, dim_cm: [14, 21])
    let doc1 = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let dim1 = doc1.get_array("dim_cm").unwrap();
    assert_eq!(dim1[0].as_f64().unwrap(), 14.5);
    assert_eq!(dim1[1].as_f64().unwrap(), 21.5);

    // Check doc 2 (status A, dim_cm: [8.5, 11])
    let doc2 = find_one(&collection, doc! { "_id": 2 }).unwrap();
    let dim2 = doc2.get_array("dim_cm").unwrap();
    assert_eq!(dim2[0].as_f64().unwrap(), 9.0);
    assert_eq!(dim2[1].as_f64().unwrap(), 11.5);

    // Check doc 3 (status D) - should be unchanged
    let doc3 = find_one(&collection, doc! { "_id": 3 }).unwrap();
    let dim3 = doc3.get_array("dim_cm").unwrap();
    assert_eq!(dim3[0].as_f64().unwrap(), 8.5);
    assert_eq!(dim3[1].as_i32().unwrap(), 11);
}

#[test]
fn test_pull_with_in_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["red", "blank", "plain"] -> pull values in ["red", "plain"]
    collection
        .update_one(
            doc! { "_id": 3 },
            doc! { "$pull": { "tags": { "$in": ["red", "plain"] } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 3 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].as_str().unwrap(), "blank");
}

#[test]
fn test_pull_with_nin_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // tags: ["red", "blank", "plain"] -> pull values NOT in ["blank"]
    collection
        .update_one(
            doc! { "_id": 3 },
            doc! { "$pull": { "tags": { "$nin": ["blank"] } } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 3 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].as_str().unwrap(), "blank");
}

#[test]
fn test_multiple_operators_same_update() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    collection
        .update_one(
            doc! { "_id": 1 },
            doc! {
                "$set": { "status": "X" },
                "$inc": { "qty": 5 },
                "$push": { "tags": "updated" },
                "$currentDate": { "modified": true }
            },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_str("status").unwrap(), "X");
    assert_eq!(doc.get_i32("qty").unwrap(), 30);
    let tags = doc.get_array("tags").unwrap();
    assert!(tags.contains(&Bson::String("updated".to_string())));
    assert!(matches!(doc.get("modified"), Some(Bson::DateTime(_))));
}

#[test]
fn test_set_array_element_by_index() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Set specific array element: tags.0
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$set": { "tags.0": "first_replaced" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags[0].as_str().unwrap(), "first_replaced");
    assert_eq!(tags[1].as_str().unwrap(), "red");
}

#[test]
fn test_unset_array_element_by_index() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // Unsetting array element sets it to null
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$unset": { "tags.0": "" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 2);
    assert!(matches!(tags[0], Bson::Null));
    assert_eq!(tags[1].as_str().unwrap(), "red");
}

#[test]
fn test_set_nested_document_in_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    // ratings.0.score
    collection
        .update_one(
            doc! { "_id": 6 },
            doc! { "$set": { "ratings.0.score": 10 } },
            UpdateOptions::default(),
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 6 }).unwrap();
    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(
        ratings[0].as_document().unwrap().get_i32("score").unwrap(),
        10
    );
}

// ============================================================================
// Upsert Tests
// ============================================================================

#[test]
fn test_upsert_insert_when_no_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 100 },
            doc! { "$set": { "item": "new_item", "qty": 10 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 100 }).unwrap();
    assert_eq!(doc.get_i32("_id").unwrap(), 100);
    assert_eq!(doc.get_str("item").unwrap(), "new_item");
    assert_eq!(doc.get_i32("qty").unwrap(), 10);
}

#[test]
fn test_upsert_updates_when_match_exists() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$set": { "qty": 999 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), 999);
    assert_eq!(doc.get_str("item").unwrap(), "journal");

    let count = collection.find(doc! {}).execute().unwrap().count();
    assert_eq!(count, 8);
}

#[test]
fn test_upsert_generates_id_when_not_in_filter() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "item": "unique_item" },
            doc! { "$set": { "qty": 5 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "item": "unique_item" }).unwrap();
    assert!(doc.contains_key("_id"));
    assert_eq!(doc.get_str("item").unwrap(), "unique_item");
    assert_eq!(doc.get_i32("qty").unwrap(), 5);
}

#[test]
fn test_upsert_preserves_equality_filter_fields() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "item": "gadget", "status": "P", "category": "electronics" },
            doc! { "$set": { "qty": 100 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "item": "gadget" }).unwrap();
    assert_eq!(doc.get_str("item").unwrap(), "gadget");
    assert_eq!(doc.get_str("status").unwrap(), "P");
    assert_eq!(doc.get_str("category").unwrap(), "electronics");
    assert_eq!(doc.get_i32("qty").unwrap(), 100);
}

#[test]
fn test_upsert_with_nested_filter_fields() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "info.type": "special", "info.level": 5 },
            doc! { "$set": { "name": "nested_doc" } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "name": "nested_doc" }).unwrap();
    let info = doc.get_document("info").unwrap();
    assert_eq!(info.get_str("type").unwrap(), "special");
    assert_eq!(info.get_i32("level").unwrap(), 5);
}

#[test]
fn test_upsert_inc_initializes_missing_field() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 200 },
            doc! { "$inc": { "counter": 10, "views": 1 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 200 }).unwrap();
    assert_eq!(doc.get_i32("counter").unwrap(), 10);
    assert_eq!(doc.get_i32("views").unwrap(), 1);
}

#[test]
fn test_upsert_mul_initializes_to_zero() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 201 },
            doc! { "$mul": { "price": 2.5 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 201 }).unwrap();
    assert_eq!(doc.get_f64("price").unwrap(), 0.0);
}

#[test]
fn test_upsert_with_push_creates_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 202 },
            doc! { "$push": { "tags": "first" } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 202 }).unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].as_str().unwrap(), "first");
}

#[test]
fn test_upsert_with_add_to_set_creates_array() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 203 },
            doc! { "$addToSet": { "categories": { "$each": ["a", "b", "c"] } } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 203 }).unwrap();
    let categories = doc.get_array("categories").unwrap();
    assert_eq!(categories.len(), 3);
}

#[test]
fn test_upsert_update_many_inserts_one_when_no_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let initial_count = collection.find(doc! {}).execute().unwrap().count();

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_many(
            doc! { "status": "NONEXISTENT" },
            doc! { "$set": { "processed": true } },
            options,
        )
        .unwrap();

    let new_count = collection.find(doc! {}).execute().unwrap().count();
    assert_eq!(new_count, initial_count + 1);

    let doc = find_one(&collection, doc! { "status": "NONEXISTENT" }).unwrap();
    assert!(doc.get_bool("processed").unwrap());
}

#[test]
fn test_upsert_update_many_updates_all_when_matches_exist() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let initial_count = collection.find(doc! {}).execute().unwrap().count();

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_many(
            doc! { "status": "A" },
            doc! { "$set": { "reviewed": true } },
            options,
        )
        .unwrap();

    let new_count = collection.find(doc! {}).execute().unwrap().count();
    assert_eq!(new_count, initial_count);

    let reviewed_count = collection
        .find(doc! { "reviewed": true })
        .execute()
        .unwrap()
        .count();
    assert_eq!(reviewed_count, 4);
}

#[test]
fn test_upsert_with_min_on_new_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 204 },
            doc! { "$min": { "lowScore": 50 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 204 }).unwrap();
    assert_eq!(doc.get_i32("lowScore").unwrap(), 50);
}

#[test]
fn test_upsert_with_max_on_new_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 205 },
            doc! { "$max": { "highScore": 100 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 205 }).unwrap();
    assert_eq!(doc.get_i32("highScore").unwrap(), 100);
}

#[test]
fn test_upsert_with_current_date() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 206 },
            doc! { "$currentDate": { "createdAt": true } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 206 }).unwrap();
    assert!(matches!(doc.get("createdAt"), Some(Bson::DateTime(_))));
}

#[test]
fn test_upsert_with_rename_on_new_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 207, "oldName": "value" },
            doc! { "$rename": { "oldName": "newName" } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 207 }).unwrap();
    assert!(!doc.contains_key("oldName"));
    assert_eq!(doc.get_str("newName").unwrap(), "value");
}

#[test]
fn test_upsert_with_bit_on_new_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 208 },
            doc! { "$bit": { "flags": { "or": 0b1010 } } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 208 }).unwrap();
    assert_eq!(doc.get_i32("flags").unwrap(), 0b1010);
}

#[test]
fn test_upsert_combines_filter_and_update_fields() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "region": "west", "type": "premium" },
            doc! { "$set": { "active": true }, "$inc": { "count": 1 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "region": "west" }).unwrap();
    assert_eq!(doc.get_str("region").unwrap(), "west");
    assert_eq!(doc.get_str("type").unwrap(), "premium");
    assert!(doc.get_bool("active").unwrap());
    assert_eq!(doc.get_i32("count").unwrap(), 1);
}

#[test]
fn test_upsert_with_complex_nested_structure() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 209 },
            doc! {
                "$set": {
                    "metadata.version": 1,
                    "metadata.tags": ["alpha", "beta"],
                    "config.enabled": true
                }
            },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 209 }).unwrap();
    let metadata = doc.get_document("metadata").unwrap();
    assert_eq!(metadata.get_i32("version").unwrap(), 1);
    let tags = metadata.get_array("tags").unwrap();
    assert_eq!(tags.len(), 2);
    let config = doc.get_document("config").unwrap();
    assert!(config.get_bool("enabled").unwrap());
}

// ============================================================================
// $setOnInsert Tests
// ============================================================================

#[test]
fn test_set_on_insert_applies_on_upsert_insert() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 300 },
            doc! { "$setOnInsert": { "defaultQty": 100, "status": "new" }, "$set": { "item": "gadget" } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 300 }).unwrap();
    assert_eq!(doc.get_i32("_id").unwrap(), 300);
    assert_eq!(doc.get_str("item").unwrap(), "gadget");
    assert_eq!(doc.get_i32("defaultQty").unwrap(), 100);
    assert_eq!(doc.get_str("status").unwrap(), "new");
}

#[test]
fn test_set_on_insert_ignored_on_update() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let doc_before = find_one(&collection, doc! { "_id": 1 }).unwrap();
    let original_qty = doc_before.get_i32("qty").unwrap();

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 1 },
            doc! { "$setOnInsert": { "qty": 999, "newField": "should_not_appear" }, "$set": { "status": "updated" } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 1 }).unwrap();
    assert_eq!(doc.get_i32("qty").unwrap(), original_qty);
    assert!(!doc.contains_key("newField"));
    assert_eq!(doc.get_str("status").unwrap(), "updated");
}

#[test]
fn test_set_on_insert_alone_on_new_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 301 },
            doc! { "$setOnInsert": { "initialized": true, "count": 0, "tags": ["default"] } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 301 }).unwrap();
    assert!(doc.get_bool("initialized").unwrap());
    assert_eq!(doc.get_i32("count").unwrap(), 0);
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].as_str().unwrap(), "default");
}

#[test]
fn test_set_on_insert_with_nested_fields() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 302 },
            doc! { "$setOnInsert": { "config.version": 1, "config.enabled": false, "meta.createdBy": "system" } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 302 }).unwrap();
    let config = doc.get_document("config").unwrap();
    assert_eq!(config.get_i32("version").unwrap(), 1);
    assert!(!config.get_bool("enabled").unwrap());
    let meta = doc.get_document("meta").unwrap();
    assert_eq!(meta.get_str("createdBy").unwrap(), "system");
}

#[test]
fn test_set_on_insert_combined_with_inc() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 303 },
            doc! { "$setOnInsert": { "baseValue": 100 }, "$inc": { "counter": 1 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 303 }).unwrap();
    assert_eq!(doc.get_i32("baseValue").unwrap(), 100);
    assert_eq!(doc.get_i32("counter").unwrap(), 1);
}

#[test]
fn test_set_on_insert_without_upsert_no_effect() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let initial_count = collection.find(doc! {}).execute().unwrap().count();

    collection
        .update_one(
            doc! { "_id": 999 },
            doc! { "$setOnInsert": { "field": "value" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let new_count = collection.find(doc! {}).execute().unwrap().count();
    assert_eq!(new_count, initial_count);

    let doc = find_one(&collection, doc! { "_id": 999 });
    assert!(doc.is_none());
}

#[test]
fn test_set_on_insert_update_many_inserts_one() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let initial_count = collection.find(doc! {}).execute().unwrap().count();

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_many(
            doc! { "category": "nonexistent" },
            doc! { "$setOnInsert": { "isNew": true }, "$set": { "processed": true } },
            options,
        )
        .unwrap();

    let new_count = collection.find(doc! {}).execute().unwrap().count();
    assert_eq!(new_count, initial_count + 1);

    let doc = find_one(&collection, doc! { "category": "nonexistent" }).unwrap();
    assert!(doc.get_bool("isNew").unwrap());
    assert!(doc.get_bool("processed").unwrap());
}

#[test]
fn test_set_on_insert_update_many_ignores_on_existing() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_many(
            doc! { "status": "A" },
            doc! { "$setOnInsert": { "shouldNotExist": true }, "$set": { "checked": true } },
            options,
        )
        .unwrap();

    let docs: Vec<Document> = collection
        .find(doc! { "status": "A" })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    for doc in docs {
        assert!(doc.get_bool("checked").unwrap());
        assert!(!doc.contains_key("shouldNotExist"));
    }
}

#[test]
fn test_upsert_false_does_not_insert() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let initial_count = collection.find(doc! {}).execute().unwrap().count();

    collection
        .update_one(
            doc! { "_id": 999 },
            doc! { "$set": { "item": "should_not_exist" } },
            UpdateOptions::default(),
        )
        .unwrap();

    let new_count = collection.find(doc! {}).execute().unwrap().count();
    assert_eq!(new_count, initial_count);

    let doc = find_one(&collection, doc! { "_id": 999 });
    assert!(doc.is_none());
}

#[test]
fn test_upsert_with_unset_on_new_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let options = UpdateOptions { upsert: true, ..Default::default() };
    collection
        .update_one(
            doc! { "_id": 210, "name": "test" },
            doc! { "$unset": { "nonexistent": "" }, "$set": { "value": 42 } },
            options,
        )
        .unwrap();

    let doc = find_one(&collection, doc! { "_id": 210 }).unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "test");
    assert_eq!(doc.get_i32("value").unwrap(), 42);
    assert!(!doc.contains_key("nonexistent"));
}
