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
