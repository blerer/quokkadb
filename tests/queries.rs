use bson::{doc, Bson, Document};
use quokkadb::QuokkaDB;
use std::collections::{BTreeSet, HashSet};
use std::iter::FromIterator;
use tempfile::TempDir;
use quokkadb::obs::logger::{LogLevel, StdoutLogger};

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
        doc! { "_id": 9, "item": "survey1", "surveys": [ { "name": "s1", "results": [ { "item": "a", "score": 5 }, { "item": "b", "score": 8 } ] }, { "name": "s2", "results": [ { "item": "c", "score": 8 }, { "item": "d", "score": 7 } ] } ] },
        doc! { "_id": 10, "item": "survey2", "surveys": [ { "name": "s3", "results": [ { "item": "e", "score": 4 }, { "item": "f", "score": 6 } ] } ] },
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

fn get_ids(results: &[Document]) -> HashSet<i32> {
    results
        .iter()
        .map(|d| d.get_i32("_id").unwrap())
        .collect()
}

#[test]
fn test_simple_equality() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "status": "A" })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 4);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![1, 2, 5, 7]));
}

#[test]
fn test_comparison_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "qty": { "$gt": 75 } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![3, 7]));
}

#[test]
fn test_logical_and() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "status": "A", "qty": { "$lt": 30 } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![1]));
}

#[test]
fn test_logical_or() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "$or": [ { "status": "A" }, { "qty": { "$lt": 30 } } ] })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 5);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![1, 2, 5, 7, 8]));
}

#[test]
fn test_logical_nor() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "$nor": [ { "qty": { "$gte": 75 } }, { "status": "A" } ] })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![6, 8]));
}

#[test]
fn test_logical_not() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "$not": { "qty": { "$gte": 50 } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 4);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![1, 5, 6, 8]));
}

#[test]
fn test_nested_field_query() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "size.uom": "in" })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![2, 3]));
}

#[test]
fn test_exists_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "ratings": { "$exists": true } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![6, 7, 8]));
}

#[test]
fn test_type_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "item": { "$type": "string" } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 10);
}

#[test]
fn test_array_all_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "tags": { "$all": ["red", "blank"] } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 4);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![1, 2, 3, 4]));
}

#[test]
fn test_array_size_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "tags": { "$size": 3 } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![3]));
}

#[test]
fn test_array_elem_match_operator() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "ratings": { "$elemMatch": { "score": { "$gt": 8 } } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![7, 8]));
}

#[test]
fn test_projection_inclusion() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let doc = collection
        .find(doc! { "_id": 1 })
        .projection(doc! { "item": 1, "status": 1 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .next()
        .unwrap();

    let keys: BTreeSet<&str> = doc.keys().map(|s| s.as_str()).collect();
    assert_eq!(keys, BTreeSet::from_iter(vec!["_id", "item", "status"]));
    assert_eq!(doc.get_str("item").unwrap(), "journal");
    assert_eq!(doc.get_str("status").unwrap(), "A");
}

#[test]
fn test_projection_exclusion() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let doc = collection
        .find(doc! { "_id": 1 })
        .projection(doc! { "size": 0, "tags": 0 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .next()
        .unwrap();

    let keys: BTreeSet<&str> = doc.keys().map(|s| s.as_str()).collect();
    assert_eq!(
        keys,
        BTreeSet::from_iter(vec!["_id", "item", "qty", "status", "dim_cm"])
    );
}

#[test]
fn test_projection_exclude_id() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let doc = collection
        .find(doc! { "_id": 1 })
        .projection(doc! { "item": 1, "status": 1, "_id": 0 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .next()
        .unwrap();

    let keys: BTreeSet<&str> = doc.keys().map(|s| s.as_str()).collect();
    assert_eq!(keys, BTreeSet::from_iter(vec!["item", "status"]));
}

#[test]
fn test_projection_slice() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let doc = collection
        .find(doc! { "_id": 3 })
        .projection(doc! { "tags": { "$slice": 2 } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .next()
        .unwrap();
    let tags = doc.get_array("tags").unwrap();
    assert_eq!(
        tags.iter().map(|b| b.as_str().unwrap()).collect::<Vec<_>>(),
        vec!["red", "blank"]
    );
}

#[test]
fn test_projection_elem_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let doc = collection
        .find(doc! { "_id": 8 })
        .projection(doc! { "ratings": { "$elemMatch": { "score": 9 } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .next()
        .unwrap();

    let ratings = doc.get_array("ratings").unwrap();
    assert_eq!(ratings.len(), 1);
    let rating_doc = ratings[0].as_document().unwrap();
    assert_eq!(rating_doc.get_str("user").unwrap(), "F");
    assert_eq!(rating_doc.get_i32("score").unwrap(), 9);
}

#[test]
fn test_projection_positional() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");

    let results: Vec<Document> = collection
        .find(doc! { "dim_cm": 14 })
        .projection(doc! { "dim_cm.$": 1 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 1);
    let doc = &results[0];
    let dim_cm = doc.get_array("dim_cm").unwrap();
    assert_eq!(dim_cm.len(), 1);
    assert_eq!(dim_cm[0], Bson::Int32(14));
}

#[test]
fn test_sort() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "qty": { "$exists": true } })
        .sort(doc! { "qty": -1 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let qtys: Vec<i32> = results
        .iter()
        .map(|d| d.get_i32("qty").unwrap())
        .collect();
    assert_eq!(qtys, vec![100, 85, 75, 50, 45, 30, 25, 20]);
}

#[test]
fn test_skip_limit() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "qty": { "$exists": true } })
        .sort(doc! { "qty": 1 })
        .skip(2)
        .limit(3)
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 3);
    let qtys: Vec<i32> = results
        .iter()
        .map(|d| d.get_i32("qty").unwrap())
        .collect();
    assert_eq!(qtys, vec![30, 45, 50]);
}

#[test]
fn test_complex_query() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    let results: Vec<Document> = collection
        .find(doc! { "status": "A", "size.uom": "cm" })
        .projection(doc! { "item": 1, "qty": 1, "_id": 0 })
        .sort(doc! { "qty": 1 })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        doc! { "item": "journal", "qty": 25 }
    );
    assert_eq!(
        results[1],
        doc! { "item": "postcard", "qty": 45 }
    );
    assert_eq!(
        results[2],
        doc! { "item": "mat", "qty": 85 }
    );
}

#[test]
fn test_elem_match_on_documents() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // Find documents where at least one element in 'ratings' has score >= 9 AND user is 'C' or 'E'
    let results: Vec<Document> = collection
        .find(doc! { "ratings": { "$elemMatch": { "score": { "$gte": 9 }, "user": { "$in": ["C", "E"] } } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![7, 8]));
}

#[test]
fn test_elem_match_with_or() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // Find documents where at least one element in 'ratings' has score 10 OR user is 'A'
    let results: Vec<Document> = collection
        .find(doc! { "ratings": { "$elemMatch": { "$or": [{ "score": 10 }, { "user": "A" }] } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![6, 8]));
}

#[test]
fn test_elem_match_with_explicit_and() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // Find documents where at least one element in 'ratings' has score >= 9 AND user is 'F'
    let results: Vec<Document> = collection
        .find(doc! { "ratings": { "$elemMatch": { "$and": [{ "score": { "$gte": 9 } }, { "user": "F" }] } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![8]));
}

#[test]
fn test_nested_elem_match() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // Find survey documents where a survey has a result with score > 7
    let results: Vec<Document> = collection
        .find(doc! { "surveys": { "$elemMatch": { "results": { "$elemMatch": { "score": { "$gt": 7 } } } } } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![9]));
}

#[test]
fn test_elem_match_empty_document() {
    let (_dir, db) = setup_db_with_data();
    let collection = db.collection("test");
    // Find documents where 'ratings' array is not empty
    let results: Vec<Document> = collection
        .find(doc! { "ratings": { "$elemMatch": {} } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(get_ids(&results), HashSet::from_iter(vec![6, 7, 8]));

    // Find documents where 'tags' array is not empty (doc 6 has empty 'tags' array)
    let results_tags: Vec<Document> = collection
        .find(doc! { "tags": { "$elemMatch": {} } })
        .execute()
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(results_tags.len(), 7);
    assert_eq!(
        get_ids(&results_tags),
        HashSet::from_iter(vec![1, 2, 3, 4, 5, 7, 8])
    );
}
