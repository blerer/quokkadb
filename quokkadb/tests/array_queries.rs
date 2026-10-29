mod common;

use bson::{Document, doc};
use tempfile::TempDir;

#[test]
fn array_query_semantics_through_collection_scans() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("arrays").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "a": [1, 10], "items": [{"x": 1}, {"y": 10}]},
            doc! {"_id": 2, "a": [5, 5_i64, 5.0], "items": [{"x": 1, "y": 10}]},
            doc! {"_id": 3, "a": []},
            doc! {"_id": 4, "a": null},
            doc! {"_id": 5},
            doc! {"_id": 6, "a": [[1, 10]]},
            doc! {"_id": 7, "a": [null, 1]},
        ])
        .unwrap();
    let cases: Vec<(Document, Vec<i32>)> = vec![
        (doc! {"a": {"$gt": 8, "$lt": 3}}, vec![1]),
        (doc! {"a": {"$elemMatch": {"$gt": 8, "$lt": 3}}}, vec![]),
        (doc! {"a": 1}, vec![1, 7]),
        (doc! {"a": [1, 10]}, vec![1, 6]),
        (doc! {"a": [10, 1]}, vec![]),
        (doc! {"a": []}, vec![3]),
        (doc! {"a": {"$in": [1, 5]}}, vec![1, 2, 7]),
        (doc! {"a": {"$ne": 1}}, vec![2, 3, 4, 5, 6]),
        (doc! {"a": {"$nin": [1, 5]}}, vec![3, 4, 5, 6]),
        (doc! {"a": {"$not": {"$eq": 1}}}, vec![2, 3, 4, 5, 6]),
        (doc! {"a": {"$not": {"$gt": 8}}}, vec![2, 3, 4, 5, 7]),
        (doc! {"a": null}, vec![4, 5, 7]),
        (doc! {"a": {"$ne": null}}, vec![1, 2, 3, 6]),
        (doc! {"a": {"$in": [null]}}, vec![4, 5, 7]),
        (doc! {"a": {"$nin": [null]}}, vec![1, 2, 3, 6]),
        (doc! {"a": 5_i64}, vec![2]),
        (doc! {"a": {"$all": []}}, vec![]),
        (
            doc! {"a": {"$not": {"$all": [1, 10]}}},
            vec![2, 3, 4, 5, 6, 7],
        ),
        (
            doc! {"$and": [{"a": {"$in": [1]}}, {"a": {"$in": [10]}}]},
            vec![1],
        ),
        (doc! {"a": {"$in": [1], "$gt": 8}}, vec![1]),
        (doc! {"items.x": 1, "items.y": 10}, vec![1, 2]),
        (doc! {"items": {"$elemMatch": {"x": 1, "y": 10}}}, vec![2]),
        (doc! {"items.x": {"$ne": 1}}, vec![3, 4, 5, 6, 7]),
        (doc! {"items.x": {"$exists": false}}, vec![3, 4, 5, 6, 7]),
        (doc! {"items.x": null}, vec![1, 3, 4, 5, 6, 7]),
        (doc! {"a.0": 1}, vec![1, 6]),
        (
            doc! {"$nor": [{"a": {"$gt": 8, "$lt": 3}}]},
            vec![2, 3, 4, 5, 6, 7],
        ),
    ];
    for (filter, expected) in cases {
        let documents = collection
            .find(filter.clone())
            .hint_collection_scan()
            .sort(doc! {"_id": 1})
            .execute()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let ids: Vec<_> = documents
            .iter()
            .map(|doc| doc.get_i32("_id").unwrap())
            .collect();
        assert_eq!(ids, expected, "filter: {filter:?}");
    }
}

#[test]
fn array_membership_uses_canonical_numeric_comparison() {
    use bson::{Bson, Decimal128};
    use std::str::FromStr;
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("numeric_arrays").create_if_missing();
    let decimal = Decimal128::from_str("5").unwrap();
    collection
        .insert_many([
            doc! {"_id": 1, "a": [decimal]},
            doc! {"_id": 2, "a": [9_007_199_254_740_993_i64]},
            doc! {"_id": 3, "a": [f64::NAN]},
            doc! {"_id": 4, "a": [f64::INFINITY]},
        ])
        .unwrap();
    for (value, expected) in [
        (Bson::Int32(5), vec![1]),
        (Bson::Double(9_007_199_254_740_992.0), vec![]),
        (Bson::Double(f64::NAN), vec![3]),
        (Bson::Double(f64::INFINITY), vec![4]),
    ] {
        for query in [
            doc! {"a": value.clone()},
            doc! {"a": [value.clone()]},
            doc! {"a": {"$in": [value]}},
        ] {
            let docs = collection
                .find(query.clone())
                .hint_collection_scan()
                .execute_collect()
                .unwrap();
            let ids: Vec<_> = docs.iter().map(|doc| doc.get_i32("_id").unwrap()).collect();
            assert_eq!(ids, expected, "query: {query:?}");
        }
    }
}

#[test]
fn all_predicates_can_match_different_dotted_path_branches() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("branches").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "items": [{"x": 1}, {"x": 10}]},
            doc! {"_id": 2, "items": [{"x": 1}]},
            doc! {"_id": 3, "items": []},
        ])
        .unwrap();
    for (query, expected) in [
        (doc! {"items.x": {"$all": [1, 10]}}, vec![1]),
        (doc! {"items.x": {"$not": {"$all": [1, 10]}}}, vec![2, 3]),
        (doc! {"items.x": {"$exists": false}}, vec![3]),
    ] {
        let docs = collection
            .find(query.clone())
            .hint_collection_scan()
            .sort(doc! {"_id": 1})
            .execute_collect()
            .unwrap();
        let ids: Vec<_> = docs.iter().map(|doc| doc.get_i32("_id").unwrap()).collect();
        assert_eq!(ids, expected, "query: {query:?}");
    }
}
