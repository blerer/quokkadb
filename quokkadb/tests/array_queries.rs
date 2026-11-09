mod common;

use bson::{Document, doc};
use quokkadb::{Collection, ExplainNode, ExplainOperator, ExplainOperatorKind};
use tempfile::TempDir;

fn insert_fixture(collection: &Collection, documents: impl IntoIterator<Item = Document>) {
    collection
        .insert_many(documents.into_iter().map(|mut document| {
            document.insert("test_group", 1);
            document
        }))
        .unwrap();
}

fn plan_uses_index(node: &ExplainNode, name: &str) -> bool {
    matches!(&node.operator, ExplainOperator::IndexScan { index_name, .. } if index_name == name)
        || node
            .children
            .iter()
            .any(|child| plan_uses_index(child, name))
}

fn assert_scan_modes(
    collection: &Collection,
    query: Document,
    expected: &[i32],
    compound_indexes: &[String],
    standalone: Option<&str>,
) {
    let mut modes = vec![("collection", None), ("automatic", None)];
    modes.extend(
        compound_indexes
            .iter()
            .map(|name| ("compound", Some(name.as_str()))),
    );
    if let Some(name) = standalone {
        modes.push(("standalone", Some(name)));
    }
    for (mode, index) in modes {
        let filter = if mode == "compound" {
            doc! {"$and": [{"test_group": 1}, query.clone()]}
        } else {
            query.clone()
        };
        let mut find = collection.find(filter).sort(doc! {"_id": 1});
        if mode == "collection" {
            find = find.hint_collection_scan();
        }
        if let Some(name) = index {
            find = find.hint(name.to_owned());
        }
        let plan = find.explain().unwrap();
        if let Some(name) = index {
            assert!(
                plan_uses_index(&plan.root, name)
                    || plan.root.contains_operator(ExplainOperatorKind::NoOp),
                "query: {query:?}, mode: {mode}, index: {name}, plan: {plan:?}"
            );
        }
        let documents = find.execute_collect().unwrap();
        let ids: Vec<_> = documents
            .iter()
            .map(|doc| doc.get_i32("_id").unwrap())
            .collect();
        assert_eq!(
            ids, expected,
            "query: {query:?}, mode: {mode}, index: {index:?}"
        );
    }
}

#[test]
fn array_query_semantics_across_scan_modes() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("arrays").create_if_missing();
    insert_fixture(
        &collection,
        [
            doc! {"_id": 1, "a": [1, 10], "items": [{"x": 1}, {"y": 10}]},
            doc! {"_id": 2, "a": [5, 5_i64, 5.0], "items": [{"x": 1, "y": 10}]},
            doc! {"_id": 3, "a": []},
            doc! {"_id": 4, "a": null},
            doc! {"_id": 5},
            doc! {"_id": 6, "a": [[1, 10]]},
            doc! {"_id": 7, "a": [null, 1]},
        ],
    );
    let a_index = collection.create_index(doc! {"a": 1}).unwrap();
    let items_index = collection
        .create_index(doc! {"items.x": 1, "items.y": 1})
        .unwrap();
    let compound_indexes = [
        collection
            .create_index(doc! {"test_group": 1, "a": 1})
            .unwrap(),
        collection
            .create_index(doc! {"test_group": 1, "items.x": 1, "items.y": 1})
            .unwrap(),
    ];
    let a = Some(a_index.as_str());
    let items = Some(items_index.as_str());
    let cases: Vec<(Document, Vec<i32>, Option<&str>)> = vec![
        (doc! {"a": {"$gt": 8, "$lt": 3}}, vec![1], a),
        (
            doc! {"a": {"$elemMatch": {"$gt": 8, "$lt": 3}}},
            vec![],
            None,
        ),
        (doc! {"a": 1}, vec![1, 7], a),
        (doc! {"a": [1, 10]}, vec![1, 6], a),
        (doc! {"a": [10, 1]}, vec![], a),
        (doc! {"a": []}, vec![3], a),
        (doc! {"a": {"$in": [1, 5]}}, vec![1, 2, 7], None),
        (doc! {"a": {"$ne": 1}}, vec![2, 3, 4, 5, 6], None),
        (doc! {"a": {"$nin": [1, 5]}}, vec![3, 4, 5, 6], None),
        (doc! {"a": {"$not": {"$eq": 1}}}, vec![2, 3, 4, 5, 6], None),
        (
            doc! {"a": {"$not": {"$gt": 8}}},
            vec![2, 3, 4, 5, 6, 7],
            None,
        ),
        (doc! {"a": null}, vec![4, 5, 7], a),
        (doc! {"a": {"$ne": null}}, vec![1, 2, 3, 6], None),
        (doc! {"a": {"$in": [null]}}, vec![4, 5, 7], None),
        (doc! {"a": {"$nin": [null]}}, vec![1, 2, 3, 6], None),
        (doc! {"a": 5_i64}, vec![2], a),
        (doc! {"a": {"$all": []}}, vec![], None),
        (
            doc! {"a": {"$not": {"$all": [1, 10]}}},
            vec![2, 3, 4, 5, 6, 7],
            None,
        ),
        (
            doc! {"$and": [{"a": {"$in": [1]}}, {"a": {"$in": [10]}}]},
            vec![1],
            None,
        ),
        (doc! {"a": {"$in": [1], "$gt": 8}}, vec![1], a),
        (doc! {"items.x": 1, "items.y": 10}, vec![1, 2], items),
        (
            doc! {"items": {"$elemMatch": {"x": 1, "y": 10}}},
            vec![2],
            None,
        ),
        (doc! {"items.x": {"$ne": 1}}, vec![3, 4, 5, 6, 7], None),
        (
            doc! {"items.x": {"$exists": false}},
            vec![3, 4, 5, 6, 7],
            None,
        ),
        (doc! {"items.x": null}, vec![1, 3, 4, 5, 6, 7], items),
        (doc! {"a.0": 1}, vec![1, 6], None),
        (
            doc! {"$nor": [{"a": {"$gt": 8, "$lt": 3}}]},
            vec![2, 3, 4, 5, 6, 7],
            None,
        ),
    ];
    // All cases also run through both compound indexes using the scalar prefix.
    for (filter, expected, standalone) in cases {
        assert_scan_modes(
            &collection,
            filter,
            &expected,
            &compound_indexes,
            standalone,
        );
    }
}

#[test]
fn scalar_ranges_recheck_empty_array_index_entries() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("empty_ranges").create_if_missing();
    insert_fixture(
        &collection,
        [
            doc! {"_id": 1, "a": []},
            doc! {"_id": 2, "a": [[]]},
            doc! {"_id": 3, "a": [1, 10]},
            doc! {"_id": 4, "a": 5},
        ],
    );
    let index = collection.create_index(doc! {"a": 1}).unwrap();
    let compound = collection
        .create_index(doc! {"test_group": 1, "a": 1})
        .unwrap();
    for (query, expected) in [
        (doc! {"a": {"$gt": 5}}, vec![3]),
        (doc! {"a": {"$gte": 5}}, vec![3, 4]),
        (doc! {"a": {"$lt": 5}}, vec![3]),
        (doc! {"a": {"$lte": 5}}, vec![3, 4]),
    ] {
        assert_scan_modes(
            &collection,
            query,
            &expected,
            std::slice::from_ref(&compound),
            Some(&index),
        );
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
    insert_fixture(
        &collection,
        [
            doc! {"_id": 1, "a": [decimal]},
            doc! {"_id": 2, "a": [9_007_199_254_740_993_i64]},
            doc! {"_id": 3, "a": [f64::NAN]},
            doc! {"_id": 4, "a": [f64::INFINITY]},
        ],
    );
    let index = collection.create_index(doc! {"a": 1}).unwrap();
    let compound = collection
        .create_index(doc! {"test_group": 1, "a": 1})
        .unwrap();
    for (value, expected) in [
        (Bson::Int32(5), vec![1]),
        (Bson::Double(9_007_199_254_740_992.0), vec![]),
        (Bson::Double(f64::NAN), vec![3]),
        (Bson::Double(f64::INFINITY), vec![4]),
    ] {
        for (query, standalone) in [
            (doc! {"a": value.clone()}, Some(index.as_str())),
            (doc! {"a": [value.clone()]}, Some(index.as_str())),
            (doc! {"a": {"$in": [value]}}, None),
        ] {
            assert_scan_modes(
                &collection,
                query,
                &expected,
                std::slice::from_ref(&compound),
                standalone,
            );
        }
    }
}

#[test]
fn all_predicates_can_match_different_dotted_path_branches() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("branches").create_if_missing();
    insert_fixture(
        &collection,
        [
            doc! {"_id": 1, "items": [{"x": 1}, {"x": 10}]},
            doc! {"_id": 2, "items": [{"x": 1}]},
            doc! {"_id": 3, "items": []},
        ],
    );
    collection.create_index(doc! {"items.x": 1}).unwrap();
    let compound = collection
        .create_index(doc! {"test_group": 1, "items.x": 1})
        .unwrap();
    for (query, expected) in [
        (doc! {"items.x": {"$all": [1, 10]}}, vec![1]),
        (doc! {"items.x": {"$not": {"$all": [1, 10]}}}, vec![2, 3]),
        (doc! {"items.x": {"$exists": false}}, vec![3]),
    ] {
        assert_scan_modes(
            &collection,
            query,
            &expected,
            std::slice::from_ref(&compound),
            None,
        );
    }
}

#[test]
fn nested_elem_match_preserves_nested_element_correlation_across_scan_modes() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("nested_arrays").create_if_missing();
    insert_fixture(
        &collection,
        [
            doc! {
                "_id": 1,
                "surveys": [
                    {"name": "preferred", "results": [{"score": 9}, {"score": 1}]},
                    {"name": "other", "results": [{"score": 2}]},
                ],
            },
            doc! {
                "_id": 2,
                "surveys": [
                    {"name": "preferred", "results": [{"score": 2}]},
                    {"name": "other", "results": [{"score": 9}]},
                ],
            },
            doc! {
                "_id": 3,
                "surveys": [{"name": "preferred", "results": [{"score": 7}]}],
            },
            doc! {"_id": 4, "surveys": []},
        ],
    );
    let compound = collection
        .create_index(doc! {"test_group": 1, "surveys.results.score": 1})
        .unwrap();

    let query = doc! {
        "surveys": {
            "$elemMatch": {
                "name": "preferred",
                "results": {"$elemMatch": {"score": {"$gt": 7}}},
            },
        },
    };
    assert_scan_modes(
        &collection,
        query,
        &[1],
        std::slice::from_ref(&compound),
        None,
    );
}

#[test]
fn array_size_and_type_match_explicit_values_across_scan_modes() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("array_types").create_if_missing();
    insert_fixture(
        &collection,
        [
            doc! {"_id": 1, "a": [1, 2]},
            doc! {"_id": 2, "a": []},
            doc! {"_id": 3, "a": [1]},
            doc! {"_id": 4, "a": [[1]]},
            doc! {"_id": 5, "a": 1},
            doc! {"_id": 6, "a": "one"},
            doc! {"_id": 7, "a": null},
            doc! {"_id": 8},
        ],
    );
    let compound = collection
        .create_index(doc! {"test_group": 1, "a": 1})
        .unwrap();

    for (query, expected) in [
        (doc! {"a": {"$size": 0}}, vec![2]),
        (doc! {"a": {"$size": 1}}, vec![3, 4]),
        (doc! {"a": {"$size": 2}}, vec![1]),
        (doc! {"a": {"$type": "array"}}, vec![1, 2, 3, 4]),
        (doc! {"a": {"$type": "int"}}, vec![5]),
        (doc! {"a": {"$type": "string"}}, vec![6]),
        (doc! {"a": {"$type": "null"}}, vec![7]),
    ] {
        assert_scan_modes(
            &collection,
            query,
            &expected,
            std::slice::from_ref(&compound),
            None,
        );
    }
}
