mod common;

use bson::{Document, doc};
use quokkadb::error::Error;
use quokkadb::{ExplainDirection, ExplainNode, ExplainOperator, ExplainOperatorKind};
use tempfile::TempDir;

fn explain_index_scan(node: &ExplainNode) -> Option<(&str, ExplainDirection, usize, bool)> {
    if let ExplainOperator::IndexScan {
        index_name,
        direction,
        equality_prefix_len,
        has_range,
    } = &node.operator
    {
        return Some((index_name, *direction, *equality_prefix_len, *has_range));
    }

    node.children.iter().find_map(explain_index_scan)
}

fn setup_index_query_collection() -> (TempDir, quokkadb::Collection) {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("users").create_if_missing();
    collection
        .insert_many((0..40).map(|id| {
            doc! {
                "_id": id,
                "status": if id % 2 == 0 { "active" } else { "inactive" },
                "age": 20 + (id % 10),
                "team": if id % 3 == 0 { "red" } else { "blue" },
                "note": format!("note-{id}"),
            }
        }))
        .unwrap();
    (directory, collection)
}

fn assert_index_scan(
    collection: &quokkadb::Collection,
    filter: Document,
    sort: Option<Document>,
    expected_index: &str,
    expected_direction: ExplainDirection,
    expected_equality_prefix_len: usize,
    expected_has_range: bool,
) {
    let mut find = collection.find(filter);
    if let Some(sort) = sort {
        find = find.sort(sort);
    }

    let explain = find.explain().unwrap();
    assert_eq!(
        explain_index_scan(&explain.root),
        Some((
            expected_index,
            expected_direction,
            expected_equality_prefix_len,
            expected_has_range,
        )),
        "unexpected explain plan: {explain:?}"
    );
}

fn assert_indexed_and_collection_scan_ids(
    collection: &quokkadb::Collection,
    index_name: &str,
    filter: Document,
    expected_ids: &[i32],
) {
    let indexed = collection
        .find(filter.clone())
        .hint(index_name.to_owned())
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap();
    let scanned = collection
        .find(filter.clone())
        .hint_collection_scan()
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap();
    let automatic = collection
        .find(filter.clone())
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap();

    let ids = |documents: &[Document]| {
        documents
            .iter()
            .map(|document| document.get_i32("_id").unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&indexed), expected_ids, "indexed result for {filter:?}");
    assert_eq!(ids(&scanned), expected_ids, "scan result for {filter:?}");
    assert_eq!(
        indexed, scanned,
        "indexed and scanned results differ for {filter:?}"
    );
    assert_eq!(
        automatic, scanned,
        "automatic and scanned results differ for {filter:?}"
    );
}

fn setup_type_bracketing_collection() -> (TempDir, quokkadb::Collection, String) {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("type_bracketing").create_if_missing();
    collection
        .insert_many([
            doc! { "_id": 1, "group": 1, "value": 1 },
            doc! { "_id": 2, "group": 1, "value": 5_i64 },
            doc! { "_id": 3, "group": 1, "value": "alpha" },
            doc! { "_id": 4, "group": 1, "value": "zeta" },
            doc! { "_id": 5, "group": 1, "value": { "x": 1 } },
            doc! { "_id": 6, "group": 1, "value": { "x": 2 } },
            doc! { "_id": 7, "group": 1, "value": [100] },
            doc! { "_id": 8, "group": 1, "value": null },
            doc! { "_id": 9, "group": 1 },
            doc! { "_id": 10, "group": 2, "value": 5 },
        ])
        .unwrap();
    let index_name = collection
        .create_index(doc! { "group": 1, "value": 1 })
        .unwrap();
    (directory, collection, index_name)
}

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

#[test]
fn multikey_index_queries_survive_database_restart() {
    let directory = TempDir::new().unwrap();
    let index_name = {
        let db = common::open_db(directory.path());
        let collection = db.collection("restart_arrays").create_if_missing();
        collection
            .insert_many([
                doc! {
                    "_id": 1,
                    "group": 1,
                    "items": [{"x": 1, "y": 0}, {"x": 0, "y": 2}],
                },
                doc! {
                    "_id": 2,
                    "group": 1,
                    "items": [{"x": 1, "y": 2}],
                },
                doc! {
                    "_id": 3,
                    "group": 1,
                    "items": [{"x": 1, "y": 3}],
                },
            ])
            .unwrap();
        collection
            .create_index(doc! {"group": 1, "items.x": 1, "items.y": 1})
            .unwrap()
    };

    let db = common::open_db(directory.path());
    let collection = db.collection("restart_arrays");
    let indexes = collection.list_indexes().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, index_name);

    assert_multikey_query_results(
        &collection,
        &index_name,
        doc! {"group": 1, "items": {"$elemMatch": {"x": 1, "y": 2}}},
        &[2],
    );
    assert_multikey_query_results(
        &collection,
        &index_name,
        doc! {"group": 1, "items.x": 1, "items.y": 2},
        &[1, 2],
    );
}

#[test]
fn equality_query_uses_single_field_index() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection.create_index(doc! { "status": 1 }).unwrap();
    let filter = doc! { "status": "active" };

    assert_index_scan(
        &collection,
        filter.clone(),
        None,
        &index_name,
        ExplainDirection::Forward,
        1,
        false,
    );

    let documents = collection.find(filter).execute_collect().unwrap();
    assert_eq!(documents.len(), 20);
    assert!(
        documents
            .iter()
            .all(|document| document.get_str("status").unwrap() == "active")
    );
}

#[test]
fn range_query_uses_single_field_index() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection.create_index(doc! { "age": 1 }).unwrap();
    let filter = doc! { "age": { "$gte": 26, "$lt": 29 } };

    assert_index_scan(
        &collection,
        filter.clone(),
        None,
        &index_name,
        ExplainDirection::Forward,
        0,
        true,
    );

    let documents = collection.find(filter).execute_collect().unwrap();
    assert_eq!(documents.len(), 12);
    assert!(
        documents
            .iter()
            .all(|document| (26..29).contains(&document["age"].as_i32().unwrap()))
    );
}

#[test]
fn document_lower_bound_uses_bounded_index_range() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("documents").create_if_missing();
    collection
        .insert_many([
            doc! { "_id": 1, "value": { "x": 1 } },
            doc! { "_id": 2, "value": { "x": 2 } },
            doc! { "_id": 3, "value": { "x": 3 } },
            doc! { "_id": 4, "value": "not a document" },
        ])
        .unwrap();
    let index_name = collection.create_index(doc! { "value": 1 }).unwrap();

    for (operator, expected_ids) in [("$gt", vec![2, 3]), ("$gte", vec![1, 2, 3])] {
        let filter = doc! {
            "value": {
                (operator): { "x": 1 }
            }
        };
        let explain = collection
            .find(filter.clone())
            .hint(index_name.clone())
            .explain()
            .unwrap();
        assert_eq!(
            explain_index_scan(&explain.root),
            Some((index_name.as_str(), ExplainDirection::Forward, 0, true)),
            "unexpected explain plan for {operator}: {explain:?}"
        );

        let indexed = collection
            .find(filter.clone())
            .hint(index_name.clone())
            .sort(doc! { "_id": 1 })
            .execute_collect()
            .unwrap();
        let scanned = collection
            .find(filter)
            .hint_collection_scan()
            .sort(doc! { "_id": 1 })
            .execute_collect()
            .unwrap();

        assert_eq!(indexed, scanned);
        let indexed_ids: Vec<_> = indexed
            .iter()
            .map(|document| document.get_i32("_id").unwrap())
            .collect();
        assert_eq!(indexed_ids, expected_ids);
    }
}

#[test]
fn type_bracketed_two_sided_ranges_match_collection_scans() {
    let (_directory, collection, index_name) = setup_type_bracketing_collection();

    for (filter, expected_ids) in [
        (
            doc! { "group": 1, "value": { "$gte": 0, "$lt": 10 } },
            vec![1, 2],
        ),
        (
            doc! { "group": 1, "value": { "$gte": "alpha", "$lt": "zeta" } },
            vec![3],
        ),
        (
            doc! {
                "group": 1,
                "value": { "$gte": { "x": 1 }, "$lt": { "x": 3 } }
            },
            vec![5, 6],
        ),
        (
            doc! { "group": 1, "value": { "$gte": 0, "$lt": "zeta" } },
            vec![],
        ),
    ] {
        assert_indexed_and_collection_scan_ids(&collection, &index_name, filter, &expected_ids);
    }

    assert_index_scan(
        &collection,
        doc! { "group": 1, "value": { "$gte": { "x": 1 } } },
        None,
        &index_name,
        ExplainDirection::Forward,
        1,
        true,
    );
}

#[test]
fn negated_type_bracketed_ranges_match_collection_scans() {
    let (_directory, collection, index_name) = setup_type_bracketing_collection();

    for (filter, expected_ids) in [
        (
            doc! { "group": 1, "value": { "$not": { "$gte": 5 } } },
            vec![1, 3, 4, 5, 6, 8, 9],
        ),
        (
            doc! {
                "group": 1,
                "$nor": [{ "value": { "$gte": 5 } }]
            },
            vec![1, 3, 4, 5, 6, 8, 9],
        ),
    ] {
        assert_indexed_and_collection_scan_ids(&collection, &index_name, filter, &expected_ids);
    }
}

#[test]
fn strict_null_ranges_exclude_missing_fields_through_an_index() {
    let (_directory, collection, index_name) = setup_type_bracketing_collection();

    for (operator, expected_ids) in [
        ("$gt", vec![]),
        ("$gte", vec![8]),
        ("$lt", vec![]),
        ("$lte", vec![8]),
    ] {
        let filter = doc! {
            "group": 1,
            "value": { (operator): bson::Bson::Null }
        };
        assert_indexed_and_collection_scan_ids(&collection, &index_name, filter, &expected_ids);
    }
}

#[test]
fn mixed_type_primary_key_ranges_match_collection_scans() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("mixed_ids").create_if_missing();
    collection
        .insert_many([
            doc! { "_id": 1, "name": "number-one" },
            doc! { "_id": 5_i64, "name": "number-five" },
            doc! { "_id": "alpha", "name": "string-alpha" },
            doc! { "_id": "zeta", "name": "string-zeta" },
            doc! { "_id": { "x": 1 }, "name": "document-one" },
            doc! { "_id": { "x": 2 }, "name": "document-two" },
        ])
        .unwrap();
    for (filter, expected_names) in [
        (
            doc! { "_id": { "$gte": 0, "$lt": 10 } },
            vec!["number-one", "number-five"],
        ),
        (
            doc! { "_id": { "$gte": "alpha", "$lt": "zeta" } },
            vec!["string-alpha"],
        ),
        (
            doc! { "_id": { "$gte": { "x": 1 }, "$lt": { "x": 3 } } },
            vec!["document-one", "document-two"],
        ),
        (doc! { "_id": { "$gte": 0, "$lt": "zeta" } }, vec![]),
    ] {
        let automatic = collection
            .find(filter.clone())
            .sort(doc! { "_id": 1 })
            .execute_collect()
            .unwrap();
        let scanned = collection
            .find(filter.clone())
            .hint_collection_scan()
            .sort(doc! { "_id": 1 })
            .execute_collect()
            .unwrap();
        assert_eq!(
            automatic
                .iter()
                .map(|document| document.get_str("name").unwrap().to_owned())
                .collect::<Vec<_>>(),
            expected_names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
            "automatic result for {filter:?}"
        );
        assert_eq!(
            scanned
                .iter()
                .map(|document| document.get_str("name").unwrap().to_owned())
                .collect::<Vec<_>>(),
            expected_names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
            "scan result for {filter:?}"
        );
        assert_eq!(
            automatic, scanned,
            "automatic and scanned results differ for {filter:?}"
        );
    }
}

#[test]
fn compound_index_uses_equality_prefix() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection
        .create_index(doc! { "status": 1, "age": 1 })
        .unwrap();
    let filter = doc! { "status": "active" };

    assert_index_scan(
        &collection,
        filter.clone(),
        None,
        &index_name,
        ExplainDirection::Forward,
        1,
        false,
    );

    let documents = collection.find(filter).execute_collect().unwrap();
    assert_eq!(documents.len(), 20);
    assert!(
        documents
            .iter()
            .all(|document| document.get_str("status").unwrap() == "active")
    );
}

#[test]
fn compound_index_uses_equality_prefix_and_range() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection
        .create_index(doc! { "status": 1, "age": 1 })
        .unwrap();
    let filter = doc! { "status": "active", "age": { "$gte": 26, "$lt": 29 } };

    assert_index_scan(
        &collection,
        filter.clone(),
        None,
        &index_name,
        ExplainDirection::Forward,
        1,
        true,
    );

    let documents = collection.find(filter).execute_collect().unwrap();
    assert_eq!(documents.len(), 8);
    assert!(documents.iter().all(|document| {
        document.get_str("status").unwrap() == "active"
            && (26..29).contains(&document["age"].as_i32().unwrap())
    }));
}

#[test]
fn query_hint_uses_named_index_and_reports_missing_index() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection.create_index(doc! { "status": 1 }).unwrap();

    let explain = collection
        .find(doc! { "status": "active" })
        .hint(index_name.clone())
        .explain()
        .unwrap();
    assert_eq!(
        explain_index_scan(&explain.root),
        Some((index_name.as_str(), ExplainDirection::Forward, 1, false)),
        "unexpected explain plan: {explain:?}"
    );

    let error = collection
        .find(doc! { "status": "active" })
        .hint("missing")
        .explain()
        .unwrap_err();
    assert!(matches!(
        error,
        Error::IndexNotFound {
            collection_name,
            index_name,
            id: _,
        } if collection_name == "users" && index_name == "missing"
    ));
}

#[test]
fn collection_scan_hint_forces_collection_scan() {
    let (_directory, collection) = setup_index_query_collection();
    collection.create_index(doc! { "status": 1 }).unwrap();

    let explain = collection
        .find(doc! { "status": "active" })
        .hint_collection_scan()
        .explain()
        .unwrap();
    assert!(
        explain
            .root
            .contains_operator(ExplainOperatorKind::CollectionScan)
    );
    assert!(
        !explain
            .root
            .contains_operator(ExplainOperatorKind::IndexScan)
    );
}

#[test]
fn index_scan_provides_forward_and_reverse_sort_order() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection
        .create_index(doc! { "status": 1, "age": -1 })
        .unwrap();
    let filter = doc! { "status": "active" };

    assert_index_scan(
        &collection,
        filter.clone(),
        Some(doc! { "status": 1, "age": -1 }),
        &index_name,
        ExplainDirection::Forward,
        1,
        false,
    );
    let descending = collection
        .find(filter.clone())
        .sort(doc! { "status": 1, "age": -1 })
        .execute_collect()
        .unwrap();
    assert_eq!(descending.len(), 20);
    assert!(
        descending
            .windows(2)
            .all(|pair| pair[0]["age"].as_i32().unwrap() >= pair[1]["age"].as_i32().unwrap())
    );

    assert_index_scan(
        &collection,
        filter.clone(),
        Some(doc! { "status": -1, "age": 1 }),
        &index_name,
        ExplainDirection::Reverse,
        1,
        false,
    );
    let ascending = collection
        .find(filter)
        .sort(doc! { "status": -1, "age": 1 })
        .execute_collect()
        .unwrap();
    assert_eq!(ascending.len(), 20);
    assert!(
        ascending
            .windows(2)
            .all(|pair| pair[0]["age"].as_i32().unwrap() <= pair[1]["age"].as_i32().unwrap())
    );
}

#[test]
fn indexed_query_applies_unindexed_residual_filter() {
    let (_directory, collection) = setup_index_query_collection();
    let index_name = collection.create_index(doc! { "status": 1 }).unwrap();
    let filter = doc! { "status": "active", "team": "red" };

    assert_index_scan(
        &collection,
        filter.clone(),
        None,
        &index_name,
        ExplainDirection::Forward,
        1,
        false,
    );

    let documents = collection.find(filter).execute_collect().unwrap();
    assert_eq!(documents.len(), 7);
    assert!(
        documents
            .iter()
            .all(|document| document.get_str("status").unwrap() == "active"
                && document.get_str("team").unwrap() == "red")
    );
}

#[test]
fn unindexed_query_uses_collection_scan() {
    let (_directory, collection) = setup_index_query_collection();
    collection.create_index(doc! { "status": 1 }).unwrap();
    let filter = doc! { "note": "note-7" };

    let explain = collection.find(filter.clone()).explain().unwrap();
    assert!(
        explain
            .root
            .contains_operator(ExplainOperatorKind::CollectionScan)
    );
    assert_eq!(
        explain_index_scan(&explain.root),
        None,
        "unindexed predicate unexpectedly selected an index: {explain:?}"
    );

    let documents = collection.find(filter).execute_collect().unwrap();
    assert_eq!(
        documents,
        vec![doc! {
            "_id": 7,
            "status": "inactive",
            "age": 27,
            "team": "blue",
            "note": "note-7",
        }]
    );
}

#[test]
fn sort_not_supported_by_index_does_not_use_index() {
    let (_directory, collection) = setup_index_query_collection();
    collection.create_index(doc! { "status": 1 }).unwrap();

    let explain = collection
        .find(doc! {})
        .sort(doc! { "age": 1 })
        .explain()
        .unwrap();
    assert!(
        explain
            .root
            .contains_operator(ExplainOperatorKind::CollectionScan)
    );
    assert_eq!(
        explain_index_scan(&explain.root),
        None,
        "index that cannot provide the requested sort was selected: {explain:?}"
    );
    assert!(
        explain
            .root
            .contains_operator(ExplainOperatorKind::ExternalMergeSort)
            || explain
                .root
                .contains_operator(ExplainOperatorKind::TopKHeapSort),
        "expected a sort operator in explain plan: {explain:?}"
    );

    let documents = collection
        .find(doc! {})
        .sort(doc! { "age": 1 })
        .execute_collect()
        .unwrap();
    assert_eq!(documents.len(), 40);
    assert!(
        documents
            .windows(2)
            .all(|pair| pair[0]["age"].as_i32().unwrap() <= pair[1]["age"].as_i32().unwrap())
    );
}

#[test]
fn explain_reports_limit_without_executing_the_query() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("users").create_if_missing();
    collection
        .insert_many((0..10).map(|id| doc! { "_id": id }))
        .unwrap();

    let reads_before_explain = db.metrics().executor().read_queries();
    let explain = collection.find(doc! {}).skip(2).limit(5).explain().unwrap();

    assert_eq!(
        explain.root.operator,
        ExplainOperator::Limit {
            skip: Some(2),
            limit: Some(5),
        }
    );
    assert_eq!(db.metrics().executor().read_queries(), reads_before_explain);

    collection
        .find(doc! {})
        .skip(2)
        .limit(5)
        .execute_collect()
        .unwrap();
    assert_eq!(
        db.metrics().executor().read_queries(),
        reads_before_explain + 1
    );
}

#[test]
fn explain_observes_missing_collection_policy() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());

    let error = db
        .collection("missing")
        .find(doc! {})
        .explain()
        .unwrap_err();
    assert!(matches!(error, Error::CollectionNotFound { .. }));

    let explain = db
        .collection("missing_if_created")
        .create_if_missing()
        .find(doc! {})
        .explain()
        .unwrap();
    assert_eq!(explain.root.operator, ExplainOperator::NoOp);
}

#[test]
fn multikey_independent_predicates_and_elem_match_reach_executor() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("array_predicates").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "group": 1, "a": [1, 2, 10, 10]},
            doc! {"_id": 2, "group": 1, "a": [2, 8]},
            doc! {"_id": 3, "group": 1, "a": [6, 7]},
            doc! {"_id": 4, "group": 1, "a": [1, 2]},
            doc! {"_id": 5, "group": 2, "a": [1, 10]},
        ])
        .unwrap();
    let index = collection.create_index(doc! {"group": 1, "a": 1}).unwrap();
    for (filter, ids) in [
        (doc! {"group": 1, "a": {"$gt": 5, "$lt": 3}}, vec![1, 2]),
        (
            doc! {"group": 1, "a": {"$elemMatch": {"$gt": 5, "$lt": 3}}},
            vec![],
        ),
        (
            doc! {"group": 1, "a": {"$elemMatch": {"$gt": 5, "$lt": 9}}},
            vec![2, 3],
        ),
    ] {
        assert_multikey_query_results(&collection, &index, filter, &ids);
    }

    // Without a sort, pagination consumes the index iterator directly. Document 1
    // has multiple matching keys but must consume only one position in the page.
    let page = collection
        .find(doc! {"group": 1, "a": {"$gt": 5, "$lt": 3}})
        .hint(index)
        .skip(1)
        .limit(1)
        .execute_collect()
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].get_i32("_id").unwrap(), 2);
}

#[test]
fn multikey_shared_paths_preserve_element_correlation() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("shared_paths").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "group": 1, "items": [{"x": 1, "y": 0}, {"x": 0, "y": 2}]},
            doc! {"_id": 2, "group": 1, "items": [{"x": 1, "y": 2}, {"x": 1, "y": 2}]},
            doc! {"_id": 3, "group": 1, "items": [{"x": 1, "y": 0}]},
            doc! {"_id": 4, "group": 2, "items": [{"x": 1, "y": 2}]},
        ])
        .unwrap();
    let index = collection
        .create_index(doc! {"group": 1, "items.x": 1, "items.y": 1})
        .unwrap();
    assert_multikey_query_results(
        &collection,
        &index,
        doc! {"group": 1, "items.x": 1, "items.y": 2},
        &[1, 2],
    );
    assert_multikey_query_results(
        &collection,
        &index,
        doc! {"group": 1, "items": {"$elemMatch": {"x": 1, "y": 2}}},
        &[2],
    );
}

#[test]
fn multikey_empty_missing_and_null_values_match_collection_scans() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("array_nulls").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "group": 1},
            doc! {"_id": 2, "group": 1, "a": null},
            doc! {"_id": 3, "group": 1, "a": []},
            doc! {"_id": 4, "group": 1, "a": [null, 1]},
            doc! {"_id": 5, "group": 1, "a": [[]]},
            doc! {"_id": 6, "group": 1, "a": [1]},
        ])
        .unwrap();
    let index = collection.create_index(doc! {"group": 1, "a": 1}).unwrap();
    for (predicate, ids) in [
        (doc! {"$eq": null}, vec![1, 2, 4]),
        (doc! {"$eq": []}, vec![3, 5]),
        (doc! {"$exists": false}, vec![1]),
        (doc! {"$exists": true}, vec![2, 3, 4, 5, 6]),
    ] {
        assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": predicate}, &ids);
    }
}

#[test]
fn multikey_index_entries_track_array_updates_replacements_and_deletes() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("array_index_writes").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "group": 1, "a": [1, 2]},
            doc! {"_id": 2, "group": 1, "a": [2, 3]},
            doc! {"_id": 3, "group": 1, "a": [3, 4]},
        ])
        .unwrap();
    let index = collection.create_index(doc! {"group": 1, "a": 1}).unwrap();

    assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": 2}, &[1, 2]);

    collection
        .update_one(doc! {"_id": 1}, doc! {"$set": {"a": 9}})
        .unwrap();
    assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": 2}, &[2]);
    assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": 9}, &[1]);

    collection
        .replace_one(doc! {"_id": 2}, doc! {"_id": 2, "group": 1, "a": [5, 6]})
        .unwrap();
    assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": 3}, &[3]);
    assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": 5}, &[2]);

    collection.delete_one(doc! {"_id": 3}).unwrap();
    assert_multikey_query_results(&collection, &index, doc! {"group": 1, "a": 4}, &[]);
}

// Exercise parsing, normalization, parameterization, planning, and execution through
// the public API. Explicit IDs prevent two access paths sharing a bug from passing.
fn assert_multikey_query_results(
    collection: &quokkadb::Collection,
    index: &str,
    filter: Document,
    expected_ids: &[i32],
) {
    for direction in [1, -1] {
        let mut ordered_ids = expected_ids.to_vec();
        if direction == -1 {
            ordered_ids.reverse();
        }
        for page in [false, true] {
            let mut indexed = collection
                .find(filter.clone())
                .hint(index.to_owned())
                .sort(doc! {"_id": direction});
            let mut scanned = collection
                .find(filter.clone())
                .hint_collection_scan()
                .sort(doc! {"_id": direction});
            let mut automatic = collection
                .find(filter.clone())
                .sort(doc! {"_id": direction});
            let expected: Vec<_> = if page {
                indexed = indexed.skip(1).limit(2);
                scanned = scanned.skip(1).limit(2);
                automatic = automatic.skip(1).limit(2);
                ordered_ids.iter().copied().skip(1).take(2).collect()
            } else {
                ordered_ids.clone()
            };
            assert!(explain_index_scan(&indexed.explain().unwrap().root).is_some());
            let actual = indexed.execute_collect().unwrap();
            let ids: Vec<_> = actual
                .iter()
                .map(|doc| doc.get_i32("_id").unwrap())
                .collect();
            assert_eq!(
                ids, expected,
                "filter: {filter:?}, direction: {direction}, page: {page}"
            );
            assert_eq!(
                actual,
                scanned.execute_collect().unwrap(),
                "filter: {filter:?}"
            );
            assert_eq!(
                actual,
                automatic.execute_collect().unwrap(),
                "filter: {filter:?}"
            );
        }
    }
}

#[test]
fn array_bounds_match_collection_scans_and_separate_cached_plans() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("array_bounds").create_if_missing();
    collection
        .insert_many([
            doc! {"_id": 1, "category": 1, "a": [1, 2]},
            doc! {"_id": 2, "category": 1, "a": [[1, 2], 3]},
            doc! {"_id": 3, "category": 1, "a": []},
            doc! {"_id": 4, "category": 1, "a": [[]]},
            doc! {"_id": 5, "category": 2, "a": [1, 2]},
            doc! {"_id": 6, "category": 1, "a": 1},
            doc! {"_id": 7, "category": 1, "a": [2, 3]},
        ])
        .unwrap();
    let single = collection.create_index(doc! {"a": 1}).unwrap();
    let compound = collection
        .create_index(doc! {"category": 1, "a": 1})
        .unwrap();

    for (predicate, single_ids, compound_ids) in [
        (doc! {"$eq": 1}, vec![1, 5, 6], vec![1, 6]),
        (doc! {"$gt": 1}, vec![1, 2, 5, 7], vec![1, 2, 7]),
        (doc! {"$gte": 1}, vec![1, 2, 5, 6, 7], vec![1, 2, 6, 7]),
    ] {
        assert_indexed_and_collection_scan_ids(
            &collection,
            &single,
            doc! {"a": predicate.clone()},
            &single_ids,
        );
        assert_indexed_and_collection_scan_ids(
            &collection,
            &compound,
            doc! {"category": 1, "a": predicate},
            &compound_ids,
        );
    }

    // No writes below: all parameter variants execute under the same schema version.
    for (index, prefix) in [(single, false), (compound, true)] {
        for operator in ["$eq", "$gt", "$gte", "$lt", "$lte"] {
            for value in [
                bson::bson!(1),
                bson::bson!([1, 2]),
                bson::bson!(1),
                bson::bson!([]),
                bson::bson!([[1, 2]]),
            ] {
                let mut comparison = Document::new();
                comparison.insert(operator, value);
                let mut filter = doc! {"a": comparison};
                if prefix {
                    filter.insert("category", 1);
                }
                let expected = collection
                    .find(filter.clone())
                    .hint_collection_scan()
                    .sort(doc! {"_id": 1})
                    .execute_collect()
                    .unwrap();
                let actual = collection
                    .find(filter.clone())
                    .hint(index.clone())
                    .sort(doc! {"_id": 1})
                    .execute_collect()
                    .unwrap();
                assert_eq!(actual, expected, "filter: {filter:?}");
                let automatic = collection
                    .find(filter.clone())
                    .sort(doc! {"_id": 1})
                    .execute_collect()
                    .unwrap();
                assert_eq!(automatic, expected, "filter: {filter:?}");
            }
        }
    }
}

#[test]
fn multikey_sort_replans_and_matches_document_sort() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("multikey_sort").create_if_missing();
    collection.insert_one(doc! {"_id": 1, "a": 5}).unwrap();
    let index = collection.create_index(doc! {"a": 1}).unwrap();
    for direction in [1, -1] {
        collection
            .find(doc! {})
            .hint(index.clone())
            .sort(doc! {"a": direction})
            .execute_collect()
            .unwrap();
    }
    // Array value ordering differs from the order of the first matching index element.
    collection
        .insert_many([
            doc! {"_id": 2, "a": [9, 1]},
            doc! {"_id": 3, "a": [2, 8]},
            doc! {"_id": 4, "a": [4, 7]},
        ])
        .unwrap();
    for direction in [1, -1] {
        for filter in [doc! {}, doc! {"a": {"$gt": 3}}, doc! {"_id": {"$gt": 1}}] {
            for page in [false, true] {
                let mut indexed = collection
                    .find(filter.clone())
                    .hint(index.clone())
                    .sort(doc! {"a": direction});
                let mut scanned = collection
                    .find(filter.clone())
                    .hint_collection_scan()
                    .sort(doc! {"a": direction});
                if page {
                    indexed = indexed.skip(1).limit(2);
                    scanned = scanned.skip(1).limit(2);
                }
                if !filter.contains_key("a") {
                    assert!(matches!(
                        indexed.explain(),
                        Err(Error::InvalidRequest(message))
                            if message.contains("Cannot use hinted index")
                    ));
                    continue;
                }
                let explain = indexed.explain().unwrap();
                assert!(explain_index_scan(&explain.root).is_some());
                assert!(
                    explain
                        .root
                        .contains_operator(ExplainOperatorKind::ExternalMergeSort)
                        || explain
                            .root
                            .contains_operator(ExplainOperatorKind::TopKHeapSort)
                );
                assert_eq!(
                    indexed.execute_collect().unwrap(),
                    scanned.execute_collect().unwrap()
                );
            }
        }
    }
}

#[test]
fn compound_multikey_bounds_replan_and_match_collection_scan() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("items").create_if_missing();
    collection
        .insert_one(doc! {"_id": 1, "items": {"x": 1, "y": 2}})
        .unwrap();
    let index = collection
        .create_index(doc! {"items.x": 1, "items.y": 1})
        .unwrap();
    let filter = doc! {"items.x": 1, "items.y": 2};
    // Populate the cache while both indexed paths are scalar.
    assert_eq!(
        collection
            .find(filter.clone())
            .hint(index.clone())
            .execute_collect()
            .unwrap()
            .len(),
        1
    );
    let explain = collection
        .find(filter.clone())
        .hint(index.clone())
        .explain()
        .unwrap();
    assert_eq!(explain_index_scan(&explain.root).unwrap().2, 2);

    collection
        .insert_many([
            doc! {"_id": 2, "items": [{"x": 1, "y": 0}, {"x": 0, "y": 2}]},
            doc! {"_id": 3, "items": [{"x": 1, "y": 0}, {"x": 1, "y": 3}]},
            doc! {"_id": 4, "items": [{"x": 0, "y": 2}]},
        ])
        .unwrap();
    let explain = collection
        .find(filter.clone())
        .hint(index.clone())
        .explain()
        .unwrap();
    assert_eq!(explain_index_scan(&explain.root).unwrap().2, 1);
    let ids = |documents: Vec<Document>| {
        let mut ids: Vec<_> = documents
            .iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect();
        ids.sort();
        ids
    };
    let indexed = collection
        .find(filter.clone())
        .hint(index.clone())
        .execute_collect()
        .unwrap();
    let scanned = collection
        .find(filter.clone())
        .hint_collection_scan()
        .execute_collect()
        .unwrap();
    assert_eq!(ids(indexed), vec![1, 2]);
    assert_eq!(ids(scanned), vec![1, 2]);
    let limited = collection
        .find(filter)
        .hint(index)
        .limit(2)
        .execute_collect()
        .unwrap();
    assert_eq!(ids(limited), vec![1, 2]);
}
