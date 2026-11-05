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
                // Scalar equality exercises cache separation in both directions.
                // Scalar ranges over empty arrays have separate index semantics.
                if operator != "$eq" && !matches!(&value, bson::Bson::Array(_)) {
                    continue;
                }
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
