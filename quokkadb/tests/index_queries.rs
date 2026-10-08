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
            .contains_operator(ExplainOperatorKind::InMemorySort)
            || explain
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
