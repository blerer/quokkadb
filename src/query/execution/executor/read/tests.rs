use super::*;
use crate::query::execution::executor::test_utils::{
    assert_insert_one_result, executor_test_runtime, full_scan_plan, insert_docs, insert_one,
    inserted_id, inserted_ids, point_search_query, write_batch,
};
use crate::query::execution::QueryExecutor;
use crate::query::expr_fn::{
    all, and, at_least, at_most, elem_match, exists, field, field_filters, greater_than, has_type,
    interval, less_than, ne, nor, not, or, point, proj_array_elements, proj_elem_match, proj_field,
    proj_fields, proj_slice, size, within,
};
use crate::query::physical_plan::{IndexScanRangeExpr, PhysicalPlan};
use crate::query::{make_sort_field, BsonValue, Parameters, Projection, SortOrder};
use crate::storage::catalog::{IndexDefinition, IndexOptions, OrderedIndexField};
use crate::storage::operation::Operation;
use crate::storage::Direction;
use crate::util::bson_utils;
use crate::util::bson_utils::BsonKey;
use bson::{doc, Bson, Document};
use std::sync::Arc;

#[test]
fn test_execution_roundtrip() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test")?;

    // 2. InsertOne
    let doc1 = doc! { "name": "doc1", "value": 1 };
    let inserted_id1 = inserted_id(insert_one(&executor, collection_id, &doc1)?);

    // 3. PointSearch for the inserted doc
    let mut params = Parameters::new();
    let point_search_plan =
        point_search_query(collection_id, &mut params, BsonValue(inserted_id1.clone()));

    let mut point_search_result = executor.execute_cached(point_search_plan, &params)?;
    let found_doc1 = point_search_result.next().unwrap()?;
    assert!(point_search_result.next().is_none());

    let mut expected_doc1 = doc1.clone();
    expected_doc1.insert("_id", inserted_id1.clone());
    assert_eq!(found_doc1, expected_doc1);

    // 4. InsertMany
    let doc2 = doc! { "name": "doc2", "value": 2 };
    let doc3 = doc! { "name": "doc3", "value": 3 };
    let insert_many_plan = PhysicalPlan::InsertMany {
        collection: collection_id,
        documents: vec![doc2.to_vec()?, doc3.to_vec()?],
    };

    let inserted_ids = inserted_ids(executor.execute_direct(insert_many_plan, None, false)?);
    assert_eq!(inserted_ids.len(), 2);
    let inserted_id2 = inserted_ids[0].clone();
    let inserted_id3 = inserted_ids[1].clone();

    // 5. CollectionScan
    let scan_plan = full_scan_plan(collection_id);

    let scan_results = executor.execute_cached(scan_plan, &Parameters::new())?;
    let mut found_docs: Vec<Document> = scan_results.collect::<Result<Vec<_>>>()?;

    // The order of results from scan is based on key order.
    // ObjectIds are mostly monotonic, but to be safe let's sort by _id.
    found_docs.sort_by(|a, b| {
        let id_a = a.get("_id").unwrap();
        let id_b = b.get("_id").unwrap();
        bson_utils::cmp_bson(id_a, id_b)
    });

    let mut expected_docs = vec![];
    let mut expected_doc1_with_id = doc1;
    expected_doc1_with_id.insert("_id", inserted_id1);
    expected_docs.push(expected_doc1_with_id);

    let mut expected_doc2_with_id = doc2;
    expected_doc2_with_id.insert("_id", inserted_id2);
    expected_docs.push(expected_doc2_with_id);

    let mut expected_doc3_with_id = doc3;
    expected_doc3_with_id.insert("_id", inserted_id3);
    expected_docs.push(expected_doc3_with_id);

    expected_docs.sort_by(|a, b| {
        let id_a = a.get("_id").unwrap();
        let id_b = b.get("_id").unwrap();
        bson_utils::cmp_bson(id_a, id_b)
    });

    assert_eq!(found_docs, expected_docs);

    Ok(())
}

#[test]
fn test_search_and_scan_edge_cases() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_edge")?;

    // Insert some docs with known integer _id's for predictable range scans
    let doc1 = doc! { "_id": 10i32, "name": "doc10" };
    let doc2 = doc! { "_id": 20i32, "name": "doc20" };
    let doc3 = doc! { "_id": 30i32, "name": "doc30" };
    let doc_to_delete = doc! { "_id": 40i32, "name": "doc40_to_delete" };

    for doc in [&doc1, &doc2, &doc3, &doc_to_delete] {
        assert_insert_one_result(
            insert_one(&executor, collection_id, doc)?,
            doc.get("_id").unwrap().clone(),
        );
    }

    // 2. PointSearch for non-existent key
    let mut params_non_exist = Parameters::new();
    let plan_non_exist = point_search_query(collection_id, &mut params_non_exist, 99_i32);
    let mut result_non_exist = executor.execute_cached(plan_non_exist, &params_non_exist)?;
    assert!(
        result_non_exist.next().is_none(),
        "PointSearch for non-existent key should be empty"
    );

    // 3. PointSearch for deleted key
    // 3a. Delete the document via direct storage engine write
    let key_to_delete = BsonValue(Bson::Int32(40)).try_into_key()?;
    let delete_op = Operation::new_delete(collection_id, 0, key_to_delete);
    storage_engine.write(write_batch(vec![delete_op]), false)?;

    // 3b. Search for it
    let mut params_deleted = Parameters::new();
    let plan_deleted = point_search_query(collection_id, &mut params_deleted, 40_i32);
    let mut result_deleted = executor.execute_cached(plan_deleted, &params_deleted)?;
    assert!(
        result_deleted.next().is_none(),
        "PointSearch for deleted key should be empty"
    );

    // 4. CollectionScan with range completely outside data
    let mut params_scan_outside = Parameters::new();
    let start_outside = params_scan_outside.collect_parameter(BsonValue(Bson::Int32(100)));
    let plan_scan_outside = Arc::new(PhysicalPlan::CollectionScan {
        collection: collection_id,
        range: Interval::at_least(start_outside.clone()),
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });
    let mut result_scan_outside =
        executor.execute_cached(plan_scan_outside, &params_scan_outside)?;
    assert!(
        result_scan_outside.next().is_none(),
        "Scan for range outside data should be empty"
    );

    // 5. CollectionScan with partial range
    let mut params_scan_partial = Parameters::new();
    let start_partial = params_scan_partial.collect_parameter(BsonValue(Bson::Int32(15)));
    let end_partial = params_scan_partial.collect_parameter(BsonValue(Bson::Int32(25)));
    let plan_scan_partial = Arc::new(PhysicalPlan::CollectionScan {
        collection: collection_id,
        range: Interval::closed_open(start_partial.clone(), end_partial.clone()),
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });
    let mut result_scan_partial =
        executor.execute_cached(plan_scan_partial, &params_scan_partial)?;
    let found_doc = result_scan_partial.next().unwrap()?;
    assert_eq!(found_doc, doc2.clone());
    assert!(
        result_scan_partial.next().is_none(),
        "Partial scan should find exactly one document"
    );

    // 6. CollectionScan with unbounded start
    let mut params_scan_unbounded_start = Parameters::new();
    let end_unbounded_start =
        params_scan_unbounded_start.collect_parameter(BsonValue(Bson::Int32(20)));
    let plan_scan_unbounded_start = Arc::new(PhysicalPlan::CollectionScan {
        collection: collection_id,
        range: Interval::less_than(end_unbounded_start.clone()),
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });
    let mut result_unbounded_start =
        executor.execute_cached(plan_scan_unbounded_start, &params_scan_unbounded_start)?;
    let found_doc_unbounded_start = result_unbounded_start.next().unwrap()?;
    assert_eq!(found_doc_unbounded_start, doc1.clone());
    assert!(
        result_unbounded_start.next().is_none(),
        "Scan with unbounded start should find one doc"
    );

    // 7. CollectionScan with unbounded end
    let mut params_scan_unbounded_end = Parameters::new();
    let start_unbounded_end =
        params_scan_unbounded_end.collect_parameter(BsonValue(Bson::Int32(20)));
    let plan_scan_unbounded_end = Arc::new(PhysicalPlan::CollectionScan {
        collection: collection_id,
        range: Interval::greater_than(start_unbounded_end.clone()),
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });
    let mut result_unbounded_end =
        executor.execute_cached(plan_scan_unbounded_end, &params_scan_unbounded_end)?;
    let found_doc_unbounded_end = result_unbounded_end.next().unwrap()?;
    assert_eq!(found_doc_unbounded_end, doc3.clone());
    assert!(
        result_unbounded_end.next().is_none(),
        "Scan with unbounded end should find one doc"
    );

    // 8. CollectionScan with full range in reverse
    let plan_scan_reverse = Arc::new(PhysicalPlan::CollectionScan {
        collection: collection_id,
        range: Interval::all(),
        direction: Direction::Reverse,
        filter: None,
        projection: None,
    });
    let result_scan_reverse = executor.execute_cached(plan_scan_reverse, &Parameters::new())?;
    let found_docs_reverse: Vec<Document> = result_scan_reverse.collect::<Result<Vec<_>>>()?;
    assert_eq!(found_docs_reverse.len(), 3);
    assert_eq!(found_docs_reverse[0], doc3.clone());
    assert_eq!(found_docs_reverse[1], doc2.clone());
    assert_eq!(found_docs_reverse[2], doc1.clone());

    Ok(())
}

#[test]
fn test_index_scan_single_field_equality_execution() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_index_eq")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("a")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    for doc in [
        doc! { "_id": 1_i32, "a": 10_i32, "tag": "x" },
        doc! { "_id": 2_i32, "a": 20_i32, "tag": "y" },
        doc! { "_id": 3_i32, "a": 10_i32, "tag": "z" },
    ] {
        insert_one(&executor, collection_id, &doc)?;
    }

    let mut params = Parameters::new();
    let a_eq = params.collect_parameter(10_i32.into());
    let plan = Arc::new(PhysicalPlan::IndexScan {
        collection: collection_id,
        index: index_id,
        range: IndexScanRangeExpr {
            equal_prefix: vec![a_eq],
            tail: None,
        },
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });

    let results = executor.execute_cached(plan, &params)?;
    let docs: Vec<Document> = results.collect::<Result<_>>()?;
    let ids: Vec<i32> = docs.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, vec![1, 3]);

    Ok(())
}

#[test]
fn test_index_scan_compound_prefix_plus_tail_range_execution() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_index_compound")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![
            OrderedIndexField::asc("a"),
            OrderedIndexField::desc("b"),
        ]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    for doc in [
        doc! { "_id": 1_i32, "a": 10_i32, "b": 30_i32 },
        doc! { "_id": 2_i32, "a": 10_i32, "b": 20_i32 },
        doc! { "_id": 3_i32, "a": 10_i32, "b": 10_i32 },
        doc! { "_id": 4_i32, "a": 9_i32, "b": 50_i32 },
    ] {
        insert_one(&executor, collection_id, &doc)?;
    }

    let mut params = Parameters::new();
    let a_eq = params.collect_parameter(10_i32.into());
    let b_gt = params.collect_parameter(20_i32.into());
    let plan = Arc::new(PhysicalPlan::IndexScan {
        collection: collection_id,
        index: index_id,
        range: IndexScanRangeExpr {
            equal_prefix: vec![a_eq],
            tail: Some(Interval::greater_than(b_gt)),
        },
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });

    let results = executor.execute_cached(plan, &params)?;
    let docs: Vec<Document> = results.collect::<Result<_>>()?;
    let ids: Vec<i32> = docs.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, vec![1]);

    Ok(())
}

#[test]
fn test_index_scan_reverse_direction_and_residual_filter_execution() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_index_reverse")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("a")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    for doc in [
        doc! { "_id": 1_i32, "a": 10_i32, "kind": "keep" },
        doc! { "_id": 2_i32, "a": 20_i32, "kind": "drop" },
        doc! { "_id": 3_i32, "a": 30_i32, "kind": "keep" },
    ] {
        insert_one(&executor, collection_id, &doc)?;
    }

    let mut params = Parameters::new();
    let kind_keep = params.collect_parameter(BsonValue(Bson::String("keep".to_string())));
    let plan = Arc::new(PhysicalPlan::IndexScan {
        collection: collection_id,
        index: index_id,
        range: IndexScanRangeExpr {
            equal_prefix: vec![],
            tail: None,
        },
        direction: Direction::Reverse,
        filter: Some(field_filters(
            field(["kind"]),
            [interval(point(&kind_keep))],
        )),
        projection: None,
    });

    let results = executor.execute_cached(plan, &params)?;
    let docs: Vec<Document> = results.collect::<Result<_>>()?;
    let ids: Vec<i32> = docs.iter().map(|d| d.get_i32("_id").unwrap()).collect();
    assert_eq!(ids, vec![3, 1]);

    Ok(())
}

#[test]
fn test_limit_plan_execution() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_limit")?;

    // 2. Insert 5 documents
    let docs: Vec<Document> = (1..=5)
        .map(|i| doc! { "_id": i, "name": format!("doc{}", i) })
        .collect();

    insert_docs(&executor, collection_id, docs.iter())?;

    // 3. Create a base scan plan to feed the limit plan
    let scan_plan = full_scan_plan(collection_id);

    // 4. Test cases

    // Case 1: limit only
    let limit_plan = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: None,
        limit: Some(3),
    });
    let results = executor.execute_cached(limit_plan, &Parameters::new())?;
    let found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
    assert_eq!(found_docs.len(), 3);
    assert_eq!(found_docs[0].get_i32("_id")?, 1);
    assert_eq!(found_docs[1].get_i32("_id")?, 2);
    assert_eq!(found_docs[2].get_i32("_id")?, 3);

    // Case 2: skip only
    let limit_plan_skip = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: Some(2),
        limit: None,
    });
    let results_skip = executor.execute_cached(limit_plan_skip, &Parameters::new())?;
    let found_docs_skip: Vec<Document> = results_skip.collect::<Result<Vec<_>>>()?;
    assert_eq!(found_docs_skip.len(), 3);
    assert_eq!(found_docs_skip[0].get_i32("_id")?, 3);
    assert_eq!(found_docs_skip[1].get_i32("_id")?, 4);
    assert_eq!(found_docs_skip[2].get_i32("_id")?, 5);

    // Case 3: skip and limit
    let limit_plan_both = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: Some(1),
        limit: Some(2),
    });
    let results_both = executor.execute_cached(limit_plan_both, &Parameters::new())?;
    let found_docs_both: Vec<Document> = results_both.collect::<Result<Vec<_>>>()?;
    assert_eq!(found_docs_both.len(), 2);
    assert_eq!(found_docs_both[0].get_i32("_id")?, 2);
    assert_eq!(found_docs_both[1].get_i32("_id")?, 3);

    // Case 4: limit > number of docs
    let limit_plan_large = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: None,
        limit: Some(10),
    });
    let results_large = executor.execute_cached(limit_plan_large, &Parameters::new())?;
    let found_docs_large: Vec<Document> = results_large.collect::<Result<Vec<_>>>()?;
    assert_eq!(found_docs_large.len(), 5);

    // Case 5: skip > number of docs
    let limit_plan_skip_large = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: Some(10),
        limit: None,
    });
    let results_skip_large = executor.execute_cached(limit_plan_skip_large, &Parameters::new())?;
    let found_docs_skip_large: Vec<Document> = results_skip_large.collect::<Result<Vec<_>>>()?;
    assert!(found_docs_skip_large.is_empty());

    // Case 6: limit is zero
    let limit_plan_zero = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: None,
        limit: Some(0),
    });
    let results_zero = executor.execute_cached(limit_plan_zero, &Parameters::new())?;
    let found_docs_zero: Vec<Document> = results_zero.collect::<Result<Vec<_>>>()?;
    assert!(found_docs_zero.is_empty());

    // Case 7: skip and limit over edge
    let limit_plan_edge = Arc::new(PhysicalPlan::Limit {
        input: scan_plan.clone(),
        skip: Some(3),
        limit: Some(5), // limit is larger than remaining items
    });
    let results_edge = executor.execute_cached(limit_plan_edge, &Parameters::new())?;
    let found_docs_edge: Vec<Document> = results_edge.collect::<Result<Vec<_>>>()?;
    assert_eq!(found_docs_edge.len(), 2);
    assert_eq!(found_docs_edge[0].get_i32("_id")?, 4);
    assert_eq!(found_docs_edge[1].get_i32("_id")?, 5);

    Ok(())
}

fn assert_sorted_results(
    executor: &QueryExecutor,
    plan: Arc<PhysicalPlan>,
    expected_ids: &[i32],
) -> Result<()> {
    let results = executor.execute_cached(plan, &Parameters::new())?;
    let found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
    let found_ids: Vec<i32> = found_docs
        .iter()
        .map(|d| d.get_i32("_id").unwrap())
        .collect();
    assert_eq!(found_ids, expected_ids);
    Ok(())
}

#[test]
fn test_sort_plans_execution() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_sorts")?;

    // 2. Insert test data
    let docs = vec![
        doc! { "_id": 1, "name": "c", "value": 10.0 },
        doc! { "_id": 2, "name": "a", "value": 30.0 },
        doc! { "_id": 3, "name": "b", "value": 20.0 },
        doc! { "_id": 4, "name": "a", "value": 10.0 },
        doc! { "_id": 5, "name": "c", "value": 5.0 },
    ];
    insert_docs(&executor, collection_id, docs.iter())?;

    // 3. Define sort fields and expected order
    let sort_fields = Arc::new(vec![
        make_sort_field(vec!["name".into()], SortOrder::Ascending),
        make_sort_field(vec!["value".into()], SortOrder::Ascending),
    ]);
    let expected_ids = vec![4, 2, 3, 5, 1];

    let scan_plan = full_scan_plan(collection_id);

    // --- In-Memory Sort ---
    let mem_sort_plan = Arc::new(PhysicalPlan::InMemorySort {
        input: scan_plan.clone(),
        sort_fields: sort_fields.clone(),
    });
    assert_sorted_results(&executor, mem_sort_plan, &expected_ids)?;

    // --- External Merge Sort ---
    let ext_sort_plan = Arc::new(PhysicalPlan::ExternalMergeSort {
        input: scan_plan.clone(),
        sort_fields: sort_fields.clone(),
        max_in_memory_rows: 2,
    });
    assert_sorted_results(&executor, ext_sort_plan, &expected_ids)?;

    // --- Top-K Heap Sort ---
    // k=3
    let topk_plan = Arc::new(PhysicalPlan::TopKHeapSort {
        input: scan_plan.clone(),
        sort_fields: sort_fields.clone(),
        k: 3,
    });
    assert_sorted_results(&executor, topk_plan, &expected_ids[..3])?;

    // k=0
    let topk_plan_0 = Arc::new(PhysicalPlan::TopKHeapSort {
        input: scan_plan.clone(),
        sort_fields: sort_fields.clone(),
        k: 0,
    });
    assert_sorted_results(&executor, topk_plan_0, &[])?;

    // k > items
    let topk_plan_10 = Arc::new(PhysicalPlan::TopKHeapSort {
        input: scan_plan,
        sort_fields: sort_fields.clone(),
        k: 10,
    });
    assert_sorted_results(&executor, topk_plan_10, &expected_ids)?;

    Ok(())
}

// Helper to run a filter plan and check the _id's of the results.
fn run_filter_test(
    executor: &QueryExecutor,
    collection_id: u32,
    filter_expr: Arc<Expr>,
    parameters: Parameters,
    expected_ids: &[i32],
) -> Result<()> {
    let scan_plan = full_scan_plan(collection_id);

    let filter_plan = Arc::new(PhysicalPlan::Filter {
        input: scan_plan,
        predicate: filter_expr,
    });

    let results = executor.execute_cached(filter_plan, &parameters)?;
    let mut found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
    found_docs.sort_by_key(|d| d.get_i32("_id").unwrap());

    let found_ids: Vec<i32> = found_docs
        .iter()
        .map(|d| d.get_i32("_id").unwrap())
        .collect();
    assert_eq!(
        expected_ids, found_ids,
        "Filter test failed. Expected: {:?}, Found: {:?}",
        expected_ids, found_ids
    );
    Ok(())
}

#[test]
fn test_filter_plan_execution() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_filters")?;

    // 2. Insert test data
    let docs = vec![
        doc! { "_id": 1, "name": "c", "value": 10.0, "tags": ["a", "b"], "items": [ doc!{ "k": "x", "v": 1 }, doc!{ "k": "y", "v": 2 } ] },
        doc! { "_id": 2, "name": "a", "value": 30.0, "tags": ["b", "c"], "nested": { "val": 5 } },
        doc! { "_id": 3, "name": "b", "value": 20.0, "tags": ["c", "d"] },
        doc! { "_id": 4, "name": "a", "value": 10.0, "tags": ["d", "a"] },
        doc! { "_id": 5, "name": "c", "value": 5.0 }, // no tags
        doc! { "_id": 6, "name": "d", "value": Bson::Null, "tags": [] },
    ];
    insert_docs(&executor, collection_id, docs.iter())?;

    // $eq
    let mut params = Parameters::new();
    let p = params.collect_parameter("a".into());
    let filter = field_filters(field(["name"]), [interval(point(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[2, 4])?;

    // $gt
    let mut params = Parameters::new();
    let p = params.collect_parameter(15.0.into());
    let filter = field_filters(field(["value"]), [interval(greater_than(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[2, 3])?;

    // $in
    let mut params = Parameters::new();
    let p = params.collect_parameter(vec!["a", "b"].into());
    let filter = field_filters(field(["name"]), [within(p)]);
    run_filter_test(&executor, collection_id, filter, params, &[2, 3, 4])?;

    // $exists: true
    let filter = field_filters(field(["nested"]), [exists(true)]);
    run_filter_test(&executor, collection_id, filter, Parameters::new(), &[2])?;

    // $exists: false
    let filter = field_filters(field(["nested"]), [exists(false)]);
    run_filter_test(
        &executor,
        collection_id,
        filter,
        Parameters::new(),
        &[1, 3, 4, 5, 6],
    )?;

    // $type: "double"
    let mut params = Parameters::new();
    let p = params.collect_parameter("double".into());
    let filter = field_filters(field(["value"]), [has_type(p, false)]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 2, 3, 4, 5])?;

    // $size: 2
    let mut params = Parameters::new();
    let p = params.collect_parameter(2.into());
    let filter = field_filters(field(["tags"]), [size(p, false)]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 2, 3, 4])?;

    // $all
    let mut params = Parameters::new();
    let p = params.collect_parameter(vec!["a", "d"].into());
    let filter = field_filters(field(["tags"]), [all(p)]);
    run_filter_test(&executor, collection_id, filter, params, &[4])?;

    // $elemMatch on documents
    let mut params = Parameters::new();
    let p_k = params.collect_parameter("y".into());
    let p_v = params.collect_parameter(2.into());
    let filter = field_filters(
        field(["items"]),
        [elem_match(vec![
            field_filters(field(["k"]), [interval(point(&p_k))]),
            field_filters(field(["v"]), [interval(point(&p_v))]),
        ])],
    );
    run_filter_test(&executor, collection_id, filter, params, &[1])?;

    // $and
    let mut params = Parameters::new();
    let p_name = params.collect_parameter("a".into());
    let p_val = params.collect_parameter(10.0.into());
    let f1 = field_filters(field(["name"]), [interval(point(&p_name))]);
    let f2 = field_filters(field(["value"]), [interval(point(&p_val))]);
    let filter = and(vec![f1, f2]);
    run_filter_test(&executor, collection_id, filter, params, &[4])?;

    // $or
    let mut params = Parameters::new();
    let p_name = params.collect_parameter("b".into());
    let p_val = params.collect_parameter(5.0.into());
    let f1 = field_filters(field(["name"]), [interval(point(&p_name))]);
    let f2 = field_filters(field(["value"]), [interval(point(&p_val))]);
    let filter = or(vec![f1, f2]);
    run_filter_test(&executor, collection_id, filter, params, &[3, 5])?;

    // $not (top-level)
    let mut params = Parameters::new();
    let p = params.collect_parameter("a".into());
    let inner_filter = field_filters(field(["name"]), [interval(point(&p))]);
    let filter = not(inner_filter);
    run_filter_test(&executor, collection_id, filter, params, &[1, 3, 5, 6])?;

    // $nor
    let mut params = Parameters::new();
    let p_name1 = params.collect_parameter("a".into());
    let p_name2 = params.collect_parameter("b".into());
    let f1 = field_filters(field(["name"]), [interval(point(&p_name1))]);
    let f2 = field_filters(field(["name"]), [interval(point(&p_name2))]);
    let filter = nor(vec![f1, f2]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 5, 6])?;

    // $lt
    let mut params = Parameters::new();
    let p = params.collect_parameter(15.0.into());
    let filter = field_filters(field(["value"]), [interval(less_than(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 4, 5, 6])?; // Also match Null

    // $lte
    let mut params = Parameters::new();
    let p = params.collect_parameter(10.0.into());
    let filter = field_filters(field(["value"]), [interval(at_most(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 4, 5, 6])?;

    // $gte
    let mut params = Parameters::new();
    let p = params.collect_parameter(20.0.into());
    let filter = field_filters(field(["value"]), [interval(at_least(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[2, 3])?;

    // $ne
    let mut params = Parameters::new();
    let p = params.collect_parameter("a".into());
    let filter = field_filters(field(["name"]), [ne(p)]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 3, 5, 6])?;

    // $nin
    let mut params = Parameters::new();
    let p = params.collect_parameter(vec!["a", "b"].into());
    let filter = field_filters(field(["name"]), [not(within(p))]);
    run_filter_test(&executor, collection_id, filter, params, &[1, 5, 6])?;

    // Nested logical operators: $or inside $and
    let mut params = Parameters::new();
    let p1 = params.collect_parameter("a".into());
    let p2 = params.collect_parameter("b".into());
    let p3 = params.collect_parameter(10.0.into());
    let f1 = or(vec![
        field_filters(field(["name"]), [interval(point(&p1))]),
        field_filters(field(["name"]), [interval(point(&p2))]),
    ]);
    let f2 = field_filters(field(["value"]), [interval(point(&p3))]);
    let filter = and(vec![f1, f2]);
    run_filter_test(&executor, collection_id, filter, params, &[4])?;

    // Complex/nested field path
    let mut params = Parameters::new();
    let p = params.collect_parameter(5.into());
    let filter = field_filters(field(["nested", "val"]), [interval(point(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[2])?;

    // Null check: field is null
    let mut params = Parameters::new();
    let p = params.collect_parameter(BsonValue(Bson::Null));
    let filter = field_filters(field(["value"]), [interval(point(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[6])?;

    // Null check: field does not exist (should not match null)
    let mut params = Parameters::new();
    let p = params.collect_parameter(BsonValue(Bson::Null));
    let filter = field_filters(field(["tags"]), [interval(point(&p))]);
    run_filter_test(&executor, collection_id, filter, params, &[5])?;

    Ok(())
}

#[test]
fn test_projection_plan_execution() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_projections")?;

    // 2. Insert test data
    let test_doc = doc! {
        "_id": 1,
        "name": "test_doc",
        "scalar": 123,
        "nested": { "a": 1, "b": "hello" },
        "array_scalar": [10, 20, 30, 40, 50],
        "array_doc": [
            doc!{ "val": 10, "tag": "a" },
            doc!{ "val": 20, "tag": "b" },
            doc!{ "val": 30, "tag": "a" },
            doc!{ "val": 40, "tag": "c" },
        ]
    };
    insert_one(&executor, collection_id, &test_doc)?;

    // Helper to run a projection and check the result
    fn run_projection_test(
        executor: &QueryExecutor,
        collection_id: u32,
        projection: Projection,
        parameters: Parameters,
        expected_doc: Document,
    ) -> Result<()> {
        let scan_plan = full_scan_plan(collection_id);

        let projection_plan = Arc::new(PhysicalPlan::Projection {
            input: scan_plan,
            projection: Arc::new(projection),
        });

        let results = executor.execute_cached(projection_plan, &parameters)?;
        let found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs.len(), 1);
        assert_eq!(
            found_docs[0], expected_doc,
            "\nProjection test failed.\nExpected: {:#?}\n   Found: {:#?}",
            expected_doc, found_docs[0]
        );
        Ok(())
    }

    // --- Test Cases ---

    // Case 1: Simple include
    let proj_expr_1 = proj_fields(vec![("name", proj_field()), ("scalar", proj_field())]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_1),
        Parameters::new(),
        doc! { "name": "test_doc", "scalar": 123 },
    )?;

    // Case 2: Nested include
    let proj_expr_2 = proj_fields(vec![
        ("name", proj_field()),
        ("nested", proj_fields(vec![("b", proj_field())])),
    ]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_2),
        Parameters::new(),
        doc! { "name": "test_doc", "nested": { "b": "hello" } },
    )?;

    // Case 3: Simple exclude
    let proj_expr_3 = proj_fields(vec![
        ("scalar", proj_field()),
        ("array_scalar", proj_field()),
        ("array_doc", proj_field()),
    ]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Exclude(proj_expr_3),
        Parameters::new(),
        doc! { "_id": 1, "name": "test_doc", "nested": { "a": 1, "b": "hello" } },
    )?;

    // Case 4: Slice projection { array_scalar: { $slice: [1, 2] } }
    let proj_expr_4 = proj_fields(vec![("array_scalar", proj_slice(Some(1), 2))]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_4),
        Parameters::new(),
        doc! { "array_scalar": [20, 30] },
    )?;

    // Case 5: ElemMatch projection
    let mut params_5 = Parameters::new();
    let p_5 = params_5.collect_parameter("a".into());
    let filter_5 = field_filters(field(["tag"]), [interval(point(&p_5))]);
    let proj_expr_5 = proj_fields(vec![("array_doc", proj_elem_match(filter_5))]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_5),
        params_5,
        doc! {
            "array_doc": [
                doc!{ "val": 10, "tag": "a" },
                doc!{ "val": 30, "tag": "a" },
            ]
        },
    )?;

    // Case 6: ArrayElements projection (non-standard, returns a document)
    let proj_expr_7 = proj_fields(vec![(
        "array_scalar",
        proj_array_elements(vec![(1, proj_field()), (3, proj_field())]),
    )]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_7),
        Parameters::new(),
        doc! { "array_scalar": doc! { "1": 20, "3": 40 } },
    )?;

    // Case 7: ElemMatch with no matches
    let mut params_8 = Parameters::new();
    let p_8 = params_8.collect_parameter("z".into()); // No element has tag "z"
    let filter_8 = field_filters(field(["tag"]), [interval(point(&p_8))]);
    let proj_expr_8 = proj_fields(vec![("array_doc", proj_elem_match(filter_8))]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_8),
        params_8,
        doc! { "array_doc": []}, // Expect empty array since no matches
    )?;

    // Case 8: Projection on a non-existent field
    let proj_expr_10 = proj_fields(vec![("non_existent", proj_field())]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_10),
        Parameters::new(),
        doc! {}, // Expect an empty document
    )?;

    // Case 9: Array projection on a non-array field
    let proj_expr_11 = proj_fields(vec![("scalar", proj_slice(None, 2))]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_11),
        Parameters::new(),
        doc! {}, // Field should be omitted
    )?;

    // Case 10: Slice with only negative limit (last N elements)
    let proj_expr_12 = proj_fields(vec![("array_scalar", proj_slice(None, -2))]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_12),
        Parameters::new(),
        doc! { "array_scalar": [40, 50] },
    )?;

    // Case 11: Slice with zero skip and negative limit
    let proj_expr_13 = proj_fields(vec![("array_scalar", proj_slice(Some(0), -2))]);
    run_projection_test(
        &executor,
        collection_id,
        Projection::Include(proj_expr_13),
        Parameters::new(),
        doc! { "array_scalar": [40, 50] },
    )?;

    Ok(())
}

#[test]
fn test_multipoint_search_execution() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_multipoint")?;

    // 2. Insert test data
    let docs = vec![
        doc! { "_id": 1, "name": "a", "value": 10 },
        doc! { "_id": 2, "name": "b", "value": 20 },
        doc! { "_id": 3, "name": "c", "value": 10 },
        doc! { "_id": 4, "name": "d", "value": 20 },
        doc! { "_id": 5, "name": "e", "value": 10 },
    ];
    insert_docs(&executor, collection_id, docs.iter())?;

    // --- Test Cases ---

    // Case 1: Forward direction, some keys don't exist
    let mut params1 = Parameters::new();
    let keys1 = params1.collect_parameter(BsonValue(Bson::Array(vec![
        Bson::Int32(5),
        Bson::Int32(1),
        Bson::Int32(99), // non-existent
        Bson::Int32(3),
    ])));
    let plan1 = Arc::new(PhysicalPlan::MultiPointSearch {
        collection: collection_id,
        keys: keys1,
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });

    let results1 = executor.execute_cached(plan1, &params1)?;
    let found_docs1: Vec<Document> = results1.collect::<Result<_>>()?;
    let found_ids1: Vec<i32> = found_docs1
        .iter()
        .map(|d| d.get_i32("_id").unwrap())
        .collect();
    assert_eq!(found_ids1, vec![1, 3, 5]);

    // Case 2: Reverse direction
    let mut params2 = Parameters::new();
    let keys2 = params2.collect_parameter(BsonValue(Bson::Array(vec![
        Bson::Int32(5),
        Bson::Int32(1),
        Bson::Int32(3),
    ])));
    let plan2 = Arc::new(PhysicalPlan::MultiPointSearch {
        collection: collection_id,
        keys: keys2,
        direction: Direction::Reverse,
        filter: None,
        projection: None,
    });

    let results2 = executor.execute_cached(plan2, &params2)?;
    let found_docs2: Vec<Document> = results2.collect::<Result<_>>()?;
    let found_ids2: Vec<i32> = found_docs2
        .iter()
        .map(|d| d.get_i32("_id").unwrap())
        .collect();
    assert_eq!(found_ids2, vec![5, 3, 1]);

    // Case 3: With residual filter
    let mut params3 = Parameters::new();
    let keys3 = params3.collect_parameter(BsonValue(Bson::Array(vec![
        Bson::Int32(1),
        Bson::Int32(2),
        Bson::Int32(3),
        Bson::Int32(4),
    ])));
    let p_val = params3.collect_parameter(10.into());
    let filter3 = field_filters(field(["value"]), [interval(point(&p_val))]);

    let plan3 = Arc::new(PhysicalPlan::MultiPointSearch {
        collection: collection_id,
        keys: keys3,
        direction: Direction::Forward,
        filter: Some(filter3),
        projection: None,
    });

    let results3 = executor.execute_cached(plan3, &params3)?;
    let found_docs3: Vec<Document> = results3.collect::<Result<_>>()?;
    let found_ids3: Vec<i32> = found_docs3
        .iter()
        .map(|d| d.get_i32("_id").unwrap())
        .collect();
    assert_eq!(found_ids3, vec![1, 3]);

    Ok(())
}

#[test]
fn test_execute_cached_at_snapshot() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_snapshot")?;

    // 2. Insert initial doc
    let initial_doc = doc! { "_id": 1_i32, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    // 3. Get snapshot
    let snapshot1 = storage_engine.last_visible_sequence();

    // 4. Update the doc directly in storage
    let key = BsonValue(Bson::Int32(1)).try_into_key()?;
    let update_op = Operation::new_put(
        collection_id,
        0,
        key.clone(),
        doc! { "_id": 1_i32, "value": "updated" }.to_vec()?,
    );
    storage_engine.write(write_batch(vec![update_op]), false)?;

    // 5. Query at snapshot
    let mut params = Parameters::new();
    let point_search_plan = point_search_query(collection_id, &mut params, 1_i32);

    let mut result_at_snapshot =
        executor.execute_cached_at_snapshot(point_search_plan.clone(), &params, Some(snapshot1))?;
    let doc_at_snapshot = result_at_snapshot.next().unwrap()?;
    assert!(result_at_snapshot.next().is_none());
    assert_eq!(doc_at_snapshot, initial_doc);

    // 6. Query at latest
    let mut result_latest = executor.execute_cached(point_search_plan, &params)?;
    let doc_latest = result_latest.next().unwrap()?;
    assert!(result_latest.next().is_none());
    assert_eq!(doc_latest.get_str("value")?, "updated");

    // 7. Test with scan and deletes
    let doc_to_delete = doc! { "_id": 2_i32, "value": "to_delete" };
    insert_one(&executor, collection_id, &doc_to_delete)?;

    let snapshot2 = storage_engine.last_visible_sequence();

    let key_to_delete = BsonValue(Bson::Int32(2)).try_into_key()?;
    let delete_op = Operation::new_delete(collection_id, 0, key_to_delete);
    storage_engine.write(write_batch(vec![delete_op]), false)?;

    let scan_plan = full_scan_plan(collection_id);

    // Scan at snapshot2 should see both documents (doc1 is updated, doc2 exists)
    let results_at_snapshot2 = executor.execute_cached_at_snapshot(
        scan_plan.clone(),
        &Parameters::new(),
        Some(snapshot2),
    )?;
    let mut docs_at_snapshot2: Vec<Document> = results_at_snapshot2.collect::<Result<_>>()?;
    assert_eq!(docs_at_snapshot2.len(), 2);
    docs_at_snapshot2.sort_by_key(|d| d.get_i32("_id").unwrap());

    assert_eq!(docs_at_snapshot2[0].get_i32("_id")?, 1);
    assert_eq!(docs_at_snapshot2[0].get_str("value")?, "updated");
    assert_eq!(docs_at_snapshot2[1].get_i32("_id")?, 2);
    assert_eq!(docs_at_snapshot2[1].get_str("value")?, "to_delete");

    // Scan at latest should see only one document
    let results_latest_scan = executor.execute_cached(scan_plan, &Parameters::new())?;
    let docs_latest_scan: Vec<Document> = results_latest_scan.collect::<Result<_>>()?;
    assert_eq!(docs_latest_scan.len(), 1);
    assert_eq!(docs_latest_scan[0].get_i32("_id")?, 1);
    assert_eq!(docs_latest_scan[0].get_str("value")?, "updated");

    Ok(())
}
