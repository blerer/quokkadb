use super::*;
use crate::error::{Error, Result};
use crate::query::execution::executor::test_utils::*;
use crate::query::execution::QueryExecutor;
use crate::query::physical_plan::{IndexScanRangeExpr, PhysicalPlan};
use crate::query::update_fn::*;
use crate::query::*;
use crate::storage::catalog::{IndexDefinition, IndexOptions, OrderedIndexField};
use crate::storage::count_stats::CountStatsKey;
use crate::storage::Direction;
use crate::util::bson_utils::BsonKey;
use bson::doc;
use bson::{Bson, Document};
use std::sync::Arc;

fn index_scan_eq(
    executor: &QueryExecutor,
    collection_id: u32,
    index_id: u32,
    value: impl Into<BsonValue>,
) -> Result<Vec<Document>> {
    let mut params = Parameters::new();
    let eq = params.collect_parameter(value.into());
    let plan = Arc::new(PhysicalPlan::IndexScan {
        collection: collection_id,
        index: index_id,
        range: IndexScanRangeExpr {
            equal_prefix: vec![eq],
            tail: None,
        },
        direction: Direction::Forward,
        filter: None,
        projection: None,
    });

    executor
        .execute_cached(plan, &params)?
        .collect::<Result<_>>()
}

#[test]
fn test_insert_duplicate_key_preflight_check() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_duplicates")?;

    // 2. Insert a document with a known ID
    let doc1 = doc! { "_id": 1_i32, "name": "doc1" };
    insert_one(&executor, collection_id, &doc1)?;

    // 3. Try to insert another document with the same ID
    let doc1_dup = doc! { "_id": 1_i32, "name": "doc1_dup" };
    let insert_dup_plan = PhysicalPlan::InsertOne {
        collection: collection_id,
        document: doc1_dup.to_vec()?,
    };
    let result = executor.execute_direct(insert_dup_plan, None);
    match result {
        Err(Error::InvalidRequest(msg)) => {
            assert!(msg.starts_with("Duplicate key error"));
            assert!(msg.contains("_id: 1"));
        }
        Err(err) => panic!("Expected InvalidRequest for duplicate key, got {:?}", err),
        Ok(_) => panic!("Expected error for duplicate key, got Ok"),
    }

    // 4. InsertMany with a duplicate within the batch
    let doc2 = doc! { "_id": 2_i32, "name": "doc2" };
    let doc2_dup = doc! { "_id": 2_i32, "name": "doc2_dup" };
    let insert_many_intra_batch_dup_plan = PhysicalPlan::InsertMany {
        collection: collection_id,
        documents: vec![doc2.to_vec()?, doc2_dup.to_vec()?],
    };
    let result_many_intra = executor.execute_direct(insert_many_intra_batch_dup_plan, None);
    match result_many_intra {
        Err(Error::InvalidRequest(msg)) => {
            assert!(msg.starts_with("Duplicate key error"));
            assert!(msg.contains("_id: 2"));
        }
        Err(err) => panic!("Expected InvalidRequest for duplicate key, got {:?}", err),
        Ok(_) => panic!("Expected error for duplicate key, got Ok"),
    }

    // 5. InsertMany with a duplicate that already exists in the collection
    let doc3 = doc! { "_id": 3_i32, "name": "doc3" };
    let insert_many_existing_dup_plan = PhysicalPlan::InsertMany {
        collection: collection_id,
        documents: vec![doc3.to_vec()?, doc1.to_vec()?], // doc1 has _id: 1
    };
    let result_many_existing = executor.execute_direct(insert_many_existing_dup_plan, None);
    match result_many_existing {
        Err(Error::InvalidRequest(msg)) => {
            assert!(msg.starts_with("Duplicate key error"));
            assert!(msg.contains("_id: 1"));
        }
        Err(err) => panic!("Expected InvalidRequest for duplicate key, got {:?}", err),
        Ok(_) => panic!("Expected error for duplicate key, got Ok"),
    }

    // 6. Verify that no partial insert happened from the failed InsertMany
    let mut params = Parameters::new();
    let point_search_plan = point_search_query(collection_id, &mut params, 3_i32);
    let mut search_result = executor.execute_cached(point_search_plan, &params)?;
    assert!(
        search_result.next().is_none(),
        "Document with _id: 3 should not have been inserted"
    );

    Ok(())
}

#[test]
fn test_delete_one_removes_index_entries() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_delete_one_indexes")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("value")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    insert_docs(
        &executor,
        collection_id,
        [
            &doc! { "_id": 1, "value": "alpha" },
            &doc! { "_id": 2, "value": "beta" },
        ],
    )?;

    let deleted = execute_delete_one(&executor, collection_id, 1)?;
    assert_delete_result(deleted, 1);

    assert!(index_scan_eq(&executor, collection_id, index_id, "alpha")?.is_empty());
    let remaining = index_scan_eq(&executor, collection_id, index_id, "beta")?;
    assert_eq!(remaining, vec![doc! { "_id": 2, "value": "beta" }]);

    Ok(())
}

#[test]
fn test_delete_many_removes_index_entries() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_delete_many_indexes")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("value")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    insert_docs(
        &executor,
        collection_id,
        [
            &doc! { "_id": 1, "value": "alpha" },
            &doc! { "_id": 2, "value": "beta" },
            &doc! { "_id": 3, "value": "gamma" },
        ],
    )?;

    let deleted = execute_delete_many(&executor, collection_id, full_scan_plan(collection_id))?;
    assert_delete_result(deleted, 3);

    assert!(index_scan_eq(&executor, collection_id, index_id, "alpha")?.is_empty());
    assert!(index_scan_eq(&executor, collection_id, index_id, "beta")?.is_empty());
    assert!(index_scan_eq(&executor, collection_id, index_id, "gamma")?.is_empty());

    Ok(())
}

#[test]
fn test_find_one_and_delete_removes_index_entries() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_delete_indexes")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("value")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    insert_docs(
        &executor,
        collection_id,
        [
            &doc! { "_id": 1, "value": "alpha" },
            &doc! { "_id": 2, "value": "beta" },
        ],
    )?;

    let deleted = execute_find_one_and_delete(&executor, collection_id, 1)?;
    assert_find_one_and_delete_result(deleted, Some(doc! { "_id": 1, "value": "alpha" }));

    assert!(index_scan_eq(&executor, collection_id, index_id, "alpha")?.is_empty());
    let remaining = index_scan_eq(&executor, collection_id, index_id, "beta")?;
    assert_eq!(remaining, vec![doc! { "_id": 2, "value": "beta" }]);

    Ok(())
}

#[test]
fn test_find_one_and_update_rewrites_index_entries() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update_indexes")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("value")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    insert_docs(
        &executor,
        collection_id,
        [
            &doc! { "_id": 1, "value": "alpha" },
            &doc! { "_id": 2, "value": "beta" },
        ],
    )?;

    let update_expr = update([set([field_name("value")], "updated")]);
    let updated = execute_find_one_and_update_with_expr_and_upsert(
        &executor,
        collection_id,
        1,
        update_expr,
        false,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_update_result(updated, Some(doc! { "_id": 1, "value": "alpha" }));

    assert!(index_scan_eq(&executor, collection_id, index_id, "alpha")?.is_empty());
    let updated_docs = index_scan_eq(&executor, collection_id, index_id, "updated")?;
    assert_eq!(updated_docs, vec![doc! { "_id": 1, "value": "updated" }]);
    let unchanged = index_scan_eq(&executor, collection_id, index_id, "beta")?;
    assert_eq!(unchanged, vec![doc! { "_id": 2, "value": "beta" }]);

    Ok(())
}

#[test]
fn test_replace_one_preserves_existing_id_when_replacement_omits_id() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_replace_one")?;

    insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial", "extra": true },
    )?;

    let result = execute_replace_one(
        &executor,
        collection_id,
        1,
        doc! { "value": "replacement" },
        false,
    )?;
    assert_update_result(result, 1, 1, Option::<Bson>::None);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "replacement" });

    Ok(())
}

#[test]
fn test_replace_one_rejects_changing_id() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_replace_one_id")?;

    insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;

    let result = execute_replace_one(
        &executor,
        collection_id,
        1,
        doc! { "_id": 2, "value": "replacement" },
        false,
    );

    match result {
        Err(Error::InvalidRequest(message)) => {
            assert_eq!(
                message,
                "The _id field cannot be changed in a replacement document"
            );
        }
        Err(err) => panic!("Expected InvalidRequest, got {:?}", err),
        Ok(result) => panic!("Expected error, got success {:?}", result),
    }

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "initial" });

    Ok(())
}

#[test]
fn test_find_one_and_replace_returns_previous_document_by_default() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_replace")?;

    insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;

    let result = execute_find_one_and_replace(
        &executor,
        collection_id,
        1,
        doc! { "value": "replacement" },
        false,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_replace_result(result, Some(doc! { "_id": 1, "value": "initial" }));

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "replacement" });

    Ok(())
}

#[test]
fn test_find_one_and_replace_returns_new_document_when_requested() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_replace")?;

    insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;

    let result = execute_find_one_and_replace(
        &executor,
        collection_id,
        1,
        doc! { "value": "replacement" },
        false,
        ReturnDocument::After,
    )?;
    assert_find_one_and_replace_result(result, Some(doc! { "_id": 1, "value": "replacement" }));

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "replacement" });

    Ok(())
}

#[test]
fn test_find_one_and_replace_returns_none_when_no_match() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_replace")?;

    insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;

    let result = execute_find_one_and_replace(
        &executor,
        collection_id,
        2,
        doc! { "value": "replacement" },
        false,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_replace_result(result, None);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "initial" });

    Ok(())
}

#[test]
fn test_find_one_and_replace_rewrites_index_entries() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_replace_indexes")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("value")]),
        IndexOptions::default(),
    )?;
    let index_id = index.id;

    insert_docs(
        &executor,
        collection_id,
        [
            &doc! { "_id": 1, "value": "alpha" },
            &doc! { "_id": 2, "value": "beta" },
        ],
    )?;

    let result = execute_find_one_and_replace(
        &executor,
        collection_id,
        1,
        doc! { "value": "updated" },
        false,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_replace_result(result, Some(doc! { "_id": 1, "value": "alpha" }));

    assert!(index_scan_eq(&executor, collection_id, index_id, "alpha")?.is_empty());
    let updated_docs = index_scan_eq(&executor, collection_id, index_id, "updated")?;
    assert_eq!(updated_docs, vec![doc! { "_id": 1, "value": "updated" }]);
    let unchanged = index_scan_eq(&executor, collection_id, index_id, "beta")?;
    assert_eq!(unchanged, vec![doc! { "_id": 2, "value": "beta" }]);

    Ok(())
}

#[test]
fn test_replace_one_retries_after_concurrent_delete() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_replace_one_retry")?;

    insert_one(
        executor.as_ref(),
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;

    let replace_handle = spawn_paused_replace_one(
        executor.clone(),
        hook.clone(),
        collection_id,
        1,
        doc! { "value": "replacement" },
        false,
    );

    hook.wait_until_hit();

    let delete_executor = executor.clone();
    let delete_doc = execute_delete_one(&delete_executor, collection_id, 1)?;
    assert_delete_result(delete_doc, 1);

    hook.release();

    let replace_result = replace_handle.join().unwrap()?;
    assert_update_result(replace_result, 0, 0, Option::<Bson>::None);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());

    Ok(())
}

#[test]
fn test_find_one_and_replace_retries_after_concurrent_update() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_replace_retry")?;

    insert_one(
        executor.as_ref(),
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;

    let replace_handle = spawn_paused_find_one_and_replace(
        executor.clone(),
        hook.clone(),
        collection_id,
        1,
        doc! { "value": "replacement" },
        false,
        ReturnDocument::Before,
    );

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_doc = execute_update_one(&concurrent_executor, collection_id, 1, "updated")?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    hook.release();

    let replace_result = replace_handle.join().unwrap()?;
    assert_find_one_and_replace_result(replace_result, Some(doc! { "_id": 1, "value": "updated" }));

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "replacement" });

    Ok(())
}

#[test]
fn test_update_one_succeeds_on_retry() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    // 2. Arrange to fail the next precondition check, simulating a conflict
    storage_engine.fail_next_precondition_checks(1);

    // 3. Act: prepare and execute UpdateOne
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);

    let update_expr = update([set([field_name("value")], "updated")]);

    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: false,
    };

    // The executor will attempt the update, fail, retry, and then succeed.
    let result = executor.execute_direct(update_plan, Some(params))?;
    assert_update_result(result, 1, 1, Option::<Bson>::None);

    // 4. Assert final state
    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc.get_str("value")?, "updated");
    assert_eq!(final_doc.get_i32("_id")?, 1);
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_update_one_fails_after_retry_timeout() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;
    // 2. Arrange to fail many times to ensure timeout is reached
    storage_engine.fail_next_precondition_checks(20);

    // 3. Act: prepare UpdateOne
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);

    let update_expr = update([set([field_name("value")], "updated")]);

    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: false,
    };

    let result = executor.execute_direct(update_plan, Some(params));

    // 4. Assert: Expect a VersionConflict error
    match result {
        Err(Error::VersionConflict { .. }) => {
            // This is the expected error after timeout
        }
        Err(e) => panic!("Expected a VersionConflict error, but got {:?}", e),
        Ok(_) => panic!("Expected an error, but the update succeeded"),
    }

    // 5. Assert that the document was not changed
    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc.get_str("value")?, "initial");

    Ok(())
}

#[test]
fn test_update_one_retries_after_concurrent_delete() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let update_handle =
        spawn_paused_update_one(executor.clone(), hook.clone(), collection_id, 1, "updated");

    hook.wait_until_hit();

    let delete_executor = executor.clone();
    let delete_doc = execute_delete_one(&delete_executor, collection_id, 1)?;
    assert_delete_result(delete_doc, 1);

    hook.release();

    let update_doc = update_handle.join().unwrap()?;
    assert_update_result(update_doc, 0, 0, Option::<Bson>::None);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_find_one_and_update_returns_previous_document_by_default() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    let update_expr = update([set([field_name("value")], "updated")]);
    let result = execute_find_one_and_update_with_expr_and_upsert(
        &executor,
        collection_id,
        1,
        update_expr,
        false,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_update_result(result, Some(doc! { "_id": 1, "value": "initial" }));

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "updated" });

    Ok(())
}

#[test]
fn test_find_one_and_update_returns_new_document_when_requested() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    let update_expr = update([set([field_name("value")], "updated")]);
    let result = execute_find_one_and_update_with_expr_and_upsert(
        &executor,
        collection_id,
        1,
        update_expr,
        false,
        ReturnDocument::After,
    )?;
    assert_find_one_and_update_result(result, Some(doc! { "_id": 1, "value": "updated" }));

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "updated" });

    Ok(())
}

#[test]
fn test_find_one_and_update_returns_none_when_no_match() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    let update_expr = update([set([field_name("value")], "updated")]);
    let result = execute_find_one_and_update_with_expr_and_upsert(
        &executor,
        collection_id,
        2,
        update_expr,
        false,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_update_result(result, None);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "initial" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_find_one_and_update_retries_after_concurrent_delete() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("value")], "paused")]);
    let update_handle = spawn_paused_find_one_and_update_with_expr_and_upsert(
        executor.clone(),
        hook.clone(),
        collection_id,
        1,
        paused_expr,
        false,
        ReturnDocument::Before,
    );

    hook.wait_until_hit();

    let delete_executor = executor.clone();
    let delete_doc = execute_delete_one(&delete_executor, collection_id, 1)?;
    assert_delete_result(delete_doc, 1);

    hook.release();

    let update_doc = update_handle.join().unwrap()?;
    assert_find_one_and_update_result(update_doc, None);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_find_one_and_update_retries_after_concurrent_update_same_field() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("value")], "paused")]);
    let update_handle = spawn_paused_find_one_and_update_with_expr_and_upsert(
        executor.clone(),
        hook.clone(),
        collection_id,
        1,
        paused_expr,
        false,
        ReturnDocument::Before,
    );

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_doc = execute_update_one(&concurrent_executor, collection_id, 1, "concurrent")?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "concurrent" });

    hook.release();

    let update_doc = update_handle.join().unwrap()?;
    assert_find_one_and_update_result(update_doc, Some(doc! { "_id": 1, "value": "concurrent" }));

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "paused" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_find_one_and_update_retries_after_concurrent_update_disjoint_fields() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("paused_field")], "paused")]);
    let update_handle = spawn_paused_find_one_and_update_with_expr_and_upsert(
        executor.clone(),
        hook.clone(),
        collection_id,
        1,
        paused_expr,
        false,
        ReturnDocument::After,
    );

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_expr = update([set([field_name("concurrent_field")], "concurrent")]);
    let concurrent_doc =
        execute_update_one_with_expr(&concurrent_executor, collection_id, 1, concurrent_expr)?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(
        mid_doc,
        doc! {
            "_id": 1,
            "value": "initial",
            "concurrent_field": "concurrent",
        }
    );

    hook.release();

    let update_doc = update_handle.join().unwrap()?;
    assert_find_one_and_update_result(
        update_doc,
        Some(doc! {
            "_id": 1,
            "value": "initial",
            "concurrent_field": "concurrent",
            "paused_field": "paused",
        }),
    );

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(
        final_doc,
        doc! {
            "_id": 1,
            "value": "initial",
            "concurrent_field": "concurrent",
            "paused_field": "paused",
        }
    );
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_find_one_and_delete_returns_deleted_document() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_delete")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    let result = execute_find_one_and_delete(&executor, collection_id, 1)?;
    assert_find_one_and_delete_result(result, Some(doc! { "_id": 1, "value": "initial" }));

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_find_one_and_delete_retries_after_concurrent_update() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_delete")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle =
        spawn_paused_find_one_and_delete(executor.clone(), hook.clone(), collection_id, 1);

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_doc = execute_update_one(&concurrent_executor, collection_id, 1, "updated")?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    hook.release();

    let deleted_doc = delete_handle.join().unwrap()?;
    assert_find_one_and_delete_result(deleted_doc, Some(doc! { "_id": 1, "value": "updated" }));

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_find_one_and_delete_retries_after_concurrent_delete() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_delete")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle =
        spawn_paused_find_one_and_delete(executor.clone(), hook.clone(), collection_id, 1);

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_delete = execute_delete_one(&concurrent_executor, collection_id, 1)?;
    assert_delete_result(concurrent_delete, 1);

    hook.release();

    let deleted_doc = delete_handle.join().unwrap()?;
    assert_find_one_and_delete_result(deleted_doc, None);
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_delete_one_deletes_matching_document() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_one")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);
    let delete_plan = PhysicalPlan::DeleteOne {
        collection: collection_id,
        query: query_plan,
    };

    let result = executor.execute_direct(delete_plan, Some(params))?;
    assert_delete_result(result, 1);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_delete_one_returns_zero_when_no_match() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_delete_one_no_match")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 2_i32);
    let delete_plan = PhysicalPlan::DeleteOne {
        collection: collection_id,
        query: query_plan,
    };

    let result = executor.execute_direct(delete_plan, Some(params))?;
    assert_delete_result(result, 0);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc.get_str("value")?, "initial");
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_delete_one_succeeds_on_retry() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_one_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    storage_engine.fail_next_precondition_checks(1);

    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);
    let delete_plan = PhysicalPlan::DeleteOne {
        collection: collection_id,
        query: query_plan,
    };

    let result = executor.execute_direct(delete_plan, Some(params))?;
    assert_delete_result(result, 1);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_delete_one_fails_after_retry_timeout() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_delete_one_retry_timeout")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;
    storage_engine.fail_next_precondition_checks(20);

    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);
    let delete_plan = PhysicalPlan::DeleteOne {
        collection: collection_id,
        query: query_plan,
    };

    let result = executor.execute_direct(delete_plan, Some(params));

    match result {
        Err(Error::VersionConflict { .. }) => {}
        Err(e) => panic!("Expected a VersionConflict error, but got {:?}", e),
        Ok(_) => panic!("Expected an error, but the delete succeeded"),
    }

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc.get_str("value")?, "initial");
    assert_eq!(final_doc.get_i32("_id")?, 1);
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_delete_many_deletes_matching_documents() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_many")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    let doc3 = doc! { "_id": 3, "value": "third" };
    insert_docs(&executor, collection_id, [&doc1, &doc2, &doc3])?;

    let delete_plan = full_scan_plan(collection_id);
    let result = execute_delete_many(&executor, collection_id, delete_plan)?;
    assert_delete_result(result, 3);

    for id in [1_i32, 2, 3] {
        let mut verify_params = Parameters::new();
        let verify_plan = point_search_query(collection_id, &mut verify_params, id);
        let mut results = executor.execute_cached(verify_plan, &verify_params)?;
        assert!(results.next().is_none());
    }
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_delete_many_returns_zero_when_no_match() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_delete_many_no_match")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    insert_docs(&executor, collection_id, [&doc1, &doc2])?;

    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 99_i32);
    let delete_plan = PhysicalPlan::DeleteMany {
        collection: collection_id,
        query: query_plan,
    };

    let result = executor.execute_direct(delete_plan, Some(params))?;
    assert_delete_result(result, 0);

    let final_doc1 = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc1, doc! { "_id": 1, "value": "first" });
    let final_doc2 = read_stored_doc(&storage_engine, collection_id, 2)?;
    assert_eq!(final_doc2, doc! { "_id": 2, "value": "second" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(2)
    );

    Ok(())
}

#[test]
fn test_delete_many_does_not_retry_on_conflict() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_many_retry")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    insert_docs(&executor, collection_id, [&doc1, &doc2])?;

    storage_engine.fail_next_precondition_checks(1);

    let delete_plan = full_scan_plan(collection_id);
    let result = execute_delete_many(&executor, collection_id, delete_plan);

    match result {
        Err(Error::VersionConflict { .. }) => {}
        Err(err) => panic!("Expected VersionConflict, got {:?}", err),
        Ok(doc) => panic!("Expected VersionConflict, got success {:?}", doc),
    }

    let final_doc1 = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc1, doc! { "_id": 1, "value": "first" });
    let final_doc2 = read_stored_doc(&storage_engine, collection_id, 2)?;
    assert_eq!(final_doc2, doc! { "_id": 2, "value": "second" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(2)
    );

    Ok(())
}

#[test]
fn test_delete_one_retries_after_concurrent_update() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_one")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), hook.clone(), collection_id, 1);

    hook.wait_until_hit();

    let update_executor = executor.clone();
    let update_doc = execute_update_one(&update_executor, collection_id, 1, "updated")?;
    assert_update_result(update_doc, 1, 1, Option::<Bson>::None);

    let updated_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(updated_doc, doc! { "_id": 1, "value": "updated" });

    hook.release();

    let delete_doc = delete_handle.join().unwrap()?;
    assert_delete_result(delete_doc, 1);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_delete_one_retries_after_concurrent_delete() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_one")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), hook.clone(), collection_id, 1);

    hook.wait_until_hit();

    let second_delete_executor = executor.clone();
    let second_delete_doc = execute_delete_one(&second_delete_executor, collection_id, 1)?;
    assert_delete_result(second_delete_doc, 1);

    hook.release();

    let delete_doc = delete_handle.join().unwrap()?;
    assert_delete_result(delete_doc, 0);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut results = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(results.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_update_one_retries_after_concurrent_update_same_field() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_update_handle =
        spawn_paused_update_one(executor.clone(), hook.clone(), collection_id, 1, "paused");

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_doc = execute_update_one(&concurrent_executor, collection_id, 1, "concurrent")?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "concurrent" });

    hook.release();

    let paused_doc = paused_update_handle.join().unwrap()?;
    assert_update_result(paused_doc, 1, 1, Option::<Bson>::None);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "paused" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_update_one_retries_after_concurrent_update_disjoint_fields() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("paused_field")], "paused")]);
    let paused_update_handle = spawn_paused_update_one_with_expr(
        executor.clone(),
        hook.clone(),
        collection_id,
        1,
        paused_expr,
    );

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_expr = update([set([field_name("concurrent_field")], "concurrent")]);
    let concurrent_doc =
        execute_update_one_with_expr(&concurrent_executor, collection_id, 1, concurrent_expr)?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(
        mid_doc,
        doc! {
            "_id": 1,
            "value": "initial",
            "concurrent_field": "concurrent",
        }
    );

    hook.release();

    let paused_doc = paused_update_handle.join().unwrap()?;
    assert_update_result(paused_doc, 1, 1, Option::<Bson>::None);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(
        final_doc,
        doc! {
            "_id": 1,
            "value": "initial",
            "concurrent_field": "concurrent",
            "paused_field": "paused",
        }
    );
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]

fn test_update_many_fails_after_concurrent_delete_without_partial_success() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateManyBeforeCommit));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_update_many")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    insert_docs(executor.as_ref(), collection_id, [&doc1, &doc2])?;

    let paused_query = full_scan_plan(collection_id);
    let paused_expr = update([set([field_name("status")], "updated")]);
    let paused_handle = spawn_paused_update_many(
        executor.clone(),
        hook.clone(),
        collection_id,
        paused_query,
        paused_expr,
        false,
    );

    hook.wait_until_hit();

    let delete_executor = executor.clone();
    let delete_doc = execute_delete_one(&delete_executor, collection_id, 1)?;
    assert_delete_result(delete_doc, 1);

    hook.release();

    match paused_handle.join().unwrap() {
        Err(Error::VersionConflict(_)) => {}
        Err(err) => panic!("Expected VersionConflict, got {:?}", err),
        Ok(doc) => panic!("Expected VersionConflict, got success {:?}", doc),
    }

    let mut missing_params = Parameters::new();
    let missing_plan = point_search_query(collection_id, &mut missing_params, 1_i32);
    let mut missing_results = executor.execute_cached(missing_plan, &missing_params)?;
    assert!(missing_results.next().is_none());

    let final_doc2 = read_stored_doc(&storage_engine, collection_id, 2)?;
    assert_eq!(final_doc2, doc! { "_id": 2, "value": "second" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_update_many_fails_after_concurrent_update_without_partial_success() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateManyBeforeCommit));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_update_many")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    insert_docs(executor.as_ref(), collection_id, [&doc1, &doc2])?;

    let paused_query = full_scan_plan(collection_id);
    let paused_expr = update([set([field_name("status")], "updated")]);
    let paused_handle = spawn_paused_update_many(
        executor.clone(),
        hook.clone(),
        collection_id,
        paused_query,
        paused_expr,
        false,
    );

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_doc = execute_update_one(&concurrent_executor, collection_id, 1, "changed")?;
    assert_update_result(concurrent_doc, 1, 1, Option::<Bson>::None);

    let mid_doc1 = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc1, doc! { "_id": 1, "value": "changed" });

    hook.release();

    match paused_handle.join().unwrap() {
        Err(Error::VersionConflict(_)) => {}
        Err(err) => panic!("Expected VersionConflict, got {:?}", err),
        Ok(doc) => panic!("Expected VersionConflict, got success {:?}", doc),
    }

    let final_doc1 = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc1, doc! { "_id": 1, "value": "changed" });

    let final_doc2 = read_stored_doc(&storage_engine, collection_id, 2)?;
    assert_eq!(final_doc2, doc! { "_id": 2, "value": "second" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(2)
    );

    Ok(())
}

#[test]
fn test_insert_one_manual_id_fails_after_concurrent_insert_same_key() -> Result<()> {
    let hook = Arc::new(PausingHook::new(
        ExecutorFailpoint::InsertManualAfterPreflightBeforeWrite,
    ));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert")?;

    let paused_handle = spawn_paused_insert_one(
        executor.clone(),
        hook.clone(),
        collection_id,
        doc! { "_id": 1, "value": "paused" },
    );

    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let concurrent_doc = insert_one(
        &concurrent_executor,
        collection_id,
        &doc! { "_id": 1, "value": "concurrent" },
    )?;
    assert_insert_one_result(concurrent_doc, 1);

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "concurrent" });

    hook.release();

    match paused_handle.join().unwrap() {
        Err(Error::InvalidRequest(message)) => {
            assert_eq!(message, "Duplicate key error. dup key: { _id: 1 }");
        }
        Err(err) => panic!("Expected duplicate key InvalidRequest, got {:?}", err),
        Ok(doc) => panic!("Expected duplicate key error, got success {:?}", doc),
    }

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "concurrent" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_insert_one_manual_id_succeeds_after_delete_same_key() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert")?;

    let inserted_doc = insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;
    assert_insert_one_result(inserted_doc, 1);

    let delete_doc = execute_delete_one(&executor, collection_id, 1)?;
    assert_delete_result(delete_doc, 1);
    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut verify_result = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(verify_result.next().is_none());

    let reinserted_doc = insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "replacement" },
    )?;
    assert_insert_one_result(reinserted_doc, 1);

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "replacement" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_insert_many_manual_id_succeeds_after_delete_same_key() -> Result<()> {
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert_many")?;

    let inserted_doc = insert_one(
        &executor,
        collection_id,
        &doc! { "_id": 1, "value": "initial" },
    )?;
    assert_insert_one_result(inserted_doc, 1);

    let delete_doc = execute_delete_one(&executor, collection_id, 1)?;
    assert_delete_result(delete_doc, 1);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut verify_result = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(verify_result.next().is_none());

    let docs = vec![
        doc! { "_id": 1, "value": "replacement" },
        doc! { "_id": 2, "value": "second" },
    ];
    let reinserted_doc = insert_many(&executor, collection_id, &docs)?;
    assert_eq!(
        reinserted_doc,
        WriteResult::InsertMany {
            inserted_ids: vec![Bson::Int32(1), Bson::Int32(2)],
        }
    );

    let final_doc1 = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc1, doc! { "_id": 1, "value": "replacement" });
    let final_doc2 = read_stored_doc(&storage_engine, collection_id, 2)?;
    assert_eq!(final_doc2, doc! { "_id": 2, "value": "second" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(2)
    );

    Ok(())
}

#[test]
fn test_insert_one_manual_id_fails_while_concurrent_delete_same_key_is_pending() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), hook.clone(), collection_id, 1);
    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    match insert_one(
        &concurrent_executor,
        collection_id,
        &doc! { "_id": 1, "value": "replacement" },
    ) {
        Err(Error::InvalidRequest(message)) => {
            assert_eq!(message, "Duplicate key error. dup key: { _id: 1 }");
        }
        Err(err) => panic!("Expected duplicate key InvalidRequest, got {:?}", err),
        Ok(doc) => panic!("Expected duplicate key error, got success {:?}", doc),
    }

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "initial" });

    hook.release();

    let delete_doc = delete_handle.join().unwrap()?;
    assert_delete_result(delete_doc, 1);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut verify_result = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(verify_result.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]
fn test_insert_many_manual_id_fails_while_concurrent_delete_same_key_is_pending() -> Result<()> {
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert_many")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), hook.clone(), collection_id, 1);
    hook.wait_until_hit();

    let concurrent_executor = executor.clone();
    let docs = vec![
        doc! { "_id": 1, "value": "replacement" },
        doc! { "_id": 2, "value": "second" },
    ];
    match insert_many(&concurrent_executor, collection_id, &docs) {
        Err(Error::InvalidRequest(message)) => {
            assert_eq!(message, "Duplicate key error. dup key: { _id: 1 }");
        }
        Err(err) => panic!("Expected duplicate key InvalidRequest, got {:?}", err),
        Ok(doc) => panic!("Expected duplicate key error, got success {:?}", doc),
    }

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "initial" });
    let user_key_2 = BsonValue::from(2_i32).try_into_key()?;
    assert!(storage_engine
        .read(collection_id, 0, &user_key_2, None)?
        .is_none());

    hook.release();

    let delete_doc = delete_handle.join().unwrap()?;
    assert_delete_result(delete_doc, 1);

    let mut verify_params = Parameters::new();
    let verify_plan = point_search_query(collection_id, &mut verify_params, 1_i32);
    let mut verify_result = executor.execute_cached(verify_plan, &verify_params)?;
    assert!(verify_result.next().is_none());
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        None
    );

    Ok(())
}

#[test]

fn test_update_many_does_not_retry_on_conflict() -> Result<()> {
    // 1. Setup
    let runtime = executor_test_runtime()?;
    let storage_engine = runtime.storage_engine.clone();
    let executor = runtime.executor.clone();
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let doc1 = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &doc1)?;

    // 2. Arrange to fail the next precondition check
    storage_engine.fail_next_precondition_checks(1);

    // 3. Act: prepare and execute UpdateMany
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);

    let update_expr = update([set([field_name("value")], "updated")]);

    let update_plan = PhysicalPlan::UpdateMany {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: false,
    };

    let result = executor.execute_direct(update_plan, Some(params));

    // 4. Assert: Expect an immediate VersionConflict error
    match result {
        Err(Error::VersionConflict { .. }) => {
            // Correct: the operation failed without retrying.
        }
        Err(e) => panic!("Expected a VersionConflict error, but got {:?}", e),
        Ok(_) => panic!("Expected an error, but the update succeeded"),
    }

    // 5. Assert that the document was not changed
    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc.get_str("value")?, "initial");

    Ok(())
}

#[test]
fn count_written_documents_handles_single_document_edge_cases() {
    assert_eq!(
        count_written_documents(&WriteResult::SingleDocument {
            affected_count: 1,
            document: None,
        },),
        1
    );
    assert_eq!(
        count_written_documents(&WriteResult::SingleDocument {
            affected_count: 1,
            document: Some(doc! { "_id": 1 }),
        },),
        1
    );
    assert_eq!(
        count_written_documents(&WriteResult::SingleDocument {
            affected_count: 0,
            document: None,
        },),
        0
    );
    assert_eq!(
        count_written_documents(&WriteResult::SingleDocument {
            affected_count: 1,
            document: Some(doc! { "_id": 1 }),
        },),
        1
    );
    assert_eq!(
        count_written_documents(&WriteResult::SingleDocument {
            affected_count: 0,
            document: None,
        },),
        0
    );
}
