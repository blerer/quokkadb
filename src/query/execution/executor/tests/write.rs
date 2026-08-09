use super::*;
use crate::util::bson_utils::BsonKey;
use bson::doc;

#[test]
fn test_insert_duplicate_key_preflight_check() -> Result<()> {
    // 1. Setup
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
fn test_update_one_succeeds_on_retry() -> Result<()> {
    // 1. Setup
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let update_handle = spawn_paused_update_one(executor.clone(), collection_id, 1, "updated");

    hook.wait_until_hit();

    let delete_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("value")], "paused")]);
    let update_handle = spawn_paused_find_one_and_update_with_expr_and_upsert(
        executor.clone(),
        collection_id,
        1,
        paused_expr,
        false,
        ReturnDocument::Before,
    );

    hook.wait_until_hit();

    let delete_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("value")], "paused")]);
    let update_handle = spawn_paused_find_one_and_update_with_expr_and_upsert(
        executor.clone(),
        collection_id,
        1,
        paused_expr,
        false,
        ReturnDocument::Before,
    );

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_find_one_and_update")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("paused_field")], "paused")]);
    let update_handle = spawn_paused_find_one_and_update_with_expr_and_upsert(
        executor.clone(),
        collection_id,
        1,
        paused_expr,
        false,
        ReturnDocument::After,
    );

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
fn test_delete_one_deletes_matching_document() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
fn test_delete_one_retries_after_concurrent_update() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_one")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), collection_id, 1);

    hook.wait_until_hit();

    let update_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_delete_one")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), collection_id, 1);

    hook.wait_until_hit();

    let second_delete_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_update_handle =
        spawn_paused_update_one(executor.clone(), collection_id, 1, "paused");

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let paused_expr = update([set([field_name("paused_field")], "paused")]);
    let paused_update_handle =
        spawn_paused_update_one_with_expr(executor.clone(), collection_id, 1, paused_expr);

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateManyBeforeCommit));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_update_many")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    insert_docs(executor.as_ref(), collection_id, [&doc1, &doc2])?;

    let paused_query = full_scan_plan(collection_id);
    let paused_expr = update([set([field_name("status")], "updated")]);
    let paused_handle = spawn_paused_update_many(
        executor.clone(),
        collection_id,
        paused_query,
        paused_expr,
        false,
    );

    hook.wait_until_hit();

    let delete_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::UpdateManyBeforeCommit));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_update_many")?;

    let doc1 = doc! { "_id": 1, "value": "first" };
    let doc2 = doc! { "_id": 2, "value": "second" };
    insert_docs(executor.as_ref(), collection_id, [&doc1, &doc2])?;

    let paused_query = full_scan_plan(collection_id);
    let paused_expr = update([set([field_name("status")], "updated")]);
    let paused_handle = spawn_paused_update_many(
        executor.clone(),
        collection_id,
        paused_query,
        paused_expr,
        false,
    );

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(
        ExecutorFailpoint::InsertManualAfterPreflightBeforeWrite,
    ));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert")?;

    let paused_handle = spawn_paused_insert_one(
        executor.clone(),
        collection_id,
        doc! { "_id": 1, "value": "paused" },
    );

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), collection_id, 1);
    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(ExecutorFailpoint::DeleteOneAfterRead));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_insert_many")?;

    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(executor.as_ref(), collection_id, &initial_doc)?;

    let delete_handle = spawn_paused_delete_one(executor.clone(), collection_id, 1);
    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
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
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
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
