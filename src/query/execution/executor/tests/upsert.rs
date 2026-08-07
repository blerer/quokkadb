use super::*;
use bson::doc;

#[test]
fn test_update_one_upsert_retries_after_concurrent_upsert_same_key() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(
        ExecutorFailpoint::UpdateOneUpsertAfterNoMatch,
    ));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    let paused_expr = update([set([field_name("value")], "paused")]);
    let paused_handle = spawn_paused_update_one_with_expr_and_upsert(
        executor.clone(),
        collection_id,
        1,
        paused_expr,
        true,
    );

    hook.wait_until_hit();

    let concurrent_executor = QueryExecutor::new(storage_engine.clone());
    let concurrent_expr = update([set([field_name("value")], "concurrent")]);
    let concurrent_doc = execute_update_one_with_expr_and_upsert(
        &concurrent_executor,
        collection_id,
        1,
        concurrent_expr,
        true,
    )?;
    assert_eq!(
        concurrent_doc,
        doc! { "matched_count": 0, "modified_count": 0, "upserted_id": 1 }
    );

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "concurrent" });

    hook.release();

    let paused_doc = paused_handle.join().unwrap()?;
    assert_eq!(paused_doc, doc! { "matched_count": 1, "modified_count": 1 });

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "paused" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_update_one_upsert_retries_after_concurrent_insert_same_key() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let hook = Arc::new(PausingHook::new(
        ExecutorFailpoint::UpdateOneUpsertAfterNoMatch,
    ));
    let executor = Arc::new(QueryExecutor::with_test_hook(
        storage_engine.clone(),
        hook.clone(),
    ));
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    let paused_expr = update([set([field_name("value")], "paused")]);
    let paused_handle = spawn_paused_update_one_with_expr_and_upsert(
        executor.clone(),
        collection_id,
        1,
        paused_expr,
        true,
    );

    hook.wait_until_hit();

    let insert_executor = QueryExecutor::new(storage_engine.clone());
    let insert_doc = insert_one(
        &insert_executor,
        collection_id,
        &doc! { "_id": 1, "value": "inserted" },
    )?;
    assert_eq!(insert_doc, doc! { "inserted_id": 1 });

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "inserted" });

    hook.release();

    let paused_doc = paused_handle.join().unwrap()?;
    assert_eq!(paused_doc, doc! { "matched_count": 1, "modified_count": 1 });

    let final_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(final_doc, doc! { "_id": 1, "value": "paused" });
    assert_eq!(
        storage_engine.count_stat(&CountStatsKey::Collection(collection_id)),
        Some(1)
    );

    Ok(())
}

#[test]
fn test_update_one_upsert_inserts_when_no_match() -> Result<()> {
    // 1. Setup
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    // 2. Execute UpdateOne with upsert=true on empty collection
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);

    let update_expr = update([set([field_name("value")], "created")]);

    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: true,
    };

    let result = executor.execute_direct(update_plan, Some(params))?;
    let result_doc = result.into_iter().next().unwrap()?;

    assert_eq!(result_doc.get_i32("matched_count")?, 0);
    assert_eq!(result_doc.get_i32("modified_count")?, 0);
    assert_eq!(result_doc.get_i32("upserted_id")?, 1);

    // 3. Verify the document was inserted
    let doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(doc.get_i32("_id")?, 1);
    assert_eq!(doc.get_str("value")?, "created");

    Ok(())
}

#[test]
fn test_update_one_upsert_updates_when_match_exists() -> Result<()> {
    // 1. Setup
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    // Insert initial doc
    let initial_doc = doc! { "_id": 1, "value": "initial" };
    insert_one(&executor, collection_id, &initial_doc)?;

    // 2. Execute UpdateOne with upsert=true
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, 1_i32);

    let update_expr = update([set([field_name("value")], "updated")]);

    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: true,
    };

    let result = executor.execute_direct(update_plan, Some(params))?;
    let result_doc = result.into_iter().next().unwrap()?;

    assert_eq!(result_doc.get_i32("matched_count")?, 1);
    assert_eq!(result_doc.get_i32("modified_count")?, 1);
    assert!(result_doc.get("upserted_id").is_none());

    // 3. Verify the document was updated
    let doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(doc.get_str("value")?, "updated");

    Ok(())
}

#[test]
fn test_update_many_upsert_inserts_when_no_match() -> Result<()> {
    // 1. Setup
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    // 2. Execute UpdateMany with upsert=true on empty collection
    let mut params = Parameters::new();
    let p_val = params.collect_parameter(BsonValue(Bson::String("target".to_string())));

    let query_plan = Arc::new(PhysicalPlan::Filter {
        input: full_scan_plan(collection_id),
        predicate: field_filters(field(["name"]), [interval(point(&p_val))]),
    });

    let update_expr = update([set([field_name("status")], "processed")]);

    let update_plan = PhysicalPlan::UpdateMany {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: true,
    };

    let result = executor.execute_direct(update_plan, Some(params))?;
    let result_doc = result.into_iter().next().unwrap()?;

    assert_eq!(result_doc.get_i32("matched_count")?, 0);
    assert_eq!(result_doc.get_i32("modified_count")?, 0);
    assert!(result_doc.get("upserted_id").is_some());

    // 3. Verify document was created with equality condition from query
    let scan_plan = full_scan_plan(collection_id);
    let results = executor.execute_cached(scan_plan, &Parameters::new())?;
    let docs: Vec<Document> = results.collect::<Result<_>>()?;
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get_str("name")?, "target");
    assert_eq!(docs[0].get_str("status")?, "processed");

    Ok(())
}

#[test]
fn test_upsert_with_nested_equality_conditions() -> Result<()> {
    // 1. Setup
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    // 2. Execute UpdateOne with upsert=true with nested field equality
    let mut params = Parameters::new();
    let key_expr = params.collect_parameter(BsonValue(Bson::Int32(42)));
    let p_nested = params.collect_parameter(BsonValue(Bson::String("nested_val".to_string())));

    let filter = field_filters(field(["data", "inner"]), [interval(point(&p_nested))]);

    let query_plan = Arc::new(PhysicalPlan::PointSearch {
        collection: collection_id,
        key: key_expr,
        filter: Some(filter),
        projection: None,
    });

    let update_expr = update([set([field_name("extra")], "added")]);

    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: true,
    };

    let result = executor.execute_direct(update_plan, Some(params))?;
    let result_doc = result.into_iter().next().unwrap()?;

    assert_eq!(result_doc.get_i32("upserted_id")?, 42);

    // 3. Verify the nested structure was created
    let doc = read_stored_doc(&storage_engine, collection_id, 42)?;
    assert_eq!(doc.get_i32("_id")?, 42);
    assert_eq!(doc.get_str("extra")?, "added");
    let data = doc.get_document("data")?;
    assert_eq!(data.get_str("inner")?, "nested_val");

    Ok(())
}
