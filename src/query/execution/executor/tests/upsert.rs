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
    assert_update_result(concurrent_doc, 0, 0, Some(1));

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "concurrent" });

    hook.release();

    let paused_doc = paused_handle.join().unwrap()?;
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
    assert_insert_one_result(insert_doc, 1);

    let mid_doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(mid_doc, doc! { "_id": 1, "value": "inserted" });

    hook.release();

    let paused_doc = paused_handle.join().unwrap()?;
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
    assert_update_result(result, 0, 0, Some(1));

    // 3. Verify the document was inserted
    let doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(doc.get_i32("_id")?, 1);
    assert_eq!(doc.get_str("value")?, "created");

    Ok(())
}

#[test]
fn test_find_one_and_update_upsert_before_returns_none() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    let update_expr = update([set([field_name("value")], "created")]);
    let result = execute_find_one_and_update_with_expr_and_upsert(
        &executor,
        collection_id,
        1,
        update_expr,
        true,
        ReturnDocument::Before,
    )?;
    assert_find_one_and_update_result(result, None);

    let doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(doc, doc! { "_id": 1, "value": "created" });

    Ok(())
}

#[test]
fn test_find_one_and_update_upsert_after_returns_inserted_document() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert")?;

    let update_expr = update([set([field_name("value")], "created")]);
    let result = execute_find_one_and_update_with_expr_and_upsert(
        &executor,
        collection_id,
        1,
        update_expr,
        true,
        ReturnDocument::After,
    )?;
    assert_find_one_and_update_result(result, Some(doc! { "_id": 1, "value": "created" }));

    let doc = read_stored_doc(&storage_engine, collection_id, 1)?;
    assert_eq!(doc, doc! { "_id": 1, "value": "created" });

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
    assert_update_result(result, 1, 1, Option::<Bson>::None);

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
    match result {
        WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id: Some(_),
        } => {
            assert_eq!(matched_count, 0);
            assert_eq!(modified_count, 0);
        }
        other => panic!("expected Update write result with upserted_id, got {other:?}"),
    }

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
    assert_update_result(result, 0, 0, Some(42));

    // 3. Verify the nested structure was created
    let doc = read_stored_doc(&storage_engine, collection_id, 42)?;
    assert_eq!(doc.get_i32("_id")?, 42);
    assert_eq!(doc.get_str("extra")?, "added");
    let data = doc.get_document("data")?;
    assert_eq!(data.get_str("inner")?, "nested_val");

    Ok(())
}

#[test]
fn test_update_one_upsert_extracts_residual_filter_through_index_scan() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert_index_scan")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("name")]),
        IndexOptions::default(),
    )?;

    let mut params = Parameters::new();
    let name_eq = params.collect_parameter(BsonValue(Bson::String("target".to_string())));
    let category_eq = params.collect_parameter(BsonValue(Bson::String("books".to_string())));

    let query_plan = Arc::new(PhysicalPlan::IndexScan {
        collection: collection_id,
        index: index.id,
        range: IndexScanRangeExpr {
            equal_prefix: vec![name_eq],
            tail: None,
        },
        direction: Direction::Forward,
        filter: Some(field_filters(
            field(["category"]),
            [interval(point(&category_eq))],
        )),
        projection: None,
    });

    let update_expr = update([set([field_name("status")], "processed")]);
    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: true,
    };

    let result = executor.execute_direct(update_plan, Some(params))?;
    match result {
        WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id: Some(_),
        } => {
            assert_eq!(matched_count, 0);
            assert_eq!(modified_count, 0);
        }
        other => panic!("expected Update write result with upserted_id, got {other:?}"),
    }

    let docs: Vec<Document> = executor
        .execute_cached(full_scan_plan(collection_id), &Parameters::new())?
        .collect::<Result<_>>()?;
    assert_eq!(docs.len(), 1);
    let inserted_id = docs[0].get("_id").unwrap().clone();
    let doc = read_stored_doc(&storage_engine, collection_id, inserted_id.clone())?;
    assert_eq!(doc, docs[0]);
    assert_eq!(
        doc,
        doc! { "_id": inserted_id, "category": "books", "status": "processed" }
    );

    Ok(())
}

#[test]
fn test_update_one_upsert_extracts_query_fields_through_topk_wrapper() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_upsert_topk")?;

    let mut params = Parameters::new();
    let category_eq = params.collect_parameter(BsonValue(Bson::String("books".to_string())));

    let query_plan = Arc::new(PhysicalPlan::TopKHeapSort {
        input: Arc::new(PhysicalPlan::Filter {
            input: full_scan_plan(collection_id),
            predicate: field_filters(field(["category"]), [interval(point(&category_eq))]),
        }),
        sort_fields: Arc::new(vec![make_sort_field(
            vec!["category".into()],
            SortOrder::Ascending,
        )]),
        k: 1,
    });

    let update_expr = update([set([field_name("status")], "processed")]);
    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: true,
    };

    let result = executor.execute_direct(update_plan, Some(params))?;
    match result {
        WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id: Some(_),
        } => {
            assert_eq!(matched_count, 0);
            assert_eq!(modified_count, 0);
        }
        other => panic!("expected Update write result with upserted_id, got {other:?}"),
    }

    let docs: Vec<Document> = executor
        .execute_cached(full_scan_plan(collection_id), &Parameters::new())?
        .collect::<Result<_>>()?;
    assert_eq!(docs.len(), 1);
    let inserted_id = docs[0].get("_id").unwrap().clone();
    let doc = read_stored_doc(&storage_engine, collection_id, inserted_id.clone())?;
    assert_eq!(doc, docs[0]);
    assert_eq!(
        doc,
        doc! { "_id": inserted_id, "category": "books", "status": "processed" }
    );

    Ok(())
}

#[test]
fn test_replace_one_upsert_extracts_id_through_topk_wrapper() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id = storage_engine.create_collection_if_not_exists("test_replace_topk")?;

    let mut params = Parameters::new();
    let query_plan = Arc::new(PhysicalPlan::TopKHeapSort {
        input: point_search_query(collection_id, &mut params, 42_i32),
        sort_fields: Arc::new(vec![make_sort_field(
            vec!["_id".into()],
            SortOrder::Ascending,
        )]),
        k: 1,
    });

    let replace_plan = PhysicalPlan::ReplaceOne {
        collection: collection_id,
        query: query_plan,
        replacement: doc! { "value": "created" },
        upsert: true,
    };

    let result = executor.execute_direct(replace_plan, Some(params))?;
    assert_update_result(result, 0, 0, Some(42));

    let doc = read_stored_doc(&storage_engine, collection_id, 42)?;
    assert_eq!(doc, doc! { "_id": 42, "value": "created" });

    Ok(())
}

#[test]
fn test_replace_one_upsert_extracts_id_from_index_scan_filter() -> Result<()> {
    let (storage_engine, _dir) = storage_engine()?;
    let executor = QueryExecutor::new(storage_engine.clone());
    let collection_id =
        storage_engine.create_collection_if_not_exists("test_replace_index_scan")?;
    let index = storage_engine.create_index(
        collection_id,
        IndexDefinition::Regular(vec![OrderedIndexField::asc("name")]),
        IndexOptions::default(),
    )?;

    let mut params = Parameters::new();
    let name_eq = params.collect_parameter(BsonValue(Bson::String("target".to_string())));
    let id_eq = params.collect_parameter(BsonValue(Bson::Int32(7)));

    let query_plan = Arc::new(PhysicalPlan::IndexScan {
        collection: collection_id,
        index: index.id,
        range: IndexScanRangeExpr {
            equal_prefix: vec![name_eq],
            tail: None,
        },
        direction: Direction::Forward,
        filter: Some(field_filters(field(["_id"]), [interval(point(&id_eq))])),
        projection: None,
    });

    let replace_plan = PhysicalPlan::ReplaceOne {
        collection: collection_id,
        query: query_plan,
        replacement: doc! { "value": "created" },
        upsert: true,
    };

    let result = executor.execute_direct(replace_plan, Some(params))?;
    assert_update_result(result, 0, 0, Some(7));

    let doc = read_stored_doc(&storage_engine, collection_id, 7)?;
    assert_eq!(doc, doc! { "_id": 7, "value": "created" });

    Ok(())
}
