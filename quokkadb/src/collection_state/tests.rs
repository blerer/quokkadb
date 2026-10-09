use super::{CollectionState, Hint, PreparedQuery};
use crate::QuokkaDB;
use crate::error::Error;
use crate::query::ReturnDocument;
use crate::query::execution::WriteResult;
use crate::query::expr_fn::{eq, field, field_filters, lit, within};
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::update_fn::{field_name, set, update};
use crate::query::{IndexKeyField, IndexKeySpec};
use bson::{Document, doc};
use tempfile::TempDir;

fn setup_collection() -> (TempDir, QuokkaDB, CollectionState) {
    let directory = tempfile::tempdir().unwrap();
    let db = QuokkaDB::open(directory.path()).unwrap();
    db.create_collection("items").unwrap();
    db.collection("items")
        .insert_many([
            doc! { "_id": 1, "status": "ready", "extra": "old" },
            doc! { "_id": 2, "status": "waiting", "extra": "other" },
        ])
        .unwrap();

    let state = CollectionState::new(db.db_impl.clone(), "items".to_string());
    (directory, db, state)
}

fn prepare_scan(state: &CollectionState, hint: Option<&Hint>) -> PreparedQuery {
    state
        .prepare_query(hint, |collection, hint| {
            Ok(LogicalPlanBuilder::scan(collection, hint).build_arc())
        })
        .unwrap()
        .unwrap()
}

fn prepare_primary_key_query(
    state: &CollectionState,
    value: impl Into<bson::Bson>,
) -> PreparedQuery {
    let condition = field_filters(field(["_id"]), [eq(lit(value.into()))]);
    state
        .prepare_query(None, |collection, hint| {
            Ok(LogicalPlanBuilder::scan(collection, hint)
                .filter(condition)
                .build_arc())
        })
        .unwrap()
        .unwrap()
}

fn prepare_primary_key_multi_query(state: &CollectionState) -> PreparedQuery {
    let condition = field_filters(
        field(["_id"]),
        [within(lit(bson::Bson::Array(vec![
            bson::Bson::Int32(1),
            bson::Bson::Int32(2),
        ])))],
    );
    state
        .prepare_query(None, |collection, hint| {
            Ok(LogicalPlanBuilder::scan(collection, hint)
                .filter(condition)
                .build_arc())
        })
        .unwrap()
        .unwrap()
}

fn collect_ids(documents: crate::collection::QueryOutput) -> Vec<i32> {
    let mut ids: Vec<_> = documents
        .map(|result| result.unwrap().get_i32("_id").unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

fn collect_documents(documents: crate::collection::QueryOutput) -> Vec<Document> {
    let mut documents: Vec<_> = documents.map(|result| result.unwrap()).collect();
    documents.sort_by_key(|document| document.get_i32("_id").unwrap());
    documents
}

enum WriteEntryPoint {
    ExecuteWrite,
    ExecuteDelete,
}

fn assert_collection_drop_race_returns_not_found(
    entry_point: WriteEntryPoint,
    build_plan: impl FnOnce(u32, Option<u32>) -> LogicalPlan,
) {
    let (_directory, _db, state) = setup_collection();
    let result = match entry_point {
        WriteEntryPoint::ExecuteWrite => state.execute_write(
            None,
            |collection, hint| {
                state.drop_collection().unwrap();
                Ok(build_plan(collection, hint))
            },
            false,
        ),
        WriteEntryPoint::ExecuteDelete => state.execute_delete(
            None,
            |collection, hint| {
                state.drop_collection().unwrap();
                Ok(build_plan(collection, hint))
            },
            false,
            false,
        ),
    };

    assert!(matches!(
        result,
        Err(Error::CollectionNotFound { id: Some(_), .. })
    ));
}

#[derive(Clone, Copy)]
enum IndexChange {
    DropHinted,
    Create,
}

fn indexed_status_scan(collection: u32, hint: Option<u32>) -> LogicalPlanBuilder {
    LogicalPlanBuilder::scan(collection, hint)
        .filter(field_filters(field(["status"]), [eq(lit("ready"))]))
}

fn run_index_change_race(
    entry_point: WriteEntryPoint,
    change: IndexChange,
    expected_new_index_match: Option<(&'static str, Vec<i32>)>,
    additional_matching_document: bool,
    build_plan: impl FnOnce(u32, Option<u32>) -> LogicalPlan,
) -> Result<WriteResult, Error> {
    let (_directory, db, state) = setup_collection();
    if additional_matching_document {
        db.collection("items")
            .insert_one(doc! { "_id": 3, "status": "ready", "extra": "old" })
            .unwrap();
    }
    let index_name = state.create_index(
        IndexKeySpec::new(vec![IndexKeyField::asc(vec!["status".into()])]),
        Default::default(),
    )?;
    let hint = Hint::Index(index_name.clone());
    let (planned, _) = state
        .plan_query(Some(&hint), |collection, hint| {
            Ok(indexed_status_scan(collection, hint).build_arc())
        })?
        .unwrap();
    let index_id = state
        .db
        .get_collection("items")
        .unwrap()
        .get_index_by_name(&index_name)
        .unwrap()
        .id;
    assert!(matches!(
        planned.as_ref(),
        PhysicalPlan::IndexScan { index, .. } if *index == index_id
    ));

    let mut created_index_name = None;
    let mut build_plan = Some(build_plan);
    let mut change_index_and_build_plan = |collection, hint| {
        assert_eq!(hint, Some(index_id));
        match change {
            IndexChange::DropHinted => state.drop_index(&index_name)?,
            IndexChange::Create => {
                created_index_name = Some(state.create_index(
                    IndexKeySpec::new(vec![IndexKeyField::asc(vec!["extra".into()])]),
                    Default::default(),
                )?);
            }
        }
        Ok(build_plan.take().unwrap()(collection, hint))
    };
    let result = match entry_point {
        WriteEntryPoint::ExecuteWrite => state.execute_write(
            Some(&hint),
            |collection, hint| change_index_and_build_plan(collection, hint),
            false,
        ),
        WriteEntryPoint::ExecuteDelete => state.execute_delete(
            Some(&hint),
            |collection, hint| change_index_and_build_plan(collection, hint),
            false,
            false,
        ),
    };

    if let Some((indexed_value, expected_ids)) = expected_new_index_match {
        let created_index_name = created_index_name.unwrap();
        let documents = state
            .execute_query(
                Some(&Hint::Index(created_index_name)),
                |collection, hint| {
                    Ok(LogicalPlanBuilder::scan(collection, hint)
                        .filter(field_filters(field(["extra"]), [eq(lit(indexed_value))]))
                        .build_arc())
                },
            )
            .unwrap();
        assert_eq!(collect_ids(documents), expected_ids);
    } else {
        let documents = state
            .execute_query(None, |collection, hint| {
                Ok(LogicalPlanBuilder::scan(collection, hint).build_arc())
            })
            .unwrap();
        let documents = collect_documents(documents);
        let mut expected_documents = vec![
            doc! { "_id": 1, "status": "ready", "extra": "old" },
            doc! { "_id": 2, "status": "waiting", "extra": "other" },
        ];
        if additional_matching_document {
            expected_documents.push(doc! { "_id": 3, "status": "ready", "extra": "old" });
        }
        assert_eq!(documents, expected_documents);
    }

    result
}

#[test]
fn prepared_collection_scan_still_works_after_index_creation() {
    let (_directory, db, state) = setup_collection();
    let prepared = prepare_scan(&state, None);

    db.collection("items")
        .create_index(doc! { "status": 1 })
        .unwrap();
    db.collection("items")
        .insert_one(doc! { "_id": 3, "status": "new" })
        .unwrap();

    let documents = state.execute_prepared_query(prepared).unwrap();
    assert_eq!(collect_ids(documents), vec![1, 2, 3]);
}

#[test]
fn prepared_index_scan_errors_if_index_is_dropped_before_execution() {
    let (_directory, db, state) = setup_collection();
    let index_name = db
        .collection("items")
        .create_index(doc! { "status": 1 })
        .unwrap();
    let index_id = state
        .db
        .get_collection("items")
        .unwrap()
        .get_index_by_name(&index_name)
        .unwrap()
        .id;
    let hint = Hint::Index(index_name.clone());
    let prepared = prepare_scan(&state, Some(&hint));

    assert!(matches!(
        prepared.plan.as_ref(),
        PhysicalPlan::IndexScan { index, .. } if *index == index_id
    ));

    state.drop_index(&index_name).unwrap();
    db.collection("items")
        .insert_one(doc! { "_id": 3, "status": "new" })
        .unwrap();

    match state.execute_prepared_query(prepared) {
        Err(Error::IndexNotFound {
            collection_name,
            index_name: actual_index_name,
            id,
        }) => {
            assert_eq!(collection_name, "items");
            assert_eq!(actual_index_name, index_name);
            assert_eq!(id, Some(index_id));
        }
        Err(error) => panic!("expected IndexNotFound, got {error:?}"),
        Ok(_) => panic!("expected a dropped-index error"),
    }
}

#[test]
fn prepared_query_after_collection_drop_obeys_collection_policy() {
    let (_directory, db, strict_state) = setup_collection();
    let prepared_strict = vec![
        prepare_primary_key_query(&strict_state, 1),
        prepare_primary_key_multi_query(&strict_state),
        prepare_scan(&strict_state, None),
    ];

    let mut create_if_missing_state =
        CollectionState::new(strict_state.db.clone(), "items".to_string());
    create_if_missing_state.create_if_missing();
    let prepared_create_if_missing = vec![
        prepare_primary_key_query(&create_if_missing_state, 1),
        prepare_primary_key_multi_query(&create_if_missing_state),
        prepare_scan(&create_if_missing_state, None),
    ];

    for (prepared, expected_plan) in
        prepared_strict
            .iter()
            .zip(["PointSearch", "MultiPointSearch", "CollectionScan"])
    {
        match (expected_plan, prepared.plan.as_ref()) {
            ("PointSearch", PhysicalPlan::PointSearch { .. })
            | ("MultiPointSearch", PhysicalPlan::MultiPointSearch { .. })
            | ("CollectionScan", PhysicalPlan::CollectionScan { .. }) => {}
            (_, plan) => panic!("expected {expected_plan}, got {plan:?}"),
        }
    }

    strict_state.drop_collection().unwrap();
    db.create_collection("sequence-advance").unwrap();
    db.collection("sequence-advance")
        .insert_one(doc! { "_id": 1 })
        .unwrap();

    for prepared in prepared_strict {
        assert!(matches!(
            strict_state.execute_prepared_query(prepared),
            Err(Error::CollectionNotFound { name, .. }) if name == "items"
        ));
    }

    for prepared in prepared_create_if_missing {
        let documents = create_if_missing_state
            .execute_prepared_query(prepared)
            .unwrap();
        assert!(collect_ids(documents).is_empty());
    }
}

#[test]
fn insert_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, _| LogicalPlan::InsertOne {
            collection,
            document: doc! { "value": "inserted" }.to_vec().unwrap(),
        },
    );
}

#[test]
fn insert_many_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, _| LogicalPlan::InsertMany {
            collection,
            documents: vec![
                doc! { "value": "first" }.to_vec().unwrap(),
                doc! { "value": "second" }.to_vec().unwrap(),
            ],
        },
    );
}

#[test]
fn update_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .update_one(update([set([field_name("status")], "updated")]), false)
                .build()
        },
    );
}

#[test]
fn update_many_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .update_many(update([set([field_name("status")], "updated")]), false)
                .build()
        },
    );
}

#[test]
fn find_one_and_update_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .find_one_and_update(
                    update([set([field_name("status")], "updated")]),
                    None,
                    false,
                    ReturnDocument::After,
                )
                .build()
        },
    );
}

#[test]
fn delete_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteDelete,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .delete_one()
                .build()
        },
    );
}

#[test]
fn find_one_and_delete_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .find_one_and_delete(None)
                .build()
        },
    );
}

#[test]
fn replace_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .replace_one(doc! { "status": "replaced" }, false)
                .build()
        },
    );
}

#[test]
fn find_one_and_replace_returns_collection_not_found_if_collection_is_dropped_during_setup() {
    assert_collection_drop_race_returns_not_found(
        WriteEntryPoint::ExecuteWrite,
        |collection, hint| {
            LogicalPlanBuilder::scan(collection, hint)
                .filter(field_filters(field(["_id"]), [eq(lit(1))]))
                .find_one_and_replace(
                    doc! { "status": "replaced" },
                    None,
                    false,
                    ReturnDocument::After,
                )
                .build()
        },
    );
}

fn assert_one_document_updated(result: Result<WriteResult, Error>) {
    assert!(matches!(
        result,
        Ok(WriteResult::Update {
            matched_count: 1,
            modified_count: 1,
            upserted_id: None,
        })
    ));
}

fn assert_one_document_deleted(result: Result<WriteResult, Error>) {
    assert!(matches!(
        result,
        Ok(WriteResult::Delete { deleted_count: 1 })
    ));
}

fn assert_hinted_index_was_dropped(result: Result<WriteResult, Error>) {
    assert!(matches!(result, Err(Error::IndexNotFound { .. })));
}

fn assert_single_document_result(result: Result<WriteResult, Error>, expected: Document) {
    match result {
        Ok(WriteResult::SingleDocument {
            affected_count: 1,
            document: Some(document),
        }) => assert_eq!(document, expected),
        other => panic!("expected one returned document, got {other:?}"),
    }
}

fn assert_many_documents_updated(result: Result<WriteResult, Error>, count: u64) {
    assert!(matches!(
        result,
        Ok(WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id: None,
        }) if matched_count == count && modified_count == count
    ));
}

fn assert_many_documents_deleted(result: Result<WriteResult, Error>, count: u64) {
    assert!(matches!(
        result,
        Ok(WriteResult::Delete { deleted_count }) if deleted_count == count
    ));
}

#[test]
fn update_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::DropHinted,
        None,
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .update_one(update([set([field_name("extra")], "updated")]), false)
                .build()
        },
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn update_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::Create,
        Some(("updated", vec![1])),
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .update_one(update([set([field_name("extra")], "updated")]), false)
                .build()
        },
    );
    assert_one_document_updated(result);
}

#[test]
fn delete_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteDelete,
        IndexChange::DropHinted,
        None,
        false,
        |collection, hint| indexed_status_scan(collection, hint).delete_one().build(),
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn delete_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteDelete,
        IndexChange::Create,
        Some(("old", vec![])),
        false,
        |collection, hint| indexed_status_scan(collection, hint).delete_one().build(),
    );
    assert_one_document_deleted(result);
}

#[test]
fn replace_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::DropHinted,
        None,
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .replace_one(doc! { "extra": "updated" }, false)
                .build()
        },
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn replace_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::Create,
        Some(("updated", vec![1])),
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .replace_one(doc! { "extra": "updated" }, false)
                .build()
        },
    );
    assert_one_document_updated(result);
}

#[test]
fn find_one_and_update_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::DropHinted,
        None,
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .find_one_and_update(
                    update([set([field_name("extra")], "updated")]),
                    None,
                    false,
                    ReturnDocument::After,
                )
                .build()
        },
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn find_one_and_update_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::Create,
        Some(("updated", vec![1])),
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .find_one_and_update(
                    update([set([field_name("extra")], "updated")]),
                    None,
                    false,
                    ReturnDocument::After,
                )
                .build()
        },
    );
    assert_single_document_result(
        result,
        doc! { "_id": 1, "status": "ready", "extra": "updated" },
    );
}

#[test]
fn find_one_and_delete_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::DropHinted,
        None,
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .find_one_and_delete(None)
                .build()
        },
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn find_one_and_delete_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::Create,
        Some(("old", vec![])),
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .find_one_and_delete(None)
                .build()
        },
    );
    assert_single_document_result(result, doc! { "_id": 1, "status": "ready", "extra": "old" });
}

#[test]
fn find_one_and_replace_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::DropHinted,
        None,
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .find_one_and_replace(
                    doc! { "extra": "updated" },
                    None,
                    false,
                    ReturnDocument::After,
                )
                .build()
        },
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn find_one_and_replace_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::Create,
        Some(("updated", vec![1])),
        false,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .find_one_and_replace(
                    doc! { "extra": "updated" },
                    None,
                    false,
                    ReturnDocument::After,
                )
                .build()
        },
    );
    assert_single_document_result(result, doc! { "_id": 1, "extra": "updated" });
}

#[test]
fn update_many_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::DropHinted,
        None,
        true,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .update_many(update([set([field_name("extra")], "updated")]), false)
                .build()
        },
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn update_many_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteWrite,
        IndexChange::Create,
        Some(("updated", vec![1, 3])),
        true,
        |collection, hint| {
            indexed_status_scan(collection, hint)
                .update_many(update([set([field_name("extra")], "updated")]), false)
                .build()
        },
    );
    assert_many_documents_updated(result, 2);
}

#[test]
fn delete_many_returns_index_not_found_if_hinted_index_is_dropped_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteDelete,
        IndexChange::DropHinted,
        None,
        true,
        |collection, hint| indexed_status_scan(collection, hint).delete_many().build(),
    );
    assert_hinted_index_was_dropped(result);
}

#[test]
fn delete_many_succeeds_if_another_index_is_added_before_read() {
    let result = run_index_change_race(
        WriteEntryPoint::ExecuteDelete,
        IndexChange::Create,
        Some(("old", vec![])),
        true,
        |collection, hint| indexed_status_scan(collection, hint).delete_many().build(),
    );
    assert_many_documents_deleted(result, 2);
}

#[test]
fn empty_collection_and_published_index_are_immediately_queryable() {
    let directory = tempfile::tempdir().unwrap();
    let db = QuokkaDB::open(directory.path()).unwrap();
    db.create_collection("empty").unwrap();
    let state = CollectionState::new(db.db_impl.clone(), "empty".to_string());
    let prepared = prepare_scan(&state, None);
    assert!(collect_ids(state.execute_prepared_query(prepared).unwrap()).is_empty());
    let index = db
        .collection("empty")
        .create_index(doc! { "status": 1 })
        .unwrap();
    let prepared = prepare_scan(&state, Some(&Hint::Index(index)));
    assert!(collect_ids(state.execute_prepared_query(prepared).unwrap()).is_empty());
}
