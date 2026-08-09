use super::*;
use crate::error::{Error, Result};
use crate::query::expr_fn::{
    all, and, at_least, at_most, elem_match, exists, field, field_filters, greater_than, has_type,
    interval, less_than, ne, nor, not, or, point, proj_array_elements, proj_elem_match, proj_field,
    proj_fields, proj_slice, size, within,
};
use crate::query::physical_plan::IndexScanRangeExpr;
use crate::query::update::UpdateExpr;
use crate::query::update_fn::{field_name, set, update};
use crate::query::{make_sort_field, ReturnDocument, SortOrder};
use crate::query::{BsonValue, Expr, Parameters, Projection};
use crate::storage::catalog::{IndexDefinition, IndexOptions, OrderedIndexField};
use crate::storage::count_stats::{CountStats, CountStatsKey};
use crate::storage::operation::Operation;
use crate::storage::test_utils::storage_engine;
use crate::storage::write_batch::WriteBatch;
use crate::storage::Direction;
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;
use bson::{Bson, Document};
use std::io::Cursor;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Default)]
struct PauseState {
    hit: bool,
    released: bool,
}

struct PausingHook {
    point: ExecutorFailpoint,
    state: Arc<(Mutex<PauseState>, Condvar)>,
}

impl PausingHook {
    fn new(point: ExecutorFailpoint) -> Self {
        Self {
            point,
            state: Arc::new((Mutex::new(PauseState::default()), Condvar::new())),
        }
    }

    fn wait_until_hit(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        while !state.hit {
            state = condvar.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.released = true;
        condvar.notify_all();
    }
}

impl ExecutorTestHook for PausingHook {
    fn hit(&self, point: ExecutorFailpoint) {
        if point != self.point {
            return;
        }

        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.hit = true;
        condvar.notify_all();

        while !state.released {
            state = condvar.wait(state).unwrap();
        }
    }
}

fn write_batch(operations: Vec<Operation>) -> WriteBatch {
    WriteBatch::new(operations, CountStats::default())
}

fn assert_insert_one_result(result: WriteResult, inserted_id: impl Into<Bson>) {
    assert_eq!(
        result,
        WriteResult::InsertOne {
            inserted_id: inserted_id.into(),
        }
    );
}

fn inserted_id(result: WriteResult) -> Bson {
    match result {
        WriteResult::InsertOne { inserted_id } => inserted_id,
        other => panic!("expected InsertOne write result, got {other:?}"),
    }
}

fn inserted_ids(result: WriteResult) -> Vec<Bson> {
    match result {
        WriteResult::InsertMany { inserted_ids } => inserted_ids,
        other => panic!("expected InsertMany write result, got {other:?}"),
    }
}

fn assert_update_result(
    result: WriteResult,
    matched_count: u64,
    modified_count: u64,
    upserted_id: Option<impl Into<Bson>>,
) {
    assert_eq!(
        result,
        WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id: upserted_id.map(Into::into),
        }
    );
}

fn assert_delete_result(result: WriteResult, deleted_count: u64) {
    assert_eq!(result, WriteResult::Delete { deleted_count });
}

fn assert_find_one_and_update_result(result: WriteResult, expected: Option<Document>) {
    assert_eq!(result, WriteResult::FindOneAndUpdate { document: expected });
}

fn insert_one(executor: &QueryExecutor, collection_id: u32, doc: &Document) -> Result<WriteResult> {
    executor.execute_direct(
        PhysicalPlan::InsertOne {
            collection: collection_id,
            document: doc.to_vec()?,
        },
        None,
    )
}

fn insert_docs<'a>(
    executor: &QueryExecutor,
    collection_id: u32,
    docs: impl IntoIterator<Item = &'a Document>,
) -> Result<()> {
    for doc in docs {
        insert_one(executor, collection_id, doc)?;
    }
    Ok(())
}

fn insert_many(
    executor: &QueryExecutor,
    collection_id: u32,
    docs: &[Document],
) -> Result<WriteResult> {
    executor.execute_direct(
        PhysicalPlan::InsertMany {
            collection: collection_id,
            documents: docs
                .iter()
                .map(|doc| doc.to_vec())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        },
        None,
    )
}

fn full_scan_plan(collection_id: u32) -> Arc<PhysicalPlan> {
    Arc::new(PhysicalPlan::CollectionScan {
        collection: collection_id,
        range: Interval::all(),
        direction: Direction::Forward,
        filter: None,
        projection: None,
    })
}

fn read_stored_doc(
    storage_engine: &StorageEngine,
    collection_id: u32,
    id: impl Into<BsonValue>,
) -> Result<Document> {
    let user_key = id.into().try_into_key()?;
    let doc_bytes = storage_engine
        .read(collection_id, 0, &user_key, None)?
        .unwrap()
        .1;
    Ok(Document::from_reader(Cursor::new(doc_bytes))?)
}

fn point_search_query(
    collection_id: u32,
    params: &mut Parameters,
    id: impl Into<BsonValue>,
) -> Arc<PhysicalPlan> {
    let key = params.collect_parameter(id.into());
    Arc::new(PhysicalPlan::PointSearch {
        collection: collection_id,
        key,
        filter: None,
        projection: None,
    })
}

fn execute_update_one(
    executor: &QueryExecutor,
    collection_id: u32,
    id: i32,
    value: &str,
) -> Result<WriteResult> {
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, id);
    let update_expr = update([set([field_name("value")], value)]);
    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: false,
    };

    executor.execute_direct(update_plan, Some(params))
}

fn execute_update_one_with_expr(
    executor: &QueryExecutor,
    collection_id: u32,
    id: i32,
    update_expr: UpdateExpr,
) -> Result<WriteResult> {
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, id);
    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert: false,
    };

    executor.execute_direct(update_plan, Some(params))
}

fn execute_delete_one(
    executor: &QueryExecutor,
    collection_id: u32,
    id: i32,
) -> Result<WriteResult> {
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, id);
    let delete_plan = PhysicalPlan::DeleteOne {
        collection: collection_id,
        query: query_plan,
    };

    executor.execute_direct(delete_plan, Some(params))
}

fn spawn_paused_update_one(
    executor: Arc<QueryExecutor>,
    collection_id: u32,
    id: i32,
    value: &'static str,
) -> JoinHandle<Result<WriteResult>> {
    thread::spawn(move || execute_update_one(executor.as_ref(), collection_id, id, value))
}

fn spawn_paused_delete_one(
    executor: Arc<QueryExecutor>,
    collection_id: u32,
    id: i32,
) -> JoinHandle<Result<WriteResult>> {
    thread::spawn(move || execute_delete_one(executor.as_ref(), collection_id, id))
}

fn spawn_paused_update_one_with_expr(
    executor: Arc<QueryExecutor>,
    collection_id: u32,
    id: i32,
    update_expr: UpdateExpr,
) -> JoinHandle<Result<WriteResult>> {
    thread::spawn(move || {
        execute_update_one_with_expr(executor.as_ref(), collection_id, id, update_expr)
    })
}

fn execute_update_one_with_expr_and_upsert(
    executor: &QueryExecutor,
    collection_id: u32,
    id: i32,
    update_expr: UpdateExpr,
    upsert: bool,
) -> Result<WriteResult> {
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, id);
    let update_plan = PhysicalPlan::UpdateOne {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        upsert,
    };

    executor.execute_direct(update_plan, Some(params))
}

fn execute_find_one_and_update_with_expr_and_upsert(
    executor: &QueryExecutor,
    collection_id: u32,
    id: i32,
    update_expr: UpdateExpr,
    upsert: bool,
    return_document: ReturnDocument,
) -> Result<WriteResult> {
    let mut params = Parameters::new();
    let query_plan = point_search_query(collection_id, &mut params, id);
    let update_plan = PhysicalPlan::FindOneAndUpdate {
        collection: collection_id,
        query: query_plan,
        update: update_expr,
        projection: None,
        upsert,
        return_document,
    };

    executor.execute_direct(update_plan, Some(params))
}

fn execute_update_many(
    executor: &QueryExecutor,
    collection_id: u32,
    query: Arc<PhysicalPlan>,
    update_expr: UpdateExpr,
    upsert: bool,
) -> Result<WriteResult> {
    let params = Parameters::new();
    let update_plan = PhysicalPlan::UpdateMany {
        collection: collection_id,
        query,
        update: update_expr,
        upsert,
    };

    executor.execute_direct(update_plan, Some(params))
}

fn spawn_paused_update_many(
    executor: Arc<QueryExecutor>,
    collection_id: u32,
    query: Arc<PhysicalPlan>,
    update_expr: UpdateExpr,
    upsert: bool,
) -> JoinHandle<Result<WriteResult>> {
    thread::spawn(move || {
        execute_update_many(executor.as_ref(), collection_id, query, update_expr, upsert)
    })
}

fn spawn_paused_insert_one(
    executor: Arc<QueryExecutor>,
    collection_id: u32,
    doc: Document,
) -> JoinHandle<Result<WriteResult>> {
    thread::spawn(move || insert_one(executor.as_ref(), collection_id, &doc))
}

fn spawn_paused_update_one_with_expr_and_upsert(
    executor: Arc<QueryExecutor>,
    collection_id: u32,
    id: i32,
    update_expr: UpdateExpr,
    upsert: bool,
) -> JoinHandle<Result<WriteResult>> {
    thread::spawn(move || {
        execute_update_one_with_expr_and_upsert(
            executor.as_ref(),
            collection_id,
            id,
            update_expr,
            upsert,
        )
    })
}
mod read;
mod upsert;
mod write;
