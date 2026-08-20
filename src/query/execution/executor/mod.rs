use crate::error::Result;
use crate::obs::metrics::{self, Counter, Histogram, MetricRegistry};
use bson::Bson;
use bson::Document;
use sonyflake::Sonyflake;
use std::sync::{Arc, Mutex};

pub type QueryOutput = Box<dyn Iterator<Item = Result<Document>>>;

#[derive(Clone)]
pub(crate) struct Metrics {
    pub(crate) read_queries: Arc<Counter>,
    pub(crate) write_queries: Arc<Counter>,
    pub(crate) read_query_duration: Arc<Histogram>,
    pub(crate) write_query_duration: Arc<Histogram>,
    pub(crate) rows_returned: Arc<Counter>,
    pub(crate) documents_written: Arc<Counter>,
    pub(crate) collection_scans: Arc<Counter>,
    pub(crate) index_scans: Arc<Counter>,
    pub(crate) point_searches: Arc<Counter>,
    pub(crate) multi_point_searches: Arc<Counter>,
    pub(crate) in_memory_sorts: Arc<Counter>,
    pub(crate) external_merge_sorts: Arc<Counter>,
    pub(crate) top_k_sorts: Arc<Counter>,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            read_queries: Counter::new(),
            write_queries: Counter::new(),
            read_query_duration: Histogram::new_time_histogram(),
            write_query_duration: Histogram::new_time_histogram(),
            rows_returned: Counter::new(),
            documents_written: Counter::new(),
            collection_scans: Counter::new(),
            index_scans: Counter::new(),
            point_searches: Counter::new(),
            multi_point_searches: Counter::new(),
            in_memory_sorts: Counter::new(),
            external_merge_sorts: Counter::new(),
            top_k_sorts: Counter::new(),
        }
    }

    pub(crate) fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_counter(
                metrics::names::executor::READ_QUERIES,
                self.read_queries.clone(),
            )
            .register_counter(
                metrics::names::executor::WRITE_QUERIES,
                self.write_queries.clone(),
            )
            .register_histogram(
                metrics::names::executor::READ_QUERY_DURATION,
                self.read_query_duration.clone(),
            )
            .register_histogram(
                metrics::names::executor::WRITE_QUERY_DURATION,
                self.write_query_duration.clone(),
            )
            .register_counter(
                metrics::names::executor::ROWS_RETURNED,
                self.rows_returned.clone(),
            )
            .register_counter(
                metrics::names::executor::DOCUMENTS_WRITTEN,
                self.documents_written.clone(),
            )
            .register_counter(
                metrics::names::executor::COLLECTION_SCANS,
                self.collection_scans.clone(),
            )
            .register_counter(
                metrics::names::executor::INDEX_SCANS,
                self.index_scans.clone(),
            )
            .register_counter(
                metrics::names::executor::POINT_SEARCHES,
                self.point_searches.clone(),
            )
            .register_counter(
                metrics::names::executor::MULTI_POINT_SEARCHES,
                self.multi_point_searches.clone(),
            )
            .register_counter(
                metrics::names::executor::IN_MEMORY_SORTS,
                self.in_memory_sorts.clone(),
            )
            .register_counter(
                metrics::names::executor::EXTERNAL_MERGE_SORTS,
                self.external_merge_sorts.clone(),
            )
            .register_counter(
                metrics::names::executor::TOP_K_SORTS,
                self.top_k_sorts.clone(),
            );
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteResult {
    InsertOne {
        inserted_id: Bson,
    },
    InsertMany {
        inserted_ids: Vec<Bson>,
    },
    Update {
        matched_count: u64,
        modified_count: u64,
        upserted_id: Option<Bson>,
    },
    SingleDocument {
        affected_count: u64,
        document: Option<Document>,
    },
    Delete {
        deleted_count: u64,
    },
}

#[cfg(test)]
pub(crate) use super::query_executor::QueryExecutor;
pub(crate) use read::ReadExecutor;
pub(crate) use write::WriteExecutor;

#[cfg(test)]
use crate::query::physical_plan::PhysicalPlan;
#[cfg(test)]
use std::cell::RefCell;

mod bind;
mod read;
mod upsert;
mod write;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutorFailpoint {
    UpdateOneAfterRead,
    UpdateOneUpsertAfterNoMatch,
    UpdateManyBeforeCommit,
    InsertManualAfterPreflightBeforeWrite,
    DeleteOneAfterRead,
    DeleteManyBeforeCommit,
}

#[cfg(test)]
pub(crate) trait ExecutorTestHook: Send + Sync {
    fn hit(&self, point: ExecutorFailpoint);
}

#[cfg(test)]
thread_local! {
    static EXECUTOR_TEST_HOOK: RefCell<Option<Arc<dyn ExecutorTestHook>>> = RefCell::new(None);
}

#[cfg(test)]
pub(crate) fn with_executor_test_hook<T>(
    hook: Arc<dyn ExecutorTestHook>,
    f: impl FnOnce() -> T,
) -> T {
    EXECUTOR_TEST_HOOK.with(|cell| {
        let previous = cell.replace(Some(hook));
        let result = f();
        cell.replace(previous);
        result
    })
}

#[cfg(test)]
pub(crate) fn invoke_executor_test_hook(point: ExecutorFailpoint) {
    EXECUTOR_TEST_HOOK.with(|cell| {
        if let Some(hook) = cell.borrow().as_ref() {
            hook.hit(point);
        }
    });
}

pub(super) fn generate_bson_id(id_generator: &Mutex<Sonyflake>) -> bson::Bson {
    let new_id = id_generator.lock().unwrap().next_id().unwrap();
    bson::Bson::Int64(
        i64::try_from(new_id.to_u64()).expect("Sonyflake IDs must fit into signed 64-bit BSON"),
    )
}
#[cfg(test)]
mod test_utils;
