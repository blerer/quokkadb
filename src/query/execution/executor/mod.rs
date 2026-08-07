use crate::error::Result;
use bson::Document;
use sonyflake::Sonyflake;
use std::sync::Mutex;

pub type QueryOutput = Box<dyn Iterator<Item = Result<Document>>>;
#[cfg(test)]
pub(crate) use super::query_executor::QueryExecutor;
pub(crate) use read::ReadExecutor;
pub(crate) use write::WriteExecutor;

#[cfg(test)]
use crate::query::physical_plan::PhysicalPlan;
#[cfg(test)]
use crate::storage::storage_engine::StorageEngine;

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
}

#[cfg(test)]
pub(crate) trait ExecutorTestHook: Send + Sync {
    fn hit(&self, point: ExecutorFailpoint);
}

pub(super) fn generate_bson_id(id_generator: &Mutex<Sonyflake>) -> bson::Bson {
    let new_id = id_generator.lock().unwrap().next_id().unwrap();
    bson::Bson::Int64(
        i64::try_from(new_id.to_u64()).expect("Sonyflake IDs must fit into signed 64-bit BSON"),
    )
}

#[cfg(test)]
mod tests;
