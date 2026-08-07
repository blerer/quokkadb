use crate::error::Result;
#[cfg(test)]
use crate::query::execution::executor::ExecutorTestHook;
use crate::query::execution::executor::{QueryOutput, ReadExecutor, WriteExecutor};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::Parameters;
use crate::storage::storage_engine::StorageEngine;
use sonyflake::Sonyflake;
use std::sync::{Arc, Mutex};

/// Executes a physical query plan.
pub struct QueryExecutor {
    read_executor: ReadExecutor,
    write_executor: WriteExecutor,
}

impl QueryExecutor {
    /// Creates a new `QueryExecutor`.
    pub fn new(storage_engine: Arc<StorageEngine>) -> Self {
        let read_executor = ReadExecutor::new(storage_engine.clone());
        let id_generator = Arc::new(Self::new_id_generator());
        let write_executor = WriteExecutor::new(
            storage_engine,
            read_executor.clone(),
            id_generator,
            #[cfg(test)]
            None,
        );

        Self {
            read_executor,
            write_executor,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_hook(
        storage_engine: Arc<StorageEngine>,
        test_hook: Arc<dyn ExecutorTestHook>,
    ) -> Self {
        let read_executor = ReadExecutor::new(storage_engine.clone());
        let id_generator = Arc::new(Self::new_id_generator());
        let write_executor = WriteExecutor::new(
            storage_engine,
            read_executor.clone(),
            id_generator,
            Some(test_hook),
        );

        Self {
            read_executor,
            write_executor,
        }
    }

    fn new_id_generator() -> Mutex<Sonyflake> {
        Mutex::new(
            Sonyflake::builder()
                .machine_id(&|| Ok(0))
                .finalize()
                .unwrap(),
        )
    }

    pub fn execute_direct(
        &self,
        plan: PhysicalPlan,
        parameters: Option<Parameters>,
    ) -> Result<QueryOutput> {
        self.write_executor.execute_direct(plan, parameters)
    }

    /// Executes the given physical plan using the latest visible data.
    pub fn execute_cached(
        &self,
        plan: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        self.read_executor
            .execute_cached_at_snapshot(plan, parameters, None)
    }

    /// Executes a query plan against a specific data snapshot.
    /// If `snapshot` is `None`, it uses the latest visible data.
    pub fn execute_cached_at_snapshot(
        &self,
        plan: Arc<PhysicalPlan>,
        parameters: &Parameters,
        snapshot: Option<u64>,
    ) -> Result<QueryOutput> {
        self.read_executor
            .execute_cached_at_snapshot(plan, parameters, snapshot)
    }
}
