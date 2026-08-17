use crate::error::Result;
use crate::obs::metrics::MetricRegistry;
use crate::query::execution::executor::{
    Metrics, QueryOutput, ReadExecutor, WriteExecutor, WriteResult,
};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::Parameters;
use crate::storage::storage_engine::StorageEngine;
use sonyflake::Sonyflake;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Executes a physical query plan.
pub struct QueryExecutor {
    metrics: Metrics,
    read_executor: ReadExecutor,
    write_executor: WriteExecutor,
}

impl QueryExecutor {
    pub fn new_with_metrics(
        storage_engine: Arc<StorageEngine>,
        metric_registry: &mut MetricRegistry,
    ) -> Self {
        let metrics = Metrics::new();
        metrics.register_to(metric_registry);
        let read_executor = ReadExecutor::new(storage_engine.clone(), metrics.clone());
        let id_generator = Arc::new(Self::new_id_generator());
        let write_executor = WriteExecutor::new(
            storage_engine,
            read_executor.clone(),
            id_generator,
            metrics.clone(),
        );

        Self {
            metrics,
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
    ) -> Result<WriteResult> {
        self.metrics.write_queries.inc();
        let start = Instant::now();
        let result = self.write_executor.execute_direct(plan, parameters);
        self.metrics
            .write_query_duration
            .record(start.elapsed().as_micros() as u64);

        result
    }

    /// Executes the given physical plan using the latest visible data.
    pub fn execute_cached(
        &self,
        plan: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        self.execute_cached_at_snapshot(plan, parameters, None)
    }

    /// Executes a query plan against a specific data snapshot.
    /// If `snapshot` is `None`, it uses the latest visible data.
    pub fn execute_cached_at_snapshot(
        &self,
        plan: Arc<PhysicalPlan>,
        parameters: &Parameters,
        snapshot: Option<u64>,
    ) -> Result<QueryOutput> {
        self.metrics.read_queries.inc();
        let start = Instant::now();
        let output = self
            .read_executor
            .execute_cached_at_snapshot(plan, parameters, snapshot)?;

        Ok(Box::new(InstrumentedQueryOutput::new(
            output,
            self.metrics.clone(),
            start,
        )))
    }
}

struct InstrumentedQueryOutput {
    inner: QueryOutput,
    metrics: Metrics,
    start: Instant,
    rows_returned: u64,
    recorded: bool,
}

impl InstrumentedQueryOutput {
    fn new(inner: QueryOutput, metrics: Metrics, start: Instant) -> Self {
        Self {
            inner,
            metrics,
            start,
            rows_returned: 0,
            recorded: false,
        }
    }

    fn record_if_needed(&mut self) {
        if self.recorded {
            return;
        }

        self.metrics.rows_returned.inc_by(self.rows_returned);
        self.metrics
            .read_query_duration
            .record(self.start.elapsed().as_micros() as u64);
        self.recorded = true;
    }
}

impl Iterator for InstrumentedQueryOutput {
    type Item = Result<bson::Document>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(document)) => {
                self.rows_returned += 1;
                Some(Ok(document))
            }
            Some(Err(err)) => {
                self.record_if_needed();
                Some(Err(err))
            }
            None => {
                self.record_if_needed();
                None
            }
        }
    }
}

impl Drop for InstrumentedQueryOutput {
    fn drop(&mut self) {
        self.record_if_needed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::logger::test_instance;
    use crate::obs::metrics::names;
    use crate::options::options::Options;
    use crate::storage::Direction;
    use crate::util::interval::Interval;
    use bson::{doc, Bson, Document};
    use tempfile::tempdir;

    struct ExecutorTestRuntime {
        _dir: tempfile::TempDir,
        metric_registry: MetricRegistry,
        storage_engine: Arc<StorageEngine>,
        executor: QueryExecutor,
    }

    fn executor_test_runtime() -> Result<ExecutorTestRuntime> {
        let dir = tempdir()?;
        let mut metric_registry = MetricRegistry::new();
        let storage_engine = StorageEngine::new(
            test_instance(),
            &mut metric_registry,
            Arc::new(Options::lightweight()),
            dir.path(),
        )?;
        let executor =
            QueryExecutor::new_with_metrics(storage_engine.clone(), &mut metric_registry);

        Ok(ExecutorTestRuntime {
            _dir: dir,
            metric_registry,
            storage_engine,
            executor,
        })
    }

    fn point_search_plan(
        collection_id: u32,
        parameters: &mut Parameters,
        id: impl Into<crate::query::BsonValue>,
    ) -> Arc<PhysicalPlan> {
        let key = parameters.collect_parameter(id.into());
        Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key,
            filter: None,
            projection: None,
        })
    }

    #[test]
    fn execute_direct_records_write_metrics() -> Result<()> {
        let runtime = executor_test_runtime()?;
        let collection_id = runtime
            .storage_engine
            .create_collection_if_not_exists("items")?;

        let result = runtime.executor.execute_direct(
            PhysicalPlan::InsertOne {
                collection: collection_id,
                document: doc! { "_id": 1, "name": "a" }.to_vec()?,
            },
            None,
        )?;

        assert_eq!(
            result,
            WriteResult::InsertOne {
                inserted_id: Bson::Int32(1),
            }
        );
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::WRITE_QUERIES),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::DOCUMENTS_WRITTEN),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .histogram(names::executor::WRITE_QUERY_DURATION)
                .snapshot()
                .count,
            1
        );

        Ok(())
    }

    #[test]
    fn execute_cached_records_read_metrics_after_consumption() -> Result<()> {
        let runtime = executor_test_runtime()?;
        let collection_id = runtime
            .storage_engine
            .create_collection_if_not_exists("items")?;
        runtime.executor.execute_direct(
            PhysicalPlan::InsertOne {
                collection: collection_id,
                document: doc! { "_id": 1, "name": "a" }.to_vec()?,
            },
            None,
        )?;

        let mut params = Parameters::new();
        let output = runtime.executor.execute_cached(
            point_search_plan(collection_id, &mut params, 1_i32),
            &params,
        )?;
        let rows: Vec<Document> = output.collect::<Result<Vec<_>>>()?;

        assert_eq!(rows, vec![doc! { "_id": 1, "name": "a" }]);
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::READ_QUERIES),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::ROWS_RETURNED),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::POINT_SEARCHES),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .histogram(names::executor::READ_QUERY_DURATION)
                .snapshot()
                .count,
            1
        );

        Ok(())
    }

    #[test]
    fn execute_cached_records_metrics_when_dropped_without_iteration() -> Result<()> {
        let runtime = executor_test_runtime()?;
        let collection_id = runtime
            .storage_engine
            .create_collection_if_not_exists("items")?;

        let params = Parameters::new();
        let output = runtime.executor.execute_cached(
            Arc::new(PhysicalPlan::CollectionScan {
                collection: collection_id,
                range: Interval::all(),
                direction: Direction::Forward,
                filter: None,
                projection: None,
            }),
            &params,
        )?;
        drop(output);

        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::READ_QUERIES),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::ROWS_RETURNED),
            0
        );
        assert_eq!(
            runtime
                .metric_registry
                .counter_value(names::executor::COLLECTION_SCANS),
            1
        );
        assert_eq!(
            runtime
                .metric_registry
                .histogram(names::executor::READ_QUERY_DURATION)
                .snapshot()
                .count,
            1
        );

        Ok(())
    }
}
