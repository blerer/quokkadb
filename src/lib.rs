extern crate core;

pub mod collection;
pub mod error;
mod io;
pub mod obs;
pub mod options;
mod query;
mod storage;
mod util;

use crate::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use bson::Document;
use crate::collection::Collection;
use crate::obs::logger::{LoggerAndTracer, NoOpLogger};
use crate::obs::metrics::MetricRegistry;
use crate::options::options::Options;
use query::execution::executor::QueryExecutor;
use query::logical_plan::LogicalPlan;
use crate::query::optimizer::optimizer::Optimizer;
use crate::query::physical_plan::PhysicalPlan;
use crate::storage::storage_engine::StorageEngine;

pub struct QuokkaDB {
    options: Arc<Options>,
    db_impl: Arc<DbImpl>,

}

impl QuokkaDB {
    pub fn open(path: &Path) -> error::Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    pub fn open_with_options(path: &Path, options: Options) -> error::Result<Self> {
        Self::open_with_options_and_logger(path, options, Arc::new(NoOpLogger {}))
    }

    pub fn open_with_logger(path: &Path, logger: Arc<dyn LoggerAndTracer>) -> error::Result<Self> {
        Self::open_with_options_and_logger(path, Options::default(), logger)
    }

    pub fn open_with_options_and_logger(path: &Path,
                                        options: Options,
                                        logger: Arc<dyn LoggerAndTracer>) -> error::Result<Self> {

        let options = Arc::new(options);
        let mut metric_registry = MetricRegistry::new();
        let storage_engine = StorageEngine::new(logger.clone(), &mut metric_registry, options.clone(), path)?;
        let optimizer = Arc::new(Optimizer::new(logger)); // Add normalization rules as needed
        let executor = Arc::new(QueryExecutor::new(storage_engine.clone()));
        let db_impl = Arc::new(DbImpl {
            optimizer,
            executor,
            storage_engine,
        });

        Ok(QuokkaDB {
            options,
            db_impl,
        })
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(self.db_impl.clone(), name.to_string())
    }
}

struct DbImpl {
    optimizer: Arc<Optimizer>,
    executor: Arc<QueryExecutor>,
    storage_engine: Arc<StorageEngine>,
}

impl DbImpl {
    pub fn create_collection_if_not_exists(self: &Arc<Self>, name: &str) -> error::Result<u32> {
        Ok(self.storage_engine.create_collection_if_not_exists(name)?)
    }

    pub fn get_collection_id(self: &Arc<Self>, name: &str) -> Option<u32> {
        self.storage_engine.catalog().get_collection_by_name(name).and_then(|c| Some(c.id))
    }

    pub fn execute_write(&self, logical_plan: LogicalPlan) -> error::Result<Document> {
        let physical_plan = match logical_plan {
            LogicalPlan::InsertOne { collection, document } => {
                PhysicalPlan::InsertOne {
                    collection,
                    document,
                }
            }
            LogicalPlan::InsertMany { collection, documents } => {
                PhysicalPlan::InsertMany {
                    collection,
                    documents,
                }
            }
            _ => panic!("Unsupported write operation {:?}", logical_plan),
        };

        self.executor.execute_direct(physical_plan)?.next().unwrap()
    }

    pub fn execute_query(&self, logical_plan: Arc<LogicalPlan>) -> error::Result<Box<dyn Iterator<Item = error::Result<Document>>>> {

        // First, normalize the logical plan
        let normalized_plan = self.optimizer.normalize(logical_plan);
        // Then, parametrize the plan to collect parameters
        let (logical_plan, parameters) = self.optimizer.parametrize(normalized_plan);

        // Checks the statement cache for the physical plan

        // If the plan is not cached, optimize it
        let catalog = self.storage_engine.catalog();
        let physical_plan = self.optimizer.optimize(logical_plan, catalog);

        self.executor.execute_cached(physical_plan, &parameters)
    }
}
