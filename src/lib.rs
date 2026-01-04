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
use std::path::{Path};
use std::sync::Arc;
use bson::Document;
use bson::doc;
use crate::collection::{Collection, CreateCollectionOptions};
use crate::obs::logger::{LoggerAndTracer, NoOpLogger, LogLevel};
use crate::obs::metrics::MetricRegistry;
use crate::options::options::Options;
use query::execution::executor::QueryExecutor;
use query::logical_plan::LogicalPlan;
use crate::query::optimizer::optimizer::Optimizer;
use crate::query::Parameters;
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
        let optimizer = Arc::new(Optimizer::new(logger.clone())); // Add normalization rules as needed
        let executor = Arc::new(QueryExecutor::new(storage_engine.clone()));
        let db_impl = Arc::new(DbImpl {
            logger,
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

    /// Creates a new collection with the given name and default options.
    /// Returns an error if a collection with the same name already exists.
    pub fn create_collection(&self, name: &str) -> error::Result<()> {
        self.db_impl.create_collection(name, CreateCollectionOptions::default())?;
        Ok(())
    }

    /// Creates a new collection with the given name and options.
    /// Returns an error if a collection with the same name already exists.
    pub fn create_collection_with_options(
        &self,
        name: &str,
        options: CreateCollectionOptions,
    ) -> error::Result<()> {
        self.db_impl.create_collection(name, options)?;
        Ok(())
    }

    /// Drops the collection with the given name.
    /// Returns an error if the collection does not exist.
    pub fn drop_collection(&self, name: &str) -> error::Result<()> {
        self.db_impl.drop_collection(name)?;
        Ok(())
    }

    /// Renames a collection from `old_name` to `new_name`.
    /// Returns an error if the source collection does not exist or if the target name already exists.
    pub fn rename_collection(&self, old_name: &str, new_name: &str) -> error::Result<()> {
        self.db_impl.rename_collection(old_name, new_name)?;
        Ok(())
    }

    /// Returns a list of all collections in the database.
    /// Each document contains metadata about a collection, similar to MongoDB's listCollections.
    pub fn list_collections(&self) -> Vec<Document> {
        let catalog = self.db_impl.storage_engine.catalog();
        catalog
            .list_collections()
            .map(|c| {
                doc! {
                    "name": &c.name,
                    "type": "collection",
                    "options": {
                        "idCreationStrategy": format!("{:?}", c.options.id_creation_strategy)
                    },
                    "info": {
                        "id": c.id as i64,
                        "createdAt": c.created_at as i64,
                    }
                }
            })
            .collect()
    }
}

struct DbImpl {
    logger: Arc<dyn LoggerAndTracer>,
    optimizer: Arc<Optimizer>,
    executor: Arc<QueryExecutor>,
    storage_engine: Arc<StorageEngine>,
}

impl DbImpl {
    pub fn create_collection_if_not_exists(self: &Arc<Self>, name: &str) -> error::Result<u32> {
        Ok(self.storage_engine.create_collection_if_not_exists(name)?)
    }

    pub fn create_collection(self: &Arc<Self>, name: &str, options: CreateCollectionOptions) -> error::Result<u32> {
        Ok(self.storage_engine.create_collection(name, options.into())?)
    }

    pub fn drop_collection(self: &Arc<Self>, name: &str) -> error::Result<()> {
        Ok(self.storage_engine.drop_collection(name)?)
    }

    pub fn rename_collection(self: &Arc<Self>, old_name: &str, new_name: &str) -> error::Result<()> {
        Ok(self.storage_engine.rename_collection(old_name, new_name)?)
    }

    pub fn get_collection_id(self: &Arc<Self>, name: &str) -> Option<u32> {
        self.storage_engine.catalog().get_collection_by_name(name).and_then(|c| Some(c.id))
    }

    pub fn execute_write(&self, logical_plan: LogicalPlan) -> error::Result<Document> {
        let (physical_plan, parameters) = match logical_plan {
            LogicalPlan::InsertOne { collection, document } => {
                (PhysicalPlan::InsertOne { collection, document, }, None)
            }
            LogicalPlan::InsertMany { collection, documents } => {
                (PhysicalPlan::InsertMany { collection, documents, }, None)
            }
            LogicalPlan::UpdateOne { collection, query, update, upsert} => {
                let (parameters, query) = self.optimize_query(query);
                (PhysicalPlan::UpdateOne { collection, query, update, upsert}, Some(parameters))
            }
            LogicalPlan::UpdateMany { collection, query, update, upsert} => {
                let (parameters, query) = self.optimize_query(query);
                (PhysicalPlan::UpdateMany { collection, query, update, upsert}, Some(parameters))
            }
            _ => panic!("Unsupported write operation {:?}", logical_plan),
        };

        self.executor.execute_direct(physical_plan, parameters)?.next().unwrap()
    }

    pub fn execute_query(&self, logical_plan: Arc<LogicalPlan>) -> error::Result<Box<dyn Iterator<Item = error::Result<Document>>>> {

        let (parameters, physical_plan) = self.optimize_query(logical_plan);

        self.executor.execute_cached(physical_plan, &parameters)
    }

    fn optimize_query(&self, logical_plan: Arc<LogicalPlan>) -> (Parameters, Arc<PhysicalPlan>) {
        // First, normalize the logical plan
        let normalized_plan = self.optimizer.normalize(logical_plan);
        // Then, parametrize the plan to collect parameters
        let (logical_plan, parameters) = self.optimizer.parametrize(normalized_plan);

        // Checks the statement cache for the physical plan

        // If the plan is not cached, optimize it
        let catalog = self.storage_engine.catalog();
        let physical_plan = self.optimizer.optimize(logical_plan, catalog);
        (parameters, physical_plan)
    }
}

impl Drop for DbImpl {
    fn drop(&mut self) {
        if let Err(e) = self.storage_engine.shutdown() {
            error!(self.logger, "An error occurred during shutdown: {}", e);
        } else {
            info!(self.logger, "Quokkadb shutdown completed successfully");
        }
    }
}
