extern crate core;

pub mod collection;
pub mod error;
mod io;
pub mod metrics;
pub mod obs;
pub mod options;
mod query;
mod storage;
mod util;

use crate::collection::CreateIndexOptions;
use crate::collection::{Collection, CreateCollectionOptions, IdCreationStrategy};
use crate::error::Error;
use crate::metrics::Metrics;
use crate::obs::metrics::MetricRegistry;
use crate::obs::observability::Observability;
use crate::options::options::Options;
use crate::query::execution::WriteResult;
use crate::query::optimizer::optimizer::Optimizer;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::{IndexKeySpec, Parameters};
use crate::storage::catalog::CollectionMetadata;
use crate::storage::count_stats::CountStatsKey;
use crate::storage::storage_engine::StorageEngine;
use bson::Document;
use query::execution::QueryExecutor;
use query::logical_plan::LogicalPlan;
use std::path::Path;
use std::sync::Arc;
use tracing::debug_span;
use tracing::span::EnteredSpan;

#[derive(Clone)]
pub struct QuokkaDB {
    options: Arc<Options>,
    db_impl: Arc<DbImpl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionInfo {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    pub id_creation_strategy: IdCreationStrategy,
}

impl QuokkaDB {
    pub fn open(path: &Path) -> error::Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    pub fn open_with_options(path: &Path, options: Options) -> error::Result<Self> {
        options.validate()?;

        let options = Arc::new(options);
        let observability = Observability::new(path);
        let _instance_span = observability.instance_span().clone().entered();
        let mut metric_registry = MetricRegistry::new();
        let storage_engine = StorageEngine::new(&mut metric_registry, options.clone(), path)?;
        let optimizer = Arc::new(Optimizer::new()); // Add normalization rules as needed
        let executor = Arc::new(QueryExecutor::new_with_metrics(
            storage_engine.clone(),
            &mut metric_registry,
        ));
        let db_impl = Arc::new(DbImpl {
            observability,
            metrics: metric_registry.clone(),
            optimizer,
            executor,
            storage_engine,
        });

        tracing::debug!(event = "db.opened");

        Ok(QuokkaDB { options, db_impl })
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(self.db_impl.clone(), name.to_string())
    }

    pub fn metrics(&self) -> Metrics<'_> {
        Metrics {
            registry: &self.db_impl.metrics,
        }
    }

    /// Creates a new collection with the given name and default options.
    /// Returns an error if a collection with the same name already exists.
    pub fn create_collection(&self, name: &str) -> error::Result<()> {
        self.db_impl
            .create_collection(name, CreateCollectionOptions::default())?;
        Ok(())
    }

    /// Creates a collection builder for a new collection with the given name.
    pub fn create_collection_with(&self, name: &str) -> CreateCollection<'_> {
        CreateCollection::new(self, name)
    }

    /// Returns typed metadata for all collections in the database.
    pub fn list_collections(&self) -> Vec<CollectionInfo> {
        let catalog = self.db_impl.storage_engine.catalog();
        catalog
            .list_collections()
            .map(|c| CollectionInfo {
                id: c.id,
                name: c.name.clone(),
                created_at: c.created_at,
                id_creation_strategy: c.options.id_creation_strategy.clone().into(),
            })
            .collect()
    }
}

pub struct CreateCollection<'a> {
    db: &'a QuokkaDB,
    name: String,
    options: CreateCollectionOptions,
}

impl<'a> CreateCollection<'a> {
    fn new(db: &'a QuokkaDB, name: &str) -> Self {
        Self {
            db,
            name: name.to_string(),
            options: CreateCollectionOptions::default(),
        }
    }

    /// Sets the ID creation strategy for the collection.
    pub fn id_creation_strategy(mut self, strategy: IdCreationStrategy) -> Self {
        self.options.id_creation_strategy = strategy;
        self
    }

    /// Executes the collection creation operation.
    pub fn execute(self) -> error::Result<()> {
        self.db
            .db_impl
            .create_collection(&self.name, self.options)?;
        Ok(())
    }
}

struct DbImpl {
    observability: Arc<Observability>,
    metrics: MetricRegistry,
    optimizer: Arc<Optimizer>,
    executor: Arc<QueryExecutor>,
    storage_engine: Arc<StorageEngine>,
}

struct OperationSpans {
    _instance: EnteredSpan,
    _operation: EnteredSpan,
}

impl DbImpl {
    pub fn create_collection_if_not_exists(self: &Arc<Self>, name: &str) -> error::Result<u32> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("create_collection_if_not_exists", collection = %name)
                .entered(),
        };
        Ok(self.storage_engine.create_collection_if_not_exists(name)?)
    }

    pub fn create_collection(
        self: &Arc<Self>,
        name: &str,
        options: CreateCollectionOptions,
    ) -> error::Result<u32> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("create_collection", collection = %name).entered(),
        };
        Ok(self
            .storage_engine
            .create_collection(name, options.into())?)
    }

    pub fn drop_collection(self: &Arc<Self>, name: &str) -> error::Result<()> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("drop_collection", collection = %name).entered(),
        };
        Ok(self.storage_engine.drop_collection(name)?)
    }

    pub fn rename_collection(
        self: &Arc<Self>,
        old_name: &str,
        new_name: &str,
    ) -> error::Result<()> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("rename_collection", collection = %old_name).entered(),
        };
        Ok(self.storage_engine.rename_collection(old_name, new_name)?)
    }

    pub fn get_collection(self: &Arc<Self>, name: &str) -> Option<Arc<CollectionMetadata>> {
        self.storage_engine.catalog().get_collection_by_name(name)
    }

    pub fn create_index(
        self: &Arc<Self>,
        collection_id: u32,
        spec: IndexKeySpec,
        options: CreateIndexOptions,
    ) -> error::Result<String> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("create_index").entered(),
        };
        Ok(self
            .storage_engine
            .create_index(collection_id, spec.into(), options.into())?
            .name)
    }

    pub fn drop_index(self: &Arc<Self>, collection_id: u32, index_id: u32) -> error::Result<()> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("drop_index").entered(),
        };
        Ok(self.storage_engine.drop_index(collection_id, index_id)?)
    }

    pub fn estimated_document_count(&self, collection_id: u32) -> error::Result<u64> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("estimated_document_count").entered(),
        };
        let count = self
            .storage_engine
            .count_stat(&CountStatsKey::Collection(collection_id))
            .unwrap_or(0);

        Ok(count as u64)
    }

    pub fn execute_write(&self, logical_plan: LogicalPlan) -> error::Result<WriteResult> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("execute_write").entered(),
        };
        let (physical_plan, parameters) = match logical_plan {
            LogicalPlan::InsertOne {
                collection,
                document,
            } => (
                PhysicalPlan::InsertOne {
                    collection,
                    document,
                },
                None,
            ),
            LogicalPlan::InsertMany {
                collection,
                documents,
            } => (
                PhysicalPlan::InsertMany {
                    collection,
                    documents,
                },
                None,
            ),
            LogicalPlan::UpdateOne {
                collection,
                query,
                update,
                upsert,
            } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::UpdateOne {
                        collection,
                        query,
                        update,
                        upsert,
                    },
                    Some(parameters),
                )
            }
            LogicalPlan::UpdateMany {
                collection,
                query,
                update,
                upsert,
            } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::UpdateMany {
                        collection,
                        query,
                        update,
                        upsert,
                    },
                    Some(parameters),
                )
            }
            LogicalPlan::FindOneAndUpdate {
                collection,
                query,
                update,
                projection,
                upsert,
                return_document,
            } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::FindOneAndUpdate {
                        collection,
                        query,
                        update,
                        projection,
                        upsert,
                        return_document,
                    },
                    Some(parameters),
                )
            }
            LogicalPlan::ReplaceOne {
                collection,
                query,
                replacement,
                upsert,
            } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::ReplaceOne {
                        collection,
                        query,
                        replacement,
                        upsert,
                    },
                    Some(parameters),
                )
            }
            LogicalPlan::FindOneAndReplace {
                collection,
                query,
                replacement,
                projection,
                upsert,
                return_document,
            } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::FindOneAndReplace {
                        collection,
                        query,
                        replacement,
                        projection,
                        upsert,
                        return_document,
                    },
                    Some(parameters),
                )
            }
            LogicalPlan::FindOneAndDelete {
                collection,
                query,
                projection,
            } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::FindOneAndDelete {
                        collection,
                        query,
                        projection,
                    },
                    Some(parameters),
                )
            }
            LogicalPlan::DeleteOne { collection, query } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::DeleteOne { collection, query },
                    Some(parameters),
                )
            }
            LogicalPlan::DeleteMany { collection, query } => {
                let (parameters, query) = self.optimize_query(query);
                (
                    PhysicalPlan::DeleteMany { collection, query },
                    Some(parameters),
                )
            }
            _ => panic!("Unsupported write operation {:?}", logical_plan),
        };

        self.executor.execute_direct(physical_plan, parameters)
    }

    pub fn execute_query(
        &self,
        logical_plan: Arc<LogicalPlan>,
    ) -> error::Result<Box<dyn Iterator<Item = error::Result<Document>>>> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("execute_query").entered(),
        };
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
        let physical_plan =
            self.optimizer
                .optimize(logical_plan, catalog, self.storage_engine.as_ref());
        (parameters, physical_plan)
    }
}

impl Drop for DbImpl {
    fn drop(&mut self) {
        let _instance_span = self.observability.instance_span().clone().entered();
        if let Err(e) = self.storage_engine.shutdown() {
            tracing::warn!(event = "db.shutdown_failed", error = %e);
        } else {
            tracing::debug!(event = "db.shutdown_completed");
        }
    }
}
