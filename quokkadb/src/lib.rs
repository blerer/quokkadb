#![deny(missing_docs)]
#![doc = r#"
QuokkaDB is an embedded application database for Rust applications.

Use [`QuokkaDB`] to open a database directory, then choose the typed API when
documents have a Rust model or the [`Collection`] API when the document shape
is dynamic. The typed API is usually the best starting point:

```rust,no_run
use quokkadb::{QuokkaDB, QuokkaDocument, QuokkaId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: QuokkaId,
    name: String,
}

# fn main() -> quokkadb::Result<()> {
let db = QuokkaDB::open("./my-app-data")?;
let users = db.typed_collection::<User>("users").create_if_missing();
users.insert_one(User {
    id: QuokkaId::new(),
    name: "Ada".to_owned(),
})?;
let ada = users.find_one(|user| user.name.eq("Ada"))?;
assert!(ada.is_some());
# Ok(())
# }
```

The database is persistent and does not require a separate server. See the
[getting started guide](https://github.com/blerer/quokkadb/blob/main/docs/getting-started.md)
for a complete application workflow, and the
[API reference](https://github.com/blerer/quokkadb/blob/main/docs/api-reference.md)
for the document and typed APIs.
"#]

extern crate core;
extern crate self as quokkadb;

/// BSON document collections and the document-oriented CRUD API.
pub mod collection;
mod collection_state;
/// Typed field expressions, filters, updates, and model traits.
mod document;
/// Errors returned by database, collection, and query operations.
mod error;
pub mod explain;
mod id;
mod index_builder;
mod io;
pub mod metrics;
/// Observability support used by the database and its metrics facade.
mod obs;
/// Database durability, memory, and storage configuration.
pub mod options;
mod query;
mod storage;
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
mod testing;
/// Typed model fields, expressions, and collection operations.
pub mod typed;
/// Typed collection handles and query/write builders.
mod typed_collection;
mod util;

pub use crate::document::{
    Filter, Index, QuokkaDocument, QuokkaScalar, QuokkaType, Sort, Update, not,
};
pub use crate::error::{Error, Result};
pub use crate::id::QuokkaId;
pub use crate::options::{CompressorType, Options, StorageQuantity, StorageUnit, WalDurability};
pub use crate::query::ReturnDocument;
pub use quokkadb_derive::{QuokkaDocument, QuokkaScalar, QuokkaType};

pub use crate::collection::Collection;
use crate::index_builder::IndexBuilder;
use crate::metrics::Metrics;
use crate::obs::metrics::MetricRegistry;
use crate::obs::observability::Observability;
use crate::query::execution::WriteResult;
use crate::query::optimizer::optimizer::Optimizer;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::query_cache::QueryCache;
use crate::query::{IndexKeySpec, Parameters};
use crate::storage::catalog::CollectionMetadata;
use crate::storage::catalog::{
    CollectionOptions as InternalCollectionOptions,
    IdCreationStrategy as InternalIdCreationStrategy, IndexOptions,
};
use crate::storage::count_stats::CountStatsKey;
use crate::storage::index_build_state::IndexBuildKey;
use crate::storage::storage_engine::{IndexBuildSnapshot, StorageEngine};
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub use crate::testing::TestControl;
pub use crate::typed::TypedCollection;

/// Support paths used by the derive macros.
#[doc(hidden)]
pub mod __private {
    pub use crate::document::{
        ObjectField, PartialObjectField, PartialQueryFieldType, QueryFieldType, TypedPath,
    };
    pub use crate::document::{QuokkaDocument, QuokkaScalar, QuokkaType};
}
use query::execution::QueryExecutor;
use query::logical_plan::LogicalPlan;
use std::path::Path;
use std::sync::Arc;
use tracing::debug_span;
use tracing::span::EnteredSpan;

#[derive(Clone)]
/// A handle to an opened QuokkaDB database.
///
/// Opening a path creates the database directory when necessary. Cloning a
/// handle is cheap; clones refer to the same database instance and may be used
/// by application threads.
pub struct QuokkaDB {
    options: Arc<Options>,
    db_impl: Arc<DbImpl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata describing a collection registered in a database.
pub struct CollectionInfo {
    /// Stable identifier assigned to the collection.
    pub id: u32,
    /// Collection name.
    pub name: String,
    /// Creation timestamp stored by the catalog.
    pub created_at: u64,
    /// Policy used when inserted documents do or do not provide `_id`.
    pub id_creation_strategy: IdCreationStrategy,
}

impl QuokkaDB {
    /// Opens a database with [`Options::default`].
    ///
    /// The path identifies the directory owned by the application. Existing
    /// data is reopened; a missing directory is created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Opens a database with explicitly configured options.
    ///
    /// Options are validated before any database work begins. Use
    /// [`Options`] presets when only broad resource or
    /// durability choices are needed.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        options.validate()?;
        let path = path.as_ref();

        let options = Arc::new(options);
        let observability = Observability::new(path);
        let _instance_span = observability.instance_span().clone().entered();
        let mut metric_registry = MetricRegistry::new();
        let (storage_engine, pending_builds) =
            StorageEngine::new(&mut metric_registry, options.clone(), path)?;
        let index_builder =
            IndexBuilder::new(&mut metric_registry, &options, storage_engine.clone());
        index_builder.cleanup_stale_index_build_states(&pending_builds)?;
        let optimizer = Arc::new(Optimizer::new(&options));
        let query_cache = Arc::new(QueryCache::new(
            &mut metric_registry,
            options.query_cache_size().to_bytes() as u64,
        ));
        let executor = Arc::new(QueryExecutor::new_with_metrics(
            storage_engine.clone(),
            &mut metric_registry,
        ));
        let db_impl = Arc::new(DbImpl {
            observability,
            metrics: metric_registry.clone(),
            optimizer,
            query_cache,
            executor,
            index_builder,
            storage_engine,
        });
        Self::resume_pending_index_builds(&db_impl, pending_builds)?;

        tracing::debug!(event = "db.opened");

        Ok(QuokkaDB { options, db_impl })
    }

    fn resume_pending_index_builds(
        db_impl: &Arc<DbImpl>,
        pending_builds: Vec<IndexBuildSnapshot>,
    ) -> Result<()> {
        for build in pending_builds {
            db_impl
                .index_builder
                .build_index(build.key, &build.snapshot)?;
        }
        Ok(())
    }

    /// Returns the options selected when this database was opened.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Returns a handle for a BSON document collection.
    ///
    /// The handle is lazy. Use [`Collection::create_if_missing`] when writes
    /// should create a missing collection automatically.
    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(self.db_impl.clone(), name.to_string())
    }

    /// Returns a handle for a collection storing values of the typed model `T`.
    ///
    /// `T` must implement [`QuokkaDocument`], normally through the derive macro
    /// re-exported by this crate.
    pub fn typed_collection<T: QuokkaDocument>(&self, name: &str) -> TypedCollection<T> {
        TypedCollection::new(self.db_impl.clone(), name.to_string())
    }

    /// Returns read-only counters and histograms for this database instance.
    pub fn metrics(&self) -> Metrics<'_> {
        Metrics {
            registry: &self.db_impl.metrics,
        }
    }

    /// Creates a new collection with the given name and default options.
    /// Returns an error if a collection with the same name already exists.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        self.create_collection_with(name).execute()
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

/// Builder for creating a collection with collection-level options.
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
    pub fn execute(self) -> Result<()> {
        self.db
            .db_impl
            .create_collection_with_options(&self.name, self.options)?;
        Ok(())
    }
}

/// Strategy for generating document `_id` fields in a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdCreationStrategy {
    /// IDs are auto-generated by the system and supplied IDs are rejected.
    Generated,
    /// Every inserted document must provide an `_id` value.
    Manual,
    /// Accept supplied IDs and generate missing IDs (the default).
    #[default]
    Mixed,
}

impl From<IdCreationStrategy> for InternalIdCreationStrategy {
    fn from(value: IdCreationStrategy) -> Self {
        match value {
            IdCreationStrategy::Generated => InternalIdCreationStrategy::Generated,
            IdCreationStrategy::Manual => InternalIdCreationStrategy::Manual,
            IdCreationStrategy::Mixed => InternalIdCreationStrategy::Mixed,
        }
    }
}

impl From<InternalIdCreationStrategy> for IdCreationStrategy {
    fn from(value: InternalIdCreationStrategy) -> Self {
        match value {
            InternalIdCreationStrategy::Generated => IdCreationStrategy::Generated,
            InternalIdCreationStrategy::Manual => IdCreationStrategy::Manual,
            InternalIdCreationStrategy::Mixed => IdCreationStrategy::Mixed,
        }
    }
}

/// Options for creating a collection.
#[derive(Debug, Clone, Default)]
pub(crate) struct CreateCollectionOptions {
    pub(crate) id_creation_strategy: IdCreationStrategy,
}

impl From<CreateCollectionOptions> for InternalCollectionOptions {
    fn from(value: CreateCollectionOptions) -> Self {
        InternalCollectionOptions {
            id_creation_strategy: value.id_creation_strategy.into(),
        }
    }
}

/// Options for creating an index.
#[derive(Debug, Clone, Default)]
pub(crate) struct CreateIndexOptions {
    name: Option<String>,
}

impl From<CreateIndexOptions> for IndexOptions {
    fn from(value: CreateIndexOptions) -> Self {
        IndexOptions { name: value.name }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum CollectionPolicy {
    #[default]
    Strict,
    CreateIfMissing,
}

struct DbImpl {
    observability: Arc<Observability>,
    metrics: MetricRegistry,
    optimizer: Arc<Optimizer>,
    query_cache: Arc<QueryCache>,
    executor: Arc<QueryExecutor>,
    index_builder: IndexBuilder,
    storage_engine: Arc<StorageEngine>,
}

struct OperationSpans {
    _instance: EnteredSpan,
    _operation: EnteredSpan,
}

impl DbImpl {
    pub fn create_collection(
        self: &Arc<Self>,
        name: &str,
        if_not_exists: bool,
    ) -> Result<Arc<CollectionMetadata>> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("create_collection", collection = %name, if_not_exists)
                .entered(),
        };
        Ok(self.storage_engine.create_collection(name, if_not_exists)?)
    }

    pub fn create_collection_with_options(
        self: &Arc<Self>,
        name: &str,
        options: CreateCollectionOptions,
    ) -> Result<Arc<CollectionMetadata>> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("create_collection", collection = %name).entered(),
        };
        let self1 = &self.storage_engine;
        let options1 = options.into();
        Ok(self1.create_collection_with_options(name, options1, false)?)
    }

    pub fn get_collection(self: &Arc<Self>, name: &str) -> Option<Arc<CollectionMetadata>> {
        Some(self.storage_engine.catalog().get_collection_by_name(name)?)
    }

    pub fn drop_collection(self: &Arc<Self>, name: &str) -> Result<()> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("drop_collection", collection = %name).entered(),
        };
        self.storage_engine.drop_collection(name)?;
        Ok(())
    }

    pub fn rename_collection(self: &Arc<Self>, old_name: &str, new_name: &str) -> Result<()> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("rename_collection", collection = %old_name).entered(),
        };
        Ok(self.storage_engine.rename_collection(old_name, new_name)?)
    }

    pub fn create_index(
        self: &Arc<Self>,
        collection_id: u32,
        spec: IndexKeySpec,
        options: CreateIndexOptions,
    ) -> Result<String> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("create_index").entered(),
        };
        let created_index =
            self.storage_engine
                .create_index(collection_id, spec.into(), options.into())?;
        if let Some(build_snapshot) = created_index.build_snapshot {
            let key = IndexBuildKey::new(collection_id, created_index.id);
            self.index_builder.build_index(key, &build_snapshot)?;
        }
        Ok(created_index.name)
    }

    pub fn drop_index(self: &Arc<Self>, collection_id: u32, index_id: u32) -> Result<()> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("drop_index").entered(),
        };
        self.storage_engine.drop_index(collection_id, index_id)?;
        Ok(())
    }

    pub fn estimated_document_count(&self, collection_id: u32) -> Result<u64> {
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

    pub fn execute_write(
        &self,
        metadata: &CollectionMetadata,
        logical_plan: LogicalPlan,
        sync: bool,
    ) -> Result<WriteResult> {
        let _spans = OperationSpans {
            _instance: self.observability.instance_span().clone().entered(),
            _operation: debug_span!("execute_write", collection = %metadata.id).entered(),
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
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
                let (parameters, query) = self.optimize_query(metadata, query)?;
                (
                    PhysicalPlan::DeleteOne { collection, query },
                    Some(parameters),
                )
            }
            LogicalPlan::DeleteMany { collection, query } => {
                let (parameters, query) = self.optimize_query(metadata, query)?;
                (
                    PhysicalPlan::DeleteMany { collection, query },
                    Some(parameters),
                )
            }
            _ => panic!("Unsupported write operation {:?}", logical_plan),
        };

        self.executor
            .execute_direct(physical_plan, parameters, sync)
    }

    fn optimize_query(
        &self,
        metadata: &CollectionMetadata,
        logical_plan: Arc<LogicalPlan>,
    ) -> Result<(Parameters, Arc<PhysicalPlan>)> {
        let normalized_plan = self.optimizer.normalize(logical_plan);
        // Then, parametrize the plan to collect parameters
        let (logical_plan, parameters) = self.optimizer.parametrize(normalized_plan);

        let logical_plan_for_cache = logical_plan.clone();
        let physical_plan = self.query_cache.get_or_insert_with(
            metadata.id,
            metadata.version,
            logical_plan_for_cache,
            || {
                self.optimizer
                    .optimize(logical_plan, &metadata, self.storage_engine.as_ref())
            },
        )?;
        Ok((parameters, physical_plan))
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
