use crate::error::Error;
use crate::query::execution::WriteResult;
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::parser;
use crate::storage::catalog::{
    CollectionOptions as InternalCollectionOptions,
    IdCreationStrategy as InternalIdCreationStrategy, IndexDefinition,
    IndexDirection as InternalIndexDirection, IndexOptions, OrderedIndexField,
};
use crate::DbImpl;
use bson::{serialize_to_vec, Bson, Document};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum CollectionPolicy {
    #[default]
    Strict,
    CreateIfMissing,
}

/// Represents a collection in the database.
/// Provides methods to perform CRUD operations on the collection.
pub struct Collection {
    db_impl: Arc<DbImpl>,
    collection: String,
    policy: CollectionPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexDirection {
    Ascending,
    Descending,
}

impl From<InternalIndexDirection> for IndexDirection {
    fn from(value: InternalIndexDirection) -> Self {
        match value {
            InternalIndexDirection::Ascending => IndexDirection::Ascending,
            InternalIndexDirection::Descending => IndexDirection::Descending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexFieldInfo {
    pub path: String,
    pub direction: IndexDirection,
}

impl From<&OrderedIndexField> for IndexFieldInfo {
    fn from(value: &OrderedIndexField) -> Self {
        Self {
            path: value.path.to_string(),
            direction: value.direction.clone().into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub id: u32,
    pub name: String,
    pub fields: Vec<IndexFieldInfo>,
}

impl IndexInfo {
    fn from_definition(id: u32, name: String, definition: &IndexDefinition) -> Self {
        let fields = match definition {
            IndexDefinition::Regular(fields) => fields.iter().map(IndexFieldInfo::from).collect(),
        };

        Self { id, name, fields }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertOneResult {
    pub inserted_id: Bson,
}

impl InsertOneResult {
    fn from_write_result(result: WriteResult) -> Self {
        match result {
            WriteResult::InsertOne { inserted_id } => Self { inserted_id },
            other => panic!("expected InsertOne write result, got {other:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertManyResult {
    pub inserted_ids: Vec<Bson>,
}

impl InsertManyResult {
    fn from_write_result(result: WriteResult) -> Self {
        match result {
            WriteResult::InsertMany { inserted_ids } => Self { inserted_ids },
            other => panic!("expected InsertMany write result, got {other:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
    pub upserted_id: Option<Bson>,
}

impl UpdateResult {
    fn from_write_result(result: WriteResult) -> Self {
        match result {
            WriteResult::Update {
                matched_count,
                modified_count,
                upserted_id,
            } => Self {
                matched_count,
                modified_count,
                upserted_id,
            },
            other => panic!("expected Update write result, got {other:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

impl DeleteResult {
    fn from_write_result(result: WriteResult) -> Self {
        match result {
            WriteResult::Delete { deleted_count } => Self { deleted_count },
            other => panic!("expected Delete write result, got {other:?}"),
        }
    }
}

impl Collection {
    pub(crate) fn new(db_impl: Arc<DbImpl>, collection: String) -> Collection {
        Collection {
            db_impl,
            collection,
            policy: CollectionPolicy::Strict,
        }
    }

    /// Returns a collection handle that will create the collection on first write.
    /// Queries against a missing collection return an empty result set.
    pub fn create_if_missing(mut self) -> Self {
        self.policy = CollectionPolicy::CreateIfMissing;
        self
    }

    fn get_collection_metadata(&self) -> Result<Arc<crate::storage::catalog::CollectionMetadata>> {
        self.db_impl
            .get_collection(&self.collection)
            .ok_or_else(|| Error::CollectionNotFound {
                name: self.collection.clone(),
                id: None,
            })
    }

    fn collection_id_for_write(&self) -> Result<u32> {
        match self.policy {
            CollectionPolicy::Strict => Ok(self.get_collection_metadata()?.id),
            CollectionPolicy::CreateIfMissing => self
                .db_impl
                .create_collection_if_not_exists(&self.collection),
        }
    }

    /// Creates an index on the collection with the specified keys.
    /// # Arguments
    /// * `keys` - The keys for the index, specified as a BSON document.
    /// Returns a `Result` containing the name of the created index or an error.
    pub fn create_index(&self, keys: Document) -> Result<String> {
        self.execute_create_index(keys, CreateIndexOptions::default())
    }

    /// Creates an index builder for the collection with the specified keys.
    pub fn create_index_with(&self, keys: Document) -> CreateIndex<'_> {
        CreateIndex::new(self, keys)
    }

    fn execute_create_index(&self, keys: Document, options: CreateIndexOptions) -> Result<String> {
        let collection_id = self.collection_id_for_write()?;
        let spec = parser::parse_index_keys(&keys)?;
        self.db_impl.create_index(collection_id, spec, options)
    }

    /// Returns the active indexes for the collection.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        let collection = match self.policy {
            CollectionPolicy::Strict => Some(self.get_collection_metadata()?),
            CollectionPolicy::CreateIfMissing => self.db_impl.get_collection(&self.collection),
        };

        let Some(collection) = collection else {
            return Ok(Vec::new());
        };

        Ok(collection
            .active_indexes()
            .into_iter()
            .map(|index| IndexInfo::from_definition(index.id, index.name(), &index.definition))
            .collect())
    }

    /// Drops an index from the collection by its name.
    /// # Arguments
    /// * `name` - The name of the index to drop.
    /// Returns a `Result` indicating success or failure.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        let collection = self.get_collection_metadata()?;
        let index = collection
            .get_index_by_name(name)
            .ok_or_else(|| Error::IndexNotFound {
                collection_name: self.collection.clone(),
                index_name: name.to_string(),
                id: None,
            })?;

        self.db_impl.drop_index(collection.id, index.id)
    }

    /// Drops this collection.
    pub fn drop_collection(&self) -> Result<()> {
        self.db_impl.drop_collection(&self.collection)
    }

    /// Renames this collection and returns a handle for the new name.
    pub fn rename(&self, new_name: &str) -> Result<Collection> {
        self.db_impl.rename_collection(&self.collection, new_name)?;
        Ok(Collection {
            db_impl: self.db_impl.clone(),
            collection: new_name.to_string(),
            policy: self.policy,
        })
    }

    /// Returns the estimated number of documents in the collection based on storage count stats.
    pub fn estimated_document_count(&self) -> Result<u64> {
        let collection_id = match self.policy {
            CollectionPolicy::Strict => self.get_collection_metadata()?.id,
            CollectionPolicy::CreateIfMissing => {
                match self.db_impl.get_collection(&self.collection) {
                    Some(collection) => collection.id,
                    None => return Ok(0),
                }
            }
        };

        self.db_impl.estimated_document_count(collection_id)
    }

    /// Inserts a single document into the collection.
    /// # Arguments
    /// * `document` - The document to insert, which must implement the `Serialize` trait.
    /// Returns a `Result` containing the inserted document id or an error.
    /// # Example
    /// let doc = doc! { "name": "Alice", "age": 30 };
    /// collection.insert_one(doc)?;
    pub fn insert_one(&self, document: impl Serialize) -> Result<InsertOneResult> {
        let collection_id = self.collection_id_for_write()?;

        let plan = LogicalPlan::InsertOne {
            collection: collection_id,
            document: serialize_to_vec(&document)?,
        };

        Ok(InsertOneResult::from_write_result(
            self.db_impl.execute_write(plan)?,
        ))
    }

    /// Inserts multiple documents into the collection.
    /// # Arguments
    /// * `documents` - An iterable collection of documents to insert, each implementing the `Serialize` trait.
    /// Returns a `Result` containing the inserted document ids or an error.
    pub fn insert_many(
        &self,
        documents: impl IntoIterator<Item = impl Serialize>,
    ) -> Result<InsertManyResult> {
        let collection_id = self.collection_id_for_write()?;

        let mut serialized = Vec::new();
        for doc in documents {
            serialized.push(serialize_to_vec(&doc)?);
        }

        let plan = LogicalPlan::InsertMany {
            collection: collection_id,
            documents: serialized,
        };

        Ok(InsertManyResult::from_write_result(
            self.db_impl.execute_write(plan)?,
        ))
    }

    /// Updates a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// Returns a `Result` containing update metadata or an error.
    pub fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.execute_update_one(filter, update, UpdateOptions::default())
    }

    /// Creates an update operation builder for updating a single matching document.
    pub fn update_one_with(&self, filter: Document, update: Document) -> UpdateOne<'_> {
        UpdateOne::new(self, filter, update)
    }

    /// Updates multiple documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// Returns a `Result` containing update metadata or an error.
    pub fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.execute_update_many(filter, update, UpdateOptions::default())
    }

    /// Creates an update operation builder for updating all matching documents.
    pub fn update_many_with(&self, filter: Document, update: Document) -> UpdateMany<'_> {
        UpdateMany::new(self, filter, update)
    }

    fn execute_update_one(
        &self,
        filter: Document,
        update: Document,
        options: UpdateOptions,
    ) -> Result<UpdateResult> {
        let collection_id = self.collection_id_for_write()?;

        let conditions = parser::parse_conditions(&filter)?;
        let mut builder = LogicalPlanBuilder::scan(collection_id).filter(conditions);
        if let Some(sort) = &options.sort {
            let sort = parser::parse_sort(sort)?;
            builder = builder.sort(Arc::new(sort));
        }
        let query = builder.limit(None, Some(1)).build();
        let update = parser::parse_update(&update, options.array_filters)?;

        let plan = LogicalPlan::UpdateOne {
            collection: collection_id,
            query,
            update,
            upsert: options.upsert,
        };

        Ok(UpdateResult::from_write_result(
            self.db_impl.execute_write(plan)?,
        ))
    }

    fn execute_update_many(
        &self,
        filter: Document,
        update: Document,
        options: UpdateOptions,
    ) -> Result<UpdateResult> {
        let collection_id = self.collection_id_for_write()?;

        let conditions = parser::parse_conditions(&filter)?;
        let query = LogicalPlanBuilder::scan(collection_id)
            .filter(conditions)
            .build();
        let update = parser::parse_update(&update, options.array_filters)?;

        let plan = LogicalPlan::UpdateMany {
            collection: collection_id,
            query,
            update,
            upsert: options.upsert,
        };

        Ok(UpdateResult::from_write_result(
            self.db_impl.execute_write(plan)?,
        ))
    }

    /// Deletes a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to delete.
    /// Returns a `Result` containing delete metadata or an error.
    pub fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        let collection_id = match self.policy {
            CollectionPolicy::Strict => self.get_collection_metadata()?.id,
            CollectionPolicy::CreateIfMissing => {
                match self.db_impl.get_collection(&self.collection) {
                    Some(collection) => collection.id,
                    None => return Ok(DeleteResult { deleted_count: 0 }),
                }
            }
        };

        let conditions = parser::parse_conditions(&filter)?;
        let query = LogicalPlanBuilder::scan(collection_id)
            .filter(conditions)
            .build();

        let plan = LogicalPlan::DeleteOne {
            collection: collection_id,
            query,
        };

        Ok(DeleteResult::from_write_result(
            self.db_impl.execute_write(plan)?,
        ))
    }

    /// Creates a query to find documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents.
    /// Returns a `Query` object that can be further modified and executed.
    pub fn find(&self, filter: Document) -> Query {
        Query::new(
            self.db_impl.clone(),
            self.collection.clone(),
            self.policy,
            filter,
        )
    }
}

#[derive(Default)]
struct UpdateOptions {
    array_filters: Option<Vec<Document>>,
    upsert: bool,
    sort: Option<Document>,
}

pub struct UpdateOne<'a> {
    collection: &'a Collection,
    filter: Document,
    update: Document,
    options: UpdateOptions,
}

impl<'a> UpdateOne<'a> {
    fn new(collection: &'a Collection, filter: Document, update: Document) -> Self {
        Self {
            collection,
            filter,
            update,
            options: UpdateOptions::default(),
        }
    }

    /// Sets the array filters for the update operation.
    /// These filters specify which elements in an array should be updated.
    pub fn array_filters(mut self, filters: Vec<Document>) -> Self {
        self.options.array_filters = Some(filters);
        self
    }

    /// Sets whether to perform an upsert if no documents match the query.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Sets the sort order used to choose which matching document to update.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Executes the update operation.
    pub fn execute(self) -> Result<UpdateResult> {
        self.collection
            .execute_update_one(self.filter, self.update, self.options)
    }
}

pub struct UpdateMany<'a> {
    collection: &'a Collection,
    filter: Document,
    update: Document,
    options: UpdateOptions,
}

impl<'a> UpdateMany<'a> {
    fn new(collection: &'a Collection, filter: Document, update: Document) -> Self {
        Self {
            collection,
            filter,
            update,
            options: UpdateOptions::default(),
        }
    }

    /// Sets the array filters for the update operation.
    /// These filters specify which elements in an array should be updated.
    pub fn array_filters(mut self, filters: Vec<Document>) -> Self {
        self.options.array_filters = Some(filters);
        self
    }

    /// Sets whether to perform an upsert if no documents match the query.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Executes the update operation.
    pub fn execute(self) -> Result<UpdateResult> {
        self.collection
            .execute_update_many(self.filter, self.update, self.options)
    }
}

/// Represents a query on a collection.
/// Provides methods to set query parameters and execute the query.
pub struct Query {
    db_impl: Arc<DbImpl>,
    collection: String,
    policy: CollectionPolicy,
    filter: Document, // Unified filter representation using Expr
    projection: Option<Document>,
    sort: Option<Document>,
    limit: Option<usize>,
    skip: Option<usize>,
}

impl Query {
    fn new(
        db_impl: Arc<DbImpl>,
        collection: String,
        policy: CollectionPolicy,
        filter: Document,
    ) -> Query {
        Query {
            db_impl,
            collection,
            policy,
            filter,
            projection: None,
            sort: None,
            limit: None,
            skip: None,
        }
    }

    /// Sets the projection for the query.
    /// # Arguments
    /// * `projection` - The projection document specifying which fields to include or exclude.
    /// Returns the modified Query instance for chaining.
    pub fn projection(mut self, projection: Document) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Sets the sort order for the query.
    /// # Arguments
    /// * `sort` - The sort document specifying the fields and their sort order.
    /// Returns the modified Query instance for chaining.
    pub fn sort(mut self, sort: Document) -> Self {
        self.sort = Some(sort);
        self
    }

    /// Sets the limit for the number of documents to return.
    /// # Arguments
    /// * `limit` - The maximum number of documents to return.
    /// Returns the modified Query instance for chaining.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the number of documents to skip.
    /// # Arguments
    /// * `value` - The number of documents to skip.
    /// Returns the modified Query instance for chaining.
    pub fn skip(mut self, value: usize) -> Self {
        self.skip = Some(value);
        self
    }

    /// Executes the query and returns an iterator over the resulting documents.
    /// Returns a `Result` containing an iterator of documents or an error.
    pub fn execute(&self) -> Result<Box<dyn Iterator<Item = Result<Document>>>> {
        let collection_id = match self.policy {
            CollectionPolicy::Strict => {
                let collection =
                    self.db_impl
                        .get_collection(&self.collection)
                        .ok_or_else(|| Error::CollectionNotFound {
                            name: self.collection.clone(),
                            id: None,
                        })?;
                collection.id
            }
            CollectionPolicy::CreateIfMissing => {
                let collection = self.db_impl.get_collection(&self.collection);
                match collection {
                    Some(collection) => collection.id,
                    None => return Ok(Box::new(std::iter::empty())),
                }
            }
        };

        let conditions = parser::parse_conditions(&self.filter)?;

        let mut builder = LogicalPlanBuilder::scan(collection_id).filter(conditions);

        if let Some(projection) = &self.projection {
            let projection = parser::parse_projection(&projection)?;
            builder = builder.project(Arc::new(projection));
        }

        if let Some(sort) = &self.sort {
            let sort = parser::parse_sort(&sort)?;
            builder = builder.sort(Arc::new(sort));
        }

        if self.limit.is_some() || self.skip.is_some() {
            builder = builder.limit(self.skip, self.limit);
        }

        self.db_impl.execute_query(builder.build())
    }
}

/// Strategy for generating document `_id` fields in a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdCreationStrategy {
    /// IDs are auto-generated by the system.
    Generated,
    /// IDs must be provided manually by the user.
    Manual,
    /// A mix of auto-generated and manual IDs (default).
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

pub struct CreateIndex<'a> {
    collection: &'a Collection,
    keys: Document,
    options: CreateIndexOptions,
}

impl<'a> CreateIndex<'a> {
    fn new(collection: &'a Collection, keys: Document) -> Self {
        Self {
            collection,
            keys,
            options: CreateIndexOptions::default(),
        }
    }

    /// Sets the name of the index.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.options.name = Some(name.into());
        self
    }

    /// Executes the index creation operation.
    pub fn execute(self) -> Result<String> {
        self.collection
            .execute_create_index(self.keys, self.options)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
