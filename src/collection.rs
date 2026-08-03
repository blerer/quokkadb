use crate::error::Error;
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

#[derive(Debug, Clone, PartialEq)]
pub struct InsertManyResult {
    pub inserted_ids: Vec<Bson>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
    pub upserted_id: Option<Bson>,
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

    fn extract_u64_field(result: &Document, field: &str) -> Result<u64> {
        let value = result.get(field).ok_or_else(|| {
            Error::UnexpectedError(format!(
                "Write result did not contain expected field '{}'",
                field
            ))
        })?;

        match value {
            Bson::Int32(value) if *value >= 0 => Ok(*value as u64),
            Bson::Int64(value) if *value >= 0 => Ok(*value as u64),
            _ => Err(Error::UnexpectedError(format!(
                "Write result field '{}' had unexpected value {:?}",
                field, value
            ))),
        }
    }

    fn parse_insert_one_result(result: Document) -> Result<InsertOneResult> {
        let inserted_id = result.get("inserted_id").cloned().ok_or_else(|| {
            Error::UnexpectedError(
                "InsertOne result did not contain expected field 'inserted_id'".to_string(),
            )
        })?;

        Ok(InsertOneResult { inserted_id })
    }

    fn parse_insert_many_result(result: Document) -> Result<InsertManyResult> {
        let inserted_ids = result
            .get_array("inserted_ids")
            .map_err(|_| {
                Error::UnexpectedError(
                    "InsertMany result did not contain expected field 'inserted_ids'".to_string(),
                )
            })?
            .to_vec();

        Ok(InsertManyResult { inserted_ids })
    }

    fn parse_update_result(result: Document) -> Result<UpdateResult> {
        let matched_count = Self::extract_u64_field(&result, "matched_count")?;
        let modified_count = Self::extract_u64_field(&result, "modified_count")?;
        let upserted_id = result.get("upserted_id").cloned();

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id,
        })
    }

    /// Creates an index on the collection with the specified keys and options.
    /// # Arguments
    /// * `keys` - The keys for the index, specified as a BSON document.
    /// * `options` - Options for creating the index, such as the index name.
    /// Returns a `Result` containing the name of the created index or an error.
    pub fn create_index_with_options(
        &self,
        keys: Document,
        options: CreateIndexOptions,
    ) -> Result<String> {
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

        Self::parse_insert_one_result(self.db_impl.execute_write(plan)?)
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

        Self::parse_insert_many_result(self.db_impl.execute_write(plan)?)
    }

    /// Updates a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// * `options` - Options for the update operation.
    /// Returns a `Result` containing update metadata or an error.
    pub fn update_one(
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

        let plan = LogicalPlan::UpdateOne {
            collection: collection_id,
            query,
            update,
            upsert: options.upsert,
        };

        Self::parse_update_result(self.db_impl.execute_write(plan)?)
    }

    /// Updates multiple documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// * `options` - Options for the update operation.
    /// Returns a `Result` containing update metadata or an error.
    pub fn update_many(
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

        Self::parse_update_result(self.db_impl.execute_write(plan)?)
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

/// Options for update operations.
#[derive(Default)]
pub struct UpdateOptions {
    /// Optional array filters for updating elements in arrays.
    pub array_filters: Option<Vec<Document>>,
    /// Whether to perform an upsert if no documents match the query.
    pub upsert: bool,
}

/// Builder for UpdateOptions.
pub struct UpdateOptionsBuilder {
    array_filters: Option<Vec<Document>>,
    upsert: bool,
}

impl UpdateOptionsBuilder {
    pub fn new() -> Self {
        UpdateOptionsBuilder {
            array_filters: None,
            upsert: false,
        }
    }

    /// Sets the array filters for the update operation.
    /// These filters specify which elements in an array should be updated.
    /// # Arguments
    /// * `filters` - A vector of documents representing the array filters.
    /// Returns the builder instance for chaining.
    pub fn array_filters(mut self, filters: Vec<Document>) -> Self {
        self.array_filters = Some(filters);
        self
    }

    /// Sets whether to perform an upsert if no documents match the query.
    /// # Arguments
    /// * `upsert` - A boolean indicating whether to perform an upsert.
    /// Returns the builder instance for chaining.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.upsert = upsert;
        self
    }

    /// Builds the UpdateOptions instance.
    /// Returns the constructed UpdateOptions.
    pub fn build(self) -> UpdateOptions {
        UpdateOptions {
            array_filters: self.array_filters,
            upsert: self.upsert,
        }
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
pub struct CreateCollectionOptions {
    /// Strategy for creating document `_id` fields.
    pub id_creation_strategy: IdCreationStrategy,
}

impl CreateCollectionOptions {
    /// Creates a new builder for `CreateCollectionOptions`.
    pub fn builder() -> CreateCollectionOptionsBuilder {
        CreateCollectionOptionsBuilder::new()
    }
}

impl From<CreateCollectionOptions> for InternalCollectionOptions {
    fn from(value: CreateCollectionOptions) -> Self {
        InternalCollectionOptions {
            id_creation_strategy: value.id_creation_strategy.into(),
        }
    }
}

/// Builder for `CreateCollectionOptions`.
pub struct CreateCollectionOptionsBuilder {
    id_creation_strategy: IdCreationStrategy,
}

impl CreateCollectionOptionsBuilder {
    /// Creates a new builder with default options.
    pub fn new() -> Self {
        Self {
            id_creation_strategy: IdCreationStrategy::default(),
        }
    }

    /// Sets the ID creation strategy for the collection.
    pub fn id_creation_strategy(mut self, strategy: IdCreationStrategy) -> Self {
        self.id_creation_strategy = strategy;
        self
    }

    /// Builds the `CreateCollectionOptions`.
    pub fn build(self) -> CreateCollectionOptions {
        CreateCollectionOptions {
            id_creation_strategy: self.id_creation_strategy,
        }
    }
}

impl Default for CreateCollectionOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Options for creating an index.
#[derive(Debug, Clone, Default)]
pub struct CreateIndexOptions {
    /// The name of the index
    pub name: Option<String>,
}

impl CreateIndexOptions {
    /// Creates a new builder for `CreateIndexOptions`.
    pub fn builder() -> CreateIndexOptionsBuilder {
        CreateIndexOptionsBuilder::new()
    }
}

impl From<CreateIndexOptions> for IndexOptions {
    fn from(value: CreateIndexOptions) -> Self {
        IndexOptions { name: value.name }
    }
}

/// Builder for `CreateIndexOptions`.
pub struct CreateIndexOptionsBuilder {
    name: Option<String>,
}

impl CreateIndexOptionsBuilder {
    /// Creates a new builder with default options.
    pub fn new() -> Self {
        Self { name: None }
    }

    /// Sets the name of the index.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Builds the `CreateIndexOptions`.
    pub fn build(self) -> CreateIndexOptions {
        CreateIndexOptions { name: self.name }
    }
}

impl Default for CreateIndexOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
