use crate::error::Error;
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::parser;
use bson::{to_vec, Document};
use std::sync::Arc;
use serde::Serialize;
use crate::DbImpl;

/// Represents a collection in the database.
/// Provides methods to perform CRUD operations on the collection.
pub struct Collection {
    db_impl: Arc<DbImpl>,
    collection: String,
}

impl Collection {
    pub(crate) fn new(db_impl: Arc<DbImpl>, collection: String) -> Collection {
        Collection { db_impl, collection }
    }

    /// Inserts a single document into the collection.
    /// # Arguments
    /// * `document` - The document to insert, which must implement the `Serialize` trait.
    /// Returns a `Result` containing the inserted document or an error.
    /// # Example
    /// let doc = doc! { "name": "Alice", "age": 30 };
    /// collection.insert_one(doc)?;
    pub fn insert_one(
        &self,
        document: impl Serialize,
    ) -> Result<Document> {

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let plan = LogicalPlan::InsertOne {
            collection: collection_id,
            document: to_vec(&document)?,
        };

        self.db_impl.execute_write(plan)
    }

    /// Inserts multiple documents into the collection.
    /// # Arguments
    /// * `documents` - An iterable collection of documents to insert, each implementing the `Serialize` trait.
    /// Returns a `Result` containing the inserted documents or an error.
    pub fn insert_many(
        &self,
        documents: impl IntoIterator<Item = impl Serialize>,
    ) -> Result<Document> {

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let mut serialized = Vec::new();
        for doc in documents {
            serialized.push(to_vec(&doc)?);
        }

        let plan = LogicalPlan::InsertMany {
            collection: collection_id,
            documents: serialized,
        };

        self.db_impl.execute_write(plan)
    }

    /// Updates a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// * `options` - Options for the update operation.
    /// Returns a `Result` containing the updated document or an error.
    pub fn update_one(&self,
                      filter: Document,
                      update: Document,
                      options: UpdateOptions
    ) -> Result<Document> {

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let conditions = parser::parse_conditions(&filter)?;
        let query = LogicalPlanBuilder::scan(collection_id).filter(conditions).build();
        let update = parser::parse_update(&update, options.array_filters)?;

        let plan = LogicalPlan::UpdateOne {
            collection: collection_id,
            query,
            update,
            upsert: options.upsert,
        };

        self.db_impl.execute_write(plan)
    }

    /// Updates multiple documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// * `options` - Options for the update operation.
    /// Returns a `Result` containing the updated documents or an error.
    pub fn update_many(&self,
                       filter: Document,
                       update: Document,
                       options: UpdateOptions
    ) -> Result<Document> {

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let conditions = parser::parse_conditions(&filter)?;
        let query = LogicalPlanBuilder::scan(collection_id).filter(conditions).build();
        let update = parser::parse_update(&update, options.array_filters)?;

        let plan = LogicalPlan::UpdateMany {
            collection: collection_id,
            query,
            update,
            upsert: options.upsert,
        };

        self.db_impl.execute_write(plan)
    }

    /// Creates a query to find documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents.
    /// Returns a `Query` object that can be further modified and executed.
    pub fn find(&self, filter: Document) -> Query {
        Query::new(self.db_impl.clone(), self.collection.clone(), filter)
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
    filter: Document, // Unified filter representation using Expr
    projection: Option<Document>,
    sort: Option<Document>,
    limit: Option<usize>,
    skip: Option<usize>,
}

impl Query {
    fn new(db_impl: Arc<DbImpl>, collection: String, filter: Document) -> Query {
        Query {
            db_impl,
            collection,
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
    pub fn projection(&mut self, projection: Document) -> &mut Self {
        self.projection = Some(projection);
        self
    }

    /// Sets the sort order for the query.
    /// # Arguments
    /// * `sort` - The sort document specifying the fields and their sort order.
    /// Returns the modified Query instance for chaining.
    pub fn sort(&mut self, sort: Document) -> &mut Self {
        self.sort = Some(sort);
        self
    }

    /// Sets the limit for the number of documents to return.
    /// # Arguments
    /// * `limit` - The maximum number of documents to return.
    /// Returns the modified Query instance for chaining.
    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Sets the number of documents to skip.
    /// # Arguments
    /// * `value` - The number of documents to skip.
    /// Returns the modified Query instance for chaining.
    pub fn skip(&mut self, value: usize) -> &mut Self {
        self.skip = Some(value);
        self
    }

    /// Executes the query and returns an iterator over the resulting documents.
    /// Returns a `Result` containing an iterator of documents or an error.
    pub fn execute(&self) -> Result<Box<dyn Iterator<Item=Result<Document>>>> {

        let collection_id = self.db_impl.get_collection_id(&self.collection);

        if collection_id.is_none() {
            return Ok(Box::new(std::iter::empty()));
        }

        let conditions = parser::parse_conditions(&self.filter)?;

        let mut builder = LogicalPlanBuilder::scan(collection_id.unwrap()).filter(conditions);

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

pub type Result<T> = std::result::Result<T, Error>;
