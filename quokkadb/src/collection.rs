//! BSON document collections and their CRUD/query builders.
//!
//! [`Collection`] provides direct access to BSON documents for applications
//! requiring dynamic schemas, compatibility with existing BSON integrations,
//! or queries that cannot be expressed through the typed API.
//!
//! For applications using Rust structs, prefer [`crate::typed::TypedCollection`],
//! which provides compile-time checked field expressions and a more ergonomic
//! interface.
//!
//! Most applications use the builder methods without naming the builder types
//! directly. The public builder and result types are available here for
//! callers that need explicit type annotations or advanced composition.
//!
//! ## Where to look
//!
//! - Use [`Collection::insert_one`] or [`Collection::insert_many`] to store
//!   serializable values, including BSON documents.
//! - Use [`Collection::find`] and [`crate::collection::Find`] for filtering,
//!   projections, sorts, limits, skips, hints, and query plans.
//! - Use [`Collection::update_one`], [`Collection::update_many`], or the
//!   corresponding builders for BSON update documents.
//! - Use [`Collection::create_index`] or [`crate::collection::CreateIndex`] for
//!   BSON-key indexes.
//! - Inspect operation metadata through [`crate::collection::InsertOneResult`],
//!   [`crate::collection::InsertManyResult`],
//!   [`crate::collection::UpdateResult`], and [`crate::collection::DeleteResult`].
//!
//! ## Query dynamic documents
//!
//! BSON documents can be inserted and queried directly. The query result is a
//! `Vec<bson::Document>` when collected, so callers can deserialize it into an
//! application type, pass it to another BSON-aware component, or inspect fields
//! dynamically.
//!
//! ```rust,no_run
//! use bson::doc;
//! use quokkadb::QuokkaDB;
//!
//! # fn main() -> quokkadb::Result<()> {
//! let db = QuokkaDB::open("./my-app-data")?;
//! let users = db.collection("users").create_if_missing();
//!
//! users.insert_one(doc! {
//!     "email": "ada@example.com",
//!     "name": "Ada",
//!     "active": true,
//! })?;
//!
//! let active_users = users
//!     .find(doc! { "active": true })
//!     .sort(doc! { "name": 1 })
//!     .limit(20)
//!     .execute_collect()?;
//! # let _ = active_users;
//! # Ok(())
//! # }
//! ```
//!
//! ## Compose writes, indexes, and projections
//!
//! Builder methods are useful when an operation needs options such as a named
//! index, an upsert, durability synchronization, or a projection. The BSON
//! shape stays explicit at each step:
//!
//! ```rust,no_run
//! use bson::doc;
//! use quokkadb::QuokkaDB;
//!
//! # fn main() -> quokkadb::Result<()> {
//! let db = QuokkaDB::open("./my-app-data")?;
//! let users = db.collection("users").create_if_missing();
//!
//! let index_name = users
//!     .create_index_with(doc! { "email": 1 })
//!     .name("users_by_email")
//!     .execute()?;
//!
//! let update = users
//!     .update_one_with(
//!         doc! { "email": "ada@example.com" },
//!         doc! { "$set": { "active": true } },
//!     )
//!     .upsert(true)
//!     .sync()
//!     .execute()?;
//!
//! let ada = users
//!     .find_one_with(doc! { "email": "ada@example.com" })
//!     .projection(doc! { "_id": 0, "email": 1, "active": 1 })
//!     .execute()?;
//! # let _ = (index_name, update, ada);
//! # Ok(())
//! # }
//! ```
//!
//! See the [document API reference](https://github.com/blerer/quokkadb/blob/main/docs/api-reference.md)
//! and the [find](https://github.com/blerer/quokkadb/blob/main/docs/guides/find-queries.md)
//! and [update](https://github.com/blerer/quokkadb/blob/main/docs/guides/update-data.md)
//! guides for the complete option set and behavior details.

use crate::collection_state::{CollectionState, Hint};
use crate::explain::{ExplainNode, ExplainOperator, ExplainPlan};
use crate::query::ReturnDocument;
use crate::query::execution::WriteResult;
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::parser;
use crate::storage::catalog::{
    IndexDefinition, IndexDirection as InternalIndexDirection, OrderedIndexField,
};
use crate::{CreateIndexOptions, DbImpl, Result};
use bson::{Bson, Document, serialize_to_vec};
use serde::Serialize;
use std::sync::Arc;

/// A lazy stream of BSON documents returned by a collection query.
pub type QueryOutput = Box<dyn Iterator<Item = Result<Document>>>;

/// Represents a collection in the database.
/// Provides methods to perform CRUD operations on the collection.
pub struct Collection {
    state: CollectionState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Direction in which an index is traversed.
pub enum IndexDirection {
    /// Traverses keys from the smallest value to the largest.
    Ascending,
    /// Traverses keys from the largest value to the smallest.
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
/// One field in an index definition.
pub struct IndexFieldInfo {
    /// Dot-separated BSON path indexed by this field.
    pub path: String,
    /// Ordering used for this field.
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
/// Metadata describing an active collection index.
pub struct IndexInfo {
    /// Stable identifier assigned to the index.
    pub id: u32,
    /// Index name accepted by query hints and index-management methods.
    pub name: String,
    /// Ordered fields that make up the index key.
    pub fields: Vec<IndexFieldInfo>,
}

impl IndexInfo {
    pub(crate) fn from_definition(id: u32, name: String, definition: &IndexDefinition) -> Self {
        let fields = match definition {
            IndexDefinition::Regular(fields) => fields.iter().map(IndexFieldInfo::from).collect(),
        };

        Self { id, name, fields }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Acknowledgement returned after inserting one BSON document.
pub struct InsertOneResult {
    /// The `_id` stored for the inserted document.
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
/// Acknowledgement returned after inserting multiple BSON documents.
pub struct InsertManyResult {
    /// The stored `_id` for each inserted document, in input order.
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
/// Counts returned by an update or replacement operation.
pub struct UpdateResult {
    /// Number of documents selected by the filter.
    pub matched_count: u64,
    /// Number of selected documents whose contents changed.
    pub modified_count: u64,
    /// ID of the newly inserted document when an upsert occurred.
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
/// Count returned by a delete operation.
pub struct DeleteResult {
    /// Number of documents removed.
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
    pub(crate) fn new(db: Arc<DbImpl>, name: String) -> Collection {
        Collection {
            state: CollectionState::new(db, name),
        }
    }

    /// Returns a collection handle that will create the collection on first write.
    /// Queries against a missing collection return an empty result set.
    pub fn create_if_missing(mut self) -> Self {
        self.state.create_if_missing();
        self
    }

    /// Creates an index on the collection with the specified keys.
    /// # Arguments
    /// * `keys` - The keys for the index, specified as a BSON document.
    /// Returns a `Result` containing the name of the created index or an error.
    pub fn create_index(&self, keys: Document) -> Result<String> {
        CreateIndex::new(&self.state, keys).execute()
    }

    /// Creates an index builder for the collection with the specified keys.
    pub fn create_index_with(&self, keys: Document) -> CreateIndex<'_> {
        CreateIndex::new(&self.state, keys)
    }

    /// Returns the active indexes for the collection.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        self.state.list_indexes()
    }

    /// Drops an index from the collection by its name.
    /// # Arguments
    /// * `name` - The name of the index to drop.
    /// Returns a `Result` indicating success or failure.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        self.state.drop_index(name)
    }

    /// Drops this collection.
    pub fn drop_collection(&self) -> Result<()> {
        self.state.drop_collection()
    }

    /// Renames this collection and returns a handle for the new name.
    pub fn rename(&self, new_name: &str) -> Result<Collection> {
        Ok(Collection {
            state: self.state.rename(new_name)?,
        })
    }

    /// Returns the estimated number of documents in the collection based on storage count stats.
    pub fn estimated_document_count(&self) -> Result<u64> {
        self.state.estimated_document_count()
    }

    /// Inserts a single document into the collection.
    /// # Arguments
    /// * `document` - The document to insert, which must implement the `Serialize` trait.
    /// Returns a `Result` containing the inserted document id or an error.
    /// # Example
    /// let doc = doc! { "name": "Alice", "age": 30 };
    /// collection.insert_one(doc)?;
    pub fn insert_one(&self, document: impl Serialize) -> Result<InsertOneResult> {
        self.insert_one_with(document)?.execute()
    }

    /// Creates an insert operation builder for inserting a single document.
    pub fn insert_one_with(&self, document: impl Serialize) -> Result<InsertOne<'_>> {
        InsertOne::new(&self.state, document)
    }

    /// Inserts multiple documents into the collection.
    /// # Arguments
    /// * `documents` - An iterable collection of documents to insert, each implementing the `Serialize` trait.
    /// Returns a `Result` containing the inserted document ids or an error.
    pub fn insert_many(
        &self,
        documents: impl IntoIterator<Item = impl Serialize>,
    ) -> Result<InsertManyResult> {
        self.insert_many_with(documents)?.execute()
    }

    /// Creates an insert operation builder for inserting multiple documents.
    pub fn insert_many_with(
        &self,
        documents: impl IntoIterator<Item = impl Serialize>,
    ) -> Result<InsertMany<'_>> {
        InsertMany::new(&self.state, documents)
    }

    /// Updates a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// Returns a `Result` containing update metadata or an error.
    pub fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.update_one_with(filter, update).execute()
    }

    /// Creates an update operation builder for updating a single matching document.
    pub fn update_one_with(&self, filter: Document, update: Document) -> UpdateOne<'_> {
        UpdateOne::new(&self.state, filter, update)
    }

    /// Updates multiple documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents to update.
    /// * `update` - The update document specifying the modifications to apply.
    /// Returns a `Result` containing update metadata or an error.
    pub fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.update_many_with(filter, update).execute()
    }

    /// Creates an update operation builder for updating all matching documents.
    pub fn update_many_with(&self, filter: Document, update: Document) -> UpdateMany<'_> {
        UpdateMany::new(&self.state, filter, update)
    }

    /// Deletes a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to delete.
    /// Returns a `Result` containing delete metadata or an error.
    pub fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        self.delete_one_with(filter).execute()
    }

    /// Creates a delete operation builder for deleting a single matching document.
    pub fn delete_one_with(&self, filter: Document) -> DeleteOne<'_> {
        DeleteOne::new(&self.state, filter)
    }

    /// Deletes all documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents to delete.
    /// Returns a `Result` containing delete metadata or an error.
    pub fn delete_many(&self, filter: Document) -> Result<DeleteResult> {
        self.delete_many_with(filter).execute()
    }

    /// Creates a delete operation builder for deleting all matching documents.
    pub fn delete_many_with(&self, filter: Document) -> DeleteMany<'_> {
        DeleteMany::new(&self.state, filter)
    }

    /// Finds a single document, deletes it, and returns the deleted document.
    pub fn find_one_and_delete(&self, filter: Document) -> Result<Option<Document>> {
        self.find_one_and_delete_with(filter).execute()
    }

    /// Creates a find-one-and-delete operation builder.
    pub fn find_one_and_delete_with(&self, filter: Document) -> FindOneAndDelete<'_> {
        FindOneAndDelete::new(&self.state, filter)
    }

    /// Creates a query to find documents in the collection that match the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the documents.
    /// Returns a `Find` object that can be further modified and executed.
    pub fn find(&self, filter: Document) -> Find<'_> {
        Find::new(&self.state, filter)
    }

    /// Finds a single document in the collection that matches the filter.
    pub fn find_one(&self, filter: Document) -> Result<Option<Document>> {
        self.find_one_with(filter).execute()
    }

    /// Creates a find-one operation builder for finding a single matching document.
    pub fn find_one_with(&self, filter: Document) -> FindOne<'_> {
        FindOne::new(&self.state, filter)
    }

    /// Finds a single document, updates it, and returns either the previous or updated document.
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: Document,
    ) -> Result<Option<Document>> {
        self.find_one_and_update_with(filter, update).execute()
    }

    /// Creates a find-one-and-update operation builder.
    pub fn find_one_and_update_with(
        &self,
        filter: Document,
        update: Document,
    ) -> FindOneAndUpdate<'_> {
        FindOneAndUpdate::new(&self.state, filter, update)
    }

    /// Replaces a single document in the collection that matches the filter.
    /// # Arguments
    /// * `filter` - The filter document to match the document to replace.
    /// * `replacement` - The replacement document.
    /// Returns a `Result` containing update metadata or an error.
    pub fn replace_one(&self, filter: Document, replacement: Document) -> Result<UpdateResult> {
        self.replace_one_with(filter, replacement).execute()
    }

    /// Creates a replace operation builder for replacing a single matching document.
    pub fn replace_one_with(&self, filter: Document, replacement: Document) -> ReplaceOne<'_> {
        ReplaceOne::new(&self.state, filter, replacement)
    }

    /// Finds a single document, replaces it, and returns either the previous or replacement document.
    pub fn find_one_and_replace(
        &self,
        filter: Document,
        replacement: Document,
    ) -> Result<Option<Document>> {
        self.find_one_and_replace_with(filter, replacement)
            .execute()
    }

    /// Creates a find-one-and-replace operation builder.
    pub fn find_one_and_replace_with(
        &self,
        filter: Document,
        replacement: Document,
    ) -> FindOneAndReplace<'_> {
        FindOneAndReplace::new(&self.state, filter, replacement)
    }
}

#[derive(Default)]
struct UpdateOptions {
    array_filters: Option<Vec<Document>>,
    sync: bool,
    upsert: bool,
    sort: Option<Document>,
    hint: Option<Hint>,
}

#[derive(Default)]
struct DeleteOptions {
    sync: bool,
    sort: Option<Document>,
    hint: Option<Hint>,
}

#[derive(Default)]
struct FindOneAndUpdateOptions {
    projection: Option<Document>,
    sync: bool,
    sort: Option<Document>,
    upsert: bool,
    return_document: ReturnDocument,
    hint: Option<Hint>,
}

#[derive(Default)]
struct FindOneAndDeleteOptions {
    projection: Option<Document>,
    sync: bool,
    sort: Option<Document>,
    hint: Option<Hint>,
}

#[derive(Default)]
struct ReplaceOneOptions {
    sync: bool,
    upsert: bool,
    sort: Option<Document>,
    hint: Option<Hint>,
}

#[derive(Default)]
struct FindOneAndReplaceOptions {
    projection: Option<Document>,
    sync: bool,
    sort: Option<Document>,
    upsert: bool,
    return_document: ReturnDocument,
    hint: Option<Hint>,
}

#[derive(Default)]
struct FindOptions {
    projection: Option<Document>,
    sort: Option<Document>,
    limit: Option<usize>,
    skip: Option<usize>,
    hint: Option<Hint>,
}

/// Builder for inserting one serialized document.
pub struct InsertOne<'a> {
    state: &'a CollectionState,
    document: Vec<u8>,
    sync: bool,
}

impl<'a> InsertOne<'a> {
    fn new(state: &'a CollectionState, document: impl Serialize) -> Result<Self> {
        Ok(Self {
            state,
            document: serialize_to_vec(&document)?,
            sync: false,
        })
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Executes the insert operation.
    pub fn execute(self) -> Result<InsertOneResult> {
        let build_plan = |collection, _hint_id| {
            Ok(LogicalPlan::InsertOne {
                collection,
                document: self.document,
            })
        };

        Ok(InsertOneResult::from_write_result(
            self.state.execute_write(None, build_plan, self.sync)?,
        ))
    }
}

/// Builder for inserting multiple serialized documents.
pub struct InsertMany<'a> {
    state: &'a CollectionState,
    documents: Vec<Vec<u8>>,
    sync: bool,
}

impl<'a> InsertMany<'a> {
    fn new(
        state: &'a CollectionState,
        documents: impl IntoIterator<Item = impl Serialize>,
    ) -> Result<Self> {
        let mut serialized = Vec::new();
        for doc in documents {
            serialized.push(serialize_to_vec(&doc)?);
        }

        Ok(Self {
            state,
            documents: serialized,
            sync: false,
        })
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Executes the insert operation.
    pub fn execute(self) -> Result<InsertManyResult> {
        let build_plan = |collection: u32, _hint_id| {
            Ok(LogicalPlan::InsertMany {
                collection,
                documents: self.documents,
            })
        };

        Ok(InsertManyResult::from_write_result(
            self.state.execute_write(None, build_plan, self.sync)?,
        ))
    }
}

/// Builder for updating the first matching document.
pub struct UpdateOne<'a> {
    state: &'a CollectionState,
    filter: Document,
    update: Document,
    options: UpdateOptions,
}

impl<'a> UpdateOne<'a> {
    fn new(state: &'a CollectionState, filter: Document, update: Document) -> Self {
        Self {
            state,
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

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Sets the sort order used to choose which matching document to update.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Executes the update operation.
    pub fn execute(self) -> Result<UpdateResult> {
        let build_plan = |collection, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;
            let update = parser::parse_update(&self.update, self.options.array_filters)?;
            update.validate_id_modifications(self.options.upsert)?;

            Ok(LogicalPlanBuilder::scan(collection, hint_id)
                .filter(conditions)
                .sort(sort)
                .update_one(update, self.options.upsert)
                .build())
        };

        Ok(UpdateResult::from_write_result(self.state.execute_write(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
        )?))
    }
}

/// Builder for updating every matching document.
pub struct UpdateMany<'a> {
    state: &'a CollectionState,
    filter: Document,
    update: Document,
    options: UpdateOptions,
}

impl<'a> UpdateMany<'a> {
    fn new(state: &'a CollectionState, filter: Document, update: Document) -> Self {
        Self {
            state,
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

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the update operation.
    pub fn execute(self) -> Result<UpdateResult> {
        let build_plan = |collection, hint_id| {
            let update = parser::parse_update(&self.update, self.options.array_filters)?;
            update.validate_id_modifications(self.options.upsert)?;
            let conditions = parser::parse_conditions(&self.filter)?;

            Ok(LogicalPlanBuilder::scan(collection, hint_id)
                .filter(conditions)
                .update_many(update, self.options.upsert)
                .build())
        };

        Ok(UpdateResult::from_write_result(self.state.execute_write(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
        )?))
    }
}

/// Builder for deleting the first matching document.
pub struct DeleteOne<'a> {
    state: &'a CollectionState,
    filter: Document,
    options: DeleteOptions,
}

impl<'a> DeleteOne<'a> {
    fn new(state: &'a CollectionState, filter: Document) -> Self {
        Self {
            state,
            filter,
            options: DeleteOptions::default(),
        }
    }

    /// Sets the sort order used to choose which matching document to delete.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the delete operation.
    pub fn execute(self) -> Result<DeleteResult> {
        let build_plan = |collection, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;

            Ok(LogicalPlanBuilder::scan(collection, hint_id)
                .filter(conditions)
                .sort(sort)
                .delete_one()
                .build())
        };

        Ok(DeleteResult::from_write_result(self.state.execute_delete(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
            false,
        )?))
    }
}

/// Builder for deleting every matching document.
pub struct DeleteMany<'a> {
    state: &'a CollectionState,
    filter: Document,
    options: DeleteOptions,
}

impl<'a> DeleteMany<'a> {
    fn new(state: &'a CollectionState, filter: Document) -> Self {
        Self {
            state,
            filter,
            options: DeleteOptions::default(),
        }
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the delete operation.
    pub fn execute(self) -> Result<DeleteResult> {
        let build_plan = |collection, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;

            Ok(LogicalPlanBuilder::scan(collection, hint_id)
                .filter(conditions)
                .delete_many()
                .build())
        };

        Ok(DeleteResult::from_write_result(self.state.execute_delete(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
            false,
        )?))
    }
}

/// Builder for finding and deleting one matching document.
pub struct FindOneAndDelete<'a> {
    state: &'a CollectionState,
    filter: Document,
    options: FindOneAndDeleteOptions,
}

impl<'a> FindOneAndDelete<'a> {
    fn new(state: &'a CollectionState, filter: Document) -> Self {
        Self {
            state,
            filter,
            options: FindOneAndDeleteOptions::default(),
        }
    }

    /// Sets the projection for the returned document.
    pub fn projection(mut self, projection: Document) -> Self {
        self.options.projection = Some(projection);
        self
    }

    /// Sets the sort order used to choose which matching document to delete.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the operation.
    pub fn execute(self) -> Result<Option<Document>> {
        let build_plan = |collection, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;
            let projection = parser::parse_optional_projection(self.options.projection)?;

            Ok(LogicalPlanBuilder::scan(collection, hint_id)
                .filter(conditions)
                .sort(sort)
                .find_one_and_delete(projection)
                .build())
        };

        Ok(document_from_write_result(self.state.execute_delete(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
            true,
        )?))
    }
}

/// Builder for finding at most one matching BSON document.
pub struct FindOne<'a> {
    state: &'a CollectionState,
    filter: Document,
    projection: Option<Document>,
    sort: Option<Document>,
    hint: Option<Hint>,
}

impl<'a> FindOne<'a> {
    fn new(state: &'a CollectionState, filter: Document) -> Self {
        Self {
            state,
            filter,
            projection: None,
            sort: None,
            hint: None,
        }
    }

    /// Sets the projection for the query.
    pub fn projection(mut self, projection: Document) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Sets the sort order used to choose which matching document to return.
    pub fn sort(mut self, sort: Document) -> Self {
        self.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.hint = Some(Hint::CollectionScan);
        self
    }

    /// Executes the query and returns the first matching document, if any.
    pub fn execute(self) -> Result<Option<Document>> {
        let build_plan = |collection_id, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let projection = parser::parse_optional_projection(self.projection.clone())?;
            let sort = parser::parse_optional_sort(self.sort.as_ref())?;
            Ok(LogicalPlanBuilder::scan(collection_id, hint_id)
                .filter(conditions)
                .project(projection)
                .sort(sort)
                .limit(None, Some(1))
                .build_arc())
        };

        self.state
            .execute_query(self.hint.as_ref(), build_plan)?
            .next()
            .transpose()
    }
}

/// Builder for finding and updating one matching document.
pub struct FindOneAndUpdate<'a> {
    state: &'a CollectionState,
    filter: Document,
    update: Document,
    options: FindOneAndUpdateOptions,
}

impl<'a> FindOneAndUpdate<'a> {
    fn new(state: &'a CollectionState, filter: Document, update: Document) -> Self {
        Self {
            state,
            filter,
            update,
            options: FindOneAndUpdateOptions::default(),
        }
    }

    /// Sets the projection for the returned document.
    pub fn projection(mut self, projection: Document) -> Self {
        self.options.projection = Some(projection);
        self
    }

    /// Sets the sort order used to choose which matching document to update.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Sets whether to perform an upsert if no documents match the query.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Sets whether to return the document before or after the update.
    pub fn return_document(mut self, return_document: ReturnDocument) -> Self {
        self.options.return_document = return_document;
        self
    }

    /// Executes the operation.
    pub fn execute(self) -> Result<Option<Document>> {
        let build_plan = |col, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;
            let update = parser::parse_update(&self.update, None)?;
            update.validate_id_modifications(self.options.upsert)?;
            let projection = parser::parse_optional_projection(self.options.projection)?;

            Ok(LogicalPlanBuilder::scan(col, hint_id)
                .filter(conditions)
                .sort(sort)
                .find_one_and_update(
                    update,
                    projection,
                    self.options.upsert,
                    self.options.return_document,
                )
                .build())
        };

        Ok(document_from_write_result(self.state.execute_write(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
        )?))
    }
}

/// Builder for replacing the first matching document.
pub struct ReplaceOne<'a> {
    state: &'a CollectionState,
    filter: Document,
    replacement: Document,
    options: ReplaceOneOptions,
}

impl<'a> ReplaceOne<'a> {
    fn new(state: &'a CollectionState, filter: Document, replacement: Document) -> Self {
        Self {
            state,
            filter,
            replacement,
            options: ReplaceOneOptions::default(),
        }
    }

    /// Sets whether to perform an upsert if no documents match the query.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Sets the sort order used to choose which matching document to replace.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Executes the replace operation.
    pub fn execute(self) -> Result<UpdateResult> {
        let build_plan = |col, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;
            let replacement = parser::parse_replacement(&self.replacement)?;

            Ok(LogicalPlanBuilder::scan(col, hint_id)
                .filter(conditions)
                .sort(sort)
                .replace_one(replacement, self.options.upsert)
                .build())
        };

        Ok(UpdateResult::from_write_result(self.state.execute_write(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
        )?))
    }
}

/// Builder for finding and replacing one matching document.
pub struct FindOneAndReplace<'a> {
    state: &'a CollectionState,
    filter: Document,
    replacement: Document,
    options: FindOneAndReplaceOptions,
}

impl<'a> FindOneAndReplace<'a> {
    fn new(state: &'a CollectionState, filter: Document, replacement: Document) -> Self {
        Self {
            state,
            filter,
            replacement,
            options: FindOneAndReplaceOptions::default(),
        }
    }

    /// Sets the projection for the returned document.
    pub fn projection(mut self, projection: Document) -> Self {
        self.options.projection = Some(projection);
        self
    }

    /// Sets the sort order used to choose which matching document to replace.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Sets whether to perform an upsert if no documents match the query.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    ///
    /// This overrides the database's configured WAL durability for this operation only. It does
    /// not flush the memtable or wait for SSTable work.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Sets whether to return the document before or after the replacement.
    pub fn return_document(mut self, return_document: ReturnDocument) -> Self {
        self.options.return_document = return_document;
        self
    }

    /// Executes the operation.
    pub fn execute(self) -> Result<Option<Document>> {
        let build_plan = |collection, hint_id| {
            let conditions = parser::parse_conditions(&self.filter)?;
            let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;
            let projection = parser::parse_optional_projection(self.options.projection)?;
            let replacement = parser::parse_replacement(&self.replacement)?;

            Ok(LogicalPlanBuilder::scan(collection, hint_id)
                .filter(conditions)
                .sort(sort)
                .find_one_and_replace(
                    replacement,
                    projection,
                    self.options.upsert,
                    self.options.return_document,
                )
                .build())
        };

        Ok(document_from_write_result(self.state.execute_write(
            self.options.hint.as_ref(),
            build_plan,
            self.options.sync,
        )?))
    }
}

/// Represents a query on a collection.
/// Provides methods to set query parameters and execute the query.
/// Builder for querying multiple BSON documents.
pub struct Find<'a> {
    state: &'a CollectionState,
    filter: Document, // Unified filter representation using Expr
    options: FindOptions,
}

impl<'a> Find<'a> {
    fn new(state: &'a CollectionState, filter: Document) -> Find<'a> {
        Find {
            state,
            filter,
            options: FindOptions::default(),
        }
    }

    /// Sets the projection for the query.
    /// # Arguments
    /// * `projection` - The projection document specifying which fields to include or exclude.
    /// Returns the modified Find instance for chaining.
    pub fn projection(mut self, projection: Document) -> Self {
        self.options.projection = Some(projection);
        self
    }

    /// Sets the sort order for the query.
    /// # Arguments
    /// * `sort` - The sort document specifying the fields and their sort order.
    /// Returns the modified Find instance for chaining.
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Forces the query to use the index with the given name.
    pub fn hint(mut self, index_name: impl Into<String>) -> Self {
        self.options.hint = Some(Hint::Index(index_name.into()));
        self
    }

    /// Forces the query to use the collection scan.
    pub fn hint_collection_scan(mut self) -> Self {
        self.options.hint = Some(Hint::CollectionScan);
        self
    }

    /// Sets the limit for the number of documents to return.
    /// # Arguments
    /// * `limit` - The maximum number of documents to return.
    /// Returns the modified Find instance for chaining.
    pub fn limit(mut self, limit: usize) -> Self {
        self.options.limit = Some(limit);
        self
    }

    /// Sets the number of documents to skip.
    /// # Arguments
    /// * `skip` - The number of documents to skip.
    /// Returns the modified Find instance for chaining.
    pub fn skip(mut self, skip: usize) -> Self {
        self.options.skip = Some(skip);
        self
    }

    fn build_logical_plan(
        &self,
        collection_id: u32,
        hint_id: Option<u32>,
    ) -> Result<Arc<LogicalPlan>> {
        let conditions = parser::parse_conditions(&self.filter)?;
        let projection = parser::parse_optional_projection(self.options.projection.clone())?;
        let sort = parser::parse_optional_sort(self.options.sort.as_ref())?;

        Ok(LogicalPlanBuilder::scan(collection_id, hint_id)
            .filter(conditions)
            .project(projection)
            .sort(sort)
            .limit(self.options.skip, self.options.limit)
            .build_arc())
    }

    /// Executes the query and returns an iterator over the resulting documents.
    /// Returns a `Result` containing an iterator of documents or an error.
    pub fn execute(&self) -> Result<QueryOutput> {
        self.state
            .execute_query(self.options.hint.as_ref(), |collection_id, hint_id| {
                self.build_logical_plan(collection_id, hint_id)
            })
    }

    /// Returns the planned operators for this query without executing it.
    ///
    /// The operators are a diagnostic view of the current plan; optimizer
    /// choices may change as the optimizer evolves. For typed queries, see
    /// [`TypedFind::explain`](crate::typed::TypedFind::explain).
    pub fn explain(&self) -> Result<ExplainPlan> {
        let planned = self
            .state
            .plan_query(self.options.hint.as_ref(), |collection_id, hint_id| {
                self.build_logical_plan(collection_id, hint_id)
            })?;

        Ok(match planned {
            Some((plan, collection)) => ExplainPlan::from_physical_plan(&plan, &collection),
            None => ExplainPlan::new(ExplainNode::new(ExplainOperator::NoOp, vec![])),
        })
    }

    /// Executes the query and collects all matching documents.
    pub fn execute_collect(&self) -> Result<Vec<Document>> {
        self.execute()?.collect()
    }
}

/// Builder for creating a BSON-key index with optional naming.
pub struct CreateIndex<'a> {
    state: &'a CollectionState,
    keys: Document,
    options: CreateIndexOptions,
}

impl<'a> CreateIndex<'a> {
    pub(crate) fn new(state: &'a CollectionState, keys: Document) -> Self {
        Self {
            state,
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
        let keys = parser::parse_index_keys(&self.keys)?;
        self.state.create_index(keys, self.options)
    }
}

fn document_from_write_result(result: WriteResult) -> Option<Document> {
    match result {
        WriteResult::SingleDocument { document, .. } => document,
        other => panic!("expected SingleDocument write result, got {other:?}"),
    }
}
