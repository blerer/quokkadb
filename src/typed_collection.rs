use super::*;
use crate::collection::{DeleteResult, IndexInfo, QueryOutput, UpdateResult};
use crate::collection_state::CollectionState;
use crate::document::ReturnDocument;
use crate::query::execution::WriteResult;
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::{Projection, SortField};
use bson::{Document, serialize_to_document, serialize_to_vec};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

/// Represents a collection in the database.
/// Provides methods to perform CRUD operations on the collection in a typed way.
pub struct TypedCollection<T: QuokkaDocument> {
    state: CollectionState,
    _marker: PhantomData<fn() -> T>,
}

/// The lazy results of a typed query.
pub type TypedQueryOutput<T> = Box<dyn Iterator<Item = Result<T>>>;

pub(crate) fn deserialize_query_output<T: DeserializeOwned>(
    query_output: QueryOutput,
) -> TypedQueryOutput<T> {
    Box::new(
        query_output
            .map(|document| -> Result<T> { Ok(bson::deserialize_from_document(document?)?) }),
    )
}

enum TypedResultDecoder<T> {
    Bson,
    Custom(Box<dyn Fn(Document) -> Result<T>>),
}

fn deserialize_query_output_with<T: DeserializeOwned + 'static>(
    query_output: QueryOutput,
    decoder: TypedResultDecoder<T>,
) -> TypedQueryOutput<T> {
    match decoder {
        TypedResultDecoder::Bson => deserialize_query_output(query_output),
        TypedResultDecoder::Custom(decode) => Box::new(query_output.map(move |document| {
            let document = document?;
            decode(document)
        })),
    }
}

fn deserialize_document_with<T: DeserializeOwned>(
    document: Option<Document>,
    decoder: TypedResultDecoder<T>,
) -> Result<Option<T>> {
    document
        .map(|document| match decoder {
            TypedResultDecoder::Bson => Ok(bson::deserialize_from_document(document)?),
            TypedResultDecoder::Custom(decode) => decode(document),
        })
        .transpose()
}

/// Builds a query for typed documents.
pub struct TypedFind<'a, T: QuokkaDocument, R = T, ProjectionState = NoProjection> {
    state: &'a CollectionState,
    filter: Filter<T>,
    options: FindOptions,
    decoder: TypedResultDecoder<R>,
    _result: PhantomData<fn() -> (R, ProjectionState)>,
}

/// Marks a query that has not selected a projection mode.
#[doc(hidden)]
pub struct NoProjection;

/// Marks a query with an inclusion projection.
#[doc(hidden)]
pub struct InclusionProjection;

/// Marks a query with an exclusion projection.
#[doc(hidden)]
pub struct ExclusionProjection;

#[derive(Default)]
struct FindOptions {
    projection: Option<Arc<Projection>>,
    sort: Option<Arc<Vec<SortField>>>,
    limit: Option<usize>,
    skip: Option<usize>,
}

impl FindOptions {
    fn with_projection(mut self, projection: Arc<Projection>) -> Self {
        self.projection = Some(projection);
        self
    }
}

impl<'a, T: QuokkaDocument, R> TypedFind<'a, T, R, NoProjection> {
    fn new(state: &'a CollectionState, filter: Filter<T>) -> Self {
        Self {
            state,
            filter,
            options: FindOptions::default(),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields and deserializes matching documents into another result type.
    pub fn include<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFind<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFind {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFind {
            state,
            filter,
            options: options.with_projection(Projection::typed_include(
                include(T::root_fields()).projection_paths(),
            )),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields without the document `_id` and deserializes matching documents into another result type.
    pub fn include_without_id<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFind<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFind {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFind {
            state,
            filter,
            options: options.with_projection(Projection::typed_include_without_id(
                include(T::root_fields()).projection_paths(),
            )),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Excludes fields and deserializes matching documents into another result type.
    pub fn exclude<S, NewResult: DeserializeOwned + 'static>(
        self,
        exclude: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFind<'a, T, NewResult, ExclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFind {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFind {
            state,
            filter,
            options: options.with_projection(Projection::typed_exclude(
                exclude(T::root_fields()).projection_paths(),
            )),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Selects one or more typed fields into a value or tuple result.
    pub fn select<S>(
        self,
        select: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFind<'a, T, S::Output, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let selection = select(T::root_fields());
        let TypedFind {
            state,
            filter,
            options,
            ..
        } = self;
        let projection =
            Projection::typed_include_without_id(selection.selection_projection_paths());

        TypedFind {
            state,
            filter,
            options: options.with_projection(projection),
            decoder: TypedResultDecoder::Custom(Box::new(move |document| {
                selection.decode(&document)
            })),
            _result: PhantomData,
        }
    }
}

impl<'a, T: QuokkaDocument, R, ProjectionState> TypedFind<'a, T, R, ProjectionState> {
    /// Sets the sort order for the query.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Sets the maximum number of documents to return.
    pub fn limit(mut self, limit: usize) -> Self {
        self.options.limit = Some(limit);
        self
    }

    /// Sets the number of documents to skip.
    pub fn skip(mut self, skip: usize) -> Self {
        self.options.skip = Some(skip);
        self
    }

    /// Executes the query.
    pub fn execute(self) -> Result<TypedQueryOutput<R>>
    where
        R: DeserializeOwned + 'static,
    {
        let TypedFind {
            state,
            filter,
            options,
            decoder,
            ..
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .project(options.projection)
                .sort(options.sort)
                .limit(options.skip, options.limit)
                .build_arc())
        };

        Ok(deserialize_query_output_with(
            state.execute_query(build_plan)?,
            decoder,
        ))
    }

    /// Executes the query and collects all matching models.
    pub fn execute_collect(self) -> Result<Vec<R>>
    where
        R: DeserializeOwned + 'static,
    {
        self.execute()?.collect()
    }
}

/// Builds a query for one typed document.
pub struct TypedFindOne<'a, T: QuokkaDocument, R = T, ProjectionState = NoProjection> {
    state: &'a CollectionState,
    filter: Filter<T>,
    options: FindOptions,
    decoder: TypedResultDecoder<R>,
    _result: PhantomData<fn() -> (R, ProjectionState)>,
}

impl<'a, T: QuokkaDocument, R> TypedFindOne<'a, T, R, NoProjection> {
    fn new(state: &'a CollectionState, filter: Filter<T>) -> Self {
        Self {
            state,
            filter,
            options: FindOptions::default(),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields and deserializes the returned document into another result type.
    pub fn include<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOne<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFindOne {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFindOne {
            state,
            filter,
            options: options.with_projection(Projection::typed_include(
                include(T::root_fields()).projection_paths(),
            )),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields without the document `_id` and deserializes the returned document into another result type.
    pub fn include_without_id<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOne<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFindOne {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFindOne {
            state,
            filter,
            options: options.with_projection(Projection::typed_include_without_id(
                include(T::root_fields()).projection_paths(),
            )),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Excludes fields and deserializes the returned document into another result type.
    pub fn exclude<S, NewResult: DeserializeOwned + 'static>(
        self,
        exclude: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOne<'a, T, NewResult, ExclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFindOne {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFindOne {
            state,
            filter,
            options: options.with_projection(Projection::typed_exclude(
                exclude(T::root_fields()).projection_paths(),
            )),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Selects one or more typed fields into a value or tuple result.
    pub fn select<S>(
        self,
        select: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOne<'a, T, S::Output, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let selection = select(T::root_fields());
        let TypedFindOne {
            state,
            filter,
            options,
            ..
        } = self;
        let projection =
            Projection::typed_include_without_id(selection.selection_projection_paths());

        TypedFindOne {
            state,
            filter,
            options: options.with_projection(projection),
            decoder: TypedResultDecoder::Custom(Box::new(move |document| {
                selection.decode(&document)
            })),
            _result: PhantomData,
        }
    }
}

impl<'a, T: QuokkaDocument, R, ProjectionState> TypedFindOne<'a, T, R, ProjectionState> {
    /// Sets the sort order used to choose the matching document.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Executes the query and returns the first matching typed document, if any.
    pub fn execute(self) -> Result<Option<R>>
    where
        R: DeserializeOwned + 'static,
    {
        let TypedFindOne {
            state,
            filter,
            options,
            decoder,
            ..
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .project(options.projection)
                .sort(options.sort)
                .limit(None, Some(1))
                .build_arc())
        };

        deserialize_query_output_with(state.execute_query(build_plan)?, decoder)
            .next()
            .transpose()
    }
}

#[derive(Default)]
struct UpdateOptions {
    sync: bool,
    upsert: bool,
    sort: Option<Arc<Vec<SortField>>>,
}

#[derive(Default)]
struct FindOneAndModifyOptions {
    projection: Option<Arc<Projection>>,
    sync: bool,
    sort: Option<Arc<Vec<SortField>>>,
    upsert: bool,
    return_document: ReturnDocument,
}

fn update_result_from_write_result(result: WriteResult) -> UpdateResult {
    match result {
        WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id,
        } => UpdateResult {
            matched_count,
            modified_count,
            upserted_id,
        },
        other => panic!("expected Update write result, got {other:?}"),
    }
}

/// Builds an update operation for one typed document.
pub struct TypedUpdateOne<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    filter: Filter<T>,
    update: Update<T>,
    options: UpdateOptions,
}

impl<'a, T: QuokkaDocument> TypedUpdateOne<'a, T> {
    fn new(state: &'a CollectionState, filter: Filter<T>, update: Update<T>) -> Self {
        Self {
            state,
            filter,
            update,
            options: UpdateOptions::default(),
        }
    }

    /// Sets the sort order used to choose which matching document to update.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Sets whether to insert a document if no documents match the filter.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the update operation.
    pub fn execute(self) -> Result<UpdateResult> {
        let TypedUpdateOne {
            state,
            filter,
            update,
            options,
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .sort(options.sort)
                .update_one(update.into_update_expr(), options.upsert)
                .build())
        };

        Ok(update_result_from_write_result(
            state.execute_write(build_plan, options.sync)?,
        ))
    }
}

/// Builds an update operation for all matching typed documents.
pub struct TypedUpdateMany<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    filter: Filter<T>,
    update: Update<T>,
    options: UpdateOptions,
}

impl<'a, T: QuokkaDocument> TypedUpdateMany<'a, T> {
    fn new(state: &'a CollectionState, filter: Filter<T>, update: Update<T>) -> Self {
        Self {
            state,
            filter,
            update,
            options: UpdateOptions::default(),
        }
    }

    /// Sets whether to insert a document if no documents match the filter.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the update operation.
    pub fn execute(self) -> Result<UpdateResult> {
        let TypedUpdateMany {
            state,
            filter,
            update,
            options,
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .update_many(update.into_update_expr(), options.upsert)
                .build())
        };

        Ok(update_result_from_write_result(
            state.execute_write(build_plan, options.sync)?,
        ))
    }
}

enum TypedFindOneAndModifyOperation<T: QuokkaDocument> {
    Update(Update<T>),
    Replace(Document),
}

/// Builds an operation that finds, modifies, and returns one typed document.
pub struct TypedFindOneAndModify<'a, T: QuokkaDocument, R = T, ProjectionState = NoProjection> {
    state: &'a CollectionState,
    filter: Filter<T>,
    operation: TypedFindOneAndModifyOperation<T>,
    options: FindOneAndModifyOptions,
    decoder: TypedResultDecoder<R>,
    _result: PhantomData<fn() -> ProjectionState>,
}

/// Builds an operation that finds, updates, and returns one typed document.
pub type TypedFindOneAndUpdate<'a, T, R = T, ProjectionState = NoProjection> =
    TypedFindOneAndModify<'a, T, R, ProjectionState>;

/// Builds an operation that finds, replaces, and returns one typed document.
pub type TypedFindOneAndReplace<'a, T, R = T, ProjectionState = NoProjection> =
    TypedFindOneAndModify<'a, T, R, ProjectionState>;

impl<'a, T: QuokkaDocument, R> TypedFindOneAndModify<'a, T, R, NoProjection> {
    fn new_update(state: &'a CollectionState, filter: Filter<T>, update: Update<T>) -> Self {
        Self {
            state,
            filter,
            operation: TypedFindOneAndModifyOperation::Update(update),
            options: FindOneAndModifyOptions::default(),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    fn new_replace(state: &'a CollectionState, filter: Filter<T>, replacement: Document) -> Self {
        Self {
            state,
            filter,
            operation: TypedFindOneAndModifyOperation::Replace(replacement),
            options: FindOneAndModifyOptions::default(),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields and deserializes the returned document into another result type.
    pub fn include<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndModify<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        self.with_projection(
            Projection::typed_include(include(T::root_fields()).projection_paths()),
            TypedResultDecoder::Bson,
        )
    }

    /// Includes fields without the document `_id` and deserializes the returned document into another result type.
    pub fn include_without_id<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndModify<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        self.with_projection(
            Projection::typed_include_without_id(include(T::root_fields()).projection_paths()),
            TypedResultDecoder::Bson,
        )
    }

    /// Excludes fields and deserializes the returned document into another result type.
    pub fn exclude<S, NewResult: DeserializeOwned + 'static>(
        self,
        exclude: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndModify<'a, T, NewResult, ExclusionProjection>
    where
        S: TypedSelection<T>,
    {
        self.with_projection(
            Projection::typed_exclude(exclude(T::root_fields()).projection_paths()),
            TypedResultDecoder::Bson,
        )
    }

    /// Selects one or more typed fields into a value or tuple result.
    pub fn select<S>(
        self,
        select: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndModify<'a, T, S::Output, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let selection = select(T::root_fields());
        self.with_projection(
            Projection::typed_include_without_id(selection.selection_projection_paths()),
            TypedResultDecoder::Custom(Box::new(move |document| selection.decode(&document))),
        )
    }
}

impl<'a, T: QuokkaDocument, R, ProjectionState> TypedFindOneAndModify<'a, T, R, ProjectionState> {
    fn with_projection<NewResult, NewProjectionState>(
        self,
        projection: Arc<Projection>,
        decoder: TypedResultDecoder<NewResult>,
    ) -> TypedFindOneAndModify<'a, T, NewResult, NewProjectionState> {
        TypedFindOneAndModify {
            state: self.state,
            filter: self.filter,
            operation: self.operation,
            options: FindOneAndModifyOptions {
                projection: Some(projection),
                ..self.options
            },
            decoder,
            _result: PhantomData,
        }
    }

    /// Sets the sort order used to choose which matching document to modify.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Sets whether to insert a document if no documents match the filter.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Sets whether to return the document before or after the modification.
    pub fn return_document(mut self, return_document: ReturnDocument) -> Self {
        self.options.return_document = return_document;
        self
    }

    /// Executes the operation and returns the selected typed document, if any.
    pub fn execute(self) -> Result<Option<R>>
    where
        R: DeserializeOwned + 'static,
    {
        let TypedFindOneAndModify {
            state,
            filter,
            operation,
            options,
            decoder,
            ..
        } = self;
        let build_plan = |collection| {
            let builder = LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .sort(options.sort);
            Ok(match operation {
                TypedFindOneAndModifyOperation::Update(update) => builder
                    .find_one_and_update(
                        update.into_update_expr(),
                        options.projection,
                        options.upsert,
                        options.return_document,
                    )
                    .build(),
                TypedFindOneAndModifyOperation::Replace(replacement) => builder
                    .find_one_and_replace(
                        replacement,
                        options.projection,
                        options.upsert,
                        options.return_document,
                    )
                    .build(),
            })
        };

        let result = state.execute_write(build_plan, options.sync)?;
        let document = match result {
            WriteResult::SingleDocument { document, .. } => document,
            other => panic!("expected SingleDocument write result, got {other:?}"),
        };
        deserialize_document_with(document, decoder)
    }
}

/// Builds a replacement operation for one typed document.
pub struct TypedReplaceOne<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    filter: Filter<T>,
    replacement: Document,
    options: UpdateOptions,
}

impl<'a, T: QuokkaDocument> TypedReplaceOne<'a, T> {
    fn new(state: &'a CollectionState, filter: Filter<T>, replacement: T) -> Result<Self> {
        Ok(Self {
            state,
            filter,
            replacement: serialize_to_document(&replacement)?,
            options: UpdateOptions::default(),
        })
    }

    /// Sets the sort order used to choose which matching document to replace.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Sets whether to insert a document if no documents match the filter.
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the replacement operation.
    pub fn execute(self) -> Result<UpdateResult> {
        let TypedReplaceOne {
            state,
            filter,
            replacement,
            options,
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .sort(options.sort)
                .replace_one(replacement, options.upsert)
                .build())
        };

        Ok(update_result_from_write_result(
            state.execute_write(build_plan, options.sync)?,
        ))
    }
}

#[derive(Default)]
struct DeleteOptions {
    sync: bool,
    sort: Option<Arc<Vec<SortField>>>,
}

#[derive(Default)]
struct FindOneAndDeleteOptions {
    projection: Option<Arc<Projection>>,
    sync: bool,
    sort: Option<Arc<Vec<SortField>>>,
}

/// Builds a delete operation for one typed document.
pub struct TypedDeleteOne<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    filter: Filter<T>,
    options: DeleteOptions,
}

impl<'a, T: QuokkaDocument> TypedDeleteOne<'a, T> {
    fn new(state: &'a CollectionState, filter: Filter<T>) -> Self {
        Self {
            state,
            filter,
            options: DeleteOptions::default(),
        }
    }

    /// Sets the sort order used to choose which matching document to delete.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the delete operation.
    pub fn execute(self) -> Result<DeleteResult> {
        let TypedDeleteOne {
            state,
            filter,
            options,
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .sort(options.sort)
                .delete_one()
                .build())
        };

        let result = state.execute_delete(build_plan, options.sync, false)?;
        Ok(match result {
            WriteResult::Delete { deleted_count } => DeleteResult { deleted_count },
            other => panic!("expected Delete write result, got {other:?}"),
        })
    }
}

/// Builds a delete operation for all matching typed documents.
pub struct TypedDeleteMany<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    filter: Filter<T>,
    options: DeleteOptions,
}

impl<'a, T: QuokkaDocument> TypedDeleteMany<'a, T> {
    fn new(state: &'a CollectionState, filter: Filter<T>) -> Self {
        Self {
            state,
            filter,
            options: DeleteOptions::default(),
        }
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the delete operation.
    pub fn execute(self) -> Result<DeleteResult> {
        let TypedDeleteMany {
            state,
            filter,
            options,
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .delete_many()
                .build())
        };

        let result = state.execute_delete(build_plan, options.sync, false)?;
        Ok(match result {
            WriteResult::Delete { deleted_count } => DeleteResult { deleted_count },
            other => panic!("expected Delete write result, got {other:?}"),
        })
    }
}

/// Builds an operation that finds, deletes, and returns one typed document.
pub struct TypedFindOneAndDelete<'a, T: QuokkaDocument, R = T, ProjectionState = NoProjection> {
    state: &'a CollectionState,
    filter: Filter<T>,
    options: FindOneAndDeleteOptions,
    decoder: TypedResultDecoder<R>,
    _result: PhantomData<fn() -> (R, ProjectionState)>,
}

impl<'a, T: QuokkaDocument, R> TypedFindOneAndDelete<'a, T, R, NoProjection> {
    fn new(state: &'a CollectionState, filter: Filter<T>) -> Self {
        Self {
            state,
            filter,
            options: FindOneAndDeleteOptions::default(),
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields and deserializes the returned document into another result type.
    pub fn include<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndDelete<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFindOneAndDelete {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFindOneAndDelete {
            state,
            filter,
            options: FindOneAndDeleteOptions {
                projection: Some(Projection::typed_include(
                    include(T::root_fields()).projection_paths(),
                )),
                ..options
            },
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Includes fields without the document `_id` and deserializes the returned document into another result type.
    pub fn include_without_id<S, NewResult: DeserializeOwned + 'static>(
        self,
        include: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndDelete<'a, T, NewResult, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFindOneAndDelete {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFindOneAndDelete {
            state,
            filter,
            options: FindOneAndDeleteOptions {
                projection: Some(Projection::typed_include_without_id(
                    include(T::root_fields()).projection_paths(),
                )),
                ..options
            },
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Excludes fields and deserializes the returned document into another result type.
    pub fn exclude<S, NewResult: DeserializeOwned + 'static>(
        self,
        exclude: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndDelete<'a, T, NewResult, ExclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let TypedFindOneAndDelete {
            state,
            filter,
            options,
            ..
        } = self;
        TypedFindOneAndDelete {
            state,
            filter,
            options: FindOneAndDeleteOptions {
                projection: Some(Projection::typed_exclude(
                    exclude(T::root_fields()).projection_paths(),
                )),
                ..options
            },
            decoder: TypedResultDecoder::Bson,
            _result: PhantomData,
        }
    }

    /// Selects one or more typed fields into a value or tuple result.
    pub fn select<S>(
        self,
        select: impl FnOnce(T::Fields<T>) -> S,
    ) -> TypedFindOneAndDelete<'a, T, S::Output, InclusionProjection>
    where
        S: TypedSelection<T>,
    {
        let selection = select(T::root_fields());
        let TypedFindOneAndDelete {
            state,
            filter,
            options,
            ..
        } = self;

        TypedFindOneAndDelete {
            state,
            filter,
            options: FindOneAndDeleteOptions {
                projection: Some(Projection::typed_include_without_id(
                    selection.selection_projection_paths(),
                )),
                ..options
            },
            decoder: TypedResultDecoder::Custom(Box::new(move |document| {
                selection.decode(&document)
            })),
            _result: PhantomData,
        }
    }
}

impl<'a, T: QuokkaDocument, R, ProjectionState> TypedFindOneAndDelete<'a, T, R, ProjectionState> {
    /// Sets the sort order used to choose which matching document to delete.
    pub fn sort(mut self, sort: impl FnOnce(T::Fields<T>) -> Sort<T>) -> Self {
        self.options.sort = Some(Arc::new(sort(T::root_fields()).into_fields()));
        self
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.options.sync = true;
        self
    }

    /// Executes the operation and returns the deleted typed document, if any.
    pub fn execute(self) -> Result<Option<R>>
    where
        R: DeserializeOwned + 'static,
    {
        let TypedFindOneAndDelete {
            state,
            filter,
            options,
            decoder,
            ..
        } = self;
        let build_plan = |collection| {
            Ok(LogicalPlanBuilder::scan(collection)
                .filter(filter.into_expr())
                .sort(options.sort)
                .find_one_and_delete(options.projection)
                .build())
        };

        let result = state.execute_delete(build_plan, options.sync, true)?;
        let document = match result {
            WriteResult::SingleDocument { document, .. } => document,
            other => panic!("expected SingleDocument write result, got {other:?}"),
        };
        deserialize_document_with(document, decoder)
    }
}

impl<T: QuokkaDocument> TypedCollection<T> {
    pub(crate) fn new(db: Arc<DbImpl>, name: String) -> Self {
        Self {
            state: CollectionState::new(db, name),
            _marker: PhantomData,
        }
    }

    pub fn create_if_missing(mut self) -> Self {
        self.state.create_if_missing();
        self
    }

    /// Inserts a typed document into the collection.
    pub fn insert_one(&self, document: T) -> Result<TypedInsertOneResult<T>> {
        self.insert_one_with(document)?.execute()
    }

    /// Creates an insert operation builder for a typed document.
    pub fn insert_one_with(&self, document: T) -> Result<TypedInsertOne<'_, T>> {
        TypedInsertOne::new(&self.state, document)
    }

    /// Inserts multiple typed documents into the collection.
    pub fn insert_many(
        &self,
        documents: impl IntoIterator<Item = T>,
    ) -> Result<TypedInsertManyResult<T>> {
        self.insert_many_with(documents)?.execute()
    }

    /// Creates an insert operation builder for multiple typed documents.
    pub fn insert_many_with(
        &self,
        documents: impl IntoIterator<Item = T>,
    ) -> Result<TypedInsertMany<'_, T>> {
        TypedInsertMany::new(&self.state, documents)
    }

    /// Updates one typed document matching the filter.
    pub fn update_one(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        update: impl FnOnce(&T::Fields<T>) -> Update<T>,
    ) -> Result<UpdateResult> {
        self.update_one_with(filter, update).execute()
    }

    /// Creates an update operation builder for one matching typed document.
    pub fn update_one_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        update: impl FnOnce(&T::Fields<T>) -> Update<T>,
    ) -> TypedUpdateOne<'_, T> {
        TypedUpdateOne::new(
            &self.state,
            filter(&T::root_fields()),
            update(&T::root_fields()),
        )
    }

    /// Updates all typed documents matching the filter.
    pub fn update_many(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        update: impl FnOnce(&T::Fields<T>) -> Update<T>,
    ) -> Result<UpdateResult> {
        self.update_many_with(filter, update).execute()
    }

    /// Creates an update operation builder for all matching typed documents.
    pub fn update_many_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        update: impl FnOnce(&T::Fields<T>) -> Update<T>,
    ) -> TypedUpdateMany<'_, T> {
        TypedUpdateMany::new(
            &self.state,
            filter(&T::root_fields()),
            update(&T::root_fields()),
        )
    }

    /// Finds, updates, and returns one typed document matching the filter.
    pub fn find_one_and_update(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        update: impl FnOnce(&T::Fields<T>) -> Update<T>,
    ) -> Result<Option<T>> {
        self.find_one_and_update_with(filter, update).execute()
    }

    /// Creates a find-one-and-update operation builder for typed documents.
    pub fn find_one_and_update_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        update: impl FnOnce(&T::Fields<T>) -> Update<T>,
    ) -> TypedFindOneAndUpdate<'_, T> {
        TypedFindOneAndModify::new_update(
            &self.state,
            filter(&T::root_fields()),
            update(&T::root_fields()),
        )
    }

    /// Replaces one typed document matching the filter.
    pub fn replace_one(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        replacement: T,
    ) -> Result<UpdateResult> {
        self.replace_one_with(filter, replacement)?.execute()
    }

    /// Creates a replacement operation builder for one matching typed document.
    pub fn replace_one_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        replacement: T,
    ) -> Result<TypedReplaceOne<'_, T>> {
        TypedReplaceOne::new(&self.state, filter(&T::root_fields()), replacement)
    }

    /// Finds, replaces, and returns one typed document matching the filter.
    pub fn find_one_and_replace(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        replacement: T,
    ) -> Result<Option<T>> {
        self.find_one_and_replace_with(filter, replacement)?
            .execute()
    }

    /// Creates a find-one-and-replace operation builder for typed documents.
    pub fn find_one_and_replace_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
        replacement: T,
    ) -> Result<TypedFindOneAndReplace<'_, T>> {
        Ok(TypedFindOneAndModify::new_replace(
            &self.state,
            filter(&T::root_fields()),
            serialize_to_document(&replacement)?,
        ))
    }

    /// Deletes a single typed document matching the filter closure.
    pub fn delete_one(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> Result<DeleteResult> {
        self.delete_one_with(filter).execute()
    }

    /// Creates a delete operation builder for one typed document matching the filter closure.
    pub fn delete_one_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> TypedDeleteOne<'_, T> {
        TypedDeleteOne::new(&self.state, filter(&T::root_fields()))
    }

    /// Deletes all typed documents matching the filter closure.
    pub fn delete_many(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> Result<DeleteResult> {
        self.delete_many_with(filter).execute()
    }

    /// Creates a delete operation builder for all typed documents matching the filter closure.
    pub fn delete_many_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> TypedDeleteMany<'_, T> {
        TypedDeleteMany::new(&self.state, filter(&T::root_fields()))
    }

    /// Finds, deletes, and returns a single typed document matching the filter closure.
    pub fn find_one_and_delete(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> Result<Option<T>> {
        self.find_one_and_delete_with(filter).execute()
    }

    /// Creates a find-one-and-delete operation builder for typed documents.
    pub fn find_one_and_delete_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> TypedFindOneAndDelete<'_, T> {
        TypedFindOneAndDelete::new(&self.state, filter(&T::root_fields()))
    }

    /// Creates a query for typed documents matching the filter closure.
    pub fn find(&self, filter: impl FnOnce(&T::Fields<T>) -> Filter<T>) -> TypedFind<'_, T> {
        TypedFind::new(&self.state, filter(&T::root_fields()))
    }

    /// Finds one typed document matching the filter closure.
    pub fn find_one(&self, filter: impl FnOnce(&T::Fields<T>) -> Filter<T>) -> Result<Option<T>> {
        self.find_one_with(filter).execute()
    }

    /// Creates a find-one operation builder for a typed document matching the filter closure.
    pub fn find_one_with(
        &self,
        filter: impl FnOnce(&T::Fields<T>) -> Filter<T>,
    ) -> TypedFindOne<'_, T> {
        TypedFindOne::new(&self.state, filter(&T::root_fields()))
    }

    /// Creates an index from fields of this collection's document type.
    pub fn create_index(&self, fields: impl FnOnce(T::Fields<T>) -> Index<T>) -> Result<String> {
        self.create_index_with(fields).execute()
    }

    /// Creates an index builder from fields of this collection's document type.
    pub fn create_index_with(
        &self,
        fields: impl FnOnce(T::Fields<T>) -> Index<T>,
    ) -> TypedCreateIndex<'_, T> {
        TypedCreateIndex::new(&self.state, fields(T::root_fields()))
    }

    /// Returns the active indexes for the collection.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        self.state.list_indexes()
    }

    /// Drops an index from the collection by its name.
    pub fn drop_index(&self, name: &str) -> Result<()> {
        self.state.drop_index(name)
    }

    /// Drops this collection.
    pub fn drop_collection(&self) -> Result<()> {
        self.state.drop_collection()
    }

    /// Renames this collection and returns a handle for the new name.
    pub fn rename(&self, new_name: &str) -> Result<Self> {
        let state = self.state.rename(new_name)?;
        Ok(TypedCollection {
            state,
            _marker: PhantomData,
        })
    }

    /// Returns the estimated number of documents in the collection based on storage count stats.
    pub fn estimated_document_count(&self) -> Result<u64> {
        self.state.estimated_document_count()
    }
}

/// The result of inserting one typed document.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedInsertOneResult<T: QuokkaDocument> {
    pub inserted_id: T::Id,
}

impl<T: QuokkaDocument> TypedInsertOneResult<T> {
    fn from_write_result(result: WriteResult) -> Result<Self> {
        match result {
            WriteResult::InsertOne { inserted_id } => Ok(Self {
                inserted_id: bson::deserialize_from_bson(inserted_id)?,
            }),
            other => panic!("expected InsertOne write result, got {other:?}"),
        }
    }
}

/// The result of inserting multiple typed documents.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedInsertManyResult<T: QuokkaDocument> {
    pub inserted_ids: Vec<T::Id>,
}

impl<T: QuokkaDocument> TypedInsertManyResult<T> {
    fn from_write_result(result: WriteResult) -> Result<Self> {
        match result {
            WriteResult::InsertMany { inserted_ids } => Ok(Self {
                inserted_ids: inserted_ids
                    .into_iter()
                    .map(bson::deserialize_from_bson)
                    .collect::<std::result::Result<_, _>>()?,
            }),
            other => panic!("expected InsertMany write result, got {other:?}"),
        }
    }
}

/// Builds an insert operation for a typed document.
pub struct TypedInsertOne<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    document: Vec<u8>,
    sync: bool,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: QuokkaDocument> TypedInsertOne<'a, T> {
    fn new(state: &'a CollectionState, document: T) -> Result<Self> {
        Ok(Self {
            state,
            document: serialize_to_vec(&document)?,
            sync: false,
            _marker: PhantomData,
        })
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Executes the insert operation.
    pub fn execute(self) -> Result<TypedInsertOneResult<T>> {
        let build_plan = |collection| {
            Ok(LogicalPlan::InsertOne {
                collection,
                document: self.document,
            })
        };

        TypedInsertOneResult::from_write_result(self.state.execute_write(build_plan, self.sync)?)
    }
}

/// Builds an insert operation for multiple typed documents.
pub struct TypedInsertMany<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    documents: Vec<Vec<u8>>,
    sync: bool,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T: QuokkaDocument> TypedInsertMany<'a, T> {
    fn new(state: &'a CollectionState, documents: impl IntoIterator<Item = T>) -> Result<Self> {
        let mut serialized = Vec::new();
        for document in documents {
            serialized.push(serialize_to_vec(&document)?);
        }

        Ok(Self {
            state,
            documents: serialized,
            sync: false,
            _marker: PhantomData,
        })
    }

    /// Forces this write to sync its WAL record to durable storage before `execute()` returns.
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Executes the insert operation.
    pub fn execute(self) -> Result<TypedInsertManyResult<T>> {
        let build_plan = |collection| {
            Ok(LogicalPlan::InsertMany {
                collection,
                documents: self.documents,
            })
        };

        TypedInsertManyResult::from_write_result(self.state.execute_write(build_plan, self.sync)?)
    }
}

/// Builds an index for a typed collection.
pub struct TypedCreateIndex<'a, T: QuokkaDocument> {
    state: &'a CollectionState,
    index: Index<T>,
    options: CreateIndexOptions,
}

impl<'a, T: QuokkaDocument> TypedCreateIndex<'a, T> {
    fn new(state: &'a CollectionState, index: Index<T>) -> Self {
        Self {
            state,
            index,
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
        self.state
            .create_index(self.index.into_key_spec()?, self.options)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize, quokkadb_derive::QuokkaDocument)]
    struct User {
        #[quokka(id)]
        #[serde(rename = "_id")]
        id: u64,
        name: String,
    }

    #[test]
    fn deserialize_query_output_preserves_document_errors() {
        let query_output: QueryOutput = Box::new(
            vec![
                Ok(doc! { "_id": 1_i64, "name": "Alice" }),
                Ok(doc! { "_id": 2_i64, "name": 42_i32 }),
            ]
            .into_iter(),
        );

        let mut output = deserialize_query_output::<User>(query_output);
        assert_eq!(
            output.next().unwrap().unwrap(),
            User {
                id: 1,
                name: "Alice".to_string(),
            }
        );
        assert!(matches!(output.next().unwrap(), Err(Error::BsonError(_))));
    }
}
