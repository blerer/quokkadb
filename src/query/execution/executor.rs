use crate::error::{Error, Result};
use crate::query::execution::indexes::{Index, Indexes};
use crate::query::execution::{filters, projections, set_path_value, sorts, updates};
use crate::query::physical_plan::{IndexScanRangeExpr, PhysicalPlan};
use crate::query::update::UpdateExpr;
use crate::query::{BsonValue, Expr, Parameters};
use crate::storage::catalog::IdCreationStrategy;
use crate::storage::count_stats::CountStatsBuilder;
use crate::storage::internal_key::{extract_operation_type, extract_record_key};
use crate::storage::operation::{Operation, OperationType};
use crate::storage::storage_engine::{StorageEngine, StorageError};
use crate::storage::write_batch::{Precondition, Preconditions, WriteBatch};
use crate::storage::Direction;
use crate::util::bson_utils::{self, BsonKey};
use crate::util::interval::Interval;
use bson::{doc, serialize_to_vec, Bson, Document, RawDocument};
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type QueryOutput = Box<dyn Iterator<Item = Result<Document>>>;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutorFailpoint {
    UpdateOneAfterRead,
    UpdateOneUpsertAfterNoMatch,
    UpdateManyBeforeCommit,
    InsertManualAfterPreflightBeforeWrite,
    DeleteOneAfterRead,
}

#[cfg(test)]
pub(crate) trait ExecutorTestHook: Send + Sync {
    fn hit(&self, point: ExecutorFailpoint);
}

/// Executes a physical query plan.
pub struct QueryExecutor {
    storage_engine: Arc<StorageEngine>,
    id_generator: Mutex<Sonyflake>,
    #[cfg(test)]
    test_hook: Option<Arc<dyn ExecutorTestHook>>,
}

impl QueryExecutor {
    /// Creates a new `QueryExecutor`.
    pub fn new(storage_engine: Arc<StorageEngine>) -> Self {
        Self {
            storage_engine,
            id_generator: Self::new_id_generator(),
            #[cfg(test)]
            test_hook: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_hook(
        storage_engine: Arc<StorageEngine>,
        test_hook: Arc<dyn ExecutorTestHook>,
    ) -> Self {
        Self {
            storage_engine,
            id_generator: Self::new_id_generator(),
            test_hook: Some(test_hook),
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
    ) -> Result<QueryOutput> {
        match plan {
            PhysicalPlan::InsertMany {
                collection,
                documents,
            } => self.perform_insert_many(collection, documents),
            PhysicalPlan::InsertOne {
                collection,
                document,
            } => self.perform_insert_one(collection, document),
            PhysicalPlan::UpdateOne {
                collection,
                query,
                update,
                upsert,
            } => {
                let parameters = parameters.expect("Parameters must be provided for UpdateOne");

                self.perform_update_one(collection, query, &update, upsert, &parameters)
            }
            PhysicalPlan::UpdateMany {
                collection,
                query,
                update,
                upsert,
            } => {
                let parameters = parameters.expect("Parameters must be provided for UpdateMany");

                self.perform_update_many(collection, query, &update, upsert, &parameters)
            }
            PhysicalPlan::DeleteOne { collection, query } => {
                let parameters = parameters.expect("Parameters must be provided for DeleteOne");

                self.perform_delete_one(collection, query, &parameters)
            }
            _ => {
                // Other plans, should be cached
                unreachable!("Direct execution not supported for plan: {:?}", plan);
            }
        }
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
        match plan.as_ref() {
            PhysicalPlan::CollectionScan {
                collection,
                range,
                direction,
                filter,
                projection: _, // Projection pushdown is not yet supported at this level
            } => self.perform_collection_scan(
                &parameters,
                snapshot,
                collection,
                range,
                direction,
                filter,
            ),
            PhysicalPlan::PointSearch {
                collection,
                key,
                filter,
                projection: _,
            } => self.perform_point_search(&parameters, snapshot, collection, key, filter),
            PhysicalPlan::IndexScan {
                collection,
                index,
                range,
                direction,
                filter,
                projection: _,
            } => self.perform_index_scan(
                &parameters,
                snapshot,
                collection,
                index,
                range,
                direction,
                filter,
            ),
            PhysicalPlan::MultiPointSearch {
                collection,
                keys,
                direction,
                filter,
                projection: _,
            } => self.perform_multi_point_search(
                &parameters,
                snapshot,
                collection,
                keys,
                direction,
                filter,
            ),
            PhysicalPlan::Filter { input, predicate } => {
                let filter = filters::to_filter(predicate.clone(), &parameters);
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(input_iter.filter(move |res| {
                    if res.is_err() {
                        true
                    } else {
                        filter(res.as_ref().unwrap())
                    }
                })))
            }
            PhysicalPlan::Projection { input, projection } => {
                let projector = projections::to_projector(projection, &parameters)?;
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(
                    input_iter.map(move |res| res.and_then(|doc| projector(doc))),
                ))
            }
            PhysicalPlan::InMemorySort { input, sort_fields } => {
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                sorts::in_memory_sort(input_iter, &sort_fields)
            }
            PhysicalPlan::ExternalMergeSort {
                input,
                sort_fields,
                max_in_memory_rows,
            } => {
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                sorts::external_merge_sort(input_iter, sort_fields.clone(), *max_in_memory_rows)
            }
            PhysicalPlan::TopKHeapSort {
                input,
                sort_fields,
                k,
            } => {
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                sorts::top_k_heap_sort(input_iter, sort_fields.clone(), *k)
            }
            PhysicalPlan::Limit { input, skip, limit } => {
                let mut iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                if let Some(s) = skip {
                    iter = Box::new(iter.skip(*s));
                }
                if let Some(l) = limit {
                    iter = Box::new(iter.take(*l));
                }
                Ok(iter)
            }
            _ => {
                unreachable!("Non-parametrized physical plan: {:?}", plan);
            }
        }
    }

    fn perform_update_many(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        let snapshot = self.storage_engine.last_visible_sequence();
        let mut iter =
            self.execute_cached_at_snapshot(query.clone(), &parameters, Some(snapshot))?;

        let mut operations = Vec::new();
        let mut preconditions = Vec::new();
        let mut count_stats = CountStatsBuilder::new();
        let mut matched_count = 0;
        let mut modified_count = 0;
        let mut upserted_id: Option<Bson> = None;

        let mut next = iter.next();

        if next.is_some() {
            let updater = updates::to_updater(&update, false)?;

            while let Some(doc_result) = next {
                let old_doc = doc_result?;
                matched_count += 1;
                let new_doc = updater(old_doc.clone())?;

                let indices = self.indices(collection);

                indices.append_delete_ops(&mut operations, &old_doc, &mut count_stats)?;

                let user_key = new_doc.get("_id").unwrap().try_into_key()?;
                operations.push(Operation::new_put(
                    collection,
                    0,
                    user_key.clone(),
                    new_doc.to_vec()?,
                ));
                indices.append_put_ops(&mut operations, &new_doc, &mut count_stats)?;

                preconditions.push(Precondition::VersionMatch {
                    collection,
                    index: 0,
                    user_key,
                });
                modified_count += 1;
                next = iter.next();
            }
        } else if upsert {
            let (new_doc, generated_id) = self.perform_upsert(&query, update, parameters)?;
            upserted_id = Some(generated_id.clone());

            let user_key = generated_id.try_into_key()?;
            let indices = self.indices(collection);
            count_stats.inc_collection(collection, 1);
            indices.append_put_ops(&mut operations, &new_doc, &mut count_stats)?;
            operations.push(Operation::new_put(
                collection,
                0,
                user_key.clone(),
                new_doc.to_vec()?,
            ));
            preconditions.push(Precondition::VersionMatch {
                collection,
                index: 0,
                user_key,
            });
        } else {
            let result = doc! { "matched_count": matched_count, "modified_count": 0 };
            return Ok(Box::new(std::iter::once(Ok(result))));
        }

        let batch = WriteBatch::new_with_preconditions(
            operations,
            Preconditions::new(snapshot, preconditions),
            count_stats.build(),
        );
        #[cfg(test)]
        self.invoke_test_hook(ExecutorFailpoint::UpdateManyBeforeCommit);
        self.storage_engine.write(batch)?;

        let result = if let Some(id) = upserted_id {
            doc! { "matched_count": matched_count, "modified_count": modified_count, "upserted_id": id }
        } else {
            doc! { "matched_count": matched_count, "modified_count": modified_count }
        };
        Ok(Box::new(std::iter::once(Ok(result))))
    }

    fn perform_update_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        let start_time = Instant::now();
        let mut attempt = 0;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter =
                self.execute_cached_at_snapshot(query.clone(), &parameters, Some(snapshot))?;

            let result_doc = if let Some(doc_result) = iter.next() {
                let old_doc = doc_result?;
                let updater = updates::to_updater(&update, false)?;
                let new_doc = updater(old_doc.clone())?;

                let user_key = new_doc.get("_id").unwrap().try_into_key()?;

                let new_doc_bytes = serialize_to_vec(&new_doc)?;
                #[cfg(test)]
                self.invoke_test_hook(ExecutorFailpoint::UpdateOneAfterRead);
                match self.write_document(
                    collection,
                    snapshot,
                    user_key,
                    Some(old_doc.clone()),
                    new_doc.clone(),
                    new_doc_bytes,
                ) {
                    Ok(_) => doc! { "matched_count": 1, "modified_count": 1 },
                    Err(e) => {
                        on_version_conflict(e, &start_time, &mut attempt)?;
                        continue;
                    }
                }
            } else if upsert {
                let (new_doc, upserted_id) = self.perform_upsert(&query, update, parameters)?;
                let user_key = upserted_id.clone().try_into_key()?;

                let new_doc_bytes = serialize_to_vec(&new_doc)?;
                #[cfg(test)]
                self.invoke_test_hook(ExecutorFailpoint::UpdateOneUpsertAfterNoMatch);
                match self.write_document(
                    collection,
                    snapshot,
                    user_key,
                    None,
                    new_doc.clone(),
                    new_doc_bytes,
                ) {
                    Ok(_) => {
                        doc! { "matched_count": 0, "modified_count": 0, "upserted_id": upserted_id }
                    }
                    Err(e) => {
                        on_version_conflict(e, &start_time, &mut attempt)?;
                        continue;
                    }
                }
            } else {
                doc! { "matched_count": 0, "modified_count": 0 }
            };

            return Ok(Box::new(std::iter::once(Ok(result_doc))));
        }
    }

    fn perform_delete_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        let start_time = Instant::now();
        let mut attempt = 0;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter =
                self.execute_cached_at_snapshot(query.clone(), &parameters, Some(snapshot))?;

            let result_doc = if let Some(doc_result) = iter.next() {
                let old_doc = doc_result?;
                let user_key = old_doc.get("_id").unwrap().try_into_key()?;

                #[cfg(test)]
                self.invoke_test_hook(ExecutorFailpoint::DeleteOneAfterRead);
                match self.delete_document(collection, snapshot, user_key, &old_doc) {
                    Ok(_) => doc! { "deleted_count": 1 },
                    Err(e) => {
                        on_version_conflict(e, &start_time, &mut attempt)?;
                        continue;
                    }
                }
            } else {
                doc! { "deleted_count": 0 }
            };

            return Ok(Box::new(std::iter::once(Ok(result_doc))));
        }
    }

    fn perform_insert_one(&self, collection: u32, document: Vec<u8>) -> Result<QueryOutput> {
        let mut doc = document;
        let id_strategy = self.get_id_creation_strategy(collection);
        let id = self.ensure_id(&mut doc, &id_strategy)?;
        let user_key = id.try_into_key()?;

        let mut operations = Vec::new();
        let mut count_stats = CountStatsBuilder::new();
        operations.push(Operation::new_put(
            collection,
            0,
            user_key.clone(),
            doc.clone(),
        ));

        let indices = self.indices(collection);
        let raw_doc = RawDocument::from_bytes(&doc)?;
        count_stats.inc_collection(collection, 1);
        indices.append_put_ops_raw(&mut operations, raw_doc, &mut count_stats)?;

        // For Generated strategy, IDs are guaranteed unique, so skip precondition checks.
        let batch = if id_strategy == IdCreationStrategy::Generated {
            WriteBatch::new(operations, count_stats.build())
        } else {
            let snapshot = self.storage_engine.last_visible_sequence();

            // check for duplicate key.
            if Self::primary_key_exists(&self.storage_engine, collection, &user_key, snapshot)? {
                return Err(Self::duplicate_key_error(&id));
            }

            let precondition = Precondition::VersionMatch {
                collection,
                index: 0,
                user_key,
            };
            #[cfg(test)]
            self.invoke_test_hook(ExecutorFailpoint::InsertManualAfterPreflightBeforeWrite);
            WriteBatch::new_with_preconditions(
                operations,
                Preconditions::new(snapshot, vec![precondition]),
                count_stats.build(),
            )
        };

        self.storage_engine.write(batch).map_err(|e| match e {
            StorageError::VersionConflict { .. } => Self::duplicate_key_error(&id),
            _ => e.into(),
        })?;

        Ok(Box::new(std::iter::once(Ok(doc! { "inserted_id": id }))))
    }

    fn perform_insert_many(&self, collection: u32, documents: Vec<Vec<u8>>) -> Result<QueryOutput> {
        if documents.is_empty() {
            return Ok(Box::new(std::iter::once(Ok(
                doc! { "inserted_ids": Bson::Array(vec![]) },
            ))));
        }

        let id_strategy = self.get_id_creation_strategy(collection);

        let mut documents_with_ids: Vec<(Vec<u8>, Bson, Vec<u8>)> =
            Vec::with_capacity(documents.len());

        // generate IDs and prepare data.
        for mut doc in documents {
            let id = self.ensure_id(&mut doc, &id_strategy)?;
            let user_key = id.try_into_key()?;
            documents_with_ids.push((doc, id, user_key));
        }

        let indices = self.indices(collection);

        // For Generated strategy, IDs are guaranteed unique, so skip duplicate checks.
        let (batch, ids, seen_keys) = if id_strategy == IdCreationStrategy::Generated {
            let mut operations = Vec::new();
            let mut count_stats = CountStatsBuilder::new();
            let mut ids = Vec::with_capacity(documents_with_ids.len());
            for (doc, id, user_key) in documents_with_ids {
                ids.push(id);
                count_stats.inc_collection(collection, 1);
                operations.push(Operation::new_put(
                    collection,
                    0,
                    user_key.clone(),
                    doc.clone(),
                ));
                let raw_doc = RawDocument::from_bytes(&doc)?;
                indices.append_put_ops_raw(&mut operations, raw_doc, &mut count_stats)?;
            }
            (WriteBatch::new(operations, count_stats.build()), ids, None)
        } else {
            let snapshot = self.storage_engine.last_visible_sequence();

            // checks for duplicates (both within the batch and against storage).
            // This mimics `ordered: true` behavior, failing on the first error.
            let mut seen_keys = HashMap::new();
            for (_, id, user_key) in &documents_with_ids {
                if seen_keys.insert(user_key.clone(), id.clone()).is_some()
                    || Self::primary_key_exists(
                        &self.storage_engine,
                        collection,
                        user_key,
                        snapshot,
                    )?
                {
                    return Err(Self::duplicate_key_error(id));
                }
            }

            // build operations if all checks passed.
            let mut operations = Vec::new();
            let mut count_stats = CountStatsBuilder::new();
            let mut preconditions_vec = Vec::with_capacity(documents_with_ids.len());
            let mut ids = Vec::with_capacity(documents_with_ids.len());
            for (doc, id, user_key) in documents_with_ids {
                count_stats.inc_collection(collection, 1);
                preconditions_vec.push(Precondition::VersionMatch {
                    collection,
                    index: 0,
                    user_key: user_key.clone(),
                });
                operations.push(Operation::new_put(
                    collection,
                    0,
                    user_key.clone(),
                    doc.clone(),
                ));
                let raw_doc = RawDocument::from_bytes(&doc)?;
                indices.append_put_ops_raw(&mut operations, raw_doc, &mut count_stats)?;
                ids.push(id);
            }

            (
                WriteBatch::new_with_preconditions(
                    operations,
                    Preconditions::new(snapshot, preconditions_vec),
                    count_stats.build(),
                ),
                ids,
                Some(seen_keys),
            )
        };

        #[cfg(test)]
        if id_strategy != IdCreationStrategy::Generated {
            self.invoke_test_hook(ExecutorFailpoint::InsertManualAfterPreflightBeforeWrite);
        }

        if let Err(e) = self.storage_engine.write(batch) {
            match e {
                StorageError::VersionConflict {
                    user_key: conflicting_key,
                    ..
                } => {
                    // A key was inserted concurrently.
                    let id = seen_keys.as_ref().unwrap().get(&conflicting_key).unwrap();
                    Err(Self::duplicate_key_error(&id))
                }
                _ => Err(e.into()),
            }
        } else {
            let result =
                doc! { "inserted_ids": ids.into_iter().map(Bson::from).collect::<Vec<_>>() };
            Ok(Box::new(std::iter::once(Ok(result))))
        }
    }

    fn duplicate_key_error(id: &Bson) -> Error {
        Error::InvalidRequest(format!("Duplicate key error. dup key: {{ _id: {} }}", id))
    }

    fn primary_key_exists(
        storage_engine: &StorageEngine,
        collection: u32,
        user_key: &[u8],
        snapshot: u64,
    ) -> Result<bool> {
        Ok(storage_engine
            .read(collection, 0, user_key, Some(snapshot))?
            .is_some_and(|(internal_key, _)| {
                extract_operation_type(&internal_key) != OperationType::Delete
            }))
    }

    /// Returns the `IdCreationStrategy` for the given collection.
    fn get_id_creation_strategy(&self, collection: u32) -> IdCreationStrategy {
        self.storage_engine
            .catalog()
            .get_collection_by_id(&collection)
            .map(|meta| meta.options.id_creation_strategy.clone())
            .unwrap_or_default()
    }

    /// Ensures that the document has an `_id` field according to the collection's strategy.
    ///
    /// - `Generated`: Always generates an ID, fails if one is already present.
    /// - `Manual`: Requires the user to provide an ID, fails if missing.
    /// - `Mixed`: Generates an ID if missing, uses the provided one otherwise.
    fn ensure_id(&self, doc: &mut Vec<u8>, strategy: &IdCreationStrategy) -> Result<Bson> {
        let existing_id = RawDocument::from_bytes(doc)?.get("_id")?;

        match *strategy {
            IdCreationStrategy::Generated => {
                if existing_id.is_some() {
                    return Err(Error::InvalidRequest(
                        "Cannot specify _id for a collection with generated IDs".to_string(),
                    ));
                }
                let bson = self.generate_id();
                bson_utils::prepend_field(doc, "_id", &bson)?;
                Ok(bson)
            }
            IdCreationStrategy::Manual => match existing_id {
                Some(id) => Ok(Bson::try_from(id)?),
                None => Err(Error::InvalidRequest(
                    "Document must contain an _id field for this collection".to_string(),
                )),
            },
            IdCreationStrategy::Mixed => match existing_id {
                Some(id) => Ok(Bson::try_from(id)?),
                None => {
                    let bson = self.generate_id();
                    bson_utils::prepend_field(doc, "_id", &bson)?;
                    Ok(bson)
                }
            },
        }
    }

    fn generate_id(&self) -> Bson {
        let new_id = self.id_generator.lock().unwrap().next_id().unwrap();
        let bson = Bson::Int64(
            i64::try_from(new_id.to_u64()).expect("Sonyflake IDs must fit into signed 64-bit BSON"),
        );
        bson
    }

    fn perform_multi_point_search(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        keys: &Arc<Expr>,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

        let keys_values = Self::bind_parameter(keys, &parameters);
        let keys_array = if let BsonValue(Bson::Array(arr)) = keys_values {
            arr
        } else {
            unreachable!(
                "Expected array for MultiPointSearch keys, got {:?}",
                keys_values
            );
        };

        let mut keys_as_bson_values: Vec<BsonValue> =
            keys_array.into_iter().map(BsonValue).collect();

        // Sort keys to ensure consistent order for storage engine lookups
        keys_as_bson_values.sort();

        let key_iterator: Box<dyn Iterator<Item = BsonValue>> = if *direction == Direction::Reverse
        {
            Box::new(keys_as_bson_values.into_iter().rev())
        } else {
            Box::new(keys_as_bson_values.into_iter())
        };

        let storage_engine = self.storage_engine.clone();
        let collection = *collection;

        let iter = key_iterator.filter_map(move |key| match key.try_into_key() {
            Ok(storage_key) => {
                match storage_engine.read(collection, 0, &storage_key, snapshot) {
                    Ok(Some((k, v))) => {
                        let op = extract_operation_type(&k);
                        if op == OperationType::Put {
                            match Document::from_reader(Cursor::new(v)) {
                                Ok(doc) => {
                                    if filter.as_ref().map_or(true, |f| f(&doc)) {
                                        Some(Ok(doc))
                                    } else {
                                        None
                                    }
                                }
                                Err(e) => Some(Err(e.into())),
                            }
                        } else {
                            None // Deleted
                        }
                    }
                    Ok(None) => None, // Not found
                    Err(e) => Some(Err(e.into())),
                }
            }
            Err(e) => Some(Err(e.into())),
        });

        Ok(Box::new(iter))
    }

    fn perform_point_search(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        key: &Arc<Expr>,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

        let key = Self::bind_key_parameter(key, &parameters)?;
        let result = self.storage_engine.read(*collection, 0, &key, snapshot)?;
        let iter: QueryOutput = match result {
            Some((k, v)) => {
                let op = extract_operation_type(&k);
                match op {
                    OperationType::Delete => Box::new(std::iter::empty()),
                    OperationType::Put => {
                        let result = Document::from_reader(Cursor::new(v));

                        if result.is_err() {
                            return Ok(Box::new(std::iter::once(result.map_err(|e| e.into()))));
                        }

                        let doc = result?;

                        match &filter {
                            Some(filter) => {
                                if filter(&doc) {
                                    Box::new(std::iter::once(Ok(doc)))
                                } else {
                                    Box::new(std::iter::empty())
                                }
                            }
                            None => Box::new(std::iter::once(Ok(doc))),
                        }
                    }
                    _ => unreachable!("Unexpected operation type: {:?}", op),
                }
            }
            None => Box::new(std::iter::empty()),
        };
        Ok(iter)
    }

    fn perform_collection_scan(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        range: &Interval<Arc<Expr>>,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let range = Self::bind_key_range_parameters(range, &parameters)?;

        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

        Ok(Box::new(
            self.storage_engine
                .range_scan(
                    *collection,
                    0, // This is table scan so index is 0
                    &range,
                    snapshot,
                    direction.clone(),
                )?
                .filter_map(move |res| {
                    let doc = match res {
                        Ok((k, v)) => {
                            let op = extract_operation_type(&k);
                            match op {
                                OperationType::Delete => return None,
                                OperationType::Put => {
                                    // Deserialize the value into a Document
                                    let doc = Document::from_reader(Cursor::new(v));
                                    match doc {
                                        Err(e) => return Some(Err(e.into())),
                                        Ok(doc) => doc,
                                    }
                                }
                                _ => unreachable!("Unexpected operation type: {:?}", op),
                            }
                        }
                        Err(e) => return Some(Err(e.into())),
                    };

                    match &filter {
                        Some(filter) => {
                            if filter(&doc) {
                                Some(Ok(doc))
                            } else {
                                None
                            }
                        }
                        None => Some(Ok(doc)),
                    }
                }),
        ))
    }

    fn perform_index_scan(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        index: &u32,
        range: &IndexScanRangeExpr,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let index_metadata = self
            .storage_engine
            .catalog()
            .get_collection_by_id(collection)
            .unwrap()
            .get_index_by_id(*index)
            .unwrap();
        let index_codec = Index::from(*collection, &index_metadata);
        let bound_range = index_codec.bind_range_expr(range, parameters)?;

        let filter = filter
            .clone()
            .map(|predicate| filters::to_filter(predicate, parameters));

        let storage_engine = self.storage_engine.clone();
        let collection = *collection;
        let secondary_index = *index;

        Ok(Box::new(
            storage_engine
                .range_scan(
                    collection,
                    secondary_index,
                    &bound_range,
                    snapshot,
                    direction.clone(),
                )?
                .filter_map(move |res| {
                    let primary_key = match res {
                        Ok((k, v)) => {
                            let op = extract_operation_type(&k);
                            match op {
                                OperationType::Delete => return None,
                                OperationType::Put => {
                                    let user_key = extract_record_key(&k);
                                    match Index::extract_id_from_entry_bytes(user_key, &v) {
                                        Ok(id) => id.to_vec(),
                                        Err(e) => return Some(Err(e.into())),
                                    }
                                }
                                _ => unreachable!("Unexpected operation type: {:?}", op),
                            }
                        }
                        Err(e) => return Some(Err(e.into())),
                    };

                    let doc_bytes = match storage_engine.read(collection, 0, &primary_key, snapshot)
                    {
                        Ok(Some((k, v))) => {
                            let op = extract_operation_type(&k);
                            match op {
                                OperationType::Delete => return None,
                                OperationType::Put => v,
                                _ => unreachable!("Unexpected operation type: {:?}", op),
                            }
                        }
                        Ok(None) => return None,
                        Err(e) => return Some(Err(e.into())),
                    };

                    let doc = match Document::from_reader(Cursor::new(doc_bytes)) {
                        Ok(doc) => doc,
                        Err(e) => return Some(Err(e.into())),
                    };

                    match &filter {
                        Some(filter) if !filter(&doc) => None,
                        _ => Some(Ok(doc)),
                    }
                }),
        ))
    }

    /// Builds the upsert updater, creates the new document from the query's equality
    /// conditions and the update expression, and returns `(new_doc, id)`.
    fn perform_upsert(
        &self,
        query: &Arc<PhysicalPlan>,
        update: &UpdateExpr,
        parameters: &Parameters,
    ) -> Result<(Document, Bson)> {
        let updater = updates::to_updater(update, true)?;
        self.create_upsert_document(query, parameters, &updater)
    }

    /// Creates a new document for an upsert operation by:
    /// 1. Extracting equality conditions from the query to build a base document
    /// 2. Applying the update operations to the base document
    /// 3. Ensuring the document has an `_id` field
    fn create_upsert_document(
        &self,
        query: &PhysicalPlan,
        parameters: &Parameters,
        updater: &Box<dyn Fn(Document) -> Result<Document> + Send + Sync>,
    ) -> Result<(Document, Bson)> {
        let mut new_doc = self.create_base_document_from_query(query, parameters)?;
        new_doc = updater(new_doc)?;

        let id = if new_doc.contains_key("_id") {
            new_doc.get("_id").unwrap().clone()
        } else {
            let id = self.generate_id();
            new_doc.insert("_id", id.clone());
            id
        };

        Ok((new_doc, id))
    }

    /// Constructs a base document from equality conditions in the query plan.
    fn create_base_document_from_query(
        &self,
        query: &PhysicalPlan,
        parameters: &Parameters,
    ) -> Result<Document> {
        let mut doc = Document::new();
        self.extract_equality_conditions(query, parameters, &mut doc);
        Ok(doc)
    }

    /// Extracts equality conditions from a query plan into a document.
    fn extract_equality_conditions(
        &self,
        query: &PhysicalPlan,
        parameters: &Parameters,
        doc: &mut Document,
    ) {
        match query {
            PhysicalPlan::PointSearch { key, filter, .. } => {
                let BsonValue(id) = Self::bind_parameter(key, parameters);
                doc.insert("_id", id);
                if let Some(filter_expr) = filter {
                    self.extract_equality_from_expr(filter_expr, parameters, doc);
                }
            }
            PhysicalPlan::CollectionScan { filter, .. } => {
                if let Some(filter_expr) = filter {
                    self.extract_equality_from_expr(filter_expr, parameters, doc);
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                self.extract_equality_conditions(input, parameters, doc);
                self.extract_equality_from_expr(predicate, parameters, doc);
            }
            PhysicalPlan::Limit { input, .. } => {
                self.extract_equality_conditions(input, parameters, doc);
            }
            _ => {}
        }
    }

    /// Extracts equality conditions from an expression tree into a document.
    fn extract_equality_from_expr(&self, expr: &Expr, parameters: &Parameters, doc: &mut Document) {
        match expr {
            Expr::And(exprs) => {
                for e in exprs {
                    self.extract_equality_from_expr(e, parameters, doc);
                }
            }
            Expr::FieldFilters { field, filters } => {
                if let Expr::Field(path) = field.as_ref() {
                    for filter in filters {
                        if let Some(value) = self.extract_point_value(filter, parameters) {
                            set_path_value(doc, path, value.0);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Extracts a point (equality) value from an interval expression.
    fn extract_point_value(&self, expr: &Expr, parameters: &Parameters) -> Option<BsonValue> {
        match expr {
            Expr::Interval(interval) if interval.is_point() => interval
                .start_bound_value()
                .and_then(|e| self.resolve_expr_value(&e, parameters)),
            _ => None,
        }
    }

    /// Resolves a placeholder expression to a concrete Bson value.
    fn resolve_expr_value(&self, expr: &Expr, parameters: &Parameters) -> Option<BsonValue> {
        match expr {
            Expr::Placeholder(idx) => Some(parameters.get(*idx).clone()),
            _ => None,
        }
    }

    /// Writes a single document to storage as a put operation with a `VersionMatch` precondition.
    fn write_document(
        &self,
        collection: u32,
        snapshot: u64,
        user_key: Vec<u8>,
        old_doc: Option<Document>,
        new_doc: Document,
        new_doc_bytes: Vec<u8>,
    ) -> std::result::Result<(), StorageError> {
        let mut operations = Vec::new();
        let mut count_stats = CountStatsBuilder::new();

        let indices = self.indices(collection);

        if let Some(doc) = old_doc.as_ref() {
            indices.append_delete_ops(&mut operations, doc, &mut count_stats)?;
        }

        operations.push(Operation::new_put(
            collection,
            0,
            user_key.clone(),
            new_doc_bytes,
        ));

        if old_doc.is_none() {
            count_stats.inc_collection(collection, 1);
        }
        indices.append_put_ops(&mut operations, &new_doc, &mut count_stats)?;

        let precondition = Precondition::VersionMatch {
            collection,
            index: 0,
            user_key,
        };
        let batch = WriteBatch::new_with_preconditions(
            operations,
            Preconditions::new(snapshot, vec![precondition]),
            count_stats.build(),
        );
        self.storage_engine.write(batch)
    }

    /// Deletes a single document from storage with a `VersionMatch` precondition.
    fn delete_document(
        &self,
        collection: u32,
        snapshot: u64,
        user_key: Vec<u8>,
        old_doc: &Document,
    ) -> std::result::Result<(), StorageError> {
        let mut operations = Vec::new();
        let mut count_stats = CountStatsBuilder::new();

        let indices = self.indices(collection);
        indices.append_delete_ops(&mut operations, old_doc, &mut count_stats)?;
        operations.push(Operation::new_delete(collection, 0, user_key.clone()));
        count_stats.inc_collection(collection, -1);

        let precondition = Precondition::VersionMatch {
            collection,
            index: 0,
            user_key,
        };
        let batch = WriteBatch::new_with_preconditions(
            operations,
            Preconditions::new(snapshot, vec![precondition]),
            count_stats.build(),
        );
        self.storage_engine.write(batch)
    }

    fn indices(&self, collection: u32) -> Indexes {
        let metadata = self
            .storage_engine
            .catalog()
            .get_collection_by_id(&collection)
            .unwrap();
        Indexes::from_collection(&metadata)
    }

    fn bind_key_range_parameters(
        range: &Interval<Arc<Expr>>,
        parameters: &Parameters,
    ) -> Result<Interval<Vec<u8>>> {
        let start = Self::bind_key_bound_parameter(range.start_bound(), &parameters)?;
        let end = Self::bind_key_bound_parameter(range.end_bound(), &parameters)?;
        let range = Interval::new(start, end);
        Ok(range)
    }

    #[cfg(test)]
    fn invoke_test_hook(&self, point: ExecutorFailpoint) {
        if let Some(test_hook) = &self.test_hook {
            test_hook.hit(point);
        }
    }

    fn bind_key_bound_parameter(
        start: Bound<&Arc<Expr>>,
        parameters: &Parameters,
    ) -> Result<Bound<Vec<u8>>> {
        let start = match start {
            Bound::Included(b) => {
                Bound::Included(Self::bind_parameter(b, &parameters).try_into_key()?)
            }
            Bound::Excluded(b) => {
                Bound::Excluded(Self::bind_parameter(b, &parameters).try_into_key()?)
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(start)
    }

    fn bind_key_parameter(expr: &Expr, parameters: &Parameters) -> Result<Vec<u8>> {
        if let Expr::Placeholder(idx) = expr {
            Ok(parameters.get(*idx).try_into_key()?)
        } else {
            unreachable!("Expecting placeholder but was: {:?}", expr);
        }
    }

    fn bind_parameter(expr: &Expr, parameters: &Parameters) -> BsonValue {
        if let Expr::Placeholder(idx) = expr {
            parameters.get(*idx).clone()
        } else {
            unreachable!("Expecting placeholder but was: {:?}", expr)
        }
    }
}

/// Handles a storage write error inside a retry loop.
///
/// If the error is a `VersionConflict` and the deadline has not been reached,
/// sleeps for the appropriate backoff duration, increments `attempt`, and returns
/// `Ok(())` so the caller can `continue` to the next iteration.
///
/// Otherwise, returns the error converted to [`Error`].
fn on_version_conflict(e: StorageError, start_time: &Instant, attempt: &mut u32) -> Result<()> {
    match e {
        StorageError::VersionConflict { .. } => {
            if start_time.elapsed() >= Duration::from_secs(5) {
                return Err(e.into());
            }
            std::thread::sleep(calculate_backoff(*attempt));
            *attempt += 1;
            Ok(())
        }
        _ => Err(e.into()),
    }
}

/// Calculates an exponential backoff duration.
fn calculate_backoff(attempt: u32) -> Duration {
    let base_delay = Duration::from_millis(5);
    let max_delay = Duration::from_secs(1);

    // Exponential backoff: base * 2^attempt
    let mut backoff = base_delay.as_millis() as u64 * 2_u64.pow(attempt);
    if backoff > max_delay.as_millis() as u64 {
        backoff = max_delay.as_millis() as u64;
    }

    Duration::from_millis(backoff)
}

#[cfg(test)]
#[path = "executor/tests/mod.rs"]
mod tests;
