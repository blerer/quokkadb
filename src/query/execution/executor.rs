use crate::error::{Error, Result};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::{BsonValue, Expr, Parameters};
use crate::query::execution::{filters, projections, sorts, updates};
use crate::storage::Direction;
use crate::storage::internal_key::extract_operation_type;
use crate::storage::operation::{Operation, OperationType};
use crate::storage::storage_engine::{StorageEngine, StorageError};
use crate::storage::write_batch::{Precondition, Preconditions, WriteBatch};
use crate::util::bson_utils::{self, BsonKey};
use crate::util::interval::Interval;
use bson::{doc, Bson, Document, RawDocument};
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};
use crate::query::update::UpdateExpr;

pub type QueryOutput = Box<dyn Iterator<Item = Result<Document>>>;

/// Executes a physical query plan.
pub struct QueryExecutor {
    storage_engine: Arc<StorageEngine>,
    id_generator: Mutex<Sonyflake>,
}

impl QueryExecutor {
    /// Creates a new `QueryExecutor`.
    pub fn new(storage_engine: Arc<StorageEngine>) -> Self {
        Self {
            storage_engine,
            id_generator: Mutex::new(Sonyflake::new().unwrap()),
        }
    }

    pub fn execute_direct(&self, plan: PhysicalPlan, parameters: Option<Parameters>) -> Result<QueryOutput> {
        match plan {
            PhysicalPlan::InsertMany { collection, documents, } => {
                self.perform_insert_many(collection, documents)
            }
            PhysicalPlan::InsertOne { collection, document, } => {
                self.perform_insert_one(collection, document)
            }
            PhysicalPlan::UpdateOne { collection, query, update } => {
                let parameters = parameters.expect("Parameters must be provided for UpdateOne");

                self.perform_update_one(collection, query, &update, &parameters)
            }
            PhysicalPlan::UpdateMany { collection, query, update } => {
                let parameters = parameters.expect("Parameters must be provided for UpdateMany");

                self.perform_update_many(collection, query, &update, &parameters)
            }
            _ => {
                // Other plans, should be cached
                panic!("Direct execution not supported for plan: {:?}", plan);
            }
        }
    }

    /// Executes the given physical plan using the latest visible data.
    pub fn execute_cached(&self, plan: Arc<PhysicalPlan>, parameters: &Parameters) -> Result<QueryOutput> {
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
            } => {
                self.perform_collection_scan(&parameters, snapshot, collection, range, direction, filter)
            }
            PhysicalPlan::PointSearch {
                collection,
                key,
                filter,
                projection: _,
            } => {
                self.perform_point_search(&parameters, snapshot, collection, key, filter)
            }
            PhysicalPlan::IndexScan {
                collection: _,
                index: _,
                range: _,
                filter: _,
                projection: _,
            } => {
                todo!()
            }
            PhysicalPlan::MultiPointSearch {
                collection,
                keys,
                direction,
                filter,
                projection: _,
            } => {
                self.perform_multi_point_search(&parameters, snapshot, collection, keys, direction, filter)
            }
            PhysicalPlan::Filter { input, predicate } => {
                let filter = filters::to_filter(predicate.clone(), &parameters);
                let input_iter = self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(input_iter.filter(move |res| {
                    if res.is_err() { true } else { filter(res.as_ref().unwrap()) }
                })))
            }
            PhysicalPlan::Projection { input, projection } => {
                let projector = projections::to_projector(projection, &parameters)?;
                let input_iter = self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(input_iter.map(move |res| {
                    res.and_then(|doc| projector(doc))
                })))
            }
            PhysicalPlan::InMemorySort {
                input,
                sort_fields,
            } => {
                let input_iter = self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                sorts::in_memory_sort(input_iter, &sort_fields)
            }
            PhysicalPlan::ExternalMergeSort {
                input,
                sort_fields,
                max_in_memory_rows,
            } => {
                let input_iter = self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                sorts::external_merge_sort(input_iter, sort_fields.clone(), *max_in_memory_rows)
            }
            PhysicalPlan::TopKHeapSort {
                input,
                sort_fields,
                k,
            } => {
                let input_iter = self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                sorts::top_k_heap_sort(input_iter, sort_fields.clone(), *k)
            }
            PhysicalPlan::Limit {
                input,
                skip,
                limit,
            } => {
                let mut iter = self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                if let Some(s) = skip {
                    iter = Box::new(iter.skip(*s));
                }
                if let Some(l) = limit {
                    iter = Box::new(iter.take(*l));
                }
                Ok(iter)
            }
            _ => {
                panic!("Non-parametrized physical plan: {:?}", plan);
            }
        }
    }

    fn perform_update_many(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        let updater = updates::to_updater(&update)?;

        let snapshot = self.storage_engine.last_visible_sequence();
        let iter =
            self.execute_cached_at_snapshot(query.clone(), &parameters, Some(snapshot))?;

        let mut operations = Vec::new();
        let mut preconditions = Vec::new();
        let mut matched_count = 0;
        let mut modified_count = 0;

        for doc_result in iter {
            let doc = doc_result?;
            matched_count += 1;
            let new_doc = updater(doc)?;

            let user_key = new_doc.get("_id").unwrap().try_into_key()?;

            operations.push(Operation::new_put(
                collection,
                0,
                user_key.clone(),
                bson::to_vec(&new_doc)?,
            ));
            preconditions.push(Precondition::MustNotExist {
                collection,
                index: 0,
                user_key,
            });
            modified_count += 1;
        }

        if operations.is_empty() {
            let result = doc! { "matched_count": matched_count, "modified_count": 0 };
            return Ok(Box::new(std::iter::once(Ok(result))));
        }

        let batch = WriteBatch::new_with_preconditions(
            operations,
            Preconditions::new(snapshot, preconditions),
        );
        self.storage_engine.write(batch)?;

        let result = doc! { "matched_count": matched_count, "modified_count": modified_count };
        Ok(Box::new(std::iter::once(Ok(result))))
    }

    fn perform_update_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        const MAX_RETRY_DURATION: Duration = Duration::from_secs(5);
        let start_time = Instant::now();
        let mut attempt = 0;

        let updater = updates::to_updater(&update)?;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter =
                self.execute_cached_at_snapshot(query.clone(), &parameters, Some(snapshot))?;

            let result_doc = if let Some(doc_result) = iter.next() {
                let doc = doc_result?;
                let new_doc = updater(doc)?;

                let user_key = new_doc.get("_id").unwrap().try_into_key()?;

                let operation = Operation::new_put(
                    collection,
                    0,
                    user_key.clone(),
                    bson::to_vec(&new_doc)?,
                );

                let precondition = Precondition::MustNotExist {
                    collection,
                    index: 0,
                    user_key,
                };

                let preconditions = Preconditions::new(snapshot, vec![precondition]);
                let write_batch =
                    WriteBatch::new_with_preconditions(vec![operation], preconditions);

                match self.storage_engine.write(write_batch) {
                    Ok(_) => doc! { "matched_count": 1, "modified_count": 1 },
                    Err(e @ StorageError::VersionConflict { .. }) => {
                        if start_time.elapsed() >= MAX_RETRY_DURATION {
                            return Err(e.into());
                        }
                        std::thread::sleep(calculate_backoff(attempt));
                        attempt += 1;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            } else {
                doc! { "matched_count": 0, "modified_count": 0 }
            };

            return Ok(Box::new(std::iter::once(Ok(result_doc))));
        }
    }

    fn perform_insert_one(&self, collection: u32, document: Vec<u8>) -> Result<QueryOutput> {
        let mut doc = document;
        let id = self.prepend_id_if_needed(&mut doc)?;
        let user_key = id.try_into_key()?;

        let snapshot = self.storage_engine.last_visible_sequence();

        // check for duplicate key.
        if self.storage_engine.read(collection, 0, &user_key, None)?.is_some() {
            return Err(Self::duplicate_key_error(&id));
        }

        let operation = Operation::new_put(collection, 0, user_key.clone(), doc);
        let precondition = Precondition::MustNotExist {
            collection,
            index: 0,
            user_key,
        };
        let preconditions = Preconditions::new(
            snapshot,
            vec![precondition],
        );
        let batch =
            WriteBatch::new_with_preconditions(vec![operation], preconditions);

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

        let mut documents_with_ids: Vec<(Vec<u8>, Bson, Vec<u8>)> =
            Vec::with_capacity(documents.len());

        // generate IDs and prepare data.
        for mut doc in documents {
            let id = self.prepend_id_if_needed(&mut doc)?;
            let user_key = id.try_into_key()?;
            documents_with_ids.push((doc, id, user_key));
        }

        let snapshot = self.storage_engine.last_visible_sequence();

        // checks for duplicates (both within the batch and against storage).
        // This mimics `ordered: true` behavior, failing on the first error.
        let mut seen_keys = HashMap::new();
        for (_, id, user_key) in &documents_with_ids {
            if seen_keys.insert(user_key.clone(), id.clone()).is_some()
                || self.storage_engine.read(collection, 0, user_key, Some(snapshot))?.is_some() {
                return Err(Self::duplicate_key_error(id));
            }
        }

        // build operations if all checks passed.
        let mut operations = Vec::with_capacity(documents_with_ids.len());
        let mut preconditions = Vec::with_capacity(documents_with_ids.len());
        let mut ids = Vec::with_capacity(documents_with_ids.len());
        for (doc, id, user_key) in documents_with_ids {
            ids.push(id);
            preconditions.push(Precondition::MustNotExist {
                collection,
                index: 0,
                user_key: user_key.clone(),
            });
            operations.push(Operation::new_put(collection, 0, user_key, doc));
        }

        let preconditions = Preconditions::new(snapshot, preconditions);
        let batch = WriteBatch::new_with_preconditions(operations, preconditions);
        if let Err(e) = self.storage_engine.write(batch) {
            match e {
                StorageError::VersionConflict { user_key: conflicting_key, .. } => {
                    // A key was inserted concurrently.
                    let id = seen_keys.get(&conflicting_key).unwrap();
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
        Error::InvalidRequest(format!(
            "Duplicate key error. dup key: {{ _id: {} }}",
            id
        ))
    }

    /// Ensures that each document has an `_id` field, prepending it if necessary.
    fn prepend_id_if_needed(&self, mut doc: &mut Vec<u8>) -> Result<Bson> {
        let id = RawDocument::from_bytes(&doc)?.get("_id")?;

        let id: Bson = match id {
            Some(id) => id.to_raw_bson().try_into()?,
            None => {
                let new_id = self.id_generator.lock().unwrap().next_id().unwrap();
                let bson = Bson::Int64(new_id as i64);
                bson_utils::prepend_field(&mut doc, "_id", &bson)?;
                bson
            }
        };
        Ok(id)
    }

    fn perform_multi_point_search(&self,
                                  parameters: &Parameters,
                                  snapshot: Option<u64>,
                                  collection: &u32,
                                  keys: &Arc<Expr>,
                                  direction: &Direction,
                                  filter: &Option<Arc<Expr>>
    ) -> Result<QueryOutput> {

        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

        let keys_values = Self::bind_parameter(keys, &parameters);
        let keys_array = if let BsonValue(Bson::Array(arr)) = keys_values {
            arr
        } else {
            panic!("Expected array for MultiPointSearch keys, got {:?}", keys_values);
        };

        let mut keys_as_bson_values: Vec<BsonValue> =
            keys_array.into_iter().map(BsonValue).collect();

        // Sort keys to ensure consistent order for storage engine lookups
        keys_as_bson_values.sort();

        let key_iterator: Box<dyn Iterator<Item=BsonValue>> =
            if *direction == Direction::Reverse {
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

    fn perform_point_search(&self,
                            parameters: &Parameters,
                            snapshot: Option<u64>,
                            collection: &u32,
                            key: &Arc<Expr>,
                            filter: &Option<Arc<Expr>>
    ) -> Result<QueryOutput> {

        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter.clone().and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

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
                            },
                            None => Box::new(std::iter::once(Ok(doc)))
                        }
                    }
                    _ => panic!("Unexpected operation type: {:?}", op),
                }
            }
            None => Box::new(std::iter::empty()),
        };
        Ok(iter)
    }

    fn perform_collection_scan(&self,
                               parameters: &Parameters,
                               snapshot: Option<u64>,
                               collection: &u32,
                               range: &Interval<Arc<Expr>>,
                               direction: &Direction,
                               filter: &Option<Arc<Expr>>
    ) -> Result<QueryOutput> {

        let range = Self::bind_key_range_parameters(range, &parameters)?;

        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter.clone().and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

        Ok(Box::new(self.storage_engine.range_scan(
            *collection,
            0, // This is table scan so index is 0
            &range,
            snapshot,
            direction.clone(),
        )?.filter_map(move |res| {
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
                                Ok(doc) => doc
                            }
                        },
                        _ => panic!("Unexpected operation type: {:?}", op),
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
                },
                None => Some(Ok(doc))
            }
        })))
    }

    fn bind_key_range_parameters(range: &Interval<Arc<Expr>>, parameters: &Parameters) -> Result<Interval<Vec<u8>>> {
        let start = Self::bind_key_bound_parameter(range.start_bound(), &parameters)?;
        let end = Self::bind_key_bound_parameter(range.end_bound(), &parameters)?;
        let range = Interval::new(start, end);
        Ok(range)
    }

    fn bind_key_bound_parameter(start: Bound<&Arc<Expr>>, parameters: &Parameters) -> Result<Bound<Vec<u8>>> {
        let start = match start {
            Bound::Included(b) => Bound::Included(Self::bind_parameter(b, &parameters).try_into_key()?),
            Bound::Excluded(b) => Bound::Excluded(Self::bind_parameter(b, &parameters).try_into_key()?),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(start)
    }

    fn bind_key_parameter(expr: &Expr, parameters: &Parameters) -> Result<Vec<u8>> {
        if let Expr::Placeholder(idx) = expr {
            Ok(parameters.get(*idx).try_into_key()?)
        } else {
            panic!("Expecting placeholder but was: {:?}", expr);
        }
    }

    fn bind_parameter(expr: &Expr, parameters: &Parameters) -> BsonValue {
        if let Expr::Placeholder(idx) = expr {
            parameters.get(*idx).clone()
        } else {
            panic!("Expecting placeholder but was: {:?}", expr)
        }
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
mod tests {
    use super::*;
    use crate::error::{Error, Result};
    use crate::query::{make_sort_field, SortOrder};
    use crate::query::{BsonValue, Expr, Parameters, Projection};
    use crate::storage::operation::Operation;
    use crate::storage::test_utils::storage_engine;
    use crate::storage::write_batch::WriteBatch;
    use crate::storage::Direction;
    use bson::{doc, Bson, Document};
    use std::sync::Arc;
    use crate::query::expr_fn::{all, and, elem_match, exists, field, field_filters, has_type, ne, nor, not, or, size, within, proj_array_elements, proj_elem_match, proj_field, proj_fields, proj_slice, interval, point, greater_than, less_than, at_most, at_least};
    use crate::query::update_fn::{field_name, set, update};

    #[test]
    fn test_insert_duplicate_key_preflight_check() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_duplicates")?;

        // 2. Insert a document with a known ID
        let doc1 = doc! { "_id": 1_i32, "name": "doc1" };
        let insert_one_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&doc1)?,
        };
        executor.execute_direct(insert_one_plan, None)?.count(); // Consume iterator

        // 3. Try to insert another document with the same ID
        let doc1_dup = doc! { "_id": 1_i32, "name": "doc1_dup" };
        let insert_dup_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&doc1_dup)?,
        };
        let result = executor.execute_direct(insert_dup_plan, None);
        match result {
            Err(Error::InvalidRequest(msg)) => {
                assert!(msg.starts_with("Duplicate key error"));
                assert!(msg.contains("_id: 1"));
            }
            Err(err) => panic!("Expected InvalidRequest for duplicate key, got {:?}", err),
            Ok(_) => panic!("Expected error for duplicate key, got Ok"),
        }

        // 4. InsertMany with a duplicate within the batch
        let doc2 = doc! { "_id": 2_i32, "name": "doc2" };
        let doc2_dup = doc! { "_id": 2_i32, "name": "doc2_dup" };
        let insert_many_intra_batch_dup_plan = PhysicalPlan::InsertMany {
            collection: collection_id,
            documents: vec![bson::to_vec(&doc2)?, bson::to_vec(&doc2_dup)?],
        };
        let result_many_intra = executor.execute_direct(insert_many_intra_batch_dup_plan, None);
        match result_many_intra {
            Err(Error::InvalidRequest(msg)) => {
                assert!(msg.starts_with("Duplicate key error"));
                assert!(msg.contains("_id: 2"));
            }
            Err(err) => panic!("Expected InvalidRequest for duplicate key, got {:?}", err),
            Ok(_) => panic!("Expected error for duplicate key, got Ok"),
        }

        // 5. InsertMany with a duplicate that already exists in the collection
        let doc3 = doc! { "_id": 3_i32, "name": "doc3" };
        let insert_many_existing_dup_plan = PhysicalPlan::InsertMany {
            collection: collection_id,
            documents: vec![bson::to_vec(&doc3)?, bson::to_vec(&doc1)?], // doc1 has _id: 1
        };
        let result_many_existing = executor.execute_direct(insert_many_existing_dup_plan, None);
        match result_many_existing {
            Err(Error::InvalidRequest(msg)) => {
                assert!(msg.starts_with("Duplicate key error"));
                assert!(msg.contains("_id: 1"));
            }
            Err(err) => panic!("Expected InvalidRequest for duplicate key, got {:?}", err),
            Ok(_) => panic!("Expected error for duplicate key, got Ok"),
        }

        // 6. Verify that no partial insert happened from the failed InsertMany
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(Bson::Int32(3)));
        let point_search_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr,
            filter: None,
            projection: None,
        });
        let mut search_result = executor.execute_cached(point_search_plan, &params)?;
        assert!(
            search_result.next().is_none(),
            "Document with _id: 3 should not have been inserted"
        );

        Ok(())
    }

    #[test]
    fn test_execution_roundtrip() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test")?;

        // 2. InsertOne
        let doc1 = doc! { "name": "doc1", "value": 1 };
        let doc1_bytes = bson::to_vec(&doc1)?;
        let insert_one_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: doc1_bytes,
        };
        let mut insert_one_result = executor.execute_direct(insert_one_plan, None)?;
        let result_doc = insert_one_result.next().unwrap()?;
        assert!(insert_one_result.next().is_none());
        let inserted_id1 = result_doc.get("inserted_id").unwrap().clone();

        // 3. PointSearch for the inserted doc
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(inserted_id1.clone()));
        let point_search_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr.clone(),
            filter: None,
            projection: None,
        });

        let mut point_search_result = executor.execute_cached(point_search_plan, &params)?;
        let found_doc1 = point_search_result.next().unwrap()?;
        assert!(point_search_result.next().is_none());

        let mut expected_doc1 = doc1.clone();
        expected_doc1.insert("_id", inserted_id1.clone());
        assert_eq!(found_doc1, expected_doc1);

        // 4. InsertMany
        let doc2 = doc! { "name": "doc2", "value": 2 };
        let doc3 = doc! { "name": "doc3", "value": 3 };
        let insert_many_plan = PhysicalPlan::InsertMany {
            collection: collection_id,
            documents: vec![bson::to_vec(&doc2)?, bson::to_vec(&doc3)?],
        };

        let mut insert_many_result = executor.execute_direct(insert_many_plan, None)?;
        let result_doc_many = insert_many_result.next().unwrap()?;
        assert!(insert_many_result.next().is_none());
        let inserted_ids = result_doc_many.get_array("inserted_ids").unwrap();
        assert_eq!(inserted_ids.len(), 2);
        let inserted_id2 = inserted_ids[0].clone();
        let inserted_id3 = inserted_ids[1].clone();

        // 5. CollectionScan
        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });

        let scan_results = executor.execute_cached(scan_plan, &Parameters::new())?;
        let mut found_docs: Vec<Document> = scan_results.collect::<Result<Vec<_>>>()?;

        // The order of results from scan is based on key order.
        // ObjectIds are mostly monotonic, but to be safe let's sort by _id.
        found_docs.sort_by(|a, b| {
            let id_a = a.get("_id").unwrap();
            let id_b = b.get("_id").unwrap();
            bson_utils::cmp_bson(id_a, id_b)
        });

        let mut expected_docs = vec![];
        let mut expected_doc1_with_id = doc1;
        expected_doc1_with_id.insert("_id", inserted_id1);
        expected_docs.push(expected_doc1_with_id);

        let mut expected_doc2_with_id = doc2;
        expected_doc2_with_id.insert("_id", inserted_id2);
        expected_docs.push(expected_doc2_with_id);

        let mut expected_doc3_with_id = doc3;
        expected_doc3_with_id.insert("_id", inserted_id3);
        expected_docs.push(expected_doc3_with_id);

        expected_docs.sort_by(|a, b| {
            let id_a = a.get("_id").unwrap();
            let id_b = b.get("_id").unwrap();
            bson_utils::cmp_bson(id_a, id_b)
        });

        assert_eq!(found_docs, expected_docs);

        Ok(())
    }

    #[test]
    fn test_search_and_scan_edge_cases() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_edge")?;

        // Insert some docs with known integer _id's for predictable range scans
        let doc1 = doc! { "_id": 10i32, "name": "doc10" };
        let doc2 = doc! { "_id": 20i32, "name": "doc20" };
        let doc3 = doc! { "_id": 30i32, "name": "doc30" };
        let doc_to_delete = doc! { "_id": 40i32, "name": "doc40_to_delete" };

        for doc in [&doc1, &doc2, &doc3, &doc_to_delete] {
            let doc_bytes = bson::to_vec(doc)?;
            let insert_plan = PhysicalPlan::InsertOne {
                collection: collection_id,
                document: doc_bytes,
            };
            let mut result = executor.execute_direct(insert_plan, None)?;
            let result_doc = result.next().unwrap()?;
            assert_eq!(
                doc.get("_id").unwrap(),
                result_doc.get("inserted_id").unwrap()
            );
            assert!(result.next().is_none());
        }

        // 2. PointSearch for non-existent key
        let mut params_non_exist = Parameters::new();
        let key_expr_non_exist = params_non_exist.collect_parameter(BsonValue(Bson::Int32(99)));
        let plan_non_exist = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr_non_exist.clone(),
            filter: None,
            projection: None,
        });
        let mut result_non_exist = executor.execute_cached(plan_non_exist, &params_non_exist)?;
        assert!(
            result_non_exist.next().is_none(),
            "PointSearch for non-existent key should be empty"
        );

        // 3. PointSearch for deleted key
        // 3a. Delete the document via direct storage engine write
        let key_to_delete = BsonValue(Bson::Int32(40)).try_into_key()?;
        let delete_op = Operation::new_delete(collection_id, 0, key_to_delete);
        storage_engine.write(WriteBatch::new(vec![delete_op]))?;

        // 3b. Search for it
        let mut params_deleted = Parameters::new();
        let key_expr_deleted = params_deleted.collect_parameter(BsonValue(Bson::Int32(40)));
        let plan_deleted = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr_deleted.clone(),
            filter: None,
            projection: None,
        });
        let mut result_deleted = executor.execute_cached(plan_deleted, &params_deleted)?;
        assert!(
            result_deleted.next().is_none(),
            "PointSearch for deleted key should be empty"
        );

        // 4. CollectionScan with range completely outside data
        let mut params_scan_outside = Parameters::new();
        let start_outside = params_scan_outside.collect_parameter(BsonValue(Bson::Int32(100)));
        let plan_scan_outside = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::at_least(start_outside.clone()),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });
        let mut result_scan_outside =
            executor.execute_cached(plan_scan_outside, &params_scan_outside)?;
        assert!(
            result_scan_outside.next().is_none(),
            "Scan for range outside data should be empty"
        );

        // 5. CollectionScan with partial range
        let mut params_scan_partial = Parameters::new();
        let start_partial = params_scan_partial.collect_parameter(BsonValue(Bson::Int32(15)));
        let end_partial = params_scan_partial.collect_parameter(BsonValue(Bson::Int32(25)));
        let plan_scan_partial = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::closed_open(start_partial.clone(), end_partial.clone()),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });
        let mut result_scan_partial =
            executor.execute_cached(plan_scan_partial, &params_scan_partial)?;
        let found_doc = result_scan_partial.next().unwrap()?;
        assert_eq!(found_doc, doc2.clone());
        assert!(
            result_scan_partial.next().is_none(),
            "Partial scan should find exactly one document"
        );

        // 6. CollectionScan with unbounded start
        let mut params_scan_unbounded_start = Parameters::new();
        let end_unbounded_start =
            params_scan_unbounded_start.collect_parameter(BsonValue(Bson::Int32(20)));
        let plan_scan_unbounded_start = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::less_than(end_unbounded_start.clone()),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });
        let mut result_unbounded_start =
            executor.execute_cached(plan_scan_unbounded_start, &params_scan_unbounded_start)?;
        let found_doc_unbounded_start = result_unbounded_start.next().unwrap()?;
        assert_eq!(found_doc_unbounded_start, doc1.clone());
        assert!(
            result_unbounded_start.next().is_none(),
            "Scan with unbounded start should find one doc"
        );

        // 7. CollectionScan with unbounded end
        let mut params_scan_unbounded_end = Parameters::new();
        let start_unbounded_end =
            params_scan_unbounded_end.collect_parameter(BsonValue(Bson::Int32(20)));
        let plan_scan_unbounded_end = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::greater_than(start_unbounded_end.clone()),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });
        let mut result_unbounded_end =
            executor.execute_cached(plan_scan_unbounded_end, &params_scan_unbounded_end)?;
        let found_doc_unbounded_end = result_unbounded_end.next().unwrap()?;
        assert_eq!(found_doc_unbounded_end, doc3.clone());
        assert!(
            result_unbounded_end.next().is_none(),
            "Scan with unbounded end should find one doc"
        );

        // 8. CollectionScan with full range in reverse
        let plan_scan_reverse = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::all(),
            direction: Direction::Reverse,
            filter: None,
            projection: None,
        });
        let result_scan_reverse = executor.execute_cached(plan_scan_reverse, &Parameters::new())?;
        let found_docs_reverse: Vec<Document> = result_scan_reverse.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs_reverse.len(), 3);
        assert_eq!(found_docs_reverse[0], doc3.clone());
        assert_eq!(found_docs_reverse[1], doc2.clone());
        assert_eq!(found_docs_reverse[2], doc1.clone());

        Ok(())
    }

    #[test]
    fn test_limit_plan_execution() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_limit")?;

        // 2. Insert 5 documents
        let docs: Vec<Document> = (1..=5)
            .map(|i| doc! { "_id": i, "name": format!("doc{}", i) })
            .collect();

        for doc in &docs {
            let insert_plan = PhysicalPlan::InsertOne {
                collection: collection_id,
                document: bson::to_vec(doc)?,
            };
            // We use execute_direct for inserts which returns a result document.
            // We need to consume it.
            let mut result = executor.execute_direct(insert_plan, None)?;
            result.next().unwrap()?;
        }

        // 3. Create a base scan plan to feed the limit plan
        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });

        // 4. Test cases

        // Case 1: limit only
        let limit_plan = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: None,
            limit: Some(3),
        });
        let results = executor.execute_cached(limit_plan, &Parameters::new())?;
        let found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs.len(), 3);
        assert_eq!(found_docs[0].get_i32("_id").unwrap(), 1);
        assert_eq!(found_docs[1].get_i32("_id").unwrap(), 2);
        assert_eq!(found_docs[2].get_i32("_id").unwrap(), 3);

        // Case 2: skip only
        let limit_plan_skip = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: Some(2),
            limit: None,
        });
        let results_skip = executor.execute_cached(limit_plan_skip, &Parameters::new())?;
        let found_docs_skip: Vec<Document> = results_skip.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs_skip.len(), 3);
        assert_eq!(found_docs_skip[0].get_i32("_id").unwrap(), 3);
        assert_eq!(found_docs_skip[1].get_i32("_id").unwrap(), 4);
        assert_eq!(found_docs_skip[2].get_i32("_id").unwrap(), 5);

        // Case 3: skip and limit
        let limit_plan_both = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: Some(1),
            limit: Some(2),
        });
        let results_both = executor.execute_cached(limit_plan_both, &Parameters::new())?;
        let found_docs_both: Vec<Document> = results_both.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs_both.len(), 2);
        assert_eq!(found_docs_both[0].get_i32("_id").unwrap(), 2);
        assert_eq!(found_docs_both[1].get_i32("_id").unwrap(), 3);

        // Case 4: limit > number of docs
        let limit_plan_large = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: None,
            limit: Some(10),
        });
        let results_large = executor.execute_cached(limit_plan_large, &Parameters::new())?;
        let found_docs_large: Vec<Document> = results_large.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs_large.len(), 5);

        // Case 5: skip > number of docs
        let limit_plan_skip_large = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: Some(10),
            limit: None,
        });
        let results_skip_large =
            executor.execute_cached(limit_plan_skip_large, &Parameters::new())?;
        let found_docs_skip_large: Vec<Document> =
            results_skip_large.collect::<Result<Vec<_>>>()?;
        assert!(found_docs_skip_large.is_empty());

        // Case 6: limit is zero
        let limit_plan_zero = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: None,
            limit: Some(0),
        });
        let results_zero = executor.execute_cached(limit_plan_zero, &Parameters::new())?;
        let found_docs_zero: Vec<Document> = results_zero.collect::<Result<Vec<_>>>()?;
        assert!(found_docs_zero.is_empty());

        // Case 7: skip and limit over edge
        let limit_plan_edge = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: Some(3),
            limit: Some(5), // limit is larger than remaining items
        });
        let results_edge = executor.execute_cached(limit_plan_edge, &Parameters::new())?;
        let found_docs_edge: Vec<Document> = results_edge.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs_edge.len(), 2);
        assert_eq!(found_docs_edge[0].get_i32("_id").unwrap(), 4);
        assert_eq!(found_docs_edge[1].get_i32("_id").unwrap(), 5);

        Ok(())
    }

    fn assert_sorted_results(
        executor: &QueryExecutor,
        plan: Arc<PhysicalPlan>,
        expected_ids: &[i32],
    ) -> Result<()> {
        let results = executor.execute_cached(plan, &Parameters::new())?;
        let found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
        let found_ids: Vec<i32> = found_docs
            .iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect();
        assert_eq!(found_ids, expected_ids);
        Ok(())
    }

    #[test]
    fn test_sort_plans_execution() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_sorts")?;

        // 2. Insert test data
        let docs = vec![
            doc! { "_id": 1, "name": "c", "value": 10.0 },
            doc! { "_id": 2, "name": "a", "value": 30.0 },
            doc! { "_id": 3, "name": "b", "value": 20.0 },
            doc! { "_id": 4, "name": "a", "value": 10.0 },
            doc! { "_id": 5, "name": "c", "value": 5.0 },
        ];
        for doc in &docs {
            let insert_plan = PhysicalPlan::InsertOne {
                collection: collection_id,
                document: bson::to_vec(doc)?,
            };
            let mut result = executor.execute_direct(insert_plan, None)?;
            result.next().unwrap()?; // consume result
        }

        // 3. Define sort fields and expected order
        let sort_fields = Arc::new(vec![
            make_sort_field(vec!["name".into()], SortOrder::Ascending),
            make_sort_field(vec!["value".into()], SortOrder::Ascending),
        ]);
        let expected_ids = vec![4, 2, 3, 5, 1];

        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });

        // --- In-Memory Sort ---
        let mem_sort_plan = Arc::new(PhysicalPlan::InMemorySort {
            input: scan_plan.clone(),
            sort_fields: sort_fields.clone(),
        });
        assert_sorted_results(&executor, mem_sort_plan, &expected_ids)?;

        // --- External Merge Sort ---
        let ext_sort_plan = Arc::new(PhysicalPlan::ExternalMergeSort {
            input: scan_plan.clone(),
            sort_fields: sort_fields.clone(),
            max_in_memory_rows: 2,
        });
        assert_sorted_results(&executor, ext_sort_plan, &expected_ids)?;

        // --- Top-K Heap Sort ---
        // k=3
        let topk_plan = Arc::new(PhysicalPlan::TopKHeapSort {
            input: scan_plan.clone(),
            sort_fields: sort_fields.clone(),
            k: 3,
        });
        assert_sorted_results(&executor, topk_plan, &expected_ids[..3])?;

        // k=0
        let topk_plan_0 = Arc::new(PhysicalPlan::TopKHeapSort {
            input: scan_plan.clone(),
            sort_fields: sort_fields.clone(),
            k: 0,
        });
        assert_sorted_results(&executor, topk_plan_0, &[])?;

        // k > items
        let topk_plan_10 = Arc::new(PhysicalPlan::TopKHeapSort {
            input: scan_plan,
            sort_fields: sort_fields.clone(),
            k: 10,
        });
        assert_sorted_results(&executor, topk_plan_10, &expected_ids)?;

        Ok(())
    }

    // Helper to run a filter plan and check the _id's of the results.
    fn run_filter_test(
        executor: &QueryExecutor,
        collection_id: u32,
        filter_expr: Arc<Expr>,
        parameters: Parameters,
        expected_ids: &[i32],
    ) -> Result<()> {
        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });

        let filter_plan = Arc::new(PhysicalPlan::Filter {
            input: scan_plan,
            predicate: filter_expr,
        });

        let results = executor.execute_cached(filter_plan, &parameters)?;
        let mut found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
        found_docs.sort_by_key(|d| d.get_i32("_id").unwrap());

        let found_ids: Vec<i32> = found_docs
            .iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect();
        assert_eq!(
            expected_ids, found_ids,
            "Filter test failed. Expected: {:?}, Found: {:?}",
            expected_ids, found_ids
        );
        Ok(())
    }

    #[test]
    fn test_filter_plan_execution() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_filters")?;

        // 2. Insert test data
        let docs = vec![
            doc! { "_id": 1, "name": "c", "value": 10.0, "tags": ["a", "b"], "items": [ doc!{ "k": "x", "v": 1 }, doc!{ "k": "y", "v": 2 } ] },
            doc! { "_id": 2, "name": "a", "value": 30.0, "tags": ["b", "c"], "nested": { "val": 5 } },
            doc! { "_id": 3, "name": "b", "value": 20.0, "tags": ["c", "d"] },
            doc! { "_id": 4, "name": "a", "value": 10.0, "tags": ["d", "a"] },
            doc! { "_id": 5, "name": "c", "value": 5.0 }, // no tags
            doc! { "_id": 6, "name": "d", "value": Bson::Null, "tags": [] },
        ];
        for doc in &docs {
            let insert_plan = PhysicalPlan::InsertOne {
                collection: collection_id,
                document: bson::to_vec(doc)?,
            };
            let mut result = executor.execute_direct(insert_plan, None)?;
            result.next().unwrap()?; // consume result
        }

        // $eq
        let mut params = Parameters::new();
        let p = params.collect_parameter("a".into());
        let filter = field_filters(
            field(["name"]), [interval(point(&p))]
        );
        run_filter_test(&executor, collection_id, filter, params, &[2, 4])?;

        // $gt
        let mut params = Parameters::new();
        let p = params.collect_parameter(15.0.into());
        let filter = field_filters(
            field(["value"]), [interval(greater_than(&p))]
        );
        run_filter_test(&executor, collection_id, filter, params, &[2, 3])?;

        // $in
        let mut params = Parameters::new();
        let p = params.collect_parameter(vec!["a", "b"].into());
        let filter = field_filters(field(["name"]), [within(p)]);
        run_filter_test(&executor, collection_id, filter, params, &[2, 3, 4])?;

        // $exists: true
        let filter = field_filters(field(["nested"]), [exists(true)]);
        run_filter_test(&executor, collection_id, filter, Parameters::new(), &[2])?;

        // $exists: false
        let filter = field_filters(field(["nested"]), [exists(false)]);
        run_filter_test(
            &executor,
            collection_id,
            filter,
            Parameters::new(),
            &[1, 3, 4, 5, 6],
        )?;

        // $type: "double"
        let mut params = Parameters::new();
        let p = params.collect_parameter("double".into());
        let filter = field_filters(field(["value"]), [has_type(p, false)]);
        run_filter_test(
            &executor,
            collection_id,
            filter,
            params,
            &[1, 2, 3, 4, 5],
        )?;

        // $size: 2
        let mut params = Parameters::new();
        let p = params.collect_parameter(2.into());
        let filter = field_filters(field(["tags"]), [size(p, false)]);
        run_filter_test(
            &executor,
            collection_id,
            filter,
            params,
            &[1, 2, 3, 4],
        )?;

        // $all
        let mut params = Parameters::new();
        let p = params.collect_parameter(vec!["a", "d"].into());
        let filter = field_filters(field(["tags"]), [all(p)]);
        run_filter_test(&executor, collection_id, filter, params, &[4])?;

        // $elemMatch on documents
        let mut params = Parameters::new();
        let p_k = params.collect_parameter("y".into());
        let p_v = params.collect_parameter(2.into());
        let filter = field_filters(field(["items"]),
                                   [elem_match(
                                       vec![
                                           field_filters(field(["k"]), [interval(point(&p_k))]),
                                           field_filters(field(["v"]), [interval(point(&p_v))])
                                       ]
                                   )]);
        run_filter_test(&executor, collection_id, filter, params, &[1])?;

        // $and
        let mut params = Parameters::new();
        let p_name = params.collect_parameter("a".into());
        let p_val = params.collect_parameter(10.0.into());
        let f1 = field_filters(field(["name"]), [interval(point(&p_name))]);
        let f2 = field_filters(field(["value"]), [interval(point(&p_val))]);
        let filter = and(vec![f1, f2]);
        run_filter_test(&executor, collection_id, filter, params, &[4])?;

        // $or
        let mut params = Parameters::new();
        let p_name = params.collect_parameter("b".into());
        let p_val = params.collect_parameter(5.0.into());
        let f1 = field_filters(field(["name"]), [interval(point(&p_name))]);
        let f2 = field_filters(field(["value"]), [interval(point(&p_val))]);
        let filter = or(vec![f1, f2]);
        run_filter_test(&executor, collection_id, filter, params, &[3, 5])?;

        // $not (top-level)
        let mut params = Parameters::new();
        let p = params.collect_parameter("a".into());
        let inner_filter = field_filters(field(["name"]), [interval(point(&p))]);
        let filter = not(inner_filter);
        run_filter_test(&executor, collection_id, filter, params, &[1, 3, 5, 6])?;

        // $nor
        let mut params = Parameters::new();
        let p_name1 = params.collect_parameter("a".into());
        let p_name2 = params.collect_parameter("b".into());
        let f1 = field_filters(field(["name"]), [interval(point(&p_name1))]);
        let f2 = field_filters(field(["name"]), [interval(point(&p_name2))]);
        let filter = nor(vec![f1, f2]);
        run_filter_test(&executor, collection_id, filter, params, &[1, 5, 6])?;

        // $lt
        let mut params = Parameters::new();
        let p = params.collect_parameter(15.0.into());
        let filter = field_filters(field(["value"]), [interval(less_than(&p))]);
        run_filter_test(&executor, collection_id, filter, params, &[1, 4, 5, 6])?; // Also match Null

        // $lte
        let mut params = Parameters::new();
        let p = params.collect_parameter(10.0.into());
        let filter = field_filters(field(["value"]), [interval(at_most(&p))]);
        run_filter_test(&executor, collection_id, filter, params, &[1, 4, 5, 6])?;

        // $gte
        let mut params = Parameters::new();
        let p = params.collect_parameter(20.0.into());
        let filter = field_filters(field(["value"]), [interval(at_least(&p))]);
        run_filter_test(&executor, collection_id, filter, params, &[2, 3])?;

        // $ne
        let mut params = Parameters::new();
        let p = params.collect_parameter("a".into());
        let filter = field_filters(field(["name"]), [ne(p)]);
        run_filter_test(&executor, collection_id, filter, params, &[1, 3, 5, 6])?;

        // $nin
        let mut params = Parameters::new();
        let p = params.collect_parameter(vec!["a", "b"].into());
        let filter = field_filters(field(["name"]), [not(within(p))]);
        run_filter_test(&executor, collection_id, filter, params, &[1, 5, 6])?;

        // Nested logical operators: $or inside $and
        let mut params = Parameters::new();
        let p1 = params.collect_parameter("a".into());
        let p2 = params.collect_parameter("b".into());
        let p3 = params.collect_parameter(10.0.into());
        let f1 = or(vec![
            field_filters(field(["name"]), [interval(point(&p1))]),
            field_filters(field(["name"]), [interval(point(&p2))]),
        ]);
        let f2 = field_filters(field(["value"]), [interval(point(&p3))]);
        let filter = and(vec![f1, f2]);
        run_filter_test(&executor, collection_id, filter, params, &[4])?;

        // Complex/nested field path
        let mut params = Parameters::new();
        let p = params.collect_parameter(5.into());
        let filter = field_filters(field(["nested", "val"]), [interval(point(&p))]);
        run_filter_test(&executor, collection_id, filter, params, &[2])?;

        // Null check: field is null
        let mut params = Parameters::new();
        let p = params.collect_parameter(BsonValue(Bson::Null));
        let filter = field_filters(field(["value"]), [interval(point(&p))]);
        run_filter_test(&executor, collection_id, filter, params, &[6])?;

        // Null check: field does not exist (should not match null)
        let mut params = Parameters::new();
        let p = params.collect_parameter(BsonValue(Bson::Null));
        let filter = field_filters(field(["tags"]), [interval(point(&p))]);
        run_filter_test(&executor, collection_id, filter, params, &[5])?;

        Ok(())
    }

    #[test]
    fn test_projection_plan_execution() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_projections")?;

        // 2. Insert test data
        let test_doc = doc! {
            "_id": 1,
            "name": "test_doc",
            "scalar": 123,
            "nested": { "a": 1, "b": "hello" },
            "array_scalar": [10, 20, 30, 40, 50],
            "array_doc": [
                doc!{ "val": 10, "tag": "a" },
                doc!{ "val": 20, "tag": "b" },
                doc!{ "val": 30, "tag": "a" },
                doc!{ "val": 40, "tag": "c" },
            ]
        };
        let insert_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&test_doc)?,
        };
        let mut result = executor.execute_direct(insert_plan, None)?;
        result.next().unwrap()?; // consume result

        // Helper to run a projection and check the result
        fn run_projection_test(
            executor: &QueryExecutor,
            collection_id: u32,
            projection: Projection,
            parameters: Parameters,
            expected_doc: Document,
        ) -> Result<()> {
            let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
                collection: collection_id,
                range: Interval::all(),
                direction: Direction::Forward,
                filter: None,
                projection: None,
            });

            let projection_plan = Arc::new(PhysicalPlan::Projection {
                input: scan_plan,
                projection: Arc::new(projection),
            });

            let results = executor.execute_cached(projection_plan, &parameters)?;
            let found_docs: Vec<Document> = results.collect::<Result<Vec<_>>>()?;
            assert_eq!(found_docs.len(), 1);
            assert_eq!(
                found_docs[0], expected_doc,
                "\nProjection test failed.\nExpected: {:#?}\n   Found: {:#?}",
                expected_doc, found_docs[0]
            );
            Ok(())
        }

        // --- Test Cases ---

        // Case 1: Simple include
        let proj_expr_1 = proj_fields(vec![("name", proj_field()), ("scalar", proj_field())]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_1),
            Parameters::new(),
            doc! { "name": "test_doc", "scalar": 123 },
        )?;

        // Case 2: Nested include
        let proj_expr_2 = proj_fields(vec![
            ("name", proj_field()),
            ("nested", proj_fields(vec![("b", proj_field())])),
        ]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_2),
            Parameters::new(),
            doc! { "name": "test_doc", "nested": { "b": "hello" } },
        )?;

        // Case 3: Simple exclude
        let proj_expr_3 = proj_fields(vec![
            ("scalar", proj_field()),
            ("array_scalar", proj_field()),
            ("array_doc", proj_field()),
        ]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Exclude(proj_expr_3),
            Parameters::new(),
            doc! { "_id": 1, "name": "test_doc", "nested": { "a": 1, "b": "hello" } },
        )?;

        // Case 4: Slice projection { array_scalar: { $slice: [1, 2] } }
        let proj_expr_4 = proj_fields(vec![("array_scalar", proj_slice(Some(1), 2))]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_4),
            Parameters::new(),
            doc! { "array_scalar": [20, 30] },
        )?;

        // Case 5: ElemMatch projection
        let mut params_5 = Parameters::new();
        let p_5 = params_5.collect_parameter("a".into());
        let filter_5 = field_filters(field(["tag"]), [interval(point(&p_5))]);
        let proj_expr_5 = proj_fields(vec![("array_doc", proj_elem_match(filter_5))]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_5),
            params_5,
            doc! {
                "array_doc": [
                    doc!{ "val": 10, "tag": "a" },
                    doc!{ "val": 30, "tag": "a" },
                ]
            },
        )?;

        // Case 6: ArrayElements projection (non-standard, returns a document)
        let proj_expr_7 = proj_fields(vec![(
            "array_scalar",
            proj_array_elements(vec![(1, proj_field()), (3, proj_field())]),
        )]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_7),
            Parameters::new(),
            doc! { "array_scalar": doc! { "1": 20, "3": 40 } },
        )?;

        // Case 7: ElemMatch with no matches
        let mut params_8 = Parameters::new();
        let p_8 = params_8.collect_parameter("z".into()); // No element has tag "z"
        let filter_8 = field_filters(field(["tag"]), [interval(point(&p_8))]);
        let proj_expr_8 = proj_fields(vec![("array_doc", proj_elem_match(filter_8))]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_8),
            params_8,
            doc! { "array_doc": []}, // Expect empty array since no matches
        )?;

        // Case 8: Projection on a non-existent field
        let proj_expr_10 = proj_fields(vec![("non_existent", proj_field())]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_10),
            Parameters::new(),
            doc! {}, // Expect an empty document
        )?;

        // Case 9: Array projection on a non-array field
        let proj_expr_11 = proj_fields(vec![("scalar", proj_slice(None, 2))]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_11),
            Parameters::new(),
            doc! {}, // Field should be omitted
        )?;

        // Case 10: Slice with only negative limit (last N elements)
        let proj_expr_12 = proj_fields(vec![("array_scalar", proj_slice(None, -2))]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_12),
            Parameters::new(),
            doc! { "array_scalar": [40, 50] },
        )?;

        // Case 11: Slice with zero skip and negative limit
        let proj_expr_13 = proj_fields(vec![("array_scalar", proj_slice(Some(0), -2))]);
        run_projection_test(
            &executor,
            collection_id,
            Projection::Include(proj_expr_13),
            Parameters::new(),
            doc! { "array_scalar": [40, 50] },
        )?;

        Ok(())
    }

    #[test]
    fn test_multipoint_search_execution() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_multipoint")?;

        // 2. Insert test data
        let docs = vec![
            doc! { "_id": 1, "name": "a", "value": 10 },
            doc! { "_id": 2, "name": "b", "value": 20 },
            doc! { "_id": 3, "name": "c", "value": 10 },
            doc! { "_id": 4, "name": "d", "value": 20 },
            doc! { "_id": 5, "name": "e", "value": 10 },
        ];
        for doc in &docs {
            let insert_plan = PhysicalPlan::InsertOne {
                collection: collection_id,
                document: bson::to_vec(doc)?,
            };
            let mut result = executor.execute_direct(insert_plan, None)?;
            result.next().unwrap()?; // consume result
        }

        // --- Test Cases ---

        // Case 1: Forward direction, some keys don't exist
        let mut params1 = Parameters::new();
        let keys1 = params1.collect_parameter(BsonValue(Bson::Array(vec![
            Bson::Int32(5),
            Bson::Int32(1),
            Bson::Int32(99), // non-existent
            Bson::Int32(3),
        ])));
        let plan1 = Arc::new(PhysicalPlan::MultiPointSearch {
            collection: collection_id,
            keys: keys1,
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });

        let results1 = executor.execute_cached(plan1, &params1)?;
        let found_docs1: Vec<Document> = results1.collect::<Result<_>>()?;
        let found_ids1: Vec<i32> = found_docs1.iter().map(|d| d.get_i32("_id").unwrap()).collect();
        assert_eq!(found_ids1, vec![1, 3, 5]);

        // Case 2: Reverse direction
        let mut params2 = Parameters::new();
        let keys2 = params2.collect_parameter(BsonValue(Bson::Array(vec![
            Bson::Int32(5),
            Bson::Int32(1),
            Bson::Int32(3),
        ])));
        let plan2 = Arc::new(PhysicalPlan::MultiPointSearch {
            collection: collection_id,
            keys: keys2,
            direction: Direction::Reverse,
            filter: None,
            projection: None,
        });

        let results2 = executor.execute_cached(plan2, &params2)?;
        let found_docs2: Vec<Document> = results2.collect::<Result<_>>()?;
        let found_ids2: Vec<i32> = found_docs2.iter().map(|d| d.get_i32("_id").unwrap()).collect();
        assert_eq!(found_ids2, vec![5, 3, 1]);

        // Case 3: With residual filter
        let mut params3 = Parameters::new();
        let keys3 = params3.collect_parameter(BsonValue(Bson::Array(vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int32(3),
            Bson::Int32(4),
        ])));
        let p_val = params3.collect_parameter(10.into());
        let filter3 = field_filters(field(["value"]), [interval(point(&p_val))]);

        let plan3 = Arc::new(PhysicalPlan::MultiPointSearch {
            collection: collection_id,
            keys: keys3,
            direction: Direction::Forward,
            filter: Some(filter3),
            projection: None,
        });

        let results3 = executor.execute_cached(plan3, &params3)?;
        let found_docs3: Vec<Document> = results3.collect::<Result<_>>()?;
        let found_ids3: Vec<i32> = found_docs3.iter().map(|d| d.get_i32("_id").unwrap()).collect();
        assert_eq!(found_ids3, vec![1, 3]);

        Ok(())
    }

    #[test]
    fn test_execute_cached_at_snapshot() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_snapshot")?;

        // 2. Insert initial doc
        let initial_doc = doc! { "_id": 1_i32, "value": "initial" };
        let insert_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&initial_doc)?,
        };
        executor.execute_direct(insert_plan, None)?.count(); // Consume iterator

        // 3. Get snapshot
        let snapshot1 = storage_engine.last_visible_sequence();

        // 4. Update the doc directly in storage
        let key = BsonValue(Bson::Int32(1)).try_into_key()?;
        let update_op = Operation::new_put(
            collection_id,
            0,
            key.clone(),
            bson::to_vec(&doc! { "_id": 1_i32, "value": "updated" })?,
        );
        storage_engine.write(WriteBatch::new(vec![update_op]))?;

        // 5. Query at snapshot
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(Bson::Int32(1)));
        let point_search_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr,
            filter: None,
            projection: None,
        });

        let mut result_at_snapshot =
            executor.execute_cached_at_snapshot(point_search_plan.clone(), &params, Some(snapshot1))?;
        let doc_at_snapshot = result_at_snapshot.next().unwrap()?;
        assert!(result_at_snapshot.next().is_none());
        assert_eq!(doc_at_snapshot, initial_doc);

        // 6. Query at latest
        let mut result_latest = executor.execute_cached(point_search_plan, &params)?;
        let doc_latest = result_latest.next().unwrap()?;
        assert!(result_latest.next().is_none());
        assert_eq!(doc_latest.get_str("value").unwrap(), "updated");

        // 7. Test with scan and deletes
        let doc_to_delete = doc! { "_id": 2_i32, "value": "to_delete" };
        let insert_plan_2 = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&doc_to_delete)?,
        };
        executor.execute_direct(insert_plan_2, None)?.count();

        let snapshot2 = storage_engine.last_visible_sequence();

        let key_to_delete = BsonValue(Bson::Int32(2)).try_into_key()?;
        let delete_op = Operation::new_delete(collection_id, 0, key_to_delete);
        storage_engine.write(WriteBatch::new(vec![delete_op]))?;

        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        });

        // Scan at snapshot2 should see both documents (doc1 is updated, doc2 exists)
        let results_at_snapshot2 =
            executor.execute_cached_at_snapshot(scan_plan.clone(), &Parameters::new(), Some(snapshot2))?;
        let mut docs_at_snapshot2: Vec<Document> = results_at_snapshot2.collect::<Result<_>>()?;
        assert_eq!(docs_at_snapshot2.len(), 2);
        docs_at_snapshot2.sort_by_key(|d| d.get_i32("_id").unwrap());

        assert_eq!(docs_at_snapshot2[0].get_i32("_id").unwrap(), 1);
        assert_eq!(docs_at_snapshot2[0].get_str("value").unwrap(), "updated");
        assert_eq!(docs_at_snapshot2[1].get_i32("_id").unwrap(), 2);
        assert_eq!(docs_at_snapshot2[1].get_str("value").unwrap(), "to_delete");

        // Scan at latest should see only one document
        let results_latest_scan = executor.execute_cached(scan_plan, &Parameters::new())?;
        let docs_latest_scan: Vec<Document> = results_latest_scan.collect::<Result<_>>()?;
        assert_eq!(docs_latest_scan.len(), 1);
        assert_eq!(docs_latest_scan[0].get_i32("_id").unwrap(), 1);
        assert_eq!(docs_latest_scan[0].get_str("value").unwrap(), "updated");

        Ok(())
    }

    #[test]
    fn test_update_one_succeeds_on_retry() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

        let initial_doc = doc! { "_id": 1, "value": "initial" };
        let insert_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&initial_doc)?,
        };
        executor.execute_direct(insert_plan, None)?.count();

        // 2. Arrange to fail the next precondition check, simulating a conflict
        storage_engine.fail_next_precondition_checks(1);

        // 3. Act: prepare and execute UpdateOne
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(Bson::Int32(1)));
        let query_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr,
            filter: None,
            projection: None,
        });

        let update_expr = update([set([field_name("value")], "updated")]);

        let update_plan = PhysicalPlan::UpdateOne {
            collection: collection_id,
            query: query_plan,
            update: update_expr,
        };

        // The executor will attempt the update, fail, retry, and then succeed.
        let result = executor.execute_direct(update_plan, Some(params))?;
        let result_doc = result.into_iter().next().unwrap()?;
        assert_eq!(result_doc.get_i32("matched_count").unwrap(), 1);
        assert_eq!(result_doc.get_i32("modified_count").unwrap(), 1);

        // 4. Assert final state
        let user_key = BsonValue(Bson::Int32(1)).try_into_key()?;
        let final_doc_bytes = storage_engine.read(collection_id, 0, &user_key, None)?.unwrap().1;
        let final_doc = Document::from_reader(Cursor::new(final_doc_bytes))?;
        assert_eq!(final_doc.get_str("value").unwrap(), "updated");
        assert_eq!(final_doc.get_i32("_id").unwrap(), 1);

        Ok(())
    }

    #[test]
    fn test_update_one_fails_after_retry_timeout() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

        let initial_doc = doc! { "_id": 1, "value": "initial" };
        let insert_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&initial_doc)?,
        };
        executor.execute_direct(insert_plan, None)?.count();
        let user_key = BsonValue(Bson::Int32(1)).try_into_key()?;

        // 2. Arrange to fail many times to ensure timeout is reached
        storage_engine.fail_next_precondition_checks(20);

        // 3. Act: prepare UpdateOne
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(Bson::Int32(1)));
        let query_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr,
            filter: None,
            projection: None,
        });

        let update_expr = update([set([field_name("value")], "updated")]);

        let update_plan = PhysicalPlan::UpdateOne {
            collection: collection_id,
            query: query_plan,
            update: update_expr,
        };

        let result = executor.execute_direct(update_plan, Some(params));

        // 4. Assert: Expect a VersionConflict error
        match result {
            Err(Error::VersionConflict { .. }) => {
                // This is the expected error after timeout
            }
            Err(e) => panic!("Expected a VersionConflict error, but got {:?}", e),
            Ok(_) => panic!("Expected an error, but the update succeeded"),
        }

        // 5. Assert that the document was not changed
        let final_doc_bytes = storage_engine.read(collection_id, 0, &user_key, None)?.unwrap().1;
        let final_doc = Document::from_reader(Cursor::new(final_doc_bytes))?;
        assert_eq!(final_doc.get_str("value").unwrap(), "initial");

        Ok(())
    }

    #[test]
    fn test_update_many_does_not_retry_on_conflict() -> Result<()> {
        // 1. Setup
        let (storage_engine, _dir) = storage_engine()?;
        let executor = QueryExecutor::new(storage_engine.clone());
        let collection_id = storage_engine.create_collection_if_not_exists("test_retry")?;

        let doc1 = doc! { "_id": 1, "value": "initial" };
        let insert_plan = PhysicalPlan::InsertOne {
            collection: collection_id,
            document: bson::to_vec(&doc1)?,
        };
        executor.execute_direct(insert_plan, None)?.count();

        // 2. Arrange to fail the next precondition check
        storage_engine.fail_next_precondition_checks(1);

        // 3. Act: prepare and execute UpdateMany
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(Bson::Int32(1)));
        let query_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            key: key_expr,
            filter: None,
            projection: None,
        });

        let update_expr = update([set([field_name("value")], "updated")]);

        let update_plan = PhysicalPlan::UpdateMany {
            collection: collection_id,
            query: query_plan,
            update: update_expr,
        };

        let result = executor.execute_direct(update_plan, Some(params));

        // 4. Assert: Expect an immediate VersionConflict error
        match result {
            Err(Error::VersionConflict { .. }) => {
                // Correct: the operation failed without retrying.
            }
            Err(e) => panic!("Expected a VersionConflict error, but got {:?}", e),
            Ok(_) => panic!("Expected an error, but the update succeeded"),
        }

        // 5. Assert that the document was not changed
        let user_key = BsonValue(Bson::Int32(1)).try_into_key()?;
        let final_doc_bytes = storage_engine.read(collection_id, 0, &user_key, None)?.unwrap().1;
        let final_doc = Document::from_reader(Cursor::new(final_doc_bytes))?;
        assert_eq!(final_doc.get_str("value").unwrap(), "initial");

        Ok(())
    }
}
