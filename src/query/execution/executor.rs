use crate::error::Result;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::{BsonValue, Expr, Parameters};
use crate::query::execution::{filters, projections, sorts, updates};
use crate::storage::internal_key::extract_operation_type;
use crate::storage::operation::{Operation, OperationType};
use crate::storage::storage_engine::StorageEngine;
use crate::storage::write_batch::WriteBatch;
use crate::util::bson_utils::{self, BsonKey};
use crate::util::interval::Interval;
use bson::oid::ObjectId;
use bson::{doc, Bson, Document, RawDocument};
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

pub type QueryOutput = Box<dyn Iterator<Item = Result<Document>>>;

/// Executes a physical query plan.
pub struct QueryExecutor {
    storage_engine: Arc<StorageEngine>,
}

impl QueryExecutor {
    /// Creates a new `QueryExecutor`.
    pub fn new(storage_engine: Arc<StorageEngine>) -> Self {
        Self { storage_engine }
    }

    pub fn execute_direct(&self, plan: PhysicalPlan, parameters: Option<Parameters>) -> Result<QueryOutput> {
        match plan {
            PhysicalPlan::InsertMany {
                collection,
                documents,
            } => {
                let mut operations = Vec::with_capacity(documents.len());
                let mut ids = Vec::with_capacity(documents.len());
                for mut doc in documents {

                    let id = Self::prepend_id_if_needed(&mut doc)?;
                    ids.push(id.clone());
                    // Convert the BSON _id to a key
                    let user_key = id.try_into_key()?;

                    operations.push(Operation::new_put(collection, 0, user_key, doc));
                }

                let batch = WriteBatch::new(operations);
                self.storage_engine.write(batch)?;

                let result = doc! { "inserted_ids": ids.into_iter().map(Bson::from).collect::<Vec<_>>() };

                Ok(Box::new(std::iter::once(Ok(result))))
            }
            PhysicalPlan::InsertOne {
                collection,
                document,
            } => {
                let mut doc = document;
                let id = Self::prepend_id_if_needed(&mut doc)?;
                // Convert the BSON _id to a key
                let user_key = id.try_into_key()?;

                let operation = Operation::new_put(collection, 0, user_key, doc);
                self.storage_engine.write(WriteBatch::new(vec!(operation)))?;

                let result = doc! { "inserted_id": id };

                Ok(Box::new(std::iter::once(Ok(result))))
            }
            PhysicalPlan::UpdateOne { collection, query, update } => {
                let parameters = parameters.expect("Parameters must be provided for UpdateOne");

                let mut iter = self.execute_cached(query, &parameters)?;

                let updater = updates::to_updater(&update)?;

                if let Some(doc) = iter.next() {
                    let new_doc =  updater(doc?)?;

                    let user_key = new_doc.get("_id").unwrap().try_into_key()?;

                    let operation = Operation::new_put(collection, 0, user_key, bson::to_vec(&new_doc)?);
                    self.storage_engine.write(WriteBatch::new(vec!(operation)))?;

                    let result = doc! { "matched_count": 1, "modified_count": 1 };
                    Ok(Box::new(std::iter::once(Ok(result))))
                } else {
                    let result = doc! { "matched_count": 0, "modified_count": 0 };
                    Ok(Box::new(std::iter::once(Ok(result))))
                }
            }
            PhysicalPlan::UpdateMany { collection, query, update } => {
                let parameters = parameters.expect("Parameters must be provided for UpdateMany");

                let iter = self.execute_cached(query, &parameters)?;

                let updater = updates::to_updater(&update)?;

                let mut operations = Vec::new();
                let mut matched_count = 0;
                let mut modified_count = 0;

                for doc in iter {
                    matched_count += 1;
                    let new_doc = updater(doc?)?;

                    let user_key = new_doc.get("_id").unwrap().try_into_key()?;

                    operations.push(Operation::new_put(collection, 0, user_key, bson::to_vec(&new_doc)?));
                    modified_count += 1;
                }

                if !operations.is_empty() {
                    self.storage_engine.write(WriteBatch::new(operations))?;
                }

                let result = doc! { "matched_count": matched_count, "modified_count": modified_count };
                Ok(Box::new(std::iter::once(Ok(result))))
            }
            _ => {
                // Other plans, should be cached
                panic!("Direct execution not supported for plan: {:?}", plan);
            }
        }
    }

    /// Ensures that each document has an `_id` field, prepending it if necessary.
    fn prepend_id_if_needed(mut doc: &mut Vec<u8>) -> Result<Bson> {

        let id = RawDocument::from_bytes(&doc)?.get("_id")?;

        let id: Bson = match id {
            Some(id) => {
                id.to_raw_bson().try_into()?
            },
            None => {
                let bson = Bson::ObjectId(ObjectId::new());
                bson_utils::prepend_field(&mut doc, "_id", &bson)?;
                bson
            }
        };
        Ok(id)
    }

    /// Executes the given physical plan.
    pub fn execute_cached(&self, plan: Arc<PhysicalPlan>, parameters: &Parameters) -> Result<QueryOutput> {
        match plan.as_ref() {
            PhysicalPlan::CollectionScan {
                collection,
                range,
                direction,
                filter,
                projection: _, // Projection pushdown is not yet supported at this level
            } => {
                let range = Self::bind_key_range_parameters(range, &parameters)?;

                // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
                let filter = filter.clone().and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

                Ok(Box::new(self.storage_engine.range_scan(
                    *collection,
                    0, // This is table scan so index is 0
                    &range,
                    None,
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
            PhysicalPlan::PointSearch {
                collection,
                key,
                filter,
                projection: _,
            } => {
                // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
                let filter = filter.clone().and_then(|predicate| Some(filters::to_filter(predicate, &parameters)));

                let key = Self::bind_key_parameter(key, &parameters)?;
                let result = self.storage_engine.read(*collection, 0, &key, None)?;
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
                                    None =>  Box::new(std::iter::once(Ok(doc)))
                                }
                             }
                            _ => panic!("Unexpected operation type: {:?}", op),
                        }
                    }
                    None => Box::new(std::iter::empty()),
                };
                Ok(iter)
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
            PhysicalPlan::Filter { input, predicate } => {
                let filter = filters::to_filter(predicate.clone(), &parameters);
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                Ok(Box::new(input_iter.filter(move |res| {
                    if res.is_err() { true } else { filter(res.as_ref().unwrap()) }
                })))
            }
            PhysicalPlan::Projection { input, projection } => {
                let projector = projections::to_projector(projection, &parameters)?;
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                Ok(Box::new(input_iter.map(move |res| {
                    res.and_then(|doc| projector(doc))
                })))
            }
            PhysicalPlan::InMemorySort {
                input,
                sort_fields,
            } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                sorts::in_memory_sort(input_iter, &sort_fields)
            }
            PhysicalPlan::ExternalMergeSort {
                input,
                sort_fields,
                max_in_memory_rows,
            } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                sorts::external_merge_sort(input_iter, sort_fields.clone(), *max_in_memory_rows)
            }
            PhysicalPlan::TopKHeapSort {
                input,
                sort_fields,
                k,
            } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                sorts::top_k_heap_sort(input_iter, sort_fields.clone(), *k)
            }
            PhysicalPlan::Limit {
                input,
                skip,
                limit,
            } => {
                let mut iter = self.execute_cached(input.clone(), parameters)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::query::{make_sort_field, SortOrder};
    use crate::query::{BsonValue, Expr, Parameters, Projection};
    use crate::storage::test_utils::storage_engine;
    use crate::storage::Direction;
    use bson::{doc, Bson, Document};
    use std::sync::Arc;
    use crate::query::expr_fn::{all, and, elem_match, exists, field, field_filters, has_type, ne, nor, not, or, size, within, proj_array_elements, proj_elem_match, proj_field, proj_fields, proj_slice, interval, point, greater_than, less_than, at_most, at_least};

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
}
