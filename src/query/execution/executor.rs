use crate::error::{Error, Result};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::{get_path_value, BsonValue, BsonValueRef, ComparisonOperator, Expr, Parameters};
use crate::query::execution::sort;
use crate::storage::internal_key::extract_operation_type;
use crate::storage::operation::{Operation, OperationType};
use crate::storage::storage_engine::StorageEngine;
use crate::storage::write_batch::WriteBatch;
use crate::util::bson_utils::{self, BsonKey};
use crate::util::interval::Interval;
use bson::oid::ObjectId;
use bson::{doc, Bson, Document, RawDocument};
use std::cmp::Ordering;
use std::io::Cursor;
use std::ops::Bound;
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

    pub fn execute_direct(&self, plan: PhysicalPlan) -> Result<QueryOutput> {
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
            _ => {
                // Other plans, should be cached
                Err(Error::InvalidArgument(format!(
                    "Direct execution not supported for plan: {:?}",
                    plan
                )))
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
    pub fn execute_cached(&self, plan: Arc<PhysicalPlan>, parameters: Parameters) -> Result<QueryOutput> {
        match plan.as_ref() {
            PhysicalPlan::CollectionScan {
                collection,
                start,
                end,
                direction,
                projection: _, // Projection pushdown is not yet supported at this level
            } => {
                let range = Self::bind_key_range_parameters(start, end, &parameters)?;

                Ok(Box::new(self.storage_engine.range_scan(
                    *collection,
                    0, // This is table scan so index is 0
                    &range,
                    None,
                    direction.clone(),
                )?.filter_map(|res| {
                    match res {
                        Ok((k, v)) => {
                            let op = extract_operation_type(&k);
                            match op {
                                OperationType::Delete => None,
                                OperationType::Put => {
                                    // Deserialize the value into a Document
                                    let doc = Document::from_reader(Cursor::new(v));
                                    match doc {
                                        Err(e) => Some(Err(e.into())),
                                        Ok(doc) => Some(Ok(doc))
                                    }
                                },
                                _ => Some(Err(Error::InvalidState(format!("Unexpected operation type: {:?}", op)))),
                            }
                        }
                        Err(e) => Some(Err(e.into())),
                    }
                })))
            }
            PhysicalPlan::PointSearch {
                collection,
                index,
                key,
                projection: _,
            } => {
                let key = Self::bind_key_parameter(key, &parameters)?;
                let result = self.storage_engine.read(*collection, *index, &key, None)?;
                let iter: QueryOutput = match result {
                    Some((k, v)) => {
                        let op = extract_operation_type(&k);
                        match op {
                            OperationType::Delete => Box::new(std::iter::empty()),
                            OperationType::Put => {
                                Box::new(std::iter::once(Document::from_reader(Cursor::new(v))
                                    .map_err(|e| e.into())))
                            }
                            _ => return Err(Error::InvalidState(format!("Unexpected operation type: {:?}", op))),
                        }
                    }
                    None => Box::new(std::iter::empty()),
                };
                Ok(iter)
            }
            PhysicalPlan::IndexScan {
                collection,
                index,
                start ,
                end,
                projection: _,
            } => {
                todo!()
            }
            PhysicalPlan::Filter { input, predicate } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                let predicate = predicate.clone();
                let filtered_iter = input_iter.filter_map(move |res| match res {
                    Ok(doc) => match eval_predicate(&doc, &predicate) {
                        Ok(true) => Some(Ok(doc)),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                });
                Ok(Box::new(filtered_iter))
            }
            PhysicalPlan::Projection { input, projection } => {
                todo!()
            }
            PhysicalPlan::InMemorySort {
                input,
                sort_fields,
            } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                sort::in_memory_sort(input_iter, &sort_fields)
            }
            PhysicalPlan::ExternalMergeSort {
                input,
                sort_fields,
                max_in_memory_rows,
            } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                sort::external_merge_sort(input_iter, sort_fields.clone(), *max_in_memory_rows)
            }
            PhysicalPlan::TopKHeapSort {
                input,
                sort_fields,
                k,
            } => {
                let input_iter = self.execute_cached(input.clone(), parameters)?;
                sort::top_k_heap_sort(input_iter, sort_fields.clone(), *k)
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
                Err(Error::InvalidArgument(format!(
                    "Non-parametrized physical plan: {:?}",
                    plan
                )))
            }
        }
    }

    fn bind_key_range_parameters(start: &Bound<Expr>, end: &Bound<Expr>, parameters: &Parameters) -> Result<Interval<Vec<u8>>> {
        let start = Self::bind_key_bound_parameter(start, &parameters)?;
        let end = Self::bind_key_bound_parameter(end, &parameters)?;
        let range = Interval::new(start, end);
        Ok(range)
    }

    fn bind_key_bound_parameter(start: &Bound<Expr>, parameters: &Parameters) -> Result<Bound<Vec<u8>>> {
        let start = match start {
            Bound::Included(b) => Bound::Included(Self::bind_parameter(b, &parameters)?.try_into_key()?),
            Bound::Excluded(b) => Bound::Excluded(Self::bind_parameter(b, &parameters)?.try_into_key()?),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(start)
    }

    fn bind_bound_parameter(start: &Bound<Expr>, parameters: &Parameters) -> Result<Bound<BsonValue>> {
        let start = match start {
            Bound::Included(b) => Bound::Included(Self::bind_parameter(b, &parameters)?),
            Bound::Excluded(b) => Bound::Excluded(Self::bind_parameter(b, &parameters)?),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(start)
    }

    fn bind_key_parameter(expr: &Expr, parameters: &Parameters) -> Result<Vec<u8>> {
        if let Expr::Placeholder(idx) = expr {
            Ok(parameters.get(*idx)?.try_into_key()?)
        } else {
            Err(Error::InvalidState(format!("Expecting placeholder but was: {:?}", expr)))
        }
    }

    fn bind_parameter(expr: &Expr, parameters: &Parameters) -> Result<BsonValue> {
        if let Expr::Placeholder(idx) = expr {
            Ok(parameters.get(*idx)?.clone())
        } else {
            Err(Error::InvalidState(format!("Expecting placeholder but was: {:?}", expr)))
        }
    }
}

/// Evaluates a predicate expression against a document.
fn eval_predicate(doc: &Document, predicate: &Expr) -> Result<bool> {
    match predicate {
        Expr::And(exprs) => {
            for expr in exprs {
                if !eval_predicate(doc, expr)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(exprs) => {
            for expr in exprs {
                if eval_predicate(doc, expr)? {
                    return Ok(true);
                }
            }
            Ok(exprs.is_empty())
        }
        Expr::Not(expr) => Ok(!eval_predicate(doc, expr)?),
        Expr::FieldFilters { field, filters } => {
            let field_path = if let Expr::Field(path) = field.as_ref() {
                path
            } else {
                return Err(Error::InvalidArgument(format!("Unsupported field expression in filter: {:?}", field)));
            };

            let doc_val = get_path_value(doc, field_path);

            for filter in filters {
                if !eval_filter(doc_val, filter)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Err(Error::InvalidArgument(format!(
            "Unsupported predicate expression: {:?}",
            predicate
        ))),
    }
}

/// Evaluates a single filter condition against a value from a document.
fn eval_filter(doc_val: Option<BsonValueRef>, filter: &Expr) -> Result<bool> {
    match filter {
        Expr::Comparison { operator, value } => {
            let literal = if let Expr::Literal(val) = value.as_ref() {
                val
            } else {
                return Err(Error::InvalidArgument("Comparison value must be a literal".to_string()));
            };
            let doc_val = doc_val.unwrap_or(BsonValueRef(&Bson::Null));

            let literal = literal.as_ref();
            let ordering = doc_val.cmp(&literal);

            Ok(match operator {
                ComparisonOperator::Eq => doc_val == literal,
                ComparisonOperator::Ne => doc_val != literal,
                ComparisonOperator::Gt => ordering == Ordering::Greater,
                ComparisonOperator::Gte => ordering != Ordering::Less,
                ComparisonOperator::Lt => ordering == Ordering::Less,
                ComparisonOperator::Lte => ordering != Ordering::Greater,
                _ => return Err(Error::InvalidArgument(format!(
                    "Unsupported comparison operator: {:?}",
                    operator
                ))),
            })
        }
        Expr::Exists(exists) => {
            let value_exists = doc_val.is_some() && doc_val != Some(BsonValueRef(&Bson::Null));
            Ok(value_exists == *exists)
        }
        _ => Err(Error::InvalidArgument(format!("Unsupported filter expression: {:?}", filter))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::query::{make_sort_field, SortOrder};
    use crate::query::{BsonValue, Parameters};
    use crate::storage::test_utils::storage_engine;
    use crate::storage::Direction;
    use bson::{doc, Bson, Document};
    use std::ops::Bound;
    use std::sync::Arc;

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
        let mut insert_one_result = executor.execute_direct(insert_one_plan)?;
        let result_doc = insert_one_result.next().unwrap()?;
        assert!(insert_one_result.next().is_none());
        let inserted_id1 = result_doc.get("inserted_id").unwrap().clone();

        // 3. PointSearch for the inserted doc
        let mut params = Parameters::new();
        let key_expr = params.collect_parameter(BsonValue(inserted_id1.clone()));
        let point_search_plan = Arc::new(PhysicalPlan::PointSearch {
            collection: collection_id,
            index: 0, // primary key
            key: (*key_expr).clone(),
            projection: None,
        });

        let mut point_search_result = executor.execute_cached(point_search_plan, params)?;
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

        let mut insert_many_result = executor.execute_direct(insert_many_plan)?;
        let result_doc_many = insert_many_result.next().unwrap()?;
        assert!(insert_many_result.next().is_none());
        let inserted_ids = result_doc_many.get_array("inserted_ids").unwrap();
        assert_eq!(inserted_ids.len(), 2);
        let inserted_id2 = inserted_ids[0].clone();
        let inserted_id3 = inserted_ids[1].clone();

        // 5. CollectionScan
        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            direction: Direction::Forward,
            projection: None,
        });

        let scan_results = executor.execute_cached(scan_plan, Parameters::new())?;
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
            let mut result = executor.execute_direct(insert_plan)?;
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
            index: 0, // primary key
            key: (*key_expr_non_exist).clone(),
            projection: None,
        });
        let mut result_non_exist = executor.execute_cached(plan_non_exist, params_non_exist)?;
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
            index: 0, // primary key
            key: (*key_expr_deleted).clone(),
            projection: None,
        });
        let mut result_deleted = executor.execute_cached(plan_deleted, params_deleted)?;
        assert!(
            result_deleted.next().is_none(),
            "PointSearch for deleted key should be empty"
        );

        // 4. CollectionScan with range completely outside data
        let mut params_scan_outside = Parameters::new();
        let start_outside = params_scan_outside.collect_parameter(BsonValue(Bson::Int32(100)));
        let plan_scan_outside = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            start: Bound::Included((*start_outside).clone()),
            end: Bound::Unbounded,
            direction: Direction::Forward,
            projection: None,
        });
        let mut result_scan_outside =
            executor.execute_cached(plan_scan_outside, params_scan_outside)?;
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
            start: Bound::Included((*start_partial).clone()),
            end: Bound::Excluded((*end_partial).clone()),
            direction: Direction::Forward,
            projection: None,
        });
        let mut result_scan_partial =
            executor.execute_cached(plan_scan_partial, params_scan_partial)?;
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
            start: Bound::Unbounded,
            end: Bound::Excluded((*end_unbounded_start).clone()),
            direction: Direction::Forward,
            projection: None,
        });
        let mut result_unbounded_start =
            executor.execute_cached(plan_scan_unbounded_start, params_scan_unbounded_start)?;
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
            start: Bound::Excluded((*start_unbounded_end).clone()),
            end: Bound::Unbounded,
            direction: Direction::Forward,
            projection: None,
        });
        let mut result_unbounded_end =
            executor.execute_cached(plan_scan_unbounded_end, params_scan_unbounded_end)?;
        let found_doc_unbounded_end = result_unbounded_end.next().unwrap()?;
        assert_eq!(found_doc_unbounded_end, doc3.clone());
        assert!(
            result_unbounded_end.next().is_none(),
            "Scan with unbounded end should find one doc"
        );

        // 8. CollectionScan with full range in reverse
        let plan_scan_reverse = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            direction: Direction::Reverse,
            projection: None,
        });
        let result_scan_reverse = executor.execute_cached(plan_scan_reverse, Parameters::new())?;
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
            let mut result = executor.execute_direct(insert_plan)?;
            result.next().unwrap()?;
        }

        // 3. Create a base scan plan to feed the limit plan
        let scan_plan = Arc::new(PhysicalPlan::CollectionScan {
            collection: collection_id,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            direction: Direction::Forward,
            projection: None,
        });

        // 4. Test cases

        // Case 1: limit only
        let limit_plan = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: None,
            limit: Some(3),
        });
        let results = executor.execute_cached(limit_plan, Parameters::new())?;
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
        let results_skip = executor.execute_cached(limit_plan_skip, Parameters::new())?;
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
        let results_both = executor.execute_cached(limit_plan_both, Parameters::new())?;
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
        let results_large = executor.execute_cached(limit_plan_large, Parameters::new())?;
        let found_docs_large: Vec<Document> = results_large.collect::<Result<Vec<_>>>()?;
        assert_eq!(found_docs_large.len(), 5);

        // Case 5: skip > number of docs
        let limit_plan_skip_large = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: Some(10),
            limit: None,
        });
        let results_skip_large =
            executor.execute_cached(limit_plan_skip_large, Parameters::new())?;
        let found_docs_skip_large: Vec<Document> =
            results_skip_large.collect::<Result<Vec<_>>>()?;
        assert!(found_docs_skip_large.is_empty());

        // Case 6: limit is zero
        let limit_plan_zero = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: None,
            limit: Some(0),
        });
        let results_zero = executor.execute_cached(limit_plan_zero, Parameters::new())?;
        let found_docs_zero: Vec<Document> = results_zero.collect::<Result<Vec<_>>>()?;
        assert!(found_docs_zero.is_empty());

        // Case 7: skip and limit over edge
        let limit_plan_edge = Arc::new(PhysicalPlan::Limit {
            input: scan_plan.clone(),
            skip: Some(3),
            limit: Some(5), // limit is larger than remaining items
        });
        let results_edge = executor.execute_cached(limit_plan_edge, Parameters::new())?;
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
        let results = executor.execute_cached(plan, Parameters::new())?;
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
            let mut result = executor.execute_direct(insert_plan)?;
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
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            direction: Direction::Forward,
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
}
