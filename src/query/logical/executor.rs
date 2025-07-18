use crate::error::{Error, Result};
use crate::query::logical::logical_plan::{SortField, SortOrder};
use crate::query::logical::physical_plan::PhysicalPlan;
use crate::query::logical::{BsonValue, ComparisonOperator, Expr, Parameters, PathComponent};
use crate::storage::operation::{Operation, OperationType};
use crate::storage::storage_engine::StorageEngine;
use crate::storage::write_batch::WriteBatch;
use crate::util::bson_utils::{self, BsonKey};
use bson::oid::ObjectId;
use bson::{Bson, Document, RawDocument};
use std::cmp::Ordering;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use crate::storage::internal_key::extract_operation_type;
use crate::util::interval::Interval;

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
                    // Ensure each document has an _id field
                    let id = RawDocument::from_bytes(&doc)?.get("_id")?;

                    let user_key = match id {
                        Some(id) => {
                            let bson: Bson = id.to_raw_bson().try_into()?;
                            ids.push(bson.clone());
                            bson.try_into_key()?
                        },
                        None => {
                            let bson = Bson::ObjectId(ObjectId::new());
                            bson_utils::prepend_field(&mut doc,"_id", &bson)?;
                            ids.push(bson.clone());
                            bson.try_into_key()?
                        }
                    };

                    operations.push(Operation::new_put(collection, 0, user_key, doc));
                }

                let batch = WriteBatch::new(operations);
                self.storage_engine.write(batch)?;

                let mut result = Document::new();
                let array = Bson::Array(ids.into_iter().map(Bson::from).collect());
                result.insert("inserted_ids", array);
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
                let mut rows: Vec<Document> = input_iter.collect::<Result<Vec<_>>>()?;
                rows.sort_by(|a, b| sort_documents(a, b, &sort_fields));
                Ok(Box::new(rows.into_iter().map(Ok)))
            }
            PhysicalPlan::ExternalMergeSort { .. } => {
                todo!("External merge sort not yet implemented")
            }
            PhysicalPlan::TopKHeapSort { .. } => {
                todo!("Top-K heap sort not yet implemented")
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
fn eval_filter(doc_val: Option<&Bson>, filter: &Expr) -> Result<bool> {
    match filter {
        Expr::Comparison { operator, value } => {
            let literal = if let Expr::Literal(val) = value.as_ref() {
                val
            } else {
                return Err(Error::InvalidArgument("Comparison value must be a literal".to_string()));
            };
            let doc_val_unwrapped = doc_val.unwrap_or(&Bson::Null);

            let ordering = bson_utils::cmp_bson(doc_val_unwrapped, &literal.0);
            let doc_bson_val = BsonValue(doc_val_unwrapped.clone());

            Ok(match operator {
                ComparisonOperator::Eq => doc_bson_val == *literal,
                ComparisonOperator::Ne => doc_bson_val != *literal,
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
            let value_exists = doc_val.is_some() && doc_val != Some(&Bson::Null);
            Ok(value_exists == *exists)
        }
        _ => Err(Error::InvalidArgument(format!("Unsupported filter expression: {:?}", filter))),
    }
}

/// Extracts a BSON value from a document given a path.
fn get_path_value<'a>(doc: &'a Document, path: &[PathComponent]) -> Option<&'a Bson> {
    if path.is_empty() {
        return None;
    }

    let mut current = match path.first()? {
        PathComponent::FieldName(name) => doc.get(name)?,
        _ => return None,
    };

    for component in path.iter().skip(1) {
        current = match (component, current) {
            (PathComponent::FieldName(name), Bson::Document(d)) => d.get(name)?,
            (PathComponent::ArrayElement(index), Bson::Array(a)) => a.get(*index)?,
            _ => return None,
        };
    }

    Some(current)
}

/// Compares two documents based on a list of sort fields.
fn sort_documents(a: &Document, b: &Document, sort_fields: &[SortField]) -> Ordering {
    for sf in sort_fields {
        if let Expr::Field(path) = sf.field.as_ref() {
            let val_a = get_path_value(a, path).unwrap_or(&Bson::Null);
            let val_b = get_path_value(b, path).unwrap_or(&Bson::Null);
            match bson_utils::cmp_bson(val_a, val_b) {
                Ordering::Equal => continue,
                ord => {
                    return if sf.order == SortOrder::Ascending {
                        ord
                    } else {
                        ord.reverse()
                    };
                }
            }
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical::logical_plan::{SortField, SortOrder};
    use crate::query::logical::{Expr, PathComponent};
    use bson::{doc, Bson};
    use std::cmp::Ordering;

    #[test]
    fn test_get_path_value() {
        let doc = doc! {
            "a": 1,
            "b": { "c": "hello" },
            "d": [10, 20, { "e": 30 }],
        };

        // Simple field
        assert_eq!(
            get_path_value(&doc, &[PathComponent::FieldName("a".to_string())]),
            Some(&Bson::Int32(1))
        );

        // Nested field
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("b".to_string()),
                    PathComponent::FieldName("c".to_string())
                ]
            ),
            Some(&Bson::String("hello".to_string()))
        );

        // Array element
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("d".to_string()),
                    PathComponent::ArrayElement(1)
                ]
            ),
            Some(&Bson::Int32(20))
        );

        // Nested in array
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("d".to_string()),
                    PathComponent::ArrayElement(2),
                    PathComponent::FieldName("e".to_string())
                ]
            ),
            Some(&Bson::Int32(30))
        );

        // Non-existent field
        assert_eq!(
            get_path_value(&doc, &[PathComponent::FieldName("z".to_string())]),
            None
        );

        // Partially correct path
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("b".to_string()),
                    PathComponent::FieldName("z".to_string())
                ]
            ),
            None
        );

        // Path into non-document
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("a".to_string()),
                    PathComponent::FieldName("z".to_string())
                ]
            ),
            None
        );

        // Index into non-array
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("a".to_string()),
                    PathComponent::ArrayElement(0)
                ]
            ),
            None
        );

        // Empty path
        assert_eq!(get_path_value(&doc, &[]), None);
    }

    fn make_sort_field(path: Vec<PathComponent>, order: SortOrder) -> SortField {
        SortField {
            field: Arc::new(Expr::Field(path)),
            order,
        }
    }

    #[test]
    fn test_sort_documents() {
        let doc1 = doc! { "a": 1, "b": "xyz", "c": { "d": 10 } };
        let doc2 = doc! { "a": 2, "b": "abc", "c": { "d": 20 } };
        let doc3 = doc! { "a": 2, "b": "xyz", "c": { "d": 5 } };

        // Sort by 'a' ascending
        let sort_fields = vec![make_sort_field(
            vec![PathComponent::FieldName("a".to_string())],
            SortOrder::Ascending,
        )];
        assert_eq!(sort_documents(&doc1, &doc2, &sort_fields), Ordering::Less);
        assert_eq!(
            sort_documents(&doc2, &doc1, &sort_fields),
            Ordering::Greater
        );
        assert_eq!(sort_documents(&doc2, &doc3, &sort_fields), Ordering::Equal);

        // Sort by 'a' descending
        let sort_fields = vec![make_sort_field(
            vec![PathComponent::FieldName("a".to_string())],
            SortOrder::Descending,
        )];
        assert_eq!(
            sort_documents(&doc1, &doc2, &sort_fields),
            Ordering::Greater
        );
        assert_eq!(sort_documents(&doc2, &doc1, &sort_fields), Ordering::Less);

        // Sort by 'b' ascending
        let sort_fields = vec![make_sort_field(
            vec![PathComponent::FieldName("b".to_string())],
            SortOrder::Ascending,
        )];
        assert_eq!(
            sort_documents(&doc1, &doc2, &sort_fields),
            Ordering::Greater
        ); // "xyz" > "abc"
        assert_eq!(sort_documents(&doc2, &doc1, &sort_fields), Ordering::Less);

        // Multi-key sort: 'a' asc, then 'b' asc
        let sort_fields = vec![
            make_sort_field(
                vec![PathComponent::FieldName("a".to_string())],
                SortOrder::Ascending,
            ),
            make_sort_field(
                vec![PathComponent::FieldName("b".to_string())],
                SortOrder::Ascending,
            ),
        ];
        // doc2(a:2, b:"abc") vs doc3(a:2, b:"xyz")
        assert_eq!(sort_documents(&doc2, &doc3, &sort_fields), Ordering::Less);

        // Multi-key sort: 'a' asc, then 'c.d' desc
        let sort_fields = vec![
            make_sort_field(
                vec![PathComponent::FieldName("a".to_string())],
                SortOrder::Ascending,
            ),
            make_sort_field(
                vec![
                    PathComponent::FieldName("c".to_string()),
                    PathComponent::FieldName("d".to_string()),
                ],
                SortOrder::Descending,
            ),
        ];
        // doc2(a:2, c.d:20) vs doc3(a:2, c.d:5) -> 20 > 5, so with desc it's Less
        assert_eq!(sort_documents(&doc2, &doc3, &sort_fields), Ordering::Less);

        // Sort on nested key
        let sort_fields = vec![make_sort_field(
            vec![
                PathComponent::FieldName("c".to_string()),
                PathComponent::FieldName("d".to_string()),
            ],
            SortOrder::Ascending,
        )];
        assert_eq!(sort_documents(&doc1, &doc2, &sort_fields), Ordering::Less); // 10 < 20
        assert_eq!(sort_documents(&doc3, &doc1, &sort_fields), Ordering::Less); // 5 < 10

        // Field missing in one doc
        let doc4 = doc! { "b": "only b" };
        let sort_fields = vec![make_sort_field(
            vec![PathComponent::FieldName("a".to_string())],
            SortOrder::Ascending,
        )];
        // doc1 has "a": 1, doc4 has no "a", so it's Null. Null is smaller than anything else.
        assert_eq!(
            sort_documents(&doc1, &doc4, &sort_fields),
            Ordering::Greater
        );
        assert_eq!(sort_documents(&doc4, &doc1, &sort_fields), Ordering::Less);

        // No sort fields
        assert_eq!(sort_documents(&doc1, &doc2, &[]), Ordering::Equal);
    }
}
