use crate::error::{Error, Result};
use crate::query::logical::logical_plan::{SortField, SortOrder};
use crate::query::logical::physical_plan::PhysicalPlan;
use crate::query::logical::{BsonValue, ComparisonOperator, Expr, PathComponent};
use crate::storage::storage_engine::StorageEngine;
use crate::storage::Direction;
use bson::{Bson, Document};
use std::cmp::Ordering;
use std::sync::Arc;
use crate::util::bson_utils;

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

    /// Executes the given physical plan.
    pub fn execute(&self, plan: Arc<PhysicalPlan>) -> Result<QueryOutput> {
        match plan.as_ref() {
            PhysicalPlan::TableScan {
                collection,
                range,
                direction,
                projection: _, // Projection pushdown is not yet supported at this level
            } => {
                Ok(Box::new(self.storage_engine.range_scan(
                        *collection,
                        0, // This is table scan so index is 0
                        range,
                        None,
                        direction.clone(),
                    )?.map(|res| {
                        res.map_err(Into::into)
                        .and_then(|kv| bson::from_slice(&kv.1).map_err(Into::into))
                    })))
            }
            PhysicalPlan::PointSearch {
                collection,
                index,
                key,
                projection: _,
            } => {
                let result = self.storage_engine.read(*collection, *index, &key, None)?;
                let iter: QueryOutput = match result {
                    Some(kv_pair) => {
                        let doc = bson::from_slice(&kv_pair.1).map_err(Into::into);
                        Box::new(std::iter::once(doc))
                    }
                    None => Box::new(std::iter::empty()),
                };
                Ok(iter)
            }
            PhysicalPlan::IndexScan {
                collection,
                index,
                range,
                projection: _,
            } => {
                Ok(Box::new(self.storage_engine.range_scan(
                    *collection,
                    *index,
                    range,
                    None,
                    Direction::Forward,
                )?.map(|res| {
                    res.map_err(Into::into)
                        .and_then(|kv| bson::from_slice(&kv.1).map_err(Into::into))
                })))
            }
            PhysicalPlan::Insert { .. } => {
                // To implement Insert, we need the definition of `storage::write_batch::Operation`
                // and a way to construct primary keys and handle index updates.
                todo!("Insert operation not yet implemented")
            }
            PhysicalPlan::Filter { input, predicate } => {
                let input_iter = self.execute(input.clone())?;
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
            PhysicalPlan::Projection { input, fields } => {
                let input_iter = self.execute(input.clone())?;
                let fields = fields.clone();
                let projected_iter = input_iter.map(move |res| {
                    res.map(|doc| {
                        let mut new_doc = Document::new();
                        for (new_name, old_name) in &fields {
                            if let Some(value) = doc.get(old_name) {
                                new_doc.insert(new_name.clone(), value.clone());
                            }
                        }
                        new_doc
                    })
                });
                Ok(Box::new(projected_iter))
            }
            PhysicalPlan::InMemorySort {
                input,
                sort_fields,
            } => {
                let input_iter = self.execute(input.clone())?;
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
                let mut iter = self.execute(input.clone())?;
                if let Some(s) = skip {
                    iter = Box::new(iter.skip(*s));
                }
                if let Some(l) = limit {
                    iter = Box::new(iter.take(*l));
                }
                Ok(iter)
            }
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
