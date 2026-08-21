use super::{bind, Metrics, QueryOutput};
use crate::error::Result;
use crate::query::execution::{filters, indexes::Index};
use crate::query::physical_plan::{IndexScanRangeExpr, PhysicalPlan};
use crate::query::{BsonValue, Expr, Parameters};
use crate::storage::internal_key::{extract_operation_type, extract_record_key};
use crate::storage::operation::OperationType;
use crate::storage::Direction;
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;
use bson::{Bson, Document};
use std::io::Cursor;
use std::sync::Arc;
use tracing::{trace_span, Span};

use crate::storage::storage_engine::StorageEngine;

/// Internal role for read execution.
///
/// This role owns cached-plan dispatch and depends only on read-specific state.
#[derive(Clone)]
pub(crate) struct ReadExecutor {
    pub(super) storage_engine: Arc<StorageEngine>,
    metrics: Metrics,
}

impl ReadExecutor {
    pub(crate) fn new(storage_engine: Arc<StorageEngine>, metrics: Metrics) -> Self {
        Self {
            storage_engine,
            metrics,
        }
    }

    pub(crate) fn execute_cached_at_snapshot(
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
                projection: _,
            } => {
                self.metrics.collection_scans.inc();
                self.perform_collection_scan(
                    parameters, snapshot, collection, range, direction, filter,
                )
            }
            PhysicalPlan::PointSearch {
                collection,
                key,
                filter,
                projection: _,
            } => {
                self.metrics.point_searches.inc();
                self.perform_point_search(parameters, snapshot, collection, key, filter)
            }
            PhysicalPlan::IndexScan {
                collection,
                index,
                range,
                direction,
                filter,
                projection: _,
            } => {
                self.metrics.index_scans.inc();
                self.perform_index_scan(
                    parameters, snapshot, collection, index, range, direction, filter,
                )
            }
            PhysicalPlan::MultiPointSearch {
                collection,
                keys,
                direction,
                filter,
                projection: _,
            } => {
                self.metrics.multi_point_searches.inc();
                self.perform_multi_point_search(
                    parameters, snapshot, collection, keys, direction, filter,
                )
            }
            PhysicalPlan::Filter { input, predicate } => {
                let filter = filters::to_filter(predicate.clone(), parameters);
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
                let projector =
                    crate::query::execution::projections::to_projector(projection, parameters)?;
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(
                    input_iter.map(move |res| res.and_then(|doc| projector(doc))),
                ))
            }
            PhysicalPlan::InMemorySort { input, sort_fields } => {
                self.metrics.in_memory_sorts.inc();
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                crate::query::execution::sorts::in_memory_sort(input_iter, sort_fields)
            }
            PhysicalPlan::ExternalMergeSort {
                input,
                sort_fields,
                max_in_memory_rows,
            } => {
                self.metrics.external_merge_sorts.inc();
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                crate::query::execution::sorts::external_merge_sort(
                    input_iter,
                    sort_fields.clone(),
                    *max_in_memory_rows,
                )
            }
            PhysicalPlan::TopKHeapSort {
                input,
                sort_fields,
                k,
            } => {
                self.metrics.top_k_sorts.inc();
                let input_iter =
                    self.execute_cached_at_snapshot(input.clone(), parameters, snapshot)?;
                crate::query::execution::sorts::top_k_heap_sort(input_iter, sort_fields.clone(), *k)
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
            _ => unreachable!("Non-parametrized physical plan: {:?}", plan),
        }
    }

    pub(super) fn perform_multi_point_search(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        keys: &Arc<Expr>,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let access_span = trace_span!(
            "multi_point_search",
            snapshot = ?snapshot,
            direction = ?direction
        );
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, parameters)));

        let keys_values = bind::bind_parameter(keys, parameters);
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
            Ok(storage_key) => match storage_engine.read(collection, 0, &storage_key, snapshot) {
                Ok(Some((k, v))) => {
                    let op = extract_operation_type(&k);
                    if op == OperationType::Put {
                        match Document::from_reader(Cursor::new(v)) {
                            Ok(doc) => {
                                if filter.as_ref().is_none_or(|f| f(&doc)) {
                                    Some(Ok(doc))
                                } else {
                                    None
                                }
                            }
                            Err(e) => Some(Err(e.into())),
                        }
                    } else {
                        None
                    }
                }
                Ok(None) => None,
                Err(e) => Some(Err(e.into())),
            },
            Err(e) => Some(Err(e.into())),
        });

        Ok(Box::new(AccessTrackedQueryOutput::new(
            Box::new(iter),
            access_span,
        )))
    }

    pub(super) fn perform_point_search(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        key: &Arc<Expr>,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let access_span = trace_span!(
            "point_search",
            snapshot = ?snapshot
        );
        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, parameters)));

        let key = bind::bind_key_parameter(key, parameters)?;
        let storage_engine = self.storage_engine.clone();
        let collection = *collection;
        let mut executed = false;
        let iter: QueryOutput = Box::new(std::iter::from_fn(move || {
            if executed {
                return None;
            }
            executed = true;

            let result = match storage_engine.read(collection, 0, &key, snapshot) {
                Ok(result) => result,
                Err(err) => return Some(Err(err.into())),
            };

            match result {
                Some((k, v)) => {
                    let op = extract_operation_type(&k);
                    match op {
                        OperationType::Delete => None,
                        OperationType::Put => match Document::from_reader(Cursor::new(v)) {
                            Ok(doc) => match &filter {
                                Some(filter) if !filter(&doc) => None,
                                _ => Some(Ok(doc)),
                            },
                            Err(err) => Some(Err(err.into())),
                        },
                        _ => unreachable!("Unexpected operation type: {:?}", op),
                    }
                }
                None => None,
            }
        }));
        Ok(Box::new(AccessTrackedQueryOutput::new(iter, access_span)))
    }

    pub(super) fn perform_collection_scan(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        range: &Interval<Arc<Expr>>,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let access_span = trace_span!(
            "collection_scan",
            snapshot = ?snapshot,
            direction = ?direction
        );
        let range = bind::bind_key_range_parameters(range, parameters)?;

        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, parameters)));

        let iter = self
            .storage_engine
            .range_scan(*collection, 0, &range, snapshot, direction.clone())?
            .filter_map(move |res| {
                let doc = match res {
                    Ok((k, v)) => {
                        let op = extract_operation_type(&k);
                        match op {
                            OperationType::Delete => return None,
                            OperationType::Put => {
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
            });

        Ok(Box::new(AccessTrackedQueryOutput::new(
            Box::new(iter),
            access_span,
        )))
    }

    pub(super) fn perform_index_scan(
        &self,
        parameters: &Parameters,
        snapshot: Option<u64>,
        collection: &u32,
        index: &u32,
        range: &IndexScanRangeExpr,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<QueryOutput> {
        let access_span = trace_span!(
            "index_scan",
            index = *index,
            snapshot = ?snapshot,
            direction = ?direction
        );
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
        let primary_lookup_span = access_span.clone();

        let iter = storage_engine
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

                let doc_bytes = {
                    let _lookup_span = trace_span!(
                        parent: &primary_lookup_span,
                        "primary_lookup",
                        source_index = secondary_index,
                        snapshot = ?snapshot
                    )
                    .entered();

                    match storage_engine.read(collection, 0, &primary_key, snapshot) {
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
                    }
                };

                let doc = match Document::from_reader(Cursor::new(doc_bytes)) {
                    Ok(doc) => doc,
                    Err(e) => return Some(Err(e.into())),
                };

                match &filter {
                    Some(filter) if !filter(&doc) => None,
                    _ => Some(Ok(doc)),
                }
            });

        Ok(Box::new(AccessTrackedQueryOutput::new(
            Box::new(iter),
            access_span,
        )))
    }
}

struct AccessTrackedQueryOutput {
    inner: QueryOutput,
    span: Span,
    started: bool,
    finished: bool,
}

impl AccessTrackedQueryOutput {
    fn new(inner: QueryOutput, span: Span) -> Self {
        Self {
            inner,
            span,
            started: false,
            finished: false,
        }
    }

    fn start_if_needed(&mut self) {
        if self.started {
            return;
        }

        self.started = true;
    }

    fn finish_if_needed(&mut self) {
        if self.finished {
            return;
        }

        self.finished = true;
    }
}

impl Iterator for AccessTrackedQueryOutput {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        let span = self.span.clone();
        let _span = span.enter();
        self.start_if_needed();

        let next = self.inner.next();
        if next.is_none() {
            self.finish_if_needed();
        }

        next
    }
}

impl Drop for AccessTrackedQueryOutput {
    fn drop(&mut self) {
        let span = self.span.clone();
        let _span = span.enter();
        self.finish_if_needed();
    }
}

#[cfg(test)]
mod tests;
