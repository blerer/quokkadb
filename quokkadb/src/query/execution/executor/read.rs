use super::{ExecutionOutput, ExecutionRow, Metrics, bind};
use crate::error::{Error, Result};
use crate::query::execution::{filters, indexes::Index, projections, sorts};
use crate::query::physical_plan::{IndexScanRangeExpr, PhysicalPlan};
use crate::query::{BsonValue, Expr, Parameters};
use crate::storage::Direction;
use crate::storage::ScanCachePolicy;
use crate::storage::internal_key::{extract_operation_type, extract_record_key};
use crate::storage::operation::OperationType;
use crate::storage::snapshot_manager::Snapshot;
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;
use bson::Bson;
use std::sync::Arc;
use tracing::{Span, trace_span};

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

    /// Executes a cached plan under a snapshot lease while preserving source BSON sizes.
    ///
    /// When `snapshot` is `None`, the executor acquires a fresh snapshot for the
    /// full operation and keeps it alive until the returned iterator is dropped.
    pub(crate) fn execute_cached_at_snapshot(
        &self,
        plan: Arc<PhysicalPlan>,
        parameters: &Parameters,
        snapshot: Option<Snapshot>,
    ) -> Result<ExecutionOutput> {
        let snapshot = snapshot.unwrap_or_else(|| self.storage_engine.acquire_snapshot());
        let output = self.execute_cached_with_snapshot(plan, parameters, snapshot.clone())?;
        Ok(Box::new(SnapshotTrackedExecutionOutput::new(
            output, snapshot,
        )))
    }

    fn execute_cached_with_snapshot(
        &self,
        plan: Arc<PhysicalPlan>,
        parameters: &Parameters,
        snapshot: Snapshot,
    ) -> Result<ExecutionOutput> {
        if let PhysicalPlan::CollectionScan { collection, .. }
        | PhysicalPlan::PointSearch { collection, .. }
        | PhysicalPlan::MultiPointSearch { collection, .. } = plan.as_ref()
        {
            self.collection_at_snapshot(*collection, &snapshot)?;
        }
        match plan.as_ref() {
            PhysicalPlan::NoOp => Ok(Box::new(std::iter::empty())),
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
                    self.execute_cached_with_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(input_iter.filter(move |res| {
                    if res.is_err() {
                        true
                    } else {
                        filter(&res.as_ref().unwrap().document)
                    }
                })))
            }
            PhysicalPlan::Projection { input, projection } => {
                let projector = projections::to_projector(projection, parameters)?;
                let input_iter =
                    self.execute_cached_with_snapshot(input.clone(), parameters, snapshot)?;
                Ok(Box::new(input_iter.map(move |res| {
                    res.and_then(|mut row| {
                        row.document = projector(row.document)?;
                        Ok(row)
                    })
                })))
            }
            PhysicalPlan::ExternalMergeSort {
                input,
                sort_fields,
                max_in_memory_rows,
            } => {
                self.metrics.external_merge_sorts.inc();
                let input_iter =
                    self.execute_cached_with_snapshot(input.clone(), parameters, snapshot)?;
                sorts::external_merge_sort(
                    input_iter,
                    sort_fields.clone(),
                    *max_in_memory_rows,
                    sorts::SpillMetrics {
                        spills: self.metrics.external_merge_spills.clone(),
                        spill_bytes: self.metrics.external_merge_spill_bytes.clone(),
                    },
                )
            }
            PhysicalPlan::TopKHeapSort {
                input,
                sort_fields,
                k,
            } => {
                self.metrics.top_k_sorts.inc();
                let input_iter =
                    self.execute_cached_with_snapshot(input.clone(), parameters, snapshot)?;
                sorts::top_k_heap_sort(input_iter, sort_fields.clone(), *k)
            }
            PhysicalPlan::Limit { input, skip, limit } => {
                let mut iter =
                    self.execute_cached_with_snapshot(input.clone(), parameters, snapshot)?;
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
        snapshot: Snapshot,
        collection: &u32,
        keys: &Arc<Expr>,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<ExecutionOutput> {
        let access_span = trace_span!(
            "multi_point_search",
            snapshot = snapshot.sequence(),
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
        let snapshot_for_reads = snapshot.clone();

        let iter = key_iterator.filter_map(move |key| match key.try_into_key() {
            Ok(storage_key) => match storage_engine.read_at_snapshot(
                collection,
                0,
                &storage_key,
                &snapshot_for_reads,
            ) {
                Ok(Some((k, v))) => {
                    let op = extract_operation_type(&k);
                    if op == OperationType::Put {
                        match ExecutionRow::from_bson_bytes(&v) {
                            Ok(doc) => {
                                if filter.as_ref().is_none_or(|f| f(&doc.document)) {
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

        Ok(Box::new(AccessTrackedExecutionOutput::new(
            Box::new(iter),
            access_span,
        )))
    }

    pub(super) fn perform_point_search(
        &self,
        parameters: &Parameters,
        snapshot: Snapshot,
        collection: &u32,
        key: &Arc<Expr>,
        filter: &Option<Arc<Expr>>,
    ) -> Result<ExecutionOutput> {
        let access_span = trace_span!("point_search", snapshot = snapshot.sequence());
        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, parameters)));

        let key = bind::bind_key_parameter(key, parameters)?;
        let storage_engine = self.storage_engine.clone();
        let collection = *collection;
        let snapshot_for_reads = snapshot.clone();
        let mut executed = false;
        let iter: ExecutionOutput = Box::new(std::iter::from_fn(move || {
            if executed {
                return None;
            }
            executed = true;

            let result =
                match storage_engine.read_at_snapshot(collection, 0, &key, &snapshot_for_reads) {
                    Ok(result) => result,
                    Err(err) => return Some(Err(err.into())),
                };

            match result {
                Some((k, v)) => {
                    let op = extract_operation_type(&k);
                    match op {
                        OperationType::Delete => None,
                        OperationType::Put => match ExecutionRow::from_bson_bytes(&v) {
                            Ok(doc) => match &filter {
                                Some(filter) if !filter(&doc.document) => None,
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
        Ok(Box::new(AccessTrackedExecutionOutput::new(
            iter,
            access_span,
        )))
    }

    pub(super) fn perform_collection_scan(
        &self,
        parameters: &Parameters,
        snapshot: Snapshot,
        collection: &u32,
        range: &Interval<Arc<Expr>>,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<ExecutionOutput> {
        let access_span = trace_span!(
            "collection_scan",
            snapshot = snapshot.sequence(),
            direction = ?direction
        );
        let range = bind::bind_key_range_parameters(range, parameters)?;

        // TODO: for now the filtering happen after deserialization to a document but should be perform in the future on the byte representation
        let filter = filter
            .clone()
            .and_then(|predicate| Some(filters::to_filter(predicate, parameters)));

        let iter = self
            .storage_engine
            .range_scan_at_snapshot(
                *collection,
                0,
                &range,
                &snapshot,
                direction.clone(),
                ScanCachePolicy::Normal,
            )?
            .filter_map(move |res| {
                let doc = match res {
                    Ok((k, v)) => {
                        let op = extract_operation_type(&k);
                        match op {
                            OperationType::Delete => return None,
                            OperationType::Put => {
                                let doc = ExecutionRow::from_bson_bytes(&v);
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
                        if filter(&doc.document) {
                            Some(Ok(doc))
                        } else {
                            None
                        }
                    }
                    None => Some(Ok(doc)),
                }
            });

        Ok(Box::new(AccessTrackedExecutionOutput::new(
            Box::new(iter),
            access_span,
        )))
    }

    fn collection_at_snapshot(
        &self,
        collection: u32,
        snapshot: &Snapshot,
    ) -> Result<Arc<crate::storage::catalog::CollectionMetadata>> {
        let catalog = self.storage_engine.catalog();
        catalog
            .get_collection_at(collection, snapshot.sequence())
            .ok_or_else(|| Error::CollectionNotFound {
                name: catalog
                    .get_collection_by_id(&collection)
                    .map(|metadata| metadata.name.clone())
                    .unwrap_or_default(),
                id: Some(collection),
            })
    }

    pub(super) fn perform_index_scan(
        &self,
        parameters: &Parameters,
        snapshot: Snapshot,
        collection: &u32,
        index: &u32,
        range: &IndexScanRangeExpr,
        direction: &Direction,
        filter: &Option<Arc<Expr>>,
    ) -> Result<ExecutionOutput> {
        let access_span = trace_span!(
            "index_scan",
            index = *index,
            snapshot = snapshot.sequence(),
            direction = ?direction
        );
        let collection_metadata = self.collection_at_snapshot(*collection, &snapshot)?;
        let index_metadata = collection_metadata
            .get_queryable_index_at(*index, snapshot.sequence())
            .ok_or_else(|| Error::IndexNotFound {
                collection_name: collection_metadata.name.clone(),
                index_name: collection_metadata
                    .get_index_by_id(*index)
                    .map(|metadata| metadata.name())
                    .unwrap_or_else(|| format!("index id {index}")),
                id: Some(*index),
            })?;
        let index_codec = Index::from(*collection, &index_metadata);
        let bound_range = index_codec.bind_range_expr(range, parameters)?;

        let filter = filter
            .clone()
            .map(|predicate| filters::to_filter(predicate, parameters));

        let storage_engine = self.storage_engine.clone();
        let collection = *collection;
        let secondary_index = *index;
        let primary_lookup_span = access_span.clone();
        let snapshot_for_reads = snapshot.clone();

        let iter = storage_engine
            .range_scan_at_snapshot(
                collection,
                secondary_index,
                &bound_range,
                &snapshot,
                direction.clone(),
                ScanCachePolicy::Normal,
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
                        snapshot = snapshot_for_reads.sequence()
                    )
                    .entered();

                    match storage_engine.read_at_snapshot(
                        collection,
                        0,
                        &primary_key,
                        &snapshot_for_reads,
                    ) {
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

                let doc = match ExecutionRow::from_bson_bytes(&doc_bytes) {
                    Ok(doc) => doc,
                    Err(e) => return Some(Err(e.into())),
                };

                match &filter {
                    Some(filter) if !filter(&doc.document) => None,
                    _ => Some(Ok(doc)),
                }
            });

        Ok(Box::new(AccessTrackedExecutionOutput::new(
            Box::new(iter),
            access_span,
        )))
    }
}

struct SnapshotTrackedExecutionOutput {
    inner: ExecutionOutput,
    _snapshot: Snapshot,
}

impl SnapshotTrackedExecutionOutput {
    fn new(inner: ExecutionOutput, snapshot: Snapshot) -> Self {
        Self {
            inner,
            _snapshot: snapshot,
        }
    }
}

impl Iterator for SnapshotTrackedExecutionOutput {
    type Item = Result<ExecutionRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

struct AccessTrackedExecutionOutput {
    inner: ExecutionOutput,
    span: Span,
    started: bool,
    finished: bool,
}

impl AccessTrackedExecutionOutput {
    fn new(inner: ExecutionOutput, span: Span) -> Self {
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

impl Iterator for AccessTrackedExecutionOutput {
    type Item = Result<ExecutionRow>;

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

impl Drop for AccessTrackedExecutionOutput {
    fn drop(&mut self) {
        let span = self.span.clone();
        let _span = span.enter();
        self.finish_if_needed();
    }
}

#[cfg(test)]
mod tests;
