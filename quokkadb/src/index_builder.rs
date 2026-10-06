use crate::obs::metrics::{self, AtomicGauge, Counter, MetricRegistry};
use crate::obs::metrics::names::index_build;
use crate::options::options::Options;
use crate::query::execution::indexes::{Indexes, OperationsCountAndSize};
use crate::storage::count_stats::{CountStats, CountStatsBuilder};
use crate::storage::index_build_state::{IndexBuildKey, IndexBuildState};
use crate::storage::internal_key::{extract_operation_type, extract_user_key};
use crate::storage::operation::{Operation, OperationType};
use crate::storage::snapshot_manager::Snapshot;
use crate::storage::storage_engine::{IndexBuildSnapshot, StorageEngine, StorageError};
use crate::storage::write_batch::{Preconditions, WriteBatch};
use crate::storage::Direction;
use bson::RawDocument;
#[cfg(test)]
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{Error, ErrorKind, Result};
use std::ops::Bound;
use std::sync::Arc;

struct Batch {
    operations: Vec<Operation>,
    count_stats: CountStatsBuilder,
    document_count: usize,
    payload_bytes: usize,
}

enum BatchCommitStatus {
    Committed,
    Cancelled,
}

pub(crate) struct IndexBuilder {
    storage_engine: Arc<StorageEngine>,
    max_payload_bytes: usize,
    metrics: Metrics,
    #[cfg(test)]
    test_hook: RefCell<Option<Box<dyn FnOnce()>>>,
}

impl IndexBuilder {
    pub(crate) fn new(
        metric_registry: &mut MetricRegistry,
        options: &Options,
        storage_engine: Arc<StorageEngine>,
    ) -> Self {
        let max_payload_bytes = options.index_build_batch_size().to_bytes();
        let metrics = Metrics::new();
        metrics.register_to(metric_registry);
        Self {
            storage_engine,
            max_payload_bytes,
            metrics,
            #[cfg(test)]
            test_hook: RefCell::new(None),
        }
    }

    #[cfg(test)]
    fn with_test_hook(mut self, hook: impl FnOnce() + 'static) -> Self {
        self.test_hook = RefCell::new(Some(Box::new(hook)));
        self
    }

    #[cfg(test)]
    fn run_test_hook(&self) {
        if let Some(hook) = self.test_hook.borrow_mut().take() {
            hook();
        }
    }

    pub(crate) fn cleanup_stale_index_build_states(
        &self,
        pending_builds: &[IndexBuildSnapshot],
    ) -> Result<()> {
        let pending_keys: BTreeSet<_> = pending_builds.iter().map(|build| build.key).collect();
        let snapshot = self.storage_engine.acquire_snapshot();
        let mut stale_keys = Vec::new();
        for entry in self.storage_engine.range_scan_at_snapshot(
            crate::storage::INTERNAL_INDEX_BUILD_COLLECTION_ID,
            0,
            &(..),
            &snapshot,
            Direction::Forward,
        )? {
            let (internal_key, _) = entry?;
            if extract_operation_type(&internal_key) == OperationType::Delete {
                continue;
            }

            let key = IndexBuildKey::decode(extract_user_key(&internal_key))?;
            if !pending_keys.contains(&key) {
                stale_keys.push(key);
            }
        }
        self.delete_states(stale_keys)
    }

    pub(crate) fn build_index(
        &self,
        key: IndexBuildKey,
        snapshot: &Snapshot,
    ) -> crate::error::Result<()> {
        let Some(indexes) = self.pending_index_for_build(key) else {
            self.delete_state(key)?;
            return Ok(());
        };
        let _active_build = self.metrics.start_build();
        match self.backfill_index(key, snapshot, indexes) {
            Ok(BackfillOutcome::Completed) => {
                self.metrics.succeeded.inc();
                Ok(())
            }
            Ok(BackfillOutcome::Cancelled) => {
                self.metrics.cancelled.inc();
                Ok(())
            }
            Err(build_error) => {
                self.metrics.failed.inc();
                tracing::error!(
                    collection_id = key.collection_id,
                    index_id = key.index_id,
                    error = %build_error,
                    "index backfill failed"
                );
                let drop_result = self
                    .storage_engine
                    .drop_index(key.collection_id, key.index_id);
                self.handle_build_failure(key, build_error, drop_result)
            }
        }
    }

    fn handle_build_failure(
        &self,
        key: IndexBuildKey,
        build_error: Error,
        drop_result: crate::storage::storage_engine::StorageResult<()>,
    ) -> crate::error::Result<()> {
        match drop_result {
            Ok(()) => match self.delete_state(key) {
                Ok(()) => Err(build_error.into()),
                Err(state_error) => {
                    tracing::error!(
                        collection_id = key.collection_id,
                        index_id = key.index_id,
                        backfill_error = %build_error,
                        state_cleanup_error = %state_error,
                        "failed to delete index build state after dropping index"
                    );
                    Err(state_error.into())
                }
            },
            Err(drop_error) => {
                tracing::error!(
                    collection_id = key.collection_id,
                    index_id = key.index_id,
                    backfill_error = %build_error,
                    drop_error = %drop_error,
                    "failed to drop index after backfill failure"
                );
                Err(drop_error.into())
            }
        }
    }

    fn backfill_index(
        &self,
        key: IndexBuildKey,
        snapshot: &Snapshot,
        indexes: Indexes,
    ) -> Result<BackfillOutcome> {
        let mut state = self.load_state(key)?;
        if state.processed_document_count > 0 {
            self.metrics.resumed.inc();
        }
        let user_key_range = match state.last_processed_primary_key.as_ref() {
            Some(primary_key) => (Bound::Excluded(primary_key.clone()), Bound::Unbounded),
            None => (Bound::Unbounded, Bound::Unbounded),
        };
        let mut entries = self.storage_engine.range_scan_at_snapshot(
            key.collection_id,
            0,
            &user_key_range,
            snapshot,
            Direction::Forward,
        )?;

        loop {
            let Some(batch) = self.read_batch(&mut entries, &indexes, &mut state)? else {
                break;
            };
            match self.commit_batch(key, &state, batch)? {
                BatchCommitStatus::Committed => {
                    #[cfg(test)]
                    self.run_test_hook();
                }
                BatchCommitStatus::Cancelled => {
                    tracing::debug!(
                        collection_id = key.collection_id,
                        index_id = key.index_id,
                        "index build stopped because its collection or index was dropped"
                    );
                    self.delete_state(key)?;
                    return Ok(BackfillOutcome::Cancelled);
                }
            }
        }

        match self
            .storage_engine
            .mark_index_queryable(key.collection_id, key.index_id)
        {
            Ok(()) => {}
            Err(StorageError::CollectionNotFound { .. } | StorageError::IndexNotFound { .. }) => {
                self.delete_state(key)?;
                return Ok(BackfillOutcome::Cancelled);
            }
            Err(error) => {
                return Err(Error::new(
                    error.as_io_error().map_or(ErrorKind::Other, Error::kind),
                    error.to_string(),
                ));
            }
        }
        self.delete_state(key)?;
        tracing::debug!(
            collection_id = key.collection_id,
            index_id = key.index_id,
            processed_document_count = state.processed_document_count,
            indexed_entry_count = state.indexed_entry_count,
            "index build scan completed and index is queryable"
        );
        Ok(BackfillOutcome::Completed)
    }

    fn load_state(&self, key: IndexBuildKey) -> Result<IndexBuildState> {
        let snapshot = self.storage_engine.acquire_snapshot();
        match self.storage_engine.read_at_snapshot(
            crate::storage::INTERNAL_INDEX_BUILD_COLLECTION_ID,
            0,
            &key.encode(),
            &snapshot,
        )? {
            Some((_, bytes)) => IndexBuildState::decode(&bytes),
            None => Ok(IndexBuildState::new()),
        }
    }

    fn read_batch(
        &self,
        entries: &mut dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
        indexes: &Indexes,
        state: &mut IndexBuildState,
    ) -> Result<Option<Batch>> {
        let mut batch = Batch {
            operations: Vec::new(),
            count_stats: CountStatsBuilder::new(),
            document_count: 0,
            payload_bytes: 0,
        };

        while batch.payload_bytes < self.max_payload_bytes {
            let Some(entry) = entries.next() else {
                break;
            };
            let (internal_key, value) = entry?;
            match extract_operation_type(&internal_key) {
                OperationType::Delete => continue,
                OperationType::Put => {}
                operation_type => unreachable!(
                    "unexpected operation type in source collection scan: {:?}",
                    operation_type
                ),
            }
            let document = RawDocument::from_bytes(&value)
                .map_err(|error| Error::new(ErrorKind::InvalidData, error.to_string()))?;
            let OperationsCountAndSize { count, size_bytes } = indexes.append_put_ops_raw(
                &mut batch.operations,
                document,
                &mut batch.count_stats,
            )?;
            let user_key = extract_user_key(&internal_key);
            state.last_processed_primary_key = Some(user_key.to_vec());
            batch.payload_bytes += size_bytes;
            state.processed_document_count += 1;
            state.indexed_entry_count += count as u64;
            batch.document_count += 1;
        }

        if batch.document_count == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    fn commit_batch(
        &self,
        key: IndexBuildKey,
        state: &IndexBuildState,
        mut batch: Batch,
    ) -> Result<BatchCommitStatus> {
        batch.operations.push(new_put_state_operation(key, state));
        let mut preconditions = Preconditions::new(self.storage_engine.acquire_snapshot());
        preconditions.add_index_not_dropped(key.collection_id, key.index_id);
        match self.storage_engine.write(
            WriteBatch::new_with_preconditions(
                batch.operations,
                batch.count_stats.build(),
                preconditions,
            ),
            true,
        ) {
            Ok(()) => Ok(BatchCommitStatus::Committed),
            Err(StorageError::CollectionNotFound { .. } | StorageError::IndexNotFound { .. }) => {
                Ok(BatchCommitStatus::Cancelled)
            }
            Err(error) => Err(Error::new(
                error.as_io_error().map_or(ErrorKind::Other, Error::kind),
                error.to_string(),
            )),
        }
    }

    fn delete_state(&self, key: IndexBuildKey) -> Result<()> {
        self.delete_states(std::iter::once(key))
    }

    fn delete_states(&self, keys: impl IntoIterator<Item = IndexBuildKey>) -> Result<()> {
        let operations = keys
            .into_iter()
            .map(|key| {
                Operation::new_delete(
                    crate::storage::INTERNAL_INDEX_BUILD_COLLECTION_ID,
                    0,
                    key.encode(),
                )
            })
            .collect::<Vec<_>>();
        if operations.is_empty() {
            return Ok(());
        }
        // Sync checkpoint cleanup before reporting success. Index publication
        // is retained on recovery even if this write fails, in which case startup
        // cleanup removes the stale checkpoint.
        self.storage_engine
            .write(WriteBatch::new(operations, CountStats::default()), true)
            .map_err(|error| {
                Error::new(
                    error.as_io_error().map_or(ErrorKind::Other, Error::kind),
                    error.to_string(),
                )
            })
    }

    fn pending_index_for_build(&self, key: IndexBuildKey) -> Option<Indexes> {
        let catalog = self.storage_engine.catalog();
        let collection = catalog.get_collection_by_id(&key.collection_id)?;
        if collection.is_dropped() {
            tracing::debug!(
                collection_id = key.collection_id,
                index_id = key.index_id,
                "index build stopped because its collection was dropped"
            );
            return None;
        }
        let index_metadata = collection.get_index_by_id(key.index_id)?;
        if index_metadata.is_dropped() {
            tracing::debug!(
                collection_id = key.collection_id,
                index_id = key.index_id,
                "index build stopped because its index was dropped"
            );
            return None;
        }
        if index_metadata.is_queryable() {
            return None;
        }
        Some(Indexes::for_index(key.collection_id, &index_metadata))
    }
}

enum BackfillOutcome {
    Completed,
    Cancelled,
}

struct Metrics {
    active: Arc<AtomicGauge>,
    succeeded: Arc<Counter>,
    failed: Arc<Counter>,
    cancelled: Arc<Counter>,
    resumed: Arc<Counter>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            active: AtomicGauge::new(),
            succeeded: Counter::new(),
            failed: Counter::new(),
            cancelled: Counter::new(),
            resumed: Counter::new(),
        }
    }

    fn register_to(&self, registry: &mut MetricRegistry) {
        registry
            .register_gauge(index_build::ACTIVE, self.active.clone())
            .register_counter(index_build::SUCCEEDED, self.succeeded.clone())
            .register_counter(index_build::FAILED, self.failed.clone())
            .register_counter(index_build::CANCELLED, self.cancelled.clone())
            .register_counter(index_build::RESUMED, self.resumed.clone());
    }

    fn start_build(&self) -> ActiveBuild<'_> {
        self.active.inc();
        ActiveBuild(&self.active)
    }
}

struct ActiveBuild<'a>(&'a AtomicGauge);

impl Drop for ActiveBuild<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

fn new_put_state_operation(key: IndexBuildKey, state: &IndexBuildState) -> Operation {
    Operation::new_put(
        crate::storage::INTERNAL_INDEX_BUILD_COLLECTION_ID,
        0,
        key.encode(),
        state.encode(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::{Options, WalDurability};
    use crate::options::storage_quantity::StorageQuantity;
    use crate::storage::catalog::{
        CollectionMetadata, IndexDefinition, IndexOptions, OrderedIndexField,
    };
    use crate::storage::count_stats::{CountStats, CountStatsKey};
    use crate::storage::index_build_state::IndexBuildKey;
    use crate::storage::operation::Operation;
    use crate::storage::storage_engine::{CreatedIndex, StorageEngine};
    use crate::storage::write_batch::WriteBatch;
    use crate::util::bson_utils::BsonKey;
    use bson::{doc, Bson};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    fn insert_document(storage_engine: &Arc<StorageEngine>, collection: u32, id: i32, name: &str) {
        let operation = Operation::new_put(
            collection,
            0,
            Bson::Int32(id).try_into_key().unwrap(),
            doc! { "_id": id, "name": name }.to_vec().unwrap(),
        );
        storage_engine
            .write(
                WriteBatch::new_for_test(vec![operation], CountStats::default()),
                false,
            )
            .unwrap();
    }

    fn delete_document(storage_engine: &Arc<StorageEngine>, collection: u32, id: i32) {
        let operation =
            Operation::new_delete(collection, 0, Bson::Int32(id).try_into_key().unwrap());
        storage_engine
            .write(
                WriteBatch::new_for_test(vec![operation], CountStats::default()),
                false,
            )
            .unwrap();
    }

    fn test_options() -> Arc<Options> {
        let options = Arc::new(
            Options::lightweight().with_index_build_batch_size(StorageQuantity::from_bytes(1)),
        );
        options
    }

    fn index_build_test_storage(
        metric_registry: &mut MetricRegistry,
        options: &Arc<Options>,
    ) -> (tempfile::TempDir, Arc<StorageEngine>, u32) {
        let directory = tempdir().unwrap();
        let (storage_engine, _pending_builds) = StorageEngine::new(
            metric_registry,
            options.clone(),
            directory.path(),
        )
        .unwrap();
        let collection_id = storage_engine
            .create_collection("index_build", true)
            .unwrap();
        (directory, storage_engine, collection_id)
    }

    fn reopen_index_build_test_storage(
        directory: &tempfile::TempDir,
    ) -> (Arc<StorageEngine>, Vec<IndexBuildSnapshot>) {
        StorageEngine::new(
            &mut MetricRegistry::default(),
            Arc::new(Options::lightweight()),
            directory.path(),
        )
        .unwrap()
    }

    fn count_records(storage_engine: &StorageEngine, collection: u32, index: u32) -> usize {
        let snapshot = storage_engine.acquire_snapshot();
        storage_engine
            .range_scan_at_snapshot(collection, index, &(..), &snapshot, Direction::Forward)
            .unwrap()
            .count()
    }

    fn assert_record_count(
        storage_engine: &StorageEngine,
        collection: u32,
        index: u32,
        expected_count: usize,
    ) {
        let actual_count = count_records(storage_engine, collection, index);
        assert_eq!(
            actual_count, expected_count,
            "expected {} records in collection {} index {}, but found {}",
            expected_count, collection, index, actual_count
        );
    }

    fn get_collection(
        storage_engine: &StorageEngine,
        collection_id: u32,
    ) -> Arc<CollectionMetadata> {
        storage_engine
            .catalog()
            .get_collection_by_id(&collection_id)
            .unwrap()
    }

    fn assert_index_queryable(storage_engine: &StorageEngine, collection_id: u32, index_id: u32) {
        assert!(get_collection(storage_engine, collection_id).is_index_queryable(index_id));
    }

    fn assert_checkpoint_type(
        storage_engine: &StorageEngine,
        key: IndexBuildKey,
        op_type: OperationType,
    ) {
        assert_eq!(
            extract_operation_type(&read_checkpoint(storage_engine, &key).0),
            op_type
        );
    }

    fn assert_count_stat(
        storage_engine: &StorageEngine,
        collection: u32,
        index: u32,
        expected_count: i64,
    ) {
        let key = CountStatsKey::Index { collection, index };
        let actual_count = storage_engine.count_stat(&key).unwrap_or(0);
        assert_eq!(
            actual_count, expected_count,
            "expected count stat {:?} to be {}, but found {}",
            key, expected_count, actual_count
        );
    }

    fn read_checkpoint(storage_engine: &StorageEngine, key: &IndexBuildKey) -> (Vec<u8>, Vec<u8>) {
        let snapshot = storage_engine.acquire_snapshot();
        storage_engine
            .read_at_snapshot(
                crate::storage::INTERNAL_INDEX_BUILD_COLLECTION_ID,
                0,
                &key.encode(),
                &snapshot,
            )
            .unwrap()
            .unwrap()
    }

    fn create_index(storage_engine: &Arc<StorageEngine>, collection_id: u32) -> CreatedIndex {
        storage_engine
            .create_index(
                collection_id,
                IndexDefinition::Regular(vec![OrderedIndexField::asc("name")]),
                IndexOptions::default(),
            )
            .unwrap()
    }

    #[test]
    fn index_build_writes_entries_and_checkpoint() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        insert_document(&storage_engine, collection_id, 1, "one");
        insert_document(&storage_engine, collection_id, 2, "two");
        insert_document(&storage_engine, collection_id, 3, "three");
        delete_document(&storage_engine, collection_id, 2);

        let created_index = create_index(&storage_engine, collection_id);
        let build_snapshot = created_index.build_snapshot.unwrap();
        let build_key = IndexBuildKey::new(collection_id, created_index.id);
        let builder = IndexBuilder::new(&mut registry, &options, storage_engine.clone());
        builder.build_index(build_key, &build_snapshot).unwrap();

        assert_eq!(registry.gauge_value(index_build::ACTIVE), 0);
        assert_eq!(registry.counter_value(index_build::SUCCEEDED), 1);

        assert_index_queryable(&storage_engine, collection_id, created_index.id);
        assert_record_count(&storage_engine, collection_id, created_index.id, 2);
        assert_checkpoint_type(&storage_engine, build_key, OperationType::Delete);
    }

    #[test]
    fn index_build_on_empty_collection_publishes_index_and_deletes_checkpoint() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        let created_index = create_index(&storage_engine, collection_id);
        let key = IndexBuildKey::new(collection_id, created_index.id);

        IndexBuilder::new(
            &mut registry,
            &options,
            storage_engine.clone(),
        )
        .build_index(key, &created_index.build_snapshot.unwrap())
        .unwrap();

        assert_index_queryable(&storage_engine, collection_id, key.index_id);
        assert_record_count(&storage_engine, collection_id, key.index_id, 0);
        assert_checkpoint_type(&storage_engine, key, OperationType::Delete);
    }

    #[test]
    fn empty_index_build_is_durable_with_buffered_wal() {
        assert_empty_index_build_is_durable(WalDurability::Buffered);
    }

    #[test]
    fn empty_index_build_is_durable_with_process_safe_wal() {
        assert_empty_index_build_is_durable(WalDurability::ProcessSafe);
    }

    fn assert_empty_index_build_is_durable(wal_durability: WalDurability) {
        let directory = tempdir().unwrap();
        let options = Arc::new(Options::lightweight().with_wal_durability(wal_durability));
        let mut metrics = MetricRegistry::default();
        let (storage_engine, _) =
            StorageEngine::new(&mut metrics, options.clone(), directory.path()).unwrap();
        let collection_id = storage_engine.create_collection("empty", true).unwrap();
        let created_index = create_index(&storage_engine, collection_id);
        let key = IndexBuildKey::new(collection_id, created_index.id);
        let syncs_before_build = metrics.counter_value(metrics::names::wal::SYNCS);

        IndexBuilder::new(&mut metrics, &options, storage_engine.clone())
            .build_index(key, &created_index.build_snapshot.unwrap())
            .unwrap();

        // There are no backfill writes: the checkpoint deletion must sync the
        // WAL sequence that anchors creation and publication in the catalog.
        assert_eq!(
            metrics.counter_value(metrics::names::wal::SYNCS),
            syncs_before_build + 1
        );
        assert_index_queryable(&storage_engine, collection_id, key.index_id);
        // Reopen without shutdown or an explicit flush supplying durability.
        drop(storage_engine);

        let (restarted, pending_builds) = reopen_index_build_test_storage(&directory);
        assert!(pending_builds.is_empty());
        assert_index_queryable(&restarted, collection_id, key.index_id);
        assert_record_count(&restarted, collection_id, key.index_id, 0);
        assert_checkpoint_type(&restarted, key, OperationType::Delete);
        restarted.shutdown().unwrap();
    }

    #[test]
    fn cleanup_stale_index_build_states_preserves_pending_and_deletes_stale_states() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        let pending_index = create_index(&storage_engine, collection_id);
        let stale_index = storage_engine
            .create_index(
                collection_id,
                IndexDefinition::Regular(vec![OrderedIndexField::asc("other")]),
                IndexOptions::default(),
            )
            .unwrap();
        let pending_key = IndexBuildKey::new(collection_id, pending_index.id);
        let stale_key = IndexBuildKey::new(collection_id, stale_index.id);

        storage_engine
            .write(
                WriteBatch::new_for_test(
                    vec![
                        new_put_state_operation(pending_key, &IndexBuildState::new()),
                        new_put_state_operation(stale_key, &IndexBuildState::new()),
                    ],
                    CountStats::default(),
                ),
                false,
            )
            .unwrap();

        let pending_builds = vec![IndexBuildSnapshot {
            key: pending_key,
            snapshot: pending_index.build_snapshot.unwrap(),
        }];
        IndexBuilder::new(
            &mut registry,
            &options,
            storage_engine.clone(),
        )
        .cleanup_stale_index_build_states(&pending_builds)
        .unwrap();

        assert_checkpoint_type(&storage_engine, pending_key, OperationType::Put);
        assert_checkpoint_type(&storage_engine, stale_key, OperationType::Delete);
    }

    #[test]
    fn interrupted_index_build_resumes_from_checkpoint_after_storage_restart() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        for id in 1..=3 {
            insert_document(&storage_engine, collection_id, id, "same");
        }
        let created_index = create_index(&storage_engine, collection_id);
        let index_id = created_index.id;
        let key = IndexBuildKey::new(collection_id, index_id);
        let build_snapshot = created_index.build_snapshot.unwrap();
        storage_engine.wal_fail_write_after(1);
        let builder = IndexBuilder::new(
            &mut registry,
            &options,
            storage_engine.clone(),
        );
        let result = builder.build_index(key, &build_snapshot);
        assert!(result.is_err());

        let (checkpoint_key, checkpoint_value) = read_checkpoint(&storage_engine, &key);
        assert_eq!(extract_operation_type(&checkpoint_key), OperationType::Put);
        let checkpoint = IndexBuildState::decode(&checkpoint_value).unwrap();
        assert_eq!(checkpoint.processed_document_count, 1);
        assert!(checkpoint.last_processed_primary_key.is_some());

        drop(build_snapshot);
        drop(builder);
        storage_engine.shutdown().unwrap();
        drop(storage_engine);

        let (restarted, pending_builds) = reopen_index_build_test_storage(&directory);
        assert_eq!(pending_builds.len(), 1);
        let pending = pending_builds.into_iter().next().unwrap();
        assert_eq!(pending.key, key);
        let mut registry = MetricRegistry::default();
        IndexBuilder::new(&mut registry, &options, restarted.clone())
            .build_index(pending.key, &pending.snapshot)
            .unwrap();
        assert_eq!(registry.counter_value(index_build::RESUMED), 1);

        assert_index_queryable(&restarted, collection_id, index_id);
        assert_record_count(&restarted, collection_id, index_id, 3);
        assert_count_stat(&restarted, collection_id, index_id, 3);
        assert_checkpoint_type(&restarted, key, OperationType::Delete);
        restarted.shutdown().unwrap();
    }

    #[test]
    fn restart_after_checkpoint_cleanup_failure_retains_published_index() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        for id in 1..=3 {
            insert_document(&storage_engine, collection_id, id, "same");
        }
        let created_index = create_index(&storage_engine, collection_id);
        let index_id = created_index.id;
        let key = IndexBuildKey::new(collection_id, index_id);
        let build_snapshot = created_index.build_snapshot.unwrap();
        // We want the writes to succeed, but not the deletion of the checkpoint, so we fail after 3 writes (the number of documents).
        storage_engine.wal_fail_write_after(3);
        let builder = IndexBuilder::new(
            &mut registry,
            &options,
            storage_engine.clone(),
        );
        let result = builder.build_index(key, &build_snapshot);
        assert!(result.is_err());
        assert_record_count(&storage_engine, collection_id, index_id, 3);
        let (checkpoint_key, checkpoint_value) = read_checkpoint(&storage_engine, &key);
        assert_eq!(extract_operation_type(&checkpoint_key), OperationType::Put);
        let checkpoint = IndexBuildState::decode(&checkpoint_value).unwrap();
        assert_eq!(checkpoint.processed_document_count, 3);
        assert!(checkpoint.last_processed_primary_key.is_some());

        drop(build_snapshot);
        drop(builder);
        storage_engine.shutdown().unwrap();
        drop(storage_engine);

        let (restarted, pending_builds) = reopen_index_build_test_storage(&directory);
        assert!(pending_builds.is_empty());
        assert_index_queryable(&restarted, collection_id, index_id);
        IndexBuilder::new(&mut MetricRegistry::default(), &options, restarted.clone())
            .cleanup_stale_index_build_states(&pending_builds)
            .unwrap();

        assert_index_queryable(&restarted, collection_id, index_id);
        assert_record_count(&restarted, collection_id, index_id, 3);
        assert_count_stat(&restarted, collection_id, index_id, 3);
        assert_checkpoint_type(&restarted, key, OperationType::Delete);
        restarted.shutdown().unwrap();
    }

    #[test]
    fn failed_backfill_drops_the_index_and_returns_the_backfill_error() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        let operation = Operation::new_put(collection_id, 0, vec![1], vec![0xff]);
        storage_engine
            .write(
                WriteBatch::new_for_test(vec![operation], CountStats::default()),
                false,
            )
            .unwrap();
        let created_index = create_index(&storage_engine, collection_id);
        let key = IndexBuildKey::new(collection_id, created_index.id);
        storage_engine
            .write(
                WriteBatch::new_for_test(
                    vec![new_put_state_operation(key, &IndexBuildState::new())],
                    CountStats::default(),
                ),
                false,
            )
            .unwrap();
        let error = IndexBuilder::new(&mut registry, &options, storage_engine.clone())
            .build_index(key, &created_index.build_snapshot.unwrap())
            .unwrap_err();

        assert_eq!(registry.gauge_value(index_build::ACTIVE), 0);
        assert_eq!(registry.counter_value(index_build::FAILED), 1);
        assert!(matches!(error, crate::error::Error::Io(_)));
        assert!(get_collection(&storage_engine, collection_id).is_index_dropped(created_index.id));
        assert_checkpoint_type(&storage_engine, key, OperationType::Delete);
    }

    #[test]
    fn failed_index_drop_error_takes_precedence_over_backfill_error() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, _collection_id) = index_build_test_storage(&mut registry, &options);
        let key = IndexBuildKey::new(crate::storage::FIRST_USER_COLLECTION_ID, 1);
        let build_error = Error::new(ErrorKind::InvalidData, "backfill failed");
        let drop_error = StorageError::ErrorMode("storage failed".to_string());

        let error = IndexBuilder::new(
            &mut registry,
            &options,
            storage_engine.clone(),
        )
        .handle_build_failure(key, build_error, Err(drop_error))
        .unwrap_err();

        assert!(
            matches!(error, crate::error::Error::ErrorMode(message) if message == "storage failed")
        );
    }

    #[test]
    fn failed_checkpoint_cleanup_error_takes_precedence_over_backfill_error() {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        let created_index = create_index(&storage_engine, collection_id);
        let key = IndexBuildKey::new(collection_id, created_index.id);
        storage_engine
            .write(
                WriteBatch::new_for_test(
                    vec![new_put_state_operation(key, &IndexBuildState::new())],
                    CountStats::default(),
                ),
                false,
            )
            .unwrap();
        storage_engine
            .drop_index(collection_id, created_index.id)
            .unwrap();
        storage_engine.wal_fail_write_after(0);

        let error = IndexBuilder::new(
            &mut registry,
            &options,
            storage_engine.clone(),
        )
        .handle_build_failure(
            key,
            Error::new(ErrorKind::InvalidData, "backfill failed"),
            Ok(()),
        )
        .unwrap_err();

        assert!(error.to_string().contains("Injected error on append"));
        assert_checkpoint_type(&storage_engine, key, OperationType::Put);
    }

    fn assert_batch_is_cancelled_by_drop(drop_collection: bool) {
        let options = test_options();
        let mut registry = MetricRegistry::default();
        let (_directory, storage_engine, collection_id) = index_build_test_storage(&mut registry, &options);
        for id in 1..=3 {
            insert_document(&storage_engine, collection_id, id, "one");
        }
        let created_index = create_index(&storage_engine, collection_id);
        let key = IndexBuildKey::new(collection_id, created_index.id);
        let snapshot = created_index.build_snapshot.unwrap();
        let hook_storage = storage_engine.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let hook_dropped = dropped.clone();
        let mut registry = MetricRegistry::default();
        let builder = IndexBuilder::new(&mut registry, &options, storage_engine.clone())
            .with_test_hook(move || {
                if drop_collection {
                    hook_storage.drop_collection("index_build").unwrap();
                } else {
                    hook_storage
                        .drop_index(collection_id, created_index.id)
                        .unwrap();
                }
                hook_dropped.store(true, Ordering::Relaxed);
            });
        builder.build_index(key, &snapshot).unwrap();

        assert_eq!(registry.gauge_value(index_build::ACTIVE), 0);
        assert_eq!(registry.counter_value(index_build::CANCELLED), 1);
        assert_eq!(registry.counter_value(index_build::SUCCEEDED), 0);
        assert!(dropped.load(Ordering::Relaxed));
        assert_checkpoint_type(&storage_engine, key, OperationType::Delete);
        if !drop_collection {
            assert!(
                get_collection(&storage_engine, collection_id).is_index_dropped(created_index.id)
            );
        }
    }

    #[test]
    fn collection_drop_cancels_an_index_build_batch() {
        assert_batch_is_cancelled_by_drop(true);
    }

    #[test]
    fn index_drop_cancels_an_index_build_batch() {
        assert_batch_is_cancelled_by_drop(false);
    }
}
