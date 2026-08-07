use super::read::ReadExecutor;
use super::QueryOutput;
#[cfg(test)]
use super::{ExecutorFailpoint, ExecutorTestHook};
use crate::error::Error;
use crate::error::Result;
use crate::query::execution::indexes::Indexes;
use crate::query::execution::updates;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::update::UpdateExpr;
use crate::query::Parameters;
use crate::storage::catalog::IdCreationStrategy;
use crate::storage::count_stats::CountStatsBuilder;
use crate::storage::internal_key::extract_operation_type;
use crate::storage::operation::Operation;
use crate::storage::operation::OperationType;
use crate::storage::storage_engine::StorageEngine;
use crate::storage::storage_engine::StorageError;
use crate::storage::write_batch::{Precondition, Preconditions, WriteBatch};
use crate::util::bson_utils;
use crate::util::bson_utils::BsonKey;
use bson::{doc, serialize_to_vec, Bson, Document, RawDocument};
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_BASE_DELAY: Duration = Duration::from_millis(5);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

pub(crate) struct WriteExecutor {
    pub(super) storage_engine: Arc<StorageEngine>,
    pub(super) read_executor: ReadExecutor,
    pub(super) id_generator: Arc<Mutex<Sonyflake>>,
    #[cfg(test)]
    pub(super) test_hook: Option<Arc<dyn ExecutorTestHook>>,
}

impl WriteExecutor {
    pub(crate) fn new(
        storage_engine: Arc<StorageEngine>,
        read_executor: ReadExecutor,
        id_generator: Arc<Mutex<Sonyflake>>,
        #[cfg(test)] test_hook: Option<Arc<dyn ExecutorTestHook>>,
    ) -> Self {
        Self {
            storage_engine,
            read_executor,
            id_generator,
            #[cfg(test)]
            test_hook,
        }
    }

    pub(crate) fn execute_direct(
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
            _ => unreachable!("Direct execution not supported for plan: {:?}", plan),
        }
    }

    pub(super) fn duplicate_key_error(id: &Bson) -> Error {
        Error::InvalidRequest(format!("Duplicate key error. dup key: {{ _id: {} }}", id))
    }

    pub(super) fn primary_key_exists(
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

    pub(super) fn get_id_creation_strategy(&self, collection: u32) -> IdCreationStrategy {
        self.storage_engine
            .catalog()
            .get_collection_by_id(&collection)
            .map(|meta| meta.options.id_creation_strategy.clone())
            .unwrap_or_default()
    }

    pub(super) fn ensure_id(
        &self,
        doc: &mut Vec<u8>,
        strategy: &IdCreationStrategy,
    ) -> Result<Bson> {
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

    pub(super) fn generate_id(&self) -> Bson {
        super::generate_bson_id(self.id_generator.as_ref())
    }

    pub(super) fn indices(&self, collection: u32) -> Indexes {
        let metadata = self
            .storage_engine
            .catalog()
            .get_collection_by_id(&collection)
            .unwrap();
        Indexes::from_collection(&metadata)
    }

    pub(super) fn perform_update_many(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        let snapshot = self.storage_engine.last_visible_sequence();
        let mut iter = self.read_executor.execute_cached_at_snapshot(
            query.clone(),
            parameters,
            Some(snapshot),
        )?;

        let mut operations = Vec::new();
        let mut preconditions = Vec::new();
        let mut count_stats = CountStatsBuilder::new();
        let mut matched_count = 0;
        let mut modified_count = 0;
        let mut upserted_id: Option<Bson> = None;

        let mut next = iter.next();

        if next.is_some() {
            let updater = updates::to_updater(update, false)?;

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

    pub(super) fn perform_update_one(
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
            let mut iter = self.read_executor.execute_cached_at_snapshot(
                query.clone(),
                parameters,
                Some(snapshot),
            )?;

            let result_doc = if let Some(doc_result) = iter.next() {
                let old_doc = doc_result?;
                let updater = updates::to_updater(update, false)?;
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

    pub(super) fn perform_delete_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<QueryOutput> {
        let start_time = Instant::now();
        let mut attempt = 0;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter = self.read_executor.execute_cached_at_snapshot(
                query.clone(),
                parameters,
                Some(snapshot),
            )?;

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

    pub(super) fn perform_insert_one(
        &self,
        collection: u32,
        document: Vec<u8>,
    ) -> Result<QueryOutput> {
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

        let batch = if id_strategy == IdCreationStrategy::Generated {
            WriteBatch::new(operations, count_stats.build())
        } else {
            let snapshot = self.storage_engine.last_visible_sequence();

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

    pub(super) fn perform_insert_many(
        &self,
        collection: u32,
        documents: Vec<Vec<u8>>,
    ) -> Result<QueryOutput> {
        if documents.is_empty() {
            return Ok(Box::new(std::iter::once(Ok(
                doc! { "inserted_ids": Bson::Array(vec![]) },
            ))));
        }

        let id_strategy = self.get_id_creation_strategy(collection);

        let mut documents_with_ids: Vec<(Vec<u8>, Bson, Vec<u8>)> =
            Vec::with_capacity(documents.len());

        for mut doc in documents {
            let id = self.ensure_id(&mut doc, &id_strategy)?;
            let user_key = id.try_into_key()?;
            documents_with_ids.push((doc, id, user_key));
        }

        let indices = self.indices(collection);

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
                    let id = seen_keys.as_ref().unwrap().get(&conflicting_key).unwrap();
                    Err(Self::duplicate_key_error(id))
                }
                _ => Err(e.into()),
            }
        } else {
            let result =
                doc! { "inserted_ids": ids.into_iter().map(Bson::from).collect::<Vec<_>>() };
            Ok(Box::new(std::iter::once(Ok(result))))
        }
    }

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

    #[cfg(test)]
    fn invoke_test_hook(&self, point: ExecutorFailpoint) {
        if let Some(test_hook) = &self.test_hook {
            test_hook.hit(point);
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
            if start_time.elapsed() >= RETRY_TIMEOUT {
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
    let mut backoff = RETRY_BASE_DELAY.as_millis() as u64 * 2_u64.pow(attempt);
    if backoff > RETRY_MAX_DELAY.as_millis() as u64 {
        backoff = RETRY_MAX_DELAY.as_millis() as u64;
    }

    Duration::from_millis(backoff)
}
