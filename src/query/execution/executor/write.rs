use super::read::ReadExecutor;
use super::Metrics;
use super::WriteResult;
#[cfg(test)]
use super::{invoke_executor_test_hook, ExecutorFailpoint};
use crate::error::Error;
use crate::error::Result;
use crate::query::execution::indexes::Indexes;
use crate::query::execution::projections;
use crate::query::execution::updates;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::update::UpdateExpr;
use crate::query::{Parameters, Projection, ReturnDocument};
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
use bson::{serialize_to_vec, Bson, Document, RawDocument};
use sonyflake::Sonyflake;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::trace_span;

const RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_BASE_DELAY: Duration = Duration::from_millis(5);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

pub(crate) struct WriteExecutor {
    pub(super) storage_engine: Arc<StorageEngine>,
    pub(super) read_executor: ReadExecutor,
    pub(super) id_generator: Arc<Mutex<Sonyflake>>,
    metrics: Metrics,
}

impl WriteExecutor {
    pub(crate) fn new(
        storage_engine: Arc<StorageEngine>,
        read_executor: ReadExecutor,
        id_generator: Arc<Mutex<Sonyflake>>,
        metrics: Metrics,
    ) -> Self {
        Self {
            storage_engine,
            read_executor,
            id_generator,
            metrics,
        }
    }

    pub(crate) fn execute_direct(
        &self,
        plan: PhysicalPlan,
        parameters: Option<Parameters>,
    ) -> Result<WriteResult> {
        let result = match plan {
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
            PhysicalPlan::FindOneAndUpdate {
                collection,
                query,
                update,
                projection,
                upsert,
                return_document,
            } => {
                let parameters =
                    parameters.expect("Parameters must be provided for FindOneAndUpdate");
                self.perform_find_one_and_update(
                    collection,
                    query,
                    &update,
                    projection,
                    upsert,
                    return_document,
                    &parameters,
                )
            }
            PhysicalPlan::ReplaceOne {
                collection,
                query,
                replacement,
                upsert,
            } => {
                let parameters = parameters.expect("Parameters must be provided for ReplaceOne");
                self.perform_replace_one(collection, query, replacement, upsert, &parameters)
            }
            PhysicalPlan::FindOneAndReplace {
                collection,
                query,
                replacement,
                projection,
                upsert,
                return_document,
            } => {
                let parameters =
                    parameters.expect("Parameters must be provided for FindOneAndReplace");
                self.perform_find_one_and_replace(
                    collection,
                    query,
                    replacement,
                    projection,
                    upsert,
                    return_document,
                    &parameters,
                )
            }
            PhysicalPlan::FindOneAndDelete {
                collection,
                query,
                projection,
            } => {
                let parameters =
                    parameters.expect("Parameters must be provided for FindOneAndDelete");
                self.perform_find_one_and_delete(collection, query, projection, &parameters)
            }
            PhysicalPlan::DeleteOne { collection, query } => {
                let parameters = parameters.expect("Parameters must be provided for DeleteOne");
                self.perform_delete_one(collection, query, &parameters)
            }
            PhysicalPlan::DeleteMany { collection, query } => {
                let parameters = parameters.expect("Parameters must be provided for DeleteMany");
                self.perform_delete_many(collection, query, &parameters)
            }
            _ => unreachable!("Direct execution not supported for plan: {:?}", plan),
        };

        if let Ok(write_result) = &result {
            self.metrics
                .documents_written
                .inc_by(count_written_documents(write_result));
        }

        result
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
    ) -> Result<WriteResult> {
        let snapshot = self.storage_engine.last_visible_sequence();
        let _span = trace_span!("update_many", collection, upsert, snapshot,).entered();
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
            return Ok(WriteResult::Update {
                matched_count,
                modified_count: 0,
                upserted_id: None,
            });
        }

        let batch = WriteBatch::new_with_preconditions(
            operations,
            Preconditions::new(snapshot, preconditions),
            count_stats.build(),
        );
        #[cfg(test)]
        self.invoke_test_hook(ExecutorFailpoint::UpdateManyBeforeCommit);
        self.storage_engine.write(batch)?;

        Ok(WriteResult::Update {
            matched_count,
            modified_count,
            upserted_id,
        })
    }

    pub(super) fn perform_update_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let _span = trace_span!("update_one", collection, upsert).entered();
        match self.perform_single_document_update(collection, query, update, upsert, parameters)? {
            SingleDocumentUpdateResult::Updated { .. } => Ok(WriteResult::Update {
                matched_count: 1,
                modified_count: 1,
                upserted_id: None,
            }),
            SingleDocumentUpdateResult::Upserted { upserted_id, .. } => Ok(WriteResult::Update {
                matched_count: 0,
                modified_count: 0,
                upserted_id: Some(upserted_id),
            }),
            SingleDocumentUpdateResult::NoMatch => Ok(WriteResult::Update {
                matched_count: 0,
                modified_count: 0,
                upserted_id: None,
            }),
        }
    }

    pub(super) fn perform_find_one_and_update(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        projection: Option<Arc<Projection>>,
        upsert: bool,
        return_document: ReturnDocument,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let _span = trace_span!(
            "find_one_and_update",
            collection,
            upsert,
            return_document = ?return_document
        )
        .entered();
        match self.perform_single_document_update(collection, query, update, upsert, parameters)? {
            SingleDocumentUpdateResult::Updated { old_doc, new_doc } => {
                let returned = match return_document {
                    ReturnDocument::Before => old_doc,
                    ReturnDocument::After => new_doc,
                };
                Ok(WriteResult::SingleDocument {
                    affected_count: 1,
                    document: Some(self.apply_return_projection(
                        returned,
                        projection.as_ref(),
                        parameters,
                    )?),
                })
            }
            SingleDocumentUpdateResult::Upserted { new_doc, .. } => {
                let document = match return_document {
                    ReturnDocument::Before => None,
                    ReturnDocument::After => Some(self.apply_return_projection(
                        new_doc,
                        projection.as_ref(),
                        parameters,
                    )?),
                };
                Ok(WriteResult::SingleDocument {
                    affected_count: 1,
                    document,
                })
            }
            SingleDocumentUpdateResult::NoMatch => Ok(WriteResult::SingleDocument {
                affected_count: 0,
                document: None,
            }),
        }
    }

    pub(super) fn perform_replace_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        replacement: Document,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let _span = trace_span!("replace_one", collection, upsert).entered();
        match self.perform_single_document_replace(
            collection,
            query,
            replacement,
            upsert,
            parameters,
        )? {
            SingleDocumentReplaceResult::Replaced { .. } => Ok(WriteResult::Update {
                matched_count: 1,
                modified_count: 1,
                upserted_id: None,
            }),
            SingleDocumentReplaceResult::Upserted { upserted_id, .. } => Ok(WriteResult::Update {
                matched_count: 0,
                modified_count: 0,
                upserted_id: Some(upserted_id),
            }),
            SingleDocumentReplaceResult::NoMatch => Ok(WriteResult::Update {
                matched_count: 0,
                modified_count: 0,
                upserted_id: None,
            }),
        }
    }

    pub(super) fn perform_find_one_and_replace(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        replacement: Document,
        projection: Option<Arc<Projection>>,
        upsert: bool,
        return_document: ReturnDocument,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let _span = trace_span!(
            "find_one_and_replace",
            collection,
            upsert,
            return_document = ?return_document
        )
        .entered();
        match self.perform_single_document_replace(
            collection,
            query,
            replacement,
            upsert,
            parameters,
        )? {
            SingleDocumentReplaceResult::Replaced { old_doc, new_doc } => {
                let returned = match return_document {
                    ReturnDocument::Before => old_doc,
                    ReturnDocument::After => new_doc,
                };
                Ok(WriteResult::SingleDocument {
                    affected_count: 1,
                    document: Some(self.apply_return_projection(
                        returned,
                        projection.as_ref(),
                        parameters,
                    )?),
                })
            }
            SingleDocumentReplaceResult::Upserted { new_doc, .. } => {
                let document = match return_document {
                    ReturnDocument::Before => None,
                    ReturnDocument::After => Some(self.apply_return_projection(
                        new_doc,
                        projection.as_ref(),
                        parameters,
                    )?),
                };
                Ok(WriteResult::SingleDocument {
                    affected_count: 1,
                    document,
                })
            }
            SingleDocumentReplaceResult::NoMatch => Ok(WriteResult::SingleDocument {
                affected_count: 0,
                document: None,
            }),
        }
    }

    pub(super) fn perform_delete_one(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let _span = trace_span!("delete_one", collection).entered();
        match self.perform_single_document_delete(collection, query, parameters)? {
            SingleDocumentDeleteResult::Deleted { .. } => {
                Ok(WriteResult::Delete { deleted_count: 1 })
            }
            SingleDocumentDeleteResult::NoMatch => Ok(WriteResult::Delete { deleted_count: 0 }),
        }
    }

    pub(super) fn perform_delete_many(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let snapshot = self.storage_engine.last_visible_sequence();
        let _span = trace_span!("delete_many", collection, snapshot,).entered();
        let mut iter =
            self.read_executor
                .execute_cached_at_snapshot(query, parameters, Some(snapshot))?;

        let mut operations = Vec::new();
        let mut preconditions = Vec::new();
        let mut count_stats = CountStatsBuilder::new();
        let mut deleted_count = 0;
        let indices = self.indices(collection);

        while let Some(doc_result) = iter.next() {
            let old_doc = doc_result?;
            let user_key = old_doc.get("_id").unwrap().try_into_key()?;

            indices.append_delete_ops(&mut operations, &old_doc, &mut count_stats)?;
            operations.push(Operation::new_delete(collection, 0, user_key.clone()));
            count_stats.inc_collection(collection, -1);
            preconditions.push(Precondition::VersionMatch {
                collection,
                index: 0,
                user_key,
            });
            deleted_count += 1;
        }

        if deleted_count == 0 {
            return Ok(WriteResult::Delete { deleted_count: 0 });
        }

        let batch = WriteBatch::new_with_preconditions(
            operations,
            Preconditions::new(snapshot, preconditions),
            count_stats.build(),
        );
        #[cfg(test)]
        self.invoke_test_hook(ExecutorFailpoint::DeleteManyBeforeCommit);
        self.storage_engine.write(batch)?;

        Ok(WriteResult::Delete { deleted_count })
    }

    pub(super) fn perform_find_one_and_delete(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        projection: Option<Arc<Projection>>,
        parameters: &Parameters,
    ) -> Result<WriteResult> {
        let _span = trace_span!("find_one_and_delete", collection).entered();
        match self.perform_single_document_delete(collection, query, parameters)? {
            SingleDocumentDeleteResult::Deleted { old_doc } => Ok(WriteResult::SingleDocument {
                affected_count: 1,
                document: Some(self.apply_return_projection(
                    old_doc,
                    projection.as_ref(),
                    parameters,
                )?),
            }),
            SingleDocumentDeleteResult::NoMatch => Ok(WriteResult::SingleDocument {
                affected_count: 0,
                document: None,
            }),
        }
    }

    fn apply_return_projection(
        &self,
        doc: Document,
        projection: Option<&Arc<Projection>>,
        parameters: &Parameters,
    ) -> Result<Document> {
        let Some(projection) = projection else {
            return Ok(doc);
        };
        let projector = projections::to_projector(projection, parameters)?;
        projector(doc)
    }

    fn perform_single_document_update(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        update: &UpdateExpr,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<SingleDocumentUpdateResult> {
        let start_time = Instant::now();
        let mut attempt = 0;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter = self.read_executor.execute_cached_at_snapshot(
                query.clone(),
                parameters,
                Some(snapshot),
            )?;

            if let Some(doc_result) = iter.next() {
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
                    Ok(_) => {
                        return Ok(SingleDocumentUpdateResult::Updated { old_doc, new_doc });
                    }
                    Err(e) => {
                        on_version_conflict(e, &start_time, &mut attempt)?;
                        continue;
                    }
                }
            }

            if !upsert {
                return Ok(SingleDocumentUpdateResult::NoMatch);
            }

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
                    return Ok(SingleDocumentUpdateResult::Upserted {
                        new_doc,
                        upserted_id,
                    });
                }
                Err(e) => {
                    on_version_conflict(e, &start_time, &mut attempt)?;
                }
            }
        }
    }

    fn perform_single_document_replace(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        replacement: Document,
        upsert: bool,
        parameters: &Parameters,
    ) -> Result<SingleDocumentReplaceResult> {
        let start_time = Instant::now();
        let mut attempt = 0;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter = self.read_executor.execute_cached_at_snapshot(
                query.clone(),
                parameters,
                Some(snapshot),
            )?;

            if let Some(doc_result) = iter.next() {
                let old_doc = doc_result?;
                let new_doc = self.prepare_replacement_document(&old_doc, &replacement)?;
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
                    Ok(_) => {
                        return Ok(SingleDocumentReplaceResult::Replaced { old_doc, new_doc });
                    }
                    Err(e) => {
                        on_version_conflict(e, &start_time, &mut attempt)?;
                        continue;
                    }
                }
            }

            if !upsert {
                return Ok(SingleDocumentReplaceResult::NoMatch);
            }

            let (new_doc, upserted_id) =
                self.perform_replacement_upsert(&query, &replacement, parameters)?;
            let user_key = upserted_id.clone().try_into_key()?;
            let new_doc_bytes = serialize_to_vec(&new_doc)?;

            match self.write_document(
                collection,
                snapshot,
                user_key,
                None,
                new_doc.clone(),
                new_doc_bytes,
            ) {
                Ok(_) => {
                    return Ok(SingleDocumentReplaceResult::Upserted {
                        new_doc,
                        upserted_id,
                    });
                }
                Err(e) => {
                    on_version_conflict(e, &start_time, &mut attempt)?;
                }
            }
        }
    }

    fn perform_single_document_delete(
        &self,
        collection: u32,
        query: Arc<PhysicalPlan>,
        parameters: &Parameters,
    ) -> Result<SingleDocumentDeleteResult> {
        let start_time = Instant::now();
        let mut attempt = 0;

        loop {
            let snapshot = self.storage_engine.last_visible_sequence();
            let mut iter = self.read_executor.execute_cached_at_snapshot(
                query.clone(),
                parameters,
                Some(snapshot),
            )?;

            if let Some(doc_result) = iter.next() {
                let old_doc = doc_result?;
                let user_key = old_doc.get("_id").unwrap().try_into_key()?;

                #[cfg(test)]
                self.invoke_test_hook(ExecutorFailpoint::DeleteOneAfterRead);
                match self.delete_document(collection, snapshot, user_key, &old_doc) {
                    Ok(_) => return Ok(SingleDocumentDeleteResult::Deleted { old_doc }),
                    Err(e) => {
                        on_version_conflict(e, &start_time, &mut attempt)?;
                        continue;
                    }
                }
            }

            return Ok(SingleDocumentDeleteResult::NoMatch);
        }
    }

    pub(super) fn perform_insert_one(
        &self,
        collection: u32,
        document: Vec<u8>,
    ) -> Result<WriteResult> {
        let _span = trace_span!("insert_one", collection).entered();
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

        Ok(WriteResult::InsertOne { inserted_id: id })
    }

    pub(super) fn perform_insert_many(
        &self,
        collection: u32,
        documents: Vec<Vec<u8>>,
    ) -> Result<WriteResult> {
        let _span = trace_span!("insert_many", collection).entered();
        if documents.is_empty() {
            return Ok(WriteResult::InsertMany {
                inserted_ids: Vec::new(),
            });
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
            Ok(WriteResult::InsertMany { inserted_ids: ids })
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

    fn prepare_replacement_document(
        &self,
        old_doc: &Document,
        replacement: &Document,
    ) -> Result<Document> {
        let old_id = old_doc.get("_id").unwrap().clone();

        match replacement.get("_id") {
            Some(new_id) if *new_id != old_id => Err(Error::InvalidRequest(
                "The _id field cannot be changed in a replacement document".to_string(),
            )),
            Some(_) => Ok(replacement.clone()),
            None => Ok(prepend_id_to_document(old_id, replacement)),
        }
    }

    #[cfg(test)]
    fn invoke_test_hook(&self, point: ExecutorFailpoint) {
        invoke_executor_test_hook(point);
    }
}

fn count_written_documents(result: &WriteResult) -> u64 {
    match result {
        WriteResult::InsertOne { .. } => 1,
        WriteResult::InsertMany { inserted_ids } => inserted_ids.len() as u64,
        WriteResult::Update {
            modified_count,
            upserted_id,
            ..
        } => modified_count + u64::from(upserted_id.is_some()),
        WriteResult::Delete { deleted_count } => *deleted_count,
        WriteResult::SingleDocument { affected_count, .. } => *affected_count,
    }
}

enum SingleDocumentUpdateResult {
    Updated {
        old_doc: Document,
        new_doc: Document,
    },
    Upserted {
        new_doc: Document,
        upserted_id: Bson,
    },
    NoMatch,
}

enum SingleDocumentReplaceResult {
    Replaced {
        old_doc: Document,
        new_doc: Document,
    },
    Upserted {
        new_doc: Document,
        upserted_id: Bson,
    },
    NoMatch,
}

enum SingleDocumentDeleteResult {
    Deleted { old_doc: Document },
    NoMatch,
}

pub(super) fn prepend_id_to_document(id: Bson, doc: &Document) -> Document {
    let mut new_doc = Document::new();
    new_doc.insert("_id", id);
    for (key, value) in doc {
        if key != "_id" {
            new_doc.insert(key.clone(), value.clone());
        }
    }
    new_doc
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
                tracing::debug!(
                    attempts = *attempt,
                    elapsed_ms = start_time.elapsed().as_millis(),
                    "write retry timeout reached after version conflicts"
                );
                return Err(e.into());
            }
            let backoff = calculate_backoff(*attempt);
            tracing::trace!(
                attempt = *attempt,
                elapsed_ms = start_time.elapsed().as_millis(),
                backoff_ms = backoff.as_millis(),
                "retrying write after version conflict"
            );
            std::thread::sleep(backoff);
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

#[cfg(test)]
mod tests;
