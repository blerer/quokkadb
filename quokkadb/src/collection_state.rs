use crate::collection::{IndexInfo, QueryOutput};
use crate::error::{Error, collection_not_found_error, index_not_found_error};
use crate::query::IndexKeySpec;
use crate::query::execution::WriteResult;
use crate::query::logical_plan::LogicalPlan;
use crate::{CollectionPolicy, CreateIndexOptions, DbImpl};
use std::sync::Arc;

pub(crate) struct CollectionState {
    db: Arc<DbImpl>,
    name: String,
    policy: CollectionPolicy,
}

impl CollectionState {
    pub(crate) fn new(db: Arc<DbImpl>, name: String) -> Self {
        Self {
            db,
            name,
            policy: CollectionPolicy::Strict,
        }
    }

    pub(crate) fn create_if_missing(&mut self) {
        self.policy = CollectionPolicy::CreateIfMissing;
    }

    fn resolve_collection_id(&self) -> Result<u32> {
        let collection_id = self.db.get_collection_id(&self.name);
        if collection_id.is_none() {
            match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => {
                    Ok(self.db.create_collection(&self.name, true)?)
                }
            }
        } else {
            Ok(collection_id.unwrap())
        }
    }

    pub(crate) fn drop_collection(&self) -> Result<()> {
        self.db.drop_collection(&self.name)
    }

    pub(crate) fn rename(&self, new_name: &str) -> Result<Self> {
        self.db.rename_collection(&self.name, new_name)?;
        Ok(Self {
            db: self.db.clone(),
            name: new_name.to_string(),
            policy: self.policy,
        })
    }

    pub(crate) fn create_index(
        &self,
        spec: IndexKeySpec,
        options: CreateIndexOptions,
    ) -> Result<String> {
        let collection_id = self.resolve_collection_id()?;
        self.db.create_index(collection_id, spec, options)
    }

    pub(crate) fn drop_index(&self, index_name: &str) -> Result<()> {
        let collection = self
            .db
            .get_collection(&self.name)
            .ok_or_else(|| collection_not_found_error(&self.name))?;

        let index = collection
            .get_index_by_name(index_name)
            .ok_or_else(|| index_not_found_error(&self.name, index_name))?;

        self.db.drop_index(collection.id, index.id)
    }

    pub(crate) fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        let collection = self.db.get_collection(&self.name);

        let Some(collection) = collection else {
            return match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => Ok(vec![]),
            };
        };

        Ok(collection
            .active_indexes()
            .into_iter()
            .map(|index| IndexInfo::from_definition(index.id, index.name(), &index.definition))
            .collect())
    }

    pub(crate) fn estimated_document_count(&self) -> Result<u64> {
        let Some(collection_id) = self.db.get_collection_id(&self.name) else {
            return match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => Ok(0),
            };
        };

        self.db.estimated_document_count(collection_id)
    }

    pub(crate) fn execute_write(
        &self,
        build_plan: impl FnOnce(u32) -> Result<LogicalPlan>,
        sync: bool,
    ) -> Result<WriteResult> {
        let collection_id = self.resolve_collection_id()?;
        let plan = build_plan(collection_id)?;
        self.db.execute_write(collection_id, plan, sync)
    }

    pub(crate) fn execute_delete(
        &self,
        build_plan: impl FnOnce(u32) -> Result<LogicalPlan>,
        sync: bool,
        return_document: bool,
    ) -> Result<WriteResult> {
        let Some(collection_id) = self.db.get_collection_id(&self.name) else {
            return if return_document {
                Ok(WriteResult::SingleDocument {
                    document: None,
                    affected_count: 0,
                })
            } else {
                Ok(WriteResult::Delete { deleted_count: 0 })
            };
        };

        let plan = build_plan(collection_id)?;
        self.db.execute_write(collection_id, plan, sync)
    }

    pub(crate) fn execute_query(
        &self,
        build_plan: impl FnOnce(u32) -> Result<Arc<LogicalPlan>>,
    ) -> Result<QueryOutput> {
        let Some(collection_id) = self.db.get_collection_id(&self.name) else {
            return match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => Ok(Box::new(std::iter::empty())),
            };
        };

        let plan = build_plan(collection_id)?;
        self.db.execute_query(collection_id, plan)
    }
}

type Result<T> = std::result::Result<T, Error>;
