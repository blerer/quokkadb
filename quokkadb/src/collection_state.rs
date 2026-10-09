use crate::collection::{IndexInfo, QueryOutput};
use crate::error::{Error, collection_not_found_error, index_not_found_error};
use crate::query::IndexKeySpec;
use crate::query::Parameters;
use crate::query::execution::WriteResult;
use crate::query::logical_plan::LogicalPlan;
use crate::query::physical_plan::PhysicalPlan;
use crate::storage::catalog::CollectionMetadata;
use crate::{CollectionPolicy, CreateIndexOptions, DbImpl};
use std::sync::Arc;

pub(crate) struct CollectionState {
    db: Arc<DbImpl>,
    name: String,
    policy: CollectionPolicy,
}

#[derive(Debug, Clone)]
pub(crate) enum Hint {
    Index(String),
    CollectionScan,
}

pub(crate) struct PreparedQuery {
    collection: Arc<CollectionMetadata>,
    plan: Arc<PhysicalPlan>,
    parameters: Parameters,
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

    fn resolve_collection(&self) -> Result<Arc<CollectionMetadata>> {
        let collection = self.db.get_collection(&self.name);
        if collection.is_none() {
            match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => {
                    Ok(self.db.create_collection(&self.name, true)?)
                }
            }
        } else {
            Ok(collection.unwrap())
        }
    }

    fn resolve_hint_id(
        collection: &CollectionMetadata,
        hint: Option<&Hint>,
    ) -> Result<Option<u32>> {
        match hint {
            None => Ok(None),
            Some(Hint::CollectionScan) => Ok(Some(0)),
            Some(Hint::Index(index_name)) => collection
                .get_index_by_name(index_name)
                .map(|index| Some(index.id))
                .ok_or_else(|| index_not_found_error(&collection.name, index_name)),
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
        let collection = self.resolve_collection()?;
        self.db.create_index(collection.id, spec, options)
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
        let Some(collection) = self.db.get_collection(&self.name) else {
            return match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => Ok(0),
            };
        };

        self.db.estimated_document_count(collection.id)
    }

    pub(crate) fn execute_write(
        &self,
        hint: Option<&Hint>,
        build_plan: impl FnOnce(u32, Option<u32>) -> Result<LogicalPlan>,
        sync: bool,
    ) -> Result<WriteResult> {
        let collection = self.resolve_collection()?;
        let hint_id = Self::resolve_hint_id(&collection, hint)?;
        let plan = build_plan(collection.id, hint_id)?;
        self.db.execute_write(&collection, plan, sync)
    }

    pub(crate) fn execute_delete(
        &self,
        hint: Option<&Hint>,
        build_plan: impl FnOnce(u32, Option<u32>) -> Result<LogicalPlan>,
        sync: bool,
        return_document: bool,
    ) -> Result<WriteResult> {
        let Some(collection) = self.db.get_collection(&self.name) else {
            return if return_document {
                Ok(WriteResult::SingleDocument {
                    document: None,
                    affected_count: 0,
                })
            } else {
                Ok(WriteResult::Delete { deleted_count: 0 })
            };
        };

        let hint_id = Self::resolve_hint_id(&collection, hint)?;
        let plan = build_plan(collection.id, hint_id)?;
        self.db.execute_write(&collection, plan, sync)
    }

    pub(crate) fn execute_query(
        &self,
        hint: Option<&Hint>,
        build_plan: impl FnOnce(u32, Option<u32>) -> Result<Arc<LogicalPlan>>,
    ) -> Result<QueryOutput> {
        match self.prepare_query(hint, build_plan)? {
            Some(prepared) => self.execute_prepared_query(prepared),
            None => Ok(Box::new(std::iter::empty())),
        }
    }

    pub(crate) fn prepare_query(
        &self,
        hint: Option<&Hint>,
        build_plan: impl FnOnce(u32, Option<u32>) -> Result<Arc<LogicalPlan>>,
    ) -> Result<Option<PreparedQuery>> {
        let Some(collection) = self.db.get_collection(&self.name) else {
            return match self.policy {
                CollectionPolicy::Strict => Err(collection_not_found_error(&self.name)),
                CollectionPolicy::CreateIfMissing => Ok(None),
            };
        };

        let hint_id = Self::resolve_hint_id(&collection, hint)?;
        let logical_plan = build_plan(collection.id, hint_id)?;
        let (parameters, plan) = self.db.optimize_query(&collection, logical_plan)?;
        Ok(Some(PreparedQuery {
            collection,
            plan,
            parameters,
        }))
    }

    pub(crate) fn execute_prepared_query(&self, prepared: PreparedQuery) -> Result<QueryOutput> {
        match self
            .db
            .executor
            .execute_cached(prepared.plan, &prepared.parameters)
        {
            Err(Error::CollectionNotFound { .. })
                if self.policy == CollectionPolicy::CreateIfMissing =>
            {
                Ok(Box::new(std::iter::empty()))
            }
            result => result,
        }
    }

    pub(crate) fn plan_query(
        &self,
        hint: Option<&Hint>,
        build_plan: impl FnOnce(u32, Option<u32>) -> Result<Arc<LogicalPlan>>,
    ) -> Result<Option<(Arc<PhysicalPlan>, Arc<CollectionMetadata>)>> {
        let Some(prepared) = self.prepare_query(hint, build_plan)? else {
            return Ok(None);
        };

        Ok(Some((prepared.plan, prepared.collection)))
    }
}

type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
