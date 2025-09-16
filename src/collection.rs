use crate::error::Error;
use crate::query::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use crate::query::parser;
use bson::{to_vec, Document};
use std::sync::Arc;
use serde::Serialize;
use crate::DbImpl;

pub struct Collection {
    db_impl: Arc<DbImpl>,
    collection: String,
}

impl Collection {
    pub(crate) fn new(db_impl: Arc<DbImpl>, collection: String) -> Collection {
        Collection { db_impl, collection }
    }

    pub fn insert_one(
        &self,
        document: impl Serialize,
    ) -> Result<Document> {

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let plan = LogicalPlan::InsertOne {
            collection: collection_id,
            document: to_vec(&document)?,
        };

        self.db_impl.execute_write(plan)
    }

    pub fn insert_many(
        &self,
        documents: impl IntoIterator<Item = impl Serialize>,
    ) -> Result<Document> {

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let mut serialized = Vec::new();
        for doc in documents {
            serialized.push(to_vec(&doc)?);
        }

        let plan = LogicalPlan::InsertMany {
            collection: collection_id,
            documents: serialized,
        };

        self.db_impl.execute_write(plan)
    }

    pub fn find(&self, filter: Document) -> Query {
        Query::new(self.db_impl.clone(), self.collection.clone(), filter)
    }
}

pub struct Query {
    db_impl: Arc<DbImpl>,
    collection: String,
    filter: Document, // Unified filter representation using Expr
    projection: Option<Document>,
    sort: Option<Document>,
    limit: Option<usize>,
    skip: Option<usize>,
}

impl Query {
    fn new(db_impl: Arc<DbImpl>, collection: String, filter: Document) -> Query {
        Query {
            db_impl,
            collection,
            filter,
            projection: None,
            sort: None,
            limit: None,
            skip: None,
        }
    }

    pub fn projection(&mut self, projection: Document) -> &mut Self {
        self.projection = Some(projection);
        self
    }

    pub fn sort(&mut self, sort: Document) -> &mut Self {
        self.sort = Some(sort);
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn skip(&mut self, value: usize) -> &mut Self {
        self.skip = Some(value);
        self
    }

    pub fn execute(&self) -> Result<Box<dyn Iterator<Item=Result<Document>>>> {

        let collection_id = self.db_impl.get_collection_id(&self.collection);

        if collection_id.is_none() {
            return Ok(Box::new(std::iter::empty()));
        }

        let conditions = parser::parse_conditions(&self.filter)?;

        let mut builder = LogicalPlanBuilder::scan(collection_id.unwrap()).filter(conditions);

        if let Some(projection) = &self.projection {
            let projection = parser::parse_projection(&projection)?;
            builder = builder.project(Arc::new(projection));
        }

        if let Some(sort) = &self.sort {
            let sort = parser::parse_sort(&sort)?;
            builder = builder.sort(Arc::new(sort));
        }

        if self.limit.is_some() || self.skip.is_some() {
            builder = builder.limit(self.skip, self.limit);
        }

        self.db_impl.execute_query(builder.build())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
