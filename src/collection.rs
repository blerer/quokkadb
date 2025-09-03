use crate::error::Error;
use crate::query::logical_plan::LogicalPlanBuilder;
use crate::query::parser;
use bson::Document;
use std::sync::Arc;
use crate::DbImpl;

pub struct Collection {
    db_impl: Arc<DbImpl>,
    collection: String,
}

impl Collection {
    pub fn new(db_impl: Arc<DbImpl>, collection: String) -> Collection {
        Collection { db_impl, collection }
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
    pub fn new(db_impl: Arc<DbImpl>, collection: String, filter: Document) -> Query {
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

        let collection_id = self.db_impl.create_collection_if_not_exists(&self.collection)?;

        let conditions = parser::parse_conditions(&self.filter)?;

        let mut builder = LogicalPlanBuilder::scan(collection_id).filter(conditions);

        if let Some(projection) = &self.projection {
            let projection = parser::parse_projection(&projection)?;
            builder = builder.project(Arc::new(projection));
        }

        if self.limit.is_some() || self.skip.is_some() {
            builder = builder.limit(self.skip, self.limit);
        }

        if let Some(sort) = &self.sort {
            let sort = parser::parse_sort(&sort)?;
            builder = builder.sort(Arc::new(sort));
        }

        self.db_impl.execute_plan(builder.build())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
