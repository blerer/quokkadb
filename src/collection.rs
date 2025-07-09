use crate::error::Error;
use crate::query::logical::logical_plan::LogicalPlan;
use crate::query::logical::parser;
use bson::Document;
use std::sync::Arc;

pub struct Collection {
    collection: String,
}

impl Collection {
    pub fn new(collection: String) -> Collection {
        Collection { collection }
    }

    pub fn find(&self, filter: Document) -> Query {
        Query::new(self.collection.clone(), filter)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    collection: String,
    filter: Document, // Unified filter representation using Expr
    projection: Option<Document>,
    sort: Option<Document>,
    limit: Option<usize>,
    skip: Option<usize>,
}

impl Query {
    pub fn new(collection: String, filter: Document) -> Query {
        Query {
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

    pub fn execute(&self) -> Result<Vec<Document>> {
        let mut plan = LogicalPlan::TableScan {
            collection: self.collection.clone(),
            filter: None,
            projection: None,
            sort: None,
        };

        let conditions = parser::parse_conditions(&self.filter)?;

        plan = LogicalPlan::Filter {
            input: Arc::new(plan),
            condition: conditions,
        };

        if let Some(projection) = &self.projection {
            let projection = parser::parse_projection(&projection)?;
            plan = LogicalPlan::Projection {
                input: Arc::new(plan),
                projection: Arc::new(projection),
            }
        }

        if self.limit.is_some() || self.skip.is_some() {
            plan = LogicalPlan::Limit {
                input: Arc::new(plan),
                limit: self.limit.clone(),
                skip: self.skip.clone(),
            }
        }

        Err(Error::InvalidArgument("Not implemented".to_string()))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
