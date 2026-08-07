use super::bind;
use super::write::WriteExecutor;
use crate::error::Result;
use crate::query::execution::{set_path_value, updates};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::update::UpdateExpr;
use crate::query::{BsonValue, Expr, Parameters};
use bson::{Bson, Document};
use std::sync::Arc;

impl WriteExecutor {
    pub(super) fn perform_upsert(
        &self,
        query: &Arc<PhysicalPlan>,
        update: &UpdateExpr,
        parameters: &Parameters,
    ) -> Result<(Document, Bson)> {
        let updater = updates::to_updater(update, true)?;
        let mut new_doc = self.create_base_document_from_query(query, parameters)?;
        new_doc = updater(new_doc)?;

        let id = if new_doc.contains_key("_id") {
            new_doc.get("_id").unwrap().clone()
        } else {
            let id = self.generate_id();
            new_doc.insert("_id", id.clone());
            id
        };

        Ok((new_doc, id))
    }

    fn create_base_document_from_query(
        &self,
        query: &PhysicalPlan,
        parameters: &Parameters,
    ) -> Result<Document> {
        let mut doc = Document::new();
        self.extract_equality_conditions(query, parameters, &mut doc);
        Ok(doc)
    }

    fn extract_equality_conditions(
        &self,
        query: &PhysicalPlan,
        parameters: &Parameters,
        doc: &mut Document,
    ) {
        match query {
            PhysicalPlan::PointSearch { key, filter, .. } => {
                let BsonValue(id) = bind::bind_parameter(key, parameters);
                doc.insert("_id", id);
                if let Some(filter_expr) = filter {
                    self.extract_equality_from_expr(filter_expr, parameters, doc);
                }
            }
            PhysicalPlan::CollectionScan { filter, .. } => {
                if let Some(filter_expr) = filter {
                    self.extract_equality_from_expr(filter_expr, parameters, doc);
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                self.extract_equality_conditions(input, parameters, doc);
                self.extract_equality_from_expr(predicate, parameters, doc);
            }
            PhysicalPlan::Limit { input, .. } => {
                self.extract_equality_conditions(input, parameters, doc);
            }
            _ => {}
        }
    }

    fn extract_equality_from_expr(&self, expr: &Expr, parameters: &Parameters, doc: &mut Document) {
        match expr {
            Expr::And(exprs) => {
                for e in exprs {
                    self.extract_equality_from_expr(e, parameters, doc);
                }
            }
            Expr::FieldFilters { field, filters } => {
                if let Expr::Field(path) = field.as_ref() {
                    for filter in filters {
                        if let Some(value) = self.extract_point_value(filter, parameters) {
                            set_path_value(doc, path, value.0);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn extract_point_value(&self, expr: &Expr, parameters: &Parameters) -> Option<BsonValue> {
        match expr {
            Expr::Interval(interval) if interval.is_point() => interval
                .start_bound_value()
                .and_then(|e| self.resolve_expr_value(&e, parameters)),
            _ => None,
        }
    }

    fn resolve_expr_value(&self, expr: &Expr, parameters: &Parameters) -> Option<BsonValue> {
        match expr {
            Expr::Placeholder(idx) => Some(parameters.get(*idx).clone()),
            _ => None,
        }
    }
}
