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

    pub(super) fn perform_replacement_upsert(
        &self,
        query: &Arc<PhysicalPlan>,
        replacement: &Document,
        parameters: &Parameters,
    ) -> Result<(Document, Bson)> {
        let id = replacement
            .get("_id")
            .cloned()
            .or_else(|| self.extract_upsert_id_from_query(query, parameters))
            .unwrap_or_else(|| self.generate_id());

        let new_doc = if replacement.contains_key("_id") {
            replacement.clone()
        } else {
            super::write::prepend_id_to_document(id.clone(), replacement)
        };

        Ok((new_doc, id))
    }

    fn extract_upsert_id_from_query(
        &self,
        query: &PhysicalPlan,
        parameters: &Parameters,
    ) -> Option<Bson> {
        match query {
            PhysicalPlan::PointSearch { key, filter, .. } => {
                let BsonValue(id) = bind::bind_parameter(key, parameters);
                Some(id).or_else(|| {
                    filter
                        .as_ref()
                        .and_then(|expr| self.extract_upsert_id_from_expr(expr, parameters))
                })
            }
            PhysicalPlan::IndexScan { filter, .. }
            | PhysicalPlan::CollectionScan { filter, .. } => filter
                .as_ref()
                .and_then(|expr| self.extract_upsert_id_from_expr(expr, parameters)),
            PhysicalPlan::MultiPointSearch { filter, .. } => filter
                .as_ref()
                .and_then(|expr| self.extract_upsert_id_from_expr(expr, parameters)),
            PhysicalPlan::Filter { input, predicate } => self
                .extract_upsert_id_from_query(input, parameters)
                .or_else(|| self.extract_upsert_id_from_expr(predicate, parameters)),
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::InMemorySort { input, .. }
            | PhysicalPlan::ExternalMergeSort { input, .. }
            | PhysicalPlan::TopKHeapSort { input, .. }
            | PhysicalPlan::Limit { input, .. } => {
                self.extract_upsert_id_from_query(input, parameters)
            }
            _ => None,
        }
    }

    fn extract_upsert_id_from_expr(&self, expr: &Expr, parameters: &Parameters) -> Option<Bson> {
        match expr {
            Expr::And(exprs) => exprs
                .iter()
                .find_map(|expr| self.extract_upsert_id_from_expr(expr, parameters)),
            Expr::FieldFilters { field, filters } => {
                let Expr::Field(path) = field.as_ref() else {
                    return None;
                };
                if path.len() != 1 || path[0] != "_id".into() {
                    return None;
                }
                filters
                    .iter()
                    .find_map(|filter| self.extract_point_value(filter, parameters))
                    .map(|value| value.0)
            }
            _ => None,
        }
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
            PhysicalPlan::IndexScan { filter, .. }
            | PhysicalPlan::CollectionScan { filter, .. } => {
                if let Some(filter_expr) = filter {
                    self.extract_equality_from_expr(filter_expr, parameters, doc);
                }
            }
            PhysicalPlan::MultiPointSearch { filter, .. } => {
                if let Some(filter_expr) = filter {
                    self.extract_equality_from_expr(filter_expr, parameters, doc);
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                self.extract_equality_conditions(input, parameters, doc);
                self.extract_equality_from_expr(predicate, parameters, doc);
            }
            PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::InMemorySort { input, .. }
            | PhysicalPlan::ExternalMergeSort { input, .. }
            | PhysicalPlan::TopKHeapSort { input, .. }
            | PhysicalPlan::Limit { input, .. } => {
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
