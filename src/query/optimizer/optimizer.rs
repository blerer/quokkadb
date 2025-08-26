use crate::query::logical_plan::{transform_up_filter, LogicalPlan};
use crate::query::physical_plan::PhysicalPlan;
use crate::query::{format_path, ComparisonOperator, Expr, Parameters};
use crate::storage::catalog::{Catalog, IndexMetadata};
use std::collections::HashMap;
use std::sync::Arc;
use crate::query::optimizer::normalization_rules;
use crate::query::optimizer::normalization_rules::NormalisationRule;

pub type Cost = f64;

pub struct CostEstimator {}

impl CostEstimator {
    /// Estimates the cost of a single physical plan node, without considering its inputs.
    pub fn estimate_node_cost(&self, plan: &PhysicalPlan) -> Cost {
        match plan {
            PhysicalPlan::CollectionScan { .. } => 1000.0,
            PhysicalPlan::IndexScan { .. } => 100.0,
            PhysicalPlan::PointSearch { .. } => 1.0,
            PhysicalPlan::Filter { .. } => 1.0,
            PhysicalPlan::Projection { .. } => 0.5,
            PhysicalPlan::InMemorySort { .. } => 500.0,
            PhysicalPlan::ExternalMergeSort { .. } => 5000.0,
            PhysicalPlan::TopKHeapSort { .. } => 100.0,
            PhysicalPlan::Limit { .. } => 0.1,
            _ => f64::INFINITY,
        }
    }
}

pub struct Optimizer {
    normalization_rules: Arc<dyn NormalisationRule>,
    cost_estimator: CostEstimator,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            normalization_rules: Arc::new(normalization_rules::all_normalization_rules()),
            cost_estimator: CostEstimator {},
        }
    }

    pub fn optimize(&self, plan: Arc<LogicalPlan>, catalog: Arc<Catalog>) -> Arc<PhysicalPlan> {
        todo!()
    }

    /// Recursively calculates the total cost of a physical plan tree.
    fn get_plan_cost(&self, plan: &PhysicalPlan) -> Cost {
        let node_cost = self.cost_estimator.estimate_node_cost(plan);

        let input_cost: Cost = match plan {
            PhysicalPlan::Union { inputs } => inputs.iter().map(|input| self.get_plan_cost(input)).sum(),
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::InMemorySort { input, .. }
            | PhysicalPlan::ExternalMergeSort { input, .. }
            | PhysicalPlan::TopKHeapSort { input, .. }
            | PhysicalPlan::Limit { input, .. } => self.get_plan_cost(input),
            // Leaf nodes have no input cost.
            PhysicalPlan::CollectionScan { .. }
            | PhysicalPlan::NoOp
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::PointSearch { .. }
            | PhysicalPlan::InsertOne { .. }
            | PhysicalPlan::InsertMany { .. } => 0.0,
        };

        node_cost + input_cost
    }

    /// Analyzes an expression to find sargable filters based on available indexes and the primary key.
    pub fn find_sargable_filters(
        &self,
        expr: &Expr,
        indexes: Vec<Arc<IndexMetadata>>,
    ) -> Vec<SargableFilter> {

        // Flatten the expression into a list of conjuncts
        let mut conjuncts = Vec::new();
        if let Expr::And(conditions) = expr {
            for cond in conditions {
                conjuncts.push(cond.as_ref());
            }
        } else {
            conjuncts.push(expr);
        }

        // Map field names to their corresponding filter expressions
        let field_filters_map: HashMap<String, &Expr> = conjuncts
            .iter()
            .filter_map(|e| {
                if let Expr::FieldFilters { field, .. } = e {
                    if let Expr::Field(path) = field.as_ref() {
                        return Some((format_path(path), *e));
                    }
                }
                None
            })
            .collect();

        let mut sargable_filters = Vec::new();

        // Handle primary key separately
        if let Some(Expr::FieldFilters { field, filters }) =
            field_filters_map.get("_id").map(|e| *e)
        {
            let (sargable_exprs, residual_exprs): (Vec<_>, Vec<_>) =
                filters.iter().cloned().partition(|f| is_sargable_leaf(f));

            if !sargable_exprs.is_empty() {
                let residual = if residual_exprs.is_empty() {
                    None
                } else {
                    Some(Arc::new(Expr::FieldFilters {
                        field: field.clone(),
                        filters: residual_exprs,
                    }))
                };

                for sargable_expr in sargable_exprs {
                    let (kind, expr) = get_filter_kind_and_expr(sargable_expr);
                    sargable_filters.push(SargableFilter {
                        field_name: "_id".to_string(),
                        access_kind: AccessKind::PrimaryKey,
                        filter_kind: kind,
                        expr,
                        residual: residual.clone(),
                    });
                }
            }
        }

        // Handle other indexes
        for index in indexes {
            let mut matched_prefix_len = 0;
            for (field_name, _) in &index.fields {
                if field_filters_map.contains_key(field_name) {
                    matched_prefix_len += 1;
                } else {
                    break;
                }
            }

            if matched_prefix_len > 0 {
                for i in 0..matched_prefix_len {
                    let (field_name, _) = &index.fields[i];
                    if let Some(Expr::FieldFilters { field, filters }) =
                        field_filters_map.get(field_name).map(|e| *e)
                    {
                        let (sargable_exprs, residual_exprs): (Vec<_>, Vec<_>) =
                            filters.iter().cloned().partition(|f| is_sargable_leaf(f));

                        if sargable_exprs.is_empty() {
                            // If a field in the prefix has no sargable filter, we can't use the index
                            // for subsequent fields in the key.
                            break;
                        }

                        let residual = if residual_exprs.is_empty() {
                            None
                        } else {
                            Some(Arc::new(Expr::FieldFilters {
                                field: field.clone(),
                                filters: residual_exprs,
                            }))
                        };

                        for sargable_expr in sargable_exprs {
                            let (kind, expr) = get_filter_kind_and_expr(sargable_expr);
                            sargable_filters.push(SargableFilter {
                                field_name: field_name.clone(),
                                access_kind: AccessKind::Index(index.clone()),
                                filter_kind: kind,
                                expr,
                                residual: residual.clone(),
                            });
                        }
                    }
                }
            }
        }
        sargable_filters
    }

    pub fn normalize(&self, plan: LogicalPlan) -> Arc<LogicalPlan> {
        let current_plan = Arc::new(plan);
        self.normalization_rules.apply(current_plan)
    }

    pub fn parametrize(&self, plan: Arc<LogicalPlan>) -> (Arc<LogicalPlan>, Parameters) {
        use std::cell::RefCell;

        let parameters = RefCell::new(Parameters::new());
        let plan = transform_up_filter(plan, &|c| {
            Optimizer::parametrize_expr(c, &mut parameters.borrow_mut())
        });
        (plan, parameters.into_inner())
    }

    fn parametrize_expr(expr: Arc<Expr>, params: &mut Parameters) -> Arc<Expr> {
        match expr.as_ref() {
            Expr::Literal(value) => {
                // If the expression is a literal, we can parameterize it
                params.collect_parameter(value.clone())
            },
            _ => expr,
        }
    }
}

/// Returns true if an expression is sargable (can be used with an index).
fn is_sargable_leaf(expr: &Arc<Expr>) -> bool {
    match expr.as_ref() {
        // $eq, $gte, $gte, $lt, and $lte have been converted into interval expressions during normalization.
        // So we only need to check for intervals, $in and $exists here.
        Expr::Comparison { operator, value } => {
            matches!(value.as_ref(), Expr::Placeholder(_)) && matches!(operator, ComparisonOperator::In)
        }
        Expr::Interval(_) => true,
        Expr::Exists(true) => true,
        _ => false,
    }
}

/// Extracts the `FilterKind` and the expression itself from a sargable expression.
/// Panics if the expression is not a sargable leaf.
fn get_filter_kind_and_expr(expr: Arc<Expr>) -> (FilterKind, Arc<Expr>) {
    match expr.as_ref() {
        Expr::Comparison { operator, .. } => match operator {
            ComparisonOperator::Eq => (FilterKind::Eq, expr),
            ComparisonOperator::In => (FilterKind::In, expr),
            _ => unreachable!(), // Should be filtered by is_sargable_leaf
        },
        Expr::Interval(_) => (FilterKind::Range, expr),
        Expr::Exists(true) => (FilterKind::Exists, expr),
        _ => unreachable!(), // Should be filtered by is_sargable_leaf
    }
}

#[derive(Debug, PartialEq)]
struct SargableFilter {
    field_name: String,
    access_kind: AccessKind,
    filter_kind: FilterKind,
    expr: Arc<Expr>,
    residual: Option<Arc<Expr>>,
}

#[derive(Clone, Debug, PartialEq)]
enum AccessKind {
    PrimaryKey,
    Index(Arc<IndexMetadata>),
}

#[derive(Debug, PartialEq)]
enum FilterKind {
    Eq,
    Range,
    In,
    Exists,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::expr_fn::{and, eq, field, field_filters, lit};
    use crate::query::logical_plan::LogicalPlanBuilder;
    use crate::query::{BsonValue, ComparisonOperator, Expr};

    #[test]
    fn test_parametrize_simple_filter() {
        let optimizer = Optimizer::new();
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection)
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0), &BsonValue::from(10));

        // Check plan
        let expected_condition = Arc::new(Expr::FieldFilters {
            field: field(["a"]),
            filters: vec![Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(0)),
            })],
        });
        let expected_plan = LogicalPlanBuilder::scan(collection)
            .filter(expected_condition)
            .build();

        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_complex_filter() {
        let optimizer = Optimizer::new();
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection)
            .filter(and(vec![
                field_filters(field(["a"]), vec![eq(lit(10))]),
                field_filters(field(["b"]), vec![eq(lit("hello"))]),
            ]))
            .build();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 2);
        assert_eq!(params.get(0), &BsonValue::from(10));
        assert_eq!(params.get(1), &BsonValue::from("hello"));

        // Check plan
        let expected_condition = and(vec![
            Arc::new(Expr::FieldFilters {
                field: field(["a"]),
                filters: vec![Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Eq,
                    value: Arc::new(Expr::Placeholder(0)),
                })],
            }),
            Arc::new(Expr::FieldFilters {
                field: field(["b"]),
                filters: vec![Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Eq,
                    value: Arc::new(Expr::Placeholder(1)),
                })],
            }),
        ]);

        let expected_plan = LogicalPlanBuilder::scan(collection)
            .filter(expected_condition)
            .build();

        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_no_literals() {
        let optimizer = Optimizer::new();
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection)
            .filter(field_filters(
                field(["a"]),
                vec![Arc::new(Expr::Exists(true))],
            ))
            .build();
        let plan_clone = plan.clone();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 0);

        // Check plan
        assert_eq!(parametrized_plan, plan_clone);
    }

    use crate::query::expr_fn::{exists, interval, ne};
    use crate::storage::catalog::{IndexMetadata, Order};
    use crate::util::interval::Interval;

    fn make_index(id: u32, name: &str, fields: Vec<(&str, Order)>) -> Arc<IndexMetadata> {
        Arc::new(IndexMetadata {
            id,
            name: name.to_string(),
            fields: fields
                .into_iter()
                .map(|(s, o)| (s.to_string(), o))
                .collect(),
        })
    }

    #[test]
    fn test_sargable_primary_key() {
        let optimizer = Optimizer::new();
        let filter = field_filters(
            field(["_id"]),
            vec![interval(Interval::closed(lit(1), lit(1)))],
        );

        let sargable = optimizer.find_sargable_filters(&filter, vec![]);

        assert_eq!(sargable.len(), 1);
        assert_eq!(
            sargable[0],
            SargableFilter {
                field_name: "_id".to_string(),
                access_kind: AccessKind::PrimaryKey,
                filter_kind: FilterKind::Range,
                expr: interval(Interval::closed(lit(1), lit(1))),
                residual: None,
            }
        );
    }

    #[test]
    fn test_sargable_single_field_index() {
        let optimizer = Optimizer::new();
        let index_a = make_index(1, "index_a", vec![("a", Order::Ascending)]);
        let filter = field_filters(field(["a"]), vec![exists(true)]);

        let sargable = optimizer.find_sargable_filters(&filter, vec![index_a.clone()]);
        assert_eq!(sargable.len(), 1);
        assert_eq!(
            sargable[0],
            SargableFilter {
                field_name: "a".to_string(),
                access_kind: AccessKind::Index(index_a),
                filter_kind: FilterKind::Exists,
                expr: exists(true),
                residual: None,
            }
        );
    }

    #[test]
    fn test_sargable_compound_index_prefix() {
        let optimizer = Optimizer::new();
        let index_b_c =
            make_index(1, "index_b_c", vec![("b", Order::Ascending), ("c", Order::Ascending)]);
        let filter = and(vec![
            field_filters(
                field(["b"]),
                vec![interval(Interval::closed(lit(1), lit(1)))],
            ),
            field_filters(field(["d"]), vec![exists(true)]), // non-indexed part
        ]);

        let sargable = optimizer.find_sargable_filters(&filter, vec![index_b_c.clone()]);
        assert_eq!(sargable.len(), 1);
        assert_eq!(
            sargable[0],
            SargableFilter {
                field_name: "b".to_string(),
                access_kind: AccessKind::Index(index_b_c),
                filter_kind: FilterKind::Range,
                expr: interval(Interval::closed(lit(1), lit(1))),
                residual: None,
            }
        );
    }

    #[test]
    fn test_sargable_compound_index_full() {
        let optimizer = Optimizer::new();
        let index_b_c =
            make_index(1, "index_b_c", vec![("b", Order::Ascending), ("c", Order::Ascending)]);
        let filter = and(vec![
            field_filters(
                field(["b"]),
                vec![interval(Interval::closed(lit(1), lit(1)))],
            ),
            field_filters(field(["c"]), vec![exists(true)]),
        ]);

        let sargable = optimizer.find_sargable_filters(&filter, vec![index_b_c.clone()]);
        assert_eq!(sargable.len(), 2);

        assert_eq!(
            sargable[0],
            SargableFilter {
                field_name: "b".to_string(),
                access_kind: AccessKind::Index(index_b_c.clone()),
                filter_kind: FilterKind::Range,
                expr: interval(Interval::closed(lit(1), lit(1))),
                residual: None,
            }
        );
        assert_eq!(
            sargable[1],
            SargableFilter {
                field_name: "c".to_string(),
                access_kind: AccessKind::Index(index_b_c),
                filter_kind: FilterKind::Exists,
                expr: exists(true),
                residual: None,
            }
        );
    }

    #[test]
    fn test_sargable_with_residual() {
        let optimizer = Optimizer::new();
        let index_a = make_index(1, "index_a", vec![("a", Order::Ascending)]);
        let filter = field_filters(
            field(["a"]),
            vec![interval(Interval::closed(lit(1), lit(1))), ne(lit(5))],
        );

        let sargable = optimizer.find_sargable_filters(&filter, vec![index_a.clone()]);
        assert_eq!(sargable.len(), 1);
        assert_eq!(
            sargable[0],
            SargableFilter {
                field_name: "a".to_string(),
                access_kind: AccessKind::Index(index_a),
                filter_kind: FilterKind::Range,
                expr: interval(Interval::closed(lit(1), lit(1))),
                residual: Some(field_filters(field(["a"]), vec![ne(lit(5))])),
            }
        );
    }

    #[test]
    fn test_sargable_none_if_not_sargable() {
        let optimizer = Optimizer::new();
        let index_a = make_index(1, "index_a", vec![("a", Order::Ascending)]);
        let filter = field_filters(field(["a"]), vec![ne(lit(5))]);

        let sargable = optimizer.find_sargable_filters(&filter, vec![index_a]);
        assert!(sargable.is_empty());
    }

    #[test]
    fn test_sargable_none_if_not_indexed() {
        let optimizer = Optimizer::new();
        let filter = field_filters(field(["a"]), vec![interval(Interval::closed(lit(1), lit(1)))]);

        let sargable = optimizer.find_sargable_filters(&filter, vec![]);
        assert!(sargable.is_empty());
    }
}
