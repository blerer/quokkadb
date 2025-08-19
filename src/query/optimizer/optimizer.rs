use crate::query::logical_plan::LogicalPlan;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::tree_node::TreeNode;
use crate::query::{Expr, Parameters, Projection, SortField};
use crate::storage::catalog::Catalog;
use crate::storage::Direction;
use std::ops::Bound;
use std::sync::Arc;
use crate::query::optimizer::normalization_rules;

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
    normalization_rules: Vec<Arc<dyn NormalisationRule>>,
    cost_estimator: CostEstimator,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            normalization_rules: normalization_rules::all_normalization_rules(),
            cost_estimator: CostEstimator {},
        }
    }

    pub fn optimize(&self, plan: Arc<LogicalPlan>, catalog: Arc<Catalog>) -> Arc<PhysicalPlan> {
        self.optimize_node(plan, catalog)
    }

    fn optimize_node(&self, plan: Arc<LogicalPlan>, catalog: Arc<Catalog>) -> Arc<PhysicalPlan> {
        match plan.as_ref() {
            LogicalPlan::NoOp => Arc::new(PhysicalPlan::NoOp),
            LogicalPlan::CollectionScan {
                collection,
                projection,
                filter,
                sort,
            } => {
                let scan_plan = self.plan_scan(*collection, projection, filter, &catalog);
                if let Some(sort_fields) = sort {
                    self.plan_sort(scan_plan, sort_fields.clone())
                } else {
                    scan_plan
                }
            }
            LogicalPlan::Filter { input, condition } => {
                let optimized_input = self.optimize_node(input.clone(), catalog);
                Arc::new(PhysicalPlan::Filter {
                    input: optimized_input,
                    predicate: condition.clone(),
                })
            }
            LogicalPlan::Projection { input, projection } => {
                let optimized_input = self.optimize_node(input.clone(), catalog);
                Arc::new(PhysicalPlan::Projection {
                    input: optimized_input,
                    projection: projection.clone(),
                })
            }
            LogicalPlan::Sort {
                input,
                sort_fields,
            } => {
                let optimized_input = self.optimize_node(input.clone(), catalog);
                self.plan_sort(optimized_input, sort_fields.clone())
            }
            LogicalPlan::Limit { input, skip, limit } => {
                // Heuristic optimization: transform Sort + Limit into TopKHeapSort
                if let LogicalPlan::Sort {
                    input: sort_input,
                    sort_fields,
                } = input.as_ref()
                {
                    if let Some(k) = limit {
                        let total_limit = k + skip.unwrap_or(0);
                        let optimized_input = self.optimize_node(sort_input.clone(), catalog);

                        let top_k_plan = Arc::new(PhysicalPlan::TopKHeapSort {
                            input: optimized_input,
                            sort_fields: sort_fields.clone(),
                            k: total_limit,
                        });

                        // Re-apply Limit on top of TopK to handle skip
                        return Arc::new(PhysicalPlan::Limit {
                            input: top_k_plan,
                            skip: *skip,
                            limit: *limit,
                        });
                    }
                }

                // Default behavior
                let optimized_input = self.optimize_node(input.clone(), catalog);
                Arc::new(PhysicalPlan::Limit {
                    input: optimized_input,
                    skip: *skip,
                    limit: *limit,
                })
            }
        }
    }

    /// Chooses the best physical sort operator based on cost.
    fn plan_sort(&self, input: Arc<PhysicalPlan>, sort_fields: Arc<Vec<SortField>>) -> Arc<PhysicalPlan> {
        let in_memory = Arc::new(PhysicalPlan::InMemorySort {
            input: input.clone(),
            sort_fields: sort_fields.clone(),
        });
        let external = Arc::new(PhysicalPlan::ExternalMergeSort {
            input,
            sort_fields,
            max_in_memory_rows: 100_000,
        });

        if self.get_plan_cost(&in_memory) <= self.get_plan_cost(&external) {
            in_memory
        } else {
            external
        }
    }

    /// Determines the physical access path for a scan.
    fn plan_scan(
        &self,
        collection: u32,
        projection: &Option<Arc<Projection>>,
        filter: &Option<Arc<Expr>>,
        _catalog: &Arc<Catalog>,
    ) -> Arc<PhysicalPlan> {
        todo!()
    }

    /// Recursively calculates the total cost of a physical plan tree.
    fn get_plan_cost(&self, plan: &PhysicalPlan) -> Cost {
        let node_cost = self.cost_estimator.estimate_node_cost(plan);

        let input_cost: Cost = match plan {
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

    pub fn normalize(&self, plan: LogicalPlan) -> Arc<LogicalPlan> {
        let mut current_plan = Arc::new(plan);
        for rule in &self.normalization_rules {
            current_plan = rule.apply(current_plan);
        }
        current_plan
    }

    pub fn parametrize(&self, plan: Arc<LogicalPlan>) -> (Arc<LogicalPlan>, Parameters) {
        use std::cell::RefCell;

        let parameters = RefCell::new(Parameters::new());
        let plan = plan.transform_up(&|node: Arc<LogicalPlan>|
            match node.as_ref() {
                LogicalPlan::Filter { input, condition } => {
                    let expr = condition.clone().transform_up(&|c| {
                        Optimizer::parametrize_expr(c, &mut parameters.borrow_mut())
                    });
                    Arc::new(LogicalPlan::Filter {
                        input: input.clone(),
                        condition: expr,
                    })
                },
                _ => node,
            });
        (plan, parameters.into_inner())
    }

    fn parametrize_expr(expr: Arc<Expr>, params: &mut Parameters) -> Arc<Expr> {
        // This function is a placeholder for future parameterization logic
        // Currently, it just returns the input expression unchanged
        match expr.as_ref() {
            Expr::Literal(value) => {
                // If the expression is a literal, we can parameterize it
                params.collect_parameter(value.clone())
            },
            _ => expr,
        }
    }
}

pub trait NormalisationRule {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan>;
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
}
