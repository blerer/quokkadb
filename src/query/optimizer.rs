use crate::query::logical::logical_plan::LogicalPlan;
use std::rc::Rc;
use std::sync::Arc;
use crate::query::logical::{Expr, Parameters};
use crate::query::logical::physical_plan::PhysicalPlan;
use crate::query::tree_node::TreeNode;

pub struct Optimizer {
    normalization_rules: Vec<Arc<dyn NormalisationRule>>,
}

impl Optimizer {
    pub fn new(normalization_rules: Vec<Arc<dyn NormalisationRule>>) -> Self {
        Self {
            normalization_rules,
        }
    }

    pub fn optimize(&self, plan: LogicalPlan) -> PhysicalPlan {
        todo!()
    }

    pub fn normalize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut current_plan = Arc::new(plan);
        for rule in &self.normalization_rules {
            current_plan = rule.apply(current_plan);
        }
        current_plan.as_ref().clone()
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
    use crate::query::logical::expr_fn::{and, eq, field, field_filters};
    use crate::query::logical::logical_plan::LogicalPlanBuilder;
    use crate::query::logical::{BsonValue, ComparisonOperator, Expr};

    #[test]
    fn test_parametrize_simple_filter() {
        let optimizer = Optimizer::new(vec![]);
        let plan = LogicalPlanBuilder::scan("db", "collection")
            .filter(field_filters(field(["a"]), vec![eq(10)]))
            .build();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0), Some(&BsonValue::from(10)));

        // Check plan
        let expected_condition = Arc::new(Expr::FieldFilters {
            field: field(["a"]),
            filters: vec![Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(0)),
            })],
        });
        let expected_plan = LogicalPlanBuilder::scan("db", "collection")
            .filter(expected_condition)
            .build();

        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_complex_filter() {
        let optimizer = Optimizer::new(vec![]);
        let plan = LogicalPlanBuilder::scan("db", "collection")
            .filter(and(vec![
                field_filters(field(["a"]), vec![eq(10)]),
                field_filters(field(["b"]), vec![eq("hello")]),
            ]))
            .build();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 2);
        assert_eq!(params.get(0), Some(&BsonValue::from(10)));
        assert_eq!(params.get(1), Some(&BsonValue::from("hello")));

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

        let expected_plan = LogicalPlanBuilder::scan("db", "collection")
            .filter(expected_condition)
            .build();

        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_no_literals() {
        let optimizer = Optimizer::new(vec![]);
        let plan = LogicalPlanBuilder::scan("db", "collection")
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
