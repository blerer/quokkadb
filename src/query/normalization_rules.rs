use crate::query::logical::logical_plan::LogicalPlan;
use crate::query::logical::Expr;
use crate::query::optimizer::NormalisationRule;
use crate::query::tree_node::TreeNode;
use std::rc::Rc;
use std::sync::Arc;

pub struct SimplifyExpressions;

impl NormalisationRule for SimplifyExpressions {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        plan.transform_up(&|node: Arc<LogicalPlan>| match node.as_ref() {
            LogicalPlan::Filter { input, condition } => {
                let expr = condition.clone().transform_up(&|c| Self::simplify_expr(c));
                Arc::new(LogicalPlan::Filter {
                    input: input.clone(),
                    condition: expr,
                })
            }
            _ => node,
        })
    }
}

impl SimplifyExpressions {
    fn simplify_expr(expr: Arc<Expr>) -> Arc<Expr> {
        match expr.as_ref() {
            // ---- Flatten Nested Operators ----
            Expr::And(conditions) => Arc::new(Expr::And(Self::flatten_and(conditions))),
            Expr::Or(conditions) => Arc::new(Expr::Or(Self::flatten_or(conditions))),
            Expr::Nor(conditions) => Arc::new(Expr::Nor(Self::flatten_nor(conditions))),

            // // ---- Remove Duplicates ----
            // Expr::And(conditions) => Rc::new(Expr::And(Self::remove_duplicates(conditions))),
            // Expr::Or(conditions) => Rc::new(Expr::Or(Self::remove_duplicates(conditions))),
            // Expr::Nor(conditions) => Rc::new(Expr::Nor(Self::remove_duplicates(conditions))),
            //
            // // ---- Identity Rules ----
            // Expr::And(conditions) if conditions.is_empty() => Rc::new(Expr::Literal(true.into())),
            // Expr::Or(conditions) if conditions.is_empty() => Rc::new(Expr::Literal(false.into())),
            // Expr::Nor(conditions) if conditions.is_empty() => Rc::new(Expr::Literal(true.into())),
            // Expr::Not(inner) if matches!(inner.as_ref(), Expr::Literal(bson) if bson == &Bson::Boolean(true))
            // => Rc::new(Expr::Literal(false.into())),
            // Expr::Not(inner) if matches!(inner.as_ref(), Expr::Literal(bson) if bson == &Bson::Boolean(false))
            // => Rc::new(Expr::Literal(true.into())),
            //
            // // ---- Complementarity ----
            // Expr::And(conditions) if Self::contains_negation(conditions) => Rc::new(Expr::Literal(false.into())),
            // Expr::Or(conditions) if Self::contains_negation(conditions) => Rc::new(Expr::Literal(true.into())),
            // Expr::Nor(conditions) if Self::contains_negation(conditions) => Rc::new(Expr::Literal(false.into())),
            //
            // // ---- DeMorgan’s Laws ----
            // Expr::Not(inner) => match inner.as_ref() {
            //     Expr::And(conditions) => Rc::new(Expr::Or(conditions.iter().map(|c| Rc::new(Expr::Not(c.clone()))).collect())),
            //     Expr::Or(conditions) => Rc::new(Expr::And(conditions.iter().map(|c| Rc::new(Expr::Not(c.clone()))).collect())),
            //     Expr::Nor(conditions) => Rc::new(Expr::Or(conditions.clone())), // $nor(A, B) -> not($or(A, B))
            //     _ => expr.clone(),
            // },
            _ => expr.clone(),
        }
    }

    fn flatten_and(expressions: &[Arc<Expr>]) -> Vec<Arc<Expr>> {
        let mut result = Vec::new();
        for expr in expressions {
            if let Expr::And(sub_exprs) = expr.as_ref() {
                result.extend(sub_exprs.clone());
            } else {
                result.push(expr.clone());
            }
        }
        result
    }

    fn flatten_or(expressions: &[Arc<Expr>]) -> Vec<Arc<Expr>> {
        let mut result = Vec::new();
        for expr in expressions {
            if let Expr::Or(sub_exprs) = expr.as_ref() {
                result.extend(sub_exprs.clone());
            } else {
                result.push(expr.clone());
            }
        }
        result
    }

    fn flatten_nor(expressions: &[Arc<Expr>]) -> Vec<Arc<Expr>> {
        let mut result = Vec::new();
        for expr in expressions {
            if let Expr::Nor(sub_exprs) = expr.as_ref() {
                result.extend(sub_exprs.clone());
            } else {
                result.push(expr.clone());
            }
        }
        result
    }

    // fn remove_duplicates(expressions: &[Rc<Expr>]) -> Vec<Rc<Expr>> {
    //     let mut seen = HashSet::new();
    //     let mut unique = Vec::new();
    //
    //     for expr in expressions {
    //         if seen.insert(expr.to_string()) { // Assuming `Expr` has a `to_string()`
    //             unique.push(expr.clone());
    //         }
    //     }
    //
    //     unique
    // }

    // fn contains_negation(expressions: &[Rc<Expr>]) -> bool {
    //     let mut expr_set = HashSet::new();
    //
    //     for expr in expressions {
    //         match expr.as_ref() {
    //             Expr::Not(inner) => {
    //                 if expr_set.contains(&inner.to_string()) {
    //                     return true; // Found A and !A
    //                 }
    //             }
    //             _ => {
    //                 expr_set.insert(expr.to_string());
    //             }
    //         }
    //     }
    //
    //     false
    // }
}

pub struct SimplifyNotExpressions;

impl NormalisationRule for SimplifyNotExpressions {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        plan.transform_down(&|node: Arc<LogicalPlan>| match node.as_ref() {
            LogicalPlan::Filter {
                input, condition, ..
            } => {
                let new_condition = condition.clone().transform_down(&Self::push_down_not);
                Arc::new(LogicalPlan::Filter {
                    input: input.clone(),
                    condition: new_condition,
                })
            }
            _ => node,
        })
    }
}

impl SimplifyNotExpressions {
    fn push_down_not(expr: Arc<Expr>) -> Arc<Expr> {
        match expr.as_ref() {
            Expr::Not(expr) => expr.clone().negate(),
            _ => expr.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical::expr_fn::*;
    use crate::query::logical::logical_plan::LogicalPlanBuilder;

    #[test]
    fn test_not_normalization() {
        // Test Double Negation: NOT(NOT(X)) → X
        let original = not(not(eq(5)));
        let transformed = eq(5);

        check_expr_transformation(SimplifyNotExpressions {}, original, transformed);

        // Test Negation of Comparison Operators
        check_expr_transformation(SimplifyNotExpressions {}, not(gt(5)), lte(5));
        check_expr_transformation(SimplifyNotExpressions {}, not(lt(5)), gte(5));
        check_expr_transformation(SimplifyNotExpressions {}, not(eq(5)), ne(5));
        check_expr_transformation(SimplifyNotExpressions {}, not(ne(5)), eq(5));

        // Test De Morgan's Laws
        let original = not(and([eq(1), gt(2)]));
        let transformed = or([ne(1), lte(2)]);

        check_expr_transformation(SimplifyNotExpressions {}, original, transformed);

        let original = not(or([eq(1), lt(2)]));
        let transformed = and([ne(1), gte(2)]);

        check_expr_transformation(SimplifyNotExpressions {}, original, transformed);
    }

    fn check_expr_transformation(
        rule: impl NormalisationRule,
        original: Arc<Expr>,
        transformed: Arc<Expr>,
    ) {
        let input_plan = scan_with_filter(original);
        let output_plan = rule.apply(input_plan);

        let expected_plan = scan_with_filter(transformed);
        assert_eq!(expected_plan, output_plan);
    }

    fn scan_with_filter(filter: Arc<Expr>) -> Arc<LogicalPlan> {
        LogicalPlanBuilder::scan("test_db", "users")
            .filter(filter)
            .build()
    }
}
