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
