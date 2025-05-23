use crate::query::logical::logical_plan::LogicalPlan;
use std::rc::Rc;
use std::sync::Arc;

pub struct Optimizer {
    normalization_rules: Vec<Arc<dyn NormalisationRule>>,
}

impl Optimizer {
    pub fn new(normalization_rules: Vec<Arc<dyn NormalisationRule>>) -> Self {
        Self {
            normalization_rules,
        }
    }

    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.normalize(plan)
    }

    pub fn normalize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut current_plan = Rc::new(plan);
        for rule in &self.normalization_rules {
            current_plan = rule.apply(current_plan);
        }
        current_plan.as_ref().clone()
    }
}

pub trait NormalisationRule {
    fn apply(&self, plan: Rc<LogicalPlan>) -> Rc<LogicalPlan>;
}
