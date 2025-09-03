use std::ops::Bound;
use std::sync::Arc;

use crate::query::logical_plan::{transform_up_filter, LogicalPlan};
use crate::query::optimizer::normalization_rules;
use crate::query::optimizer::normalization_rules::NormalisationRule;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::{Expr, Interval, Parameters, SortField};
use crate::query::tree_node::TreeNode;
use crate::storage::catalog::Catalog;
use crate::storage::Direction;

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

type GroupId = usize;

// Direct physical candidate (built bottom-up)
#[derive(Clone)]
struct Candidate {
    plan: Arc<PhysicalPlan>,
    provides: Provides,
    cost: Cost,
}

/// A memoization table for the optimizer's search.
struct Memo {
    groups: Vec<Group>,
    // TODO: Add a map from logical plan hash to group ID to avoid duplicate groups.
}

impl Memo {
    fn new() -> Self {
        Self { groups: Vec::new() }
    }

    fn add_group(&mut self, plan: Arc<LogicalPlan>) -> GroupId {
        // For now, always add a new group.
        let group_id = self.groups.len();
        self.groups.push(Group {
            logical: vec![plan],
            best_plans: Vec::new(),
        });
        group_id
    }
}

/// A group represents a set of logically equivalent expressions.
struct Group {
    logical: Vec<Arc<LogicalPlan>>,
    /// The best physical plans found so far for this group, for a given set of required properties.
    /// We use a Vec because ReqProps cannot be a HashMap key (due to SortField).
    best_plans: Vec<(ReqProps, Arc<Best>)>,
}

/// Properties required from a physical plan by its parent.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReqProps {
    order: Option<Vec<SortField>>,
    limit: Option<usize>,
}

/// The best (cheapest) physical plan found for a given group and required properties.
#[derive(Debug, Clone)]
struct Best {
    plan: Arc<PhysicalPlan>,
    cost: Cost,
    provides: Provides,
}

/// Properties provided by a physical plan.
#[derive(Debug, Clone, PartialEq)]
struct Provides {
    order: Option<Vec<SortField>>,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            normalization_rules: Arc::new(normalization_rules::all_normalization_rules()),
            cost_estimator: CostEstimator {},
        }
    }

    pub fn optimize(&self, plan: Arc<LogicalPlan>, catalog: Arc<Catalog>) -> Arc<PhysicalPlan> {

        let mut memo = Memo::new();
        let root_group_id = memo.add_group(plan);

        // TODO: Find a way to extract required properties from the logical plan
        let required_props = ReqProps {
            order: None,
            limit: None,
        };

        self.best_expr(
            &mut memo,
            catalog.as_ref(),
            root_group_id,
            &required_props,
        )
        .plan
        .clone()
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
            }
            _ => expr,
        }
    }

    fn best_expr(
        &self,
        memo: &mut Memo,
        catalog: &Catalog,
        group_id: GroupId,
        req: &ReqProps,
    ) -> Arc<Best> {
        // 1. Check memoized best for this (group, req)
        for (memo_req, best) in &memo.groups[group_id].best_plans {
            if memo_req == req { return best.clone(); }
        }

        let logical_plan = memo.groups[group_id].logical[0].clone();

        // 2. Compute required child properties for this node
        let child_reqs = required_props_for_children(logical_plan.as_ref(), req);

        // 3. Add children to memo and compute their bests bottom-up
        let child_nodes = logical_plan.children();
        assert_eq!(child_nodes.len(), child_reqs.len());
        let mut child_bests: Vec<Arc<Best>> = Vec::with_capacity(child_nodes.len());
        for (i, child_node) in child_nodes.into_iter().enumerate() {
            let child_group = memo.add_group(child_node);
            let child_best = self.best_expr(memo, catalog, child_group, &child_reqs[i]);
            child_bests.push(child_best);
        }

        // 4. Implement this node directly into physical alternatives
        let mut candidates = self.implement_node(logical_plan.as_ref(), &child_bests, catalog);

        // 5. Enforce unmet properties per candidate
        let mut best: Option<Candidate> = None;
        for cand in candidates.drain(..) {
            let enforced = self.enforce_if_needed(cand, req);
            if best.as_ref().map_or(true, |b| enforced.cost < b.cost) {
                best = Some(enforced);
            }
        }

        let best = best.expect("Failed to produce a physical plan");
        let best = Arc::new(Best { plan: best.plan, cost: best.cost, provides: best.provides });

        // 6. Cache and return
        memo.groups[group_id].best_plans.push((req.clone(), best.clone()));
        best
    }

    fn enforce_if_needed(&self, mut cand: Candidate, req: &ReqProps) -> Candidate {
        // Enforce order
        if req.order.is_some() && cand.provides.order.as_ref() != req.order.as_ref() {
            // Choose a simple enforcer; you can add heuristics (TopK vs InMemorySort) using req.limit
            let sort_fields = Arc::new(req.order.clone().unwrap());
            let cost_enf = self.cost_estimator.estimate_node_cost(&PhysicalPlan::InMemorySort {
                input: cand.plan.clone(),
                sort_fields: sort_fields.clone(),
            });
            let plan = Arc::new(PhysicalPlan::InMemorySort {
                input: match &cand.plan { p => p.clone() },
                sort_fields: sort_fields.clone(),
            });
            let provides = Provides { order: Some((*sort_fields).clone()) };
            cand = Candidate { plan, cost: cand.cost + cost_enf, provides };
        }

        // Enforce limit if parent requires one that isn't guaranteed otherwise.
        if req.limit.is_some() {
            let (skip, limit) = (None, req.limit); // we only track limit in req; adapt if needed
            let cost_enf = self.cost_estimator.estimate_node_cost(&PhysicalPlan::Limit {
                input: cand.plan.clone(),
                skip,
                limit,
            });
            let plan = Arc::new(PhysicalPlan::Limit {
                input: match &cand.plan { p => p.clone() },
                skip,
                limit,
            });
            // Limit preserves order
            let provides = cand.provides.clone();
            cand = Candidate { plan, cost: cand.cost + cost_enf, provides };
        }

        cand
    }

    fn implement_node(
        &self,
        node: &LogicalPlan,
        child_bests: &[Arc<Best>],
        catalog: &Catalog,
    ) -> Vec<Candidate> {
        let mut out = Vec::new();

        match node {
            LogicalPlan::NoOp => {
                let plan = Arc::new(PhysicalPlan::NoOp);
                out.push(Candidate {
                    provides: derive_provides(&plan, &[]),
                    cost: self.cost_estimator.estimate_node_cost(&plan),
                    plan,
                });
            }
            LogicalPlan::CollectionScan { collection, filter, .. } => {
                // Baseline: full scan with optional residual filter
                let plan = Arc::new(PhysicalPlan::CollectionScan {
                    collection: *collection,
                    range: Interval::new(Bound::Unbounded, Bound::Unbounded),
                    direction: Direction::Forward,
                    filter: filter.clone(),
                    projection: None,
                });
                out.push(Candidate {
                    provides: derive_provides(&plan, &[]),
                    cost: self.cost_estimator.estimate_node_cost(&plan),
                    plan,
                });
                // TODO: index-aware alternatives (PointSearch / IndexScan) using catalog and filter
            }
            LogicalPlan::Filter { condition, .. } => {
                let child = &child_bests[0];
                let plan = Arc::new(PhysicalPlan::Filter { input: child.plan.clone(), predicate: condition.clone() });
                out.push(Candidate {
                    provides: derive_provides(&plan, &[child.provides.clone()]),
                    cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
                    plan,
                });
            }
            LogicalPlan::Projection { projection, .. } => {
                let child = &child_bests[0];
                let plan = Arc::new(PhysicalPlan::Projection { input: child.plan.clone(), projection: projection.clone() });
                out.push(Candidate {
                    provides: derive_provides(&plan, &[child.provides.clone()]),
                    cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
                    plan,
                });
            }
            LogicalPlan::Sort { sort_fields, .. } => {
                let child = &child_bests[0];
                // If child already provides the order, propagate (no extra work). We'll still compute enforcer later if needed.
                let plan = Arc::new(PhysicalPlan::InMemorySort { input: child.plan.clone(), sort_fields: sort_fields.clone() });
                out.push(Candidate {
                    provides: derive_provides(&plan, &[child.provides.clone()]),
                    cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
                    plan,
                });
            }
            LogicalPlan::Limit { skip, limit, .. } => {
                let child = &child_bests[0];
                let plan = Arc::new(PhysicalPlan::Limit { input: child.plan.clone(), skip: *skip, limit: *limit });
                out.push(Candidate {
                    provides: derive_provides(&plan, &[child.provides.clone()]),
                    cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
                    plan,
                });
            }
        }

        out
    }
}

fn required_props_for_children(node: &LogicalPlan, req: &ReqProps) -> Vec<ReqProps> {
    match node {
        LogicalPlan::Sort { sort_fields, .. } => vec![ReqProps {
            order: Some((**sort_fields).clone()),
            limit: req.limit,
        }],
        LogicalPlan::Limit { limit, .. } => vec![ReqProps {
            order: req.order.clone(),
            limit: req.limit.or(*limit),
        }],
        LogicalPlan::Filter { .. } | LogicalPlan::Projection { .. } => vec![req.clone()],
        LogicalPlan::CollectionScan { .. } | LogicalPlan::NoOp => vec![],
    }
}


fn derive_provides(plan: &PhysicalPlan, child_provides: &[Provides]) -> Provides {
    match plan {
        PhysicalPlan::InMemorySort { sort_fields, .. }
        | PhysicalPlan::ExternalMergeSort { sort_fields, .. }
        | PhysicalPlan::TopKHeapSort { sort_fields, .. } => Provides {
            order: Some((**sort_fields).clone()),
        },
        PhysicalPlan::Filter { .. } | PhysicalPlan::Projection { .. } | PhysicalPlan::Limit { .. } => {
            // Filter/Projection/Limit preserve ordering
            Provides { order: child_provides.get(0).and_then(|p| p.order.clone()) }
        }
        PhysicalPlan::CollectionScan { .. } => Provides { order: None },
        PhysicalPlan::IndexScan { .. } => {
            // For a future enhancement: if the index matches order, provide Some(...)
            Provides { order: None }
        }
        PhysicalPlan::PointSearch { .. } => Provides { order: None },
        PhysicalPlan::Union { .. } => Provides { order: None },
        PhysicalPlan::NoOp => Provides { order: None },
        _ => panic!("derive_provides not implemented for {:?}", plan),
    }
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
