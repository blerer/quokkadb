use std::collections::HashMap;
use std::convert::Into;
use std::sync::Arc;
use std::time::Instant;
use crate::event;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::obs::logger::LoggerAndTracer;
use crate::query::logical_plan::{transform_down_filter, LogicalPlan};
use crate::query::optimizer::normalization_rules;
use crate::query::optimizer::normalization_rules::NormalisationRule;
use crate::query::physical_plan::PhysicalPlan;
use crate::query::tree_node::TreeNode;
use crate::query::{ComparisonOperator, Expr, Interval, Limit, Parameters, Projection, SortField, SortOrder};
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
            PhysicalPlan::MultiPointSearch { .. } => 10.0,
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
    logger: Arc<dyn LoggerAndTracer>,
    normalization_rules: Arc<dyn NormalisationRule>,
    cost_estimator: CostEstimator,
}

type GroupId = usize;

#[derive(PartialEq, Eq, Hash, Clone)]
enum LogicalPlanKey {
    NoOp,
    CollectionScan {
        collection: u32,
        projection: Option<Vec<u8>>,
        filter: Option<Arc<Expr>>,
        sort: Option<Arc<Vec<SortField>>>,
    },
    Filter {
        input: GroupId,
        condition: Arc<Expr>,
    },
    Projection {
        input: GroupId,
        projection: Vec<u8>,
    },
    Sort {
        input: GroupId,
        sort_fields: Arc<Vec<SortField>>,
    },
    Limit {
        input: GroupId,
        limit: Limit,
    },
}

impl LogicalPlanKey {
    fn from_plan(plan: &LogicalPlan, child_groups: &[GroupId]) -> Self {
        match plan {
            LogicalPlan::NoOp => {
                assert!(child_groups.is_empty());
                LogicalPlanKey::NoOp
            }
            LogicalPlan::CollectionScan {
                collection,
                projection,
                filter,
                sort,
            } => {
                assert!(child_groups.is_empty());
                LogicalPlanKey::CollectionScan {
                    collection: *collection,
                    projection: projection.as_ref().map(|p| {
                        let mut writer = ByteWriter::new();
                        p.write_to(&mut writer);
                        writer.take_buffer()
                    }),
                    filter: filter.clone(),
                    sort: sort.clone(),
                }
            }
            LogicalPlan::Filter { condition, .. } => {
                assert_eq!(child_groups.len(), 1);
                LogicalPlanKey::Filter {
                    input: child_groups[0],
                    condition: condition.clone(),
                }
            }
            LogicalPlan::Projection { projection, .. } => {
                assert_eq!(child_groups.len(), 1);
                let mut writer = ByteWriter::new();
                projection.write_to(&mut writer);
                LogicalPlanKey::Projection {
                    input: child_groups[0],
                    projection: writer.take_buffer(),
                }
            }
            LogicalPlan::Sort { sort_fields, .. } => {
                assert_eq!(child_groups.len(), 1);
                LogicalPlanKey::Sort {
                    input: child_groups[0],
                    sort_fields: sort_fields.clone(),
                }
            }
            LogicalPlan::Limit { limit, .. } => {
                assert_eq!(child_groups.len(), 1);
                LogicalPlanKey::Limit {
                    input: child_groups[0],
                    limit: limit.clone(),
                }
            }
            _ => panic!("Unsupported logical plan node in key generation: {:?}", plan)
        }
    }
}

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
    group_ids: HashMap<LogicalPlanKey, GroupId>,
}

impl Memo {
    fn new() -> Self {
        Self {
            groups: Vec::new(),
            group_ids: HashMap::new(),
        }
    }

    fn add_group(&mut self, plan: Arc<LogicalPlan>) -> GroupId {
        let child_group_ids: Vec<GroupId> = plan
            .children()
            .into_iter()
            .map(|c| self.add_group(c))
            .collect();

        let key = LogicalPlanKey::from_plan(plan.as_ref(), &child_group_ids);

        if let Some(&group_id) = self.group_ids.get(&key) {
            return group_id;
        }

        let group_id = self.groups.len();
        self.groups.push(Group {
            logical: vec![plan],
            best_plans: HashMap::new(),
        });
        self.group_ids.insert(key, group_id);
        group_id
    }
}

/// A group represents a set of logically equivalent expressions.
struct Group {
    logical: Vec<Arc<LogicalPlan>>,
    /// The best physical plans found so far for this group, for a given set of required properties.
    best_plans: HashMap<ReqProps, Arc<Best>>,
}

/// Properties required from a physical plan by its parent.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
struct ReqProps {
    order: Option<Vec<SortField>>,
    limit: Option<Limit>,
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
    limit: Option<Limit>,
}

impl Optimizer {
    pub fn new(logger: Arc<dyn LoggerAndTracer>,) -> Self {
        Self {
            logger: logger.clone(),
            normalization_rules: Arc::new(normalization_rules::all_normalization_rules()),
            cost_estimator: CostEstimator {},
        }
    }

    pub fn optimize(&self, plan: Arc<LogicalPlan>, catalog: Arc<Catalog>) -> Arc<PhysicalPlan> {

        event!(self.logger, "optimization start, logical_plan={:?}", plan);
        let start = Instant::now();

        if matches!(plan.as_ref(), &LogicalPlan::NoOp) {
            return Arc::new(PhysicalPlan::NoOp);
        }

        let mut memo = Memo::new();
        let root_group_id = memo.add_group(plan);

        let required_props = ReqProps::default(); // No required properties for the root

        let physical_plan = self.best_expr(
            catalog.as_ref(),
            &mut memo,
            root_group_id,
            &required_props,
        )
        .plan
        .clone();

        let duration = start.elapsed();
        event!(self.logger, "optimization done, duration={}µs, physical_plan={:?}",
            duration.as_micros(),
            physical_plan);

        physical_plan
    }

    pub fn normalize(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        event!(self.logger, "normalization start, logical_plan={:?}", plan);
        let start = Instant::now();

        let logical_plan = self.normalization_rules.apply(plan);

        let duration = start.elapsed();
        event!(self.logger, "normalization done,duration={}µs, normalized_plan={:?}",
            duration.as_micros(),
            logical_plan);
        logical_plan
    }

    pub fn parametrize(&self, plan: Arc<LogicalPlan>) -> (Arc<LogicalPlan>, Parameters) {
        use std::cell::RefCell;

        let parameters = RefCell::new(Parameters::new());
        let plan = transform_down_filter(plan, &|c| {
            Optimizer::parametrize_expr(c, &mut parameters.borrow_mut())
        });
        (plan, parameters.into_inner())
    }

    fn parametrize_expr(expr: Arc<Expr>, params: &mut Parameters) -> Arc<Expr> {
        match expr.as_ref() {
            Expr::Interval(interval) => {
                if interval.is_point() {
                    let bound = interval.start_bound_value().unwrap();
                    let bound = if let Expr::Literal(value) = bound.as_ref() {
                        params.collect_parameter(value.clone())
                    } else {
                        unreachable!("Interval bound should be literals")
                    };
                    Arc::new(Expr::Interval(Interval::closed(bound.clone(), bound.clone())))
                } else {
                   expr.clone()
                }

            }
            Expr::Literal(value) => {
                // If the expression is a literal, we can parameterize it
                params.collect_parameter(value.clone())
            }
            _ => expr,
        }
    }

    fn best_expr(
        &self,
        catalog: &Catalog,
        memo: &mut Memo,
        group_id: GroupId,
        req: &ReqProps,
    ) -> Arc<Best> {
        // 1. Check memoized best for this (group, req)
        if let Some(best) = memo.groups[group_id].best_plans.get(req) {
            return best.clone();
        }

        let logical_plan = memo.groups[group_id].logical[0].clone();

        // 2. Compute required child properties
        let child_reqs = required_props_for_children(logical_plan.as_ref(), req);

        // 3. Add children to memo and compute their bests bottom-up
        let child_nodes = logical_plan.children();
        let mut child_bests: Vec<Arc<Best>> = Vec::with_capacity(child_nodes.len());
        for child_node in child_nodes.into_iter() {
            let child_group = memo.add_group(child_node);
            let child_best = self.best_expr(catalog, memo, child_group, &child_reqs);
            child_bests.push(child_best);
        }

        // 4. Implement this node directly into physical alternatives
        let mut candidates = self.implement_node(catalog, logical_plan.as_ref(), &child_bests, req);

        // 5. Select best candidate based on cost
        let mut best: Option<Candidate> = None;
        for candidate in candidates.drain(..) {
            if best.as_ref().map_or(true, |b| candidate.cost < b.cost) {
                best = Some(candidate);
            }
        }

        let best = best.expect("Failed to produce a physical plan");
        let best = Arc::new(Best { plan: best.plan, cost: best.cost, provides: best.provides });

        // 6. Cache and return
        memo.groups[group_id]
            .best_plans
            .insert(req.clone(), best.clone());
        best
    }

    fn implement_node(
        &self,
        catalog: &Catalog,
        node: &LogicalPlan,
        child_bests: &[Arc<Best>],
        req: &ReqProps,
    ) -> Vec<Candidate> {

        match node {
            LogicalPlan::CollectionScan { collection, filter, .. } => {
                assert!(child_bests.is_empty());
                self.implement_collection_scan_node(catalog, *collection, filter, req)
            }
            LogicalPlan::Filter { condition, .. } => {
                assert_eq!(child_bests.len(), 1);
                self.implement_filter_node(&child_bests, condition)
            }
            LogicalPlan::Projection { projection, .. } => {
                assert_eq!(child_bests.len(), 1);
                self.implement_projection_node(&child_bests, projection)
            }
            LogicalPlan::Sort { sort_fields, .. } => {
                assert_eq!(child_bests.len(), 1);
                self.implement_sort_node(&child_bests, sort_fields, req)
            }
            LogicalPlan::Limit { limit, .. } => {
                assert_eq!(child_bests.len(), 1);
                self.implement_limit_node(&child_bests, limit)
            }
            _ => panic!("Unsupported logical plan node in implementation: {:?}", node),
        }
    }

    fn implement_filter_node(&self, child_bests: &[Arc<Best>], condition: &Arc<Expr>) -> Vec<Candidate> {
        let child = &child_bests[0];
        let plan = Arc::new(PhysicalPlan::Filter { input: child.plan.clone(), predicate: condition.clone() });
        vec![Candidate {
            provides: child.provides.clone(),
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
            plan,
        }]
    }

    fn implement_projection_node(&self, child_bests: &[Arc<Best>], projection: &Arc<Projection>) -> Vec<Candidate> {
        let child = &child_bests[0];
        let plan = Arc::new(PhysicalPlan::Projection { input: child.plan.clone(), projection: projection.clone() });
        vec![Candidate {
            provides: child.provides.clone(),
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
            plan,
        }]
    }

    fn implement_limit_node(&self, child_bests: &[Arc<Best>], limit: &Limit) -> Vec<Candidate> {
        let child = &child_bests[0];

        if let Some(provided_limit) = &child.provides.limit {
            let provided_skip = provided_limit.skip.unwrap_or(0);
            let requested_skip = limit.skip.unwrap_or(0);

            let limit_is_sufficient = match (provided_limit.limit, limit.limit) {
                (Some(p), Some(r)) => p <= r,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => true,
            };

            if provided_skip == requested_skip && limit_is_sufficient {
                return vec![Candidate {
                    provides: child.provides.clone(),
                    cost: child.cost,
                    plan: child.plan.clone(),
                }];
            }
        }

        let plan = Arc::new(PhysicalPlan::Limit {
            input: child.plan.clone(),
            skip: limit.skip,
            limit: limit.limit,
        });
        vec![Candidate {
            provides: Provides {
                order: child.provides.order.clone(),
                limit: Some(limit.clone()),
            },
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
            plan,
        }]
    }

    fn implement_collection_scan_node(
        &self,
        catalog: &Catalog,
        collection: u32,
        filter: &Option<Arc<Expr>>,
        req: &ReqProps,
    ) -> Vec<Candidate> {

        let primary_key = Expr::Field(vec!["_id".into()]);

        let expr = if let Some(expr) = filter {
            expr
        } else {
            // If there are no filters, it means that all documents need to be read
            return vec![self.create_pk_range_scan_candidate(
                collection,
                primary_key,
                Interval::all(),
                None,
                req
            )]
        };

        // Flatten the expression into a list of conjuncts
        let mut conjuncts = Vec::new();
        if let Expr::And(conditions) = expr.as_ref() {
            for cond in conditions {
                conjuncts.push(cond.as_ref());
            }
        } else {
            conjuncts.push(expr);
        }

        // Map field names to their corresponding filter expressions
        let field_filters_map: HashMap<&Expr, &Expr> = conjuncts
            .iter()
            .filter_map(|e| {
                if let Expr::FieldFilters { field, .. } = e {
                    Some((field.as_ref(), *e))
                } else {
                    None
                }
            })
            .collect();

        let mut candidates = Vec::new();

        // Handle primary key separately
        if let Some(Expr::FieldFilters { field: _field, filters }) =
            field_filters_map.get(&primary_key).map(|e| *e)
        {
            // After normalization, we know that:
            // * if the field filters exists it must contain at least one filter
            // * if there is a sargable filter it will be the first filter
            assert!(!filters.is_empty(), "There should be at least one filter");
            
            let first_filter = filters.first().unwrap();
            
            if is_sargable(first_filter) {

                let sargable = first_filter;
                let residual = compute_residual_filter(expr.clone(), &primary_key);

                match sargable.as_ref() {
                    Expr::Comparison { operator, value } => match operator {
                        ComparisonOperator::In => {
                            let residual = compute_residual_filter(expr.clone(), &primary_key);
                            let candidate = self.create_multipoint_search_candidate(
                                collection,
                                value.clone(),
                                residual,
                                req,
                            );
                            // This is likely the best plan, so we can just return it.
                            return vec![candidate];
                        }
                        _ => unreachable!(), // Should be filtered by is_sargable_leaf
                    },
                    Expr::Interval(interval) => {
                        if interval.is_point() {
                            // Point search
                            let key = interval.start_bound_value().unwrap().clone();
                            let candidate = self.create_point_search_candidate(
                                collection,
                                key,
                                residual
                            );
                            // If we can answer to the query with a point search, we know that it is
                            // the fastest that we can use. So, instead of exploring other alternative
                            // we can directly return it
                            return vec![candidate];
                        } else {
                            // Range scan
                            let candidate = self.create_pk_range_scan_candidate(
                                collection,
                                primary_key,
                                interval.clone(),
                                residual,
                                req
                            );
                            candidates.push(candidate);
                        }
                    },
                    Expr::Exists(true) => {
                        let candidate = self.create_pk_range_scan_candidate(
                            collection,
                            primary_key,
                            Interval::all(),
                            residual,
                            req
                        );
                        candidates.push(candidate);
                    },
                    _ => unreachable!(), // Should be filtered by is_sargable_leaf
                }
            }
        } else {
            // If there are no restriction on the primary key then a full scan + raw filtering is a candidate.
            candidates.push(self.create_pk_range_scan_candidate(
                collection,
                primary_key,
                Interval::all(),
                filter.clone(),
                req
            ))
        }

        let metadata = catalog.get_collection_by_id(&collection)
            .expect("The collection should exists");

        // Handle indexes
        for _index in metadata.indexes.values() {

            todo!("Implement the logic for index candidates")

            // let mut matched_prefix_len = 0;
            // for (field_name, _) in &index.fields {
            //     if field_filters_map.contains_key(field_name) {
            //         matched_prefix_len += 1;
            //     } else {
            //         break;
            //     }
            // }
            //
            // if matched_prefix_len > 0 {
            //     for i in 0..matched_prefix_len {
            //         let (field_name, _) = &index.fields[i];
            //         if let Some(Expr::FieldFilters { field, filters }) =
            //             field_filters_map.get(field_name).map(|e| *e)
            //         {
            //
            //         }
            //     }
            // }
        }
        candidates
    }

    fn create_point_search_candidate(&self,
                                     collection: u32,
                                     key: Arc<Expr>,
                                     residual: Option<Arc<Expr>>
    ) -> Candidate {

        let plan = Arc::new(PhysicalPlan::PointSearch {
            collection,
            key,
            filter: residual,
            projection: None,
        });

        let provides = Provides {
            order: None,
            limit: Some(Limit { skip: None, limit: Some(1) })
        };

        Candidate {
            provides,
            cost: self.cost_estimator.estimate_node_cost(&plan),
            plan,
        }
    }

    fn create_pk_range_scan_candidate(&self,
                                      collection: u32,
                                      primary_key: Expr,
                                      interval: Interval<Arc<Expr>>,
                                      residual: Option<Arc<Expr>>,
                                      req: &ReqProps
    ) -> Candidate {

        let (direction, sort_field) = if matches!(req.order.as_deref().and_then(|fields| fields.first()),
            Some(sort_field) if sort_field.field.as_ref() == &primary_key && sort_field.order == SortOrder::Descending
                            ) {
            (Direction::Reverse, SortField::desc(Arc::new(primary_key)))
        } else {
            (Direction::Forward, SortField::asc(Arc::new(primary_key)))
        };

        let plan = Arc::new(PhysicalPlan::CollectionScan {
            collection,
            range: interval,
            direction,
            filter: residual,
            projection: None,
        });

        let provides = Provides {
            order: Some(vec![sort_field]),
            limit: None,
        };

        let candidate = Candidate {
            provides,
            cost: self.cost_estimator.estimate_node_cost(&plan),
            plan,
        };
        candidate
    }

    fn create_multipoint_search_candidate(
        &self,
        collection: u32,
        keys: Arc<Expr>,
        residual: Option<Arc<Expr>>,
        req: &ReqProps,
    ) -> Candidate {
        let primary_key = Expr::Field(vec!["_id".into()]);

        let (direction, sort_field) = if matches!(req.order.as_deref().and_then(|fields| fields.first()),
            Some(sort_field) if sort_field.field.as_ref() == &primary_key && sort_field.order == SortOrder::Descending
                            ) {
            (Direction::Reverse, Some(SortField::desc(Arc::new(primary_key))))
        } else {
            (Direction::Forward, Some(SortField::asc(Arc::new(primary_key))))
        };

        let plan = Arc::new(PhysicalPlan::MultiPointSearch {
            collection,
            keys,
            direction,
            filter: residual,
            projection: None,
        });

        let provides = Provides {
            order: sort_field.map(|sf| vec![sf]),
            limit: None,
        };

        Candidate {
            provides,
            cost: self.cost_estimator.estimate_node_cost(&plan),
            plan,
        }
    }

    fn implement_sort_node(&self,
                           child_bests: &[Arc<Best>],
                           sort_fields: &Arc<Vec<SortField>>,
                           req: &ReqProps,
    ) -> Vec<Candidate> {
        // TODO: the parameter should be exposed at the Options levels as a number of bytes and
        // we should use cardinality information to deduct the number of rows.
        const MAX_IN_MEMORY_ROWS: usize = 5000;

        let child = &child_bests[0];
        // If the child is returning data already sorted in the correct order or a single result
        // we can simply return the child.
        if Some(sort_fields.as_ref()) == child.provides.order.as_ref()
            || Some(Limit { skip: None, limit: Some(1)}) == child.provides.limit {

            return vec![Candidate {
                provides: child.provides.clone(),
                cost: child.cost,
                plan: child.plan.clone(),
            }]
        }

        let mut candidates = Vec::new();

        // If a limit is requested we can use a Top K heap sort.
        if let Some(limit) = &req.limit {
            if let Some(count) = limit.limit {
                let k = limit.skip.unwrap_or(0) + count;

                if k < MAX_IN_MEMORY_ROWS {
                    let plan = Arc::new(PhysicalPlan::TopKHeapSort {
                        input: child.plan.clone(),
                        sort_fields: sort_fields.clone(),
                        k,
                    });

                    let provides = Provides {
                        order: Some(sort_fields.as_ref().clone()),
                        limit: Some(Limit { skip: None, limit: Some(k) }),
                    };

                    let cost = child.cost + self.cost_estimator.estimate_node_cost(&plan);

                    candidates.push(Candidate { plan, provides, cost, })
                }
            }
        }

        let plan = Arc::new(PhysicalPlan::ExternalMergeSort {
            input: child.plan.clone(),
            sort_fields: sort_fields.clone(),
            max_in_memory_rows: MAX_IN_MEMORY_ROWS,
        });

        let provides = Provides {
            order: Some(sort_fields.as_ref().clone()),
            limit: child.provides.limit.clone(),
        };

        candidates.push(Candidate {
            provides,
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan),
            plan,
        });

        candidates
    }
}


fn compute_residual_filter(original: Arc<Expr>, used_field: &Expr) -> Option<Arc<Expr>> {
    let filters = match original.as_ref() {
        Expr::And(filters) => filters,
        Expr::FieldFilters { .. } => &vec![original],
        _ => unreachable!("Unexpected expression: {:?}", original)
    };

    let new_filters: Vec<Arc<Expr>> = filters
        .iter()
        .filter_map(|filter| {
            if let Expr::FieldFilters { field, filters } = filter.as_ref() {
                if field.as_ref() == used_field {
                    return if filters.len() > 1 {
                        // Keep the residual filters for the used field.
                        Some(Arc::new(Expr::FieldFilters {
                            field: field.clone(),
                            filters: filters.iter().skip(1).cloned().collect(),
                        }))
                    } else {
                        // The only filter was used, so this part is gone.
                        None
                    }
                }
            }
            Some(filter.clone())
        })
        .collect();

    match new_filters.len() {
        0 => None,
        1 => new_filters.into_iter().next(),
        _ => Some(Arc::new(Expr::And(new_filters)))
    }
}

/// Returns true if an expression is sargable (can be used with an index).
fn is_sargable(expr: &Arc<Expr>) -> bool {
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

fn required_props_for_children(node: &LogicalPlan, req: &ReqProps) -> ReqProps {
    match node {
        LogicalPlan::Sort { sort_fields, .. } => ReqProps {
            order: Some((**sort_fields).clone()),
            limit: req.limit.clone(),
        },
        LogicalPlan::Limit { limit, .. } => ReqProps {
            order: req.order.clone(),
            limit: Some(limit.clone()),
        },
        _ => req.clone(),
    }
}

#[cfg(test)]
mod parametrize_test {
    use crate::obs::logger::test_instance;
    use super::*;
    use crate::query::expr_fn::{and, eq, exists, field, field_filters, interval, lit, placeholder};
    use crate::query::logical_plan::LogicalPlanBuilder;
    use crate::query::BsonValue;

    #[test]
    fn test_parametrize_simple_filter() {
        let optimizer = Optimizer::new(test_instance());
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection)
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build();

        // Let's first check when the filter stay at the filter level
        let (parametrized_plan, params) = optimizer.parametrize(plan.clone());

        // Check params
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0), &BsonValue::from(10));

        // Check plan
        let expected_filter = field_filters(field(["a"]), vec![eq(placeholder(0))]);
        let expected_plan = LogicalPlanBuilder::scan(collection)
            .filter(expected_filter)
            .build();

        assert_eq!(parametrized_plan, expected_plan);

        // Let's check when the filter is pushed down at the collection scan level
        let plan = optimizer.normalize(plan.clone());

        // Let's first check when the filter stay at the filter level
        let (parametrized_plan, params) = optimizer.parametrize(plan.clone());

        // Check params
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0), &BsonValue::from(10));

        // Check plan
        let expected_filter = field_filters(
            field(["a"]),
            vec![interval(Interval::closed(placeholder(0), placeholder(0)))]
        );
        let expected_plan = LogicalPlanBuilder::scan_with_filters_and_projections(
            collection,
            Some(expected_filter),
            None,
            None,
        ).build();
        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_complex_filter() {
        let optimizer = Optimizer::new(test_instance());
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
            field_filters(field(["a"]), vec![eq(placeholder(0))]),
            field_filters(field(["b"]), vec![eq(placeholder(1))]),
        ]);

        let expected_plan = LogicalPlanBuilder::scan(collection)
            .filter(expected_condition)
            .build();

        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_no_literals() {
        let optimizer = Optimizer::new(test_instance());
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection)
            .filter(field_filters(field(["a"]), vec![exists(true)]))
            .build();
        let plan_clone = plan.clone();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 0);

        // Check plan
        assert_eq!(parametrized_plan, plan_clone);
    }
}

#[cfg(test)]
mod optimizer_tests {
    use crate::obs::logger::test_instance;
    use super::*;
    use crate::query::expr_fn::{and, eq, exists, field, field_filters, gt, include, interval, lit, placeholder, proj_field, proj_fields, within};
    use crate::query::logical_plan::LogicalPlanBuilder;
    use bson::Bson;

    const COLLECTION: u32 = 10;

    #[test]
    fn test_optimize_noop() {
        check_optimization(Arc::new(LogicalPlan::NoOp), PhysicalPlan::NoOp);
    }

    #[test]
    fn test_optimize_collection_scan_no_filter() {
        let input = LogicalPlanBuilder::scan(COLLECTION).build();
        let output = full_scan_plan();

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_scan_with_non_pk_filter() {
        let filters = field_filters(field(["a"]), vec![gt(lit(10))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .filter(filters.clone())
            .build();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: Some(field_filters(field(["a"]), [interval(Interval::greater_than(placeholder(0)))])),
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_projection() {
        let projection = include(proj_fields([("name", proj_field())]));
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .project(projection.clone())
            .build();

        let output =  PhysicalPlan::Projection {
            input: Arc::new(full_scan_plan()),
            projection,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_limit() {
        let input = LogicalPlanBuilder::scan(COLLECTION).limit(Some(10), Some(20)).build();
        let output = PhysicalPlan::Limit {
            input: Arc::new(full_scan_plan()),
            skip: Some(10),
            limit: Some(20),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_point_search() {
        let filters = field_filters(field(["_id"]), vec![eq(lit(123))]);
        let input = LogicalPlanBuilder::scan(COLLECTION).filter(filters).build();

        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_range_scan() {
        let filters = field_filters(field(["_id"]), vec![gt(lit(123))]);
        let input = LogicalPlanBuilder::scan(COLLECTION).filter(filters).build();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(placeholder(0)),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_range_scan_with_residual() {
        let filters = and(vec![
            field_filters(field(["_id"]), vec![gt(lit(123))]),
            field_filters(field(["a"]), vec![gt(lit(10))]),
        ]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .filter(filters)
            .build();

        // The residual filter will also be normalized and parametrized.
        let residual_filter = field_filters(
            field(["a"]),
            [interval(Interval::greater_than(placeholder(1)))],
        );

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(placeholder(0)),
            direction: Direction::Forward,
            filter: Some(residual_filter),
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_pk_exists_true_is_full_scan() {
        let filters = field_filters(field(["_id"]), vec![exists(true)]);
        let input = LogicalPlanBuilder::scan(COLLECTION).filter(filters).build();
        let output = full_scan_plan();
        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_sort_elimination_pk_asc() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["_id"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .sort(sort_fields)
            .build();

        // Sort is eliminated because collection scan provides data sorted by _id asc.
        let output = full_scan_plan();

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_sort_elimination_pk_desc() {
        let sort_fields = Arc::new(vec![SortField::desc(field(["_id"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .sort(sort_fields)
            .build();

        // Sort is eliminated, and scan direction is reversed.
        let mut output = full_scan_plan();
        if let PhysicalPlan::CollectionScan { direction, .. } = &mut output {
            *direction = Direction::Reverse;
        }

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_sort_elimination_point_search() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .filter(field_filters(field(["_id"]), vec![eq(lit(123))]))
            .sort(sort_fields)
            .build();

        // Sort is eliminated because point search returns at most one row.
        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_topk_heap_sort() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .sort(sort_fields.clone())
            .limit(Some(5), Some(10))
            .build();

        let topk_sort = PhysicalPlan::TopKHeapSort {
            input: Arc::new(full_scan_plan()),
            sort_fields,
            k: 15,
        };

        let output = PhysicalPlan::Limit {
            input: Arc::new(topk_sort),
            skip: Some(5),
            limit: Some(10),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_external_merge_sort() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .sort(sort_fields.clone())
            .build();

        let output = PhysicalPlan::ExternalMergeSort {
            input: Arc::new(full_scan_plan()),
            sort_fields,
            max_in_memory_rows: 5000,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_limit_elimination() {
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .filter(field_filters(field(["_id"]), vec![eq(lit(123))]))
            .limit(None, Some(10))
            .build();

        // Limit is eliminated because point search returns at most one row, which
        // satisfies the limit of 10.
        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_range_scan_with_sort_and_limit() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .filter(field_filters(field(["_id"]), vec![gt(lit(123))]))
            .sort(sort_fields.clone())
            .limit(Some(5), Some(10))
            .build();

        let scan = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(placeholder(0)),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        let topk_sort = PhysicalPlan::TopKHeapSort {
            input: Arc::new(scan),
            sort_fields,
            k: 15,
        };

        let output = PhysicalPlan::Limit {
            input: Arc::new(topk_sort),
            skip: Some(5),
            limit: Some(10),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_limit_elimination_with_heapsort() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .sort(sort_fields.clone())
            .limit(None, Some(10))
            .build();

        let output = PhysicalPlan::TopKHeapSort {
            input: Arc::new(full_scan_plan()),
            sort_fields,
            k: 10,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_multipoint_search() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let filters = field_filters(field(["_id"]), vec![within(lit(values))]);
        let input = LogicalPlanBuilder::scan(COLLECTION).filter(filters).build();

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: placeholder(0),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_multipoint_search_desc() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let filters = field_filters(field(["_id"]), vec![within(lit(values))]);
        let sort_fields = Arc::new(vec![SortField::desc(field(["_id"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION)
            .filter(filters)
            .sort(sort_fields)
            .build();

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: placeholder(0),
            direction: Direction::Reverse,
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_multipoint_search_with_residual() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let filters = and(vec![
            field_filters(field(["_id"]), vec![within(lit(values))]),
            field_filters(field(["a"]), vec![gt(lit(10))]),
        ]);
        let input = LogicalPlanBuilder::scan(COLLECTION).filter(filters).build();

        let residual_filter = field_filters(
            field(["a"]),
            [interval(Interval::greater_than(placeholder(1)))],
        );

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: placeholder(0),
            direction: Direction::Forward,
            filter: Some(residual_filter),
            projection: None,
        };

        check_optimization(input, output);
    }

    fn check_optimization(input: Arc<LogicalPlan>, output:PhysicalPlan) {

        let optimizer = Optimizer::new(test_instance());
        // First, normalize the logical plan
        let normalized_plan = optimizer.normalize(input);
        // Then, parametrize the plan to collect parameters
        let (logical_plan, _parameters) = optimizer.parametrize(normalized_plan);
        let catalog = test_catalog();
        let physical_plan = optimizer.optimize(logical_plan, catalog);
        assert_eq!(physical_plan.as_ref(), &output)
    }

    fn full_scan_plan() -> PhysicalPlan {
        PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        }
    }

    fn test_catalog() -> Arc<Catalog> {
        let mut catalog = Catalog::new();
        catalog = catalog.add_collection("test", COLLECTION, 2);
        Arc::new(catalog)
    }
}
