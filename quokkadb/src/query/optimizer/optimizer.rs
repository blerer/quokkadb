use crate::error::{Error, Result};
use crate::io::bitset::BitSet;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::options::options::Options;
use crate::query::expr::Expr;
use crate::query::logical_plan::{LogicalPlan, transform_down_filter};
use crate::query::optimizer::normalization_rules;
use crate::query::optimizer::normalization_rules::NormalisationRule;
use crate::query::physical_plan::{IndexScanRangeExpr, PhysicalPlan};
use crate::query::tree_node::TreeNode;
use crate::query::{
    ComparisonOperator, Interval, Limit, Parameters, Projection, SortField, SortOrder,
};
use crate::storage::Direction;
use crate::storage::catalog::{
    CollectionMetadata, IndexDefinition, IndexDirection, OrderedIndexField,
};
use crate::storage::count_stats::{CountStatSource, CountStatsKey};
use std::collections::HashMap;
use std::convert::Into;
use std::sync::Arc;
use std::time::Instant;
use tracing::trace_span;

pub type Cost = f64;

pub struct CostEstimator {}

impl CostEstimator {
    /// Estimates the cost of a single physical plan node, without considering its inputs.
    pub fn estimate_node_cost(
        &self,
        plan: &PhysicalPlan,
        count_stats: &dyn CountStatSource,
    ) -> Cost {
        match plan {
            PhysicalPlan::CollectionScan { collection, .. } => count_stats
                .count_stat(&CountStatsKey::Collection(*collection))
                .map(|count| count.max(1) as f64)
                .unwrap_or(1000.0),
            PhysicalPlan::IndexScan {
                collection,
                index,
                range,
                filter,
                ..
            } => {
                if let Some(index_count) = count_stats.count_stat(&CountStatsKey::Index {
                    collection: *collection,
                    index: *index,
                }) {
                    let mut cost = index_count.max(1) as f64;

                    cost /= 1.0 + range.equal_prefix.len() as f64;
                    if range.tail.is_some() {
                        cost *= 0.75;
                    }
                    if filter.is_some() {
                        cost *= 1.1;
                    }

                    cost.max(1.0)
                } else {
                    let mut cost = 100.0;

                    cost -= range.equal_prefix.len() as f64 * 20.0;
                    if range.tail.is_some() {
                        cost -= 10.0;
                    }
                    if filter.is_some() {
                        cost += 5.0;
                    }

                    cost.max(10.0)
                }
            }
            PhysicalPlan::PointSearch { .. } => 1.0,
            PhysicalPlan::MultiPointSearch { .. } => 10.0,
            PhysicalPlan::Filter { .. } => 1.0,
            PhysicalPlan::Projection { .. } => 0.5,
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
    sort_memory_limit: usize,
}

type GroupId = usize;

#[derive(PartialEq, Eq, Hash, Clone)]
enum LogicalPlanKey {
    NoOp,
    CollectionScan {
        collection: u32,
        hint: Option<u32>,
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
                hint,
                projection,
                filter,
                sort,
            } => {
                assert!(child_groups.is_empty());
                LogicalPlanKey::CollectionScan {
                    collection: *collection,
                    hint: hint.clone(),
                    projection: projection.as_ref().map(|p| {
                        let mut writer = ByteWriter::new();
                        p.write_to(&mut writer, 1);
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
                projection.write_to(&mut writer, 1);
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
            _ => panic!(
                "Unsupported logical plan node in key generation: {:?}",
                plan
            ),
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
    pub fn new(options: &Options) -> Self {
        Self {
            normalization_rules: Arc::new(normalization_rules::all_normalization_rules()),
            cost_estimator: CostEstimator {},
            sort_memory_limit: options.sort_memory_limit().to_bytes(),
        }
    }

    pub fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        metadata: &CollectionMetadata,
        count_stats: &dyn CountStatSource,
    ) -> Result<Arc<PhysicalPlan>> {
        let _span = trace_span!("optimization").entered();
        let start = Instant::now();

        if matches!(plan.as_ref(), &LogicalPlan::NoOp) {
            return Ok(Arc::new(PhysicalPlan::NoOp));
        }

        let mut memo = Memo::new();
        let root_group_id = memo.add_group(plan);

        let required_props = ReqProps::default(); // No required properties for the root

        let physical_plan = self
            .best_expr(
                metadata,
                count_stats,
                &mut memo,
                root_group_id,
                &required_props,
            )?
            .plan
            .clone();

        let duration = start.elapsed();
        tracing::trace!(
            duration_micros = duration.as_micros(),
            physical_plan = ?physical_plan,
            "optimization finished"
        );

        Ok(physical_plan)
    }

    pub fn normalize(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        let _span = trace_span!("normalization").entered();
        let start = Instant::now();

        let logical_plan = self.normalization_rules.apply(plan);

        let duration = start.elapsed();
        tracing::trace!(
            duration_micros = duration.as_micros(),
            normalized_plan = ?logical_plan,
            "normalization finished"
        );
        logical_plan
    }

    pub fn parametrize(&self, plan: Arc<LogicalPlan>) -> (Arc<LogicalPlan>, Parameters) {
        use std::cell::RefCell;
        let _span = trace_span!("parameterization").entered();

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
                    let bound = match bound.as_ref() {
                        Expr::Literal(value) => params.collect_parameter(value.clone()),
                        Expr::Placeholder { .. } => bound.clone(),
                        _ => unreachable!("Interval bound should be a value expression"),
                    };
                    Arc::new(Expr::Interval(Interval::closed(
                        bound.clone(),
                        bound.clone(),
                    )))
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
        metadata: &CollectionMetadata,
        count_stats: &dyn CountStatSource,
        memo: &mut Memo,
        group_id: GroupId,
        req: &ReqProps,
    ) -> Result<Arc<Best>> {
        // 1. Check memoized best for this (group, req)
        if let Some(best) = memo.groups[group_id].best_plans.get(req) {
            return Ok(best.clone());
        }

        let logical_plan = memo.groups[group_id].logical[0].clone();

        // 2. Compute required child properties
        let child_reqs = required_props_for_children(logical_plan.as_ref(), req);

        // 3. Add children to memo and compute their bests bottom-up
        let child_nodes = logical_plan.children();
        let mut child_bests: Vec<Arc<Best>> = Vec::with_capacity(child_nodes.len());
        for child_node in child_nodes.into_iter() {
            let child_group = memo.add_group(child_node);
            let child_best =
                self.best_expr(metadata, count_stats, memo, child_group, &child_reqs)?;
            child_bests.push(child_best);
        }

        // 4. Implement this node directly into physical alternatives
        let mut candidates = self.implement_node(
            metadata,
            count_stats,
            logical_plan.as_ref(),
            &child_bests,
            req,
        )?;

        // 5. Select best candidate based on cost
        let mut best: Option<Candidate> = None;
        for candidate in candidates.drain(..) {
            if best.as_ref().map_or(true, |b| candidate.cost < b.cost) {
                best = Some(candidate);
            }
        }

        let best = best.ok_or_else(|| {
            Error::InvalidRequest("The requested index cannot be used for this query".to_string())
        })?;
        let best = Arc::new(Best {
            plan: best.plan,
            cost: best.cost,
            provides: best.provides,
        });

        // 6. Cache and return
        memo.groups[group_id]
            .best_plans
            .insert(req.clone(), best.clone());
        Ok(best)
    }

    fn implement_node(
        &self,
        metadata: &CollectionMetadata,
        count_stats: &dyn CountStatSource,
        node: &LogicalPlan,
        child_bests: &[Arc<Best>],
        req: &ReqProps,
    ) -> Result<Vec<Candidate>> {
        match node {
            LogicalPlan::CollectionScan {
                collection,
                hint,
                filter,
                ..
            } => {
                assert!(child_bests.is_empty());
                assert_eq!(
                    collection, &metadata.id,
                    "Scan collection must match optimizer metadata"
                );
                self.implement_collection_scan_node(metadata, count_stats, *hint, filter, req)
            }
            LogicalPlan::Filter { condition, .. } => {
                assert_eq!(child_bests.len(), 1);
                Ok(self.implement_filter_node(count_stats, &child_bests, condition))
            }
            LogicalPlan::Projection { projection, .. } => {
                assert_eq!(child_bests.len(), 1);
                Ok(self.implement_projection_node(count_stats, &child_bests, projection))
            }
            LogicalPlan::Sort { sort_fields, .. } => {
                assert_eq!(child_bests.len(), 1);
                Ok(
                    self.implement_sort_node(
                        &metadata,
                        count_stats,
                        &child_bests,
                        sort_fields,
                        req,
                    ),
                )
            }
            LogicalPlan::Limit { limit, .. } => {
                assert_eq!(child_bests.len(), 1);
                Ok(self.implement_limit_node(count_stats, &child_bests, limit))
            }
            _ => panic!(
                "Unsupported logical plan node in implementation: {:?}",
                node
            ),
        }
    }

    fn implement_filter_node(
        &self,
        count_stats: &dyn CountStatSource,
        child_bests: &[Arc<Best>],
        condition: &Arc<Expr>,
    ) -> Vec<Candidate> {
        let child = &child_bests[0];
        let plan = Arc::new(PhysicalPlan::Filter {
            input: child.plan.clone(),
            predicate: condition.clone(),
        });
        vec![Candidate {
            provides: child.provides.clone(),
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        }]
    }

    fn implement_projection_node(
        &self,
        count_stats: &dyn CountStatSource,
        child_bests: &[Arc<Best>],
        projection: &Arc<Projection>,
    ) -> Vec<Candidate> {
        let child = &child_bests[0];
        let plan = Arc::new(PhysicalPlan::Projection {
            input: child.plan.clone(),
            projection: projection.clone(),
        });
        vec![Candidate {
            provides: child.provides.clone(),
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        }]
    }

    fn implement_limit_node(
        &self,
        count_stats: &dyn CountStatSource,
        child_bests: &[Arc<Best>],
        limit: &Limit,
    ) -> Vec<Candidate> {
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
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        }]
    }

    fn implement_collection_scan_node(
        &self,
        metadata: &CollectionMetadata,
        count_stats: &dyn CountStatSource,
        hint: Option<u32>,
        filter: &Option<Arc<Expr>>,
        req: &ReqProps,
    ) -> Result<Vec<Candidate>> {
        let hinted_index = Self::get_hint_info(metadata, hint)?;

        let primary_key = Expr::Field(vec!["_id".into()]);
        let mut candidates = if hint.is_some_and(|index_id| index_id != 0) {
            Vec::new()
        } else {
            vec![self.create_pk_range_scan_candidate(
                count_stats,
                metadata.id,
                primary_key.clone(),
                Interval::all(),
                filter.clone(),
                req,
            )]
        };

        if let Some(expr) = filter {
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

            // Handle primary-key lookups when no secondary index was explicitly requested.
            if hint.is_none() || hint == Some(0) {
                if let Some(Expr::FieldFilters {
                    field: _field,
                    filters,
                }) = field_filters_map.get(&primary_key).map(|e| *e)
                {
                    assert!(!filters.is_empty(), "There should be at least one filter");

                    if let Some((position, sargable)) = filters
                        .iter()
                        .enumerate()
                        .filter(|(_, filter)| is_sargable(filter))
                        .min_by_key(|(_, filter)| match filter.as_ref() {
                            Expr::Interval(interval) if interval.is_point() => 0,
                            Expr::Comparison {
                                operator: ComparisonOperator::In,
                                ..
                            } => 1,
                            Expr::Interval(_) => 2,
                            _ => 3,
                        })
                    {
                        let used_predicates = [(Arc::new(primary_key.clone()), position)];
                        let residual = compute_residual_filter(expr.clone(), &used_predicates);

                        match sargable.as_ref() {
                            Expr::Comparison {
                                operator: ComparisonOperator::In,
                                value,
                            } => {
                                let candidate = self.create_multipoint_search_candidate(
                                    count_stats,
                                    metadata.id,
                                    value.clone(),
                                    residual,
                                    req,
                                );
                                // This is likely the best plan, so we can just return it.
                                return Ok(vec![candidate]);
                            }
                            Expr::Interval(interval) => {
                                if interval.is_point()
                                    && matches!(
                                        interval.start_bound_value().unwrap().as_ref(),
                                        Expr::Literal(_) | Expr::Placeholder { .. }
                                    )
                                {
                                    // Point search
                                    let key = interval.start_bound_value().unwrap().clone();
                                    let candidate = self.create_point_search_candidate(
                                        count_stats,
                                        metadata.id,
                                        key,
                                        residual,
                                    );
                                    // If we can answer to the query with a point search, we know
                                    // that it is the fastest that we can use. So, instead of
                                    // exploring other alternatives, we can return it directly.
                                    return Ok(vec![candidate]);
                                } else {
                                    // Range scan
                                    candidates[0] = self.create_pk_range_scan_candidate(
                                        count_stats,
                                        metadata.id,
                                        primary_key.clone(),
                                        interval.clone(),
                                        residual,
                                        req,
                                    );
                                }
                            }
                            Expr::Exists(true) => {
                                candidates[0] = self.create_pk_range_scan_candidate(
                                    count_stats,
                                    metadata.id,
                                    primary_key.clone(),
                                    Interval::all(),
                                    residual,
                                    req,
                                );
                            }
                            _ => unreachable!(), // Should be filtered by is_sargable_leaf
                        }
                    }
                }
            }

            self.add_index_scan_candidates(
                count_stats,
                metadata,
                filter,
                Some(&field_filters_map),
                hint,
                req,
                &mut candidates,
            );
        } else {
            self.add_index_scan_candidates(
                count_stats,
                metadata,
                filter,
                None,
                hint,
                req,
                &mut candidates,
            );
        }

        if let Some((index_name, collection_name)) = hinted_index {
            if candidates.is_empty() {
                return Err(Error::InvalidRequest(format!(
                    "Cannot use hinted index '{}' on collection '{}': no candidate scan can use it for this query",
                    index_name, collection_name
                )));
            }
        }

        Ok(candidates)
    }

    fn get_hint_info(
        metadata: &CollectionMetadata,
        hint: Option<u32>,
    ) -> Result<Option<(String, String)>> {
        let hinted_index = if let Some(index_id) = hint {
            if index_id == 0 {
                Some(("collection scan".to_string(), metadata.name.clone()))
            } else {
                let index =
                    metadata
                        .get_index_by_id(index_id)
                        .ok_or_else(|| Error::IndexNotFound {
                            collection_name: metadata.name.clone(),
                            index_name: format!("index id {index_id}"),
                            id: Some(index_id),
                        })?;

                if !index.is_queryable() {
                    return Err(Error::InvalidRequest(format!(
                        "Cannot use hinted index '{}' on collection '{}': the index is not queryable",
                        index.name(),
                        metadata.name
                    )));
                }

                Some((index.name().to_string(), metadata.name.clone()))
            }
        } else {
            None
        };
        Ok(hinted_index)
    }

    fn add_index_scan_candidates(
        &self,
        count_stats: &dyn CountStatSource,
        collection: &CollectionMetadata,
        filter: &Option<Arc<Expr>>,
        field_filters_map: Option<&HashMap<&Expr, &Expr>>,
        hint: Option<u32>,
        req: &ReqProps,
        candidates: &mut Vec<Candidate>,
    ) {
        for index in collection.queryable_indexes() {
            if hint.is_some_and(|hinted_index| hinted_index != index.id) {
                continue;
            }
            let is_hinted = hint.is_some();

            let index_fields = match &index.definition {
                IndexDefinition::Regular(fields) => fields,
            };

            let maybe_range = field_filters_map.and_then(|filters| {
                build_index_scan_range_expr(index_fields, index.multikey_fields, filters)
            });

            if let Some((range, used_predicates)) = maybe_range {
                let residual = filter.clone().and_then(|original_filter| {
                    compute_residual_filter(original_filter, &used_predicates)
                });

                candidates.push(self.create_index_scan_candidate(
                    count_stats,
                    collection.id,
                    index.id,
                    range,
                    residual,
                    req,
                    index_fields,
                    index.multikey_fields,
                ));
            } else if req.order.as_ref().is_some_and(|order| {
                index_scan_direction(index_fields, order.as_slice()).is_some()
                    && index_sort_prefix_is_scalar(index.multikey_fields, order.len())
            }) || hinted_index_can_use_unbounded_scan(
                is_hinted,
                filter,
                req,
                index_fields,
                field_filters_map,
            ) {
                candidates.push(self.create_index_scan_candidate(
                    count_stats,
                    collection.id,
                    index.id,
                    IndexScanRangeExpr {
                        equal_prefix: vec![],
                        tail: None,
                    },
                    filter.clone(),
                    req,
                    index_fields,
                    index.multikey_fields,
                ));
            }
        }
    }

    fn create_point_search_candidate(
        &self,
        count_stats: &dyn CountStatSource,
        collection: u32,
        key: Arc<Expr>,
        residual: Option<Arc<Expr>>,
    ) -> Candidate {
        let plan = Arc::new(PhysicalPlan::PointSearch {
            collection,
            key,
            filter: residual,
            projection: None,
        });

        let provides = Provides {
            order: None,
            limit: Some(Limit {
                skip: None,
                limit: Some(1),
            }),
        };

        Candidate {
            provides,
            cost: self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        }
    }

    fn create_pk_range_scan_candidate(
        &self,
        count_stats: &dyn CountStatSource,
        collection: u32,
        primary_key: Expr,
        interval: Interval<Arc<Expr>>,
        residual: Option<Arc<Expr>>,
        req: &ReqProps,
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
            cost: self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        };
        candidate
    }

    fn create_index_scan_candidate(
        &self,
        count_stats: &dyn CountStatSource,
        collection: u32,
        index: u32,
        range: IndexScanRangeExpr,
        residual: Option<Arc<Expr>>,
        req: &ReqProps,
        index_fields: &[OrderedIndexField],
        multikey_fields: BitSet,
    ) -> Candidate {
        let (direction, provides_order) = if let Some(req_order) = req.order.as_ref() {
            if let Some(direction) = index_scan_direction(index_fields, req_order.as_slice())
                && index_sort_prefix_is_scalar(multikey_fields, req_order.len())
            {
                (direction, Some(req_order.clone()))
            } else {
                (Direction::Forward, None)
            }
        } else {
            (Direction::Forward, None)
        };

        let plan = Arc::new(PhysicalPlan::IndexScan {
            collection,
            index,
            range,
            direction,
            filter: residual,
            projection: None,
        });

        Candidate {
            provides: Provides {
                order: provides_order,
                limit: None,
            },
            cost: self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        }
    }

    fn create_multipoint_search_candidate(
        &self,
        count_stats: &dyn CountStatSource,
        collection: u32,
        keys: Arc<Expr>,
        residual: Option<Arc<Expr>>,
        req: &ReqProps,
    ) -> Candidate {
        let primary_key = Expr::Field(vec!["_id".into()]);

        let (direction, sort_field) = if matches!(req.order.as_deref().and_then(|fields| fields.first()),
        Some(sort_field) if sort_field.field.as_ref() == &primary_key && sort_field.order == SortOrder::Descending
                        ) {
            (
                Direction::Reverse,
                Some(SortField::desc(Arc::new(primary_key))),
            )
        } else {
            (
                Direction::Forward,
                Some(SortField::asc(Arc::new(primary_key))),
            )
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
            cost: self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        }
    }

    fn implement_sort_node(
        &self,
        metadata: &CollectionMetadata,
        count_stats: &dyn CountStatSource,
        child_bests: &[Arc<Best>],
        sort_fields: &Arc<Vec<SortField>>,
        req: &ReqProps,
    ) -> Vec<Candidate> {
        let child = &child_bests[0];
        let estimated_row_bytes = self.estimate_sort_row_bytes(metadata, count_stats);
        let max_in_memory_rows = (self.sort_memory_limit / estimated_row_bytes).max(1);

        // If the child is returning data already sorted in the correct order or a single result
        // we can simply return the child.
        if Some(sort_fields.as_ref()) == child.provides.order.as_ref()
            || Some(Limit {
                skip: None,
                limit: Some(1),
            }) == child.provides.limit
        {
            return vec![Candidate {
                provides: child.provides.clone(),
                cost: child.cost,
                plan: child.plan.clone(),
            }];
        }

        let mut candidates = Vec::new();

        // If a limit is requested we can use a Top K heap sort.
        if let Some(limit) = &req.limit {
            if let Some(count) = limit.limit {
                let k = limit.skip.unwrap_or(0) + count;

                if k < max_in_memory_rows {
                    let plan = Arc::new(PhysicalPlan::TopKHeapSort {
                        input: child.plan.clone(),
                        sort_fields: sort_fields.clone(),
                        k,
                    });

                    let provides = Provides {
                        order: Some(sort_fields.as_ref().clone()),
                        limit: Some(Limit {
                            skip: None,
                            limit: Some(k),
                        }),
                    };

                    let cost =
                        child.cost + self.cost_estimator.estimate_node_cost(&plan, count_stats);

                    candidates.push(Candidate {
                        plan,
                        provides,
                        cost,
                    })
                }
            }
        }

        let plan = Arc::new(PhysicalPlan::ExternalMergeSort {
            input: child.plan.clone(),
            sort_fields: sort_fields.clone(),
            max_in_memory_rows,
        });

        let provides = Provides {
            order: Some(sort_fields.as_ref().clone()),
            limit: child.provides.limit.clone(),
        };

        candidates.push(Candidate {
            provides,
            cost: child.cost + self.cost_estimator.estimate_node_cost(&plan, count_stats),
            plan,
        });

        candidates
    }

    fn estimate_sort_row_bytes(
        &self,
        metadata: &CollectionMetadata,
        count_stats: &dyn CountStatSource,
    ) -> usize {
        const FALLBACK_ESTIMATED_ROW_BYTES: usize = 1024;
        const MIN_ESTIMATED_ROW_BYTES: usize = 256;

        let Some(count) = count_stats.count_stat(&CountStatsKey::Collection(metadata.id)) else {
            return FALLBACK_ESTIMATED_ROW_BYTES;
        };
        let Some(total_bytes) =
            count_stats.count_stat(&CountStatsKey::CollectionDocumentBytes(metadata.id))
        else {
            return FALLBACK_ESTIMATED_ROW_BYTES;
        };

        if count <= 0 || total_bytes <= 0 {
            return FALLBACK_ESTIMATED_ROW_BYTES;
        }

        let average_bytes = (total_bytes as u64) / (count as u64);
        let average_bytes = usize::try_from(average_bytes).unwrap_or(usize::MAX);
        average_bytes.saturating_mul(3).max(MIN_ESTIMATED_ROW_BYTES)
    }
}

/// Determines whether an explicitly hinted index can be considered without a
/// bounded key range.
///
/// A hinted index may be scanned in full when the query has neither a filter
/// nor an ordering requirement. It may also be scanned without bounds when a
/// predicate on an indexed field has an array-valued interval bound. Such a
/// bound is not safe for a secondary-index range because the index stores
/// expanded array elements while document filtering compares the complete
/// array; the original predicate therefore remains as a residual filter.
///
/// Unhinted indexes are never allowed through this fallback; unbounded scans
/// that provide the requested ordering are considered separately by the caller.
fn hinted_index_can_use_unbounded_scan(
    is_hinted: bool,
    filter: &Option<Arc<Expr>>,
    req: &ReqProps,
    index_fields: &[OrderedIndexField],
    field_filters_map: Option<&HashMap<&Expr, &Expr>>,
) -> bool {
    if !is_hinted {
        return false;
    }

    if filter.is_none() && req.order.is_none() {
        return true;
    }

    field_filters_map.is_some_and(|filters| index_has_array_bound_filter(index_fields, filters))
}

fn index_has_array_bound_filter(
    index_fields: &[OrderedIndexField],
    field_filters_map: &HashMap<&Expr, &Expr>,
) -> bool {
    index_fields.iter().any(|index_field| {
        let field = Expr::Field((&index_field.path).into());
        let Some(Expr::FieldFilters { filters, .. }) = field_filters_map.get(&field) else {
            return false;
        };

        filters.iter().any(|predicate| {
            matches!(predicate.as_ref(), Expr::Interval(interval) if interval_has_array_bound(interval))
        })
    })
}

fn interval_has_array_bound(interval: &Interval<Arc<Expr>>) -> bool {
    use std::ops::{Bound, RangeBounds};
    [interval.start_bound(), interval.end_bound()].into_iter().any(|bound| {
        matches!(bound, Bound::Included(value) | Bound::Excluded(value)
            if matches!(value.as_ref(), Expr::Placeholder { kind: crate::query::ParameterKind::Array, .. }))
    })
}

/// Builds the tightest safe range for the leading fields of a compound index.
///
/// Bounds must form a contiguous index prefix: point intervals extend the
/// equality prefix, while the first non-point interval becomes the tail range
/// and ends bound construction. Predicates that cannot be represented safely by
/// index keys, including array-valued bounds, remain available to the caller as
/// residual filters. Once one indexed field is known to be multikey, a second
/// multikey field is not bounded because independent predicates may match
/// different array elements without `$elemMatch` correlation.
///
/// The returned field positions identify predicates fully satisfied by the bounds
/// and removable from the residual filter. Multikey range predicates still need
/// document rechecking: an empty array's stored key may pass a scalar bound even
/// though the array has no matching element.
fn build_index_scan_range_expr(
    index_fields: &[OrderedIndexField],
    multikey_fields: BitSet,
    field_filters_map: &HashMap<&Expr, &Expr>,
) -> Option<(IndexScanRangeExpr, Vec<(Arc<Expr>, usize)>)> {
    let mut equal_prefix = Vec::new();
    let mut used_predicates = Vec::new();
    let mut tail = None;

    let mut used_multikey_field = false;
    for (field_position, index_field) in index_fields.iter().enumerate() {
        if multikey_fields.contains(field_position as u8) {
            // Independent predicates can match different array elements. Without
            // correlation evidence, bounding a second multikey field can miss
            // documents. Stop here to preserve a contiguous index prefix and
            // leave this field's predicates in the residual filter.
            if used_multikey_field {
                break;
            }
            used_multikey_field = true;
        }
        let field_expr = Arc::new(Expr::Field((&index_field.path).into()));
        let Some(Expr::FieldFilters { filters, .. }) =
            field_filters_map.get(field_expr.as_ref()).copied()
        else {
            break;
        };

        let Some((position, interval)) = select_index_interval(filters) else {
            break;
        };

        if interval.is_point()
            && matches!(
                interval.start_bound_value().unwrap().as_ref(),
                Expr::Literal(_) | Expr::Placeholder { .. }
            )
        {
            equal_prefix.push(interval.start_bound_value().unwrap().clone());
            used_predicates.push((field_expr, position));
        } else {
            tail = Some(interval.clone());
            if !multikey_fields.contains(field_position as u8) {
                used_predicates.push((field_expr, position));
            }
            break;
        }
    }

    if equal_prefix.is_empty() && tail.is_none() {
        None
    } else {
        Some((IndexScanRangeExpr { equal_prefix, tail }, used_predicates))
    }
}

/// Selects the safest interval that can be used for an index bound.
///
/// Array-valued bounds are excluded because a secondary index stores expanded
/// array elements, while the document filter may compare the complete array.
/// Among the remaining intervals, point intervals are preferred over ranges so
/// they can extend the compound index's equality prefix. The returned position
/// identifies the original filter for residual-filter bookkeeping.
fn select_index_interval(filters: &[Arc<Expr>]) -> Option<(usize, &Interval<Arc<Expr>>)> {
    filters
        .iter()
        .enumerate()
        .filter_map(|(position, filter)| match filter.as_ref() {
            Expr::Interval(interval) if !interval_has_array_bound(interval) => {
                Some((position, interval))
            }
            _ => None,
        })
        .min_by_key(|(_, interval)| !interval.is_point())
}

fn index_fields_as_sort_fields(index_fields: &[OrderedIndexField]) -> Vec<SortField> {
    index_fields
        .iter()
        .map(|index_field| {
            let field = Arc::new(Expr::Field((&index_field.path).into()));
            match index_field.direction {
                IndexDirection::Ascending => SortField::asc(field),
                IndexDirection::Descending => SortField::desc(field),
            }
        })
        .collect()
}

fn index_provides_order(provided_order: &[SortField], requested_order: &[SortField]) -> bool {
    requested_order.len() <= provided_order.len()
        && requested_order == &provided_order[..requested_order.len()]
}

fn index_sort_prefix_is_scalar(multikey_fields: BitSet, prefix_len: usize) -> bool {
    // Document sorting compares whole array values, while index entries order
    // expanded elements. Only scalar sort fields can provide document ordering;
    // multikey fields beyond the requested prefix do not affect that ordering.
    (0..prefix_len).all(|position| !multikey_fields.contains(position as u8))
}

fn index_scan_direction(
    index_fields: &[OrderedIndexField],
    requested_order: &[SortField],
) -> Option<Direction> {
    let provided_order = index_fields_as_sort_fields(index_fields);

    if index_provides_order(&provided_order, requested_order) {
        return Some(Direction::Forward);
    }

    let reversed_order: Vec<SortField> = provided_order
        .into_iter()
        .map(|sort_field| sort_field.reverse())
        .collect();

    if index_provides_order(&reversed_order, requested_order) {
        Some(Direction::Reverse)
    } else {
        None
    }
}

fn compute_residual_filter(
    original: Arc<Expr>,
    used_predicates: &[(Arc<Expr>, usize)],
) -> Option<Arc<Expr>> {
    let filters: &[Arc<Expr>] = match original.as_ref() {
        Expr::And(filters) => filters,
        Expr::FieldFilters { .. } => std::slice::from_ref(&original),
        _ => unreachable!("Unexpected expression: {:?}", original),
    };

    let new_filters: Vec<Arc<Expr>> = filters
        .iter()
        .filter_map(|filter| {
            if let Expr::FieldFilters { field, filters } = filter.as_ref() {
                let residual_filters: Vec<Arc<Expr>> = filters
                    .iter()
                    .enumerate()
                    .filter_map(|(position, filter)| {
                        let is_used = used_predicates.iter().any(|(used_field, used_position)| {
                            field.as_ref() == used_field.as_ref() && position == *used_position
                        });
                        (!is_used).then(|| filter.clone())
                    })
                    .collect();

                if residual_filters.len() != filters.len() {
                    return if residual_filters.is_empty() {
                        // All filters for this field were used.
                        None
                    } else {
                        // Keep the remaining filters for the used field.
                        Some(Arc::new(Expr::FieldFilters {
                            field: field.clone(),
                            filters: residual_filters,
                        }))
                    };
                }
            }
            Some(filter.clone())
        })
        .collect();

    match new_filters.len() {
        0 => None,
        1 => new_filters.into_iter().next(),
        _ => Some(Arc::new(Expr::And(new_filters))),
    }
}

/// Returns true if an expression is sargable (can be used with an index).
fn is_sargable(expr: &Arc<Expr>) -> bool {
    match expr.as_ref() {
        // $eq, $gte, $gt, $lt, and $lte have been converted into interval expressions during
        // normalization. `$in` intentionally remains a Comparison for multipoint lookups.
        Expr::Comparison {
            operator: ComparisonOperator::In,
            value,
        } => matches!(value.as_ref(), Expr::Placeholder { .. }),
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
mod tests;
