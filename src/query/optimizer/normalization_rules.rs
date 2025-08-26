//! # Query Normalization Rules
//!
//! This module implements a sequence of normalization rules that are applied to a logical query plan
//! to simplify it and prepare it for optimization and execution. The rules are applied in a specific
//! order to ensure correctness and maximize simplification opportunities. The pipeline is designed to
//! be idempotent, meaning applying it multiple times to the same plan will not change the result
//! after the first application.
//!
//! The normalization pipeline consists of the following rules, applied in sequence:
//!
//! 1.  **`DeMorganNorToAnd`**: This rule applies De Morgan's laws to transform `NOR` expressions
//!     into equivalent `AND` expressions with negated conditions (e.g., `NOR(A, B)` becomes
//!     `AND(NOT(A), NOT(B))`). This simplifies the variety of logical operators the subsequent
//!     rules need to handle.
//!
//! 2.  **`PushDownNotExpressions`**: This rule pushes `NOT` operators down the expression tree,
//!     closer to the leaf nodes. For example, `NOT(AND(A, B))` becomes `OR(NOT(A), NOT(B))`, and
//!     `NOT(x > 5)` becomes `x <= 5`. This helps in simplifying expressions by removing negations
//!     at higher levels.
//!
//! 3.  **`SimplifyLogicalOperators`**: This is a crucial cleanup rule that works from the bottom up.
//!     It flattens nested `AND` and `OR` expressions, removes duplicate conditions, and performs
//!     constant folding (e.g., `A AND TRUE` becomes `A`). It also detects contradictions
//!     (`A AND NOT A` becomes `FALSE`) and tautologies (`A OR NOT A` becomes `TRUE`), which can
//!     significantly simplify or even eliminate parts of the query.
//!
//! 4.  **`CombineComparisonsToInterval`**: This rule identifies multiple range comparison predicates
//!     (`$gt`, `$gte`, `$lt`, `$lte`, `$eq`) on the same field and combines them into a single,
//!     more compact `Interval` expression. For example, `x > 5 AND x < 10` becomes `x IN (5, 10)`.
//!     This simplifies the representation of range queries.
//!
//! 5.  **`CoalesceSameFieldDisjunctions`**: This rule simplifies `OR` expressions that contain
//!     multiple predicates on the same field. It unions overlapping or adjacent intervals, merges
//!     `$in` lists by taking the union of their values, and simplifies redundant conditions (e.g.,
//!     `x > 5 OR x > 2` becomes `x > 2`). This reduces the complexity of disjunctive predicates,
//!     potentially enabling further optimizations.
//!
//! 6.  **`CoalesceFieldPredicates`**: This rule further refines predicates on a single field. It
//!     intersects multiple `$in` lists, filters the values of an `$in` list against an `Interval`
//!     on the same field, and removes redundant `$exists: true` predicates if a more specific
//!     sargable predicate (like `$in` or an interval) is already present. This ensures that each
//!     field has at most one of each kind of sargable predicate, simplifying index matching.
//!
//! 7.  **`EliminateRedundantFilter`**: After all expression simplifications, this rule cleans up
//!     the logical plan tree. If a `Filter` node has a condition that has been simplified to
//!     `AlwaysTrue`, the filter is removed entirely. If the condition is `AlwaysFalse`, the entire
//!     sub-plan fed by the filter is replaced with a `NoOp` node, as it can produce no results.
//!
//! 8.  **`PushDownFiltersToScan`**: As a final step, this rule pushes down eligible filter predicates
//!     from `Filter` nodes into the `CollectionScan` nodes. This is a critical optimization that
//!     allows the storage engine to use indexes or perform efficient filtering at the data source,
//!     significantly reducing the amount of data that needs to be processed by higher-level
//!     operators.
use crate::query::logical_plan::{transform_down_filter, transform_up_filter, LogicalPlan};
use crate::query::{BsonValue, BsonValueRef, ComparisonOperator, Expr};
use crate::query::tree_node::TreeNode;
use crate::util::interval::Interval;
use std::collections::{HashMap, HashSet};
use std::ops::RangeBounds;
use std::sync::Arc;

use bson::Bson;
use crate::util::bson_utils;

const MAX_OR_CARTESIAN_PRODUCT: usize = 64;


pub trait NormalisationRule {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan>;
}

/// Returns the list of normalization rules to be applied in sequence.
pub fn all_normalization_rules() -> impl NormalisationRule {
    CompositeRule::new(vec![
        Arc::new(DeMorganNorToAnd {}),
        Arc::new(PushDownNotExpressions {}),
        Arc::new(SimplifyLogicalOperators {}),
        Arc::new(CombineComparisonsToInterval {}),
        Arc::new(CoalesceSameFieldDisjunctions {}),
        Arc::new(CoalesceFieldPredicates {}),
        Arc::new(EliminateRedundantFilter {}),
        Arc::new(PushDownFiltersToScan {}),
    ])
}

#[derive(Clone)]
pub struct CompositeRule {
    rules: Vec<Arc<dyn NormalisationRule>>,
}

impl CompositeRule {
    pub fn new(rules: Vec<Arc<dyn NormalisationRule>>) -> Self {
        Self { rules }
    }
}

impl NormalisationRule for CompositeRule {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        self.rules
            .iter()
            .fold(plan, |p, rule| rule.apply(p))
    }

}

/// Normalization rule to apply De Morgan's Law for NOR expressions in logical plans.
/// This rule transforms NOR expressions into AND expressions with negated conditions.
#[derive(Clone)]
pub struct DeMorganNorToAnd;

impl NormalisationRule for DeMorganNorToAnd {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_down_filter(plan, &|expr| Self::de_morgan_nor_to_and(expr))
    }
}

impl DeMorganNorToAnd {
    fn de_morgan_nor_to_and(expr: Arc<Expr>) -> Arc<Expr> {
        match expr.as_ref() {
            Expr::Nor(conditions) => {
                // De Morgan's Law: NOR(A, B) → AND(NOT(A), NOT(B))
                Arc::new(Expr::And(
                    conditions.iter().map(|c| Arc::new(Expr::Not(c.clone()))).collect(),
                ))
            }
            _ => expr.clone(),
        }
    }
}

/// Normalization rule to simplify NOT expressions in logical plans.
#[derive(Clone)]
pub struct PushDownNotExpressions;

impl NormalisationRule for PushDownNotExpressions {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_down_filter(plan,&|expr| Self::push_down_not(expr))
    }
}

impl PushDownNotExpressions {
    fn push_down_not(expr: Arc<Expr>) -> Arc<Expr> {
        match expr.as_ref() {
            Expr::Not(expr) => expr.clone().negate(),
            _ => expr.clone(),
        }
    }
}

/// Normalization rule to simplify logical operators in expressions by flattening nested AND/OR,
/// removing duplicates, folding constants, and detecting contradictions or tautologies.
#[derive(Clone)]
pub struct SimplifyLogicalOperators;

impl NormalisationRule for SimplifyLogicalOperators {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_up_filter(plan, &|expr| Self::simplify_operators(expr))
    }
}

impl SimplifyLogicalOperators {
    fn simplify_operators(expr: Arc<Expr>) -> Arc<Expr> {
        match expr.as_ref() {
            // No need to flatten Nor expressions here, as they are handled by DeMorganNorToAnd
            Expr::And(conditions) => Self::simplify_and(conditions),
            Expr::Or(conditions) => Self::simplify_or(conditions),
            Expr::FieldFilters { field, filters } => Self::simplify_field_filters(field, filters),
            Expr::Comparison { operator, value }
                if *operator == ComparisonOperator::In || *operator == ComparisonOperator::Nin =>
                {
                    if let Expr::Literal(bson_val) = value.as_ref() {
                        if let Bson::Array(arr) = bson_val.to_bson() {
                            if arr.is_empty() {
                                return if *operator == ComparisonOperator::In {
                                    Arc::new(Expr::AlwaysFalse) // IN [] => FALSE
                                } else {
                                    Arc::new(Expr::AlwaysTrue) // NIN [] => TRUE
                                };
                            }
                        }
                    }
                    expr.clone()
                },
            Expr::Nor(_) => {
                panic!("De Morgan's Law should have been applied before this rule")
            }
            _ => expr.clone(),
        }
    }

    fn simplify_and(expressions: &[Arc<Expr>]) -> Arc<Expr> {
        // First, flatten nested ANDs
        let mut flattened: Vec<Arc<Expr>> = Vec::new();
        for expr in expressions {
            if let Expr::And(sub_exprs) = expr.as_ref() {
                flattened.extend(sub_exprs.clone());
            } else {
                flattened.push(expr.clone());
            }
        }

        // Group FieldFilters by field and collect other expressions.
        let mut field_filters_map: HashMap<Arc<Expr>, Vec<Arc<Expr>>> = HashMap::new();
        let mut other_exprs: Vec<Arc<Expr>> = Vec::new();

        for e in flattened {
            match e.as_ref() {
                Expr::AlwaysTrue => continue, // Drop TRUE
                Expr::AlwaysFalse => return Arc::new(Expr::AlwaysFalse), // A AND FALSE => FALSE
                Expr::FieldFilters { field, filters } => {
                    field_filters_map
                        .entry(field.clone())
                        .or_default()
                        .extend(filters.clone());
                }
                _ => other_exprs.push(e),
            }
        }

        // Reconstruct expressions, with merged FieldFilters
        let mut result = other_exprs;
        for (field, filters) in field_filters_map {
            if !filters.is_empty() {
                result.push(Arc::new(Expr::FieldFilters {
                    field,
                    filters,
                }));
            }
        }

        // Apply deduplication and contradiction detection
        let mut final_result: Vec<Arc<Expr>> = Vec::new();
        let mut seen: HashSet<Arc<Expr>> = HashSet::new();

        for e in result {
            let neg = e.negate();
            if seen.contains(&neg) {
                // A AND NOT A => FALSE
                return Arc::new(Expr::AlwaysFalse);
            }

            if seen.insert(e.clone()) {
                // Keep first occurrence only (remove duplicates)
                final_result.push(e);
            }
        }
        // Sort the result to ensure consistent ordering (important when comparing queries shape
        final_result.sort();
        match final_result.len() {
            0 => Arc::new(Expr::AlwaysTrue), // AND() == TRUE
            1 => final_result.remove(0),
            _ => Arc::new(Expr::And(final_result)),
        }
    }

    fn simplify_or(expressions: &[Arc<Expr>]) -> Arc<Expr> {
        // First, flatten nested ORs
        let mut flattened: Vec<Arc<Expr>> = Vec::new();
        for expr in expressions {
            if let Expr::Or(sub_exprs) = expr.as_ref() {
                flattened.extend(sub_exprs.clone());
            } else {
                flattened.push(expr.clone());
            }
        }

        // Apply constant folding, deduplication, and tautology detection
        let mut result: Vec<Arc<Expr>> = Vec::new();
        let mut seen: HashSet<Arc<Expr>> = HashSet::new();

        for e in flattened {
            match e.as_ref() {
                Expr::AlwaysTrue => return Arc::new(Expr::AlwaysTrue), // A OR TRUE => TRUE
                Expr::AlwaysFalse => continue, // Drop FALSE
                _ => {}
            }

            let neg = e.negate();
            if seen.contains(&neg) {
                // A OR NOT A => TRUE
                return Arc::new(Expr::AlwaysTrue);
            }

            if seen.insert(e.clone()) {
                // Keep first occurrence only (remove duplicates)
                result.push(e);
            }
        }
        // Sort the result to ensure consistent ordering (important when comparing queries shape
        result.sort();
        match result.len() {
            0 => Arc::new(Expr::AlwaysFalse), // OR() == FALSE
            1 => result.remove(0),
            _ => Arc::new(Expr::Or(result)),
        }
    }

    fn simplify_field_filters(field: &Arc<Expr>, filters: &[Arc<Expr>]) -> Arc<Expr> {
        // Apply constant folding, deduplication, and contradiction detection
        let mut result: Vec<Arc<Expr>> = Vec::new();
        let mut seen: HashSet<Arc<Expr>> = HashSet::new();

        for e in filters {
            match e.as_ref() {
                Expr::AlwaysTrue => continue, // Drop TRUE
                Expr::AlwaysFalse => return Arc::new(Expr::AlwaysFalse), // A AND FALSE => FALSE
                _ => {}
            }

            let neg = e.clone().negate();
            if seen.contains(&neg) {
                // A AND NOT A => FALSE
                return Arc::new(Expr::AlwaysFalse);
            }

            if seen.insert(e.clone()) {
                // Keep first occurrence only (remove duplicates)
                result.push(e.clone());
            }
        }
        // Sort the result to ensure consistent ordering (important when comparing queries shape
        result.sort();
        match result.len() {
            0 => Arc::new(Expr::AlwaysTrue), // FieldFilters with no filters is a tautology
            _ => Arc::new(Expr::FieldFilters {
                field: field.clone(),
                filters: result,
            }),
        }
    }
}

/// Normalization rule to combine multiple range/equality comparisons on the same field
/// into a single interval expression.
#[derive(Clone)]
pub struct CombineComparisonsToInterval;

impl NormalisationRule for CombineComparisonsToInterval {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_up_filter(plan, &|expr| Self::combine_comparisons_to_interval(expr))
    }
}

impl CombineComparisonsToInterval {
    fn combine_comparisons_to_interval(expr: Arc<Expr>) -> Arc<Expr> {
        if let Expr::FieldFilters { field, filters } = expr.as_ref() {
            let mut combined_interval: Interval<Arc<Expr>> = Interval::all();
            let mut other_filters = Vec::new();
            let mut comparisons_found = false;

            for f in filters.iter() {
                if let Expr::Comparison { operator, value } = f.as_ref() {
                    if !matches!(value.as_ref(), Expr::Literal(_)) {
                        other_filters.push(f.clone());
                        continue;
                    }

                    let interval = match operator {
                        // We convert only the operators that are shape stable. Even if $in, $nin and $ne
                        // could be converted to a set of intervals, converting them would lead to a combinatorial explosion
                        // of shapes.
                        ComparisonOperator::Eq => {
                            Some(Interval::closed(value.clone(), value.clone()))
                        }
                        ComparisonOperator::Gt => Some(Interval::greater_than(value.clone())),
                        ComparisonOperator::Gte => Some(Interval::at_least(value.clone())),
                        ComparisonOperator::Lt => Some(Interval::less_than(value.clone())),
                        ComparisonOperator::Lte => Some(Interval::at_most(value.clone())),
                        _ => None,
                    };

                    if let Some(interval) = interval {
                        comparisons_found = true;
                        if let Some(intersection) = combined_interval.intersection(&interval) {
                            combined_interval = intersection;
                        } else {
                            // The intersection is empty, which means a contradiction.
                            return Arc::new(Expr::AlwaysFalse);
                        }
                    } else {
                        other_filters.push(f.clone());
                    }
                } else {
                    other_filters.push(f.clone());
                }
            }

            if !comparisons_found {
                return expr.clone();
            }

            if combined_interval != Interval::all() {
                other_filters.push(Arc::new(Expr::Interval(combined_interval)));
            }

            other_filters.sort();
            if other_filters.is_empty() {
                Arc::new(Expr::AlwaysTrue)
            } else {
                Arc::new(Expr::FieldFilters {
                    field: field.clone(),
                    filters: other_filters,
                })
            }
        } else {
            expr.clone()
        }
    }
}

/// Normalization rule to coalesce multiple sargable predicates on the same field
/// into a single predicate where possible. This rule intersects multiple `$in` clauses,
/// filters `$in` values against `$gt`/`$lt` intervals, and removes redundant `$exists: true`
/// predicates.
#[derive(Clone)]
pub struct CoalesceFieldPredicates;

impl NormalisationRule for CoalesceFieldPredicates {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_up_filter(plan, &|expr| Self::coalesce_field_predicates(expr))
    }
}

impl CoalesceFieldPredicates {
    fn coalesce_field_predicates(expr: Arc<Expr>) -> Arc<Expr> {
        if let Expr::FieldFilters { field, filters } = expr.as_ref() {

            // Previous rules ensure that filters are sorted and deduplicated. Therefore,
            // based on the Expr enum order we will have: Comparison, Interval, Exists and then others in that order.
            let mut new_filters: Vec<Arc<Expr>> = Vec::new();
            let mut previous: Option<Arc<Expr>> = None;

            for f in filters {
                match f.as_ref() {
                    Expr::Comparison {
                        operator: ComparisonOperator::In,
                        value,
                    } => {
                        if let Some(expr) = &previous {
                            match expr.as_ref() {
                                Expr::Comparison {
                                    operator: ComparisonOperator::In,
                                    value: _,
                                } => {
                                    // Merge two IN clauses by intersecting their values
                                    let merged = coalesce_in_with_in(expr, f);
                                    if matches!(merged.as_ref(), Expr::AlwaysFalse) {
                                        return Arc::new(Expr::AlwaysFalse);
                                    }
                                    previous = Some(merged);
                                }
                                _ => panic!("Unexpected expression before IN clause: {:?}", expr),
                            }
                        } else {

                            if let Expr::Literal(BsonValue(Bson::Array(array))) = value.as_ref() {
                                let mut array = array.iter().cloned().collect::<Vec<_>>();
                                array.sort_by(|a, b| bson_utils::cmp_bson(a, b));
                                previous = Some(Arc::new(Expr::Comparison {
                                    operator: ComparisonOperator::In,
                                    value: Arc::new(Expr::Literal(BsonValue::from(array))),
                                    }));

                            } else {
                                panic!("IN operator requires an array literal as its value");
                            }
                        }
                    }
                    Expr::Interval(..) => {
                        if let Some(expr) = &previous {
                            match expr.as_ref() {
                                Expr::Comparison {
                                    operator: ComparisonOperator::In,
                                    value: _,
                                } => {
                                    // Merge IN clause with Interval by filtering IN values against the interval
                                    let merged = coalesce_in_with_interval(expr, f);
                                    if matches!(merged.as_ref(), Expr::AlwaysFalse) {
                                        return Arc::new(Expr::AlwaysFalse);
                                    }
                                    previous = Some(merged);
                                }
                                _ => panic!("Unexpected expression before an interval clause: {:?}", expr),
                            }
                        } else {
                            previous = Some(f.clone());
                        }
                    }
                    Expr::Exists(true) => {
                        if previous.is_some() {
                            continue; // Drop redundant Exists(true)
                        } else {
                            previous = Some(f.clone());
                        }
                    } ,
                    _ => new_filters.push(f.clone()),
                }
            }

            if previous.is_some() {
                new_filters.push(previous.unwrap());
            }
            new_filters.sort();

            if new_filters.is_empty() {
                Arc::new(Expr::AlwaysTrue)
            } else {
                Arc::new(Expr::FieldFilters {
                    field: field.clone(),
                    filters: new_filters,
                })
            }
        } else {
            expr.clone()
        }
    }
}

/// Normalization rule to coalesce multiple disjunctive predicates on the same field
/// into a single predicate where possible. This rule unions multiple `$in` clauses,
/// unions overlapping or adjacent intervals, and handles `Exists(true)` subsumption.
#[derive(Clone)]
pub struct CoalesceSameFieldDisjunctions;

impl NormalisationRule for CoalesceSameFieldDisjunctions {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_up_filter(plan, &|expr| Self::coalesce_disjunctions(expr))
    }
}

impl CoalesceSameFieldDisjunctions {
    fn coalesce_disjunctions(expr: Arc<Expr>) -> Arc<Expr> {
        if let Expr::Or(disjuncts) = expr.as_ref() {
            let mut field_filters_map: HashMap<Arc<Expr>, Vec<Arc<Expr>>> = HashMap::new();
            let mut other_disjuncts: Vec<Arc<Expr>> = Vec::new();

            for d in disjuncts {
                if let Expr::FieldFilters { field, filters } = d.as_ref() {
                    // Only attempt to coalesce if the FieldFilters contains a single predicate.
                    // OR-ing an AND expression like `a > 5 AND a < 10` with another predicate
                    // on `a` can result in multi-intervals, which is not shape-stable.
                    if filters.len() == 1 {
                        field_filters_map
                            .entry(field.clone())
                            .or_default()
                            .push(filters[0].clone());
                    } else {
                        other_disjuncts.push(d.clone());
                    }
                } else {
                    other_disjuncts.push(d.clone());
                }
            }

            let mut new_disjuncts = other_disjuncts;
            for (field, filters) in field_filters_map {
                if let Some(coalesced) = Self::coalesce_same_field_disjuncts(&filters) {
                    new_disjuncts.push(Arc::new(Expr::FieldFilters {
                        field,
                        filters: vec![coalesced],
                    }));
                } else {
                    // Could not coalesce, add original FieldFilters back.
                    for filter in filters {
                        new_disjuncts.push(Arc::new(Expr::FieldFilters {
                            field: field.clone(),
                            filters: vec![filter],
                        }));
                    }
                }
            }

            SimplifyLogicalOperators::simplify_or(&new_disjuncts)
        } else {
            expr.clone()
        }
    }

    /// Attempts to coalesce a list of disjunctive predicates on the same field.
    fn coalesce_same_field_disjuncts(filters: &[Arc<Expr>]) -> Option<Arc<Expr>> {

        let mut previous = None;

        for f in filters {
            match f.as_ref() {
                Expr::Comparison {
                    operator: ComparisonOperator::In,
                    value : _value,
                } => {
                    previous = if previous.is_none() {
                        Some(f.clone())
                    } else {
                        coalesce_in_disjunctions(&previous.unwrap(), f)
                    };
                }
                Expr::Interval(i) => {
                    previous = if previous.is_none() {
                        Some(f.clone())
                    } else {
                        if let Expr::Interval(prev_i) = previous.as_ref().unwrap().as_ref() {
                            if let Some(union) = prev_i.union(i) {
                                Some(Arc::new(Expr::Interval(union)))
                            } else {
                                return None; // Disjoint intervals cannot be coalesced
                            }
                        } else {
                            return None; // Cannot coalesce Interval with non-Interval
                        }
                     };
                },
                Expr::Exists(true) => return Some(f.clone()), // Exists(true) subsumes all other predicates
                _ => return None, // Cannot coalesce if there are other types of predicates
            }
        }
        previous
    }
}

fn coalesce_in_disjunctions(left: &Arc<Expr>, right: &Arc<Expr>) -> Option<Arc<Expr>> {

    let (
        Expr::Comparison { operator: ComparisonOperator::In, value: left_value },
        Expr::Comparison { operator: ComparisonOperator::In, value: right_value }
    ) = (left.as_ref(), right.as_ref()) else {
        panic!("coalesce_in_disjunctions should only be called with two IN expressions");
    };

    let (
        Expr::Literal(BsonValue(Bson::Array(left_arr))),
        Expr::Literal(BsonValue(Bson::Array(right_arr)))
    ) = (left_value.as_ref(), right_value.as_ref()) else {
        panic!("IN values must be arrays");
    };

    let mut combined: HashSet<BsonValueRef> = left_arr.into_iter().map(|e| BsonValueRef(e)).collect();
    combined.extend(right_arr.into_iter().map(|e| BsonValueRef(e)));

    Some(Arc::new(Expr::Comparison {
        operator: ComparisonOperator::In,
        value: Arc::new(Expr::Literal(BsonValue::from(combined))),
    }))
}

fn coalesce_in_with_in(left: &Arc<Expr>, right: &Arc<Expr>) -> Arc<Expr> {

    let (
        Expr::Comparison { operator: ComparisonOperator::In, value: left_value },
        Expr::Comparison { operator: ComparisonOperator::In, value: right_value }
    ) = (left.as_ref(), right.as_ref()) else {
        panic!("merge_in_with_in should only be called with two IN expressions");
    };

    let (
        Expr::Literal(BsonValue(Bson::Array(left_arr))),
        Expr::Literal(BsonValue(Bson::Array(right_arr)))
    ) = (left_value.as_ref(), right_value.as_ref()) else {
        panic!("IN values must be arrays");
    };

    let set: HashSet<_> = left_arr.into_iter().map(BsonValueRef).collect();
    let mut intersection: Vec<_> = right_arr.into_iter().filter(|e| set.contains(&BsonValueRef(e))).cloned().collect();

    if intersection.is_empty() {
        return Arc::new(Expr::AlwaysFalse);
    }

    intersection.sort_by(|a, b| bson_utils::cmp_bson(a, b));

    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::In,
        value: Arc::new(Expr::Literal(BsonValue::from(intersection))),
    })
}

fn coalesce_in_with_interval(left: &Arc<Expr>, right: &Arc<Expr>) -> Arc<Expr> {
    match (left.as_ref(), right.as_ref()) {
        (
            Expr::Comparison { operator: ComparisonOperator::In, value: left_values },
            Expr::Interval(interval),
        ) => {
            if let Expr::Literal(array) = left_values.as_ref() {
                if let Bson::Array(array) = array.to_bson() {
                    let remaining: Vec<_> = array
                        .into_iter()
                        .filter(|e| {
                            interval
                                .contains(&Arc::new(Expr::Literal(BsonValue(e.clone()))))
                        })
                        .collect();

                    return if remaining.is_empty() {
                        Arc::new(Expr::AlwaysFalse)
                    } else {
                        Arc::new(Expr::Comparison {
                            operator: ComparisonOperator::In,
                            value: Arc::new(Expr::Literal(BsonValue::from(remaining))),
                        })
                    };
                }
            }
        }
        _ => {}
    }
    panic!("merge_in_with_interval should only be called with IN and Interval expressions");
}

/// Normalization rule to eliminate redundant filters.
/// A filter with an `AlwaysTrue` condition is removed.
/// A filter with an `AlwaysFalse` condition replaces the sub-plan with a `NoOp` node.
#[derive(Clone)]
pub struct EliminateRedundantFilter;

impl NormalisationRule for EliminateRedundantFilter {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        plan.transform_up(&|node| {
            if node
                .children()
                .iter()
                .any(|child| matches!(child.as_ref(), LogicalPlan::NoOp))
            {
                return Arc::new(LogicalPlan::NoOp);
            }

            match node.as_ref() {
                LogicalPlan::Filter { input, condition } => match condition.as_ref() {
                    Expr::AlwaysTrue => input.clone(),
                    Expr::AlwaysFalse => Arc::new(LogicalPlan::NoOp),
                    _ => node.clone(),
                },
                _ => node.clone(),
            }
        })
    }
}

/// Builds an AND expression from a list of expressions, removing `AlwaysTrue`.
fn build_and_clauses(exprs: &[Arc<Expr>]) -> Arc<Expr> {
    let exprs: Vec<_> = exprs
        .iter()
        .filter(|e| !matches!(e.as_ref(), Expr::AlwaysTrue))
        .cloned()
        .collect();

    match exprs.len() {
        0 => Arc::new(Expr::AlwaysTrue),
        1 => exprs.into_iter().next().unwrap(),
        _ => Arc::new(Expr::And(exprs)),
    }
}

/// Computes the cartesian product of a series of lists.
fn cartesian_product<T: Clone>(lists: &[Vec<T>]) -> Vec<Vec<T>> {
    let mut result: Vec<Vec<T>> = vec![vec![]];
    for list in lists {
        let mut next_result = Vec::new();
        for res_item in &result {
            for item in list {
                let mut new_item = res_item.clone();
                new_item.push(item.clone());
                next_result.push(new_item);
            }
        }
        result = next_result;
        if result.is_empty() {
            // This happens if a list is empty.
            return vec![];
        }
    }
    result
}

/// Distributes an OR expression over ANDs to convert it to Conjunctive Normal Form (CNF).
/// This is a shallow, non-recursive transformation.
/// Example: OR(AND(A, B), C) -> AND(OR(A, C), OR(B, C))
/// The transformation is only applied if the resulting cartesian product is smaller
/// than MAX_OR_CARTESIAN_PRODUCT.
fn distribute_or(expr: Arc<Expr>) -> Arc<Expr> {
    if let Expr::Or(disjuncts) = expr.as_ref() {
        if disjuncts.is_empty() {
            return expr;
        }

        let conjunct_lists: Vec<Vec<Arc<Expr>>> = disjuncts
            .iter()
            .map(|d| match d.as_ref() {
                Expr::And(conjuncts) => conjuncts.clone(),
                _ => vec![d.clone()],
            })
            .collect();

        // Calculate product size and check against the limit
        let mut product_size_opt: Option<usize> = Some(1);
        for list in &conjunct_lists {
            if list.is_empty() {
                // One of the disjuncts is AND([]), which is AlwaysTrue.
                // OR(..., AlwaysTrue) is AlwaysTrue.
                return Arc::new(Expr::AlwaysTrue);
            }
            product_size_opt = product_size_opt.and_then(|p| p.checked_mul(list.len()));
        }

        if let Some(product_size) = product_size_opt {
            if product_size <= MAX_OR_CARTESIAN_PRODUCT {
                // It's safe to distribute.
                let product = cartesian_product(&conjunct_lists);

                let new_clauses: Vec<Arc<Expr>> = product
                    .into_iter()
                    .map(|mut p| {
                        if p.len() > 1 {
                            p.sort(); // For canonical form
                            Arc::new(Expr::Or(p))
                        } else {
                            p.into_iter().next().unwrap()
                        }
                    })
                    .collect();

                if new_clauses.len() == 1 {
                    return new_clauses.into_iter().next().unwrap();
                } else {
                    let mut sorted_clauses = new_clauses;
                    sorted_clauses.sort(); // For canonical form
                    return Arc::new(Expr::And(sorted_clauses));
                }
            }
        }
        // Overflow or too large. Return original expression.
        return expr;
    }
    expr
}

/// Normalization rule to push down filters to the scan level.
/// This allows the storage engine to use indexes or perform byte-level filtering.
#[derive(Clone)]
pub struct PushDownFiltersToScan;

impl NormalisationRule for PushDownFiltersToScan {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        plan.transform_up(&|node| {
            if let LogicalPlan::Filter { input, condition } = node.as_ref() {
                if let LogicalPlan::CollectionScan {
                    collection,
                    projection,
                    filter: scan_filter,
                    sort,
                } = input.as_ref()
                {
                    assert_eq!(scan_filter, &None, "Scan filter should be None before pushing down");

                    let condition = distribute_or(condition.clone());
                    let (pushable, residual) = split_pushable(&condition);

                    // If nothing can be pushed down, return the original plan node.
                    if matches!(pushable.as_ref(), Expr::AlwaysTrue) {
                        return node.clone();
                    }

                    let new_scan = Arc::new(LogicalPlan::CollectionScan {
                        collection: *collection,
                        projection: projection.clone(),
                        filter: Some(pushable),
                        sort: sort.clone(),
                    });

                    return match residual.as_ref() {
                        Expr::AlwaysTrue => new_scan, // Filter is fully pushed down, remove it.
                        _ => Arc::new(LogicalPlan::Filter {
                            input: new_scan,
                            condition: residual,
                        }),
                    };
                }
            }
            node.clone()
        })
    }
}

/// Splits an expression into a pushable part and a residual part.
/// The pushable part contains conditions that can be handled by the storage engine.
/// The residual part contains conditions that must be evaluated by a filter operator.
fn split_pushable(expr: &Arc<Expr>) -> (Arc<Expr>, Arc<Expr>) {
    match expr.as_ref() {
        Expr::And(conditions) => {
            let (pushable_parts, residual_parts): (Vec<_>, Vec<_>) =
                conditions.iter().map(split_pushable).unzip();

            let pushable = build_and_clauses(&pushable_parts);
            let residual = build_and_clauses(&residual_parts);

            (pushable, residual)
        }
        Expr::Or(conditions) => {
            let (pushable_parts, residual_parts): (Vec<_>, Vec<_>) =
                conditions.iter().map(split_pushable).unzip();

            // If any sub-expression has a residual part (an Expr different from AlwaysTrue),
            // the entire OR is considered residual.
            if residual_parts
                .iter()
                .any(|r| !matches!(r.as_ref(), Expr::AlwaysTrue))
            {
                (Arc::new(Expr::AlwaysTrue), expr.clone())
            } else {
                // As EliminateRedundantFilter should have eliminated all the AlwaysFalse, we should not have any.
                // We  also should not any AlwaysTrue as it would mean that we had some residual part.
                let pushable = match pushable_parts.len() {
                    0 => Arc::new(Expr::AlwaysFalse),
                    1 => pushable_parts.into_iter().next().unwrap(),
                    _ => Arc::new(Expr::Or(pushable_parts)),
                };
                (pushable, Arc::new(Expr::AlwaysTrue))
            }
        }
        Expr::FieldFilters { field, filters } => {
            let (pushable_filters, residual_filters): (Vec<_>, Vec<_>) = filters
                .iter()
                .cloned()
                .partition(|e| is_pushable_leaf_filter(e));

            fn build_field_filters(field: &Arc<Expr>, filters: Vec<Arc<Expr>>) -> Arc<Expr> {

                if filters.is_empty() {
                    Arc::new(Expr::AlwaysTrue)
                } else {
                    Arc::new(Expr::FieldFilters {
                        field: field.clone(),
                        filters,
                    })
                }
            }

            let pushable = build_field_filters(field, pushable_filters);
            let residual = build_field_filters(field, residual_filters);

            (pushable, residual)
        }
        _ => (Arc::new(Expr::AlwaysTrue), expr.clone()),
    }
}

/// Checks if a leaf filter expression is pushable to the storage engine.
/// Pushable operators include simple comparisons, $in, $exists: true, and $type.
fn is_pushable_leaf_filter(expr: &Expr) -> bool {
    match expr {
        Expr::Comparison { operator: ComparisonOperator::In, value } if matches!(value.as_ref(), Expr::Literal(_)) => true,
        Expr::Interval(_) => true,
        Expr::Exists(true) => true,
        Expr::Type {
            negated: false,
            bson_type: _bson_type,
        } => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::expr_fn::*;
    use crate::query::logical_plan::LogicalPlanBuilder;
    use crate::util::interval::Interval;
    use std::sync::Arc;
    use crate::query::{PathComponent, ProjectionExpr};

    #[test]
    fn test_not_normalization() {
        // Test Double Negation: NOT(NOT(X)) → X
        let original = not(not(eq(lit(5))));
        let transformed = eq(lit(5));

        check_expr_transformation(PushDownNotExpressions {}, original, transformed);

        // Test Negation of Comparison Operators
        check_expr_transformation(PushDownNotExpressions {}, not(gt(lit(5))), lte(lit(5)));
        check_expr_transformation(PushDownNotExpressions {}, not(lt(lit(5))), gte(lit(5)));
        check_expr_transformation(PushDownNotExpressions {}, not(eq(lit(5))), ne(lit(5)));
        check_expr_transformation(PushDownNotExpressions {}, not(ne(lit(5))), eq(lit(5)));

        // Test De Morgan's Laws
        let original = not(and([eq(lit(1)), gt(lit(2))]));
        let transformed = or([ne(lit(1)), lte(lit(2))]);

        check_expr_transformation(PushDownNotExpressions {}, original, transformed);

        let original = not(or([eq(lit(1)), lt(lit(2))]));
        let transformed = and([ne(lit(1)), gte(lit(2))]);

        check_expr_transformation(PushDownNotExpressions {}, original, transformed);
    }

    #[test]
    fn test_push_down_not_with_de_morgan() {
        let original = not(and([not(eq(lit(1))), eq(lit(2))]));
        let transformed = or([eq(lit(1)), ne(lit(2))]);
        check_expr_transformation(PushDownNotExpressions {}, original, transformed);
    }

    #[test]
    fn test_or_deduplication_simplification() {
        let p = eq(lit(1));
        let original = or([p.clone(), p.clone()]);
        let transformed = p.clone();

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_or_tautology_simplification() {
        let p = eq(lit(1));
        let original = or([p.clone(), not(p.clone())]);
        let transformed = Arc::new(Expr::AlwaysTrue);

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_and_contradiction_simplification() {
        let p = eq(lit(1));
        let original = and([p.clone(), not(p.clone())]);
        let transformed = Arc::new(Expr::AlwaysFalse);

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_constant_folding_and_true() {
        let p = eq(lit(1));
        let original = and([p.clone(), Arc::new(Expr::AlwaysTrue)]);
        let transformed = p.clone();

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_constant_folding_or_false() {
        let p = eq(lit(1));
        let original = or([p.clone(), Arc::new(Expr::AlwaysFalse)]);
        let transformed = p.clone();

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_and_flattening() {
        let p1 = eq(lit(1));
        let p2 = gt(lit(2));
        let p3 = lt(lit(3));
        let original = and([p1.clone(), and([p2.clone(), p3.clone()])]);
        let transformed = and([p1, p2, p3]);

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_or_flattening() {
        let p1 = eq(lit(1));
        let p2 = gt(lit(2));
        let p3 = lt(lit(3));
        let original = or([p1.clone(), or([p2.clone(), p3.clone()])]);
        let transformed = or([p1, p2, p3]);

        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
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
        let collection = 22;
        LogicalPlanBuilder::scan(collection)
            .filter(filter)
            .build()
    }

    #[test]
    fn test_in_empty_array_is_false() {
        let original = within(lit(Vec::<i32>::new()));
        let transformed = Arc::new(Expr::AlwaysFalse);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_nin_empty_array_is_true() {
        let original = nin(lit(Vec::<i32>::new()));
        let transformed = Arc::new(Expr::AlwaysTrue);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_nested_nin_and_nested_in() {
        let original = and([
            field_filters(field(["a"]), [within(lit(Vec::<i32>::new()))]),
            field_filters(field(["b"]), [eq(lit(2))])
        ]);
        let transformed = Arc::new(Expr::AlwaysFalse);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_de_morgan_nor_to_and() {
        let original = nor([eq(lit(1)), gt(lit(2))]);
        // NOR(A, B) -> AND(NOT A, NOT B)
        let transformed = and([not(eq(lit(1))), not(gt(lit(2)))]);
        check_expr_transformation(DeMorganNorToAnd {}, original, transformed);
    }

    #[test]
    fn test_field_filters_deduplication() {
        let p = eq(lit(1));
        let original = field_filters(field(["a"]), [p.clone(), p.clone()]);
        let transformed = field_filters(field(["a"]), [p]);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_field_filters_contradiction() {
        let p = eq(lit(1));
        let original = field_filters(field(["a"]), [p.clone(), not(p)]);
        let transformed = Arc::new(Expr::AlwaysFalse);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_field_filters_tautology_is_simplified() {
        // A FieldFilters with only AlwaysTrue inside is a tautology for that field,
        // which simplifies to an overall AlwaysTrue expression.
        let original = field_filters(field(["a"]), [Arc::new(Expr::AlwaysTrue)]);
        let transformed = Arc::new(Expr::AlwaysTrue);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);

        // When combined with other filters, AlwaysTrue should be dropped.
        let p = eq(lit(1));
        let original = field_filters(field(["a"]), [p.clone(), Arc::new(Expr::AlwaysTrue)]);
        let transformed = field_filters(field(["a"]), [p]);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_push_down_not_exists() {
        check_expr_transformation(PushDownNotExpressions {}, not(exists(true)), exists(false));
        check_expr_transformation(PushDownNotExpressions {}, not(exists(false)), exists(true));
    }

    #[test]
    fn test_exists_contradiction() {
        let original = and([exists(true), exists(false)]);
        let transformed = Arc::new(Expr::AlwaysFalse);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_or_with_in_empty_array() {
        // OR(X, IN([])) => OR(X, FALSE) => X
        let p = eq(lit(1));
        let original = or([p.clone(), within(lit(Vec::<i32>::new()))]);
        let transformed = p;
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_and_with_nin_empty_array() {
        // AND(X, NIN([])) => AND(X, TRUE) => X
        let p = eq(lit(1));
        let original = and([p.clone(), nin(lit(Vec::<i32>::new()))]);
        let transformed = p;
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_and_canonical_ordering() {
        let p1 = eq(lit(1));
        let p2 = gt(lit(2));
        let p3 = lt(lit(3));
        // Build in non-sorted order
        let original = and([p2.clone(), p3.clone(), p1.clone()]);
        // Expect sorted order
        let mut expected_exprs = vec![p1, p2, p3];
        expected_exprs.sort();
        let transformed = and(expected_exprs);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_nor_simplification_pipeline() {
        let p = eq(lit(1));
        let original = nor([p.clone(), not(p)]);
        let transformed = Arc::new(Expr::AlwaysFalse);

        let rules = CompositeRule::new(vec![
            Arc::new(DeMorganNorToAnd{}),
            Arc::new(PushDownNotExpressions{}),
            Arc::new(SimplifyLogicalOperators{}),]);
        check_expr_transformation(rules, original, transformed);
    }

    #[test]
    fn test_normalization_in_projection_elem_match() {
        // An expression inside an ElemMatch that can be simplified
        let p = eq(lit(1));
        let original_filter = or([p.clone(), not(p.clone())]); // Should be AlwaysTrue

        let projection = include(proj_elem_match(original_filter));

        let plan = LogicalPlanBuilder::scan(123).project(projection).build();

        // Apply simplification rule
        let rule = SimplifyLogicalOperators {};
        let normalized_plan = rule.apply(plan);

        // Check that the filter inside ElemMatch was simplified
        let expected_filter = Arc::new(Expr::AlwaysTrue);
        let expected_projection = include(proj_elem_match(expected_filter));
        let expected_plan = LogicalPlanBuilder::scan(123)
            .project(expected_projection)
            .build();

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_eliminate_always_true_filter() {
        let plan = LogicalPlanBuilder::scan(123)
            .filter(Arc::new(Expr::AlwaysTrue))
            .build();

        let rule = EliminateRedundantFilter {};
        let normalized_plan = rule.apply(plan);

        // The filter should be removed, leaving only the scan
        let expected_plan = LogicalPlanBuilder::scan(123).build();

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_eliminate_always_false_filter() {
        let plan = LogicalPlanBuilder::scan(123)
            .filter(Arc::new(Expr::AlwaysFalse))
            .limit(None, Some(10)) // Adding a limit to ensure we have a non-terminal plan
            .build();

        let rule = EliminateRedundantFilter {};
        let normalized_plan = rule.apply(plan);

        // The filter should be replaced by NoOp
        let expected_plan = Arc::new(LogicalPlan::NoOp);

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_simple_filter() {
        let filter = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            projection: None,
            filter: Some(filter),
            sort: None,
        });
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_partial_and() {
        let pushable_filter = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let residual_filter = field_filters(field(["b"]), [size(lit(2), false)]);
        let filter = and([pushable_filter.clone(), residual_filter.clone()]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = {
            let scan = Arc::new(LogicalPlan::CollectionScan {
                collection: 123,
                projection: None,
                filter: Some(pushable_filter),
                sort: None,
            });
            Arc::new(LogicalPlan::Filter {
                input: scan,
                condition: residual_filter,
            })
        };

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_or_all_pushable() {
        let filter1 = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let filter2 = field_filters(field(["b"]), [interval(Interval::greater_than(lit(10)))]);
        let filter = or([filter1.clone(), filter2.clone()]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            projection: None,
            filter: Some(filter),
            sort: None,
        });
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_or_not_all_pushable() {
        let filter1 = field_filters(field(["a"]), [eq(lit(1))]);
        let filter2 = field_filters(field(["b"]), [size(lit(2), false)]);
        let filter = or([filter1.clone(), filter2.clone()]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan.clone());

        // The OR has a non-pushable part, so nothing is pushed down.
        assert_eq!(normalized_plan, plan);
    }

    #[test]
    fn test_no_push_down_for_non_scannable_filter() {
        let filter = field_filters(field(["a"]), [size(lit(2), false)]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan.clone());

        assert_eq!(normalized_plan, plan);
    }

    #[test]
    fn test_push_down_exists_and_type() {
        let filter = and([
            field_filters(field(["a"]), [exists(true)]),
            field_filters(field(["b"]), [has_type(lit("string"), false)]),
        ]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let mut sorted_filters = match filter.as_ref() {
            Expr::And(filters) => filters.clone(),
            _ => panic!(),
        };
        sorted_filters.sort();
        let sorted_filter = Arc::new(Expr::And(sorted_filters));

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            projection: None,
            filter: Some(sorted_filter),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_distribute_or_simple() {
        let a = eq(lit("a"));
        let b = eq(lit("b"));
        let c = eq(lit("c"));

        // OR(AND(A, B), C) -> AND(OR(A, C), OR(B, C))
        let original = or([and([a.clone(), b.clone()]), c.clone()]);

        let mut or1_children = vec![a.clone(), c.clone()];
        or1_children.sort();
        let or1 = or(or1_children);

        let mut or2_children = vec![b.clone(), c.clone()];
        or2_children.sort();
        let or2 = or(or2_children);

        let mut and_children = vec![or1, or2];
        and_children.sort();
        let expected = and(and_children);

        assert_eq!(distribute_or(original), expected);
    }

    #[test]
    fn test_distribute_or_multiple_ands() {
        let a = eq(lit("a"));
        let b = eq(lit("b"));
        let c = eq(lit("c"));
        let d = eq(lit("d"));

        // OR(AND(A, B), AND(C, D)) -> AND(OR(A,C), OR(A,D), OR(B,C), OR(B,D))
        let original = or([and([a.clone(), b.clone()]), and([c.clone(), d.clone()])]);

        let mut or1_children = vec![a.clone(), c.clone()];
        or1_children.sort();
        let or1 = or(or1_children);

        let mut or2_children = vec![a.clone(), d.clone()];
        or2_children.sort();
        let or2 = or(or2_children);

        let mut or3_children = vec![b.clone(), c.clone()];
        or3_children.sort();
        let or3 = or(or3_children);

        let mut or4_children = vec![b.clone(), d.clone()];
        or4_children.sort();
        let or4 = or(or4_children);

        let mut and_children = vec![or1, or2, or3, or4];
        and_children.sort();
        let expected = and(and_children);

        assert_eq!(distribute_or(original), expected);
    }

    #[test]
    fn test_distribute_or_no_op() {
        // Not an OR, no change
        let a = eq(lit("a"));
        let b = eq(lit("b"));
        let original_and = and([a.clone(), b.clone()]);
        assert_eq!(distribute_or(original_and.clone()), original_and);

        // Empty OR, no change
        let original_empty_or = or(vec![]);
        assert_eq!(
            distribute_or(original_empty_or.clone()),
            original_empty_or
        );

    }

    #[test]
    fn test_distribute_or_with_always_true() {
        let a = eq(lit("a"));
        // OR(A, AND([])) -> OR(A, TRUE) -> TRUE
        let original = or([a, and(vec![])]);
        let expected = Arc::new(Expr::AlwaysTrue);
        assert_eq!(distribute_or(original), expected);
    }

    #[test]
    fn test_distribute_or_too_large_product() {
        // 9 * 8 = 72 > 64, should not distribute.
        let conjuncts1: Vec<_> = (0..9).map(|i| eq(lit(i))).collect();
        let conjuncts2: Vec<_> = (10..18).map(|i| eq(lit(i))).collect();
        let original = or([and(conjuncts1), and(conjuncts2)]);
        assert_eq!(distribute_or(original.clone()), original);
    }

    #[test]
    fn test_distribute_or_within_product_limit() {
        // 8 * 8 = 64 <= 64, should distribute.
        let conjuncts1: Vec<_> = (0..8).map(|i| eq(lit(i))).collect();
        let conjuncts2: Vec<_> = (10..18).map(|i| eq(lit(i))).collect();
        let original = or([and(conjuncts1.clone()), and(conjuncts2.clone())]);

        let transformed = distribute_or(original.clone());
        assert_ne!(transformed, original);
        assert!(matches!(transformed.as_ref(), Expr::And(_)));
    }

    #[test]
    fn test_push_down_split_field_filters() {
        let filter = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1))), size(lit(2), false)]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let pushable = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let residual = field_filters(field(["a"]), [size(lit(2), false)]);

        let expected_plan = {
            let scan = Arc::new(LogicalPlan::CollectionScan {
                collection: 123,
                projection: None,
                filter: Some(pushable),
                sort: None,
            });
            Arc::new(LogicalPlan::Filter {
                input: scan,
                condition: residual,
            })
        };

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_no_push_down_for_non_pushable_exists_and_type() {
        // exists(false) is not pushable
        let filter = field_filters(field(["a"]), [exists(false)]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);

        // negated $type is not pushable
        let filter = field_filters(field(["b"]), [has_type(lit("string"), true)]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);
    }

    #[test]
    fn test_push_down_in_with_non_empty_array() {
        let filter = field_filters(field(["a"]), [within(lit(vec![1, 2, 3]))]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();
        let normalized_plan = PushDownFiltersToScan {}.apply(plan);
        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            projection: None,
            filter: Some(filter),
            sort: None,
        });
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_no_push_down_for_elem_match_and_all() {
        let filter = field_filters(field(["a"]), [elem_match([eq(lit(1))])]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);

        let filter = field_filters(field(["b"]), [all(lit(vec![1, 2]))]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);
    }

    #[test]
    fn test_push_down_or_unlocked_by_distribution() {
        let a_eq = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let b_gt = field_filters(field(["b"]), [interval(Interval::greater_than(lit(2)))]);
        let c_eq = field_filters(field(["c"]), [interval(Interval::closed(lit(3), lit(3)))]);

        // OR(AND(A, B), C) which is pushable after distribution
        let filter = or([and([a_eq.clone(), b_gt.clone()]), c_eq.clone()]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter).build();
        let normalized_plan = PushDownFiltersToScan {}.apply(plan);

        // Expected distributed form: AND(OR(A, C), OR(B, C))
        let mut or_children = vec![a_eq.clone(), c_eq.clone()];
        or_children.sort();
        let or1 = or(or_children);

        let mut or_children = vec![b_gt.clone(), c_eq.clone()];
        or_children.sort();
        let or2 = or(or_children);

        let mut and_children = vec![or1, or2];
        and_children.sort();
        let expected_filter = and(and_children);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            projection: None,
            filter: Some(expected_filter),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_partial_push_down_or_unlocked_by_distribution() {
        let pushable_a = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let non_pushable_x = field_filters(field(["x"]), [size(lit(2), false)]);
        let pushable_b = field_filters(field(["b"]), [interval(Interval::greater_than(lit(10)))]);

        // OR(AND(A, X), B) -> AND(OR(A, B), OR(X, B))
        let filter = or([
            and([pushable_a.clone(), non_pushable_x.clone()]),
            pushable_b.clone(),
        ]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter).build();
        let normalized_plan = PushDownFiltersToScan {}.apply(plan);

        // OR(A, B) is pushable
        let mut pushable_or_children = vec![pushable_a.clone(), pushable_b.clone()];
        pushable_or_children.sort();
        let pushable_part = or(pushable_or_children);

        // OR(X, B) is residual because X is not pushable
        let mut residual_or_children = vec![non_pushable_x.clone(), pushable_b.clone()];
        residual_or_children.sort();
        let residual_part = or(residual_or_children);

        let expected_plan = {
            let scan = Arc::new(LogicalPlan::CollectionScan {
                collection: 123,
                projection: None,
                filter: Some(pushable_part),
                sort: None,
            });
            Arc::new(LogicalPlan::Filter {
                input: scan,
                condition: residual_part,
            })
        };

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_no_push_down_when_or_distribution_is_too_large() {
        // 9 * 8 = 72 > 64, should not distribute.
        let conjuncts1: Vec<_> = (0..9)
            .map(|i| field_filters(field(["a"]), [eq(lit(i))]))
            .collect();
        let mut conjuncts2: Vec<_> = (10..17)
            .map(|i| field_filters(field(["b"]), [eq(lit(i))]))
            .collect();
        // Add a non-pushable part to one of the ANDs
        conjuncts2.push(field_filters(field(["c"]), [size(lit(2), false)]));

        let filter = or([and(conjuncts1), and(conjuncts2)]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan.clone());

        // Since it's not distributed, and one part of the OR is not pushable,
        // the whole OR is residual. Plan should be unchanged.
        assert_eq!(normalized_plan, plan);
    }

    #[test]
    fn test_de_morgan_nor_with_multiple_operands() {
        let original = nor([eq(lit(1)), gt(lit(2)), lt(lit(0))]);
        let transformed = and([not(eq(lit(1))), not(gt(lit(2))), not(lt(lit(0)))]);
        check_expr_transformation(DeMorganNorToAnd {}, original, transformed);
    }

    #[test]
    fn test_de_morgan_nor_in_field_filters() {
        let original = field_filters(field(["a"]), [nor([eq(lit(1)), gt(lit(2))])]);
        let transformed =
            field_filters(field(["a"]), [and([not(eq(lit(1))), not(gt(lit(2)))])]);
        check_expr_transformation(DeMorganNorToAnd {}, original, transformed);
    }

    #[test]
    fn test_push_down_not_type_and_size() {
        check_expr_transformation(
            PushDownNotExpressions {},
            not(has_type(lit("string"), false)),
            has_type(lit("string"), true),
        );
        check_expr_transformation(
            PushDownNotExpressions {},
            not(size(lit(2), false)),
            size(lit(2), true),
        );
    }

    #[test]
    fn test_or_canonical_ordering() {
        let p1 = eq(lit(1));
        let p2 = gt(lit(2));
        let p3 = lt(lit(3));
        // Build in non-sorted order
        let original = or([p2.clone(), p3.clone(), p1.clone()]);
        // Expect sorted order
        let mut expected_exprs = vec![p1, p2, p3];
        expected_exprs.sort();
        let transformed = or(expected_exprs);
        check_expr_transformation(SimplifyLogicalOperators {}, original, transformed);
    }

    #[test]
    fn test_simplify_empty_and_or() {
        check_expr_transformation(
            SimplifyLogicalOperators {},
            and(vec![]),
            Arc::new(Expr::AlwaysTrue),
        );
        check_expr_transformation(
            SimplifyLogicalOperators {},
            or(vec![]),
            Arc::new(Expr::AlwaysFalse),
        );
    }

    #[test]
    fn test_eliminate_always_false_filter_propagation() {
        let plan = LogicalPlanBuilder::scan(123)
            .filter(Arc::new(Expr::AlwaysFalse))
            .project(include(proj_fields(Vec::<(PathComponent, Arc<ProjectionExpr>)>::new())))
            .sort(Arc::new(vec![]))
            .limit(None, Some(10))
            .build();

        let rule = EliminateRedundantFilter {};
        let normalized_plan = rule.apply(plan);

        // The whole plan above the scan should collapse to NoOp
        let expected_plan = Arc::new(LogicalPlan::NoOp);

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_for_literal_comparison() {
        let filter = field_filters(field(["a"]), [interval(Interval::closed(lit(0), lit(0)))]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();
        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan.clone());

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            projection: None,
            filter: Some(filter.clone()),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_combine_comparisons_to_interval() {
        // a > 5 AND a < 10  => a in (5, 10)
        let original = field_filters(field(["a"]), [gt(lit(5)), lt(lit(10))]);
        let transformed = field_filters(
            field(["a"]),
            [interval(Interval::open(lit(5), lit(10)))],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // a >= 5 AND a <= 10 => a in [5, 10]
        let original = field_filters(field(["a"]), [gte(lit(5)), lte(lit(10))]);
        let transformed = field_filters(
            field(["a"]),
            [interval(Interval::closed(lit(5), lit(10)))],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // a > 5 AND a < 3 => FALSE
        let original = field_filters(field(["a"]), [gt(lit(5)), lt(lit(3))]);
        let transformed = Arc::new(Expr::AlwaysFalse);
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // a > 5 AND a == 7 => a in [7, 7]
        let original = field_filters(field(["a"]), [gt(lit(5)), eq(lit(7))]);
        let transformed = field_filters(
            field(["a"]),
            [interval(Interval::closed(lit(7), lit(7)))],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // a > 5 AND a != 7 => remains as is, but a > 5 becomes interval
        let original = field_filters(field(["a"]), [gt(lit(5)), ne(lit(7))]);
        let mut filters = vec![
            ne(lit(7)),
            interval(Interval::greater_than(lit(5))),
        ];
        filters.sort();
        let transformed = Arc::new(Expr::FieldFilters {
            field: field(["a"]),
            filters,
        });
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // No change if no suitable comparisons
        let original = field_filters(field(["a"]), [ne(lit(5)), nin(lit(vec![1, 2]))]);
        check_expr_transformation(CombineComparisonsToInterval {}, original.clone(), original);
    }

    #[test]
    fn test_merge_field_filters_and_combine_to_interval() {
        // This test checks the interaction of two rules:
        // 1. SimplifyLogicalOperators should merge multiple FieldFilters on the same field under an AND.
        // 2. CombineComparisonsToInterval should then convert the combined comparisons into an Interval.
        let original_filter = and([
            field_filters(field(["b"]), [eq(lit(100))]),
            field_filters(field(["a"]), [lt(lit(10))]),
            field_filters(field(["a"]), [gte(lit(5))]),
        ]);

        // We need a plan to apply rules to.
        let plan = scan_with_filter(original_filter);

        // Apply rules in sequence, as the optimizer would.
        let plan_after_simplify = SimplifyLogicalOperators {}.apply(plan);
        let final_plan = CombineComparisonsToInterval {}.apply(plan_after_simplify);

        // Define the expected outcome.
        // The comparisons on "a" should become a single interval [5, 10).
        let filter_a = field_filters(
            field(["a"]),
            [interval(Interval::closed_open(lit(5), lit(10)))],
        );
        // The comparisons on "a" should become a single interval (100, 100).
        let filter_b = field_filters(
            field(["b"]),
            [interval(Interval::closed(lit(100), lit(100)))]);

        // The final expression should be an AND of the two, in canonical (sorted) order.
        let mut expected_filters = vec![filter_a, filter_b];
        expected_filters.sort();
        let expected_filter = and(expected_filters);

        let expected_plan = scan_with_filter(expected_filter);

        assert_eq!(final_plan, expected_plan);
    }

    #[test]
    fn test_coalesce_intersect_in_clauses() {
        let original = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 3, 2])), // Unsorted
                within(lit(vec![4, 3, 2])), // Unsorted
            ],
        );
        // Intersection is [2, 3], which should be sorted.
        let transformed = field_filters(field(["a"]), [within(lit(vec![2, 3]))]);
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_intersect_in_clauses_empty_result() {
        let original = field_filters(
            field(["a"]),
            [within(lit(vec![1, 2])), within(lit(vec![3, 4]))],
        );
        let transformed = Arc::new(Expr::AlwaysFalse);
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_in_with_interval() {
        // This test requires multiple rules to run in sequence.
        let original_filter = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 12, 5, 10])), // Unsorted
                gt(lit(3)),
                lte(lit(10)),
            ],
        );

        let plan = scan_with_filter(original_filter);

        let plan_after_combine = CombineComparisonsToInterval {}.apply(plan);
        let final_plan = CoalesceFieldPredicates {}.apply(plan_after_combine);

        // Expected: gt(3) and lte(10) become interval (3, 10].
        // Then IN is filtered by interval, resulting in [5, 10], which should be sorted.
        let expected_filter = field_filters(field(["a"]), [within(lit(vec![5, 10]))]);
        let expected_plan = scan_with_filter(expected_filter);

        assert_eq!(final_plan, expected_plan);
    }

    #[test]
    fn test_coalesce_in_with_interval_empty_result() {
        let original_filter =
            field_filters(field(["a"]), [within(lit(vec![1, 2, 3])), gt(lit(5))]);

        let plan = scan_with_filter(original_filter);

        let plan_after_combine = CombineComparisonsToInterval {}.apply(plan);
        let final_plan = CoalesceFieldPredicates {}.apply(plan_after_combine);

        let expected_plan = scan_with_filter(Arc::new(Expr::AlwaysFalse));
        assert_eq!(final_plan, expected_plan);
    }

    #[test]
    fn test_coalesce_remove_redundant_exists_true() {
        let rules = CompositeRule::new(vec![
            Arc::new(CombineComparisonsToInterval {}),
            Arc::new(CoalesceFieldPredicates {}),
        ]);

        // With IN
        let original_in = field_filters(field(["a"]), [within(lit(vec![1, 2])), exists(true)]);
        let transformed_in = field_filters(field(["a"]), [within(lit(vec![1, 2]))]);
        check_expr_transformation(rules.clone(), original_in, transformed_in);

        // With Interval
        let original_interval = field_filters(field(["a"]), [exists(true), gt(lit(5))]);
        let transformed_interval =
            field_filters(field(["a"]), [interval(Interval::greater_than(lit(5)))]);
        check_expr_transformation(rules.clone(), original_interval, transformed_interval);
    }

    #[test]
    fn test_coalesce_remove_redundant_exists_true_with_other_filters() {
        // With IN and another filter
        let original = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 2])),
                nin(lit(vec![3])),
                exists(true),
            ],
        );
        // exists(true) is dropped, other filters are kept and sorted
        let transformed =
            field_filters(field(["a"]), [within(lit(vec![1, 2])), nin(lit(vec![3]))]);
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_keep_exists_true() {
        // exists(true) should be kept if there are no other sargable predicates like IN or Interval.
        // The rule also ensures canonical ordering of filters.
        let original = field_filters(field(["a"]), [exists(true), nin(lit(vec![1, 2]))]);

        // Expected order after sort is nin then exists
        let transformed = field_filters(field(["a"]), [nin(lit(vec![1, 2])), exists(true)]);

        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_in_with_unbounded_interval() {
        let rules = CompositeRule::new(vec![
            Arc::new(CombineComparisonsToInterval {}),
            Arc::new(CoalesceFieldPredicates {}),
        ]);

        // a > 5
        let original = field_filters(field(["a"]), [within(lit(vec![1, 10, 6])), gt(lit(5))]);
        let transformed = field_filters(field(["a"]), [within(lit(vec![6, 10]))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // a <= 6
        let original = field_filters(field(["a"]), [within(lit(vec![10, 1, 6])), lte(lit(6))]);
        let transformed = field_filters(field(["a"]), [within(lit(vec![1, 6]))]);
        check_expr_transformation(rules, original, transformed);
    }

    #[test]
    fn test_coalesce_canonical_in_list() {
        // The rule should sort the values inside an IN list for canonical representation,
        // even if no other coalescing happens.
        let original = field_filters(field(["a"]), [within(lit(vec![3, 1, 2]))]);
        let transformed = field_filters(field(["a"]), [within(lit(vec![1, 2, 3]))]);
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_disjunctions_intervals() {
        let rules = CompositeRule::new(vec![
            Arc::new(CombineComparisonsToInterval {}),
            Arc::new(CoalesceSameFieldDisjunctions {}),
        ]);

        // OR(a < 5, a < 10) => a < 10
        let original = or([
            field_filters(field(["a"]), [lt(lit(5))]),
            field_filters(field(["a"]), [lt(lit(10))]),
        ]);
        let transformed =
            field_filters(field(["a"]), [interval(Interval::less_than(lit(10)))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // OR(a > 5, a > 2) => a > 2
        let original = or([
            field_filters(field(["a"]), [gt(lit(5))]),
            field_filters(field(["a"]), [gt(lit(2))]),
        ]);
        let transformed =
            field_filters(field(["a"]), [interval(Interval::greater_than(lit(2)))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // OR([1, 5], [3, 7]) => [1, 7]
        let original = or([
            field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(5)))]),
            field_filters(field(["a"]), [interval(Interval::closed(lit(3), lit(7)))]),
        ]);
        let transformed =
            field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(7)))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // Disjoint intervals are not coalesced
        let original = or([
            field_filters(field(["a"]), [interval(Interval::at_most(lit(2)))]),
            field_filters(
                field(["a"]),
                [interval(Interval::greater_than(lit(3)))],
            ),
        ]);
        check_expr_transformation(rules.clone(), original.clone(), original);
    }

    #[test]
    fn test_coalesce_disjunctions_in_lists() {
        // OR(a IN [1,2], a IN [2,3]) => a IN [1,2,3]
        let original = or([
            field_filters(field(["a"]), [within(lit(vec![1, 2]))]),
            field_filters(field(["a"]), [within(lit(vec![2, 3]))]),
        ]);
        let transformed = field_filters(field(["a"]), [within(lit(vec![1, 2, 3]))]);
        check_expr_transformation(CoalesceSameFieldDisjunctions {}, original, transformed);
    }

    #[test]
    fn test_coalesce_disjunctions_exists() {
        // OR(a > 5, exists(true)) => exists(true)
        let original = or([
            field_filters(field(["a"]), [gt(lit(5))]),
            field_filters(field(["a"]), [exists(true)]),
        ]);
        let transformed = field_filters(field(["a"]), [exists(true)]);

        let rules = CompositeRule::new(vec![
            Arc::new(CombineComparisonsToInterval {}),
            Arc::new(CoalesceSameFieldDisjunctions {}),
        ]);
        check_expr_transformation(rules, original, transformed);
    }

    #[test]
    fn test_coalesce_no_change() {
        // A single interval should not be changed.
        let original_interval =
            field_filters(field(["a"]), [interval(Interval::at_least(lit(5)))]);
        check_expr_transformation(
            CoalesceFieldPredicates {},
            original_interval.clone(),
            original_interval,
        );

        // A single IN with sorted values should not be changed.
        let original_in = field_filters(field(["a"]), [within(lit(vec![1, 2, 3]))]);
        check_expr_transformation(CoalesceFieldPredicates {}, original_in.clone(), original_in);
    }

    #[test]
    fn test_full_pipeline_simplification() {
        // Original: NOR( a > 10, OR(b < 5, b < 2), NOT(c == 1) )
        // After DeMorgan: AND( NOT(a > 10), NOT(OR(b < 5, b < 2)), NOT(NOT(c == 1)) )
        // After PushDownNot: AND( a <= 10, AND(b >= 5, b >= 2), c == 1 )
        // After Simplify: AND( a <= 10, b >= 5, c == 1 )
        // After Combine + Pushdown: Scan with filter AND( a <= 10, b >= 5, c == 1 ) converted to intervals
        let original = nor([
            field_filters(field(["a"]), [gt(lit(10))]),
            // Use OR to test disjunction coalescing. OR(b<5, b<2) -> b<5. NOT(b<5) -> b>=5
            or([
                field_filters(field(["b"]), [lt(lit(5))]),
                field_filters(field(["b"]), [lt(lit(2))]),
            ]),
            not(field_filters(field(["c"]), [eq(lit(1))])),
        ]);

        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let filter_a = field_filters(
            field(["a"]),
            [interval(Interval::at_most(lit(10)))],
        );
        let filter_b =
            field_filters(field(["b"]), [interval(Interval::at_least(lit(5)))]);
        let filter_c = field_filters(
            field(["c"]),
            [interval(Interval::closed(lit(1), lit(1)))],
        );

        let mut expected_filters = vec![filter_a, filter_b, filter_c];
        expected_filters.sort();
        let expected_filter = and(expected_filters);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 22,
            projection: None,
            filter: Some(expected_filter),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_full_pipeline_coalesce_and_interval() {
        // Original: AND( a > 5, a < 15, a IN [1, 8, 20], exists(true) )
        // After Simplify: FieldFilters(a, [gt(5), lt(15), in([1,8,20]), exists(true)])
        // After Combine: FieldFilters(a, [interval((5, 15)), in([1,8,20]), exists(true)])
        // After Coalesce: FieldFilters(a, [in([8])])
        // After Pushdown: Scan with filter
        let original = and([
            field_filters(field(["a"]), [gt(lit(5)), lt(lit(15))]),
            field_filters(
                field(["a"]),
                [within(lit(vec![1, 8, 20])), exists(true)],
            ),
        ]);
        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let expected_filter = field_filters(field(["a"]), [within(lit(vec![8]))]);
        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 22,
            projection: None,
            filter: Some(expected_filter),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_full_pipeline_contradiction_to_noop() {
        // Original: AND( a > 10, a < 5 )
        // After Combine: AlwaysFalse
        // After EliminateFilter: NoOp plan
        let original = field_filters(field(["a"]), [gt(lit(10)), lt(lit(5))]);
        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let expected_plan = Arc::new(LogicalPlan::NoOp);
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_full_pipeline_partial_pushdown() {
        // Original: AND( a > 10, size(b) == 2 )
        // `a > 10` is pushable. `size(b) == 2` is not.
        // Expected: Scan with filter on `a` and residual Filter operator for `b`.
        let original = and([
            field_filters(field(["a"]), [gt(lit(10))]),
            field_filters(field(["b"]), [size(lit(2), false)]),
        ]);
        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let pushable =
            field_filters(field(["a"]), [interval(Interval::greater_than(lit(10)))]);
        let residual = field_filters(field(["b"]), [size(lit(2), false)]);

        let expected_scan = Arc::new(LogicalPlan::CollectionScan {
            collection: 22,
            projection: None,
            filter: Some(pushable),
            sort: None,
        });
        let expected_plan = Arc::new(LogicalPlan::Filter {
            input: expected_scan,
            condition: residual,
        });

        assert_eq!(normalized_plan, expected_plan);
    }
}
