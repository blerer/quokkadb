//! # Query Normalization Rules
//!
//! This module implements a sequence of normalization rules that are applied to a logical query plan
//! to simplify it and prepare it for optimization and execution. The rules are applied in a specific
//! order to ensure correctness and enable subsequent simplifications within a single pipeline pass.
//! Each rule is applied once, in order; individual rules perform their own tree traversals.
//!
//! QuokkaDB prioritizes low normalization overhead as a lightweight embedded database. Normalization
//! does not restart the pipeline or repeat it until no further changes occur (a fixed point).
//! This means some further simplifications may remain after the pass. New rules should account for
//! dependencies on earlier and later rules through their ordering, rather than introduce repeated
//! pipeline execution. Idempotence on repeated application is not part of the pipeline contract.
//!
//! The examples below show expression shapes rather than Rust constructors. In the examples, `A`,
//! `B`, and `C` are predicates, and `x` is a field. An interval is written using mathematical
//! bounds, for example `x in (5, +inf)` means `x > 5`.
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
//!     range negations remain structural because arrays may have multiple witnesses. This removes negations
//!     at higher levels.
//!
//! 3.  **`SimplifyLogicalOperators`**: This is a crucial cleanup rule that works from the bottom up.
//!     It flattens nested `AND` and `OR` expressions, removes duplicate conditions, and performs
//!     constant folding (e.g., `A AND TRUE` becomes `A`). It also detects contradictions
//!     (`A AND NOT A` becomes `FALSE`) and tautologies (`A OR NOT A` becomes `TRUE`), which can
//!     significantly simplify or even eliminate parts of the query.
//!
//! 4.  **`CombineComparisonsToInterval`**: Converts each comparison to its own interval.
//!     Separate predicates retain separate witnesses, including on array-valued fields.
//!
//! 5.  **`CoalesceSameFieldDisjunctions`**: This rule simplifies `OR` expressions that contain
//!     multiple predicates on the same field. It unions overlapping or adjacent intervals, merges
//!     `$in` lists by taking the union of their values, and simplifies redundant conditions (e.g.,
//!     `x > 5 OR x > 2` becomes `x > 2`). This reduces the complexity of disjunctive predicates,
//!     potentially enabling further optimizations.
//!
//! 6.  **`CoalesceFieldPredicates`**: Canonicalizes each `$in` list separately and
//!     removes `$exists: true` only when another predicate requires a present value.
//!     Independent predicates are never intersected: array elements may provide
//!     different witnesses for each predicate.
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
//!
//! ## Expression-level examples
//!
//! ```text
//! NOR(A, B)                         -> AND(NOT(A), NOT(B))
//! NOT(AND(A, B))                    -> OR(NOT(A), NOT(B))
//! AND(A, TRUE, A)                   -> A
//! AND(A, NOT(A))                    -> FALSE
//! OR(A, FALSE, A)                   -> A
//! OR(A, NOT(A))                     -> TRUE
//! x > 5 AND x < 10                  -> x in (5, +inf) AND x in (-inf, 10)
//! x > 5 OR x > 10                   -> x > 5
//! x IN [3, 1, 3]                    -> x IN [1, 3]
//! Filter(TRUE, scan)                -> scan
//! Filter(FALSE, scan)               -> NoOp
//! ```
//!
//! The interval example is intentionally not an intersection. A document such as
//! `{ x: [1, 10] }` may satisfy `x > 5` with one array element and `x < 10` with another.
//! Rewriting those predicates to `x in (5, 10)` would incorrectly require one element to satisfy
//! both conditions.
//!
//! Similarly, `NOT(x > 5)` remains structural rather than becoming `x <= 5`, because arrays and
//! missing fields do not have the scalar complement semantics required for that rewrite. The same
//! care applies to `$all`, `$size`, `$elemMatch`, array equality, and `$in`/`$nin`: these operations
//! are not interchangeable with ordinary scalar intervals.
//!
//! Filter pushdown preserves a residual expression when the scan cannot evaluate every predicate:
//!
//! ```text
//! Filter(AND(x > 5, SIZE(x) = 2), CollectionScan)
//!     -> Filter(SIZE(x) = 2, CollectionScan(filter = x > 5))
//! ```
//!
//! An `OR` containing a residual predicate remains residual as a whole, so pushdown does not
//! change the query's boolean or array semantics.
use crate::query::logical_plan::{LogicalPlan, transform_down_filter, transform_up_filter};
use crate::query::tree_node::TreeNode;
use crate::query::{BsonValue, BsonValueRef, ComparisonOperator, Expr};
use crate::util::interval::Interval;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bson::Bson;

const MAX_OR_CARTESIAN_PRODUCT: usize = 64;

pub trait NormalisationRule: Send + Sync {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan>;
}

/// Returns the ordered normalization pipeline, which applies each rule once per invocation.
///
/// The returned composite applies the rules in the same order described in the module
/// documentation. For example, `NOR(x > 5, x < 1)` becomes
/// `AND(NOT(x > 5), NOT(x < 1))` before later simplification and pushdown.
/// Rules have their own tree traversals; the pipeline does not iterate to a fixed point.
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

/// Applies a sequence of rules once in the supplied order.
///
/// Each rule receives the previous rule's output. Rules are not revisited when a later rule
/// exposes another simplification opportunity; callers must choose the order accordingly.
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
        self.rules.iter().fold(plan, |p, rule| rule.apply(p))
    }
}

/// Normalization rule to apply De Morgan's Law for NOR expressions in logical plans.
/// This rule transforms NOR expressions into AND expressions with negated conditions.
///
/// ```text
/// NOR(A, B, C) -> AND(NOT(A), NOT(B), NOT(C))
/// ```
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
                    conditions
                        .iter()
                        .map(|c| Arc::new(Expr::Not(c.clone())))
                        .collect(),
                ))
            }
            _ => expr.clone(),
        }
    }
}

/// Normalization rule to simplify NOT expressions in logical plans.
///
/// ```text
/// NOT(AND(A, B)) -> OR(NOT(A), NOT(B))
/// NOT(OR(A, B))  -> AND(NOT(A), NOT(B))
/// NOT(EXISTS(x)) -> NOT-EXISTS(x)
/// ```
///
/// Range comparisons remain wrapped in `NOT`: `NOT(x > 5)` is not rewritten to `x <= 5`,
/// because arrays and missing fields do not have the required scalar complement semantics.
#[derive(Clone)]
pub struct PushDownNotExpressions;

impl NormalisationRule for PushDownNotExpressions {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        transform_down_filter(plan, &|expr| Self::push_down_not(expr))
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
///
/// ```text
/// AND(A, AND(B, TRUE), A) -> AND(A, B)
/// OR(A, OR(B, FALSE), A)  -> OR(A, B)
/// AND(A, NOT(A))           -> FALSE
/// OR(A, NOT(A))            -> TRUE
/// ```
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
            Expr::Comparison {
                operator: ComparisonOperator::In | ComparisonOperator::Nin,
                ..
            }
            | Expr::All(_) => canonicalize_set_predicate(expr),
            Expr::Not(child) => match child.as_ref() {
                Expr::AlwaysTrue => Arc::new(Expr::AlwaysFalse),
                Expr::AlwaysFalse => Arc::new(Expr::AlwaysTrue),
                _ => expr,
            },
            Expr::Nor(_) => {
                unreachable!("De Morgan's Law should have been applied before this rule")
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
        let mut field_filters_map: BTreeMap<Arc<Expr>, Vec<Arc<Expr>>> = BTreeMap::new();
        let mut other_exprs: Vec<Arc<Expr>> = Vec::new();

        for e in flattened {
            match e.as_ref() {
                Expr::AlwaysTrue => continue,                            // Drop TRUE
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
                let merged = Self::simplify_field_filters(&field, &filters);
                match merged.as_ref() {
                    Expr::AlwaysFalse => return merged,
                    Expr::AlwaysTrue => {}
                    _ => result.push(merged),
                }
            }
        }

        // Apply deduplication and contradiction detection
        let mut final_result: Vec<Arc<Expr>> = Vec::new();
        let mut seen: BTreeSet<Arc<Expr>> = BTreeSet::new();

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
        remove_absorbed(&mut final_result, true);
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
        let mut seen: BTreeSet<Arc<Expr>> = BTreeSet::new();

        for e in flattened {
            match e.as_ref() {
                Expr::AlwaysTrue => return Arc::new(Expr::AlwaysTrue), // A OR TRUE => TRUE
                Expr::AlwaysFalse => continue,                         // Drop FALSE
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
        remove_absorbed(&mut result, false);
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
        let mut seen: BTreeSet<Arc<Expr>> = BTreeSet::new();

        for e in filters {
            match e.as_ref() {
                Expr::AlwaysTrue => continue,                            // Drop TRUE
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
        if result
            .iter()
            .any(|filter| matches!(filter.as_ref(), Expr::Exists(false)))
            && result.iter().any(|filter| predicate_requires_value(filter))
        {
            return Arc::new(Expr::AlwaysFalse);
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

/// Converts range/equality comparisons into independent interval expressions.
/// Combining them would incorrectly require a single array element to satisfy all predicates.
///
/// ```text
/// x = 5  -> x in [5, 5]
/// x >= 5 -> x in [5, +inf)
/// x < 10 -> x in (-inf, 10)
/// ```
///
/// When several predicates address an array-valued field, each interval remains independent:
/// `x > 5 AND x < 10` is not collapsed into the single interval `x in (5, 10)`.
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
            // Each predicate may match a different array element. Preserve its
            // own interval until scalar or same-element semantics are proven.
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
                        other_filters.push(Arc::new(Expr::Interval(interval)));
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

/// Canonicalizes field predicates without intersecting independent array matches.
///
/// This rule canonicalizes each set predicate separately and removes predicates that are implied
/// by a narrower numeric interval, while preserving independent witnesses:
///
/// ```text
/// x IN [3, 1, 3] AND x IN [2, 1] -> x IN [1, 3] AND x IN [1, 2]
/// x > 5 AND x > 10               -> x > 10
/// ```
///
/// It deliberately does not intersect independent array predicates. An array such as `[1, 10]`
/// can satisfy `x < 5 AND x > 5`, so rewriting that expression as an unsatisfiable scalar range
/// would change its meaning.
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
            // Canonicalize each set separately: independent predicates may
            // have different array elements as witnesses.
            let mut filters: Vec<_> = filters
                .iter()
                .cloned()
                .map(canonicalize_set_predicate)
                .collect();
            if filters
                .iter()
                .any(|filter| predicate_requires_value(filter))
            {
                if filters
                    .iter()
                    .any(|filter| matches!(filter.as_ref(), Expr::Exists(false)))
                {
                    return Arc::new(Expr::AlwaysFalse);
                }
                filters.retain(|filter| !matches!(filter.as_ref(), Expr::Exists(true)));
            }
            filters.sort();
            filters.dedup();

            // A narrower numeric interval supplies a witness for a containing
            // interval, even when another predicate needs an independent witness.
            // Never intersect independent predicates: that would change array matches.
            let intervals: Vec<_> = filters
                .iter()
                .filter_map(|filter| match filter.as_ref() {
                    Expr::Interval(interval) if interval_has_numeric_bounds(interval) => {
                        Some(interval.clone())
                    }
                    _ => None,
                })
                .collect();
            filters.retain(|filter| match filter.as_ref() {
                Expr::Interval(interval) if interval_has_numeric_bounds(interval) => !intervals
                    .iter()
                    .any(|other| other != interval && interval.contains_interval(other)),
                _ => true,
            });
            SimplifyLogicalOperators::simplify_field_filters(field, &filters)
        } else {
            match expr.as_ref() {
                Expr::And(_) | Expr::Or(_) | Expr::Not(_) => {
                    SimplifyLogicalOperators::simplify_operators(expr)
                }
                _ => expr,
            }
        }
    }
}

/// Normalization rule to coalesce multiple disjunctive predicates on the same field
/// into a single predicate where possible. This rule unions multiple `$in` clauses,
/// unions overlapping or adjacent intervals, and handles `Exists(true)` subsumption.
///
/// ```text
/// (x > 5) OR (x > 10)             -> x > 5
/// (x IN [1, 2]) OR (x IN [2, 3])   -> x IN [1, 2, 3]
/// (x > 5) OR (x < 1)               -> (x > 5) OR (x < 1)
/// ```
///
/// Only compatible numeric intervals are unioned. Array equality and set predicates are kept in
/// their original operation family instead of being converted into scalar ranges.
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
            let mut field_filters_map: BTreeMap<Arc<Expr>, Vec<Arc<Expr>>> = BTreeMap::new();
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
                for filter in Self::coalesce_same_field_disjuncts(&filters) {
                    new_disjuncts.push(SimplifyLogicalOperators::simplify_field_filters(
                        &field,
                        &[filter],
                    ));
                }
            }

            SimplifyLogicalOperators::simplify_or(&new_disjuncts)
        } else {
            expr.clone()
        }
    }

    /// Coalesces compatible subsets while preserving other alternatives.
    fn coalesce_same_field_disjuncts(filters: &[Arc<Expr>]) -> Vec<Arc<Expr>> {
        let has_exists = filters
            .iter()
            .any(|filter| matches!(filter.as_ref(), Expr::Exists(true)));
        let mut result = Vec::new();
        let mut membership = None;
        let mut intervals = Vec::new();
        for filter in filters {
            if has_exists && predicate_requires_value(filter) {
                continue;
            }
            match filter.as_ref() {
                Expr::Comparison {
                    operator: ComparisonOperator::In,
                    value,
                } if matches!(value.as_ref(), Expr::Literal(BsonValue(Bson::Array(_)))) => {
                    membership = match membership {
                        None => Some(filter.clone()),
                        Some(previous) => coalesce_in_disjunctions(&previous, filter),
                    };
                }
                // Numeric intervals share element-wise comparison semantics.
                // Array/null equality must not be folded into a range.
                Expr::Interval(interval) if interval_has_numeric_bounds(interval) => {
                    intervals.push(interval.clone())
                }
                _ => result.push(filter.clone()),
            }
        }
        intervals.sort();
        let mut merged: Vec<Interval<Arc<Expr>>> = Vec::new();
        for interval in intervals {
            if let Some(previous) = merged.last_mut()
                && let Some(union) = previous.union(&interval)
            {
                *previous = union;
            } else {
                merged.push(interval);
            }
        }
        result.extend(
            merged
                .into_iter()
                .map(|interval| Arc::new(Expr::Interval(interval))),
        );
        if let Some(membership) = membership {
            result.push(canonicalize_set_predicate(membership));
        }
        result
    }
}

/// Canonicalizes literal set operands without touching placeholders or scalar literals.
fn canonicalize_set_predicate(expr: Arc<Expr>) -> Arc<Expr> {
    let value = match expr.as_ref() {
        Expr::Comparison {
            operator: ComparisonOperator::In | ComparisonOperator::Nin,
            value,
        }
        | Expr::All(value) => value,
        _ => return expr,
    };
    let Expr::Literal(BsonValue(Bson::Array(values))) = value.as_ref() else {
        return expr;
    };
    if values.is_empty() {
        return Arc::new(
            if matches!(
                expr.as_ref(),
                Expr::Comparison {
                    operator: ComparisonOperator::Nin,
                    ..
                }
            ) {
                Expr::AlwaysTrue
            } else {
                Expr::AlwaysFalse
            },
        );
    }
    let values: BTreeSet<_> = values.iter().map(BsonValueRef).collect();
    let value = Arc::new(Expr::Literal(BsonValue::from(values)));
    Arc::new(match expr.as_ref() {
        Expr::Comparison { operator, .. } => Expr::Comparison {
            operator: *operator,
            value,
        },
        Expr::All(_) => Expr::All(value),
        _ => unreachable!(),
    })
}

/// Structural implication only: leaves are compared exactly, without range reasoning.
fn structurally_implies(left: &Arc<Expr>, right: &Arc<Expr>) -> bool {
    if left == right {
        return true;
    }
    match (left.as_ref(), right.as_ref()) {
        (_, Expr::And(children)) => children
            .iter()
            .all(|child| structurally_implies(left, child)),
        (Expr::Or(children), _) => children
            .iter()
            .all(|child| structurally_implies(child, right)),
        (Expr::And(children), _) => children
            .iter()
            .any(|child| structurally_implies(child, right)),
        (_, Expr::Or(children)) => children
            .iter()
            .any(|child| structurally_implies(left, child)),
        (
            Expr::FieldFilters {
                field: left_field,
                filters: left_filters,
            },
            Expr::FieldFilters {
                field: right_field,
                filters: right_filters,
            },
        ) => {
            left_field == right_field
                && right_filters
                    .iter()
                    .all(|filter| left_filters.contains(filter))
        }
        _ => false,
    }
}

fn remove_absorbed(expressions: &mut Vec<Arc<Expr>>, conjunction: bool) {
    if expressions.len() < 2 {
        return;
    }
    let retained: Vec<_> = expressions
        .iter()
        .enumerate()
        .filter_map(|(index, expr)| {
            let redundant = expressions.iter().enumerate().any(|(other_index, other)| {
                if index == other_index {
                    return false;
                }
                let (stronger, weaker) = if conjunction {
                    (other, expr)
                } else {
                    (expr, other)
                };
                structurally_implies(stronger, weaker)
                    && (other_index < index || !structurally_implies(weaker, stronger))
            });
            (!redundant).then(|| expr.clone())
        })
        .collect();
    *expressions = retained;
}

fn predicate_requires_value(expr: &Expr) -> bool {
    match expr {
        Expr::Type { negated: false, .. }
        | Expr::Size { negated: false, .. }
        | Expr::ElemMatch(_) => true,
        Expr::Comparison {
            operator: ComparisonOperator::Eq,
            value,
        } => {
            matches!(value.as_ref(), Expr::Literal(BsonValue(value)) if !matches!(value, Bson::Null))
        }
        Expr::Comparison {
            operator:
                ComparisonOperator::Gt
                | ComparisonOperator::Gte
                | ComparisonOperator::Lt
                | ComparisonOperator::Lte,
            ..
        } => true,
        Expr::Interval(interval) => {
            !interval.is_point()
                || !matches!(
                    interval.start_bound_value().as_deref(),
                    Some(Expr::Literal(BsonValue(Bson::Null)))
                )
        }
        Expr::Comparison {
            operator: ComparisonOperator::In,
            value,
        } => matches!(value.as_ref(),
            Expr::Literal(BsonValue(Bson::Array(values))) if !values.iter().any(|value| matches!(value, Bson::Null))),
        _ => false,
    }
}

fn interval_has_numeric_bounds(interval: &Interval<Arc<Expr>>) -> bool {
    [interval.start_bound_value(), interval.end_bound_value()]
        .into_iter()
        .flatten()
        .all(|bound| {
            matches!(
                bound.as_ref(),
                Expr::Literal(BsonValue(
                    Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
                ))
            )
        })
}

fn coalesce_in_disjunctions(left: &Arc<Expr>, right: &Arc<Expr>) -> Option<Arc<Expr>> {
    let (
        Expr::Comparison {
            operator: ComparisonOperator::In,
            value: left_value,
        },
        Expr::Comparison {
            operator: ComparisonOperator::In,
            value: right_value,
        },
    ) = (left.as_ref(), right.as_ref())
    else {
        unreachable!("coalesce_in_disjunctions should only be called with two IN expressions");
    };

    let (
        Expr::Literal(BsonValue(Bson::Array(left_arr))),
        Expr::Literal(BsonValue(Bson::Array(right_arr))),
    ) = (left_value.as_ref(), right_value.as_ref())
    else {
        unreachable!("IN values must be arrays");
    };

    let mut combined: BTreeSet<BsonValueRef> =
        left_arr.into_iter().map(|e| BsonValueRef(e)).collect();
    combined.extend(right_arr.into_iter().map(|e| BsonValueRef(e)));

    Some(Arc::new(Expr::Comparison {
        operator: ComparisonOperator::In,
        value: Arc::new(Expr::Literal(BsonValue::from(combined))),
    }))
}

/// Normalization rule to eliminate redundant filters.
/// A filter with an `AlwaysTrue` condition is removed.
/// A filter with an `AlwaysFalse` condition replaces the sub-plan with a `NoOp` node.
///
/// ```text
/// Filter(TRUE, input)  -> input
/// Filter(FALSE, input) -> NoOp
/// ```
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
///
/// ```text
/// Filter(AND(x > 5, SIZE(x) = 2), CollectionScan)
///     -> Filter(SIZE(x) = 2, CollectionScan(filter = x > 5))
/// ```
///
/// Comparisons, intervals, `$in`, `$exists: true`, and positive `$type` predicates can be pushed
/// down. Operations such as `$size` and `$elemMatch` remain residual. If an `OR` contains any
/// residual predicate, the entire `OR` remains residual so that boolean and array semantics are
/// preserved.
#[derive(Clone)]
pub struct PushDownFiltersToScan;

impl NormalisationRule for PushDownFiltersToScan {
    fn apply(&self, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        plan.transform_up(&|node| {
            if let LogicalPlan::Filter { input, condition } = node.as_ref() {
                if let LogicalPlan::CollectionScan {
                    collection,
                    hint,
                    projection,
                    filter: scan_filter,
                    sort,
                } = input.as_ref()
                {
                    assert_eq!(
                        scan_filter, &None,
                        "Scan filter should be None before pushing down"
                    );

                    let condition = distribute_or(condition.clone());
                    let (pushable, residual) = split_pushable(&condition);

                    // If nothing can be pushed down, return the original plan node.
                    if matches!(pushable.as_ref(), Expr::AlwaysTrue) {
                        return node.clone();
                    }

                    let new_scan = Arc::new(LogicalPlan::CollectionScan {
                        collection: *collection,
                        hint: hint.clone(),
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
        Expr::Comparison {
            operator: ComparisonOperator::In,
            value,
        } if matches!(value.as_ref(), Expr::Literal(_)) => true,
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
    use crate::query::{PathComponent, ProjectionExpr};
    use crate::util::interval::Interval;
    use std::sync::Arc;

    #[test]
    fn normalization_preserves_array_matches_and_parameter_binding() {
        use crate::query::execution::to_filter;
        use crate::query::{Parameters, parser};
        use bson::{Document, doc};
        use std::cell::RefCell;

        fn matches(plan: &LogicalPlan, doc: &Document) -> bool {
            let evaluate = |expr: &Arc<Expr>| {
                let parameters = RefCell::new(Parameters::new());
                let bound = expr.clone().transform_up(&|expr| match expr.as_ref() {
                    Expr::Literal(value) => {
                        parameters.borrow_mut().collect_parameter(value.clone())
                    }
                    _ => expr,
                });
                to_filter(bound, &parameters.into_inner())(doc)
            };
            match plan {
                LogicalPlan::CollectionScan { filter, .. } => filter.as_ref().is_none_or(evaluate),
                LogicalPlan::Filter { input, condition } => {
                    matches(input, doc) && evaluate(condition)
                }
                LogicalPlan::NoOp => false,
                _ => panic!("unexpected test plan: {plan:?}"),
            }
        }
        let documents = [
            doc! {},
            doc! {"a": null},
            doc! {"a": []},
            doc! {"a": 1},
            doc! {"a": 7},
            doc! {"a": [7, 1]},
            doc! {"a": 10.0},
            doc! {"a": [3, 10]},
            doc! {"a": "string"},
            doc! {"a": {"b": 1}},
            doc! {"a": [1, 10]},
            doc! {"a": [[1, 10]]},
            doc! {"a": [null, 1]},
            doc! {"a": [5, 5_i64, 5.0]},
            doc! {"items": [{"x": 1}, {"y": 10}]},
            doc! {"items": [{"x": 1, "y": 10}]},
        ];
        let queries = [
            doc! {"$and": [{"a": 1}, {"a": {"$ne": 1}}]},
            doc! {"a": {"$eq": 7, "$gt": 5, "$lt": 3}},
            doc! {"a": {"$gt": 5, "$gte": 10}},
            doc! {"a": {"$gt": 5, "$gte": 5}},
            doc! {"a": {"$all": [7, 1, 7]}},
            doc! {"a": {"$not": {"$all": []}}},
            doc! {"a": {"$nin": [5, 1, 5_i64, 5.0]}},
            doc! {"a": {"$exists": false, "$size": 0}},
            doc! {"a": {"$exists": false, "$type": "null"}},
            doc! {"a": {"$exists": false, "$elemMatch": {"$gt": 0}}},
            doc! {"a": {"$exists": false, "$eq": null}},
            doc! {"a": {"$exists": false, "$nin": [1]}},
            doc! {"$or": [{"a": 1}, {"a": 1, "b": 2}]},
            doc! {"$and": [{"a": 1}, {"$or": [{"a": 1}, {"b": 2}]}]},
            doc! {"$or": [{"a": {"$gt": 8}}, {"a": {"$in": [1]}}, {"a": {"$in": [5]}}]},
            doc! {"$or": [{"a": {"$exists": true}}, {"a": {"$gt": 8}}, {"a": null}]},
            doc! {"$or": [{"a": {"$lt": 5}}, {"a": {"$lte": 3}}, {"a": {"$gt": 8}}]},
            doc! {"items.x": {"$exists": false, "$type": "int"}},
            doc! {"items.x": {"$exists": false, "$eq": null}},
            doc! {"items.x": {"$all": [1, 1]}},
            doc! {"a": {"$gt": 8, "$lt": 3}},
            doc! {"a": {"$elemMatch": {"$gt": 8, "$lt": 3}}},
            doc! {"a": [1, 10]},
            doc! {"a": []},
            doc! {"a": {"$gt": [1]}},
            doc! {"a": {"$ne": 1}},
            doc! {"a": {"$nin": [null, 5]}},
            doc! {"a": {"$not": {"$gt": 8, "$lt": 3}}},
            doc! {"a": {"$not": {"$all": [1, 10]}}},
            doc! {"a": {"$all": []}},
            doc! {"$and": [{"a": {"$in": [1]}}, {"a": {"$in": [10]}}]},
            doc! {"a": {"$in": [1], "$gt": 8}},
            doc! {"a": {"$in": [null], "$exists": true}},
            doc! {"$or": [{"a": null}, {"a": {"$exists": true}}]},
            doc! {"$or": [{"a": {"$in": [null]}}, {"a": {"$exists": true}}]},
            doc! {"$or": [{"a": {"$gt": 8}}, {"a": {"$in": [1]}}, {"a": {"$in": [5]}}]},
            doc! {"items.x": 1, "items.y": 10},
            doc! {"items": {"$elemMatch": {"x": 1, "y": 10}}},
            doc! {"items.x": {"$ne": 1}},
            doc! {"items.x": {"$exists": false}},
            doc! {"items.x": null},
            doc! {"$nor": [{"a": {"$gt": 8, "$lt": 3}}]},
        ];
        for query in queries {
            let original = scan_with_filter(parser::parse_conditions(&query).unwrap());
            let normalized = all_normalization_rules().apply(original.clone());
            for document in &documents {
                assert_eq!(
                    matches(&original, document),
                    matches(&normalized, document),
                    "query {query:?}, document {document:?}, normalized {normalized:?}"
                );
            }
        }
    }

    #[test]
    fn test_not_normalization() {
        // Test Double Negation: NOT(NOT(X)) → X
        let original = not(not(eq(lit(5))));
        let transformed = eq(lit(5));

        check_expr_transformation(PushDownNotExpressions {}, original, transformed);

        // Equality complements are safe; range negation must remain structural.
        check_expr_transformation(PushDownNotExpressions {}, not(gt(lit(5))), not(gt(lit(5))));
        check_expr_transformation(PushDownNotExpressions {}, not(lt(lit(5))), not(lt(lit(5))));
        check_expr_transformation(PushDownNotExpressions {}, not(eq(lit(5))), ne(lit(5)));
        check_expr_transformation(PushDownNotExpressions {}, not(ne(lit(5))), eq(lit(5)));

        // Test De Morgan's Laws
        let original = not(and([eq(lit(1)), gt(lit(2))]));
        let transformed = or([ne(lit(1)), not(gt(lit(2)))]);

        check_expr_transformation(PushDownNotExpressions {}, original, transformed);

        let original = not(or([eq(lit(1)), lt(lit(2))]));
        let transformed = and([ne(lit(1)), not(lt(lit(2)))]);

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

    #[test]
    fn merged_field_predicates_detect_contradictions() {
        check_expr_transformation(
            SimplifyLogicalOperators,
            and([
                field_filters(field(["a"]), [eq(lit(1))]),
                field_filters(field(["a"]), [ne(lit(1))]),
            ]),
            Arc::new(Expr::AlwaysFalse),
        );
    }

    #[test]
    fn absorption_handles_boolean_and_merged_field_conjunctions() {
        let a = field_filters(field(["a"]), [eq(lit(1))]);
        for b in [
            field_filters(field(["b"]), [eq(lit(2))]),
            field_filters(field(["a"]), [gt(lit(0))]),
        ] {
            check_expr_transformation(
                SimplifyLogicalOperators,
                or([a.clone(), and([a.clone(), b.clone()])]),
                a.clone(),
            );
            check_expr_transformation(
                SimplifyLogicalOperators,
                and([a.clone(), or([a.clone(), b])]),
                a.clone(),
            );
        }
    }

    #[test]
    fn conjunction_removes_only_containing_numeric_intervals() {
        for (weaker, stronger) in [
            (Interval::greater_than(lit(5)), Interval::at_least(lit(10))),
            (Interval::at_least(lit(5)), Interval::greater_than(lit(5))),
            (Interval::at_most(lit(5)), Interval::less_than(lit(5))),
            (Interval::at_least(lit(5)), Interval::at_least(lit(5_i64))),
        ] {
            let stronger = interval(stronger);
            check_expr_transformation(
                CoalesceFieldPredicates,
                field_filters(field(["a"]), [interval(weaker), stronger.clone()]),
                field_filters(field(["a"]), [stronger]),
            );
        }
    }

    #[test]
    fn disjunction_coalesces_compatible_subsets() {
        let rules = CompositeRule::new(vec![
            Arc::new(CombineComparisonsToInterval),
            Arc::new(CoalesceSameFieldDisjunctions),
        ]);
        let input = or([
            field_filters(field(["a"]), [lt(lit(5))]),
            field_filters(field(["a"]), [lt(lit(3))]),
            field_filters(field(["a"]), [gt(lit(8))]),
            field_filters(field(["a"]), [within(lit(vec![1, 2]))]),
            field_filters(field(["a"]), [within(lit(vec![2, 7]))]),
            field_filters(field(["a"]), [size(lit(2), false)]),
        ]);
        let expected = SimplifyLogicalOperators::simplify_or(&[
            field_filters(field(["a"]), [interval(Interval::less_than(lit(5)))]),
            field_filters(field(["a"]), [interval(Interval::greater_than(lit(8)))]),
            field_filters(field(["a"]), [within(lit(vec![1, 2, 7]))]),
            field_filters(field(["a"]), [size(lit(2), false)]),
        ]);
        check_expr_transformation(rules, input, expected);
    }

    #[test]
    fn disjunction_handles_empty_membership_and_touching_bounds() {
        check_expr_transformation(
            CoalesceSameFieldDisjunctions,
            or([
                field_filters(field(["a"]), [within(lit(Vec::<i32>::new()))]),
                field_filters(field(["a"]), [within(lit(vec![1]))]),
            ]),
            field_filters(field(["a"]), [within(lit(vec![1]))]),
        );
        let left = field_filters(field(["a"]), [interval(Interval::less_than(lit(5)))]);
        let right = field_filters(field(["a"]), [interval(Interval::greater_than(lit(5)))]);
        let disjoint = or([left.clone(), right]);
        check_expr_transformation(CoalesceSameFieldDisjunctions, disjoint.clone(), disjoint);
        check_expr_transformation(
            CoalesceSameFieldDisjunctions,
            or([
                left,
                field_filters(field(["a"]), [interval(Interval::at_least(lit(5)))]),
            ]),
            field_filters(field(["a"]), [interval(Interval::all())]),
        );
    }

    #[test]
    fn disjunction_presence_subsumes_only_present_alternatives() {
        let null = field_filters(
            field(["a"]),
            [interval(Interval::closed(lit(Bson::Null), lit(Bson::Null)))],
        );
        let present = field_filters(field(["a"]), [exists(true)]);
        let expected = SimplifyLogicalOperators::simplify_or(&[null.clone(), present.clone()]);
        check_expr_transformation(
            CoalesceSameFieldDisjunctions,
            or([
                null,
                present,
                field_filters(field(["a"]), [interval(Interval::greater_than(lit(5)))]),
            ]),
            expected,
        );
    }

    #[test]
    fn set_operands_are_canonical_and_empty_all_folds() {
        for constructor in [within, nin, all] {
            check_expr_transformation(
                SimplifyLogicalOperators,
                field_filters(field(["a"]), [constructor(lit(vec![3, 1, 3, 2]))]),
                field_filters(field(["a"]), [constructor(lit(vec![1, 2, 3]))]),
            );
        }
        check_expr_transformation(
            SimplifyLogicalOperators,
            field_filters(field(["a"]), [all(lit(Vec::<i32>::new()))]),
            Arc::new(Expr::AlwaysFalse),
        );
        check_expr_transformation(
            SimplifyLogicalOperators,
            field_filters(field(["a"]), [not(all(lit(Vec::<i32>::new())))]),
            Arc::new(Expr::AlwaysTrue),
        );
    }

    #[test]
    fn missing_field_contradictions_respect_null_and_negation() {
        for predicate in [
            eq(lit(1)),
            gt(lit(1)),
            within(lit(vec![1])),
            size(lit(0), false),
            Arc::new(Expr::Type {
                bson_type: lit("null"),
                negated: false,
            }),
            elem_match([gt(lit(1))]),
        ] {
            check_expr_transformation(
                SimplifyLogicalOperators,
                field_filters(field(["a"]), [exists(false), predicate]),
                Arc::new(Expr::AlwaysFalse),
            );
        }
        for predicate in [
            eq(lit(Bson::Null)),
            within(lit(vec![Bson::Null])),
            nin(lit(vec![1])),
            size(lit(0), true),
        ] {
            let mut filters = vec![exists(false), predicate];
            filters.sort();
            let input = field_filters(field(["a"]), filters);
            check_expr_transformation(SimplifyLogicalOperators, input.clone(), input);
        }
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
        LogicalPlanBuilder::scan(collection, None)
            .filter(filter)
            .build_arc()
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
            field_filters(field(["b"]), [eq(lit(2))]),
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
            Arc::new(DeMorganNorToAnd {}),
            Arc::new(PushDownNotExpressions {}),
            Arc::new(SimplifyLogicalOperators {}),
        ]);
        check_expr_transformation(rules, original, transformed);
    }

    #[test]
    fn test_normalization_in_projection_elem_match() {
        // An expression inside an ElemMatch that can be simplified
        let p = eq(lit(1));
        let original_filter = or([p.clone(), not(p.clone())]); // Should be AlwaysTrue

        let projection = Some(include(proj_elem_match(original_filter)));

        let plan = LogicalPlanBuilder::scan(123, None)
            .project(projection)
            .build_arc();

        // Apply simplification rule
        let rule = SimplifyLogicalOperators {};
        let normalized_plan = rule.apply(plan);

        // Check that the filter inside ElemMatch was simplified
        let expected_filter = Arc::new(Expr::AlwaysTrue);
        let expected_projection = Some(include(proj_elem_match(expected_filter)));
        let expected_plan = LogicalPlanBuilder::scan(123, None)
            .project(expected_projection)
            .build_arc();

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_eliminate_always_true_filter() {
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(Arc::new(Expr::AlwaysTrue))
            .build_arc();

        let rule = EliminateRedundantFilter {};
        let normalized_plan = rule.apply(plan);

        // The filter should be removed, leaving only the scan
        let expected_plan = LogicalPlanBuilder::scan(123, None).build_arc();

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_eliminate_always_false_filter() {
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(Arc::new(Expr::AlwaysFalse))
            .limit(None, Some(10)) // Adding a limit to ensure we have a non-terminal plan
            .build_arc();

        let rule = EliminateRedundantFilter {};
        let normalized_plan = rule.apply(plan);

        // The filter should be replaced by NoOp
        let expected_plan = Arc::new(LogicalPlan::NoOp);

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_simple_filter() {
        let filter = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            hint: None,
            projection: None,
            filter: Some(filter),
            sort: None,
        });
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_filter_preserves_scan_hint() {
        let filter = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let plan = LogicalPlanBuilder::scan(123, Some(17))
            .filter(filter.clone())
            .build_arc();

        let normalized_plan = PushDownFiltersToScan {}.apply(plan);

        assert!(matches!(
            normalized_plan.as_ref(),
            LogicalPlan::CollectionScan {
                hint: Some(17),
                filter: Some(scan_filter),
                ..
            } if scan_filter == &filter
        ));
    }

    #[test]
    fn test_push_down_partial_and() {
        let pushable_filter =
            field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let residual_filter = field_filters(field(["b"]), [size(lit(2), false)]);
        let filter = and([pushable_filter.clone(), residual_filter.clone()]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter)
            .build_arc();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = {
            let scan = Arc::new(LogicalPlan::CollectionScan {
                collection: 123,
                hint: None,
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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            hint: None,
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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan.clone());

        // The OR has a non-pushable part, so nothing is pushed down.
        assert_eq!(normalized_plan, plan);
    }

    #[test]
    fn test_no_push_down_for_non_scannable_filter() {
        let filter = field_filters(field(["a"]), [size(lit(2), false)]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();

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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();

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
            hint: None,
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
        assert_eq!(distribute_or(original_empty_or.clone()), original_empty_or);
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
        let filter = field_filters(
            field(["a"]),
            [
                interval(Interval::closed(lit(1), lit(1))),
                size(lit(2), false),
            ],
        );
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let pushable = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let residual = field_filters(field(["a"]), [size(lit(2), false)]);

        let expected_plan = {
            let scan = Arc::new(LogicalPlan::CollectionScan {
                collection: 123,
                hint: None,
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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);

        // negated $type is not pushable
        let filter = field_filters(field(["b"]), [has_type(lit("string"), true)]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);
    }

    #[test]
    fn test_push_down_in_with_non_empty_array() {
        let filter = field_filters(field(["a"]), [within(lit(vec![1, 2, 3]))]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();
        let normalized_plan = PushDownFiltersToScan {}.apply(plan);
        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            hint: None,
            projection: None,
            filter: Some(filter),
            sort: None,
        });
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_no_push_down_for_elem_match_and_all() {
        let filter = field_filters(field(["a"]), [elem_match([eq(lit(1))])]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);

        let filter = field_filters(field(["b"]), [all(lit(vec![1, 2]))]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();
        assert_eq!(PushDownFiltersToScan {}.apply(plan.clone()), plan);
    }

    #[test]
    fn test_push_down_or_unlocked_by_distribution() {
        let a_eq = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let b_gt = field_filters(field(["b"]), [interval(Interval::greater_than(lit(2)))]);
        let c_eq = field_filters(field(["c"]), [interval(Interval::closed(lit(3), lit(3)))]);

        // OR(AND(A, B), C) which is pushable after distribution
        let filter = or([and([a_eq.clone(), b_gt.clone()]), c_eq.clone()]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter)
            .build_arc();
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
            hint: None,
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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter)
            .build_arc();
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
                hint: None,
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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter)
            .build_arc();

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
        let transformed = field_filters(field(["a"]), [and([not(eq(lit(1))), not(gt(lit(2)))])]);
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
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(Arc::new(Expr::AlwaysFalse))
            .project(Some(include(proj_fields(Vec::<(
                PathComponent,
                Arc<ProjectionExpr>,
            )>::new()))))
            .sort(Some(Arc::new(vec![])))
            .limit(None, Some(10))
            .build_arc();

        let rule = EliminateRedundantFilter {};
        let normalized_plan = rule.apply(plan);

        // The whole plan above the scan should collapse to NoOp
        let expected_plan = Arc::new(LogicalPlan::NoOp);

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_push_down_for_literal_comparison() {
        let filter = field_filters(field(["a"]), [interval(Interval::closed(lit(0), lit(0)))]);
        let plan = LogicalPlanBuilder::scan(123, None)
            .filter(filter.clone())
            .build_arc();
        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan.clone());

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 123,
            hint: None,
            projection: None,
            filter: Some(filter.clone()),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_combine_comparisons_to_interval() {
        // Separate range predicates may match different array elements.
        let original = field_filters(field(["a"]), [gt(lit(5)), lt(lit(10))]);
        let transformed = field_filters(
            field(["a"]),
            [
                interval(Interval::less_than(lit(10))),
                interval(Interval::greater_than(lit(5))),
            ],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // Inclusive bounds also retain separate witnesses.
        let original = field_filters(field(["a"]), [gte(lit(5)), lte(lit(10))]);
        let transformed = field_filters(
            field(["a"]),
            [
                interval(Interval::at_most(lit(10))),
                interval(Interval::at_least(lit(5))),
            ],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // Disjoint ranges are satisfiable by an array such as [1, 10].
        let original = field_filters(field(["a"]), [gt(lit(5)), lt(lit(3))]);
        let transformed = field_filters(
            field(["a"]),
            [
                interval(Interval::less_than(lit(3))),
                interval(Interval::greater_than(lit(5))),
            ],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // Equality does not subsume another predicate on an array.
        let original = field_filters(field(["a"]), [gt(lit(5)), eq(lit(7))]);
        let transformed = field_filters(
            field(["a"]),
            [
                interval(Interval::greater_than(lit(5))),
                interval(Interval::closed(lit(7), lit(7))),
            ],
        );
        check_expr_transformation(CombineComparisonsToInterval {}, original, transformed);

        // a > 5 AND a != 7 => remains as is, but a > 5 becomes interval
        let original = field_filters(field(["a"]), [gt(lit(5)), ne(lit(7))]);
        let mut filters = vec![ne(lit(7)), interval(Interval::greater_than(lit(5)))];
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
        // 2. CombineComparisonsToInterval preserves independent intervals.
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
        // The comparisons on "a" retain separate intervals.
        let filter_a = field_filters(
            field(["a"]),
            [
                interval(Interval::less_than(lit(10))),
                interval(Interval::at_least(lit(5))),
            ],
        );
        // Equality on "b" becomes a point interval.
        let filter_b = field_filters(
            field(["b"]),
            [interval(Interval::closed(lit(100), lit(100)))],
        );

        // The final expression should be an AND of the two, in canonical (sorted) order.
        let mut expected_filters = vec![filter_a, filter_b];
        expected_filters.sort();
        let expected_filter = and(expected_filters);

        let expected_plan = scan_with_filter(expected_filter);

        assert_eq!(final_plan, expected_plan);
    }

    #[test]
    fn test_coalesce_preserves_independent_in_clauses() {
        let original = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 3, 2])), // Unsorted
                within(lit(vec![4, 3, 2])), // Unsorted
            ],
        );
        // Sort each list without intersecting independent matches.
        let transformed = field_filters(
            field(["a"]),
            [within(lit(vec![1, 2, 3])), within(lit(vec![2, 3, 4]))],
        );
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_preserves_disjoint_in_clauses() {
        let original = field_filters(
            field(["a"]),
            [within(lit(vec![1, 2])), within(lit(vec![3, 4]))],
        );
        let transformed = original.clone();
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

        // Preserve each witness while canonicalizing the IN alternatives.
        let expected_filter = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 5, 10, 12])),
                interval(Interval::at_most(lit(10))),
                interval(Interval::greater_than(lit(3))),
            ],
        );
        let expected_plan = scan_with_filter(expected_filter);

        assert_eq!(final_plan, expected_plan);
    }

    #[test]
    fn test_coalesce_preserves_disjoint_membership_and_range() {
        let original_filter = field_filters(field(["a"]), [within(lit(vec![1, 2, 3])), gt(lit(5))]);

        let plan = scan_with_filter(original_filter);

        let plan_after_combine = CombineComparisonsToInterval {}.apply(plan);
        let final_plan = CoalesceFieldPredicates {}.apply(plan_after_combine);

        let expected_plan = scan_with_filter(field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 2, 3])),
                interval(Interval::greater_than(lit(5))),
            ],
        ));
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
            [within(lit(vec![1, 2])), nin(lit(vec![3])), exists(true)],
        );
        // exists(true) is dropped, other filters are kept and sorted
        let transformed = field_filters(field(["a"]), [within(lit(vec![1, 2])), nin(lit(vec![3]))]);
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
        let transformed = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 6, 10])),
                interval(Interval::greater_than(lit(5))),
            ],
        );
        check_expr_transformation(rules.clone(), original, transformed);

        // a <= 6
        let original = field_filters(field(["a"]), [within(lit(vec![10, 1, 6])), lte(lit(6))]);
        let transformed = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 6, 10])),
                interval(Interval::at_most(lit(6))),
            ],
        );
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
        let transformed = field_filters(field(["a"]), [interval(Interval::less_than(lit(10)))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // OR(a > 5, a > 2) => a > 2
        let original = or([
            field_filters(field(["a"]), [gt(lit(5))]),
            field_filters(field(["a"]), [gt(lit(2))]),
        ]);
        let transformed = field_filters(field(["a"]), [interval(Interval::greater_than(lit(2)))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // OR([1, 5], [3, 7]) => [1, 7]
        let original = or([
            field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(5)))]),
            field_filters(field(["a"]), [interval(Interval::closed(lit(3), lit(7)))]),
        ]);
        let transformed = field_filters(field(["a"]), [interval(Interval::closed(lit(1), lit(7)))]);
        check_expr_transformation(rules.clone(), original, transformed);

        // Disjoint intervals are not coalesced
        let original = or([
            field_filters(field(["a"]), [interval(Interval::at_most(lit(2)))]),
            field_filters(field(["a"]), [interval(Interval::greater_than(lit(3)))]),
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
        let original_interval = field_filters(field(["a"]), [interval(Interval::at_least(lit(5)))]);
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
    fn test_coalesce_range_containing_equality_point() {
        let original = field_filters(
            field(["a"]),
            [
                interval(Interval::greater_than(lit(5))),
                interval(Interval::closed(lit(7), lit(7))),
            ],
        );
        let transformed = field_filters(field(["a"]), [interval(Interval::closed(lit(7), lit(7)))]);
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);

        let original = field_filters(
            field(["a"]),
            [
                interval(Interval::at_most(lit(10))),
                interval(Interval::closed(lit(7), lit(7))),
            ],
        );
        let transformed = field_filters(field(["a"]), [interval(Interval::closed(lit(7), lit(7)))]);
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_coalesce_does_not_intersect_disjoint_array_predicates() {
        let original = field_filters(
            field(["a"]),
            [
                interval(Interval::greater_than(lit(5))),
                interval(Interval::less_than(lit(3))),
                interval(Interval::closed(lit(7), lit(7))),
            ],
        );
        let transformed = field_filters(
            field(["a"]),
            [
                interval(Interval::less_than(lit(3))),
                interval(Interval::closed(lit(7), lit(7))),
            ],
        );
        check_expr_transformation(CoalesceFieldPredicates {}, original, transformed);
    }

    #[test]
    fn test_full_pipeline_simplification() {
        // Original: NOR( a > 10, OR(b < 5, b < 2), NOT(c == 1) )
        // After DeMorgan: AND( NOT(a > 10), NOT(OR(b < 5, b < 2)), NOT(NOT(c == 1)) )
        // Range negations stay above the scan; equality remains pushable.
        let original = nor([
            field_filters(field(["a"]), [gt(lit(10))]),
            // Negated ranges retain their separate predicates.
            or([
                field_filters(field(["b"]), [lt(lit(5))]),
                field_filters(field(["b"]), [lt(lit(2))]),
            ]),
            not(field_filters(field(["c"]), [eq(lit(1))])),
        ]);

        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let filter_a = field_filters(field(["a"]), [not(gt(lit(10)))]);
        let filter_b = field_filters(field(["b"]), [not(lt(lit(2))), not(lt(lit(5)))]);
        let filter_c = field_filters(field(["c"]), [interval(Interval::closed(lit(1), lit(1)))]);
        let expected_plan = Arc::new(LogicalPlan::Filter {
            input: Arc::new(LogicalPlan::CollectionScan {
                collection: 22,
                hint: None,
                projection: None,
                filter: Some(filter_c),
                sort: None,
            }),
            condition: and([filter_a, filter_b]),
        });
        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_full_pipeline_coalesce_and_interval() {
        // Original: AND( a > 5, a < 15, a IN [1, 8, 20], exists(true) )
        // After Simplify: FieldFilters(a, [gt(5), lt(15), in([1,8,20]), exists(true)])
        // After Combine: independent intervals and membership remain.
        // Coalescing removes only the redundant existence check.
        // After Pushdown: Scan with filter
        let original = and([
            field_filters(field(["a"]), [gt(lit(5)), lt(lit(15))]),
            field_filters(field(["a"]), [within(lit(vec![1, 8, 20])), exists(true)]),
        ]);
        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let expected_filter = field_filters(
            field(["a"]),
            [
                within(lit(vec![1, 8, 20])),
                interval(Interval::less_than(lit(15))),
                interval(Interval::greater_than(lit(5))),
            ],
        );
        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 22,
            hint: None,
            projection: None,
            filter: Some(expected_filter),
            sort: None,
        });

        assert_eq!(normalized_plan, expected_plan);
    }

    #[test]
    fn test_full_pipeline_preserves_disjoint_array_bounds() {
        // Original: AND( a > 10, a < 5 )
        // Different elements can satisfy each bound; do not replace the scan with NoOp.
        let original = field_filters(field(["a"]), [gt(lit(10)), lt(lit(5))]);
        let plan = scan_with_filter(original);
        let normalized_plan = all_normalization_rules().apply(plan);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
            collection: 22,
            hint: None,
            projection: None,
            sort: None,
            filter: Some(field_filters(
                field(["a"]),
                [
                    interval(Interval::less_than(lit(5))),
                    interval(Interval::greater_than(lit(10))),
                ],
            )),
        });
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

        let pushable = field_filters(field(["a"]), [interval(Interval::greater_than(lit(10)))]);
        let residual = field_filters(field(["b"]), [size(lit(2), false)]);

        let expected_scan = Arc::new(LogicalPlan::CollectionScan {
            collection: 22,
            hint: None,
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
