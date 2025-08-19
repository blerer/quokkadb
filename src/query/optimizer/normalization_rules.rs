use crate::query::logical_plan::LogicalPlan;
use crate::query::{ComparisonOperator, Expr, Projection, ProjectionExpr};
use crate::query::optimizer::optimizer::NormalisationRule;
use crate::query::tree_node::TreeNode;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bson::Bson;

const MAX_OR_CARTESIAN_PRODUCT: usize = 64;

/// Returns the list of normalization rules to be applied in sequence.
pub fn all_normalization_rules() -> Vec<Arc<dyn NormalisationRule>> {
    vec![
        Arc::new(DeMorganNorToAnd {}),
        Arc::new(PushDownNotExpressions {}),
        Arc::new(SimplifyLogicalOperators {}),
        Arc::new(EliminateRedundantFilter {}),
        Arc::new(PushDownFiltersToScan {}),
    ]
}

/// Normalization rule to apply De Morgan's Law for NOR expressions in logical plans.
/// This rule transforms NOR expressions into AND expressions with negated conditions.
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

        // Apply constant folding, deduplication, and contradiction detection
        let mut result: Vec<Arc<Expr>> = Vec::new();
        let mut seen: HashSet<Arc<Expr>> = HashSet::new();

        for e in flattened {
            match e.as_ref() {
                Expr::AlwaysTrue => continue, // Drop TRUE
                Expr::AlwaysFalse => return Arc::new(Expr::AlwaysFalse), // A AND FALSE => FALSE
                _ => {}
            }

            let neg = e.negate();
            if seen.contains(&neg) {
                // A AND NOT A => FALSE
                return Arc::new(Expr::AlwaysFalse);
            }

            if seen.insert(e.clone()) {
                // Keep first occurrence only (remove duplicates)
                result.push(e);
            }
        }
        // Sort the result to ensure consistent ordering (important when comparing queries shape
        result.sort();
        match result.len() {
            0 => Arc::new(Expr::AlwaysTrue), // AND() == TRUE
            1 => result.remove(0),
            _ => Arc::new(Expr::And(result)),
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

/// Normalization rule to eliminate redundant filters.
/// A filter with an `AlwaysTrue` condition is removed.
/// A filter with an `AlwaysFalse` condition replaces the subplan with a `NoOp` node.
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

/// Merges multiple expressions into a single AND expression, combining `FieldFilters` on the same field.
fn merge_and_clauses(exprs: &[Arc<Expr>]) -> Arc<Expr> {

    // Filter out AlwaysTrue, which is the identity for AND
    let exprs: Vec<_> = exprs
        .into_iter()
        .filter(|e| !matches!(e.as_ref(), Expr::AlwaysTrue))
        .cloned()
        .collect();

    if exprs.is_empty() {
        return Arc::new(Expr::AlwaysTrue);
    }

    // Group FieldFilters by field
    let mut field_filters_map: HashMap<Arc<Expr>, Vec<Arc<Expr>>> = HashMap::new();
    let mut other_exprs = Vec::new();

    for expr in exprs {
        if let Expr::FieldFilters { field, filters } = expr.as_ref() {
            field_filters_map
                .entry(field.clone())
                .or_default()
                .extend(filters.clone());
        } else {
            other_exprs.push(expr);
        }
    }

    // Reconstruct expressions, with merged FieldFilters
    let mut merged_exprs = other_exprs;
    for (field, filters) in field_filters_map {
        if !filters.is_empty() {
            merged_exprs.push(Arc::new(Expr::FieldFilters {
                field,
                filters,
            }));
        }
    }

    // Sort for deterministic plan shape
    merged_exprs.sort();

    match merged_exprs.len() {
        0 => Arc::new(Expr::AlwaysTrue),
        1 => merged_exprs.into_iter().next().unwrap(),
        _ => Arc::new(Expr::And(merged_exprs)),
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

            let pushable = merge_and_clauses(&pushable_parts);
            let residual = merge_and_clauses(&residual_parts);

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
        Expr::Comparison { operator, value } => {
            matches!(value.as_ref(), Expr::Literal(_))
                && matches!(
                    operator,
                    ComparisonOperator::Eq
                        | ComparisonOperator::Lt
                        | ComparisonOperator::Lte
                        | ComparisonOperator::Gt
                        | ComparisonOperator::Gte
                        | ComparisonOperator::In
                )
        }
        Expr::Exists(true) => true,
        Expr::Type {
            negated: false,
            bson_type,
        } => matches!(bson_type.as_ref(), Expr::Literal(_)),
        _ => false,
    }
}

/// Transforms the logical plan in a bottom-up way by applying a function to all filter expressions,
/// including those nested within projection expressions.
fn transform_up_filter<F>(plan: Arc<LogicalPlan>, function: F) -> Arc<LogicalPlan>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Sync + Clone,
{
    plan.transform_up(&|node: Arc<LogicalPlan>| match node.as_ref() {
        LogicalPlan::Filter { input, condition } => {
            let expr = condition.clone().transform_up(&|c| function(c));
            Arc::new(LogicalPlan::Filter {
                input: input.clone(),
                condition: expr,
            })
        }
        LogicalPlan::Projection { input, projection } => {
            let projection = Arc::new(match projection.as_ref() {
                Projection::Include(proj_exprs) => {
                    let new_projection = transform_up_proj_expr_filters(&function, &proj_exprs);
                    Projection::Include(new_projection)
                }
                Projection::Exclude(proj_exprs) => {
                    let new_projection = transform_up_proj_expr_filters(&function, &proj_exprs);
                    Projection::Exclude(new_projection)
                }
            });

            Arc::new(LogicalPlan::Projection {
                input: input.clone(),
                projection,
            })
        }
        _ => node,
    })
}

fn transform_up_proj_expr_filters<F>(function: F, proj_exprs: &Arc<ProjectionExpr>) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Sync + Clone,
{
    proj_exprs.clone().transform_up(&|c| transform_up_proj_elem_match_filter(c, function.clone()))
}

fn transform_up_proj_elem_match_filter<F>(
    proj_expr: Arc<ProjectionExpr>,
    function: F,
) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Sync + Clone,
{
    match proj_expr.as_ref() {
        ProjectionExpr::ElemMatch { filter } => {
            let new_expr = filter.clone().transform_up(&|c| function(c));
            Arc::new(ProjectionExpr::ElemMatch{ filter: new_expr })
        }
        _ => proj_expr.clone(),
    }
}

/// Transforms the logical plan in a top-down way by applying a function to all filter expressions,
/// including those nested within projection expressions.
fn transform_down_filter<F>(plan: Arc<LogicalPlan>, function: F) -> Arc<LogicalPlan>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Sync + Clone,
{
    plan.transform_down(&|node: Arc<LogicalPlan>| {
        match node.as_ref() {
            LogicalPlan::Filter { input, condition } => {
                let expr = condition.clone().transform_down(&function);
                Arc::new(LogicalPlan::Filter {
                    input: input.clone(),
                    condition: expr,
                })
            }
            LogicalPlan::Projection { input, projection } => {
                let projection = Arc::new(match projection.as_ref() {
                    Projection::Include(proj_exprs) => {
                        let new_projection = transform_down_proj_expr_filters(&function, proj_exprs);
                        Projection::Include(new_projection)
                    }
                    Projection::Exclude(proj_exprs) => {
                        let new_projection = transform_down_proj_expr_filters(&function, proj_exprs);
                        Projection::Exclude(new_projection)
                    }
                });

                Arc::new(LogicalPlan::Projection {
                    input: input.clone(),
                    projection,
                })
            }
            _ => node.clone(),
        }
    })
}

fn transform_down_proj_expr_filters<F>(
    function: &F,
    proj_exprs: &Arc<ProjectionExpr>,
) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Sync + Clone,
{
    proj_exprs
        .clone()
        .transform_down(&|c| transform_down_proj_elem_match_filter(c, function))
}

fn transform_down_proj_elem_match_filter<F>(
    proj_expr: Arc<ProjectionExpr>,
    function: &F,
) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Sync + Clone,
{
    match proj_expr.as_ref() {
        ProjectionExpr::ElemMatch { filter } => {
            let new_expr = filter.clone().transform_down(function);
            Arc::new(ProjectionExpr::ElemMatch { filter: new_expr })
        }
        _ => proj_expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::expr_fn::*;
    use crate::query::logical_plan::LogicalPlanBuilder;
    use std::sync::Arc;
    use crate::query::PathComponent;

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
        // NOR(A, NOT A) should become FALSE
        // 1. DeMorgan: NOR(A, NOT A) -> AND(NOT A, NOT(NOT A))
        // 2. Pushdown: AND(NOT A, A)
        // 3. Simplify: FALSE
        let p = eq(lit(1));
        let original_plan = scan_with_filter(nor([p.clone(), not(p)]));

        // Apply rules in sequence
        let plan_after_demorgan = DeMorganNorToAnd {}.apply(original_plan);
        let plan_after_pushdown = PushDownNotExpressions {}.apply(plan_after_demorgan);
        let final_plan = SimplifyLogicalOperators {}.apply(plan_after_pushdown);

        let expected_plan = scan_with_filter(Arc::new(Expr::AlwaysFalse));
        assert_eq!(expected_plan, final_plan);
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
        let filter = field_filters(field(["a"]), [eq(lit(1))]);
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
        let pushable_filter = field_filters(field(["a"]), [eq(lit(1))]);
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
        let filter1 = field_filters(field(["a"]), [eq(lit(1))]);
        let filter2 = field_filters(field(["b"]), [gt(lit(10))]);
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
        let filter = field_filters(field(["a"]), [eq(lit(1)), size(lit(2), false)]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter.clone()).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let pushable = field_filters(field(["a"]), [eq(lit(1))]);
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
    fn test_push_down_merge_field_filters_from_and() {
        let filter = and([
            field_filters(field(["a"]), [eq(lit(1))]),
            field_filters(field(["a"]), [gt(lit(0))]),
        ]);
        let plan = LogicalPlanBuilder::scan(123).filter(filter).build();

        let rule = PushDownFiltersToScan {};
        let normalized_plan = rule.apply(plan);

        let expected_plan = Arc::new(LogicalPlan::CollectionScan {
                collection: 123,
                projection: None,
                filter: Some(field_filters(field(["a"]), [eq(lit(1)), gt(lit(0))])),
                sort: None,
        });

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
        let a_eq = field_filters(field(["a"]), [eq(lit(1))]);
        let b_gt = field_filters(field(["b"]), [gt(lit(2))]);
        let c_eq = field_filters(field(["c"]), [eq(lit(3))]);

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
        let pushable_a = field_filters(field(["a"]), [eq(lit(1))]);
        let non_pushable_x = field_filters(field(["x"]), [size(lit(2), false)]);
        let pushable_b = field_filters(field(["b"]), [gt(lit(10))]);

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
        let filter = field_filters(field(["a"]), [eq(lit(0))]);
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
}
