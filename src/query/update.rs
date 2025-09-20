use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;
use crate::query::Expr;
use crate::Error;

/// Represents a component in an update path, which can include positional operators.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UpdatePathComponent {
    /// A named field (e.g., "field" in "document.field").
    FieldName(String),
    /// An array index (e.g., "0" in "array.0").
    ArrayElement(usize),
    /// The first positional operator (`$`).
    FirstMatch,
    /// The all positional operator (`$[]`).
    AllElements,
    /// The filtered positional operator (`$[<identifier>]`).
    Filtered(String),
}

impl fmt::Display for UpdatePathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdatePathComponent::FieldName(name) => write!(f, "{}", name),
            UpdatePathComponent::ArrayElement(index) => write!(f, "{}", index),
            UpdatePathComponent::FirstMatch => write!(f, "$"),
            UpdatePathComponent::AllElements => write!(f, "$[]"),
            UpdatePathComponent::Filtered(id) => write!(f, "$[{}]", id),
        }
    }
}

/// A path for an update operation, composed of `UpdatePathComponent`s.
pub type UpdatePath = Vec<UpdatePathComponent>;

/// Represents a single update operation.
/// Each variant corresponds to a MongoDB update operator.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateOp {
    /// `$set`: Sets the value of a field.
    Set { path: UpdatePath, value: Arc<Expr> },
    /// `$unset`: Removes a field.
    Unset { path: UpdatePath },
    /// `$inc`: Increments a field by a specified amount.
    Inc { path: UpdatePath, amount: Arc<Expr> },
    /// `$mul`: Multiplies a field by a specified amount.
    Mul { path: UpdatePath, factor: Arc<Expr> },
    /// `$min`: Updates the field to a specified value if the specified value is less than the current value.
    Min { path: UpdatePath, value: Arc<Expr> },
    /// `$max`: Updates the field to a specified value if the specified value is greater than the current value.
    Max { path: UpdatePath, value: Arc<Expr> },
    /// `$rename`: Renames a field.
    Rename { from: UpdatePath, to: UpdatePath },

    // Temporal
    /// `$currentDate`: Sets the field to the current date, either as a BSON Date or a Timestamp.
    CurrentDate { path: UpdatePath, type_hint: CurrentDateType },

    // Array ops
    /// `$addToSet`: Adds elements to an array only if they do not already exist in the set.
    AddToSet { path: UpdatePath, values: EachOrSingle<Arc<Expr>> },
    /// `$push`: Appends a value to an array.
    Push { path: UpdatePath, spec: PushSpec<Arc<Expr>> },
    /// `$pop`: Removes the first or last element of an array.
    Pop { path: UpdatePath, from: PopFrom },          // First/Last
    /// `$pull`: Removes all array elements that match a specified query.
    Pull { path: UpdatePath, criterion: PullCriterion },
    /// `$pullAll`: Removes all instances of the specified values from an existing array.
    PullAll { path: UpdatePath, values: Vec<Arc<Expr>> },

    // Bitwise
    /// `$bit`: Performs bitwise AND, OR, and XOR updates.
    Bit { path: UpdatePath, and: Option<i64>, or: Option<i64>, xor: Option<i64> },
}

/// Type specifier for the `$currentDate` operator.
#[derive(Debug, Clone, PartialEq)]
pub enum CurrentDateType {
    /// BSON Date type.
    Date,
    /// BSON Timestamp type.
    Timestamp,
}

/// Represents an argument that can be a single value or multiple values via `$each`.
/// Used by `$addToSet` and `$push`.
#[derive(Debug, Clone, PartialEq)]
pub enum EachOrSingle<T> {
    /// A single value to add or push.
    Single(T),
    /// `$each`: A list of values to add or push.
    Each(Vec<T>),
}

/// Specification for a `$push` operation, including modifiers.
#[derive(Debug, Clone, PartialEq)]
pub struct PushSpec<T> {
    /// `$each` or single value to push.
    pub values: EachOrSingle<T>,
    /// `$position`: The position in the array to insert the elements.
    pub position: Option<i32>,
    /// `$slice`: The number of elements to keep in the array.
    pub slice: Option<i32>,
    /// `$sort`: The order of elements in the array.
    pub sort: Option<PushSort>,
}

/// Sorting specification for the `$push` operator.
#[derive(Debug, Clone, PartialEq)]
pub enum PushSort {
    /// Sort in ascending order (1).
    Ascending,
    /// Sort in descending order (-1).
    Descending,
    /// Sort documents in an array by specified fields.
    ByFields(BTreeMap<String, i32>),
}

/// Specifies which end of an array to `$pop` from.
#[derive(Debug, Clone, PartialEq)]
pub enum PopFrom {
    /// Remove the first element (`-1`).
    First,
    /// Remove the last element (`1`).
    Last,
}

/// Criterion for a `$pull` operation.
#[derive(Debug, Clone, PartialEq)]
pub enum PullCriterion {
    /// Pull elements equal to a specific value.
    Equals(Arc<Expr>),
    /// Pull elements matching a filter expression.
    Matches(Arc<Expr>),
}

/// Represents a parsed update document.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateExpr {
    /// The list of update operations to perform.
    pub ops: Vec<UpdateOp>,
    /// `$arrayFilters`: Conditions for filtered positional operators (`$[<identifier>]`).
    pub array_filters: Vec<ArrayFilter>,
}

/// A filter for a positional array update, used with `arrayFilters`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayFilter {
    /// The identifier, e.g., "x" in `$[x]`.
    pub identifier: String,
    /// The query predicate that elements bound to the identifier must satisfy.
    pub predicate: Arc<Expr>,
}

fn validate_pull_criterion_expr(expr: &Expr) -> Result<(), Error> {
    match expr {
        Expr::Comparison { value, .. } => {
            if !matches!(value.as_ref(), Expr::Literal(_)) {
                return Err(Error::InvalidRequest(format!(
                    "$pull predicate value must be a literal, got {:?}",
                    value
                )));
            }
        }
        Expr::FieldFilters { filters, .. } => {
            for filter in filters {
                validate_pull_criterion_expr(filter.as_ref())?;
            }
        }
        Expr::And(exprs) | Expr::Or(exprs) => {
            for sub_expr in exprs {
                validate_pull_criterion_expr(sub_expr.as_ref())?;
            }
        }
        Expr::Not(sub_expr) => {
            validate_pull_criterion_expr(sub_expr.as_ref())?;
        }
        _ => {
            return Err(Error::InvalidRequest(format!(
                "Unsupported predicate in $pull: {:?}",
                expr
            )));
        }
    }
    Ok(())
}

impl UpdateExpr {
    /// Validates the semantic correctness of the update expression.
    pub fn validate(&self) -> Result<(), Error> {
        if self.ops.is_empty() {
            return Err(Error::InvalidRequest("Update document cannot be empty".to_string()));
        }

        let mut used_identifiers = HashSet::new();
        let mut modified_paths: HashSet<&UpdatePath> = HashSet::new();

        for op in &self.ops {
            let paths: Vec<&UpdatePath> = match op {
                UpdateOp::Set { path, .. } => vec![path],
                UpdateOp::Unset { path } => vec![path],
                UpdateOp::Inc { path, .. } => vec![path],
                UpdateOp::Mul { path, .. } => vec![path],
                UpdateOp::Min { path, .. } => vec![path],
                UpdateOp::Max { path, .. } => vec![path],
                UpdateOp::Rename { from, to } => vec![from, to],
                UpdateOp::CurrentDate { path, .. } => vec![path],
                UpdateOp::AddToSet { path, .. } => vec![path],
                UpdateOp::Push { path, .. } => vec![path],
                UpdateOp::Pop { path, .. } => vec![path],
                UpdateOp::Pull { path, .. } => vec![path],
                UpdateOp::PullAll { path, .. } => vec![path],
                UpdateOp::Bit { path, .. } => vec![path],
            };

            for path in paths {
                let path_str = path.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(".");
                for &existing_path in modified_paths.iter() {
                    if path.starts_with(existing_path) || existing_path.starts_with(path) {
                        return Err(Error::InvalidRequest(format!(
                            "Updating the path '{}' would create a conflict with another path in the same update",
                            path_str
                        )));
                    }
                }
                modified_paths.insert(path);

                for component in path {
                    match component {
                        UpdatePathComponent::FieldName(name) => validate_field_name(name)?,
                        UpdatePathComponent::Filtered(id) => {
                            if id.is_empty() {
                                return Err(Error::InvalidRequest(
                                    "Filtered positional operator identifier cannot be empty.".to_string(),
                                ));
                            }
                            let mut chars = id.chars();
                            if !matches!(chars.next(), Some(c) if c.is_ascii_lowercase()) {
                                return Err(Error::InvalidRequest(
                                    "Filtered positional operator identifier must start with a lowercase letter."
                                        .to_string(),
                                ));
                            }
                            if !chars.all(|c| c.is_ascii_alphanumeric()) {
                                return Err(Error::InvalidRequest(
                                    "Filtered positional operator identifier can only contain alphanumeric characters."
                                        .to_string(),
                                ));
                            }
                            used_identifiers.insert(id.as_str());
                        }
                        _ => {}
                    }
                }
            }

            match op {
                UpdateOp::Rename { from, to } => {
                    if from == to {
                        return Err(Error::InvalidRequest(
                            "$rename source and destination must be different".to_string(),
                        ));
                    }
                    if to.iter().any(|p| !matches!(p, UpdatePathComponent::FieldName(_) | UpdatePathComponent::ArrayElement(_))) {
                        let to_path_str = to.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(".");
                        return Err(Error::InvalidRequest(format!(
                            "$rename destination path cannot contain positional operators: {}",
                            to_path_str
                        )));
                    }
                    if from.starts_with(&to) {
                        return Err(Error::InvalidRequest(format!(
                            "Cannot rename to a path that is a prefix of the current path. Found rename from {:?} to {:?}",
                            from, to
                        )));
                    }
                    if to.is_empty() {
                        return Err(Error::InvalidRequest(
                            "$rename target cannot be empty".to_string(),
                        ));
                    }
                    if let Some(UpdatePathComponent::FieldName(name)) = from.last() {
                        if name == "_id" {
                            return Err(Error::InvalidRequest(
                                "the source field for $rename cannot be _id".to_string(),
                            ));
                        }
                    }
                    if let Some(UpdatePathComponent::FieldName(name)) = to.last() {
                        if name == "_id" {
                            return Err(Error::InvalidRequest(
                                "the destination field for $rename cannot be _id".to_string(),
                            ));
                        }
                    }
                }
                UpdateOp::Push { spec, .. } => {
                    if (spec.position.is_some() || spec.slice.is_some() || spec.sort.is_some())
                        && !matches!(spec.values, EachOrSingle::Each(_))
                    {
                        return Err(Error::InvalidRequest(
                            "$push with modifiers must include $each".to_string(),
                        ));
                    }
                }
                UpdateOp::Bit { and, or, xor, .. } => {
                    if and.is_none() && or.is_none() && xor.is_none() {
                        return Err(Error::InvalidRequest(
                            "$bit operator document cannot be empty".to_string(),
                        ));
                    }
                }
                _ => {}
            }
        }

        if !used_identifiers.is_empty() && self.array_filters.is_empty() {
            return Err(Error::InvalidRequest(
                "Filtered positional operator is used but arrayFilters is not specified".to_string(),
            ));
        }

        let mut defined_identifiers = HashSet::new();
        for f in &self.array_filters {
            if !defined_identifiers.insert(f.identifier.as_str()) {
                return Err(Error::InvalidRequest(format!(
                    "Found multiple array filters with the same name '{}'",
                    f.identifier
                )));
            }
        }

        for used_id in &used_identifiers {
            if !defined_identifiers.contains(used_id) {
                return Err(Error::InvalidRequest(format!(
                    "No array filter found for identifier '{}' in path",
                    used_id
                )));
            }
        }

        for defined_id in &defined_identifiers {
            if !used_identifiers.contains(defined_id) {
                 return Err(Error::InvalidRequest(format!(
                    "The array filter for identifier '{}' was not used in the update",
                    defined_id
                )));
            }
        }

        for op in &self.ops {
            match op {
                UpdateOp::Set { value, .. }
                | UpdateOp::Inc { amount: value, .. }
                | UpdateOp::Mul { factor: value, .. }
                | UpdateOp::Min { value, .. }
                | UpdateOp::Max { value, .. } => {
                    if !matches!(value.as_ref(), Expr::Literal(_)) {
                        return Err(Error::InvalidRequest(format!(
                            "Update operator value must be a literal, but got: {:?}",
                            value
                        )));
                    }
                }
                UpdateOp::AddToSet { values, .. } | UpdateOp::Push { spec: PushSpec { values, .. }, .. } => {
                    match values {
                        EachOrSingle::Single(expr) => {
                            if !matches!(expr.as_ref(), Expr::Literal(_)) {
                                return Err(Error::InvalidRequest(format!(
                                    "Update operator value must be a literal, but got: {:?}",
                                    expr
                                )));
                            }
                        }
                        EachOrSingle::Each(exprs) => {
                            for expr in exprs {
                                if !matches!(expr.as_ref(), Expr::Literal(_)) {
                                    return Err(Error::InvalidRequest(format!(
                                        "Update operator value in $each must be a literal, but got: {:?}",
                                        expr
                                    )));
                                }
                            }
                        }
                    }
                }
                UpdateOp::PullAll { values, .. } => {
                    for expr in values {
                        if !matches!(expr.as_ref(), Expr::Literal(_)) {
                            return Err(Error::InvalidRequest(format!(
                                "Update operator value in $pullAll must be a literal, but got: {:?}",
                                expr
                            )));
                        }
                    }
                }
                UpdateOp::Pull { criterion, .. } => {
                    match criterion {
                        PullCriterion::Equals(expr) => {
                            if !matches!(expr.as_ref(), Expr::Literal(_)) {
                                return Err(Error::InvalidRequest(format!(
                                    "Update operator value in $pull must be a literal, but got: {:?}",
                                    expr
                                )));
                            }
                        }
                        PullCriterion::Matches(expr) => {
                            validate_pull_criterion_expr(expr.as_ref())?;
                        }
                    }
                }
                // Ops without values or already handled: Unset, Rename, CurrentDate, Pop, Bit
                _ => {}
            }
        }

        Ok(())
    }
}



/// Validates a MongoDB field name.
pub(crate) fn validate_field_name(field_name: &str) -> Result<(), Error> {
    // Check for empty field name
    if field_name.is_empty() {
        return Err(Error::InvalidRequest(
            "Field name cannot be empty.".to_string(),
        ));
    }

    // Check for reserved characters
    if field_name.contains('.') {
        return Err(Error::InvalidRequest(
            "Field name cannot contain '.'. Nested paths should use dot-separated keys."
                .to_string(),
        ));
    }
    if field_name.starts_with('$') {
        return Err(Error::InvalidRequest(
            "Field name cannot start with '$'. This is reserved for operators.".to_string(),
        ));
    }
    if field_name.contains('\0') {
        return Err(Error::InvalidRequest(
            "Field name cannot contain null characters ('\\0').".to_string(),
        ));
    }

    // Check for length
    if field_name.len() > 255 {
        return Err(Error::InvalidRequest(
            "Field name cannot exceed 255 characters.".to_string(),
        ));
    }

    // If all checks pass
    Ok(())
}
