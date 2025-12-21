use crate::error::{Error, Result};
use crate::query::update::{
    CurrentDateType, EachOrSingle, PopFrom, PullCriterion, PushSort, PushSpec, UpdateExpr, UpdateOp,
    UpdatePath, UpdatePathComponent,
};
use crate::query::{get_path_value, BsonValue, BsonValueRef, Expr, Parameters};
use crate::util::bson_utils::{
    add_numeric, multiply_numeric, perform_bitwise_op, BsonArithmeticError,
};
use bson::{Bson, Document};
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::query::execution::filters;
use crate::query::execution::updates::Mode::{CreateIfMissing, Get};

pub fn to_updater(
    update: &UpdateExpr,
) -> Result<Box<dyn Fn(Document) -> Result<Document> + Send + Sync>> {

    let array_filters = Arc::new(to_value_filters(&update.array_filters));

    let mut operations: Vec<Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync>> = vec![];
    for op in &update.ops {
        match op {
            UpdateOp::Set { path, value } => {
                let value_bson = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $set after validation"),
                };
                operations.push(Box::new(to_set_operation(
                    path.clone(),
                    value_bson,
                    array_filters.clone(),
                )));
            }
            UpdateOp::Unset { path } => {
                // Unset with an empty path is a no-op
                if !path.is_empty() {
                    operations.push(Box::new(to_unset_operation(path.clone())));
                }
            }
            UpdateOp::Rename { from, to } => {

                // cannot rename root document
                if !from.is_empty() {
                    operations.push(Box::new(to_rename_operation(from.clone(), to.clone())));
                }
            }
            UpdateOp::Push { path, spec } => {
                let spec_bson = {
                    let values = match &spec.values {
                        EachOrSingle::Single(expr) => {
                            let value = match expr.as_ref() {
                                Expr::Literal(v) => v,
                                _ => panic!("Non-literal value in $push after validation"),
                            };
                            EachOrSingle::Single(value.to_bson())
                        }
                        EachOrSingle::Each(exprs) => {
                            let bson_values = exprs
                                .iter()
                                .map(|value_expr| match value_expr.as_ref() {
                                    Expr::Literal(v) => v.to_bson(),
                                    _ => panic!(
                                        "Non-literal value in $push with $each after validation"
                                    ),
                                })
                                .collect();
                            EachOrSingle::Each(bson_values)
                        }
                    };

                    PushSpec {
                        values,
                        position: spec.position,
                        slice: spec.slice,
                        sort: spec.sort.clone(),
                    }
                };
                operations.push(Box::new(to_push_operation(
                    path.clone(),
                    spec_bson,
                    array_filters.clone(),
                )));
            }
            UpdateOp::Inc { path, amount } => {
                let amount_bson = match amount.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $inc after validation"),
                };
                operations.push(Box::new(to_inc_operation(
                    path.clone(),
                    amount_bson,
                    array_filters.clone(),
                )));
            }
            UpdateOp::Min { path, value } => {
                let new_value = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $min after validation"),
                };
                operations.push(Box::new(to_min_operation(
                    path.clone(),
                    new_value,
                    array_filters.clone(),
                )));
            }
            UpdateOp::Max { path, value } => {
                let new_value = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $max after validation"),
                };
                operations.push(Box::new(to_max_operation(
                    path.clone(),
                    new_value,
                    array_filters.clone(),
                )));
            }
            UpdateOp::Mul { path, factor } => {
                let factor_bson = match factor.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $mul after validation"),
                };
                operations.push(Box::new(to_mul_operation(
                    path.clone(),
                    factor_bson,
                    array_filters.clone(),
                )));
            }
            UpdateOp::Pull { path, criterion } => {
                operations.push(Box::new(to_pull_operation(path.clone(), criterion.clone())));
            }
            UpdateOp::PullAll { path, values } => {
                let bson_values = values
                    .iter()
                    .map(|value_expr| match value_expr.as_ref() {
                        Expr::Literal(v) => v.clone(),
                        _ => panic!("Non-literal value in $pullAll after validation"),
                    })
                    .collect();
                operations.push(Box::new(to_pull_all_operation(path.clone(), bson_values)));
            }
            UpdateOp::Pop { path, from } => {
                operations.push(Box::new(to_pop_operation(path.clone(), from.clone())));
            }
            UpdateOp::Bit { path, and, or, xor } => {
                let (and, or, xor) = (*and, *or, *xor);
                operations.push(Box::new(to_bit_operation(
                    path.clone(),
                    and,
                    or,
                    xor,
                    array_filters.clone(),
                )));
            }
            UpdateOp::CurrentDate { path, type_hint } => {
                let type_hint = type_hint.clone();
                operations.push(Box::new(to_current_date_operation(
                    path.clone(),
                    type_hint,
                    array_filters.clone(),
                )));
            }
            UpdateOp::AddToSet { path, values } => {

                let values_to_add = match values {
                    EachOrSingle::Single(expr) => {
                        vec![{ match expr.as_ref() {
                            Expr::Literal(v) => v.clone(),
                            _ => panic!("Non-literal value in $addToSet after validation"),
                        }}]
                    }
                    EachOrSingle::Each(exprs) => {
                        exprs
                            .iter()
                            .map(|value_expr| match value_expr.as_ref() {
                                Expr::Literal(v) => v.clone(),
                                _ => panic!(
                                    "Non-literal value in $addToSet with $each after validation"
                                ),
                            })
                            .collect()
                    }
                };
                let values_to_add = dedup_values(&values_to_add);
                operations.push(Box::new(to_add_to_set_operation(
                    path.clone(),
                    values_to_add,
                    array_filters.clone(),
                )));
            }
        }
    }

    Ok(Box::new(move |mut doc: Document| {
        for operation in &operations {
            operation(&mut doc)?;
        }
        Ok(doc)
    }))
}

fn to_value_filters(array_filters: &BTreeMap<String, Arc<Expr>>) -> BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>> {
    let mut filters_map: BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>> = BTreeMap::new();
    let params = Parameters::new();
    for (identifier, expr) in array_filters {
        let filter_fn = filters::to_value_filter(expr.clone(), &params);
        filters_map.insert(identifier.clone(), filter_fn);
    }
    filters_map
}

fn to_set_operation(
    path: UpdatePath,
    value_bson: Bson,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(path, move |_| Ok(value_bson.clone()), array_filters)
}

/// The $currentDate operation sets the value of a field to the current date or timestamp.
/// If the field does not exist, it will be created.
fn to_current_date_operation(
    path: UpdatePath,
    type_hint: CurrentDateType,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path,
        move |_| {
            let value = match type_hint {
                CurrentDateType::Date => Bson::DateTime(bson::DateTime::now()),
                CurrentDateType::Timestamp => {
                    let now = std::time::SystemTime::now();
                    let since_epoch = now
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| Error::InvalidRequest(format!("System time error: {}", e)))?;
                    Bson::Timestamp(bson::Timestamp {
                        time: since_epoch.as_secs() as u32,
                        increment: 0,
                    })
                }
            };
            Ok(value)
        },
        array_filters,
    )
}

/// The $mul operation multiplies the value of the field by a specified factor.
/// If the field does not exist, it is set to 0.
fn to_mul_operation(
    path: UpdatePath,
    factor_bson: Bson,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path,
        move |existing| {
            multiply_numeric(existing, &factor_bson).map_err(|e| match e {
                BsonArithmeticError::LhsNotNumeric => {
                    Error::InvalidRequest("Cannot $mul non-numeric field".to_string())
                }
                BsonArithmeticError::RhsNotNumeric => {
                    Error::InvalidRequest("Invalid $mul factor; must be a number".to_string())
                }
                BsonArithmeticError::Overflow => {
                    Error::InvalidRequest("integer overflow in $mul".to_string())
                }
                _ => Error::InvalidRequest("Unexpected arithmetic error in $mul".to_string()),
            })
        },
        array_filters,
    )
}

/// The $bit operation performs bitwise operations on integer fields.
/// It supports AND, OR, and XOR operations, which can be specified individually or in combination.
/// If the field does not exist, it is treated as if it were set to 0.
/// If the existing field value is not an integer, an error is returned.
fn to_bit_operation(
    path: UpdatePath,
    and: Option<i64>,
    or: Option<i64>,
    xor: Option<i64>,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path,
        move |existing| {
            perform_bitwise_op(existing, and, or, xor).map_err(|e| match e {
                BsonArithmeticError::LhsNotInteger => {
                    Error::InvalidRequest("Cannot apply $bit to a non-integer field".to_string())
                }
                _ => Error::InvalidRequest("Unexpected arithmetic error in $bit".to_string()),
            })
        },
        array_filters,
    )
}

/// The $max operation updates the value of the field to a specified value if the specified
/// value is greater than the current value of the field. If the field does not exist,
/// the $max operation sets the field to the specified value.
/// If the existing value is equal to the specified value, no change is made.
fn to_max_operation(
    path: UpdatePath,
    new_value: Bson,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path,
        move |existing| {
            if let Some(existing_bson) = existing {
                Ok(BsonValueRef(existing_bson)
                    .max(BsonValueRef(&new_value))
                    .to_owned_bson())
            } else {
                Ok(new_value.clone())
            }
        },
        array_filters,
    )
}

/// The $inc operation increments the value of the field by a specified amount.
/// If the field does not exist, it is set to the amount.
fn to_inc_operation(
    path: UpdatePath,
    amount_bson: Bson,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path,
        move |existing| {
            add_numeric(existing, &amount_bson).map_err(|e| match e {
                BsonArithmeticError::LhsNotNumeric => {
                    Error::InvalidRequest("Cannot $inc non-numeric field".to_string())
                }
                BsonArithmeticError::RhsNotNumeric => {
                    Error::InvalidRequest("Invalid $inc amount; must be a number".to_string())
                }
                BsonArithmeticError::Overflow => {
                    Error::InvalidRequest("integer overflow in $inc".to_string())
                }
                _ => Error::InvalidRequest("Unexpected arithmetic error in $inc".to_string()),
            })
        },
        array_filters,
    )
}

/// The $min operation updates the value of the field to a specified value if the specified
/// value is less than the current value of the field. If the field does not exist,
/// the $min operation sets the field to the specified value.
fn to_min_operation(
    path: UpdatePath,
    new_value: Bson,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path,
        move |existing| {
            if let Some(existing_bson) = existing {
                Ok(BsonValueRef(existing_bson)
                    .min(BsonValueRef(&new_value))
                    .to_owned_bson())
            } else {
                Ok(new_value.clone())
            }
        },
        array_filters,
    )
}

/// A generic helper for update operators that work by replacing a field's value.
///
/// This function creates and returns an update operation (a closure) that, when executed,
/// will traverse the document structure according to the given `path`. If intermediate
/// documents or arrays are missing, it creates them.
///
/// The provided `mutator` closure is then called with the existing `Bson` value at the
/// target path (or `None` if it doesn't exist). The `mutator`'s responsibility is to
/// return a new `Bson` value, which will then replace the old one.
///
/// This abstraction is ideal for operators like `$set`, `$inc`, `$mul`, `$min`, and `$max`,
/// where the logic can be expressed as a transformation from an old value to a new one.
fn to_field_level_operation<F>(
    path: UpdatePath,
    mutator: F,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + 'static
where
    F: Fn(Option<&Bson>) -> Result<Bson> + Send + Sync + 'static,
{
    let mutator = Arc::new(mutator);
    move |doc: &mut Document| {
        validate_non_empty_path(&path)?;

        let parent_path = &path[..path.len() - 1];
        let last_component = path.last().unwrap();

        traverse_and_apply(
            BsonComponent::Document(doc),
            parent_path,
            CreateIfMissing,
            &array_filters,
            &|parent_component| {
                apply_mutation(
                    parent_component,
                    last_component,
                    mutator.as_ref(),
                    &array_filters,
                )
            },
        )
    }
}

fn validate_non_empty_path(path: &UpdatePath) -> Result<()> {
    if path.is_empty() {
        Err(Error::InvalidRequest("Update path cannot be empty".to_string()))
    } else {
        Ok(())
    }
}

fn apply_mutation<F>(
    parent: BsonComponent,
    last_component: &UpdatePathComponent,
    mutator: &F,
    array_filters: &BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>,
) -> Result<()>
where
    F: Fn(Option<&Bson>) -> Result<Bson>,
{
    match (parent, last_component) {
        (BsonComponent::Document(doc), UpdatePathComponent::FieldName(name)) => {
            let new_val = mutator(doc.get(name))?;
            doc.insert(name.clone(), new_val);
        }
        (BsonComponent::Array(arr), UpdatePathComponent::ArrayElement(idx)) => {
            let existing = if *idx < arr.len() { Some(&arr[*idx]) } else { None };
            let new_val = mutator(existing)?;
            while arr.len() <= *idx {
                arr.push(Bson::Null);
            }
            arr[*idx] = new_val;
        }
        (BsonComponent::MissingField { parent, field_name }, UpdatePathComponent::FieldName(name)) => {
            let new_val = mutator(None)?;
            let mut new_doc = Document::new();
            new_doc.insert(name.clone(), new_val);
            parent.insert(field_name, Bson::Document(new_doc));
        }
        (BsonComponent::MissingField { parent, field_name }, UpdatePathComponent::ArrayElement(idx)) => {
            let new_val = mutator(None)?;
            let mut new_arr = Vec::new();
            while new_arr.len() < *idx {
                new_arr.push(Bson::Null);
            }
            new_arr.push(new_val);
            parent.insert(field_name, Bson::Array(new_arr));
        }
        (
            BsonComponent::MissingArrayElement { parent, index },
            UpdatePathComponent::FieldName(name),
        ) => {
            while parent.len() <= index {
                parent.push(Bson::Null);
            }
            let new_val = mutator(None)?;
            let mut new_doc = Document::new();
            new_doc.insert(name.clone(), new_val);
            parent[index] = Bson::Document(new_doc);
        }
        (
            BsonComponent::MissingArrayElement { parent, index },
            UpdatePathComponent::ArrayElement(idx),
        ) => {
            while parent.len() <= index {
                parent.push(Bson::Null);
            }
            let new_val = mutator(None)?;
            let mut new_arr = Vec::new();
            while new_arr.len() <= *idx {
                new_arr.push(Bson::Null);
            }
            new_arr[*idx] = new_val;
            parent[index] = Bson::Array(new_arr);
        }
        (BsonComponent::Array(arr), UpdatePathComponent::AllElements) => {
            for i in 0..arr.len() {
                arr[i] = mutator(Some(&arr[i]))?;
            }
        }
        (BsonComponent::Array(arr), UpdatePathComponent::Filtered(identifier)) => {
            let filter = array_filters.get(identifier).ok_or_else(|| {
                Error::InvalidRequest(format!(
                    "No array filter found for identifier '{}'",
                    identifier
                ))
            })?;
            for i in 0..arr.len() {
                if filter(Some(BsonValueRef(&arr[i]))) {
                    arr[i] = mutator(Some(&arr[i]))?;
                }
            }
        }
        (c, p) => {
            return Err(Error::InvalidRequest(format!(
                "Invalid path for update: cannot use {:?} on {:?}",
                p, c
            )))
        }
    }
    Ok(())
}


fn to_unset_operation(path: UpdatePath) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {
        if path.is_empty() {
            return Ok(()); // Unsetting the root is a no-op
        }

        let parent_path = &path[..path.len() - 1];
        let last_component = &path[path.len() - 1];
        let empty_filters = BTreeMap::new();

        traverse_and_apply(
            BsonComponent::Document(doc),
            parent_path,
            Get,
            &empty_filters,
            &|parent_component| {
                match (parent_component, last_component) {
                    (BsonComponent::Document(doc), UpdatePathComponent::FieldName(name)) => {
                        doc.remove(name);
                    }
                    (BsonComponent::Array(arr), UpdatePathComponent::ArrayElement(index)) => {
                        if *index < arr.len() {
                            arr[*index] = Bson::Null;
                        }
                    }
                    (BsonComponent::Document(_), UpdatePathComponent::ArrayElement(..)) => {} // No-op
                    (BsonComponent::Array(_), UpdatePathComponent::FieldName(..)) => {} // No-op
                    _ => {} // Missing* variants mean nothing to unset
                }
                Ok(())
            },
        )
    }
}

/// The $rename operation renames a field in a document to a new name.
/// If the source field does not exist, the operation is a no-op.
/// If the destination field already exists, it will be overwritten.
/// The operation supports nested paths, handling missing fields
/// by treating them as no-ops. Renaming to or from array elements is not supported
/// and will result in an error.
fn to_rename_operation(
    from: UpdatePath,
    to: UpdatePath,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {

        // --- Extract value from 'from' path ---
        let from_parent_path = &from[..from.len() - 1];
        let from_last_component = &from[from.len() - 1];

        let value_to_move = {
            let root_component_for_from = BsonComponent::Document(doc);
            // Rename does not support positional operators, so we use an empty filter map.
            let from_parent_component =
                traverse(root_component_for_from, from_parent_path, Get)?;

            if let Some(parent) = from_parent_component {
                match (parent, from_last_component) {
                    (BsonComponent::Document(d), UpdatePathComponent::FieldName(name)) => {
                        d.remove(name)
                    }
                    (BsonComponent::Document(_), c) => {
                        return Err(Error::InvalidRequest(format!(
                            "the source for $rename must be a document field, not {:?}",
                            c
                        )));
                    }
                    (BsonComponent::Array(_), _) => {
                        return Err(Error::InvalidRequest(
                            "the source for $rename must be a document field, not inside an array"
                                .to_string()));
                    }
                    // Path doesn't fully exist, so it's a no-op.
                    (BsonComponent::MissingField { .. }, _) => None,
                    (BsonComponent::MissingArrayElement { .. }, _) => None,
                }
            } else {
                None // Parent path does not exist. No-op.
            }
        };

        if let Some(value) = value_to_move {
            // --- Set value at 'to' path ---
            let to_parent_path = &to[..to.len() - 1];
            let to_last_component = &to[to.len() - 1];

            if !matches!(to_last_component, UpdatePathComponent::FieldName(_)) {
                return Err(Error::InvalidRequest(format!(
                    "the destination for $rename must be a document field, not {:?}",
                    to_last_component
                )));
            }

            let root_component_for_to_set = BsonComponent::Document(doc);
            // Rename does not support positional operators, so we use an empty filter map.
            let to_parent_component = traverse(
                root_component_for_to_set,
                to_parent_path,
                CreateIfMissing,
            )?
            .ok_or_else(|| {
                Error::InvalidRequest("Failed to traverse to parent for $rename".to_string())
            })?;

            // Use the generic field mutator for the set part of the rename.
            let empty_filters = BTreeMap::new();
            apply_mutation(
                to_parent_component,
                to_last_component,
                &|_| Ok(value.clone()),
                &empty_filters,
            )?;
        }

        Ok(())
    }
}

/// A generic helper for update operators that modify an array's contents in-place.
///
/// This function creates and returns an update operation that handles the common logic for
/// array-modifying operators like `$push` and `$addToSet`. It traverses the document to
/// find the target array, creating intermediate documents and the array itself if they
/// do not exist.
///
/// If the target path exists but does not point to an array, it returns an error.
///
/// The provided `mutator` closure (`Fn(&mut Vec<Bson>)`) is given a mutable reference
/// to the target array, allowing it to perform efficient in-place modifications
/// (e.g., adding or removing elements).
///
/// The `op_name` is used for generating user-friendly error messages.
fn to_array_modifier_operation<F>(
    path: UpdatePath,
    mutator: F,
    op_name: &'static str,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync + 'static>
where
    F: Fn(&mut Vec<Bson>) -> Result<()> + Send + Sync + 'static,
{
    let mutator = Arc::new(mutator);
    Box::new(move |doc: &mut Document| {
        validate_non_empty_path(&path)?;

        let last_component = path.last().unwrap();
        let parent_path = &path[..path.len() - 1];

        traverse_and_apply(
            BsonComponent::Document(doc),
            parent_path,
            CreateIfMissing,
            &array_filters,
            &|parent_component| match (parent_component, last_component) {
                (BsonComponent::Document(doc), UpdatePathComponent::FieldName(name)) => {
                    match doc.get_mut(name) {
                        Some(Bson::Array(arr)) => mutator(arr),
                        Some(_) => Err(Error::InvalidRequest(format!(
                            "Cannot {} to non-array field: {}",
                            op_name, name
                        ))),
                        None => {
                            let mut arr = vec![];
                            mutator(&mut arr)?;
                            doc.insert(name.clone(), Bson::Array(arr));
                            Ok(())
                        }
                    }
                }
                (BsonComponent::Array(arr), UpdatePathComponent::ArrayElement(idx)) => {
                    if *idx < arr.len() {
                        match &mut arr[*idx] {
                            Bson::Array(sub_arr) => mutator(sub_arr),
                            Bson::Null => {
                                let mut sub_arr = vec![];
                                mutator(&mut sub_arr)?;
                                arr[*idx] = Bson::Array(sub_arr);
                                Ok(())
                            }
                            _ => Err(Error::InvalidRequest(format!(
                                "Cannot {} to non-array element at index {}",
                                op_name, *idx
                            ))),
                        }
                    } else {
                        while arr.len() < *idx {
                            arr.push(Bson::Null);
                        }
                        let mut sub_arr = vec![];
                        mutator(&mut sub_arr)?;
                        arr.push(Bson::Array(sub_arr));
                        Ok(())
                    }
                }
                (BsonComponent::MissingField { parent, field_name }, UpdatePathComponent::FieldName(name)) => {
                    let mut arr = vec![];
                    mutator(&mut arr)?;
                    let mut new_doc = Document::new();
                    new_doc.insert(name.clone(), Bson::Array(arr));
                    parent.insert(field_name, Bson::Document(new_doc));
                    Ok(())
                }
                (BsonComponent::MissingField { parent, field_name }, UpdatePathComponent::ArrayElement(idx)) => {
                    let mut arr = vec![];
                    mutator(&mut arr)?;
                    let mut new_arr = Vec::new();
                    while new_arr.len() < *idx {
                        new_arr.push(Bson::Null);
                    }
                    new_arr.push(Bson::Array(arr));
                    parent.insert(field_name, Bson::Array(new_arr));
                    Ok(())
                }
                (BsonComponent::MissingArrayElement { parent, index }, UpdatePathComponent::FieldName(name)) => {
                    while parent.len() <= index {
                        parent.push(Bson::Null);
                    }
                    let mut arr = vec![];
                    mutator(&mut arr)?;
                    let mut new_doc = Document::new();
                    new_doc.insert(name.clone(), Bson::Array(arr));
                    parent[index] = Bson::Document(new_doc);
                    Ok(())
                }
                (BsonComponent::MissingArrayElement { parent, index }, UpdatePathComponent::ArrayElement(idx)) => {
                    while parent.len() <= index {
                        parent.push(Bson::Null);
                    }
                    let mut arr = vec![];
                    mutator(&mut arr)?;
                    let mut new_arr = Vec::new();
                    while new_arr.len() < *idx {
                        new_arr.push(Bson::Null);
                    }
                    new_arr.push(Bson::Array(arr));
                    parent[index] = Bson::Array(new_arr);
                    Ok(())
                }
                (component, path_comp) => Err(Error::InvalidRequest(format!(
                    "Invalid path for {}: cannot use {:?} on {:?}",
                    op_name, path_comp, component
                ))),
            },
        )
    })
}

/// The $push operation appends values to an array field. If the field does not exist,
/// it creates a new array with the provided values. If the field exists but is not an array,
/// an error is returned. The operation supports nested paths and array elements,
/// handling missing fields and out-of-bounds indices by creating the necessary structure.
fn to_push_operation(
    path: UpdatePath,
    spec: PushSpec<Bson>,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync> {
    let values_to_add = match spec.values.clone() {
        EachOrSingle::Single(v) => vec![v],
        EachOrSingle::Each(vs) => vs,
    };

    if values_to_add.is_empty() && spec.slice.is_none() {
        return Box::new(move |_: &mut Document| Ok(()));
    }

    let push_fn = to_push_fn(values_to_add, spec.position);
    let sort_fn = to_sort_fn(spec.sort);
    let slice_fn = to_slice_fn(spec.slice);

    let mutator = move |arr: &mut Vec<Bson>| -> Result<()> {
        push_fn(arr);
        sort_fn(arr);
        slice_fn(arr);
        Ok(())
    };

    to_array_modifier_operation(path, mutator, "$push", array_filters)
}

fn to_push_fn(values: Vec<Bson>, position: Option<i32>) -> Box<dyn Fn(&mut Vec<Bson>) + Send + Sync> {
    Box::new(move |arr: &mut Vec<Bson>| {
        if !values.is_empty() {
            let temp_values = values.clone();
            if let Some(position) = position {
                let pos = position;
                if pos >= arr.len() as i32 {
                    arr.extend(temp_values);
                } else {
                    let pos = if pos < 0 {
                        (arr.len() as isize + (pos as isize)).max(0) as usize
                    } else {
                        pos as usize
                    };
                    let tail = arr.split_off(pos);
                    arr.extend(temp_values);
                    arr.extend(tail);
                }
            } else {
                arr.extend(temp_values);
            }
        }
    })
}

fn to_sort_fn(sort: Option<PushSort>) -> Box<dyn Fn(&mut Vec<Bson>) + Send + Sync> {
    if let Some(sort) = sort {
        match sort {
            PushSort::Ascending => {
                Box::new(move |arr: &mut Vec<Bson>| {
                    arr.sort_by(|a, b| BsonValueRef(a).cmp(&BsonValueRef(b)))
                })
            }
            PushSort::Descending => {
                Box::new(move |arr: &mut Vec<Bson>| {
                    arr.sort_by(|a, b| BsonValueRef(b).cmp(&BsonValueRef(a)))
                })
            }
            PushSort::ByFields(fields) => {
                use crate::query::PathComponent;
                use std::cmp::Ordering;
                Box::new(move |arr: &mut Vec<Bson>| {
                    arr.sort_by(|a, b| {
                        let doc_a = a.as_document();
                        let doc_b = b.as_document();

                        if let (Some(doc_a), Some(doc_b)) = (doc_a, doc_b) {
                            for (field, order) in &fields {
                                let path: Vec<_> = field
                                    .split('.')
                                    .map(|s| PathComponent::FieldName(s.to_string()))
                                    .collect();
                                let val_a = get_path_value(doc_a, &path)
                                    .unwrap_or_else(|| BsonValueRef(&Bson::Null));
                                let val_b = get_path_value(doc_b, &path)
                                    .unwrap_or_else(|| BsonValueRef(&Bson::Null));

                                let ord = val_a.cmp(&val_b);
                                if ord != Ordering::Equal {
                                    return if *order > 0 { ord } else { ord.reverse() };
                                }
                            }
                            Ordering::Equal
                        } else {
                            BsonValueRef(a).cmp(&BsonValueRef(b))
                        }
                    });
                })
            }
        }
    } else {
        Box::new(|_| {})
    }
}

fn to_slice_fn(slice: Option<i32>) -> Box<dyn Fn(&mut Vec<Bson>) + Send + Sync> {
    if let Some(slice) = slice {
        if slice == 0 {
            Box::new(|arr: &mut Vec<Bson>| arr.clear())
        } else if slice > 0 {
            Box::new(move |arr: &mut Vec<Bson>| arr.truncate(slice as usize))
        } else {
            let keep_len = (-slice) as usize;
            Box::new(move |arr: &mut Vec<Bson>| {
                if arr.len() > keep_len {
                    *arr = arr.drain(arr.len() - keep_len..).collect();
                }
            })
        }
    } else {
        Box::new(|_| {})
    }
}


/// The $pull operation removes all instances of a value or values that match a specified condition
/// from an existing array. If the target field does not exist or is not an array, the operation
/// is a no-op. The operation supports nested paths and array elements, handling missing fields
/// and out-of-bounds indices by treating them as no-ops. The removal is based on equality
/// or a predicate, depending on the criterion provided.
fn to_pull_operation(
    path: UpdatePath,
    criterion: PullCriterion,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    let filter: Box<dyn Fn(&Bson) -> bool + Send + Sync> = match criterion {
        PullCriterion::Equals(expr) => {
            let val_to_remove = match expr.as_ref() {
                Expr::Literal(v) => v.to_bson(),
                _ => panic!("Non-literal value in $pull criterion after validation"),
            };
            Box::new(move |item: &Bson| item == &val_to_remove)
        }
        PullCriterion::Matches(predicate) => {
            let value_filter = filters::to_value_filter(predicate, &Parameters::empty());
            Box::new(move |item: &Bson| value_filter(Some(BsonValueRef(item))))
        }
    };

    move |doc: &mut Document| {
        let pull_mutator = |arr: &mut Vec<Bson>| {
            arr.retain(|item| !filter(item));
            Ok(())
        };
        let mutator = Arc::new(pull_mutator);

        // $pull does not support arrayFilters.
        let empty_filters = BTreeMap::new();

        traverse_and_apply(
            BsonComponent::Document(doc),
            &path,
            Get,
            &empty_filters,
            &|component| match component {
                BsonComponent::Array(arr) => mutator(arr),
                _ => Err(Error::InvalidRequest(format!(
                    "Cannot $pull from non-array field at path: {:?}",
                    path
                ))),
            },
        )
    }
}

/// The $pullAll operation removes all instances of the specified values from an existing array.
/// If the target field does not exist or is not an array, the operation is a no-op.
/// The operation supports nested paths and array elements, handling missing fields
/// and out-of-bounds indices by treating them as no-ops. The removal is based on equality
/// with the provided values.
fn to_pull_all_operation(
    path: UpdatePath,
    values: Vec<BsonValue>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {
        let pull_all_mutator = |arr: &mut Vec<Bson>| {
            arr.retain(|item| !values.contains(&BsonValue::from(item.clone())));
            Ok(())
        };
        let mutator = Arc::new(pull_all_mutator);

        // $pullAll does not support arrayFilters.
        let empty_filters = BTreeMap::new();

        traverse_and_apply(
            BsonComponent::Document(doc),
            &path,
            Get,
            &empty_filters,
            &|component| match component {
                BsonComponent::Array(arr) => mutator(arr),
                _ => Err(Error::InvalidRequest(format!(
                    "Cannot $pullAll from non-array field at path: {:?}",
                    path
                ))),
            },
        )
    }
}

/// The $pop operation removes the first or last element from an array.
/// If the target field does not exist or is not an array, the operation is a no-op.
fn to_pop_operation(
    path: UpdatePath,
    from: PopFrom,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {
        let pop_mutator = |arr: &mut Vec<Bson>| {
            if !arr.is_empty() {
                match from {
                    PopFrom::First => {
                        arr.remove(0);
                    }
                    PopFrom::Last => {
                        arr.pop();
                    }
                }
            }
            Ok(())
        };
        let mutator = Arc::new(pop_mutator);

        // $pop does not support arrayFilters.
        let empty_filters = BTreeMap::new();

        traverse_and_apply(
            BsonComponent::Document(doc),
            &path,
            Get,
            &empty_filters,
            &|component| match component {
                BsonComponent::Array(arr) => mutator(arr),
                _ => Err(Error::InvalidRequest(format!(
                    "Cannot $pop from non-array field at path: {:?}",
                    path
                ))),
            },
        )
    }
}

/// The $addToSet operation adds values to an array only if they do not already exist in the array,
/// ensuring uniqueness. If the target field does not exist, it creates a new array with
/// the provided values. If the field exists but is not an array, an error is returned.
/// The operation supports nested paths and array elements, handling missing fields and
/// out-of-bounds indices by creating the necessary structure. Values are compared for equality
/// before insertion to prevent duplicates.
fn to_add_to_set_operation(
    path: UpdatePath,
    values: Vec<BsonValue>,
    array_filters: Arc<BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>>,
) -> Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync> {
    let mutator = move |arr: &mut Vec<Bson>| {
        push_unique(arr, &values);
        Ok(())
    };
    to_array_modifier_operation(path, mutator, "$addToSet", array_filters)
}

fn push_unique(arr: &mut Vec<Bson>, values: &[BsonValue]) {
    for v in values {
        if !arr.iter().any(|item| v.as_ref() == BsonValueRef(item)) {
            arr.push(v.to_bson());
        }
    }
}

fn dedup_values(values: &[BsonValue]) -> Vec<BsonValue> {
    let mut out = Vec::with_capacity(values.len());
    for v in values {
        if !out.contains(v) {
            out.push(v.clone());
        }
    }
    out
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum Mode {
    Get,
    CreateIfMissing,
}

#[derive(Debug)]
enum BsonComponent<'a> {
    Document(&'a mut Document),
    Array(&'a mut bson::Array),
    MissingField {
        parent: &'a mut Document,
        field_name: String,
    },
    MissingArrayElement {
        parent: &'a mut bson::Array,
        index: usize,
    },
}

impl<'a> BsonComponent<'a> {
    fn step(
        self,
        path_component: &UpdatePathComponent,
        mode: Mode,
    ) -> Result<Option<BsonComponent<'a>>> {
        match self {
            BsonComponent::Document(doc) => match path_component {
                UpdatePathComponent::FieldName(name) => {
                    if doc.contains_key(name.as_str()) {
                        match doc.get_mut(name).unwrap() {
                            Bson::Document(d) => Ok(Some(BsonComponent::Document(d))),
                            Bson::Array(a) => Ok(Some(BsonComponent::Array(a))),
                            _ => Err(Error::InvalidRequest(format!(
                                "Cannot traverse into non-container field '{}'",
                                name
                            ))),
                        }
                    } else if mode == Get {
                        Ok(None)
                    } else {
                        Ok(Some(BsonComponent::MissingField {
                            parent: doc,
                            field_name: name.clone(),
                        }))
                    }
                }
                UpdatePathComponent::ArrayElement(index) => Err(Error::InvalidRequest(format!(
                    "Cannot use array index {} to index into a document",
                    index
                ))),
                c @ UpdatePathComponent::AllElements
                | c @ UpdatePathComponent::Filtered(_) => Err(Error::InvalidRequest(format!(
                    "Cannot apply positional operator ('{}') to a document",
                    c
                ))),
            },
            BsonComponent::Array(arr) => match path_component {
                UpdatePathComponent::ArrayElement(index) => {
                    if *index < arr.len() {
                        match &mut arr[*index] {
                            Bson::Document(d) => Ok(Some(BsonComponent::Document(d))),
                            Bson::Array(a) => Ok(Some(BsonComponent::Array(a))),
                            _ => Err(Error::InvalidRequest(format!(
                                "Cannot traverse into non-container element at index {}",
                                index
                            ))),
                        }
                    } else if mode == Get {
                        Ok(None)
                    } else {
                        Ok(Some(BsonComponent::MissingArrayElement {
                            parent: arr,
                            index: *index,
                        }))
                    }
                }
                UpdatePathComponent::FieldName(name) => Err(Error::InvalidRequest(format!(
                    "Cannot use field name '{}' to index into an array",
                    name
                ))),
                c => Err(Error::InvalidRequest(format!(
                    "Invalid operator '{}' on an array in this context",
                    c
                ))),
            },
            BsonComponent::MissingField {
                parent,
                field_name,
            } => match path_component {
                UpdatePathComponent::FieldName(name) => {
                    let new_doc = Document::new();
                    parent.insert(&field_name, Bson::Document(new_doc));
                    let new_parent = match parent.get_mut(&field_name) {
                        Some(Bson::Document(d)) => d,
                        _ => unreachable!(),
                    };
                    Ok(Some(BsonComponent::MissingField {
                        parent: new_parent,
                        field_name: name.clone(),
                    }))
                }
                UpdatePathComponent::ArrayElement(index) => {
                    let new_arr = bson::Array::new();
                    parent.insert(&field_name, Bson::Array(new_arr));
                    let new_parent = match parent.get_mut(&field_name) {
                        Some(Bson::Array(a)) => a,
                        _ => unreachable!(),
                    };
                    Ok(Some(BsonComponent::MissingArrayElement {
                        parent: new_parent,
                        index: *index,
                    }))
                }
                UpdatePathComponent::AllElements | UpdatePathComponent::Filtered(_) => {
                    // Positional operator on a path that does not exist is a no-op.
                    Ok(None)
                }
            },
            BsonComponent::MissingArrayElement { parent, index } => {
                while parent.len() <= index {
                    parent.push(Bson::Null);
                }
                match path_component {
                    UpdatePathComponent::FieldName(name) => {
                        parent[index] = Bson::Document(Document::new());
                        let new_parent = match &mut parent[index] {
                            Bson::Document(d) => d,
                            _ => unreachable!(),
                        };
                        Ok(Some(BsonComponent::MissingField {
                            parent: new_parent,
                            field_name: name.clone(),
                        }))
                    }
                    UpdatePathComponent::ArrayElement(idx) => {
                        parent[index] = Bson::Array(bson::Array::new());
                        let new_parent = match &mut parent[index] {
                            Bson::Array(a) => a,
                            _ => unreachable!(),
                        };
                        Ok(Some(BsonComponent::MissingArrayElement {
                            parent: new_parent,
                            index: *idx,
                        }))
                    }
                    c => Err(Error::InvalidRequest(format!(
                        "Cannot use positional operator in this context: {}",
                        c
                    ))),
                }
            }
        }
    }
}

/// A recursive traversal function that applies a given closure (`applier`) to all BSON
/// components targeted by a path. This function is the core of the update logic, capable
/// of handling positional operators that cause a path to branch and target multiple locations.
///
/// - For standard path components, it traverses one level deeper.
/// - For `$[]` (AllElements), it iterates over the entire array, recursively calling itself
///   for each element.
/// - For `$[<identifier>]` (Filtered), it uses the `array_filters` map to find the matching
///   filter, iterates over the array, and recursively calls itself only for elements that
///   match the filter.
///
/// If the path is fully resolved, it executes the `applier` on the final component(s).
fn traverse_and_apply<'a, F>(
    component: BsonComponent<'a>,
    path: &[UpdatePathComponent],
    mode: Mode,
    array_filters: &BTreeMap<String, Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>>,
    applier: &F,
) -> Result<()>
where
    F: Fn(BsonComponent) -> Result<()>,
{
    if path.is_empty() {
        return applier(component);
    }

    let current_pc = &path[0];
    let rest_path = &path[1..];

    // Positional operators cause path branching.
    match component {
        BsonComponent::Array(arr) => {
            match current_pc {
                UpdatePathComponent::AllElements => {
                    for i in 0..arr.len() {
                        let mut temp_array = std::mem::take(arr);
                        let elem_comp = match BsonComponent::Array(&mut temp_array)
                            .step(&UpdatePathComponent::ArrayElement(i), mode)?
                        {
                            Some(comp) => comp,
                            None => {
                                *arr = temp_array;
                                continue;
                            }
                        };
                        traverse_and_apply(elem_comp, rest_path, mode, array_filters, applier)?;
                        *arr = temp_array;
                    }
                    return Ok(());
                }
                UpdatePathComponent::Filtered(identifier) => {
                    let filter = array_filters.get(identifier).ok_or_else(|| {
                        Error::InvalidRequest(format!(
                            "No array filter found for identifier '{}'",
                            identifier
                        ))
                    })?;
                    let mut indices_to_update = Vec::new();
                    for (i, item) in arr.iter().enumerate() {
                        if filter(Some(BsonValueRef(item))) {
                            indices_to_update.push(i);
                        }
                    }
                    for i in indices_to_update {
                        let mut temp_array = std::mem::take(arr);
                        let elem_comp = match BsonComponent::Array(&mut temp_array)
                            .step(&UpdatePathComponent::ArrayElement(i), mode)?
                        {
                            Some(comp) => comp,
                            None => {
                                *arr = temp_array;
                                continue;
                            }
                        };
                        traverse_and_apply(elem_comp, rest_path, mode, array_filters, applier)?;
                        *arr = temp_array;
                    }
                    return Ok(());
                }
                _ => {
                    // Not a positional operator, treat as a standard step on an array.
                    if let Some(next_component) = BsonComponent::Array(arr).step(current_pc, mode)?
                    {
                        traverse_and_apply(
                            next_component,
                            rest_path,
                            mode,
                            array_filters,
                            applier,
                        )?;
                    }
                }
            }
        }
        component => {
            // Standard, non-branching traversal step.
            if let Some(next_component) = component.step(current_pc, mode)? {
                traverse_and_apply(next_component, rest_path, mode, array_filters, applier)?;
            }
        }
    }

    Ok(())
}

/// Traverses a document to a single target location.
/// This is a simplified, non-branching version of `traverse_and_apply` for operations
/// like `$rename` that do not support positional operators.
fn traverse<'a>(
    mut component: BsonComponent<'a>,
    path: &[UpdatePathComponent],
    mode: Mode,
) -> Result<Option<BsonComponent<'a>>> {
    for path_component in path {
        // Positional operators are not supported in this traversal mode.
        if matches!(
            path_component,
            UpdatePathComponent::AllElements
                | UpdatePathComponent::Filtered(_)
        ) {
            return Err(Error::InvalidRequest(format!(
                "Positional operator {} not supported in this context",
                path_component
            )));
        }
        component = match component.step(path_component, mode)? {
            Some(c) => c,
            None => return Ok(None),
        };
    }
    Ok(Some(component))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::update_fn::{
        add_to_set_each, add_to_set_single, bit, current_date, inc, max, min, mul, pop, pull_all,
        pull_eq, pull_matches, rename, set, unset, update,
    };
    use bson::doc;
    use std::sync::Arc;

    fn field(s: &str) -> UpdatePathComponent {
        UpdatePathComponent::FieldName(s.to_string())
    }

    fn index(i: usize) -> UpdatePathComponent {
        UpdatePathComponent::ArrayElement(i)
    }

    #[test]
    fn test_set_simple() {
        let update_expr = update([set([field("a")], 10)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! {};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": 10 });

        let doc = doc! { "a": 1 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": 10 });
    }

    #[test]
    fn test_set_nested() {
        let update_expr = update([set(
            [field("a"), field("b")],
            10,
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! {};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": { "b": 10 } });

        let doc = doc! { "a": { "b": 1 } };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": { "b": 10 } });

        let doc = doc! { "a": {} };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": { "b": 10 } });
    }

    #[test]
    fn test_set_nested_within_array() {
        let update_expr = update([set(
            [field("a"), index(2), field("b")],
            30,
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": [{"b" : 0},  {"b" : 10}, { "b": 20 }]};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [{"b" : 0},  {"b" : 10}, { "b": 30 }]});

        let doc = doc! { "a": []};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [Bson::Null,  Bson::Null, { "b": 30 }]});

        let doc = doc! {};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [Bson::Null,  Bson::Null, { "b": 30 }]});
    }

    #[test]
    fn test_set_array_nested_within_array() {
        let update_expr = update([set(
            [field("a"), index(2), index(1)],
            30,
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [[1, 2, 3],  [4, 5, 6], [7, 30, 9]]});

        let doc = doc! { "a": [[1, 2, 3], [4, 5, 6], [7]]};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [[1, 2, 3],  [4, 5, 6], [7, 30]]});

        let doc = doc! { "a": [[1, 2, 3], [4, 5, 6]]};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [[1, 2, 3],  [4, 5, 6], [Bson::Null, 30]]});

        let doc = doc! { "a": []};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [Bson::Null,  Bson::Null, [Bson::Null, 30]]});

        let doc = doc! {};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [Bson::Null,  Bson::Null, [Bson::Null, 30]]});
    }

    #[test]
    fn test_set_array() {
        let update_expr = update([set([field("a"), index(0)], 10)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": [] };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [10] });

        let doc = doc! { "a": [1] };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [10] });

        let doc = doc! {};
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [10] });
    }

    #[test]
    fn test_set_array_out_of_bounds() {
        let update_expr = update([set([field("a"), index(2)], 10)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": [1] };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [1, Bson::Null, 10] });
    }

    #[test]
    fn test_unset_simple() {
        let update_expr = update([unset([field("a")])]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 1, "b": 2 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "b": 2 });

        let doc = doc! { "b": 2 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "b": 2 });
    }

    #[test]
    fn test_unset_nested() {
        let update_expr = update([unset([field("a"), field("b")])]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": { "b": 1, "c": 2}, "d": 3 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": { "c": 2 }, "d": 3 });
    }

    #[test]
    fn test_unset_array_element() {
        let update_expr = update([unset([field("a"), index(1)])]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": [1, 2, 3] };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": [1, Bson::Null, 3] });
    }

    #[test]
    fn test_inc_simple() {
        let update_expr = update([inc([field("a")], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 10, "b": 2 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": 15, "b": 2 });
    }

    #[test]
    fn test_inc_non_existent_field() {
        let update_expr = update([inc([field("a")], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "b": 2 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "b": 2, "a": 5 });
    }

    #[test]
    fn test_inc_nested() {
        let update_expr = update([inc([field("a"), field("b")], -3)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": { "b": 5 } };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": { "b": 2 } });
    }

    #[test]
    fn test_inc_wrong_type() {
        let update_expr = update([inc([field("a")], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": "hello" };
        let result = updater(doc);
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_simple() {
        let update_expr = update([rename(
            [field("a")],
            [field("c")],
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 1, "b": 2 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "c": 1, "b": 2 });
    }

    #[test]
    fn test_rename_non_existent() {
        let update_expr = update([rename(
            [field("a")],
            [field("c")],
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "b": 2 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "b": 2 });
    }

    #[test]
    fn test_rename_nested() {
        let update_expr = update([rename(
            [field("a"), field("b")],
            [field("a"), field("c")],
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": { "b": 1 }, "d": 4 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": { "c": 1 }, "d": 4 });
    }

    #[test]
    fn test_multiple_ops() {
        let update_expr = update([
            set([field("a")], 20),
            unset([field("b")]),
            inc([field("c")], 1),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 1, "b": 2, "c": 3 };
        let updated_doc = updater(doc).unwrap();
        assert_eq!(updated_doc, doc! { "a": 20, "c": 4 });
    }

    // -----------------------------
    // Arithmetic: $mul, $min, $max
    // -----------------------------
    #[test]
    fn test_mul_simple() {
        let update_expr = update([mul([field("a")], 2)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 10 };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 20 });
    }

    #[test]
    fn test_mul_missing_field_defaults_to_zero() {
        // missing 'a' treated as 0; 0 * 5 = 0
        let update_expr = update([mul([field("a")], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! {};
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 0 });
    }

    #[test]
    fn test_mul_wrong_type_errors() {
        let update_expr = update([mul([field("a")], 3)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": "oops" };
        let result = updater(doc);
        assert!(result.is_err());
    }

    #[test]
    fn test_min_updates_when_lower() {
        let update_expr = update([min([field("a")], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 10 };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 5 });
    }

    #[test]
    fn test_min_noop_when_not_lower() {
        let update_expr = update([min([field("a")], 20)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 10 };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 10 });
    }

    #[test]
    fn test_min_sets_when_missing() {
        let update_expr = update([min([field("a")], 7)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! {};
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 7 });
    }

    #[test]
    fn test_max_updates_when_higher() {
        let update_expr = update([max([field("a")], 20)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 10 };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 20 });
    }

    #[test]
    fn test_max_noop_when_not_higher() {
        let update_expr = update([max([field("a")], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "a": 10 };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 10 });
    }

    #[test]
    fn test_max_sets_when_missing() {
        let update_expr = update([max([field("a")], 21)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! {};
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "a": 21 });
    }

    // -----------------------------
    // $currentDate
    // -----------------------------
    use crate::query::update::CurrentDateType;

    #[test]
    fn test_current_date_boolean_true_sets_date() {
        let update_expr = update([current_date([field("a")], CurrentDateType::Date)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! {};
        let updated = updater(doc).unwrap();
        assert!(matches!(updated.get("a"), Some(Bson::DateTime(_))));
    }

    #[test]
    fn test_current_date_type_date_nested_creates_path() {
        let update_expr = update([current_date(
            [field("a"), field("b")],
            CurrentDateType::Date,
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        match updated.get("a") {
            Some(Bson::Document(inner)) => assert!(matches!(inner.get("b"), Some(Bson::DateTime(_)))),
            other => panic!("expected document at 'a', got {:?}", other),
        }
    }

    #[test]
    fn test_current_date_type_timestamp() {
        let update_expr = update([current_date([field("ts")], CurrentDateType::Timestamp)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert!(matches!(updated.get("ts"), Some(Bson::Timestamp(_))));
    }

    // -----------------------------
    // $addToSet
    // -----------------------------

    #[test]
    fn test_add_to_set_single() {
        let update_expr = update([add_to_set_single([field("a")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        // missing -> create array with [1]
        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": [1] });

        // existing without value -> append
        let updated = updater(doc! { "a": [2] }).unwrap();
        assert_eq!(updated, doc! { "a": [2, 1] });

        // existing with value -> no duplicate
        let updated = updater(doc! { "a": [1, 2] }).unwrap();
        assert_eq!(updated, doc! { "a": [1, 2] });
    }

    #[test]
    fn test_add_to_set_each_dedup_and_order() {
        let update_expr = update([add_to_set_each([field("a")], [1, 2, 2])]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [2] }).unwrap();
        // 2 already exists, 1 is appended once
        assert_eq!(updated, doc! { "a": [2, 1] });
    }

    #[test]
    fn test_add_to_set_on_non_array_errors() {
        let update_expr = update([add_to_set_single([field("a")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let result = updater(doc! { "a": "oops" });
        assert!(result.is_err());
    }

    #[test]
    fn test_add_to_set_creates_missing_nested_document() {
        let update_expr = update([add_to_set_single([field("a"), field("b")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": { "b": [1] } });
    }

    #[test]
    fn test_add_to_set_creates_missing_deeply_nested_path() {
        let update_expr = update([add_to_set_single([field("a"), field("b"), field("c")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": { "b": { "c": [1] } } });
    }

    #[test]
    fn test_add_to_set_creates_missing_array_element_in_path() {
        let update_expr = update([add_to_set_single([field("a"), index(1), field("b")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [] }).unwrap();
        assert_eq!(updated, doc! { "a": [Bson::Null, { "b": [1] }] });
    }

    #[test]
    fn test_add_to_set_each_creates_missing_nested_path() {
        let update_expr = update([add_to_set_each([field("x"), field("y")], [1, 2, 3])]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "x": { "y": [1, 2, 3] } });
    }

    // -----------------------------
    // $push
    // -----------------------------
    use crate::query::update_fn::{push_single, push_spec, push_each_spec, by_fields_sort};

    #[test]
    fn test_push_simple_and_create_array() {
        let update_expr = update([
            push_single([field("a")], 1),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        // create
        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": [1] });

        // append
        let updated = updater(doc! { "a": [1] }).unwrap();
        assert_eq!(updated, doc! { "a": [1, 1] });
    }

    #[test]
    fn test_push_literal_document_with_modifier_like_key() {
        let update_expr = update([
            push_single([field("items")], doc! { "name": "item1", "$slice": 5 }),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "items": [] }).unwrap();
        assert_eq!(updated, doc! { "items": [ { "name": "item1", "$slice": 5 } ] });
    }

    #[test]
    fn test_push_with_position_each() {
        let spec = push_each_spec([10, 11], Some(1), None, None);
        let update_expr = update([
            push_spec([field("a")], spec),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [1, 2, 3] }).unwrap();
        assert_eq!(updated, doc! { "a": [1, 10, 11, 2, 3] });
    }

    #[test]
    fn test_push_with_negative_position_each() {
        let spec = push_each_spec([10, 11], Some(-2), None, None);
        let update_expr = update([
            push_spec([field("a")], spec),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [1, 2, 3] }).unwrap();
        assert_eq!(updated, doc! { "a": [1, 10, 11, 2, 3] });
    }

    #[test]
    fn test_push_with_negative_position_greater_then_array_length_each() {
        let spec = push_each_spec([10, 11], Some(-5), None, None);
        let update_expr = update([
            push_spec([field("a")], spec),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [1, 2, 3] }).unwrap();
        assert_eq!(updated, doc! { "a": [10, 11, 1, 2, 3] });
    }

    #[test]
    fn test_push_with_sort_and_slice() {
        // Start with one quiz, push two, then sort by score desc and slice -2 (keep last two)
        let spec = push_each_spec(
            [doc! { "wk": 5, "score": 8 }, doc! { "wk": 6, "score": 7 }],
            None,
            Some(-2),
            Some(by_fields_sort(std::collections::BTreeMap::from([("score".into(), -1)]))),
        );
        let update_expr = update([
            push_spec([field("quizzes")], spec),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "quizzes": [ { "wk": 4, "score": 9 } ] }).unwrap();
        assert_eq!(updated, doc! { "quizzes": [ { "wk": 5, "score": 8 }, { "wk": 6, "score": 7 } ] });
    }

    #[test]
    fn test_push_on_non_array_errors() {
        let update_expr = update([
            push_single([field("a")], 1),
        ]);
        let updater = to_updater(&update_expr).unwrap();

        let result = updater(doc! { "a": "oops" });
        assert!(result.is_err());
    }

    #[test]
    fn test_push_creates_missing_nested_document() {
        let update_expr = update([push_single([field("a"), field("b")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": { "b": [1] } });
    }

    #[test]
    fn test_push_creates_missing_deeply_nested_path() {
        let update_expr = update([push_single([field("a"), field("b"), field("c")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": { "b": { "c": [1] } } });
    }

    #[test]
    fn test_push_creates_missing_array_element_in_path() {
        let update_expr = update([push_single([field("a"), index(1), field("b")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [] }).unwrap();
        assert_eq!(updated, doc! { "a": [Bson::Null, { "b": [1] }] });
    }

    #[test]
    fn test_push_creates_missing_nested_array_in_path() {
        let update_expr = update([push_single([field("a"), index(0), index(0)], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": [ [[1]] ] });
    }

    #[test]
    fn test_push_creates_nested_path() {
        let update_expr = update([
            push_single([field("metadata"), field("history")], "event1"),
        ]);

        let updater = to_updater(&update_expr).unwrap();
        let updated = updater(doc! { "_id": 1,
            "item": "journal",
            "dim_cm": [ 14, 21 ] }).unwrap();

        assert_eq!(updated,
                   doc! { "_id": 1,
                       "item": "journal",
                       "dim_cm": [ 14, 21 ],
                       "metadata": { "history": ["event1"] } });
    }

    // -----------------------------
    // $pop
    // -----------------------------
    use crate::query::update::PopFrom;

    #[test]
    fn test_pop_first_and_last() {
        let update_expr_first = update([pop([field("a")], PopFrom::First)]);
        let update_expr_last = update([pop([field("a")], PopFrom::Last)]);
        let up_first = to_updater(&update_expr_first).unwrap();
        let up_last = to_updater(&update_expr_last).unwrap();

        let updated = up_first(doc! { "a": [1, 2, 3] }).unwrap();
        assert_eq!(updated, doc! { "a": [2, 3] });

        let updated = up_last(doc! { "a": [1, 2, 3] }).unwrap();
        assert_eq!(updated, doc! { "a": [1, 2] });
    }

    #[test]
    fn test_pop_from_empty_or_missing_is_noop() {
        let update_expr = update([pop([field("a")], PopFrom::First)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": [] }).unwrap();
        assert_eq!(updated, doc! { "a": [] });

        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! {});
    }

    #[test]
    fn test_pop_on_non_array_errors() {
        let update_expr = update([pop([field("a")], PopFrom::First)]);
        let updater = to_updater(&update_expr).unwrap();

        let result = updater(doc! { "a": "oops" });
        assert!(result.is_err());
    }

    // -----------------------------
    // $pull and $pullAll
    // -----------------------------
    use crate::query::{expr_fn as ef};

    #[test]
    fn test_pull_equals_scalar() {
        let update_expr = update([pull_eq([field("a")], "x")]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": ["x", "y", "x"] }).unwrap();
        assert_eq!(updated, doc! { "a": ["y"] });
    }

    #[test]
    fn test_pull_matches_operator_only() {
        let update_expr = update([pull_matches([field("scores")], ef::gte(ef::lit(80)))]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "scores": [70, 80, 90] }).unwrap();
        assert_eq!(updated, doc! { "scores": [70] });
    }

    #[test]
    fn test_pull_matches_nested_document() {
        // pull documents matching { score: 8, wk: 5 }
        let criterion = ef::and([
            ef::field_filters(ef::field(["score"]), [ef::eq(ef::lit(8))]),
            ef::field_filters(ef::field(["wk"]), [ef::eq(ef::lit(5))]),
        ]);

        let update_expr = update([pull_matches([field("quizzes")], criterion)]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "quizzes": [ { "score": 8, "wk": 5 }, { "score": 7, "wk": 6 } ] }).unwrap();
        assert_eq!(updated, doc! { "quizzes": [ { "score": 7, "wk": 6 } ] });
    }

    #[test]
    fn test_pull_all_removes_all_instances() {
        let update_expr = update([pull_all([field("a")], ["x", "z"])]);
        let updater = to_updater(&update_expr).unwrap();

        let updated = updater(doc! { "a": ["x", "y", "x", "w"] }).unwrap();
        assert_eq!(updated, doc! { "a": ["y", "w"] });
    }

    #[test]
    fn test_pull_on_non_array_errors() {
        let update_expr = update([pull_eq([field("a")], 1)]);
        let updater = to_updater(&update_expr).unwrap();

        let result = updater(doc! { "a": "oops" });
        assert!(result.is_err());
    }

    // -----------------------------
    // $bit
    // -----------------------------
    #[test]
    fn test_bit_and_or_xor() {
        // AND
        let and_expr = update([bit([field("a")], Some(0b1010), None, None)]);
        let updater_and = to_updater(&and_expr).unwrap();
        let updated = updater_and(doc! { "a": 0b1100 }).unwrap();
        assert_eq!(updated, doc! { "a": 0b1000 });

        // OR
        let or_expr = update([bit([field("a")], None, Some(0b0001), None)]);
        let updater_or = to_updater(&or_expr).unwrap();
        let updated = updater_or(doc! { "a": 0b1000 }).unwrap();
        assert_eq!(updated, doc! { "a": 0b1001 });

        // XOR
        let xor_expr = update([bit([field("a")], None, None, Some(0b0011))]);
        let updater_xor = to_updater(&xor_expr).unwrap();
        let updated = updater_xor(doc! { "a": 0b1001 }).unwrap();
        assert_eq!(updated, doc! { "a": 0b1001 ^ 0b0011 });
    }

    #[test]
    fn test_bit_on_missing_defaults_to_zero() {
        // OR 0b0010 to missing field => 0b0010
        let update_expr = update([bit(
            [field("a")],
            None,
            Some(0b0010),
            None,
        )]);
        let updater = to_updater(&update_expr).unwrap();
        let updated = updater(doc! {}).unwrap();
        assert_eq!(updated, doc! { "a": 0b0010 });
    }

    #[test]
    fn test_bit_on_non_integer_errors() {
        let update_expr = update([bit([field("a")], Some(1), None, None)]);
        let updater = to_updater(&update_expr).unwrap();
        let result = updater(doc! { "a": "oops" });
        assert!(result.is_err());
    }

    // -----------------------------
    // Positional operators: $[] and $[id] with arrayFilters
    // -----------------------------
    use crate::query::update_fn::{all, update_with_filters, filter};

    #[test]
    fn test_positional_all_elements_set_field() {
        // $set: { "grades.$[].mean": 100 }
        let update_expr = update([set(
            [field("grades"), all(), field("mean")],
            100,
        )]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "grades": [ { "mean": 70 }, { "mean": 80 } ] };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "grades": [ { "mean": 100 }, { "mean": 100 } ] });
    }

    #[test]
    fn test_filtered_positional_with_array_filters() {
        // $set: { "grades.$[elem].mean": 100 }, arrayFilters: [{ "elem.grade": { "$gte": 85 } }]
        let ops = [set(
            [field("grades"), filter("elem"), field("mean")],
            100,
        )];
        let filters = [(
            "elem".to_string(),
            ef::field_filters(ef::field(["grade"]), [ef::gte(ef::lit(85))])
        )];
        let update_expr = Arc::new(update_with_filters(ops, filters));
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "grades": [ { "grade": 90, "mean": 70 }, { "grade": 80, "mean": 60 } ] };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "grades": [ { "grade": 90, "mean": 100 }, { "grade": 80, "mean": 60 } ] });
    }

    #[test]
    fn test_positional_inc_on_scalar_array() {
        // $inc: { "scores.$[]": 5 }
        let update_expr = update([inc([field("scores"), all()], 5)]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "scores": [10, 20, 30] };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "scores": [15, 25, 35] });
    }

    #[test]
    fn test_positional_unset_field() {
        // $unset: { "items.$[].b": "" }
        let update_expr = update([unset([field("items"), all(), field("b")])]);
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "items": [ { "a": 1, "b": 2 }, { "a": 3, "b": 4 } ] };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "items": [ { "a": 1 }, { "a": 3 } ] });
    }

    #[test]
    fn test_positional_push_to_nested_array() {
        // $push: { "grades.$[].scores": 100 }
        use crate::query::update_fn::push_single;
        let update_expr = update([push_single([field("grades"), all(), field("scores")], 100)]);
        let updater = to_updater(&update_expr).unwrap();
        let doc = doc! { "grades": [ { "scores": [70] }, { "scores": [80] } ] };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "grades": [ { "scores": [70, 100] }, { "scores": [80, 100] } ] });
    }

    #[test]
    fn test_nested_positional_operators_set() {
        // $set: { "schools.$[].classes.$[].students.$[].passed": true }
        let update_expr = update([set(
            [field("schools"), all(), field("classes"), all(), field("students"), all(), field("passed")],
            true
        )]);
        let updater = to_updater(&update_expr).unwrap();
        let doc = doc! {
            "schools": [
                { "classes": [
                    { "students": [ {"name": "A"}, {"name": "B"} ] },
                    { "students": [ {"name": "C"} ] }
                ] },
                { "classes": [
                    { "students": [ {"name": "D"} ] }
                ] }
            ]
        };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! {
            "schools": [
                { "classes": [
                    { "students": [ {"name": "A", "passed": true}, {"name": "B", "passed": true} ] },
                    { "students": [ {"name": "C", "passed": true} ] }
                ] },
                { "classes": [
                    { "students": [ {"name": "D", "passed": true} ] }
                ] }
            ]
        });
    }

    #[test]
    fn test_positional_on_empty_array_is_noop() {
        let update_expr = update([set([field("grades"), all(), field("score")], 100)]);
        let updater = to_updater(&update_expr).unwrap();
        let doc = doc! { "grades": [] };
        let original_doc = doc.clone();
        let updated = updater(doc).unwrap();
        assert_eq!(updated, original_doc);
    }

    #[test]
    fn test_positional_on_missing_field_is_noop() {
        let update_expr = update([set([field("grades"), all(), field("score")], 100)]);
        let updater = to_updater(&update_expr).unwrap();
        let doc = doc! { "name": "test" };
        let original_doc = doc.clone();
        let updated = updater(doc).unwrap();
        assert_eq!(updated, original_doc);
    }

    #[test]
    fn test_positional_on_non_array_field_errors() {
        let update_expr = update([set([field("grades"), all(), field("score")], 100)]);
        let updater = to_updater(&update_expr).unwrap();
        let doc = doc! { "grades": { "not": "an array" } };
        let result = updater(doc);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot apply positional operator ('$[]') to a document"
        );
    }

    #[test]
    fn test_filtered_positional_on_heterogeneous_array() {
        // only update documents where grade is >= 85
        let ops = [set([field("grades"), filter("elem"), field("mean")], 100)];
        let filters = [(
            "elem".to_string(),
            ef::field_filters(ef::field(["grade"]), [ef::gte(ef::lit(85))])
        )];
        let update_expr = Arc::new(update_with_filters(ops, filters));
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "grades": [
            { "grade": 90, "mean": 70 },
            "not a document",
            { "grade": 80, "mean": 60 },
            Bson::Null
        ]};
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "grades": [
            { "grade": 90, "mean": 100 },
            "not a document",
            { "grade": 80, "mean": 60 },
            Bson::Null
        ]});
    }

    #[test]
    fn test_filtered_positional_with_no_matches_is_noop() {
        let ops = [set([field("grades"), filter("elem"), field("mean")], 100)];
        let filters = [("elem".to_string(), ef::field_filters(ef::field(["grade"]), [ef::gte(ef::lit(95))]))];
        let update_expr = Arc::new(update_with_filters(ops, filters));
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "grades": [ { "grade": 90, "mean": 70 }, { "grade": 80, "mean": 60 } ] };
        let original_doc = doc.clone();
        let updated = updater(doc).unwrap();
        assert_eq!(updated, original_doc);
    }
}
