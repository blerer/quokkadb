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
use crate::query::execution::filters;
use crate::query::execution::updates::Mode::{CreateIfMissing, Get};

pub fn to_updater(
    update: &UpdateExpr,
) -> Result<Box<dyn Fn(Document) -> Result<Document> + Send + Sync>> {
    let mut operations: Vec<Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync>> = vec![];
    for op in &update.ops {
        match op {
            UpdateOp::Set { path, value } => {

                validate_non_empty_path(&path)?;

                let value_bson = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $set after validation"),
                };
                operations.push(Box::new(to_set_operation(path, value_bson)));
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
                operations.push(Box::new(to_push_operation(path.clone(), spec_bson)));
            }
            UpdateOp::Inc { path, amount } => {
                let amount_bson = match amount.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $inc after validation"),
                };
                operations.push(Box::new(to_inc_operation(path, amount_bson)));
            }
            UpdateOp::Min { path, value } => {
                let new_value = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $min after validation"),
                };
                operations.push(Box::new(to_min_operation(path, new_value)));
            }
            UpdateOp::Max { path, value } => {
                let new_value = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $max after validation"),
                };
                operations.push(Box::new(to_max_operation(path, new_value)));
            }
            UpdateOp::Mul { path, factor } => {
                let factor_bson = match factor.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $mul after validation"),
                };
                operations.push(Box::new(to_mul_operation(path, factor_bson)));
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
                operations.push(Box::new(to_bit_operation(path, and, or, xor)));
            }
            UpdateOp::CurrentDate { path, type_hint } => {
                let type_hint = type_hint.clone();
                operations.push(Box::new(to_current_date_operation(path, type_hint)));
            }
            UpdateOp::AddToSet { path, values } => {

                validate_non_empty_path(path)?;

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
                operations
                    .push(Box::new(to_add_to_set_operation(path.clone(), values_to_add)));
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

fn to_set_operation(path: &UpdatePath, value_bson: Bson) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path.clone(),
        move |_| Ok(value_bson.clone()),
    )
}

/// The $currentDate operation sets the value of a field to the current date or timestamp.
/// If the field does not exist, it will be created.
fn to_current_date_operation(
    path: &UpdatePath,
    type_hint: CurrentDateType
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {

    to_field_level_operation(
        path.clone(),
        move |_| {
            let value = match type_hint {
                CurrentDateType::Date => Bson::DateTime(bson::DateTime::now()),
                CurrentDateType::Timestamp => {
                    let now = std::time::SystemTime::now();
                    let since_epoch = now
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| {
                            Error::InvalidRequest(format!("System time error: {}", e))
                        })?;
                    Bson::Timestamp(bson::Timestamp {
                        time: since_epoch.as_secs() as u32,
                        increment: 0,
                    })
                }
            };
            Ok(value)
        },
    )
}

/// The $mul operation multiplies the value of the field by a specified factor.
/// If the field does not exist, it is set to 0.
fn to_mul_operation(path: &UpdatePath, factor_bson: Bson) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path.clone(),
        move |existing| {
            multiply_numeric(existing, &factor_bson).map_err(|e| match e {
                BsonArithmeticError::LhsNotNumeric => Error::InvalidRequest(
                    "Cannot $mul non-numeric field".to_string(),
                ),
                BsonArithmeticError::RhsNotNumeric => Error::InvalidRequest(
                    "Invalid $mul factor; must be a number".to_string(),
                ),
                BsonArithmeticError::Overflow => {
                    Error::InvalidRequest("integer overflow in $mul".to_string())
                }
                _ => {
                    Error::InvalidRequest("Unexpected arithmetic error in $mul".to_string())
                }
            })
        },
    )
}

/// The $bit operation performs bitwise operations on integer fields.
/// It supports AND, OR, and XOR operations, which can be specified individually or in combination.
/// If the field does not exist, it is treated as if it were set to 0.
/// If the existing field value is not an integer, an error is returned.
fn to_bit_operation(
    path: &UpdatePath,
    and: Option<i64>,
    or: Option<i64>,
    xor: Option<i64>
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {

    to_field_level_operation(
        path.clone(),
        move |existing| {
            perform_bitwise_op(existing, and, or, xor).map_err(|e| match e {
                BsonArithmeticError::LhsNotInteger => Error::InvalidRequest(
                    "Cannot apply $bit to a non-integer field".to_string(),
                ),
                _ => Error::InvalidRequest(
                    "Unexpected arithmetic error in $bit".to_string(),
                ),
            })
        },
    )
}

/// The $max operation updates the value of the field to a specified value if the specified
/// value is greater than the current value of the field. If the field does not exist,
/// the $max operation sets the field to the specified value.
/// If the existing value is equal to the specified value, no change is made.
fn to_max_operation(path: &UpdatePath, new_value: Bson) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path.clone(),
        move |existing| {
            if let Some(existing_bson) = existing {
                Ok(BsonValueRef(existing_bson).max(BsonValueRef(&new_value)).to_owned_bson())
            } else {
                Ok(new_value.clone())
            }
        },
    )
}

/// The $inc operation increments the value of the field by a specified amount.
/// If the field does not exist, it is set to the amount.
fn to_inc_operation(path: &UpdatePath, amount_bson: Bson) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {
    to_field_level_operation(
        path.clone(),
        move |existing| {
            add_numeric(existing, &amount_bson).map_err(|e| match e {
                BsonArithmeticError::LhsNotNumeric => Error::InvalidRequest(
                    "Cannot $inc non-numeric field".to_string(),
                ),
                BsonArithmeticError::RhsNotNumeric => Error::InvalidRequest(
                    "Invalid $inc amount; must be a number".to_string(),
                ),
                BsonArithmeticError::Overflow => {
                    Error::InvalidRequest("integer overflow in $inc".to_string())
                }
                _ => {
                    Error::InvalidRequest("Unexpected arithmetic error in $inc".to_string())
                }
            })
        },
    )
}

/// The $min operation updates the value of the field to a specified value if the specified
/// value is less than the current value of the field. If the field does not exist,
/// the $min operation sets the field to the specified value.
fn to_min_operation(
    path: &UpdatePath,
    new_value: Bson
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + Sized {

    to_field_level_operation(
        path.clone(),
        move |existing| {
            if let Some(existing_bson) = existing {
                Ok(BsonValueRef(existing_bson).min(BsonValueRef(&new_value)).to_owned_bson())
            } else {
                Ok(new_value.clone())
            }
        },
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
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync + 'static
where
    F: Fn(Option<&Bson>) -> Result<Bson> + Send + Sync + 'static,
{
    move |doc: &mut Document| {

        let parent_path = &path[..path.len() - 1];
        let root_component = BsonComponent::Document(doc);

        let parent_component = traverse(root_component, parent_path, CreateIfMissing)?.ok_or_else(
            || Error::InvalidRequest("Failed to traverse to parent for update".to_string()),
        )?;

        let last_component = &path[path.len() - 1];
        apply_mutation(parent_component, last_component, &mutator)
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
        let parent_path = &path[..path.len() - 1];
        let root_component = BsonComponent::Document(doc);
        if let Some(parent_component) = traverse(root_component, parent_path, Get)? {
            let last_component = &path[path.len() - 1];
            match (parent_component, last_component) {
                (BsonComponent::Document(doc), UpdatePathComponent::FieldName(name)) => {
                    doc.remove(name);
                }
                (BsonComponent::Array(arr), UpdatePathComponent::ArrayElement(index)) => {
                    if *index < arr.len() {
                        arr[*index] = Bson::Null;
                    }
                }
                (BsonComponent::Document(_), UpdatePathComponent::ArrayElement(..)) => {}
                (BsonComponent::Array(_), UpdatePathComponent::FieldName(..)) => {}
                _ => {} // Missing* variants mean nothing to unset
            }
        }
        Ok(())
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
            let from_parent_component = traverse(root_component_for_from, from_parent_path, Get)?;

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
                        return Err(Error::InvalidRequest(format!(
                            "the source for $rename must be a document field, not inside an array"
                        )));
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
            let to_parent_component = traverse(
                root_component_for_to_set,
                to_parent_path,
                CreateIfMissing,
            )?
            .ok_or_else(|| {
                Error::InvalidRequest("Failed to traverse to parent for $rename".to_string())
            })?;

            // Use the generic field mutator for the set part of the rename.
            apply_mutation(to_parent_component, to_last_component, &|_| Ok(value.clone()))?;
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
) -> Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync + 'static>
where
    F: Fn(&mut Vec<Bson>) -> Result<()> + Send + Sync + 'static,
{
    Box::new(move |doc: &mut Document| {
        if path.is_empty() {
            return Err(Error::InvalidRequest(format!(
                "Update path for {} cannot be empty",
                op_name
            )));
        }
        let last_component = path.last().unwrap();
        let parent_path = &path[..path.len() - 1];

        let root_component = BsonComponent::Document(doc);
        let parent_component = traverse(root_component, parent_path, CreateIfMissing)?.ok_or_else(
            || Error::InvalidRequest(format!("Failed to traverse to parent for {}", op_name)),
        )?;

        match (parent_component, last_component) {
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
            (component, path_comp) => Err(Error::InvalidRequest(format!(
                "Invalid path for {}: cannot use {:?} on {:?}",
                op_name, path_comp, component
            ))),
        }
    })
}

/// The $push operation appends values to an array field. If the field does not exist,
/// it creates a new array with the provided values. If the field exists but is not an array,
/// an error is returned. The operation supports nested paths and array elements,
/// handling missing fields and out-of-bounds indices by creating the necessary structure.
fn to_push_operation(
    path: UpdatePath,
    spec: PushSpec<Bson>,
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

    to_array_modifier_operation(path, mutator, "$push")
}

fn to_push_fn(values: Vec<Bson>, position: Option<i32>) -> Box<dyn Fn(&mut Vec<Bson>) + Send + Sync> {
    Box::new(move |arr: &mut Vec<Bson>| {
        if !values.is_empty() {
            let temp_values = values.clone();
            if let Some(position) = position {
                let pos = position as usize;
                if pos >= arr.len() {
                    arr.extend(temp_values);
                } else {
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

/// Retrieves a mutable reference to an array at a specific path within a document.
///
/// This function traverses the document according to the `path`. Unlike other helpers in
/// this module, it operates in a strict "get" mode: it will **not** create any missing
/// documents or arrays along the path.
///
/// - If the path successfully resolves to an array, it returns `Ok(Some(&mut Vec<Bson>))`.
/// - If the path does not exist at any point, it returns `Ok(None)`.
/// - If the path resolves to a BSON type that is not an array, it returns an `Err`.
///
/// This makes it suitable for operators like `$pull` and `$pop`, which are no-ops if the
/// target array doesn't exist and should fail if the target is not an array.
fn get_array_mut<'a>(
    doc: &'a mut Document,
    path: &UpdatePath,
) -> Result<Option<&'a mut Vec<Bson>>> {
    let root_component = BsonComponent::Document(doc);
    if let Some(target_component) = traverse(root_component, path, Get)? {
        match target_component {
            BsonComponent::Array(arr) => Ok(Some(arr)),
            _ => Err(Error::InvalidRequest(format!(
                "Cannot perform array operation on non-array at path: {:?}",
                path
            ))),
        }
    } else {
        Ok(None)
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
        if let Some(arr) = get_array_mut(doc, &path)? {
            arr.retain(|item| !filter(item));
        }
        Ok(())
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
        if let Some(arr) = get_array_mut(doc, &path)? {
            arr.retain(|item| !values.contains(&BsonValue::from(item.clone())));
        }
        Ok(())
    }
}

/// The $pop operation removes the first or last element from an array.
/// If the target field does not exist or is not an array, the operation is a no-op.
fn to_pop_operation(
    path: UpdatePath,
    from: PopFrom,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {
        if let Some(arr) = get_array_mut(doc, &path)? {
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
        }
        Ok(())
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
) -> Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync> {
    let mutator = move |arr: &mut Vec<Bson>| {
        push_unique(arr, &values);
        Ok(())
    };
    to_array_modifier_operation(path, mutator, "$addToSet")
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
                    } else if mode == Mode::Get {
                        Ok(None)
                    } else {
                        Ok(Some(BsonComponent::MissingField {
                            parent: doc,
                            field_name: name.clone(),
                        }))
                    }
                }
                c => Err(Error::InvalidRequest(format!(
                    "Cannot index document with non-field component: {}",
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
                    } else if mode == Mode::Get {
                        Ok(None)
                    } else {
                        Ok(Some(BsonComponent::MissingArrayElement {
                            parent: arr,
                            index: *index,
                        }))
                    }
                }
                c => Err(Error::InvalidRequest(format!(
                    "Cannot index array with non-array component: {}",
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
                c => Err(Error::InvalidRequest(format!(
                    "Positional operators not supported in this context: {}",
                    c
                ))),
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
                        "Positional operators not supported in this context: {}",
                        c
                    ))),
                }
            }
        }
    }
}

fn traverse<'a>(
    mut component: BsonComponent<'a>,
    path: &[UpdatePathComponent],
    mode: Mode,
) -> Result<Option<BsonComponent<'a>>> {
    for path_component in path {
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
    use crate::query::update_fn::{all, update_with_filters, array_filter, filter};

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
        let filters = vec![array_filter("elem", ef::field_filters(ef::field(["elem", "grade"]), [ef::gte(ef::lit(85))]))];
        let update_expr = Arc::new(update_with_filters(ops, filters));
        let updater = to_updater(&update_expr).unwrap();

        let doc = doc! { "grades": [ { "grade": 90, "mean": 70 }, { "grade": 80, "mean": 60 } ] };
        let updated = updater(doc).unwrap();
        assert_eq!(updated, doc! { "grades": [ { "grade": 90, "mean": 100 }, { "grade": 80, "mean": 60 } ] });
    }
}

