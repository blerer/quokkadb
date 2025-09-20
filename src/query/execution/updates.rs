use crate::error::{Error, Result};
use crate::query::update::{
    CurrentDateType, EachOrSingle, PopFrom, PullCriterion, PushSort, PushSpec, UpdateExpr, UpdateOp,
    UpdatePath, UpdatePathComponent,
};
use crate::query::{get_path_value, BsonValue, ComparisonOperator, Expr};
use crate::util::bson_utils::{
    add_numeric, multiply_numeric, perform_bitwise_op, BsonArithmeticError,
};
use bson::{Bson, Document};
use std::cmp::PartialEq;
use crate::query::execution::updates::Mode::{CreateIfMissing, Get};

pub fn to_updater(
    update: &UpdateExpr,
) -> Result<Box<dyn Fn(Document) -> Result<Document> + Send + Sync>> {
    let mut operations: Vec<Box<dyn Fn(&mut Document) -> Result<()> + Send + Sync>> = vec![];
    for op in &update.ops {
        match op {
            UpdateOp::Set { path, value } => {

                validate_path(&path)?;

                let value_bson = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $set after validation"),
                };
                operations.push(Box::new(to_field_level_operation(
                    path.clone(),
                    move |_| Ok(value_bson.clone()),
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
                operations.push(Box::new(to_push_operation(path.clone(), spec_bson)));
            }
            UpdateOp::Inc { path, amount } => {
                let amount_bson = match amount.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $inc after validation"),
                };
                operations.push(Box::new(to_field_level_operation(
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
                )));
            }
            UpdateOp::Min { path, value } => {
                let value_bson = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $min after validation"),
                };
                operations.push(Box::new(to_field_level_operation(
                    path.clone(),
                    move |existing| {
                        let new_value = value_bson.clone();
                        if let Some(existing_bson) = existing {
                            if BsonValue::from(existing_bson.clone())
                                < BsonValue::from(new_value.clone())
                            {
                                Ok(existing_bson.clone())
                            } else {
                                Ok(new_value)
                            }
                        } else {
                            Ok(new_value)
                        }
                    },
                )));
            }
            UpdateOp::Max { path, value } => {
                let value_bson = match value.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $max after validation"),
                };
                operations.push(Box::new(to_field_level_operation(
                    path.clone(),
                    move |existing| {
                        let new_value = value_bson.clone();
                        if let Some(existing_bson) = existing {
                            if BsonValue::from(existing_bson.clone())
                                > BsonValue::from(new_value.clone())
                            {
                                Ok(existing_bson.clone())
                            } else {
                                Ok(new_value)
                            }
                        } else {
                            Ok(new_value)
                        }
                    },
                )));
            }
            UpdateOp::Mul { path, factor } => {
                let factor_bson = match factor.as_ref() {
                    Expr::Literal(v) => v.to_bson(),
                    _ => panic!("Non-literal value in $mul after validation"),
                };
                operations.push(Box::new(to_field_level_operation(
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
                operations.push(Box::new(to_field_level_operation(
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
                )));
            }
            UpdateOp::CurrentDate { path, type_hint } => {
                let type_hint = type_hint.clone();
                operations.push(Box::new(to_field_level_operation(
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
                )));
            }
            UpdateOp::AddToSet { path, values } => {

                validate_path(path)?;

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

fn validate_path(path: &UpdatePath) -> Result<()> {
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

fn to_push_operation(
    path: UpdatePath,
    spec: PushSpec<Bson>,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {
        let last_component = path
            .last()
            .ok_or_else(|| Error::InvalidRequest("$push path cannot be empty".to_string()))?;

        let parent_path = &path[..path.len() - 1];
        let root_component = BsonComponent::Document(doc);

        let parent_component = traverse(root_component, parent_path, CreateIfMissing)?.ok_or_else(
            || Error::InvalidRequest("Failed to traverse to parent for $push".to_string()),
        )?;

        let values_to_add = match spec.values.clone() {
            EachOrSingle::Single(v) => vec![v],
            EachOrSingle::Each(vs) => vs,
        };

        if values_to_add.is_empty() && spec.slice.is_none() {
            return Ok(());
        }

        let apply_to_array = |arr: &mut Vec<Bson>| -> Result<()> {
            if !values_to_add.is_empty() {
                if let Some(position) = spec.position {
                    if position < 0 {
                        return Err(Error::InvalidRequest(
                            "$position must be non-negative".to_string(),
                        ));
                    }
                    let pos = position as usize;
                    let temp_values = values_to_add;
                    if pos >= arr.len() {
                        arr.extend(temp_values);
                    } else {
                        let tail = arr.split_off(pos);
                        arr.extend(temp_values);
                        arr.extend(tail);
                    }
                } else {
                    arr.extend(values_to_add);
                }
            }

            if let Some(sort_spec) = &spec.sort {
                if spec.slice.is_none() {
                    return Err(Error::InvalidRequest(
                        "$sort requires $slice to be present".to_string(),
                    ));
                }

                match sort_spec {
                    PushSort::Ascending => {
                        arr.sort_by(|a, b| BsonValue::from(a.clone()).cmp(&BsonValue::from(b.clone())))
                    }
                    PushSort::Descending => {
                        arr.sort_by(|a, b| BsonValue::from(b.clone()).cmp(&BsonValue::from(a.clone())))
                    }
                    PushSort::ByFields(fields) => {
                        use crate::query::PathComponent;
                        use std::cmp::Ordering;

                        arr.sort_by(|a, b| {
                            let doc_a = a.as_document();
                            let doc_b = b.as_document();

                            if let (Some(doc_a), Some(doc_b)) = (doc_a, doc_b) {
                                for (field, order) in fields {
                                    let path: Vec<_> = field
                                        .split('.')
                                        .map(|s| PathComponent::FieldName(s.to_string()))
                                        .collect();
                                    let val_a = get_path_value(doc_a, &path)
                                        .map(|v| v.to_owned())
                                        .unwrap_or_else(|| BsonValue::from(Bson::Null));
                                    let val_b = get_path_value(doc_b, &path)
                                        .map(|v| v.to_owned())
                                        .unwrap_or_else(|| BsonValue::from(Bson::Null));

                                    let ord = val_a.cmp(&val_b);
                                    if ord != Ordering::Equal {
                                        return if *order > 0 { ord } else { ord.reverse() };
                                    }
                                }
                                Ordering::Equal
                            } else {
                                BsonValue::from(a.clone()).cmp(&BsonValue::from(b.clone()))
                            }
                        });
                    }
                }
            }

            if let Some(slice) = spec.slice {
                if slice == 0 {
                    arr.clear();
                } else if slice > 0 {
                    arr.truncate(slice as usize);
                } else {
                    let keep_len = (-slice) as usize;
                    if arr.len() > keep_len {
                        *arr = arr.drain(arr.len() - keep_len..).collect();
                    }
                }
            }
            Ok(())
        };

        match (parent_component, last_component) {
            (BsonComponent::Document(doc), UpdatePathComponent::FieldName(name)) => {
                match doc.get_mut(name) {
                    Some(Bson::Array(arr)) => apply_to_array(arr),
                    Some(_) => Err(Error::InvalidRequest(format!(
                        "Cannot $push to non-array field: {}",
                        name
                    ))),
                    None => {
                        let mut arr = vec![];
                        apply_to_array(&mut arr)?;
                        doc.insert(name.clone(), Bson::Array(arr));
                        Ok(())
                    }
                }
            }
            (BsonComponent::Array(arr), UpdatePathComponent::ArrayElement(idx)) => {
                if *idx < arr.len() {
                    match &mut arr[*idx] {
                        Bson::Array(sub_arr) => apply_to_array(sub_arr),
                        _ => Err(Error::InvalidRequest(
                            "Cannot $push to non-array element".to_string(),
                        )),
                    }
                } else {
                    while arr.len() < *idx {
                        arr.push(Bson::Null);
                    }
                    let mut sub_arr = vec![];
                    apply_to_array(&mut sub_arr)?;
                    arr.push(Bson::Array(sub_arr));
                    Ok(())
                }
            }
            (component, path_comp) => Err(Error::InvalidRequest(format!(
                "Invalid path for $push: cannot use {:?} on {:?}",
                path_comp, component
            ))),
        }
    }
}

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

fn to_pull_operation(
    path: UpdatePath,
    criterion: PullCriterion,
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {
        if let Some(arr) = get_array_mut(doc, &path)? {
            match criterion.clone() {
                PullCriterion::Equals(expr) => {
                    let val_to_remove = match expr.as_ref() {
                        Expr::Literal(v) => v,
                        _ => panic!("Non-literal value in $pull criterion after validation"),
                    };
                    arr.retain(|item| BsonValue::from(item.clone()) != *val_to_remove);
                }
                PullCriterion::Matches(predicate) => {
                    let mut i = 0;
                    while i < arr.len() {
                        if satisfies(&arr[i], predicate.as_ref())? {
                            arr.remove(i);
                        } else {
                            i += 1;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

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
) -> impl Fn(&mut Document) -> Result<()> + Send + Sync {
    move |doc: &mut Document| {

        let parent_path = &path[..path.len() - 1];
        let root_component = BsonComponent::Document(doc);

        let parent_component = traverse(root_component, parent_path, CreateIfMissing)?.ok_or_else(
            || Error::InvalidRequest("Failed to traverse to parent for $addToSet".to_string()),
        )?;

        let last_component = &path[path.len() - 1];
        apply_add_to_set(parent_component, last_component, &values)
    }
}

fn push_unique(arr: &mut Vec<Bson>, values: &[BsonValue]) {
    for v in values {
        if !arr.iter().any(|item| *v == BsonValue::from(item.clone())) {
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

fn new_array_bson(values: &[BsonValue],) -> Bson {
    Bson::Array(values.iter().map(|v| v.to_bson()).collect())
}

fn apply_add_to_set(
    parent: BsonComponent,
    last_component: &UpdatePathComponent,
    values: &[BsonValue],
) -> Result<()> {
    match (parent, last_component) {
        (BsonComponent::Document(doc), UpdatePathComponent::FieldName(name)) => {
            if let Some(val) = doc.get_mut(name) {
                if let Some(arr) = val.as_array_mut() {
                    push_unique(arr, values);
                } else {
                    return Err(Error::InvalidRequest(format!(
                        "Cannot $addToSet to non-array field: {}",
                        name
                    )));
                }
            } else {
                doc.insert(name.clone(), new_array_bson(values));
            }
        }
        (BsonComponent::Array(arr), UpdatePathComponent::ArrayElement(idx)) => {
            if *idx < arr.len() {
                if arr[*idx] == Bson::Null {
                    arr[*idx] = new_array_bson(values);
                } else if let Some(sub_arr) = arr[*idx].as_array_mut() {
                    push_unique(sub_arr, values);
                } else {
                    return Err(Error::InvalidRequest(format!(
                        "Cannot $addToSet to non-array element at index {}",
                        idx
                    )));
                }
            } else {
                while arr.len() < *idx {
                    arr.push(Bson::Null);
                }
                arr.push(new_array_bson(values));
            }
        }
        (BsonComponent::MissingField { parent, field_name }, UpdatePathComponent::FieldName(name)) => {
            let mut new_doc = Document::new();
            new_doc.insert(name.clone(), new_array_bson(values));
            parent.insert(field_name, Bson::Document(new_doc));
        }
        (
            BsonComponent::MissingField { parent, field_name },
            UpdatePathComponent::ArrayElement(idx),
        ) => {
            let mut outer_array = vec![];
            while outer_array.len() < *idx {
                outer_array.push(Bson::Null);
            }
            outer_array.push(new_array_bson(values));
            parent.insert(field_name, Bson::Array(outer_array));
        }
        (
            BsonComponent::MissingArrayElement { parent, index },
            UpdatePathComponent::FieldName(name),
        ) => {
            while parent.len() <= index {
                parent.push(Bson::Null);
            }
            let mut new_doc = Document::new();
            new_doc.insert(name.clone(), new_array_bson(values));
            parent[index] = Bson::Document(new_doc);
        }
        (
            BsonComponent::MissingArrayElement { parent, index },
            UpdatePathComponent::ArrayElement(idx),
        ) => {
            while parent.len() <= index {
                parent.push(Bson::Null);
            }
            let mut outer_array = vec![];
            while outer_array.len() < *idx {
                outer_array.push(Bson::Null);
            }
            outer_array.push(new_array_bson(values));
            parent[index] = Bson::Array(outer_array);
        }
        (c, p) => {
            return Err(Error::InvalidRequest(format!(
                "Invalid path for $addToSet: cannot use {:?} on {:?}",
                p, c
            )))
        }
    }
    Ok(())
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

// A simplified expression evaluator for $pull `Matches` clauses.
fn satisfies(value: &Bson, predicate: &Expr) -> Result<bool> {
    match predicate {
        Expr::Comparison {
            operator,
            value: pred_val_expr,
        } => {
            let pred_val = match pred_val_expr.as_ref() {
                Expr::Literal(v) => v,
                _ => panic!("Non-literal predicate value in $pull after validation"),
            };

            let bson_val = BsonValue::from(value.clone());

            Ok(match operator {
                ComparisonOperator::Eq => bson_val == *pred_val,
                ComparisonOperator::Ne => bson_val != *pred_val,
                ComparisonOperator::Gt => bson_val > *pred_val,
                ComparisonOperator::Gte => bson_val >= *pred_val,
                ComparisonOperator::Lt => bson_val < *pred_val,
                ComparisonOperator::Lte => bson_val <= *pred_val,
                ComparisonOperator::In | ComparisonOperator::Nin => {
                    if let Bson::Array(arr) = pred_val.to_bson() {
                        let contains = arr.iter().any(|b| BsonValue::from(b.clone()) == bson_val);
                        if *operator == ComparisonOperator::In {
                            contains
                        } else {
                            !contains
                        }
                    } else {
                        return Err(Error::InvalidRequest(format!(
                            "$in/$nin requires an array, got {:?}",
                            pred_val
                        )));
                    }
                }
            })
        }
        Expr::FieldFilters { field, filters } => {
            let doc = if let Bson::Document(d) = value {
                d
            } else {
                return Ok(false); // Can't apply field filter to non-document
            };

            let path = if let Expr::Field(p) = field.as_ref() {
                p
            } else {
                return Err(Error::InvalidRequest(format!(
                    "$pull field path must be a Field expression, got {:?}",
                    field
                )));
            };

            let target_val_owned =
                get_path_value(doc, path).map_or(Bson::Null, |v| v.to_owned().to_bson());

            for filter in filters {
                if !satisfies(&target_val_owned, filter.as_ref())? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::And(exprs) => {
            for expr in exprs {
                if !satisfies(value, expr.as_ref())? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(exprs) => {
            for expr in exprs {
                if satisfies(value, expr.as_ref())? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(expr) => Ok(!satisfies(value, expr.as_ref())?),
        _ => Err(Error::InvalidRequest(format!(
            "Unsupported predicate in $pull: {:?}",
            predicate
        ))),
    }
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

