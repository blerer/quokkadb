use std::collections::{BTreeMap, HashSet};
use crate::query::{Projection, ProjectionExpr, SortField, SortOrder};
use crate::query::{
    BsonValue, ComparisonOperator, ComparisonOperator::*, Expr, PathComponent,
};
use crate::query::update::{
    CurrentDateType, EachOrSingle, PopFrom, PullCriterion, PushSort, PushSpec,
    UpdateExpr, UpdateOp, UpdatePathComponent,
};
use crate::Error;
use bson::{Bson, Document};
use std::sync::{Arc, LazyLock};

static SCALAR_OPERATIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
        HashSet::from([
            "$eq", "$ne", "$gt", "$gte", "$lt", "$lte",
            "$in", "$nin", "$exists", "$type", "$size", "$all",
            "$elemMatch"])
    });


/// Parses a BSON `Document` representing a query filter into an `Expr`.
pub fn parse_conditions(doc: &Document) -> Result<Arc<Expr>, Error> {
    let mut conditions = Vec::new();

    for (key, value) in doc.iter() {
        match key.as_str() {
            // Logical Operators
            "$and" | "$or" | "$nor" => {
                let parsed_conditions = parse_logical_operator(key, value)?;
                conditions.push(Arc::new(parsed_conditions));
            }
            "$not" => {
                if let Bson::Document(sub_doc) = value {
                    let parsed_condition = parse_conditions(sub_doc)?;
                    conditions.push(Arc::new(Expr::Not(parsed_condition)));
                } else {
                    return Err(Error::InvalidRequest(
                        "Invalid format for $not; must be a document".to_string(),
                    ));
                }
            }
            _ => {
                // Handle fields (regular or wildcard)
                let field = parse_field(key)?;
                if matches!(field, Expr::PositionalField(_)) {
                    return Err(Error::InvalidRequest(format!(
                        "Positional fields are not supported in condition: {}",
                        key
                    )));
                }
                conditions.push(parse_field_conditions(field, value)?);
            }
        }
    }

    // Combine conditions into an `And` if there are multiple
    if conditions.len() == 1 {
        Ok(conditions.remove(0))
    } else {
        Ok(Arc::new(Expr::And(conditions)))
    }
}

fn parse_field_conditions(field: Expr, value: &Bson) -> Result<Arc<Expr>, Error> {
    Ok(Arc::new(Expr::FieldFilters {
        field: Arc::new(field),
        filters: parse_predicates(value)?,
    }))
}

/// Parses logical operators ($and, $or) into an `Expr`.
fn parse_logical_operator(operator: &str, value: &Bson) -> Result<Expr, Error> {
    if let Bson::Array(sub_docs) = value {
        let mut parsed_conditions = Vec::with_capacity(sub_docs.len());
        for bson in sub_docs {
            if let Bson::Document(sub_doc) = bson {
                parsed_conditions.push(parse_conditions(sub_doc)?);
            } else {
                return Err(Error::InvalidRequest(format!(
                    "Invalid format for {}; must be an array of documents",
                    operator
                )));
            }
        }

        match operator {
            "$and" => Ok(Expr::And(parsed_conditions)),
            "$or" => Ok(Expr::Or(parsed_conditions)),
            "$nor" => Ok(Expr::Nor(parsed_conditions)),
            _ => Err(Error::InvalidRequest(format!(
                "Unknown logical operator: {}",
                operator
            ))),
        }
    } else {
        Err(Error::InvalidRequest(format!(
            "Invalid format for {}; must be an array",
            operator
        )))
    }
}

/// Parses predicate conditions (e.g., `$eq`, `$gt`) for a specific field or wildcard.
fn parse_predicates(value: &Bson) -> Result<Vec<Arc<Expr>>, Error> {
    let mut predicates = Vec::new();
    if let Bson::Document(sub_docs) = value {
        for (key, value) in sub_docs.iter() {
            match key.as_str() {
                // Comparison Operators
                "$eq" => predicates.push(new_predicate(Eq, value)),
                "$ne" => predicates.push(new_predicate(Ne, value)),
                "$gt" => predicates.push(new_predicate(Gt, value)),
                "$gte" => predicates.push(new_predicate(Gte, value)),
                "$lt" => predicates.push(new_predicate(Lt, value)),
                "$lte" => predicates.push(new_predicate(Lte, value)),
                "$in" => predicates.push(new_predicate(In, value)),
                "$nin" => predicates.push(new_predicate(Nin, value)),
                "$exists" => {
                    if let Bson::Boolean(exists) = value {
                        predicates.push(Arc::new(Expr::Exists(*exists)));
                    } else {
                        return Err(Error::InvalidRequest(
                            "$exists must be a boolean".to_string(),
                        ));
                    }
                }
                "$type" => {
                    if let Some(bson_type) = parse_bson_type(value) {
                        predicates.push(Arc::new(Expr::Type {
                            bson_type: Arc::new(Expr::Literal(BsonValue(bson_type))),
                            negated: false,
                        }))
                    } else {
                        return Err(Error::InvalidRequest(
                            "$type must be a valid BSON type".to_string(),
                        ));
                    }
                }
                "$size" => {
                    if let Bson::Int32(size) = value {
                        let size = Arc::new(Expr::Literal(BsonValue(Bson::Int32(*size))));
                        predicates.push(Arc::new(Expr::Size {
                            size,
                            negated: false,
                        }))
                    } else {
                        return Err(Error::InvalidRequest(
                            "$size must be an integer".to_string(),
                        ));
                    }
                }
                "$all" => {
                    if let Bson::Array(values) = value {
                        predicates.push(Arc::new(Expr::All(
                            Arc::new(Expr::Literal(BsonValue(Bson::Array(values.clone())))),
                        )))
                    } else {
                        return Err(Error::InvalidRequest("$all must be an array".to_string()));
                    }
                }
                "$elemMatch" => {
                    if let Bson::Document(doc) = value {
                        let is_operator_only = doc.iter().all(|(k, _)| SCALAR_OPERATIONS.contains(k.as_str()));

                        if is_operator_only {
                            // Scalar array case: operators only
                            let sub_preds = parse_predicates(value)?; // Vec<Arc<Expr>>
                            predicates.push(Arc::new(Expr::ElemMatch(sub_preds)));
                        } else {
                            // Array of documents case: fields and nested conditions
                            let nested = parse_conditions(doc)?; // Arc<Expr>
                            let sub_preds = match nested.as_ref() {
                                Expr::And(children) => children.clone(), // flatten top-level AND
                                _ => vec![nested],                       // preserve OR/NOT/etc. as a single subexpr
                            };
                            predicates.push(Arc::new(Expr::ElemMatch(sub_preds)));
                        }
                    } else {
                        return Err(Error::InvalidRequest("$elemMatch must be a document".to_string()));
                    }
                }
                _ => return Err(Error::InvalidRequest(format!("Unknown operator: {}", key))),
            }
        }
    } else {
        // Implicit equality for direct field values
        predicates.push(new_predicate(Eq, value));
    }
    Ok(predicates)
}

fn new_predicate(operator: ComparisonOperator, value: &Bson) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator,
        value: Arc::new(Expr::Literal(BsonValue(value.clone()))),
    })
}

fn parse_bson_type(value: &Bson) -> Option<Bson> {
    match value {
        // Numeric BSON types
        Bson::Int32(1) | Bson::Int64(1) => Some(Bson::Int32(1)), //double
        Bson::Int32(2) | Bson::Int64(2) => Some(Bson::Int32(2)), // string
        Bson::Int32(3) | Bson::Int64(3) => Some(Bson::Int32(3)), // document
        Bson::Int32(4) | Bson::Int64(4) => Some(Bson::Int32(4)), // array
        Bson::Int32(5) | Bson::Int64(5) => Some(Bson::Int32(5)), // binData
        Bson::Int32(8) | Bson::Int64(8) => Some(Bson::Int32(8)), // bool
        Bson::Int32(9) | Bson::Int64(9) => Some(Bson::Int32(9)), // date
        Bson::Int32(10) | Bson::Int64(10) => Some(Bson::Int32(10)), // null
        Bson::Int32(16) | Bson::Int64(16) => Some(Bson::Int32(16)), // int
        Bson::Int32(18) | Bson::Int64(18) => Some(Bson::Int32(18)), // long

        // String-based aliases
        Bson::String(alias) => match alias.as_str() {
            "double" => Some(Bson::Int32(1)),
            "string" => Some(Bson::Int32(2)),
            "object" => Some(Bson::Int32(3)),
            "array" => Some(Bson::Int32(4)),
            "binData" => Some(Bson::Int32(5)),
            "bool" => Some(Bson::Int32(8)),
            "date" => Some(Bson::Int32(9)),
            "null" => Some(Bson::Int32(10)),
            "int" => Some(Bson::Int32(16)),
            "long" => Some(Bson::Int32(18)),
            _ => None,
        },

        _ => None,
    }
}

/// Parses a projection document into a `Projection`.
///
/// - Fields with value `1` are included (inclusion projection).
/// - Fields with value `0` are excluded (exclusion projection).
/// - Fields with a document value are treated as special projection operators
///   (e.g., `$slice`, `$elemMatch`), which are only valid in inclusion projections.
/// - Mixing inclusion and exclusion in the same projection is not allowed.
/// - Returns an error for invalid projection values, unknown operators,
///   or if the projection document is empty.
pub fn parse_projection(doc: &Document) -> Result<Projection, Error> {
    let mut include_fields = ProjectionExpr::Fields { children: BTreeMap::new() };
    let mut exclude_fields = ProjectionExpr::Fields { children: BTreeMap::new() };
    let mut has_id = false;
    let mut exclude_id = false;

    for (key, value) in doc.iter() {
        if key == "_id" {
            has_id = true;
            if matches!(value, Bson::Int32(0) | Bson::Int64(0)) {
                exclude_id = true;
            }
        }
        let field = parse_field(key)?;
        match value {
            Bson::Int32(1) | Bson::Int64(1) => match field {
                Expr::PositionalField(_) => {
                    return Err(Error::InvalidRequest(format!(
                        "The positional operator '$' is not supported: {}",
                        key
                    )));
                }
                Expr::Field(path) => {
                    include_fields.add_expr(&path, 0, Arc::new(ProjectionExpr::Field))?;
                }
                _ => panic!("Invalid projection value for field '{}': expected field or positional field", key),
            },
            Bson::Int32(0) | Bson::Int64(0) => match field {
                Expr::Field(path) => {
                    exclude_fields.add_expr(&path, 0, Arc::new(ProjectionExpr::Field))?;
                }
                Expr::PositionalField(_) => {
                    return Err(Error::InvalidRequest(format!(
                        "Invalid projection value for field '{}': expected field",
                        key
                    )));
                }
                _ => panic!("Invalid projection value for field '{}': expected field", key),
            },
            Bson::Document(projection_doc) => {
                let path = match field {
                    Expr::Field(ref path) => path,
                    Expr::PositionalField(_) => {
                        return Err(Error::InvalidRequest(format!(
                            "Invalid projection value for field '{}': expected field",
                            key
                        )));
                    }
                    _ => panic!("Invalid projection value for field '{}': expected field", key),
                };

                if projection_doc.len() != 1 {
                    return Err(Error::InvalidRequest(format!(
                        "Projection document for field '{}' must have exactly one operator.",
                        key
                    )));
                }

                let (op, op_value) = projection_doc.iter().next().unwrap();
                let projection_expr = match op.as_str() {
                    "$slice" => parse_slice_projection(op_value)?,
                    "$elemMatch" => parse_elem_match_projection(op_value)?,
                    _ => {
                        return Err(Error::InvalidRequest(format!(
                            "Unknown projection operator: {}",
                            op
                        )))
                    }
                };

                include_fields.add_expr(path, 0, Arc::new(projection_expr))?;
            }
            _ => {
                return Err(Error::InvalidRequest(format!(
                    "Invalid projection value for field '{}'",
                    key
                )))
            }
        }
    }

    let id_path = vec![PathComponent::FieldName("_id".to_string())];

    match (!include_fields.is_empty(), !exclude_fields.is_empty()) {
        (true, false) => {
            // If _id is not specified, add it to the include_fields
            if !has_id {
                include_fields.add_expr(&id_path, 0, Arc::new(ProjectionExpr::Field))?;
            }
            Ok(Projection::Include(Arc::new(include_fields)))
        }
        (false, true) => Ok(Projection::Exclude(Arc::new(exclude_fields))),
        (true, true) => {
            if exclude_id && exclude_fields.children().len() == 1 {
                // Only _id is excluded, remove it from inclusion
                include_fields.remove_expr(&vec!["_id".into()], 0)?;
            } else {
                return Err(Error::InvalidRequest(
                    "Projection cannot have a mix of inclusion and exclusion.".to_string(),
                ));
            }
            Ok(Projection::Include(Arc::new(include_fields)))
        },
        (false, false) => Err(Error::InvalidRequest(
            "Projection document cannot be empty".to_string(),
        )),
    }
}

fn parse_slice_projection(value: &Bson) -> Result<ProjectionExpr, Error> {
    match value {
        Bson::Int32(n) if *n > 0 || *n < 0 => {
            Ok(ProjectionExpr::Slice { skip: None, limit: *n })
        }
        Bson::Int64(n) if *n > 0 || *n < 0 => {
            let limit = (*n).try_into().map_err(|_| Error::InvalidRequest("$slice value out of i32 range".to_string()))?;
            Ok(ProjectionExpr::Slice { skip: None, limit })
        }
        Bson::Array(arr) => {
            if arr.len() != 2 {
                return Err(Error::InvalidRequest("$slice array must have exactly two elements".to_string()));
            }
            let skip = match &arr[0] {
                Bson::Int32(n) if *n >= 0 => Some(*n),
                Bson::Int64(n) if *n >= 0 => Some((*n).try_into().map_err(|_| Error::InvalidRequest("$slice skip out of i32 range".to_string()))?),
                _ => return Err(Error::InvalidRequest("$slice skip must be a non-negative integer".to_string())),
            };
            let limit = match &arr[1] {
                Bson::Int32(n) if *n > 0 => *n,
                Bson::Int64(n) if *n > 0 => (*n).try_into().map_err(|_| Error::InvalidRequest("$slice limit out of i32 range".to_string()))?,
                Bson::Int32(n) if *n < 0 && skip == Some(0) => *n,
                Bson::Int64(n) if *n < 0 && skip == Some(0) => (*n).try_into().map_err(|_| Error::InvalidRequest("$slice limit out of i32 range".to_string()))?,
                Bson::Int32(n) if *n < 0 && skip != Some(0) => return Err(Error::InvalidRequest("$slice with negative limit and non-zero skip is invalid".to_string())),
                Bson::Int64(n) if *n < 0 && skip != Some(0) => return Err(Error::InvalidRequest("$slice with negative limit and non-zero skip is invalid".to_string())),
                _ => return Err(Error::InvalidRequest("$slice limit must be a non-zero integer".to_string())),
            };
            Ok(ProjectionExpr::Slice { skip, limit })
        }
        _ => Err(Error::InvalidRequest("$slice must be an integer or an array of two integers".to_string())),
    }
}

fn parse_elem_match_projection(value: &Bson) -> Result<ProjectionExpr, Error> {
    if let Bson::Document(doc) = value {
        let filter = parse_conditions(doc)?;
        Ok(ProjectionExpr::ElemMatch { filter })
    } else {
        Err(Error::InvalidRequest(
            "$elemMatch projection value must be a document".to_string(),
        ))
    }
}

fn parse_field(s: &String) -> Result<Expr, Error> {
    if s.ends_with(".$") {
        parse_field_path(s.trim_end_matches(".$")).map(Expr::PositionalField)
    } else {
        parse_field_path(s).map(Expr::Field)
    }
}

fn parse_field_path(path: &str) -> Result<Vec<PathComponent>, Error> {
    path.split('.').map(|c| parse_path_component(c)).collect()
}

fn parse_path_component(component: &str) -> Result<PathComponent, Error> {
    if let Ok(index) = component.parse::<usize>() {
        Ok(PathComponent::ArrayElement(index))
    } else {
        crate::query::update::validate_field_name(component)?;
        Ok(PathComponent::FieldName(component.to_string()))
    }
}

pub fn parse_sort(doc: &Document) -> Result<Vec<SortField>, Error> {
    let mut fields = Vec::new();

    for (key, value) in doc.iter() {
        let order = match value.as_i32() {
            Some(1) => SortOrder::Ascending,
            Some(-1) => SortOrder::Descending,
            _ => {
                return Err(Error::InvalidRequest(format!(
                    "Invalid sort order for field '{}'",
                    key
                )))
            }
        };

        let field = parse_field(key)?;

        match field {
            Expr::PositionalField(_) => {
                return Err(Error::InvalidRequest(format!(
                    "Positional fields cannot used for sorting: {}",
                    key
                )))
            }
            _ => (), // Valid case,
        }

        fields.push(SortField {
            field: Arc::new(parse_field(key)?),
            order,
        });
    }

    Ok(fields)
}

fn parse_array_filters(filters: &[Document]) -> Result<BTreeMap<String, Arc<Expr>>, Error> {
    let mut parsed_filters = BTreeMap::new();
    for filter_doc in filters {

        let mut previous_identifier = None;
        let mut predicates = Vec::with_capacity(filter_doc.len());

        for (field_name, predicate_doc_bson) in filter_doc.iter() {
            let (identifier, predicate) =
                parse_identifier_and_predicate(field_name, predicate_doc_bson)?;

            match &previous_identifier {
                Some(previous) => {
                    if previous != &identifier {
                        return Err(Error::InvalidRequest(format!(
                            "Array filters must have a single identifier. Found '{}' and '{}'",
                            previous,
                            &identifier,
                        )));
                    }
                }
                None => previous_identifier = Some(identifier),
            }
            predicates.push(predicate)
        }

        let identifier = previous_identifier.unwrap();
        let predicate = if predicates.len() > 1 {
            Arc::new(Expr::And(predicates))
        } else {
            predicates.into_iter().next().unwrap()
        };

        if parsed_filters.insert(identifier.clone(), predicate).is_some() {
            return Err(Error::InvalidRequest(format!(
                "Found multiple array filters with the same identifier '{}'",
                identifier
            )));
        }
    }

    Ok(parsed_filters)
}

fn parse_identifier_and_predicate(
    field_name: &String,
    predicate: &Bson
) -> Result<(String, Arc<Expr>), Error> {

    let components = parse_field_path(field_name)?;

    let identifier = if let PathComponent::FieldName(s) = &components[0] {
        s.to_string()
    } else {
        return Err(Error::InvalidRequest(
            "Array filter identifier must be a field name".to_string(),
        ));
    };

    let predicate = if components.len() > 1 {
        let field = Expr::Field(components[1..].to_vec());
        parse_field_conditions(field, predicate)
    } else {
        let predicate_doc = predicate.as_document().ok_or_else(|| {
            Error::InvalidRequest(format!(
                "Array filter for identifier '{}' must reference at least one subfield (e.g. '{}.x').",
                &components[0],
                &components[0]))
        })?;

        parse_conditions(predicate_doc)
    }?;

    Ok((identifier, predicate))
}

/// Parses a BSON document representing an update operation into an `UpdateExpr`.
pub fn parse_update(update: &Document, array_filters: Option<Vec<Document>>) -> Result<UpdateExpr, Error> {
    let mut ops = Vec::new();

    for (key, value) in update.iter() {
        if !key.starts_with('$') {
            return Err(Error::InvalidRequest(format!(
                "Update operator must start with '$': {}",
                key
            )));
        }

        match key.as_str() {
            "$set" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$set value must be a document".to_string())
                })?;
                for (path, val) in sub_doc {
                    ops.push(UpdateOp::Set {
                        path: parse_update_path(path)?,
                        value: Arc::new(Expr::Literal(BsonValue(val.clone()))),
                    });
                }
            }
            "$setOnInsert" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$setOnInsert value must be a document".to_string())
                })?;
                for (path, val) in sub_doc {
                    ops.push(UpdateOp::SetOnInsert {
                        path: parse_update_path(path)?,
                        value: Arc::new(Expr::Literal(BsonValue(val.clone()))),
                    });
                }
            }
            "$unset" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$unset value must be a document".to_string())
                })?;
                for (path, _) in sub_doc {
                    ops.push(UpdateOp::Unset { path: parse_update_path(path)? });
                }
            }
            "$inc" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$inc value must be a document".to_string())
                })?;
                for (path, val) in sub_doc {
                    ops.push(UpdateOp::Inc {
                        path: parse_update_path(path)?,
                        amount: Arc::new(Expr::Literal(BsonValue(val.clone()))),
                    });
                }
            }
            "$mul" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$mul value must be a document".to_string())
                })?;
                for (path, val) in sub_doc {
                    ops.push(UpdateOp::Mul {
                        path: parse_update_path(path)?,
                        factor: Arc::new(Expr::Literal(BsonValue(val.clone()))),
                    });
                }
            }
            "$min" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$min value must be a document".to_string())
                })?;
                for (path, val) in sub_doc {
                    ops.push(UpdateOp::Min {
                        path: parse_update_path(path)?,
                        value: Arc::new(Expr::Literal(BsonValue(val.clone()))),
                    });
                }
            }
            "$max" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$max value must be a document".to_string())
                })?;
                for (path, val) in sub_doc {
                    ops.push(UpdateOp::Max {
                        path: parse_update_path(path)?,
                        value: Arc::new(Expr::Literal(BsonValue(val.clone()))),
                    });
                }
            }
            "$rename" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$rename value must be a document".to_string())
                })?;
                for (from_path_str, to_path_str_bson) in sub_doc {
                    let to_path_str = to_path_str_bson.as_str().ok_or_else(|| {
                        Error::InvalidRequest("$rename target path must be a string".to_string())
                    })?;
                    let from = parse_update_path(from_path_str)?;
                    let to = parse_update_path(to_path_str)?;
                    ops.push(UpdateOp::Rename { from, to });
                }
            }
            "$currentDate" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$currentDate value must be a document".to_string())
                })?;
                for (path_str, type_spec) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    let type_hint = match type_spec {
                        Bson::Boolean(true) => CurrentDateType::Date,
                        Bson::Document(doc) => {
                            if doc.len() == 1 && doc.contains_key("$type") {
                                match doc.get("$type").unwrap().as_str() {
                                    Some("date") => CurrentDateType::Date,
                                    Some("timestamp") => CurrentDateType::Timestamp,
                                    _ => {
                                        return Err(Error::InvalidRequest(
                                            "$currentDate type must be 'date' or 'timestamp'"
                                                .to_string(),
                                        ))
                                    }
                                }
                            } else {
                                return Err(Error::InvalidRequest(
                                    "Invalid $currentDate specification".to_string(),
                                ));
                            }
                        }
                        _ => {
                            return Err(Error::InvalidRequest(
                                "Invalid $currentDate value".to_string(),
                            ))
                        }
                    };
                    ops.push(UpdateOp::CurrentDate { path, type_hint });
                }
            }
            "$addToSet" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$addToSet value must be a document".to_string())
                })?;
                for (path_str, val) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    let values = parse_each_or_single(val, "$addToSet")?;
                    ops.push(UpdateOp::AddToSet { path, values });
                }
            }
            "$push" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$push value must be a document".to_string())
                })?;
                for (path_str, value_spec) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    ops.push(UpdateOp::Push { path, spec: parse_push_spec(value_spec)? });
                }
            }
            "$pop" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$pop value must be a document".to_string())
                })?;
                for (path_str, val) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    let from = match val.as_i32() {
                        Some(-1) => PopFrom::First,
                        Some(1) => PopFrom::Last,
                        _ => {
                            return Err(Error::InvalidRequest("$pop value must be 1 or -1".to_string()))
                        }
                    };
                    ops.push(UpdateOp::Pop { path, from });
                }
            }
            "$pull" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$pull value must be a document".to_string())
                })?;
                for (path_str, criterion_bson) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    let criterion = if let Bson::Document(doc) = criterion_bson {
                        let is_operator_only =
                            doc.iter().all(|(k, _)| SCALAR_OPERATIONS.contains(k.as_str()));
                        if is_operator_only {
                            let predicates = parse_predicates(criterion_bson)?;
                            let expr = if predicates.len() == 1 {
                                predicates.into_iter().next().unwrap()
                            } else {
                                Arc::new(Expr::And(predicates))
                            };
                            PullCriterion::Matches(expr)
                        } else {
                            PullCriterion::Matches(parse_conditions(doc)?)
                        }
                    } else {
                        PullCriterion::Equals(Arc::new(Expr::Literal(BsonValue(
                            criterion_bson.clone(),
                        ))))
                    };
                    ops.push(UpdateOp::Pull { path, criterion });
                }
            }
            "$pullAll" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$pullAll value must be a document".to_string())
                })?;
                for (path_str, values_bson) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    let values_arr = values_bson.as_array().ok_or_else(|| {
                        Error::InvalidRequest("$pullAll value must be an array".to_string())
                    })?;
                    let values = values_arr
                        .iter()
                        .map(|v| Arc::new(Expr::Literal(BsonValue(v.clone()))))
                        .collect();
                    ops.push(UpdateOp::PullAll { path, values });
                }
            }
            "$bit" => {
                let sub_doc = value.as_document().ok_or_else(|| {
                    Error::InvalidRequest("$bit value must be a document".to_string())
                })?;
                for (path_str, op_doc_bson) in sub_doc {
                    let path = parse_update_path(path_str)?;
                    let op_doc = op_doc_bson.as_document().ok_or_else(|| {
                        Error::InvalidRequest("$bit operator values must be a document".to_string())
                    })?;
                    let mut and = None;
                    let mut or = None;
                    let mut xor = None;
                    for (op, val) in op_doc {
                        let int_val = match val {
                            Bson::Int32(n) => *n as i64,
                            Bson::Int64(n) => *n,
                            _ => {
                                return Err(Error::InvalidRequest(format!(
                                    "$bit values must be integers but was {:?}",
                                    val
                                )))
                            }
                        };
                        match op.as_str() {
                            "and" => and = Some(int_val),
                            "or" => or = Some(int_val),
                            "xor" => xor = Some(int_val),
                            _ => {
                                return Err(Error::InvalidRequest(format!(
                                    "Unknown $bit operator: {}",
                                    op
                                )))
                            }
                        }
                    }
                    ops.push(UpdateOp::Bit { path, and, or, xor });
                }
            }
            _ => return Err(Error::InvalidRequest(format!("Unknown update operator: {}", key))),
        }
    }

    let parsed_array_filters = if let Some(filters) = array_filters {
        parse_array_filters(&filters)?
    } else {
        BTreeMap::new()
    };

    let update_expr = UpdateExpr { ops, array_filters: parsed_array_filters };
    update_expr.validate()?;
    Ok(update_expr)
}

fn parse_update_path(path: &str) -> Result<Vec<UpdatePathComponent>, Error> {
    path.split('.').map(parse_update_path_component).collect()
}

fn parse_update_path_component(component: &str) -> Result<UpdatePathComponent, Error> {
    if component == "$" {
        return Err(Error::InvalidRequest(
            "The positional operator '$' is not supported.".to_string(),
        ));
    } else if component == "$[]" {
        Ok(UpdatePathComponent::AllElements)
    } else if component.starts_with("$[") && component.ends_with(']') {
        let identifier = &component[2..component.len() - 1];
        Ok(UpdatePathComponent::Filtered(identifier.to_string()))
    } else if let Ok(index) = component.parse::<usize>() {
        Ok(UpdatePathComponent::ArrayElement(index))
    } else {
        Ok(UpdatePathComponent::FieldName(component.to_string()))
    }
}

fn parse_each_or_single(value: &Bson, op_name: &str) -> Result<EachOrSingle<Arc<Expr>>, Error> {
    if let Some(doc) = value.as_document() {
        if doc.len() == 1 && doc.contains_key("$each") {
            if let Some(arr) = doc.get("$each").unwrap().as_array() {
                let exprs = arr
                    .iter()
                    .map(|v| Arc::new(Expr::Literal(BsonValue(v.clone()))))
                    .collect();
                return Ok(EachOrSingle::Each(exprs));
            } else {
                return Err(Error::InvalidRequest(format!(
                    "{} with $each must have an array value",
                    op_name
                )));
            }
        }
    }
    Ok(EachOrSingle::Single(Arc::new(Expr::Literal(BsonValue(value.clone())))))
}

fn parse_push_spec(value: &Bson) -> Result<PushSpec<Arc<Expr>>, Error> {
    if let Some(doc) = value.as_document() {
        // If the document contains any non-modifier keys, treat it as a literal push.
        let has_non_modifier_keys =
            doc.keys().any(|k| !matches!(k.as_str(), "$each" | "$position" | "$slice" | "$sort"));
        if has_non_modifier_keys {
            let values = EachOrSingle::Single(Arc::new(Expr::Literal(BsonValue(value.clone()))));
            return Ok(PushSpec { values, position: None, slice: None, sort: None });
        }

        let mut values = None;
        let mut position = None;
        let mut slice = None;
        let mut sort = None;

        for (key, val) in doc {
            match key.as_str() {
                "$each" => {
                    let arr = val.as_array().ok_or_else(|| {
                        Error::InvalidRequest("$push with $each must have an array value".to_string())
                    })?;
                    let exprs = arr
                        .iter()
                        .map(|v| Arc::new(Expr::Literal(BsonValue(v.clone()))))
                        .collect();
                    values = Some(EachOrSingle::Each(exprs));
                }
                "$position" => {
                    position = Some(
                        val.as_i32()
                            .ok_or_else(|| Error::InvalidRequest("$position must be an integer".to_string()))?,
                    );
                }
                "$slice" => {
                    slice = Some(
                        val.as_i32()
                            .ok_or_else(|| Error::InvalidRequest("$slice must be an integer".to_string()))?,
                    );
                }
                "$sort" => match val {
                    Bson::Int32(1) | Bson::Int64(1) => sort = Some(PushSort::Ascending),
                    Bson::Int32(-1) | Bson::Int64(-1) => sort = Some(PushSort::Descending),
                    Bson::Document(sort_doc) => {
                        let mut fields = BTreeMap::new();
                        for (field, order) in sort_doc {
                            let order_val = order.as_i32().ok_or_else(|| {
                                Error::InvalidRequest("sort order must be 1 or -1".to_string())
                            })?;
                            if order_val != 1 && order_val != -1 {
                                return Err(Error::InvalidRequest(
                                    "sort order must be 1 or -1".to_string(),
                                ));
                            }
                            fields.insert(field.clone(), order_val);
                        }
                        sort = Some(PushSort::ByFields(fields));
                    }
                    _ => return Err(Error::InvalidRequest("invalid $sort value".to_string())),
                },
                _ => {
                    // This case is handled by the check at the beginning, but as a safeguard:
                    let values =
                        EachOrSingle::Single(Arc::new(Expr::Literal(BsonValue(value.clone()))));
                    return Ok(PushSpec { values, position: None, slice: None, sort: None });
                }
            }
        }

        let values = values.ok_or_else(|| {
            Error::InvalidRequest("$push with modifiers must include $each".to_string())
        })?;
        Ok(PushSpec { values, position, slice, sort })
    } else {
        // Simple push
        let values = EachOrSingle::Single(Arc::new(Expr::Literal(BsonValue(value.clone()))));
        Ok(PushSpec { values, position: None, slice: None, sort: None })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::expr_fn::*;
    use bson::{doc, Bson};
    use crate::query::update_fn;
    use crate::query::update_fn::{by_fields_sort, field_name, filter, inc, pull_eq, pull_matches, push_each_spec, push_single, push_spec, set, set_on_insert, unset, update};

    #[cfg(test)]
    mod bson_type_parsing {
        use super::*;

        #[test]
        fn test_parse_bson_type_numeric() {
            assert_eq!(parse_bson_type(&Bson::Int32(1)), Some(Bson::Int32(1)));
            assert_eq!(parse_bson_type(&Bson::Int32(2)), Some(Bson::Int32(2)));
            assert_eq!(parse_bson_type(&Bson::Int32(3)), Some(Bson::Int32(3)));
            assert_eq!(parse_bson_type(&Bson::Int32(10)), Some(Bson::Int32(10)));
            assert_eq!(parse_bson_type(&Bson::Int32(999)), None); // Invalid type
        }

        #[test]
        fn test_parse_bson_type_alias() {
            assert_eq!(
                parse_bson_type(&Bson::String("double".to_string())),
                Some(Bson::Int32(1))
            );
            assert_eq!(
                parse_bson_type(&Bson::String("string".to_string())),
                Some(Bson::Int32(2))
            );
            assert_eq!(
                parse_bson_type(&Bson::String("bool".to_string())),
                Some(Bson::Int32(8))
            );
            assert_eq!(parse_bson_type(&Bson::String("unknown".to_string())), None);
            // Invalid alias
        }
    }

    #[cfg(test)]
    mod conditions_parsing {
        use super::*;
        #[test]
        fn test_parse_conditions_with_exists() {
            let doc = doc! { "field": { "$exists": true } };
            let parsed = parse_conditions(&doc).unwrap();
            let expected = field_filters(field(["field"]), [exists(true)]);
            assert_eq!(expected, parsed);
        }

        #[test]
        fn test_parse_conditions_with_type() {
            let doc = doc! { "field": { "$type": "string" } };
            let parsed = parse_conditions(&doc).unwrap();
            let expected = field_filters(field(["field"]), [has_type(lit(2), false)]);
            assert_eq!(expected, parsed);
        }

        #[test]
        fn test_parse_conditions_with_size() {
            let doc = doc! { "arrayField": { "$size": 3 } };
            let parsed = parse_conditions(&doc).unwrap();
            let expected = field_filters(field(["arrayField"]), [size(lit(3), false)]);
            assert_eq!(expected, parsed);
        }

        #[test]
        fn test_parse_conditions_with_all() {
            let doc = doc! { "tags": { "$all": ["tag1", "tag2"] } };
            let parsed = parse_conditions(&doc).unwrap();
            let expected = field_filters(field(["tags"]), [all(lit(vec!("tag1", "tag2")))]);
            assert_eq!(expected, parsed);
        }

        #[test]
        fn test_parse_conditions_with_elem_match() {
            let doc = doc! { "nestedArray": { "$elemMatch": { "$gt": 22, "$lt": 30 } } };
            let parsed = parse_conditions(&doc).unwrap();
            let expected = field_filters(field(["nestedArray"]), [elem_match([gt(lit(22)), lt(lit(30))])]);
            assert_eq!(expected, parsed);
        }

        #[test]
        fn test_parse_conditions_valid() {
            let filter = doc! {
                "$and": [
                    { "age": { "$gte": 18 } },
                    { "status": "active" },
                    { "tags": { "$all": ["tag1", "tag2"] } }
                ]
            };

            let parsed = parse_conditions(&filter);
            assert!(parsed.is_ok());
            let expected = and([
                field_filters(field(["age"]), [gte(lit(18))]),
                field_filters(field(["status"]), [eq(lit("active"))]),
                field_filters(field(["tags"]), [all(lit(vec!("tag1", "tag2")))]),
            ]);

            assert_eq!(expected, parsed.unwrap());
        }

        #[test]
        fn test_parse_conditions_and_or_combination() {
            let filter = doc! {
                "$and": [
                    { "$or": [
                        { "field1": { "$gte": 10 } },
                        { "field2": { "$eq": "value" } }
                    ] },
                    { "field3": { "$lt": 20 } }
                ]
            };

            let parsed = parse_conditions(&filter);
            assert!(parsed.is_ok());
            let expected = and([
                or([
                    field_filters(field(["field1"]), [gte(lit(10))]),
                    field_filters(field(["field2"]), [eq(lit("value"))]),
                ]),
                field_filters(field(["field3"]), [lt(lit(20))]),
            ]);
            assert_eq!(expected, parsed.unwrap());
        }

        #[test]
        fn test_parse_conditions_not_with_and_or() {
            let filter = doc! {
                "$not": {
                    "$and": [
                        { "field1": { "$ne": 5 } },
                        { "$or": [
                            { "field2": { "$in": [1, 2, 3] } },
                            { "field3": { "$exists": true } }
                        ] }
                    ]
                }
            };

            let parsed = parse_conditions(&filter);
            assert!(parsed.is_ok());

            let expected = not(and([
                field_filters(field(["field1"]), [ne(lit(5))]),
                or([
                    field_filters(field(["field2"]), [within(lit(vec![1, 2, 3]))]),
                    field_filters(field(["field3"]), [exists(true)]),
                ]),
            ]));

            assert_eq!(expected, parsed.unwrap());
        }

        #[test]
        fn test_parse_conditions_nor_with_or() {
            let filter = doc! {
                "$nor": [
                    { "$or": [
                        { "field1": { "$lte": 15 } },
                        { "field2": { "$type": "string" } }
                    ] },
                    { "field3": { "$size": 3 } }
                ]
            };

            let parsed = parse_conditions(&filter);

            let expected = nor([
                or([
                    field_filters(field(["field1"]), [lte(lit(15))]),
                    field_filters(field(["field2"]), [has_type(lit(2), false)]),
                ]),
                field_filters(field(["field3"]), [size(lit(3), false)]),
            ]);

            assert_eq!(expected, parsed.unwrap());
        }

        #[test]
        fn test_parse_conditions_complex_and_or_nor() {
            let filter = doc! {
                "$and": [
                    { "$nor": [
                        { "field1": { "$gt": 50 } },
                        { "field2": { "$eq": "test" } }
                    ] },
                    { "$or": [
                        { "field3": { "$lt": 20 } },
                        { "$not": { "field4": { "$exists": false } } }
                    ] }
                ]
            };

            let parsed = parse_conditions(&filter);

            let expected = and([
                nor([
                    field_filters(field(["field1"]), [gt(lit(50))]),
                    field_filters(field(["field2"]), [eq(lit("test"))]),
                ]),
                or([
                    field_filters(field(["field3"]), [lt(lit(20))]),
                    not(field_filters(field(["field4"]), [exists(false)])),
                ]),
            ]);

            assert_eq!(expected, parsed.unwrap());
        }

        #[test]
        fn test_parse_conditions_invalid_operator() {
            let filter = doc! { "age": { "$invalidOp": 18 } };
            let parsed = parse_conditions(&filter);
            assert!(parsed.is_err());
            assert_eq!(
                parsed.unwrap_err().to_string(),
                "Unknown operator: $invalidOp"
            );
        }

        #[test]
        fn test_parse_conditions_non_document_filter() {
            let filter = doc! {"$and": ["not a document"]};
            let parsed = parse_conditions(&filter);
            assert!(parsed.is_err());
            assert_eq!(
                parsed.unwrap_err().to_string(),
                "Invalid format for $and; must be an array of documents"
            );
        }
    }

    #[cfg(test)]
    mod sort_parsing {

        use super::*;
        #[test]
        fn test_parse_sort_valid() {
            let sort = doc! { "name": 1, "age": -1 };

            let parsed = parse_sort(&sort);
            assert!(parsed.is_ok());
            assert_eq!(
                parsed.unwrap(),
                vec![
                    SortField {
                        field: field(["name"]),
                        order: SortOrder::Ascending,
                    },
                    SortField {
                        field: field(["age"]),
                        order: SortOrder::Descending,
                    },
                ]
            );
        }

        #[test]
        fn test_parse_sort_invalid_order() {
            let sort = doc! { "name": 2 };

            let parsed = parse_sort(&sort);
            assert!(parsed.is_err());
            assert_eq!(
                parsed.unwrap_err().to_string(),
                "Invalid sort order for field 'name'"
            );
        }

        #[test]
        fn test_parse_sort_invalid_field() {
            let sort = doc! { "name.$": 1, "age": 1 };

            let parsed = parse_sort(&sort);
            assert!(parsed.is_err());
            assert_eq!(
                parsed.unwrap_err().to_string(),
                "Positional fields cannot used for sorting: name.$"
            );
        }

        #[test]
        fn test_parse_conditions_with_elem_match_on_documents() {
            use bson::doc;

            let filter = doc! {
        "items": { "$elemMatch": { "product": "xyz", "score": { "$gte": 8 } } }
    };

            let parsed = parse_conditions(&filter).unwrap();
            let expected = field_filters(
                field(["items"]),
                [elem_match([
                    field_filters(field(["product"]), [eq(lit("xyz"))]),
                    field_filters(field(["score"]), [gte(lit(8))]),
                ])],
            );

            assert_eq!(expected, parsed);
        }

        #[test]
        fn test_parse_conditions_elem_match_invalid_non_document() {
            let filter = doc! { "a": { "$elemMatch": 1 } };
            let err = parse_conditions(&filter).unwrap_err();
            assert_eq!(err.to_string(), "$elemMatch must be a document");
        }

        #[test]
        fn test_parse_conditions_elem_match_with_or() {
            let filter = doc! {
        "a": { "$elemMatch": { "$or": [ { "x": 1 }, { "y": 2 } ] } }
    };
            let parsed = parse_conditions(&filter).unwrap();
            let expected = field_filters(
                field(["a"]),
                [elem_match([
                    or([
                        field_filters(field(["x"]), [eq(lit(1))]),
                        field_filters(field(["y"]), [eq(lit(2))]),
                    ])
                ])]
            );
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_conditions_elem_match_with_explicit_and_flattened() {
            let filter =
                doc! {"a": { "$elemMatch": { "$and": [ { "x": 1 }, { "y": { "$gt": 2 } } ] } }};
            let parsed = parse_conditions(&filter).unwrap();
            let expected = field_filters(
                field(["a"]),
                [elem_match([
                    field_filters(field(["x"]), [eq(lit(1))]),
                    field_filters(field(["y"]), [gt(lit(2))]),
                ])],
            );
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_conditions_nested_elem_match() {
            let filter =
                doc! { "a": { "$elemMatch": { "b": { "$elemMatch": { "$gt": 5 } } } } };
            let parsed = parse_conditions(&filter).unwrap();
            let expected = field_filters(
                field(["a"]),
                [elem_match([
                    field_filters(field(["b"]), [elem_match([gt(lit(5))])])
                ])]
            );
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_conditions_elem_match_empty_document() {
            let filter = doc! { "a": { "$elemMatch": {} } };
            let parsed = parse_conditions(&filter).unwrap();
            let expected = field_filters(
                field(["a"]),
                [elem_match(Vec::<Arc<Expr>>::new())],
            );
            assert_eq!(parsed, expected);
        }
    }

    #[cfg(test)]
    mod projection_parsing {
        use super::*;

        #[test]
        fn test_parse_projection_with_slice() {
            let projection = doc! { "comments": { "$slice": 5 } };
            let parsed = parse_projection(&projection).unwrap();
            let expected = Projection::Include(proj_fields([
                ("_id", proj_field()),
                ("comments", proj_slice(None, 5))
            ]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_with_slice_array() {
            let projection = doc! { "comments": { "$slice": [10, 5] } };
            let parsed = parse_projection(&projection).unwrap();
            let expected = Projection::Include(proj_fields([
                ("_id", proj_field()),
                ("comments", proj_slice(Some(10), 5))
            ]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_with_slice_invalid() {
            let projection = doc! { "comments": { "$slice": "foo" } };
            let err = parse_projection(&projection).unwrap_err();
            assert_eq!(
                err.to_string(),
                "$slice must be an integer or an array of two integers"
            );

            let projection = doc! { "comments": { "$slice": [1, 2, 3] } };
            let err = parse_projection(&projection).unwrap_err();
            assert_eq!(
                err.to_string(),
                "$slice array must have exactly two elements"
            );

            let projection = doc! { "comments": { "$slice": [1, 0] } };
            let err = parse_projection(&projection).unwrap_err();
            assert_eq!(err.to_string(), "$slice limit must be a non-zero integer");

            let projection = doc! { "comments": { "$slice": [1, -1] } };
            let err = parse_projection(&projection).unwrap_err();
            assert_eq!(err.to_string(), "$slice with negative limit and non-zero skip is invalid");
        }

        #[test]
        fn test_parse_projection_with_elem_match() {
            let projection = doc! { "students": { "$elemMatch": { "school": "Hogwarts" } } };
            let parsed = parse_projection(&projection).unwrap();
            let expected = Projection::Include(proj_fields([("_id", proj_field()), ("students", proj_elem_match(field_filters(field(["school"]), [eq(lit("Hogwarts"))])))]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_with_elem_match_complex() {
            let projection =
                doc! { "grades": { "$elemMatch": { "grade": { "$gte": 85 }, "mean": { "$gt": 90 } } } };
            let parsed = parse_projection(&projection).unwrap();
            let expected = Projection::Include(proj_fields([
                ("_id", proj_field()),
                ("grades", proj_elem_match(
                    and([
                        field_filters(field(["grade"]), [gte(lit(85))]),
                        field_filters(field(["mean"]), [gt(lit(90))]),
                    ]),
                )
                )]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_with_elem_match_invalid() {
            let projection = doc! { "students": { "$elemMatch": "not a doc" } };
            let err = parse_projection(&projection).unwrap_err();
            assert_eq!(
                err.to_string(),
                "$elemMatch projection value must be a document"
            );
        }

        #[test]
        fn test_parse_projection_with_multiple_operators() {
            let projection = doc! { "students": { "$elemMatch": { "a": 1 }, "$slice": 5 } };
            let result = parse_projection(&projection);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Projection document for field 'students' must have exactly one operator."
            );
        }

        #[test]
        fn test_parse_projection_simple_inclusion() {
            let projection = doc! { "a": 1, "b": 1 };
            let parsed = parse_projection(&projection).unwrap();
            // _id is implicitly included
            let expected = Projection::Include(proj_fields([
                ("a", proj_field()),
                ("b", proj_field()),
                ("_id", proj_field()),
            ]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_simple_inclusion_with_explicit_id() {
            let projection = doc! { "a": 1, "b": 1, "_id": 1 };
            let parsed = parse_projection(&projection).unwrap();
            let expected = Projection::Include(proj_fields([
                ("a", proj_field()),
                ("b", proj_field()),
                ("_id", proj_field()),
            ]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_simple_exclusion() {
            let projection = doc! { "a": 0, "b": 0 };
            let parsed = parse_projection(&projection).unwrap();
            // _id is implicitly included
            let expected = Projection::Exclude(proj_fields([
                ("a", proj_field()),
                ("b", proj_field()),
            ]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_exclusion_with_explicit_id() {
            // Excluding other fields means _id is included by default.
            let projection = doc! { "a": 0, "_id": 1 };
            let result = parse_projection(&projection);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Projection cannot have a mix of inclusion and exclusion."
            );
        }

        #[test]
        fn test_parse_projection_inclusion_and_id_exclusion() {
            let projection = doc! { "a": 1, "b": 1, "_id": 0 };
            let parsed = parse_projection(&projection).unwrap();
            let expected = Projection::Include(proj_fields([
                ("a", proj_field()),
                ("b", proj_field()),
            ]));
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_projection_mixing_inclusion_and_exclusion_is_error() {
            let projection = doc! { "a": 1, "b": 0 };
            let result = parse_projection(&projection);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Projection cannot have a mix of inclusion and exclusion."
            );
        }

        #[test]
        fn test_parse_projection_empty_is_error() {
            let projection = doc! {};
            let result = parse_projection(&projection);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Projection document cannot be empty"
            );
        }

        #[test]
        fn test_parse_projection_invalid_value_is_error() {
            let projection = doc! { "a": "invalid" };
            let result = parse_projection(&projection);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Invalid projection value for field 'a'"
            );
        }

        #[test]
        fn test_parse_projection_positional() {
            let projection = doc! { "a.$": 1 };
            let err = parse_projection(&projection).unwrap_err();
            assert_eq!(
                err.to_string(),
                "The positional operator '$' is not supported: a.$"
            );
        }

        #[test]
        fn test_parse_projection_positional_exclusion_is_error() {
            let projection = doc! { "a.$": 0 };
            let result = parse_projection(&projection);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                "Invalid projection value for field 'a.$': expected field"
            );
        }
    }

    #[cfg(test)]
    mod update_parsing {
        use super::*;

        #[test]
        fn test_parse_update_set() {
            let update_doc = doc! { "$set": { "a": 1, "b.c": "hello" } };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                set([field_name("a")], 1),
                set([field_name("b"), field_name("c")], "hello")
            ]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_set_on_insert() {
            let update_doc = doc! { "$setOnInsert": { "createdAt": "2024-01-01", "version": 1 } };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                set_on_insert([field_name("createdAt")], "2024-01-01"),
                set_on_insert([field_name("version")], 1),
            ]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_set_and_set_on_insert() {
            let update_doc = doc! {
                "$set": { "lastModified": "2024-06-01" },
                "$setOnInsert": { "createdAt": "2024-01-01" }
            };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                set([field_name("lastModified")], "2024-06-01"),
                set_on_insert([field_name("createdAt")], "2024-01-01"),
            ]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_set_on_insert_nested_path() {
            let update_doc = doc! { "$setOnInsert": { "meta.created": true } };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                set_on_insert([field_name("meta"), field_name("created")], true),
            ]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_set_on_insert_invalid() {
            let update_doc = doc! { "$setOnInsert": "not a document" };
            let err = parse_update(&update_doc, None).unwrap_err();
            assert_eq!(err.to_string(), "$setOnInsert value must be a document");
        }

        #[test]
        fn test_parse_update_inc_unset() {
            let update_doc = doc! {
                "$inc": { "a": 1_i64, "b.c": -2_i64 },
                "$unset": { "d": "", "e.f": "" }
            };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                inc([field_name("a")], 1_i64),
                inc([field_name("b"), field_name("c")], -2_i64),
                unset([field_name("d")]),
                unset([field_name("e"), field_name("f")]),
            ]);

            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_push() {
            let update_doc = doc! {
                "$push": {
                    "scores": 89,
                    "quizzes": {
                        "$each": [
                            doc!{ "wk": 5, "score": 8 },
                            doc!{ "wk": 6, "score": 7 }
                        ],
                        "$slice": -2,
                        "$sort": { "score": -1 }
                    }
                }
            };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                push_single([field_name("scores")], 89),
                push_spec(
                    [field_name("quizzes")],
                    push_each_spec([
                            doc! { "wk": 5, "score": 8 },
                            doc! { "wk": 6, "score": 7 },
                        ],
                        None,
                        Some(-2),
                        Some(by_fields_sort(BTreeMap::from([(
                            "score".to_string(),
                            -1
                        )]))),
                    ),
                ),
            ]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_push_literal_doc_with_modifier_like_key() {
            let update_doc = doc! {
                "$push": {
                    "items": { "name": "item1", "$slice": 5 }
                }
            };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected =
                update([push_single([field_name("items")], doc! { "name": "item1", "$slice": 5 })]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_pull() {
            let update_doc = doc! {
                "$pull": {
                    "scores": { "$gte": 80 },
                    "items": "item1",
                    "quizzes": { "score": 8, "wk": 5 }
                }
            };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([
                pull_matches([field_name("scores")], gte(lit(80))),
                pull_eq([field_name("items")], "item1"),
                pull_matches(
                    [field_name("quizzes")],
                    and([
                        field_filters(field(["score"]), [eq(lit(8))]),
                        field_filters(field(["wk"]), [eq(lit(5))]),
                    ]),
                ),
            ]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_positional_operators() {
            let update_doc = doc! {
                "$set": {
                    "grades.$": 80
                }
            };
            let err = parse_update(&update_doc, None).unwrap_err();
            assert_eq!(
                err.to_string(),
                "The positional operator '$' is not supported."
            );

            let update_doc = doc! {
                "$set": {
                    "grades.$[]": 85
                }
            };
            let parsed = parse_update(&update_doc, None).unwrap();
            let expected = update([set([field_name("grades"), update_fn::all()], 85)]);
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_invalid_operator() {
            let update_doc = doc! { "$invalidOp": { "a": 1 } };
            let err = parse_update(&update_doc, None).unwrap_err();
            assert_eq!(err.to_string(), "Unknown update operator: $invalidOp");
        }

        #[test]
        fn test_parse_update_field_update_not_supported() {
            let update_doc = doc! { "a": 1 };
            let err = parse_update(&update_doc, None).unwrap_err();
            assert_eq!(err.to_string(), "Update operator must start with '$': a");
        }

        #[test]
        fn test_parse_update_with_array_filters() {
            let update_doc = doc! { "$set": { "grades.$[elem].mean": 100 } };
            let array_filters = Some(vec![doc! { "elem": { "grade": { "$gte": 85 } } }]);
            let parsed = parse_update(&update_doc, array_filters).unwrap();
            let expected_ops =
                vec![set([field_name("grades"), filter("elem"), field_name("mean")], 100)];
            let expected_filters = BTreeMap::from([(
                "elem".to_string(),
                field_filters(field(["grade"]), [gte(lit(85))]),
            )]);
            let expected = UpdateExpr { ops: expected_ops, array_filters: expected_filters };
            assert_eq!(parsed, expected);
        }

        #[test]
        fn test_parse_update_array_filters_not_provided() {
            let update_doc = doc! { "$set": { "grades.$[elem].mean": 100 } };
            let err = parse_update(&update_doc, None).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Filtered positional operator is used but arrayFilters is not specified"
            );
        }

        #[test]
        fn test_parse_update_array_filters_identifier_not_found() {
            let update_doc = doc! { "$set": { "grades.$[elem].mean": 100 } };
            let array_filters = Some(vec![doc! { "item": { "grade": { "$gte": 85 } } }]);
            let err = parse_update(&update_doc, array_filters).unwrap_err();
            assert_eq!(
                err.to_string(),
                "No array filter found for identifier 'elem' in path"
            );
        }

        #[test]
        fn test_parse_update_array_filters_invalid_filter_doc() {
            let update_doc = doc! { "$set": { "a": 1 } };
            let array_filters = Some(vec![doc! { "elem": { "g": 1 }, "elem2": { "h": 2 } }]);
            let err = parse_update(&update_doc, array_filters).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Array filters must have a single identifier. Found 'elem' and 'elem2'"
            );

            let array_filters_not_doc = Some(vec![doc! { "elem": "not a doc" }]);
            let err = parse_update(&update_doc, array_filters_not_doc).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Array filter for identifier 'elem' must reference at least one subfield (e.g. 'elem.x')."
            );
        }

        #[cfg(test)]
        mod array_filters_parsing {
            use super::*;

            #[test]
            fn test_parse_array_filters_simple() {
                let filters = vec![doc! { "elem": { "grade": { "$gte": 85 } } }];
                let parsed = parse_array_filters(&filters).unwrap();
                let expected = BTreeMap::from([(
                    "elem".to_string(),
                    field_filters(field(["grade"]), [gte(lit(85))]),
                )]);
                assert_eq!(parsed, expected);
            }

            #[test]
            fn test_parse_array_filters_with_path_in_identifier() {
                let filters = vec![doc! { "elem.grade": { "$gte": 85 } }];
                let parsed = parse_array_filters(&filters).unwrap();
                let expected = BTreeMap::from([(
                    "elem".to_string(),
                    field_filters(field(["grade"]), [gte(lit(85))]),
                )]);
                assert_eq!(parsed, expected);
            }

            #[test]
            fn test_parse_array_filters_multiple_filters() {
                let filters = vec![
                    doc! { "elem": { "grade": { "$gte": 85 } } },
                    doc! { "item.price": { "$lt": 100 } },
                ];
                let parsed = parse_array_filters(&filters).unwrap();
                let expected = BTreeMap::from([
                    (
                        "elem".to_string(),
                        field_filters(field(["grade"]), [gte(lit(85))]),
                    ),
                    (
                        "item".to_string(),
                        field_filters(field(["price"]), [lt(lit(100))]),
                    ),
                ]);
                assert_eq!(parsed, expected);
            }

            #[test]
            fn test_parse_array_filters_duplicate_identifier_error() {
                let filters = vec![
                    doc! { "elem": { "grade": { "$gte": 85 } } },
                    doc! { "elem": { "grade": { "$lt": 95 } } },
                ];
                let err = parse_array_filters(&filters).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "Found multiple array filters with the same identifier 'elem'"
                );
            }

            #[test]
            fn test_parse_array_filters_error_multiple_identifiers() {
                let filters = vec![doc! { "elem": { "g": 1 }, "elem2": { "h": 2 } }];
                let err = parse_array_filters(&filters).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "Array filters must have a single identifier. Found 'elem' and 'elem2'"
                );
            }

            #[test]
            fn test_parse_array_filters_error_not_a_document() {
                let filters = vec![doc! { "elem": "not a doc" }];
                let err = parse_array_filters(&filters).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "Array filter for identifier 'elem' must reference at least one subfield (e.g. 'elem.x')."
                );
            }

            #[test]
            fn test_parse_array_filters_error_numeric_identifier() {
                let filters = vec![doc! { "0.grade": { "$gte": 85 } }];
                let err = parse_array_filters(&filters).unwrap_err();
                assert_eq!(err.to_string(), "Array filter identifier must be a field name");
            }
        }
    }
}
