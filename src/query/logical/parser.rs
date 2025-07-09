use crate::query::logical::logical_plan::{Projection, SortField, SortOrder};
use crate::query::logical::{
    BsonValue, ComparisonOperator, ComparisonOperator::*, Expr, PathComponent,
};
use crate::Error;
use bson::{Bson, Document};
use std::sync::Arc;

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
                    return Err(Error::InvalidArgument(
                        "Invalid format for $not; must be a document".to_string(),
                    ));
                }
            }
            _ => {
                // Handle fields (regular or wildcard)
                let field = parse_field(key)?;
                if matches!(field, Expr::PositionalField(_)) {
                    return Err(Error::InvalidArgument(format!(
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
                return Err(Error::InvalidArgument(format!(
                    "Invalid format for {}; must be an array of documents",
                    operator
                )));
            }
        }

        match operator {
            "$and" => Ok(Expr::And(parsed_conditions)),
            "$or" => Ok(Expr::Or(parsed_conditions)),
            "$nor" => Ok(Expr::Nor(parsed_conditions)),
            _ => Err(Error::InvalidArgument(format!(
                "Unknown logical operator: {}",
                operator
            ))),
        }
    } else {
        Err(Error::InvalidArgument(format!(
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
                        return Err(Error::InvalidArgument(
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
                        return Err(Error::InvalidArgument(
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
                        return Err(Error::InvalidArgument(
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
                        return Err(Error::InvalidArgument("$all must be an array".to_string()));
                    }
                }
                "$elemMatch" => {
                    if let Bson::Document(_) = value {
                        let sub_condition = parse_predicates(value)?;
                        predicates.push(Arc::new(Expr::ElemMatch(sub_condition)))
                    } else {
                        return Err(Error::InvalidArgument(
                            "$elemMatch must be a document".to_string(),
                        ));
                    }
                }
                _ => return Err(Error::InvalidArgument(format!("Unknown operator: {}", key))),
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

pub fn parse_projection(doc: &Document) -> Result<Projection, Error> {
    let mut include_fields = Vec::new();
    let mut exclude_fields = Vec::new();

    for (key, value) in doc.iter() {
        match value {
            Bson::Int32(1) | Bson::Int64(1) => {
                include_fields.push(Arc::new(parse_field(key)?));
            }
            Bson::Int32(0) | Bson::Int64(0) => {
                let field = parse_field(key)?;
                if matches!(field, Expr::PositionalField(_)) {
                    return Err(Error::InvalidArgument(format!(
                        "Positional fields cannot be excluded: {}",
                        key
                    )));
                }
                exclude_fields.push(Arc::new(field));
            }
            Bson::Document(projection_doc) => {
                let field = Arc::new(parse_field(key)?);
                if projection_doc.len() != 1 {
                    return Err(Error::InvalidArgument(format!(
                        "Projection document for field '{}' must have exactly one operator.",
                        key
                    )));
                }

                let (op, op_value) = projection_doc.iter().next().unwrap();
                let projection_expr = match op.as_str() {
                    "$slice" => parse_slice_projection(field, op_value)?,
                    "$elemMatch" => parse_elem_match_projection(field, op_value)?,
                    _ => {
                        return Err(Error::InvalidArgument(format!(
                            "Unknown projection operator: {}",
                            op
                        )))
                    }
                };
                include_fields.push(projection_expr);
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "Invalid projection value for field '{}'",
                    key
                )))
            }
        }
    }

    match (!include_fields.is_empty(), !exclude_fields.is_empty()) {
        (true, false) => Ok(Projection::Include(include_fields)),
        (false, true) => Ok(Projection::Exclude(exclude_fields)),
        (true, true) => Err(Error::InvalidArgument(
            "Cannot mix inclusion and exclusion projections".to_string(),
        )),
        (false, false) => Err(Error::InvalidArgument(
            "Projection document cannot be empty".to_string(),
        )),
    }
}

fn parse_slice_projection(field: Arc<Expr>, value: &Bson) -> Result<Arc<Expr>, Error> {
    let (skip, limit) = match value {
        Bson::Int32(n) => (*n, None),
        Bson::Int64(n) => (*n as i32, None),
        Bson::Array(arr) => {
            if arr.len() != 2 {
                return Err(Error::InvalidArgument(
                    "$slice array must have exactly two elements".to_string(),
                ));
            }
            let skip = match &arr[0] {
                Bson::Int32(n) => *n,
                Bson::Int64(n) => *n as i32,
                _ => {
                    return Err(Error::InvalidArgument(
                        "$slice first element must be an integer".to_string(),
                    ))
                }
            };
            let limit = match &arr[1] {
                Bson::Int32(n) if *n > 0 => Some(*n as u32),
                Bson::Int64(n) if *n > 0 => Some(*n as u32),
                _ => {
                    return Err(Error::InvalidArgument(
                        "$slice limit must be a positive integer".to_string(),
                    ))
                }
            };
            (skip, limit)
        }
        _ => {
            return Err(Error::InvalidArgument(
                "$slice must be an integer or an array of two integers".to_string(),
            ))
        }
    };

    Ok(Arc::new(Expr::ProjectionSlice { field, skip, limit }))
}

fn parse_elem_match_projection(field: Arc<Expr>, value: &Bson) -> Result<Arc<Expr>, Error> {
    if let Bson::Document(doc) = value {
        let expr = parse_conditions(doc)?;
        Ok(Arc::new(Expr::ProjectionElemMatch { field, expr }))
    } else {
        Err(Error::InvalidArgument(
            "$elemMatch projection value must be a document".to_string(),
        ))
    }
}

fn parse_field(s: &String) -> Result<Expr, Error> {
    if s.ends_with(".$") {
        parse_field_path(s.trim_end_matches(".$")).map(Expr::PositionalField)
    } else if s.ends_with(".*") {
        parse_field_path(s.trim_end_matches(".*")).map(Expr::WildcardField)
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
        validate_field_name(component)?;
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
                return Err(Error::InvalidArgument(format!(
                    "Invalid sort order for field '{}'",
                    key
                )))
            }
        };

        let field = parse_field(key)?;

        match field {
            Expr::PositionalField(_) => {
                return Err(Error::InvalidArgument(format!(
                    "Positional fields cannot used for sorting: {}",
                    key
                )))
            }
            Expr::WildcardField(_) => {
                return Err(Error::InvalidArgument(format!(
                    "Wildcard fields cannot used for sorting: {}",
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

/// Validates a MongoDB field name.
fn validate_field_name(field_name: &str) -> Result<(), Error> {
    // Check for empty field name
    if field_name.is_empty() {
        return Err(Error::InvalidArgument(
            "Field name cannot be empty.".to_string(),
        ));
    }

    // Check for reserved characters
    if field_name.contains('.') {
        return Err(Error::InvalidArgument(
            "Field name cannot contain '.'. Nested paths should use dot-separated keys."
                .to_string(),
        ));
    }
    if field_name.starts_with('$') {
        return Err(Error::InvalidArgument(
            "Field name cannot start with '$'. This is reserved for operators.".to_string(),
        ));
    }
    if field_name.contains('\0') {
        return Err(Error::InvalidArgument(
            "Field name cannot contain null characters ('\\0').".to_string(),
        ));
    }

    // Check for length
    if field_name.len() > 255 {
        return Err(Error::InvalidArgument(
            "Field name cannot exceed 255 characters.".to_string(),
        ));
    }

    // If all checks pass
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical::expr_fn::*;
    use bson::{doc, Bson};

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
        let expected = field_filters(field(["field"]), [has_type(2, false)]);
        assert_eq!(expected, parsed);
    }

    #[test]
    fn test_parse_conditions_with_size() {
        let doc = doc! { "arrayField": { "$size": 3 } };
        let parsed = parse_conditions(&doc).unwrap();
        let expected = field_filters(field(["arrayField"]), [size(3, false)]);
        assert_eq!(expected, parsed);
    }

    #[test]
    fn test_parse_conditions_with_all() {
        let doc = doc! { "tags": { "$all": ["tag1", "tag2"] } };
        let parsed = parse_conditions(&doc).unwrap();
        let expected = field_filters(field(["tags"]), [all(vec!("tag1", "tag2"))]);
        assert_eq!(expected, parsed);
    }

    #[test]
    fn test_parse_conditions_with_elem_match() {
        let doc = doc! { "nestedArray": { "$elemMatch": { "$gt": 22, "$lt": 30 } } };
        let parsed = parse_conditions(&doc).unwrap();
        let expected = field_filters(field(["nestedArray"]), [elem_match([gt(22), lt(30)])]);
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
            field_filters(field(["age"]), [gte(18)]),
            field_filters(field(["status"]), [eq("active")]),
            field_filters(field(["tags"]), [all(vec!("tag1", "tag2"))]),
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
                field_filters(field(["field1"]), [gte(10)]),
                field_filters(field(["field2"]), [eq("value")]),
            ]),
            field_filters(field(["field3"]), [lt(20)]),
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
            field_filters(field(["field1"]), [ne(5)]),
            or([
                field_filters(field(["field2"]), [within(vec![1, 2, 3])]),
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
                field_filters(field(["field1"]), [lte(15)]),
                field_filters(field(["field2"]), [has_type(2, false)]),
            ]),
            field_filters(field(["field3"]), [size(3, false)]),
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
                field_filters(field(["field1"]), [gt(50)]),
                field_filters(field(["field2"]), [eq("test")]),
            ]),
            or([
                field_filters(field(["field3"]), [lt(20)]),
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

    #[test]
    fn test_parse_projection_include() {
        let projection = doc! {"name": 1, "age": 1};
        let parsed = parse_projection(&projection);
        assert!(parsed.is_ok());
        assert_eq!(
            parsed.unwrap(),
            Projection::Include(vec![field(["name"]), field(["age"]),])
        );
    }

    #[test]
    fn test_parse_projection_include_with_wildcard() {
        let projection = doc! {"name.*": 1, "age": 1};
        let parsed = parse_projection(&projection);
        assert!(parsed.is_ok());
        assert_eq!(
            parsed.unwrap(),
            Projection::Include(vec![wildcard_field(["name"]), field(["age"]),])
        );
    }

    #[test]
    fn test_parse_projection_exclude() {
        let projection = doc! {"password": 0, "secret": 0};
        let parsed = parse_projection(&projection);
        assert!(parsed.is_ok());
        assert_eq!(
            parsed.unwrap(),
            Projection::Exclude(vec![field(["password"]), field(["secret"]),])
        );
    }

    #[test]
    fn test_parse_projection_mixed_include_exclude() {
        let projection = doc! {"name": 1, "password": 0};
        let parsed = parse_projection(&projection);
        assert!(parsed.is_err());
        assert_eq!(
            parsed.unwrap_err().to_string(),
            "Cannot mix inclusion and exclusion projections"
        );
    }

    #[test]
    fn test_parse_projection_invalid_value() {
        let projection = doc! { "name": "invalid" };

        let parsed = parse_projection(&projection);
        assert!(parsed.is_err());
        assert_eq!(
            parsed.unwrap_err().to_string(),
            "Invalid projection value for field 'name'"
        );
    }

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
    fn test_parse_sort_invalid_wildcard_field() {
        let sort = doc! { "name.*": 1, "age": 1 };

        let parsed = parse_sort(&sort);
        assert!(parsed.is_err());
        assert_eq!(
            parsed.unwrap_err().to_string(),
            "Wildcard fields cannot used for sorting: name.*"
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
    fn test_parse_projection_with_slice() {
        let projection = doc! { "comments": { "$slice": 5 } };
        let parsed = parse_projection(&projection).unwrap();
        let expected = Projection::Include(vec![projection_slice(field(["comments"]), 5, None)]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_parse_projection_with_slice_array() {
        let projection = doc! { "comments": { "$slice": [10, 5] } };
        let parsed = parse_projection(&projection).unwrap();
        let expected =
            Projection::Include(vec![projection_slice(field(["comments"]), 10, Some(5))]);
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
        assert_eq!(err.to_string(), "$slice limit must be a positive integer");

        let projection = doc! { "comments": { "$slice": [1, -1] } };
        let err = parse_projection(&projection).unwrap_err();
        assert_eq!(err.to_string(), "$slice limit must be a positive integer");
    }

    #[test]
    fn test_parse_projection_with_elem_match() {
        let projection = doc! { "students": { "$elemMatch": { "school": "Hogwarts" } } };
        let parsed = parse_projection(&projection).unwrap();
        let expected = Projection::Include(vec![projection_elem_match(
            field(["students"]),
            field_filters(field(["school"]), [eq("Hogwarts")]),
        )]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_parse_projection_with_elem_match_complex() {
        let projection =
            doc! { "grades": { "$elemMatch": { "grade": { "$gte": 85 }, "mean": { "$gt": 90 } } } };
        let parsed = parse_projection(&projection).unwrap();
        let expected = Projection::Include(vec![projection_elem_match(
            field(["grades"]),
            and([
                field_filters(field(["grade"]), [gte(85)]),
                field_filters(field(["mean"]), [gt(90)]),
            ]),
        )]);
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
}
