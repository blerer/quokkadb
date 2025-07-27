use crate::query::{get_path_value, BsonValue, BsonValueRef, ComparisonOperator, Expr, Parameters};
use bson::{Bson, Document};
use std::collections::HashSet;
use std::sync::Arc;
use bson::spec::ElementType;

/// Converts a numeric BSON type code to an `ElementType`.
fn get_element_type_from_code(code: u8) -> Option<ElementType> {
    match code {
        1 => Some(ElementType::Double),
        2 => Some(ElementType::String),
        3 => Some(ElementType::EmbeddedDocument),
        4 => Some(ElementType::Array),
        5 => Some(ElementType::Binary),
        6 => Some(ElementType::Undefined),
        7 => Some(ElementType::ObjectId),
        8 => Some(ElementType::Boolean),
        9 => Some(ElementType::DateTime),
        10 => Some(ElementType::Null),
        11 => Some(ElementType::RegularExpression),
        13 => Some(ElementType::JavaScriptCode),
        14 => Some(ElementType::Symbol),
        15 => Some(ElementType::JavaScriptCodeWithScope),
        16 => Some(ElementType::Int32),
        17 => Some(ElementType::Timestamp),
        18 => Some(ElementType::Int64),
        19 => Some(ElementType::Decimal128),
        127 => Some(ElementType::MaxKey),
        255 => Some(ElementType::MinKey),
        _ => None,
    }
}

/// Checks if a BSON value's type matches the specified type.
/// The type can be specified as a string alias or a numeric type code.
fn check_bson_type(value: BsonValueRef, type_spec: &BsonValue) -> bool {
    let type_spec_bson = &type_spec.0;
    let value_type = value.0.element_type();

    // Handle "number" alias, which matches multiple numeric types
    if let Bson::String(s) = type_spec_bson {
        if s == "number" {
            return matches!(
                value_type,
                ElementType::Double
                    | ElementType::Int32
                    | ElementType::Int64
                    | ElementType::Decimal128
            );
        }
    }

    // Get the expected BSON element type from the type specifier
    let expected_type = match type_spec_bson {
        Bson::Int32(i) => u8::try_from(*i).ok().and_then(get_element_type_from_code),
        Bson::Int64(i) => u8::try_from(*i).ok().and_then(get_element_type_from_code),
        Bson::Double(f) => {
            if f.trunc() == *f && *f >= 0.0 && *f <= u8::MAX as f64 {
                get_element_type_from_code(*f as u8)
            } else {
                None
            }
        }
        Bson::String(s) => match s.as_str() {
            "double" => Some(ElementType::Double),
            "string" => Some(ElementType::String),
            "object" => Some(ElementType::EmbeddedDocument),
            "array" => Some(ElementType::Array),
            "binData" => Some(ElementType::Binary),
            "undefined" => Some(ElementType::Undefined),
            "objectId" => Some(ElementType::ObjectId),
            "bool" => Some(ElementType::Boolean),
            "date" => Some(ElementType::DateTime),
            "null" => Some(ElementType::Null),
            "regex" => Some(ElementType::RegularExpression),
            "dbPointer" => Some(ElementType::DbPointer),
            "javascript" => Some(ElementType::JavaScriptCode),
            "symbol" => Some(ElementType::Symbol),
            "javascriptWithScope" => Some(ElementType::JavaScriptCodeWithScope),
            "int" => Some(ElementType::Int32),
            "timestamp" => Some(ElementType::Timestamp),
            "long" => Some(ElementType::Int64),
            "decimal" => Some(ElementType::Decimal128),
            "minKey" => Some(ElementType::MinKey),
            "maxKey" => Some(ElementType::MaxKey),
            _ => None,
        },
        _ => None,
    };

    match expected_type {
        Some(et) => value_type == et,
        None => false, // Invalid type specifier results in no match
    }
}

/// Compares a value against a document value using the specified comparison operator.
fn compare_value(
    operator: ComparisonOperator,
    comparison_value: BsonValueRef,
    document_value: Option<BsonValueRef>,
) -> bool {
    // Handle special cases for array comparisons
    if let Some(BsonValueRef(Bson::Array(array))) = document_value {
        if operator != ComparisonOperator::In && operator != ComparisonOperator::Nin {
            return array
                .iter()
                .any(|elem| compare_value(operator, comparison_value, Some(BsonValueRef(elem))));
        }
    }

    let document_value = match document_value {
        Some(v) => v,
        None => {
            return match operator {
                ComparisonOperator::Eq => comparison_value.0.as_null().is_some(),
                ComparisonOperator::Ne => !comparison_value.0.as_null().is_some(),
                _ => false,
            };
        }
    };

    // For comparison operators, null should never match
    match operator {
        ComparisonOperator::Gt
        | ComparisonOperator::Gte
        | ComparisonOperator::Lt
        | ComparisonOperator::Lte => {
            if document_value.0.as_null().is_some() {
                return false;
            }
        }
        _ => {}
    }

    match operator {
        ComparisonOperator::Eq => document_value == comparison_value,
        ComparisonOperator::Ne => document_value != comparison_value,
        ComparisonOperator::Gt => document_value > comparison_value,
        ComparisonOperator::Gte => document_value >= comparison_value,
        ComparisonOperator::Lt => document_value < comparison_value,
        ComparisonOperator::Lte => document_value <= comparison_value,
        ComparisonOperator::In => {
            if let BsonValueRef(Bson::Array(array)) = comparison_value {
                array.iter().any(|item| BsonValueRef(item) == document_value)
            } else {
                false
            }
        }
        ComparisonOperator::Nin => {
            if let BsonValueRef(Bson::Array(array)) = comparison_value {
                !array.iter().any(|item| BsonValueRef(item) == document_value)
            } else {
                true
            }
        }
    }
}

/// Converts a value filter expression into a function that can be applied to document values.
fn to_value_filter(
    filter: Arc<Expr>,
    parameters: &Parameters,
) -> Box<dyn Fn(Option<BsonValueRef>, &Document) -> bool + Send + Sync> {

    match filter.as_ref() {
        Expr::Comparison { operator, value } => {
            let operator = *operator;
            let comparison_value = match value.as_ref() {
                Expr::Placeholder(idx) => parameters.get(*idx).clone(),
                _ => panic!("Comparison value must be a placeholder"),
            };
            Box::new(move |field_value, _doc| compare_value(operator, comparison_value.as_ref(), field_value))
        }
        // Implements the `$exists` filter as:
        // - `$exists: true` matches documents where the field is present.
        // - `$exists: false` matches documents where the field is missing.
        Expr::Exists(exists) => {
            let exists = *exists;
            Box::new(move |field_value, _doc| {
                field_value.is_some() == exists
            })
        }
        // Implements the `$elemMatch` filter as:
        // - Matches documents where at least one element in an array matches all the specified filter conditions.
        // - If the array element is a document, all document-level filters must match.
        // - If the array element is a scalar, all value-level filters must match.
        Expr::ElemMatch(filters) => {
            let is_doc_filter = matches!(filters.first().map(|f| f.as_ref()), Some(Expr::FieldFilters { .. }));

            if is_doc_filter {
                let elem_filters: Vec<_> = to_filters(filters, parameters);
                Box::new(move |field_value, _doc| {
                    elem_match_array(field_value, |elem| {
                        if let Bson::Document(sub_doc) = elem {
                            elem_filters.iter().all(|f| f(sub_doc))
                        } else {
                            false
                        }
                    })
                })
            } else {
                let elem_filters = to_value_filters(filters, parameters);
                Box::new(move |field_value, doc| {
                    elem_match_array(field_value, |elem| {
                        let elem = BsonValueRef(elem);
                        elem_filters.iter().all(|f| f(Some(elem), doc))
                    })
                })
            }
        }
        // Implements the `$type` filter as:
        // - Matches documents where a field's BSON type matches the specified type.
        // - The `$type` value must be a placeholder that resolves to a BSON type code or string alias.
        // - If negated, matches documents where the field's BSON type does not match the specified type.
        Expr::Type { bson_type, negated } => {
            let type_spec = match bson_type.as_ref() {
                Expr::Placeholder(idx) => parameters.get(*idx).clone(),
                _ => panic!("$type value must be a placeholder"),
            };
            let negated = *negated;

            Box::new(move |field_value, _doc| {
                let result = if let Some(field_value) = field_value {
                    check_bson_type(field_value, &type_spec)
                } else {
                    false
                };
                result != negated
            })
        }
        // Implements the `$size` filter as:
        // - Matches documents where an array field has a specific size.
        // - The `$size` value must be a placeholder that resolves to an integer.
        // - If negated, matches documents where the array field does not have the specified size.
        Expr::Size { size, negated } => {
            let size = match size.as_ref() {
                Expr::Placeholder(idx) => match &parameters.get(*idx).0 {
                    Bson::Int32(i) => *i as usize,
                    Bson::Int64(i) => *i as usize,
                    _ => panic!("$size must be an integer"),
                },
                _ => panic!("$size value must be a placeholder"),
            };
            let negated = *negated;
            Box::new(move |field_value, _doc| {
                let result = if let Some(BsonValueRef(Bson::Array(arr))) = field_value {
                    arr.len() == size
                } else {
                    false
                };
                result != negated
            })
        }
        // Implements the `$all` filter as:
        // - Matches documents where an array field contains all specified values.
        // - The `$all` value must be an array placeholder.
        Expr::All(value) => {
            let comparison_array = match value.as_ref() {
                Expr::Placeholder(idx) => match &parameters.get(*idx).0 {
                    Bson::Array(arr) => arr.clone(),
                    _ => panic!("$all value must be an array placeholder"),
                },
                _ => panic!("$all value must be a placeholder"),
            };

            Box::new(move |field_value, _doc| {
                if let Some(BsonValueRef(Bson::Array(array))) = field_value {
                    let set: HashSet<_> = array.iter().map(BsonValueRef).collect();
                    comparison_array
                        .iter()
                        .all(|val| set.contains(&BsonValueRef(val)))
                } else {
                    false
                }
            })
        },
        Expr::Not(child) => {
            let inner = to_value_filter(child.clone(), parameters);
            Box::new(move |field_value, doc| !inner(field_value, doc))
        }
        _ => panic!("Unsupported value filter: {:?}", filter),
    }
}


fn elem_match_array<F>(field_value: Option<BsonValueRef>, matcher: F,) -> bool
where
    F: Fn(&Bson) -> bool,
{
    if let Some(BsonValueRef(Bson::Array(arr))) = field_value {
        arr.iter().any(matcher)
    } else {
        false
    }
}

/// Compiles an expression into a filter function that can be applied to documents.
pub fn to_filter(expr: Arc<Expr>, parameters: &Parameters) -> Box<dyn Fn(&Document) -> bool + Send + Sync> {

    match expr.as_ref() {
        Expr::And(children) => {
            let children_filters = to_filters(children, parameters);
            Box::new(move |doc: &Document| children_filters.iter().all(|f| f(doc)))
        }
        Expr::Or(children) => {
            let children_filters = to_filters(children, parameters);
            Box::new(move |doc: &Document| children_filters.iter().any(|f| f(doc)))
        }
        Expr::Not(child) => {
            let child_filter = to_filter(child.clone(), parameters);
            Box::new(move |doc: &Document| !child_filter(doc))
        }
        Expr::Nor(children) => {
            let children_filters = to_filters(children, parameters);
            Box::new(move |doc: &Document| !children_filters.iter().any(|f| f(doc)))
        }
        Expr::AlwaysTrue => Box::new(|_| true),
        Expr::AlwaysFalse => Box::new(|_| false),
        Expr::FieldFilters { field, filters } => {
            let path = match field.as_ref() {
                Expr::Field(path) => path.clone(),
                _ => panic!("Expected a field expression"),
            };
            let value_filters = to_value_filters(filters, parameters);
            Box::new(move |doc: &Document| {
                let field_value = get_path_value(doc, &path);
                value_filters.iter().all(|f| f(field_value, doc))
            })
        },
        _ => panic!("Unsupported top-level filter: {:?}", expr),
    }
}

fn to_value_filters(filters: &Vec<Arc<Expr>>, parameters: &Parameters) -> Vec<Box<dyn Fn(Option<BsonValueRef>, &Document) -> bool + Send + Sync>> {
    filters.iter()
           .map(|f| to_value_filter(f.clone(), parameters))
           .collect()
}

fn to_filters(children: &Vec<Arc<Expr>>, parameters: &Parameters) -> Vec<Box<dyn Fn(&Document) -> bool + Send + Sync>> {
    children.iter()
            .map(|c| to_filter(c.clone(), parameters))
            .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use crate::query::{BsonValue, BsonValueRef};
    use bson::{doc, Bson, Decimal128};

    #[test]
    fn test_check_bson_type_by_string_alias() {
        assert!(check_bson_type(
            BsonValueRef(&Bson::String("hello".to_string())),
            &BsonValue::from("string")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Int32(1)),
            &BsonValue::from("int")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Int64(1)),
            &BsonValue::from("long")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Double(1.0)),
            &BsonValue::from("double")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Array(vec![])),
            &BsonValue::from("array")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Document(doc! {})),
            &BsonValue::from("object")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Boolean(true)),
            &BsonValue::from("bool")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Null),
            &BsonValue::from("null")
        ));
        assert!(check_bson_type(
            BsonValueRef(&Bson::Decimal128(Decimal128::from_str("1").unwrap())),
            &BsonValue::from("decimal")
        ));

        assert!(!check_bson_type(
            BsonValueRef(&Bson::String("hello".to_string())),
            &BsonValue::from("int")
        ));
    }

    #[test]
    fn test_check_bson_type_by_numeric_code() {
        let s = Bson::String("hello".to_string());

        // String is type 2
        assert!(check_bson_type(BsonValueRef(&s), &BsonValue::from(2)));
        assert!(check_bson_type(
            BsonValueRef(&s),
            &BsonValue::from(2i64)
        ));
        assert!(check_bson_type(
            BsonValueRef(&s),
            &BsonValue(Bson::Double(2.0))
        ));

        // Int32 is type 16
        assert!(check_bson_type(BsonValueRef(&Bson::Int32(1)), &BsonValue::from(16)));
        // Int64 is type 18
        assert!(check_bson_type(BsonValueRef(&Bson::Int64(1)), &BsonValue::from(18)));
        // Double is type 1
        assert!(check_bson_type(BsonValueRef(&Bson::Double(1.0)), &BsonValue::from(1)));

        // Mismatch
        assert!(!check_bson_type(BsonValueRef(&Bson::Int32(1)), &BsonValue::from(1)));
    }

    #[test]
    fn test_check_bson_type_for_number_alias() {
        let number_spec = BsonValue::from("number");

        assert!(check_bson_type(BsonValueRef(&Bson::Int32(1)), &number_spec));
        assert!(check_bson_type(BsonValueRef(&Bson::Int64(1)), &number_spec));
        assert!(check_bson_type(BsonValueRef(&Bson::Double(1.0)), &number_spec));
        assert!(check_bson_type(BsonValueRef(&Bson::Decimal128(Decimal128::from_str("1").unwrap())), &number_spec));
        assert!(!check_bson_type(BsonValueRef(&Bson::String("hello".to_string())), &number_spec));
    }

    #[test]
    fn test_check_bson_type_with_invalid_spec() {
        let i32 = Bson::Int32(1);

        // Invalid string
        assert!(!check_bson_type(
            BsonValueRef(&i32),
            &BsonValue::from("not-a-type")
        ));
        // Invalid number code
        assert!(!check_bson_type(
            BsonValueRef(&i32),
            &BsonValue::from(12345)
        ));
        // Invalid spec type
        assert!(!check_bson_type(
            BsonValueRef(&i32),
            &BsonValue::from(true)
        ));
    }
}
