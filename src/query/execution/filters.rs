use crate::query::{get_path_value, BsonValue, BsonValueRef, ComparisonOperator, Expr, Parameters};
use bson::{Bson, Document};
use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use bson::spec::ElementType;
use crate::util::interval::Interval;

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

pub fn to_type_filter(type_spec: &Bson) -> Box<dyn Fn(ElementType) -> bool + Send + Sync> {

    // Handle "number" alias, which matches multiple numeric types
    if let Bson::String(s) = type_spec {
        if s == "number" {
            return Box::new(move |value_type|
                matches!(value_type,
                    ElementType::Double
                        | ElementType::Int32
                        | ElementType::Int64
                        | ElementType::Decimal128
            ));
        }
    }

    // Get the expected BSON element type from the type specifier
    let expected_type = match type_spec {
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
        Some(et) => Box::new(move |value_type| value_type == et),
        None => Box::new(move |_value_type| false), // Invalid type specifier results in no match
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
                ComparisonOperator::Ne => !comparison_value.0.as_null().is_some(),
                _ => false,
            };
        }
    };

    match operator {
        ComparisonOperator::Eq => document_value == comparison_value,
        ComparisonOperator::Gt => document_value > comparison_value,
        ComparisonOperator::Gte => document_value >= comparison_value,
        ComparisonOperator::Lt => document_value < comparison_value,
        ComparisonOperator::Lte => document_value <= comparison_value,
        ComparisonOperator::Ne => document_value != comparison_value,
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
pub fn to_value_filter(
    filter: Arc<Expr>,
    parameters: &Parameters,
) -> Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync> {

    match filter.as_ref() {
        Expr::FieldFilters { field, filters } => {
            let path = match field.as_ref() {
                Expr::Field(path) => path.clone(),
                _ => panic!("Expected a field expression"),
            };
            let value_filters = to_value_filters(filters, parameters);
            Box::new(move |field_value| {
                if field_value.is_none() {
                    return false; // If the field is missing, no filters can match
                }
                let doc = match field_value.unwrap() {
                    BsonValueRef(Bson::Document(doc)) => doc,
                    _ => return false, // If the field value is not a document, no filters can match
                };
                // Resolve the field value using the path
                let field_value = get_path_value(doc, &path);
                value_filters.iter().all(|f| f(field_value))
            })
        }
        Expr::Comparison { operator, value } => {
            let operator = *operator;
            let comparison_value = match value.as_ref() {
                Expr::Placeholder(idx) => parameters.get(*idx).clone(),
                Expr::Literal(val) => val.clone(),
                _ => panic!("Comparison value must be a placeholder or a literal but was {:?}", value),
            };
            Box::new(move |field_value| compare_value(operator, comparison_value.as_ref(), field_value))
        }
        Expr::Interval(interval) => {

            let start = resolve_bound(interval.start_bound(), parameters);
            let end = resolve_bound(interval.end_bound(), parameters);
            let interval = Interval::new(start, end);

            Box::new(move |field_value| {
                match field_value {
                    // Arrays are compared element-wise. $eq: null matches if any element is null or if the whole field is missing
                    Some(BsonValueRef(Bson::Array(array))) => array.iter().any(|val| contains(&interval, BsonValueRef(&val))),
                    Some(val) => contains(&interval, val),
                    // $eq: null matches also missing fields so we need to check for point intervals with null bounds
                    None => interval.is_point() && interval.start_bound_value().and_then(|v| v.0.as_null()).is_some(), //
                }
            })
        }
        // Implements the `$exists` filter as:
        // - `$exists: true` matches documents where the field is present.
        // - `$exists: false` matches documents where the field is missing.
        Expr::Exists(exists) => {
            let exists = *exists;
            Box::new(move |field_value| {
                field_value.is_some() == exists
            })
        }
        // Implements the `$elemMatch` filter as:
        // - Matches documents where at least one element in an array matches all the specified filter conditions.
        // - If the array element is a document, all document-level filters must match.
        // - If the array element is a scalar, all value-level filters must match.
        Expr::ElemMatch(filters) => {
            let is_doc_filter = matches!(filters.first().map(|f| f.as_ref()), Some(Expr::FieldFilters { .. }) | Some(Expr::And(_)) | Some(Expr::Or(_)) | Some(Expr::Not(_)));

            if is_doc_filter {
                let elem_filters: Vec<_> = to_filters(filters, parameters);
                Box::new(move |field_value| {
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
                Box::new(move |field_value| {
                    elem_match_array(field_value, |elem| {
                        let elem = BsonValueRef(elem);
                        elem_filters.iter().all(|f| f(Some(elem)))
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
            // Precompute the type filter function
            let type_filter = to_type_filter(&type_spec.0);

            Box::new(move |field_value| {
                let result = if let Some(field_value) = field_value {
                    type_filter(field_value.0.element_type())
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
            Box::new(move |field_value| {
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

            Box::new(move |field_value| {
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
            Box::new(move |field_value| !inner(field_value))
        },
        Expr::And(children) => {
            let filters = to_value_filters(children, parameters);
            Box::new(move |field_value| filters.iter().all(|f| f(field_value)))
        }
        Expr::Or(children) => {
            let filters = to_value_filters(children, parameters);
            Box::new(move |field_value| filters.iter().any(|f| f(field_value)))
        }
        Expr::Nor(children) => {
            let filters = to_value_filters(children, parameters);
            Box::new(move |field_value| !filters.iter().any(|f| f(field_value)))
        }
        _ => panic!("Unsupported value filter: {:?}", filter),
    }
}

fn contains(interval: &Interval<BsonValue>, item: BsonValueRef) -> bool
{
    (match interval.start_bound() {
        Bound::Included(start) => start.as_ref() <= item,
        Bound::Excluded(start) => start.as_ref() < item,
        Bound::Unbounded => true,
    }) && (match interval.end_bound() {
        Bound::Included(end) => item <= end.as_ref(),
        Bound::Excluded(end) => item < end.as_ref(),
        Bound::Unbounded => true,
    })
}

fn resolve_bound(bound: Bound<&Arc<Expr>>, parameters: &Parameters) ->Bound<BsonValue> {
    let get_value = |expr: &Arc<Expr>| match expr.as_ref() {
        Expr::Placeholder(idx) => parameters.get(*idx).clone(),
        _ => panic!("Interval bound must be a placeholder"),
    };
    match bound {
        Bound::Included(expr) => Bound::Included(get_value(expr)),
        Bound::Excluded(expr) => Bound::Excluded(get_value(expr)),
        Bound::Unbounded => Bound::Unbounded,
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
        Expr::FieldFilters { field, filters } => {
            let path = match field.as_ref() {
                Expr::Field(path) => path.clone(),
                _ => panic!("Expected a field expression"),
            };
            let value_filters = to_value_filters(filters, parameters);
            Box::new(move |doc: &Document| {
                let field_value = get_path_value(doc, &path);
                value_filters.iter().all(|f| f(field_value))
            })
        },
        _ => panic!("Unsupported top-level filter: {:?}", expr),
    }
}

fn to_value_filters(filters: &Vec<Arc<Expr>>, parameters: &Parameters) -> Vec<Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync>> {
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
    use bson::{doc, Bson, Decimal128};

    #[test]
    fn test_to_type_filter_with_string_alias() {

        assert!(matches_type_spec(
            &Bson::String("hello".to_string()),
            &Bson::from("string")
        ));
        assert!(matches_type_spec(
            &Bson::Int32(1),
            &Bson::from("int")
        ));
        assert!(matches_type_spec(
            &Bson::Int64(1),
            &Bson::from("long")
        ));
        assert!(matches_type_spec(
            &Bson::Double(1.0),
            &Bson::from("double")
        ));
        assert!(matches_type_spec(
            &Bson::Array(vec![]),
            &Bson::from("array")
        ));
        assert!(matches_type_spec(
            &Bson::Document(doc! {}),
            &Bson::from("object")
        ));
        assert!(matches_type_spec(
            &Bson::Boolean(true),
            &Bson::from("bool")
        ));
        assert!(matches_type_spec(
            &Bson::Null,
            &Bson::from("null")
        ));
        assert!(matches_type_spec(
            &Bson::Decimal128(Decimal128::from_str("1").unwrap()),
            &Bson::from("decimal")
        ));

        assert!(!matches_type_spec(
            &Bson::String("hello".to_string()),
            &Bson::from("int")
        ));
    }

    fn matches_type_spec(value: &Bson, type_spec: &Bson) -> bool {
        let type_filter = to_type_filter(type_spec);
        type_filter(value.element_type())
    }

    #[test]
    fn test_check_bson_type_by_numeric_code() {
        let s = Bson::String("hello".to_string());

        // String is type 2
        assert!(matches_type_spec(&Bson::from(&s), &Bson::from(2)));
        assert!(matches_type_spec(&Bson::from(&s), &Bson::from(2i64)));
        assert!(matches_type_spec(&Bson::from(&s), &Bson::Double(2.0)));

        // Int32 is type 16
        assert!(matches_type_spec(&Bson::Int32(1), &Bson::from(16)));
        // Int64 is type 18
        assert!(matches_type_spec(&Bson::Int64(1), &Bson::from(18)));
        // Double is type 1
        assert!(matches_type_spec(&Bson::Double(1.0), &Bson::from(1)));

        // Mismatch
        assert!(!matches_type_spec(&Bson::Int32(1), &Bson::from(1)));
    }

    #[test]
    fn test_check_bson_type_for_number_alias() {
        let number_spec = Bson::from("number");

        assert!(matches_type_spec(&Bson::Int32(1), &number_spec));
        assert!(matches_type_spec(&Bson::Int64(1), &number_spec));
        assert!(matches_type_spec(&Bson::Double(1.0), &number_spec));
        assert!(matches_type_spec(&Bson::Decimal128(Decimal128::from_str("1").unwrap()), &number_spec));
        assert!(!matches_type_spec(&Bson::String("hello".to_string()), &number_spec));
    }

    #[test]
    fn test_check_bson_type_with_invalid_spec() {
        let i32 = Bson::Int32(1);

        // Invalid string
        assert!(!matches_type_spec(
            &Bson::from(&i32),
            &Bson::from("not-a-type")
        ));
        // Invalid number code
        assert!(!matches_type_spec(
            &Bson::from(&i32),
            &Bson::from(12345)
        ));
        // Invalid spec type
        assert!(!matches_type_spec(
            &Bson::from(&i32),
            &Bson::from(true)
        ));
    }
}
