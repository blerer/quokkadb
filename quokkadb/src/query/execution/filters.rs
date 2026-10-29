use crate::query::{BsonValue, BsonValueRef, ComparisonOperator, Expr, Parameters, PathComponent};
use crate::util::interval::Interval;
use bson::spec::ElementType;
use bson::{Bson, Document};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

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
            return Box::new(move |value_type| {
                matches!(
                    value_type,
                    ElementType::Double
                        | ElementType::Int32
                        | ElementType::Int64
                        | ElementType::Decimal128
                )
            });
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
    compare_value_with_array_traversal(operator, comparison_value, document_value, true)
}

fn compare_value_with_array_traversal(
    operator: ComparisonOperator,
    comparison_value: BsonValueRef,
    document_value: Option<BsonValueRef>,
    expand_arrays: bool,
) -> bool {
    use ComparisonOperator::*;
    match operator {
        Ne => {
            return !compare_value_with_array_traversal(
                Eq,
                comparison_value,
                document_value,
                expand_arrays,
            );
        }
        Nin => {
            return !compare_value_with_array_traversal(
                In,
                comparison_value,
                document_value,
                expand_arrays,
            );
        }
        In => {
            return match comparison_value.0 {
                Bson::Array(values) => values.iter().any(|value| {
                    compare_value_with_array_traversal(
                        Eq,
                        BsonValueRef(value),
                        document_value,
                        expand_arrays,
                    )
                }),
                _ => false,
            };
        }
        _ => {}
    }
    let Some(document_value) = document_value else {
        return operator == Eq && matches!(comparison_value.0, Bson::Null);
    };
    let compare = |value: BsonValueRef| match operator {
        Eq => value.cmp(&comparison_value).is_eq(),
        Gt => value > comparison_value,
        Gte => value >= comparison_value,
        Lt => value < comparison_value,
        Lte => value <= comparison_value,
        _ => unreachable!("negative and membership comparisons handled above"),
    };
    if expand_arrays && let Bson::Array(values) = document_value.0 {
        // Array equality also checks the whole array. Only immediate elements
        // participate in membership: nested arrays are values, not flattened lists.
        if matches!(comparison_value.0, Bson::Array(_)) && compare(document_value) {
            return true;
        }
        return values.iter().any(|value| compare(BsonValueRef(value)));
    }
    compare(document_value)
}

/// Converts a value filter expression into a function that can be applied to document values.
pub fn to_value_filter(
    filter: Arc<Expr>,
    parameters: &Parameters,
) -> Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync> {
    to_value_filter_with_array_traversal(filter, parameters, true)
}

fn to_value_filter_with_array_traversal(
    filter: Arc<Expr>,
    parameters: &Parameters,
    expand_arrays: bool,
) -> Box<dyn Fn(Option<BsonValueRef>) -> bool + Send + Sync> {
    match filter.as_ref() {
        Expr::FieldFilters { field, filters } => {
            let document_filter = to_filter(
                Arc::new(Expr::FieldFilters {
                    field: field.clone(),
                    filters: filters.clone(),
                }),
                parameters,
            );
            Box::new(move |field_value| match field_value {
                Some(BsonValueRef(Bson::Document(doc))) => document_filter(doc),
                _ => false,
            })
        }

        Expr::Comparison { operator, value } => {
            let operator = *operator;
            let comparison_value = match value.as_ref() {
                Expr::Placeholder(idx) => parameters.get(*idx).clone(),
                Expr::Literal(val) => val.clone(),
                _ => unreachable!(
                    "Comparison value must be a placeholder or a literal but was {:?}",
                    value
                ),
            };
            Box::new(move |field_value| {
                compare_value_with_array_traversal(
                    operator,
                    comparison_value.as_ref(),
                    field_value,
                    expand_arrays,
                )
            })
        }
        Expr::Interval(interval) => {
            let start = resolve_bound(interval.start_bound(), parameters);
            let end = resolve_bound(interval.end_bound(), parameters);
            let interval = Interval::new(start, end);
            let point_value = interval
                .is_point()
                .then(|| interval.start_bound_value().unwrap());
            // Array-valued bounds can compare the entire array as well as
            // its immediate elements, just like the original comparison.
            let array_bound = [interval.start_bound(), interval.end_bound()]
                .into_iter()
                .any(|bound| {
                    matches!(
                        bound,
                        Bound::Included(BsonValue(Bson::Array(_)))
                            | Bound::Excluded(BsonValue(Bson::Array(_)))
                    )
                });

            Box::new(move |field_value| {
                if let Some(value) = &point_value {
                    return compare_value_with_array_traversal(
                        ComparisonOperator::Eq,
                        value.as_ref(),
                        field_value,
                        expand_arrays,
                    );
                }
                if array_bound && let Some(value @ BsonValueRef(Bson::Array(_))) = field_value {
                    if contains(&interval, value) {
                        return true;
                    }
                }
                match field_value {
                    // Arrays are compared element-wise. $eq: null matches if any element is null or if the whole field is missing
                    Some(BsonValueRef(Bson::Array(array))) if expand_arrays => array
                        .iter()
                        .any(|val| contains(&interval, BsonValueRef(&val))),
                    Some(val) => contains(&interval, val),
                    // $eq: null matches also missing fields so we need to check for point intervals with null bounds
                    None => {
                        interval.is_point()
                            && interval
                                .start_bound_value()
                                .and_then(|v| v.0.as_null())
                                .is_some()
                    } //
                }
            })
        }
        // Implements the `$exists` filter as:
        // - `$exists: true` matches documents where the field is present.
        // - `$exists: false` matches documents where the field is missing.
        Expr::Exists(exists) => {
            let exists = *exists;
            Box::new(move |field_value| field_value.is_some() == exists)
        }
        // Implements the `$elemMatch` filter as:
        // - Matches documents where at least one element in an array matches all the specified filter conditions.
        // - If the array element is a document, all document-level filters must match.
        // - If the array element is a scalar, all value-level filters must match.
        Expr::ElemMatch(filters) => {
            let is_doc_filter = filters.iter().any(is_document_elem_match_filter);

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
                let elem_filters: Vec<_> = filters
                    .iter()
                    .map(|filter| {
                        to_value_filter_with_array_traversal(filter.clone(), parameters, false)
                    })
                    .collect();
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
                Expr::Literal(val) => val.clone(),
                _ => unreachable!("$type value must be a placeholder or literal"),
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
                    _ => unreachable!("$size must be an integer"),
                },
                Expr::Literal(BsonValue(Bson::Int32(i))) => *i as usize,
                Expr::Literal(BsonValue(Bson::Int64(i))) => *i as usize,
                _ => unreachable!("$size value must be a placeholder or integer literal"),
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
                    _ => unreachable!("$all value must be an array placeholder"),
                },
                Expr::Literal(BsonValue(Bson::Array(arr))) => arr.clone(),
                _ => unreachable!("$all value must be a placeholder or array literal"),
            };

            Box::new(move |field_value| {
                !comparison_array.is_empty()
                    && comparison_array.iter().all(|value| {
                        compare_value(ComparisonOperator::Eq, BsonValueRef(value), field_value)
                    })
            })
        }
        Expr::Not(child) => {
            let inner =
                to_value_filter_with_array_traversal(child.clone(), parameters, expand_arrays);
            Box::new(move |field_value| !inner(field_value))
        }
        Expr::And(children) => {
            let filters: Vec<_> = children
                .iter()
                .map(|child| {
                    to_value_filter_with_array_traversal(child.clone(), parameters, expand_arrays)
                })
                .collect();
            Box::new(move |field_value| filters.iter().all(|f| f(field_value)))
        }
        Expr::Or(children) => {
            let filters: Vec<_> = children
                .iter()
                .map(|child| {
                    to_value_filter_with_array_traversal(child.clone(), parameters, expand_arrays)
                })
                .collect();
            Box::new(move |field_value| filters.iter().any(|f| f(field_value)))
        }
        Expr::Nor(children) => {
            let filters: Vec<_> = children
                .iter()
                .map(|child| {
                    to_value_filter_with_array_traversal(child.clone(), parameters, expand_arrays)
                })
                .collect();
            Box::new(move |field_value| !filters.iter().any(|f| f(field_value)))
        }
        Expr::AlwaysTrue => Box::new(|_| true),
        Expr::AlwaysFalse => Box::new(|_| false),
        _ => unreachable!("Unsupported value filter: {:?}", filter),
    }
}

fn is_document_elem_match_filter(expr: &Arc<Expr>) -> bool {
    match expr.as_ref() {
        Expr::FieldFilters { field, .. } => {
            matches!(field.as_ref(), Expr::Field(path) if !path.is_empty())
        }
        Expr::And(children) | Expr::Or(children) | Expr::Nor(children) => {
            children.iter().any(is_document_elem_match_filter)
        }
        Expr::Not(child) => is_document_elem_match_filter(child),
        _ => false,
    }
}

fn contains(interval: &Interval<BsonValue>, item: BsonValueRef) -> bool {
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

fn resolve_bound(bound: Bound<&Arc<Expr>>, parameters: &Parameters) -> Bound<BsonValue> {
    let get_value = |expr: &Arc<Expr>| match expr.as_ref() {
        Expr::Placeholder(idx) => parameters.get(*idx).clone(),
        Expr::Literal(val) => val.clone(),
        _ => unreachable!("Interval bound must be a placeholder or literal"),
    };
    match bound {
        Bound::Included(expr) => Bound::Included(get_value(expr)),
        Bound::Excluded(expr) => Bound::Excluded(get_value(expr)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn elem_match_array<F>(field_value: Option<BsonValueRef>, matcher: F) -> bool
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
pub fn to_filter(
    expr: Arc<Expr>,
    parameters: &Parameters,
) -> Box<dyn Fn(&Document) -> bool + Send + Sync> {
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
                _ => unreachable!("Expected a field expression"),
            };
            let filters: Vec<_> = filters
                .iter()
                .map(|filter| to_path_filter(path.clone(), filter.clone(), parameters))
                .collect();
            Box::new(move |doc: &Document| filters.iter().all(|filter| filter(doc)))
        }
        Expr::AlwaysTrue => Box::new(|_| true),
        Expr::AlwaysFalse => Box::new(|_| false),
        _ => unreachable!("Unsupported top-level filter: {:?}", expr),
    }
}

/// Compiles a predicate over all values reached by a filter path. Negation must
/// surround the traversal: one nonmatching branch cannot negate a matching one.
fn to_path_filter(
    path: Vec<PathComponent>,
    expr: Arc<Expr>,
    parameters: &Parameters,
) -> Box<dyn Fn(&Document) -> bool + Send + Sync> {
    if let Expr::All(value) = expr.as_ref() {
        let values = match value.as_ref() {
            Expr::Placeholder(index) => &parameters.get(*index).0,
            Expr::Literal(value) => &value.0,
            _ => unreachable!("$all requires a literal or placeholder"),
        };
        let Bson::Array(values) = values else {
            unreachable!("$all requires an array");
        };
        let filters: Vec<_> = values
            .iter()
            .map(|value| {
                to_path_filter(
                    path.clone(),
                    Arc::new(Expr::Comparison {
                        operator: ComparisonOperator::Eq,
                        value: Arc::new(Expr::Literal(BsonValue(value.clone()))),
                    }),
                    parameters,
                )
            })
            .collect();
        return Box::new(move |doc| {
            !filters.is_empty() && filters.iter().all(|filter| filter(doc))
        });
    }
    let positive = match expr.as_ref() {
        Expr::Not(child) => Some(child.clone()),
        Expr::Comparison {
            operator: ComparisonOperator::Ne,
            value,
        } => Some(Arc::new(Expr::Comparison {
            operator: ComparisonOperator::Eq,
            value: value.clone(),
        })),
        Expr::Comparison {
            operator: ComparisonOperator::Nin,
            value,
        } => Some(Arc::new(Expr::Comparison {
            operator: ComparisonOperator::In,
            value: value.clone(),
        })),
        Expr::Exists(false) => Some(Arc::new(Expr::Exists(true))),
        Expr::Type {
            bson_type,
            negated: true,
        } => Some(Arc::new(Expr::Type {
            bson_type: bson_type.clone(),
            negated: false,
        })),
        Expr::Size {
            size,
            negated: true,
        } => Some(Arc::new(Expr::Size {
            size: size.clone(),
            negated: false,
        })),
        _ => None,
    };
    if let Some(positive) = positive {
        let filter = to_path_filter(path, positive, parameters);
        return Box::new(move |doc| !filter(doc));
    }
    if let Expr::And(children) | Expr::Or(children) | Expr::Nor(children) = expr.as_ref() {
        let filters: Vec<_> = children
            .iter()
            .map(|child| to_path_filter(path.clone(), child.clone(), parameters))
            .collect();
        return match expr.as_ref() {
            Expr::And(_) => Box::new(move |doc| filters.iter().all(|filter| filter(doc))),
            Expr::Or(_) => Box::new(move |doc| filters.iter().any(|filter| filter(doc))),
            _ => Box::new(move |doc| !filters.iter().any(|filter| filter(doc))),
        };
    }
    let filter = to_value_filter(expr, parameters);
    Box::new(move |doc| {
        let Some((PathComponent::FieldName(name), rest)) = path.split_first() else {
            return filter(None);
        };
        any_path_value(doc.get(name), rest, filter.as_ref())
    })
}

/// Filter-only traversal; strict projection and update lookup remains unchanged.
fn any_path_value(
    value: Option<&Bson>,
    path: &[PathComponent],
    predicate: &dyn Fn(Option<BsonValueRef>) -> bool,
) -> bool {
    let Some((component, rest)) = path.split_first() else {
        return predicate(value.map(BsonValueRef));
    };
    match (value, component) {
        (Some(Bson::Document(doc)), PathComponent::FieldName(name)) => {
            any_path_value(doc.get(name), rest, predicate)
        }
        (Some(Bson::Array(array)), PathComponent::ArrayElement(index)) => {
            any_path_value(array.get(*index), rest, predicate)
        }
        (Some(Bson::Array(array)), PathComponent::FieldName(_)) => {
            if array.is_empty() {
                return predicate(None);
            }
            array
                .iter()
                .any(|element| any_path_value(Some(element), path, predicate))
        }
        _ => predicate(None),
    }
}

fn to_filters(
    children: &Vec<Arc<Expr>>,
    parameters: &Parameters,
) -> Vec<Box<dyn Fn(&Document) -> bool + Send + Sync>> {
    children
        .iter()
        .map(|c| to_filter(c.clone(), parameters))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Bson, Decimal128, doc};
    use std::str::FromStr;

    #[test]
    fn test_to_type_filter_with_string_alias() {
        assert!(matches_type_spec(
            &Bson::String("hello".to_string()),
            &Bson::from("string")
        ));
        assert!(matches_type_spec(&Bson::Int32(1), &Bson::from("int")));
        assert!(matches_type_spec(&Bson::Int64(1), &Bson::from("long")));
        assert!(matches_type_spec(&Bson::Double(1.0), &Bson::from("double")));
        assert!(matches_type_spec(
            &Bson::Array(vec![]),
            &Bson::from("array")
        ));
        assert!(matches_type_spec(
            &Bson::Document(doc! {}),
            &Bson::from("object")
        ));
        assert!(matches_type_spec(&Bson::Boolean(true), &Bson::from("bool")));
        assert!(matches_type_spec(&Bson::Null, &Bson::from("null")));
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
        assert!(matches_type_spec(
            &Bson::Decimal128(Decimal128::from_str("1").unwrap()),
            &number_spec
        ));
        assert!(!matches_type_spec(
            &Bson::String("hello".to_string()),
            &number_spec
        ));
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
        assert!(!matches_type_spec(&Bson::from(&i32), &Bson::from(12345)));
        // Invalid spec type
        assert!(!matches_type_spec(&Bson::from(&i32), &Bson::from(true)));
    }
}
