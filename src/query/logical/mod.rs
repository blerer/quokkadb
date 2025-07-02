use crate::query::tree_node::TreeNode;
use bson::Bson;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

pub(crate) mod expr_fn;
mod executor;
pub mod logical_plan;
pub mod physical_plan;
pub(crate) mod parser;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Expr {
    /// Field reference
    Field(Vec<PathComponent>),
    /// Wildcard field reference (e.g., "field.*")
    WildcardField(Vec<PathComponent>),
    ///  Positional projection in queries (e.g. "array.$")
    PositionalField(Vec<PathComponent>),
    /// Literal values
    Literal(BsonValue),
    /// Multiple filters on the same field (e.g., `{ "price": { "$ne": 1.99, "$exists": true } }`)
    FieldFilters {
        field: Rc<Expr>,
        filters: Vec<Rc<Expr>>, // List of filters on the same field
    },
    /// A single comparison (e.g., `$gt: 5`, `$eq: "Alice"`)
    Comparison {
        operator: ComparisonOperator, // `$eq`, `$ne`, `$gt`, `$lt`, etc.
        value: Rc<Expr>,              // The literal value
    },
    And(Vec<Rc<Expr>>),
    Or(Vec<Rc<Expr>>),
    Not(Rc<Expr>),
    Nor(Vec<Rc<Expr>>),
    /// Field existence
    Exists(bool),
    /// Type check
    Type {
        bson_type: BsonType,
        negated: bool, // If the expression has been negated (e.g. $not: { $type : "string" })
    },
    // Array-specific operations
    Size {
        size: usize,
        negated: bool,
    },
    All(Vec<BsonValue>),
    ElemMatch(Vec<Rc<Expr>>),
    /// Represents an expression that is always true (e.g. $and: [])
    AlwaysTrue,
    /// Represents an expression that is always false (e.g. $or: [])
    AlwaysFalse,
}

impl TreeNode for Expr {
    type Child = Expr;

    /// Return references to the children of the current node
    fn children(&self) -> Vec<Rc<Self::Child>> {
        match self {
            Expr::FieldFilters {
                field,
                filters: predicates,
                ..
            } => {
                let mut children = Vec::with_capacity(predicates.len() + 1);
                children.push(field.clone());
                children.extend(predicates.into_iter().cloned());
                children
            }
            Expr::Comparison { value, .. } => vec![value.clone()],
            Expr::And(elements) => elements.iter().cloned().collect(),
            Expr::Or(elements) => elements.iter().cloned().collect(),
            Expr::Not(expr) => vec![expr.clone()],
            Expr::Nor(elements) => elements.iter().cloned().collect(),
            Expr::ElemMatch(predicates) => predicates.iter().cloned().collect(),
            _ => vec![], // Leaf nodes have no children
        }
    }

    fn with_new_children(self: Rc<Self>, children: Vec<Rc<Self::Child>>) -> Rc<Self> {
        match self.as_ref() {
            Expr::FieldFilters { .. } => {
                let mut iter = children.into_iter();
                let field = iter.next().unwrap();
                let predicates = iter.collect();
                Rc::new(Expr::FieldFilters {
                    field,
                    filters: predicates,
                })
            }
            Expr::Comparison { operator, .. } => Rc::new(Expr::Comparison {
                operator: operator.clone(),
                value: Self::get_first(children),
            }),
            Expr::And(_) => Rc::new(Expr::And(children)),
            Expr::Or(_) => Rc::new(Expr::Or(children)),
            Expr::Not(_) => Rc::new(Expr::Not(Self::get_first(children))),
            Expr::Nor(_) => Rc::new(Expr::Nor(children)),
            Expr::ElemMatch { .. } => Rc::new(Expr::ElemMatch(children)),
            _ => self, // No changes needed for leaf nodes
        }
    }
}

impl Expr {
    fn get_first(children: Vec<Rc<Expr>>) -> Rc<Expr> {
        children.into_iter().next().unwrap()
    }

    pub fn negate(&self) -> Rc<Expr> {
        match self {
            Expr::Not(expr) => expr.clone(),
            Expr::Nor(exprs) => Rc::new(Expr::Or(exprs.iter().cloned().collect())),
            Expr::And(exprs) => Rc::new(Expr::Or(exprs.iter().map(|e| e.negate()).collect())),
            Expr::Or(exprs) => Rc::new(Expr::And(exprs.iter().map(|e| e.negate()).collect())),
            Expr::FieldFilters {
                field,
                filters: predicates,
            } => {
                if predicates.len() == 1 {
                    return Rc::new(Expr::FieldFilters {
                        field: field.clone(),
                        filters: predicates.iter().map(|e| e.negate()).collect(),
                    });
                }

                // If there are multiple expression we need to apply De Morgan's Laws
                // not(x and y) simplifies to not(x) or not(y)
                let field_predicates = predicates
                    .iter()
                    .map(|e| {
                        Rc::new(Expr::FieldFilters {
                            field: field.clone(),
                            filters: vec![e.negate()],
                        })
                    })
                    .collect();

                Rc::new(Expr::FieldFilters {
                    field: field.clone(),
                    filters: vec![Rc::new(Expr::Or(field_predicates))],
                })
            }
            Expr::Comparison { operator, value } => Rc::new(Expr::Comparison {
                operator: operator.negate(),
                value: value.clone(),
            }),
            Expr::Exists(bool) => Rc::new(Expr::Exists(!*bool)),
            Expr::All(values) => {
                let value = BsonValue(Bson::Array(values.iter().map(|v| v.to_bson()).collect()));
                Rc::new(Expr::Comparison {
                    operator: ComparisonOperator::Nin,
                    value: Rc::new(Expr::Literal(value)),
                })
            }
            Expr::Type {
                bson_type,
                negated: not,
            } => Rc::new(Expr::Type {
                bson_type: *bson_type,
                negated: !*not,
            }),
            Expr::Size { size, negated } => Rc::new(Expr::Size {
                size: *size,
                negated: !*negated,
            }),
            Expr::AlwaysTrue => Rc::new(Expr::AlwaysFalse),
            Expr::AlwaysFalse => Rc::new(Expr::AlwaysTrue),
            _ => Rc::new(self.clone()),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub enum ComparisonOperator {
    Eq,  // `$eq`
    Ne,  // `$ne`
    Gt,  // `$gt`
    Gte, // `$gte`
    Lt,  // `$lt`
    Lte, // `$lte`
    In,  // `$in`
    Nin, // `$nin`
}

impl ComparisonOperator {
    fn negate(&self) -> ComparisonOperator {
        match self {
            ComparisonOperator::Eq => ComparisonOperator::Ne,
            ComparisonOperator::Ne => ComparisonOperator::Eq,
            ComparisonOperator::Gt => ComparisonOperator::Lte,
            ComparisonOperator::Gte => ComparisonOperator::Lt,
            ComparisonOperator::Lt => ComparisonOperator::Gte,
            ComparisonOperator::Lte => ComparisonOperator::Gt,
            ComparisonOperator::In => ComparisonOperator::Nin,
            ComparisonOperator::Nin => ComparisonOperator::In,
        }
    }

    /// Converts a `ComparisonOperator` to a MongoDB-style string (`$eq`, `$gt`, etc.)
    fn to_string(&self) -> String {
        match self {
            ComparisonOperator::Eq => "$eq".to_string(),
            ComparisonOperator::Ne => "$ne".to_string(),
            ComparisonOperator::Gt => "$gt".to_string(),
            ComparisonOperator::Gte => "$gte".to_string(),
            ComparisonOperator::Lt => "$lt".to_string(),
            ComparisonOperator::Lte => "$lte".to_string(),
            ComparisonOperator::In => "$in".to_string(),
            ComparisonOperator::Nin => "$nin".to_string(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub enum BsonType {
    Double,
    String,
    Array,
    Document,
    Boolean,
    Null,
    RegularExpression,
    JavaScriptCode,
    JavaScriptCodeWithScope,
    Int32,
    Int64,
    Timestamp,
    Binary,
    ObjectId,
    DateTime,
    Decimal128,
    MaxKey,
    MinKey,
}

/// Represents a component in a field path
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum PathComponent {
    FieldName(String),   // A named field (e.g., "field" in "document.field")
    ArrayElement(usize), // An array index (e.g., "0" in "array.0")
}

impl From<&str> for PathComponent {
    fn from(value: &str) -> Self {
        PathComponent::FieldName(value.to_string())
    }
}

impl From<usize> for PathComponent {
    fn from(index: usize) -> Self {
        PathComponent::ArrayElement(index)
    }
}

#[derive(Debug, Clone)]
pub struct BsonValue(pub Bson);

impl BsonValue {
    pub fn to_bson(&self) -> Bson {
        self.0.clone()
    }
}

impl PartialEq for BsonValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            // Handle NaN correctly (MongoDB: NaN == NaN)
            (Bson::Double(x), Bson::Double(y)) if x.is_nan() && y.is_nan() => true,
            // Exact match for same BSON type
            (Bson::Int32(x), Bson::Int32(y)) => x == y,
            (Bson::Int64(x), Bson::Int64(y)) => x == y,
            (Bson::Double(x), Bson::Double(y)) => (x - y).abs() < f64::EPSILON,

            // Normalize and compare mixed numeric types
            (Bson::Int32(x), Bson::Int64(y)) => *x as i64 == *y,
            (Bson::Int32(x), Bson::Double(y)) => (*x as f64 - *y).abs() < f64::EPSILON,
            (Bson::Int64(x), Bson::Double(y)) => (*x as f64 - *y).abs() < f64::EPSILON,
            (Bson::Int64(x), Bson::Int32(y)) => *x == *y as i64,
            (Bson::Double(x), Bson::Int32(y)) => (*x - *y as f64).abs() < f64::EPSILON,
            (Bson::Double(x), Bson::Int64(y)) => (*x - *y as f64).abs() < f64::EPSILON,

            // Compare objects/maps in a canonical order
            (Bson::Document(a), Bson::Document(b)) => {
                let a_sorted: BTreeMap<_, _> = a.iter().collect();
                let b_sorted: BTreeMap<_, _> = b.iter().collect();
                a_sorted == b_sorted
            }

            // Compare arrays element-wise
            (Bson::Array(a), Bson::Array(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b)
                        .all(|(x, y)| BsonValue(x.clone()) == BsonValue(y.clone()))
            }

            // Compare regex patterns and options
            (Bson::RegularExpression(a), Bson::RegularExpression(b)) => {
                a.pattern == b.pattern && a.options == b.options
            }

            // Default strict equality for other types
            _ => self.0 == other.0, // Default case
        }
    }
}

impl Eq for BsonValue {}

impl Hash for BsonValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            // Normalize NaN to a fixed value
            Bson::Int32(x) => x.hash(state),
            Bson::Int64(x) => x.hash(state),
            Bson::Double(x) => {
                if x.is_nan() {
                    0x7FF8_0000_0000_0000u64.hash(state)
                } else {
                    x.to_bits().hash(state)
                }
            } // Normalize floating point hashing
            Bson::String(s) => s.hash(state),
            Bson::Boolean(b) => b.hash(state),

            // Hash BSON arrays
            Bson::Array(arr) => {
                for elem in arr {
                    BsonValue(elem.clone()).hash(state);
                }
            }

            // Hash BSON documents (sorted order)
            Bson::Document(doc) => {
                let sorted: BTreeMap<_, _> = doc.iter().collect();
                for (key, value) in sorted {
                    key.hash(state);
                    BsonValue(value.clone()).hash(state);
                }
            }

            Bson::RegularExpression(regex) => {
                regex.pattern.hash(state);
                regex.options.hash(state);
            }

            _ => (),
        }
    }
}

impl From<i32> for BsonValue {
    fn from(value: i32) -> Self {
        BsonValue(Bson::Int32(value))
    }
}

impl From<i64> for BsonValue {
    fn from(value: i64) -> Self {
        BsonValue(Bson::Int64(value))
    }
}

impl From<&str> for BsonValue {
    fn from(value: &str) -> Self {
        BsonValue(Bson::String(value.to_string()))
    }
}

impl From<bool> for BsonValue {
    fn from(value: bool) -> Self {
        BsonValue(bson::Bson::Boolean(value))
    }
}

impl<T> From<Vec<T>> for BsonValue
where
    T: Into<Bson>, // Ensure each element can be converted into `Bson`
{
    fn from(values: Vec<T>) -> Self {
        BsonValue(Bson::Array(values.into_iter().map(|v| v.into()).collect()))
    }
}

#[macro_export]
macro_rules! bson_value {
    ( $($tokens:tt)* ) => {
        BsonValue(bson::bson!($($tokens)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Bson, Regex};

    #[test]
    fn test_basic_numeric_equality() {
        assert_eq!(bson_value!(5), bson_value!(5));
        assert_eq!(bson_value!(5_i32), bson_value!(5_i32));
        assert_eq!(bson_value!(5_i64), bson_value!(5_i64));
        assert_eq!(bson_value!(5_i32), bson_value!(5_i64));
        assert_eq!(bson_value!(5.0_f64), bson_value!(5.0_f64));
        assert_eq!(bson_value!(5_i32), bson_value!(5.0_f64));
        assert_eq!(bson_value!(5_i64), bson_value!(5.0_f64));

        assert_ne!(bson_value!(5), bson_value!(6));
        assert_ne!(bson_value!(5.0_f64), bson_value!(6.0_f64));
    }

    #[test]
    fn test_floating_point_comparisons() {
        assert_eq!(bson_value!(5.0000001_f64), bson_value!(5.0000001_f64));
        assert_eq!(bson_value!(5.0_f64), bson_value!(5.0_f64));

        // Ensure different numbers are NOT equal
        assert_ne!(bson_value!(5.1_f64), bson_value!(5.0_f64));
        assert_ne!(bson_value!(5.00001_f64), bson_value!(5.0_f64));
    }

    #[test]
    fn test_nan_comparisons() {
        assert_eq!(
            bson_value!(f64::NAN),
            bson_value!(f64::NAN),
            "NaN should be equal to NaN"
        );
        assert_ne!(bson_value!(5.1_f64), bson_value!(f64::NAN));
    }

    #[test]
    fn test_boolean_equality() {
        assert_eq!(bson_value!(true), bson_value!(true));
        assert_eq!(bson_value!(false), bson_value!(false));
        assert_ne!(bson_value!(true), bson_value!(false));
    }

    #[test]
    fn test_string_equality() {
        assert_eq!(bson_value!("hello"), bson_value!("hello"));
        assert_ne!(bson_value!("hello"), bson_value!("world"));
    }

    #[test]
    fn test_array_equality() {
        let array1 = bson_value!([1, 2, 3]);
        let array2 = bson_value!([1, 2, 3]);
        let array3 = bson_value!([3, 2, 1]);

        assert_eq!(&array1, &array2);
        assert_ne!(&array1, &array3);
    }

    #[test]
    fn test_document_equality() {
        let doc1 = bson_value!({ "a": 1, "b": 2 });
        let doc2 = bson_value!({ "b": 2, "a": 1 }); // Different order, should be equal
        let doc3 = bson_value!({ "a": 1, "b": 3 });

        assert_eq!(
            &doc1, &doc2,
            "Document equality should be order-independent"
        );
        assert_ne!(&doc1, &doc3);
    }

    #[test]
    fn test_nested_document_equality() {
        let doc1 = bson_value!({ "a": { "b": 1, "c": 2 }, "d": 3 });
        let doc2 = bson_value!({ "d": 3, "a": { "c": 2, "b": 1 } });

        assert_eq!(&doc1, &doc2);
    }

    #[test]
    fn test_mixed_document_array_equality() {
        let complex1 = bson_value!({
            "users": [
                { "name": "Alice", "age": 25 },
                { "name": "Bob", "age": 30 }
            ]
        });

        let complex2 = bson_value!({
            "users": [
                { "name": "Alice", "age": 25 },
                { "name": "Bob", "age": 30 }
            ]
        });

        let complex3 = bson_value!({
            "users": [
                { "name": "Bob", "age": 30 },
                { "name": "Alice", "age": 25 }
            ]
        });

        assert_eq!(&complex1, &complex2);
        assert_ne!(&complex1, &complex3, "Array order should matter");
    }

    #[test]
    fn test_regex_equality() {
        let regex1 = BsonValue(Bson::RegularExpression(Regex {
            pattern: "abc.*".to_string(),
            options: "i".to_string(),
        }));

        let regex2 = BsonValue(Bson::RegularExpression(Regex {
            pattern: "abc.*".to_string(),
            options: "i".to_string(),
        }));

        let regex3 = BsonValue(Bson::RegularExpression(Regex {
            pattern: "abc.*".to_string(),
            options: "".to_string(),
        }));

        assert_eq!(&regex1, &regex2);
        assert_ne!(&regex1, &regex3, "Regex options should matter");
    }

    #[test]
    fn test_null_equality() {
        assert_eq!(BsonValue(Bson::Null), BsonValue(Bson::Null));
    }

    #[test]
    fn test_different_types_not_equal() {
        assert_ne!(bson_value!(5), bson_value!("5"));
        assert_ne!(bson_value!(true), bson_value!(1));
        assert_ne!(bson_value!(Bson::Null), bson_value!(false));
    }
}
