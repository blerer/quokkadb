use crate::query::tree_node::TreeNode;
use bson::Bson;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::util::bson_utils::BsonKey;

pub(crate) mod expr_fn;
pub(crate) mod executor;
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
    /// Placeholder for parameters. After normalization, literals would be replaced by placeholders
    /// and the parameters captured. Allowing optimizations output to be cached and reused.
    Placeholder(u32),
    /// Multiple filters on the same field (e.g., `{ "price": { "$ne": 1.99, "$exists": true } }`)
    FieldFilters {
        field: Arc<Expr>,
        filters: Vec<Arc<Expr>>, // List of filters on the same field
    },
    /// A single comparison (e.g., `$gt: 5`, `$eq: "Alice"`)
    Comparison {
        operator: ComparisonOperator, // `$eq`, `$ne`, `$gt`, `$lt`, etc.
        value: Arc<Expr>,              // The literal value
    },
    And(Vec<Arc<Expr>>),
    Or(Vec<Arc<Expr>>),
    Not(Arc<Expr>),
    Nor(Vec<Arc<Expr>>),
    /// Field existence
    Exists(bool),
    /// Type check
    Type {
        bson_type: Arc<Expr>,
        negated: bool, // If the expression has been negated (e.g. $not: { $type : "string" })
    },
    // Array-specific operations
    Size {
        size: Arc<Expr>,
        negated: bool,
    },
    All(Arc<Expr>),
    ElemMatch(Vec<Arc<Expr>>),
    /// Represents an expression that is always true (e.g. $and: [])
    AlwaysTrue,
    /// Represents an expression that is always false (e.g. $or: [])
    AlwaysFalse,
    // Projection operators
    ProjectionSlice {
        field: Arc<Expr>,
        skip: i32,
        limit: Option<i32>,
    },
    ProjectionElemMatch {
        field: Arc<Expr>,
        expr: Arc<Expr>,
    },
}

impl TreeNode for Expr {
    type Child = Expr;

    /// Return references to the children of the current node
    fn children(&self) -> Vec<Arc<Self::Child>> {
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
            Expr::ProjectionSlice { field, .. } => vec![field.clone()],
            Expr::ProjectionElemMatch { field, expr, .. } => vec![field.clone(), expr.clone()],
            _ => vec![], // Leaf nodes have no children
        }
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self::Child>>) -> Arc<Self> {
        match self.as_ref() {
            Expr::FieldFilters { .. } => {
                let mut iter = children.into_iter();
                let field = iter.next().unwrap();
                let predicates = iter.collect();
                Arc::new(Expr::FieldFilters {
                    field,
                    filters: predicates,
                })
            }
            Expr::Comparison { operator, .. } => Arc::new(Expr::Comparison {
                operator: operator.clone(),
                value: Self::get_first(children),
            }),
            Expr::And(_) => Arc::new(Expr::And(children)),
            Expr::Or(_) => Arc::new(Expr::Or(children)),
            Expr::Not(_) => Arc::new(Expr::Not(Self::get_first(children))),
            Expr::Nor(_) => Arc::new(Expr::Nor(children)),
            Expr::ElemMatch { .. } => Arc::new(Expr::ElemMatch(children)),
            Expr::ProjectionSlice { skip, limit, .. } => Arc::new(Expr::ProjectionSlice {
                field: Self::get_first(children),
                skip: *skip,
                limit: *limit,
            }),
            Expr::ProjectionElemMatch { .. } => {
                let mut iter = children.into_iter();
                let field = iter.next().unwrap();
                let expr = iter.next().unwrap();
                Arc::new(Expr::ProjectionElemMatch { field, expr })
            }
            _ => self, // No changes needed for leaf nodes
        }
    }
}

impl Expr {
    fn get_first(children: Vec<Arc<Expr>>) -> Arc<Expr> {
        children.into_iter().next().unwrap()
    }

    pub fn negate(&self) -> Arc<Expr> {
        match self {
            Expr::Not(expr) => expr.clone(),
            Expr::Nor(exprs) => Arc::new(Expr::Or(exprs.iter().cloned().collect())),
            Expr::And(exprs) => Arc::new(Expr::Or(exprs.iter().map(|e| e.negate()).collect())),
            Expr::Or(exprs) => Arc::new(Expr::And(exprs.iter().map(|e| e.negate()).collect())),
            Expr::FieldFilters {
                field,
                filters: predicates,
            } => {
                if predicates.len() == 1 {
                    return Arc::new(Expr::FieldFilters {
                        field: field.clone(),
                        filters: predicates.iter().map(|e| e.negate()).collect(),
                    });
                }

                // If there are multiple expression we need to apply De Morgan's Laws
                // not(x and y) simplifies to not(x) or not(y)
                let field_predicates = predicates
                    .iter()
                    .map(|e| {
                        Arc::new(Expr::FieldFilters {
                            field: field.clone(),
                            filters: vec![e.negate()],
                        })
                    })
                    .collect();

                Arc::new(Expr::FieldFilters {
                    field: field.clone(),
                    filters: vec![Arc::new(Expr::Or(field_predicates))],
                })
            }
            Expr::Comparison { operator, value } => Arc::new(Expr::Comparison {
                operator: operator.negate(),
                value: value.clone(),
            }),
            Expr::Exists(bool) => Arc::new(Expr::Exists(!*bool)),
            Expr::All(values) => {
                Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Nin,
                    value: values.clone(),
                })
            }
            Expr::Type {
                bson_type,
                negated: not,
            } => Arc::new(Expr::Type {
                bson_type: bson_type.clone(),
                negated: !*not,
            }),
            Expr::Size { size, negated } => Arc::new(Expr::Size {
                size: size.clone(),
                negated: !*negated,
            }),
            Expr::AlwaysTrue => Arc::new(Expr::AlwaysFalse),
            Expr::AlwaysFalse => Arc::new(Expr::AlwaysTrue),
            _ => Arc::new(self.clone()),
        }
    }
}

impl Serializable for Expr {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            1 => {
                let expr = Arc::new(Self::read_from(reader)?);
                Ok(Expr::Not(expr))
            }
            2 => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::Nor(exprs))
            }
            3 => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::And(exprs))
            }
            4 => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::Or(exprs))
            }
            5 => {
                let field = Arc::new(Self::read_from(reader)?);
                let filters = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::FieldFilters { field, filters })
            }
            6 => {
                let operator = ComparisonOperator::read_from(reader)?;
                let value = Arc::new(Self::read_from(reader)?);
                Ok(Expr::Comparison { operator, value })
            }
            7 => {
                let exists = reader.read_u8()? == 1;
                Ok(Expr::Exists(exists))
            }
            8 => {
                let values = Arc::new(Self::read_from(reader)?);
                Ok(Expr::All(values))
            }
            9 => {
                let bson_type = Arc::new(Self::read_from(reader)?);
                let negated = reader.read_u8()? == 1;
                Ok(Expr::Type {
                    bson_type,
                    negated,
                })
            }
            10 => {
                let size = Arc::new(Self::read_from(reader)?);
                let negated = reader.read_u8()? == 1;
                Ok(Expr::Size { size, negated })
            }
            11 => Ok(Expr::AlwaysTrue),
            12 => Ok(Expr::AlwaysFalse),
            13 => {
                let path = Vec::<PathComponent>::read_from(reader)?;
                Ok(Expr::Field(path))
            }
            14 => {
                let path = Vec::<PathComponent>::read_from(reader)?;
                Ok(Expr::WildcardField(path))
            }
            15 => {
                let path = Vec::<PathComponent>::read_from(reader)?;
                Ok(Expr::PositionalField(path))
            }
            16 => {
                let idx = reader.read_varint_u32()?;
                Ok(Expr::Placeholder(idx))
            }
            17 => {
                let predicates = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::ElemMatch(predicates))
            }
            18 => {
                let field = Arc::new(Self::read_from(reader)?);
                let skip = i32::read_from(reader)?;
                let limit = Option::<i32>::read_from(reader)?;
                Ok(Expr::ProjectionSlice { field, skip, limit })
            }
            20 => {
                let field = Arc::new(Self::read_from(reader)?);
                let expr = Arc::new(Self::read_from(reader)?);
                Ok(Expr::ProjectionElemMatch { field, expr })
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unknown Expr tag: {}", tag),
            )),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            Expr::Not(expr) => {
                writer.write_u8(1);
                expr.write_to(writer);
            },
            Expr::Nor(exprs) => {
                writer.write_u8(2);
                exprs.write_to(writer);
            },
            Expr::And(exprs) => {
                writer.write_u8(3);
                exprs.write_to(writer);
            },
            Expr::Or(exprs) => {
                writer.write_u8(4);
                exprs.write_to(writer);
            },
            Expr::FieldFilters {
                field,
                filters,
            } => {
                writer.write_u8(5);
                field.write_to(writer);
                filters.write_to(writer);
            }
            Expr::Comparison { operator, value } => {
                writer.write_u8(6);
                operator.write_to(writer);
                value.write_to(writer);
            },
            Expr::Exists(bool) => {
                writer.write_u8(7);
                writer.write_u8(if *bool { 1 } else { 0 });
            },
            Expr::All(values) => {
                writer.write_u8(8);
                values.write_to(writer);
            }
            Expr::Type {
                bson_type,
                negated,
            } => {
                writer.write_u8(9);
                bson_type.write_to(writer);
                writer.write_u8(if *negated { 1 } else { 0 });
            },
            Expr::Size { size, negated } => {
                writer.write_u8(10);
                size.write_to(writer);
                writer.write_u8(if *negated { 1 } else { 0 });
            },
            Expr::AlwaysTrue => {
                writer.write_u8(11);
            },
            Expr::AlwaysFalse => {
                writer.write_u8(12);
            },
            Expr::Field(path) => {
                writer.write_u8(13);
                path.write_to(writer);
            }
            Expr::WildcardField(path) => {
                writer.write_u8(14);
                path.write_to(writer);
            }
            Expr::PositionalField(path) => {
                writer.write_u8(15);
                path.write_to(writer);
            }
            Expr::Placeholder(idx) => {
                writer.write_u8(16);
                writer.write_varint_u32(*idx); // Write the placeholder index for extra safety
            }
            Expr::ElemMatch(predicates) => {
                writer.write_u8(17);
                predicates.write_to(writer);
            }
            Expr::ProjectionSlice { field, skip, limit } => {
                writer.write_u8(18);
                field.write_to(writer);
                skip.write_to(writer);
                limit.write_to(writer);
            }
            Expr::ProjectionElemMatch { field, expr } => {
                writer.write_u8(20);
                field.write_to(writer);
                expr.write_to(writer);
            }
            Expr::Literal(_) => {
                panic!("LogicalPlans should never be serialized before parametrization.");
            }
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

impl Serializable for ComparisonOperator {

    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            0 => Ok(ComparisonOperator::Eq),
            1 => Ok(ComparisonOperator::Ne),
            2 => Ok(ComparisonOperator::Gt),
            3 => Ok(ComparisonOperator::Gte),
            4 => Ok(ComparisonOperator::Lt),
            5 => Ok(ComparisonOperator::Lte),
            6 => Ok(ComparisonOperator::In),
            7 => Ok(ComparisonOperator::Nin),
            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown comparison operator byte",)),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            ComparisonOperator::Eq => 0,
            ComparisonOperator::Ne => 1,
            ComparisonOperator::Gt => 2,
            ComparisonOperator::Gte => 3,
            ComparisonOperator::Lt => 4,
            ComparisonOperator::Lte => 5,
            ComparisonOperator::In => 6,
            ComparisonOperator::Nin => 7,
        };
        writer.write_u8(byte);
    }
}

/// Represents a component in a field path
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum PathComponent {
    FieldName(String),   // A named field (e.g., "field" in "document.field")
    ArrayElement(usize), // An array index (e.g., "0" in "array.0")
}

impl Serializable for PathComponent {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            0 => {
                let name = reader.read_str()?;
                Ok(PathComponent::FieldName(name.to_string()))
            }
            1 => {
                let index = reader.read_varint_u32()? as usize;
                Ok(PathComponent::ArrayElement(index))
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown path component type",
            )),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            PathComponent::FieldName(name) => {
                writer.write_u8(0); // 0 for field name
                writer.write_str(name);
            }
            PathComponent::ArrayElement(index) => {
                writer.write_u8(1); // 1 for array element
                writer.write_varint_u32(*index as u32);
            }
        }
    }
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

impl BsonKey for BsonValue {

    fn try_into_key(&self) -> Result<Vec<u8>> {
        self.0.try_into_key()
    }
}

#[macro_export]
macro_rules! bson_value {
    ( $($tokens:tt)* ) => {
        BsonValue(bson::bson!($($tokens)*))
    };
}

pub struct Parameters {
    parameters: Vec<BsonValue>,
}

impl Parameters {
    pub fn new() -> Self {
        Self {
            parameters: Vec::new(),
        }
    }

    pub fn collect_parameter(&mut self, value: BsonValue) -> Arc<Expr> {
        let idx = self.parameters.len() as u32;
        self.parameters.push(value);
        Arc::new(Expr::Placeholder(idx))
    }

    pub fn get(&self, index: u32) -> Result<&BsonValue> {
        self.parameters.get(index as usize).ok_or(
            Error::new(
                ErrorKind::InvalidInput,
                format!("Parameter index {} out of bounds", index),
            ),
        )
    }

    pub fn len(&self) -> usize {
        self.parameters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::byte_writer::ByteWriter;
    use crate::io::serializable::check_serialization_round_trip;
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

    #[test]
    #[should_panic(expected = "LogicalPlans should never be serialized before parametrization.")]
    fn test_serialize_literal_panics() {
        let expr = Expr::Literal(bson_value!(10));
        let mut writer = ByteWriter::new();
        expr.write_to(&mut writer);
    }

    #[test]
    fn test_expr_serialization_round_trip() {
        // Simple expressions
        check_serialization_round_trip(Expr::AlwaysTrue);
        check_serialization_round_trip(Expr::AlwaysFalse);
        check_serialization_round_trip(Expr::Exists(true));
        check_serialization_round_trip(Expr::Placeholder(42));

        // Field expressions
        check_serialization_round_trip(Expr::Field(vec!["a".into(), "b".into()]));
        check_serialization_round_trip(Expr::WildcardField(vec!["a".into(), "b".into()]));
        check_serialization_round_trip(Expr::PositionalField(vec!["a".into(), 0.into()]));

        // Comparison
        let comparison = Expr::Comparison {
            operator: ComparisonOperator::Eq,
            value: Arc::new(Expr::Placeholder(0)),
        };
        check_serialization_round_trip(comparison);

        // Logical expressions
        let not_expr = Expr::Not(Arc::new(Expr::Exists(true)));
        check_serialization_round_trip(not_expr);

        let and_expr = Expr::And(vec![
            Arc::new(Expr::Exists(true)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(1)),
            }),
        ]);
        check_serialization_round_trip(and_expr);

        let or_expr = Expr::Or(vec![
            Arc::new(Expr::Exists(false)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(2)),
            }),
        ]);
        check_serialization_round_trip(or_expr);

        let nor_expr = Expr::Nor(vec![
            Arc::new(Expr::Exists(true)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(3)),
            }),
        ]);
        check_serialization_round_trip(nor_expr);

        // FieldFilters
        let field_filters = Expr::FieldFilters {
            field: Arc::new(Expr::Field(vec!["price".into()])),
            filters: vec![
                Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Gt,
                    value: Arc::new(Expr::Placeholder(4)),
                }),
                Arc::new(Expr::Exists(true)),
            ],
        };
        check_serialization_round_trip(field_filters);

        // Array expressions
        let all_expr = Expr::All(Arc::new(Expr::Placeholder(5)));
        check_serialization_round_trip(all_expr);

        let elem_match_expr = Expr::ElemMatch(vec![Arc::new(Expr::Comparison {
            operator: ComparisonOperator::Gte,
            value: Arc::new(Expr::Placeholder(6)),
        })]);
        check_serialization_round_trip(elem_match_expr);

        // Type and Size
        let type_expr = Expr::Type {
            bson_type: Arc::new(Expr::Placeholder(7)),
            negated: true,
        };
        check_serialization_round_trip(type_expr);

        let size_expr = Expr::Size {
            size: Arc::new(Expr::Placeholder(8)),
            negated: false,
        };
        check_serialization_round_trip(size_expr);

        // Projection
        let proj_slice = Expr::ProjectionSlice {
            field: Arc::new(Expr::Field(vec!["array".into()])),
            skip: 10,
            limit: Some(20),
        };
        check_serialization_round_trip(proj_slice);

        let proj_slice_no_limit = Expr::ProjectionSlice {
            field: Arc::new(Expr::Field(vec!["array".into()])),
            skip: 5,
            limit: None,
        };
        check_serialization_round_trip(proj_slice_no_limit);

        let proj_elem_match = Expr::ProjectionElemMatch {
            field: Arc::new(Expr::Field(vec!["array".into()])),
            expr: Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(9)),
            }),
        };
        check_serialization_round_trip(proj_elem_match);
    }
}
