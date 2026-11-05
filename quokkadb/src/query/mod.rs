use crate::error;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::io::size_estimate::SizeEstimate;
use crate::query::expr::{Expr, ProjectionExpr};
use crate::storage::catalog::{IndexDefinition, IndexDirection, IndexPath, OrderedIndexField};
use crate::util::bson_utils;
use crate::util::bson_utils::{BsonKey, TypedKey};
use crate::util::interval::Interval;
use bson::spec::ElementType;
use bson::{Bson, Document};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Result;
use std::sync::Arc;

pub(crate) mod execution;
pub(crate) mod expr;
#[cfg(test)]
pub(crate) mod expr_fn;
pub(crate) mod logical_plan;
pub(crate) mod optimizer;
pub(crate) mod parser;
pub(crate) mod physical_plan;
pub(crate) mod query_cache;
mod tree_node;
pub(crate) mod update;
#[cfg(test)]
pub(crate) mod update_fn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnDocument {
    #[default]
    Before,
    After,
}

/// Parameter classification needed for safe index planning and cache reuse.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub enum ParameterKind {
    NonArray,
    Array,
}

/// An endpoint of a query interval, distinct from the expression it contains.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum QueryEndpoint<T> {
    Value(T),
}

impl<T> QueryEndpoint<T> {
    pub(crate) fn value(&self) -> &T {
        match self {
            Self::Value(value) => value,
        }
    }
}

impl<T: SizeEstimate> SizeEstimate for QueryEndpoint<T> {
    fn estimated_heap_size(&self) -> usize {
        self.value().estimated_heap_size()
    }
}

impl<T: Serializable> Serializable for QueryEndpoint<T> {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        match reader.read_u8()? {
            0 => Ok(Self::Value(T::read_from(reader, version)?)),
            tag => unreachable!("Invalid query endpoint tag: {tag}"),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        match self {
            Self::Value(value) => {
                writer.write_u8(0);
                value.write_to(writer, version);
            }
        }
    }
}

/// Projection for included or excluded fields
/// This is used to specify which fields to include or exclude in the result set of a query.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Include(Arc<ProjectionExpr>), // Fields to include
    Exclude(Arc<ProjectionExpr>), // Fields to exclude
}

impl Serializable for Projection {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(Projection::Include(Arc::<ProjectionExpr>::read_from(
                reader, version,
            )?)),
            1 => Ok(Projection::Exclude(Arc::<ProjectionExpr>::read_from(
                reader, version,
            )?)),
            _ => unreachable!("Invalid tag for Projection: {}", tag),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        match self {
            Projection::Include(exprs) => {
                writer.write_u8(0);
                exprs.write_to(writer, version);
            }
            Projection::Exclude(exprs) => {
                writer.write_u8(1);
                exprs.write_to(writer, version);
            }
        }
    }
}

impl SizeEstimate for Projection {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Projection::Include(expr) | Projection::Exclude(expr) => expr.estimated_heap_size(),
        }
    }
}

/// Represents the sort order for a field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    pub fn reverse(&self) -> SortOrder {
        match self {
            SortOrder::Ascending => SortOrder::Descending,
            SortOrder::Descending => SortOrder::Ascending,
        }
    }
}

impl Serializable for SortOrder {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, _version: u32) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(SortOrder::Ascending),
            1 => Ok(SortOrder::Descending),
            _ => unreachable!("Invalid tag for SortOrder"),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, _version: u32) {
        let byte = match self {
            SortOrder::Ascending => 0,
            SortOrder::Descending => 1,
        };
        writer.write_u8(byte);
    }
}

/// Represents a sorting instruction for a query.
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct SortField {
    pub field: Arc<Expr>,
    pub order: SortOrder,
}

impl SortField {
    pub fn asc(field: Arc<Expr>) -> SortField {
        SortField {
            field,
            order: SortOrder::Ascending,
        }
    }

    pub fn desc(field: Arc<Expr>) -> SortField {
        SortField {
            field,
            order: SortOrder::Descending,
        }
    }

    pub fn reverse(&self) -> SortField {
        SortField {
            field: self.field.clone(),
            order: self.order.reverse(),
        }
    }
}

impl Serializable for SortField {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        let field = Arc::<Expr>::read_from(reader, version)?;
        let order = SortOrder::read_from(reader, version)?;
        Ok(SortField { field, order })
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        self.field.write_to(writer, version);
        self.order.write_to(writer, version);
    }
}

impl SizeEstimate for SortField {
    fn estimated_heap_size(&self) -> usize {
        self.field.estimated_heap_size()
    }
}

#[cfg(test)]
pub fn make_sort_field(path: Vec<PathComponent>, order: SortOrder) -> SortField {
    SortField {
        field: Arc::new(Expr::Field(path)),
        order,
    }
}

/// Represents the ordered key specification of an index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKeySpec {
    pub fields: Vec<IndexKeyField>,
}

impl IndexKeySpec {
    pub fn new(fields: Vec<IndexKeyField>) -> Self {
        Self { fields }
    }

    pub fn validate(&self) -> error::Result<()> {
        for (i, left) in self.fields.iter().enumerate() {
            if left
                .path
                .iter()
                .any(|component| matches!(component, PathComponent::ArrayElement(_)))
            {
                return Err(error::Error::InvalidRequest(format!(
                    "Indexed path '{}' cannot contain array elements",
                    format_path(&left.path)
                )));
            }

            for right in self.fields.iter().skip(i + 1) {
                if left.path == right.path {
                    return Err(error::Error::InvalidRequest(format!(
                        "Indexed path '{}' is specified more than once",
                        format_path(&left.path)
                    )));
                }

                if left.path.starts_with(&right.path) || right.path.starts_with(&left.path) {
                    return Err(error::Error::InvalidRequest(format!(
                        "Indexed paths '{}' and '{}' conflict: no indexed path may be a prefix of another",
                        format_path(&left.path),
                        format_path(&right.path)
                    )));
                }
            }
        }

        Ok(())
    }
}

impl From<IndexKeySpec> for IndexDefinition {
    fn from(spec: IndexKeySpec) -> Self {
        let fields = spec
            .fields
            .into_iter()
            .map(|field| {
                let direction = match field.kind {
                    IndexKeyKind::Regular(SortOrder::Ascending) => IndexDirection::Ascending,
                    IndexKeyKind::Regular(SortOrder::Descending) => IndexDirection::Descending,
                };

                let path = IndexPath {
                    components: field
                        .path
                        .into_iter()
                        .map(|component| match component {
                            PathComponent::FieldName(name) => name,
                            PathComponent::ArrayElement(_index) => {
                                unreachable!("Array elements should not be present in index paths")
                            }
                        })
                        .collect(),
                };

                OrderedIndexField { path, direction }
            })
            .collect();

        IndexDefinition::Regular(fields)
    }
}

/// Represents a single field entry in an index key specification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKeyField {
    pub path: Vec<PathComponent>,
    pub kind: IndexKeyKind,
}

impl IndexKeyField {
    pub fn asc(path: Vec<PathComponent>) -> Self {
        Self {
            path,
            kind: IndexKeyKind::Regular(SortOrder::Ascending),
        }
    }

    pub fn desc(path: Vec<PathComponent>) -> Self {
        Self {
            path,
            kind: IndexKeyKind::Regular(SortOrder::Descending),
        }
    }
}

/// Represents the index-specific behavior associated with a key field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexKeyKind {
    Regular(SortOrder),
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
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
}

impl Serializable for ComparisonOperator {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, _version: u32) -> Result<Self> {
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
            _ => unreachable!("Unknown comparison operator byte"),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, _version: u32) {
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

impl PathComponent {
    pub(crate) fn expect_field_name(&self) -> &str {
        match self {
            Self::FieldName(name) => name,
            Self::ArrayElement(_) => unreachable!("expected a field-name path component"),
        }
    }
}

impl PartialOrd for PathComponent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PathComponent {
    fn cmp(&self, other: &Self) -> Ordering {
        use PathComponent::*;
        match (self, other) {
            (FieldName(a), FieldName(b)) => a.cmp(b),
            (ArrayElement(a), ArrayElement(b)) => a.cmp(b),
            (FieldName(_), ArrayElement(_)) => Ordering::Less,
            (ArrayElement(_), FieldName(_)) => Ordering::Greater,
        }
    }
}

impl Serializable for PathComponent {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, _version: u32) -> Result<Self> {
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
            _ => unreachable!("Unknown path component type"),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, _version: u32) {
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

impl SizeEstimate for PathComponent {
    fn estimated_heap_size(&self) -> usize {
        match self {
            PathComponent::FieldName(name) => name.estimated_heap_size(),
            PathComponent::ArrayElement(size) => size.estimated_heap_size(),
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

impl Into<Vec<PathComponent>> for &IndexPath {
    fn into(self) -> Vec<PathComponent> {
        self.components
            .iter()
            .map(|e| PathComponent::from(e.as_str()))
            .collect()
    }
}

impl fmt::Display for PathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathComponent::FieldName(name) => write!(f, "{}", name),
            PathComponent::ArrayElement(index) => write!(f, "{}", index),
        }
    }
}

/// Extracts a BSON value from a document given a path.
pub fn get_path_value<'a>(doc: &'a Document, path: &[PathComponent]) -> Option<BsonValueRef<'a>> {
    if path.is_empty() {
        return None;
    }

    let mut current = match path.first()? {
        PathComponent::FieldName(name) => doc.get(name)?,
        _ => return None,
    };

    for component in path.iter().skip(1) {
        current = match (component, current) {
            (PathComponent::FieldName(name), Bson::Document(d)) => d.get(name)?,
            (PathComponent::ArrayElement(index), Bson::Array(a)) => a.get(*index)?,
            _ => return None,
        };
    }

    Some(BsonValueRef(current))
}

pub fn format_path(path: &[PathComponent]) -> String {
    path.iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Limit {
    pub skip: Option<usize>,  // Number of rows to skip
    pub limit: Option<usize>, // Maximum number of rows to return
}

impl Serializable for Limit {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        let skip = Option::<usize>::read_from(reader, version)?;
        let limit = Option::<usize>::read_from(reader, version)?;
        Ok(Limit { skip, limit })
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        self.skip.write_to(writer, version);
        self.limit.write_to(writer, version);
    }
}

impl SizeEstimate for Limit {
    fn estimated_heap_size(&self) -> usize {
        self.skip.estimated_heap_size() + self.limit.estimated_heap_size()
    }
}

#[derive(Debug, Clone)]
pub struct BsonValue(pub Bson);

impl BsonValue {
    pub fn to_bson(&self) -> Bson {
        self.0.clone()
    }

    pub fn element_type(&self) -> ElementType {
        self.0.element_type()
    }

    pub fn as_ref(&self) -> BsonValueRef<'_> {
        BsonValueRef(&self.0)
    }
}

impl SizeEstimate for BsonValue {
    fn estimated_heap_size(&self) -> usize {
        bson_utils::estimate_bson_heap_size(&self.0)
    }
}

impl PartialEq for BsonValue {
    fn eq(&self, other: &Self) -> bool {
        bson_utils::bson_eq(&self.0, &other.0)
    }
}

impl Eq for BsonValue {}

impl Hash for BsonValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        bson_utils::bson_hash(&self.0, state);
    }
}

impl PartialOrd for BsonValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(bson_utils::cmp_bson(&self.0, &other.0))
    }
}

impl Ord for BsonValue {
    fn cmp(&self, other: &Self) -> Ordering {
        bson_utils::cmp_bson(&self.0, &other.0)
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

impl From<f64> for BsonValue {
    fn from(value: f64) -> Self {
        BsonValue(Bson::Double(value))
    }
}

impl From<&str> for BsonValue {
    fn from(value: &str) -> Self {
        BsonValue(Bson::String(value.to_string()))
    }
}

impl From<bool> for BsonValue {
    fn from(value: bool) -> Self {
        BsonValue(Bson::Boolean(value))
    }
}

impl<T> From<BTreeSet<T>> for BsonValue
where
    T: Into<Bson>, // Ensure each element can be converted into `Bson`
{
    fn from(values: BTreeSet<T>) -> Self {
        BsonValue(Bson::Array(values.into_iter().map(|v| v.into()).collect()))
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

impl From<Document> for BsonValue {
    fn from(value: Document) -> Self {
        BsonValue(Bson::Document(value))
    }
}

impl From<Bson> for BsonValue {
    fn from(value: Bson) -> Self {
        BsonValue(value)
    }
}

impl BsonKey for BsonValue {
    fn try_into_key(&self) -> Result<Vec<u8>> {
        self.0.try_into_key()
    }

    fn try_into_typed_key(&self) -> Result<TypedKey> {
        self.0.try_into_typed_key()
    }

    fn try_into_id_typed_key(&self) -> Result<TypedKey> {
        self.0.try_into_id_typed_key()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BsonValueRef<'a>(pub &'a Bson);

impl<'a> BsonValueRef<'a> {
    pub fn to_owned(&self) -> BsonValue {
        BsonValue(self.0.clone())
    }

    pub fn to_owned_bson(&self) -> Bson {
        self.0.clone()
    }
}

impl<'a> PartialEq for BsonValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        bson_utils::bson_eq(&self.0, &other.0)
    }
}

impl<'a> Eq for BsonValueRef<'a> {}

impl<'a> Hash for BsonValueRef<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        bson_utils::bson_hash(&self.0, state);
    }
}

impl<'a> PartialOrd for BsonValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(bson_utils::cmp_bson(&self.0, &other.0))
    }
}

impl<'a> Ord for BsonValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        bson_utils::cmp_bson(&self.0, &other.0)
    }
}

impl From<BsonValueRef<'_>> for Bson {
    fn from(value: BsonValueRef) -> Self {
        value.to_owned().to_bson()
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

    pub fn empty() -> Self {
        Self {
            parameters: Vec::with_capacity(0),
        }
    }

    pub fn collect_parameter(&mut self, value: BsonValue) -> Arc<Expr> {
        let idx = self.parameters.len() as u32;
        let kind = if matches!(&value.0, Bson::Array(_)) {
            ParameterKind::Array
        } else {
            ParameterKind::NonArray
        };
        self.parameters.push(value);
        Arc::new(Expr::Placeholder { index: idx, kind })
    }

    pub fn get(&self, index: u32) -> &BsonValue {
        let param = self.parameters.get(index as usize);
        if param.is_none() {
            panic!("Parameter index {} out of bounds", index);
        }
        param.unwrap()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.parameters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::byte_writer::ByteWriter;
    use crate::io::serializable::check_serialization_round_trip;
    use crate::query::expr_fn::*;
    use bson::raw::CString;
    use bson::{Bson, Regex, doc};

    #[test]
    fn parameter_kinds_classify_and_round_trip() {
        for (value, kind) in [
            (Bson::Int32(1), ParameterKind::NonArray),
            (Bson::Document(doc! {"a": 1}), ParameterKind::NonArray),
            (Bson::Array(vec![]), ParameterKind::Array),
            (Bson::Array(vec![Bson::Array(vec![])]), ParameterKind::Array),
        ] {
            let mut parameters = Parameters::new();
            let expr = parameters.collect_parameter(value.clone().into());
            assert_eq!(expr.as_ref(), &Expr::Placeholder { index: 0, kind });
            assert_eq!(parameters.get(0), &BsonValue(value));
            check_serialization_round_trip(expr, 1);
        }
    }

    #[test]
    fn endpoint_serialization_round_trip() {
        let endpoint = QueryEndpoint::Value(placeholder(3));
        let mut writer = ByteWriter::new();
        endpoint.write_to(&mut writer, 0);
        let bytes = writer.take_buffer();
        assert_eq!(bytes[0], 0);
        let decoded = QueryEndpoint::<Arc<Expr>>::read_from(&ByteReader::new(bytes), 0).unwrap();
        assert_eq!(decoded, endpoint);
    }

    #[test]
    fn test_get_path_value() {
        let doc = doc! {
            "a": 1,
            "b": { "c": "hello" },
            "d": [10, 20, { "e": 30 }],
        };

        // Simple field
        assert_eq!(
            get_path_value(&doc, &[PathComponent::FieldName("a".to_string())]),
            Some(BsonValueRef(&Bson::Int32(1)))
        );

        // Nested field
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("b".to_string()),
                    PathComponent::FieldName("c".to_string())
                ]
            ),
            Some(BsonValueRef(&Bson::String("hello".to_string())))
        );

        // Array element
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("d".to_string()),
                    PathComponent::ArrayElement(1)
                ]
            ),
            Some(BsonValueRef(&Bson::Int32(20)))
        );

        // Nested in array
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("d".to_string()),
                    PathComponent::ArrayElement(2),
                    PathComponent::FieldName("e".to_string())
                ]
            ),
            Some(BsonValueRef(&Bson::Int32(30)))
        );

        // Non-existent field
        assert_eq!(
            get_path_value(&doc, &[PathComponent::FieldName("z".to_string())]),
            None
        );

        // Partially correct path
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("b".to_string()),
                    PathComponent::FieldName("z".to_string())
                ]
            ),
            None
        );

        // Path into non-document
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("a".to_string()),
                    PathComponent::FieldName("z".to_string())
                ]
            ),
            None
        );

        // Index into non-array
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("a".to_string()),
                    PathComponent::ArrayElement(0)
                ]
            ),
            None
        );

        // Empty path
        assert_eq!(get_path_value(&doc, &[]), None);
    }

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
            pattern: CString::try_from("abc.*").unwrap(),
            options: CString::try_from("i").unwrap(),
        }));

        let regex2 = BsonValue(Bson::RegularExpression(Regex {
            pattern: CString::try_from("abc.*").unwrap(),
            options: CString::try_from("i").unwrap(),
        }));

        let regex3 = BsonValue(Bson::RegularExpression(Regex {
            pattern: CString::try_from("abc.*").unwrap(),
            options: CString::try_from("").unwrap(),
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
        expr.write_to(&mut writer, 1);
    }

    #[test]
    fn test_expr_serialization_round_trip() {
        // Simple expressions
        check_serialization_round_trip(Expr::AlwaysTrue, 1);
        check_serialization_round_trip(Expr::AlwaysFalse, 1);
        check_serialization_round_trip(Expr::Exists(true), 1);
        check_serialization_round_trip(
            Expr::Placeholder {
                index: 42,
                kind: ParameterKind::NonArray,
            },
            1,
        );

        // Field expressions
        check_serialization_round_trip(Expr::Field(vec!["a".into(), "b".into()]), 1);
        check_serialization_round_trip(Expr::PositionalField(vec!["a".into(), 0.into()]), 1);

        // Comparison
        let comparison = Expr::Comparison {
            operator: ComparisonOperator::Eq,
            value: Arc::new(Expr::Placeholder {
                index: 0,
                kind: ParameterKind::NonArray,
            }),
        };
        check_serialization_round_trip(comparison, 1);

        // Logical expressions
        let not_expr = Expr::Not(Arc::new(Expr::Exists(true)));
        check_serialization_round_trip(not_expr, 1);

        let and_expr = Expr::And(vec![
            Arc::new(Expr::Exists(true)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder {
                    index: 1,
                    kind: ParameterKind::NonArray,
                }),
            }),
        ]);
        check_serialization_round_trip(and_expr, 1);

        let or_expr = Expr::Or(vec![
            Arc::new(Expr::Exists(false)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder {
                    index: 2,
                    kind: ParameterKind::NonArray,
                }),
            }),
        ]);
        check_serialization_round_trip(or_expr, 1);

        let nor_expr = Expr::Nor(vec![
            Arc::new(Expr::Exists(true)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder {
                    index: 3,
                    kind: ParameterKind::NonArray,
                }),
            }),
        ]);
        check_serialization_round_trip(nor_expr, 1);

        // FieldFilters
        let field_filters = Expr::FieldFilters {
            field: Arc::new(Expr::Field(vec!["price".into()])),
            filters: vec![
                Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Gt,
                    value: Arc::new(Expr::Placeholder {
                        index: 4,
                        kind: ParameterKind::NonArray,
                    }),
                }),
                Arc::new(Expr::Exists(true)),
            ],
        };
        check_serialization_round_trip(field_filters, 1);

        // Array expressions
        let all_expr = Expr::All(Arc::new(Expr::Placeholder {
            index: 5,
            kind: crate::query::ParameterKind::NonArray,
        }));
        check_serialization_round_trip(all_expr, 1);

        let elem_match_expr = Expr::ElemMatch(vec![Arc::new(Expr::Comparison {
            operator: ComparisonOperator::Gte,
            value: Arc::new(Expr::Placeholder {
                index: 6,
                kind: ParameterKind::NonArray,
            }),
        })]);
        check_serialization_round_trip(elem_match_expr, 1);

        // Type and Size
        let type_expr = Expr::Type {
            bson_type: Arc::new(Expr::Placeholder {
                index: 7,
                kind: ParameterKind::NonArray,
            }),
            negated: true,
        };
        check_serialization_round_trip(type_expr, 1);

        let size_expr = Expr::Size {
            size: Arc::new(Expr::Placeholder {
                index: 8,
                kind: ParameterKind::NonArray,
            }),
            negated: false,
        };
        check_serialization_round_trip(size_expr, 1);
    }

    #[test]
    fn test_projection_serialization_round_trip() {
        // Simple ProjectionExpr
        check_serialization_round_trip(proj_field(), 1);
        check_serialization_round_trip(proj_slice(Some(5), 10), 1);
        check_serialization_round_trip(proj_slice(None, 20), 1);
        let elem_match_expr = proj_elem_match(eq(placeholder(0)));
        check_serialization_round_trip((*elem_match_expr).clone(), 1);

        // Complex ProjectionExpr
        let complex_projection_expr = proj_fields([
            ("field_a", proj_field()),
            ("field_b", proj_fields([("nested_field", proj_field())])),
            (
                "array_field",
                proj_array_elements([(0, proj_slice(Some(5), 10))]),
            ),
        ]);
        check_serialization_round_trip(complex_projection_expr.clone(), 1);

        // Projection
        check_serialization_round_trip(Projection::Include(complex_projection_expr.clone()), 1);
        check_serialization_round_trip(Projection::Exclude(complex_projection_expr.clone()), 1);

        let simple_include = Projection::Include(proj_fields([("a", proj_field())]));
        check_serialization_round_trip(simple_include, 1);

        let simple_exclude = Projection::Exclude(proj_fields([("b", proj_field())]));
        check_serialization_round_trip(simple_exclude, 1);
    }

    #[test]
    fn test_projection_expr_add_remove() {
        // Initial empty projection
        let mut proj = proj_fields::<_, &str>([]);
        let proj = Arc::get_mut(&mut proj).unwrap();
        assert!(proj.is_empty());

        // Add a simple field "a"
        let path_a: Vec<PathComponent> = vec!["a".into()];
        proj.add_expr(&path_a, 0, proj_field()).unwrap();
        assert_eq!(*proj, *proj_fields([("a", proj_field())]));

        // Add a nested field "b.c"
        let path_b_c: Vec<PathComponent> = vec!["b".into(), "c".into()];
        proj.add_expr(&path_b_c, 0, proj_field()).unwrap();

        let expected = proj_fields([
            ("a", proj_field()),
            ("b", proj_fields([("c", proj_field())])),
        ]);
        assert_eq!(*proj, *expected);

        // Add an array projection "d" with element "0"
        let path_d: Vec<PathComponent> = vec!["d".into()];
        let proj_array_d = proj_array_elements([(0, proj_field())]);
        proj.add_expr(&path_d, 0, proj_array_d).unwrap();

        let expected_d = proj_array_elements([(0, proj_field())]);
        let children_of_root = match &proj {
            ProjectionExpr::Fields { children } => children,
            _ => panic!("Expected Fields projection"),
        };
        assert_eq!(
            children_of_root.get(&"d".into()).unwrap().as_ref(),
            expected_d.as_ref()
        );

        // --- Test removals ---

        // Remove "a"
        proj.remove_expr(&path_a, 0).unwrap();
        let children_of_root = match &proj {
            ProjectionExpr::Fields { children } => children,
            _ => panic!("Expected Fields projection"),
        };
        assert!(!children_of_root.contains_key(&"a".into()));

        // Remove "b.c" - this should also remove "b" as it becomes empty
        proj.remove_expr(&path_b_c, 0).unwrap();
        let children_of_root = match &proj {
            ProjectionExpr::Fields { children } => children,
            _ => panic!("Expected Fields projection"),
        };
        assert!(children_of_root.get(&"b".into()).is_none());
        assert!(!proj.is_empty()); // "d" should still be there

        // Remove "d"
        proj.remove_expr(&path_d, 0).unwrap();
        assert!(proj.is_empty());
    }

    #[test]
    fn test_projection_expr_errors() {
        let mut proj = (*proj_fields::<_, &str>([])).clone();

        // Add "a"
        let path_a: Vec<PathComponent> = vec!["a".into()];
        proj.add_expr(&path_a, 0, proj_field()).unwrap();

        // Error: Try to add "a.b" when "a" is already a terminal field
        let path_a_b: Vec<PathComponent> = vec!["a".into(), "b".into()];
        let result = proj.add_expr(&path_a_b, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid projection specification for path a.b"
        );

        // Error: Try to add "a" again, which is an invalid replacement
        let result = proj.add_expr(&path_a, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid projection specification for path a"
        );

        // Error: Invalid path component type (array index for Fields)
        let path_invalid_component: Vec<PathComponent> = vec![0.into()];
        let result = proj.add_expr(&path_invalid_component, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot use the array index 0 in path: 0. Expecting a field name."
        );

        let mut proj_array = proj_array_elements::<_, usize>([]);
        let proj_array = Arc::get_mut(&mut proj_array).unwrap();

        // Error: Invalid path component type (field name for ArrayElements)
        let path_invalid_component_for_array: Vec<PathComponent> = vec!["field".into()];
        let result = proj_array.add_expr(&path_invalid_component_for_array, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot use the field name field in path: field. Expecting an array element."
        );

        // Test removing non-existent path is a no-op
        let path_z: Vec<PathComponent> = vec!["z".into()];
        let original_proj = proj.clone();
        proj.remove_expr(&path_z, 0).unwrap();
        assert_eq!(proj, original_proj);
    }

    #[test]
    fn test_index_key_spec_validate_accepts_distinct_paths() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["name".into()]),
            IndexKeyField::desc(vec!["age".into()]),
            IndexKeyField::asc(vec!["address".into(), "city".into()]),
        ]);

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_index_key_spec_validate_accepts_sibling_nested_paths() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["address".into(), "city".into()]),
            IndexKeyField::desc(vec!["address".into(), "zip".into()]),
        ]);

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_index_key_spec_converts_to_index_definition() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["name".into()]),
            IndexKeyField::desc(vec!["address".into(), "zip".into()]),
        ]);

        let definition = IndexDefinition::from(spec);

        assert_eq!(
            definition,
            IndexDefinition::Regular(vec![
                OrderedIndexField::asc("name"),
                OrderedIndexField::desc(vec!["address", "zip"]),
            ])
        );
    }

    #[test]
    fn test_index_key_spec_validate_rejects_array_path() {
        let spec = IndexKeySpec::new(vec![IndexKeyField::asc(vec![
            "items".into(),
            0.into(),
            "sku".into(),
        ])]);

        let err = spec.validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Indexed path 'items.0.sku' cannot contain array elements"
        );
    }

    #[test]
    fn test_index_key_spec_validate_rejects_duplicate_path() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["name".into()]),
            IndexKeyField::desc(vec!["name".into()]),
        ]);

        let err = spec.validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Indexed path 'name' is specified more than once"
        );
    }

    #[test]
    fn test_index_key_spec_validate_rejects_parent_before_child() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["name".into()]),
            IndexKeyField::desc(vec!["name".into(), "first".into()]),
        ]);

        let err = spec.validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Indexed paths 'name' and 'name.first' conflict: no indexed path may be a prefix of another"
        );
    }

    #[test]
    fn test_index_key_spec_validate_rejects_child_before_parent() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["name".into(), "first".into()]),
            IndexKeyField::desc(vec!["name".into()]),
        ]);

        let err = spec.validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Indexed paths 'name.first' and 'name' conflict: no indexed path may be a prefix of another"
        );
    }

    #[test]
    fn test_index_key_spec_validate_rejects_array_prefix_conflict() {
        let spec = IndexKeySpec::new(vec![
            IndexKeyField::asc(vec!["items".into(), 0.into()]),
            IndexKeyField::desc(vec!["items".into(), 0.into(), "sku".into()]),
        ]);

        let err = spec.validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Indexed path 'items.0' cannot contain array elements"
        );
    }
}
