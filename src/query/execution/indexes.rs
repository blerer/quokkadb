use crate::io::byte_reader::ByteReader;
use crate::io::{unexpected_eof, varint};
use crate::query::physical_plan::IndexScanRangeExpr;
use crate::query::{
    get_path_value, get_path_value_from_raw, BsonValue, Expr, Parameters, PathComponent, SortOrder,
};
use crate::storage::catalog::{
    CollectionMetadata, IndexDefinition, IndexDirection, IndexMetadata, OrderedIndexField,
};
use crate::storage::operation::Operation;
use crate::util::bson_utils::{decode_bson_from_key_readers, BsonKey, TypedKey};
use crate::util::interval::Interval;
use bson::{Bson, Document, RawDocument};
use std::io::Result;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, LazyLock};

static ID_PATH: LazyLock<Vec<PathComponent>> = LazyLock::new(|| vec!["_id".into()]);

pub struct Indexes {
    indices: Vec<Index>,
}

impl Indexes {
    pub fn from_collection(collection: &CollectionMetadata) -> Self {
        let indices = collection
            .active_indexes()
            .iter()
            .map(|index| Index::from(collection.id, index))
            .collect();
        Self { indices }
    }

    pub fn append_put_ops(
        &self,
        operations: &mut Vec<Operation>,
        document: &Document,
    ) -> Result<()> {
        let key_source = &DocumentKeySource::BsonDocument(document);
        let doc_id = Self::extract_id_key(key_source)?;
        for index in self.indices.iter() {
            index.append_put_op(operations, key_source, &doc_id)?;
        }
        Ok(())
    }

    pub fn append_put_ops_raw(
        &self,
        operations: &mut Vec<Operation>,
        document: &RawDocument,
    ) -> Result<()> {
        let key_source = &DocumentKeySource::RawDocument(document);
        let doc_id = Self::extract_id_key(key_source)?;
        for index in self.indices.iter() {
            index.append_put_op(operations, key_source, &doc_id)?;
        }
        Ok(())
    }

    pub fn append_delete_ops(
        &self,
        operations: &mut Vec<Operation>,
        document: &Document,
    ) -> Result<()> {
        let key_source = &DocumentKeySource::BsonDocument(document);
        let doc_id = Self::extract_id_key(key_source)?;
        for index in self.indices.iter() {
            index.append_delete_op(operations, key_source, &doc_id)?;
        }
        Ok(())
    }

    fn extract_id_key(key_source: &DocumentKeySource) -> Result<TypedKey> {
        key_source.encode_field(&ID_PATH, &SortOrder::Ascending)
    }
}

pub struct IndexKeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct EncodedIndexKeyPart {
    key: Vec<u8>,
    key_type: Vec<u8>,
}

impl From<TypedKey> for EncodedIndexKeyPart {
    fn from(value: TypedKey) -> Self {
        Self {
            key: value.key,
            key_type: value.key_type,
        }
    }
}

struct EncodedIndexKeyPrefix {
    parts: Vec<EncodedIndexKeyPart>,
}

impl EncodedIndexKeyPrefix {
    fn from_parts(parts: Vec<EncodedIndexKeyPart>) -> Self {
        Self { parts }
    }

    fn write_key_prefix_to(&self, out: &mut Vec<u8>) {
        for part in &self.parts {
            out.extend_from_slice(&part.key);
        }
    }

    fn write_key_metadata_to(&self, out: &mut Vec<u8>) {
        for part in &self.parts {
            varint::write_u32(part.key.len() as u32, out);
            out.extend_from_slice(&part.key_type);
        }
    }
}

pub struct Index {
    collection_id: u32,
    id: u32,
    field_codecs: Vec<IndexFieldCodec>,
}

impl Index {
    pub fn from(collection_id: u32, index: &IndexMetadata) -> Self {
        let id = index.id;
        match &index.definition {
            IndexDefinition::Regular(fields) => {
                let field_codecs = fields
                    .iter()
                    .map(|key| IndexFieldCodec::from(key))
                    .collect();
                Self {
                    collection_id,
                    id,
                    field_codecs,
                }
            }
        }
    }

    fn append_put_op(
        &self,
        operations: &mut Vec<Operation>,
        key_source: &DocumentKeySource,
        doc_id: &TypedKey,
    ) -> Result<()> {
        let IndexKeyValue { key, value } = self.extract_index_entry(key_source, doc_id)?;
        let op = Operation::new_put(self.collection_id, self.id, key, value);
        operations.push(op);
        Ok(())
    }

    fn append_delete_op(
        &self,
        operations: &mut Vec<Operation>,
        key_source: &DocumentKeySource,
        doc_id: &TypedKey,
    ) -> Result<()> {
        let IndexKeyValue { key, value: _ } = self.extract_index_entry(key_source, doc_id)?;
        let op = Operation::new_delete(self.collection_id, self.id, key);
        operations.push(op);
        Ok(())
    }

    fn extract_index_entry(
        &self,
        key_source: &DocumentKeySource,
        doc_id: &TypedKey,
    ) -> Result<IndexKeyValue> {
        let encoded_prefix = self.encode_index_prefix(key_source)?;
        let key = self.encode_full_index_key(&encoded_prefix, doc_id);
        let value = self.encode_index_entry_value(&encoded_prefix, doc_id);
        Ok(IndexKeyValue { key, value })
    }

    fn encode_index_prefix(&self, key_source: &DocumentKeySource) -> Result<EncodedIndexKeyPrefix> {
        let parts = self
            .field_codecs
            .iter()
            .map(|codec| codec.encode_from_source(key_source))
            .map(|result| result.map(EncodedIndexKeyPart::from))
            .collect::<Result<Vec<_>>>()?;
        Ok(EncodedIndexKeyPrefix::from_parts(parts))
    }

    fn encode_full_index_key(
        &self,
        encoded_prefix: &EncodedIndexKeyPrefix,
        doc_id: &TypedKey,
    ) -> Vec<u8> {
        let mut key = Vec::new();
        encoded_prefix.write_key_prefix_to(&mut key);
        key.extend_from_slice(&doc_id.key);
        key
    }

    fn encode_index_entry_value(
        &self,
        encoded_prefix: &EncodedIndexKeyPrefix,
        doc_id: &TypedKey,
    ) -> Vec<u8> {
        // The index value is:
        // [value_len u32 LE][_id_len varint][_id_key_type bytes]([key_len varint][key_type bytes])*
        // * value_len: the value length is necessary because the sstable reader expects the
        //     length to be the first 4 bytes of a value (same as for Bson documents).
        // * _id_length: having the _id length allows for fast extraction of the _id from the key
        //     without having to perform some parsing or repeating the information in the value.
        // * _id_key_type bytes: the key_type bytes for the _id value, length-delimited by a
        //     trailing 0x00 byte (the key_type encoding is self-delimiting per field, so a single
        //     byte suffices for all scalar _id types; containers are terminated by their own
        //     structure). We store them explicitly so that decode can fully round-trip _id.
        // * key_len: the length of the key. It is needed as the bytes might need to be flipped
        //     if the sort direction is descending.
        // * key types: encoded information about the index key parts that allow decoding the keys
        //      to take advantage of covering indexes or to perform some filtering at the index
        //      level.
        let mut value = Vec::new();
        value.extend_from_slice(&[0u8; 4]);
        varint::write_u32(doc_id.key.len() as u32, &mut value);
        varint::write_u32(doc_id.key_type.len() as u32, &mut value);
        value.extend_from_slice(&doc_id.key_type);
        encoded_prefix.write_key_metadata_to(&mut value);

        let value_size = (value.len() as u32).to_le_bytes();
        value[0..4].copy_from_slice(&value_size);
        value
    }

    pub fn bind_range_expr(
        &self,
        range_expr: &IndexScanRangeExpr,
        parameters: &Parameters,
    ) -> Result<Interval<Vec<u8>>> {
        if range_expr.equal_prefix.is_empty() && range_expr.tail.is_none() {
            return Ok(Interval::all());
        }

        let prefix_len = range_expr.equal_prefix.len();

        let equal_prefix = range_expr
            .equal_prefix
            .iter()
            .zip(self.field_codecs.iter())
            .map(|(expr, codec)| {
                let value = resolve_bound_expr(expr, parameters);
                codec.encode_value(&value)
            })
            .collect::<Result<Vec<_>>>()?;
        let equal_prefix = EncodedIndexKeyPrefix::from_parts(
            equal_prefix
                .into_iter()
                .map(EncodedIndexKeyPart::from)
                .collect(),
        );

        let remaining_fields_codecs = &self.field_codecs[prefix_len..];
        match &range_expr.tail {
            Some(interval) => {
                assert!(
                    prefix_len < self.field_codecs.len(),
                    "Index range tail requires an index field after the equality prefix"
                );
                let start_bound = match remaining_fields_codecs[0].order {
                    SortOrder::Ascending => interval.start_bound(),
                    SortOrder::Descending => interval.end_bound(),
                };
                let end_bound = match remaining_fields_codecs[0].order {
                    SortOrder::Ascending => interval.end_bound(),
                    SortOrder::Descending => interval.start_bound(),
                };

                let start = bind_tail_bound(
                    &equal_prefix,
                    remaining_fields_codecs,
                    start_bound,
                    BoundSide::Start,
                    parameters,
                )?;
                let end = bind_tail_bound(
                    &equal_prefix,
                    remaining_fields_codecs,
                    end_bound,
                    BoundSide::End,
                    parameters,
                )?;

                Ok(Interval::new(start, end))
            }
            None => {
                let start = Bound::Included(encode_index_range_key(
                    &equal_prefix,
                    None,
                    remaining_fields_codecs,
                    SuffixExtremum::Low,
                )?);
                let end = Bound::Included(encode_index_range_key(
                    &equal_prefix,
                    None,
                    remaining_fields_codecs,
                    SuffixExtremum::High,
                )?);
                Ok(Interval::new(start, end))
            }
        }
    }

    pub fn extract_id<'a>(index_key_value: &'a IndexKeyValue) -> &'a [u8] {
        Self::extract_id_from_entry_bytes(&index_key_value.key, &index_key_value.value)
            .expect("index entry produced by encoder must contain a valid _id")
    }

    pub(crate) fn extract_id_from_entry_bytes<'a>(key: &'a [u8], value: &[u8]) -> Result<&'a [u8]> {
        if value.len() < 5 {
            return Err(unexpected_eof("index entry value is truncated"));
        }

        let (id_len, _) = varint::read_u32(value, 4);
        let id_len = id_len as usize;
        if key.len() < id_len {
            return Err(unexpected_eof(
                "index entry key is shorter than stored _id length",
            ));
        }

        // The _id bytes are appended at the end of the key.
        let parts_key_len = key.len() - id_len;
        Ok(&key[parts_key_len..])
    }

    pub fn decode_index_entry(&self, index_key_value: IndexKeyValue) -> Result<Document> {
        let key = &index_key_value.key;
        let value = &index_key_value.value;

        if value.len() < 5 {
            return Err(unexpected_eof("index entry value is truncated"));
        }

        // Parse the value buffer.
        // Layout: [value_len u32 LE][_id_len varint][_id_key_type_len varint][_id_key_type bytes]
        //         ([key_len varint][key_type bytes])*
        let value_reader = ByteReader::new(value.as_slice());
        value_reader.skip(4)?; // skip u32 value_len prefix

        let _id_len = value_reader.read_varint_u32()? as usize;
        let id_key_type_len = value_reader.read_varint_u32()? as usize;
        let id_key_type_bytes = value_reader.read_fixed_slice(id_key_type_len)?.to_vec();

        // The _id bytes are appended at the end of the key.
        let id_key_bytes = Self::extract_id_from_entry_bytes(key, value)?;
        let parts_key_len = key.len() - id_key_bytes.len();

        let mut key_offset = 0usize;
        let mut doc = Document::new();

        for codec in self.field_codecs.iter() {
            let part_key_len = value_reader.read_varint_u32()? as usize;
            if key_offset + part_key_len > parts_key_len {
                return Err(unexpected_eof("index entry key is truncated"));
            }
            // Slice the raw key bytes for this part, un-inverting if descending.
            let raw_part_key = &key[key_offset..key_offset + part_key_len];
            key_offset += part_key_len;
            let part_key_owned: Vec<u8> = if matches!(codec.order, SortOrder::Descending) {
                raw_part_key.iter().map(|&b| !b).collect()
            } else {
                raw_part_key.to_vec()
            };

            // Drive decode_key_inner jointly over the key and value (type) readers.
            // This advances value_reader by exactly the key_type bytes for this field,
            // which are self-delimiting and require no explicit length prefix.
            let key_reader = ByteReader::new(part_key_owned.as_slice());
            let bson_value = decode_bson_from_key_readers(&key_reader, &value_reader)?;

            // Use set_path_value so that nested paths (e.g. "address.city") are
            // reconstructed as proper nested Documents rather than flat dot-notation keys.
            crate::query::execution::set_path_value(&mut doc, &codec.path, bson_value);
        }

        // Decode the _id from the key suffix using the stored key_type bytes.
        let id_key_bytes = ByteReader::new(id_key_bytes);
        let id_bson = decode_bson_from_key_readers(&id_key_bytes, &ByteReader::new(&id_key_type_bytes))?;
        doc.insert("_id", id_bson);

        Ok(doc)
    }
}

struct IndexFieldCodec {
    pub(super) path: Vec<PathComponent>,
    pub(super) order: SortOrder,
}

impl IndexFieldCodec {
    pub fn from(key_part: &OrderedIndexField) -> Self {
        let OrderedIndexField { path, direction } = key_part;
        let path: Vec<PathComponent> = path.into();
        let order = match direction {
            IndexDirection::Ascending => SortOrder::Ascending,
            IndexDirection::Descending => SortOrder::Descending,
        };
        Self { path, order }
    }

    pub fn encode_from_source(&self, source: &DocumentKeySource) -> Result<TypedKey> {
        let value = source.resolve_field_value(&self.path);
        self.encode_value(&value)
    }

    pub fn encode_value(&self, value: &BsonValue) -> Result<TypedKey> {
        to_typed_key(value, &self.order)
    }

    pub fn encode_low_sentinel(&self) -> Result<TypedKey> {
        let sentinel = sentinel_for_order(&self.order, SuffixExtremum::Low);
        self.encode_value(&sentinel)
    }

    pub fn encode_high_sentinel(&self) -> Result<TypedKey> {
        let sentinel = sentinel_for_order(&self.order, SuffixExtremum::High);
        self.encode_value(&sentinel)
    }

    pub fn decode(&self, raw_key_bytes: &[u8], key_type_bytes: &[u8]) -> Result<(String, Bson)> {
        let part_key_owned: Vec<u8> = if matches!(self.order, SortOrder::Descending) {
            raw_key_bytes.iter().map(|&b| !b).collect()
        } else {
            raw_key_bytes.to_vec()
        };

        let bson_value = decode_bson_from_key_readers(
            &ByteReader::new(&part_key_owned),
            &ByteReader::new(key_type_bytes),
        )?;

        let field_name = self
            .path
            .iter()
            .map(|c| match c {
                PathComponent::FieldName(s) => s.clone(),
                PathComponent::ArrayElement(i) => i.to_string(),
            })
            .collect::<Vec<_>>()
            .join(".");

        Ok((field_name, bson_value))
    }
}

fn invert_bytes(bytes: &mut Vec<u8>) {
    for i in 0..bytes.len() {
        bytes[i] = !bytes[i];
    }
}

#[derive(Clone, Copy)]
enum BoundSide {
    Start,
    End,
}

#[derive(Clone, Copy)]
enum SuffixExtremum {
    Low,
    High,
}

fn bind_tail_bound(
    equal_prefix: &EncodedIndexKeyPrefix,
    remaining_field_codecs: &[IndexFieldCodec],
    bound: Bound<&Arc<Expr>>,
    side: BoundSide,
    parameters: &Parameters,
) -> Result<Bound<Vec<u8>>> {
    let tail_codec = &remaining_field_codecs[0];
    let after_tail_field_codecs = &remaining_field_codecs[1..];
    match bound {
        Bound::Included(expr) => {
            let value = resolve_bound_expr(expr, parameters);
            let tail = tail_codec.encode_value(&value)?;
            let key = encode_index_range_key(
                equal_prefix,
                Some(&tail),
                after_tail_field_codecs,
                if matches!(side, BoundSide::Start) {
                    SuffixExtremum::Low
                } else {
                    SuffixExtremum::High
                },
            )?;
            Ok(Bound::Included(key))
        }
        Bound::Excluded(expr) => {
            let value = resolve_bound_expr(expr, parameters);
            let tail = tail_codec.encode_value(&value)?;
            let key = encode_index_range_key(
                equal_prefix,
                Some(&tail),
                after_tail_field_codecs,
                if matches!(side, BoundSide::Start) {
                    SuffixExtremum::High
                } else {
                    SuffixExtremum::Low
                },
            )?;
            Ok(Bound::Excluded(key))
        }
        Bound::Unbounded => {
            let key = encode_index_range_key(
                equal_prefix,
                None,
                remaining_field_codecs,
                if matches!(side, BoundSide::Start) {
                    SuffixExtremum::Low
                } else {
                    SuffixExtremum::High
                },
            )?;
            Ok(Bound::Included(key))
        }
    }
}

fn encode_index_range_key(
    equal_prefix: &EncodedIndexKeyPrefix,
    tail: Option<&TypedKey>,
    remaining_field_codecs: &[IndexFieldCodec],
    suffix_extremum: SuffixExtremum,
) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    equal_prefix.write_key_prefix_to(&mut key);
    if let Some(tail) = tail {
        key.extend_from_slice(&tail.key);
    }
    append_open_range_suffix(
        &mut key,
        remaining_field_codecs,
        suffix_extremum,
    )?;
    Ok(key)
}

fn append_open_range_suffix(
    out: &mut Vec<u8>,
    remaining_field_codecs: &[IndexFieldCodec],
    suffix_extremum: SuffixExtremum,
) -> Result<()> {
    for codec in remaining_field_codecs {
        let encoded = match suffix_extremum {
            SuffixExtremum::Low => codec.encode_low_sentinel()?,
            SuffixExtremum::High => codec.encode_high_sentinel()?,
        };
        out.extend_from_slice(&encoded.key);
    }

    let id_key = encode_id_sentinel(suffix_extremum)?;
    out.extend_from_slice(&id_key.key);
    Ok(())
}

fn encode_id_sentinel(suffix_extremum: SuffixExtremum) -> Result<TypedKey> {
    let sentinel = sentinel_for_order(&SortOrder::Ascending, suffix_extremum);
    to_typed_key(&sentinel, &SortOrder::Ascending)
}

fn sentinel_for_order(order: &SortOrder, extremum: SuffixExtremum) -> BsonValue {
    match (order, extremum) {
        (SortOrder::Ascending, SuffixExtremum::Low) => BsonValue(Bson::MinKey),
        (SortOrder::Ascending, SuffixExtremum::High) => BsonValue(Bson::MaxKey),
        (SortOrder::Descending, SuffixExtremum::Low) => BsonValue(Bson::MaxKey),
        (SortOrder::Descending, SuffixExtremum::High) => BsonValue(Bson::MinKey),
    }
}

fn resolve_bound_expr(expr: &Arc<Expr>, parameters: &Parameters) -> BsonValue {
    match expr.as_ref() {
        Expr::Placeholder(idx) => parameters.get(*idx).clone(),
        Expr::Literal(value) => value.clone(),
        _ => unreachable!("Index scan bounds only support placeholders and literals: {:?}", expr),
    }
}

pub enum DocumentKeySource<'a> {
    BsonDocument(&'a Document),
    RawDocument(&'a RawDocument),
}

impl<'a> DocumentKeySource<'a> {
    pub fn resolve_field_value(&self, path: &Vec<PathComponent>) -> BsonValue {
        match self {
            DocumentKeySource::BsonDocument(doc) => match get_path_value(doc, path) {
                Some(v) => v.to_owned(),
                None => BsonValue(Bson::Null),
            },
            DocumentKeySource::RawDocument(doc) => match get_path_value_from_raw(doc, path) {
                Some(v) => v.to_owned(),
                None => BsonValue(Bson::Null),
            },
        }
    }

    pub fn encode_field(&self, path: &Vec<PathComponent>, order: &SortOrder) -> Result<TypedKey> {
        let value = self.resolve_field_value(path);
        to_typed_key(&value, order)
    }
}

fn to_typed_key(value: &BsonValue, order: &SortOrder) -> Result<TypedKey> {
    let TypedKey { mut key, key_type } = value.try_into_typed_key()?;
    if matches!(order, SortOrder::Descending) {
        invert_bytes(&mut key);
    }
    Ok(TypedKey { key, key_type })
}

#[cfg(test)]
mod tests {
    use super::{DocumentKeySource, Index, IndexKeyValue, Indexes};
    use crate::io::varint;
    use crate::query::physical_plan::IndexScanRangeExpr;
    use crate::storage::catalog::{
        CollectionMetadata, CollectionOptions, IndexDefinition, IndexDirection, IndexMetadata,
        IndexPath, OrderedIndexField,
    };
    use crate::storage::operation::OperationType;
    use crate::util::interval::Interval;
    use crate::util::bson_utils::BsonKey;
    use bson::{
        doc, oid::ObjectId, raw::RawDocumentBuf, spec::BinarySubtype, Binary, Bson, DateTime,
        Decimal128, Document, Timestamp,
    };
    use crate::query::{BsonValue, Expr, Parameters};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn make_index(fields: Vec<(&str, IndexDirection)>) -> Index {
        let metadata = make_index_metadata(1, fields);
        Index::from(42, &metadata)
    }

    fn extract_and_decode(index: &Index, doc: &Document) -> Document {
        let doc_id = {
            let id_val = doc.get("_id").expect("document must have _id");
            id_val
                .try_into_typed_key()
                .expect("_id must be convertible to key")
        };

        let key_source = DocumentKeySource::BsonDocument(doc);
        let entry = index
            .extract_index_entry(&key_source, &doc_id)
            .expect("extract should succeed");
        index
            .decode_index_entry(entry)
            .expect("decode should succeed")
    }

    fn extract_key(index: &Index, doc: &Document) -> Vec<u8> {
        let doc_id = {
            let id_val = doc.get("_id").expect("document must have _id");
            id_val
                .try_into_typed_key()
                .expect("_id must be convertible to key")
        };

        let key_source = DocumentKeySource::BsonDocument(doc);
        index
            .extract_index_entry(&key_source, &doc_id)
            .expect("extract should succeed")
            .key
    }

    fn make_collection(indexes: Vec<IndexMetadata>) -> CollectionMetadata {
        let next_index_id = indexes.iter().map(|index| index.id).max().unwrap_or(0) + 1;
        let indexes = indexes
            .into_iter()
            .map(|index| (index.id, Arc::new(index)))
            .collect::<BTreeMap<_, _>>();
        let mut collection = CollectionMetadata::new(42, "test", 1, CollectionOptions::default());
        collection.next_index_id = next_index_id;
        collection.indexes = indexes;
        collection
    }

    fn make_index_metadata(id: u32, fields: Vec<(&str, IndexDirection)>) -> IndexMetadata {
        let ordered_fields = fields
            .into_iter()
            .map(|(name, direction)| OrderedIndexField {
                path: IndexPath {
                    components: name.split('.').map(|s| s.to_string()).collect(),
                },
                direction,
            })
            .collect();
        IndexMetadata::new(id, IndexDefinition::Regular(ordered_fields), 1)
    }

    fn supported_id_values() -> Vec<(&'static str, Bson)> {
        vec![
            ("min_key", Bson::MinKey),
            ("null", Bson::Null),
            ("max_key", Bson::MaxKey),
            ("bool_false", Bson::Boolean(false)),
            ("bool_true", Bson::Boolean(true)),
            ("string", Bson::String("user-42".to_string())),
            (
                "object_id",
                Bson::ObjectId(ObjectId::from_bytes([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                ])),
            ),
            (
                "datetime",
                Bson::DateTime(DateTime::from_millis(1_735_689_600_123)),
            ),
            (
                "timestamp",
                Bson::Timestamp(Timestamp {
                    time: 1234,
                    increment: 7,
                }),
            ),
            (
                "binary",
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: vec![0, 1, 2, 255],
                }),
            ),
            ("double", Bson::Double(3.25)),
            ("int32", Bson::Int32(42)),
            ("int64", Bson::Int64(1_000_000_000_000)),
            (
                "decimal128",
                Bson::Decimal128("12.50".parse::<Decimal128>().unwrap()),
            ),
            ("document_simple", Bson::Document(doc! { "k": "v" })),
            (
                "document_nested",
                Bson::Document(doc! { "outer": { "inner": 9_i32 }, "flag": true }),
            ),
            (
                "array_scalar",
                Bson::Array(vec![Bson::Int32(1), Bson::String("two".to_string())]),
            ),
            (
                "array_nested",
                Bson::Array(vec![
                    Bson::Document(doc! { "x": 1_i32 }),
                    Bson::Array(vec![Bson::Boolean(true), Bson::Null]),
                ]),
            ),
        ]
    }

    fn assert_id_round_trip(label: &str, actual: &Bson, expected: &Bson) {
        match (actual, expected) {
            (Bson::Decimal128(actual), Bson::Decimal128(expected)) => {
                assert_eq!(
                    Bson::Decimal128(*actual).try_into_key().unwrap(),
                    Bson::Decimal128(*expected).try_into_key().unwrap(),
                    "failed id round trip for {label}"
                );
            }
            _ => assert_eq!(actual, expected, "failed id round trip for {label}"),
        }
    }

    // ---------------------------------------------------------------------------
    // Basic single-field ascending
    // ---------------------------------------------------------------------------

    #[test]
    fn single_field_ascending_round_trip_string() {
        let index = make_index(vec![("name", IndexDirection::Ascending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "name": "alice" };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(decoded, doc! {"_id": id, "name": "alice"});
    }

    #[test]
    fn single_field_ascending_round_trip_int32() {
        let index = make_index(vec![("score", IndexDirection::Ascending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "score": 42_i32 };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(decoded, doc! { "_id": id, "score": 42_i32 });
    }

    #[test]
    fn single_field_ascending_round_trip_int64() {
        let index = make_index(vec![("counter", IndexDirection::Ascending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "counter": Bson::Int64(1_000_000_000_000_i64) };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(
            decoded,
            doc! { "_id": id, "counter": Bson::Int64(1_000_000_000_000_i64) }
        );
    }

    #[test]
    fn single_field_ascending_round_trip_double() {
        let index = make_index(vec![("ratio", IndexDirection::Ascending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "ratio": 3.14_f64 };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(decoded, doc! { "_id": id, "ratio": 3.14_f64 });
    }

    #[test]
    fn single_field_ascending_round_trip_boolean() {
        let index = make_index(vec![("active", IndexDirection::Ascending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "active": true };

        let decoded = extract_and_decode(&index, &document);

        assert_eq!(decoded, doc! { "_id": id, "active": true });
    }

    #[test]
    fn missing_indexed_field_is_encoded_as_null() {
        let index = make_index(vec![("missing", IndexDirection::Ascending)]);
        let document = doc! { "_id": 56i64, "present": 1_i32 };

        let decoded = extract_and_decode(&index, &document);

        assert_eq!(decoded, doc! { "_id": 56i64, "missing": Bson::Null });
    }

    // ---------------------------------------------------------------------------
    // Single-field descending
    // ---------------------------------------------------------------------------

    #[test]
    fn single_field_descending_round_trip_int32() {
        let index = make_index(vec![("score", IndexDirection::Descending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "score": -7_i32 };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(decoded, doc! { "_id": id, "score": -7_i32 });
    }

    #[test]
    fn single_field_descending_round_trip_string() {
        let index = make_index(vec![("tag", IndexDirection::Descending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "tag": "zulu" };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(decoded, doc! { "_id": id, "tag": "zulu" });
    }

    #[test]
    fn descending_key_sorts_opposite_to_ascending() {
        // Produce two entries with ascending and two with descending index.
        // The descending entries should sort in reverse order of the values.
        let asc_index = make_index(vec![("v", IndexDirection::Ascending)]);
        let desc_index = make_index(vec![("v", IndexDirection::Descending)]);

        let id1 = 1i64;
        let id2 = 2i64;
        let doc_lo = doc! { "_id": id1, "v": 1_i32 };
        let doc_hi = doc! { "_id": id2, "v": 2_i32 };

        let asc_key_lo = {
            let ks = DocumentKeySource::BsonDocument(&doc_lo);
            let id_key = Indexes::extract_id_key(&ks).unwrap();
            asc_index.extract_index_entry(&ks, &id_key).unwrap().key
        };
        let asc_key_hi = {
            let ks = DocumentKeySource::BsonDocument(&doc_hi);
            let id_key = Indexes::extract_id_key(&ks).unwrap();
            asc_index.extract_index_entry(&ks, &id_key).unwrap().key
        };
        let desc_key_lo = {
            let ks = DocumentKeySource::BsonDocument(&doc_lo);
            let id_key = Indexes::extract_id_key(&ks).unwrap();
            desc_index.extract_index_entry(&ks, &id_key).unwrap().key
        };
        let desc_key_hi = {
            let ks = DocumentKeySource::BsonDocument(&doc_hi);
            let id_key = Indexes::extract_id_key(&ks).unwrap();
            desc_index.extract_index_entry(&ks, &id_key).unwrap().key
        };

        assert!(
            asc_key_lo < asc_key_hi,
            "ascending: lo should sort before hi"
        );
        assert!(
            desc_key_lo > desc_key_hi,
            "descending: lo should sort after hi"
        );
    }

    // ---------------------------------------------------------------------------
    // Multi-field indexes
    // ---------------------------------------------------------------------------

    #[test]
    fn multi_field_ascending_round_trip() {
        let index = make_index(vec![
            ("last", IndexDirection::Ascending),
            ("first", IndexDirection::Ascending),
        ]);
        let id = 56i64;
        let document = doc! { "_id": id, "last": "smith", "first": "john" };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(
            &decoded,
            &doc! { "_id": id, "last": "smith", "first": "john" }
        );
    }

    #[test]
    fn multi_field_mixed_directions_round_trip() {
        let index = make_index(vec![
            ("category", IndexDirection::Ascending),
            ("rank", IndexDirection::Descending),
        ]);
        let id = 56i64;
        let document = doc! { "_id": id, "category": "books", "rank": 99_i32 };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(
            decoded,
            doc! { "_id": id, "category": "books", "rank": 99_i32 }
        );
    }

    #[test]
    fn multi_field_three_fields_round_trip() {
        let index = make_index(vec![
            ("a", IndexDirection::Ascending),
            ("b", IndexDirection::Descending),
            ("c", IndexDirection::Ascending),
        ]);
        let id = 56i64;
        let document =
            doc! { "_id": id, "a": "hello", "b": -5_i32, "c": Bson::Int64(999), "d": "world" };

        let decoded = extract_and_decode(&index, &document);
        assert_eq!(
            decoded,
            doc! { "_id": id, "a": "hello", "b": -5_i32, "c": Bson::Int64(999) }
        );
    }

    // ---------------------------------------------------------------------------
    // Value layout: _id length and key lengths are stored correctly
    // ---------------------------------------------------------------------------
    #[test]
    fn supported_id_types_round_trip_using_stored_id_key_type() {
        let index = make_index(vec![("name", IndexDirection::Ascending)]);
        for (label, id_value) in supported_id_values() {
            let document = doc! { "_id": id_value.clone(), "name": "alice" };

            let decoded = extract_and_decode(&index, &document);

            assert_eq!(decoded.get_str("name").unwrap(), "alice");
            assert_id_round_trip(label, decoded.get("_id").unwrap(), &id_value);
        }
    }

    #[test]
    fn value_layout_id_length_matches_key_suffix() {
        let index = make_index(vec![("x", IndexDirection::Ascending)]);
        let id = 56i64;
        let document = doc! { "_id": id, "x": 1_i32 };

        let id_key = { Bson::Int64(id).try_into_typed_key().unwrap() };
        let key_source = DocumentKeySource::BsonDocument(&document);
        let entry = index.extract_index_entry(&key_source, &id_key).unwrap();

        // Skip the u32 value_len prefix.
        let (id_len, _) = varint::read_u32(&entry.value, 4);
        let id_len = id_len as usize;

        // The last `id_len` bytes of the key should equal the id_key bytes.
        let key_suffix = &entry.key[entry.key.len() - id_len..];
        assert_eq!(key_suffix, id_key.key.as_slice());
    }

    // ---------------------------------------------------------------------------
    // Nested field paths
    // ---------------------------------------------------------------------------

    #[test]
    fn nested_field_round_trip() {
        let index = make_index(vec![("address.city", IndexDirection::Ascending)]);
        let id = 1i64;
        let document = doc! {
            "_id": id,
            "address": { "city": "London" }
        };

        let decoded = extract_and_decode(&index, &document);

        // The decoded document should have a nested structure, not a flat dot-key.
        assert_eq!(
            decoded,
            doc! {
                "_id": id,
                "address": { "city": "London" }
            }
        );
    }

    #[test]
    fn value_layout_prefix_and_id_key_type_bytes_are_stored() {
        let index = make_index(vec![("x", IndexDirection::Ascending)]);
        let id = "user-42";
        let id_key = Bson::String(id.to_string()).try_into_typed_key().unwrap();
        let key_source = DocumentKeySource::BsonDocument(&doc! { "_id": id, "x": 1_i32 });
        let entry = index.extract_index_entry(&key_source, &id_key).unwrap();

        let stored_len = u32::from_le_bytes(entry.value[0..4].try_into().unwrap()) as usize;
        assert_eq!(stored_len, entry.value.len());

        let (id_len, offset) = varint::read_u32(&entry.value, 4);
        assert_eq!(id_len as usize, id_key.key.len());

        let (id_key_type_len, offset) = varint::read_u32(&entry.value, offset);
        assert_eq!(id_key_type_len as usize, id_key.key_type.len());
        assert_eq!(
            &entry.value[offset..offset + id_key.key_type.len()],
            id_key.key_type.as_slice()
        );
    }

    #[test]
    fn append_put_ops_emits_only_active_indexes() {
        let active = IndexMetadata::new(
            1,
            IndexDefinition::Regular(vec![OrderedIndexField {
                path: IndexPath {
                    components: vec!["name".to_string()],
                },
                direction: IndexDirection::Ascending,
            }]),
            1,
        );
        let mut dropped = IndexMetadata::new(
            2,
            IndexDefinition::Regular(vec![OrderedIndexField {
                path: IndexPath {
                    components: vec!["age".to_string()],
                },
                direction: IndexDirection::Ascending,
            }]),
            1,
        );
        dropped.dropped_at = Some(2);
        let collection = make_collection(vec![active, dropped]);
        let indices = Indexes::from_collection(&collection);
        let document = doc! { "_id": 7i64, "name": "alice", "age": 30_i32 };
        let mut operations = Vec::new();

        indices.append_put_ops(&mut operations, &document).unwrap();

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].operation_type, OperationType::Put);
        assert_eq!(operations[0].collection, 42);
        assert_eq!(operations[0].index, 1);
        assert!(!operations[0].value().is_empty());
    }

    #[test]
    fn append_delete_ops_emits_delete_operation_with_empty_value() {
        let collection = make_collection(vec![make_index_metadata(
            1,
            vec![("name", IndexDirection::Ascending)],
        )]);
        let indices = Indexes::from_collection(&collection);
        let document = doc! { "_id": 7i64, "name": "alice" };
        let mut operations = Vec::new();

        indices
            .append_delete_ops(&mut operations, &document)
            .unwrap();

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].operation_type, OperationType::Delete);
        assert_eq!(operations[0].collection, 42);
        assert_eq!(operations[0].index, 1);
        assert!(operations[0].value().is_empty());
    }

    #[test]
    fn append_put_ops_raw_matches_bson_document_variant() {
        let collection = make_collection(vec![make_index_metadata(
            1,
            vec![("address.city", IndexDirection::Ascending)],
        )]);
        let indices = Indexes::from_collection(&collection);
        let document = doc! {
            "_id": 7i64,
            "address": { "city": "London" }
        };
        let raw = RawDocumentBuf::from_document(&document).unwrap();
        let mut bson_ops = Vec::new();
        let mut raw_ops = Vec::new();

        indices.append_put_ops(&mut bson_ops, &document).unwrap();
        indices
            .append_put_ops_raw(&mut raw_ops, raw.as_ref())
            .unwrap();

        assert_eq!(bson_ops.len(), 1);
        assert_eq!(raw_ops.len(), 1);
        assert_eq!(bson_ops[0], raw_ops[0]);
    }

    #[test]
    fn bind_range_expr_empty_is_unbounded() {
        let index = make_index(vec![("a", IndexDirection::Ascending)]);

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![],
                    tail: None,
                },
                &Parameters::empty(),
            )
            .unwrap();

        assert_eq!(range, Interval::all());
    }

    #[test]
    fn bind_range_expr_single_field_equality_contains_matching_keys_only() {
        let index = make_index(vec![("a", IndexDirection::Ascending)]);
        let mut parameters = Parameters::new();
        let bound = parameters.collect_parameter(10_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![bound],
                    tail: None,
                },
                &parameters,
            )
            .unwrap();

        let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32 });
        let lower = extract_key(&index, &doc! { "_id": 2_i64, "a": 9_i32 });
        let higher = extract_key(&index, &doc! { "_id": 3_i64, "a": 11_i32 });

        assert!(range.contains(&matching));
        assert!(!range.contains(&lower));
        assert!(!range.contains(&higher));
    }

    #[test]
    fn bind_range_expr_compound_full_equality_contains_only_matching_prefix() {
        let index = make_index(vec![
            ("a", IndexDirection::Ascending),
            ("b", IndexDirection::Descending),
        ]);
        let mut parameters = Parameters::new();
        let a = parameters.collect_parameter(10_i32.into());
        let b = parameters.collect_parameter(20_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![a, b],
                    tail: None,
                },
                &parameters,
            )
            .unwrap();

        let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 20_i32 });
        let wrong_a = extract_key(&index, &doc! { "_id": 2_i64, "a": 9_i32, "b": 20_i32 });
        let wrong_b = extract_key(&index, &doc! { "_id": 3_i64, "a": 10_i32, "b": 19_i32 });

        assert!(range.contains(&matching));
        assert!(!range.contains(&wrong_a));
        assert!(!range.contains(&wrong_b));
    }

    #[test]
    fn bind_range_expr_compound_prefix_plus_tail_range_contains_expected_keys() {
        let index = make_index(vec![
            ("a", IndexDirection::Ascending),
            ("b", IndexDirection::Descending),
        ]);
        let mut parameters = Parameters::new();
        let prefix = parameters.collect_parameter(10_i32.into());
        let tail = parameters.collect_parameter(5_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![prefix],
                    tail: Some(Interval::less_than(tail)),
                },
                &parameters,
            )
            .unwrap();

        let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 4_i32 });
        let wrong_prefix = extract_key(&index, &doc! { "_id": 2_i64, "a": 9_i32, "b": 4_i32 });
        let wrong_tail = extract_key(&index, &doc! { "_id": 3_i64, "a": 10_i32, "b": 6_i32 });

        assert!(range.contains(&matching));
        assert!(!range.contains(&wrong_prefix));
        assert!(!range.contains(&wrong_tail));
    }

    #[test]
    fn bind_range_expr_tail_inclusive_and_exclusive_bounds_are_respected() {
        let index = make_index(vec![
            ("a", IndexDirection::Ascending),
            ("b", IndexDirection::Ascending),
        ]);
        let mut parameters = Parameters::new();
        let prefix = parameters.collect_parameter(10_i32.into());
        let included = parameters.collect_parameter(5_i32.into());
        let excluded = parameters.collect_parameter(8_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![prefix],
                    tail: Some(Interval::new(
                        std::ops::Bound::Included(included),
                        std::ops::Bound::Excluded(excluded),
                    )),
                },
                &parameters,
            )
            .unwrap();

        let lower_included = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 5_i32 });
        let middle = extract_key(&index, &doc! { "_id": 2_i64, "a": 10_i32, "b": 7_i32 });
        let upper_excluded =
            extract_key(&index, &doc! { "_id": 3_i64, "a": 10_i32, "b": 8_i32 });

        assert!(range.contains(&lower_included));
        assert!(range.contains(&middle));
        assert!(!range.contains(&upper_excluded));
    }

    #[test]
    fn bind_range_expr_descending_single_field_swaps_interval_orientation() {
        let index = make_index(vec![("a", IndexDirection::Descending)]);
        let mut parameters = Parameters::new();
        let lower = parameters.collect_parameter(5_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![],
                    tail: Some(Interval::greater_than(lower)),
                },
                &parameters,
            )
            .unwrap();

        let above = extract_key(&index, &doc! { "_id": 1_i64, "a": 6_i32 });
        let boundary = extract_key(&index, &doc! { "_id": 2_i64, "a": 5_i32 });
        let below = extract_key(&index, &doc! { "_id": 3_i64, "a": 4_i32 });

        assert!(range.contains(&above));
        assert!(!range.contains(&boundary));
        assert!(!range.contains(&below));
    }

    #[test]
    fn bind_range_expr_unbounded_tail_stays_within_equality_prefix() {
        let index = make_index(vec![
            ("a", IndexDirection::Ascending),
            ("b", IndexDirection::Ascending),
            ("c", IndexDirection::Descending),
        ]);
        let mut parameters = Parameters::new();
        let prefix = parameters.collect_parameter(10_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![prefix],
                    tail: Some(Interval::all()),
                },
                &parameters,
            )
            .unwrap();

        let matching_1 =
            extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 0_i32, "c": 9_i32 });
        let matching_2 =
            extract_key(&index, &doc! { "_id": 2_i64, "a": 10_i32, "b": 99_i32, "c": -1_i32 });
        let wrong_prefix =
            extract_key(&index, &doc! { "_id": 3_i64, "a": 11_i32, "b": 0_i32, "c": 9_i32 });

        assert!(range.contains(&matching_1));
        assert!(range.contains(&matching_2));
        assert!(!range.contains(&wrong_prefix));
    }

    #[test]
    fn bind_range_expr_allows_literal_tail_sentinels() {
        let index = make_index(vec![
            ("a", IndexDirection::Ascending),
            ("b", IndexDirection::Ascending),
        ]);
        let mut parameters = Parameters::new();
        let prefix = parameters.collect_parameter(7_i32.into());

        let range = index
            .bind_range_expr(
                &IndexScanRangeExpr {
                    equal_prefix: vec![prefix],
                    tail: Some(Interval::closed(
                        Arc::new(Expr::Literal(BsonValue(Bson::MinKey))),
                        Arc::new(Expr::Literal(BsonValue(Bson::MaxKey))),
                    )),
                },
                &parameters,
            )
            .unwrap();

        let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 7_i32, "b": 123_i32 });
        let wrong_prefix = extract_key(&index, &doc! { "_id": 2_i64, "a": 8_i32, "b": 123_i32 });

        assert!(range.contains(&matching));
        assert!(!range.contains(&wrong_prefix));
    }

    #[test]
    fn decode_index_entry_returns_error_for_truncated_value() {
        let index = make_index(vec![("name", IndexDirection::Ascending)]);
        let document = doc! { "_id": 7i64, "name": "alice" };
        let key_source = DocumentKeySource::BsonDocument(&document);
        let id_key = Indexes::extract_id_key(&key_source).unwrap();
        let mut entry = index.extract_index_entry(&key_source, &id_key).unwrap();
        entry.value.truncate(4);

        let result = index.decode_index_entry(entry);

        assert!(result.is_err());
    }

    #[test]
    fn decode_index_entry_returns_error_for_invalid_id_key_type_length() {
        let index = make_index(vec![("name", IndexDirection::Ascending)]);
        let entry = IndexKeyValue {
            key: vec![1, 2, 3],
            value: vec![6, 0, 0, 0, 3, 10],
        };

        let result = index.decode_index_entry(entry);

        assert!(result.is_err());
    }
}
