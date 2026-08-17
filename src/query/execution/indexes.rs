use crate::io::byte_reader::ByteReader;
use crate::io::{unexpected_eof, varint};
use crate::query::physical_plan::IndexScanRangeExpr;
use crate::query::{
    get_path_value, get_path_value_from_raw, BsonValue, Expr, Parameters, PathComponent, SortOrder,
};
use crate::storage::catalog::{
    CollectionMetadata, IndexDefinition, IndexDirection, IndexMetadata, OrderedIndexField,
};
use crate::storage::count_stats::CountStatsBuilder;
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
        count_stats: &mut CountStatsBuilder,
    ) -> Result<()> {
        let key_source = &DocumentKeySource::BsonDocument(document);
        let doc_id = Self::extract_id_key(key_source)?;
        for index in self.indices.iter() {
            index.append_put_op(operations, key_source, &doc_id, count_stats)?;
        }
        Ok(())
    }

    pub fn append_put_ops_raw(
        &self,
        operations: &mut Vec<Operation>,
        document: &RawDocument,
        count_stats: &mut CountStatsBuilder,
    ) -> Result<()> {
        let key_source = &DocumentKeySource::RawDocument(document);
        let doc_id = Self::extract_id_key(key_source)?;
        for index in self.indices.iter() {
            index.append_put_op(operations, key_source, &doc_id, count_stats)?;
        }
        Ok(())
    }

    pub fn append_delete_ops(
        &self,
        operations: &mut Vec<Operation>,
        document: &Document,
        count_stats: &mut CountStatsBuilder,
    ) -> Result<()> {
        let key_source = &DocumentKeySource::BsonDocument(document);
        let doc_id = Self::extract_id_key(key_source)?;
        for index in self.indices.iter() {
            index.append_delete_op(operations, key_source, &doc_id, count_stats)?;
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
        count_stats: &mut CountStatsBuilder,
    ) -> Result<()> {
        let IndexKeyValue { key, value } = self.extract_index_entry(key_source, doc_id)?;
        let op = Operation::new_put(self.collection_id, self.id, key, value);
        operations.push(op);
        count_stats.inc_index(self.collection_id, self.id, 1);
        Ok(())
    }

    fn append_delete_op(
        &self,
        operations: &mut Vec<Operation>,
        key_source: &DocumentKeySource,
        doc_id: &TypedKey,
        count_stats: &mut CountStatsBuilder,
    ) -> Result<()> {
        let IndexKeyValue { key, value: _ } = self.extract_index_entry(key_source, doc_id)?;
        let op = Operation::new_delete(self.collection_id, self.id, key);
        operations.push(op);
        count_stats.inc_index(self.collection_id, self.id, -1);
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
        let id_bson =
            decode_bson_from_key_readers(&id_key_bytes, &ByteReader::new(&id_key_type_bytes))?;
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
    append_open_range_suffix(&mut key, remaining_field_codecs, suffix_extremum)?;
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
        _ => unreachable!(
            "Index scan bounds only support placeholders and literals: {:?}",
            expr
        ),
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
mod tests;
