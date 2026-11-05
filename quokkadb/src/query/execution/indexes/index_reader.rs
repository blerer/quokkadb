use super::{EncodedIndexKeyPrefix, IndexKeyValue, sort_order, to_typed_key, validated_index_path};
use crate::io::byte_reader::ByteReader;
use crate::io::unexpected_eof;
use crate::query::QueryEndpoint;
use crate::query::physical_plan::IndexScanRangeExpr;
use crate::query::{BsonValue, Expr, Parameters, PathComponent, SortOrder};
use crate::storage::catalog::{IndexDefinition, IndexMetadata, OrderedIndexField};
use crate::util::bson_utils::{TypedKey, decode_bson_from_key_readers};
use crate::util::interval::Interval;
use bson::{Bson, Document};
use std::collections::Bound;
use std::io::Result;
use std::ops::RangeBounds;
use std::sync::Arc;

struct IndexFieldSpec {
    path: Vec<PathComponent>,
    order: SortOrder,
}

impl IndexFieldSpec {
    fn from(key_part: &OrderedIndexField) -> Self {
        Self {
            path: validated_index_path(&key_part.path),
            order: sort_order(&key_part.direction),
        }
    }
}

/// Builds scan bounds and decodes persisted entries for an index without
/// building the write-side path tree.
pub(crate) struct IndexReadCodec {
    fields: Vec<IndexFieldSpec>,
}

impl IndexReadCodec {
    pub(crate) fn from(index: &IndexMetadata) -> Self {
        let IndexDefinition::Regular(fields) = &index.definition;
        Self {
            fields: fields.iter().map(IndexFieldSpec::from).collect(),
        }
    }

    pub(crate) fn bind_range_expr(
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
            .zip(self.fields.iter())
            .map(|(expr, field)| {
                let value = resolve_bound_expr(expr, parameters);
                to_typed_key(&value, &field.order)
            })
            .collect::<Result<Vec<_>>>()?;
        let equal_prefix = EncodedIndexKeyPrefix::from_parts(equal_prefix);

        let remaining_fields = &self.fields[prefix_len..];
        match &range_expr.tail {
            Some(interval) => {
                assert!(
                    prefix_len < self.fields.len(),
                    "Index range tail requires an index field after the equality prefix"
                );
                let start_bound = match remaining_fields[0].order {
                    SortOrder::Ascending => interval.start_bound(),
                    SortOrder::Descending => interval.end_bound(),
                };
                let end_bound = match remaining_fields[0].order {
                    SortOrder::Ascending => interval.end_bound(),
                    SortOrder::Descending => interval.start_bound(),
                };

                let start = bind_tail_bound(
                    &equal_prefix,
                    remaining_fields,
                    start_bound,
                    BoundSide::Start,
                    parameters,
                )?;
                let end = bind_tail_bound(
                    &equal_prefix,
                    remaining_fields,
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
                    remaining_fields,
                    SuffixExtremum::Low,
                )?);
                let end = Bound::Included(encode_index_range_key(
                    &equal_prefix,
                    None,
                    remaining_fields,
                    SuffixExtremum::High,
                )?);
                Ok(Interval::new(start, end))
            }
        }
    }
}

impl IndexReadCodec {
    #[allow(dead_code)]
    pub(crate) fn decode(&self, index_key_value: IndexKeyValue) -> Result<Document> {
        let key = &index_key_value.key;
        let value = &index_key_value.value;

        if value.len() < 2 {
            return Err(unexpected_eof("index entry value is truncated"));
        }

        // Parse the value buffer.
        // Layout: [_id_len varint][_id_key_type_len varint][_id_key_type bytes]
        //         ([key_len varint][key_type bytes])*
        let value_reader = ByteReader::new(value.as_slice());

        let id_len = value_reader.read_varint_u32()? as usize;
        let id_key_type_len = value_reader.read_varint_u32()? as usize;
        let id_key_type_bytes = value_reader.read_fixed_slice(id_key_type_len)?.to_vec();

        // The _id bytes are appended at the end of the key.
        if key.len() < id_len {
            return Err(unexpected_eof(
                "index entry key is shorter than stored _id length",
            ));
        }
        let parts_key_len = key.len() - id_len;
        let id_key_bytes = &key[parts_key_len..];

        let mut key_offset = 0usize;
        let mut doc = Document::new();

        for field in &self.fields {
            let part_key_len = value_reader.read_varint_u32()? as usize;
            if key_offset + part_key_len > parts_key_len {
                return Err(unexpected_eof("index entry key is truncated"));
            }
            // Slice the raw key bytes for this part, un-inverting if descending.
            let raw_part_key = &key[key_offset..key_offset + part_key_len];
            key_offset += part_key_len;
            let mut part_key = raw_part_key.to_vec();
            if matches!(field.order, SortOrder::Descending) {
                for byte in &mut part_key {
                    *byte = !*byte;
                }
            }

            // Drive decode_key_inner jointly over the key and value (type) readers.
            // This advances value_reader by exactly the key_type bytes for this field,
            // which are self-delimiting and require no explicit length prefix.
            let key_reader = ByteReader::new(part_key.as_slice());
            let bson_value = decode_bson_from_key_readers(&key_reader, &value_reader)?;

            // Use set_path_value so that nested paths (e.g. "address.city") are
            // reconstructed as proper nested Documents rather than flat dot-notation keys.
            crate::query::execution::set_path_value(&mut doc, &field.path, bson_value);
        }

        // Decode the _id from the key suffix using the stored key_type bytes.
        let id_key_bytes = ByteReader::new(id_key_bytes);
        let id_bson =
            decode_bson_from_key_readers(&id_key_bytes, &ByteReader::new(&id_key_type_bytes))?;
        doc.insert("_id", id_bson);

        Ok(doc)
    }
}

#[derive(Clone, Copy)]
enum SuffixExtremum {
    Low,
    High,
}

fn bind_tail_bound(
    equal_prefix: &EncodedIndexKeyPrefix,
    remaining_fields: &[IndexFieldSpec],
    bound: Bound<&QueryEndpoint<Arc<Expr>>>,
    side: BoundSide,
    parameters: &Parameters,
) -> Result<Bound<Vec<u8>>> {
    let tail_order = &remaining_fields[0].order;
    let after_tail_fields = &remaining_fields[1..];
    let is_start = matches!(side, BoundSide::Start);
    let (tail, suffix_fields, inclusive, suffix_extremum) = match bound {
        Bound::Included(expr) => {
            let value = resolve_bound_expr(expr.value(), parameters);
            let tail = to_typed_key(&value, tail_order)?;
            let suffix_extremum = if is_start {
                SuffixExtremum::Low
            } else {
                SuffixExtremum::High
            };
            (Some(tail), after_tail_fields, true, suffix_extremum)
        }
        Bound::Excluded(expr) => {
            let value = resolve_bound_expr(expr.value(), parameters);
            let tail = to_typed_key(&value, tail_order)?;
            let suffix_extremum = if is_start {
                SuffixExtremum::High
            } else {
                SuffixExtremum::Low
            };
            (Some(tail), after_tail_fields, false, suffix_extremum)
        }
        Bound::Unbounded => {
            let suffix_extremum = if is_start {
                SuffixExtremum::Low
            } else {
                SuffixExtremum::High
            };
            (None, remaining_fields, true, suffix_extremum)
        }
    };
    let key = encode_index_range_key(equal_prefix, tail.as_ref(), suffix_fields, suffix_extremum)?;
    if inclusive {
        Ok(Bound::Included(key))
    } else {
        Ok(Bound::Excluded(key))
    }
}

fn encode_index_range_key(
    equal_prefix: &EncodedIndexKeyPrefix,
    tail: Option<&TypedKey>,
    remaining_fields: &[IndexFieldSpec],
    suffix_extremum: SuffixExtremum,
) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    equal_prefix.write_key_prefix_to(&mut key);
    if let Some(tail) = tail {
        key.extend_from_slice(&tail.key);
    }
    append_open_range_suffix(&mut key, remaining_fields, suffix_extremum)?;
    Ok(key)
}

fn append_open_range_suffix(
    out: &mut Vec<u8>,
    remaining_fields: &[IndexFieldSpec],
    suffix_extremum: SuffixExtremum,
) -> Result<()> {
    for field in remaining_fields {
        let sentinel = sentinel_for_order(&field.order, suffix_extremum);
        let encoded = to_typed_key(&sentinel, &field.order)?;
        out.extend_from_slice(&encoded.key);
    }

    let id_sentinel = sentinel_for_order(&SortOrder::Ascending, suffix_extremum);
    let id_key = to_typed_key(&id_sentinel, &SortOrder::Ascending)?;
    out.extend_from_slice(&id_key.key);
    Ok(())
}

#[derive(Clone, Copy)]
enum BoundSide {
    Start,
    End,
}

fn resolve_bound_expr(expr: &Arc<Expr>, parameters: &Parameters) -> BsonValue {
    match expr.as_ref() {
        Expr::Placeholder { index: idx, .. } => parameters.get(*idx).clone(),
        Expr::Literal(value) => value.clone(),
        _ => unreachable!(
            "Index scan bounds only support placeholders and literals: {:?}",
            expr
        ),
    }
}

fn sentinel_for_order(order: &SortOrder, extremum: SuffixExtremum) -> BsonValue {
    match (order, extremum) {
        (SortOrder::Ascending, SuffixExtremum::Low) => BsonValue(Bson::MinKey),
        (SortOrder::Ascending, SuffixExtremum::High) => BsonValue(Bson::MaxKey),
        (SortOrder::Descending, SuffixExtremum::Low) => BsonValue(Bson::MaxKey),
        (SortOrder::Descending, SuffixExtremum::High) => BsonValue(Bson::MinKey),
    }
}

#[cfg(test)]
mod tests;
