use super::{EncodedIndexKeyPrefix, IndexKeyValue, sort_order, to_typed_key, validated_index_path};
use crate::io::varint;
use crate::query::{BsonValue, PathComponent, SortOrder};
use crate::storage::catalog::{
    CollectionMetadata, IndexDefinition, IndexMetadata, OrderedIndexField,
};
use crate::storage::count_stats::CountStatsBuilder;
use crate::storage::operation::Operation;
use crate::util::bson_utils::TypedKey;
use bson::{Bson, Document, RawBsonRef, RawDocument};
use std::collections::BTreeMap;
use std::io::Result;

/// Borrowed BSON input used while traversing an index path.
#[derive(Clone, Copy)]
pub(crate) enum IndexSource<'a> {
    Document(&'a Document),
    Bson(&'a Bson),
    Raw(RawBsonRef<'a>),
}

impl<'a> IndexSource<'a> {
    pub(super) fn from_document(document: &'a Document) -> Self {
        Self::Document(document)
    }

    pub(super) fn from_raw_document(document: &'a RawDocument) -> Self {
        Self::Raw(RawBsonRef::Document(document))
    }

    fn get_field(self, name: &str) -> Option<Self> {
        match self {
            Self::Document(document) => document.get(name).map(Self::Bson),
            Self::Bson(Bson::Document(document)) => document.get(name).map(Self::Bson),
            Self::Raw(RawBsonRef::Document(document)) => {
                document.get(name).ok().flatten().map(Self::Raw)
            }
            _ => None,
        }
    }

    fn array_iter(self) -> Option<IndexArrayIter<'a>> {
        match self {
            Self::Bson(Bson::Array(values)) => Some(IndexArrayIter::Bson(values.iter())),
            Self::Raw(RawBsonRef::Array(values)) => Some(IndexArrayIter::Raw(values.into_iter())),
            _ => None,
        }
    }

    fn into_bson(self) -> Option<Bson> {
        match self {
            Self::Document(_) => unreachable!("root documents are not index key values"),
            Self::Bson(value) => Some(value.clone()),
            Self::Raw(value) => Bson::try_from(value).ok(),
        }
    }
}

enum IndexArrayIter<'a> {
    Bson(std::slice::Iter<'a, Bson>),
    Raw(bson::raw::RawArrayIter<'a>),
}

impl<'a> Iterator for IndexArrayIter<'a> {
    type Item = crate::error::Result<IndexSource<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Bson(iter) => iter.next().map(|value| Ok(IndexSource::Bson(value))),
            Self::Raw(iter) => iter
                .next()
                .map(|value| value.map(IndexSource::Raw).map_err(Into::into)),
        }
    }
}

#[derive(Default)]
pub(crate) struct OperationsCountAndSize {
    pub(crate) count: usize,
    pub(crate) size_in_bytes: usize,
}

impl OperationsCountAndSize {
    fn add(&mut self, other: Self) {
        self.count += other.count;
        self.size_in_bytes += other.size_in_bytes;
    }

    fn add_operation(&mut self, size_bytes: usize) {
        self.count += 1;
        self.size_in_bytes += size_bytes;
    }
}

pub(crate) struct Indexes {
    indices: Vec<Index>,
}

impl Indexes {
    pub(crate) fn for_index(collection_id: u32, index: &IndexMetadata) -> Self {
        Self {
            indices: vec![Index::from(collection_id, index)],
        }
    }

    pub(crate) fn from_collection(collection: &CollectionMetadata) -> Self {
        // Keep all active indexes here, including indexes that are still being
        // built. The background builder relies on ordinary writes maintaining
        // those indexes while query planning uses queryable_indexes().
        let indices = collection
            .active_indexes()
            .iter()
            .map(|index| Index::from(collection.id, index))
            .collect();
        Self { indices }
    }

    pub(crate) fn append_put_ops(
        &self,
        operations: &mut Vec<Operation>,
        document: &Document,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        self.append_put_ops_from_source(
            operations,
            IndexSource::from_document(document),
            count_stats,
        )
    }

    pub(crate) fn append_put_ops_raw(
        &self,
        operations: &mut Vec<Operation>,
        document: &RawDocument,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        self.append_put_ops_from_source(
            operations,
            IndexSource::from_raw_document(document),
            count_stats,
        )
    }

    fn append_put_ops_from_source(
        &self,
        operations: &mut Vec<Operation>,
        source: IndexSource<'_>,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        let doc_id = Self::extract_id_key(source)?;
        let mut result = OperationsCountAndSize::default();
        for index in &self.indices {
            result.add(index.append_put_ops(operations, source, &doc_id, count_stats)?);
        }
        Ok(result)
    }

    pub(crate) fn append_delete_ops(
        &self,
        operations: &mut Vec<Operation>,
        document: &Document,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        let source = IndexSource::from_document(document);
        let doc_id = Self::extract_id_key(source)?;
        let mut result = OperationsCountAndSize::default();
        for index in &self.indices {
            result.add(index.append_delete_ops(operations, source, &doc_id, count_stats)?);
        }
        Ok(result)
    }

    pub(crate) fn append_update_ops(
        &self,
        operations: &mut Vec<Operation>,
        old_document: &Document,
        new_document: &Document,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        let old_source = IndexSource::from_document(old_document);
        let new_source = IndexSource::from_document(new_document);
        let old_doc_id = Self::extract_id_key(old_source)?;
        let new_doc_id = Self::extract_id_key(new_source)?;
        let mut result = OperationsCountAndSize::default();

        for index in &self.indices {
            result.add(index.append_update_ops(
                operations,
                old_source,
                new_source,
                &old_doc_id,
                &new_doc_id,
                count_stats,
            )?);
        }

        Ok(result)
    }

    fn extract_id_key(source: IndexSource<'_>) -> Result<TypedKey> {
        encode_source_value(source.get_field("_id"), &SortOrder::Ascending)
    }
}

pub(crate) struct Index {
    collection_id: u32,
    id: u32,
    extraction: IndexCodecTree,
}

impl Index {
    pub(crate) fn from(collection_id: u32, index: &IndexMetadata) -> Self {
        let id = index.id;
        match &index.definition {
            IndexDefinition::Regular(fields) => {
                let mut extraction = IndexCodecTree::default();
                for (position, field) in fields.iter().enumerate() {
                    extraction.insert_field(field, position);
                }
                Self {
                    collection_id,
                    id,
                    extraction,
                }
            }
        }
    }

    fn append_put_ops(
        &self,
        operations: &mut Vec<Operation>,
        source: IndexSource<'_>,
        doc_id: &TypedKey,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        let entries = self.generate_entries(source, doc_id)?;
        let mut result = OperationsCountAndSize::default();
        for IndexKeyValue { key, value } in entries {
            self.append_put_op(operations, key, value, &mut result, count_stats);
        }
        Ok(result)
    }

    fn append_delete_ops(
        &self,
        operations: &mut Vec<Operation>,
        source: IndexSource<'_>,
        doc_id: &TypedKey,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        let entries = self.generate_entries(source, doc_id)?;
        let mut result = OperationsCountAndSize::default();
        for IndexKeyValue { key, value: _ } in entries {
            self.append_delete_op(operations, key, &mut result, count_stats);
        }
        Ok(result)
    }

    fn append_put_op(
        &self,
        operations: &mut Vec<Operation>,
        key: Vec<u8>,
        value: Vec<u8>,
        result: &mut OperationsCountAndSize,
        count_stats: &mut CountStatsBuilder,
    ) {
        let operation = Operation::new_put(self.collection_id, self.id, key, value);
        result.add_operation(operation.payload_size_bytes());
        operations.push(operation);
        count_stats.inc_index(self.collection_id, self.id, 1);
    }

    fn append_delete_op(
        &self,
        operations: &mut Vec<Operation>,
        key: Vec<u8>,
        result: &mut OperationsCountAndSize,
        count_stats: &mut CountStatsBuilder,
    ) {
        let operation = Operation::new_delete(self.collection_id, self.id, key);
        result.add_operation(operation.payload_size_bytes());
        operations.push(operation);
        count_stats.inc_index(self.collection_id, self.id, -1);
    }

    fn append_update_ops(
        &self,
        operations: &mut Vec<Operation>,
        old_source: IndexSource<'_>,
        new_source: IndexSource<'_>,
        old_doc_id: &TypedKey,
        new_doc_id: &TypedKey,
        count_stats: &mut CountStatsBuilder,
    ) -> crate::error::Result<OperationsCountAndSize> {
        let old_entries = self.generate_entries(old_source, old_doc_id)?;
        let mut new_entries = self
            .generate_entries(new_source, new_doc_id)?
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<BTreeMap<_, _>>();
        let mut result = OperationsCountAndSize::default();

        for IndexKeyValue {
            key,
            value: old_value,
        } in old_entries
        {
            let Some(new_value) = new_entries.remove(&key) else {
                self.append_delete_op(operations, key, &mut result, count_stats);
                continue;
            };

            if new_value == old_value {
                continue;
            }

            // Delete first so a changed entry with the same key is replaced by
            // the new value at the same storage sequence number.
            self.append_delete_op(operations, key.clone(), &mut result, count_stats);
            self.append_put_op(operations, key, new_value, &mut result, count_stats);
        }

        for (key, value) in new_entries {
            self.append_put_op(operations, key, value, &mut result, count_stats);
        }

        Ok(result)
    }

    pub(super) fn generate_entries(
        &self,
        source: IndexSource<'_>,
        doc_id: &TypedKey,
    ) -> crate::error::Result<Vec<IndexKeyValue>> {
        let expansion = self.extraction.evaluate(Some(source), &[])?;
        let mut entries = BTreeMap::new();
        for mut assignments in expansion.rows {
            assignments.sort_unstable_by_key(|(position, _)| *position);
            let parts = assignments
                .into_iter()
                .enumerate()
                .map(|(expected_position, (position, part))| {
                    assert_eq!(
                        position, expected_position,
                        "index tree must assign every index field exactly once"
                    );
                    part
                })
                .collect();
            let prefix = EncodedIndexKeyPrefix::from_parts(parts);
            let key = self.encode_full_index_key(&prefix, doc_id);
            entries
                .entry(key)
                .or_insert_with(|| self.encode_index_entry_value(&prefix, doc_id));
        }
        Ok(entries
            .into_iter()
            .map(|(key, value)| IndexKeyValue { key, value })
            .collect())
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
        // [_id_len varint][_id_key_type_len varint][_id_key_type bytes]
        //     ([key_len varint][key_type bytes])*
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
        varint::write_u32(doc_id.key.len() as u32, &mut value);
        varint::write_u32(doc_id.key_type.len() as u32, &mut value);
        value.extend_from_slice(&doc_id.key_type);
        encoded_prefix.write_key_metadata_to(&mut value);

        value
    }
}

/// Shared prefixes determine traversal; leaf positions determine compound-key order.
enum IndexCodecTree {
    Internal(BTreeMap<String, IndexCodecTree>),
    Leaf { order: SortOrder, position: usize },
}

impl Default for IndexCodecTree {
    fn default() -> Self {
        Self::Internal(BTreeMap::new())
    }
}

impl IndexCodecTree {
    fn insert_field(&mut self, field: &OrderedIndexField, position: usize) {
        let path = validated_index_path(&field.path);
        self.insert(&path, sort_order(&field.direction), position);
    }

    fn insert(&mut self, path: &[PathComponent], order: SortOrder, position: usize) {
        let head = &path[0];
        let tail = &path[1..];
        let name = head.expect_field_name();
        let Self::Internal(children) = self else {
            panic!("index paths must not overlap");
        };

        if tail.is_empty() {
            assert!(
                children
                    .insert(name.to_owned(), Self::Leaf { order, position })
                    .is_none(),
                "duplicate index path"
            );
        } else {
            children
                .entry(name.to_owned())
                .or_default()
                .insert(tail, order, position);
        }
    }

    fn first_field(&self, path: &[PathComponent]) -> IndexArrayField {
        match self {
            Self::Leaf { position, .. } => IndexArrayField {
                position: *position,
                path: path.to_vec(),
            },
            Self::Internal(children) => children
                .iter()
                .map(|(name, child)| {
                    let mut child_path = path.to_vec();
                    child_path.push(name.as_str().into());
                    child.first_field(&child_path)
                })
                .min_by_key(|field| field.position)
                .unwrap(),
        }
    }

    fn evaluate<'a>(
        &self,
        value: Option<IndexSource<'a>>,
        path: &[PathComponent],
    ) -> crate::error::Result<IndexExpansion> {
        match self {
            Self::Leaf { order, position } => {
                // A terminal array expands once; its array-valued elements
                // remain whole key values. An empty array is itself a key value.
                if let Some(array) = value.and_then(IndexSource::array_iter) {
                    let mut rows = Vec::new();
                    for element in array {
                        rows.push(vec![(
                            *position,
                            encode_source_value(Some(element?), order)?,
                        )]);
                    }
                    if rows.is_empty() {
                        rows.push(vec![(*position, encode_source_value(value, order)?)]);
                    }
                    Ok(IndexExpansion {
                        rows,
                        array_field: Some(IndexArrayField {
                            position: *position,
                            path: path.to_vec(),
                        }),
                    })
                } else {
                    Ok(IndexExpansion {
                        rows: vec![vec![(*position, encode_source_value(value, order)?)]],
                        array_field: None,
                    })
                }
            }
            Self::Internal(children) => {
                if let Some(array) = value.and_then(IndexSource::array_iter) {
                    let array_field = self.first_field(path);
                    let mut rows = Vec::new();
                    for element in array {
                        rows.extend(self.evaluate(Some(element?), path)?.rows);
                    }
                    // An empty intermediate array supplies missing descendants.
                    if rows.is_empty() {
                        rows = self.evaluate(None, path)?.rows;
                    }
                    return Ok(IndexExpansion {
                        rows,
                        array_field: Some(array_field),
                    });
                }
                let mut result = IndexExpansion {
                    rows: vec![Vec::new()],
                    array_field: None,
                };
                for (name, child) in children {
                    let mut child_path = path.to_vec();
                    child_path.push(name.as_str().into());
                    let child = child
                        .evaluate(value.and_then(|value| value.get_field(name)), &child_path)?;
                    result.combine(child)?;
                }
                Ok(result)
            }
        }
    }
}

/// Each row assigns values to this subtree's compound-key positions.
/// Array provenance is explicit: even an empty or singleton array counts.
struct IndexExpansion {
    rows: Vec<Vec<(usize, TypedKey)>>,
    array_field: Option<IndexArrayField>,
}

struct IndexArrayField {
    position: usize,
    path: Vec<PathComponent>,
}

fn encode_source_value(value: Option<IndexSource<'_>>, order: &SortOrder) -> Result<TypedKey> {
    let value = BsonValue(value.and_then(IndexSource::into_bson).unwrap_or(Bson::Null));
    to_typed_key(&value, order)
}

impl IndexExpansion {
    fn combine(&mut self, other: Self) -> crate::error::Result<()> {
        if let (Some(left), Some(right)) = (&self.array_field, &other.array_field) {
            let (first, second) = if left.position < right.position {
                (left, right)
            } else {
                (right, left)
            };
            return Err(crate::error::Error::InvalidRequest(format!(
                "Cannot index parallel arrays for paths '{}' and '{}'",
                crate::query::format_path(&first.path),
                crate::query::format_path(&second.path)
            )));
        }
        if other.array_field.is_some() {
            // The accumulated siblings are scalar: broadcast them to each row.
            let scalar = self.rows.pop().unwrap();
            self.rows = other.rows;
            for row in &mut self.rows {
                row.extend_from_slice(&scalar);
            }
            self.array_field = other.array_field;
        } else {
            let scalar = &other.rows[0];
            for row in &mut self.rows {
                row.extend_from_slice(scalar);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_utils::{make_index_metadata, to_raw, to_typed_key};
    use super::{Index, IndexSource, Indexes};
    use crate::io::byte_writer::ByteWriter;
    use crate::storage::catalog::{
        CollectionMetadata, CollectionOptions, IndexDefinition, IndexDirection, IndexMetadata,
        IndexPath, OrderedIndexField,
    };
    use crate::storage::count_stats::{CountStatsBuilder, CountStatsKey};
    use crate::storage::operation::OperationType;
    use crate::util::bson_utils::BsonKey;
    use bson::{Bson, doc};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn expected_index_entry(index_value: Bson, id: Bson) -> (Vec<u8>, Vec<u8>) {
        let index_key = index_value.try_into_typed_key().unwrap();
        let id_key = to_typed_key(id);

        let mut key = index_key.key.clone();
        key.extend_from_slice(&id_key.key);

        let mut value_writer = ByteWriter::new();
        value_writer
            .write_varint_u32(id_key.key.len() as u32)
            .write_length_prefixed_slice(&id_key.key_type)
            .write_varint_u32(index_key.key.len() as u32);
        let mut value = value_writer.take_buffer();
        value.extend_from_slice(&index_key.key_type);

        (key, value)
    }

    #[test]
    fn tree_extraction_rejects_parallel_arrays_at_different_depths() {
        let index = make_index(vec![
            ("z.parts.x", IndexDirection::Ascending),
            ("a", IndexDirection::Ascending),
        ]);
        for document in [
            doc! {"_id": 1, "z": {"parts": [{"x": 1}]}, "a": []},
            doc! {"_id": 1, "z": [{"parts": []}], "a": [2]},
        ] {
            let id = to_typed_key(1);
            for source in [
                IndexSource::from_document(&document),
                IndexSource::from_raw_document(&to_raw(&document)),
            ] {
                let error = index.generate_entries(source, &id).err().unwrap();
                assert_eq!(
                    error.to_string(),
                    "Cannot index parallel arrays for paths 'z.parts.x' and 'a'"
                );
            }
        }
    }

    #[test]
    fn generated_entries_preserve_empty_nested_and_numeric_semantics() {
        let index = make_index(vec![("a", IndexDirection::Ascending)]);
        for (value, expected) in [
            (bson::bson!([]), 1),
            (bson::bson!([[1, 2], [3]]), 2),
            (bson::bson!([5, 5_i64, 5.0]), 1),
        ] {
            let document = doc! {"_id": 1, "a": value};
            let id = to_typed_key(1);
            let source = IndexSource::from_document(&document);
            assert_eq!(index.generate_entries(source, &id).unwrap().len(), expected);
        }
    }

    #[test]
    fn independent_arrays_rejected_in_raw_and_decoded_documents() {
        let indexes = Indexes {
            indices: vec![make_index(vec![
                ("a", IndexDirection::Ascending),
                ("b", IndexDirection::Ascending),
            ])],
        };
        for document in [
            doc! {"_id": 1, "a": [1], "b": [2]},
            doc! {"_id": 1, "a": [], "b": []},
        ] {
            let raw = to_raw(&document);
            let mut operations = Vec::new();
            let mut stats = CountStatsBuilder::new();
            let decoded_error = indexes
                .append_put_ops(&mut operations, &document, &mut stats)
                .err()
                .unwrap();
            let raw_error = indexes
                .append_put_ops_raw(&mut operations, &raw, &mut stats)
                .err()
                .unwrap();
            assert_eq!(
                decoded_error.to_string(),
                "Cannot index parallel arrays for paths 'a' and 'b'"
            );
            assert_eq!(decoded_error.to_string(), raw_error.to_string());
            assert!(operations.is_empty());
        }
    }

    #[test]
    fn parallel_arrays_fail_without_appending_operations() {
        let indexes = Indexes {
            indices: vec![make_index(vec![
                ("items.x", IndexDirection::Ascending),
                ("items.y", IndexDirection::Ascending),
            ])],
        };
        let document = doc! {"_id": 1, "items": [{"x": [1, 2], "y": [3, 4]}]};
        let mut operations = Vec::new();
        let error = indexes
            .append_put_ops(&mut operations, &document, &mut CountStatsBuilder::new())
            .err()
            .unwrap();
        assert!(matches!(error, crate::error::Error::InvalidRequest(_)));
        assert_eq!(
            error.to_string(),
            "Cannot index parallel arrays for paths 'items.x' and 'items.y'"
        );
        assert!(operations.is_empty());
    }

    #[test]
    fn multikey_raw_put_and_delete_counts_agree() {
        let indexes = Indexes {
            indices: vec![make_index(vec![("a", IndexDirection::Ascending)])],
        };
        let document = doc! {"_id": 1, "a": [1, 2, 2]};
        let raw = to_raw(&document);
        let mut decoded_ops = Vec::new();
        let mut raw_ops = Vec::new();
        let mut deletes = Vec::new();
        let decoded = indexes
            .append_put_ops(&mut decoded_ops, &document, &mut CountStatsBuilder::new())
            .unwrap();
        let raw_result = indexes
            .append_put_ops_raw(&mut raw_ops, &raw, &mut CountStatsBuilder::new())
            .unwrap();
        let deleted = indexes
            .append_delete_ops(&mut deletes, &document, &mut CountStatsBuilder::new())
            .unwrap();
        assert_eq!(decoded.count, 2);
        assert_eq!(raw_result.count, 2);
        assert_eq!(deleted.count, 2);
        assert_eq!(decoded.size_in_bytes, raw_result.size_in_bytes);
        assert_eq!(decoded_ops, raw_ops);
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
            let ks = IndexSource::from_document(&doc_lo);
            let id_key = Indexes::extract_id_key(ks).unwrap();
            asc_index
                .generate_entries(ks, &id_key)
                .unwrap()
                .remove(0)
                .key
        };
        let asc_key_hi = {
            let ks = IndexSource::from_document(&doc_hi);
            let id_key = Indexes::extract_id_key(ks).unwrap();
            asc_index
                .generate_entries(ks, &id_key)
                .unwrap()
                .remove(0)
                .key
        };
        let desc_key_lo = {
            let ks = IndexSource::from_document(&doc_lo);
            let id_key = Indexes::extract_id_key(ks).unwrap();
            desc_index
                .generate_entries(ks, &id_key)
                .unwrap()
                .remove(0)
                .key
        };
        let desc_key_hi = {
            let ks = IndexSource::from_document(&doc_hi);
            let id_key = Indexes::extract_id_key(ks).unwrap();
            desc_index
                .generate_entries(ks, &id_key)
                .unwrap()
                .remove(0)
                .key
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
        let mut count_stats = CountStatsBuilder::new();

        indices
            .append_put_ops(&mut operations, &document, &mut count_stats)
            .unwrap();

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].operation_type, OperationType::Put);
        assert_eq!(operations[0].collection, 42);
        assert_eq!(operations[0].index, 1);
        assert!(!operations[0].value().is_empty());
        let count_stats = count_stats.build();
        assert_eq!(
            count_stats.count_stat(&CountStatsKey::Index {
                collection: 42,
                index: 1,
            }),
            Some(1)
        );
        assert_eq!(
            count_stats.count_stat(&CountStatsKey::Index {
                collection: 42,
                index: 2,
            }),
            None
        );
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
        let mut count_stats = CountStatsBuilder::new();

        indices
            .append_delete_ops(&mut operations, &document, &mut count_stats)
            .unwrap();

        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].operation_type, OperationType::Delete);
        assert_eq!(operations[0].collection, 42);
        assert_eq!(operations[0].index, 1);
        assert!(operations[0].value().is_empty());
        let count_stats = count_stats.build();
        assert_eq!(
            count_stats.count_stat(&CountStatsKey::Index {
                collection: 42,
                index: 1,
            }),
            Some(-1)
        );
    }

    #[test]
    fn append_update_ops_skips_unchanged_entries() {
        let collection = make_collection(vec![make_index_metadata(
            1,
            vec![("name", IndexDirection::Ascending)],
        )]);
        let indices = Indexes::from_collection(&collection);
        let old_document = doc! { "_id": 7i64, "name": "alice", "other": 1 };
        let new_document = doc! { "_id": 7i64, "name": "alice", "other": 2 };
        let mut operations = Vec::new();
        let mut count_stats = CountStatsBuilder::new();

        let result = indices
            .append_update_ops(
                &mut operations,
                &old_document,
                &new_document,
                &mut count_stats,
            )
            .unwrap();

        assert_eq!(result.count, 0);
        assert_eq!(result.size_in_bytes, 0);
        assert!(operations.is_empty());
        assert_eq!(count_stats.build(), Default::default());
    }

    #[test]
    fn append_update_ops_diffs_multikey_entries() {
        let collection = make_collection(vec![make_index_metadata(
            1,
            vec![("tags", IndexDirection::Ascending)],
        )]);
        let indices = Indexes::from_collection(&collection);
        let old_document = doc! { "_id": 7i64, "tags": [1, 2] };
        let new_document = doc! { "_id": 7i64, "tags": [2, 3] };
        let mut operations = Vec::new();
        let mut count_stats = CountStatsBuilder::new();

        let result = indices
            .append_update_ops(
                &mut operations,
                &old_document,
                &new_document,
                &mut count_stats,
            )
            .unwrap();

        let (removed_key, _) = expected_index_entry(Bson::Int32(1), Bson::Int64(7));
        let (added_key, added_value) = expected_index_entry(Bson::Int32(3), Bson::Int64(7));

        assert_eq!(result.count, 2);
        assert_eq!(operations.len(), 2);
        assert_eq!(
            operations
                .iter()
                .map(|operation| {
                    (
                        operation.operation_type,
                        operation.collection,
                        operation.index,
                        operation.user_key().to_vec(),
                        operation.value().to_vec(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (OperationType::Delete, 42, 1, removed_key, Vec::new(),),
                (OperationType::Put, 42, 1, added_key, added_value),
            ]
        );
        assert_eq!(count_stats.build(), Default::default());
    }

    #[test]
    fn append_update_ops_replaces_value_when_key_is_unchanged() {
        let collection = make_collection(vec![make_index_metadata(
            1,
            vec![("value", IndexDirection::Ascending)],
        )]);
        let indices = Indexes::from_collection(&collection);
        let old_document = doc! { "_id": 7i64, "value": 1_i32 };
        let new_document = doc! { "_id": 7i64, "value": 1_i64 };
        let mut operations = Vec::new();
        let mut count_stats = CountStatsBuilder::new();

        let result = indices
            .append_update_ops(
                &mut operations,
                &old_document,
                &new_document,
                &mut count_stats,
            )
            .unwrap();

        let (old_key, old_value) = expected_index_entry(Bson::Int32(1), Bson::Int64(7));
        let (new_key, new_value) = expected_index_entry(Bson::Int64(1), Bson::Int64(7));
        assert_eq!(old_key, new_key);
        assert_ne!(old_value, new_value);
        assert_eq!(result.count, 2);
        assert_eq!(
            operations
                .iter()
                .map(|operation| (
                    operation.operation_type,
                    operation.user_key().to_vec(),
                    operation.value().to_vec(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (OperationType::Delete, old_key, Vec::new()),
                (OperationType::Put, new_key, new_value),
            ]
        );
        assert_eq!(count_stats.build(), Default::default());
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
        let raw = to_raw(&document);
        let mut bson_ops = Vec::new();
        let mut raw_ops = Vec::new();
        let mut bson_count_stats = CountStatsBuilder::new();
        let mut raw_count_stats = CountStatsBuilder::new();

        indices
            .append_put_ops(&mut bson_ops, &document, &mut bson_count_stats)
            .unwrap();
        indices
            .append_put_ops_raw(&mut raw_ops, raw.as_ref(), &mut raw_count_stats)
            .unwrap();

        assert_eq!(bson_ops.len(), 1);
        assert_eq!(raw_ops.len(), 1);
        assert_eq!(bson_ops[0], raw_ops[0]);
        assert_eq!(bson_count_stats.build(), raw_count_stats.build());
    }

    fn make_index(fields: Vec<(&str, IndexDirection)>) -> Index {
        let metadata = make_index_metadata(1, fields);
        Index::from(42, &metadata)
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
}
