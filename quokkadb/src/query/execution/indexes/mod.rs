use crate::io::{unexpected_eof, varint};
use crate::query::{BsonValue, PathComponent, SortOrder};
use crate::storage::catalog::{IndexDirection, IndexPath};
use crate::util::bson_utils::{BsonKey, TypedKey};

pub(crate) mod index_reader;
pub(crate) mod indexes;

#[cfg(test)]
pub(crate) mod test_utils {
    use super::TypedKey;
    use crate::storage::catalog::{
        IndexDefinition, IndexDirection, IndexMetadata, IndexPath, OrderedIndexField,
    };
    use crate::util::bson_utils::BsonKey;
    use bson::{Bson, Document, raw::RawDocumentBuf};

    pub(crate) fn to_typed_key(value: impl Into<Bson>) -> TypedKey {
        value.into().try_into_typed_key().unwrap()
    }

    pub(crate) fn to_raw(document: &Document) -> RawDocumentBuf {
        RawDocumentBuf::from_bytes(document.to_vec().unwrap()).unwrap()
    }

    pub(crate) fn make_index_metadata(
        id: u32,
        fields: Vec<(&str, IndexDirection)>,
    ) -> IndexMetadata {
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
}

pub struct IndexKeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

pub struct EncodedIndexKeyPrefix {
    parts: Vec<TypedKey>,
}

impl EncodedIndexKeyPrefix {
    fn from_parts(parts: Vec<TypedKey>) -> Self {
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

fn validated_index_path(path: &IndexPath) -> Vec<PathComponent> {
    let path: Vec<PathComponent> = path.into();
    assert!(
        !path
            .iter()
            .any(|part| matches!(part, PathComponent::ArrayElement(_))),
        "numeric-position index paths are not supported"
    );
    assert!(
        matches!(path.first(), Some(PathComponent::FieldName(_))),
        "index paths must start with a field name"
    );
    path
}

fn sort_order(direction: &IndexDirection) -> SortOrder {
    match direction {
        IndexDirection::Ascending => SortOrder::Ascending,
        IndexDirection::Descending => SortOrder::Descending,
    }
}

fn to_typed_key(value: &BsonValue, order: &SortOrder) -> std::io::Result<TypedKey> {
    let TypedKey { mut key, key_type } = value.try_into_typed_key()?;
    if matches!(order, SortOrder::Descending) {
        invert_bytes(&mut key);
    }
    Ok(TypedKey { key, key_type })
}

fn invert_bytes(bytes: &mut Vec<u8>) {
    for i in 0..bytes.len() {
        bytes[i] = !bytes[i];
    }
}

pub(crate) fn extract_id_from_entry_bytes<'a>(
    key: &'a [u8],
    value: &[u8],
) -> std::io::Result<&'a [u8]> {
    if value.is_empty() {
        return Err(unexpected_eof("index entry value is truncated"));
    }

    let (id_len, _) = varint::read_u32(value, 0);
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

#[cfg(test)]
mod tests {
    use super::extract_id_from_entry_bytes;

    #[test]
    fn extract_id_from_entry_bytes_rejects_empty_values() {
        assert!(extract_id_from_entry_bytes(&[1], &[]).is_err());
    }
}
