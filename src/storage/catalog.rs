use std::collections::BTreeMap;
use std::sync::Arc;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::manifest_state::SnapshotElement;

/// The `Catalog` maintains the mapping from collection names to their metadata.
///
/// This structure represents the logical schema state of the database.
/// It is persisted in the manifest and provides access to `CollectionMetadata`
/// such as collection IDs and index definitions.
#[derive(Debug, PartialEq)]
pub struct Catalog {
    /// Mapping from collection name to its metadata.
    collections: BTreeMap<String, Arc<CollectionMetadata>>,
}

impl SnapshotElement for Catalog {

    fn read_from(reader: &ByteReader) -> std::io::Result<Self> {
        let size = reader.read_varint_u64()? as usize;
        let mut collections = BTreeMap::new();
        for _ in 0..size {
            let str = reader.read_str()?.to_string();
            let collection = Arc::new(CollectionMetadata::read_from(reader)?);
            collections.insert(str, collection);
        }
        Ok(Catalog { collections })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.collections.len() as u32);
        self.collections.iter().for_each(|(str, col)| {
            writer.write_str(str);
            col.write_to(writer)
        });
    }
}

impl Catalog {
    pub fn new() -> Self {
        Catalog { collections: BTreeMap::new() }
    }

    pub fn get_collection_id(&self, name: &String) -> Option<u32> {
        self.collections.get(name).map(|x| x.id)
    }

    pub fn add_collection(&self, name: &String, id: u32) -> Self {
        let mut collections = self.collections.clone();
        collections.insert(name.clone(), Arc::new(CollectionMetadata::new(id, name.clone())));
        Catalog { collections }
    }

    pub fn drop_collection(&self, name: &String) -> Self {
        let mut collections = self.collections.clone();
        collections.remove(name);
        Catalog { collections }
    }
}

/// Describes a collection's metadata, including its ID, name, and declared indexes.
///
/// This struct is immutable and clone-friendly via `Arc`. It supports serialization
/// to and from the manifest.
#[derive(Debug, PartialEq)]
pub struct CollectionMetadata {
    /// Globally unique collection identifier.
    id: u32,
    /// Collection name.
    name: String,
    /// Mapping from index name to its metadata.
    indexes: Arc<BTreeMap<String, Arc<IndexMetadata>>>,
}

impl CollectionMetadata {
    fn new(id: u32, name: String) -> Self {
        CollectionMetadata { id, name, indexes: Arc::new(BTreeMap::new()) }
    }

    fn read_indexes_from(reader: &ByteReader) -> std::io::Result<Arc<BTreeMap<String, Arc<IndexMetadata>>>> {
        let size = reader.read_varint_u64()? as usize;
        let mut indexes = BTreeMap::new();
        for _ in 0..size {
            let str = reader.read_str()?.to_string();
            let index = Arc::new(IndexMetadata::read_from(reader)?);
            indexes.insert(str, index);
        }
        Ok(Arc::new(indexes))
    }

    fn write_indexes_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.indexes.len() as u32);
        self.indexes.iter().for_each(|(str, idx)| {
            writer.write_str(str);
            idx.write_to(writer)
        });
    }
}

impl SnapshotElement for CollectionMetadata {

    fn read_from(reader: &ByteReader) -> std::io::Result<Self> {
        let id = reader.read_varint_u64()? as u32;
        let name = reader.read_str()?.to_string();
        let indexes = Self::read_indexes_from(reader)?;

        Ok(CollectionMetadata {
            id,
            name,
            indexes,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.id);
        writer.write_str(&self.name);
        self.write_indexes_to(writer);
    }
}

/// Describes a single index within a collection.
#[derive(Debug, PartialEq)]
pub struct IndexMetadata {
    /// Unique identifier for the index.
    id: u32,
    /// Name of the index (e.g., "by_name").
    name: String,
}

impl SnapshotElement for IndexMetadata {
    fn read_from(reader: &ByteReader) -> std::io::Result<Self> {
        let id = reader.read_varint_u32()?;
        let name = reader.read_str()?.to_string();
        Ok(IndexMetadata { id, name })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.id);
        writer.write_str(&self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::manifest_state::check_serialization_round_trip;

    #[test]
    fn test_index_metadata_serialization() {
        check_serialization_round_trip(IndexMetadata { id: 11, name: "by_name".to_string() });
    }

    #[test]
    fn test_collections_metadata_serialization() {
        check_serialization_round_trip(create_collections_with_indexes());
    }

    fn create_collections_with_indexes() -> CollectionMetadata {
        let mut col_products = CollectionMetadata::new(2, "products".to_string());
        {
            let mut indexes = BTreeMap::new();
            indexes.insert(
                "by_name".to_string(),
                Arc::new(IndexMetadata { id: 11, name: "by_name".to_string() })
            );
            indexes.insert(
                "by_price".to_string(),
                Arc::new(IndexMetadata { id: 12, name: "by_price".to_string() })
            );
            // Directly update the indexes field (accessible within the module)
            col_products.indexes = Arc::new(indexes);
        }
        col_products
    }
}