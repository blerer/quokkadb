use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::io::serializable::Serializable;

/// The `Catalog` maintains the mapping from collection names to their metadata.
///
/// This structure represents the logical schema state of the database.
/// It is persisted in the manifest and provides access to `CollectionMetadata`
/// such as collection IDs and index definitions.
#[derive(Debug, PartialEq)]
pub struct Catalog {
    /// The next collection id (the first 10 are reserved for internal collections)
    pub next_collection_id: u32,
    /// Mapping from collection name to its metadata.
    pub collections: BTreeMap<String, Arc<CollectionMetadata>>,
}

impl Serializable for Catalog {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let next_collection_id = reader.read_varint_u32()?;
        let size = reader.read_varint_u64()? as usize;
        let mut collections = BTreeMap::new();
        for _ in 0..size {
            let str = reader.read_str()?.to_string();
            let collection = Arc::new(CollectionMetadata::read_from(reader)?);
            collections.insert(str, collection);
        }
        Ok(Catalog {
            next_collection_id,
            collections,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.next_collection_id);
        writer.write_varint_u32(self.collections.len() as u32);
        self.collections.iter().for_each(|(str, col)| {
            writer.write_str(str);
            col.write_to(writer)
        });
    }
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            next_collection_id: 10,
            collections: BTreeMap::new(),
        }
    }

    pub fn get_collection(&self, name: &str) -> Option<Arc<CollectionMetadata>> {
        self.collections.get(name).cloned()
    }

    pub fn add_collection(&self, name: &str, id: u32) -> Self {
        assert_eq!(self.next_collection_id, id);
        let mut collections = self.collections.clone();
        collections.insert(
            name.to_string(),
            Arc::new(CollectionMetadata::new(id, name)),
        );
        Catalog {
            next_collection_id: id + 1,
            collections,
        }
    }

    pub fn drop_collection(&self, name: &str) -> Self {
        let mut collections = self.collections.clone();
        collections.remove(name);
        Catalog {
            next_collection_id: self.next_collection_id,
            collections,
        }
    }
}

/// Describes a collection's metadata, including its ID, name, and declared indexes.
///
/// This struct is immutable and clone-friendly via `Arc`. It supports serialization
/// to and from the manifest.
#[derive(Debug, PartialEq)]
pub struct CollectionMetadata {
    /// The id of the next index
    pub next_index_id: u32,
    /// Globally unique collection identifier.
    pub id: u32,
    /// Collection name.
    pub name: String,
    /// Mapping from index name to its metadata.
    pub indexes: BTreeMap<String, Arc<IndexMetadata>>,
}

impl CollectionMetadata {
    pub fn new(id: u32, name: &str) -> Self {
        CollectionMetadata {
            next_index_id: 0,
            id,
            name: name.to_string(),
            indexes: BTreeMap::new(),
        }
    }

    fn add_index(&self, index: IndexMetadata) -> CollectionMetadata {
        assert_eq!(self.next_index_id, index.id);
        let next_index_id = self.next_index_id + 1;
        let mut indexes = self.indexes.clone();
        indexes.insert(index.name.to_string(), Arc::new(index));
        CollectionMetadata {
            next_index_id,
            id: self.id,
            name: self.name.clone(),
            indexes,
        }
    }
}

impl Serializable for CollectionMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let next_index_id = reader.read_varint_u32()?;
        let id = reader.read_varint_u64()? as u32;
        let name = reader.read_str()?.to_string();
        let indexes = BTreeMap::<String, Arc<IndexMetadata>>::read_from(reader)?;

        Ok(CollectionMetadata {
            next_index_id,
            id,
            name,
            indexes,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.next_index_id);
        writer.write_varint_u32(self.id);
        writer.write_str(&self.name);
        self.indexes.write_to(writer);
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

impl Serializable for IndexMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
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
    use crate::io::serializable::check_serialization_round_trip;

    #[test]
    fn test_index_metadata_serialization() {
        check_serialization_round_trip(IndexMetadata {
            id: 11,
            name: "by_name".to_string(),
        });
    }

    #[test]
    fn test_collections_metadata_serialization() {
        check_serialization_round_trip(create_collections_with_indexes());
    }

    fn create_collections_with_indexes() -> CollectionMetadata {
        CollectionMetadata::new(2, "products")
            .add_index(IndexMetadata {
                id: 0,
                name: "by_name".to_string(),
            })
            .add_index(IndexMetadata {
                id: 1,
                name: "by_price".to_string(),
            })
    }
}
