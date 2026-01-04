use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use crate::io::bitset::BitSet;
use crate::io::serializable::Serializable;
use std::io::Result;

/// The `Catalog` maintains the mapping from collection names to their metadata.
///
/// This structure represents the logical schema state of the database.
/// It is persisted in the manifest and provides access to `CollectionMetadata`
/// such as collection IDs and index definitions.
#[derive(Debug, PartialEq)]
pub struct Catalog {
    /// The next collection id (the first 10 are reserved for internal collections)
    pub next_collection_id: u32,
    /// Mapping from collection id to its metadata.
    collections: BTreeMap<u32, Arc<CollectionMetadata>>,
    /// Mapping from id to name
    id_by_name: HashMap<String, u32>,
}

impl Serializable for Catalog {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let next_collection_id = reader.read_varint_u32()?;
        let size = reader.read_varint_u64()? as usize;
        let mut collections = BTreeMap::new();
        let mut id_by_name = HashMap::new();
        for _ in 0..size {
            let id = reader.read_varint_u32()?;
            let collection = Arc::new(CollectionMetadata::read_from(reader)?);
            let name = collection.name.clone();
            collections.insert(id, collection);
            id_by_name.insert(name, id);
        }
        Ok(Catalog {
            next_collection_id,
            collections,
            id_by_name,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.next_collection_id);
        writer.write_varint_u32(self.collections.len() as u32);
        self.collections.iter().for_each(|(id, col)| {
            writer.write_varint_u32(*id);
            col.write_to(writer)
        });
    }
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            next_collection_id: 10,
            collections: BTreeMap::new(),
            id_by_name: HashMap::new(),
        }
    }
    pub fn get_collection_by_name(&self, name: &str) -> Option<Arc<CollectionMetadata>> {
        self.id_by_name
            .get(name)
            .and_then(|id| self.collections.get(id).cloned())
    }

    pub fn get_collection_by_id(&self, id: &u32) -> Option<Arc<CollectionMetadata>> {
        self.collections.get(id).cloned()
    }

    pub fn collection_or_index_exist_at(&self, col: u32, idx: u32, snapshot: u64) -> bool {
        if let Some(collection) = self.get_collection_at(col, snapshot) {
            idx == 0 || collection.get_index_at(idx, snapshot).is_some()
        } else {
            false
        }
    }

    pub fn get_collection_at(&self, id: u32, snapshot: u64) -> Option<Arc<CollectionMetadata>> {
        self.collections.get(&id).and_then(|col| {
            if col.was_created_at(snapshot) && !col.was_dropped_at(snapshot) {
                Some(col.clone())
            } else {
                None
            }
        })
    }

    #[cfg(test)]
    pub fn add_collection(&self, name: &str, id: u32, created_at: u64) -> Self {
        self.add_collection_with_options(
            name,
            id,
            created_at,
            CollectionOptions::default(),
        )
    }

    pub fn add_collection_with_options(
        &self,
        name: &str,
        id: u32,
        created_at: u64,
        options: CollectionOptions
    ) -> Self {
        assert_eq!(self.next_collection_id, id);
        let mut collections = self.collections.clone();
        collections.insert(
            id,
            Arc::new(CollectionMetadata::new(id, name, created_at, options)),
        );
        let mut id_by_name = self.id_by_name.clone();
        id_by_name.insert(name.to_string(), id);
        Catalog {
            next_collection_id: id + 1,
            collections,
            id_by_name,
        }
    }

    pub fn drop_collection(&self, id: u32, dropped_at: u64) -> Self {
        let col = self.collections.get(&id).cloned().unwrap();
        let name = &col.name;
        let mut id_by_name = self.id_by_name.clone();
        let id = id_by_name.remove(name);
        let dropped_collection = Arc::new(CollectionMetadata {
            next_index_id: col.next_index_id,
            id: col.id,
            name: col.name.clone(),
            created_at: col.created_at,
            dropped_at: Some(dropped_at),
            indexes: col.indexes.clone(),
            index_id_by_name: col.index_id_by_name.clone(),
            options: col.options.clone(),
        });

        let mut collections = self.collections.clone();
        collections.insert(id.unwrap(), dropped_collection);

        Catalog {
            next_collection_id: self.next_collection_id,
            collections,
            id_by_name,
        }
    }

    /// Returns an iterator over all non-dropped collections.
    pub fn list_collections(&self) -> impl Iterator<Item = &Arc<CollectionMetadata>> {
        self.collections.values().filter(|c| c.dropped_at.is_none())
    }

    pub fn rename_collection(&self, id: u32, new_name: &str) -> Self {
        let col = self.collections.get(&id).cloned().unwrap();
        let old_name = &col.name;

        let mut id_by_name = self.id_by_name.clone();
        id_by_name.remove(old_name);
        id_by_name.insert(new_name.to_string(), id);

        let renamed_collection = Arc::new(CollectionMetadata {
            next_index_id: col.next_index_id,
            id: col.id,
            name: new_name.to_string(),
            created_at: col.created_at,
            dropped_at: col.dropped_at,
            indexes: col.indexes.clone(),
            index_id_by_name: col.index_id_by_name.clone(),
            options: col.options.clone(),
        });

        let mut collections = self.collections.clone();
        collections.insert(id, renamed_collection);

        Catalog {
            next_collection_id: self.next_collection_id,
            collections,
            id_by_name,
        }
    }
}

/// Strategy for creating collection IDs.
#[derive(Debug, PartialEq, Clone)]
pub enum IdCreationStrategy {
    /// IDs are auto-generated by the system.
    Generated,
    /// IDs are provided manually by the user.
    Manual,
    /// A mix of auto-generated and manual IDs.
    Mixed,
}

impl IdCreationStrategy {
    fn is_default(&self) -> bool {
        matches!(self, IdCreationStrategy::Mixed)
    }
}

impl Default for IdCreationStrategy {
    fn default() -> Self {
        IdCreationStrategy::Mixed
    }
}

impl Serializable for IdCreationStrategy {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            0 => Ok(IdCreationStrategy::Generated),
            1 => Ok(IdCreationStrategy::Manual),
            2 => Ok(IdCreationStrategy::Mixed),
            _ => panic!("Invalid IdCreationStrategy byte: {}", byte),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            IdCreationStrategy::Generated => 0,
            IdCreationStrategy::Manual => 1,
            IdCreationStrategy::Mixed => 2,
        };
        writer.write_u8(byte);
    }
}

/// Options for creating a collection.
#[derive(Debug, PartialEq, Default, Clone)]
pub struct CollectionOptions {
    /// Strategy for creating collection IDs.
    pub id_creation_strategy: IdCreationStrategy,
}

impl CollectionOptions {
    fn create_bitset(&self) -> BitSet {
        let mut bitset = BitSet::new();
        if !self.id_creation_strategy.is_default() {
            bitset.insert(0);
        }
        bitset
    }
}

impl fmt::Display for CollectionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CollectionOptions {{ id_creation_strategy: {:?} }}", self.id_creation_strategy)
    }
}


impl Serializable for CollectionOptions {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let bitset = BitSet::read_from(reader)?;
        Ok(CollectionOptions {
            id_creation_strategy: if bitset.contains(0) {
                IdCreationStrategy::read_from(reader)?
            } else {
                IdCreationStrategy::default()
            },
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        // We want to use a BitSet to indicate which options are set to non-default values.
        // It also allows us to easily add new options in the future.
        let bitset = self.create_bitset();
        bitset.write_to(writer);

        if bitset.contains(0) {
            self.id_creation_strategy.write_to(writer);
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
    /// Timestamp when the collection was created.
    pub created_at: u64,
    /// Timestamp when the collection was dropped (if applicable).
    pub dropped_at: Option<u64>,
    /// Mapping from index id to its metadata.
    pub indexes: BTreeMap<u32, Arc<IndexMetadata>>,
    /// Mapping from index name to id.
    index_id_by_name: HashMap<String, u32>,
    /// Options for the collection.
    pub options: CollectionOptions,
}

impl CollectionMetadata {
    pub fn new(id: u32, name: &str, created_at: u64, options: CollectionOptions) -> Self {
        CollectionMetadata {
            next_index_id: 0,
            id,
            name: name.to_string(),
            created_at,
            dropped_at: None,
            indexes: BTreeMap::new(),
            index_id_by_name: HashMap::new(),
            options,
        }
    }

    fn was_created_at(&self, snapshot: u64) -> bool {
        self.created_at <= snapshot
    }

    fn was_dropped_at(&self, snapshot: u64) -> bool {
        self.dropped_at.map_or(false, |ts| ts <= snapshot)
    }

    pub fn get_index_by_name(&self, name: &str) -> Option<Arc<IndexMetadata>> {
        self.index_id_by_name
            .get(name)
            .and_then(|id| self.indexes.get(id).cloned())
    }

    pub fn get_index_by_id(&self, id: u32) -> Option<Arc<IndexMetadata>> {
        self.indexes.get(&id).cloned()
    }

    fn add_index(&self, index: IndexMetadata) -> CollectionMetadata {
        assert_eq!(self.dropped_at, None);
        assert_eq!(self.next_index_id, index.id);
        let next_index_id = self.next_index_id + 1;
        let mut indexes = self.indexes.clone();
        let mut index_id_by_name = self.index_id_by_name.clone();
        let index_id = index.id;
        let index_name = index.name.clone();
        indexes.insert(index_id, Arc::new(index));
        index_id_by_name.insert(index_name, index_id);
        CollectionMetadata {
            next_index_id,
            id: self.id,
            name: self.name.clone(),
            created_at: self.created_at,
            dropped_at: self.dropped_at,
            indexes,
            index_id_by_name,
            options: self.options.clone(),
        }
    }

    pub fn get_index_at(&self, id: u32, snapshot: u64) -> Option<Arc<IndexMetadata>> {
        self.indexes.get(&id).and_then(|idx| {
            if idx.was_created_at(snapshot) && !idx.was_dropped_at(snapshot) {
                Some(idx.clone())
            } else {
                None
            }
        })
    }
}

impl Serializable for CollectionMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let next_index_id = reader.read_varint_u32()?;
        let id = reader.read_varint_u64()? as u32;
        let name = reader.read_str()?.to_string();
        let created_at = reader.read_varint_u64()?;
        let dropped_at = if reader.read_u8()? == 1 {
            Some(reader.read_varint_u64()?)
        } else {
            None
        };
        let size = reader.read_varint_u64()? as usize;
        let mut indexes = BTreeMap::new();
        let mut index_id_by_name = HashMap::new();
        for _ in 0..size {
            let index_id = reader.read_varint_u32()?;
            let index = Arc::new(IndexMetadata::read_from(reader)?);
            let index_name = index.name.clone();
            indexes.insert(index_id, index);
            index_id_by_name.insert(index_name, index_id);
        }
        let options = CollectionOptions::read_from(reader)?;

        Ok(CollectionMetadata {
            next_index_id,
            id,
            name,
            created_at,
            dropped_at,
            indexes,
            index_id_by_name,
            options,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.next_index_id);
        writer.write_varint_u32(self.id);
        writer.write_str(&self.name);
        writer.write_varint_u64(self.created_at);
        match self.dropped_at {
            Some(ts) => {
                writer.write_u8(1);
                writer.write_varint_u64(ts);
            }
            None => {
                writer.write_u8(0);
            }
        }
        writer.write_varint_u32(self.indexes.len() as u32);
        self.indexes.iter().for_each(|(id, index)| {
            writer.write_varint_u32(*id);
            index.write_to(writer);
        });
        self.options.write_to(writer);
    }
}

/// Specifies the ordering of an index field.
#[derive(Debug, PartialEq)]
pub enum Order {
    Ascending,
    Descending,
}

impl Serializable for Order {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            0 => Ok(Order::Ascending),
            1 => Ok(Order::Descending),
            _ => panic!("Invalid Order byte: {}", byte),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            Order::Ascending => 0,
            Order::Descending => 1,
        };
        writer.write_u8(byte);
    }
}

/// Describes a single index within a collection.
#[derive(Debug, PartialEq)]
pub struct IndexMetadata {
    /// Unique identifier for the index.
    pub id: u32,
    /// Name of the index (e.g., "by_name").
    pub name: String,
    /// List of fields and their orderings that comprise the index.
    pub fields: Vec<(String, Order)>,
    /// Timestamp when the index was created.
    pub created_at: u64,
    /// Timestamp when the index was dropped (if applicable).
    pub dropped_at: Option<u64>,
}

impl IndexMetadata {
    fn was_created_at(&self, snapshot: u64) -> bool {
        self.created_at <= snapshot
    }

    fn was_dropped_at(&self, snapshot: u64) -> bool {
        self.dropped_at.map_or(false, |ts| ts <= snapshot)
    }
}

impl Serializable for IndexMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self> {
        let id = reader.read_varint_u32()?;
        let name = reader.read_str()?.to_string();
        let fields = Vec::<(String, Order)>::read_from(reader)?;
        let created_at = reader.read_varint_u64()?;
        let dropped_at = if reader.read_u8()? == 1 {
            Some(reader.read_varint_u64()?)
        } else {
            None
        };
        Ok(IndexMetadata { id, name, fields, created_at, dropped_at })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.id);
        writer.write_str(&self.name);
        self.fields.write_to(writer);
        writer.write_varint_u64(self.created_at);
        match self.dropped_at {
            Some(ts) => {
                writer.write_u8(1);
                writer.write_varint_u64(ts);
            }
            None => {
                writer.write_u8(0);
            }
        }
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
            fields: vec![
                ("name".to_string(), Order::Ascending),
                ("age".to_string(), Order::Descending),
            ],
            created_at: 1627846261,
            dropped_at: None,
        });

        check_serialization_round_trip(IndexMetadata {
            id: 12,
            name: "by_age".to_string(),
            fields: vec![("age".to_string(), Order::Descending)],
            created_at: 1627846261,
            dropped_at: Some(1627846300),
        });
    }

    #[test]
    fn test_collections_metadata_serialization() {
        check_serialization_round_trip(create_collections_with_indexes());
    }

    #[test]
    fn test_collection_options_serialization() {
        check_serialization_round_trip(CollectionOptions::default());
        check_serialization_round_trip(CollectionOptions {
            id_creation_strategy: IdCreationStrategy::Generated,
        });
        check_serialization_round_trip(CollectionOptions {
            id_creation_strategy: IdCreationStrategy::Manual,
        });
    }

    #[test]
    fn test_collection_metadata_with_custom_options() {
        let metadata = CollectionMetadata::new(
            5,
            "custom_collection",
            1627846261,
            CollectionOptions {
                id_creation_strategy: IdCreationStrategy::Generated,
            },
        );
        check_serialization_round_trip(metadata);
    }

    #[test]
    fn test_get_collection_at() {
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_collection("products", 11, 200);
        
        // Before creation
        assert!(catalog.get_collection_at(10, 99).is_none());
        assert!(catalog.get_collection_at(11, 199).is_none());
        
        // At creation time
        assert!(catalog.get_collection_at(10, 100).is_some());
        assert!(catalog.get_collection_at(11, 200).is_some());
        
        // After creation
        assert!(catalog.get_collection_at(10, 150).is_some());
        assert!(catalog.get_collection_at(11, 300).is_some());
        
        // Non-existent collection
        assert!(catalog.get_collection_at(99, 100).is_none());
    }

    #[test]
    fn test_get_collection_at_with_dropped() {
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .drop_collection(10, 300);
        
        // Before creation
        assert!(catalog.get_collection_at(10, 99).is_none());
        
        // At creation time
        assert!(catalog.get_collection_at(10, 100).is_some());
        
        // Between creation and drop
        assert!(catalog.get_collection_at(10, 200).is_some());
        assert!(catalog.get_collection_at(10, 299).is_some());
        
        // At drop time (dropped_at > snapshot, so ts=300 means dropped)
        assert!(catalog.get_collection_at(10, 300).is_none());
        
        // After drop
        assert!(catalog.get_collection_at(10, 400).is_none());
    }

    fn create_collections_with_indexes() -> CollectionMetadata {
        CollectionMetadata::new(2, "products", 1627846261, CollectionOptions::default())
            .add_index(IndexMetadata {
                id: 0,
                name: "by_name".to_string(),
                fields: vec![
                    ("name".to_string(), Order::Ascending),
                    ("age".to_string(), Order::Descending),
                ],
                created_at: 1627846262,
                dropped_at: None,
            })
            .add_index(IndexMetadata {
                id: 1,
                name: "by_price".to_string(),
                fields: vec![
                    ("price".to_string(), Order::Ascending),
                ],
                created_at: 1627846263,
                dropped_at: None,
            })
    }
}
