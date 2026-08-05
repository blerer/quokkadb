use crate::io::bitset::BitSet;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::invalid_data;
use crate::io::serializable::Serializable;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::Result;
use std::sync::Arc;

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
            let include_in_name_lookup = collection.dropped_at.is_none();
            collections.insert(id, collection);
            if include_in_name_lookup {
                id_by_name.insert(name, id);
            }
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
        self.add_collection_with_options(name, id, created_at, CollectionOptions::default())
    }

    pub fn add_collection_with_options(
        &self,
        name: &str,
        id: u32,
        created_at: u64,
        options: CollectionOptions,
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

    /// Adds an index to an existing collection in the catalog.
    pub fn add_index_to_collection(
        &self,
        collection_id: u32,
        index_id: u32,
        definition: &IndexDefinition,
        options: &IndexOptions,
        created_at: u64,
    ) -> Self {
        let col = self.collections.get(&collection_id).cloned().unwrap();
        let index = IndexMetadata {
            id: index_id,
            definition: definition.clone(),
            created_at,
            queryable_at: None,
            dropped_at: None,
            options: options.clone(),
        };
        let updated = Arc::new(col.add_index(index));
        let mut collections = self.collections.clone();
        collections.insert(collection_id, updated);
        Catalog {
            next_collection_id: self.next_collection_id,
            collections,
            id_by_name: self.id_by_name.clone(),
        }
    }

    pub fn drop_index(&self, collection_id: u32, index_id: u32, dropped_at: u64) -> Self {
        let col = self.collections.get(&collection_id).cloned().unwrap();
        let mut collections = self.collections.clone();
        collections.insert(
            collection_id,
            Arc::new(col.drop_index(index_id, dropped_at)),
        );

        Catalog {
            next_collection_id: self.next_collection_id,
            collections,
            id_by_name: self.id_by_name.clone(),
        }
    }

    pub fn mark_index_queryable(
        &self,
        collection_id: u32,
        index_id: u32,
        queryable_at: u64,
    ) -> Self {
        let col = self.collections.get(&collection_id).cloned().unwrap();
        let mut collections = self.collections.clone();
        collections.insert(
            collection_id,
            Arc::new(col.mark_index_queryable(index_id, queryable_at)),
        );

        Catalog {
            next_collection_id: self.next_collection_id,
            collections,
            id_by_name: self.id_by_name.clone(),
        }
    }
}

mod id_creation_strategy_tags {
    pub(super) const GENERATED: u8 = 0;
    pub(super) const MANUAL: u8 = 1;
    pub(super) const MIXED: u8 = 2;
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
            id_creation_strategy_tags::GENERATED => Ok(IdCreationStrategy::Generated),
            id_creation_strategy_tags::MANUAL => Ok(IdCreationStrategy::Manual),
            id_creation_strategy_tags::MIXED => Ok(IdCreationStrategy::Mixed),
            _ => Err(invalid_data(format!(
                "Invalid IdCreationStrategy byte: {}",
                byte
            ))),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            IdCreationStrategy::Generated => id_creation_strategy_tags::GENERATED,
            IdCreationStrategy::Manual => id_creation_strategy_tags::MANUAL,
            IdCreationStrategy::Mixed => id_creation_strategy_tags::MIXED,
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
        write!(
            f,
            "CollectionOptions {{ id_creation_strategy: {:?} }}",
            self.id_creation_strategy
        )
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

/// Options for creating an index.
#[derive(Debug, PartialEq, Clone)]
pub struct IndexOptions {
    pub name: Option<String>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        IndexOptions { name: None }
    }
}

impl IndexOptions {
    fn create_bitset(&self) -> BitSet {
        let mut bitset = BitSet::new();
        if self.name.is_some() {
            bitset.insert(0);
        }
        bitset
    }

    pub fn is_equivalent_to(&self, _other: &IndexOptions) -> bool {
        true // This method is a placeholder where future options should be taken into account
    }
}

impl Serializable for IndexOptions {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let bitset = BitSet::read_from(reader)?;
        Ok(IndexOptions {
            name: if bitset.contains(0) {
                Some(String::read_from(reader)?)
            } else {
                None
            },
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let bitset = self.create_bitset();
        bitset.write_to(writer);
        if bitset.contains(0) {
            self.name.as_ref().unwrap().write_to(writer);
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
            next_index_id: 1, // zero is reserved for the collection data
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

    pub fn active_indexes(&self) -> Vec<Arc<IndexMetadata>> {
        self.indexes
            .values()
            .filter(|idx| idx.dropped_at.is_none())
            .cloned()
            .collect()
    }

    pub fn queryable_indexes(&self) -> Vec<Arc<IndexMetadata>> {
        self.indexes
            .values()
            .filter(|idx| idx.dropped_at.is_none() && idx.queryable_at.is_some())
            .cloned()
            .collect()
    }

    pub fn find_index_equivalent_to(
        &self,
        definition: &IndexDefinition,
        options: &IndexOptions,
    ) -> Option<Arc<IndexMetadata>> {
        self.active_indexes()
            .into_iter()
            .find(|index| index.is_equivalent_to(&definition, &options))
    }

    fn add_index(&self, index: IndexMetadata) -> CollectionMetadata {
        assert_eq!(self.dropped_at, None);
        assert_eq!(self.next_index_id, index.id);
        let next_index_id = self.next_index_id + 1;
        let mut indexes = self.indexes.clone();
        let mut index_id_by_name = self.index_id_by_name.clone();
        let index_id = index.id;
        let index_name = index.name();
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

    fn drop_index(&self, id: u32, dropped_at: u64) -> Self {
        let index = self.indexes.get(&id).cloned().unwrap();
        let name = &index.name();
        let mut id_by_name = self.index_id_by_name.clone();
        let index_id = id_by_name.remove(name);
        assert_eq!(index_id, Some(id));
        let dropped_index = Arc::new(IndexMetadata {
            id,
            definition: index.definition.clone(),
            created_at: index.created_at,
            queryable_at: index.queryable_at,
            dropped_at: Some(dropped_at),
            options: index.options.clone(),
        });

        let mut indexes = self.indexes.clone();
        indexes.insert(id, dropped_index);

        CollectionMetadata {
            next_index_id: self.next_index_id,
            id: self.id,
            name: self.name.clone(),
            created_at: self.created_at,
            dropped_at: self.dropped_at,
            indexes,
            index_id_by_name: id_by_name,
            options: self.options.clone(),
        }
    }

    fn mark_index_queryable(&self, id: u32, queryable_at: u64) -> Self {
        let index = self.indexes.get(&id).cloned().unwrap();
        let updated_index = Arc::new(IndexMetadata {
            id,
            definition: index.definition.clone(),
            created_at: index.created_at,
            queryable_at: Some(queryable_at),
            dropped_at: index.dropped_at,
            options: index.options.clone(),
        });

        let mut indexes = self.indexes.clone();
        indexes.insert(id, updated_index);

        CollectionMetadata {
            next_index_id: self.next_index_id,
            id: self.id,
            name: self.name.clone(),
            created_at: self.created_at,
            dropped_at: self.dropped_at,
            indexes,
            index_id_by_name: self.index_id_by_name.clone(),
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
            let include_in_name_lookup = index.dropped_at.is_none();
            if include_in_name_lookup {
                let index_name = index.name();
                index_id_by_name.insert(index_name, index_id);
            }
            indexes.insert(index_id, index);
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

#[derive(Debug, PartialEq, Clone)]
pub enum IndexDirection {
    Ascending,
    Descending,
}

impl IndexDirection {
    fn as_string(&self) -> &'static str {
        match self {
            IndexDirection::Ascending => "1",
            IndexDirection::Descending => "-1",
        }
    }
}

impl fmt::Display for IndexDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexDirection::Ascending => write!(f, "ASC"),
            IndexDirection::Descending => write!(f, "DESC"),
        }
    }
}

mod index_direction_tags {
    pub(super) const ASCENDING: u8 = 0;
    pub(super) const DESCENDING: u8 = 1;
}

impl Serializable for IndexDirection {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            index_direction_tags::ASCENDING => Ok(IndexDirection::Ascending),
            index_direction_tags::DESCENDING => Ok(IndexDirection::Descending),
            _ => Err(invalid_data(format!("Invalid SortOrder byte: {}", byte))),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            IndexDirection::Ascending => index_direction_tags::ASCENDING,
            IndexDirection::Descending => index_direction_tags::DESCENDING,
        };
        writer.write_u8(byte);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderedIndexField {
    pub path: IndexPath,
    pub direction: IndexDirection,
}

impl OrderedIndexField {
    pub fn asc<P: Into<IndexPath>>(path: P) -> Self {
        OrderedIndexField {
            path: path.into(),
            direction: IndexDirection::Ascending,
        }
    }

    pub fn desc<P: Into<IndexPath>>(path: P) -> Self {
        OrderedIndexField {
            path: path.into(),
            direction: IndexDirection::Descending,
        }
    }

    fn as_string(&self) -> String {
        format!("{}_{}", self.path.as_string(), self.direction.as_string())
    }
}

impl fmt::Display for OrderedIndexField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.path, self.direction)
    }
}

impl Serializable for OrderedIndexField {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let path = IndexPath::read_from(reader)?;
        let order = IndexDirection::read_from(reader)?;
        Ok(OrderedIndexField {
            path,
            direction: order,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.path.write_to(writer);
        self.direction.write_to(writer);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct IndexPath {
    pub(crate) components: Vec<String>,
}

impl IndexPath {
    fn as_string(&self) -> String {
        self.components.join(".")
    }
}

impl fmt::Display for IndexPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

impl Into<IndexPath> for &str {
    fn into(self) -> IndexPath {
        IndexPath {
            components: vec![self.to_string()],
        }
    }
}

impl Into<IndexPath> for Vec<&str> {
    fn into(self) -> IndexPath {
        IndexPath {
            components: self.iter().map(|e| e.to_string()).collect(),
        }
    }
}

impl Serializable for IndexPath {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let size = reader.read_varint_u64()? as usize;
        let mut components = Vec::with_capacity(size);
        for _ in 0..size {
            components.push(reader.read_str()?.to_string());
        }
        Ok(IndexPath { components })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.components.len() as u64);
        for component in &self.components {
            writer.write_str(component);
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum IndexDefinition {
    Regular(Vec<OrderedIndexField>),
}

impl IndexDefinition {
    pub fn as_string(&self) -> String {
        match self {
            IndexDefinition::Regular(fields) => {
                let field_strings: Vec<String> = fields.iter().map(|f| f.as_string()).collect();
                field_strings.join("_")
            }
        }
    }
}

impl fmt::Display for IndexDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexDefinition::Regular(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| field.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Regular({})", fields)
            }
        }
    }
}

mod index_definition_tags {
    pub(super) const REGULAR: u8 = 0;
}

impl Serializable for IndexDefinition {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            index_definition_tags::REGULAR => Ok(IndexDefinition::Regular(
                Vec::<OrderedIndexField>::read_from(reader)?,
            )),
            _ => Err(invalid_data(format!(
                "Invalid IndexDefinition tag: {}",
                tag
            ))),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            IndexDefinition::Regular(keys) => {
                writer.write_u8(index_definition_tags::REGULAR);
                keys.write_to(writer);
            }
        }
    }
}

/// Describes a single index within a collection.
#[derive(Debug, PartialEq, Clone)]
pub struct IndexMetadata {
    /// Unique identifier for the index.
    pub id: u32,
    /// The index definition.
    pub definition: IndexDefinition,
    /// Timestamp when the index was created.
    pub created_at: u64,
    /// Timestamp when the index was queryable.
    pub queryable_at: Option<u64>,
    /// Timestamp when the index was dropped.
    pub dropped_at: Option<u64>,
    /// Additional index options.
    pub options: IndexOptions,
}

impl IndexMetadata {
    #[cfg(test)]
    pub fn new(id: u32, definition: IndexDefinition, created_at: u64) -> Self {
        IndexMetadata {
            id,
            definition,
            created_at,
            queryable_at: None,
            dropped_at: None,
            options: IndexOptions::default(),
        }
    }

    pub fn name(&self) -> String {
        if let Some(name) = &self.options.name {
            name.clone()
        } else {
            self.definition.as_string()
        }
    }

    pub fn is_equivalent_to(&self, definition: &IndexDefinition, options: &IndexOptions) -> bool {
        self.definition == *definition && self.options.is_equivalent_to(options)
    }

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
        let definition = IndexDefinition::read_from(reader)?;
        let created_at = reader.read_varint_u64()?;
        let queryable_at = Option::<u64>::read_from(&reader)?;
        let dropped_at = Option::<u64>::read_from(&reader)?;
        let config = IndexOptions::read_from(reader)?;
        Ok(IndexMetadata {
            id,
            definition,
            created_at,
            queryable_at,
            dropped_at,
            options: config,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.id);
        self.definition.write_to(writer);
        writer.write_varint_u64(self.created_at);
        self.queryable_at.write_to(writer);
        self.dropped_at.write_to(writer);
        self.options.write_to(writer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::serializable::check_serialization_round_trip;

    #[test]
    fn test_index_name_formatting_single_asc_field() {
        let definition = IndexDefinition::Regular(vec![OrderedIndexField::asc("name")]);
        let index = IndexMetadata::new(0, definition, 1627846261);
        assert_eq!(index.name(), "name_1");
    }

    #[test]
    fn test_index_name_formatting_single_desc_field() {
        let definition = IndexDefinition::Regular(vec![OrderedIndexField::desc("age")]);
        let index = IndexMetadata::new(0, definition, 1627846261);
        assert_eq!(index.name(), "age_-1");
    }

    #[test]
    fn test_index_name_formatting_multiple_fields() {
        let definition = IndexDefinition::Regular(vec![
            OrderedIndexField::asc("name"),
            OrderedIndexField::desc("age"),
        ]);
        let index = IndexMetadata::new(0, definition, 1627846261);
        assert_eq!(index.name(), "name_1_age_-1");
    }

    #[test]
    fn test_index_name_formatting_nested_path() {
        let definition = IndexDefinition::Regular(vec![OrderedIndexField::asc(IndexPath {
            components: vec!["address".to_string(), "city".to_string()],
        })]);
        let index = IndexMetadata::new(0, definition, 1627846261);
        assert_eq!(index.name(), "address.city_1");
    }

    #[test]
    fn test_index_name_formatting_mixed_nested_paths() {
        let definition = IndexDefinition::Regular(vec![
            OrderedIndexField::asc(IndexPath {
                components: vec!["address".to_string(), "city".to_string()],
            }),
            OrderedIndexField::desc("score"),
        ]);
        let index = IndexMetadata::new(0, definition, 1627846261);
        assert_eq!(index.name(), "address.city_1_score_-1");
    }

    #[test]
    fn test_index_definition_display_is_descriptive() {
        let definition = IndexDefinition::Regular(vec![
            OrderedIndexField::asc(IndexPath {
                components: vec!["address".to_string(), "city".to_string()],
            }),
            OrderedIndexField::desc("score"),
        ]);

        assert_eq!(
            definition.to_string(),
            "Regular(address.city ASC, score DESC)"
        );
    }

    #[test]
    fn test_index_metadata_serialization() {
        check_serialization_round_trip(IndexMetadata {
            id: 11,
            definition: IndexDefinition::Regular(vec![
                OrderedIndexField::asc("name"),
                OrderedIndexField::desc("age"),
            ]),
            created_at: 1627846261,
            queryable_at: None,
            dropped_at: None,
            options: IndexOptions {
                name: Some("by_name".to_string()),
            },
        });

        check_serialization_round_trip(IndexMetadata {
            id: 12,
            definition: IndexDefinition::Regular(vec![OrderedIndexField::desc("age")]),
            created_at: 1627846261,
            queryable_at: Some(1627846270),
            dropped_at: Some(1627846300),
            options: IndexOptions::default(),
        });
    }

    #[test]
    fn test_index_config_serialization() {
        check_serialization_round_trip(IndexOptions::default());
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
    fn test_catalog_serialization_round_trip() {
        let users_definition = IndexDefinition::Regular(vec![
            OrderedIndexField::asc("name"),
            OrderedIndexField::desc("age"),
        ]);
        let products_definition =
            IndexDefinition::Regular(vec![OrderedIndexField::asc(IndexPath {
                components: vec!["category".to_string(), "name".to_string()],
            })]);

        let catalog = Catalog::new()
            .add_collection_with_options(
                "users",
                10,
                100,
                CollectionOptions {
                    id_creation_strategy: IdCreationStrategy::Generated,
                },
            )
            .add_index_to_collection(
                10,
                1,
                &users_definition,
                &IndexOptions {
                    name: Some("by_name".to_string()),
                },
                110,
            )
            .add_index_to_collection(
                10,
                2,
                &IndexDefinition::Regular(vec![OrderedIndexField::desc("age")]),
                &IndexOptions {
                    name: Some("by_age".to_string()),
                },
                120,
            )
            .drop_index(10, 2, 150)
            .add_collection("products", 11, 200)
            .add_index_to_collection(11, 1, &products_definition, &IndexOptions::default(), 210)
            .drop_collection(11, 300);

        check_serialization_round_trip(catalog);
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

    #[test]
    fn test_drop_index_preserves_metadata_and_removes_active_lookup() {
        let metadata = create_collections_with_indexes();

        let before_drop = metadata.get_index_by_name("by_name").unwrap();
        assert_eq!(before_drop.id, 1);
        assert_eq!(metadata.active_indexes().len(), 2);

        let dropped = metadata.drop_index(1, 1627846300);

        assert!(dropped.get_index_by_name("by_name").is_none());
        assert_eq!(dropped.active_indexes().len(), 1);
        assert_eq!(dropped.active_indexes()[0].name(), "by_price");

        let dropped_index = dropped.get_index_by_id(1).unwrap();
        assert_eq!(dropped_index.name(), "by_name");
        assert_eq!(dropped_index.created_at, 1627846262);
        assert_eq!(dropped_index.dropped_at, Some(1627846300));
        assert_eq!(
            dropped_index.definition,
            IndexDefinition::Regular(vec![
                OrderedIndexField::asc("name"),
                OrderedIndexField::desc("age"),
            ])
        );
    }

    #[test]
    fn test_readable_indexes_only_include_queryable_non_dropped_indexes() {
        let metadata = create_collections_with_indexes();
        assert_eq!(metadata.active_indexes().len(), 2);
        assert_eq!(metadata.queryable_indexes().len(), 1);
        assert_eq!(metadata.queryable_indexes()[0].name(), "by_price");

        let dropped = metadata.drop_index(2, 1627846300);
        assert!(dropped.queryable_indexes().is_empty());
    }

    #[test]
    fn test_get_index_at_respects_mvcc_boundaries() {
        let metadata = create_collections_with_indexes();

        assert!(metadata.get_index_at(1, 1627846261).is_none());
        assert!(metadata.get_index_at(1, 1627846262).is_some());
        assert!(metadata.get_index_at(1, 1627846299).is_some());

        let dropped = metadata.drop_index(1, 1627846300);
        assert!(dropped.get_index_at(1, 1627846299).is_some());
        assert!(dropped.get_index_at(1, 1627846300).is_none());
        assert!(dropped.get_index_at(1, 1627846301).is_none());

        assert!(metadata.get_index_at(99, 1627846262).is_none());
    }

    #[test]
    fn test_collection_or_index_exist_at_respects_mvcc_boundaries() {
        let definition = IndexDefinition::Regular(vec![OrderedIndexField::asc("name")]);
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_index_to_collection(
                10,
                1,
                &definition,
                &IndexOptions {
                    name: Some("by_name".to_string()),
                },
                200,
            );

        assert!(!catalog.collection_or_index_exist_at(10, 0, 99));
        assert!(catalog.collection_or_index_exist_at(10, 0, 100));
        assert!(catalog.collection_or_index_exist_at(10, 0, 250));

        assert!(!catalog.collection_or_index_exist_at(10, 1, 199));
        assert!(catalog.collection_or_index_exist_at(10, 1, 200));
        assert!(catalog.collection_or_index_exist_at(10, 1, 299));

        let dropped_index_catalog = catalog.drop_index(10, 1, 300);
        assert!(dropped_index_catalog.collection_or_index_exist_at(10, 0, 300));
        assert!(dropped_index_catalog.collection_or_index_exist_at(10, 1, 299));
        assert!(!dropped_index_catalog.collection_or_index_exist_at(10, 1, 300));
        assert!(!dropped_index_catalog.collection_or_index_exist_at(10, 1, 301));

        let dropped_collection_catalog = dropped_index_catalog.drop_collection(10, 400);
        assert!(dropped_collection_catalog.collection_or_index_exist_at(10, 0, 399));
        assert!(!dropped_collection_catalog.collection_or_index_exist_at(10, 0, 400));
        assert!(!dropped_collection_catalog.collection_or_index_exist_at(10, 1, 400));

        assert!(!catalog.collection_or_index_exist_at(10, 99, 250));
        assert!(!catalog.collection_or_index_exist_at(99, 0, 250));
        assert!(!catalog.collection_or_index_exist_at(99, 1, 250));
    }

    #[test]
    fn test_rename_collection_updates_name_lookup_and_preserves_id() {
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_collection("products", 11, 200);

        let renamed = catalog.rename_collection(10, "customers");

        assert!(renamed.get_collection_by_name("users").is_none());

        let renamed_collection = renamed.get_collection_by_name("customers").unwrap();
        assert_eq!(renamed_collection.id, 10);
        assert_eq!(renamed_collection.name, "customers");
        assert_eq!(renamed.get_collection_by_id(&10).unwrap().name, "customers");
        assert_eq!(renamed.next_collection_id, 12);
    }

    #[test]
    fn test_drop_collection_removes_name_lookup_but_preserves_id_lookup() {
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_collection("products", 11, 200);

        let dropped = catalog.drop_collection(10, 300);

        assert!(dropped.get_collection_by_name("users").is_none());

        let dropped_collection = dropped.get_collection_by_id(&10).unwrap();
        assert_eq!(dropped_collection.name, "users");
        assert_eq!(dropped_collection.dropped_at, Some(300));
        assert_eq!(dropped.next_collection_id, 12);
    }

    #[test]
    fn test_list_collections_excludes_dropped_collections() {
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_collection("products", 11, 200)
            .drop_collection(10, 300);

        let collection_names: Vec<_> = catalog
            .list_collections()
            .map(|collection| collection.name.as_str())
            .collect();

        assert_eq!(collection_names, vec!["products"]);
    }

    #[test]
    fn test_add_index_to_collection_with_explicit_name() {
        let definition = IndexDefinition::Regular(vec![
            OrderedIndexField::asc("name"),
            OrderedIndexField::desc("age"),
        ]);
        let options = IndexOptions {
            name: Some("by_name".to_string()),
        };
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_index_to_collection(10, 1, &definition, &options, 200);

        let collection = catalog.get_collection_by_id(&10).unwrap();
        assert_eq!(collection.next_index_id, 2);

        let index = collection.get_index_by_id(1).unwrap();
        assert_eq!(index.name(), "by_name");
        assert_eq!(index.definition, definition);
        assert_eq!(index.options, options);
        assert_eq!(index.created_at, 200);
        assert_eq!(index.queryable_at, None);
        assert_eq!(index.dropped_at, None);
    }

    #[test]
    fn test_add_index_to_collection_generates_name_from_definition() {
        let definition = IndexDefinition::Regular(vec![
            OrderedIndexField::asc(IndexPath {
                components: vec!["profile".to_string(), "email".to_string()],
            }),
            OrderedIndexField::desc("score"),
        ]);
        let catalog = Catalog::new()
            .add_collection("users", 10, 100)
            .add_index_to_collection(10, 1, &definition, &IndexOptions::default(), 200);

        let collection = catalog.get_collection_by_id(&10).unwrap();
        assert_eq!(collection.next_index_id, 2);

        let index = collection.get_index_by_id(1).unwrap();
        assert_eq!(index.name(), "profile.email_1_score_-1");
        assert_eq!(index.definition, definition);
        assert_eq!(index.queryable_at, None);
        assert_eq!(index.dropped_at, None);
    }

    fn create_collections_with_indexes() -> CollectionMetadata {
        CollectionMetadata::new(2, "products", 1627846261, CollectionOptions::default())
            .add_index(IndexMetadata {
                id: 1,
                definition: IndexDefinition::Regular(vec![
                    OrderedIndexField::asc("name"),
                    OrderedIndexField::desc("age"),
                ]),
                created_at: 1627846262,
                queryable_at: None,
                dropped_at: None,
                options: IndexOptions {
                    name: Some("by_name".to_string()),
                },
            })
            .add_index(IndexMetadata {
                id: 2,
                definition: IndexDefinition::Regular(vec![OrderedIndexField::asc("price")]),
                created_at: 1627846263,
                queryable_at: Some(1627846264),
                dropped_at: None,
                options: IndexOptions {
                    name: Some("by_price".to_string()),
                },
            })
    }
}
