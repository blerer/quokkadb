use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::lsm_tree::Level::{NonOverlapping, Overlapping};
use crate::storage::memtable::Memtable;
use crate::util::interval::Interval;

#[derive(Default, Debug, PartialEq)]
pub struct Levels {
    levels: Vec<Arc<Level>>,
}

impl Levels {

    fn add(&self, sst: Arc<SSTableMetadata>) -> Self {
        let sst_level = sst.level as usize;
        let mut new_levels = Vec::with_capacity(self.levels.len().max(sst_level + 1));
        for (index, level) in self.levels.iter().enumerate() {
            new_levels.push(if index == sst_level {
                Arc::new(level.add(sst.clone()))
            } else {
                level.clone()
            });
        }

        while new_levels.len() <= sst_level {
            let level = if sst_level == new_levels.len() {
                Level::new(sst.level, vec![sst.clone()])
            } else {
                Level::empty(sst.level)
            };
            new_levels.push(Arc::new(level));
        }

        Levels { levels: new_levels }
    }
}

impl SnapshotElement for Levels {

    fn read_from(reader: &ByteReader) -> Result<Self> {
        let size = reader.read_varint_u64()? as usize;
        let mut levels = Vec::with_capacity(size);
        for _ in 0..size {
            levels.push(Arc::new(Level::read_from(reader)?));
        }
        Ok(Levels { levels })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.levels.len() as u64);
        self.levels.iter().for_each(|level| {
            level.write_to(writer);
        });
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum  Level {
    // For L0, files may overlap so we simply store a Vec.
    Overlapping {level: u32, sstables: Vec<Arc<SSTableMetadata>>},
    // For L1+ levels, files are non-overlapping and sorted by min_key.
    NonOverlapping {level: u32, sstables: Vec<Arc<SSTableMetadata>>},
}

impl Level {

    fn empty(level: u32) -> Self {
       if level == 0 {
           Overlapping {level: 0, sstables: Vec::new()}
       }  else {
           NonOverlapping {level, sstables: Vec::new()}
       }
    }

    fn new(level: u32, mut sstables: Vec<Arc<SSTableMetadata>>) -> Self {
        if level == 0 {
            Overlapping {level: 0, sstables}
        }  else {
            sstables.sort_by(|a, b| a.min_key.cmp(&b.min_key));
            NonOverlapping {level, sstables}
        }
    }

    fn read_sstables_from(reader: &ByteReader) -> Result<Vec<Arc<SSTableMetadata>>> {
        let size = reader.read_varint_u32()? as usize;
        let mut sstables = Vec::with_capacity(size);
        for _ in 0..size {
            sstables.push(Arc::new(SSTableMetadata::read_from(reader)?));
        }
        Ok(sstables)
    }
    fn write_sstables_to(&self, sstables : &Vec<Arc<SSTableMetadata>>, writer: &mut ByteWriter) {
        writer.write_varint_u32(sstables.len() as u32);
        sstables.iter().for_each(|sst| {
            sst.write_to(writer);
        });
    }

    fn add(&self, sst: Arc<SSTableMetadata>) -> Self {
        match &self {
            Overlapping { level, sstables } | NonOverlapping { level, sstables } => {
                let mut copy = sstables.iter().cloned().collect::<Vec<_>>();
                copy.push(sst);
                Level::new(*level, copy)
            }
        }
    }

    /// Finds an SSTable that contains the key and is visible under the given snapshot.
    pub fn find(&self, record_key: &[u8], snapshot: u64) -> Vec<Arc<SSTableMetadata>> {
        match self {
            Overlapping {level: _, sstables} => {
                sstables.iter()
                    .filter(|sst| {
                        record_key >= sst.min_key.as_slice()
                            && record_key <= sst.max_key.as_slice()
                            && snapshot >= sst.min_sequence_number
                    })
                    .cloned()
                    .collect()
            }
            NonOverlapping{ level: _, sstables} => {
                // Binary search for the SSTable covering the key.
                let idx = sstables.binary_search_by(|sst| {
                    if record_key < sst.min_key.as_slice() {
                        Ordering::Greater
                    } else if record_key > sst.max_key.as_slice() {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                }).ok();

                idx.and_then(|i| {
                    let sst = &sstables[i];
                    if snapshot >= sst.min_sequence_number {
                        Some(vec![sst.clone()])
                    } else {
                        None
                    }
                }).unwrap_or_default()
            }
        }
    }

    /// Finds all SSTables that overlap the given interval and are visible under the snapshot.
    ///
    /// The `interval` parameter is an instance of your provided `Interval` type.
    pub fn find_range(&self, interval: &Interval<Vec<u8>>, snapshot: u64) -> Vec<Arc<SSTableMetadata>> {
        match self {
            Overlapping { level: _, sstables} => {
                sstables.iter()
                    .filter(|sst| {
                        overlaps(interval, sst)
                            && snapshot >= sst.min_sequence_number
                    })
                    .cloned()
                    .collect()
            }
            NonOverlapping { level: _, sstables} => {
                // Use binary search to find the first candidate SSTable.
                // Here we use the interval's start bound as the lower limit.
                let lower = match interval.start_bound() {
                    Bound::Included(val) | Bound::Excluded(val) => val,
                    Bound::Unbounded => &vec![], // smallest possible key
                };

                let start_idx = sstables.binary_search_by(|sst| {
                    if sst.max_key.as_slice() < lower.as_slice() {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }).unwrap_or_else(|idx| idx);

                let mut result = Vec::new();
                for sst in &sstables[start_idx..] {
                    // If the SSTable's min_key is beyond the interval's end, stop scanning.
                    if let Bound::Included(end) | Bound::Excluded(end) = interval.end_bound() {
                        if sst.min_key.as_slice() > end.as_slice() {
                            break;
                        }
                    }
                    if overlaps(interval, sst)
                        && snapshot >= sst.min_sequence_number
                    {
                        result.push(sst.clone());
                    }
                }
                result
            }
        }
    }
}

/// A helper function that checks if an SSTable's key range overlaps with the provided interval.
fn overlaps(interval: &Interval<Vec<u8>>, sst: &SSTableMetadata) -> bool {
    let lower_ok = match interval.start_bound() {
        Bound::Included(l) => &sst.max_key >= &l,
        Bound::Excluded(l) => &sst.max_key > &l,
        Bound::Unbounded => true,
    };
    let upper_ok = match interval.end_bound() {
        Bound::Included(u) => &sst.min_key <= &u,
        Bound::Excluded(u) => &sst.min_key < &u,
        Bound::Unbounded => true,
    };
    lower_ok && upper_ok
}

impl SnapshotElement for Level {

    fn read_from(reader: &ByteReader) -> Result<Self> {
        let level = reader.read_varint_u32()?;
        let sstables = Self::read_sstables_from(reader)?;
        match level {
            0 => Ok(Overlapping { level, sstables }),
            _ => Ok(NonOverlapping { level, sstables }),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match &self {
            Overlapping { level, sstables } | NonOverlapping { level, sstables } => {
                writer.write_varint_u32(*level);
                self.write_sstables_to(sstables, writer);
            }
        }
    }
}

#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub struct SSTableMetadata {
    number: u32,
    level: u32,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    min_sequence_number: u64,
    max_sequence_number: u64,
}

impl SSTableMetadata {
    pub(crate) fn new(
        number: u32,
        level: u32,
        min_key: &[u8],
        max_key: &[u8],
        min_seq: u64,
        max_seq: u64,
    ) -> SSTableMetadata {
        SSTableMetadata {
            number,
            level,
            min_key: min_key.to_vec(),
            max_key: max_key.to_vec(),
            min_sequence_number: min_seq,
            max_sequence_number: max_seq,
        }
    }
}

impl SnapshotElement for SSTableMetadata {

    fn read_from(reader: &ByteReader) -> Result<SSTableMetadata> {
        Ok(SSTableMetadata {
            number: reader.read_varint_u32()?,
            level: reader.read_varint_u32()?,
            min_key: reader.read_length_prefixed_slice()?.to_vec(),
            max_key: reader.read_length_prefixed_slice()?.to_vec(),
            min_sequence_number: reader.read_varint_u64()?,
            max_sequence_number: reader.read_varint_u64()?,
        })
    }

    fn write_to(&self, writer:&mut ByteWriter) {
        writer.write_varint_u32(self.number)
            .write_varint_u32(self.level)
            .write_length_prefixed_slice(&self.min_key)
            .write_length_prefixed_slice(&self.max_key)
            .write_varint_u64(self.min_sequence_number)
            .write_varint_u64(self.max_sequence_number);
    }
}
impl PartialOrd<Self> for SSTableMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.number.cmp(&other.number))
    }
}

impl Ord for SSTableMetadata {
    fn cmp(&self, other: &Self) -> Ordering {
        self.number.cmp(&other.number)
    }
}

#[derive(Debug, PartialEq)]
struct IndexMetadata {
    id: u32,
    name: String,
}

impl SnapshotElement for IndexMetadata {
    fn read_from(reader: &ByteReader) -> Result<Self> {
        let id = reader.read_varint_u32()?;
        let name = reader.read_str()?.to_string();
        Ok(IndexMetadata { id, name })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.id);
        writer.write_str(&self.name);
    }
}

#[derive(Debug, PartialEq)]
pub struct CollectionMetadata {
    id: u32,
    name: String,
    indexes: Arc<BTreeMap<String, Arc<IndexMetadata>>>,
}

impl CollectionMetadata {
    fn new(id: u32, name: String) -> Self {
        CollectionMetadata { id, name, indexes: Arc::new(BTreeMap::new()) }
    }

    fn read_indexes_from(reader: &ByteReader) -> Result<Arc<BTreeMap<String, Arc<IndexMetadata>>>> {
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

    fn read_from(reader: &ByteReader) -> Result<Self> {
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

pub struct LsmTree {
    /// The id of the current wal file
    pub current_log_number: u64,
    /// The id of the oldest wal file still containing some non persisted data
    pub oldest_log_number: u64,
    /// The next file number
    pub next_file_number: u64,
    /// The last sequence number
    pub last_sequence_number: u64,
    /// The collections metadata
    pub collections: Arc<BTreeMap<String, Arc<CollectionMetadata>>>,
    /// The current memtable
    pub memtable: Arc<Memtable>,
    /// The memtables pending flush
    pub imm_memtables: Arc<VecDeque<Arc<Memtable>>>,
    /// The SSTables per levels
    pub sst_levels: Arc<Levels>,
}

impl PartialEq for LsmTree {
    fn eq(&self, other: &Self) -> bool {
        self.current_log_number == other.current_log_number &&
            self.oldest_log_number == other.oldest_log_number &&
            self.next_file_number == other.next_file_number &&
            self.last_sequence_number == other.last_sequence_number &&
            self.collections == other.collections &&
            self.sst_levels == other.sst_levels
    }
}

impl fmt::Debug for LsmTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LsmTree")
            .field("current_log_number", &self.current_log_number)
            .field("oldest_log_number", &self.oldest_log_number)
            .field("next_file_number", &self.next_file_number)
            .field("last_sequence_number", &self.last_sequence_number)
            .field("collections", &self.collections)
            .field("sst_levels", &self.sst_levels)
            .finish()
    }
}

impl LsmTree {
    pub fn new(current_log_number: u64, next_file_number: u64) -> Self {
        LsmTree {
            current_log_number,
            oldest_log_number: current_log_number,
            next_file_number,
            last_sequence_number: 0,
            collections: Arc::new(BTreeMap::new()),
            memtable: Arc::new(Memtable::new()),
            imm_memtables: Arc::new(VecDeque::with_capacity(0)),
            sst_levels: Default::default(),
        }
    }

    fn write_collections_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.collections.len() as u32);
        self.collections.iter().for_each(|(str, col)| {
            writer.write_str(str);
            col.write_to(writer)
        });
    }

    fn read_collections_from(reader: &ByteReader) -> Result<Arc<BTreeMap<String, Arc<CollectionMetadata>>>> {
        let size = reader.read_varint_u64()? as usize;
        let mut collections = BTreeMap::new();
        for _ in 0..size {
            let str = reader.read_str()?.to_string();
            let collection = Arc::new(CollectionMetadata::read_from(reader)?);
            collections.insert(str, collection);
        }
        Ok(Arc::new(collections))
    }

    pub fn apply(&self, edit: &LsmTreeEdit) -> Self {
        match edit {
            LsmTreeEdit::ScheduleMemtableForFlush() => {
                let mut imm_memtables = shallow_copy(&self.imm_memtables);
                imm_memtables.push_back(self.memtable.clone());
                LsmTree {
                    current_log_number: self.current_log_number,
                    oldest_log_number: self.oldest_log_number,
                    last_sequence_number: self.last_sequence_number,
                    next_file_number: self.next_file_number,
                    collections: self.collections.clone(),
                    memtable: Arc::new(Memtable::new()),
                    imm_memtables: Arc::new(imm_memtables),
                    sst_levels: self.sst_levels.clone()
                }
            }
            LsmTreeEdit::Flush { log_id, sst} => {
                let mut imm_memtables = shallow_copy(&self.imm_memtables);
                imm_memtables.pop_front();
                let last_sequence_number = sst.max_sequence_number;
                let sst_levels = Arc::new(self.sst_levels.add(sst.clone()));
                LsmTree {
                    current_log_number: self.current_log_number,
                    oldest_log_number: *log_id,
                    last_sequence_number,
                    next_file_number: self.next_file_number,
                    collections: self.collections.clone(),
                    memtable: self.memtable.clone(),
                    imm_memtables: Arc::new(imm_memtables),
                    sst_levels
                }
            }
            LsmTreeEdit::CreateCollection { name, id } => {
                let mut collections = self.collections.as_ref().clone();
                collections.insert(name.clone(), Arc::new(CollectionMetadata::new(*id, name.clone())));
                LsmTree {
                    current_log_number: self.current_log_number,
                    oldest_log_number: self.oldest_log_number,
                    last_sequence_number: self.last_sequence_number,
                    next_file_number: self.next_file_number,
                    collections: Arc::new(collections),
                    memtable: self.memtable.clone(),
                    imm_memtables: self.imm_memtables.clone(),
                    sst_levels: self.sst_levels.clone(),
                }
            }
            LsmTreeEdit::DropCollection { name } => {
                let mut collections = self.collections.as_ref().clone();
                collections.remove(&name);
                LsmTree {
                    current_log_number: self.current_log_number,
                    oldest_log_number: self.oldest_log_number,
                    last_sequence_number: self.last_sequence_number,
                    next_file_number: self.next_file_number,
                    collections: Arc::new(collections),
                    memtable: self.memtable.clone(),
                    imm_memtables: self.imm_memtables.clone(),
                    sst_levels: self.sst_levels.clone(),
                }
            }
            LsmTreeEdit::FilesDetectedOnRestart { next_file_number} => {
                LsmTree {
                    current_log_number: self.current_log_number,
                    oldest_log_number: self.oldest_log_number,
                    last_sequence_number: self.last_sequence_number,
                    next_file_number: *next_file_number,
                    collections: self.collections.clone(),
                    memtable: self.memtable.clone(),
                    imm_memtables: self.imm_memtables.clone(),
                    sst_levels: self.sst_levels.clone(),
                }
            }
            LsmTreeEdit::ManifestRotation { manifest_id } => {
                LsmTree {
                    current_log_number: self.current_log_number,
                    oldest_log_number: self.oldest_log_number,
                    last_sequence_number: self.last_sequence_number,
                    next_file_number: manifest_id + 1,
                    collections: self.collections.clone(),
                    memtable: self.memtable.clone(),
                    imm_memtables: self.imm_memtables.clone(),
                    sst_levels: self.sst_levels.clone(),
                }
            }
            LsmTreeEdit::WalRotation { log_id} => {
                LsmTree {
                    current_log_number: *log_id,
                    oldest_log_number: self.oldest_log_number,
                    last_sequence_number: self.last_sequence_number,
                    next_file_number: log_id + 1,
                    collections: self.collections.clone(),
                    memtable: self.memtable.clone(),
                    imm_memtables: self.imm_memtables.clone(),
                    sst_levels: self.sst_levels.clone(),
                }
            }
            LsmTreeEdit::Snapshot(_) => { panic!("Snapshots should not be applied to an LSMTree"); },
        }
    }
}

impl SnapshotElement for LsmTree {
    fn read_from(reader: &ByteReader) -> Result<Self> {
        let current_log_number = reader.read_varint_u64()?;
        let oldest_log_number = reader.read_varint_u64()?;
        let next_file_number = reader.read_varint_u64()?;
        let last_sequence_number = reader.read_varint_u64()?;
        let collections = Self::read_collections_from(reader)?;
        let memtable = Arc::new(Memtable::new());
        let imm_memtables = Arc::new(VecDeque::new());
        let sst_levels = Arc::new(Levels::read_from(reader)?);

        Ok(LsmTree {
            current_log_number,
            oldest_log_number,
            next_file_number,
            last_sequence_number,
            collections,
            memtable,
            imm_memtables,
            sst_levels,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.current_log_number);
        writer.write_varint_u64(self.oldest_log_number);
        writer.write_varint_u64(self.next_file_number);
        writer.write_varint_u64(self.last_sequence_number);
        self.write_collections_to(writer);
        self.sst_levels.write_to(writer)
        // Memtables are not part of the snapshot stored in the Manifest
        // as their content is persisted through the wal
    }
}

/// Represent an element of the snapshots persisted at the beginning of the Manifest file.
/// A snapshot represent the LSM tree at a given point in time. It does not contain the Memtables
/// as their content is preserved by the write ahead log.
trait SnapshotElement {

    /// Deserialized the element from the specified ByteReader
    fn read_from(reader: &ByteReader) -> Result<Self> where Self: Sized;

    /// Serialize the element into the specified ByteWriter
    fn write_to(&self, writer: &mut ByteWriter);
}


// Generic function to create a shallow copy of any iterable collection containing `Arc<T>`
fn shallow_copy<C, T>(collection: &Arc<C>) -> C
where
        for<'a> &'a C: IntoIterator<Item = &'a Arc<T>>, // Iterates over references
        C: FromIterator<Arc<T>>,  // Supports constructing from an iterator
{
    collection.into_iter().cloned().collect() // Clone the `Arc<T>` to preserve shared ownership
}

#[derive(Debug, PartialEq)]
pub enum LsmTreeEdit {
    Snapshot(LsmTree),
    CreateCollection { name: String, id: u32 },
    DropCollection { name: String },
    WalRotation { log_id:  u64 },
    ManifestRotation {  manifest_id: u64 },
    Flush { log_id: u64, sst: Arc<SSTableMetadata>},
    ScheduleMemtableForFlush(),
    FilesDetectedOnRestart { next_file_number: u64 },
}

impl LsmTreeEdit {
    pub fn to_vec(&self) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        match self {
            LsmTreeEdit::Snapshot(tree) => {
                writer.write_u8(0);
                tree.write_to(&mut writer);
            }
            LsmTreeEdit::CreateCollection { name, id} => {
                writer.write_u8(1)
                    .write_str(&name)
                    .write_varint_u64(*id as u64);
            }
            LsmTreeEdit::DropCollection { name} => {
                writer.write_u8(2)
                    .write_str(&name);
            }
            LsmTreeEdit::WalRotation { log_id} => {
                writer.write_u8(3)
                    .write_varint_u64(*log_id);
            }
            LsmTreeEdit::ManifestRotation {  manifest_id } => {
                writer.write_u8(4)
                    .write_varint_u64(*manifest_id);
            }
            LsmTreeEdit::Flush { log_id, sst} => {
                writer.write_u8(5)
                    .write_varint_u64(*log_id);
                sst.write_to(&mut writer);
            }
            LsmTreeEdit::ScheduleMemtableForFlush() => {
                writer.write_u8(6);
            }
            LsmTreeEdit::FilesDetectedOnRestart { next_file_number} => {
                writer.write_u8(7)
                    .write_varint_u64(*next_file_number);
            }
        }
        writer.take_buffer()
    }

    pub fn try_from_vec(input: &[u8]) -> Result<LsmTreeEdit> {
        let reader = ByteReader::new(input);
        let edit = reader.read_u8()?;
        match edit {
            0 => {
                Ok(LsmTreeEdit::Snapshot(LsmTree::read_from(&reader)?))
            },
            1 => {
                let name = reader.read_str()?.to_string();
                let id = reader.read_varint_u64()? as u32;
                Ok(LsmTreeEdit::CreateCollection { name, id })
            }
            2 => {
                let name = reader.read_str()?.to_string();
                Ok(LsmTreeEdit::DropCollection { name })
            }
            3 => {
                let log_id = reader.read_varint_u64()?;
                Ok(LsmTreeEdit::WalRotation { log_id })
            }
            4 => {
                let manifest_id = reader.read_varint_u64()?;
                Ok(LsmTreeEdit::ManifestRotation { manifest_id })
            }
            5 => {
                let log_id = reader.read_varint_u64()?;
                let sst = Arc::new(SSTableMetadata::read_from(&reader)?);
                Ok(LsmTreeEdit::Flush { log_id, sst })
            }
            6 => {
                Ok(LsmTreeEdit::ScheduleMemtableForFlush {})
            }
            7 => {
                let next_file_number = reader.read_varint_u64()?;
                Ok(LsmTreeEdit::FilesDetectedOnRestart { next_file_number })
            }
            _ => Err(Error::new(ErrorKind::InvalidData, format!("LSMTreeEdit: {}", edit))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bson::Bson;
    use crate::io::byte_writer::ByteWriter;
    use crate::io::byte_reader::ByteReader;
    use crate::storage::internal_key::encode_record_key;
    use crate::util::bson_utils::BsonKey;

    #[test]
    fn test_find_overlapping() {
        // Create two overlapping SSTables:
        // sst1 covers keys 1 to 40 and is visible for snapshot 100..=200.
        // sst2 covers keys 40 to 60 and is visible for snapshot 201..=300.
        let sst1 = create_sstable(1, 0, 1, 40, 100, 200);
        let sst2 = create_sstable(2, 0, 20, 60, 201, 300);
        let level = Level::new(0, vec![sst1.clone(), sst2.clone()]);

        let empty : Vec<Arc<SSTableMetadata>> = vec![];

        // Key 2 falls only in sst1.
        let found = level.find(&record_key(2), 300);
        assert_eq!(vec![sst1.clone()], found);

        // Key 58 falls only in sst2.
        let found = level.find(&record_key(58), 300);
        assert_eq!(vec![sst2.clone()], found);

        // If the snapshot is bellow sst2 min sequence, nothing is returned
        let found = level.find(&record_key(58), 150);
        assert_eq!(empty, found);

        // Key 25 falls in sst1 and sst2
        let found = level.find(&record_key(25), 300);
        assert_eq!(vec![sst1.clone(), sst2.clone()], found);

        // A key out of range should yield None.
        assert_eq!(empty, level.find(&record_key(87), 300));
    }

    #[test]
    fn test_find_non_overlapping() {
        // For non-overlapping SSTables (e.g., L1+), assume:
        // sst1 covers 1 to 40 (snapshot 100..=200)
        // sst2 covers 41 to 60 (snapshot 201..=300)
        let sst1 = create_sstable(1, 1, 1, 40, 100, 200);
        let sst2 = create_sstable(2, 1, 41, 60, 201, 300);
        let level = Level::new(1, vec![sst1.clone(), sst2.clone()]);

        let empty : Vec<Arc<SSTableMetadata>> = vec![];

        // Key 2 should be found in sst1.
        let found = level.find(&record_key(2), 300);
        assert_eq!(vec![sst1.clone()], found);

        // Key 58 falls only in sst2.
        let found = level.find(&record_key(58), 300);
        assert_eq!(vec![sst2.clone()], found);

        // If the snapshot is bellow sst2 min sequence, nothing is returned
        let found = level.find(&record_key(58), 150);
        assert_eq!(empty, found);

        // A key out of range should yield None.
        assert_eq!(empty, level.find(&record_key(87), 300));
    }

    #[test]
    fn test_find_range_overlapping() {
        // Create three overlapping SSTables:
        // sst1: keys 1 to 60, visible for snapshot 100..=200.
        // sst2: keys 40 to 100, visible for snapshot 201..=300.
        // sst3: keys 70 to 100, visible for snapshot 301..=400.
        let sst1 = create_sstable(1, 0, 1, 60, 100, 200);
        let sst2 = create_sstable(2, 0, 40, 100, 201, 300);
        let sst3 = create_sstable(3, 0, 70, 100, 301, 400);
        let coll = Level::new(0, vec![sst1.clone(), sst2.clone(), sst3.clone()]);

        let interval = Interval::closed(record_key(50), record_key(100));

        let results = coll.find_range(&interval, 400);
        assert_eq!(3, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::greater_than(record_key(60));

        let results = coll.find_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::closed_open(record_key(61), record_key(70));

        let results = coll.find_range(&interval, 400);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst2));

        let interval = Interval::at_most(record_key(60));

        let results = coll.find_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));

        let results = coll.find_range(&interval, 150);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst1));

        let interval = Interval::greater_than(record_key(100));

        let results = coll.find_range(&interval, 400);
        assert_eq!(0, results.len());
    }
    #[test]
    fn test_find_range_non_overlapping() {
        // Create three overlapping SSTables:
        // sst1: keys 1 to 60, visible for snapshot 100..=200.
        // sst2: keys 61 to 69, visible for snapshot 150..=300.
        // sst3: keys 70 to 100, visible for snapshot 232..=400.
        let sst1 = create_sstable(1, 0, 1, 60, 100, 200);
        let sst2 = create_sstable(2, 0, 61, 69, 150, 300);
        let sst3 = create_sstable(3, 0, 70, 100, 232, 400);
        let coll = Level::new(1, vec![sst1.clone(), sst2.clone(), sst3.clone()]);

        let interval = Interval::closed(record_key(50), record_key(100));

        let results = coll.find_range(&interval, 400);
        assert_eq!(3, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::greater_than(record_key(60));

        let results = coll.find_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::closed_open(record_key(61), record_key(70));

        let results = coll.find_range(&interval, 400);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst2));

        let interval = Interval::at_most(record_key(62));

        let results = coll.find_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));

        let results = coll.find_range(&interval, 150);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));

        let results = coll.find_range(&interval, 100);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst1));

        let interval = Interval::greater_than(record_key(100));

        let results = coll.find_range(&interval, 400);
        assert_eq!(0, results.len());
    }

    #[test]
    fn test_index_metadata_serialization() {
        check_serialization_round_trip(IndexMetadata { id: 11, name: "by_name".to_string() });
    }

    #[test]
    fn test_collections_metadata_serialization() {
        check_serialization_round_trip(create_collections_with_indexes());
    }

    #[test]
    fn test_sst_metadata_serialization() {
        check_serialization_round_trip(create_level_0_sstable());
    }

    #[test]
    fn test_level_serialization() {
        check_serialization_round_trip(create_level_1());
    }

    #[test]
    fn test_levels_tree_serialization() {
        check_serialization_round_trip(create_levels());
    }

    #[test]
    fn test_lsm_tree_serialization() {
        check_serialization_round_trip(create_lsm_tree());
    }

    #[test]
    fn test_create_and_drop_collection_serialization() {
        let edit = LsmTreeEdit::CreateCollection {
            name: "my_collection".to_string(),
            id: 42,
        };
        check_edit_serialization_roundtrip(edit);

        let edit = LsmTreeEdit::DropCollection {
            name: "my_collection".to_string(),
        };
        check_edit_serialization_roundtrip(edit);
    }

    #[test]
    fn test_schedule_memtable_flush_serialization() {
        check_edit_serialization_roundtrip(LsmTreeEdit::ScheduleMemtableForFlush());
    }

    #[test]
    fn test_wal_and_manifest_rotation_serialization() {
        check_edit_serialization_roundtrip(LsmTreeEdit::WalRotation { log_id: 123 });
        check_edit_serialization_roundtrip(LsmTreeEdit::ManifestRotation { manifest_id: 456 });
    }

    #[test]
    fn test_files_detected_on_restart_serialization() {
        check_edit_serialization_roundtrip(LsmTreeEdit::FilesDetectedOnRestart {
            next_file_number: 789,
        });
    }

    #[test]
    fn test_flush_serialization
    () {
        let sst = create_sstable(1, 0, 1, 250, 100, 200);
        let edit = LsmTreeEdit::Flush {
            log_id: 10,
            sst: sst.clone(),
        };

        check_edit_serialization_roundtrip(edit);
    }

    #[test]
    fn test_snapshot_serialization() {
        let edit = LsmTreeEdit::Snapshot(create_lsm_tree());
        check_edit_serialization_roundtrip(edit);
    }

    #[test]
    fn test_apply_create_and_drop_collection() {
        let tree = LsmTree::new(1, 2);

        let tree = tree.apply(&LsmTreeEdit::CreateCollection {
            name: "docs".to_string(),
            id: 42,
        });
        assert!(tree.collections.contains_key("docs"));

        let tree = tree.apply(&LsmTreeEdit::DropCollection {
            name: "docs".to_string(),
        });
        assert!(!tree.collections.contains_key("docs"));
    }

    #[test]
    fn test_apply_schedule_and_flush_memtable() {
        let tree = LsmTree::new(1, 2);
        assert_eq!(tree.imm_memtables.len(), 0);

        let tree = tree.apply(&LsmTreeEdit::ScheduleMemtableForFlush());
        assert_eq!(tree.imm_memtables.len(), 1);

        let sst = Arc::new(SSTableMetadata::new(
            7, 0, b"a", b"z", 100, 200,
        ));
        let tree = tree.apply(&LsmTreeEdit::Flush {
            log_id: 5,
            sst: sst.clone(),
        });

        assert_eq!(tree.oldest_log_number, 5);
        assert_eq!(tree.last_sequence_number, 200);
        assert_eq!(tree.imm_memtables.len(), 0);
        assert!(tree.sst_levels.levels[0]
            .as_ref()
            .find(b"b", 200)
            .contains(&sst));
    }

    #[test]
    fn test_apply_wal_and_manifest_rotation() {
        let tree = LsmTree::new(1, 2);

        let tree = tree.apply(&LsmTreeEdit::WalRotation { log_id: 99 });
        assert_eq!(tree.current_log_number, 99);
        assert_eq!(tree.next_file_number, 100);

        let tree = tree.apply(&LsmTreeEdit::ManifestRotation { manifest_id: 150 });
        assert_eq!(tree.next_file_number, 151);
    }

    #[test]
    fn test_apply_files_detected_on_restart() {
        let tree = LsmTree::new(1, 2);
        let tree = tree.apply(&LsmTreeEdit::FilesDetectedOnRestart {
            next_file_number: 200,
        });
        assert_eq!(tree.next_file_number, 200);
    }

    fn record_key(number: i32) -> Vec<u8> {
        let user_key = Bson::Int32(number).try_into_key().unwrap();
        encode_record_key(1, 0, &user_key)
    }

    fn create_lsm_tree() -> LsmTree {
        // Create an LsmTree with a specific log number.
        let mut tree = LsmTree::new(456, 1024);

        // Create two collections.
        // Collection "users" with no indexes.
        let col_users = CollectionMetadata::new(1, "users".to_string());

        // Collection "products" with two indexes.
        let col_products = create_collections_with_indexes();

        // Populate the collections map.
        let mut collections = BTreeMap::new();
        collections.insert("users".to_string(), Arc::new(col_users));
        collections.insert("products".to_string(), Arc::new(col_products));
        tree.collections = Arc::new(collections);

        let levels = create_levels();
        tree.sst_levels = Arc::new(levels);
        tree
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

    fn create_levels() -> Levels {
        let levels = Levels {
            levels: vec![Arc::new(create_level_0()), Arc::new(create_level_1())]
        };
        levels
    }

    fn create_level_0() -> Level {
        Level::new(0, vec![Arc::new(create_level_0_sstable())])
    }

    fn create_level_0_sstable() -> SSTableMetadata {
        SSTableMetadata::new(
            1,
            0,
            &b"a".to_vec(),
            &b"m".to_vec(),
            100,
            200
        )
    }

    fn create_level_1() -> Level {
        // Level 1 with two SSTables.
        let sstable1 = SSTableMetadata {
            number: 2,
            level: 1,
            min_key: b"n".to_vec(),
            max_key: b"z".to_vec(),
            min_sequence_number: 201,
            max_sequence_number: 300,
        };
        let sstable2 = SSTableMetadata {
            number: 3,
            level: 1,
            min_key: b"aa".to_vec(),
            max_key: b"zz".to_vec(),
            min_sequence_number: 301,
            max_sequence_number: 400,
        };
        Level::new(1, vec![Arc::new(sstable1), Arc::new(sstable2)])
    }

    fn create_sstable(
        number: u32,
        level: u32,
        min_key: i32,
        max_key: i32,
        min_seq: u64,
        max_seq: u64,
    ) -> Arc<SSTableMetadata> {
        Arc::new(SSTableMetadata::new(
            number,
            level,
            &record_key(min_key),
            &record_key(max_key),
            min_seq,
            max_seq
        ))
    }

    fn check_serialization_round_trip<T>(element: T)
    where
        T: fmt::Debug + PartialEq + SnapshotElement,
    {
        // Serialize the element.
        let mut writer = ByteWriter::new();
        element.write_to(&mut writer);
        let bytes = writer.take_buffer();

        // Deserialize the element from the serialized bytes.
        let reader = ByteReader::new(&bytes);
        let deserialized = SnapshotElement::read_from(&reader)
            .expect("Deserialization should succeed");

        assert_eq!(element, deserialized);

        // Re-serialize the deserialized element.
        let mut writer2 = ByteWriter::new();
        deserialized.write_to(&mut writer2);
        let bytes2 = writer2.take_buffer();

        // Verify that the round-trip serialization is lossless.
        assert_eq!(bytes, bytes2);
    }

    fn check_edit_serialization_roundtrip(edit: LsmTreeEdit) {
        let bytes = edit.to_vec();
        let parsed = LsmTreeEdit::try_from_vec(&bytes).expect("Deserialization should succeed");
        assert_eq!(&edit, &parsed);
    }
}