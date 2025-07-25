use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::lsm_version::Level::{NonOverlapping, Overlapping};
use crate::util::interval::Interval;
use std::cmp::Ordering;
use std::io::Result;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use crate::io::serializable::Serializable;

/// Represents the persisted physical state of the LSM tree, excluding memtables.
///
/// `LsmVersion` tracks WAL file IDs, file number allocation, sequence numbers, and
/// the current SSTable layout across all levels. It is the durable version state
/// written to the manifest and reconstructed on startup.
#[derive(Default, Debug, PartialEq)]
pub struct LsmVersion {
    /// The number of the current write-ahead log file
    pub current_log_number: u64,
    /// The number of the oldest write-ahead log file still containing some non persisted data
    pub oldest_log_number: u64,
    /// The next file number
    pub next_file_number: u64,
    /// The last sequence number persisted on disk
    pub last_sequence_number: u64,
    /// The SSTables per levels
    pub sst_levels: Arc<Levels>,
}

impl LsmVersion {
    pub fn new(current_log_number: u64, next_file_number: u64) -> Self {
        LsmVersion {
            current_log_number,
            oldest_log_number: current_log_number,
            next_file_number,
            last_sequence_number: 0,
            sst_levels: Default::default(),
        }
    }

    pub fn with_new_log_file(&self, log_number: u64) -> LsmVersion {
        // As the increase of the next_file_number, the rotation and the edit are applied
        // together under the manifest lock, we are guaranty that the next_file_number will
        // be log_number + 1 unless we are in replay in which case the next_file_number
        // might already be bigger.
        let next_file_number = self.next_file_number.max(log_number + 1);
        LsmVersion {
            current_log_number: log_number,
            oldest_log_number: self.oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number,
            sst_levels: self.sst_levels.clone(),
        }
    }

    pub fn with_flushed_sstable(
        &self,
        oldest_log_number: u64,
        sst: &Arc<SSTableMetadata>,
    ) -> LsmVersion {
        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number,
            last_sequence_number: sst.max_sequence_number,
            next_file_number: self.next_file_number,
            sst_levels: Arc::new(self.sst_levels.add_sst(sst.clone())),
        }
    }

    pub fn manifest_rotation(&self, manifest_number: u64) -> LsmVersion {
        // As the increase of the next_file_number, the rotation and the edit are applied
        // together under the manifest lock, we are guaranty that the next_file_number will
        // be manifest_id + 1
        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number: self.oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number: manifest_number + 1,
            sst_levels: self.sst_levels.clone(),
        }
    }

    pub fn adjust_file_number(&self, next_file_number: u64) -> LsmVersion {
        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number: self.oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number,
            sst_levels: self.sst_levels.clone(),
        }
    }

    pub fn find<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.sst_levels.find(record_key, snapshot)
    }

    pub fn find_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.sst_levels.find_range(record_key_range, snapshot)
    }
}

impl Serializable for LsmVersion {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let current_log_number = reader.read_varint_u64()?;
        let oldest_log_number = reader.read_varint_u64()?;
        let next_file_number = reader.read_varint_u64()?;
        let last_sequence_number = reader.read_varint_u64()?;
        let sst_levels = Arc::new(Levels::read_from(reader)?);

        Ok(LsmVersion {
            current_log_number,
            oldest_log_number,
            next_file_number,
            last_sequence_number,
            sst_levels,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.current_log_number);
        writer.write_varint_u64(self.oldest_log_number);
        writer.write_varint_u64(self.next_file_number);
        writer.write_varint_u64(self.last_sequence_number);
        self.sst_levels.write_to(writer)
    }
}

/// Represents the hierarchy of SSTables organized by levels (L0 to Ln).
///
/// Each level holds a set of SSTables. Level 0 may contain overlapping tables,
/// while Level 1+ must be non-overlapping and sorted.
#[derive(Default, Debug, PartialEq)]
pub struct Levels {
    levels: Vec<Arc<Level>>,
}

impl Levels {

    pub fn add_sst(&self, sst: Arc<SSTableMetadata>) -> Self {
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
                Level::new(sst.level, vec![sst.clone()], sst.size)
            } else {
                Level::empty(sst.level)
            };
            new_levels.push(Arc::new(level));
        }

        Levels { levels: new_levels }
    }

    pub fn find<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.levels
            .iter()
            .flat_map(move |level| level.find(record_key, snapshot))
    }

    pub fn level(&self, level: usize) -> Option<&Level> {
        if level >= self.levels.len() {
            None
        } else {
            Some(self.levels[level].as_ref())
        }
    }

    pub fn sst_count(&self) -> usize {
        self.levels.iter().map(|level| level.sst_count()).sum()
    }

    pub fn total_bytes(&self) -> u64 {
        self.levels.iter().map( |level| level.total_bytes()).sum()
    }

    pub fn find_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.levels
            .iter()
            .flat_map(move |level| level.find_range(record_key_range, snapshot).into_iter())
    }
}

impl Serializable for Levels {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let levels= Vec::<Arc<Level>>::read_from(reader)?;
        Ok(Levels { levels })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.levels.write_to(writer);
    }
}

/// An individual level in the LSM tree, containing SSTables.
///
/// - `Overlapping`: Used for L0, tables may have overlapping key ranges.
/// - `NonOverlapping`: Used for L1+, tables are sorted and non-overlapping.
#[derive(Debug, PartialEq, Clone)]
pub enum Level {
    // For L0, files may overlap so we simply store a Vec.
    Overlapping {
        level: u32,
        sstables: Vec<Arc<SSTableMetadata>>,
        size: u64, // Total size of all SSTables in this level
    },
    // For L1+ levels, files are non-overlapping and sorted by min_key.
    NonOverlapping {
        level: u32,
        sstables: Vec<Arc<SSTableMetadata>>,
        size: u64, // Total size of all SSTables in this level
    },
}

impl Level {
    pub fn empty(level: u32) -> Self {
        if level == 0 {
            Overlapping {
                level: 0,
                sstables: Vec::new(),
                size: 0,
            }
        } else {
            NonOverlapping {
                level,
                sstables: Vec::new(),
                size: 0,
            }
        }
    }

    pub fn new(level: u32, mut sstables: Vec<Arc<SSTableMetadata>>, size: u64) -> Self {
        if level == 0 {
            Overlapping { level: 0, sstables, size }
        } else {
            sstables.sort_by(|a, b| a.min_key.cmp(&b.min_key));
            NonOverlapping { level, sstables, size }
        }
    }

    fn add(&self, sst: Arc<SSTableMetadata>) -> Self {
        match &self {
            Overlapping { level, sstables, size } | NonOverlapping { level, sstables, size } => {
                let size = size + sst.size;
                let mut copy = sstables.iter().cloned().collect::<Vec<_>>();
                copy.push(sst);
                Level::new(*level, copy, size)
            }
        }
    }

    /// Finds the SSTables that contains the key and are visible under the given snapshot.
    pub fn find<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
    ) -> Box<dyn Iterator<Item = Arc<SSTableMetadata>> + 'a> {
        match self {
            Overlapping { sstables, .. } => Box::new(
                sstables
                    .iter()
                    .rev() // Iterating in reverse ensures to find the newest version first, satisfying visibility rules.
                    .filter(move |sst| {
                        record_key >= sst.min_key.as_slice()
                            && record_key <= sst.max_key.as_slice()
                            && snapshot >= sst.min_sequence_number
                    })
                    .cloned(),
            ),
            NonOverlapping { sstables, .. } => {
                let iter = sstables
                    .binary_search_by(|sst| {
                        if record_key < sst.min_key.as_slice() {
                            Ordering::Greater
                        } else if record_key > sst.max_key.as_slice() {
                            Ordering::Less
                        } else {
                            Ordering::Equal
                        }
                    })
                    .ok()
                    .into_iter()
                    .filter_map(move |i| {
                        let sst = &sstables[i];
                        if snapshot >= sst.min_sequence_number {
                            Some(sst.clone())
                        } else {
                            None
                        }
                    });
                Box::new(iter)
            }
        }
    }

    /// Finds all SSTables that overlap the given interval and are visible under the snapshot.
    ///
    /// The `interval` parameter is an instance of your provided `Interval` type.
    pub fn find_range(
        &self,
        record_key_range: &Interval<Vec<u8>>,
        snapshot: u64,
    ) -> Vec<Arc<SSTableMetadata>> {
        match self {
            Overlapping { level: _, sstables, size: _ } => sstables
                .iter()
                .filter(|sst| overlaps(record_key_range, sst) && snapshot >= sst.min_sequence_number)
                .cloned()
                .collect(),
            NonOverlapping { level: _, sstables, size: _ } => {
                // Use binary search to find the first candidate SSTable.
                // Here we use the interval's start bound as the lower limit.
                let lower = match record_key_range.start_bound() {
                    Bound::Included(val) | Bound::Excluded(val) => val,
                    Bound::Unbounded => &vec![], // smallest possible key
                };

                let start_idx = sstables
                    .binary_search_by(|sst| {
                        if sst.max_key.as_slice() < lower.as_slice() {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    })
                    .unwrap_or_else(|idx| idx);

                let mut result = Vec::new();
                for sst in &sstables[start_idx..] {
                    // If the SSTable's min_key is beyond the interval's end, stop scanning.
                    if let Bound::Included(end) | Bound::Excluded(end) = record_key_range.end_bound() {
                        if sst.min_key.as_slice() > end.as_slice() {
                            break;
                        }
                    }
                    if overlaps(record_key_range, sst) && snapshot >= sst.min_sequence_number {
                        result.push(sst.clone());
                    }
                }
                result
            }
        }
    }

    pub fn total_bytes(&self) -> u64 {
        match self {
            Overlapping { size, .. } | NonOverlapping { size, .. } => *size,
        }
    }

    pub fn sst_count(&self) -> usize {
        match self {
            Overlapping { sstables, .. } | NonOverlapping { sstables, .. } => sstables.len(),
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

impl Serializable for Level {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let level = reader.read_varint_u32()?;
        let sstables = Vec::<Arc<SSTableMetadata>>::read_from(reader)?;
        let size = sstables.iter().map(|sst| sst.size).sum();
        match level {
            0 => Ok(Overlapping { level, sstables, size }),
            _ => Ok(NonOverlapping { level, sstables, size }),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match &self {
            Overlapping { level, sstables, size: _size } | NonOverlapping { level, sstables, size: _size } => {
                writer.write_varint_u32(*level);
                sstables.write_to(writer);
            }
        }
    }
}

/// Describes the metadata of an on-disk SSTable.
///
/// Includes its ID, level, key range, and sequence number range for visibility filtering.
#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub struct SSTableMetadata {
    pub number: u64,
    pub level: u32,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_sequence_number: u64,
    pub max_sequence_number: u64,
    pub size: u64, // Size of the SSTable file in bytes
}

impl SSTableMetadata {
    pub fn new(
        number: u64,
        level: u32,
        min_key: &[u8],
        max_key: &[u8],
        min_seq: u64,
        max_seq: u64,
        size: u64,
    ) -> SSTableMetadata {
        SSTableMetadata {
            number,
            level,
            min_key: min_key.to_vec(),
            max_key: max_key.to_vec(),
            min_sequence_number: min_seq,
            max_sequence_number: max_seq,
            size,
        }
    }
}

impl Serializable for SSTableMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<SSTableMetadata> {
        Ok(SSTableMetadata {
            number: reader.read_varint_u64()?,
            level: reader.read_varint_u32()?,
            min_key: reader.read_length_prefixed_slice()?.to_vec(),
            max_key: reader.read_length_prefixed_slice()?.to_vec(),
            min_sequence_number: reader.read_varint_u64()?,
            max_sequence_number: reader.read_varint_u64()?,
            size: reader.read_varint_u64()?,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer
            .write_varint_u64(self.number)
            .write_varint_u32(self.level)
            .write_length_prefixed_slice(&self.min_key)
            .write_length_prefixed_slice(&self.max_key)
            .write_varint_u64(self.min_sequence_number)
            .write_varint_u64(self.max_sequence_number)
            .write_varint_u64(self.size);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::encode_record_key;
    use crate::io::serializable::check_serialization_round_trip;
    use crate::util::bson_utils::BsonKey;
    use bson::Bson;
    use std::sync::Arc;

    #[test]
    fn test_find_overlapping() {
        // Create two overlapping SSTables:
        // sst1 covers keys 1 to 40 and is visible for snapshot 100..=200.
        // sst2 covers keys 20 to 60 and is visible for snapshot 201..=300.
        let sst1 = create_sstable(1, 0, 1, 40, 100, 200, 53_248);
        let sst2 = create_sstable(2, 0, 20, 60, 201, 300, 79_872);
        let level = Level::new(0, vec![sst1.clone(), sst2.clone()], 133_120);

        let empty: Vec<Arc<SSTableMetadata>> = vec![];

        // Key 2 falls only in sst1.
        let found: Vec<_> = level.find(&record_key(2), 300).collect();
        assert_eq!(vec![sst1.clone()], found);

        // Key 58 falls only in sst2.
        let found: Vec<_> = level.find(&record_key(58), 300).collect();
        assert_eq!(vec![sst2.clone()], found);

        // If the snapshot is bellow sst2 min sequence, nothing is returned
        let found: Vec<_> = level.find(&record_key(58), 150).collect();
        assert_eq!(empty, found);

        // Key 25 falls in sst1 and sst2
        let found: Vec<_> = level.find(&record_key(25), 300).collect();
        assert_eq!(vec![sst2.clone(), sst1.clone()], found);

        let found: Vec<_> = level.find(&record_key(87), 300).collect();
        // A key out of range should yield None.
        assert_eq!(empty, found)
    }

    #[test]
    fn test_find_non_overlapping() {
        // For non-overlapping SSTables (e.g., L1+), assume:
        // sst1 covers 1 to 40 (snapshot 100..=200)
        // sst2 covers 41 to 60 (snapshot 201..=300)
        let sst1 = create_sstable(1, 1, 1, 40, 100, 200, 53_248);
        let sst2 = create_sstable(2, 1, 41, 60, 201, 300, 79_872);
        let level = Level::new(1, vec![sst1.clone(), sst2.clone()], 133_120);

        let empty: Vec<Arc<SSTableMetadata>> = vec![];

        // Key 2 should be found in sst1.
        let found: Vec<_> = level.find(&record_key(2), 300).collect();
        assert_eq!(vec![sst1.clone()], found);

        // Key 58 falls only in sst2.
        let found: Vec<_> = level.find(&record_key(58), 300).collect();
        assert_eq!(vec![sst2.clone()], found);

        // If the snapshot is bellow sst2 min sequence, nothing is returned
        let found: Vec<_> = level.find(&record_key(58), 150).collect();
        assert_eq!(empty, found);

        // A key out of range should yield None.
        let found: Vec<_> = level.find(&record_key(87), 300).collect();
        assert_eq!(empty, found);
    }

    #[test]
    fn test_find_range_overlapping() {
        // Create three overlapping SSTables:
        // sst1: keys 1 to 60, visible for snapshot 100..=200.
        // sst2: keys 40 to 100, visible for snapshot 201..=300.
        // sst3: keys 70 to 100, visible for snapshot 301..=400.
        let sst1 = create_sstable(1, 0, 1, 60, 100, 200, 53_248);
        let sst2 = create_sstable(2, 0, 40, 100, 201, 300, 79_872);
        let sst3 = create_sstable(3, 0, 70, 100, 301, 400, 32_768);
        let coll = Level::new(0, vec![sst1.clone(), sst2.clone(), sst3.clone()], 165_888);

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
        let sst1 = create_sstable(1, 0, 1, 60, 100, 200, 53_248);
        let sst2 = create_sstable(2, 0, 61, 69, 150, 300, 79_872);
        let sst3 = create_sstable(3, 0, 70, 100, 232, 400, 32_768);
        let coll = Level::new(1, vec![sst1.clone(), sst2.clone(), sst3.clone()], 165_888);

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
    fn test_lsm_version_serialization() {
        check_serialization_round_trip(create_lsm_version());
    }

    fn record_key(number: i32) -> Vec<u8> {
        let user_key = Bson::Int32(number).try_into_key().unwrap();
        encode_record_key(1, 0, &user_key)
    }

    fn create_lsm_version() -> LsmVersion {
        // Create an LsmVersion with a specific log number.
        let mut version = LsmVersion::new(456, 1024);
        let levels = create_levels();
        version.sst_levels = Arc::new(levels);
        version
    }

    fn create_levels() -> Levels {
        let levels = Levels {
            levels: vec![Arc::new(create_level_0()), Arc::new(create_level_1())],
        };
        levels
    }

    fn create_level_0() -> Level {
        Level::new(0, vec![Arc::new(create_level_0_sstable())], 79_872)
    }

    fn create_level_0_sstable() -> SSTableMetadata {
        SSTableMetadata::new(1, 0, &b"a".to_vec(), &b"m".to_vec(), 100, 200, 79_872)
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
            size: 1024,
        };
        let sstable2 = SSTableMetadata {
            number: 3,
            level: 1,
            min_key: b"aa".to_vec(),
            max_key: b"zz".to_vec(),
            min_sequence_number: 301,
            max_sequence_number: 400,
            size: 1024,
        };
        Level::new(1, vec![Arc::new(sstable1), Arc::new(sstable2)], 2048)
    }

    fn create_sstable(
        number: u64,
        level: u32,
        min_key: i32,
        max_key: i32,
        min_seq: u64,
        max_seq: u64,
        size: u64,
    ) -> Arc<SSTableMetadata> {
        Arc::new(SSTableMetadata::new(
            number,
            level,
            &record_key(min_key),
            &record_key(max_key),
            min_seq,
            max_seq,
            size,
        ))
    }
}
