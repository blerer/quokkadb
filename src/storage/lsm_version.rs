use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::storage::lsm_version::Level::{NonOverlapping, Overlapping};
use crate::util::interval::Interval;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::iter::once;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use bson::Bson;
use crate::io::serializable::Serializable;
use crate::options::options::Options;
use crate::storage::internal_key::encode_record_key;
use crate::util::bson_utils::BsonKey;

/// Represents the persisted physical state of the LSM tree, excluding memtables.
///
/// `LsmVersion` tracks WAL file IDs, file number allocation, sequence numbers, and
/// the current SSTable layout across all levels. It is the durable version state
/// written to the manifest and reconstructed on startup.
#[derive(Debug, PartialEq)]
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
    /// The metadata of the drops associated to the unflushed data
    pub pending_drops: Vec<Arc<DropMetadata>>,
}

impl LsmVersion {
    pub fn new(current_log_number: u64, next_file_number: u64, max_levels: usize) -> Self {
        LsmVersion {
            current_log_number,
            oldest_log_number: current_log_number,
            next_file_number,
            last_sequence_number: 0,
            sst_levels: Arc::new(Levels::new(max_levels)),
            pending_drops: Vec::with_capacity(0),
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
            pending_drops: self.pending_drops.clone(),
        }
    }

    pub fn with_flushed_sstable(
        &self,
        oldest_log_number: u64,
        sst: &Arc<SSTableMetadata>,
    ) -> LsmVersion {

        let (pending_drops, drops): (Vec<_>, Vec<_>) = self.pending_drops
            .iter()
            .cloned()
            .partition(|drop| drop.drop_sequence_number > sst.max_sequence_number);

        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number,
            last_sequence_number: sst.max_sequence_number,
            next_file_number: self.next_file_number,
            sst_levels: Arc::new(self.sst_levels.add(0, once(sst.clone()), drops)),
            pending_drops,
        }
    }

    pub fn with_ignored_empty_memtable(&self, oldest_log_number: u64) -> LsmVersion {
        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number: self.next_file_number,
            sst_levels: self.sst_levels.clone(),
            pending_drops: self.pending_drops.clone(),
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
            pending_drops: self.pending_drops.clone(),
        }
    }

    pub fn adjust_file_number(&self, next_file_number: u64) -> LsmVersion {
        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number: self.oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number,
            sst_levels: self.sst_levels.clone(),
            pending_drops: self.pending_drops.clone(),
        }
    }

    pub fn with_compaction(&self,
                           output_level: usize,
                           removed_sstables: &Vec<Arc<SSTableMetadata>>,
                           added_sstables: &Vec<Arc<SSTableMetadata>>,
                           drops: &Vec<Arc<DropMetadata>>
    ) -> LsmVersion {

        let input_level = output_level - 1;

        let (removed_from_input, removed_from_output): (HashSet<_>, HashSet<_>) = removed_sstables
            .into_iter()
            .cloned()
            .partition(|sst| sst.level == input_level as u8);

        let drops : HashSet<Arc<DropMetadata>> = drops.into_iter().cloned().collect();

        let next_file_number = self.next_file_number.max(added_sstables.iter().map(|sst| sst.number).max().unwrap_or(0) + 1);

        let sst_levels = Arc::new(self.sst_levels.remove(input_level, &removed_from_input, &drops)
            .remove(output_level, &removed_from_output, &HashSet::new())
            .add(output_level, added_sstables.into_iter().cloned(), drops));

        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number: self.oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number,
            sst_levels,
            pending_drops: self.pending_drops.clone(),
        }
    }

    pub fn add_collection_drop(&self, collection: u32, sequence_number: u64) -> LsmVersion {
        let drop = DropMetadata::new_collection_drop(collection, sequence_number);
        self.add_drop(drop)
    }

    fn add_drop(&self, drop: Arc<DropMetadata>) -> LsmVersion {
        let mut copy = self.pending_drops.iter().cloned().collect::<Vec<_>>();
        copy.push(drop.clone());
        LsmVersion {
            current_log_number: self.current_log_number,
            oldest_log_number: self.oldest_log_number,
            last_sequence_number: self.last_sequence_number,
            next_file_number: self.next_file_number,
            sst_levels: self.sst_levels.clone(),
            pending_drops: copy,
        }
    }

    /// Returns the drops with a drop_sequence_number smaller or equal to the given sequence_number.
    pub fn get_drops_before_or_at(&self, sequence_number: u64) -> Vec<Arc<DropMetadata>> {
        let mut result = Vec::new();
        for drop in &self.pending_drops {
            if drop.drop_sequence_number > sequence_number {
                break;
            }
            result.push(drop.clone());
        }
        result
    }

    pub fn find_sstables<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
        min_snapshot: Option<u64>,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.sst_levels.find_sstables(record_key, snapshot, min_snapshot)
    }

    pub fn find_sstables_in_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.sst_levels.find_sstables_in_range(record_key_range, snapshot)
    }
}

impl Serializable for LsmVersion {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let current_log_number = reader.read_varint_u64()?;
        let oldest_log_number = reader.read_varint_u64()?;
        let next_file_number = reader.read_varint_u64()?;
        let last_sequence_number = reader.read_varint_u64()?;
        let sst_levels = Arc::new(Levels::read_from(reader)?);
        let pending_drops = Vec::<Arc<DropMetadata>>::read_from(reader)?;

        Ok(LsmVersion {
            current_log_number,
            oldest_log_number,
            next_file_number,
            last_sequence_number,
            sst_levels,
            pending_drops,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.current_log_number);
        writer.write_varint_u64(self.oldest_log_number);
        writer.write_varint_u64(self.next_file_number);
        writer.write_varint_u64(self.last_sequence_number);
        self.sst_levels.write_to(writer);
        self.pending_drops.write_to(writer);
    }
}

/// Represents the hierarchy of SSTables organized by levels (L0 to Ln).
///
/// Each level holds a set of SSTables. Level 0 may contain overlapping tables,
/// while Level 1+ must be non-overlapping and sorted.
#[derive(Debug, PartialEq)]
pub struct Levels {
    levels: Vec<Arc<Level>>,
}

impl Levels {

    pub fn new(max_levels: usize) -> Self {
        let levels: Vec<_> = (0..max_levels)
            .map(|i| Arc::new(Level::empty(i as u8)))
            .collect();
        Levels { levels }
    }

    pub fn add<S, D>(&self, level: usize, sstables: S, drops: D) -> Self
    where
        S: IntoIterator<Item = Arc<SSTableMetadata>>,
        D: IntoIterator<Item = Arc<DropMetadata>>,
    {
        let mut new_levels: Vec<_>  = self.levels.iter().cloned().collect();
        new_levels[level] = Arc::new(self.levels[level].add(sstables, drops));
        Levels { levels: new_levels }
    }

    pub fn remove(&self,
                  level: usize,
                  sstables: &HashSet<Arc<SSTableMetadata>>,
                  drops: &HashSet<Arc<DropMetadata>>
    ) -> Self
    {
        let mut new_levels: Vec<_>  = self.levels.iter().cloned().collect();
        new_levels[level] = Arc::new(self.levels[level].remove(sstables, drops));
        Levels { levels: new_levels }
    }

    pub fn find_sstables<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
        min_snapshot: Option<u64>,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.levels
            .iter()
            .flat_map(move |level| level.find_sstables(record_key, snapshot, min_snapshot))
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

    pub fn live_sst_numbers(&self) -> HashSet<u64> {
        self.levels
            .iter()
            .flat_map(|level| level.sstables().iter().map(|sst| sst.number))
            .collect()
    }

    pub fn total_bytes(&self) -> u64 {
        self.levels.iter().map( |level| level.total_bytes()).sum()
    }

    pub fn find_sstables_in_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
        snapshot: u64,
    ) -> impl Iterator<Item = Arc<SSTableMetadata>> + 'a {
        self.levels
            .iter()
            .flat_map(move |level| level.find_sstables_in_range(record_key_range, snapshot).into_iter())
    }

    pub fn find_drops_in_range<'a>(
        &'a self,
        record_key_range: &'a Interval<Vec<u8>>,
    ) -> impl Iterator<Item = Arc<DropMetadata>> + 'a {
        self.levels
            .iter()
            .flat_map(move |level| level.find_drops_in_range(record_key_range).into_iter())
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
        level: u8,
        sstables: Vec<Arc<SSTableMetadata>>,
        drops: Vec<Arc<DropMetadata>>, // Drops that are relevant for this level, used to filter out data during compaction.
        size: u64, // Total size of all SSTables in this level
    },
    // For L1+ levels, files are non-overlapping and sorted by min_key.
    NonOverlapping {
        level: u8,
        sstables: Vec<Arc<SSTableMetadata>>,
        drops: Vec<Arc<DropMetadata>>, // Drops that are relevant for this level, used to filter out data during compaction.
        size: u64, // Total size of all SSTables in this level
    },
}

impl Level {
    pub fn empty(level: u8) -> Self {
        if level == 0 {
            Overlapping {
                level: 0,
                sstables: Vec::new(),
                drops: Vec::new(),
                size: 0,
            }
        } else {
            NonOverlapping {
                level,
                sstables: Vec::new(),
                drops: Vec::new(),
                size: 0,
            }
        }
    }

    pub fn new(
        level: u8,
        mut sstables: Vec<Arc<SSTableMetadata>>,
        drops: Vec<Arc<DropMetadata>>,
        size: u64
    ) -> Self {
        if level == 0 {
            Overlapping { level: 0, sstables, drops, size }
        } else {
            sstables.sort_by(|a, b| a.min_key.cmp(&b.min_key));
            NonOverlapping { level, sstables, drops, size }
        }
    }

    fn add<S, D>(&self, new_sstables: S, new_drops: D) -> Self
    where
        S: IntoIterator<Item = Arc<SSTableMetadata>>,
        D: IntoIterator<Item = Arc<DropMetadata>>,
    {
        match &self {
            Overlapping { level, sstables, drops, size }
            | NonOverlapping { level, sstables, drops, size } => {
                let mut sstables_copy = sstables.iter().cloned().collect::<Vec<_>>();
                let new_sstables: Vec<_> = new_sstables.into_iter().collect();
                let new_size = size + new_sstables.iter().map(|sst| sst.size).sum::<u64>();
                sstables_copy.extend(new_sstables);
                sstables_copy.sort_by(|a, b| a.min_key.cmp(&b.min_key));

                let mut all_drops: Vec<_> = drops.iter().cloned().collect();
                all_drops.extend(new_drops);
                let merged_drops = merge_and_split_drops(all_drops);

                Level::new(*level, sstables_copy, merged_drops, new_size)
            }
        }
    }

    fn remove(&self, sstables_to_remove: &HashSet<Arc<SSTableMetadata>>, drops_to_remove: &HashSet<Arc<DropMetadata>>) -> Self
    {
        match &self {
            Overlapping { level, sstables, drops, size }
            | NonOverlapping { level, sstables, drops, size } => {
                let new_sstables = sstables.iter().cloned().filter(|sst| !sstables_to_remove.contains(sst)).collect::<Vec<_>>();
                assert_eq!(sstables.len(), new_sstables.len() + sstables_to_remove.len());

                let new_size = size - sstables_to_remove.iter().map(|sst| sst.size).sum::<u64>();

                let new_drops: Vec<_> = drops.iter().cloned().filter(|drop| !drops_to_remove.contains(drop)).collect();
                assert_eq!(drops.len(), new_drops.len() + drops_to_remove.len());

                Level::new(*level, new_sstables, new_drops, new_size)
            }
        }
    }

    /// Returns the SSTables in this level. For L0, they may have overlapping key ranges. For L1+, they are sorted and non-overlapping.
    pub fn sstables(&self) -> &[Arc<SSTableMetadata>] {
        match self {
            Overlapping { sstables, .. } | NonOverlapping { sstables, .. } => sstables.as_slice(),
        }
    }

    /// Returns the drops relevant for this level, used to filter out data during compaction.
    pub fn drops(&self) -> &[Arc<DropMetadata>] {
        match self {
            Overlapping { drops, .. } | NonOverlapping { drops, .. } => drops.as_slice(),
        }
    }

    /// Returns all items in the level, including both SSTables and drops, as a single vector.
    pub fn items(&self) -> Vec<Arc<dyn LevelItem>> {
        match self {
            Overlapping { sstables, drops, .. } | NonOverlapping { sstables, drops, .. } => {
                let mut items: Vec<Arc<dyn LevelItem>> = Vec::with_capacity(sstables.len() + drops.len());
                items.extend(sstables.iter().map(|sst| sst.clone() as Arc<dyn LevelItem>));
                items.extend(drops.iter().map(|drop| drop.clone() as Arc<dyn LevelItem>));
                items
            }
        }
    }

    /// Computes the key range that spans all items in the level, including both SSTables and drops.
    pub fn items_range(&self) -> Option<Interval<Vec<u8>>> {
        span(self.items().iter().map(|item| item.as_ref()).collect::<Vec<_>>())
    }

    pub fn compaction_score(&self, db_options: &Options) -> f64 {
        match &self {
            Overlapping { level: _, sstables: _, drops: _, size } => {
                let trigger = db_options.level0_file_num_compaction_trigger();
                let base_bytes = db_options.max_bytes_for_level_base().to_bytes() as f64;
                let file_score = self.sst_count() as f64 / trigger as f64;
                let size_score = *size as f64 / base_bytes;
                file_score.max(size_score)
            }
            NonOverlapping { level, sstables: _, drops: _, size } => {
                let base_bytes = db_options.max_bytes_for_level_base().to_bytes() as f64;
                let multiplier = db_options.max_bytes_for_level_multiplier();
                let target_bytes = base_bytes * multiplier.powi((level - 1) as i32);
                *size as f64 / target_bytes
            }
        }
    }

    /// Finds the SSTables that contain the key and are visible under the given snapshot.
    /// The `min_snapshot` is an exclusive lower bound.
    pub fn find_sstables<'a>(
        &'a self,
        record_key: &'a [u8],
        snapshot: u64,
        min_snapshot: Option<u64>,
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
                            && min_snapshot.map_or(true, |min_snap| sst.max_sequence_number > min_snap)
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
                        if snapshot >= sst.min_sequence_number
                            && min_snapshot.map_or(true, |min_snap| sst.max_sequence_number > min_snap)
                        {
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
    pub fn find_sstables_in_range(
        &self,
        record_key_range: &Interval<Vec<u8>>,
        snapshot: u64,
    ) -> Vec<Arc<SSTableMetadata>> {
        match self {
            Overlapping { level: _, sstables, drops: _, size: _ } => sstables
                .iter()
                .filter(|sst| overlaps(record_key_range, sst) && snapshot >= sst.min_sequence_number)
                .cloned()
                .collect(),
            NonOverlapping { level: _, sstables, drops: _, size: _ } => {
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

    /// Finds all drops that overlap the given interval.
    pub fn find_drops_in_range(&self, record_key_range: &Interval<Vec<u8>>) -> Vec<Arc<DropMetadata>> {
        match self {
            Overlapping { drops, .. } | NonOverlapping { drops, .. } => drops
                .iter()
                .filter(|drop| drop.key_range.intersection(record_key_range).is_some())
                .cloned()
                .collect(),
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
        let level = reader.read_u8()?;
        let sstables = Vec::<Arc<SSTableMetadata>>::read_from(reader)?;
        let drops = Vec::<Arc<DropMetadata>>::read_from(reader)?;
        let size = sstables.iter().map(|sst| sst.size).sum();
        match level {
            0 => Ok(Overlapping { level, sstables, drops, size }),
            _ => Ok(NonOverlapping { level, sstables, drops, size }),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match &self {
            Overlapping { level, sstables, drops, size: _size,  }
            | NonOverlapping { level, sstables, drops, size: _size, } => {
                writer.write_u8(*level);
                sstables.write_to(writer);
                drops.write_to(writer);
            }
        }
    }
}

/// A trait implemented by both `SSTableMetadata` and `DropMetadata` to provide a common interface
/// for finding overlapping items in a level.
pub trait LevelItem {

    /// Returns the key range of the item, used to determine overlaps and visibility.
    fn record_key_range(&self) -> Interval<Vec<u8>>;

    /// Returns the minimum sequence number for which the item is visible. For SSTables, this is the
    /// min_sequence_number, while for drops this is the drop_sequence_number.
    fn min_sequence_number(&self) -> u64;
}

/// Computes the smallest interval that contains all the key ranges of the given items.
///
/// Returns `None` if the input is empty.
pub fn span<'a, I, T>(items: I) -> Option<Interval<Vec<u8>>>
where
    I: IntoIterator<Item = &'a T>,
    T: LevelItem + 'a + ?Sized,
{
    items
        .into_iter()
        .map(|item| item.record_key_range())
        .reduce(|acc, range| acc.span(&range))
}

/// Describes the metadata of an on-disk SSTable.
///
/// Includes its ID, level, key range, and sequence number range for visibility filtering.
#[derive(Default, PartialEq, Eq, Hash, Debug)]
pub struct SSTableMetadata {
    pub number: u64,
    pub level: u8,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_sequence_number: u64,
    pub max_sequence_number: u64,
    pub size: u64, // Size of the SSTable file in bytes
}

impl SSTableMetadata {
    pub fn new(
        number: u64,
        level: u8,
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

impl LevelItem for SSTableMetadata {
    fn record_key_range(&self) -> Interval<Vec<u8>> {
        Interval::closed(self.min_key.clone(), self.max_key.clone())
    }

    fn min_sequence_number(&self) -> u64 {
        self.min_sequence_number
    }
}

impl fmt::Display for SSTableMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SSTableMetadata {{ number: {}, level: {}, min_key: {:x?}, max_key: {:x?}, min_sequence_number: {}, max_sequence_number: {}, size: {} }}",
            self.number,
            self.level,
            self.min_key,
            self.max_key,
            self.min_sequence_number,
            self.max_sequence_number,
            self.size
        )
    }
}

impl Serializable for SSTableMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<SSTableMetadata> {
        Ok(SSTableMetadata {
            number: reader.read_varint_u64()?,
            level: reader.read_u8()?,
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
            .write_u8(self.level)
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

/// Metadata for a dropped collection or index, used as a range tombstone over *record keys*
/// (`[collection][index][user_key]`).
///
/// Drops are tracked at the catalogue layer for MVCC visibility (e.g., "collection/index exists at
/// snapshot"). In addition, the LSM tree must physically carry range tombstones so flush/compaction
/// can skip obsolete data.
///
/// # Two drop kinds
///
/// Record keys are encoded as:
/// `[collection(varint u32)][index(varint u32)][user_key bytes...]`
///
/// This means that dropping an entire collection must span **all** `(collection, index, *)`
/// keyspaces, while dropping an index must span only that exact `(collection, index_id, *)`
/// keyspace (and must *not* delete `(collection, 0, *)` unless `index_id == 0`).
///
/// We model this explicitly via [`DropKind`], and provide constructors that are the only supported
/// way to create drops:
/// - [`DropMetadata::new_collection_drop`] for full collection drops
/// - [`DropMetadata::new_index_drop`] for full index drops
///
/// Narrower ranges are only produced by splitting an existing full-range drop via
/// [`DropMetadata::split_at`], e.g. for compaction partitioning.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DropMetadata {
    pub collection: u32,
    pub kind: DropKind,
    pub key_range: Interval<Vec<u8>>,
    pub drop_sequence_number: u64,
}

/// The logical kind of drop.
///
/// This is persisted in the manifest (serialization tag-based), so keep tags stable.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum DropKind {
    /// Drop the full collection across all indexes.
    Collection,
    /// Drop only one index keyspace within a collection.
    Index(u32),
}

impl Serializable for DropKind {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self>
    where
        Self: Sized
    {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(DropKind::Collection),
            1 => {
                let index_id = reader.read_varint_u32()?;
                Ok(DropKind::Index(index_id))
            }
            _ => Err(Error::new(ErrorKind::InvalidData, format!("Invalid DropKind tag: {}", tag))),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            DropKind::Collection => writer.write_u8(0),
            DropKind::Index(index_id) => writer.write_u8(1).write_varint_u32(*index_id),
        };
    }
}

/// Merges and splits drops to remove overlaps.
///
/// Overlap can only happen if an index was dropped before its collection. In those cases we need
/// to split the collection drop into non-overlapping parts.
///
/// For example, if an index is dropped at sequence 100 and its parent collection is dropped
/// at sequence 200, the collection drop is split into two parts: one covering keys before
/// the index range and one covering keys after.
fn merge_and_split_drops(mut drops: Vec<Arc<DropMetadata>>) -> Vec<Arc<DropMetadata>> {
    if drops.len() <= 1 {
        return drops;
    }

    // Sort by start bound, then by drop_sequence_number (earlier sequence first)
    drops.sort_by(|a, b| {
        a.key_range
            .cmp(&b.key_range)
            .then(a.drop_sequence_number.cmp(&b.drop_sequence_number))
    });

    let mut result: Vec<Arc<DropMetadata>> = Vec::with_capacity(drops.len());

    for drop in drops {
        if result.is_empty() {
            result.push(drop);
            continue;
        }

        let last = result.last().unwrap();

        if !last.key_range.contains_interval(&drop.key_range) {
            result.push(drop);
            continue;
        }

        // If the last drop has the same drop_sequence_number as the current one, we can keep only
        // the wider drop (which must be `last` due to containment and earlier sorting).
        if last.drop_sequence_number == drop.drop_sequence_number {
            continue;
        }

        let last = result.pop().unwrap();

        let intervals = last.key_range.remove_included_interval(&drop.key_range);
        assert_eq!(intervals.len(), 2);
        let mut iter = intervals.into_iter();

        result.push(Arc::new(DropMetadata {
            collection: last.collection,
            kind: last.kind.clone(),
            key_range: iter.next().unwrap(),
            drop_sequence_number: last.drop_sequence_number,
        }));
        result.push(drop);
        result.push(Arc::new(DropMetadata {
            collection: last.collection,
            kind: last.kind.clone(),
            key_range: iter.next().unwrap(),
            drop_sequence_number: last.drop_sequence_number,
        }));
    }

    result
}

impl DropMetadata {
    /// Creates a full-range drop for an entire collection (all indexes).
    pub fn new_collection_drop(collection: u32, drop_sequence_number: u64) -> Arc<Self> {
        let user_min_key = Bson::MinKey.try_into_key().unwrap();
        let user_max_key = Bson::MaxKey.try_into_key().unwrap();

        let min_key = encode_record_key(collection, 0, &user_min_key);
        let max_key = encode_record_key(collection, u32::MAX, &user_max_key);

        Arc::new(DropMetadata {
            collection,
            kind: DropKind::Collection,
            drop_sequence_number,
            key_range: Interval::closed(min_key, max_key),
        })
    }

    /// Creates a full-range drop for a single index within a collection.
    pub fn new_index_drop(collection: u32, index: u32, drop_sequence_number: u64) -> Arc<Self> {
        assert_ne!(index, 0, "Index ID 0 is reserved for the primary index and cannot be dropped with new_index_drop. Use new_collection_drop instead.");
        let user_min_key = Bson::MinKey.try_into_key().unwrap();
        let user_max_key = Bson::MaxKey.try_into_key().unwrap();

        let min_key = encode_record_key(collection, index, &user_min_key);
        let max_key = encode_record_key(collection, index, &user_max_key);

        Arc::new(DropMetadata {
            collection,
            kind: DropKind::Index(index),
            drop_sequence_number,
            key_range: Interval::closed(min_key, max_key),
        })
    }

    pub fn key_range_ref(&self) -> Interval<&[u8]> {
        Interval::new(
            match self.key_range.start_bound() {
                Bound::Included(v) => Bound::Included(v.as_slice()),
                Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            },
            match self.key_range.end_bound() {
                Bound::Included(v) => Bound::Included(v.as_slice()),
                Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
                Bound::Unbounded => Bound::Unbounded,
            },
        )
    }

    /// Splits the drop metadata into two non-overlapping drops at the given split key.
    /// The split key must be within the drop's key range. The resulting drops will have the same
    /// drop_sequence_number as the original one, and their key ranges will be adjusted accordingly.
    /// For example, if we have a drop covering keys [A, Z] and we split at key M, we will get two drops:
    /// - Drop 1: covering [A, M] with the same drop_sequence_number
    /// - Drop 2: covering (M, Z] with the same drop_sequence_number
    /// This is used to handle the case where a drop need to be split to match the boundaries of
    /// the partition grid when compacting for partial compaction levels.
    pub fn split_at(&self, split_key: &[u8]) -> SplitResults {
        assert!(self.key_range.contains(split_key), "Split key must be within the drop's key range");

        if self.key_range.end_bound_value_eq(split_key) {
            return SplitResults::One(Arc::new(self.clone()))
        }

        let left_range = Interval::new(
            self.key_range.start_bound().cloned(),
            Bound::Included(split_key.to_vec()),
        );
        let right_range = Interval::new(
            Bound::Excluded(split_key.to_vec()),
            self.key_range.end_bound().cloned(),
        );

        SplitResults::Two(
            Arc::new(DropMetadata {
                collection: self.collection,
                kind: self.kind.clone(),
                drop_sequence_number: self.drop_sequence_number,
                key_range: left_range,
            }),
            Arc::new(DropMetadata {
                collection: self.collection,
                kind: self.kind.clone(),
                drop_sequence_number: self.drop_sequence_number,
                key_range: right_range,
            }),
        )
    }
}

pub enum SplitResults {
    One(Arc<DropMetadata>),
    Two(Arc<DropMetadata>, Arc<DropMetadata>),
}

#[cfg(test)]
impl SplitResults {
    pub(crate) fn expect_two(self) -> (Arc<DropMetadata>, Arc<DropMetadata>) {
        match self {
            SplitResults::Two(left, right) => (left, right),
            SplitResults::One(_) => panic!("expected two parts from split_at"),
        }
    }
}

impl LevelItem for DropMetadata {
    fn record_key_range(&self) -> Interval<Vec<u8>> {
        self.key_range.clone()
    }

    fn min_sequence_number(&self) -> u64 {
        self.drop_sequence_number
    }
}

impl fmt::Display for DropMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            DropKind::Collection => write!(
                f,
                "DropMetadata {{ collection: {}, kind: Collection, key_range: {:?}, drop_sequence_number: {} }}",
                self.collection, self.key_range, self.drop_sequence_number
            ),
            DropKind::Index(index) => write!(
                f,
                "DropMetadata {{ collection: {}, kind: Index({}), key_range: {:?}, drop_sequence_number: {} }}",
                self.collection, index, self.key_range, self.drop_sequence_number
            ),
        }
    }
}

impl Serializable for DropMetadata {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let collection = reader.read_varint_u32()?;
        let kind =  DropKind::read_from(reader)?;
        let drop_sequence_number = reader.read_varint_u64()?;
        let key_range = Interval::read_from(reader)?;

        Ok(DropMetadata {
            collection,
            kind,
            drop_sequence_number,
            key_range,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u32(self.collection);
        self.kind.write_to(writer);
        writer.write_varint_u64(self.drop_sequence_number);
        self.key_range.write_to(writer);
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
    fn test_find_sstables_overlapping() {
        // Create two overlapping SSTables:
        // sst1 covers keys 1 to 40 and is visible for snapshot 100..=200.
        // sst2 covers keys 20 to 60 and is visible for snapshot 201..=300.
        let sst1 = create_sstable(1, 0, 1, 40, 100, 200, 53_248);
        let sst2 = create_sstable(2, 0, 20, 60, 201, 300, 79_872);
        let level = Level::new(0, vec![sst1.clone(), sst2.clone()], vec![], 133_120);

        let empty: Vec<Arc<SSTableMetadata>> = vec![];

        // Key 2 falls only in sst1.
        let found: Vec<_> = level.find_sstables(&record_key(2), 300, None).collect();
        assert_eq!(vec![sst1.clone()], found);

        // Key 58 falls only in sst2.
        let found: Vec<_> = level.find_sstables(&record_key(58), 300, None).collect();
        assert_eq!(vec![sst2.clone()], found);

        // If the snapshot is bellow sst2 min sequence, nothing is returned
        let found: Vec<_> = level.find_sstables(&record_key(58), 150, None).collect();
        assert_eq!(empty, found);

        // Key 25 falls in sst1 and sst2
        let found: Vec<_> = level.find_sstables(&record_key(25), 300, None).collect();
        assert_eq!(vec![sst2.clone(), sst1.clone()], found);

        let found: Vec<_> = level.find_sstables(&record_key(87), 300, None).collect();
        // A key out of range should yield None.
        assert_eq!(empty, found)
    }

    #[test]
    fn test_find_sstables_non_overlapping() {
        // For non-overlapping SSTables (e.g., L1+), assume:
        // sst1 covers 1 to 40 (snapshot 100..=200)
        // sst2 covers 41 to 60 (snapshot 201..=300)
        let sst1 = create_sstable(1, 1, 1, 40, 100, 200, 53_248);
        let sst2 = create_sstable(2, 1, 41, 60, 201, 300, 79_872);
        let level = Level::new(1, vec![sst1.clone(), sst2.clone()], vec![], 133_120);

        let empty: Vec<Arc<SSTableMetadata>> = vec![];

        // Key 2 should be found in sst1.
        let found: Vec<_> = level.find_sstables(&record_key(2), 300, None).collect();
        assert_eq!(vec![sst1.clone()], found);

        // Key 58 falls only in sst2.
        let found: Vec<_> = level.find_sstables(&record_key(58), 300, None).collect();
        assert_eq!(vec![sst2.clone()], found);

        // If the snapshot is bellow sst2 min sequence, nothing is returned
        let found: Vec<_> = level.find_sstables(&record_key(58), 150, None).collect();
        assert_eq!(empty, found);

        // A key out of range should yield None.
        let found: Vec<_> = level.find_sstables(&record_key(87), 300, None).collect();
        assert_eq!(empty, found);
    }

    #[test]
    fn test_find_sstables_with_min_snapshot_overlapping() {
        let sst1 = create_sstable(1, 0, 1, 50, 100, 200, 1000);
        let sst2 = create_sstable(2, 0, 25, 75, 201, 300, 1000);
        let sst3 = create_sstable(3, 0, 50, 100, 301, 400, 1000);
        let level = Level::new(0, vec![sst1.clone(), sst2.clone(), sst3.clone()], vec![], 3000);
        let key = record_key(50);
        let empty: Vec<Arc<SSTableMetadata>> = vec![];

        // snapshot=400, min_snapshot=None: should find sst3, sst2, sst1 (reverse order).
        let found: Vec<_> = level.find_sstables(&key, 400, None).collect();
        assert_eq!(vec![sst3.clone(), sst2.clone(), sst1.clone()], found);

        // snapshot=400, min_snapshot=Some(300): (300, 400].
        // sst1: max_seq=200 <= 300 -> filtered out.
        // sst2: max_seq=300 <= 300 -> filtered out.
        // sst3: max_seq=400 > 300 -> included.
        let found: Vec<_> = level.find_sstables(&key, 400, Some(300)).collect();
        assert_eq!(vec![sst3.clone()], found);

        // snapshot=400, min_snapshot=Some(301): (301, 400].
        // sst1/sst2 filtered out. sst3 included.
        let found: Vec<_> = level.find_sstables(&key, 400, Some(301)).collect();
        assert_eq!(vec![sst3.clone()], found);

        // snapshot=300, min_snapshot=Some(200): (200, 300].
        // sst1: max_seq=200 <= 200 -> filtered out.
        // sst2: max_seq=300 > 200 -> included.
        // sst3: min_seq=301 > snapshot=300 -> filtered out by snapshot check.
        let found: Vec<_> = level.find_sstables(&key, 300, Some(200)).collect();
        assert_eq!(vec![sst2.clone()], found);

        // snapshot=400, min_snapshot=Some(400): (400, 400] -> empty range.
        // An SSTable is visible if its max_sequence_number > min_snapshot.
        // all sst's max_seq <= 400, so all filtered out.
        let found: Vec<_> = level.find_sstables(&key, 400, Some(400)).collect();
        assert_eq!(empty, found);

        // snapshot=250, min_snapshot=Some(200): (200, 250].
        // sst1: max_seq=200 <= 200 -> filtered out.
        // sst2: max_seq=300 > 200 -> included. And snapshot=250 >= min_seq=201.
        // sst3: min_seq=301 > snapshot=250 -> filtered out.
        let found: Vec<_> = level.find_sstables(&key, 250, Some(200)).collect();
        assert_eq!(vec![sst2.clone()], found);
    }

    #[test]
    fn test_find_sstables_with_min_snapshot_non_overlapping() {
        let sst1 = create_sstable(1, 1, 1, 50, 100, 200, 1000);
        let sst2 = create_sstable(2, 1, 51, 100, 201, 300, 1000);
        let sst3 = create_sstable(3, 1, 101, 150, 301, 400, 1000);
        let level = Level::new(1, vec![sst1.clone(), sst2.clone(), sst3.clone()], vec![],3000);
        let key = record_key(25);
        let empty: Vec<Arc<SSTableMetadata>> = vec![];

        // snapshot=400, min_snapshot=None: key 25 is in sst1. snapshot=400 >= min_seq=100.
        let found: Vec<_> = level.find_sstables(&key, 400, None).collect();
        assert_eq!(vec![sst1.clone()], found);

        // snapshot=400, min_snapshot=Some(200): (200, 400].
        // key is in sst1. sst1.max_seq=200 <= 200 -> filtered out.
        let found: Vec<_> = level.find_sstables(&key, 400, Some(200)).collect();
        assert_eq!(empty, found);

        // snapshot=400, min_snapshot=Some(199): (199, 400].
        // key is in sst1. sst1.max_seq=200 > 199 -> included. And snapshot=400 >= min_seq=100.
        let found: Vec<_> = level.find_sstables(&key, 400, Some(199)).collect();
        assert_eq!(vec![sst1.clone()], found);

        // snapshot=150, min_snapshot=None: snapshot=150 >= min_seq=100.
        let found: Vec<_> = level.find_sstables(&key, 150, None).collect();
        assert_eq!(vec![sst1.clone()], found);

        // snapshot=99, min_snapshot=None: snapshot=99 < min_seq=100.
        let found: Vec<_> = level.find_sstables(&key, 99, None).collect();
        assert_eq!(empty, found);
    }

    #[test]
    fn test_find_sstables_in_range_overlapping() {
        // Create three overlapping SSTables:
        // sst1: keys 1 to 60, visible for snapshot 100..=200.
        // sst2: keys 40 to 100, visible for snapshot 201..=300.
        // sst3: keys 70 to 100, visible for snapshot 301..=400.
        let sst1 = create_sstable(1, 0, 1, 60, 100, 200, 53_248);
        let sst2 = create_sstable(2, 0, 40, 100, 201, 300, 79_872);
        let sst3 = create_sstable(3, 0, 70, 100, 301, 400, 32_768);
        let coll = Level::new(0, vec![sst1.clone(), sst2.clone(), sst3.clone()], vec![],165_888);

        let interval = Interval::closed(record_key(50), record_key(100));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(3, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::greater_than(record_key(60));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::closed_open(record_key(61), record_key(70));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst2));

        let interval = Interval::at_most(record_key(60));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));

        let results = coll.find_sstables_in_range(&interval, 150);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst1));

        let interval = Interval::greater_than(record_key(100));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(0, results.len());
    }

    #[test]
    fn test_find_sstables_in_range_non_overlapping() {
        // Create three overlapping SSTables:
        // sst1: keys 1 to 60, visible for snapshot 100..=200.
        // sst2: keys 61 to 69, visible for snapshot 150..=300.
        // sst3: keys 70 to 100, visible for snapshot 232..=400.
        let sst1 = create_sstable(1, 0, 1, 60, 100, 200, 53_248);
        let sst2 = create_sstable(2, 0, 61, 69, 150, 300, 79_872);
        let sst3 = create_sstable(3, 0, 70, 100, 232, 400, 32_768);
        let coll = Level::new(1, vec![sst1.clone(), sst2.clone(), sst3.clone()], vec![],165_888);

        let interval = Interval::closed(record_key(50), record_key(100));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(3, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::greater_than(record_key(60));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst2));
        assert!(results.contains(&sst3));

        let interval = Interval::closed_open(record_key(61), record_key(70));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst2));

        let interval = Interval::at_most(record_key(62));

        let results = coll.find_sstables_in_range(&interval, 400);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));

        let results = coll.find_sstables_in_range(&interval, 150);
        assert_eq!(2, results.len());
        assert!(results.contains(&sst1));
        assert!(results.contains(&sst2));

        let results = coll.find_sstables_in_range(&interval, 100);
        assert_eq!(1, results.len());
        assert!(results.contains(&sst1));

        let interval = Interval::greater_than(record_key(100));

        let results = coll.find_sstables_in_range(&interval, 400);
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

    #[test]
    fn test_lsm_version_serialization_with_pending_drops() {
        let mut version = LsmVersion::new(456, 1024, 2);
        let levels = create_levels();
        version.sst_levels = Arc::new(levels);

        // Add some pending drops
        version = version.add_collection_drop(10, 150);
        version = version.add_collection_drop(20, 250);
        version = version.add_collection_drop(30, 350);

        assert_eq!(version.pending_drops.len(), 3);

        check_serialization_round_trip(version);
    }

    #[test]
    fn test_lsm_version_serialization_without_pending_drops() {
        let version = LsmVersion::new(123, 456, 3);
        assert!(version.pending_drops.is_empty());
        check_serialization_round_trip(version);
    }

    #[test]
    fn test_get_drops_before_or_at_empty() {
        let version = LsmVersion::new(1, 10, 2);
        let drops = version.get_drops_before_or_at(100);
        assert!(drops.is_empty());
    }

    #[test]
    fn test_get_drops_before_or_at_single_drop() {
        let version = LsmVersion::new(1, 10, 2);
        let version = version.add_collection_drop(5, 50);

        // Before the drop sequence
        let drops = version.get_drops_before_or_at(49);
        assert!(drops.is_empty());

        // Exactly at the drop sequence
        let drops = version.get_drops_before_or_at(50);
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0].collection, 5);
        assert_eq!(drops[0].drop_sequence_number, 50);

        // After the drop sequence
        let drops = version.get_drops_before_or_at(100);
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0].collection, 5);
    }

    #[test]
    fn test_get_drops_before_or_at_multiple_drops() {
        let version = LsmVersion::new(1, 10, 2);
        let version = version.add_collection_drop(10, 100);
        let version = version.add_collection_drop(20, 200);
        let version = version.add_collection_drop(30, 300);

        // Before all drops
        let drops = version.get_drops_before_or_at(50);
        assert!(drops.is_empty());

        // Include first drop only
        let drops = version.get_drops_before_or_at(100);
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0].collection, 10);

        // Include first two drops
        let drops = version.get_drops_before_or_at(200);
        assert_eq!(drops.len(), 2);
        assert_eq!(drops[0].collection, 10);
        assert_eq!(drops[1].collection, 20);

        // Include all drops
        let drops = version.get_drops_before_or_at(300);
        assert_eq!(drops.len(), 3);

        // After all drops
        let drops = version.get_drops_before_or_at(500);
        assert_eq!(drops.len(), 3);
    }

    #[test]
    fn test_get_drops_before_or_at_boundary_conditions() {
        let version = LsmVersion::new(1, 10, 2);
        let version = version.add_collection_drop(5, 100);
        let version = version.add_collection_drop(6, 101);

        // Just before first drop
        let drops = version.get_drops_before_or_at(99);
        assert!(drops.is_empty());

        // Exactly at first drop
        let drops = version.get_drops_before_or_at(100);
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0].collection, 5);

        // Between drops
        let drops = version.get_drops_before_or_at(100);
        assert_eq!(drops.len(), 1);

        // Exactly at second drop
        let drops = version.get_drops_before_or_at(101);
        assert_eq!(drops.len(), 2);
    }

    #[test]
    fn test_drops_cleared_after_flush() {
        let version = LsmVersion::new(1, 10, 2);

        // Add drops at various sequence numbers
        let version = version.add_collection_drop(10, 100);
        let version = version.add_collection_drop(20, 200);
        let version = version.add_collection_drop(30, 300);

        assert_eq!(version.pending_drops.len(), 3);

        // Flush an SSTable with max_sequence_number = 250
        // This should clear drops with drop_sequence_number <= 250
        let sst = Arc::new(SSTableMetadata::new(
            1,
            0,
            &record_key(1),
            &record_key(100),
            1,
            250,
            1024,
        ));
        let version = version.with_flushed_sstable(1, &sst);

        // Only drop at sequence 300 should remain (300 > 250)
        assert_eq!(version.pending_drops.len(), 1);
        assert_eq!(version.pending_drops[0].collection, 30);
        assert_eq!(version.pending_drops[0].drop_sequence_number, 300);

        // get_drops_before_or_at should reflect the cleared state
        let drops = version.get_drops_before_or_at(u64::MAX);
        assert_eq!(drops.len(), 1);
        assert_eq!(drops[0].collection, 30);
    }

    #[test]
    fn test_drops_all_cleared_after_flush() {
        let version = LsmVersion::new(1, 10, 2);

        let version = version.add_collection_drop(10, 100);
        let version = version.add_collection_drop(20, 200);

        assert_eq!(version.pending_drops.len(), 2);

        // Flush an SSTable with max_sequence_number = 300
        // This should clear all drops since all have drop_sequence_number <= 300
        let sst = Arc::new(SSTableMetadata::new(
            1,
            0,
            &record_key(1),
            &record_key(100),
            1,
            300,
            1024,
        ));
        let version = version.with_flushed_sstable(1, &sst);

        assert!(version.pending_drops.is_empty());
        assert!(version.get_drops_before_or_at(u64::MAX).is_empty());
    }

    fn record_key(number: i32) -> Vec<u8> {
        let user_key = Bson::Int32(number).try_into_key().unwrap();
        encode_record_key(1, 0, &user_key)
    }

    fn create_lsm_version() -> LsmVersion {
        // Create an LsmVersion with a specific log number.
        let mut version = LsmVersion::new(456, 1024, 2);
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
        Level::new(0, vec![Arc::new(create_level_0_sstable())], vec![],79_872)
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
        Level::new(1, vec![Arc::new(sstable1), Arc::new(sstable2)], vec![],2048)
    }

    fn create_sstable(
        number: u64,
        level: u8,
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

    mod levels_add_tests {
        use std::iter::{empty, once};
        use super::*;

        #[test]
        fn test_add_to_empty_level_0() {
            let levels = Levels::new(4);
            
            let sst = create_sst(1, 0, 10, 20, 1000);
            let levels = levels.add(0, once(sst.clone()), empty());
            
            let level = levels.level(0).unwrap();
            assert_eq!(level.sst_count(), 1);
            assert_eq!(level.total_bytes(), 1000);
        }

        #[test]
        fn test_add_to_empty_level_1() {
            let levels = Levels::new(4);
            
            let sst = create_sst(1, 1, 10, 20, 2000);
            let levels = levels.add(1, once(sst.clone()), empty());
            
            let level = levels.level(1).unwrap();
            assert_eq!(level.sst_count(), 1);
            assert_eq!(level.total_bytes(), 2000);
        }

        #[test]
        fn test_add_multiple_sstables_to_level_0() {
            let levels = Levels::new(4);
            
            let sst1 = create_sst(1, 0, 10, 20, 1000);
            let sst2 = create_sst(2, 0, 15, 25, 1500);
            let levels = levels.add(0, vec![sst1, sst2], empty());
            
            let level = levels.level(0).unwrap();
            assert_eq!(level.sst_count(), 2);
            assert_eq!(level.total_bytes(), 2500);
        }

        #[test]
        fn test_add_incrementally_to_level_0() {
            let levels = Levels::new(4);
            
            // Add first SSTable
            let sst1 = create_sst(1, 0, 10, 20, 1000);
            let levels = levels.add(0, once(sst1), empty());
            
            assert_eq!(levels.level(0).unwrap().sst_count(), 1);
            assert_eq!(levels.level(0).unwrap().total_bytes(), 1000);
            
            // Add second SSTable
            let sst2 = create_sst(2, 0, 30, 40, 2000);
            let levels = levels.add(0, once(sst2), empty());
            
            assert_eq!(levels.level(0).unwrap().sst_count(), 2);
            assert_eq!(levels.level(0).unwrap().total_bytes(), 3000);
            
            // Add third SSTable
            let sst3 = create_sst(3, 0, 50, 60, 500);
            let levels = levels.add(0, once(sst3), empty());
            
            assert_eq!(levels.level(0).unwrap().sst_count(), 3);
            assert_eq!(levels.level(0).unwrap().total_bytes(), 3500);
        }

        #[test]
        fn test_add_incrementally_to_level_1() {
            let levels = Levels::new(4);
            
            // Add first SSTable
            let sst1 = create_sst(1, 1, 10, 20, 1000);
            let levels = levels.add(1, once(sst1), empty());
            
            assert_eq!(levels.level(1).unwrap().sst_count(), 1);
            assert_eq!(levels.level(1).unwrap().total_bytes(), 1000);
            
            // Add second SSTable (non-overlapping, should be sorted by min_key)
            let sst2 = create_sst(2, 1, 30, 40, 2000);
            let levels = levels.add(1, once(sst2), empty());
            
            assert_eq!(levels.level(1).unwrap().sst_count(), 2);
            assert_eq!(levels.level(1).unwrap().total_bytes(), 3000);
        }

        #[test]
        fn test_add_to_different_levels() {
            let levels = Levels::new(4);
            
            let sst0 = create_sst(1, 0, 10, 20, 1000);
            let levels = levels.add(0, once(sst0), empty());
            
            let sst1 = create_sst(2, 1, 30, 40, 2000);
            let levels = levels.add(1, once(sst1), empty());
            
            let sst2 = create_sst(3, 2, 50, 60, 3000);
            let levels = levels.add(2, once(sst2), empty());
            
            assert_eq!(levels.level(0).unwrap().sst_count(), 1);
            assert_eq!(levels.level(0).unwrap().total_bytes(), 1000);
            
            assert_eq!(levels.level(1).unwrap().sst_count(), 1);
            assert_eq!(levels.level(1).unwrap().total_bytes(), 2000);
            
            assert_eq!(levels.level(2).unwrap().sst_count(), 1);
            assert_eq!(levels.level(2).unwrap().total_bytes(), 3000);
            
            assert_eq!(levels.total_bytes(), 6000);
            assert_eq!(levels.sst_count(), 3);
        }

        #[test]
        fn test_add_with_drops() {
            let levels = Levels::new(4);

            let sst = create_sst(1, 0, 10, 20, 1000);
            let drop = DropMetadata::new_collection_drop(1, 50);

            let levels = levels.add(0, once(sst), once(drop));
            
            let level = levels.level(0).unwrap();
            assert_eq!(level.sst_count(), 1);
            assert_eq!(level.total_bytes(), 1000);
            
            // Verify drops are stored (accessed via pattern matching)
            match level {
                Level::Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 1);
                    assert_eq!(drops[0].collection, 1);
                    assert_eq!(drops[0].kind, DropKind::Collection);
                    assert_eq!(drops[0].drop_sequence_number, 50);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_add_preserves_other_levels() {
            let levels = Levels::new(4);
            
            // Add to level 0
            let sst0 = create_sst(1, 0, 10, 20, 1000);
            let levels = levels.add(0, once(sst0), empty());
            
            // Add to level 2
            let sst2 = create_sst(2, 2, 30, 40, 2000);
            let levels = levels.add(2, once(sst2), empty());
            
            // Verify level 0 is unchanged
            assert_eq!(levels.level(0).unwrap().sst_count(), 1);
            assert_eq!(levels.level(0).unwrap().total_bytes(), 1000);
            
            // Verify level 1 is still empty
            assert_eq!(levels.level(1).unwrap().sst_count(), 0);
            assert_eq!(levels.level(1).unwrap().total_bytes(), 0);
            
            // Verify level 2 has the new SSTable
            assert_eq!(levels.level(2).unwrap().sst_count(), 1);
            assert_eq!(levels.level(2).unwrap().total_bytes(), 2000);
        }

        #[test]
        fn test_add_empty_iterators() {
            let levels = Levels::new(4);
            
            // Add nothing
            let levels = levels.add(0, empty(), empty());
            
            assert_eq!(levels.level(0).unwrap().sst_count(), 0);
            assert_eq!(levels.level(0).unwrap().total_bytes(), 0);
        }

        #[test]
        fn test_live_sst_numbers_empty_levels() {
            let levels = Levels::new(4);
            let live_numbers = levels.live_sst_numbers();
            assert!(live_numbers.is_empty());
        }

        #[test]
        fn test_live_sst_numbers_after_adding_sstables_in_different_levels() {
            let levels = Levels::new(4);

            let sst0 = create_sst(10, 0, 10, 20, 1000);
            let levels = levels.add(0, once(sst0), empty());

            let sst2 = create_sst(20, 2, 30, 40, 2000);
            let levels = levels.add(2, once(sst2), empty());

            let live_numbers = levels.live_sst_numbers();
            assert_eq!(live_numbers.len(), 2);
            assert!(live_numbers.contains(&10));
            assert!(live_numbers.contains(&20));
        }

        #[test]
        fn test_live_sst_numbers_after_removing_sstable() {
            let levels = Levels::new(4);

            let sst1 = create_sst(1, 0, 10, 20, 1000);
            let sst2 = create_sst(2, 0, 30, 40, 2000);
            let levels = levels.add(0, vec![sst1.clone(), sst2.clone()], empty());

            let mut to_remove: HashSet<Arc<SSTableMetadata>> = HashSet::new();
            to_remove.insert(sst1);

            let levels = levels.remove(0, &to_remove, &HashSet::new());
            let live_numbers = levels.live_sst_numbers();

            assert_eq!(live_numbers.len(), 1);
            assert!(!live_numbers.contains(&1));
            assert!(live_numbers.contains(&2));
        }

        #[test]
        fn test_level_1_maintains_sorted_order() {
            let levels = Levels::new(4);
            
            // Add SSTables in reverse order of their keys
            let sst3 = create_sst(3, 1, 70, 80, 1000);
            let sst1 = create_sst(1, 1, 10, 20, 1000);
            let sst2 = create_sst(2, 1, 40, 50, 1000);
            
            let levels = levels.add(1, vec![sst3, sst1, sst2], empty());
            
            let level = levels.level(1).unwrap();
            match level {
                Level::NonOverlapping { sstables, .. } => {
                    // Should be sorted by min_key
                    assert_eq!(sstables[0].number, 1); // min_key: 10
                    assert_eq!(sstables[1].number, 2); // min_key: 40
                    assert_eq!(sstables[2].number, 3); // min_key: 70
                }
                _ => panic!("Expected NonOverlapping level"),
            }
        }
    }

    mod levels_remove_tests {
        use super::*;
        use std::collections::HashSet;
        use std::iter::{empty, once};

        #[test]
        fn test_level_remove_removes_only_specified_sstables_and_updates_size() {
            let sst1 = create_sst(1, 1, 10, 20, 1000);
            let sst2 = create_sst(2, 1, 30, 40, 2000);
            let sst3 = create_sst(3, 1, 50, 60, 3000);

            let total = 1000 + 2000 + 3000;
            let level = Level::new(1, vec![sst1.clone(), sst2.clone(), sst3.clone()], vec![], total);

            let mut to_remove: HashSet<Arc<SSTableMetadata>> = HashSet::new();
            to_remove.insert(sst2.clone());

            let removed = level.remove(&to_remove, &HashSet::new());

            assert_eq!(removed.sst_count(), 2);
            assert_eq!(removed.total_bytes(), total - 2000);

            let remaining_numbers: Vec<u64> = removed.sstables().iter().map(|s| s.number).collect();
            assert_eq!(remaining_numbers, vec![1, 3], "Expected to remove only sst2");
        }

        #[test]
        fn test_levels_remove_preserves_other_levels() {
            let levels = Levels::new(3);

            let sst_l0 = create_sst(1, 0, 10, 20, 1000);
            let sst_l1_a = create_sst(2, 1, 30, 40, 2000);
            let sst_l1_b = create_sst(3, 1, 50, 60, 3000);

            let levels = levels.add(0, once(sst_l0.clone()), empty());
            let levels = levels.add(1, vec![sst_l1_a.clone(), sst_l1_b.clone()], empty());

            let mut to_remove: HashSet<Arc<SSTableMetadata>> = HashSet::new();
            to_remove.insert(sst_l1_a.clone());

            let levels2 = levels.remove(1, &to_remove, &HashSet::new());

            // Level 0 unchanged
            assert_eq!(levels2.level(0).unwrap().sst_count(), 1);
            assert_eq!(levels2.level(0).unwrap().total_bytes(), 1000);

            // Level 1 updated
            assert_eq!(levels2.level(1).unwrap().sst_count(), 1);
            assert_eq!(levels2.level(1).unwrap().total_bytes(), 3000);
            assert_eq!(levels2.level(1).unwrap().sstables()[0].number, 3);

            // Global totals updated
            assert_eq!(levels2.sst_count(), 2);
            assert_eq!(levels2.total_bytes(), 1000 + 3000);
        }

        #[test]
        fn test_level_remove_removes_only_specified_drops() {
            let sst = create_sst(1, 0, 10, 20, 1000);
            let drop1 = DropMetadata::new_collection_drop(2, 100);
            let drop2 = DropMetadata::new_collection_drop(3, 150);

            let level = Level::new(0, vec![sst], vec![drop1.clone(), drop2.clone()], 1000);

            let mut drops_to_remove: HashSet<Arc<DropMetadata>> = HashSet::new();
            drops_to_remove.insert(drop1.clone());

            let removed = level.remove(&HashSet::new(), &drops_to_remove);

            assert_eq!(removed.sst_count(), 1);
            assert_eq!(removed.total_bytes(), 1000);

            match &removed {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 1);
                    assert_eq!(drops[0], drop2);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_levels_remove_sstable_and_drop_same_time() {
            let levels = Levels::new(2);

            let sst1 = create_sst(1, 0, 10, 20, 1000);
            let sst2 = create_sst(2, 0, 30, 40, 2000);
            let drop1 = DropMetadata::new_collection_drop(2, 100);

            let levels = levels.add(0, vec![sst1.clone(), sst2.clone()], once(drop1.clone()));

            let mut sst_to_remove: HashSet<Arc<SSTableMetadata>> = HashSet::new();
            sst_to_remove.insert(sst1.clone());

            let mut drops_to_remove: HashSet<Arc<DropMetadata>> = HashSet::new();
            drops_to_remove.insert(drop1.clone());

            let levels2 = levels.remove(0, &sst_to_remove, &drops_to_remove);

            let l0 = levels2.level(0).unwrap();
            assert_eq!(l0.sst_count(), 1);
            assert_eq!(l0.total_bytes(), 2000);
            assert_eq!(l0.sstables()[0].number, 2);

            match l0 {
                Overlapping { drops, .. } => assert!(drops.is_empty()),
                _ => panic!("Expected Overlapping level"),
            }
        }
    }

    mod drop_metadata_merge_tests {
        use super::*;
        use std::iter::empty;

        fn split_to_range(
            drop: &Arc<DropMetadata>,
            start: &[u8],
            end: &[u8],
        ) -> Arc<DropMetadata> {
            let (left, right) = drop.split_at(end).expect_two();
            let (_discard, mid) = left.split_at(start).expect_two();
            // `mid` is (start, end] due to split semantics; for these tests, that's sufficient as
            // we only care about containment/overlap behavior.
            // For closed ranges the tests already use Interval constructors on record keys.
            drop_sequence_eq(drop, &mid);
            drop_kind_eq(drop, &mid);
            right; // keep `right` used to ensure we didn't accidentally use it
            mid
        }

        fn drop_sequence_eq(a: &Arc<DropMetadata>, b: &Arc<DropMetadata>) {
            assert_eq!(a.drop_sequence_number, b.drop_sequence_number);
        }

        fn drop_kind_eq(a: &Arc<DropMetadata>, b: &Arc<DropMetadata>) {
            assert_eq!(a.kind, b.kind);
            assert_eq!(a.collection, b.collection);
        }

        #[test]
        fn test_no_overlap_drops_preserved() {
            let levels = Levels::new(4);

            let drop1 = DropMetadata::new_index_drop(1, 1, 100);
            let drop2 = DropMetadata::new_index_drop(2, 1, 200);

            let levels =
                levels.add(0, empty::<Arc<SSTableMetadata>>(), vec![drop1.clone(), drop2.clone()]);

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 2);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_index_drop_before_collection_drop_splits_collection() {
            let levels = Levels::new(4);

            // Index dropped at seq 100, then collection dropped at seq 200.
            // The collection drop is full-range (covers all indexes); the index drop is a subrange
            // (within one index keyspace). Collection drop must be fragmented to preserve the
            // earlier index drop sequence.
            let index_drop_full = DropMetadata::new_index_drop(1, 1, 100);
            let collection_drop_full = DropMetadata::new_collection_drop(1, 200);

            // Pick two record keys that lie within index 1 keyspace.
            let key_20 = encode_record_key(1, 1, &Bson::Int32(20).try_into_key().unwrap());
            let key_30 = encode_record_key(1, 1, &Bson::Int32(30).try_into_key().unwrap());

            let index_drop = split_to_range(&index_drop_full, &key_20, &key_30);

            let levels = levels.add(
                0,
                empty::<Arc<SSTableMetadata>>(),
                vec![index_drop.clone(), collection_drop_full.clone()],
            );

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 3, "Expected 3 drops after split, got {}", drops.len());
                    assert_eq!(drops[1].kind, DropKind::Index(1));
                    assert_eq!(drops[1].drop_sequence_number, 100);
                    assert_eq!(drops[0].kind, DropKind::Collection);
                    assert_eq!(drops[2].kind, DropKind::Collection);
                    assert_eq!(drops[0].drop_sequence_number, 200);
                    assert_eq!(drops[2].drop_sequence_number, 200);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_index_drop_at_same_time_that_collection_drop() {
            let levels = Levels::new(4);

            let index_drop_full = DropMetadata::new_index_drop(1, 1, 100);
            let collection_drop_full = DropMetadata::new_collection_drop(1, 100);

            let key_20 = encode_record_key(1, 1, &Bson::Int32(20).try_into_key().unwrap());
            let key_30 = encode_record_key(1, 1, &Bson::Int32(30).try_into_key().unwrap());
            let index_drop = split_to_range(&index_drop_full, &key_20, &key_30);

            let levels = levels.add(
                0,
                empty::<Arc<SSTableMetadata>>(),
                vec![index_drop.clone(), collection_drop_full.clone()],
            );

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 1, "Expected 1 drops {}", drops.len());
                    assert_eq!(drops[0].kind, DropKind::Collection);
                    assert_eq!(drops[0].drop_sequence_number, 100);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_empty_drops() {
            let levels = Levels::new(4);

            let levels = levels.add(0, empty::<Arc<SSTableMetadata>>(), empty::<Arc<DropMetadata>>());

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert!(drops.is_empty());
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_single_drop() {
            let levels = Levels::new(4);

            let drop = DropMetadata::new_index_drop(1, 1, 100);
            let levels = levels.add(0, empty::<Arc<SSTableMetadata>>(), vec![drop]);

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 1);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_adjacent_drops_not_merged() {
            let levels = Levels::new(4);

            let drop1 = DropMetadata::new_index_drop(1, 1, 100);
            let drop2 = DropMetadata::new_index_drop(2, 1, 200);

            let levels = levels.add(0, empty::<Arc<SSTableMetadata>>(), vec![drop1, drop2]);

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 2, "Adjacent drops should not be merged");
                }
                _ => panic!("Expected Overlapping level"),
            }
        }

        #[test]
        fn test_multiple_index_drops_split_collection() {
            let levels = Levels::new(4);

            let collection_drop_full = DropMetadata::new_collection_drop(1, 300);

            let idx1_full = DropMetadata::new_index_drop(1, 1, 100);
            let idx2_full = DropMetadata::new_index_drop(1, 2, 200);

            let key_20_i1 = encode_record_key(1, 1, &Bson::Int32(20).try_into_key().unwrap());
            let key_25_i1 = encode_record_key(1, 1, &Bson::Int32(25).try_into_key().unwrap());
            let key_40_i2 = encode_record_key(1, 2, &Bson::Int32(40).try_into_key().unwrap());
            let key_45_i2 = encode_record_key(1, 2, &Bson::Int32(45).try_into_key().unwrap());

            let idx1 = split_to_range(&idx1_full, &key_20_i1, &key_25_i1);
            let idx2 = split_to_range(&idx2_full, &key_40_i2, &key_45_i2);

            let levels = levels.add(
                0,
                empty::<Arc<SSTableMetadata>>(),
                vec![collection_drop_full, idx1, idx2],
            );

            let level = levels.level(0).unwrap();
            match level {
                Overlapping { drops, .. } => {
                    assert_eq!(drops.len(), 5, "Expected 5 drops after multiple splits, got {}", drops.len());
                    assert_eq!(drops[1].kind, DropKind::Index(1));
                    assert_eq!(drops[3].kind, DropKind::Index(2));
                    assert_eq!(drops[0].kind, DropKind::Collection);
                    assert_eq!(drops[2].kind, DropKind::Collection);
                    assert_eq!(drops[4].kind, DropKind::Collection);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }
    }

    mod split_at_tests {
        use super::*;
        use std::ops::Bound;

        fn record_key_mid(collection: u32, index: u32, val: i32) -> Vec<u8> {
            encode_record_key(collection, index, &Bson::Int32(val).try_into_key().unwrap())
        }

        #[test]
        fn test_split_collection_drop() {
            let col = 1;
            let seq = 42;
            let drop = DropMetadata::new_collection_drop(col, seq);

            let split_key = record_key_mid(col, 0, 100);
            let (left, right) = drop.split_at(&split_key).expect_two();

            assert_eq!(left.collection, col);
            assert_eq!(right.collection, col);
            assert_eq!(left.kind, DropKind::Collection);
            assert_eq!(right.kind, DropKind::Collection);
            assert_eq!(left.drop_sequence_number, seq);
            assert_eq!(right.drop_sequence_number, seq);

            assert_eq!(left.key_range.end_bound(), Bound::Included(&split_key));
            assert_eq!(right.key_range.start_bound(), Bound::Excluded(&split_key));

            assert_eq!(left.key_range.start_bound(), drop.key_range.start_bound());
            assert_eq!(right.key_range.end_bound(), drop.key_range.end_bound());
        }

        #[test]
        fn test_split_index_drop() {
            let col = 2;
            let idx = 3;
            let seq = 77;
            let drop = DropMetadata::new_index_drop(col, idx, seq);

            let split_key = record_key_mid(col, idx, 50);
            let (left, right) = drop.split_at(&split_key).expect_two();

            assert_eq!(left.collection, col);
            assert_eq!(right.collection, col);
            assert_eq!(left.kind, DropKind::Index(idx));
            assert_eq!(right.kind, DropKind::Index(idx));
            assert_eq!(left.drop_sequence_number, seq);
            assert_eq!(right.drop_sequence_number, seq);

            assert_eq!(left.key_range.end_bound(), Bound::Included(&split_key));
            assert_eq!(right.key_range.start_bound(), Bound::Excluded(&split_key));
            assert_eq!(left.key_range.start_bound(), drop.key_range.start_bound());
            assert_eq!(right.key_range.end_bound(), drop.key_range.end_bound());
        }

        #[test]
        fn test_split_boundary_semantics() {
            let col = 1;
            let idx = 1;
            let drop = DropMetadata::new_index_drop(col, idx, 10);

            let split_key = record_key_mid(col, idx, 50);
            let (left, right) = drop.split_at(&split_key).expect_two();

            assert!(left.key_range.contains(&split_key));
            assert!(!right.key_range.contains(&split_key));

            let mut key_after = split_key.clone();
            key_after.push(0);
            assert!(!left.key_range.contains(&key_after));
            assert!(right.key_range.contains(&key_after));
        }

        #[test]
        fn test_split_at_range_edges() {
            let col = 1;
            let idx = 1;
            let drop = DropMetadata::new_index_drop(col, idx, 5);

            if let Bound::Included(start) = drop.key_range.start_bound().cloned() {
                let (left_at_start, right_at_start) = drop.split_at(&start).expect_two();
                assert_eq!(left_at_start.key_range.start_bound(), Bound::Included(&start));
                assert_eq!(left_at_start.key_range.end_bound(), Bound::Included(&start));
                assert_eq!(right_at_start.key_range.start_bound(), Bound::Excluded(&start));
            }

            if let Bound::Included(end) = drop.key_range.end_bound().cloned() {
                let result = drop.split_at(&end);
                match result {
                    SplitResults::One(only) => {
                        assert_eq!(only.key_range.end_bound(), Bound::Included(&end));
                        assert_eq!(only.key_range.start_bound(), drop.key_range.start_bound());
                    }
                    SplitResults::Two(_, _) => panic!("expected One when splitting at end bound"),
                }
            }
        }

        #[test]
        fn test_split_recursive() {
            let col = 1;
            let idx = 1;
            let seq = 99;
            let drop = DropMetadata::new_index_drop(col, idx, seq);

            let split_key1 = record_key_mid(col, idx, 30);
            let split_key2 = record_key_mid(col, idx, 70);

            let (part1, remainder) = drop.split_at(&split_key1).expect_two();
            let (part2, part3) = remainder.split_at(&split_key2).expect_two();

            for part in [&part1, &part2, &part3] {
                assert_eq!(part.collection, col);
                assert_eq!(part.kind, DropKind::Index(idx));
                assert_eq!(part.drop_sequence_number, seq);
            }

            assert_eq!(part1.key_range.start_bound(), drop.key_range.start_bound());
            assert_eq!(part1.key_range.end_bound(), Bound::Included(&split_key1));

            assert_eq!(part2.key_range.start_bound(), Bound::Excluded(&split_key1));
            assert_eq!(part2.key_range.end_bound(), Bound::Included(&split_key2));

            assert_eq!(part3.key_range.start_bound(), Bound::Excluded(&split_key2));
            assert_eq!(part3.key_range.end_bound(), drop.key_range.end_bound());

            assert!(!part1.key_range.contains(&split_key2));
            assert!(!part2.key_range.contains(&split_key1));
        }

        #[test]
        #[should_panic(expected = "Split key must be within the drop's key range")]
        fn test_split_panics_outside_range() {
            let col = 1;
            let idx = 5;
            let drop = DropMetadata::new_index_drop(col, idx, 1);

            let outside_key = encode_record_key(col, 6, &Bson::Int32(1).try_into_key().unwrap());
            let _ = drop.split_at(&outside_key);
        }
    }

    mod pending_drops_invariant_tests {
        use super::*;
        use std::sync::Arc;

        #[test]
        fn test_with_flushed_sstable_partitions_pending_drops_and_preserves_sorted_order() {
            let version = LsmVersion::new(1, 10, 2);

            // Add drops in increasing order (the expected invariant).
            let version = version.add_collection_drop(10, 100);
            let version = version.add_collection_drop(20, 200);
            let version = version.add_collection_drop(30, 300);

            // Flush an SSTable up to seq=200: drops <= 200 should move into L0, >200 remain pending.
            let sst = Arc::new(SSTableMetadata::new(
                1,
                0,
                &record_key(1),
                &record_key(100),
                1,
                200,
                1024,
            ));
            let version2 = version.with_flushed_sstable(1, &sst);

            // Pending drops keep only seq 300.
            assert_eq!(version2.pending_drops.len(), 1);
            assert_eq!(version2.pending_drops[0].drop_sequence_number, 300);

            // Pending drops remain sorted.
            let seqs: Vec<u64> = version2.pending_drops.iter().map(|d| d.drop_sequence_number).collect();
            assert_eq!(seqs, vec![300]);

            // Flushed drops should be added to level 0.
            let l0 = version2.sst_levels.level(0).unwrap();
            match l0 {
                Overlapping { drops, .. } => {
                    let drop_seqs: Vec<u64> = drops.iter().map(|d| d.drop_sequence_number).collect();
                    assert_eq!(drop_seqs, vec![100, 200]);
                }
                _ => panic!("Expected Overlapping level"),
            }

            // get_drops_before_or_at should reflect only pending drops.
            let drops = version2.get_drops_before_or_at(u64::MAX);
            assert_eq!(drops.len(), 1);
            assert_eq!(drops[0].drop_sequence_number, 300);
        }

        #[test]
        fn test_with_flushed_sstable_when_all_drops_flushed_pending_empty_and_l0_contains_all_drops() {
            let version = LsmVersion::new(1, 10, 2);

            let version = version.add_collection_drop(10, 100);
            let version = version.add_collection_drop(20, 200);

            let sst = Arc::new(SSTableMetadata::new(
                1,
                0,
                &record_key(1),
                &record_key(100),
                1,
                500,
                1024,
            ));
            let version2 = version.with_flushed_sstable(1, &sst);

            assert!(version2.pending_drops.is_empty());

            let l0 = version2.sst_levels.level(0).unwrap();
            match l0 {
                Overlapping { drops, .. } => {
                    let drop_seqs: Vec<u64> = drops.iter().map(|d| d.drop_sequence_number).collect();
                    assert_eq!(drop_seqs, vec![100, 200]);
                }
                _ => panic!("Expected Overlapping level"),
            }
        }
    }

    mod span_tests {
        use super::*;

        #[test]
        fn test_span_empty() {
            let items: Vec<SSTableMetadata> = vec![];
            let result = span(&items);
            assert!(result.is_none());
        }

        #[test]
        fn test_span_single_sstable() {
            let sst = SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(20),
                100, 200, 1000,
            );
            let items = vec![sst];
            let result = span(&items);
            assert!(result.is_some());
            let interval = result.unwrap();
            assert_eq!(interval, Interval::closed(record_key(10), record_key(20)));
        }

        #[test]
        fn test_span_multiple_sstables_disjoint() {
            let sst1 = SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(20),
                100, 200, 1000,
            );
            let sst2 = SSTableMetadata::new(
                2, 0,
                &record_key(50),
                &record_key(60),
                201, 300, 1000,
            );
            let sst3 = SSTableMetadata::new(
                3, 0,
                &record_key(30),
                &record_key(40),
                301, 400, 1000,
            );
            let items = vec![sst1, sst2, sst3];
            let result = span(&items);
            assert!(result.is_some());
            let interval = result.unwrap();
            // Should span from min(10) to max(60)
            assert_eq!(interval, Interval::closed(record_key(10), record_key(60)));
        }

        #[test]
        fn test_span_multiple_sstables_overlapping() {
            let sst1 = SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(40),
                100, 200, 1000,
            );
            let sst2 = SSTableMetadata::new(
                2, 0,
                &record_key(30),
                &record_key(60),
                201, 300, 1000,
            );
            let items = vec![sst1, sst2];
            let result = span(&items);
            assert!(result.is_some());
            let interval = result.unwrap();
            assert_eq!(interval, Interval::closed(record_key(10), record_key(60)));
        }

        #[test]
        fn test_span_multiple_sstables_contained() {
            let sst1 = SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(100),
                100, 200, 1000,
            );
            let sst2 = SSTableMetadata::new(
                2, 0,
                &record_key(30),
                &record_key(50),
                201, 300, 1000,
            );
            let items = vec![sst1, sst2];
            let result = span(&items);
            assert!(result.is_some());
            let interval = result.unwrap();
            assert_eq!(interval, Interval::closed(record_key(10), record_key(100)));
        }

        #[test]
        fn test_span_single_drop() {
            let drop = DropMetadata::new_collection_drop(1, 100);
            let items = vec![drop.as_ref()];
            let result = span(items);
            assert!(result.is_some());
        }

        #[test]
        fn test_span_mixed_sstables_and_drops() {
            let sst = Arc::new(SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(20),
                100, 200, 1000,
            ));
            let drop = DropMetadata::new_collection_drop(1, 300);

            // Test with Arc<dyn LevelItem>
            let items: Vec<Arc<dyn LevelItem>> = vec![
                sst,
                drop,
            ];

            let result: Option<Interval<Vec<u8>>> = items
                .iter()
                .map(|item| item.record_key_range())
                .reduce(|acc, range| acc.span(&range));

            assert!(result.is_some());
        }

        #[test]
        fn test_span_arc_sstables() {
            let sst1 = Arc::new(SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(20),
                100, 200, 1000,
            ));
            let sst2 = Arc::new(SSTableMetadata::new(
                2, 0,
                &record_key(30),
                &record_key(40),
                201, 300, 1000,
            ));
            let items = vec![sst1, sst2];
            
            // span works with references to Arc<SSTableMetadata>
            let result = span(items.iter().map(|arc| arc.as_ref()));
            assert!(result.is_some());
            let interval = result.unwrap();
            assert_eq!(interval, Interval::closed(record_key(10), record_key(40)));
        }

        #[test]
        fn test_span_same_boundaries() {
            let sst1 = SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(20),
                100, 200, 1000,
            );
            let sst2 = SSTableMetadata::new(
                2, 0,
                &record_key(10),
                &record_key(20),
                201, 300, 1000,
            );
            let items = vec![sst1, sst2];
            let result = span(&items);
            assert!(result.is_some());
            let interval = result.unwrap();
            assert_eq!(interval, Interval::closed(record_key(10), record_key(20)));
        }

        #[test]
        fn test_span_adjacent_sstables() {
            let sst1 = SSTableMetadata::new(
                1, 0,
                &record_key(10),
                &record_key(20),
                100, 200, 1000,
            );
            let sst2 = SSTableMetadata::new(
                2, 0,
                &record_key(21),
                &record_key(30),
                201, 300, 1000,
            );
            let items = vec![sst1, sst2];
            let result = span(&items);
            assert!(result.is_some());
            let interval = result.unwrap();
            assert_eq!(interval, Interval::closed(record_key(10), record_key(30)));
        }
    }

    mod compaction_score_tests {
        use super::*;
        use crate::options::storage_quantity::{StorageQuantity, StorageUnit};

        fn test_db_options() -> Options {
            // level0_file_num_compaction_trigger = 4
            // max_bytes_for_level_base = 64 MiB
            // max_bytes_for_level_multiplier = 10.0
            Options::default()
                .with_level0_file_num_compaction_trigger(4)
                .with_max_bytes_for_level_base(StorageQuantity::new(64, StorageUnit::Mebibytes))
                .with_max_bytes_for_level_multiplier(10.0)
        }

        #[test]
        fn test_l0_compaction_score_by_file_count() {
            let opts = test_db_options();

            // 2 files, small total size -> file_score = 2/4 = 0.5, size_score ~ 0
            let level = Level::new(0, vec![
                create_sstable(1, 0, 1, 10, 100, 200, 1000),
                create_sstable(2, 0, 11, 20, 201, 300, 1000),
            ], vec![],000);
            let score = level.compaction_score(&opts);
            assert!((score - 0.5).abs() < 0.001, "Expected ~0.5, got {}", score);

            // 4 files -> file_score = 4/4 = 1.0
            let level = Level::new(0, vec![
                create_sstable(1, 0, 1, 10, 100, 200, 1000),
                create_sstable(2, 0, 11, 20, 201, 300, 1000),
                create_sstable(3, 0, 21, 30, 301, 400, 1000),
                create_sstable(4, 0, 31, 40, 401, 500, 1000),
            ], vec![],4000);
            let score = level.compaction_score(&opts);
            assert!((score - 1.0).abs() < 0.001, "Expected ~1.0, got {}", score);

            // 8 files -> file_score = 8/4 = 2.0
            let sstables: Vec<_> = (1..=8)
                .map(|i| create_sstable(i, 0, (i * 10) as i32, (i * 10 + 9) as i32, i * 100, i * 100 + 99, 1000))
                .collect();
            let level = Level::new(0, sstables, vec![],8000);
            let score = level.compaction_score(&opts);
            assert!((score - 2.0).abs() < 0.001, "Expected ~2.0, got {}", score);
        }

        #[test]
        fn test_l0_compaction_score_by_size() {
            let opts = test_db_options();
            let base_bytes = opts.max_bytes_for_level_base().to_bytes() as u64; // 64 MiB

            // 1 file but size = base_bytes -> size_score = 1.0, file_score = 0.25
            // max(0.25, 1.0) = 1.0
            let level = Level::new(0, vec![
                create_sstable(1, 0, 1, 100, 100, 200, base_bytes),
            ], vec![], base_bytes);
            let score = level.compaction_score(&opts);
            assert!((score - 1.0).abs() < 0.001, "Expected ~1.0, got {}", score);

            // 1 file, size = 2 * base_bytes -> size_score = 2.0
            let level = Level::new(0, vec![
                create_sstable(1, 0, 1, 100, 100, 200, base_bytes * 2),
            ], vec![],base_bytes * 2);
            let score = level.compaction_score(&opts);
            assert!((score - 2.0).abs() < 0.001, "Expected ~2.0, got {}", score);

            // 2 files, size = 0.5 * base_bytes -> size_score = 0.5, file_score = 0.5
            let half_size = base_bytes / 2;
            let level = Level::new(0, vec![
                create_sstable(1, 0, 1, 50, 100, 200, half_size / 2),
                create_sstable(2, 0, 51, 100, 201, 300, half_size / 2),
            ], vec![], half_size);
            let score = level.compaction_score(&opts);
            assert!((score - 0.5).abs() < 0.001, "Expected ~0.5, got {}", score);
        }

        #[test]
        fn test_l0_compaction_score_max_of_file_and_size() {
            let opts = test_db_options();
            let base_bytes = opts.max_bytes_for_level_base().to_bytes() as u64;

            // 6 files (file_score = 1.5), size = 0.5 * base (size_score = 0.5)
            // max(1.5, 0.5) = 1.5
            let half_size = base_bytes / 2;
            let per_file = half_size / 6;
            let sstables: Vec<_> = (1..=6)
                .map(|i| create_sstable(i, 0, (i * 10) as i32, (i * 10 + 9) as i32, i * 100, i * 100 + 99, per_file))
                .collect();
            let level = Level::new(0, sstables, vec![], half_size);
            let score = level.compaction_score(&opts);
            assert!((score - 1.5).abs() < 0.001, "Expected ~1.5, got {}", score);

            // 2 files (file_score = 0.5), size = 1.5 * base (size_score = 1.5)
            // max(0.5, 1.5) = 1.5
            let size = (base_bytes as f64 * 1.5) as u64;
            let level = Level::new(0, vec![
                create_sstable(1, 0, 1, 50, 100, 200, size / 2),
                create_sstable(2, 0, 51, 100, 201, 300, size / 2),
            ], vec![], size);
            let score = level.compaction_score(&opts);
            assert!((score - 1.5).abs() < 0.001, "Expected ~1.5, got {}", score);
        }

        #[test]
        fn test_l1_compaction_score() {
            let opts = test_db_options();
            let base_bytes = opts.max_bytes_for_level_base().to_bytes() as u64; // 64 MiB
            // L1 target = base_bytes * 10^(1-1) = base_bytes

            // Size = base_bytes -> score = 1.0
            let level = Level::new(1, vec![
                create_sstable(1, 1, 1, 100, 100, 200, base_bytes),
            ], vec![], base_bytes);
            let score = level.compaction_score(&opts);
            assert!((score - 1.0).abs() < 0.001, "Expected ~1.0, got {}", score);

            // Size = 0.5 * base_bytes -> score = 0.5
            let level = Level::new(1, vec![
                create_sstable(1, 1, 1, 100, 100, 200, base_bytes / 2),
            ], vec![], base_bytes / 2);
            let score = level.compaction_score(&opts);
            assert!((score - 0.5).abs() < 0.001, "Expected ~0.5, got {}", score);

            // Size = 2 * base_bytes -> score = 2.0
            let level = Level::new(1, vec![
                create_sstable(1, 1, 1, 100, 100, 200, base_bytes * 2),
            ], vec![], base_bytes * 2);
            let score = level.compaction_score(&opts);
            assert!((score - 2.0).abs() < 0.001, "Expected ~2.0, got {}", score);
        }

        #[test]
        fn test_l2_compaction_score() {
            let opts = test_db_options();
            let base_bytes = opts.max_bytes_for_level_base().to_bytes() as u64;
            let multiplier = opts.max_bytes_for_level_multiplier();
            // L2 target = base_bytes * 10^(2-1) = base_bytes * 10

            let target = (base_bytes as f64 * multiplier) as u64;

            // Size = target -> score = 1.0
            let level = Level::new(2, vec![
                create_sstable(1, 2, 1, 100, 100, 200, target),
            ], vec![], target);
            let score = level.compaction_score(&opts);
            assert!((score - 1.0).abs() < 0.001, "Expected ~1.0, got {}", score);

            // Size = 0.5 * target -> score = 0.5
            let level = Level::new(2, vec![
                create_sstable(1, 2, 1, 100, 100, 200, target / 2),
            ], vec![], target / 2);
            let score = level.compaction_score(&opts);
            assert!((score - 0.5).abs() < 0.001, "Expected ~0.5, got {}", score);
        }

        #[test]
        fn test_l3_compaction_score() {
            let opts = test_db_options();
            let base_bytes = opts.max_bytes_for_level_base().to_bytes() as u64;
            let multiplier = opts.max_bytes_for_level_multiplier();
            // L3 target = base_bytes * 10^(3-1) = base_bytes * 100

            let target = (base_bytes as f64 * multiplier.powi(2)) as u64;

            // Size = target -> score = 1.0
            let level = Level::new(3, vec![
                create_sstable(1, 3, 1, 100, 100, 200, target),
            ], vec![], target);
            let score = level.compaction_score(&opts);
            assert!((score - 1.0).abs() < 0.001, "Expected ~1.0, got {}", score);

            // Size = 1.5 * target -> score = 1.5
            let size = (target as f64 * 1.5) as u64;
            let level = Level::new(3, vec![
                create_sstable(1, 3, 1, 100, 100, 200, size),
            ], vec![], size);
            let score = level.compaction_score(&opts);
            assert!((score - 1.5).abs() < 0.001, "Expected ~1.5, got {}", score);
        }

        #[test]
        fn test_empty_level_compaction_score() {
            let opts = test_db_options();

            // Empty L0 -> file_score = 0, size_score = 0
            let level = Level::empty(0);
            let score = level.compaction_score(&opts);
            assert!((score - 0.0).abs() < 0.001, "Expected 0.0, got {}", score);

            // Empty L1 -> score = 0
            let level = Level::empty(1);
            let score = level.compaction_score(&opts);
            assert!((score - 0.0).abs() < 0.001, "Expected 0.0, got {}", score);

            // Empty L2 -> score = 0
            let level = Level::empty(2);
            let score = level.compaction_score(&opts);
            assert!((score - 0.0).abs() < 0.001, "Expected 0.0, got {}", score);
        }

    }

    fn create_sst(number: u64, level: u8, min: i32, max: i32, size: u64) -> Arc<SSTableMetadata> {
        Arc::new(SSTableMetadata::new(
            number,
            level,
            &record_key(min),
            &record_key(max),
            number * 100,
            number * 100 + 50,
            size,
        ))
    }
}
