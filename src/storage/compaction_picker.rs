//! Compaction picker for the LSM tree.
//!
//! The compaction picker examines the current LSM tree state and decides which
//! compactions should be run. It uses a **2L-Spooky** compaction strategy.
//!
//! # 2L-Spooky Compaction Strategy
//!
//! The "2L-Spooky" approach limits partial/partitioned compaction to only the
//! **bottom two levels** (L−1 and L, where L is the max level index). All levels
//! above use full preemptive merge.
//!
//! - **Full Compaction** (all levels above bottom two): All transitively overlapping
//!   files from both input and output levels are merged together. This ensures
//!   correctness for levels where files may overlap (L0) and maintains sorted runs.
//!
//! - **Partial Compaction** (bottom two levels only): Only files within the same
//!   partition are merged. Partitions are determined by the max level's SSTable
//!   boundaries, limiting write amplification.
//!
//! For example, with `max_levels=4` (L0, L1, L2, L3):
//! - Full compaction: L0→L1, L1→L2
//! - Partial compaction: L2→L3 (compacting into L3, one of the bottom two levels)
//!
//! With `max_levels=7` (L0 through L6):
//! - Full compaction: L0→L1, L1→L2, L2→L3, L3→L4
//! - Partial compaction: L4→L5, L5→L6 (bottom two levels are L5, L6)
//!
//! # Compaction Scores
//!
//! - **L0**: `max(file_count / trigger, size / base_bytes)`
//! - **Ln**: `size / target_bytes_for_level`
//!
//! A level needs compaction when its score exceeds 1.0.

use std::ops::Bound;
use crate::options::options::Options;
use crate::storage::lsm_version::{Level, Levels, SSTableMetadata};
use crate::util::interval::{has_overlapping_intervals, merge_overlapping_intervals, Interval};
use std::sync::Arc;
use crate::storage::lsm_version::Level::{NonOverlapping, Overlapping};

/// Describes a compaction job to be executed.
///
/// Contains all the information needed to perform a compaction: which files
/// to read from the input level, which files to merge from the output level,
/// and metadata about the compaction bounds.
#[derive(Debug, Clone)]
pub struct CompactionJob {
    /// The source level (e.g., 0 for L0 → L1 compaction).
    pub input_level: u8,
    /// The target level (e.g., 1 for L0 → L1 compaction).
    pub output_level: u8,
    /// Files to compact from the input level.
    pub input_files: Vec<Arc<SSTableMetadata>>,
    /// Files from the output level that overlap with the input files.
    pub output_files: Vec<Arc<SSTableMetadata>>,
    /// The key range covered by this compaction in the input level.
    pub input_key_ranges: Vec<Interval<Vec<u8>>>,
    /// The key range covered by this compaction in the output level.
    pub output_key_ranges: Vec<Interval<Vec<u8>>>,
    /// For partial compactions, the partitions grid used to determine the sstable splits.
    /// None for full compactions.
    pub partitions_grid: Option<Vec<Vec<u8>>>,
}

/// Tracks compaction scores and pending state for picking compactions.
///
/// The picker maintains ranges being compacted per level to control parallelism.
///
/// # Parallelism Rules
///
/// - **Full compaction**: No parallel compactions allowed for the source or target
///   level. If any range is being compacted (full or partial), no new full compaction
///   can start for those levels. If a full compaction is active, no other compaction
///   can start for those levels.
///
/// - **Partial compaction**: Parallel compactions are allowed for different partitions.
///   Only overlapping ranges are blocked.
#[derive(Debug, Default)]
pub struct CompactionPicker {
    /// The database options.
    options: Options,
    /// The largest level index (Lmax) in the LSM tree. Levels are indexed from 0 to Lmax.
    level_l: usize,
    /// The smallest level at which we start partitioning runs based on the file boundaries at the largest level
    level_x: usize,
    /// Ranges being compacted per level: level -> list of ranges.
    compacting_ranges: Vec<Vec<Interval<Vec<u8>>>>,
}

/// Result of computing compaction scores for all levels.
#[derive(Debug, Clone)]
struct CompactionScores {
    /// Score for each level, indexed by level number.
    pub scores: Vec<f64>,
}

impl CompactionScores {
    /// Returns an iterator over levels needing compaction (score > 1.0),
    /// ordered by descending score.
    fn levels_needing_compaction(&self) -> impl Iterator<Item = usize> {
        let mut levels_with_scores: Vec<(usize, f64)> = self
            .scores
            .iter()
            .enumerate()
            .filter(|(_, &score)| score > 1.0)
            .map(|(level, &score)| (level, score))
            .collect();

        levels_with_scores.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap());

        levels_with_scores.into_iter().map(|(level, _)| level)
    }
}

impl CompactionPicker {
    /// Creates a new compaction picker.
    pub fn new(options: &Options) -> Self {
        let max_levels = options.max_levels();
        let level_l = max_levels - 1;
        assert!(max_levels >= 2, "max_levels must be at least 2 for 2L-Spooky compaction");
        let level_x = max_levels - 2; // Start partitioning from the second to last level
        CompactionPicker {
            options: options.clone(),
            level_l,
            level_x,
            compacting_ranges: vec![Vec::new(); max_levels],
        }
    }

    /// Computes compaction scores for all levels.
    ///
    /// The last level always has a score of 0.0 since there is nowhere to compact to.
    fn compute_scores(&self, levels: &Levels) -> CompactionScores {
        let max_levels = self.options.max_levels();
        let mut scores = Vec::with_capacity(max_levels);

        for level_num in 0..max_levels as usize {
            // Last level always has score 0 - there's nowhere to compact to
            let score = if level_num == self.level_l {
                0.0
            } else {
                levels
                    .level(level_num)
                    .map(|level| level.compaction_score(&self.options))
                    .unwrap_or(0.0)
            };
            scores.push(score);
        }

        CompactionScores { scores }
    }

    /// Picks the next compaction to run, if any.
    ///
    /// Returns `None` if no compaction is needed (all scores <= 1.0) or if
    /// all candidate files are already being compacted.
    ///
    /// Tries levels in descending order of compaction score, so if the highest
    /// scoring level cannot be compacted (e.g., all files are being compacted),
    /// it will try the next highest scoring level.
    ///
    /// Uses 2L-Spooky strategy:
    /// - **Full compaction** for all levels above the bottom two (L0→L1, ..., L(max-3)→L(max-2))
    /// - **Partial compaction** for compactions into the bottom two levels (L(max-2)→L(max-1) and L(max-1)→L(max))
    ///
    /// For example, with max_levels=4 (L0, L1, L2, L3):
    /// - Full compaction: L0→L1, L1→L2
    /// - Partial compaction: L2→L3
    ///
    /// # Parallelism
    ///
    /// - Full compaction blocks entire levels: if any compaction (full or partial) is
    ///   active on the input or output level, no full compaction can start.
    /// - Partial compaction allows parallelism: only overlapping ranges are blocked.
    ///
    /// # Auto-marking
    ///
    /// When a compaction job is returned, it is automatically marked as compacting.
    /// The caller must call `unmark_compacting()` when the compaction completes
    /// (successfully or not). This design prevents race conditions between picking
    /// and marking, following the pattern used by RocksDB and Pebble.
    pub fn pick_compaction(&mut self, levels: &Levels) -> Option<CompactionJob> {
        let scores = self.compute_scores(levels);

        for input_level in scores.levels_needing_compaction() {
            let output_level = input_level + 1;

            let Some(input) = levels.level(input_level) else {
                continue;
            };
            let output = levels.level(output_level);

            // 2L-Spooky: partial compaction only when compacting into the bottom two levels
            let use_partial = input_level >= self.level_x;

            // Compute partition boundaries from the max level for compactions involving the bottom
            // two levels.
            let partitions_grid = if output_level >= self.level_x {
                Some(self.compute_partition_boundaries(levels))
            } else {
                None
            };

            if use_partial {
                // Partial compaction: range checking is done inside pick_partial_compaction
                if let Some(job) = self.pick_partial_compaction(
                    input,
                    output,
                    input_level,
                    output_level,
                    partitions_grid,
                ) {
                    self.mark_compacting(&job);
                    return Some(job);
                }
            } else {
                // Full compaction: block if any compaction is active on either level
                if self.is_level_compacting(input_level) || self.is_level_compacting(output_level) {
                    continue;
                }

                if let Some(job) = self.pick_full_compaction(input, output, input_level, output_level, partitions_grid) {
                    self.mark_compacting(&job);
                    return Some(job);
                }
            }
        }

        None
    }

    /// Returns true if any compaction is active on the given level.
    ///
    /// Used for full compaction blocking: if any range (full or partial) is being
    /// compacted on a level, full compactions involving that level are blocked.
    fn is_level_compacting(&self, level: usize) -> bool {
        self.compacting_ranges
            .get(level)
            .map(|ranges| !ranges.is_empty())
            .unwrap_or(false)
    }

    /// Returns true if there's an overlapping range being compacted on the given level.
    ///
    /// Used for partial compaction: only blocks if ranges actually overlap.
    fn are_ranges_compacting(&self, level: usize, key_ranges: &[Interval<Vec<u8>>]) -> bool {
        println!("compacting ranges for level {}: {:?}", level, self.compacting_ranges.get(level).unwrap_or(&Vec::new()));
        has_overlapping_intervals(key_ranges, self.compacting_ranges.get(level).unwrap_or(&Vec::new()))
    }

    /// Picks a full compaction for levels above the bottom two.
    ///
    /// Full compaction takes all files from the source level and merges them with
    /// all overlapping files in the target level. This is simpler than partial
    /// compaction and ensures all data is properly merged.
    fn pick_full_compaction(
        &self,
        input: &Level,
        output: Option<&Level>,
        input_level: usize,
        output_level: usize,
        partitions_grid: Option<Vec<Vec<u8>>>,
    ) -> Option<CompactionJob> {

        // Get all input files from the source level
        let input_files: Vec<_> = match input {
            Overlapping { sstables, .. } | NonOverlapping { sstables, .. } => {
                sstables.iter().cloned().collect()
            }
        };

        if input_files.is_empty() {
            return None;
        }

        // We want to avoid involving more files than necessary in the compaction, so we compute
        // the exact set of key ranges covered by the input files and only compact with overlapping
        // files in the output level. This is especially important for L0→L1 compactions where L0
        // files can be very small, and we want to avoid merging with the entire L1 level if only
        // a small portion overlaps.
        let input_key_ranges = self.collect_key_ranges(input_level, &input_files);

        // Find all overlapping files in the output level
        let output_files = self.find_overlapping_files(output, &input_key_ranges);

        let output_key_ranges = self.collect_key_ranges(output_level, &output_files);

        Some(CompactionJob {
            input_level: input_level as u8,
            output_level: output_level as u8,
            input_files,
            output_files,
            input_key_ranges,
            output_key_ranges,
            partitions_grid,
        })
    }

    /// Picks a partial compaction for L2+→Ln.
    ///
    /// Partial compaction limits the scope to files within the same partition.
    /// Partitions are determined by the max level's SSTable boundaries.
    fn pick_partial_compaction(
        &self,
        input: &Level,
        output: Option<&Level>,
        input_level: usize,
        output_level: usize,
        partitions_grid: Option<Vec<Vec<u8>>>,
    ) -> Option<CompactionJob> {
        let NonOverlapping { sstables, .. } = input else {
            panic!("Expected NonOverlapping level for partial compaction, found Overlapping");
        };

        // Find the oldest file in the input level (smallest min_sequence_number) and try to
        // compact it, then move to the next oldest, etc.
        let mut sstables: Vec<&Arc<SSTableMetadata>> = sstables.iter()
            .collect();

        sstables.sort_by(|a, b| a.min_sequence_number.cmp(&b.min_sequence_number));

        for input_file in sstables {
            let input_key_range = input_file.record_key_range();

            if self.are_ranges_compacting(input_level, &[input_key_range.clone()]) {
                continue;
            }

            // Due to the way partitions are split at the lowest level we can have files at a
            // higher level covering multiple files at a deeper level but not the opposite way
            // around (at least for now). This means that we can have a file at L2 covering multiple
            // partitions defined by L3 files but not the opposite way around.

            // Find partition range associated to the input file
            let partition_key_range = self.find_partition_key_range_for_file(&input_file, partitions_grid.as_ref().unwrap());

            // Find files in the output level that are within the same partition range
            let output_files = if let Some(output) = output {
                output.find_range(&partition_key_range, u64::MAX)
            } else {
                Vec::new()
            };

            let output_key_ranges = self.collect_key_ranges(output_level, &output_files);

            // Check if the range overlaps with any compaction on the output level
            if self.are_ranges_compacting(output_level, &output_key_ranges) {
                continue;
            }

            return Some(CompactionJob {
                input_level: input_level as u8,
                output_level: output_level as u8,
                input_files: vec![input_file.clone()],
                output_files,
                input_key_ranges: vec![input_key_range],
                output_key_ranges,
                partitions_grid,
            });
        }

        None
    }

    /// Computes the key ranges for a set of SSTables. For L0 (overlapping), merges overlapping ranges.
    /// For higher levels (non-overlapping), returns the individual ranges since they are guaranteed not to overlap.
    fn collect_key_ranges(&self,
                          level: usize,
                          sstables: &Vec<Arc<SSTableMetadata>>
    ) -> Vec<Interval<Vec<u8>>> {

        let ranges = sstables.into_iter().map(|sst| sst.record_key_range()).collect();
        if level == 0 {
            merge_overlapping_intervals(ranges)
        } else {
            ranges
        }
    }

    /// Finds all files in a level that overlap with the given key range.
    fn find_overlapping_files(
        &self,
        level: Option<&Level>,
        ranges: &[Interval<Vec<u8>>],
    ) -> Vec<Arc<SSTableMetadata>> {

        let Some(level) = level else {
            return Vec::new();
        };

        let sstables = match level {
            NonOverlapping { sstables, .. } => sstables,
            Overlapping { .. } =>
                panic!("Expected NonOverlapping level for find_overlapping_files, found Overlapping"),
        };

        let mut result = Vec::new();
        let mut range_idx = 0;
        let mut sstable_idx = 0;

        while range_idx < ranges.len() && sstable_idx < sstables.len() {
            let range = &ranges[range_idx];
            let sstable = &sstables[sstable_idx];

            // Check if sstable max_key is before range lower bound (no overlap possible)
            if sstable.max_key < range.start_bound_value().unwrap() {
                sstable_idx += 1;
                continue;
            }

            // Check if filter ends before candidate starts (no overlap possible)
            if range.end_bound_value().unwrap() < sstable.min_key {
                range_idx += 1;
                continue;
            }

            // They overlap - add candidate to result
            result.push(sstable.clone());
            sstable_idx += 1;
        }

        result
    }

    /// Marks a compaction job as active.
    ///
    /// Call this when a compaction job is scheduled to track ranges for parallelism control.
    pub fn mark_compacting(&mut self, job: &CompactionJob) {
        let level = job.input_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        for range in &job.input_key_ranges {
            ranges.push(range.clone());
            ranges.sort(); // Keep ranges sorted for efficient overlap checks
        }

        let level = job.output_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        for range in &job.output_key_ranges {
            ranges.push(range.clone());
            ranges.sort(); // Keep ranges sorted for efficient overlap checks
        }
    }

    /// Unmarks a compaction job as active.
    ///
    /// Call this when a compaction job completes (successfully or not).
    pub fn unmark_compacting(&mut self, job: &CompactionJob) {
        let level = job.input_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        for range in &job.input_key_ranges {
            if let Some(pos) = ranges.iter().position(|r| r == range) {
                ranges.remove(pos);
            }
        }

        let level = job.output_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        for range in &job.output_key_ranges {
            if let Some(pos) = ranges.iter().position(|r| r == range) {
                ranges.remove(pos);
            }
        }
    }

    /// Finds the partition index (0 to N) for a given key based on provided boundaries.
    ///
    /// Boundaries are the upper bounds of partitions (except the last partition).
    /// Uses binary search for efficient lookup.
    fn find_partition_for_key(&self, key: &[u8], boundaries: &[Vec<u8>]) -> usize {
        boundaries.binary_search_by(|probe| probe.as_slice().cmp(key)).unwrap_or_else(|idx| idx)
    }

    /// Finds the key range interval for the partition(s) that an SSTable overlaps with.
    ///
    /// Returns an interval representing the full key range covered by the partitions
    /// that the file spans. The interval bounds are:
    /// - Start: exclusive bound at the previous partition's boundary, or unbounded for partition 0
    /// - End: inclusive bound at the end partition's boundary, or unbounded for the last partition
    fn find_partition_key_range_for_file(
        &self,
        file: &SSTableMetadata,
        boundaries: &[Vec<u8>],
    ) -> Interval<Vec<u8>> {
        let start_partition = self.find_partition_for_key(&file.min_key, boundaries);
        let end_partition = self.find_partition_for_key(&file.max_key, boundaries);

        // Start bound: if partition 0, unbounded; otherwise exclusive at boundary[start_partition - 1]
        let start_bound = if start_partition == 0 {
            Bound::Unbounded
        } else {
            Bound::Excluded(boundaries[start_partition - 1].clone())
        };

        // End bound: if last partition (>= boundaries.len()), unbounded; otherwise inclusive at boundary[end_partition]
        let end_bound = if end_partition >= boundaries.len() {
            Bound::Unbounded
        } else {
            Bound::Included(boundaries[end_partition].clone())
        };

        Interval::new(start_bound, end_bound)
    }

    /// Computes partition boundaries based on the max level's structure.
    ///
    /// This always looks at the max level (deepest possible level) to determine
    /// partition boundaries. If the max level is empty, there is effectively a
    /// single partition (no boundaries). This approach ensures stable partition
    /// shapes even when compaction merges files into previously empty levels.
    ///
    /// If the max level has N SSTables, it returns N-1 boundary keys (the max_key
    /// of each SSTable except the last).
    pub fn compute_partition_boundaries(&self, levels: &Levels) -> Vec<Vec<u8>> {
        let max_level_idx = self.options.max_levels() - 1;

        let Some(level) = levels.level(max_level_idx) else {
            return Vec::new();
        };

        match level {
            NonOverlapping { sstables, .. } => {
                if sstables.len() <= 1 {
                    return Vec::new();
                }
                // Return the max_key of all but the last SSTable.
                sstables
                    .iter()
                    .take(sstables.len() - 1)
                    .map(|sst| sst.max_key.clone())
                    .collect()
            }
            Overlapping { .. } => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use std::ops::RangeBounds;

    fn test_options() -> Options {
        Options::default()
            .with_max_levels(4)
            .with_level0_file_num_compaction_trigger(4)
            .with_max_bytes_for_level_base(StorageQuantity::new(64, StorageUnit::Mebibytes))
            .with_max_bytes_for_level_multiplier(10.0)
    }

    fn create_sst(number: u64, level: u8, min: u32, max: u32, size: u64) -> Arc<SSTableMetadata> {
        Arc::new(SSTableMetadata::new(
            number,
            level,
            &min.to_be_bytes(),
            &max.to_be_bytes(),
            number * 100,      // min_sequence_number: distinct per SST for deterministic ordering
            number * 100 + 50, // max_sequence_number
            size,
        ))
    }

    #[test]
    fn test_no_compaction_needed_empty_levels() {
        let mut picker = CompactionPicker::new(&test_options());
        let levels = Levels::default();

        let scores = picker.compute_scores(&levels);
        assert_eq!(scores.levels_needing_compaction().count(), 0);

        let job = picker.pick_compaction(&levels);
        assert!(job.is_none());
    }

    #[test]
    fn test_l0_compaction_triggered_by_file_count() {
        let mut picker = CompactionPicker::new(&test_options());

        // Create L0 with 5 files (trigger is 4)
        let mut levels = Levels::default();
        for i in 1..=5 {
            let sst = create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000);
            levels = levels.add_sst(sst);
        }

        let scores = picker.compute_scores(&levels);
        assert!(scores.scores[0] > 1.0);
        let levels_needing: Vec<_> = scores.levels_needing_compaction().collect();
        assert_eq!(levels_needing, vec![0]);

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 0);
        assert_eq!(job.output_level, 1);
        assert!(!job.input_files.is_empty());
    }

    #[test]
    fn test_l0_compaction_takes_all_files() {
        let mut picker = CompactionPicker::new(&test_options());

        // Create overlapping L0 files
        // File 1: keys 10-30
        // File 2: keys 20-40 (overlaps with 1)
        // File 3: keys 35-50 (overlaps with 2)
        // File 4: keys 100-110 (does not overlap with others)
        // File 5: keys 25-45 (overlaps with 1, 2, 3)
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 0, 10, 30, 1000));
        levels = levels.add_sst(create_sst(2, 0, 20, 40, 1000));
        levels = levels.add_sst(create_sst(3, 0, 35, 50, 1000));
        levels = levels.add_sst(create_sst(4, 0, 100, 110, 1000));
        levels = levels.add_sst(create_sst(5, 0, 25, 45, 1000));

        let job = picker.pick_compaction(&levels).unwrap();

        // Full compaction takes ALL L0 files
        assert_eq!(job.input_level, 0);
        assert_eq!(job.input_key_ranges, vec![interval(10, 50), interval(100, 110)]);
        assert_eq!(job.output_level, 1);
        assert_eq!(job.output_key_ranges, Vec::<Interval<Vec<u8>>>::new());
        let input_numbers: Vec<u64> = job.input_files.iter().map(|f| f.number).collect();
        assert!(input_numbers.contains(&1));
        assert!(input_numbers.contains(&2));
        assert!(input_numbers.contains(&3));
        assert!(input_numbers.contains(&4));
        assert!(input_numbers.contains(&5));
        assert_eq!(input_numbers.len(), 5);
    }

    #[test]
    fn test_l0_compaction_includes_l1_overlap() {
        let mut picker = CompactionPicker::new(&test_options());

        // Create L0 files
        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, 10, 50, 1000));
        }

        // Create L1 files
        levels = levels.add_sst(create_sst(10, 1, 5, 25, 1000)); // overlaps
        levels = levels.add_sst(create_sst(11, 1, 30, 60, 1000)); // overlaps
        levels = levels.add_sst(create_sst(12, 1, 100, 150, 1000)); // does not overlap

        let job = picker.pick_compaction(&levels).unwrap();

        let output_numbers: Vec<u64> = job.output_files.iter().map(|f| f.number).collect();
        assert!(output_numbers.contains(&10));
        assert!(output_numbers.contains(&11));
        assert!(!output_numbers.contains(&12));
        assert_eq!(job.input_key_ranges, vec![interval(10, 50)]);
        assert_eq!(job.output_key_ranges, vec![interval(5, 25), interval(30, 60)]);
    }

    #[test]
    fn test_l0_compaction_finds_overlapping_l1_files() {
        let mut picker = CompactionPicker::new(&test_options());

        // L0 File: [100, 110]
        // L1 File A: [90, 105] (Overlaps L0)
        // L1 File B: [106, 115] (Overlaps L0)
        // L1 File C: [200, 210] (No overlap)
        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, 100, 110, 1000));
        }
        levels = levels.add_sst(create_sst(10, 1, 90, 105, 1000));
        levels = levels.add_sst(create_sst(11, 1, 106, 115, 1000));
        levels = levels.add_sst(create_sst(12, 1, 200, 210, 1000));

        let job = picker.pick_compaction(&levels).unwrap();

        let output_numbers: Vec<u64> = job.output_files.iter().map(|f| f.number).collect();
        // Should include both 10 and 11 because they overlap with L0's range [100, 110]
        assert!(output_numbers.contains(&10));
        assert!(output_numbers.contains(&11));
        assert!(!output_numbers.contains(&12));

        assert_eq!(job.input_key_ranges, vec![interval(100, 110)]);
        assert_eq!(job.output_key_ranges, vec![interval(90, 105), interval(106, 115)]);
    }

    #[test]
    fn test_level_compaction_triggered_by_size() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let mut picker = CompactionPicker::new(&options);

        // Create L1 with size > base_bytes
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 1, 10, 50, base_bytes * 2));

        let scores = picker.compute_scores(&levels);
        assert!(scores.scores[1] > 1.0);
        let levels_needing: Vec<_> = scores.levels_needing_compaction().collect();
        assert_eq!(levels_needing, vec![1]);

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 1);
        assert_eq!(job.output_level, 2);
        assert_eq!(job.input_files.len(), 1);
    }

    #[test]
    fn test_pick_auto_marks_and_unmark_compacting() {
        let mut picker = CompactionPicker::new(&test_options());

        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000));
        }

        // pick_compaction automatically marks the job
        let job = picker.pick_compaction(&levels).unwrap();

        // Picking again should return None (level is blocked)
        let job2 = picker.pick_compaction(&levels);
        assert!(job2.is_none());

        // Unmark
        picker.unmark_compacting(&job);

        // Can pick again
        let job3 = picker.pick_compaction(&levels);
        assert!(job3.is_some());
    }

    #[test]
    fn test_full_compaction_l0_to_l1() {
        // With max_levels=4, L0→L1 is full compaction (output_level 1 < max_levels-1 = 3)
        let mut picker = CompactionPicker::new(&test_options());

        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, 10, 50, 1000));
        }
        levels = levels.add_sst(create_sst(10, 1, 5, 25, 1000));
        levels = levels.add_sst(create_sst(11, 1, 30, 60, 1000));

        let job = picker.pick_compaction(&levels).unwrap();

        assert_eq!(job.input_level, 0);
        assert_eq!(job.output_level, 1);
        assert_eq!(job.input_key_ranges, vec![interval(10, 50)]);
        assert_eq!(job.output_key_ranges, vec![interval(5, 25), interval(30, 60)]);
        assert!(job.partitions_grid.is_none(), "L0→L1 should use full compaction");
        assert!(!job.input_files.is_empty());
    }

    #[test]
    fn test_full_compaction_l1_to_l2() {
        // With max_levels=4, L1→L2 is full compaction (output_level 2 < max_levels-1 = 3)
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let mut picker = CompactionPicker::new(&options);

        let mut levels = Levels::default();
        // L1 with size > base_bytes to trigger compaction
        levels = levels.add_sst(create_sst(1, 1, 10, 30, base_bytes));
        levels = levels.add_sst(create_sst(2, 1, 31, 50, base_bytes));

        // L2 files
        levels = levels.add_sst(create_sst(10, 2, 5, 25, 1000));
        levels = levels.add_sst(create_sst(11, 2, 26, 60, 1000));

        let job = picker.pick_compaction(&levels).unwrap();

        assert_eq!(job.input_level, 1);
        assert_eq!(job.input_key_ranges, vec![interval(10, 30), interval(31, 50)]);
        assert_eq!(job.output_level, 2);
        assert_eq!(job.output_key_ranges, vec![interval(5, 25), interval(26, 60)]);
        assert_eq!(job.partitions_grid, Some(vec![]), "L1→L2 should partition on output");
    }

    #[test]
    fn test_partial_compaction_l2_to_l3() {
        // With max_levels=4, L2→L3 is partial compaction (output_level 3 >= max_levels-1 = 3)
        // This is 2L-Spooky: only bottom two levels (L2, L3) use partial compaction
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = CompactionPicker::new(&options);

        let mut levels = Levels::default();

        // L2 with size > target to trigger compaction
        levels = levels.add_sst(create_sst(1, 2, 10, 30, 2 * l2_target));

        // L3 files that define partitions
        levels = levels.add_sst(create_sst(10, 3, 0, 20, 1000));
        levels = levels.add_sst(create_sst(11, 3, 21, 40, 1000));
        levels = levels.add_sst(create_sst(12, 3, 41, 60, 1000));

        let job = picker.pick_compaction(&levels).unwrap();

        assert_eq!(job.input_level, 2);
        assert_eq!(job.output_level, 3);
        assert_eq!(job.input_key_ranges, vec![interval(10, 30)]);
        assert_eq!(job.output_key_ranges, vec![interval(0, 20), interval(21, 40)]);
        assert_eq!(job.partitions_grid, Some(vec![record_key(20), record_key(40)]), "L2→L3 should use partial compaction (2L-Spooky)");
    }

    #[test]
    fn test_2l_spooky_with_more_levels() {
        // Test with max_levels=6 to verify 2L-Spooky behavior:
        // L0→L1, L1→L2, L2→L3, L3→L4 = full compaction (output < 5)
        // L4→L5 = partial compaction (output >= 5)
        let options = Arc::new(Options::default()
            .with_max_levels(6)
            .with_level0_file_num_compaction_trigger(4)
            .with_max_bytes_for_level_base(StorageQuantity::new(64, StorageUnit::Mebibytes))
            .with_max_bytes_for_level_multiplier(10.0));

        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l3_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier().powi(2)) as u64;
        let l4_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier().powi(3)) as u64;

        let mut picker = CompactionPicker::new(&options);

        // Verify L3→L4 uses full compaction (output_level 4 < max_levels-1 = 5)
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 3, 10, 30, l3_target * 2));
        levels = levels.add_sst(create_sst(10, 4, 5, 25, 1000));

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 3);
        assert_eq!(job.input_key_ranges, vec![interval(10, 30)]);
        assert_eq!(job.output_level, 4);
        assert_eq!(job.output_key_ranges, vec![interval(5, 25)]);
        assert_eq!(job.partitions_grid, Some(vec![]), "L3→L4 should partition the output");

        // Unmark compaction to allow compaction on the next level
        picker.unmark_compacting(&job);

        // Verify L4→L5 uses partial compaction (output_level 5 >= max_levels-1 = 5)
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 4, 10, 30, l4_target * 2));
        levels = levels.add_sst(create_sst(10, 5, 0, 20, 1000));
        levels = levels.add_sst(create_sst(11, 5, 21, 40, 1000));

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 4);
        assert_eq!(job.input_key_ranges, vec![interval(10, 30)]);
        assert_eq!(job.output_level, 5);
        assert_eq!(job.output_key_ranges,
                   vec![interval(0, 20), interval(21, 40),]
        );
        assert_eq!(job.partitions_grid,
                   Some(vec![record_key(20)]),
                   "L4→L5 should use partial compaction with max_levels=6");
    }

    fn interval(min: u32, max: u32) -> Interval<Vec<u8>> {
        Interval::closed(record_key(min), record_key(max))
    }

    fn record_key(k: u32) -> Vec<u8> {
        k.to_be_bytes().to_vec()
    }

    #[test]
    fn test_last_level_never_selected_even_with_other_levels() {
        let options = test_options(); // max_levels = 4
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let multiplier = options.max_bytes_for_level_multiplier();

        let mut picker = CompactionPicker::new(&test_options());

        let l3_target = (base_bytes as f64 * multiplier.powi(2)) as u64;

        // Create L0 with score < 1.0 (only 2 files, trigger is 4)
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 0, 10, 20, 1000));
        levels = levels.add_sst(create_sst(2, 0, 30, 40, 1000));

        // Create L3 (last level) with massive size
        levels = levels.add_sst(create_sst(100, 3, 10, 50, l3_target * 100));

        let scores = picker.compute_scores(&levels);

        // L0 score should be 0.5 (2 files / 4 trigger)
        assert!((scores.scores[0] - 0.5).abs() < 0.01);

        // L3 score must be 0 regardless of size
        assert_eq!(scores.scores[3], 0.0);

        // No compaction should be triggered (L0 score < 1.0, L3 score = 0)
        assert_eq!(scores.levels_needing_compaction().count(), 0);
        assert!(picker.pick_compaction(&levels).is_none());
    }

    #[test]
    fn test_highest_score_level_wins() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let mut picker = CompactionPicker::new(&test_options());

        // Create L0 with score ~1.25 (5 files, trigger=4)
        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000));
        }

        // Create L1 with score ~3.0 (3x base_bytes)
        levels = levels.add_sst(create_sst(100, 1, 10, 50, base_bytes * 3));

        let scores = picker.compute_scores(&levels);

        // L1 should have higher score
        assert!(scores.scores[1] > scores.scores[0]);
        // Both levels need compaction, but L1 has highest score so comes first
        let levels_needing: Vec<_> = scores.levels_needing_compaction().collect();
        assert_eq!(levels_needing, vec![1, 0]);

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 1);
    }

    #[test]
    fn test_compute_partition_boundaries() {
        let picker = CompactionPicker::new(&test_options()); // max_levels = 4, so max level is L3

        // Case 1: Max level (L3) is empty -> single partition (no boundaries)
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 0, 0, 100, 1000));
        levels = levels.add_sst(create_sst(2, 1, 0, 50, 1000));
        levels = levels.add_sst(create_sst(3, 1, 51, 100, 1000));
        assert!(picker.compute_partition_boundaries(&levels).is_empty());

        // Case 2: L2 has data but L3 (max level) is empty -> still single partition
        levels = levels.add_sst(create_sst(4, 2, 0, 30, 1000));
        levels = levels.add_sst(create_sst(5, 2, 31, 60, 1000));
        levels = levels.add_sst(create_sst(6, 2, 61, 100, 1000));
        assert!(picker.compute_partition_boundaries(&levels).is_empty());

        // Case 3: L3 (max level) has 2 SSTables -> 1 boundary
        levels = levels.add_sst(create_sst(7, 3, 0, 45, 1000));
        levels = levels.add_sst(create_sst(8, 3, 46, 100, 1000));

        let boundaries = picker.compute_partition_boundaries(&levels);
        assert_eq!(boundaries.len(), 1);
        assert_eq!(boundaries[0], record_key(45));

        // Case 4: L3 (max level) has 3 SSTables -> 2 boundaries
        levels = levels.add_sst(create_sst(9, 3, 101, 150, 1000));

        let boundaries = picker.compute_partition_boundaries(&levels);
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0], record_key(45));
        assert_eq!(boundaries[1], record_key(100));

        // Case 5: Max level has only 1 SSTable -> single partition (no boundaries)
        let mut levels_single = Levels::default();
        levels_single = levels_single.add_sst(create_sst(1, 3, 0, 100, 1000));
        assert!(picker.compute_partition_boundaries(&levels_single).is_empty());
    }

    #[test]
    fn test_find_partition_key_range_for_file() {
        use std::ops::Bound;

        let picker = CompactionPicker::new(&test_options());

        // Boundaries: [10, 20, 30]
        // Partition 0: keys <= 10         -> key range: (Unbounded, Included(10)]
        // Partition 1: 10 < keys <= 20    -> key range: (Excluded(10), Included(20)]
        // Partition 2: 20 < keys <= 30    -> key range: (Excluded(20), Included(30)]
        // Partition 3: keys > 30          -> key range: (Excluded(30), Unbounded)
        let boundaries = vec![record_key(10), record_key(20), record_key(30)];

        // File in partition 0 only
        let sst_p0 = create_sst(1, 3, 0, 5, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_p0, &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Included(&record_key(10)));

        // File spanning partitions 0-1
        let sst_p0_p1 = create_sst(2, 3, 5, 15, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_p0_p1, &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Included(&record_key(20)));

        // File spanning partitions 1-2
        let sst_p1_p2 = create_sst(3, 3, 15, 25, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_p1_p2, &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(10)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(30)));

        // File spanning all partitions
        let sst_all = create_sst(4, 3, 0, 100, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_all, &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Unbounded);

        // File in last partition only
        let sst_last = create_sst(5, 3, 40, 50, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_last, &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(30)));
        assert_eq!(range.end_bound(), Bound::Unbounded);

        // File in middle partition only (partition 2)
        let sst_middle = create_sst(6, 3, 22, 28, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_middle, &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(20)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(30)));

        // Empty boundaries: single partition with unbounded range
        let empty_boundaries: Vec<Vec<u8>> = Vec::new();
        let sst_unbounded = create_sst(7, 3, 0, 100, 1000);
        let range = picker.find_partition_key_range_for_file(&sst_unbounded, &empty_boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_full_compaction_blocks_level() {
        let mut picker = CompactionPicker::new(&test_options());

        // Create L0 files to trigger compaction
        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000));
        }

        // Pick first compaction (auto-marked)
        let job1 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job1.input_level, 0);
        assert!(job1.partitions_grid.is_none()); // Full compaction

        // Try to pick another L0→L1 compaction - should be blocked because
        // full compaction is active on L0 and L1
        let job2 = picker.pick_compaction(&levels);
        assert!(job2.is_none(), "Full compaction should block entire level");

        // Unmark and verify we can pick again
        picker.unmark_compacting(&job1);
        let job3 = picker.pick_compaction(&levels);
        assert!(job3.is_some(), "Should be able to pick after unmark");
    }

    #[test]
    fn test_partial_compaction_allows_parallel_different_partitions() {
        let options = test_options(); // max_levels = 4
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = CompactionPicker::new(&options);

        // Create L2 with multiple files in different key ranges
        // Use lower SST numbers for older files (lower sequence numbers)
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 2, 10, 30, l2_target)); // Partition 0 (oldest)
        levels = levels.add_sst(create_sst(2, 2, 110, 130, l2_target)); // Partition 2 (newer)

        // L3 files that define partitions: [0-50], [51-100], [101-150]
        levels = levels.add_sst(create_sst(10, 3, 0, 50, 1000));
        levels = levels.add_sst(create_sst(11, 3, 51, 100, 1000));
        levels = levels.add_sst(create_sst(12, 3, 101, 150, 1000));

        // Pick first compaction (should be partial, L2→L3, auto-marked)
        // Should pick SST 1 first (oldest by sequence number)
        let job1 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job1.input_level, 2);
        assert_eq!(job1.output_level, 3);
        assert_eq!(job1.input_key_ranges, vec![interval(10, 30)]);
        assert_eq!(job1.output_key_ranges, vec![interval(0, 50)]);
        assert_eq!(job1.partitions_grid, Some(vec![record_key(50), record_key(100)]), "L2→L3 should be partial");
        assert_eq!(job1.input_files.len(), 1);
        assert_eq!(job1.input_files[0].number, 1, "Should pick oldest file first");

        // Pick another compaction - should be able to pick the other L2 file
        // because it's in a different partition
        let job2 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job2.input_level, 2);
        assert_eq!(job2.input_key_ranges, vec![interval(110, 130)]);
        assert_eq!(job2.output_key_ranges, vec![interval(101, 150)]);
        assert!(job2.partitions_grid.is_some());
        assert_eq!(job2.input_files.len(), 1);
        assert_eq!(job2.input_files[0].number, 2, "Should pick the other file");
    }

    #[test]
    fn test_partial_compaction_blocks_overlapping_output_ranges() {
        let options = test_options(); // max_levels = 4
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = CompactionPicker::new(&options);

        // Create L2 with two non-overlapping files that map to the same L3 partition
        // File 1: [10, 30] in partition 0 (keys <= 45)
        // File 2: [35, 44] also in partition 0 (keys <= 45)
        // Both files will compact with the same L3 file, causing output range overlap
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 2, 10, 30, l2_target)); // Partition 0
        levels = levels.add_sst(create_sst(2, 2, 35, 44, l2_target)); // Partition 0

        // L3 files that define partitions: boundary at key 45
        levels = levels.add_sst(create_sst(10, 3, 0, 45, 1000));
        levels = levels.add_sst(create_sst(11, 3, 46, 100, 1000));

        // Pick first compaction (auto-marked)
        let job1 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job1.input_level, 2);
        assert_eq!(job1.input_key_ranges, vec![interval(10, 30)]);
        assert_eq!(job1.input_files[0].number, 1, "Should pick oldest file first");
        // Output should include L3 file 10 (partition 0)
        assert_eq!(job1.output_files.len(), 1);
        assert_eq!(job1.output_files[0].number, 10);
        assert_eq!(job1.output_key_ranges, vec![interval(0, 45)]);

        // Try to pick another - should be blocked because both L2 files would
        // compact with the same L3 file (output ranges overlap)
        let job2 = picker.pick_compaction(&levels);
        assert!(job2.is_none(), "Compactions with overlapping output ranges should block each other");
    }

    #[test]
    fn test_collect_key_ranges_merges_l0_overlapping() {
        let picker = CompactionPicker::new(&test_options());

        // L0 overlapping ranges should be merged
        let l0_ssts = vec![
            create_sst(1, 0, 10, 30, 1000),
            create_sst(2, 0, 25, 50, 1000), // overlaps with first
            create_sst(3, 0, 100, 120, 1000), // separate
        ];
        let l0_ranges = picker.collect_key_ranges(0, &l0_ssts);
        assert_eq!(l0_ranges.len(), 2, "L0 overlapping ranges should be merged");
        assert_eq!(l0_ranges[0], interval(10, 50));
        assert_eq!(l0_ranges[1], interval(100, 120));

        // Non-L0 ranges should NOT be merged (they are non-overlapping by definition)
        let l1_ssts = vec![
            create_sst(1, 1, 10, 30, 1000),
            create_sst(2, 1, 40, 60, 1000),
            create_sst(3, 1, 70, 90, 1000),
        ];
        let l1_ranges = picker.collect_key_ranges(1, &l1_ssts);
        assert_eq!(l1_ranges.len(), 3, "L1 ranges should not be merged");
        assert_eq!(l1_ranges[0], interval(10, 30));
        assert_eq!(l1_ranges[1], interval(40, 60));
        assert_eq!(l1_ranges[2], interval(70, 90));
    }

    #[test]
    fn test_find_overlapping_files_edge_cases() {
        let picker = CompactionPicker::new(&test_options());

        // Empty ranges -> no overlapping files
        let level = Level::new(1, vec![
            create_sst(1, 1, 10, 30, 1000),
            create_sst(2, 1, 40, 60, 1000),
        ], 2000);
        let empty_ranges: Vec<Interval<Vec<u8>>> = vec![];
        let result = picker.find_overlapping_files(Some(&level), &empty_ranges);
        assert!(result.is_empty(), "Empty ranges should return no files");

        // Ranges that don't overlap any files
        let non_overlapping_ranges = vec![interval(100, 200)];
        let result = picker.find_overlapping_files(Some(&level), &non_overlapping_ranges);
        assert!(result.is_empty(), "Non-overlapping ranges should return no files");

        // None level -> empty result
        let result = picker.find_overlapping_files(None, &vec![interval(10, 50)]);
        assert!(result.is_empty(), "None level should return no files");

        // Exact boundary match (range ends exactly where file starts)
        // Range [5, 10] and file [10, 30] - should overlap at key 10
        let boundary_ranges = vec![interval(5, 10)];
        let result = picker.find_overlapping_files(Some(&level), &boundary_ranges);
        assert_eq!(result.len(), 1, "Exact boundary should overlap");
        assert_eq!(result[0].number, 1);
    }

    #[test]
    fn test_partial_compaction_oldest_file_first() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = CompactionPicker::new(&options);

        // Create L2 files with explicit sequence numbers to verify ordering
        // SST 3 has lowest sequence number (oldest), SST 1 has highest (newest)
        let mut levels = Levels::default();
        
        // Add in non-sequential order to ensure sorting works
        let sst_newest = Arc::new(SSTableMetadata::new(
            1, 2, &record_key(10), &record_key(30), 300, 350, l2_target,
        ));
        let sst_middle = Arc::new(SSTableMetadata::new(
            2, 2, &record_key(60), &record_key(80), 200, 250, l2_target,
        ));
        let sst_oldest = Arc::new(SSTableMetadata::new(
            3, 2, &record_key(110), &record_key(130), 100, 150, l2_target,
        ));
        
        levels = levels.add_sst(sst_newest);
        levels = levels.add_sst(sst_middle);
        levels = levels.add_sst(sst_oldest);

        // L3 files defining partitions
        levels = levels.add_sst(create_sst(10, 3, 0, 50, 1000));
        levels = levels.add_sst(create_sst(11, 3, 51, 100, 1000));
        levels = levels.add_sst(create_sst(12, 3, 101, 150, 1000));

        // First pick should get the oldest file (SST 3, seq 100)
        let job1 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job1.input_files[0].number, 3, "Should pick oldest file (lowest seq num) first");
        assert_eq!(job1.input_key_ranges, vec![interval(110, 130)]);
        assert_eq!(job1.output_key_ranges, vec![interval(101, 150)]);
        assert_eq!(job1.partitions_grid, Some(vec![record_key(50), record_key(100)]), "Should use partial compaction");

        // Second pick should get middle file (SST 2, seq 200)
        let job2 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job2.input_files[0].number, 2, "Should pick second oldest file");
        assert_eq!(job2.input_key_ranges, vec![interval(60, 80)]);
        assert_eq!(job2.output_key_ranges, vec![interval(51, 100)]);
        assert_eq!(job2.partitions_grid, Some(vec![record_key(50), record_key(100)]), "Should use partial compaction");

        // Third pick should get newest file (SST 1, seq 300)
        let job3 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job3.input_files[0].number, 1, "Should pick newest file last");
        assert_eq!(job3.input_key_ranges, vec![interval(10, 30)]);
        assert_eq!(job3.output_key_ranges, vec![interval(0, 50)]);
    }

    #[test]
    fn test_l0_compaction_score_by_size() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let mut picker = CompactionPicker::new(&options);

        // Create L0 with only 2 files (below trigger of 4) but large total size
        let mut levels = Levels::default();
        levels = levels.add_sst(create_sst(1, 0, 10, 50, base_bytes)); // large file
        levels = levels.add_sst(create_sst(2, 0, 60, 100, base_bytes)); // large file

        let scores = picker.compute_scores(&levels);
        
        // File count score: 2/4 = 0.5
        // Size score: 2*base_bytes / base_bytes = 2.0
        // L0 score should be max(0.5, 2.0) = 2.0
        assert!(scores.scores[0] > 1.0, "L0 score should exceed 1.0 due to size");
        assert!((scores.scores[0] - 2.0).abs() < 0.01, "L0 score should be ~2.0");

        let job = picker.pick_compaction(&levels);
        assert!(job.is_some(), "Should trigger compaction based on size");
    }

    #[test]
    fn test_unmark_compacting_allows_recompaction() {
        let mut picker = CompactionPicker::new(&test_options());

        let mut levels = Levels::default();
        for i in 1..=5 {
            levels = levels.add_sst(create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000));
        }

        // Pick and auto-mark
        let job1 = picker.pick_compaction(&levels).unwrap();
        
        // Verify ranges are marked
        assert!(!picker.compacting_ranges[0].is_empty(), "Input level should have marked ranges");
        
        // Unmark
        picker.unmark_compacting(&job1);
        
        // Verify ranges are cleared
        assert!(picker.compacting_ranges[0].is_empty(), "Input level ranges should be cleared");
        assert!(picker.compacting_ranges[1].is_empty(), "Output level ranges should be cleared");

        // Pick again - should get same compaction
        let job2 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job2.input_level, job1.input_level);
        assert_eq!(job2.output_level, job1.output_level);
    }

    #[test]
    fn test_partial_compaction_with_empty_output_level() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = CompactionPicker::new(&options);

        let mut levels = Levels::default();
        
        // L2 file that needs compaction
        levels = levels.add_sst(create_sst(1, 2, 10, 30, l2_target * 2));

        // L3 is empty - no partition boundaries exist

        let job = picker.pick_compaction(&levels).unwrap();
        
        assert_eq!(job.input_level, 2);
        assert_eq!(job.output_level, 3);
        assert_eq!(job.input_files.len(), 1);
        assert!(job.output_files.is_empty(), "Output files should be empty when output level is empty");
        assert!(job.output_key_ranges.is_empty(), "Output key ranges should be empty");
        assert_eq!(job.partitions_grid, Some(vec![]), "Should have empty partition grid");
    }

    #[test]
    fn test_full_compaction_blocked_by_partial() {
        // If a partial compaction is running on a level, full compaction
        // involving that level should be blocked
        let options = test_options(); // max_levels = 4
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = CompactionPicker::new(&options);

        // Create a scenario where L1→L2 (full) and L2→L3 (partial) could both be triggered
        let mut levels = Levels::default();

        // L1 with high score (uses lower SST number so it's older)
        levels = levels.add_sst(create_sst(1, 1, 10, 50, base_bytes * 2));

        // L2 with high score
        levels = levels.add_sst(create_sst(2, 2, 10, 30, l2_target * 2));

        // L3 files (higher SST numbers)
        levels = levels.add_sst(create_sst(10, 3, 0, 50, 1000));
        levels = levels.add_sst(create_sst(11, 3, 51, 100, 1000));

        // Manually create a partial compaction job to simulate L2→L3 running
        let partial_job = CompactionJob {
            input_level: 2,
            output_level: 3,
            input_files: vec![create_sst(2, 2, 10, 30, l2_target * 2)],
            output_files: vec![create_sst(10, 3, 0, 50, 1000)],
            input_key_ranges: vec![interval(10, 30)],
            output_key_ranges: vec![interval(0, 50)],
            partitions_grid: Some(vec![record_key(50)]),
        };
        picker.mark_compacting(&partial_job);

        // Now try to pick L1→L2 (full compaction)
        // It should be blocked because L2 has an active partial compaction
        let scores = picker.compute_scores(&levels);
        let l1_needs = scores.scores[1] > 1.0;
        assert!(l1_needs, "L1 should need compaction");

        // pick_compaction should skip L1→L2 because L2 is involved in a compaction
        let job = picker.pick_compaction(&levels);
        // Either no job, or not L1→L2
        if let Some(j) = job {
            assert!(j.input_level != 1 || j.output_level != 2,
                "L1→L2 full compaction should be blocked by L2→L3 partial");
        }
    }
}
