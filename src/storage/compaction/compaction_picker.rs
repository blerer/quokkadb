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

use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::obs::metrics::{AtomicGauge, Counter, Histogram, MetricRegistry};
use crate::options::options::Options;
use crate::storage::lsm_version::Level::{NonOverlapping, Overlapping};
use crate::storage::lsm_version::{span, DropMetadata, Level, LevelItem, Levels, SSTableMetadata};
use crate::util::interval::{has_overlapping_intervals, Interval};
use crate::{event, info};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

/// Describes a compaction job to be executed.
///
/// Contains all the information needed to perform a compaction: which files
/// to read from the input level, which files to merge from the output level,
/// and metadata about the compaction bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionJob {
    /// Unique identifier for this compaction job, used for log/trace correlation.
    pub id: u64,
    /// The source level (e.g., 0 for L0 → L1 compaction).
    pub input_level: u8,
    /// The target level (e.g., 1 for L0 → L1 compaction).
    pub output_level: u8,
    /// Files to compact from the input level.
    pub input_files: Vec<Arc<SSTableMetadata>>,
    /// Files from the output level that overlap with the input files.
    pub output_files: Vec<Arc<SSTableMetadata>>,
    /// Any drops that need to be applied during this compaction (e.g., for range deletions).
    pub drops: Vec<Arc<DropMetadata>>,
    /// The key range covered by this compaction in the input level.
    pub input_key_range: Interval<Vec<u8>>,
    /// The key range covered by this compaction in the output level.
    pub output_key_range: Interval<Vec<u8>>,
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
pub struct CompactionPicker {
    /// Logger for events and diagnostics.
    logger: Arc<dyn LoggerAndTracer>,
    /// The database options.
    options: Options,
    /// The largest level index (Lmax) in the LSM tree. Levels are indexed from 0 to Lmax.
    level_l: usize,
    /// The smallest level at which we start partitioning runs based on the file boundaries at the largest level
    level_x: usize,
    /// Ranges being compacted per level: level -> list of ranges.
    compacting_ranges: Vec<Vec<Interval<Vec<u8>>>>,
    /// Metrics for compaction operations.
    metrics: Metrics,
    /// The number of thread used by the compaction manager
    nbr_of_threads: u8,
    /// The number of running compaction
    nbr_of_running_compactions: u8,
    /// Monotonically increasing counter used to assign unique IDs to compaction jobs.
    next_job_id: u64,
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
            .filter(|(_, &score)| score >= 1.0)
            .map(|(level, &score)| (level, score))
            .collect();

        levels_with_scores.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap());

        levels_with_scores.into_iter().map(|(level, _)| level)
    }
}

impl CompactionPicker {
    /// Creates a new compaction picker.
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: &Options,
    ) -> Self {
        let nbr_of_threads = options.compaction_threads() as u8;
        let max_levels = options.max_levels();
        let level_l = max_levels - 1;
        assert!(
            max_levels >= 2,
            "max_levels must be at least 2 for 2L-Spooky compaction"
        );
        let level_x = max_levels - 2; // Start partitioning from the second to last level
        let metrics = Metrics::new(max_levels);
        metrics.register_to(metric_registry);

        info!(
            logger,
            "CompactionPicker initialized, max_levels={}, level_x={}", max_levels, level_x
        );

        CompactionPicker {
            logger,
            options: options.clone(),
            level_l,
            level_x,
            compacting_ranges: vec![Vec::new(); max_levels],
            metrics,
            nbr_of_threads,
            nbr_of_running_compactions: 0,
            next_job_id: 0,
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
        if self.nbr_of_running_compactions >= self.nbr_of_threads {
            self.metrics.jobs_skipped_level_compacting.inc();
            event!(self.logger, "compaction_skipped reason=max_parallelism_reached, running_compactions={}, max_threads={}",
                self.nbr_of_running_compactions, self.nbr_of_threads);
            return None;
        }

        event!(self.logger, "compaction_picking start");
        let scores = self.compute_scores(levels);

        // Update score gauges
        for (level, &score) in scores.scores.iter().enumerate() {
            self.metrics.record_score(level, score);
        }

        for input_level in scores.levels_needing_compaction() {
            let output_level = input_level + 1;

            // Sanity check - output level should always exist for levels needing compaction
            // (except last level which has score 0)
            let input = levels.level(input_level).unwrap();
            let output = levels.level(output_level).unwrap();

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
                    self.metrics.jobs_picked.inc();
                    self.metrics.jobs_picked_partial.inc();
                    self.metrics.record_picked_from_level(input_level);
                    self.metrics
                        .input_files_count
                        .record(job.input_files.len() as u64);

                    info!(self.logger, "Picked partial compaction job_id={}, L{}->L{}, input_files={}, output_files={}",
                        job.id, input_level, output_level, job.input_files.len(), job.output_files.len());
                    event!(self.logger, "compaction_picking done type=partial, job_id={}, input_level={}, output_level={}, input_files={}, output_files={}",
                        job.id, input_level, output_level, job.input_files.len(), job.output_files.len());

                    return Some(job);
                } else {
                    self.metrics.jobs_skipped_range_overlap.inc();
                    event!(
                        self.logger,
                        "compaction_skipped reason=range_overlap, input_level={}, output_level={}",
                        input_level,
                        output_level
                    );
                }
            } else {
                // Full compaction: block if any compaction is active on either level
                if self.is_level_compacting(input_level) || self.is_level_compacting(output_level) {
                    self.metrics.jobs_skipped_level_compacting.inc();
                    event!(self.logger, "compaction_skipped reason=level_compacting, input_level={}, output_level={}", input_level, output_level);
                    continue;
                }

                if let Some(job) = self.pick_full_compaction(
                    input,
                    output,
                    input_level,
                    output_level,
                    partitions_grid,
                ) {
                    self.mark_compacting(&job);
                    self.metrics.jobs_picked.inc();
                    self.metrics.jobs_picked_full.inc();
                    self.metrics.record_picked_from_level(input_level);
                    self.metrics
                        .input_files_count
                        .record(job.input_files.len() as u64);

                    info!(self.logger, "Picked full compaction job_id={}, L{}->L{}, input_files={}, output_files={}",
                        job.id, input_level, output_level, job.input_files.len(), job.output_files.len());
                    event!(self.logger, "compaction_picking done type=full, job_id={}, input_level={}, output_level={}, input_files={}, output_files={}",
                        job.id, input_level, output_level, job.input_files.len(), job.output_files.len());

                    println!("{:?}", self.compacting_ranges);

                    return Some(job);
                }
            }
        }

        event!(
            self.logger,
            "compaction_skipped reason=no_level_need_compaction"
        );

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
    fn is_range_compacting(&self, level: usize, key_range: &Interval<Vec<u8>>) -> bool {
        has_overlapping_intervals(
            &[key_range.clone()],
            self.compacting_ranges.get(level).unwrap_or(&Vec::new()),
        )
    }

    /// Picks a full compaction for levels above the bottom two.
    ///
    /// Full compaction takes all files from the source level and merges them with
    /// all overlapping files in the target level. This is simpler than partial
    /// compaction and ensures all data is properly merged.
    fn pick_full_compaction(
        &mut self,
        input: &Level,
        output: &Level,
        input_level: usize,
        output_level: usize,
        partitions_grid: Option<Vec<Vec<u8>>>,
    ) -> Option<CompactionJob> {
        let id = self.next_job_id;
        self.next_job_id += 1;

        // We want to use the range including all the items in the input level. This will be used to
        // find all overlapping files in the output level.
        // We might end up picking some files in the output level that do not strictly overlap with
        // the input files, but doing it ensure that the files of the output level will be split in
        // an optimal way (minimizing the number of tiny files).
        let input_key_range = input.items_range().unwrap();

        let input_files = input.sstables().iter().cloned().collect();

        // The drops of the output level can be ignored because they should have happened before
        // the compaction and thus should not be relevant for the files we want to compact in the output level.
        let drops = input.drops().iter().cloned().collect();

        // Find all overlapping files in the output level
        let output_files = output.find_sstables_in_range(&input_key_range, u64::MAX);

        // Use input_key_range as fallback when output is empty to prevent another
        // compaction from racing to write to the same output level range
        let output_key_range = span(output_files.iter().map(|f| f.as_ref()))
            .unwrap_or_else(|| input_key_range.clone());

        Some(CompactionJob {
            id,
            input_level: input_level as u8,
            output_level: output_level as u8,
            input_files,
            output_files,
            drops,
            input_key_range,
            output_key_range,
            partitions_grid,
        })
    }

    /// Picks a partial compaction for L2+→Ln.
    ///
    /// Partial compaction limits the scope to files within the same partition.
    /// Partitions are determined by the max level's SSTable boundaries.
    fn pick_partial_compaction(
        &mut self,
        input: &Level,
        output: &Level,
        input_level: usize,
        output_level: usize,
        partitions_grid: Option<Vec<Vec<u8>>>,
    ) -> Option<CompactionJob> {
        assert!(
            matches!(input, NonOverlapping { .. }),
            "Expected NonOverlapping level for partial compaction, found Overlapping"
        );

        let id = self.next_job_id;
        self.next_job_id += 1;

        // We want to pick the oldest items that can be compacted to have a deterministic pick order
        // and avoid starvation.
        let mut items = input.items();
        items.sort_by(|a, b| a.min_sequence_number().cmp(&b.min_sequence_number()));

        for item in items {
            // Partitions change over time as the last level sstables could have been split or
            // merged. To adapt to that, we need to retrieve the current set of partitions to which
            // the item belongs and use them to pick all the sstables and drops that need to be
            // involved in the compaction.
            // As only the 2 bottom levels are partitioned, we can be sure that the partitions can
            // have only changed through a single split or merge in the last level, which means
            // that we should not have sstables or drops that will cross the boundaries of the
            // partition_key_range.
            let partition_key_range = self.find_partition_key_range_for_item(
                item.as_ref(),
                partitions_grid.as_ref().unwrap(),
            );

            if self.is_range_compacting(input_level, &partition_key_range)
                || self.is_range_compacting(output_level, &partition_key_range)
            {
                continue;
            }

            // Find files in the input level that are within the same partition range
            let input_files = input.find_sstables_in_range(&partition_key_range, u64::MAX);

            let drops = input.find_drops_in_range(&partition_key_range);

            // Find files in the output level that are within the same partition range
            let output_files = output.find_sstables_in_range(&partition_key_range, u64::MAX);

            return Some(CompactionJob {
                id,
                input_level: input_level as u8,
                output_level: output_level as u8,
                input_files,
                output_files,
                drops,
                input_key_range: partition_key_range.clone(),
                output_key_range: partition_key_range,
                partitions_grid,
            });
        }

        None
    }

    /// Marks a compaction job as active.
    ///
    /// Call this when a compaction job is scheduled to track ranges for parallelism control.
    fn mark_compacting(&mut self, job: &CompactionJob) {
        self.nbr_of_running_compactions += 1;
        let level = job.input_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        ranges.push(job.input_key_range.clone());
        ranges.sort();
        self.metrics.record_active(level, 1);

        let level = job.output_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        ranges.push(job.output_key_range.clone());
        ranges.sort();
        self.metrics.record_active(level, 1);
    }

    /// Unmarks a compaction job as active.
    ///
    /// Call this when a compaction job completes (successfully or not).
    pub fn unmark_compacting(&mut self, job: &CompactionJob) {
        let level = job.input_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        if let Some(pos) = ranges.iter().position(|r| r == &job.input_key_range) {
            ranges.remove(pos);
        }
        self.metrics.record_active(level, -1);

        let level = job.output_level as usize;
        let ranges = self.compacting_ranges.get_mut(level).unwrap();
        if let Some(pos) = ranges.iter().position(|r| r == &job.output_key_range) {
            ranges.remove(pos);
        }
        self.metrics.record_active(level, -1);
        self.nbr_of_running_compactions -= 1;
    }

    /// Finds the partition index (0 to N) for a given key based on provided boundaries.
    ///
    /// Boundaries are the upper bounds of partitions (except the last partition).
    /// Uses binary search for efficient lookup.
    fn find_partition_for_key(&self, key: &[u8], boundaries: &[Vec<u8>]) -> usize {
        boundaries
            .binary_search_by(|probe| probe.as_slice().cmp(key))
            .unwrap_or_else(|idx| idx)
    }

    /// Finds the key range interval for the partition(s) that an item overlaps with.
    ///
    /// Returns an interval representing the full key range covered by the partitions
    /// that the item spans. The interval bounds are:
    /// - Start: exclusive bound at the previous partition's boundary, or unbounded for partition 0
    /// - End: inclusive bound at the end partition's boundary, or unbounded for the last partition
    ///
    /// Handles excluded bounds correctly: if an item has `Excluded(k)` as a bound and `k` is
    /// exactly on a partition boundary, the item does not include that boundary value, so we
    /// must adjust the partition selection accordingly.
    fn find_partition_key_range_for_item(
        &self,
        item: &dyn LevelItem,
        boundaries: &[Vec<u8>],
    ) -> Interval<Vec<u8>> {
        // Handle empty boundaries: single partition covering everything
        if boundaries.is_empty() {
            return Interval::all();
        }

        let item_range = item.record_key_range();

        // Find start partition, accounting for excluded bounds
        let start_partition = match item_range.start_bound() {
            Bound::Included(k) => self.find_partition_for_key(k, boundaries),
            Bound::Excluded(k) => {
                // If start is excluded and k is exactly a boundary, the item starts
                // in the next partition (since it doesn't include k itself)
                let partition = self.find_partition_for_key(k, boundaries);
                if partition < boundaries.len() && boundaries[partition] == *k {
                    partition + 1
                } else {
                    partition
                }
            }
            Bound::Unbounded => 0,
        };

        // Find end partition, accounting for excluded bounds
        let end_partition = match item_range.end_bound() {
            Bound::Included(k) => self.find_partition_for_key(k, boundaries),
            Bound::Excluded(k) => {
                // If end is excluded and k is exactly a boundary, the item ends
                // in the previous partition (since it doesn't include k itself)
                let partition = self.find_partition_for_key(k, boundaries);
                if partition > 0 && boundaries[partition - 1] == *k {
                    partition - 1
                } else {
                    partition
                }
            }
            Bound::Unbounded => boundaries.len(), // Last partition
        };

        // Build the interval for the spanned partitions
        // Start bound: if partition 0, unbounded; otherwise excluded at previous boundary
        let start_bound = if start_partition == 0 {
            Bound::Unbounded
        } else {
            Bound::Excluded(boundaries[start_partition - 1].clone())
        };

        // End bound: if last partition, unbounded; otherwise included at partition's boundary
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
    /// A boundary is only emitted after SSTable `i` if SSTable `i+1` is at least
    /// 50% of the target SSTable size for the max level. SSTables below this
    /// threshold are folded into the preceding partition, which breaks the
    /// reinforcing cycle where tiny files produce tiny partitions that produce
    /// more tiny files.
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

                let max_level = max_level_idx as u8;
                let target_size = self.options.target_file_size_for_level(max_level);
                let min_partition_size = target_size / 2;

                // Emit a boundary after SSTable i only if SSTable i+1 is large enough
                // to deserve its own partition. Small SSTables are absorbed into the
                // preceding partition so compaction can grow them to a healthy size.
                sstables
                    .windows(2)
                    .filter(|w| w[1].size >= min_partition_size)
                    .map(|w| w[0].max_key.clone())
                    .collect()
            }
            Overlapping { .. } => Vec::new(),
        }
    }
}

/// Metrics for compaction picker operations.
struct Metrics {
    /// Total number of compaction jobs picked.
    jobs_picked: Arc<Counter>,
    /// Number of jobs using full compaction strategy.
    jobs_picked_full: Arc<Counter>,
    /// Number of jobs using partial (2L-Spooky) compaction strategy.
    jobs_picked_partial: Arc<Counter>,
    /// Jobs skipped because a level was already compacting (full compaction blocked).
    jobs_skipped_level_compacting: Arc<Counter>,
    /// Partial compactions skipped due to overlapping ranges.
    jobs_skipped_range_overlap: Arc<Counter>,
    /// Compactions picked from each level (indexed by level number).
    picked_from_level: Vec<Arc<Counter>>,
    /// Current compaction score for each level.
    score_per_level: Vec<Arc<AtomicGauge>>,
    /// Number of active compactions involving each level.
    active_per_level: Vec<Arc<AtomicGauge>>,
    /// Distribution of input file counts per compaction.
    input_files_count: Arc<Histogram>,
}

impl Metrics {
    fn new(max_levels: usize) -> Self {
        let picked_from_level = (0..max_levels).map(|_| Counter::new()).collect();
        let score_per_level = (0..max_levels).map(|_| AtomicGauge::new()).collect();
        let active_per_level = (0..max_levels).map(|_| AtomicGauge::new()).collect();

        Self {
            jobs_picked: Counter::new(),
            jobs_picked_full: Counter::new(),
            jobs_picked_partial: Counter::new(),
            jobs_skipped_level_compacting: Counter::new(),
            jobs_skipped_range_overlap: Counter::new(),
            picked_from_level,
            score_per_level,
            active_per_level,
            input_files_count: Histogram::new(&[1, 2, 4, 8, 16, 32, 64, 128, 256]),
        }
    }

    fn register_to(&self, registry: &mut MetricRegistry) {
        registry
            .register_counter("compaction.jobs.picked", self.jobs_picked.clone())
            .register_counter("compaction.jobs.picked.full", self.jobs_picked_full.clone())
            .register_counter(
                "compaction.jobs.picked.partial",
                self.jobs_picked_partial.clone(),
            )
            .register_counter(
                "compaction.jobs.skipped.level_compacting",
                self.jobs_skipped_level_compacting.clone(),
            )
            .register_counter(
                "compaction.jobs.skipped.range_overlap",
                self.jobs_skipped_range_overlap.clone(),
            )
            .register_histogram(
                "compaction.input_files.count",
                self.input_files_count.clone(),
            );

        for (level, counter) in self.picked_from_level.iter().enumerate() {
            registry.register_counter(&format!("compaction.picked.l{}", level), counter.clone());
        }

        for (level, gauge) in self.score_per_level.iter().enumerate() {
            registry.register_gauge(&format!("compaction.score.l{}", level), gauge.clone());
        }

        for (level, gauge) in self.active_per_level.iter().enumerate() {
            registry.register_gauge(&format!("compaction.active.l{}", level), gauge.clone());
        }
    }

    fn record_score(&self, level: usize, score: f64) {
        if let Some(gauge) = self.score_per_level.get(level) {
            // Store score * 100 as integer to preserve two decimal places
            gauge.set((score * 100.0) as u64);
        }
    }

    fn record_picked_from_level(&self, level: usize) {
        if let Some(counter) = self.picked_from_level.get(level) {
            counter.inc();
        }
    }

    fn record_active(&self, level: usize, delta: i64) {
        if let Some(gauge) = self.active_per_level.get(level) {
            if delta > 0 {
                gauge.inc_by(delta as u64);
            } else {
                gauge.dec_by((-delta) as u64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::logger;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::storage::internal_key::encode_record_key;
    use crate::storage::lsm_version::DropKind;
    use crate::util::bson_utils::BsonKey;
    use bson::Bson;
    use std::iter::{empty, once};
    use std::ops::RangeBounds;

    /// Default collection ID used for test SSTables and drops.
    const DEFAULT_COLLECTION: u32 = 1;
    /// Default index ID used for test SSTables and drops.
    const DEFAULT_INDEX: u32 = 0;

    fn test_options() -> Options {
        Options::default()
            .with_max_levels(4)
            .with_level0_file_num_compaction_trigger(4)
            .with_max_bytes_for_level_base(StorageQuantity::new(64, StorageUnit::Mebibytes))
            .with_max_bytes_for_level_multiplier(10.0)
            .with_compaction_threads(4)
    }

    fn test_picker(options: &Options) -> CompactionPicker {
        CompactionPicker::new(logger::test_instance(), &mut MetricRegistry::new(), options)
    }

    /// Encodes a record key with the specified collection, index, and user key value.
    fn record_key_for(collection: u32, index: u32, k: u32) -> Vec<u8> {
        let user_key = Bson::Int32(k as i32).try_into_key().unwrap();
        encode_record_key(collection, index, &user_key)
    }

    /// Encodes a record key using the default collection and index.
    fn record_key(k: u32) -> Vec<u8> {
        record_key_for(DEFAULT_COLLECTION, DEFAULT_INDEX, k)
    }

    fn create_sst_for(
        number: u64,
        level: u8,
        min: &[u8],
        max: &[u8],
        size: u64,
    ) -> Arc<SSTableMetadata> {
        Arc::new(SSTableMetadata::new(
            number,
            level,
            min,
            max,
            number * 100, // min_sequence_number: distinct per SST for deterministic ordering
            number * 100 + 50, // max_sequence_number
            size,
        ))
    }

    /// Creates an SSTable using the default collection and index.
    fn create_sst(number: u64, level: u8, min: u32, max: u32, size: u64) -> Arc<SSTableMetadata> {
        create_sst_for(
            number,
            level,
            &record_key_for(DEFAULT_COLLECTION, DEFAULT_INDEX, min),
            &record_key_for(DEFAULT_COLLECTION, DEFAULT_INDEX, max),
            size,
        )
    }

    /// Returns the minimum SSTable size (in bytes) required for a file in `level` to earn
    /// its own partition boundary (i.e., 50 % of the level's target file size).
    ///
    /// Use this in tests that populate the max level and need boundaries to be emitted.
    fn min_boundary_size(options: &Options, level: u8) -> u64 {
        options.target_file_size_for_level(level) / 2
    }

    #[test]
    fn test_no_compaction_needed_empty_levels() {
        let options = test_options();
        let mut picker = test_picker(&options);
        let levels = Levels::new(options.max_levels());

        let scores = picker.compute_scores(&levels);
        assert_eq!(scores.levels_needing_compaction().count(), 0);

        let job = picker.pick_compaction(&levels);
        assert!(job.is_none());
    }

    #[test]
    fn test_l0_no_compaction_below_trigger() {
        let options = test_options();
        let mut picker = test_picker(&options);

        // Create L0 with 3 files (trigger is 4, so below trigger — no compaction)
        let l0_ssts: Vec<_> = (1..=3)
            .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
            .collect();
        let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

        let scores = picker.compute_scores(&levels);
        assert!(
            scores.scores[0] < 1.0,
            "Score below trigger should be < 1.0"
        );
        assert_eq!(scores.levels_needing_compaction().count(), 0);
        assert!(picker.pick_compaction(&levels).is_none());
    }

    #[test]
    fn test_l0_compaction_triggered_by_file_count() {
        let options = test_options();
        let mut picker = test_picker(&options);

        // Create L0 with 4 files (trigger is 4, so exactly at trigger should compact)
        let l0_ssts: Vec<_> = (1..=4)
            .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
            .collect();
        let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

        let scores = picker.compute_scores(&levels);
        assert!(scores.scores[0] >= 1.0);
        let levels_needing: Vec<_> = scores.levels_needing_compaction().collect();
        assert_eq!(levels_needing, vec![0]);

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 0);
        assert_eq!(job.output_level, 1);
        assert!(!job.input_files.is_empty());
    }

    #[test]
    fn test_l0_compaction_takes_all_files() {
        let options = test_options();
        let mut picker = test_picker(&options);

        // Create overlapping L0 files
        // File 1: keys 10-30
        // File 2: keys 20-40 (overlaps with 1)
        // File 3: keys 35-50 (overlaps with 2)
        // File 4: keys 100-110 (does not overlap with others)
        // File 5: keys 25-45 (overlaps with 1, 2, 3)
        let l0_ssts = vec![
            create_sst(1, 0, 10, 30, 1000),
            create_sst(2, 0, 20, 40, 1000),
            create_sst(3, 0, 35, 50, 1000),
            create_sst(4, 0, 100, 110, 1000),
            create_sst(5, 0, 25, 45, 1000),
        ];
        let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

        let job = picker.pick_compaction(&levels).unwrap();

        // Full compaction takes ALL L0 files
        assert_eq!(job.input_level, 0);
        // input_key_range spans all L0 files
        assert_eq!(
            job.input_key_range,
            Interval::closed(record_key(10), record_key(110))
        );
        assert_eq!(job.output_level, 1);
        // When output level is empty, output_key_range uses input_key_range to block the range
        assert_eq!(
            job.output_key_range,
            Interval::closed(record_key(10), record_key(110))
        );
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
        let options = test_options();
        let mut picker = test_picker(&options);

        // Create L0 files
        let l0_ssts: Vec<_> = (1..=5).map(|i| create_sst(i, 0, 10, 50, 1000)).collect();

        // Create L1 files
        let l1_ssts = vec![
            create_sst(10, 1, 5, 25, 1000),    // overlaps
            create_sst(11, 1, 30, 60, 1000),   // overlaps
            create_sst(12, 1, 100, 150, 1000), // does not overlap
        ];

        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(1, l1_ssts, empty());

        let job = picker.pick_compaction(&levels).unwrap();

        let output_numbers: Vec<u64> = job.output_files.iter().map(|f| f.number).collect();
        assert!(output_numbers.contains(&10));
        assert!(output_numbers.contains(&11));
        assert!(!output_numbers.contains(&12));
        assert_eq!(job.input_key_range, interval(10, 50));
        // output_key_range spans all overlapping L1 files
        assert_eq!(
            job.output_key_range,
            Interval::closed(record_key(5), record_key(60))
        );
    }

    #[test]
    fn test_l0_compaction_rejected_due_to_compacting_level() {
        let options = test_options();
        let mut picker = test_picker(&options);

        let l0_ssts: Vec<_> = (1..=5).map(|i| create_sst(i, 0, 100, 110, 1000)).collect();
        let l1_ssts = Vec::new();

        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(1, l1_ssts.clone(), empty());

        let _job = picker.pick_compaction(&levels).unwrap();

        let l0_ssts: Vec<_> = (1..=6).map(|i| create_sst(i, 0, 100, 110, 1000)).collect();

        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(1, l1_ssts, empty());

        assert!(picker.pick_compaction(&levels).is_none()); // Mark the job as compacting to block the level
    }

    #[test]
    fn test_l0_compaction_finds_overlapping_l1_files() {
        let options = test_options();
        let mut picker = test_picker(&options);

        // L0 File: [100, 110]
        // L1 File A: [90, 105] (Overlaps L0)
        // L1 File B: [106, 115] (Overlaps L0)
        // L1 File C: [200, 210] (No overlap)
        let l0_ssts: Vec<_> = (1..=5).map(|i| create_sst(i, 0, 100, 110, 1000)).collect();
        let l1_ssts = vec![
            create_sst(10, 1, 90, 105, 1000),
            create_sst(11, 1, 106, 115, 1000),
            create_sst(12, 1, 200, 210, 1000),
        ];

        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(1, l1_ssts, empty());

        let job = picker.pick_compaction(&levels).unwrap();

        let output_numbers: Vec<u64> = job.output_files.iter().map(|f| f.number).collect();
        // Should include both 10 and 11 because they overlap with L0's range [100, 110]
        assert!(output_numbers.contains(&10));
        assert!(output_numbers.contains(&11));
        assert!(!output_numbers.contains(&12));

        assert_eq!(job.input_key_range, interval(100, 110));
        // output_key_range spans all overlapping L1 files
        assert_eq!(
            job.output_key_range,
            Interval::closed(record_key(90), record_key(115))
        );
    }

    #[test]
    fn test_level_compaction_triggered_by_size() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let mut picker = test_picker(&options);

        // Create L1 with size > base_bytes
        let mut levels = Levels::new(options.max_levels());
        levels = levels.add(1, once(create_sst(1, 1, 10, 50, base_bytes * 2)), empty());

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
        let options = test_options();
        let mut picker = test_picker(&options);

        let l0_ssts: Vec<_> = (1..=4)
            .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
            .collect();
        let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

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
    fn test_partial_compaction_l2_to_l3() {
        // With max_levels=4, L2→L3 is partial compaction (output_level 3 >= max_levels-1 = 3)
        // This is 2L-Spooky: only bottom two levels (L2, L3) use partial compaction
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
        let mut picker = test_picker(&options);

        // L2 with size > target to trigger compaction
        let l2_ssts = vec![create_sst(1, 2, 10, 30, 2 * l2_target)];

        // L3 files that define partitions — must be >= min_boundary_size to emit boundaries
        let l3_min = min_boundary_size(&options, 3);
        let l3_ssts = vec![
            create_sst(10, 3, 0, 20, l3_min),
            create_sst(11, 3, 21, 40, l3_min),
            create_sst(12, 3, 41, 60, l3_min),
        ];

        let levels = Levels::new(options.max_levels())
            .add(2, l2_ssts, empty())
            .add(3, l3_ssts, empty());

        let job = picker.pick_compaction(&levels).unwrap();

        assert_eq!(job.input_level, 2);
        assert_eq!(job.output_level, 3);
        // Both input and output key ranges use the partition range
        // Since the L2 file [10, 30] is within partition 0 [Min, 20] and partition 1 (20, 40],
        // it spans from the start of the first partition to the end of the second.
        let expected_partition_range =
            Interval::new(Bound::Unbounded, Bound::Included(record_key(40)));
        assert_eq!(job.input_key_range, expected_partition_range.clone());
        assert_eq!(job.output_key_range, expected_partition_range);
        assert_eq!(
            job.partitions_grid,
            Some(vec![record_key(20), record_key(40)]),
            "L2→L3 should use partial compaction (2L-Spooky)"
        );
    }

    #[test]
    fn test_2l_spooky_with_more_levels() {
        // Test with max_levels=6 to verify 2L-Spooky behavior:
        // L0→L1, L1→L2, L2→L3, L3→L4 = full compaction (output < 5)
        // L4→L5 = partial compaction (output >= 5)
        let options = Options::default()
            .with_max_levels(6)
            .with_level0_file_num_compaction_trigger(4)
            .with_max_bytes_for_level_base(StorageQuantity::new(64, StorageUnit::Mebibytes))
            .with_max_bytes_for_level_multiplier(10.0);

        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let l3_target =
            (base_bytes as f64 * options.max_bytes_for_level_multiplier().powi(2)) as u64;
        let l4_target =
            (base_bytes as f64 * options.max_bytes_for_level_multiplier().powi(3)) as u64;

        let mut picker = test_picker(&options);

        // Verify L3→L4 uses full compaction (output_level 4 < max_levels-1 = 5)
        let mut levels = Levels::new(options.max_levels());
        levels = levels.add(3, once(create_sst(1, 3, 10, 30, l3_target * 2)), empty());
        // L4 is the max level here for boundaries; size doesn't matter for this assertion
        levels = levels.add(4, once(create_sst(10, 4, 5, 25, 1000)), empty());

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 3);
        assert_eq!(job.input_level, 3);
        assert_eq!(job.input_key_range, interval(10, 30));
        assert_eq!(job.output_level, 4);
        assert_eq!(job.output_key_range, interval(5, 25));
        assert_eq!(
            job.partitions_grid,
            Some(vec![]),
            "L3→L4 should partition the output"
        );

        // Unmark compaction to allow compaction on the next level
        picker.unmark_compacting(&job);

        // Verify L4→L5 uses partial compaction (output_level 5 >= max_levels-1 = 5)
        // L4 SSTable [10, 30] spans partitions 0 (keys ≤ 20) and 1 (keys > 20)
        // Since partition 1 is the last partition, the range is unbounded on both ends
        let l5_min = min_boundary_size(&options, 5);
        let mut levels = Levels::new(options.max_levels());
        levels = levels.add(4, once(create_sst(1, 4, 10, 30, l4_target * 2)), empty());
        levels = levels.add(5, once(create_sst(10, 5, 0, 20, l5_min)), empty());
        levels = levels.add(5, once(create_sst(11, 5, 21, 40, l5_min)), empty());

        let job = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job.input_level, 4);
        // For partial compaction, input_key_range uses the partition range
        // The L4 file [10, 30] spans both partitions (0 and 1), so the range is fully unbounded
        let expected_partition_range = Interval::all();
        assert_eq!(job.input_key_range, expected_partition_range.clone());
        assert_eq!(job.output_level, 5);
        assert_eq!(job.output_key_range, expected_partition_range);
        assert_eq!(
            job.partitions_grid,
            Some(vec![record_key(20)]),
            "L4→L5 should use partial compaction with max_levels=6"
        );
    }

    /// Creates an interval using the default collection and index encoding.
    fn interval(min: u32, max: u32) -> Interval<Vec<u8>> {
        Interval::closed(record_key(min), record_key(max))
    }

    fn span_vec(items: Vec<Arc<dyn LevelItem>>) -> Interval<Vec<u8>> {
        span(items.iter().map(|item| item.as_ref()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn test_last_level_never_selected_even_with_other_levels() {
        let options = test_options(); // max_levels = 4
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let multiplier = options.max_bytes_for_level_multiplier();

        let mut picker = test_picker(&options);

        let l3_target = (base_bytes as f64 * multiplier.powi(2)) as u64;

        // Create L0 with score < 1.0 (only 2 files, trigger is 4)
        let l0_ssts = vec![
            create_sst(1, 0, 10, 20, 1000),
            create_sst(2, 0, 30, 40, 1000),
        ];

        // Create L3 (last level) with massive size
        let l3_ssts = vec![create_sst(100, 3, 10, 50, l3_target * 100)];

        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(3, l3_ssts, empty());

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
        let mut picker = test_picker(&options);

        // Create L0 with score ~1.25 (5 files, trigger=4)
        // (5 files is fine here — we just want L0 score > 1.0 and less than L1's)
        let l0_ssts: Vec<_> = (1..=5)
            .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
            .collect();

        // Create L1 with score ~3.0 (3x base_bytes)
        let l1_ssts = vec![create_sst(100, 1, 10, 50, base_bytes * 3)];

        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(1, l1_ssts, empty());

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
        let options = test_options();
        let picker = test_picker(&options); // max_levels = 4, so max level is L3
        let l3_min = min_boundary_size(&options, 3);

        // Case 1: Max level (L3) is empty -> single partition (no boundaries)
        let l0_ssts = vec![create_sst(1, 0, 0, 100, 1000)];
        let l1_ssts = vec![
            create_sst(2, 1, 0, 50, 1000),
            create_sst(3, 1, 51, 100, 1000),
        ];
        let levels = Levels::new(options.max_levels())
            .add(0, l0_ssts, empty())
            .add(1, l1_ssts, empty());
        assert!(picker.compute_partition_boundaries(&levels).is_empty());

        // Case 2: L2 has data but L3 (max level) is empty -> still single partition
        let l2_ssts = vec![
            create_sst(4, 2, 0, 30, 1000),
            create_sst(5, 2, 31, 60, 1000),
            create_sst(6, 2, 61, 100, 1000),
        ];
        let levels = levels.add(2, l2_ssts, empty());
        assert!(picker.compute_partition_boundaries(&levels).is_empty());

        // Case 3: L3 (max level) has 2 large SSTables -> 1 boundary
        let l3_ssts = vec![
            create_sst(7, 3, 0, 45, l3_min),
            create_sst(8, 3, 46, 100, l3_min),
        ];
        let levels = levels.add(3, l3_ssts, empty());

        let boundaries = picker.compute_partition_boundaries(&levels);
        assert_eq!(boundaries.len(), 1);
        assert_eq!(boundaries[0], record_key(45));

        // Case 4: L3 (max level) has 3 large SSTables -> 2 boundaries
        let levels = levels.add(3, vec![create_sst(9, 3, 101, 150, l3_min)], empty());

        let boundaries = picker.compute_partition_boundaries(&levels);
        assert_eq!(boundaries.len(), 2);
        assert_eq!(boundaries[0], record_key(45));
        assert_eq!(boundaries[1], record_key(100));

        // Case 5: Max level has only 1 SSTable -> single partition (no boundaries)
        let levels_single = Levels::new(options.max_levels()).add(
            3,
            vec![create_sst(1, 3, 0, 100, l3_min)],
            empty(),
        );
        assert!(picker
            .compute_partition_boundaries(&levels_single)
            .is_empty());

        // Case 6: L3 has 2 SSTables but both are too small -> no boundaries emitted
        let levels_small = Levels::new(options.max_levels()).add(
            3,
            vec![
                create_sst(1, 3, 0, 45, 1000),
                create_sst(2, 3, 46, 100, 1000),
            ],
            empty(),
        );
        assert!(
            picker
                .compute_partition_boundaries(&levels_small)
                .is_empty(),
            "Small SSTables below threshold should not emit boundaries"
        );

        // Case 7: Mixed: first SSTable large, second small -> no boundary (second is small)
        let levels_mixed = Levels::new(options.max_levels()).add(
            3,
            vec![
                create_sst(1, 3, 0, 45, l3_min),
                create_sst(2, 3, 46, 100, 1000),
            ],
            empty(),
        );
        assert!(
            picker
                .compute_partition_boundaries(&levels_mixed)
                .is_empty(),
            "Boundary should not be emitted before a small SSTable"
        );

        // Case 8: Mixed: first SSTable small, second large -> 1 boundary
        // (the second SSTable is large enough to deserve its own partition)
        let levels_mixed2 = Levels::new(options.max_levels()).add(
            3,
            vec![
                create_sst(1, 3, 0, 45, 1000),
                create_sst(2, 3, 46, 100, l3_min),
            ],
            empty(),
        );
        let boundaries = picker.compute_partition_boundaries(&levels_mixed2);
        assert_eq!(
            boundaries.len(),
            1,
            "Boundary should be emitted before the large SSTable"
        );
        assert_eq!(boundaries[0], record_key(45));
    }

    #[test]
    fn test_find_partition_key_range_for_item() {
        use std::ops::Bound;

        let picker = test_picker(&test_options());

        // Boundaries: [10, 20, 30]
        // Partition 0: keys <= 10         -> key range: (Unbounded, Included(10)]
        // Partition 1: 10 < keys <= 20    -> key range: (Excluded(10), Included(20)]
        // Partition 2: 20 < keys <= 30    -> key range: (Excluded(20), Included(30)]
        // Partition 3: keys > 30          -> key range: (Excluded(30), Unbounded)
        let boundaries = vec![record_key(10), record_key(20), record_key(30)];

        // File in partition 0 only
        let sst_p0 = create_sst(1, 3, 0, 5, 1000);
        let range = picker.find_partition_key_range_for_item(sst_p0.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Included(&record_key(10)));

        // File spanning partitions 0-1
        let sst_p0_p1 = create_sst(2, 3, 5, 15, 1000);
        let range = picker.find_partition_key_range_for_item(sst_p0_p1.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Included(&record_key(20)));

        // File spanning partitions 1-2
        let sst_p1_p2 = create_sst(3, 3, 15, 25, 1000);
        let range = picker.find_partition_key_range_for_item(sst_p1_p2.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(10)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(30)));

        // File spanning all partitions
        let sst_all = create_sst(4, 3, 0, 100, 1000);
        let range = picker.find_partition_key_range_for_item(sst_all.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Unbounded);

        // File in last partition only
        let sst_last = create_sst(5, 3, 40, 50, 1000);
        let range = picker.find_partition_key_range_for_item(sst_last.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(30)));
        assert_eq!(range.end_bound(), Bound::Unbounded);

        // File in middle partition only (partition 2)
        let sst_middle = create_sst(6, 3, 22, 28, 1000);
        let range = picker.find_partition_key_range_for_item(sst_middle.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(20)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(30)));

        // Empty boundaries: single partition with unbounded range
        let empty_boundaries: Vec<Vec<u8>> = Vec::new();
        let sst_unbounded = create_sst(7, 3, 0, 100, 1000);
        let range =
            picker.find_partition_key_range_for_item(sst_unbounded.as_ref(), &empty_boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Unbounded);

        // Test excluded bounds (as created by drop splitting)
        // Drop with excluded start at boundary 10 should be in partition 1, not 0
        let drop_excluded_start = Arc::new(DropMetadata {
            collection: 1,
            kind: DropKind::Collection,
            key_range: Interval::open_closed(record_key(10), record_key(15)),
            drop_sequence_number: 100,
        });
        let range =
            picker.find_partition_key_range_for_item(drop_excluded_start.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(10)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(20)));

        // Drop with excluded end at boundary 20 should be in partition 1, not spanning to 2
        let drop_excluded_end = Arc::new(DropMetadata {
            collection: 1,
            kind: DropKind::Collection,
            key_range: Interval::closed_open(record_key(15), record_key(20)),
            drop_sequence_number: 100,
        });
        let range =
            picker.find_partition_key_range_for_item(drop_excluded_end.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(10)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(20)));

        // Drop with both excluded bounds exactly on boundaries
        // (Excluded(10), Excluded(20)) should be in partition 1 only
        let drop_both_excluded = Arc::new(DropMetadata {
            collection: 1,
            kind: DropKind::Collection,
            key_range: Interval::open(record_key(10), record_key(20)),
            drop_sequence_number: 100,
        });
        let range =
            picker.find_partition_key_range_for_item(drop_both_excluded.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Excluded(&record_key(10)));
        assert_eq!(range.end_bound(), Bound::Included(&record_key(20)));

        // Drop spanning from partition 0 with excluded end at boundary 10
        // Should stay in partition 0
        let drop_p0_excluded_end = Arc::new(DropMetadata {
            collection: 1,
            kind: DropKind::Collection,
            key_range: Interval::closed_open(record_key(5), record_key(10)),
            drop_sequence_number: 100,
        });
        let range =
            picker.find_partition_key_range_for_item(drop_p0_excluded_end.as_ref(), &boundaries);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Included(&record_key(10)));
    }

    #[test]
    fn test_l0_compaction_score_by_size() {
        let options = test_options();
        let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
        let mut picker = test_picker(&options);

        // Create L0 with only 2 files (below trigger of 4) but large total size
        let mut levels = Levels::new(options.max_levels());
        levels = levels.add(0, once(create_sst(1, 0, 10, 50, base_bytes)), empty()); // large file
        levels = levels.add(0, once(create_sst(2, 0, 60, 100, base_bytes)), empty()); // large file

        let scores = picker.compute_scores(&levels);

        // File count score: 2/4 = 0.5
        // Size score: 2*base_bytes / base_bytes = 2.0
        // L0 score should be max(0.5, 2.0) = 2.0
        assert!(
            scores.scores[0] > 1.0,
            "L0 score should exceed 1.0 due to size"
        );
        assert!(
            (scores.scores[0] - 2.0).abs() < 0.01,
            "L0 score should be ~2.0"
        );

        let job = picker.pick_compaction(&levels);
        assert!(job.is_some(), "Should trigger compaction based on size");
    }

    #[test]
    fn test_unmark_compacting_allows_recompaction() {
        let options = test_options();
        let mut picker = test_picker(&options);

        let l0_ssts: Vec<_> = (1..=4)
            .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
            .collect();
        let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

        // Pick and auto-mark
        let job1 = picker.pick_compaction(&levels).unwrap();

        // Verify ranges are marked
        assert!(
            !picker.compacting_ranges[0].is_empty(),
            "Input level should have marked ranges"
        );

        // Unmark
        picker.unmark_compacting(&job1);

        // Verify ranges are cleared
        assert!(
            picker.compacting_ranges[0].is_empty(),
            "Input level ranges should be cleared"
        );
        assert!(
            picker.compacting_ranges[1].is_empty(),
            "Output level ranges should be cleared"
        );

        // Pick again - should get same compaction
        let job2 = picker.pick_compaction(&levels).unwrap();
        assert_eq!(job2.input_level, job1.input_level);
        assert_eq!(job2.output_level, job1.output_level);
    }

    mod full_compactions {
        use super::*;

        #[test]
        fn test_full_compaction_l0_to_l1() {
            // With max_levels=4, L0→L1 is full compaction (output_level 1 < max_levels-1 = 3)
            let options = test_options();
            let mut picker = test_picker(&options);

            let l0_ssts: Vec<_> = (1..=5).map(|i| create_sst(i, 0, 10, 50, 1000)).collect();
            let l1_ssts = vec![
                create_sst(10, 1, 5, 25, 1000),
                create_sst(11, 1, 30, 60, 1000),
            ];

            let levels = Levels::new(options.max_levels())
                .add(0, l0_ssts, empty())
                .add(1, l1_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(job.input_level, 0);
            assert_eq!(job.output_level, 1);
            assert_eq!(job.input_key_range, interval(10, 50));
            // output_key_range spans all overlapping L1 files
            assert_eq!(
                job.output_key_range,
                Interval::closed(record_key(5), record_key(60))
            );
            assert!(
                job.partitions_grid.is_none(),
                "L0→L1 should use full compaction"
            );
            assert!(!job.input_files.is_empty());
        }

        #[test]
        fn test_full_compaction_l1_to_l2() {
            // With max_levels=4, L1→L2 is full compaction (output_level 2 < max_levels-1 = 3)
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let mut picker = test_picker(&options);

            // L1 with size > base_bytes to trigger compaction
            let l1_ssts = vec![
                create_sst(1, 1, 10, 30, base_bytes),
                create_sst(2, 1, 31, 50, base_bytes),
            ];

            // L2 files
            let l2_ssts = vec![
                create_sst(10, 2, 5, 25, 1000),
                create_sst(11, 2, 26, 60, 1000),
            ];

            let levels = Levels::new(options.max_levels())
                .add(1, l1_ssts, empty())
                .add(2, l2_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(job.input_level, 1);
            // input_key_range spans all L1 files
            assert_eq!(
                job.input_key_range,
                Interval::closed(record_key(10), record_key(50))
            );
            assert_eq!(job.output_level, 2);
            // output_key_range spans all overlapping L2 files
            assert_eq!(
                job.output_key_range,
                Interval::closed(record_key(5), record_key(60))
            );
            assert_eq!(
                job.partitions_grid,
                Some(vec![]),
                "L1→L2 should partition on output"
            );
        }

        #[test]
        fn test_full_compaction_blocks_level() {
            let options = test_options();
            let mut picker = test_picker(&options);

            // Create L0 files to trigger compaction
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

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
        fn test_full_compaction_includes_all_drops_regardless_of_key_range() {
            // Full compaction should include ALL drops from input level,
            // not just those overlapping with SSTable key ranges
            let options = test_options();
            let mut picker = test_picker(&options);

            let col = 10;
            let col_2 = 11;
            let col_3 = 12;

            // L0 files with narrow key range
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| {
                    create_sst_for(
                        i,
                        0,
                        &record_key_for(col, 0, 10),
                        &record_key_for(col, 0, 20),
                        1000,
                    )
                })
                .collect();

            // Drops with key ranges that don't overlap with SSTables
            // Note: index=0 historically meant "drop entire collection".
            let drop_2 = DropMetadata::new_collection_drop(col_2, 100);
            let drop_3 = DropMetadata::new_collection_drop(col_3, 100);
            let l0_drops = vec![
                drop_2.clone(), // Outside SSTable range
                drop_3.clone(), // Outside SSTable range
            ];

            let levels =
                Levels::new(options.max_levels()).add(0, l0_ssts.clone(), l0_drops.clone());

            let job = picker.pick_compaction(&levels).unwrap();

            // Full compaction should include ALL drops from input level.
            // The input_key_range will be the span of SSTables (col 10) and Drops (col 11, col 12).
            let expected_range = span_vec(
                l0_ssts
                    .iter()
                    .map(|s| s.clone() as Arc<dyn LevelItem>)
                    .chain(l0_drops.iter().map(|d| d.clone() as Arc<dyn LevelItem>))
                    .collect(),
            );

            assert_eq!(job.input_level, 0);
            assert_eq!(job.output_level, 1);
            assert_eq!(job.input_files, l0_ssts);
            assert_eq!(job.drops, l0_drops);
            assert_eq!(job.input_key_range, expected_range);
            assert_eq!(job.output_key_range, expected_range);
        }

        #[test]
        fn test_full_compaction_blocked_by_partial() {
            // If a partial compaction is running on a level, full compaction
            // involving that level should be blocked
            let options = test_options(); // max_levels = 4
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            // Create a scenario where L1→L2 (full) and L2→L3 (partial) could both be triggered
            // L1 with high score (uses lower SST number so it's older)
            let l1_ssts = vec![create_sst(1, 1, 10, 50, base_bytes * 2)];

            // L2 with high score
            let l2_ssts = vec![create_sst(2, 2, 10, 30, l2_target * 2)];

            // L3 files (higher SST numbers)
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, 1000),
                create_sst(11, 3, 51, 100, 1000),
            ];

            let levels = Levels::new(options.max_levels())
                .add(1, l1_ssts, empty())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            // Manually create a partial compaction job to simulate L2→L3 running
            let partial_job = CompactionJob {
                id: 0,
                input_level: 2,
                output_level: 3,
                input_files: vec![create_sst(2, 2, 10, 30, l2_target * 2)],
                output_files: vec![create_sst(10, 3, 0, 50, 1000)],
                drops: vec![],
                input_key_range: interval(10, 30),
                output_key_range: interval(0, 50),
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
                assert!(
                    j.input_level != 1 || j.output_level != 2,
                    "L1→L2 full compaction should be blocked by L2→L3 partial"
                );
            }
        }
        #[test]
        fn test_full_compaction_includes_drops_from_input_level() {
            let options = test_options();
            let mut picker = test_picker(&options);

            let col = 10;
            let col_2 = 11;
            let idx = 0;

            // Create L0 files to trigger compaction
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| {
                    let min = (i * 10) as u32;
                    let max = (i * 10 + 9) as u32;
                    create_sst_for(
                        i,
                        0,
                        &record_key_for(col, idx, min),
                        &record_key_for(col, idx, max),
                        1000,
                    )
                })
                .collect();

            // Create drops in L0.
            // Historically index=0 meant "drop entire collection".
            let drop_1 = DropMetadata::new_collection_drop(col, 100);
            let drop_2 = DropMetadata::new_collection_drop(col_2, 200);
            let l0_drops = vec![drop_1.clone(), drop_2.clone()];

            let levels =
                Levels::new(options.max_levels()).add(0, l0_ssts.clone(), l0_drops.clone());

            let job = picker.pick_compaction(&levels).unwrap();

            // The items_range for L0 will span from MinKey(col=10) to MaxKey(col=11).
            let expected_range = levels
                .level(0)
                .unwrap()
                .items_range()
                .expect("L0 should have items");

            assert_eq!(job.input_level, 0);
            assert_eq!(job.output_level, 1);
            assert_eq!(job.input_files, l0_ssts);
            assert_eq!(job.drops, l0_drops);
            assert_eq!(job.input_key_range, expected_range.clone());
            assert_eq!(job.output_key_range, expected_range);
        }

        #[test]
        fn test_full_compaction_ignores_drops_from_output_level() {
            let options = test_options();
            let mut picker = test_picker(&options);

            // Create L0 files to trigger compaction
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();

            // Drop in L0 (input level) - historically index=0 meant "drop entire collection"
            let l0_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100)];

            // Drop in L1 (output level) - should be ignored
            let l1_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 200)];

            let l1_ssts = vec![create_sst(10, 1, 5, 60, 1000)];

            let levels = Levels::new(options.max_levels())
                .add(0, l0_ssts, l0_drops)
                .add(1, l1_ssts, l1_drops);

            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(
                job.drops.len(),
                1,
                "Should only include drops from input level"
            );
            assert_eq!(
                job.drops[0].drop_sequence_number, 100,
                "Should only include L0 drop"
            );
        }

        #[test]
        fn test_full_compaction_l1_to_l2_with_drops() {
            // L1→L2 is full compaction with partitions_grid (output_level >= level_x)
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let mut picker = test_picker(&options);

            // L1 files to trigger compaction
            let l1_ssts = vec![
                create_sst(1, 1, 10, 30, base_bytes),
                create_sst(2, 1, 31, 50, base_bytes),
            ];

            // Drops in L1 - historically index=0 meant "drop entire collection"
            let drop1 = DropMetadata::new_collection_drop(100, 100);
            let drop2 = DropMetadata::new_collection_drop(101, 200);
            let l1_drops = vec![drop1.clone(), drop2.clone()];

            // L2 files
            let l2_ssts = vec![
                create_sst(10, 2, 5, 25, 1000),
                create_sst(11, 2, 26, 60, 1000),
            ];

            let levels = Levels::new(options.max_levels())
                .add(1, l1_ssts, l1_drops)
                .add(2, l2_ssts, empty());

            let job = picker
                .pick_compaction(&levels)
                .expect("Should pick a compaction");

            assert_eq!(job.input_level, 1);
            assert_eq!(job.output_level, 2);
            // Full compaction includes all drops from input level
            assert_eq!(job.drops.len(), 2);
            assert!(job.drops.contains(&drop1));
            assert!(job.drops.contains(&drop2));
            // L1→L2 should have partitions_grid since output_level >= level_x (2 >= 2)
            assert!(job.partitions_grid.is_some());
        }
    }

    mod partial_compactions {
        use super::*;

        #[test]
        fn test_partial_compaction_allows_parallel_different_partitions() {
            let options = test_options(); // max_levels = 4
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // Create L2 with multiple files in different key ranges
            // Use lower SST numbers for older files (lower sequence numbers)
            let l2_ssts = vec![
                create_sst(1, 2, 10, 30, l2_target),   // Partition 0 (oldest)
                create_sst(2, 2, 110, 130, l2_target), // Partition 2 (newer)
            ];

            // L3 files that define partitions: [0-50], [51-100], [101-150]
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
                create_sst(12, 3, 101, 150, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            // Pick first compaction (should be partial, L2→L3, auto-marked)
            // Should pick SST 1 first (oldest by sequence number)
            let job1 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(job1.input_level, 2);
            assert_eq!(job1.output_level, 3);
            // Partition range for first file: (Unbounded, Included(50)]
            let partition1 = Interval::new(Bound::Unbounded, Bound::Included(record_key(50)));
            assert_eq!(job1.input_key_range, partition1.clone());
            assert_eq!(job1.output_key_range, partition1);
            assert_eq!(
                job1.partitions_grid,
                Some(vec![record_key(50), record_key(100)]),
                "L2→L3 should be partial"
            );
            assert_eq!(job1.input_files.len(), 1);
            assert_eq!(
                job1.input_files[0].number, 1,
                "Should pick oldest file first"
            );

            // Pick another compaction - should be able to pick the other L2 file
            // because it's in a different partition
            let job2 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(job2.input_level, 2);
            // Partition range for second file: (Excluded(100), Unbounded)
            let partition2 = Interval::new(Bound::Excluded(record_key(100)), Bound::Unbounded);
            assert_eq!(job2.input_key_range, partition2.clone());
            assert_eq!(job2.output_key_range, partition2);
            assert!(job2.partitions_grid.is_some());
            assert_eq!(job2.input_files.len(), 1);
            assert_eq!(job2.input_files[0].number, 2, "Should pick the other file");
        }

        #[test]
        fn test_partial_compaction_blocks_overlapping_partition_ranges() {
            let options = test_options(); // max_levels = 4
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // Create L2 with two non-overlapping files that map to the same L3 partition
            // File 1: [10, 30] in partition 0 (keys <= 45)
            // File 2: [35, 44] also in partition 0 (keys <= 45)
            // Both files map to the same partition, so they should block each other
            let l2_ssts = vec![
                create_sst(1, 2, 10, 30, l2_target), // Partition 0
                create_sst(2, 2, 35, 44, l2_target), // Partition 0
            ];

            // L3 files that define partitions: boundary at key 45
            let l3_ssts = vec![
                create_sst(10, 3, 0, 45, l3_min),
                create_sst(11, 3, 46, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            // Pick first compaction (auto-marked)
            let job1 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(job1.input_level, 2);
            assert_eq!(
                job1.input_files[0].number, 1,
                "Should pick oldest file first"
            );
            // Both input and output use the partition range
            let partition_range = Interval::new(Bound::Unbounded, Bound::Included(record_key(45)));
            assert_eq!(job1.input_key_range, partition_range.clone());
            assert_eq!(job1.output_key_range, partition_range);
            // Output should include L3 file 10 (partition 0)
            assert_eq!(job1.output_files.len(), 1);
            assert_eq!(job1.output_files[0].number, 10);

            // Try to pick another - should be blocked because both L2 files map to
            // the same partition (partition ranges overlap)
            let job2 = picker.pick_compaction(&levels);
            assert!(
                job2.is_none(),
                "Compactions in the same partition should block each other"
            );
        }

        #[test]
        fn test_partial_compaction_oldest_file_first() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // Create L2 files with explicit sequence numbers to verify ordering
            // SST 3 has lowest sequence number (oldest), SST 1 has highest (newest)
            // Add in non-sequential order to ensure sorting works
            let sst_newest = Arc::new(SSTableMetadata::new(
                1,
                2,
                &record_key(10),
                &record_key(30),
                300,
                350,
                l2_target,
            ));
            let sst_middle = Arc::new(SSTableMetadata::new(
                2,
                2,
                &record_key(60),
                &record_key(80),
                200,
                250,
                l2_target,
            ));
            let sst_oldest = Arc::new(SSTableMetadata::new(
                3,
                2,
                &record_key(110),
                &record_key(130),
                100,
                150,
                l2_target,
            ));

            let l2_ssts = vec![sst_newest, sst_middle, sst_oldest];

            // L3 files defining partitions
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
                create_sst(12, 3, 101, 150, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            // First pick should get the oldest file (SST 3, seq 100)
            let job1 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(
                job1.input_files[0].number, 3,
                "Should pick oldest file (lowest seq num) first"
            );
            // Partition range: (Excluded(100), Unbounded)
            let partition3 = Interval::new(Bound::Excluded(record_key(100)), Bound::Unbounded);
            assert_eq!(job1.input_key_range, partition3.clone());
            assert_eq!(job1.output_key_range, partition3);
            assert_eq!(
                job1.partitions_grid,
                Some(vec![record_key(50), record_key(100)]),
                "Should use partial compaction"
            );

            // Second pick should get middle file (SST 2, seq 200)
            let job2 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(
                job2.input_files[0].number, 2,
                "Should pick second oldest file"
            );
            // Partition range: (Excluded(50), Included(100)]
            let partition2 = Interval::new(
                Bound::Excluded(record_key(50)),
                Bound::Included(record_key(100)),
            );
            assert_eq!(job2.input_key_range, partition2.clone());
            assert_eq!(job2.output_key_range, partition2);
            assert_eq!(
                job2.partitions_grid,
                Some(vec![record_key(50), record_key(100)]),
                "Should use partial compaction"
            );

            // Third pick should get newest file (SST 1, seq 300)
            let job3 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(
                job3.input_files[0].number, 1,
                "Should pick newest file last"
            );
            // Partition range: (Unbounded, Included(50)]
            let partition1 = Interval::new(Bound::Unbounded, Bound::Included(record_key(50)));
            assert_eq!(job3.input_key_range, partition1.clone());
            assert_eq!(job3.output_key_range, partition1);
        }

        #[test]
        fn test_partial_compaction_with_empty_output_level() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            let mut levels = Levels::new(options.max_levels());

            // L2 file that needs compaction
            levels = levels.add(2, once(create_sst(1, 2, 10, 30, l2_target * 2)), empty());

            // L3 is empty - no partition boundaries exist

            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(job.input_level, 2);
            assert_eq!(job.output_level, 3);
            assert_eq!(job.input_files.len(), 1);
            assert!(
                job.output_files.is_empty(),
                "Output files should be empty when output level is empty"
            );
            // Even with empty output level, output_key_range uses the partition range to block the range
            assert_eq!(
                job.output_key_range,
                Interval::all(),
                "Output key range should use partition range"
            );
            assert_eq!(
                job.partitions_grid,
                Some(vec![]),
                "Should have empty partition grid"
            );
        }

        #[test]
        fn test_partial_compaction_includes_drops_in_partition_range() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 file in partition 0 (keys <= 50)
            let l2_ssts = vec![create_sst(1, 2, 10, 30, l2_target * 2)];

            // Create a single full-range drop and split it at the L3 partition boundary (key 50)
            let full_drop = DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100);
            let (drop_p0, drop_p1) = full_drop.split_at(&record_key(50)).expect_two();
            let l2_drops = vec![drop_p0.clone(), drop_p1];

            // L3 files defining partitions: partition 0 is keys <= 50, partition 1 is keys > 50
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker
                .pick_compaction(&levels)
                .expect("Should pick a compaction");

            assert_eq!(job.input_level, 2);
            assert_eq!(job.output_level, 3);
            // Should only include the fragment intersecting partition 0
            assert_eq!(
                job.drops.len(),
                1,
                "Should only include drops in the partition range"
            );
            assert_eq!(
                job.drops[0], drop_p0,
                "Should include drop fragment in partition 0"
            );
        }

        #[test]
        fn test_partial_compaction_with_drop_only_no_sstables() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 has an SSTable in partition 1 (keys > 50) and a drop fragment in partition 0 (keys <= 50).
            // We give the SSTable (seq 100) an older sequence number than the drop (seq 200)
            // so the picker selects the SSTable partition (partition 1).
            let full_drop = DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 200);
            let (drop_p0, _) = full_drop.split_at(&record_key(50)).expect_two();
            let l2_drops = vec![drop_p0];

            // create_sst(1, ..) uses seq 100.
            let l2_ssts = vec![create_sst(1, 2, 60, 80, l2_target * 2)];

            // L3 files defining partitions
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker
                .pick_compaction(&levels)
                .expect("Should pick a compaction");

            // Should pick partition 1 (keys > 50)
            assert_eq!(job.input_files.len(), 1);
            assert_eq!(job.input_files[0].number, 1);
            assert!(
                job.drops.is_empty(),
                "Drop fragment from partition 0 should not be included"
            );
            assert_eq!(job.output_files.len(), 1);
            assert_eq!(job.output_files[0].number, 11);
        }

        #[test]
        fn test_partial_compaction_drop_spans_multiple_partitions() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 file in partition 0
            let l2_ssts = vec![create_sst(1, 2, 10, 30, l2_target * 2)];

            // Drop spanning partitions 0 and 1 (uses same collection/index as SSTables)
            let l2_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100)];

            // L3 files defining partitions
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // The drop [40, 70] overlaps with partition 0 (keys <= 50)
            assert_eq!(
                job.drops.len(),
                1,
                "Drop overlapping partition should be included"
            );
        }

        #[test]
        fn test_partial_compaction_partition_unchanged() {
            // Scenario: Partition boundaries in L3 haven't changed
            // The compaction should work with existing partition structure
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 file in partition 1 (uses default collection/index)
            let l2_ssts = vec![create_sst(1, 2, 60, 80, l2_target * 2)];
            // Drop uses same collection/index as SSTables.
            // Historically index=0 meant "drop entire collection".
            let l2_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100)];

            // L3 with stable partitions
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops.clone())
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // Partition range for partition 1: (Excluded(50), Included(100)]
            let expected_range = Interval::new(Bound::Excluded(record_key(50)), Bound::Unbounded);
            assert_eq!(job.input_key_range, expected_range);
            assert_eq!(job.output_key_range, expected_range);
            assert_eq!(job.drops.len(), 1);
            assert_eq!(job.drops[0].collection, DEFAULT_COLLECTION);
            assert_eq!(job.partitions_grid, Some(vec![record_key(50)]));
        }

        #[test]
        fn test_partial_compaction_partition_split_into_two() {
            // Scenario: A partition was split into two partitions
            // The L2 file/drop might now span two partitions in L3
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 file that was originally in one partition but now spans two
            // Original partition was [0, 100], now split into [0, 50] and [51, 100]
            let l2_ssts = vec![create_sst(1, 2, 30, 70, l2_target * 2)];
            // Drop uses same collection/index as SSTables
            let l2_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100)];

            // L3 with new split partitions
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),   // Partition 0
                create_sst(11, 3, 51, 100, l3_min), // Partition 1
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // The SST [30, 70] spans both partitions 0 and 1
            // So the partition range should cover both partitions
            let expected_range = Interval::all(); // (Unbounded, Unbounded) since it spans from first to last partition
            assert_eq!(job.input_key_range, expected_range);
            assert_eq!(job.output_key_range, expected_range);

            // Drop [40, 60] also spans both partitions
            assert_eq!(job.drops.len(), 1);
            assert_eq!(job.input_files.len(), 1);

            // Output should include both L3 files
            assert_eq!(job.output_files.len(), 2);
        }

        #[test]
        fn test_partial_compaction_partition_deleted() {
            // Scenario: A partition was deleted (e.g., all data in that range was dropped)
            // Now the L3 level has fewer partitions
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            // L2 file in what was partition 1, but now partition 1 is gone
            // and this key range maps to the last (unbounded) partition
            let l2_ssts = vec![create_sst(1, 2, 60, 80, l2_target * 2)];
            // Drop uses same collection/index as SSTables
            let l2_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100)];

            // L3 with only one partition (the middle one was deleted) - single SST has no boundary
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, min_boundary_size(&options, 3)), // Only partition 0
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            let expected_range = Interval::all();
            assert_eq!(job.input_key_range, expected_range);
            assert_eq!(job.output_key_range, expected_range);
            assert_eq!(job.drops.len(), 1);
        }

        #[test]
        fn test_partial_compaction_drop_at_partition_boundary() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 file clearly in partition 0
            let l2_ssts = vec![create_sst(1, 2, 10, 30, l2_target * 2)];

            // Drop exactly at boundary [50, 50] - should be in partition 0
            // Uses same collection/index as SSTables
            let l2_drops = vec![DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100)];

            // L3 files defining partitions at 50
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // Partition 0 range: (Unbounded, Included(50)]
            let expected_range = Interval::new(Bound::Unbounded, Bound::Included(record_key(50)));
            assert_eq!(job.input_key_range, expected_range);

            // The drop [50, 50] should be included since it's at the boundary (included)
            assert_eq!(job.drops.len(), 1);
        }

        #[test]
        fn test_partial_compaction_multiple_drops_same_partition() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let mut picker = test_picker(&options);

            // L2 file in partition 0 (keys <= 50)
            let l2_ssts = vec![create_sst(1, 2, 10, 30, l2_target * 2)];

            // Create a single full drop and split it into 3 fragments all within partition 0
            // Historically index=0 meant "drop entire collection".
            let full_drop = DropMetadata::new_collection_drop(DEFAULT_COLLECTION, 100);
            let (left_of_50, _) = full_drop.split_at(&record_key(50)).expect_two();
            let (frag1, rem) = left_of_50.split_at(&record_key(20)).expect_two();
            let (frag2, frag3) = rem.split_at(&record_key(35)).expect_two();

            let l2_drops = vec![frag1.clone(), frag2.clone(), frag3.clone()];

            // L3 files defining partitions (partition 0 is keys <= 50)
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker
                .pick_compaction(&levels)
                .expect("Should pick a compaction");

            // All 3 fragments within partition 0 should be included
            assert_eq!(job.drops.len(), 3);
            assert!(job.drops.contains(&frag1));
            assert!(job.drops.contains(&frag2));
            assert!(job.drops.contains(&frag3));
        }

        #[test]
        fn test_partial_compaction_empty_l3_single_partition() {
            // When L3 is empty, there's a single partition covering everything
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            // L2 file
            let l2_sst = create_sst(1, 2, 10, 30, l2_target * 2);

            // Drops for different (collection, index) pairs.
            let drop1 = DropMetadata::new_collection_drop(100, 100);
            let drop2 = DropMetadata::new_collection_drop(101, 150);

            // L3 is empty - single partition
            let levels = Levels::new(options.max_levels()).add(
                2,
                vec![l2_sst.clone()],
                vec![drop1.clone(), drop2.clone()],
            );

            let job = picker
                .pick_compaction(&levels)
                .expect("Should pick a compaction");

            // Single partition means all drops should be included.
            assert_eq!(
                job,
                CompactionJob {
                    id: job.id,
                    input_level: 2,
                    output_level: 3,
                    input_files: vec![l2_sst],
                    output_files: vec![],
                    drops: vec![drop1, drop2],
                    input_key_range: Interval::all(),
                    output_key_range: Interval::all(),
                    partitions_grid: Some(vec![]),
                }
            );
        }

        #[test]
        fn test_partial_compaction_drop_only_input_no_sstables_in_partition() {
            // Scenario: A partition has only a drop, no SSTables
            // This can happen when all data was deleted but the drop marker remains
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            let col = 10;
            let idx = 0; // Collection data, not an index

            // L2 has only a drop in partition 0, no SSTables
            // But we need an SSTable somewhere to trigger compaction score > 1
            // Put an SSTable in partition 2 and a drop in partition 0
            // Both use same collection/index
            let (drop, _) = DropMetadata::new_collection_drop(col, 100)
                .split_at(&record_key_for(col, idx, 50))
                .expect_two(); // drop [0, 50]
            let size = l2_target * 2;
            let l2_ssts = vec![create_sst_for(
                2,
                2,
                &record_key_for(col, idx, 110),
                &record_key_for(col, idx, 130),
                size,
            )]; // partition 2
            let l2_drops = vec![drop.clone()]; // partition 0

            // L3 files defining partitions: 0=[0,50], 1=[51,100], 2=[101,150]
            let l3_min = min_boundary_size(&options, 3);
            let l3_sst_0 = create_sst_for(
                10,
                3,
                &record_key_for(col, idx, 0),
                &record_key_for(col, idx, 50),
                l3_min,
            );
            let l3_sst_1 = create_sst_for(
                11,
                3,
                &record_key_for(col, idx, 51),
                &record_key_for(col, idx, 100),
                l3_min,
            );
            let l3_sst_2 = create_sst_for(
                12,
                3,
                &record_key_for(col, idx, 101),
                &record_key_for(col, idx, 150),
                l3_min,
            );
            let l3_ssts = vec![l3_sst_0.clone(), l3_sst_1, l3_sst_2];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            // The picker iterates items by sequence number, so the drop (seq 100) comes before SST (seq 200)
            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(
                job,
                CompactionJob {
                    id: job.id,
                    input_level: 2,
                    output_level: 3,
                    input_files: vec![],
                    output_files: vec![l3_sst_0],
                    drops: vec![drop.clone()],
                    input_key_range: Interval::new(
                        Bound::Unbounded,
                        Bound::Included(record_key_for(col, idx, 50))
                    ),
                    output_key_range: Interval::new(
                        Bound::Unbounded,
                        Bound::Included(record_key_for(col, idx, 50))
                    ),
                    partitions_grid: Some(vec![
                        record_key_for(col, idx, 50),
                        record_key_for(col, idx, 100)
                    ]),
                }
            );
        }

        #[test]
        fn test_partial_compaction_partition_split_drop_spans_new_partitions() {
            // Scenario: A partition was split into two, and a drop that was in the
            // original partition now spans both new partitions
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            let col = 10;
            let idx = 0; // Collection data, not an index

            // L2 file and drop that span what is now two partitions
            // Original partition was [0, 100], now split at 50 into [0, 50] and [51, 100]
            // Both use same collection/index
            let size = l2_target * 2;
            let l2_sst = create_sst_for(
                1,
                2,
                &record_key_for(col, idx, 30),
                &record_key_for(col, idx, 70),
                size,
            );
            let drop = DropMetadata::new_collection_drop(col, 100);
            let l2_drops = vec![drop.clone()];

            // L3 with new split partitions
            let l3_min = min_boundary_size(&options, 3);
            let l3_sst_0 = create_sst_for(
                10,
                3,
                &record_key_for(col, idx, 0),
                &record_key_for(col, idx, 50),
                l3_min,
            ); // Partition 0
            let l3_sst_1 = create_sst_for(
                11,
                3,
                &record_key_for(col, idx, 51),
                &record_key_for(col, idx, 100),
                l3_min,
            ); // Partition 1
            let l3_ssts = vec![l3_sst_0.clone(), l3_sst_1.clone()];

            let levels = Levels::new(options.max_levels())
                .add(2, vec![l2_sst.clone()], l2_drops.clone())
                .add(3, l3_ssts.clone(), empty());

            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(
                job,
                CompactionJob {
                    id: job.id,
                    input_level: 2,
                    output_level: 3,
                    // The SST [30, 70] spans both partitions
                    // The partition range should cover both partitions (all)
                    input_files: vec![l2_sst],
                    output_files: vec![l3_sst_0, l3_sst_1],
                    // The drop spans both partitions and should be included
                    drops: vec![drop],
                    input_key_range: Interval::all(),
                    output_key_range: Interval::all(),
                    partitions_grid: Some(vec![record_key_for(col, idx, 50)]),
                }
            );
        }

        #[test]
        fn test_partial_compaction_partition_deleted_drop_moves_to_adjacent() {
            // Scenario: A partition was deleted, and a drop that was targeting that
            // partition now falls into the adjacent partition
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            let col = 10;
            let idx = 0; // Collection data, not an index

            // L2 file in what was partition 1 [51, 100], but partition 1 was deleted
            // Now there's only partition 0 [0, 50], so keys > 50 go to partition 1 (unbounded)
            // Both use same collection/index
            let size = l2_target * 2;
            let l2_sst = create_sst_for(
                1,
                2,
                &record_key_for(col, idx, 60),
                &record_key_for(col, idx, 80),
                size,
            );
            let l2_ssts = vec![l2_sst.clone()];
            let (_, drop) = DropMetadata::new_collection_drop(col, 100)
                .split_at(&record_key_for(col, idx, 50))
                .expect_two(); // drop (50, max]
            let l2_drops = vec![drop.clone()];

            // L3 with only partition 0 (the original partition 1 was deleted)
            let l3_sst = create_sst_for(
                10,
                3,
                &record_key_for(col, idx, 0),
                &record_key_for(col, idx, 50),
                min_boundary_size(&options, 3),
            ); // Only partition 0
            let l3_ssts = vec![l3_sst.clone()];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops.clone())
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            assert_eq!(
                job,
                CompactionJob {
                    id: job.id,
                    input_level: 2,
                    output_level: 3,
                    input_files: vec![l2_sst],
                    output_files: vec![l3_sst],
                    drops: vec![drop],
                    input_key_range: Interval::all(),
                    output_key_range: Interval::all(),
                    partitions_grid: Some(vec![]),
                }
            );
        }

        #[test]
        fn test_partial_compaction_multiple_drops_different_collections_same_partition() {
            // Scenario: Multiple drops from different collections in the same partition
            // This tests that drops from different collections can coexist and be picked
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            // L2 file in partition 0 - uses collection 1, index 0
            let collection = 1;
            let size = l2_target * 2;
            let l2_sst = create_sst_for(
                1,
                2,
                &record_key_for(collection, 0, 10),
                &record_key_for(collection, 0, 30),
                size,
            );
            let l2_ssts = vec![l2_sst.clone()];

            // Multiple drops from different collections, all with keys in partition 0
            // Each drop uses its own collection but same key range structure

            let drop_col_1 = DropMetadata::new_collection_drop(1, 100);
            let drop_col_2 = DropMetadata::new_collection_drop(2, 150);
            let drop_col_3 = DropMetadata::new_collection_drop(3, 200);
            let l2_drops = vec![drop_col_1.clone(), drop_col_2.clone(), drop_col_3.clone()];

            // L3 files defining partitions - use collection 1 to match SSTable
            let l3_min = min_boundary_size(&options, 3);
            let collection = 2;
            let l3_sst_10 = create_sst_for(
                10,
                3,
                &record_key_for(collection, 0, 0),
                &record_key_for(collection, 0, 50),
                l3_min,
            ); // Partition 0
            let collection = 3;
            let l3_sst_11 = create_sst_for(
                11,
                3,
                &record_key_for(collection, 0, 51),
                &record_key_for(collection, 0, 100),
                l3_min,
            ); // Partition 1
            let l3_ssts = vec![l3_sst_10.clone(), l3_sst_11.clone()];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // All drops in partition 0 should be included
            assert_eq!(job.input_level, 2);
            assert_eq!(job.output_level, 3);
            assert_eq!(job.input_files, vec![l2_sst]);
            assert_eq!(job.output_files, vec![l3_sst_10]);
            // The drops are picked if they overlap with the partition.
            // Drop for col 1 covers partition 0 [Unbounded, 50].
            // Drop for col 2 covers partition 0 [Unbounded, 50].
            assert_eq!(job.drops.len(), 2);
            assert!(job.drops.contains(&drop_col_1));
            assert!(job.drops.contains(&drop_col_2));
        }

        #[test]
        fn test_partial_compaction_drop_with_index_in_partition() {
            // Scenario: A drop for a specific index (not collection-wide)
            // The drop should be picked when its key range falls in the partition
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            // L2 file in partition 0 - cover collection 10 and 11 including any 10 indexes
            let l2_sst_10 = Arc::new(SSTableMetadata::new(
                10,
                2,
                &record_key_for(10, 0, 10),
                &record_key_for(11, 0, 30),
                1000,
                1050,
                l2_target * 2,
            ));

            // Drop for a specific index (index = 1, not 0) but same collection
            // Key range overlaps with partition 0
            let idx_drop = DropMetadata::new_index_drop(10, 1, 100);

            // L3 files defining partitions
            let l3_min = min_boundary_size(&options, 3);
            let l3_sst_1 = Arc::new(SSTableMetadata::new(
                1,
                3,
                &record_key_for(10, 0, 0),
                &record_key_for(11, 0, 50),
                100,
                150,
                l3_min,
            ));

            let l3_sst_2 = Arc::new(SSTableMetadata::new(
                2,
                3,
                &record_key_for(11, 0, 60),
                &record_key_for(11, 1, 50),
                100,
                150,
                l3_min,
            ));

            let levels = Levels::new(options.max_levels())
                .add(2, vec![l2_sst_10.clone()], vec![idx_drop.clone()])
                .add(3, vec![l3_sst_1.clone(), l3_sst_2], empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // The index-specific drop should be included
            // Partition 0 boundary is (col 11, idx 0, 50)
            let expected_partition_range =
                Interval::new(Bound::Unbounded, Bound::Included(record_key_for(11, 0, 50)));
            assert_eq!(
                job,
                CompactionJob {
                    id: job.id,
                    input_level: 2,
                    output_level: 3,
                    input_files: vec![l2_sst_10],
                    output_files: vec![l3_sst_1],
                    drops: vec![idx_drop],
                    input_key_range: expected_partition_range.clone(),
                    output_key_range: expected_partition_range,
                    partitions_grid: Some(vec![record_key_for(11, 0, 50)]),
                }
            );
        }

        #[test]
        fn test_partial_compaction_drop_outside_all_sstables_but_in_partition() {
            // A drop might exist in a partition without overlapping any SSTables,
            // but if there's an SSTable in the same partition (triggering compaction),
            // the drop should be included
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            let col = 10;
            let idx = 0; // Collection data, not an index
            let idx_2 = 1; // Different index for the drop
            let col_2 = 11; // Different collection for the partitions

            // L2 file in partition 0, but not overlapping with drop's user key range
            let l2_sst = create_sst_for(
                1,
                2,
                &record_key_for(col, idx, 5),
                &record_key_for(col, idx, 15),
                l2_target * 2,
            );

            // Drop in same partition but different key range - still should be included since it's in the same partition
            // This test is "index-specific drop", so keep it as an index drop (index != 0).
            let l2_drop = DropMetadata::new_index_drop(col, idx_2, 100);

            // L3 files defining partitions
            // Partition 0: keys up to (col_2, idx, 50)
            let l3_min = min_boundary_size(&options, 3);
            let boundary_key = record_key_for(col_2, idx, 50);
            let l3_10_sst =
                create_sst_for(10, 3, &record_key_for(col, idx, 1), &boundary_key, l3_min);
            let l3_11_sst = create_sst_for(
                11,
                3,
                &record_key_for(col_2, idx, 51),
                &record_key_for(col_2, idx, 3000),
                l3_min,
            );

            let levels = Levels::new(options.max_levels())
                .add(2, vec![l2_sst.clone()], vec![l2_drop.clone()])
                .add(3, vec![l3_10_sst.clone(), l3_11_sst], empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // The drop's key range (col 10, idx 1) falls into Partition 0 because
            // (col 10, idx 1) < (col 11, idx 50).
            let expected_partition_range =
                Interval::new(Bound::Unbounded, Bound::Included(boundary_key.clone()));
            assert_eq!(
                job,
                CompactionJob {
                    id: job.id,
                    input_level: 2,
                    output_level: 3,
                    input_files: vec![l2_sst],
                    output_files: vec![l3_10_sst],
                    drops: vec![l2_drop],
                    input_key_range: expected_partition_range.clone(),
                    output_key_range: expected_partition_range,
                    partitions_grid: Some(vec![boundary_key]),
                }
            );
        }

        #[test]
        fn test_partial_compaction_drop_in_different_collection_not_included() {
            // Drops for a different collection should NOT be included in partial compaction
            // if their key range doesn't overlap with the partition (due to different prefix)
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let mut picker = test_picker(&options);

            // L2 file in partition 0 using collection 1
            let collection = 1;
            let size = l2_target * 2;
            let l2_ssts = vec![create_sst_for(
                1,
                2,
                &record_key_for(collection, 0, 10),
                &record_key_for(collection, 0, 30),
                size,
            )];

            // Drop for collection 2 with same user key values
            // Due to different collection prefix, its encoded key range will be different
            let l2_drops = vec![DropMetadata::new_collection_drop(2, 100)];

            // L3 files defining partitions using collection 1
            let l3_min = min_boundary_size(&options, 3);
            let collection = 1;
            let collection1 = 1;
            let l3_ssts = vec![
                create_sst_for(
                    10,
                    3,
                    &record_key_for(collection1, 0, 0),
                    &record_key_for(collection1, 0, 50),
                    l3_min,
                ),
                create_sst_for(
                    11,
                    3,
                    &record_key_for(collection, 0, 51),
                    &record_key_for(collection, 0, 100),
                    l3_min,
                ),
            ];

            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, l2_drops)
                .add(3, l3_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // The drop for collection 2 has a different key prefix (collection=2),
            // so its key_range won't overlap with partition 0 (which has collection=1 prefix)
            // This depends on how the partition range is computed and drop filtering works
            assert_eq!(job.input_level, 2);
            // The drop should NOT be included since its key range (with collection=2 prefix)
            // doesn't overlap with the partition range (based on collection=1 boundaries)
            assert!(
                job.drops.is_empty(),
                "Drop with different collection prefix should not be included"
            );
        }
    }

    mod metrics_tests {
        use super::*;
        use crate::obs::logger;
        use crate::obs::metrics::{assert_counter_eq, assert_gauge_eq};

        fn test_picker_with_registry(options: &Options) -> (CompactionPicker, MetricRegistry) {
            let mut registry = MetricRegistry::new();
            let picker = CompactionPicker::new(logger::test_instance(), &mut registry, options);
            (picker, registry)
        }

        #[test]
        fn test_metrics_jobs_picked_counter() {
            let options = test_options();
            let (mut picker, registry) = test_picker_with_registry(&options);

            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

            assert_counter_eq(&registry, "compaction.jobs.picked", 0);

            let job = picker.pick_compaction(&levels).unwrap();
            assert_counter_eq(&registry, "compaction.jobs.picked", 1);

            picker.unmark_compacting(&job);
            let _job2 = picker.pick_compaction(&levels).unwrap();
            assert_counter_eq(&registry, "compaction.jobs.picked", 2);
        }

        #[test]
        fn test_metrics_full_vs_partial_counters() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let (mut picker, registry) = test_picker_with_registry(&options);

            // L0→L1 is full compaction
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

            let job1 = picker.pick_compaction(&levels).unwrap();
            assert_counter_eq(&registry, "compaction.jobs.picked.full", 1);
            assert_counter_eq(&registry, "compaction.jobs.picked.partial", 0);
            picker.unmark_compacting(&job1);

            // L2→L3 is partial compaction
            let l2_ssts = vec![create_sst(100, 2, 10, 30, l2_target * 2)];
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
            ];
            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            let _job2 = picker.pick_compaction(&levels).unwrap();
            assert_counter_eq(&registry, "compaction.jobs.picked.full", 1);
            assert_counter_eq(&registry, "compaction.jobs.picked.partial", 1);
        }

        #[test]
        fn test_metrics_skipped_level_compacting() {
            let options = test_options();
            let (mut picker, registry) = test_picker_with_registry(&options);

            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

            assert_counter_eq(&registry, "compaction.jobs.skipped.level_compacting", 0);

            let _job = picker.pick_compaction(&levels).unwrap();

            // Try to pick again - L0 is blocked by the active full compaction
            let job2 = picker.pick_compaction(&levels);
            assert!(job2.is_none());
            assert_counter_eq(&registry, "compaction.jobs.skipped.level_compacting", 1);
        }

        #[test]
        fn test_metrics_skipped_range_overlap() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let (mut picker, registry) = test_picker_with_registry(&options);

            // Two L2 files mapping to the same partition
            let l2_ssts = vec![
                create_sst(1, 2, 10, 30, l2_target),
                create_sst(2, 2, 35, 44, l2_target),
            ];
            let l3_ssts = vec![
                create_sst(10, 3, 0, 45, l3_min),
                create_sst(11, 3, 46, 100, l3_min),
            ];
            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            assert_counter_eq(&registry, "compaction.jobs.skipped.range_overlap", 0);

            let _job1 = picker.pick_compaction(&levels).unwrap();

            // Second pick should fail due to overlapping output ranges
            let job2 = picker.pick_compaction(&levels);
            assert!(job2.is_none());
            assert_counter_eq(&registry, "compaction.jobs.skipped.range_overlap", 1);
        }

        #[test]
        fn test_metrics_picked_from_level() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let (mut picker, registry) = test_picker_with_registry(&options);

            // L0 compaction
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

            assert_counter_eq(&registry, "compaction.picked.l0", 0);
            assert_counter_eq(&registry, "compaction.picked.l1", 0);

            let job = picker.pick_compaction(&levels).unwrap();
            assert_counter_eq(&registry, "compaction.picked.l0", 1);
            assert_counter_eq(&registry, "compaction.picked.l1", 0);
            picker.unmark_compacting(&job);

            // L1 compaction
            let l1_ssts = vec![create_sst(100, 1, 10, 50, base_bytes * 2)];
            let levels = Levels::new(options.max_levels()).add(1, l1_ssts, empty());

            let _job2 = picker.pick_compaction(&levels).unwrap();
            assert_counter_eq(&registry, "compaction.picked.l0", 1);
            assert_counter_eq(&registry, "compaction.picked.l1", 1);
        }

        #[test]
        fn test_metrics_score_per_level() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let (mut picker, registry) = test_picker_with_registry(&options);

            // Initially all scores should be 0
            assert_gauge_eq(&registry, "compaction.score.l0", 0);
            assert_gauge_eq(&registry, "compaction.score.l1", 0);
            assert_gauge_eq(&registry, "compaction.score.l3", 0);

            // Create L0 with 5 files (trigger is 4) -> score = 1.25
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();

            // Create L1 with 2x base_bytes -> score = 2.0
            let l1_ssts = vec![create_sst(100, 1, 10, 50, base_bytes * 2)];

            let levels = Levels::new(options.max_levels())
                .add(0, l0_ssts, empty())
                .add(1, l1_ssts, empty());

            let _job = picker.pick_compaction(&levels);

            // Scores are stored as score * 100 (125 for 1.25, 200 for 2.0)
            assert_gauge_eq(&registry, "compaction.score.l0", 125);
            assert_gauge_eq(&registry, "compaction.score.l1", 200);
            assert_gauge_eq(&registry, "compaction.score.l3", 0); // Last level always 0
        }

        #[test]
        fn test_metrics_active_per_level() {
            let options = test_options();
            let (mut picker, registry) = test_picker_with_registry(&options);

            assert_gauge_eq(&registry, "compaction.active.l0", 0);
            assert_gauge_eq(&registry, "compaction.active.l1", 0);

            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

            let job = picker.pick_compaction(&levels).unwrap();

            // Both input (L0) and output (L1) levels should show active
            assert_gauge_eq(&registry, "compaction.active.l0", 1);
            assert_gauge_eq(&registry, "compaction.active.l1", 1);

            picker.unmark_compacting(&job);

            // After unmark, both should be 0
            assert_gauge_eq(&registry, "compaction.active.l0", 0);
            assert_gauge_eq(&registry, "compaction.active.l1", 0);
        }

        #[test]
        fn test_metrics_active_parallel_compactions() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let l2_target = (base_bytes as f64 * options.max_bytes_for_level_multiplier()) as u64;
            let l3_min = min_boundary_size(&options, 3);
            let (mut picker, registry) = test_picker_with_registry(&options);

            // Create L2 files in different partitions for parallel compaction
            let l2_ssts = vec![
                create_sst(1, 2, 10, 30, l2_target),
                create_sst(2, 2, 110, 130, l2_target),
            ];
            let l3_ssts = vec![
                create_sst(10, 3, 0, 50, l3_min),
                create_sst(11, 3, 51, 100, l3_min),
                create_sst(12, 3, 101, 150, l3_min),
            ];
            let levels = Levels::new(options.max_levels())
                .add(2, l2_ssts, empty())
                .add(3, l3_ssts, empty());

            let job1 = picker.pick_compaction(&levels).unwrap();
            assert_gauge_eq(&registry, "compaction.active.l2", 1);
            assert_gauge_eq(&registry, "compaction.active.l3", 1);

            let job2 = picker.pick_compaction(&levels).unwrap();
            assert_gauge_eq(&registry, "compaction.active.l2", 2);
            assert_gauge_eq(&registry, "compaction.active.l3", 2);

            picker.unmark_compacting(&job1);
            assert_gauge_eq(&registry, "compaction.active.l2", 1);
            assert_gauge_eq(&registry, "compaction.active.l3", 1);

            picker.unmark_compacting(&job2);
            assert_gauge_eq(&registry, "compaction.active.l2", 0);
            assert_gauge_eq(&registry, "compaction.active.l3", 0);
        }

        #[test]
        fn test_metrics_input_files_count_histogram() {
            let options = test_options();
            let base_bytes = options.max_bytes_for_level_base().to_bytes() as u64;
            let (mut picker, registry) = test_picker_with_registry(&options);

            // L0 compaction with 5 input files
            let l0_ssts: Vec<_> = (1..=5)
                .map(|i| create_sst(i, 0, (i * 10) as u32, (i * 10 + 9) as u32, 1000))
                .collect();
            let levels = Levels::new(options.max_levels()).add(0, l0_ssts, empty());

            let job1 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(job1.input_files.len(), 5);
            picker.unmark_compacting(&job1);

            // L1 compaction with 1 input file
            let l1_ssts = vec![create_sst(100, 1, 10, 50, base_bytes * 2)];
            let levels = Levels::new(options.max_levels()).add(1, l1_ssts, empty());

            let job2 = picker.pick_compaction(&levels).unwrap();
            assert_eq!(job2.input_files.len(), 1);

            let histogram = registry
                .get_histogram("compaction.input_files.count")
                .unwrap();
            let snapshot = histogram.snapshot();
            assert_eq!(snapshot.count, 2);
            assert_eq!(snapshot.min, 1);
            assert_eq!(snapshot.max, 5);
        }

        #[test]
        fn test_metrics_no_compaction_no_increment() {
            let options = test_options();
            let (mut picker, registry) = test_picker_with_registry(&options);

            // Empty levels - no compaction needed
            let levels = Levels::new(options.max_levels());

            let job = picker.pick_compaction(&levels);
            assert!(job.is_none());

            // No counters should be incremented
            assert_counter_eq(&registry, "compaction.jobs.picked", 0);
            assert_counter_eq(&registry, "compaction.jobs.picked.full", 0);
            assert_counter_eq(&registry, "compaction.jobs.picked.partial", 0);
            assert_counter_eq(&registry, "compaction.jobs.skipped.level_compacting", 0);
            assert_counter_eq(&registry, "compaction.jobs.skipped.range_overlap", 0);
        }

        #[test]
        fn test_metrics_all_levels_registered() {
            let options = test_options(); // max_levels = 4
            let (_picker, registry) = test_picker_with_registry(&options);

            // Verify all per-level metrics are registered
            for level in 0..4 {
                assert!(
                    registry
                        .get_counter(&format!("compaction.picked.l{}", level))
                        .is_some(),
                    "Missing counter for compaction.picked.l{}",
                    level
                );
                assert!(
                    registry
                        .get_gauge(&format!("compaction.score.l{}", level))
                        .is_some(),
                    "Missing gauge for compaction.score.l{}",
                    level
                );
                assert!(
                    registry
                        .get_gauge(&format!("compaction.active.l{}", level))
                        .is_some(),
                    "Missing gauge for compaction.active.l{}",
                    level
                );
            }
        }
    }
}
