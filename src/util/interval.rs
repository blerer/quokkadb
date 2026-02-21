use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;

/// A struct representing a range with customizable bounds.
///
/// `Interval` allows you to define a range with different types of bounds:
/// - Inclusive (`[a..b]`)
/// - Exclusive (`(a..b)`)
/// - Mixed (`(a..b]` or `[a..b)`)
/// - Unbounded (`(..)`)
///
/// This struct can be used in conjunction with types like `BTreeMap` to specify range queries.
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct Interval<T> {
    start: Bound<T>,
    end: Bound<T>,
}

impl<T: Debug> Interval<T> {
    pub fn new(start: Bound<T>, end: Bound<T>) -> Self {
        Self { start, end }
    }

    /// Creates an open range `(a..b)`.
    ///
    /// Includes values `x` such that `a < x < b`.
    pub fn open(start: T, end: T) -> Self {
        Self {
            start: Bound::Excluded(start),
            end: Bound::Excluded(end),
        }
    }

    /// Creates a closed range `[a..b]`.
    ///
    /// Includes values `x` such that `a <= x <= b`.
    pub fn closed(start: T, end: T) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Included(end),
        }
    }

    /// Creates an open-closed range `(a..b]`.
    ///
    /// Includes values `x` such that `a < x <= b`.
    pub fn open_closed(start: T, end: T) -> Self {
        Self {
            start: Bound::Excluded(start),
            end: Bound::Included(end),
        }
    }

    /// Creates a closed-open range `[a..b)`.
    ///
    /// Includes values `x` such that `a <= x < b`.
    pub fn closed_open(start: T, end: T) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Excluded(end),
        }
    }

    /// Creates a range `(a..+∞)`.
    ///
    /// Includes values `x` such that `x > a`.
    pub fn greater_than(start: T) -> Self {
        Self {
            start: Bound::Excluded(start),
            end: Bound::Unbounded,
        }
    }

    /// Creates a range `[a..+∞)`.
    ///
    /// Includes values `x` such that `x >= a`.
    pub fn at_least(start: T) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Unbounded,
        }
    }

    /// Creates a range `(-∞..b)`.
    ///
    /// Includes values `x` such that `x < b`.
    pub fn less_than(end: T) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(end),
        }
    }

    /// Creates a range `(-∞..b]`.
    ///
    /// Includes values `x` such that `x <= b`.
    pub fn at_most(end: T) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(end),
        }
    }

    /// Creates a range `(-∞..+∞)`.
    ///
    /// Includes all values.
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl<T: Clone> Interval<T> {

    pub fn start_bound_value(&self) -> Option<T> {
        Self::bound_value(self.start_bound())
    }

    pub fn end_bound_value(&self) -> Option<T> {
        Self::bound_value(self.end_bound())
    }

    fn bound_value(b: Bound<&T>) -> Option<T> {
        match b {
            Bound::Included(v) | Bound::Excluded(v) => Some(v.clone()),
            Bound::Unbounded => None,
        }
    }
}

impl<T: Clone + Ord + Debug> Interval<T> {
    /// Computes the smallest interval that contains both `self` and `other`.
    ///
    /// Unlike `union`, this always succeeds even if the intervals are disjoint.
    /// The result spans from the minimum start bound to the maximum end bound.
    pub fn span(&self, other: &Self) -> Self {
        let start = if cmp_start_bounds(&self.start, &other.start) == Ordering::Less {
            self.start.clone()
        } else {
            other.start.clone()
        };

        let end = if cmp_end_bounds(&self.end, &other.end) == Ordering::Greater {
            self.end.clone()
        } else {
            other.end.clone()
        };

        Interval::new(start, end)
    }

    /// Computes the union of two intervals.
    ///
    /// Returns `Some(Interval)` if the union results in a single continuous interval.
    /// Returns `None` if the intervals are disjoint.
    pub fn union(&self, other: &Self) -> Option<Self> {
        let (lower, upper) = if self <= other {
            (self, other)
        } else {
            (other, self)
        };

        // Check for disjointness. True if lower.end < upper.start.
        let disjoint = match (&lower.end, &upper.start) {
            (Bound::Included(v1), Bound::Included(v2)) => v1 < v2,
            (Bound::Included(v1), Bound::Excluded(v2)) => v1 < v2,
            (Bound::Excluded(v1), Bound::Included(v2)) => v1 < v2,
            (Bound::Excluded(v1), Bound::Excluded(v2)) => v1 <= v2,
            (Bound::Unbounded, _) => false, // Unbounded end can't be disjoint from what's after.
            (_, Bound::Unbounded) => {
                // Should not happen if `lower` is correctly chosen, as it has a bounded start.
                // The only case is if both starts are unbounded, then they are not disjoint.
                false
            }
        };

        if disjoint {
            return None;
        }

        // They overlap or are adjacent. Compute union.
        // Start bound of the union is the start bound of the lower interval.
        let new_start = lower.start.clone();

        // End bound of the union is the maximum of the two end bounds.
        let new_end = match (&lower.end, &upper.end) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
            (Bound::Included(v1), Bound::Included(v2)) => {
                Bound::Included(v1.max(v2).clone())
            }
            (Bound::Excluded(v1), Bound::Excluded(v2)) => {
                Bound::Excluded(v1.max(v2).clone())
            }
            (Bound::Included(v1), Bound::Excluded(v2)) => {
                match v1.cmp(v2) {
                    Ordering::Less => Bound::Excluded(v2.clone()),
                    // if v1 >= v2, included bound is outer bound
                    _ => Bound::Included(v1.clone()),
                }
            }
            (Bound::Excluded(v1), Bound::Included(v2)) => {
                match v1.cmp(v2) {
                    Ordering::Greater => Bound::Excluded(v1.clone()),
                    // if v2 >= v1, included bound is outer bound
                    _ => Bound::Included(v2.clone()),
                }
            }
        };

        Some(Interval::new(new_start, new_end))
    }
}

impl<T: Clone + PartialOrd> Interval<T> {
    /// Checks if the interval represents a single point (e.g., `[a, a]`).
    ///
    /// An interval is a point if its start and end bounds are inclusive and equal.
    pub fn is_point(&self) -> bool {
        match (&self.start, &self.end) {
            (Bound::Included(s), Bound::Included(e)) => s == e,
            _ => false,
        }
    }

    /// Checks if this interval completely contains another interval.
    ///
    /// Returns `true` if `other` is entirely within the bounds of `self`.
    /// An interval `[a, b]` contains `[c, d]` if `a <= c` and `d <= b`
    /// (with appropriate handling for inclusive/exclusive bounds).
    pub fn contains_interval(&self, other: &Self) -> bool
    where
        T: Ord,
    {
        let start_ok = cmp_start_bounds(&self.start, &other.start) != Ordering::Greater;
        let end_ok = cmp_end_bounds(&self.end, &other.end) != Ordering::Less;
        start_ok && end_ok
    }

    /// Removes an included interval from this interval.
    ///
    /// Returns an iterator yielding 0, 1, or 2 intervals:
    /// - 0 intervals if `other` equals `self`
    /// - 1 interval if `other` shares a boundary with `self`
    /// - 2 intervals if `other` is strictly inside `self`
    ///
    /// # Panics
    ///
    /// Panics if `other` is not fully contained within `self`.
    pub fn remove_included_interval(&self, other: &Self) -> Vec<Self>
    where
        T: Ord + Debug,
    {
        assert!(
            self.contains_interval(other),
            "Cannot remove interval {:?} from {:?}: not contained",
            other,
            self
        );

        let start_eq = cmp_start_bounds(&self.start, &other.start) == Ordering::Equal;
        let end_eq = cmp_end_bounds(&self.end, &other.end) == Ordering::Equal;

        match (start_eq, end_eq) {
            (true, true) => {
                // other == self, nothing remains
                vec![]
            }
            (true, false) => {
                // other starts at self.start, remainder is after other.end
                let new_start = flip_bound(&other.end);
                vec![Interval::new(new_start, self.end.clone())]
            }
            (false, true) => {
                // other ends at self.end, remainder is before other.start
                let new_end = flip_bound(&other.start);
                vec![Interval::new(self.start.clone(), new_end)]
            }
            (false, false) => {
                // other is strictly inside, two remainders
                let left_end = flip_bound(&other.start);
                let right_start = flip_bound(&other.end);
                vec![
                    Interval::new(self.start.clone(), left_end),
                    Interval::new(right_start, self.end.clone()),
                ]
            }
        }
    }

    /// Computes the intersection of two intervals.
    ///
    /// Returns `None` if the intervals do not overlap.
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        use std::cmp::Ordering;

        let new_start = match (&self.start, &other.start) {
            (s, Bound::Unbounded) => s.clone(),
            (Bound::Unbounded, s) => s.clone(),
            (Bound::Included(v1), Bound::Included(v2)) => {
                if v1 >= v2 {
                    Bound::Included(v1.clone())
                } else {
                    Bound::Included(v2.clone())
                }
            }
            (Bound::Excluded(v1), Bound::Excluded(v2)) => {
                if v1 >= v2 {
                    Bound::Excluded(v1.clone())
                } else {
                    Bound::Excluded(v2.clone())
                }
            }
            (Bound::Included(v1), Bound::Excluded(v2)) => match v1.partial_cmp(v2) {
                Some(Ordering::Greater) => Bound::Included(v1.clone()),
                _ => Bound::Excluded(v2.clone()),
            },
            (Bound::Excluded(v1), Bound::Included(v2)) => match v2.partial_cmp(v1) {
                Some(Ordering::Greater) => Bound::Included(v2.clone()),
                _ => Bound::Excluded(v1.clone()),
            },
        };

        let new_end = match (&self.end, &other.end) {
            (e, Bound::Unbounded) => e.clone(),
            (Bound::Unbounded, e) => e.clone(),
            (Bound::Included(v1), Bound::Included(v2)) => {
                if v1 <= v2 {
                    Bound::Included(v1.clone())
                } else {
                    Bound::Included(v2.clone())
                }
            }
            (Bound::Excluded(v1), Bound::Excluded(v2)) => {
                if v1 <= v2 {
                    Bound::Excluded(v1.clone())
                } else {
                    Bound::Excluded(v2.clone())
                }
            }
            (Bound::Included(v1), Bound::Excluded(v2)) => match v1.partial_cmp(v2) {
                Some(Ordering::Less) => Bound::Included(v1.clone()),
                _ => Bound::Excluded(v2.clone()),
            },
            (Bound::Excluded(v1), Bound::Included(v2)) => match v2.partial_cmp(v1) {
                Some(Ordering::Less) => Bound::Included(v2.clone()),
                _ => Bound::Excluded(v1.clone()),
            },
        };

        let is_valid = match (&new_start, &new_end) {
            (Bound::Included(s), Bound::Included(e)) => s <= e,
            (Bound::Included(s), Bound::Excluded(e)) => s < e,
            (Bound::Excluded(s), Bound::Included(e)) => s < e,
            (Bound::Excluded(s), Bound::Excluded(e)) => s < e,
            _ => true, // At least one bound is Unbounded, so the interval is valid.
        };

        if is_valid {
            Some(Self {
                start: new_start,
                end: new_end,
            })
        } else {
            None
        }
    }
}

impl<T> RangeBounds<T> for Interval<T> {
    fn start_bound(&self) -> Bound<&T> {
        match &self.start {
            Bound::Included(val) => Bound::Included(val),
            Bound::Excluded(val) => Bound::Excluded(val),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&T> {
        match &self.end {
            Bound::Included(val) => Bound::Included(val),
            Bound::Excluded(val) => Bound::Excluded(val),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl<T: Serializable> Serializable for Interval<T> {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self>
    where
        Self: Sized
    {
        let start = Bound::<T>::read_from(reader)?;
        let end = Bound::<T>::read_from(reader)?;
        Ok(Self { start, end })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.start.write_to(writer);
        self.end.write_to(writer);
    }
}

/// Flips the inclusivity of a bound.
///
/// - `Included(v)` becomes `Excluded(v)`
/// - `Excluded(v)` becomes `Included(v)`
/// - `Unbounded` remains `Unbounded`
fn flip_bound<T: Clone>(bound: &Bound<T>) -> Bound<T> {
    match bound {
        Bound::Included(v) => Bound::Excluded(v.clone()),
        Bound::Excluded(v) => Bound::Included(v.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Compares two start bounds for ordering.
///
/// For start bounds, `Unbounded` is less than any bounded value.
/// When values are equal, `Included` comes before `Excluded` (a smaller interval starts later).
pub fn cmp_start_bounds<T: Ord>(a: &Bound<T>, b: &Bound<T>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(v1), Bound::Included(v2)) => v1.cmp(v2),
        (Bound::Excluded(v1), Bound::Excluded(v2)) => v1.cmp(v2),
        (Bound::Included(v1), Bound::Excluded(v2)) => v1.cmp(v2).then(Ordering::Less),
        (Bound::Excluded(v1), Bound::Included(v2)) => v1.cmp(v2).then(Ordering::Greater),
    }
}

/// Compares two end bounds for ordering.
///
/// For end bounds, `Unbounded` is greater than any bounded value.
/// When values are equal, `Included` comes after `Excluded` (a larger interval ends later).
pub fn cmp_end_bounds<T: Ord>(a: &Bound<T>, b: &Bound<T>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(v1), Bound::Included(v2)) => v1.cmp(v2),
        (Bound::Excluded(v1), Bound::Excluded(v2)) => v1.cmp(v2),
        (Bound::Included(v1), Bound::Excluded(v2)) => v1.cmp(v2).then(Ordering::Greater),
        (Bound::Excluded(v1), Bound::Included(v2)) => v1.cmp(v2).then(Ordering::Less),
    }
}

impl<T: Ord> PartialOrd for Interval<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for Interval<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_start_bounds(&self.start, &other.start)
            .then_with(|| cmp_end_bounds(&other.end, &self.end))
    }
}

/// Checks if any interval from `candidates` overlaps with any interval from `filters`.
///
/// Both input vectors must be sorted by start bound and contain non-overlapping intervals.
/// Uses a sweep line algorithm for O(n + m) time complexity.
/// Returns early on the first overlap found.
pub fn has_overlapping_intervals<T: Ord + Debug>(
    filters: &[Interval<T>],
    candidates: &[Interval<T>],
) -> bool {
    if filters.is_empty() || candidates.is_empty() {
        return false;
    }

    let mut filter_idx = 0;
    let mut candidate_idx = 0;

    while filter_idx < filters.len() && candidate_idx < candidates.len() {
        let filter = &filters[filter_idx];
        let candidate = &candidates[candidate_idx];

        // Check if candidate ends before filter starts (no overlap possible)
        if ends_before_start(&candidate.end, &filter.start) {
            candidate_idx += 1;
            continue;
        }

        // Check if filter ends before candidate starts (no overlap possible)
        if ends_before_start(&filter.end, &candidate.start) {
            filter_idx += 1;
            continue;
        }

        // They overlap
        return true;
    }

    false
}

/// Returns true if `end_bound` is strictly before `start_bound` (no overlap possible).
fn ends_before_start<T: Ord>(end_bound: &Bound<T>, start_bound: &Bound<T>) -> bool {
    match (end_bound, start_bound) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(e), Bound::Included(s)) => e < s,
        (Bound::Included(e), Bound::Excluded(s)) => e <= s,
        (Bound::Excluded(e), Bound::Included(s)) => e <= s,
        (Bound::Excluded(e), Bound::Excluded(s)) => e <= s,
    }
}

/// Merges a list of intervals into a list of non-overlapping intervals.
pub fn merge_overlapping_intervals<T: Clone + Ord + Debug>(intervals: Vec<Interval<T>>) -> Vec<Interval<T>> {
    let mut sorted_intervals = intervals;
    sorted_intervals.sort();

    let mut merged: Vec<Interval<T>> = Vec::new();
    for interval in sorted_intervals {
        if let Some(last) = merged.last_mut() {
            if let Some(union) = last.union(&interval) {
                *last = union;
                continue;
            }
        }
        merged.push(interval);
    }
    merged
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_open() {
        let range = Interval::open(1, 5);
        assert_eq!(range.start_bound(), Bound::Excluded(&1));
        assert_eq!(range.end_bound(), Bound::Excluded(&5));
    }

    #[test]
    fn test_closed() {
        let range = Interval::closed(1, 5);
        assert_eq!(range.start_bound(), Bound::Included(&1));
        assert_eq!(range.end_bound(), Bound::Included(&5));
    }

    #[test]
    fn test_open_closed() {
        let range = Interval::open_closed(1, 5);
        assert_eq!(range.start_bound(), Bound::Excluded(&1));
        assert_eq!(range.end_bound(), Bound::Included(&5));
    }

    #[test]
    fn test_closed_open() {
        let range = Interval::closed_open(1, 5);
        assert_eq!(range.start_bound(), Bound::Included(&1));
        assert_eq!(range.end_bound(), Bound::Excluded(&5));
    }

    #[test]
    fn test_greater_than() {
        let range = Interval::greater_than(1);
        assert_eq!(range.start_bound(), Bound::Excluded(&1));
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_at_least() {
        let range = Interval::at_least(1);
        assert_eq!(range.start_bound(), Bound::Included(&1));
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_less_than() {
        let range = Interval::less_than(5);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Excluded(&5));
    }

    #[test]
    fn test_at_most() {
        let range = Interval::at_most(5);
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Included(&5));
    }

    #[test]
    fn test_all() {
        let range: Interval<i32> = Interval::all();
        assert_eq!(range.start_bound(), Bound::Unbounded);
        assert_eq!(range.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_usage_with_btreemap() {
        let mut map = BTreeMap::new();
        map.insert(1, "one");
        map.insert(2, "two");
        map.insert(3, "three");
        map.insert(4, "four");

        let constructors = vec![
            Interval::open(2, 4),
            Interval::closed(2, 4),
            Interval::open_closed(2, 4),
            Interval::closed_open(2, 4),
            Interval::greater_than(2),
            Interval::at_least(2),
            Interval::less_than(4),
            Interval::at_most(4),
            Interval::all(),
        ];

        let expected_results = vec![
            vec!["three"],
            vec!["two", "three", "four"],
            vec!["three", "four"],
            vec!["two", "three"],
            vec!["three", "four"],
            vec!["two", "three", "four"],
            vec!["one", "two", "three"],
            vec!["one", "two", "three", "four"],
            vec!["one", "two", "three", "four"],
        ];

        for (range, expected) in constructors.into_iter().zip(expected_results) {
            let values: Vec<_> = map.range(range).map(|(_, v)| *v).collect();
            assert_eq!(values, expected);
        }
    }

    #[test]
    fn test_is_point() {
        assert!(Interval::closed(5, 5).is_point());
        assert!(!Interval::open(5, 5).is_point());
        assert!(!Interval::closed_open(5, 5).is_point());
        assert!(!Interval::open_closed(5, 5).is_point());
        assert!(!Interval::closed(5, 6).is_point());
        let unbounded: Interval<i32> = Interval::all();
        assert!(!unbounded.is_point());
    }

    #[test]
    fn test_intersection() {
        // Overlapping intervals
        let r1 = Interval::closed(1, 5); // [1, 5]
        let r2 = Interval::closed(3, 7); // [3, 7]
        let intersection = r1.intersection(&r2).unwrap();
        assert_eq!(intersection.start_bound(), Bound::Included(&3));
        assert_eq!(intersection.end_bound(), Bound::Included(&5));

        // Non-overlapping intervals
        let r3 = Interval::closed(1, 2); // [1, 2]
        let r4 = Interval::closed(3, 4); // [3, 4]
        assert!(r3.intersection(&r4).is_none());

        // Touching intervals
        let r5 = Interval::closed_open(1, 3); // [1, 3)
        let r6 = Interval::closed(3, 5); // [3, 5]
        assert!(r5.intersection(&r6).is_none()); // empty intersection

        let r7 = Interval::closed(1, 3); // [1, 3]
        let r8 = Interval::closed(3, 5); // [3, 5]
        let intersection2 = r7.intersection(&r8).unwrap(); // point intersection [3, 3]
        assert_eq!(intersection2.start_bound(), Bound::Included(&3));
        assert_eq!(intersection2.end_bound(), Bound::Included(&3));
        assert!(intersection2.is_point());

        // One interval containing another
        let r9 = Interval::closed(1, 10); // [1, 10]
        let r10 = Interval::open(3, 7); // (3, 7)
        let intersection3 = r9.intersection(&r10).unwrap();
        assert_eq!(intersection3.start_bound(), Bound::Excluded(&3));
        assert_eq!(intersection3.end_bound(), Bound::Excluded(&7));

        // Unbounded intervals
        let r11 = Interval::at_least(5); // [5, +inf)
        let r12 = Interval::less_than(10); // (-inf, 10)
        let intersection4 = r11.intersection(&r12).unwrap(); // [5, 10)
        assert_eq!(intersection4.start_bound(), Bound::Included(&5));
        assert_eq!(intersection4.end_bound(), Bound::Excluded(&10));

        // All interval
        let r13: Interval<i32> = Interval::all();
        let r14 = Interval::open_closed(3, 8); // (3, 8]
        let intersection5 = r13.intersection(&r14).unwrap();
        assert_eq!(intersection5.start_bound(), Bound::Excluded(&3));
        assert_eq!(intersection5.end_bound(), Bound::Included(&8));
    }

    #[test]
    fn test_union() {
        // Overlapping intervals
        let r1 = Interval::closed(1, 5); // [1, 5]
        let r2 = Interval::closed(3, 7); // [3, 7]
        let union = r1.union(&r2).unwrap();
        assert_eq!(union, Interval::closed(1, 7));

        // Adjacent intervals
        let r3 = Interval::closed_open(1, 3); // [1, 3)
        let r4 = Interval::at_least(3); // [3, +inf)
        let union2 = r3.union(&r4).unwrap();
        assert_eq!(union2, Interval::at_least(1));

        // One interval containing another
        let r5 = Interval::closed(1, 10); // [1, 10]
        let r6 = Interval::open(3, 7); // (3, 7)
        let union3 = r5.union(&r6).unwrap();
        assert_eq!(union3, r5);

        // Disjoint intervals
        let r7 = Interval::at_most(2); // (-inf, 2]
        let r8 = Interval::greater_than(3); // (3, +inf)
        assert!(r7.union(&r8).is_none());

        // Disjoint with open bound
        let r9 = Interval::less_than(3); // (-inf, 3)
        let r10 = Interval::at_least(3); // [3, +inf)
        let union4 = r9.union(&r10).unwrap();
        let all: Interval<i32> = Interval::all();
        assert_eq!(union4, all);

        // Mixed bounds at same point
        let r11 = Interval::open_closed(1, 5); // (1, 5]
        let r12 = Interval::closed_open(5, 10); // [5, 10)
        let union5 = r11.union(&r12).unwrap();
        assert_eq!(union5, Interval::open(1, 10));
    }

    #[test]
    fn test_has_overlapping_intervals_empty_inputs() {
        let empty: Vec<Interval<i32>> = vec![];
        let intervals = vec![Interval::closed(1, 5)];

        assert!(!has_overlapping_intervals(&empty, &intervals));
        assert!(!has_overlapping_intervals(&intervals, &empty));
        assert!(!has_overlapping_intervals(&empty, &empty));
    }

    #[test]
    fn test_has_overlapping_intervals_no_overlap() {
        let filters = vec![Interval::closed(1, 3), Interval::closed(10, 12)];
        let candidates = vec![Interval::closed(5, 7), Interval::closed(15, 20)];

        assert!(!has_overlapping_intervals(&filters, &candidates));
    }

    #[test]
    fn test_has_overlapping_intervals_with_overlap() {
        let filters = vec![Interval::closed(1, 10)];
        let candidates = vec![
            Interval::closed(2, 4),
            Interval::closed(5, 7),
            Interval::closed(8, 9),
        ];

        assert!(has_overlapping_intervals(&filters, &candidates));
    }

    #[test]
    fn test_has_overlapping_intervals_partial_overlap() {
        let filters = vec![Interval::closed(5, 15)];
        let candidates = vec![
            Interval::closed(1, 3),   // no overlap
            Interval::closed(4, 6),   // overlaps
            Interval::closed(10, 12), // overlaps
        ];

        assert!(has_overlapping_intervals(&filters, &candidates));
    }

    #[test]
    fn test_has_overlapping_intervals_multiple_filters() {
        let filters = vec![
            Interval::closed(1, 5),
            Interval::closed(10, 15),
            Interval::closed(20, 25),
        ];
        let candidates = vec![
            Interval::closed(7, 8),   // no overlap
            Interval::closed(17, 18), // no overlap
            Interval::closed(30, 35), // no overlap
        ];

        assert!(!has_overlapping_intervals(&filters, &candidates));

        let candidates_with_overlap = vec![
            Interval::closed(7, 8),   // no overlap
            Interval::closed(12, 14), // overlaps with [10,15]
        ];

        assert!(has_overlapping_intervals(&filters, &candidates_with_overlap));
    }

    #[test]
    fn test_has_overlapping_intervals_touching_boundaries() {
        let filters = vec![Interval::closed(5, 10)];

        // Candidate ends exactly where filter starts (inclusive-inclusive: overlaps at point)
        let candidates1 = vec![Interval::closed(1, 5)];
        assert!(has_overlapping_intervals(&filters, &candidates1));

        // Candidate ends just before filter starts (exclusive end)
        let candidates2 = vec![Interval::closed_open(1, 5)];
        assert!(!has_overlapping_intervals(&filters, &candidates2));

        // Filter with exclusive start
        let filters2 = vec![Interval::open_closed(5, 10)];
        let candidates3 = vec![Interval::closed(1, 5)];
        assert!(!has_overlapping_intervals(&filters2, &candidates3));
    }

    #[test]
    fn test_has_overlapping_intervals_unbounded() {
        let filters = vec![Interval::at_least(10)]; // [10, +inf)
        let candidates = vec![
            Interval::closed(1, 5),   // no overlap
            Interval::closed(8, 12),  // overlaps
        ];

        assert!(has_overlapping_intervals(&filters, &candidates));

        let candidates_no_overlap = vec![
            Interval::closed(1, 5), // no overlap
            Interval::closed(6, 9), // no overlap
        ];

        assert!(!has_overlapping_intervals(&filters, &candidates_no_overlap));
    }

    #[test]
    fn test_has_overlapping_intervals_early_return() {
        // Test that function returns true on first overlap without checking all
        let filters = vec![Interval::closed(1, 100)];
        let candidates = vec![
            Interval::closed(5, 10),   // overlaps - should return immediately
            Interval::closed(20, 30),  // also overlaps
            Interval::closed(50, 60),  // also overlaps
        ];

        assert!(has_overlapping_intervals(&filters, &candidates));
    }

    #[test]
    fn test_merge_overlapping_intervals_empty() {
        let intervals: Vec<Interval<i32>> = vec![];
        let merged = merge_overlapping_intervals(intervals);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_merge_overlapping_intervals_single() {
        let intervals = vec![Interval::closed(1, 5)];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![Interval::closed(1, 5)]);
    }

    #[test]
    fn test_merge_overlapping_intervals_no_overlap() {
        let intervals = vec![
            Interval::closed(1, 2),
            Interval::closed(5, 6),
            Interval::closed(10, 12),
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![
            Interval::closed(1, 2),
            Interval::closed(5, 6),
            Interval::closed(10, 12),
        ]);
    }

    #[test]
    fn test_merge_overlapping_intervals_all_overlap() {
        let intervals = vec![
            Interval::closed(1, 5),
            Interval::closed(3, 7),
            Interval::closed(6, 10),
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![Interval::closed(1, 10)]);
    }

    #[test]
    fn test_merge_overlapping_intervals_unsorted_input() {
        let intervals = vec![
            Interval::closed(10, 15),
            Interval::closed(1, 3),
            Interval::closed(5, 8),
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![
            Interval::closed(1, 3),
            Interval::closed(5, 8),
            Interval::closed(10, 15),
        ]);
    }

    #[test]
    fn test_merge_overlapping_intervals_adjacent() {
        let intervals = vec![
            Interval::closed(1, 3),
            Interval::closed(3, 5),
            Interval::closed(5, 7),
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![Interval::closed(1, 7)]);
    }

    #[test]
    fn test_merge_overlapping_intervals_mixed_bounds() {
        let intervals = vec![
            Interval::closed_open(1, 3), // [1, 3)
            Interval::closed(3, 5),      // [3, 5]
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![Interval::closed(1, 5)]);
    }

    #[test]
    fn test_merge_overlapping_intervals_open_gap() {
        let intervals = vec![
            Interval::closed_open(1, 3), // [1, 3)
            Interval::open(3, 5),        // (3, 5)
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![
            Interval::closed_open(1, 3),
            Interval::open(3, 5),
        ]);
    }

    #[test]
    fn test_merge_overlapping_intervals_contained() {
        let intervals = vec![
            Interval::closed(1, 10),
            Interval::closed(3, 5),
            Interval::closed(4, 6),
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![Interval::closed(1, 10)]);
    }

    #[test]
    fn test_merge_overlapping_intervals_with_unbounded() {
        let intervals = vec![
            Interval::at_least(5),  // [5, +inf)
            Interval::closed(1, 3), // [1, 3]
            Interval::closed(3, 6), // [3, 6]
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![
            Interval::at_least(1), // [1, +inf)
        ]);
    }

    #[test]
    fn test_merge_overlapping_intervals_multiple_groups() {
        let intervals = vec![
            Interval::closed(1, 3),
            Interval::closed(2, 4),
            Interval::closed(10, 12),
            Interval::closed(11, 15),
            Interval::closed(20, 25),
        ];
        let merged = merge_overlapping_intervals(intervals);
        assert_eq!(merged, vec![
            Interval::closed(1, 4),
            Interval::closed(10, 15),
            Interval::closed(20, 25),
        ]);
    }

    #[test]
    fn test_contains_interval_basic() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(3, 7);
        assert!(outer.contains_interval(&inner));
        assert!(!inner.contains_interval(&outer));
    }

    #[test]
    fn test_contains_interval_equal() {
        let r1 = Interval::closed(1, 5);
        let r2 = Interval::closed(1, 5);
        assert!(r1.contains_interval(&r2));
        assert!(r2.contains_interval(&r1));
    }

    #[test]
    fn test_contains_interval_same_start() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(1, 5);
        assert!(outer.contains_interval(&inner));
        assert!(!inner.contains_interval(&outer));
    }

    #[test]
    fn test_contains_interval_same_end() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(5, 10);
        assert!(outer.contains_interval(&inner));
        assert!(!inner.contains_interval(&outer));
    }

    #[test]
    fn test_contains_interval_mixed_bounds() {
        let closed = Interval::closed(1, 10);
        let open = Interval::open(1, 10);
        assert!(closed.contains_interval(&open));
        assert!(!open.contains_interval(&closed));

        let open_closed = Interval::open_closed(1, 10);
        let closed_open = Interval::closed_open(1, 10);
        assert!(!open_closed.contains_interval(&closed_open));
        assert!(!closed_open.contains_interval(&open_closed));
    }

    #[test]
    fn test_contains_interval_unbounded() {
        let all: Interval<i32> = Interval::all();
        let bounded = Interval::closed(1, 10);
        assert!(all.contains_interval(&bounded));
        assert!(!bounded.contains_interval(&all));

        let at_least = Interval::at_least(5);
        let at_most = Interval::at_most(10);
        assert!(!at_least.contains_interval(&at_most));
        assert!(!at_most.contains_interval(&at_least));

        let inner = Interval::closed(5, 10);
        assert!(at_least.contains_interval(&inner));
        assert!(at_most.contains_interval(&inner));
    }

    #[test]
    fn test_contains_interval_disjoint() {
        let r1 = Interval::closed(1, 5);
        let r2 = Interval::closed(6, 10);
        assert!(!r1.contains_interval(&r2));
        assert!(!r2.contains_interval(&r1));
    }

    #[test]
    fn test_contains_interval_partial_overlap() {
        let r1 = Interval::closed(1, 7);
        let r2 = Interval::closed(5, 10);
        assert!(!r1.contains_interval(&r2));
        assert!(!r2.contains_interval(&r1));
    }

    #[test]
    fn test_contains_interval_point() {
        let outer = Interval::closed(1, 10);
        let point = Interval::closed(5, 5);
        assert!(outer.contains_interval(&point));
        assert!(!point.contains_interval(&outer));
    }

    #[test]
    fn test_contains_interval_boundary_exclusive() {
        let outer = Interval::open(1, 10);
        let at_boundary = Interval::closed(1, 5);
        assert!(!outer.contains_interval(&at_boundary));

        let inside = Interval::closed(2, 9);
        assert!(outer.contains_interval(&inside));
    }

    #[test]
    fn test_remove_included_interval_equal() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(1, 10);
        let result = outer.remove_included_interval(&inner);
        assert!(result.is_empty());
    }

    #[test]
    fn test_remove_included_interval_same_start() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(1, 5);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Interval::open_closed(5, 10));
    }

    #[test]
    fn test_remove_included_interval_same_end() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(5, 10);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Interval::closed_open(1, 5));
    }

    #[test]
    fn test_remove_included_interval_middle() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(4, 6);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Interval::closed_open(1, 4));
        assert_eq!(result[1], Interval::open_closed(6, 10));
    }

    #[test]
    fn test_remove_included_interval_open_bounds() {
        let outer = Interval::open(1, 10);
        let inner = Interval::open(3, 7);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Interval::open_closed(1, 3));
        assert_eq!(result[1], Interval::closed_open(7, 10));
    }

    #[test]
    fn test_remove_included_interval_mixed_bounds() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::open_closed(3, 7);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Interval::closed(1, 3));
        assert_eq!(result[1], Interval::open_closed(7, 10));
    }

    #[test]
    fn test_remove_included_interval_unbounded_start() {
        let outer: Interval<i32> = Interval::at_most(10);
        let inner = Interval::closed(5, 10);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Interval::less_than(5));
    }

    #[test]
    fn test_remove_included_interval_unbounded_end() {
        let outer: Interval<i32> = Interval::at_least(1);
        let inner = Interval::closed(1, 5);
        let result = outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Interval::greater_than(5));
    }

    #[test]
    fn test_remove_included_interval_from_all() {
        let outer: Interval<i32> = Interval::all();
        let inner = Interval::closed(3, 7);
        let result= outer.remove_included_interval(&inner);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], Interval::less_than(3));
        assert_eq!(result[1], Interval::greater_than(7));
    }

    #[test]
    #[should_panic(expected = "not contained")]
    fn test_remove_included_interval_not_contained() {
        let outer = Interval::closed(1, 5);
        let inner = Interval::closed(3, 10);
        let _ = outer.remove_included_interval(&inner);
    }

    #[test]
    #[should_panic(expected = "not contained")]
    fn test_remove_included_interval_disjoint() {
        let outer = Interval::closed(1, 5);
        let inner = Interval::closed(10, 15);
        let _ = outer.remove_included_interval(&inner);
    }

    #[test]
    fn test_span_overlapping() {
        let r1 = Interval::closed(1, 5);
        let r2 = Interval::closed(3, 7);
        let span = r1.span(&r2);
        assert_eq!(span, Interval::closed(1, 7));
    }

    #[test]
    fn test_span_disjoint() {
        let r1 = Interval::closed(1, 3);
        let r2 = Interval::closed(7, 10);
        let span = r1.span(&r2);
        assert_eq!(span, Interval::closed(1, 10));
    }

    #[test]
    fn test_span_adjacent() {
        let r1 = Interval::closed_open(1, 5);
        let r2 = Interval::closed(5, 10);
        let span = r1.span(&r2);
        assert_eq!(span, Interval::closed(1, 10));
    }

    #[test]
    fn test_span_contained() {
        let outer = Interval::closed(1, 10);
        let inner = Interval::closed(3, 7);
        let span = outer.span(&inner);
        assert_eq!(span, Interval::closed(1, 10));

        let span_reverse = inner.span(&outer);
        assert_eq!(span_reverse, Interval::closed(1, 10));
    }

    #[test]
    fn test_span_same_interval() {
        let r = Interval::closed(5, 10);
        let span = r.span(&r);
        assert_eq!(span, Interval::closed(5, 10));
    }

    #[test]
    fn test_span_point_intervals() {
        let p1 = Interval::closed(3, 3);
        let p2 = Interval::closed(7, 7);
        let span = p1.span(&p2);
        assert_eq!(span, Interval::closed(3, 7));
    }

    #[test]
    fn test_span_mixed_bounds() {
        // Open vs closed bounds
        let r1 = Interval::open(1, 5);
        let r2 = Interval::closed(3, 7);
        let span = r1.span(&r2);
        // Start should be open(1) since it's "smaller" than closed(3)
        // End should be closed(7) since it's "larger" than open(5)
        assert_eq!(span, Interval::open_closed(1, 7));

        let r3 = Interval::closed(1, 5);
        let r4 = Interval::open(3, 7);
        let span2 = r3.span(&r4);
        // Start should be closed(1), end should be open(7)
        assert_eq!(span2, Interval::closed_open(1, 7));
    }

    #[test]
    fn test_span_same_value_different_bounds() {
        // When start values are equal, closed is "smaller"
        let r1 = Interval::closed(5, 10);
        let r2 = Interval::open(5, 15);
        let span = r1.span(&r2);
        assert_eq!(span, Interval::closed_open(5, 15));

        // When end values are equal, closed is "larger"
        let r3 = Interval::closed(1, 10);
        let r4 = Interval::closed(5, 10);
        let span2 = r3.span(&r4);
        assert_eq!(span2, Interval::closed(1, 10));
    }

    #[test]
    fn test_span_unbounded_start() {
        let r1: Interval<i32> = Interval::at_most(5);
        let r2 = Interval::closed(3, 10);
        let span = r1.span(&r2);
        assert_eq!(span, Interval::at_most(10));
    }

    #[test]
    fn test_span_unbounded_end() {
        let r1 = Interval::closed(1, 5);
        let r2: Interval<i32> = Interval::at_least(3);
        let span = r1.span(&r2);
        assert_eq!(span.start_bound(), Bound::Included(&1));
        assert_eq!(span.end_bound(), Bound::Unbounded);
    }

    #[test]
    fn test_span_both_unbounded() {
        let r1: Interval<i32> = Interval::at_most(5);
        let r2: Interval<i32> = Interval::at_least(3);
        let span = r1.span(&r2);
        assert_eq!(span, Interval::all());
     }

    #[test]
    fn test_span_with_all() {
        let all: Interval<i32> = Interval::all();
        let r = Interval::closed(5, 10);
        let span = all.span(&r);
        assert_eq!(span, Interval::all());

        let span_reverse = r.span(&all);
        assert_eq!(span_reverse, Interval::all());
    }

    #[test]
    fn test_span_commutative() {
        let r1 = Interval::open_closed(1, 5);
        let r2 = Interval::closed_open(8, 12);
        let span1 = r1.span(&r2);
        let span2 = r2.span(&r1);
        assert_eq!(span1, span2);
    }
}
