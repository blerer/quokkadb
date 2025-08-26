use std::cmp::Ordering;
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

impl<T> Interval<T> {
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

impl<T: Clone + Ord> Interval<T> {
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

impl<T: Ord> PartialOrd for Interval<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for Interval<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let start_cmp = match (&self.start, &other.start) {
            (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
            (Bound::Unbounded, _) => Ordering::Less,
            (_, Bound::Unbounded) => Ordering::Greater,
            (Bound::Included(v1), Bound::Included(v2)) => v1.cmp(v2),
            (Bound::Excluded(v1), Bound::Excluded(v2)) => v1.cmp(v2),
            (Bound::Included(v1), Bound::Excluded(v2)) => v1.cmp(v2).then(Ordering::Less),
            (Bound::Excluded(v1), Bound::Included(v2)) => v1.cmp(v2).then(Ordering::Greater),
        };

        start_cmp.then_with(|| {
            // reverse for end
            match (&other.end, &self.end) {
                (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
                (Bound::Unbounded, _) => Ordering::Greater,
                (_, Bound::Unbounded) => Ordering::Less,
                (Bound::Included(v1), Bound::Included(v2)) => v1.cmp(v2),
                (Bound::Excluded(v1), Bound::Excluded(v2)) => v1.cmp(v2),
                (Bound::Included(v1), Bound::Excluded(v2)) => v1.cmp(v2).then(Ordering::Greater),
                (Bound::Excluded(v1), Bound::Included(v2)) => v1.cmp(v2).then(Ordering::Less),
            }
        })
    }
}

#[cfg(test)]
mod tests {
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
}
