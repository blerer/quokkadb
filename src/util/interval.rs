use std::ops::{Bound, RangeBounds};

/// A struct representing a range with customizable bounds.
///
/// `Interval` allows you to define a range with different types of bounds:
/// - Inclusive (`[a..b]`)
/// - Exclusive (`(a..b)`)
/// - Mixed (`(a..b]` or `[a..b)`)
/// - Unbounded (`(..)`)
///
/// This struct can be used in conjunction with types like `BTreeMap` to specify range queries.
#[derive(Clone)]
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
}
