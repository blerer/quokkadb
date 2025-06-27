use crate::storage::internal_key::{extract_record_key, extract_sequence_number};
use crate::obs::logger::{LoggerAndTracer};
use std::iter::Iterator;
use std::io::Result;
use std::sync::Arc;
use crate::event;
use crate::storage::Direction;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

pub struct ForwardIterator<'a> {
    iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    snapshot: u64,
    previous: Option<Vec<u8>>,
}

impl <'a> ForwardIterator<'a> {
    pub fn new(iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>, snapshot: u64) -> Self {
        ForwardIterator {
            iter,
            snapshot,
            previous: None,
        }
    }
}

impl<'a> Iterator for ForwardIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {

        while let Some(entry) = self.iter.next() {
            return match entry {
                Ok((key, value)) => {
                    let seq = extract_sequence_number(&key);
                    if seq > self.snapshot {
                        continue;
                    }

                    let record_key = extract_record_key(&key);
                    if let Some(prev) = &self.previous {
                        if prev == record_key {
                            continue;
                        }
                    }
                    self.previous = Some(record_key.to_vec());

                    Some(Ok((key, value)))
                },
                Err(e) => {
                    // If there's an error, we return it
                    Some(Err(e))
                }
            }
        }
        None
    }
}

pub struct ReverseIterator<'a> {
    iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    snapshot: u64,
    previous: Option<(Vec<u8>, Vec<u8>)>,
}

impl <'a> ReverseIterator<'a> {
    pub fn new(iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>, snapshot: u64) -> Self {
        ReverseIterator {
            iter,
            snapshot,
            previous: None,
        }
    }
}

impl<'a> Iterator for ReverseIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {

        while let Some(entry) = self.iter.next() {
            return match entry {
                Ok((key, value)) => {
                    let seq = extract_sequence_number(&key);
                    if seq > self.snapshot {
                        continue;
                    }

                    if let Some((prev_key, _)) = &self.previous {
                        if extract_record_key(&prev_key) != extract_record_key(&key) {
                            // Record key changed, yield previous
                            return Some(Ok(self.previous.replace((key, value)).unwrap()));
                        }
                    }
                    self.previous = Some((key, value));
                    continue;
                },
                Err(e) => {
                    // If there's an error, we return it
                    Some(Err(e))
                }
            }
        }
        // At the end, yield the last buffered entry if any
        match self.previous.take() {
            Some((key, value)) => Some(Ok((key, value))),
            None => None,
        }
    }
}

pub struct TracingIterator<'a> {
    iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    logger: Arc<dyn LoggerAndTracer>,
    context: String,
    count: usize,
    exhausted: bool,
}

impl<'a> TracingIterator<'a> {
    pub fn new(iter: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>, logger: Arc<dyn LoggerAndTracer>, context: String) -> Self {
        TracingIterator {
            iter,
            logger,
            context,
            count: 0,
            exhausted: false,
        }
    }
}

impl<'a> Iterator for TracingIterator<'a>{
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next();
        match &item {
            Some(Ok(_)) => self.count += 1,
            Some(Err(_)) => {},
            None if !self.exhausted => {
                event!(self.logger, "{} count={}", self.context, self.count);
                self.exhausted = true;
            }
            None => {}
        }
        item
    }
}

// Item stored in the heap for MergeIterator, always represents an Ok value.
#[derive(Debug)]
struct MergeHeapItem {
    key: Vec<u8>,
    value: Vec<u8>,
    source_index: usize,
    direction: Direction,
}

impl Ord for MergeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare keys based on direction, then use source_index for tie-breaking to ensure stability.
        // BinaryHeap is a max-heap.
        // For Forward (min-heap logic): smaller key means "greater" priority.
        // For Reverse (max-heap logic): larger key means "greater" priority.
        match self.direction {
            Direction::Forward => other.key.cmp(&self.key),
            Direction::Reverse => self.key.cmp(&other.key),
        }
    }
}

impl PartialOrd for MergeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_index == other.source_index && self.direction == other.direction
    }
}

impl Eq for MergeHeapItem {}

pub struct MergeIterator<'a> {
    heap: BinaryHeap<MergeHeapItem>,
    sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>>,
    direction: Direction,
}

impl<'a> MergeIterator<'a> {
    pub fn new(
        mut sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>>,
        direction: Direction,
    ) -> Result<Self> {
        let mut heap = BinaryHeap::new();

        for i in 0..sources.len() {
            // Attempt to get the first item from each source.
            if let Some(item_result) = sources[i].next() {
                match item_result {
                    Ok((key, value)) => {
                        heap.push(MergeHeapItem {
                            key,
                            value,
                            source_index: i,
                            direction: direction.clone(),
                        });
                    }
                    Err(e) => {
                        // If an error is encountered during initialization, return it immediately.
                        return Err(e);
                    }
                }
            }
        }

        Ok(MergeIterator {
            heap,
            sources,
            direction,
        })
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Pop the smallest item (highest priority according to Ord impl) from the heap.
        let top_item = match self.heap.pop() {
            Some(item) => item,
            None => return None, // Heap is empty, iteration is done.
        };

        let source_idx = top_item.source_index;

        // Try to replenish the heap from the source iterator that `top_item` came from.
        if let Some(next_item_result) = self.sources[source_idx].next() {
            match next_item_result {
                Ok((key, value)) => {
                    // Successfully got the next item, push it to the heap.
                    self.heap.push(MergeHeapItem {
                        key,
                        value,
                        source_index: source_idx,
                        direction: self.direction.clone(),
                    });
                }
                Err(e) => {
                    // An error occurred fetching the next item from this source.
                    // Return this error immediately. The `top_item` is not yielded.
                    return Some(Err(e));
                }
            }
        }
        // If self.sources[source_idx].next() returned None, that source iterator is exhausted.
        // No error, and no new item to push.

        // If replenishment was successful or the source was exhausted (no error occurred),
        // return the `top_item`.
        Some(Ok((top_item.key, top_item.value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::MAX_SEQUENCE_NUMBER;
    use crate::storage::test_utils::{assert_next_entry_eq, delete_rec, put_rec};
    use std::io::{Error, ErrorKind};

    #[test]
    fn forward_iterator() {
        let col = 32;

        let entries = build_dataset(col);

        let mut iter = ForwardIterator::new(
            Box::new(entries.into_iter()),
            MAX_SEQUENCE_NUMBER,
        );

        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 9));
        assert_next_entry_eq(&mut iter, &put_rec(col, 7, 2, 11));
        assert!(iter.next().is_none());

        let entries = build_dataset(col);

        let mut iter = ForwardIterator::new(
            Box::new(entries.into_iter()),
            10,
        );

        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 9));
        assert_next_entry_eq(&mut iter, &put_rec(col, 7, 1, 10));
        assert!(iter.next().is_none());

        let entries = build_dataset(col);

        let mut iter = ForwardIterator::new(
            Box::new(entries.into_iter()),
            6,
        );

        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &put_rec(col, 5, 2, 6));
        assert!(iter.next().is_none());
    }

    #[test]
    fn reverse_iterator() {
        let col = 32;

        let entries = build_dataset(col);

        let mut iter = ReverseIterator::new(
            Box::new(entries.into_iter().rev()),
            MAX_SEQUENCE_NUMBER,
        );

        assert_next_entry_eq(&mut iter, &put_rec(col, 7, 2, 11));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 9));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert!(iter.next().is_none());

        let entries = build_dataset(col);

        let mut iter = ReverseIterator::new(
            Box::new(entries.into_iter().rev()),
            10,
        );

        assert_next_entry_eq(&mut iter, &put_rec(col, 7, 1, 10));
        assert_next_entry_eq(&mut iter, &delete_rec(col, 5, 9));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert!(iter.next().is_none());

        let entries = build_dataset(col);

        let mut iter = ReverseIterator::new(
            Box::new(entries.into_iter().rev()),
            6,
        );

        assert_next_entry_eq(&mut iter, &put_rec(col, 5, 2, 6));
        assert_next_entry_eq(&mut iter, &put_rec(col, 4, 1, 4));
        assert_next_entry_eq(&mut iter, &put_rec(col, 3, 1, 3));
        assert_next_entry_eq(&mut iter, &put_rec(col, 2, 1, 2));
        assert_next_entry_eq(&mut iter, &put_rec(col, 1, 1, 1));
        assert!(iter.next().is_none());
    }

    fn build_dataset(col: u32) -> Vec<Result<(Vec<u8>, Vec<u8>)>> {
        vec![
            Ok(put_rec(col, 1, 1, 1)),
            Ok(put_rec(col, 2, 1, 2)),
            Ok(put_rec(col, 3, 1, 3)),
            Ok(put_rec(col, 4, 1, 4)),
            Ok(delete_rec(col, 5, 9)),
            Ok(put_rec(col, 5, 4, 8)),
            Ok(put_rec(col, 5, 3, 7)),
            Ok(put_rec(col, 5, 2, 6)),
            Ok(put_rec(col, 5, 1, 5)),
            Ok(put_rec(col, 7, 2, 11)),
            Ok(put_rec(col, 7, 1, 10)),
        ]
    }

    #[test]
    fn merge_iterator_forward() {
        let col = 10;
        let iter1_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 1, 2, 2)), // user_key=1, version=2, seq=2
            Ok(put_rec(col, 3, 1, 1)), // user_key=3, version=1, seq=1
        ];
        let iter2_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 1, 1, 1)), // user_key=1, version=1, seq=1
            Ok(put_rec(col, 2, 1, 1)), // user_key=2, version=1, seq=1
        ];

        let sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(iter1_items.into_iter()),
            Box::new(iter2_items.into_iter()),
        ];

        let mut merged_iter = MergeIterator::new(sources, Direction::Forward).unwrap();

        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 1, 2, 2));
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 2, 1, 1));
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 3, 1, 1));
        assert!(merged_iter.next().is_none());
    }

    #[test]
    fn merge_iterator_empty_sources() {
        let sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(Vec::new().into_iter()),
            Box::new(Vec::new().into_iter()),
        ];
        let mut merged_iter = MergeIterator::new(sources, Direction::Forward).unwrap();
        assert!(merged_iter.next().is_none());
    }

    #[test]
    fn merge_iterator_one_empty_source() {
        let col = 10;
        let iter1_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 1, 1, 1)), // user_key=1, version=1, seq=1
        ];
        let sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(iter1_items.into_iter()),
            Box::new(Vec::new().into_iter()),
        ];

        let mut merged_iter = MergeIterator::new(sources, Direction::Forward).unwrap();
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 1, 1, 1));
        assert!(merged_iter.next().is_none());
    }

    #[test]
    fn merge_iterator_error_on_init() {
        let col = 10;
        let err1_msg = "init error iter1";
        let iter1_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![Err(Error::new(ErrorKind::Other, err1_msg))];
        let iter2_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 1, 1, 1)),
        ];

        // Error from the first iterator (iter1_items)
        let sources_err_first: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(iter1_items.into_iter()),
            Box::new(iter2_items.into_iter()), // This one won't be reached if the first errors
        ];
        
        match MergeIterator::new(sources_err_first, Direction::Forward) {
            Err(e) => assert_eq!(e.to_string(), err1_msg),
            Ok(_) => panic!("Expected MergeIterator::new to return Err for init error from first iterator"),
        }

        // Test with error from a later iterator during initialization
        let iter3_items_ok: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 2, 1, 1)), 
        ];
        let err2_msg = "init error iter4";
        let iter4_items_err: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![Err(Error::new(ErrorKind::Other, err2_msg))];

        let sources_err_later: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(iter3_items_ok.into_iter()), // This one will be successfully processed by new()
            Box::new(iter4_items_err.into_iter()), // This one will cause new() to error out
        ];

        match MergeIterator::new(sources_err_later, Direction::Forward) {
            Err(e) => assert_eq!(e.to_string(), err2_msg),
            Ok(_) => panic!("Expected MergeIterator::new to return Err for init error from later iterator"),
        }
    }


    #[test]
    fn merge_iterator_error_during_iteration() {
        let col = 10;
        let err1 = Error::new(ErrorKind::Other, "iter error");
        
        let iter1_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 1, 2, 2)), // item 1: user_key=1, version=2, seq=2
            Err(err1),                                         // item 2 (error)
            Ok(put_rec(col, 1, 1, 1)), // item 3: user_key=1, version=1, seq=1 (should not be reached if error stops stream)
        ];
        let iter2_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 2, 2, 2)), // item A: user_key=2, version=2, seq=2
        ];

        let sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(iter1_items.into_iter()),
            Box::new(iter2_items.into_iter()),
        ];
        // Initialization should be successful as the error is not in the first item of iter1_items.
        let mut merged_iter = MergeIterator::new(sources, Direction::Forward).unwrap();

        // Behavior:
        // 1. `new()` initializes heap with [key1_seq2_from_iter1, key2_seq2_from_iter2].
        // 2. merged_iter.next() pops key1_seq2.
        // 3. Tries to replenish from iter1, which yields Err(err1).
        // 4. This Err(err1) is returned immediately. key1_seq2 is NOT yielded.
        match merged_iter.next() {
            Some(Err(e)) => assert_eq!(e.to_string(), "iter error"),
            other => panic!("Expected Some(Err) for iter error first, got {:?}", other),
        }

        // After the error from iter1, the heap should still contain key2_seq2 from iter2.
        // iter1 is effectively "stuck" at the error point from MergeIterator's perspective.
        // The next call to merged_iter.next() should yield key2_seq2.
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 2, 2, 2));

        // iter2 is now exhausted. iter1 is still at error.
        assert!(merged_iter.next().is_none());
    }

    #[test]
    fn merge_iterator_reverse() {
        let col = 10;
        // Data is presented to MergeIterator already in the correct order for that source.
        // For reverse merge, source iterators should yield items in descending key order.
        let iter1_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 3, 1, 1)), // user_key=3, version=1, seq=1
            Ok(put_rec(col, 1, 2, 2)), // user_key=1, version=2, seq=2
        ];
        let iter2_items: Vec<Result<(Vec<u8>, Vec<u8>)>> = vec![
            Ok(put_rec(col, 2, 1, 1)), // user_key=2, version=1, seq=1
            Ok(put_rec(col, 1, 1, 1)), // user_key=1, version=1, seq=1
        ];

        let sources: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> = vec![
            Box::new(iter1_items.into_iter()), // Source 0
            Box::new(iter2_items.into_iter()), // Source 1
        ];

        let mut merged_iter = MergeIterator::new(sources, Direction::Reverse).unwrap();

        // Expected order for reverse merge (largest key first, then by stability if keys are same)
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 3, 1, 1));
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 2, 1, 1));
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 1, 1, 1));
        assert_next_entry_eq(&mut merged_iter, &put_rec(col, 1, 2, 2));
        assert!(merged_iter.next().is_none());
    }
}
