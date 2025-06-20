use crate::storage::internal_key::{extract_record_key, extract_sequence_number};
use crate::obs::logger::{LoggerAndTracer};
use std::iter::Iterator;
use std::io::Result;
use std::sync::Arc;
use crate::event;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::{MAX_SEQUENCE_NUMBER};
    use crate::storage::iterators::ForwardIterator;
    use crate::storage::test_utils::{assert_next_entry_eq, delete_rec, put_rec};

    #[test]
    fn test_forward_iterator() {
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
    fn test_reverse_iterator() {
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
}