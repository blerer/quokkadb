use crate::io::byte_reader::ByteReader;
use crate::io::ZeroCopy;
use crate::storage::internal_key::{encode_internal_key, extract_operation_type, extract_record_key, extract_sequence_number};
use crate::storage::operation::OperationType;
use crate::storage::sstable::BlockHandle;
use std::cmp::Ordering;
use std::io::Result;

/// A reader for SSTable blocks that supports restart-based prefix compression.
/// It can use different entry readers depending on block type (e.g., index or data).
pub struct BlockReader<'a, W: EntryReader> {
    nbr_of_restarts: usize,
    restarts: &'a [u8],
    data: ByteReader<'a>,
    reader: W,
}

impl<'a, W: EntryReader> BlockReader<'a, W> {
    /// Constructs a new `BlockReader` from a block of bytes and a reader.
    pub fn new(block: &'a [u8], reader: W) -> Result<Self> {
        let len = block.len();
        let nbr_of_restarts_offset = len - 4;
        let nbr_of_restarts = block.read_u32_le(nbr_of_restarts_offset) as usize;
        let restarts_offset = nbr_of_restarts_offset - (nbr_of_restarts * 4);

        let data = ByteReader::new(&block[..restarts_offset]);
        let restarts = &block[restarts_offset..nbr_of_restarts_offset];
        Ok(BlockReader {
            nbr_of_restarts,
            restarts,
            data,
            reader,
        })
    }

    /// Reads the restart key at the given restart index.
    fn read_restart_key(&self, offset: usize) -> Result<&[u8]> {
        let key_offset = self.read_key_offset(offset)?;
        self.data.seek(key_offset + 1)?; // Skipping the shared number which is a 0 byte
        Ok(self.data.read_length_prefixed_slice()?)
    }

    /// Reads the byte offset of a restart entry by index.
    fn read_key_offset(&self, index: usize) -> Result<usize> {
        Ok(self.restarts.read_u32_le(index << 2) as usize)
    }

    /// Searches for a key in the block using restart-based binary + linear scan.
    pub fn search(&self, record_key: &[u8], snapshot: u64) -> Result<Option<(Vec<u8>, W::Output)>> {
        let restart_key_idx = self.binary_search_restarts(record_key, snapshot)?;

        if let Some(restart_key_idx) = restart_key_idx {
            self.linear_search(record_key, snapshot, &self.data, restart_key_idx)
        } else {
            Ok(None)
        }
    }

    /// Returns an iterator over the entries in the block.
    pub fn entries(&self) -> Result<BlockEntryIterator<'_, W>> {
        self.data.seek(0)?;
        BlockEntryIterator::new(&self)
    }

    /// Performs a binary search on restart points to find a candidate start offset.
    fn binary_search_restarts(&self, record_key: &[u8], snapshot: u64) -> Result<Option<usize>> {
        let internal_key = encode_internal_key(record_key, snapshot, OperationType::MaxKey);
        let mut mid = 0;
        let mut left = 0usize;
        let mut right = self.nbr_of_restarts - 1;

        while left <= right {
            mid = (left + right) >> 1;
            let mid_key = self.read_restart_key(mid)?;
            match mid_key.cmp(&internal_key) {
                Ordering::Equal => return Ok(Some(mid)),
                Ordering::Greater => {
                    if mid == 0 {
                        // If the key has the same user key it is a match otherwise there is no match.
                        return if extract_record_key(mid_key) == record_key {
                            Ok(Some(mid))
                        } else {
                            Ok(None)
                        };
                    }
                    right = mid - 1
                }
                Ordering::Less => {
                    mid += 1;
                    left = mid;
                }
            }
        }
        Ok(Some(mid - 1))
    }

    /// Performs a linear scan from a restart offset to locate the matching entry.
    fn linear_search(
        &self,
        record_key: &[u8],
        snapshot: u64,
        data: &ByteReader,
        restart_idx: usize,
    ) -> Result<Option<(Vec<u8>, W::Output)>> {

        let mut restart_idx = restart_idx;
        let restart_offset = self.read_key_offset(restart_idx)?;
        let mut next_restart_offset = self.next_restart_offset(restart_idx)?;

        data.seek(restart_offset)?;

        let mut prev_key = Vec::new();
        let mut prev_value: Option<W::Output> = None;

        let mut restart = false;

        while data.has_remaining() {

            if Some(data.position()) == next_restart_offset {
                restart = true;
                restart_idx += 1;
                next_restart_offset = self.next_restart_offset(restart_idx)?;
            } else {
                restart = false;
            }

            let mut new_key = Vec::new();
            let shared = data.read_varint_u64()?;
            new_key.extend_from_slice(&prev_key[..shared as usize]);
            new_key.extend_from_slice(data.read_length_prefixed_slice()?);

            let output = self.reader.read(data, &new_key, if restart { &None } else { &prev_value })?;

            match self.reader.compare_keys(&new_key, record_key, snapshot, !self.data.has_remaining()) {
                ComparisonResult::ReturnCurrent => { return Ok(Some((new_key, output))) },
                ComparisonResult::ReturnPrevious => { return Ok(Some((prev_key, prev_value.unwrap()))) },
                ComparisonResult::ReturnNone => { return Ok(None) },
                ComparisonResult::Continue => { },
            };

            prev_key = new_key;
            prev_value = Some(output);
        }
        Ok(None)
    }



    fn next_restart_offset(&self, restart_idx: usize) -> Result<Option<usize>> {
        Ok(if restart_idx + 1 < self.nbr_of_restarts {
            Some(self.read_key_offset(restart_idx + 1)?)
        } else {
            None
        })
    }
}

pub struct BlockEntryIterator<'a, W: EntryReader> {
    reader: &'a BlockReader<'a, W>,
    restart_idx: usize,
    next_restart_offset: Option<usize>,
    prev_key: Vec<u8>,
    prev_value: Option<W::Output>,
}

impl<'a, W: EntryReader> BlockEntryIterator<'a, W> {
    pub fn new(reader: &'a BlockReader<'a, W>) -> Result<Self> {
        let next_restart_offset = reader.next_restart_offset(0)?;
        Ok(Self {
            reader,
            restart_idx: 0,
            next_restart_offset,
            prev_key: Vec::new(),
            prev_value: None,
        })
    }

    fn read_next(&mut self) -> Result<(Vec<u8>, W::Output)> {

        let shared = self.reader.data.read_varint_u64()? as usize;
        let mut key = Vec::with_capacity(shared);
        key.extend_from_slice(&self.prev_key[..shared]);
        key.extend_from_slice(self.reader.data.read_length_prefixed_slice()?);

        let value = self.reader.reader.read(&self.reader.data, &key, &self.prev_value)?;

        self.prev_key = key.clone();
        if Some(self.reader.data.position()) == self.next_restart_offset {
            self.restart_idx += 1;
            self.prev_value = None;
            self.next_restart_offset = self.reader.next_restart_offset(self.restart_idx)?;
        } else {
            self.prev_value = Some(value.clone());
        }

        Ok((key, value))
    }
}

impl<'a, W: EntryReader> Iterator for BlockEntryIterator<'a, W> {
    type Item = Result<(Vec<u8>, W::Output)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.reader.data.has_remaining() {
            return None;
        }

        Some(self.read_next())
    }
}

enum ComparisonResult {
    ReturnCurrent,
    ReturnPrevious,
    ReturnNone,
    Continue,
}

/// Trait for reading entries from a block.
pub trait EntryReader {
    type Output: Clone;

    /// Reads the next value, possibly using the previous value (for deltas).
    fn read(
        &self,
        data: &ByteReader,
        key: &[u8],
        prev_value: &Option<Self::Output>,
    ) -> Result<Self::Output>;

    fn compare_keys(
        &self,
        new_key: &[u8],
        record_key: &[u8],
        snapshot: u64,
        is_last: bool
    ) -> ComparisonResult;
}

/// Reader implementation for index blocks, using delta-encoded `BlockHandle`s.
pub struct IndexEntryReader;

impl EntryReader for IndexEntryReader {
    type Output = BlockHandle;

    fn read(
        &self,
        data: &ByteReader,
        _key: &[u8],
        prev_value: &Option<Self::Output>,
    ) -> Result<Self::Output> {
        Ok(if let Some(handle) = prev_value {
            let delta = data.read_varint_i64()?;
            BlockHandle {
                offset: handle.offset + handle.size,
                size: ((handle.size as i64) + delta) as u64,
            }
        } else {
            let offset = data.read_varint_u64()?;
            let size = data.read_varint_u64()?;
            BlockHandle { offset, size }
        })
    }

    fn compare_keys(&self, new_key: &[u8], record_key: &[u8], snapshot: u64, is_last: bool) -> ComparisonResult {
        match extract_record_key(&new_key).cmp(record_key) {
            Ordering::Less => {
                if is_last {
                    // We reached the end of this index blocks without finding a match so we know that the last key is within this block
                    ComparisonResult::ReturnCurrent
                } else {
                    ComparisonResult::Continue
                }
            },
            Ordering::Equal => {
                if extract_sequence_number(&new_key) <= snapshot {
                    ComparisonResult::ReturnCurrent
                } else {
                    ComparisonResult::Continue
                }
            },
            Ordering::Greater => ComparisonResult::ReturnPrevious
        }
    }
}

/// Reader implementation for data blocks, returning BSON document bytes for `Put` operations.
pub struct DataEntryReader;

impl EntryReader for DataEntryReader {
    type Output = Vec<u8>;

    fn read(
        &self,
        data: &ByteReader,
        key: &[u8],
        _prev_value: &Option<Self::Output>,
    ) -> Result<Self::Output> {
        let op = extract_operation_type(key);

        if OperationType::Put == op {
            // Read the document length (first 4 bytes) without moving the underlying cursor
            let doc_length = data.peek_i32_le()? as usize;
            data.read_fixed_slice(doc_length)
                .map(|bytes| bytes.to_vec())
        } else {
            Ok(Vec::new())
        }
    }

    fn compare_keys(&self, new_key: &[u8], record_key: &[u8], snapshot: u64, is_last: bool) -> ComparisonResult {
        match extract_record_key(&new_key).cmp(record_key) {
            Ordering::Less => ComparisonResult::Continue, // Do nothing
            Ordering::Equal => {
                if extract_sequence_number(&new_key) <= snapshot {
                    ComparisonResult::ReturnCurrent
                } else {
                    ComparisonResult::Continue
                }
            },
            Ordering::Greater => ComparisonResult::ReturnNone
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::internal_key::{
        encode_internal_key, encode_record_key, MAX_SEQUENCE_NUMBER,
    };
    use crate::storage::sstable::block_builder::{BlockBuilder, DataEntryWriter, IndexEntryWriter};
    use crate::util::bson_utils::BsonKey;
    use bson::{doc, to_vec, Bson};

    #[test]
    fn test_binary_search_restarts() {
        // Build the index block with a restart interval of 3
        let mut builder = BlockBuilder::new(3, IndexEntryWriter);
        // 1st restart
        builder.add(&put(1, 1), handle(0, 25)).unwrap();
        builder.add(&put(2, 2), handle(25, 30)).unwrap();
        builder.add(&put(3, 3), handle(55, 20)).unwrap();
        // 2nd restart
        builder.add(&put(4, 4), handle(72, 24)).unwrap();
        builder.add(&delete(5, 9), handle(96, 9)).unwrap();
        builder.add(&put(5, 8), handle(105, 48)).unwrap();
        // 3rd restart
        builder.add(&put(5, 7), handle(153, 39)).unwrap();
        builder.add(&put(5, 6), handle(192, 48)).unwrap();
        builder.add(&put(5, 5), handle(240, 26)).unwrap();
        // 4th restart
        builder.add(&put(7, 11), handle(266, 53)).unwrap();
        builder.add(&put(7, 10), handle(319, 60)).unwrap();

        let block_data = builder.finish().unwrap().1;
        let block = BlockReader::new(&block_data, IndexEntryReader).unwrap();

        assert_eq!(None, block.binary_search_restarts(&record_key(0), 1).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(1), 1).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(2), 2).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(3), 3).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(4), 4).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), 9).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), 8).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), 7).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(5), 6).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(5), 5).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(6), 11).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(7), 11).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&record_key(7), 10).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&record_key(8), 12).unwrap());

        assert_eq!(Some(0), block.binary_search_restarts(&record_key(1), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(2), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(3), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&record_key(4), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&record_key(5), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(6), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(7), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&record_key(7), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&record_key(8), MAX_SEQUENCE_NUMBER).unwrap());

        assert_eq!(Some(2), block.binary_search_restarts(&record_key(5), 2).unwrap());
    }

    #[test]
    fn test_search_index() {
        let mut builder = BlockBuilder::new(3, IndexEntryWriter);

        let key_1_1 = put(1, 1);
        let value_1_1 = handle(0, 25);

        let key_3_2 = put(3, 2);
        let value_3_2 = handle(25, 30);

        let key_5_3 = put(5, 3);
        let value_5_3 = handle(55, 20);

        let key_7_4 = put(7, 4);
        let value_7_4 = handle(72, 24);

        let key_9_9 = delete(9, 9);
        let value_9_9 = handle(96, 9);

        let key_9_8 = put(9, 8);
        let value_9_8 = handle(105, 48);

        let key_9_7 = put(9, 7);
        let value_9_7 = handle(153, 39);

        let key_9_6 = put(9, 6);
        let value_9_6 = handle(192, 48);

        let key_9_5 = put(9, 5);
        let value_9_5 = handle(240, 26);

        let key_11_11 = put(11, 11);
        let value_11_11 = handle(266, 53);

        let key_11_10 = put(11, 10);
        let value_11_10 = handle(319, 60);

        // Now adding them to the builder
        builder.add(&key_1_1, value_1_1).unwrap();
        builder.add(&key_3_2, value_3_2).unwrap();
        builder.add(&key_5_3, value_5_3).unwrap();
        builder.add(&key_7_4, value_7_4).unwrap();
        builder.add(&key_9_9, value_9_9).unwrap();
        builder.add(&key_9_8, value_9_8).unwrap();
        builder.add(&key_9_7, value_9_7).unwrap();
        builder.add(&key_9_6, value_9_6).unwrap();
        builder.add(&key_9_5, value_9_5).unwrap();
        builder.add(&key_11_11, value_11_11).unwrap();
        builder.add(&key_11_10, value_11_10).unwrap();

        let block_data = builder.finish().unwrap().1;
        let block = BlockReader::new(&block_data, IndexEntryReader).unwrap();

        assert_eq!(None, block.search(&record_key(0), 1).unwrap());
        assert_eq!(Some((key_1_1.clone(), value_1_1)), block.search(&record_key(1), 1).unwrap());
        assert_eq!(Some((key_1_1.clone(), value_1_1)), block.search(&record_key(2), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_3_2.clone(), value_3_2)), block.search(&record_key(3), 2).unwrap());
        assert_eq!(Some((key_3_2.clone(), value_3_2)), block.search(&record_key(3), 3).unwrap());
        assert_eq!(Some((key_3_2.clone(), value_3_2)), block.search(&record_key(3), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_3_2.clone(), value_3_2)), block.search(&record_key(3), 1).unwrap());
        assert_eq!(Some((key_5_3.clone(), value_5_3)), block.search(&record_key(5), 3).unwrap());
        assert_eq!(Some((key_7_4.clone(), value_7_4)), block.search(&record_key(7), 4).unwrap());
        assert_eq!(Some((key_7_4.clone(), value_7_4)), block.search(&record_key(7), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_9_9.clone(), value_9_9)), block.search(&record_key(9), 9).unwrap());
        assert_eq!(Some((key_9_9.clone(), value_9_9)), block.search(&record_key(9), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_9_8.clone(), value_9_8)), block.search(&record_key(9), 8).unwrap());
        assert_eq!(Some((key_9_7.clone(), value_9_7)), block.search(&record_key(9), 7).unwrap());
        assert_eq!(Some((key_9_6.clone(), value_9_6)), block.search(&record_key(9), 6).unwrap());
        assert_eq!(Some((key_9_5.clone(), value_9_5)), block.search(&record_key(9), 5).unwrap());
        assert_eq!(Some((key_11_11.clone(), value_11_11)), block.search(&record_key(11), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_11_11.clone(), value_11_11)), block.search(&record_key(11), 11).unwrap());
        assert_eq!(Some((key_11_10.clone(), value_11_10)), block.search(&record_key(11), 10).unwrap());
        assert_eq!(Some((key_11_10.clone(), value_11_10)), block.search(&record_key(12), 12).unwrap());

        let mut entries = block.entries().unwrap();

        assert_eq!((key_1_1.clone(), value_1_1), entries.next().unwrap().unwrap());
        assert_eq!((key_3_2.clone(), value_3_2), entries.next().unwrap().unwrap());
        assert_eq!((key_5_3.clone(), value_5_3), entries.next().unwrap().unwrap());
        assert_eq!((key_7_4.clone(), value_7_4), entries.next().unwrap().unwrap());
        assert_eq!((key_9_9.clone(), value_9_9), entries.next().unwrap().unwrap());
        assert_eq!((key_9_8.clone(), value_9_8), entries.next().unwrap().unwrap());
        assert_eq!((key_9_7.clone(), value_9_7), entries.next().unwrap().unwrap());
        assert_eq!((key_9_6.clone(), value_9_6), entries.next().unwrap().unwrap());
        assert_eq!((key_9_5.clone(), value_9_5), entries.next().unwrap().unwrap());
        assert_eq!((key_11_11.clone(), value_11_11), entries.next().unwrap().unwrap());
        assert_eq!((key_11_10.clone(), value_11_10), entries.next().unwrap().unwrap());
        assert!(entries.next().is_none());
    }

    #[test]
    fn test_search_data() {
        let mut builder = BlockBuilder::new(3, DataEntryWriter);

        let key_1_1 = put(1, 1);
        let value_1_1 = to_vec(&doc! {"id": 1, "name": "Iron Man", "year": 2008}).unwrap();

        let key_2_2 = put(2, 2);
        let value_2_2 = to_vec(&doc! {"id": 2, "name": "The Incredible Hulk", "year": 2008}).unwrap();

        let key_3_3 = put(3, 3);
        let value_3_3 = to_vec(&doc! {"id": 3, "name": "Iron Man 2", "year": 2010}).unwrap();

        let key_4_4 = put(4, 4);
        let value_4_4 = to_vec(&doc! {"id": 4, "name": "Thor", "year": 2011}).unwrap();

        let key_5_9 = delete(5, 9);
        let value_5_9 = Vec::new();

        let key_5_8 = put(5, 8);
        let value_5_8 = to_vec(&doc! {"id": 5, "name": "Captain America: The First Avenger", "year": 2011}).unwrap();

        let key_5_7 = put(5, 7);
        let value_5_7 = to_vec(&doc! {"id": 5, "name": "Captain Omerica: The First Avenger", "year": 2011}).unwrap();

        let key_5_6 = put(5, 6);
        let value_5_6 = to_vec(&doc! {"id": 5, "name": "Captan America: The First Avenger", "year": 2011}).unwrap();

        let key_5_5 = put(5, 5);
        let value_5_5 = to_vec(&doc! {"id": 5, "name": "Captoin America: The First Avenger", "year": 2011}).unwrap();

        let key_7_11 = put(7, 11);
        let value_7_11 = to_vec(&doc! {"id": 7, "name": "The Avenger", "year": 2012}).unwrap();

        let key_7_10 = put(7, 10);
        let value_7_10 = to_vec(&doc! {"id": 7, "name": "The Avenger", "year": 2013}).unwrap();

        // Add to builder
        builder.add(&key_1_1, value_1_1.clone()).unwrap();
        builder.add(&key_2_2, value_2_2.clone()).unwrap();
        builder.add(&key_3_3, value_3_3.clone()).unwrap();
        builder.add(&key_4_4, value_4_4.clone()).unwrap();
        builder.add(&key_5_9, value_5_9.clone()).unwrap();
        builder.add(&key_5_8, value_5_8.clone()).unwrap();
        builder.add(&key_5_7, value_5_7.clone()).unwrap();
        builder.add(&key_5_6, value_5_6.clone()).unwrap();
        builder.add(&key_5_5, value_5_5.clone()).unwrap();
        builder.add(&key_7_11, value_7_11.clone()).unwrap();
        builder.add(&key_7_10, value_7_10.clone()).unwrap();

        let block_data = builder.finish().unwrap().1;
        let block = BlockReader::new(&block_data, DataEntryReader).unwrap();

        assert_eq!(None, block.search(&record_key(0), 1).unwrap());
        assert_eq!(Some((key_1_1.clone(), value_1_1.clone())), block.search(&record_key(1), 1).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2.clone())), block.search(&record_key(2), 2).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2.clone())), block.search(&record_key(2), 3).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2.clone())), block.search(&record_key(2), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(None, block.search(&record_key(2), 1).unwrap());
        assert_eq!(Some((key_3_3.clone(), value_3_3.clone())), block.search(&record_key(3), 3).unwrap());
        assert_eq!(Some((key_4_4.clone(), value_4_4.clone())), block.search(&record_key(4), 4).unwrap());
        assert_eq!(Some((key_4_4.clone(), value_4_4.clone())), block.search(&record_key(4), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_5_9.clone(), value_5_9.clone())), block.search(&record_key(5), 9).unwrap());
        assert_eq!(Some((key_5_9.clone(), value_5_9.clone())), block.search(&record_key(5), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_5_8.clone(), value_5_8.clone())), block.search(&record_key(5), 8).unwrap());
        assert_eq!(Some((key_5_7.clone(), value_5_7.clone())), block.search(&record_key(5), 7).unwrap());
        assert_eq!(Some((key_5_6.clone(), value_5_6.clone())), block.search(&record_key(5), 6).unwrap());
        assert_eq!(Some((key_5_5.clone(), value_5_5.clone())), block.search(&record_key(5), 5).unwrap());
        assert_eq!(Some((key_7_11.clone(), value_7_11.clone())), block.search(&record_key(7), MAX_SEQUENCE_NUMBER).unwrap());
        assert_eq!(Some((key_7_11.clone(), value_7_11.clone())), block.search(&record_key(7), 11).unwrap());
        assert_eq!(Some((key_7_10.clone(), value_7_10.clone())), block.search(&record_key(7), 10).unwrap());
        assert_eq!(None, block.search(&record_key(8), 12).unwrap());


    }

    fn internal_key(id: i32, sequence_num: u64, op: OperationType) -> Vec<u8> {
        encode_internal_key(&record_key(id), sequence_num, op)
    }

    fn record_key(id: i32) -> Vec<u8> {
        let user_key = &Bson::Int32(id).try_into_key().unwrap();
        encode_record_key(32, 0, &user_key)
    }

    fn put(id: i32, sequence_num: u64) -> Vec<u8> {
        internal_key(id, sequence_num, OperationType::Put)
    }
    fn delete(id: i32, sequence_num: u64) -> Vec<u8> {
        internal_key(id, sequence_num, OperationType::Delete)
    }

    fn handle(offset: u64, size: u64) -> BlockHandle {
        BlockHandle::new(offset, size)
    }
}
