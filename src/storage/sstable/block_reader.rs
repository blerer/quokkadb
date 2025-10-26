use crate::io::byte_reader::ByteReader;
use crate::io::{varint, ZeroCopy};
use crate::storage::internal_key::{extract_operation_type, InternalKeyBound};
use crate::storage::operation::OperationType;
use crate::storage::sstable::BlockHandle;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::io::Result;
use std::iter;
use std::sync::Arc;

/// A reader for SSTable blocks that supports restart-based prefix compression.
/// It can use different entry readers depending on block type (e.g., index or data).
///
/// TODO: There are unnecessary cloning of the block data in the iterator. The prev value being only used for index blocks.
/// I postponed the removal until the full pipeline is implemented. I expect that once it is done we might be able to even do more optimizations.
#[derive(Clone)]
pub struct BlockReader<W: EntryReader + 'static> {
    block: Arc<[u8]>,
    restarts_offset: usize,
    nbr_of_restarts: usize,
    reader: W,
}

impl<W: EntryReader + 'static> BlockReader<W> {
    /// Constructs a new `BlockReader` from a block of bytes and a reader.
    pub fn new(block: Arc<[u8]>, reader: W) -> Result<Self> {

        let len = block.len();
        let nbr_of_restarts_offset = len - 4;
        let nbr_of_restarts = (&block[..]).read_u32_le(nbr_of_restarts_offset) as usize;
        let restarts_offset = nbr_of_restarts_offset - (nbr_of_restarts * 4);

        Ok(BlockReader {
            block,
            restarts_offset,
            nbr_of_restarts,
            reader,
        })
    }

    /// Reads the restart key at the given restart index.
    fn read_restart_key(&self, index: usize) -> Result<&[u8]> {
        let key_offset = self.read_key_offset(index)?;
        let data = &self.block[..self.restarts_offset];
        // We skip the first byte which is the shared prefix length because we know it is a zero byte.
        let (length, offset) = varint::read_u64(data, key_offset + 1);
        Ok(&data[offset..(offset + length as usize)])
    }

    /// Reads the byte offset of a restart entry by index.
    fn read_key_offset(&self, index: usize) -> Result<usize> {
        Ok(self.block.read_u32_le(self.restarts_offset + (index << 2)) as usize)
    }

    pub fn scan_forward_from(&self, bound: &InternalKeyBound) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {
        BlockEntryIterator::new_from_key(self, bound.as_bytes())
    }

    pub fn scan_reverse_from(&self, bound: &InternalKeyBound) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {
        ReverseBlockEntryIterator::new_from_key(self, bound.as_bytes())
    }

    pub fn scan_all_forward(&self) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {
        BlockEntryIterator::new(self)
    }

    pub fn scan_all_reverse(&self) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {
        ReverseBlockEntryIterator::new(self)
    }

    /// Performs a binary search on restart points to find a candidate start offset.
    fn binary_search_restarts(&self, internal_key: &[u8]) -> Result<Option<usize>> {

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
                        return Ok(None); // No valid restart point found
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

    /// Reads the next entry from the block, updating prefix state.
    fn read_entry<B: AsRef<[u8]>>(
        &self,
        data: &ByteReader<B>,
        prev_key: &Vec<u8>,
        prev_value: &Option<W::Output>,
    ) -> Result<(Vec<u8>, W::Output)> {

        let shared = data.read_varint_u64()? as usize;
        let mut key = Vec::with_capacity(shared);
        key.extend_from_slice(&prev_key[..shared]);
        key.extend_from_slice(data.read_length_prefixed_slice()?);

        let value = self.reader.read(data, &key, prev_value)?;
        Ok((key, value))
    }

    fn next_restart_offset(&self, restart_idx: usize) -> Result<Option<usize>> {
        Ok(if restart_idx + 1 < self.nbr_of_restarts {
            Some(self.read_key_offset(restart_idx + 1)?)
        } else {
            None
        })
    }
}

pub struct BlockEntryIterator<W: EntryReader + 'static> {
    reader: BlockReader<W>,
    restart_idx: usize,
    next_restart_offset: Option<usize>,
    data: ByteReader<Arc<[u8]>>,
    prev_key: Vec<u8>,
    prev_value: Option<W::Output>,
    first: Option<(Vec<u8>, W::Output)>,
}

impl<W: EntryReader + 'static> BlockEntryIterator<W> {
    fn new(reader: &BlockReader<W>) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {
        let next_restart_offset = reader.next_restart_offset(0)?;

        Ok(Box::new(Self {
            reader: reader.clone(),
            restart_idx: 0,
            next_restart_offset,
            data: ByteReader::new(reader.block.clone()),
            prev_key: Vec::new(),
            prev_value: None,
            first: None,
        }))
    }

    /// Creates a new iterator starting from a specific key.
    fn new_from_key(
        reader: &BlockReader<W>,
        internal_key: &[u8],
    ) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {

        let mut first = None;
        let restart_idx = reader.binary_search_restarts(internal_key)?;

        if restart_idx.is_none() {
            return Self::new(reader)
        }

        let mut restart_idx = restart_idx.unwrap();

        let restart_offset = reader.read_key_offset(restart_idx)?;
        let mut next_restart_offset = reader.next_restart_offset(restart_idx)?;

        let data = ByteReader::new(reader.block.clone());
        data.seek(restart_offset)?;

        let mut prev_key = Vec::new();
        let mut prev_value: Option<W::Output> = None;

        {
            let data = &data;
            while data.position() < reader.restarts_offset {

                let position = data.position();
                let restart = if Some(position) == next_restart_offset {
                    restart_idx += 1;
                    next_restart_offset = reader.next_restart_offset(restart_idx)?;
                    true
                } else {
                    false
                };
                let (new_key, output) = reader.read_entry(data, &prev_key, if restart { &None } else { &prev_value })?;

                match internal_key.cmp(&new_key) {
                    Ordering::Equal => {
                        data.seek(position)?; // Reset position to the start of the entry
                        break;
                    },
                    Ordering::Less => {
                        if reader.reader.is_index_reader() {
                            first = Some((prev_key.clone(), prev_value.clone().unwrap()));
                        }
                        data.seek(position)?; // Reset position to the start of the entry
                        // If we rewind the position we might need to rewind the restart_idx as well.
                        if restart {
                            restart_idx -= 1;
                            next_restart_offset = reader.next_restart_offset(restart_idx)?;
                        }
                        break

                    },
                    Ordering::Greater => {},
                };

                if reader.reader.is_index_reader() && data.position() == reader.restarts_offset  {
                    first = Some((new_key, output));
                } else {
                    prev_key = new_key;
                    prev_value = Some(output);
                }
            }
        }

        Ok(Box::new(Self {
            reader: reader.clone(),
            restart_idx,
            next_restart_offset,
            data,
            prev_key,
            prev_value,
            first,
        }))
    }


    fn read_next(&mut self) -> Result<(Vec<u8>, W::Output)> {

        let restart = if Some(self.data.position()) == self.next_restart_offset {
            self.restart_idx += 1;
            self.next_restart_offset = self.reader.next_restart_offset(self.restart_idx)?;
            true
        } else {
            false
        };

        let (key, value) = self.reader.read_entry(&self.data, &self.prev_key, if restart { &None } else { &self.prev_value})?;

        self.prev_key = key.clone();
        self.prev_value = Some(value.clone());

        Ok((key, value))
    }
}

impl<W: EntryReader + 'static> Iterator for BlockEntryIterator<W> {
    type Item = Result<(Vec<u8>, W::Output)>;

    fn next(&mut self) -> Option<Self::Item> {

        if self.first.is_some() {
            let first = self.first.take().unwrap();
            return Some(Ok(first));
        }

        if self.data.position() >= self.reader.restarts_offset {
            return None;
        }

        Some(self.read_next())
    }
}

pub struct ReverseBlockEntryIterator<W: EntryReader + 'static> {
    reader: BlockReader<W>,
    data: ByteReader<Arc<[u8]>>,
    restart_idx: usize,
    stack: Vec<Result<(Vec<u8>, W::Output)>>,
}

impl<W: EntryReader + 'static> ReverseBlockEntryIterator<W> {
    fn new(reader: &BlockReader<W>) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {

        let restart_idx = reader.nbr_of_restarts;

        Ok(Box::new(Self {
            reader: reader.clone(),
            data: ByteReader::new(reader.block.clone()),
            restart_idx,
            stack: Vec::new(),
        }))
    }

    /// Creates a new iterator starting from a specific key.
    fn new_from_key(
        reader: &BlockReader<W>,
        internal_key: &[u8],
    ) -> Result<Box<dyn Iterator<Item=Result<(Vec<u8>, W::Output)>>>> {

        let restart_idx = reader.binary_search_restarts(internal_key)?;

        if restart_idx.is_none() {
            return Ok(Box::new(iter::empty()));
        }

        let restart_idx = restart_idx.unwrap();

        let restart_offset = reader.read_key_offset(restart_idx)?;
        let next_restart_offset = reader.next_restart_offset(restart_idx)?
            .unwrap_or(reader.restarts_offset);

        let data = ByteReader::new(reader.block.clone());
        data.seek(restart_offset)?;

        let mut prev_key = Vec::new();
        let mut prev_value: Option<W::Output> = None;

        let mut stack = Vec::new();
        {
            let mut restart_idx = restart_idx;
            let mut next_restart_offset = next_restart_offset;

            while data.position() < reader.restarts_offset {

                let restart = if data.position() == next_restart_offset {
                    restart_idx += 1;
                    next_restart_offset = reader.next_restart_offset(restart_idx)?.unwrap_or(reader.restarts_offset);
                    true
                } else {
                    false
                };

                let (new_key, output) = reader.read_entry(&data, &prev_key, if restart { &None } else { &prev_value })?;

                match internal_key.cmp(&new_key) {
                    Ordering::Equal => {
                        stack.push(Ok((new_key, output)));
                        break;
                    },
                    Ordering::Less => {
                        break;
                    },
                    Ordering::Greater => {
                        // Continue searching
                    },
                };

                prev_key = new_key.clone();
                prev_value = Some(output.clone());

                stack.push(Ok((new_key, output)));
            }
        }

        Ok(Box::new(Self {
            reader: reader.clone(),
            data,
            restart_idx,
            stack,
        }))
    }

    fn fill_stack(&mut self) -> Result<()> {

        let restart_offset = self.reader.read_key_offset(self.restart_idx)?;
        let next_restart_offset = self.reader.next_restart_offset(self.restart_idx)?
                                                   .unwrap_or(self.reader.restarts_offset);

        self.data.seek(restart_offset)?;

        let mut prev_key = Vec::new();
        let mut prev_value: Option<W::Output> = None;

        while self.data.position() < next_restart_offset {
            let (new_key, output) = self.reader.read_entry(&self.data, &prev_key, &prev_value)?;

            prev_key = new_key.clone();
            prev_value = Some(output.clone());

            self.stack.push(Ok((new_key, output)));
        }

        Ok(())
    }
}

impl<W: EntryReader + 'static> Iterator for ReverseBlockEntryIterator<W> {
    type Item = Result<(Vec<u8>, W::Output)>;

    fn next(&mut self) -> Option<Self::Item> {

        if self.stack.is_empty() {
            if self.restart_idx == 0 {
                return None;
            }

            self.restart_idx -= 1;
            let rs = self.fill_stack();

            if rs.is_err() {
                self.restart_idx = 0; // Reset to prevent further attempts
                return Some(Err(rs.err().unwrap()));
            }
        }

        self.stack.pop()
    }
}

/// Trait for reading entries from a block.
pub trait EntryReader: Clone {
    type Output: Clone + Debug;

    fn is_index_reader(&self) -> bool ;

    /// Reads the next value, possibly using the previous value (for deltas).
    fn read<B: AsRef<[u8]>>(
        &self,
        data: &ByteReader<B>,
        key: &[u8],
        prev_value: &Option<Self::Output>,
    ) -> Result<Self::Output>;
}

/// Reader implementation for index blocks, using delta-encoded `BlockHandle`s.
#[derive(Clone)]
pub struct IndexEntryReader;

impl EntryReader for IndexEntryReader {
    type Output = BlockHandle;

    fn is_index_reader(&self) -> bool {
        true
    }

    fn read<B: AsRef<[u8]>>(
        &self,
        data: &ByteReader<B>,
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
}

/// Reader implementation for data blocks, returning BSON document bytes for `Put` operations.
#[derive(Clone)]
pub struct DataEntryReader;

impl EntryReader for DataEntryReader {
    type Output = Vec<u8>;

    fn is_index_reader(&self) -> bool {
        false
    }

    fn read<B: AsRef<[u8]>>(
        &self,
        data: &ByteReader<B>,
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
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use super::*;
    use crate::storage::internal_key::{encode_internal_key, encode_internal_key_range, encode_record_key, MAX_SEQUENCE_NUMBER};
    use crate::storage::sstable::block_builder::{BlockBuilder, DataEntryWriter, IndexEntryWriter};
    use crate::util::bson_utils::BsonKey;
    use bson::{doc, to_vec, Bson};
    use crate::storage::Direction;
    use crate::storage::test_utils::assert_next_entry_eq;
    use crate::util::interval::Interval;

    #[test]
    fn test_index_binary_search_restarts() {
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

        let block_data = Arc::from(builder.finish().unwrap().1);
        let block = BlockReader::new(block_data, IndexEntryReader).unwrap();

        assert_eq!(None, block.binary_search_restarts(&max(0, 10)).unwrap());
        assert_eq!(None, block.binary_search_restarts(&max(1, 1)).unwrap()); // Because the valid point could be in the previous block.
        assert_eq!(Some(0), block.binary_search_restarts(&min(1)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(2, 2)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&min(2)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(3, 3)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&min(3)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(4, 4)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&min(4)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(5, 9)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(5, 8)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(5, 7)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&max(5, 6)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&max(5, 5)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&min(5)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&max(6, 11)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&min(6)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&max(7, 11)).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&max(7, 10)).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&min(7)).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&max(8, 12)).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&min(8)).unwrap());

        assert_eq!(None, block.binary_search_restarts(&max(1, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(2, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(3, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(4, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(5, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&max(6, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(2), block.binary_search_restarts(&max(7, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(3), block.binary_search_restarts(&max(8, MAX_SEQUENCE_NUMBER)).unwrap());

        assert_eq!(Some(2), block.binary_search_restarts(&max(5, 2)).unwrap());
    }

    #[test]
    fn test_data_binary_search_restarts() {

        let mut builder = BlockBuilder::new(8, DataEntryWriter);

        // 1st restart
        builder.add(&put(1, 1), to_vec(&doc! {"id": 1, "name": "Iron Man", "year": 2008}).unwrap()).unwrap();
        builder.add(&put(2, 2), to_vec(&doc! {"id": 2, "name": "The Incredible Hulk", "year": 2008}).unwrap()).unwrap();
        builder.add(&put(3, 3), to_vec(&doc! {"id": 3, "name": "Iron Man 2", "year": 2010}).unwrap()).unwrap();
        builder.add(&put(4, 4), to_vec(&doc! {"id": 4, "name": "Thor", "year": 2011}).unwrap()).unwrap();
        builder.add(&delete(5, 9), vec!()).unwrap();
        builder.add(&put(5, 8), to_vec(&doc! {"id": 5, "name": "Captain America: The First Avenger", "year": 2011}).unwrap()).unwrap();
        builder.add(&put(5, 7), to_vec(&doc! {"id": 5, "name": "Captain Omerica: The First Avenger", "year": 2011}).unwrap()).unwrap();
        builder.add(&put(5, 6), to_vec(&doc! {"id": 5, "name": "Captan America: The First Avenger", "year": 2011}).unwrap()).unwrap();
        // 2nd restart
        builder.add(&put(5, 5), to_vec(&doc! {"id": 5, "name": "Captoin America: The First Avenger", "year": 2011}).unwrap()).unwrap();
        builder.add(&put(7, 11), to_vec(&doc! {"id": 7, "name": "The Avengers", "year": 2013}).unwrap()).unwrap();
        builder.add(&put(7, 10), to_vec(&doc! {"id": 7, "name": "The Avengers", "year": 2012}).unwrap()).unwrap();


        let block_data = Arc::from(builder.finish().unwrap().1);
        let block = BlockReader::new(block_data, IndexEntryReader).unwrap();

        assert_eq!(None, block.binary_search_restarts(&max(0, 10)).unwrap());
        assert_eq!(None, block.binary_search_restarts(&max(1, 1)).unwrap()); // Because the valid point could be in the previous block.
        assert_eq!(Some(0), block.binary_search_restarts(&min(1)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(2, 2)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&min(2)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(3, 3)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&min(3)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(4, 4)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&min(4)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(5, 9)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(5, 8)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(5, 7)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(5, 6)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(5, 5)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&min(5)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(6, 11)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&min(6)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(7, 11)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(7, 10)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&min(7)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(8, 12)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&min(8)).unwrap());

        assert_eq!(None, block.binary_search_restarts(&max(1, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(2, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(3, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(4, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(0), block.binary_search_restarts(&max(5, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(6, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(7, MAX_SEQUENCE_NUMBER)).unwrap());
        assert_eq!(Some(1), block.binary_search_restarts(&max(8, MAX_SEQUENCE_NUMBER)).unwrap());

        assert_eq!(Some(1), block.binary_search_restarts(&max(5, 2)).unwrap());
    }

    #[test]
    fn test_scan_index_with_gaps() {
        use super::*;

        let mut builder = BlockBuilder::new(3, IndexEntryWriter);

        // Use the same keys/values as in test_search_index
        let mut entries = vec![
            (put(1, 1), handle(0, 25)),
            (put(3, 2), handle(25, 30)),
            (put(5, 3), handle(55, 20)),
            (put(7, 4), handle(72, 24)),
            (delete(9, 9), handle(96, 9)),
            (put(9, 8), handle(105, 48)),
            (put(9, 7), handle(153, 39)),
            (put(9, 6), handle(192, 48)),
            (put(9, 5), handle(240, 26)),
            (put(11, 11), handle(266, 53)),
            (put(11, 10), handle(319, 60)),
        ];

        for (k, v) in &entries {
            builder.add(k, *v).unwrap();
        }

        let block_data = Arc::from(builder.finish().unwrap().1);
        let block = BlockReader::new(block_data, IndexEntryReader).unwrap();

        // Test Forward direction
        {
            // Test for all existing keys
            {
                let mut iter = block.scan_forward_from(&inc_start(1, 1)).unwrap();
                assert_iter_eq(&mut iter, &entries);

                let mut iter = block.scan_forward_from(&inc_start(5, 3)).unwrap();
                assert_iter_eq(&mut iter, &entries[1..]); // The entry could be in the previous block.

                let mut iter = block.scan_forward_from(&inc_start(7, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries[2..]);

                let mut iter = block.scan_forward_from(&inc_start(7, 3)).unwrap();
                assert_iter_eq(&mut iter, &entries[3..]);

                let mut iter = block.scan_forward_from(&inc_start(9, 6)).unwrap();
                assert_iter_eq(&mut iter, &entries[6..]);

                let mut iter = block.scan_forward_from(&inc_start(9, 9)).unwrap();
                assert_iter_eq(&mut iter, &entries[4..]);
            }

            // Test for keys between existing entries (non-existent keys)
            {
                let mut iter = block.scan_forward_from(&inc_start(0, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries);

                let mut iter = block.scan_forward_from(&inc_start(4, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries[1..]);

                let mut iter = block.scan_forward_from(&inc_start(12, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries[10..]);
            }
        }

        // Test Reverse direction
        {
            entries.reverse(); // Reverse the entries for reverse iteration

            // Test for all existing keys
            {
                let mut iter = block.scan_reverse_from(&exc_end(1)).unwrap();
                assert_iter_eq(&mut iter, &vec!());

                let mut iter = block.scan_reverse_from(&inc_end(1)).unwrap();
                assert_iter_eq(&mut iter, &entries[10..]);

                let mut iter = block.scan_reverse_from(&exc_end(5)).unwrap();
                assert_iter_eq(&mut iter, &entries[9..]);

                let mut iter = block.scan_reverse_from(&inc_end(5)).unwrap();
                assert_iter_eq(&mut iter, &entries[8..]);

                let mut iter = block.scan_reverse_from(&exc_end(7)).unwrap();
                assert_iter_eq(&mut iter, &entries[8..]);

                let mut iter = block.scan_reverse_from(&inc_end(7)).unwrap();
                assert_iter_eq(&mut iter, &entries[7..]);

                let mut iter = block.scan_reverse_from(&exc_end(9)).unwrap();
                assert_iter_eq(&mut iter, &entries[7..]);

                let mut iter = block.scan_reverse_from(&inc_end(9)).unwrap();
                assert_iter_eq(&mut iter, &entries[2..]);
            }

            // Test for keys between existing entries (non-existent keys)
            {
                let mut iter = block.scan_reverse_from(&exc_end(0)).unwrap();
                assert_iter_eq(&mut iter, &vec!());

                let mut iter = block.scan_reverse_from(&exc_end(4)).unwrap();
                assert_iter_eq(&mut iter, &entries[9..]);

                let mut iter = block.scan_reverse_from(&inc_end(4)).unwrap();
                assert_iter_eq(&mut iter, &entries[9..]);

                let mut iter = block.scan_reverse_from(&exc_end(12)).unwrap();
                assert_iter_eq(&mut iter, &entries);

                let mut iter = block.scan_reverse_from(&inc_end(12)).unwrap();
                assert_iter_eq(&mut iter, &entries);
            }
        }
    }

    #[test]
    fn test_scan_index_with_a_single_block() {

        let mut builder = BlockBuilder::new(3, IndexEntryWriter);

        // Use the same keys/values as in test_search_index
        let key = put(1, 1);
        let value = handle(0, 25);

        builder.add(&key, value).unwrap();

        let block_data = Arc::from(builder.finish().unwrap().1);
        let block = BlockReader::new(block_data, IndexEntryReader).unwrap();

        let mut iter = block.scan_forward_from(&inc_start(1, MAX_SEQUENCE_NUMBER)).unwrap();
        assert_iter_eq(&mut iter, &vec!((key.clone(), value.clone())));

        let mut iter = block.scan_forward_from(&inc_start(2, MAX_SEQUENCE_NUMBER)).unwrap();
        assert_iter_eq(&mut iter, &vec!((key.clone(), value.clone())));
    }

    #[test]
    fn test_scan_data_with_gaps() {
        use super::*;
        use bson::{doc, to_vec};

        let mut builder = BlockBuilder::new(3, DataEntryWriter);

        // Prepare entries: (key, value)
        let mut entries = vec![
            (put(1, 1), to_vec(&doc! {"id": 1, "name": "Iron Man"}).unwrap()),
            (put(3, 2), to_vec(&doc! {"id": 3, "name": "Iron Man 2"}).unwrap()),
            (put(5, 3), to_vec(&doc! {"id": 5, "name": "Captain America"}).unwrap()),
            (put(7, 4), to_vec(&doc! {"id": 7, "name": "Thor"}).unwrap()),
            (delete(9, 9), Vec::new()),
            (put(9, 8), to_vec(&doc! {"id": 9, "name": "Hulk"}).unwrap()),
            (put(9, 7), to_vec(&doc! {"id": 9, "name": "Hawkeye"}).unwrap()),
            (put(9, 6), to_vec(&doc! {"id": 9, "name": "Black Widow"}).unwrap()),
            (put(9, 5), to_vec(&doc! {"id": 9, "name": "Vision"}).unwrap()),
            (put(11, 11), to_vec(&doc! {"id": 11, "name": "Loki"}).unwrap()),
            (put(11, 10), to_vec(&doc! {"id": 11, "name": "Ultron"}).unwrap()),
        ];

        for (k, v) in &entries {
            builder.add(k, v.clone()).unwrap();
        }

        let block_data = Arc::from(builder.finish().unwrap().1);
        let block = BlockReader::new(block_data, DataEntryReader).unwrap();

        // Test Forward direction
        {
            // Test for all existing keys
            {
                let mut iter = block.scan_forward_from(&inc_start(1, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries);

                let mut iter = block.scan_forward_from(&exc_start(1)).unwrap();
                assert_iter_eq(&mut iter, &entries[1..]);

                let mut iter = block.scan_forward_from(&inc_start(5, 3)).unwrap();
                assert_iter_eq(&mut iter, &entries[2..]);

                let mut iter = block.scan_forward_from(&exc_start(5)).unwrap();
                assert_iter_eq(&mut iter, &entries[3..]);

                let mut iter = block.scan_forward_from(&inc_start(9, 9)).unwrap();
                assert_iter_eq(&mut iter, &entries[4..]);

                let mut iter = block.scan_forward_from(&exc_start(9)).unwrap();
                assert_iter_eq(&mut iter, &entries[9..]);

                let mut iter = block.scan_forward_from(&inc_start(9, 6)).unwrap();
                assert_iter_eq(&mut iter, &entries[7..]);
            }

            // Test for keys between existing entries (non-existent keys)
            {
                let mut iter = block.scan_forward_from(&inc_start(0, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries);

                let mut iter = block.scan_forward_from(&exc_start(0)).unwrap();
                assert_iter_eq(&mut iter, &entries);

                let mut iter = block.scan_forward_from(&inc_start(4, MAX_SEQUENCE_NUMBER)).unwrap();
                assert_iter_eq(&mut iter, &entries[2..]);

                let mut iter = block.scan_forward_from(&exc_start(4)).unwrap();
                assert_iter_eq(&mut iter, &entries[2..]);

                let mut iter = block.scan_forward_from(&inc_start(12, MAX_SEQUENCE_NUMBER)).unwrap();
                assert!(iter.next().is_none());

                let mut iter = block.scan_forward_from(&exc_start(12)).unwrap();
                assert!(iter.next().is_none());
            }
        }

        // Test Reverse direction
        {
            entries.reverse(); // Reverse the entries for reverse iteration

            // Test for all existing keys
            {
                let mut iter = block.scan_reverse_from(&exc_end(1)).unwrap();
                assert_iter_eq(&mut iter, &vec!());

                let mut iter = block.scan_reverse_from(&inc_end(1)).unwrap();
                assert_iter_eq(&mut iter, &entries[10..]);

                let mut iter = block.scan_reverse_from(&exc_end(5)).unwrap();
                assert_iter_eq(&mut iter, &entries[9..]);

                let mut iter = block.scan_reverse_from(&inc_end(5)).unwrap();
                assert_iter_eq(&mut iter, &entries[8..]);

                let mut iter = block.scan_reverse_from(&exc_end(7)).unwrap();
                assert_iter_eq(&mut iter, &entries[8..]);

                let mut iter = block.scan_reverse_from(&inc_end(7)).unwrap();
                assert_iter_eq(&mut iter, &entries[7..]);

                let mut iter = block.scan_reverse_from(&exc_end(9)).unwrap();
                assert_iter_eq(&mut iter, &entries[7..]);

                let mut iter = block.scan_reverse_from(&inc_end(9)).unwrap();
                assert_iter_eq(&mut iter, &entries[2..]);
            }

            // Test for keys between existing entries (non-existent keys)
            {
                // <= 0 - seq: MAX_SEQUENCE_NUMBER
                let mut iter = block.scan_reverse_from(&inc_end(0)).unwrap();
                assert_iter_eq(&mut iter, &vec!());

                // <= 4 - seq: MAX_SEQUENCE_NUMBER
                let mut iter = block.scan_reverse_from(&exc_end(4)).unwrap();
                assert_iter_eq(&mut iter, &entries[9..]);

                // <= 12 - seq: MAX_SEQUENCE_NUMBER
                let mut iter = block.scan_reverse_from(&exc_end(12)).unwrap();
                assert_iter_eq(&mut iter, &entries);
            }
        }
    }

    fn max(id: i32, sequence_num: u64) -> Vec<u8> {
        encode_internal_key(&record_key(id), sequence_num, OperationType::MaxKey)
    }

    fn min(id: i32) -> Vec<u8> {
        encode_internal_key(&record_key(id), 0, OperationType::MinKey)
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

    fn inc_start(id: i32, sequence_num: u64) -> InternalKeyBound {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        let range = Interval::at_least(user_key);
        let range = encode_internal_key_range(32, 0, &range, sequence_num, Direction::Forward);
        range.start_bound().clone()
    }

    fn exc_start(id: i32) -> InternalKeyBound {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        let range = Interval::greater_than(user_key);
        let range = encode_internal_key_range(32, 0, &range, MAX_SEQUENCE_NUMBER, Direction::Forward);
        range.start_bound().clone()
    }

    fn inc_end(id: i32) -> InternalKeyBound {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        let range = Interval::at_most(user_key);
        let range = encode_internal_key_range(32, 0, &range, MAX_SEQUENCE_NUMBER, Direction::Reverse);
        range.end_bound().clone()
    }

    fn exc_end(id: i32) -> InternalKeyBound {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        let range = Interval::less_than(user_key);
        let range = encode_internal_key_range(32, 0, &range, MAX_SEQUENCE_NUMBER, Direction::Reverse);
        range.end_bound().clone()
    }

    fn handle(offset: u64, size: u64) -> BlockHandle {
        BlockHandle::new(offset, size)
    }

    fn assert_iter_eq<K, V, I>(iter: &mut I, expected: &[(K, V)])
    where
        K: Debug + PartialEq,
        V: Debug + PartialEq,
        I: Iterator<Item = Result<(K, V)>>,
    {
        for expected in expected.iter() {
            assert_next_entry_eq(iter, expected);
        }
        if let Some(Ok(actual)) = iter.next() {
            panic!("Iterator has extra element: {:?}", actual);
        }
        if let Some(Err(e)) = iter.next() {
            panic!("Iterator has extra error: {:?}", e);
        }
    }
}
