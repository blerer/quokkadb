use std::cmp::Ordering;
use std::io::Result;
use crate::io::byte_reader::ByteReader;
use crate::io::ZeroCopy;
use crate::storage::internal_key::{extract_operation_type, extract_record_key, extract_sequence_number, extract_user_key};
use crate::storage::operation::OperationType;
use crate::storage::sstable::BlockHandle;

/// A reader for SSTable blocks that supports restart-based prefix compression.
/// It can use different entry readers depending on block type (e.g., index or data).
struct BlockReader<'a, W: EntryReader> {
    nbr_of_restarts: usize,
    restart_interval: usize,
    restarts: &'a [u8],
    data: ByteReader<'a>,
    reader: W,
}

impl <'a, W: EntryReader> BlockReader<'a, W> {

    /// Constructs a new `BlockReader` from a block of bytes and a reader.
    fn new(restart_interval: usize, block: &'a [u8], reader: W) -> Result<Self> {
        let len = block.len();
        let nbr_of_restarts_offset = len - 4;
        let nbr_of_restarts = block.read_u32_le(nbr_of_restarts_offset) as usize;
        println!("nbr_of_restarts {}", nbr_of_restarts);
        let restarts_offset = nbr_of_restarts_offset - (nbr_of_restarts * 4);

        let data = ByteReader::new(&block[..restarts_offset]);
        let restarts = &block[restarts_offset..nbr_of_restarts_offset];
        Ok(BlockReader { nbr_of_restarts, restart_interval, restarts, data, reader })
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
    pub fn search(&self, key: &[u8]) -> Result<Option<(Vec<u8>, W::Output)>> {

        let restart_key_offset = self.binary_search_restarts(key)?;

        if let Some(offset) = restart_key_offset {
            self.linear_search(key, &self.data, offset, self.restart_interval)
        } else {
            Ok(None)
        }
    }
    /// Performs a binary search on restart points to find a candidate start offset.
    fn binary_search_restarts(&self, key: &[u8]) -> Result<Option<usize>> {
        let mut mid = 0;
        let mut left = 0usize;
        let mut right = self.nbr_of_restarts - 1;

        while left <= right {
            mid = (left + right) >> 1;
            let mid_key = self.read_restart_key(mid)?;
            match mid_key.cmp(key) {
                Ordering::Equal => return Ok(Some(self.read_key_offset(mid)?)),
                Ordering::Greater => {
                    if mid == 0 {
                        // If the key has the same user key it is a match otherwise there is no match.
                        return if extract_user_key(mid_key) == extract_user_key(key) {
                            Ok(Some(self.read_key_offset(mid)?))
                        } else {
                            Ok(None)
                        }
                    }
                    right = mid - 1
                },
                Ordering::Less => {
                    mid += 1;
                    left = mid;
                }
            }
        }
        Ok(Some(self.read_key_offset(mid - 1)?))
    }

    /// Performs a linear scan from a restart offset to locate the matching entry.
    fn linear_search(&self,
                     key: &[u8],
                     data: &ByteReader,
                     restart_offset: usize,
                     restart_interval: usize
    ) -> Result<Option<(Vec<u8>, W::Output)>> {

        let mut count = 0;
        data.seek(restart_offset)?;

        let record_key = extract_record_key(&key);
        let mut prev_key = Vec::new();
        let mut prev_value: Option<W::Output> = None;

        while data.has_remaining() {

            let mut new_key = Vec::new();
            let shared = data.read_varint_u64()?;
            new_key.extend_from_slice(&prev_key[..shared as usize]);
            new_key.extend_from_slice(data.read_length_prefixed_slice()?);

            let output = self.reader.read(data, &new_key, prev_value)?;

            match extract_record_key(&new_key).cmp(record_key) {
                Ordering::Less => { }, // Do nothing
                Ordering::Equal => {
                    if extract_sequence_number(&new_key) <= extract_sequence_number(&key) {
                        return Ok(Some((new_key, output)))
                    }
                },
                Ordering::Greater => return Ok(None)
            };

            prev_key = new_key;
            count = count + 1;
            if count == restart_interval {
                count = 0;
                prev_value = None;
            } else {
                prev_value = Some(output);
            }
        }
        Ok(None)
    }
}

/// Trait for reading entries from a block.
pub trait EntryReader {
    type Output;

    /// Reads the next value, possibly using the previous value (for deltas).
    fn read(&self, data: &ByteReader, key: &[u8], prev_value: Option<Self::Output>) -> Result<Self::Output>;
}

/// Reader implementation for index blocks, using delta-encoded `BlockHandle`s.
pub struct IndexEntryReader;

impl EntryReader for IndexEntryReader {
    type Output = BlockHandle;

    fn read(&self, data: &ByteReader, _key: &[u8], prev_value: Option<Self::Output>) -> Result<Self::Output> {

        Ok(if let Some(handle) = prev_value {
            let delta = data.read_varint_i64()?;
            BlockHandle { offset: handle.offset + handle.size, size: ((handle.size as i64) + delta) as u64}
        } else {
            let offset = data.read_varint_u64()?;
            let size = data.read_varint_u64()?;
            BlockHandle { offset, size }
        })
    }
}

/// Reader implementation for data blocks, returning BSON document bytes for `Put` operations.
pub struct DataEntryReader;

impl EntryReader for DataEntryReader {
    type Output = Vec<u8>;

    fn read(&self, data: &ByteReader, key: &[u8], _prev_value: Option<Self::Output>) -> Result<Self::Output> {

        let op = extract_operation_type(key);

        if OperationType::Put == op {
            // Read the document length (first 4 bytes) without moving the underlying cursor
            let doc_length = data.peek_i32_le()? as usize;
            data.read_fixed_slice(doc_length).map(|bytes| bytes.to_vec())
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use bson::{doc, to_vec, Bson};
    use crate::storage::internal_key::{encode_internal_key, MAX_SEQUENCE_NUMBER};
    use crate::storage::sstable::block_builder::{BlockBuilder, DataEntryWriter, IndexEntryWriter};
    use crate::util::bson_utils::BsonKey;
    use super::*;

    #[test]
    fn test_binary_search_restarts() {
        // Build the index block with a restart interval of 3
        let mut builder = BlockBuilder::new(3, IndexEntryWriter);
        // 1st restart
        builder.add(&internal_key(1, 1, OperationType::Put), BlockHandle::new(0, 25)).unwrap();
        builder.add(&internal_key(2, 2, OperationType::Put), BlockHandle::new(25, 30)).unwrap();
        builder.add(&internal_key(3, 3, OperationType::Put), BlockHandle::new(55, 20)).unwrap();
        // 2nd restart
        builder.add(&internal_key(4, 4, OperationType::Put), BlockHandle::new(72, 24)).unwrap();
        builder.add(&internal_key(5, 9, OperationType::Delete), BlockHandle::new(96, 9)).unwrap();
        builder.add(&internal_key(5, 8, OperationType::Put), BlockHandle::new(105, 48)).unwrap();
        // 3rd restart
        builder.add(&internal_key(5, 7, OperationType::Put), BlockHandle::new(153, 39)).unwrap();
        builder.add(&internal_key(5, 6, OperationType::Put), BlockHandle::new(192, 48)).unwrap();
        builder.add(&internal_key(5, 5, OperationType::Put), BlockHandle::new(240, 26)).unwrap();
        // 4th restart
        builder.add(&internal_key(7, 11, OperationType::Put), BlockHandle::new(266, 53)).unwrap();
        builder.add(&internal_key(7, 10, OperationType::Put), BlockHandle::new(319, 60)).unwrap();

        let block_data = builder.finish().unwrap().1;
        let block = BlockReader::new(3, &block_data, IndexEntryReader).unwrap();

        let restart_offset_0 = Some(block.read_key_offset(0).unwrap());
        let restart_offset_1 = Some(block.read_key_offset(1).unwrap());
        let restart_offset_2 = Some(block.read_key_offset(2).unwrap());
        let restart_offset_3 = Some(block.read_key_offset(3).unwrap());


        assert_eq!(None, block.binary_search_restarts(&internal_key(0, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(1, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(2, 2, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(3, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(4, 4, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, 9, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, 8, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, 7, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(5, 6, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(5, 5, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(6, 11, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(7, 11, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_3, block.binary_search_restarts(&internal_key(7, 10, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_3, block.binary_search_restarts(&internal_key(8, 12, OperationType::MaxKey)).unwrap());


        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(1, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(2, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(3, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&internal_key(4, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(6, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_3, block.binary_search_restarts(&internal_key(8, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());

        assert_eq!(restart_offset_2, block.binary_search_restarts(&internal_key(5, 2, OperationType::MaxKey)).unwrap());
    }

    #[test]
    fn test_search_index() {
        let mut builder = BlockBuilder::new(3, IndexEntryWriter);

        let key_1_1 = internal_key(1, 1, OperationType::Put);
        let value_1_1 = BlockHandle::new(0, 25);

        let key_2_2 = internal_key(2, 2, OperationType::Put);
        let value_2_2 = BlockHandle::new(25, 30);

        let key_3_3 = internal_key(3, 3, OperationType::Put);
        let value_3_3 = BlockHandle::new(55, 20);

        let key_4_4 = internal_key(4, 4, OperationType::Put);
        let value_4_4 = BlockHandle::new(72, 24);

        let key_5_9 = internal_key(5, 9, OperationType::Delete);
        let value_5_9 = BlockHandle::new(96, 9);

        let key_5_8 = internal_key(5, 8, OperationType::Put);
        let value_5_8 = BlockHandle::new(105, 48);

        let key_5_7 = internal_key(5, 7, OperationType::Put);
        let value_5_7 = BlockHandle::new(153, 39);

        let key_5_6 = internal_key(5, 6, OperationType::Put);
        let value_5_6 = BlockHandle::new(192, 48);

        let key_5_5 = internal_key(5, 5, OperationType::Put);
        let value_5_5 = BlockHandle::new(240, 26);

        let key_7_11 = internal_key(7, 11, OperationType::Put);
        let value_7_11 = BlockHandle::new(266, 53);

        let key_7_10 = internal_key(7, 10, OperationType::Put);
        let value_7_10 = BlockHandle::new(319, 60);

        // Now adding them to the builder
        builder.add(&key_1_1, value_1_1).unwrap();
        builder.add(&key_2_2, value_2_2).unwrap();
        builder.add(&key_3_3, value_3_3).unwrap();
        builder.add(&key_4_4, value_4_4).unwrap();
        builder.add(&key_5_9, value_5_9).unwrap();
        builder.add(&key_5_8, value_5_8).unwrap();
        builder.add(&key_5_7, value_5_7).unwrap();
        builder.add(&key_5_6, value_5_6).unwrap();
        builder.add(&key_5_5, value_5_5).unwrap();
        builder.add(&key_7_11, value_7_11).unwrap();
        builder.add(&key_7_10, value_7_10).unwrap();

        let block_data = builder.finish().unwrap().1;
        let block = BlockReader::new(3, &block_data, IndexEntryReader).unwrap();

        assert_eq!(None, block.search(&internal_key(0, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_1_1.clone(), value_1_1)), block.search(&internal_key(1, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2)), block.search(&internal_key(2, 2, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2)), block.search(&internal_key(2, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2)), block.search(&internal_key(2, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(None, block.search(&internal_key(2, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_3_3.clone(), value_3_3)), block.search(&internal_key(3, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_4_4.clone(), value_4_4)), block.search(&internal_key(4, 4, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_4_4.clone(), value_4_4)), block.search(&internal_key(4, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_9.clone(), value_5_9)), block.search(&internal_key(5, 9, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_9.clone(), value_5_9)), block.search(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_8.clone(), value_5_8)), block.search(&internal_key(5, 8, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_7.clone(), value_5_7)), block.search(&internal_key(5, 7, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_6.clone(), value_5_6)), block.search(&internal_key(5, 6, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_5.clone(), value_5_5)), block.search(&internal_key(5, 5, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_7_11.clone(), value_7_11)), block.search(&internal_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_7_11.clone(), value_7_11)), block.search(&internal_key(7, 11, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_7_10.clone(), value_7_10)), block.search(&internal_key(7, 10, OperationType::MaxKey)).unwrap());
        assert_eq!(None, block.search(&internal_key(8, 12, OperationType::MaxKey)).unwrap());
    }

    #[test]
    fn test_search_data() {
        let mut builder = BlockBuilder::new(3, DataEntryWriter);

        let key_1_1 = internal_key(1, 1, OperationType::Put);
        let value_1_1 = to_vec(&doc! {"id": 1, "name": "Iron Man", "year": 2008}).unwrap();

        let key_2_2 = internal_key(2, 2, OperationType::Put);
        let value_2_2 = to_vec(&doc! {"id": 2, "name": "The Incredible Hulk", "year": 2008}).unwrap();

        let key_3_3 = internal_key(3, 3, OperationType::Put);
        let value_3_3 = to_vec(&doc! {"id": 3, "name": "Iron Man 2", "year": 2010}).unwrap();

        let key_4_4 = internal_key(4, 4, OperationType::Put);
        let value_4_4 = to_vec(&doc! {"id": 4, "name": "Thor", "year": 2011}).unwrap();

        let key_5_9 = internal_key(5, 9, OperationType::Delete);
        let value_5_9 = Vec::new();

        let key_5_8 = internal_key(5, 8, OperationType::Put);
        let value_5_8 = to_vec(&doc! {"id": 5, "name": "Captain America: The First Avenger", "year": 2011}).unwrap();

        let key_5_7 = internal_key(5, 7, OperationType::Put);
        let value_5_7 = to_vec(&doc! {"id": 5, "name": "Captain Omerica: The First Avenger", "year": 2011}).unwrap();

        let key_5_6 = internal_key(5, 6, OperationType::Put);
        let value_5_6 = to_vec(&doc! {"id": 5, "name": "Captan America: The First Avenger", "year": 2011}).unwrap();

        let key_5_5 = internal_key(5, 5, OperationType::Put);
        let value_5_5 = to_vec(&doc! {"id": 5, "name": "Captoin America: The First Avenger", "year": 2011}).unwrap();

        let key_7_11 = internal_key(7, 11, OperationType::Put);
        let value_7_11 = to_vec(&doc! {"id": 7, "name": "The Avenger", "year": 2012}).unwrap();

        let key_7_10 = internal_key(7, 10, OperationType::Put);
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
        let block = BlockReader::new(3, &block_data, DataEntryReader).unwrap();

        assert_eq!(None, block.search(&internal_key(0, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_1_1.clone(), value_1_1.clone())), block.search(&internal_key(1, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2.clone())), block.search(&internal_key(2, 2, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2.clone())), block.search(&internal_key(2, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_2_2.clone(), value_2_2.clone())), block.search(&internal_key(2, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(None, block.search(&internal_key(2, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_3_3.clone(), value_3_3.clone())), block.search(&internal_key(3, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_4_4.clone(), value_4_4.clone())), block.search(&internal_key(4, 4, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_4_4.clone(), value_4_4.clone())), block.search(&internal_key(4, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_9.clone(), value_5_9.clone())), block.search(&internal_key(5, 9, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_9.clone(), value_5_9.clone())), block.search(&internal_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_8.clone(), value_5_8.clone())), block.search(&internal_key(5, 8, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_7.clone(), value_5_7.clone())), block.search(&internal_key(5, 7, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_6.clone(), value_5_6.clone())), block.search(&internal_key(5, 6, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_5_5.clone(), value_5_5.clone())), block.search(&internal_key(5, 5, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_7_11.clone(), value_7_11.clone())), block.search(&internal_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_7_11.clone(), value_7_11.clone())), block.search(&internal_key(7, 11, OperationType::MaxKey)).unwrap());
        assert_eq!(Some((key_7_10.clone(), value_7_10.clone())), block.search(&internal_key(7, 10, OperationType::MaxKey)).unwrap());
        assert_eq!(None, block.search(&internal_key(8, 12, OperationType::MaxKey)).unwrap());
    }

    fn internal_key(id: i32, sequence_num: u64, op: OperationType) -> Vec<u8> {
        let user_key = &Bson::Int32(id).try_into_key().unwrap();
        encode_internal_key(32, 0, &user_key, sequence_num, op).to_vec()
    }
}