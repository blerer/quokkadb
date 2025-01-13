use ErrorKind::InvalidData;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{Result, Error, ErrorKind};
use std::sync::Arc;
use crate::storage::sstable::BlockHandle;
use crate::storage::operation::OperationType;
use crate::storage::sstable::{MAGIC_NUMBER, SSTABLE_FOOTER_LENGTH};
use crate::storage::sstable::sstable_properties::SSTableProperties;
use crate::io::compressor::{Compressor, CompressorType};
use crate::io::byte_reader::ByteReader;
use crate::io::checksum::{ChecksumStrategy, Crc32ChecksumStrategy};
use crate::io::fd_cache::FileDescriptorCache;
use crate::io::ZeroCopy;
use crate::storage::internal_key::{extract_sequence_number, extract_user_key};
use crate::storage::sstable::block_builder::EntryWriter;
use crate::storage::sstable::block_cache::BlockCache;
use crate::util::bloom_filter::BloomFilter;

pub struct SSTable {
    file_path: String,
    block_cache: Arc<BlockCache>,
    compressor: Arc<dyn Compressor>,
    checksum_strategy: Arc<dyn ChecksumStrategy>,
    properties: SSTableProperties,
    index_handle: BlockHandle,
    filter_handle: BlockHandle,
}

impl SSTable {


    pub fn open(fd_cache: Arc<FileDescriptorCache>, block_cache: Arc<BlockCache>, file_path: &str) -> Result<SSTable> {

        // The footer is stored in the last 48 bytes at the end of the file.
        let file_size = fd_cache.read_length(&file_path)?;
        let footer_handle = BlockHandle::new(file_size - SSTABLE_FOOTER_LENGTH, SSTABLE_FOOTER_LENGTH);

        // Read the footer block:
        // +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
        // | SSTable version (1 byte) | Meta-Index block handle | Index block handle | Compression type (1 byte) | padding to 40 bytes | Magic number (8 bytes) |
        // +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
        let buf = fd_cache.read_block(&file_path, footer_handle.offset, footer_handle.size as usize)?;

        // Make sure that the file is valid
        if &buf.read_u64_le(40) != MAGIC_NUMBER {
            return Err(Error::new(InvalidData, format!("Invalid magic number: {:?}", buf)))
        }

        let mut reader = ByteReader::new(&buf[..40]);

        // Read the SSTable version
        let sstable_version = reader.read_u8();

        // Read the Meta-Index block handle
        let metadata_handle = Self::read_block_handle(&mut reader)?;

        // Read the Index block handle
        let index_handle = Self::read_block_handle(&mut reader)?;

        // Create the compressor
        let compressor_type = CompressorType::try_from(reader.read_u8()?).map_err(|_| Error::new(InvalidData, format!("Invalid compressor byte {:?}", buf[0])))?;
        let compressor = compressor_type.new_compressor();

        let checksum_strategy: Arc<dyn ChecksumStrategy> = Arc::new(Crc32ChecksumStrategy {});

        let mut handles = Self::read_meta_index(&fd_cache, &compressor, &checksum_strategy, &file_path, &metadata_handle)?;
        let filter_handle = handles.remove("filter.quokkadb.BloomFilter").unwrap();
        let properties_handle = handles.remove("properties").unwrap();

        let properties = Self::read_properties(&fd_cache, &compressor, &checksum_strategy, &file_path, &properties_handle)?;

        Ok(SSTable{file_path: file_path.to_string(), block_cache, compressor, checksum_strategy, properties, index_handle, filter_handle,})
    }

    fn read_block_handle(reader: &mut ByteReader) -> Result<BlockHandle> {
        let offset= reader.read_varint_u64()?;
        let size = reader.read_varint_u64()?;
        Ok(BlockHandle::new(offset, size))
    }

    fn read_meta_index(
        fd_cache: &Arc<FileDescriptorCache>,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        file_path: &str,
        handle: &BlockHandle
    ) -> Result<HashMap<String, BlockHandle>> {

        let data = Self::read_block_skipping_cache(fd_cache, compressor, checksum_strategy, file_path, handle)?;

        let offset = data.len() - 4;
        let number_of_restarts = &data.read_u32_le(offset);
        let restarts_offset = offset - ((*number_of_restarts as usize) * 4);

        let mut handles = HashMap::new();

        let restarts_reader = ByteReader::new(&data[restarts_offset..]);
        let data_reader = ByteReader::new(&data[..restarts_offset]);
        let mut next_restart = restarts_reader.read_u32_le()? as usize;

        let mut previous_key  = Vec::new();
        let mut previous_handle : Option<BlockHandle> = None;

        while data_reader.has_remaining() {

            if next_restart == data_reader.position() {
                previous_handle = None;
                next_restart = data_reader.read_u32_le()? as usize;
            }

            let shared = data_reader.read_varint_u64()?;

            let mut key = Vec::new();
            key.extend_from_slice(&previous_key[..shared as usize]);
            key.extend_from_slice(data_reader.read_length_prefixed_slice()?);

            let handle = if let Some(h) = previous_handle {
                let size_delta = data_reader.read_i64_le()?;
                BlockHandle::new(h.offset + h.size, ((h.size as i64) + size_delta) as u64)
            } else {
                let offset = data_reader.read_varint_u64()?;
                let size = data_reader.read_varint_u64()?;
                BlockHandle::new(offset, size)
            };

            handles.insert(String::from_utf8(key.clone()).unwrap(), handle);

            previous_key = key;
            previous_handle = Some(handle);
        }

        Ok(handles)
    }

    fn read_properties(
        fd_cache: &Arc<FileDescriptorCache>,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        file_path: &str,
        handle: &BlockHandle
    ) -> Result<SSTableProperties> {

        let data = Self::read_block_skipping_cache(
            fd_cache,
            compressor,
            checksum_strategy,
            file_path,
            handle
        )?;
        SSTableProperties::from_slice(&data)
    }

    fn read_block_skipping_cache(
        fd_cache: &Arc<FileDescriptorCache>,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        file_path: &str,
        handle: &BlockHandle
    ) -> Result<Vec<u8>> {

        let compressed_data = fd_cache.read_block(file_path, handle.offset, handle.size as usize)?;
        let mut data = compressor.decompress(compressed_data.as_ref())?;
        let checksum_size = checksum_strategy.checksum_size();
        checksum_strategy.verify_checksum(&data[..checksum_size], &data[checksum_size..])?;
        data.truncate(data.len() - checksum_size);
        Ok(data)
    }

    /// Read a value by user key and optional snapshot (sequence number)
    pub fn read(&self, key: &[u8], snapshot: Option<u64>) -> Result<Option<Vec<u8>>> {

        if key > &self.properties.max_key || key < &self.properties.min_key {
            return Ok(None);
        }

        let filter_block = self.get_block(&self.filter_handle)?;

        let filter = BloomFilter::from_block(&filter_block)?;
        if !filter.contains(key) {
            return Ok(None);
        }

        // let index_block = self.get_block(&self.index_handle)?;
        // let mut candidate_blocks = self.binary_search_index(&index_block, key)?;
        //
        // while let Some(handle) = candidate_blocks.pop() {
        //     let data_block = self.get_block(&handle)?;
        //     let mut reader = ByteReader::new(&data_block);
        //
        //     let mut left = 0;
        //     let mut right = reader.len();
        //     let mut found_entry = None;
        //
        //     while left < right {
        //         let mid = (left + right) / 2;
        //         reader.seek(mid)?;
        //         let compound_key = CompoundKey::from_vec(reader.read_remaining()?);
        //
        //         match compound_key.user_key.cmp(key) {
        //             std::cmp::Ordering::Less => left = mid + 1,
        //             std::cmp::Ordering::Greater => right = mid,
        //             std::cmp::Ordering::Equal => {
        //                 found_entry = Some(compound_key);
        //                 break;
        //             }
        //         }
        //     }
        //
        //     if let Some(compound_key) = found_entry {
        //         loop {
        //             let seq_num = compound_key.extract_sequence_number();
        //             let op_type = compound_key.extract_value_type();
        //
        //             if snapshot.map_or(true, |s| seq_num <= s) {
        //                 return match op_type {
        //                     OperationType::Put => Ok(Some(reader.read_remaining()?.to_vec())),
        //                     OperationType::Delete => Ok(None),
        //                     _ => Ok(None),
        //                 };
        //             }
        //
        //             if reader.remaining() == 0 {
        //                 break;
        //             }
        //         }
        //     }
        // }
        Ok(None)
    }


    fn get_block(&self, block_handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        self.block_cache.get(&self.compressor, &self.checksum_strategy, &self.file_path, block_handle)
    }
}

pub trait BlockReader {
    type Output;
    fn linear_search(&self,
                     key: &[u8],
                     data: &ByteReader,
                     restart_offset: usize,
                     restart_interval: usize
    ) -> Result<Option<Self::Output>>;

}

pub struct IndexBlockReader;

impl BlockReader for IndexBlockReader {
    type Output = BlockHandle;

    fn linear_search(&self,
                     key: &[u8],
                     data: &ByteReader,
                     restart_offset: usize,
                     restart_interval: usize
    ) -> Result<Option<BlockHandle>> {

        let mut count = 0;
        data.seek(restart_offset)?;

        let user_key = extract_user_key(&key);
        let mut prev_key = Vec::new();
        let mut prev_value: Option<BlockHandle> = None;

        while data.has_remaining() {

            let mut new_key = Vec::new();
            let shared = data.read_varint_u64()?;
            new_key.extend_from_slice(&prev_key[..shared as usize]);
            new_key.extend_from_slice(data.read_length_prefixed_slice()?);

            println!("{:?}", &new_key);

            let handle = if let Some(handle) = prev_value {
                let delta = data.read_varint_i64()?;
                println!("delta {:?}", delta);
                BlockHandle { offset: handle.offset + handle.size, size: ((handle.size as i64) + delta) as u64}
            } else {
                let offset = data.read_varint_u64()?;
                let size = data.read_varint_u64()?;
                BlockHandle { offset, size }
            };

            println!("{:?}", handle);

            match extract_user_key(&new_key).cmp(user_key) {
                Ordering::Less => { }, // Do nothing
                Ordering::Equal => {
                    if extract_sequence_number(&new_key) <= extract_sequence_number(&key) {
                        return Ok(Some(handle))
                    } else {
                        return Ok(None)
                    }
                },
                Ordering::Greater => return Ok(None)
            };


            if &new_key == key {
                return Ok(Some(handle));
            }

            prev_key = new_key;
            count = count + 1;
            if count == restart_interval {
                count = 0;
                prev_value = None;
            } else {
                prev_value = Some(handle);
            }
        }

        Ok(None)
    }
}

struct Block<'a, W: BlockReader> {
    nbr_of_restarts: usize,
    restart_interval: usize,
    restarts: &'a [u8],
    data: ByteReader<'a>,
    reader: W,
}

impl <'a, W: BlockReader> Block<'a, W> {

    fn new(restart_interval: usize, block: &'a [u8], reader: W) -> Result<Self> {
        let len = block.len();
        let nbr_of_restarts_offset = len - 4;
        let nbr_of_restarts = block.read_u32_le(nbr_of_restarts_offset) as usize;
        let restarts_offset = nbr_of_restarts_offset - (nbr_of_restarts * 4);

        let data = ByteReader::new(&block[..restarts_offset]);
        let restarts = &block[restarts_offset..nbr_of_restarts_offset];
        Ok(Block { nbr_of_restarts, restart_interval, restarts, data, reader })
    }

    fn read_restart_key(&self, offset: usize) -> Result<&[u8]> {
        let key_offset = self.read_key_offset(offset)?;
        self.data.seek(key_offset + 1)?; // Skipping the shared number which is a 0 byte
        Ok(self.data.read_length_prefixed_slice()?)
    }

    fn read_key_offset(&self, index: usize) -> Result<usize> {
        Ok(self.restarts.read_u32_le(index << 2) as usize)
    }

    fn search(&self, key: &[u8]) -> Result<Option<W::Output>> {

        let restart_key_offset = self.binary_search_restarts(key)?;

        if let Some(offset) = restart_key_offset {
            self.reader.linear_search(key, &self.data, offset, self.restart_interval)
        } else {
            Ok(None)
        }
    }
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
}

#[cfg(test)]
mod tests {
    use bson::Bson;
    use crate::storage::internal_key::{encode_internal_key, MAX_SEQUENCE_NUMBER};
    use crate::storage::sstable::block_builder::{BlockBuilder, IndexEntryWriter};
    use crate::util::bson_utils::BsonKey;
    use super::*;

    #[test]
    fn test_binary_search_restarts() {
        let mut builder = BlockBuilder::new(3, IndexEntryWriter);
        builder.add(&component_key(1, 1, OperationType::Put), BlockHandle::new(0, 25)).unwrap();
        builder.add(&component_key(2, 2, OperationType::Put), BlockHandle::new(25, 30)).unwrap();
        builder.add(&component_key(3, 3, OperationType::Put), BlockHandle::new(55, 20)).unwrap();
        builder.add(&component_key(4, 4, OperationType::Put), BlockHandle::new(72, 24)).unwrap();
        builder.add(&component_key(5, 9, OperationType::Delete), BlockHandle::new(96, 9)).unwrap();
        builder.add(&component_key(5, 8, OperationType::Put), BlockHandle::new(105, 48)).unwrap();
        builder.add(&component_key(5, 7, OperationType::Put), BlockHandle::new(153, 39)).unwrap();
        builder.add(&component_key(5, 6, OperationType::Put), BlockHandle::new(192, 48)).unwrap();
        builder.add(&component_key(5, 5, OperationType::Put), BlockHandle::new(240, 26)).unwrap();
        builder.add(&component_key(7, 11, OperationType::Put), BlockHandle::new(266, 53)).unwrap();
        builder.add(&component_key(7, 10, OperationType::Put), BlockHandle::new(319, 60)).unwrap();
        let block_data = builder.finish().unwrap().1;
        let block = Block::new(3, &block_data, IndexBlockReader).unwrap();

        let restart_offset_0 = Some(block.read_key_offset(0).unwrap());
        let restart_offset_1 = Some(block.read_key_offset(1).unwrap());
        let restart_offset_2 = Some(block.read_key_offset(2).unwrap());
        let restart_offset_3 = Some(block.read_key_offset(3).unwrap());


        assert_eq!(None, block.binary_search_restarts(&component_key(0, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(1, 1, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(2, 2, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(3, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(4, 4, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, 9, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, 8, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, 7, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(5, 6, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(5, 5, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(6, 11, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(7, 11, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_3, block.binary_search_restarts(&component_key(7, 10, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_3, block.binary_search_restarts(&component_key(8, 12, OperationType::MaxKey)).unwrap());


        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(1, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(2, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(3, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(4, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(6, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        assert_eq!(restart_offset_3, block.binary_search_restarts(&component_key(8, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());

        assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(5, 2, OperationType::MaxKey)).unwrap());
    }

    #[test]
    fn test_search_index() {
        let mut builder = BlockBuilder::new(3, IndexEntryWriter);
        builder.add(&component_key(1, 1, OperationType::Put), BlockHandle::new(0, 25)).unwrap();
        builder.add(&component_key(2, 2, OperationType::Put), BlockHandle::new(25, 30)).unwrap();
        builder.add(&component_key(3, 3, OperationType::Put), BlockHandle::new(55, 20)).unwrap();
        builder.add(&component_key(4, 4, OperationType::Put), BlockHandle::new(72, 24)).unwrap();
        builder.add(&component_key(5, 9, OperationType::Delete), BlockHandle::new(96, 9)).unwrap();
        builder.add(&component_key(5, 8, OperationType::Put), BlockHandle::new(105, 48)).unwrap();
        builder.add(&component_key(5, 7, OperationType::Put), BlockHandle::new(153, 39)).unwrap();
        builder.add(&component_key(5, 6, OperationType::Put), BlockHandle::new(192, 48)).unwrap();
        builder.add(&component_key(5, 5, OperationType::Put), BlockHandle::new(240, 26)).unwrap();
        builder.add(&component_key(7, 11, OperationType::Put), BlockHandle::new(266, 53)).unwrap();
        builder.add(&component_key(7, 10, OperationType::Put), BlockHandle::new(319, 60)).unwrap();
        let block_data = builder.finish().unwrap().1;
        let block = Block::new(3, &block_data, IndexBlockReader).unwrap();

        let restart_offset_0 = Some(block.read_key_offset(0).unwrap());
        let restart_offset_1 = Some(block.read_key_offset(1).unwrap());
        let restart_offset_2 = Some(block.read_key_offset(2).unwrap());
        let restart_offset_3 = Some(block.read_key_offset(3).unwrap());


        // assert_eq!(None, block.search(&component_key(0, 1, OperationType::MaxKey)).unwrap());
        // assert_eq!(Some(BlockHandle::new(0, 25)), block.search(&component_key(1, 1, OperationType::MaxKey)).unwrap());
        // assert_eq!(Some(BlockHandle::new(25, 30)), block.search(&component_key(2, 2, OperationType::MaxKey)).unwrap());
        // assert_eq!(Some(BlockHandle::new(25, 30)), block.search(&component_key(2, 3, OperationType::MaxKey)).unwrap());
        // assert_eq!(Some(BlockHandle::new(25, 30)), block.search(&component_key(2, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(None, block.search(&component_key(2, 1, OperationType::MaxKey)).unwrap());
        // assert_eq!(Some(BlockHandle::new(55, 20)), block.search(&component_key(3, 3, OperationType::MaxKey)).unwrap());
        assert_eq!(Some(BlockHandle::new(72, 24)), block.search(&component_key(4, 4, OperationType::MaxKey)).unwrap());
        assert_eq!(Some(BlockHandle::new(72, 24)), block.search(&component_key(4, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(2, 2, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(3, 3, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(4, 4, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, 9, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, 8, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, 7, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(5, 6, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(5, 5, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(6, 11, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(7, 11, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_3, block.binary_search_restarts(&component_key(7, 10, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_3, block.binary_search_restarts(&component_key(8, 12, OperationType::MaxKey)).unwrap());
        //
        //
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(1, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(2, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(3, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_0, block.binary_search_restarts(&component_key(4, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_1, block.binary_search_restarts(&component_key(5, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(6, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(7, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        // assert_eq!(restart_offset_3, block.binary_search_restarts(&component_key(8, MAX_SEQUENCE_NUMBER, OperationType::MaxKey)).unwrap());
        //
        // assert_eq!(restart_offset_2, block.binary_search_restarts(&component_key(5, 2, OperationType::MaxKey)).unwrap());
    }

    fn component_key(id: i32, sequence_num: u64, op: OperationType) -> Vec<u8> {
        let collection = 32;
        let user_key = &Bson::Int32(id).try_into_key().unwrap();
        let component_key = encode_internal_key(collection, 0, &user_key, sequence_num, op);
        component_key.to_vec()
    }
}