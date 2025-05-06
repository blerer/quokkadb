use ErrorKind::InvalidData;
use std::collections::HashMap;
use std::io::{Result, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::storage::sstable::BlockHandle;
use crate::storage::sstable::{MAGIC_NUMBER, SSTABLE_FOOTER_LENGTH};
use crate::storage::sstable::sstable_properties::SSTableProperties;
use crate::io::compressor::{Compressor, CompressorType};
use crate::io::byte_reader::ByteReader;
use crate::io::checksum::{ChecksumStrategy, Crc32ChecksumStrategy};
use crate::io::fd_cache::FileDescriptorCache;
use crate::io::ZeroCopy;
use crate::storage::sstable::block_cache::BlockCache;
use crate::util::bloom_filter::BloomFilter;

pub struct SSTableReader {
    file_path: PathBuf,
    block_cache: Arc<BlockCache>,
    compressor: Arc<dyn Compressor>,
    checksum_strategy: Arc<dyn ChecksumStrategy>,
    properties: SSTableProperties,
    index_handle: BlockHandle,
    filter_handle: BlockHandle,
}

impl SSTableReader {


    pub fn open(fd_cache: Arc<FileDescriptorCache>, block_cache: Arc<BlockCache>, file_path: &Path) -> Result<SSTableReader> {

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

        Ok(SSTableReader {file_path: file_path.to_path_buf(), block_cache, compressor, checksum_strategy, properties, index_handle, filter_handle,})
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
        file_path: &Path,
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
        file_path: &Path,
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
        file_path: &Path,
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

        let filter_block = self.get_block(&self.filter_handle)?;

        let filter = BloomFilter::from_block(&filter_block)?;
        if !filter.contains(key) {
            return Ok(None);
        }


        Ok(None)
    }

    fn get_block(&self, block_handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        self.block_cache.get(&self.compressor, &self.checksum_strategy, &self.file_path, block_handle)
    }
}
