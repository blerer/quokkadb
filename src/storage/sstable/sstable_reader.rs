use ErrorKind::InvalidData;
use std::collections::HashMap;
use std::fs::File;
#[cfg(windows)]
use {
    std::io::{Seek, SeekFrom},
};
use std::io::{Result, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use crate::storage::sstable::BlockHandle;
use crate::storage::sstable::{MAGIC_NUMBER, SSTABLE_FOOTER_LENGTH};
use crate::storage::sstable::sstable_properties::SSTableProperties;
use crate::io::compressor::{Compressor, CompressorType};
use crate::io::byte_reader::ByteReader;
use crate::io::checksum::{ChecksumStrategy, Crc32ChecksumStrategy};
use crate::io::ZeroCopy;
use crate::storage::sstable::block_cache::BlockCache;
use crate::storage::sstable::block_reader::{BlockReader, DataEntryReader, IndexEntryReader};
use crate::util::bloom_filter::BloomFilter;

pub struct SSTableReader {
    file: SharedFile,
    compressor: Arc<dyn Compressor>,
    checksum_strategy: Arc<dyn ChecksumStrategy>,
    properties: SSTableProperties,
    index_handle: BlockHandle,
    filter_handle: BlockHandle,
}

impl SSTableReader {


    pub fn open(file_path: &Path) -> Result<SSTableReader> {

        let file = SharedFile::open(file_path)?;

        // The footer is stored in the last 48 bytes at the end of the file.
        let file_size = file.len()?;
        let footer_handle = BlockHandle::new(file_size - SSTABLE_FOOTER_LENGTH, SSTABLE_FOOTER_LENGTH);

        // Read the footer block:
        // +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
        // | SSTable version (1 byte) | Meta-Index block handle | Index block handle | Compression type (1 byte) | padding to 40 bytes | Magic number (8 bytes) |
        // +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
        let buf = file.read_block(footer_handle.offset, footer_handle.size as usize)?;

        // Make sure that the file is valid
        if &buf.read_u64_le(40) != MAGIC_NUMBER {
            return Err(Error::new(InvalidData, format!("Invalid magic number: {:?}", buf)))
        }

        let mut reader = ByteReader::new(&buf[..40]);

        // Read the SSTable version
        let _sstable_version = reader.read_u8();

        // Read the Meta-Index block handle
        let metadata_handle = Self::read_block_handle(&mut reader)?;

        // Read the Index block handle
        let index_handle = Self::read_block_handle(&mut reader)?;

        // Create the compressor
        let compressor_type = CompressorType::try_from(reader.read_u8()?).map_err(|_| Error::new(InvalidData, format!("Invalid compressor byte {:?}", buf[0])))?;
        let compressor = compressor_type.new_compressor();

        let checksum_strategy: Arc<dyn ChecksumStrategy> = Arc::new(Crc32ChecksumStrategy {});

        let mut handles = Self::read_meta_index(&file, &compressor, &checksum_strategy, &metadata_handle)?;
        let filter_handle = handles.remove("filter.quokkadb.BloomFilter").unwrap();
        let properties_handle = handles.remove("properties").unwrap();

        let properties = Self::read_properties(&file, &compressor, &checksum_strategy, &properties_handle)?;

        Ok(SSTableReader {file, compressor, checksum_strategy, properties, index_handle, filter_handle,})
    }

    fn read_block_handle(reader: &mut ByteReader) -> Result<BlockHandle> {
        let offset= reader.read_varint_u64()?;
        let size = reader.read_varint_u64()?;
        Ok(BlockHandle::new(offset, size))
    }

    fn read_meta_index(
        file: &SharedFile,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        handle: &BlockHandle
    ) -> Result<HashMap<String, BlockHandle>> {

        let data = Self::read_block(file, compressor, checksum_strategy, handle)?;

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
        file: &SharedFile,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        handle: &BlockHandle
    ) -> Result<SSTableProperties> {

        let data = Self::read_block(file, compressor, checksum_strategy, handle)?;
        SSTableProperties::from_slice(&data)
    }

    fn read_block(
        file: &SharedFile,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        handle: &BlockHandle
    ) -> Result<Vec<u8>> {

        let compressed_data = file.read_block(handle.offset, handle.size as usize)?;
        let mut data = compressor.decompress(compressed_data.as_ref())?;
        let checksum_size = checksum_strategy.checksum_size();
        checksum_strategy.verify_checksum(&data[..checksum_size], &data[checksum_size..])?;
        data.truncate(data.len() - checksum_size);
        Ok(data)
    }

    /// Read a value by user key and optional snapshot (sequence number)
    pub fn read(&self, block_cache: &BlockCache, internal_key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {

        let filter_block = self.get_block(block_cache, &self.filter_handle)?;

        let filter = BloomFilter::from_block(&filter_block)?;
        if !filter.contains(internal_key) {
            return Ok(None);
        }

        let index_block = self.get_block(block_cache, &self.index_handle)?;
        let index_reader = BlockReader::new(&index_block, IndexEntryReader)?;

        let block_handle = index_reader.search(internal_key)?;

        if let Some((_key, block_handle)) = block_handle {
            let data_block = self.get_block(block_cache, &block_handle)?;
            let data_reader = BlockReader::new(&index_block, DataEntryReader)?;
            data_reader.search(&data_block)
        } else {
            Ok(None)
        }
    }

    fn get_block(&self, block_cache: &BlockCache, block_handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        block_cache.get(&self.compressor, &self.checksum_strategy, &self.file, block_handle)
    }
}

#[derive(Clone, Debug)]
pub struct SharedFile {
    pub(crate) path: PathBuf,
    file: Arc<RwLock<File>>,
}

impl SharedFile {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(SharedFile{ path: path.to_path_buf(), file: Arc::new(RwLock::new(file)) })
    }

    /// Read a block of data from the cached FD (Opens a new FD if locked on Windows)
    pub fn read_block(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let file = self.file.read().unwrap(); // Read lock (non-blocking)
            file.read_at(&mut buffer, offset)?;
        }

        #[cfg(windows)]
        {
            // Try getting a write lock (since seek modifies state)
            let file_guard = self.file.try_write();

            if let Ok(mut file) = file_guard {
                // FD is free → Use cached FD
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut buffer)?;
            } else {
                // FD is locked → Open a new FD for this read (no caching)
                let mut temp_file = File::open(Path::new(self.path))?;
                temp_file.seek(SeekFrom::Start(offset))?;
                temp_file.read_exact(&mut buffer)?;
            }
        }

        Ok(buffer)
    }

    pub fn len(&self) -> Result<u64> {
        let file = self.file.read().unwrap();
        Ok(file.metadata()?.len())
    }
}
