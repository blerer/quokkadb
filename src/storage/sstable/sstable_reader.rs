use crate::io::byte_reader::ByteReader;
use crate::io::checksum::{ChecksumStrategy, Crc32ChecksumStrategy};
use crate::io::compressor::{Compressor, CompressorType};
use crate::io::ZeroCopy;
use crate::storage::sstable::block_cache::BlockCache;
use crate::storage::sstable::block_reader::{BlockReader, DataEntryReader, IndexEntryReader};
use crate::storage::sstable::sstable_properties::SSTableProperties;
use crate::storage::sstable::BlockHandle;
use crate::storage::sstable::{MAGIC_NUMBER, SSTABLE_FOOTER_LENGTH};
use crate::util::bloom_filter::BloomFilter;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Result};
#[cfg(windows)]
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use ErrorKind::InvalidData;
use std::ops::RangeBounds;
use crate::{event, info};
use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::storage::Direction;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, extract_record_key};
use crate::storage::operation::OperationType;
use crate::util::interval::Interval;

pub struct SSTableReader {
    logger: Arc<dyn LoggerAndTracer>,
    block_cache: Arc<BlockCache>,
    filename: String,
    file: SharedFile,
    compressor: Arc<dyn Compressor>,
    checksum_strategy: Arc<dyn ChecksumStrategy>,
    properties: SSTableProperties,
    index_handle: BlockHandle,
    filter_handle: BlockHandle,
}

impl SSTableReader {
    pub fn open(logger: Arc<dyn LoggerAndTracer>, block_cache: Arc<BlockCache>, file_path: &Path) -> Result<SSTableReader> {

        let filename = DbFile::new(file_path).unwrap().filename();
        let file = SharedFile::open(file_path)?;

        // The footer is stored in the last 48 bytes at the end of the file.
        let file_size = file.len()?;
        let footer_handle =
            BlockHandle::new(file_size - SSTABLE_FOOTER_LENGTH, SSTABLE_FOOTER_LENGTH);

        // Read the footer block:
        // +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
        // | SSTable version (1 byte) | Meta-Index block handle | Index block handle | Compression type (1 byte) | padding to 40 bytes | Magic number (8 bytes) |
        // +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
        let buf = file.read_block(footer_handle.offset, footer_handle.size as usize)?;

        // Make sure that the file is valid
        if &buf.read_u64_le(40) != MAGIC_NUMBER {
            return Err(Error::new(
                InvalidData,
                format!("Invalid magic number: {:?}", buf),
            ));
        }

        let mut reader = ByteReader::new(&buf[..40]);

        // Read the SSTable version
        let _sstable_version = reader.read_u8();

        // Read the Meta-Index block handle
        let metadata_handle = Self::read_block_handle(&mut reader)?;

        // Read the Index block handle
        let index_handle = Self::read_block_handle(&mut reader)?;

        // Create the compressor
        let compressor_type = CompressorType::try_from(reader.read_u8()?).map_err(|_| {
            Error::new(InvalidData, format!("Invalid compressor byte {:?}", buf[0]))
        })?;
        let compressor = compressor_type.new_compressor();

        let checksum_strategy: Arc<dyn ChecksumStrategy> = Arc::new(Crc32ChecksumStrategy {});

        let mut handles =
            Self::read_meta_index(&file, &compressor, &checksum_strategy, &metadata_handle)?;
        let filter_handle = handles.remove("filter.quokkadb.BloomFilter").unwrap();
        let properties_handle = handles.remove("properties").unwrap();

        let properties =
            Self::read_properties(&file, &compressor, &checksum_strategy, &properties_handle)?;

        info!(logger, "SSTableReader opened file={} properties={:?}", &filename, properties);

        Ok(SSTableReader {
            logger,
            block_cache,
            filename,
            file,
            compressor,
            checksum_strategy,
            properties,
            index_handle,
            filter_handle,
        })
    }

    /// Read the block handle from the byte reader
    fn read_block_handle<B: AsRef<[u8]>>(reader: &mut ByteReader<B>) -> Result<BlockHandle> {
        let offset = reader.read_varint_u64()?;
        let size = reader.read_varint_u64()?;
        Ok(BlockHandle::new(offset, size))
    }

    /// Read the meta index block from the SSTable file
    fn read_meta_index(
        file: &SharedFile,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        handle: &BlockHandle,
    ) -> Result<HashMap<String, BlockHandle>> {
        let data = Self::read_block(file, compressor, checksum_strategy, handle)?;

        let mut handles = HashMap::new();

        let reader = BlockReader::new(data, IndexEntryReader)?;
        for res in reader.scan_all_forward()?
        {
            match res {
                Ok((key, handle)) => {
                    // Ensure the key is valid UTF-8
                    if let Ok(key_str) = String::from_utf8(key.clone()) {
                        handles.insert(key_str, handle);
                    } else {
                        return Err(Error::new(
                            InvalidData,
                            format!("Invalid UTF-8 in key: {:?}", key),
                        ));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(handles)
    }

    /// Read the properties block from the SSTable file
    fn read_properties(
        file: &SharedFile,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        handle: &BlockHandle,
    ) -> Result<SSTableProperties> {
        let data = Self::read_block(file, compressor, checksum_strategy, handle)?;
        SSTableProperties::from_slice(&data)
    }

    /// Read a block of data from the file, decompress it, and verify its checksum
    fn read_block(
        file: &SharedFile,
        compressor: &Arc<dyn Compressor>,
        checksum_strategy: &Arc<dyn ChecksumStrategy>,
        handle: &BlockHandle,
    ) -> Result<Arc<[u8]>> {
        let compressed_data = file.read_block(handle.offset, handle.size as usize)?;
        let mut data = compressor.decompress(compressed_data.as_ref())?;
        let checksum_size = checksum_strategy.checksum_size();
        let checksum_offset = data.len() - checksum_size;
        checksum_strategy.verify_checksum(&data[..checksum_offset], &data[checksum_offset..])?;
        data.truncate(checksum_offset);
        Ok(Arc::from(data))
    }

    /// Get the properties of the SSTable
    pub fn properties(&self) -> &SSTableProperties {
        &self.properties
    }

    /// Read a value by user key and optional snapshot (sequence number)
    pub fn read(&self, record_key: &[u8], snapshot: u64) -> Result<Option<(Vec<u8>, Vec<u8>)>> {

        event!(self.logger, "read start file={} key={:?} snapshot={}", &self.filename, &record_key, snapshot);

        let filter_block = self.get_block(&self.filter_handle)?;

        let filter = BloomFilter::from_block(&filter_block)?;
        if !filter.contains(record_key) {
            event!(self.logger, "read filter_miss key={:?}", &record_key);
            return Ok(None);
        }

        let index_block = self.get_block(&self.index_handle)?;

        let index_reader = BlockReader::new(index_block, IndexEntryReader)?;

        let internal_key = encode_internal_key(record_key, snapshot, OperationType::max());
        let mut block_handles = index_reader.scan_forward_from(&internal_key)?;

        let block_handle = block_handles.next();

        match block_handle {
            Some(block_handle) => {
                let (_key, block_handle) = block_handle?;
                let data_block = self.get_block(&block_handle)?;
                let data_reader = BlockReader::new(data_block, DataEntryReader)?;
                let mut iter = data_reader.scan_forward_from(&internal_key)?;
                let result = iter.next();
                match result {
                    Some(result) => {
                        let (key, value) = result?;
                        if extract_record_key(&key) == record_key {
                            Ok(Some((key, value)))
                        } else {
                            Ok(None)
                        }
                    }
                    None => Ok(None)
                }

            }
            None => {
                event!(self.logger, "read index_miss key={:?}", &record_key);
                Ok(None)
            }
        }
    }

    /// Retrieves an SSTable block from the Block cache
    fn get_block(&self, block_handle: &BlockHandle) -> Result<Arc<[u8]>> {
        self.block_cache.get(
            &self.compressor,
            &self.checksum_strategy,
            &self.file,
            block_handle,
        )
    }

    pub fn range_scan(
        &self,
        range: &Interval<Vec<u8>>,
        snapshot: u64,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>>
    {
        let index_block = self.get_block(&self.index_handle)?;

        let index_reader = BlockReader::new(index_block, IndexEntryReader)?;

        if direction == Direction::Reverse {

        } else {

        }

        Err(Error::new(
            ErrorKind::Unsupported,
            "Range scan is not implemented yet",
        ))
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
        Ok(SharedFile {
            path: path.to_path_buf(),
            file: Arc::new(RwLock::new(file)),
        })
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

#[cfg(test)]
mod tests {
    use bson::doc;
    use tempfile::tempdir;
    use crate::obs::logger::test_instance;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::storage::files::DbFile;
    use super::*;
    use crate::storage::internal_key::{encode_internal_key, encode_record_key};
    use crate::storage::lsm_version::SSTableMetadata;
    use crate::storage::operation::OperationType;
    use crate::storage::sstable::sstable_writer::SSTableWriter;
    use crate::util::bson_utils::as_key_value;

    #[test]
    fn test_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Options::lightweight();
        let mut metric_registry = MetricRegistry::new();
        let block_cache = BlockCache::new(test_instance(), &mut metric_registry, &options.db);

        let inserts = vec![
            as_key_value(&doc! { "id": 1, "name": "Luke Skywalker", "role": "Jedi" }).unwrap(),
            as_key_value(&doc! { "id": 2, "name": "Darth Vader", "role": "Sith" }).unwrap(),
            as_key_value(&doc! { "id": 3, "name": "Leia Organa", "role": "Princess" }).unwrap(),
            as_key_value(&doc! { "id": 4, "name": "Han Solo", "role": "Smuggler" }).unwrap(),
        ];

        let sst_file = DbFile::new_sst(12);

        let mut writer = SSTableWriter::new(&path, &sst_file, &options, inserts.len()).unwrap();

        let mut seq = 15;
        for (user_key, value) in inserts.iter() {
            writer.add(&encode_internal_key(&encode_record_key(10, 0, user_key), seq, OperationType::Put), value).unwrap();
            seq += 1;
        }

        let sst = writer.finish().unwrap();

        let expected_size = path.join(DbFile::new_sst(12).filename()).metadata().unwrap().len();

        let expected = SSTableMetadata::new(
            12,
            0,
            &encode_record_key(10, 0, &inserts[0].0),
            &encode_record_key(10, 0, &inserts[3].0),
            15,
            18,
            expected_size,
        );
        assert_eq!(sst, expected);

        let reader = SSTableReader::open(test_instance(), block_cache, &path.join(sst_file.filename())).unwrap();
        let properties = reader.properties();

        assert_eq!(properties.num_entries, 4);
        assert_eq!(properties.max_key, expected.max_key);
        assert_eq!(properties.min_sequence, expected.min_sequence_number);
        assert_eq!(properties.max_sequence, expected.max_sequence_number);

        let mut seq = 15;
        for (user_key, value) in inserts.iter() {
            let record_key = encode_record_key(10, 0, user_key);
            let result = reader.read(&record_key, 19).unwrap();
            assert_eq!(result, Some((encode_internal_key(&record_key, seq, OperationType::Put), value.clone())));
            seq += 1;
        }
    }
}
