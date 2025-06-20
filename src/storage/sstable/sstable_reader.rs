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
use crate::storage::internal_key::{InternalKeyBound, InternalKeyRange};
use crate::storage::iterators::{ForwardIterator, ReverseIterator, TracingIterator};
use std::fs::File;
use std::io::{Error, ErrorKind, Result};
#[cfg(windows)]
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use ErrorKind::InvalidData;
use std::cmp::Ordering;
use std::fmt;
use std::rc::Rc;
use std::time::Instant;
use crate::{event, info};
use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::storage::Direction;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, extract_record_key, extract_sequence_number};
use crate::storage::operation::OperationType;

pub struct SSTableReader {
    logger: Arc<dyn LoggerAndTracer>,
    block_loader: Arc<BlockLoader>,
    filename: String,
    properties: SSTableProperties,
    index_handle: BlockHandle,
    filter_handle: BlockHandle,
}

impl SSTableReader {
    pub fn open(logger: Arc<dyn LoggerAndTracer>, block_cache: Arc<BlockCache>, file_path: &Path) -> Result<SSTableReader> {

        let filename = DbFile::new(file_path).unwrap().filename();

        let start = Instant::now();
        info!(logger, "SSTableReader opening file={}", &filename);

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

        let duration = start.elapsed();
        info!(logger, "SSTableReader opened file={}, properties={:?}, duration={}us", &filename, properties, duration.as_micros());

        let block_loader = Arc::new(BlockLoader::new(
            block_cache,
            file,
            compressor,
            checksum_strategy,
        ));

        Ok(SSTableReader {
            logger,
            block_loader,
            filename,
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

    fn get_block(&self, handle: &BlockHandle) -> Result<Arc<[u8]>> {
        self.block_loader.load_block(handle)
    }

    /// Read a value by user key and snapshot
    pub fn read(&self, record_key: &[u8], snapshot: u64) -> Result<Option<(Vec<u8>, Vec<u8>)>> {

        event!(self.logger, "read start file={}, key={:?}, snapshot={}", &self.filename, &record_key, snapshot);

        let filter_block = self.get_block(&self.filter_handle)?;

        let filter = BloomFilter::from_block(&filter_block)?;
        if !filter.contains(record_key) {
            event!(self.logger, "read filter_miss key={:?}", &record_key);
            return Ok(None);
        }

        let index_block = self.get_block(&self.index_handle)?;

        let index_reader = BlockReader::new(index_block, IndexEntryReader)?;

        let internal_key = encode_internal_key(record_key, snapshot, OperationType::Delete);
        let bound = InternalKeyBound::Bounded(internal_key);
        let mut block_handles = index_reader.scan_forward_from(&bound)?;

        let block_handle = block_handles.next();

        match block_handle {
            Some(block_handle) => {
                let (_key, block_handle) = block_handle?;
                let data_block = self.get_block(&block_handle)?;
                let data_reader = BlockReader::new(data_block, DataEntryReader)?;
                event!(self.logger, "scanning_data_block key={:?}", &record_key);
                let mut iter = data_reader.scan_forward_from(&bound)?;
                while let Some(result) = iter.next() {
                    let (key, value) = result?;
                    let current_record_key = extract_record_key(&key);
                    match current_record_key.cmp(record_key) {
                        Ordering::Less => panic!("Unexpected lower key"), // Should never happen, otherwise I did screw up the logic and need to fix it.
                        Ordering::Greater => return Ok(None),
                        Ordering::Equal => {
                            if extract_sequence_number(&key) <= snapshot {
                                return Ok(Some((key, value)));
                            }
                        }
                    }
                }
                Ok(None)
            }
            None => {
                event!(self.logger, "read index_miss key={:?}", &record_key);
                Ok(None)
            }
        }
    }

    pub fn range_scan(
        &self,
        range: Rc<InternalKeyRange>,
        snapshot: u64,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> {
        // Fetch and parse the index block
        event!(self.logger, "range_scan start file={}, range={:?}, snapshot={}, direction={:?}", &self.filename, &range, snapshot, direction);
        let index_block = self.get_block(&self.index_handle)?;
        let index_reader = BlockReader::new(index_block, IndexEntryReader)?;

        let mut sstable_iter_base: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>> = match direction {
            Direction::Forward => {
                let mut index_iter = index_reader.scan_forward_from(range.start_bound())?;

                let data_iter = match index_iter.next() {
                    Some(Ok((_index_key, handle))) => {
                        let data_block = self.block_loader.load_block(&handle)?;
                        let data_reader = BlockReader::new(data_block, DataEntryReader)?;
                        Some(data_reader.scan_forward_from(&range.start_bound())?)
                     },
                    Some(Err(e)) => return Err(e),
                    None => None, 
                };
                
                let iter = SSTableRangeScanIterator {
                    logger: self.logger.clone(),
                    block_loader: self.block_loader.clone(),
                    range: range.clone(), // Pass along for boundary checks
                    direction,
                    index_iter,
                    current_data_block_iter: data_iter,
                    count: 0,
                };
                Box::new(ForwardIterator::new(Box::new(iter), snapshot))
            }
            Direction::Reverse => {
                let mut index_iter = index_reader.scan_reverse_from(range.end_bound())?;

                let data_iter = match index_iter.next() {
                    Some(Ok((_index_key, handle))) => {
                        let data_block = self.block_loader.load_block(&handle)?;
                        let data_reader = BlockReader::new(data_block, DataEntryReader)?;
                        Some(data_reader.scan_reverse_from(range.end_bound())?)
                    },
                    Some(Err(e)) => return Err(e),
                    None => None,
                };

                let iter = SSTableRangeScanIterator {
                    logger: self.logger.clone(),
                    block_loader: self.block_loader.clone(),
                    range: range.clone(), // Pass along for boundary checks
                    direction,
                    index_iter,
                    current_data_block_iter: data_iter,
                    count: 0,
                };
                Box::new(ReverseIterator::new(Box::new(iter), snapshot))
            }
        };

        if self.logger.is_tracing_enabled() {
            sstable_iter_base = Box::new(TracingIterator::new(
                sstable_iter_base,
                self.logger.clone(),
                format!("range_scan end file={},", self.filename),
            ));
        }

        Ok(sstable_iter_base)
    }
}

struct SSTableRangeScanIterator {
    logger: Arc<dyn LoggerAndTracer>,
    block_loader: Arc<BlockLoader>,
    range: Rc<InternalKeyRange>,
    direction: Direction,
    index_iter: Box<dyn Iterator<Item = Result<(Vec<u8>, BlockHandle)>>>,
    current_data_block_iter: Option<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>>,
    count: usize,
}

impl Iterator for SSTableRangeScanIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_data_block_iter.is_none() {
                // Attempt to load the next data block iterator from the index_iter
                println!("Loading next data block from index_iter");
                match self.index_iter.next() {
                    Some(Ok((_index_key, handle))) => {
                        let data_block = match self.block_loader.load_block(&handle) {
                            Ok(block) => block,
                            Err(e) => return Some(Err(e)),
                        };
                        let data_reader = match BlockReader::new(data_block, DataEntryReader) {
                            Ok(reader) => reader,
                            Err(e) => return Some(Err(e)),
                        };

                        // For subsequent blocks, always scan the entire block.
                        // The initial block's potentially targeted scan is handled during initialization.
                        let new_data_iter_result = match self.direction {
                            Direction::Forward => data_reader.scan_all_forward(),
                            Direction::Reverse => data_reader.scan_all_reverse(),
                        };

                        match new_data_iter_result {
                            Ok(iter) => self.current_data_block_iter = Some(iter),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    // If index_iter is exhausted, we stop iterating
                    None => return None, // No more index entries
                }
            }

            // If we have a data block iterator, try to get the next item
            if let Some(ref mut data_iter) = self.current_data_block_iter {
                match data_iter.next() {
                    Some(Ok((internal_key, value))) => {
                        // Snapshot and duplicate/version filtering are handled by ForwardIterator/ReverseIterator.
                        // This iterator role is now primarily to bridge index blocks to data blocks
                        // and apply the overall scan range bounds.
                        match self.direction {
                            Direction::Forward => {
                                if self.range.end_bound().is_less_than(&internal_key) {
                                    // Item is beyond the upper bound.
                                    // Signal exhaustion for this iterator to stop producing more items.
                                    self.current_data_block_iter = None;
                                    return None;
                                }
                                self.count += 1;
                                return Some(Ok((internal_key, value)));
                            }
                            Direction::Reverse => {
                                if self.range.start_bound().is_greater_than(&internal_key) {
                                    // Item is beyond the lower bound (for reverse scan).
                                    self.current_data_block_iter = None;
                                    return None;
                                }
                                self.count += 1;
                                return Some(Ok((internal_key, value)));
                            }
                        }
                    }
                    Some(Err(e)) => {
                        self.current_data_block_iter = None; // Invalidate on error
                        return Some(Err(e));
                    }
                    None => { // Current data block exhausted
                        self.current_data_block_iter = None;
                        // Loop again to fetch the next data block via index_iter
                    }
                }
            } else {
                 // current_data_block_iter is None, and index_iter was exhausted or failed.
                return None;
            }
        }
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

impl fmt::Display for SharedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

struct BlockLoader {
    block_cache: Arc<BlockCache>,
    file: SharedFile,
    compressor: Arc<dyn Compressor>,
    checksum_strategy: Arc<dyn ChecksumStrategy>,
}

impl BlockLoader {
    pub fn new(
        block_cache: Arc<BlockCache>,
        file: SharedFile,
        compressor: Arc<dyn Compressor>,
        checksum_strategy: Arc<dyn ChecksumStrategy>,
    ) -> Self {
        BlockLoader {
            block_cache,
            file,
            compressor,
            checksum_strategy,
        }
    }

    /// Retrieves an SSTable block from the Block cache, loading it if needed.
    pub fn load_block(&self, handle: &BlockHandle) -> Result<Arc<[u8]>> {
        self.block_cache.get(
            &self.compressor,
            &self.checksum_strategy,
            &self.file,
            handle,
        )
    }
}

#[cfg(test)]
mod tests {
    use bson::{doc, Bson};
    use tempfile::tempdir;
    use crate::obs::logger::test_instance;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::storage::files::DbFile;
    use super::*;
    use crate::storage::internal_key::{encode_internal_key, encode_internal_key_range, encode_record_key, MAX_SEQUENCE_NUMBER};
    use crate::storage::lsm_version::SSTableMetadata;
    use crate::storage::operation::{Operation, OperationType};
    use std::ops::{Bound, RangeBounds};
    use crate::storage::sstable::sstable_writer::SSTableWriter;
    use crate::storage::test_utils::{delete_op, delete_rec, put_op, put_rec};
    use crate::util::bson_utils::{as_key_value, BsonKey};
    use crate::util::interval::Interval;

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


    #[test]
    fn test_search_data() {

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Options::lightweight();
        let mut metric_registry = MetricRegistry::new();
        let block_cache = BlockCache::new(test_instance(), &mut metric_registry, &options.db);

        let sst_file = DbFile::new_sst(12);

        let col = 32;

        let entries = vec![
            put_rec(col, 1, 1, 1), // 0
            put_rec(col, 2, 1, 2), // 1
            put_rec(col, 3, 1, 3), // 2
            put_rec(col, 4, 1, 4), // 3
            delete_rec(col, 5, 9), // 4
            put_rec(col, 5, 4, 8), // 5
            put_rec(col, 5, 3, 7), // 6
            put_rec(col, 5, 2, 6), // 7
            put_rec(col, 5, 1, 5), // 8
            put_rec(col, 7, 2, 11), // 9
            put_rec(col, 7, 1, 10), //10
        ];

        let len = entries.len();
        let mut writer = SSTableWriter::new(&path, &sst_file, &options, len).unwrap();

        for entry in entries.iter() {
            writer.add(&entry.0, &entry.1).unwrap();
        }

        let _sst = writer.finish().unwrap();

        let reader = SSTableReader::open(test_instance(), block_cache, &path.join(sst_file.filename())).unwrap();

        assert_search_eq(&reader, &record_key(col,0), MAX_SEQUENCE_NUMBER, None);
        assert_search_eq(&reader, &record_key(col,1), MAX_SEQUENCE_NUMBER, entries.get(0));
        assert_search_eq(&reader, &record_key(col,2), 2, entries.get(1));
        assert_search_eq(&reader, &record_key(col,2), MAX_SEQUENCE_NUMBER, entries.get(1));
        assert_search_eq(&reader, &record_key(col,2), 1, None);
        assert_search_eq(&reader, &record_key(col,3), 3, entries.get(2));
        assert_search_eq(&reader, &record_key(col,4), 4, entries.get(3));
        assert_search_eq(&reader, &record_key(col,4), MAX_SEQUENCE_NUMBER, entries.get(3));
        assert_search_eq(&reader, &record_key(col,5), MAX_SEQUENCE_NUMBER, entries.get(4));
        assert_search_eq(&reader, &record_key(col,5), 9, entries.get(4));
        assert_search_eq(&reader, &record_key(col,5), 8, entries.get(5));
        assert_search_eq(&reader, &record_key(col,5), 7, entries.get(6));
        assert_search_eq(&reader, &record_key(col,5), 6, entries.get(7));
        assert_search_eq(&reader, &record_key(col,5), 5, entries.get(8));
        assert_search_eq(&reader, &record_key(col,6), MAX_SEQUENCE_NUMBER, None);
        assert_search_eq(&reader, &record_key(col,7), MAX_SEQUENCE_NUMBER, entries.get(9));
        assert_search_eq(&reader, &record_key(col,7), 11, entries.get(9));
        assert_search_eq(&reader, &record_key(col,7), 10, entries.get(10));
        assert_search_eq(&reader, &record_key(col,8), 12, None);
    }

    #[test]
    fn test_sstable_reader_range_scan() {

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Options::lightweight();
        let mut metric_registry = MetricRegistry::new();
        let block_cache = BlockCache::new(test_instance(), &mut metric_registry, &options.db);

        let sst_file = DbFile::new_sst(1);
        let col = 32;

        let mut writer = SSTableWriter::new(&path, &sst_file, &options, 1000).unwrap();

        for i in 1..=1001 { // We need to start at 1 as 0 would be skipped

            let vec = operations_and_sequences_for(col, i);

            for (op, seq) in vec.iter() {
                // Add the operation to the writer
                writer.add(&op.internal_key(*seq), op.value()).unwrap();
            }
        }

        writer.finish().unwrap();

        let reader = Arc::new(SSTableReader::open(test_instance(), block_cache, &path.join(sst_file.filename())).unwrap());

        for snapshot in vec![MAX_SEQUENCE_NUMBER, 5000, 2000] {
            for direction in vec!(Direction::Forward, Direction::Reverse) {

                // Full Unbounded
                test_range_scan(reader.clone(), col, &Interval::all(), &mut (1..=1001), snapshot, &direction);

                // Full Included
                test_range_scan(reader.clone(), col, &Interval::closed(1, 1001), &mut (1..=1001), snapshot, &direction);

                // Full Excluded Start
                test_range_scan(reader.clone(), col, &Interval::open_closed(1, 1001), &mut (2..=1001), snapshot, &direction);

                // Full Excluded End
                test_range_scan(reader.clone(), col, &Interval::closed_open(1, 1001), &mut (1..=1001), snapshot, &direction);

                // First Half
                test_range_scan(reader.clone(), col, &Interval::closed_open(1, 500), &mut (1..500), snapshot, &direction);

                // Second Half
                test_range_scan(reader.clone(), col, &Interval::closed(500, 1001), &mut (500..=1001), snapshot, &direction);

                // Around Skipped
                test_range_scan(reader.clone(), col, &Interval::closed(6, 8), &mut (6..=8), snapshot, &direction);

                // Around Deleted
                test_range_scan(reader.clone(), col, &Interval::closed(7, 9), &mut (7..=9), snapshot, &direction);

                // Around Overridden
                test_range_scan(reader.clone(), col, &Interval::closed_open(11, 14), &mut (11..=13), snapshot, &direction);

                // Single Existing Item (e.g. ID 1)
                test_range_scan(reader.clone(), col, &Interval::closed(1, 1), &mut (1..=1), snapshot, &direction);

                // Single Skipped Item (e.g. ID 5)
                test_range_scan(reader.clone(), col, &Interval::closed(7, 7), &mut (7..=7), snapshot, &direction);

                // Single Deleted Item (e.g. ID 7)
                test_range_scan(reader.clone(), col, &Interval::closed(5, 5), &mut (5..=5), snapshot, &direction);

                // Single Overridden Item (e.g. ID 11)
                test_range_scan(reader.clone(), col, &Interval::closed(12, 12), &mut (12..=12), snapshot, &direction);

                // Empty Range (low)
                test_range_scan(reader.clone(), col, &Interval::closed(-100, -50), &mut (0..0), snapshot, &direction);

                // Empty Range (high)
                test_range_scan(reader.clone(), col, &Interval::closed(1002, 2000), &mut (0..0), snapshot, &direction);

                // Empty Range (start > end)
                test_range_scan(reader.clone(), col, &Interval::closed(100, 50), &mut (0..0), snapshot, &direction);
            }
        }
    }

    fn test_range_scan<R: RangeBounds<i32> + IntoIterator<Item=i32> + Clone>(
        reader: Arc<SSTableReader>,
        col: u32,
        selected_range: &Interval<i32>,
        expected_range: &mut R,
        snapshot: u64,
        direction: &Direction
    )
    where
        <R as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        let mut iter = reader.range_scan(
            key_range(col, selected_range, snapshot, direction.clone()),
            snapshot,
            direction.clone(),
        ).unwrap();

        check_iter(&mut iter, col, expected_range, snapshot, direction);
    }

    fn check_iter<I, R: RangeBounds<i32> + IntoIterator<Item=i32> + Clone>(
        iter: &mut I,
        col: u32,
        range: &mut R,
        snapshot: u64,
        direction: &Direction
    )
    where
        I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>, <R as IntoIterator>::IntoIter: DoubleEndedIterator,
    {

        let range: Box<dyn Iterator<Item=i32>> = if direction == &Direction::Reverse {
            Box::new(range.clone().into_iter().rev())
        } else {
            Box::new(range.clone().into_iter())
        };

        for i in range {

            if i < 1 || i > 1001 {
                // Skip out of range indices
                continue;
            }

            if i % 7 == 0 {
                // Skip every 7th item
                continue;
            }

            let mut op_and_sequences = operations_and_sequences_for(col, i);

            if op_and_sequences.is_empty() {
                panic!("No operations found for index {}", i);
            }

            match iter.next() {
                Some(Ok((key, value))) => {
                    let mut validated = false;
                    for (op, seq) in op_and_sequences {
                        if seq > snapshot {
                            continue; // Skip operations with sequence numbers greater than the snapshot
                        } else {
                            assert_eq!(key, op.internal_key(seq), "Key mismatch at index {}", i);
                            assert_eq!(value, op.value(), "Value mismatch at index {}", i);
                            validated = true;
                            break; // Break after the first valid operation
                        }
                    };
                    if !validated {
                        panic!("No valid operation found for index {} with snapshot {}", i, snapshot);
                    }
                }
                Some(Err(e)) => panic!("Unexpected error at index {}: {:?}", i, e),
                None => panic!("Iterator ended prematurely at index {}", i),
            }
        }
        // Ensure the iterator is exhausted
        assert!(iter.next().is_none(), "Iterator should be exhausted");
    }

    fn operations_and_sequences_for(col: u32, i: i32) -> Vec<(Operation, u64)> {

        if i % 7 == 0 {
            return vec![]; // Skip every 7th item
        }

        let mut vec = Vec::new();

        if i % 5 == 0 {
            // Delete operation
            vec.push((delete_op(col, i), i as u64 + 6000));
        }
        if i % 3 == 0 {
            // Override operation
            vec.push((put_op(col, i, 2), i as u64 + 3000));
        }
        // Initial put operation
        vec.push((put_op(col,i, 1), i as u64));
        vec
    }

    fn assert_search_eq(
        reader: &SSTableReader,
        key: &[u8],
        snapshot: u64,
        expected: Option<&(Vec<u8>, Vec<u8>)>,
    ) {
        let result = reader.read(key, snapshot);
        match (result, expected) {
            (Ok(actual), _exp) => { assert_eq!(actual.as_ref(), expected) }
            (Err(e), _) => panic!("Search returned error: {:?}", e),
        }
    }

    fn key_range<R>(col: u32,
                    range: &R,
                    snapshot: u64,
                    direction: Direction
    ) -> Rc<InternalKeyRange>
    where R: RangeBounds<i32>,
    {
        let start_bound = to_user_key_bound(&range.start_bound());
        let end_bound =  to_user_key_bound(&range.end_bound());
        let interval = Interval::new(start_bound, end_bound);
        encode_internal_key_range(col, 0, &interval, snapshot, direction)
    }

    fn to_user_key_bound(bound: &Bound<&i32>) -> Bound<Vec<u8>> {
        match *bound {
            Bound::Included(id) => Bound::Included(Bson::Int32(*id).try_into_key().unwrap()),
            Bound::Excluded(id) => Bound::Excluded(Bson::Int32(*id).try_into_key().unwrap()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn record_key(col: u32, id: i32) -> Vec<u8> {
        let user_key = &Bson::Int32(id).try_into_key().unwrap();
        encode_record_key(col, 0, &user_key)
    }
}
