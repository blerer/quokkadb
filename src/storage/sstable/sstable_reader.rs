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
use crate::storage::internal_key::extract_operation_type;
use std::fs::File;
use std::io::{Error, ErrorKind, Result};
#[cfg(windows)]
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use ErrorKind::InvalidData;
use std::char::ToUppercase;
use std::cmp::Ordering;
use std::mem::take;
use std::ops::RangeBounds;
use crate::{event, info};
use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::storage::Direction;
use crate::storage::files::DbFile;
use crate::storage::internal_key::{encode_internal_key, extract_record_key, extract_sequence_number};
use crate::storage::operation::OperationType;
use crate::util::interval::Interval;

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
                event!(self.logger, "scanning_data_block key={:?}", &record_key);
                let mut iter = data_reader.scan_forward_from(&internal_key)?;
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
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
        snapshot: u64,
        direction: Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> {
        // Fetch and parse the index block
        let index_block = self.get_block(&self.index_handle)?;
        let index_reader = BlockReader::new(index_block, IndexEntryReader)?;

        match direction {
            Direction::Forward => {
                let mut index_iter = if let Some(lower_bound) = &lower_bound {
                    index_reader.scan_forward_from(&lower_bound)?
                } else {
                    index_reader.scan_all_forward()?
                };

                let data_iter = match index_iter.next() {
                    Some(Ok((index_key, handle))) => {
                        let data_block = self.block_loader.load_block(&handle)?;
                        let data_reader = BlockReader::new(data_block, DataEntryReader)?;
                        Some(if let Some(lower_bound) = &lower_bound {
                            data_reader.scan_forward_from(lower_bound)?
                        } else {
                            data_reader.scan_all_forward()?
                        })
                    },
                    Some(Err(e)) => return Err(e),
                    None => return Ok(Box::new(std::iter::empty())),
                };

                Ok(Box::new(SSTableRangeScanIterator {
                    block_loader: self.block_loader.clone(),
                    lower_bound,
                    upper_bound,
                    snapshot,
                    direction,
                    index_iter,
                    current_data_block_iter: data_iter,
                    previous: None,
                }))
            }
            Direction::Reverse => {
                let mut index_iter = if let Some(upper_bound) = &upper_bound {
                    index_reader.scan_reverse_from(&upper_bound)?
                } else {
                    index_reader.scan_all_reverse()?
                };

                let data_iter = match index_iter.next() {
                    Some(Ok((index_key, handle))) => {
                        let data_block = self.block_loader.load_block(&handle)?;
                        let data_reader = BlockReader::new(data_block, DataEntryReader)?;
                        Some(if let Some(upper_bound) = &upper_bound {
                            data_reader.scan_reverse_from(upper_bound)?
                        } else {
                            data_reader.scan_all_reverse()?
                        })
                    },
                    Some(Err(e)) => return Err(e),
                    None => return Ok(Box::new(std::iter::empty())),
                };

                Ok(Box::new(SSTableRangeScanIterator {
                    block_loader: self.block_loader.clone(),
                    lower_bound,
                    upper_bound,
                    snapshot,
                    direction,
                    index_iter,
                    current_data_block_iter: data_iter,
                    previous: None,
                }))
            }
        }
    }
}

struct SSTableRangeScanIterator {
    block_loader: Arc<BlockLoader>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    snapshot: u64,
    direction: Direction,
    index_iter: Box<dyn Iterator<Item = Result<(Vec<u8>, BlockHandle)>>>,
    current_data_block_iter: Option<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>>,
    previous: Option<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for SSTableRangeScanIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_data_block_iter.is_none() {
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

                        let new_data_iter_result = if self.direction == Direction::Reverse {
                            data_reader.scan_all_reverse()
                        } else {
                            data_reader.scan_all_forward()
                        };

                        match new_data_iter_result {
                            Ok(iter) => self.current_data_block_iter = Some(iter),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None, // No more index entries
                }
            }

            if let Some(ref mut data_iter) = self.current_data_block_iter {
                match data_iter.next() {
                    Some(Ok((internal_key, value))) => {

                        let seq = extract_sequence_number(&internal_key);
                        if seq > self.snapshot {
                            continue;
                        }

                        match self.direction {
                            Direction::Forward => {
                                if let Some(upper_bound) = &self.upper_bound {
                                    if &internal_key >= upper_bound {
                                        return None; // Stop iteration at the upper bound
                                    }
                                }

                                let record_key = extract_record_key(&internal_key);

                                if let Some((prev_key, _prev_value)) = &self.previous {
                                    // If the current record key is the same as the previous one,
                                    // we skip it to avoid duplicates.
                                    if extract_record_key(prev_key) == record_key {
                                        continue;
                                    } else {
                                        return Some(Ok((internal_key, value)));
                                    }

                                }  else {
                                    self.previous = Some((internal_key.clone(), vec!())); // Store only the current key for comparison in the next iteration
                                    return Some(Ok((internal_key, value)));
                                }
                            }
                            Direction::Reverse => {
                                if let Some(lower_bound) = &self.lower_bound {
                                    if &internal_key <= lower_bound {
                                        return None; // Stop iteration at the lower bound
                                    }
                                }

                                let record_key = extract_record_key(&internal_key);

                                if let Some((prev_key, prev_value)) = &self.previous {
                                    // If the current record key is the same as the previous one,
                                    // we skip it to avoid duplicates.
                                    if extract_record_key(prev_key) == record_key {
                                        continue;
                                    } else {
                                        let prev = self.previous.replace((internal_key, value));
                                        return Some(Ok(prev.unwrap()));
                                    }

                                } else {
                                    continue;
                                }

                            }
                        }
                    }
                    Some(Err(e)) => {
                        self.current_data_block_iter = None; // Invalidate on error
                        return Some(Err(e));
                    }
                    None => {
                        self.current_data_block_iter = None; // Current data block exhausted
                        // Loop again to fetch the next data block
                    }
                }
            } else {
                // This state should ideally not be reached if None from index_iter is handled
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
    use bson::{doc, Bson, Document};
    use tempfile::tempdir;
    use crate::obs::logger::test_instance;
    use crate::obs::metrics::MetricRegistry;
    use crate::options::options::Options;
    use crate::storage::files::DbFile;
    use super::*;
    use crate::storage::internal_key::{encode_internal_key, encode_record_key, MAX_SEQUENCE_NUMBER};
    use crate::storage::lsm_version::SSTableMetadata;
    use crate::storage::operation::{Operation, OperationType};
    use crate::storage::sstable::sstable_writer::SSTableWriter;
    use crate::util::bson_utils::{as_key_value, BsonKey};

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

        let operations = vec![
            (put(col, &doc! {"id": 1, "name": "Iron Man", "year": 2008}), 1u64), // 0
            (put(col, &doc! {"id": 2, "name": "The Incredible Hulk", "year": 2008}), 2), // 1
            (put(col, &doc! {"id": 3, "name": "Iron Man 2", "year": 2010}), 3), // 2
            (put(col, &doc! {"id": 4, "name": "Thor", "year": 2011}), 4), // 3
            (delete(col,5), 9), // 4
            (put(col, &doc! {"id": 5, "name": "Captain America: The First Avenger", "year": 2011}), 8), // 5
            (put(col, &doc! {"id": 5, "name": "Captain Omerica: The First Avenger", "year": 2011}), 7), // 6
            (put(col, &doc! {"id": 5, "name": "Captan America: The First Avenger", "year": 2011}), 6), // 7
            (put(col, &doc! {"id": 5, "name": "Captoin America: The First Avenger", "year": 2011}), 5), // 8
            (put(col, &doc! {"id": 7, "name": "The Avengers", "year": 2013}), 11), // 9
            (put(col, &doc! {"id": 7, "name": "The Avengers", "year": 2012}), 10), //10
        ];

        let mut entries = operations.iter().map(|(op, seq)| {
            let internal_key = op.internal_key(*seq);
            (internal_key, op.value().to_vec())
        }).collect::<Vec<_>>();

        let len = operations.len();
        let mut writer = SSTableWriter::new(&path, &sst_file, &options, len).unwrap();

        for entry in entries.iter() {
            writer.add(&entry.0, &entry.1).unwrap();
        }

        let sst = writer.finish().unwrap();

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

    fn assert_search_eq(
        reader: &SSTableReader,
        key: &[u8],
        snapshot: u64,
        expected: Option<&(Vec<u8>, Vec<u8>)>,
    ) {
        let result = reader.read(key, snapshot);
        match (result, expected) {
            (Ok(actual), exp) => { assert_eq!(actual.as_ref(), expected) }
            (Err(e), _) => panic!("Search returned error: {:?}", e),
        }
    }

    fn record_key(col: u32, id: i32) -> Vec<u8> {
        let user_key = &Bson::Int32(id).try_into_key().unwrap();
        encode_record_key(col, 0, &user_key)
    }

    fn put(col: u32, doc: &Document) -> Operation {
        let (k, v) = as_key_value(doc).unwrap();
        Operation::new_put(col, 0, k, v)
    }

    fn delete(col: u32, id: i32) -> Operation {
        let user_key = Bson::Int32(id).try_into_key().unwrap();
        Operation::new_delete(col, 0, user_key)
    }

}
