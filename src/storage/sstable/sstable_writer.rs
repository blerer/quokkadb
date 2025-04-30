use std::io::{Write, BufWriter, Result};
use std::path::Path;
use std::sync::Arc;
use crate::io::checksum::{ChecksumStrategy, Crc32ChecksumStrategy};
use crate::io::compressor::Compressor;
use crate::io::fd_cache::{FileDescriptorCache, SharedFile};
use crate::util::bloom_filter::BloomFilter;
use crate::options::options::Options;
use crate::storage::sstable::block_builder::{BlockBuilder, DataEntryWriter, IndexEntryWriter};
use crate::storage::sstable::{BlockHandle, MAGIC_NUMBER, SSTABLE_CURRENT_VERSION, SSTABLE_FOOTER_LENGTH};
use crate::storage::sstable::sstable_properties::{SSTableProperties, SSTablePropertiesBuilder};
use crate::io::varint;
use crate::storage::files::DbFile;
use crate::storage::lsm_tree::SSTableMetadata;

/// A builder for creating Sorted String Tables (SSTables).
///
/// An SSTable is a file format used in key-value storage systems to store data persistently on disk.
/// It organizes data into sorted blocks, allowing efficient lookups and range queries. SSTables
/// are immutable once written, which simplifies concurrency and ensures write efficiency.
///
/// The SSTable format consists of:
/// - **Data Blocks**: Store key-value pairs, sorted by keys.
/// - **Index Block**: Maps key ranges to the corresponding data blocks.
/// - **Metaindex Block**: Contains metadata about optional features like Bloom filters.
/// - **Footer**: Includes block handles for the index and metaindex blocks, along with a magic number for format validation.
///
/// This builder provides methods to add data, construct the necessary metadata, and finalize the SSTable.
pub struct SSTableWriter<'a> {
    id: u64,
    data_block_builder: BlockBuilder<Vec<u8>, DataEntryWriter>,
    index_block_builder: BlockBuilder<BlockHandle, IndexEntryWriter>,
    metaindex_block_builder: BlockBuilder<BlockHandle, IndexEntryWriter>,
    bloom_filter: BloomFilter<'a>,
    output: BufWriter<SharedFile>,
    block_size: usize,
    current_block_offset: usize, // Tracks the file offset of the current block.
    properties_builder: SSTablePropertiesBuilder,
    compressor: Arc<dyn Compressor>,
    checksum_strategy: Arc<dyn ChecksumStrategy>,
}

impl<'a> SSTableWriter<'a> {
    /// Creates a new SSTableWriter instance.
    ///
    /// # Arguments
    /// - `file_path`: The file path for the SSTable.
    /// - `options`: Configuration options for the SSTable and compression.
    /// - `expected_keys`: Estimated number of keys for configuring the Bloom filter.
    ///
    /// # Returns
    /// A new instance of `SSTableWriter` configured with the provided options.
    pub fn new(
        fd_cache:Arc<FileDescriptorCache>,
        directory: &Path,
        db_file: &DbFile,
        options: &Options,
        expected_keys: usize
    ) -> Result<Self> {

        let id = db_file.id;
        let file_path = directory.join(db_file.filename());
        let file = fd_cache.get_or_open(&file_path)?;

        let sstable_options = options.sstable_options();
        let restart_interval = sstable_options.restart_interval();

        let compressor_type = sstable_options.compressor_type();
        let compressor = compressor_type.new_compressor();
        let data_block_builder = BlockBuilder::new(restart_interval, DataEntryWriter);
        let index_block_builder = BlockBuilder::new(restart_interval, IndexEntryWriter);
        let metaindex_block_builder = BlockBuilder::new(restart_interval, IndexEntryWriter);

        let database_options = options.database_options();
        let file_write_buffer_size = database_options.file_write_buffer_size();

        Ok(Self {
            id,
            data_block_builder,
            index_block_builder,
            metaindex_block_builder,
            bloom_filter: BloomFilter::new(expected_keys, sstable_options.bloom_filter_false_positive()),
            output: BufWriter::with_capacity(file_write_buffer_size.to_bytes(), file),
            block_size: sstable_options.block_size().to_bytes(),
            current_block_offset: 0,
            properties_builder: SSTablePropertiesBuilder::new(SSTABLE_CURRENT_VERSION, u8::from(compressor_type)),
            compressor,
            checksum_strategy: Arc::new(Crc32ChecksumStrategy),
        })
    }

    /// Adds a key-value pair to the SSTable.
    ///
    /// This method writes the key-value pair into the current data block. If the block size exceeds
    /// the configured maximum, the block is flushed to disk, and a new block is started.
    ///
    /// # Arguments
    /// - `key`: The key as a byte slice.
    /// - `value`: The value as a byte slice.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {

        if self.data_block_builder.estimated_size_in_bytes() + key.len() + value.len() >= self.block_size {
            self.flush_data_block()?
        }

        self.properties_builder.with_entry(&key, value.len());

        self.data_block_builder.add(&key, value.to_vec())?;
        self.bloom_filter.add(&key);

        Ok(())
    }

    /// Finalizes the SSTable by flushing all remaining data and writing metadata.
    ///
    /// This method writes the remaining data block (if any), constructs the index and metaindex
    /// blocks, and writes the footer to complete the SSTable.
    pub fn finish(&mut self) -> Result<SSTableMetadata> {
        if !self.data_block_builder.is_empty() {
            self.flush_data_block()?;
        }

        let properties = self.properties_builder.build();

        let index_handle = self.write_index_block()?;
        let bloom_filter_handle = self.write_bloom_filter()?;
        let properties_handle = self.write_properties(&properties)?;

        self.add_to_metaindex("filter.quokkadb.BloomFilter", bloom_filter_handle)?;
        self.add_to_metaindex("properties", properties_handle)?;

        let metaindex_handle = self.write_metaindex_block()?;
        self.write_footer(index_handle, metaindex_handle)?;
        Ok(SSTableMetadata::new(
            self.id,
            0,
            &properties.min_key,
            &properties.max_key,
            properties.min_sequence,
            properties.max_sequence
        ))
    }

    /// Flushes the current data block to the output file.
    ///
    /// This method finalizes the current block, writes it to disk, and adds its information to
    /// the index block. It also resets the data block builder for the next block.
    fn flush_data_block(&mut self) -> Result<()> {
        let (first_key, block) = self.data_block_builder.finish()?;
        let handle = self.finalize_and_write_block(block)?;

        // Update the properties
        self.properties_builder.with_data_block(handle.size as usize);

        // Record the first key and its offset in the index block
        if let Some(key) = first_key {
            self.index_block_builder.add(&key, handle)?;
        }
        Ok(())
    }

    /// Writes the index block to the SSTable.
    ///
    /// The index block maps key ranges to their corresponding data blocks.
    fn write_index_block(&mut self) -> Result<BlockHandle> {
        let (_, data) = self.index_block_builder.finish()?;
        self.finalize_and_write_block(data)
    }

    /// Writes the metaindex block to the SSTable.
    ///
    /// The metaindex block contains handles to metadata blocks, such as the Bloom filter block.
    fn write_metaindex_block(&mut self) -> Result<BlockHandle> {
        let (_, data) = self.metaindex_block_builder.finish()?;
        self.finalize_and_write_block(data)
    }

    /// Writes the Bloom filter to the SSTable.
    ///
    /// The Bloom filter is a probabilistic data structure that reduces the number of disk reads
    /// during key lookups by quickly ruling out non-existent keys.
    fn write_bloom_filter(&mut self) -> Result<BlockHandle> {
        let data = self.bloom_filter.to_block();
        self.finalize_and_write_block(data)
    }

    /// Writes the SSTable properties to the SSTable.
    fn write_properties(&mut self, properties: &SSTableProperties) -> Result<BlockHandle> {
        let data = properties.to_vec()?;
        self.finalize_and_write_block(data)
    }

    fn finalize_and_write_block(&mut self, block: Vec<u8>) -> Result<BlockHandle> {
        let mut block = self.append_checksum(block);
        block = self.compressor.compress(&block)?;
        self.write_block(block)
    }

    fn write_block(&mut self, block: Vec<u8>) -> Result<BlockHandle> {
        let handle = BlockHandle::new(self.current_block_offset as u64, block.len() as u64);
        self.output.write_all(&block)?;
        self.current_block_offset += block.len();
        Ok(handle)
    }

    fn append_checksum(&mut self, mut data: Vec<u8>) -> Vec<u8> {
        let checksum = self.checksum_strategy.checksum(&data);
        data.extend(&checksum);
        data
    }

    /// Adds an entry to the metaindex block.
    ///
    /// This method adds a mapping between a metadata key (e.g., "filter.builtin.BloomFilter") and
    /// its block handle to the metaindex block.
    fn add_to_metaindex(&mut self, key: &str, handle: BlockHandle) -> Result<()> {
        self.metaindex_block_builder.add(key.as_bytes(), handle)
    }

    /// Writes the SSTable footer.
    ///
    /// The footer contains handles to the index and metaindex blocks, the compression type,
    /// the SSSTable version and a magic number for identifying SSTable files.
    ///
    /// # Footer Format
    /// +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
    /// | SSTable version (1 byte) | Meta-Index block handle | Index block handle | Compression type (1 byte) | padding to 40 bytes | Magic number (8 bytes) |
    /// +--------------------------+-------------------------+--------------------+---------------------------+---------------------+------------------------+
    fn write_footer(&mut self, index_handle: BlockHandle, metaindex_handle: BlockHandle) -> Result<()> {
        let mut footer = Vec::new();

        // Store the SSTable version
        footer.push(SSTABLE_CURRENT_VERSION) ;

        // Serialize the metaindex block handle.
        self.write_block_handle(&metaindex_handle, &mut footer);

        // Serialize the index block handle.
        self.write_block_handle(&index_handle, &mut footer);

        // Store the compression type
        footer.push(u8::from(self.compressor.compressor_type()));

        // Add padding and magic number (pad to footer length - magic number size).
        let padding = vec![0; (SSTABLE_FOOTER_LENGTH  - 8u64) as usize - footer.len()];
        footer.extend(padding);
        footer.extend(MAGIC_NUMBER.to_le_bytes());

        self.output.write_all(&footer)?;
        self.output.flush()?;
        Ok(())
    }

    /// Writes a block handle into the provided buffer.
    ///
    /// A block handle consists of the block's offset and size, encoded as varints.
    fn write_block_handle(&self, handle: &BlockHandle, buffer: &mut Vec<u8>) {
        varint::write_u64(handle.offset, buffer);
        varint::write_u64(handle.size, buffer);
    }
}