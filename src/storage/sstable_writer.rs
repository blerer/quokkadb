use std::fs::File;
use std::io::{Write, BufWriter, Result};
use std::mem;
use crate::storage::block_builder::{BlockBuilder, BlockHandle, DataEntryWriter, IndexEntryWriter};
use crate::util::bloom_filter::BloomFilter;
use crate::options::options::Options;
use crate::storage::compound_key::CompoundKey;
use crate::storage::sstable_properties::SSTablePropertiesBuilder;
use crate::util::varint;

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
pub struct SSTableWriter {
    data_block_builder: BlockBuilder<Vec<u8>, DataEntryWriter>,
    index_block_builder: BlockBuilder<BlockHandle, IndexEntryWriter>,
    metaindex_block_builder: BlockBuilder<BlockHandle, IndexEntryWriter>,
    bloom_filter: BloomFilter,
    output: BufWriter<File>,
    block_size: usize,
    current_block_offset: usize, // Tracks the file offset of the current block.
    builder: SSTablePropertiesBuilder,
}

/// The SSTable files magic number
pub const MAGIC_NUMBER: &u64 = &0x88e241b785f4cff7u64;

/// The SSTable current version
pub const SSTABLE_CURRENT_VERSION: &str = "1.0";

impl SSTableWriter {
    /// Creates a new SSTableWriter instance.
    ///
    /// # Arguments
    /// - `file_path`: The file path for the SSTable.
    /// - `options`: Configuration options for the SSTable and compression.
    /// - `expected_keys`: Estimated number of keys for configuring the Bloom filter.
    ///
    /// # Returns
    /// A new instance of `SSTableWriter` configured with the provided options.
    pub fn new(file_path: &str, options: Options, expected_keys: usize) -> Result<Self> {
        let file = File::create(file_path)?;

        let sstable_options = options.sstable_options();
        let restart_interval = sstable_options.restart_interval();

        let compressor_type = sstable_options.compressor_type();
        let compressor = compressor_type.new_compressor();
        let data_block_builder = BlockBuilder::new(restart_interval, DataEntryWriter, compressor);
        let index_compressor = compressor_type.new_compressor();
        let index_block_builder = BlockBuilder::new(restart_interval, IndexEntryWriter, index_compressor);
        let metaindex_compressor = sstable_options.compressor_type().new_compressor();
        let metaindex_block_builder = BlockBuilder::new(restart_interval, IndexEntryWriter, metaindex_compressor);

        let database_options = options.database_options();
        let file_write_buffer_size = database_options.file_write_buffer_size();

        Ok(Self {
            data_block_builder,
            index_block_builder,
            metaindex_block_builder,
            bloom_filter: BloomFilter::new(expected_keys, sstable_options.bloom_filter_false_positive()),
            output: BufWriter::with_capacity(file_write_buffer_size.to_bytes(), file),
            block_size: sstable_options.block_size().to_bytes(),
            current_block_offset: 0,
            builder: SSTablePropertiesBuilder::new(SSTABLE_CURRENT_VERSION.to_string(), u8::from(compressor_type)),
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
    pub fn add(&mut self, key: &CompoundKey, value: &[u8]) -> Result<()> {

        let key_bytes = key.to_vec();

        if self.data_block_builder.estimated_size_in_bytes() + key_bytes.len() + value.len() >= self.block_size {
            self.flush_data_block()?
        }

        self.builder.with_entry(&key, key_bytes.len(), value.len());

        self.data_block_builder.add(&key_bytes, value.to_vec())?;
        self.bloom_filter.add(&key_bytes);

        Ok(())
    }

    /// Finalizes the SSTable by flushing all remaining data and writing metadata.
    ///
    /// This method writes the remaining data block (if any), constructs the index and metaindex
    /// blocks, and writes the footer to complete the SSTable.
    pub fn finish(&mut self) -> Result<()> {
        if !self.data_block_builder.is_empty() {
            self.flush_data_block()?;
        }

        let index_handle = self.write_index_block()?;
        let bloom_filter_handle = self.write_bloom_filter()?;
        let properties_handle = self.write_properties()?;

        self.add_to_metaindex("filter.quokkadb.BloomFilter", bloom_filter_handle)?;
        self.add_to_metaindex("properties", properties_handle)?;

        let metaindex_handle = self.write_metaindex_block()?;
        self.write_footer(index_handle, metaindex_handle)
    }

    /// Flushes the current data block to the output file.
    ///
    /// This method finalizes the current block, writes it to disk, and adds its information to
    /// the index block. It then resets the data block builder for the next block.
    fn flush_data_block(&mut self) -> Result<()> {
        let block = self.data_block_builder.finish()?;
        self.output.write_all(&block)?;

        self.builder.with_data_block(block.len());

        // Record the first key and current offset in the index block.
        if let Some(first_key) = self.data_block_builder.first_key() {
            self.index_block_builder.add(first_key, BlockHandle::new(self.current_block_offset, block.len()))?;
        }

        self.current_block_offset += block.len();
        self.data_block_builder.reset();
        Ok(())
    }

    /// Writes the index block to the SSTable.
    ///
    /// The index block maps key ranges to their corresponding data blocks.
    fn write_index_block(&mut self) -> Result<BlockHandle> {
        let index_block = self.index_block_builder.finish()?;
        self.write_block(index_block)
    }

    /// Writes the metaindex block to the SSTable.
    ///
    /// The metaindex block contains handles to metadata blocks, such as the Bloom filter block.
    fn write_metaindex_block(&mut self) -> Result<BlockHandle> {
        let metaindex_block = self.metaindex_block_builder.finish()?;
        self.write_block(metaindex_block)
    }

    /// Writes the Bloom filter to the SSTable.
    ///
    /// The Bloom filter is a probabilistic data structure that reduces the number of disk reads
    /// during key lookups by quickly ruling out non-existent keys.
    fn write_bloom_filter(&mut self) -> Result<BlockHandle> {
        let bloom_data = mem::take(&mut self.bloom_filter).to_vec();
        self.write_block(bloom_data)
    }

    /// Writes the SSTable properties to the SSTable.
    fn write_properties(&mut self) -> Result<BlockHandle> {
        let properties_data = self.builder.build().to_vec()?;
        self.write_block(properties_data)
    }

    fn write_block(&mut self, block: Vec<u8>) -> Result<BlockHandle> {
        let handle = BlockHandle::new(self.current_block_offset, block.len());
        self.output.write_all(&block)?;
        self.current_block_offset += block.len();
        Ok(handle)
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
    /// The footer contains handles to the index and metaindex blocks, along with a magic number
    /// for identifying the SSTable format.
    fn write_footer(&mut self, index_handle: BlockHandle, metaindex_handle: BlockHandle) -> Result<()> {
        let mut footer = Vec::new();

        // Serialize the metaindex block handle.
        self.write_block_handle(&metaindex_handle, &mut footer)?;

        // Serialize the index block handle.
        self.write_block_handle(&index_handle, &mut footer)?;

        // Add padding and magic number.
        let padding = vec![0; 40 - footer.len()];
        footer.extend(padding);
        footer.extend(MAGIC_NUMBER.to_le_bytes());

        self.output.write_all(&footer)
    }

    /// Writes a block handle into the provided buffer.
    ///
    /// A block handle consists of the block's offset and size, encoded as varints.
    fn write_block_handle(&self, handle: &BlockHandle, buffer: &mut Vec<u8>) -> Result<()> {
        varint::write_u64(handle.offset as u64, buffer)?;
        varint::write_u64(handle.size as u64, buffer)?;
        Ok(())
    }
}