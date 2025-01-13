use std::io::{Error};
use crc32fast::Hasher;
use crate::util::compressor::{Compressor, CompressorType};
use crate::util::varint;


pub struct BlockHandle {
    pub offset: usize,
    pub size: usize,
}

impl BlockHandle {
    pub fn new(offset: usize, size: usize) -> Self {
        Self { offset, size }
    }
}

/// BlockBuilder builds a block with shared prefix compression and restart points.
pub struct BlockBuilder<T, W: EntryWriter<T>> {
    data: Vec<u8>,
    entry_writer: W,
    compressor: Box<dyn Compressor>,
    restarts: Vec<u32>,
    prev_key: Vec<u8>,
    prev_value: Option<T>,
    restart_interval: usize,
    counter: usize,
    first_key: Option<Vec<u8>>,
}

impl<T, W: EntryWriter<T>> BlockBuilder<T, W> {
    /// Create a new BlockBuilder with the given restart interval.
    pub fn new(restart_interval: usize, entry_writer: W, compressor: Box<dyn Compressor>) -> Self {
        Self {
            data: Vec::new(),
            entry_writer,
            compressor,
            restarts: vec![0], // First restart point is at offset 0.
            prev_key: Vec::new(),
            prev_value: None,
            restart_interval,
            counter: 0,
            first_key: None,
        }
    }

    /// Add a key-value pair to the block.
    pub fn add(&mut self, key: &[u8], value: T) -> Result<(), Error> {

        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }

        let mut shared = 0;

        if self.counter < self.restart_interval {
            shared = self.shared_prefix_length(&self.prev_key, key);
        } else {
            // Start a new restart point.
            self.restarts.push(self.data.len() as u32);
            self.counter = 0;
        }

        self.entry_writer.write(key, shared, &value, &mut self.prev_value, &mut self.data)?;

        // Update state.
        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);
        self.prev_value = if self.counter == 0 { None } else { Some(value) };
        self.counter += 1;
        Ok(())
    }

    /// Finalize the block by adding restart points and returning the full block.
    pub fn finish(&mut self) -> Result<Vec<u8>, Error> {
        // Append restart points.
        for &restart in &self.restarts {
            self.data.extend(&restart.to_le_bytes());
        }

        // Append the number of restart points.
        self.data.extend(&(self.restarts.len() as u32).to_le_bytes());

        // Compute CRC32 checksum for the block data.
        let mut hasher = Hasher::new();
        hasher.update(&self.data);
        let checksum = hasher.finalize();

        // Append checksum to the block.
        self.data.extend(&checksum.to_le_bytes());

        self.compressor.compress(&self.data)
    }

    /// Reset the builder to start a new block.
    pub fn reset(&mut self) {
        self.data.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.prev_key.clear();
        self.counter = 0;
        self.first_key = None;
    }

    /// Calculate the shared prefix length between the last key and the new key.
    fn shared_prefix_length(&self, last_key: &[u8], key: &[u8]) -> usize {
        let mut i = 0;
        while i < last_key.len() && i < key.len() && last_key[i] == key[i] {
            i += 1;
        }
        i
    }

    /// Estimate the current uncompressed block size in bytes.
    pub fn estimated_size_in_bytes(&self) -> usize {
        self.data.len()
            + (self.restarts.len() * size_of::<u32>()) // restart array size
            + size_of::<usize>() // size of the array size (usize)
            + size_of::<u32>() // CRC32 size
    }

    /// Check if the block is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn first_key(&self) -> Option<&Vec<u8>> {
        self.first_key.as_ref()
    }

    pub fn compression_type(&self) -> CompressorType {
        self.compressor.compressor_type()
    }
}

pub trait EntryWriter<T> {
    fn write(&self,
             key: &[u8],
             shared: usize,
             value: &T,
             prev_value: &Option<T>,
             data: &mut Vec<u8>) -> Result<(), Error>;
}

pub struct DataEntryWriter;

impl EntryWriter<Vec<u8>> for DataEntryWriter {
    fn write(
        &self,
        key: &[u8],
        shared: usize,
        value: &Vec<u8>,
        _: &Option<Vec<u8>>,
        data: &mut Vec<u8>,
    ) -> Result<(), Error> {

        let non_shared = key.len() - shared;

        // Encode shared prefix length, non-shared length, and value length.
        varint::write_u64(shared as u64, data)?;
        varint::write_u64(non_shared as u64, data)?;
        varint::write_u64(value.len() as u64, data)?;

        // Add non-shared key and value to the block.
        data.extend(&key[shared..]);
        data.extend(value);

        Ok(())
    }
}

pub struct IndexEntryWriter;

impl EntryWriter<BlockHandle> for IndexEntryWriter {
    fn write(
        &self,
        key: &[u8],
        shared: usize,
        value: &BlockHandle,
        prev_value: &Option<BlockHandle>,
        data: &mut Vec<u8>,
    ) -> Result<(), Error> {

        let non_shared = key.len() - shared;

        // Encode shared prefix length and non-shared length.
        varint::write_u64(shared as u64, data)?;
        varint::write_u64(non_shared as u64, data)?;

        // Add the non-shared key.
        data.extend(&key[shared..]);

        // Add offset and size (or delta for size).
        if let Some(prev) = prev_value {
            let size_delta = value.size - prev.size;
            varint::write_u64(size_delta as u64, data)?;
        } else {
            varint::write_u64(value.offset as u64, data)?;
            varint::write_u64(value.size as u64, data)?;
        }

        Ok(())
    }
}
