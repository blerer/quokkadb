use std::io::Result;
use std::mem;
use crate::storage::sstable::BlockHandle;
use crate::io::varint;

/// BlockBuilder builds a block with shared prefix compression and restart points.
///
/// # Block Format
/// A block consists of multiple key-value entries, compressed using shared-prefix encoding.
/// The block ends with:
/// +--------------------+------------------------------+--------------------------+
/// | Restart points     | Number of restarts (4 bytes) | CRC32 Checksum (4 bytes) |
/// +--------------------+------------------------------+--------------------------+
pub struct BlockBuilder<T, W: EntryWriter<T>> {
    data: Vec<u8>,                // Stores encoded key-value entries.
    entry_writer: W,               // Writes entries using shared-prefix encoding.
    restarts: Vec<u32>,            // Restart point offsets.
    prev_key: Vec<u8>,             // Previous key for shared prefix encoding.
    prev_value: Option<T>,         // Previous value for delta encoding.
    restart_interval: usize,       // Number of entries before a restart point.
    counter: usize,                // Entry counter since last restart point.
    first_key: Option<Vec<u8>>,    // First key in the block.
}

impl<T, W: EntryWriter<T>> BlockBuilder<T, W> {
    /// Create a new BlockBuilder with the given restart interval.
    pub fn new(restart_interval: usize, entry_writer: W) -> Self {
        Self {
            data: Vec::new(),
            entry_writer,
            restarts: vec![0], // First restart point is at offset 0.
            prev_key: Vec::new(),
            prev_value: None,
            restart_interval,
            counter: 0,
            first_key: None,
        }
    }

    /// Add a key-value pair to the block.
    pub fn add(&mut self, key: &[u8], value: T) -> Result<()> {

        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }

        let mut shared = 0;

        if self.counter == 0 {
            // Start a new restart point.
            self.restarts.push(self.data.len() as u32);
        } else {
            shared = self.shared_prefix_length(&self.prev_key, key);
        }

        self.entry_writer.write(key, shared, &value, &mut self.prev_value, &mut self.data);

        // Update state.
        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);
        self.counter += 1;
        if self.counter == self.restart_interval {
            self.counter = 0;
            self.prev_value = None;
        } else {
            self.prev_value = Some(value);
        }
        Ok(())
    }

    /// Finalize the block by adding restart points and returning the firs_key and the full block.
    ///  This method will
    pub fn finish(&mut self) -> Result<(Option<Vec<u8>>, Vec<u8>)> {
        // Append restart points.
        for &restart in &self.restarts {
            self.data.extend(&restart.to_le_bytes());
        }
        self.data.extend(&(self.restarts.len() as u32).to_le_bytes());

        // Take the block and the first_key resetting the builder to start a new block.
        let block = mem::take(&mut self.data);
        let first_key = mem::take(&mut self.first_key);
        self.restarts.clear();
        self.restarts.push(0);
        self.prev_key.clear();
        self.counter = 0;

        Ok((first_key, block))
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
}


/// Trait for writing entries into a block with different formats.
pub trait EntryWriter<T> {
    fn write(&self,
             key: &[u8],
             shared: usize,
             value: &T,
             prev_value: &Option<T>,
             data: &mut Vec<u8>);
}

/// Writes key-value entries for data blocks.
///
/// # Data block entry Format
/// +--------------------------+------------------------------+------------+-------+
/// | Shared key prefix length | Non-shared key suffix length | Key Suffix | Value |
/// +--------------------------+------------------------------+-- ---------+-------+
pub struct DataEntryWriter;

impl EntryWriter<Vec<u8>> for DataEntryWriter {
    fn write(
        &self,
        key: &[u8],
        shared: usize,
        value: &Vec<u8>,
        _: &Option<Vec<u8>>,
        data: &mut Vec<u8>,
    ) {

        let non_shared = key.len() - shared;

        // Encode shared prefix length, non-shared length, and value length.
        varint::write_u64(shared as u64, data);
        varint::write_u64(non_shared as u64, data);

        // No need to store the value length as the value is a BSON document which starts with its length:
        // +-------------------------+--------------   -+-------------------+
        // | Document length (int32) | Element list ... | unsigned byte (0) |
        // +-------------------------+--------------   -+-- ----------------+
        //

        // Add non-shared key and value to the block.
        data.extend(&key[shared..]);
        data.extend(value);
    }
}

/// Writes key-value entries for index blocks.
///
/// # Index block entry Format
/// +--------------------------+------------------------------+------------+-----------------------+
/// | shared key prefix length | non-shared key suffix length | key Suffix | block handle or delta |
/// +--------------------------+------------------------------+------- ----+-----------------------+
pub struct IndexEntryWriter;

impl EntryWriter<BlockHandle> for IndexEntryWriter {
    fn write(
        &self,
        key: &[u8],
        shared: usize,
        value: &BlockHandle,
        prev_value: &Option<BlockHandle>,
        data: &mut Vec<u8>,
    ) {

        let non_shared = key.len() - shared;

        // Encode shared prefix length and non-shared length.
        varint::write_u64(shared as u64, data);
        varint::write_u64(non_shared as u64, data);

        // Add the non-shared key.
        data.extend(&key[shared..]);

        // Add offset and size (or delta for size).
        if let Some(prev) = prev_value {
            let size_delta = value.size as isize - prev.size as isize;
            varint::write_i64(size_delta as i64, data);
        } else {
            varint::write_u64(value.offset, data);
            varint::write_u64(value.size, data);
        }
    }
}
