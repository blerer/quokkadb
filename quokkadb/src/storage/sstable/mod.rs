use std::fmt;

pub mod block_builder;
pub mod block_cache;
mod block_reader;
pub mod sstable_cache;
mod sstable_properties;
pub mod sstable_reader;
pub mod sstable_writer;

/// The SSTable files magic number
pub const MAGIC_NUMBER: &u64 = &0x88e241b785f4cff7u64;

/// The SSTable current version
pub const SSTABLE_CURRENT_VERSION: u8 = 1;

/// The length of the SSTable footer
pub const SSTABLE_FOOTER_LENGTH: u64 = 48;

/// Represents a reference to a block within a file.
/// When they are not delta encoded, BlockHandles are encoded in SSTable files as:
/// +-----------------+---------------+
/// | Offset (varint) | Size (varint) |
/// +-----------------+---------------+
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BlockHandle {
    pub offset: u64, // Byte offset of the block in the file.
    pub size: u64,   // Size of the block in bytes.
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }
}

impl fmt::Display for BlockHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlockHandle {{ offset: {}, size: {} }}",
            self.offset, self.size
        )
    }
}
