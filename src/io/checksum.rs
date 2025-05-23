use crc32fast::Hasher;
use std::io::{Error, ErrorKind, Result};

pub trait ChecksumStrategy: Send + Sync {
    fn checksum_size(&self) -> usize;

    fn checksum(&self, data: &[u8]) -> Vec<u8>;

    fn verify_checksum(&self, data: &[u8], checksum: &[u8]) -> Result<()>;
}

pub struct Crc32ChecksumStrategy;

impl ChecksumStrategy for Crc32ChecksumStrategy {
    fn checksum_size(&self) -> usize {
        4
    }

    fn checksum(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();
        checksum.to_le_bytes().into()
    }

    fn verify_checksum(&self, data: &[u8], checksum: &[u8]) -> Result<()> {
        // Compute the CRC32 checksum for the data block
        let computed_checksum = self.checksum(&data);

        // Validate
        if computed_checksum != checksum {
            Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Invalid checksum: expected {:?} was {:?}",
                    checksum, computed_checksum
                ),
            ))
        } else {
            Ok(())
        }
    }
}
