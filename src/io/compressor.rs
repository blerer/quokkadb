use lz4::block;
use snap::write::FrameEncoder;
use snap::read::FrameDecoder;
use std::io::{ErrorKind, Read, Write};
use std::io::{Error};
use std::sync::Arc;

/// Enumeration representing supported compressor types.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CompressorType {
    /// No compression.
    Noop,
    /// Snappy compression.
    Snappy,
    /// LZ4 compression.
    LZ4,
}

impl From<CompressorType> for u8 {
    /// Converts a `CompressorType` into its corresponding numeric representation.
    fn from(item: CompressorType) -> Self {
        match item {
            CompressorType::Noop => 0,
            CompressorType::Snappy => 1,
            CompressorType::LZ4 => 2,
        }
    }
}

impl TryFrom<u8> for CompressorType {
    type Error = &'static str;

    /// Attempts to convert a numeric value into a `CompressorType`.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressorType::Noop),
            1 => Ok(CompressorType::Snappy),
            2 => Ok(CompressorType::LZ4),
            _ => Err("Invalid value for CompressorType"),
        }
    }
}

impl CompressorType {
    pub(crate) fn new_compressor(&self) -> Arc<dyn Compressor> {
        match self {
            CompressorType::Noop => Arc::new(NoopCompressor),
            CompressorType::Snappy => Arc::new(SnappyCompressor),
            CompressorType::LZ4 => Arc::new(Lz4Compressor),
        }
    }
}

/// Trait defining the behavior of a compressor.
pub trait Compressor: Send + Sync {
    /// Returns the type of compressor being used.
    fn compressor_type(&self) -> CompressorType;

    /// Compresses the given data and returns the compressed result.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Error>;

    /// Decompresses the given data and returns the decompressed result.
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, Error>;
}

/// Compressor implementation for Snappy.
pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Snappy
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(data)?;
        encoder.into_inner().map_err(|e| Error::new(ErrorKind::Other, e))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let mut decoder = FrameDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }
}

/// Compressor implementation for LZ4.
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compressor_type(&self) -> CompressorType {
        CompressorType::LZ4
    }

    /// Compresses data using LZ4 compression, including the uncompressed size as a prefix.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        let compressed = block::compress(data, None, false)?;
        let mut result = (data.len() as u32).to_le_bytes().to_vec(); // Include size
        result.extend(compressed);
        Ok(result)
    }

    /// Decompresses data using LZ4 compression, extracting the uncompressed size from the prefix.
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if data.len() < 4 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Data too short to include uncompressed size",
            ));
        }

        // Extract the uncompressed size (first 4 bytes)
        let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        // Decompress the remaining data
        block::decompress(&data[4..], Some(size as i32))
    }
}

/// Compressor implementation for Noop (no compression).
pub struct NoopCompressor;

impl Compressor for NoopCompressor {

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Noop
    }

    /// Simply returns the input data without modification.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }

    /// Simply returns the input data without modification.
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_compressor() {
        let compressor = NoopCompressor;
        let data = br#"{
            "key1": "value1",
            "key2": "value2",
            "key3": ["value3", "value3", "value3"],
            "key4": {
                "nested_key": "nested_value",
                "nested_array": ["nested1", "nested1", "nested1"]
            }
        }"#;

        let compressed = compressor.compress(data).expect("Compression failed");
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed).expect("Decompression failed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compressor() {
        let compressor = SnappyCompressor;
        let data = br#"{
            "key1": "value1",
            "key2": "value2",
            "key3": ["value3", "value3", "value3"],
            "key4": {
                "nested_key": "nested_value",
                "nested_array": ["nested1", "nested1", "nested1"]
            }
        }"#;

        let compressed = compressor.compress(data).expect("Compression failed");
        let decompressed = compressor.decompress(&compressed).expect("Decompression failed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compressor() {
        let compressor = Lz4Compressor;
        let data = br#"{
            "key1": "value1",
            "key2": "value2",
            "key3": ["value3", "value3", "value3"],
            "key4": {
                "nested_key": "nested_value",
                "nested_array": ["nested1", "nested1", "nested1"]
            }
        }"#;

        let compressed = compressor.compress(data).expect("Compression failed");
        let decompressed = compressor.decompress(&compressed).expect("Decompression failed");
        assert_eq!(decompressed, data);
    }
}