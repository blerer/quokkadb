use moka::sync::Cache;
use std::sync::Arc;
use std::io::Error;
use std::time::Instant;
use crate::io::checksum::ChecksumStrategy;
use crate::options::options::DatabaseOptions;
use crate::storage::sstable::BlockHandle;
use crate::io::compressor::Compressor;
use crate::obs::logger::LoggerAndTracer;
use crate::storage::sstable::sstable_reader::SharedFile;

/// Block Cache (LRU, retrieves blocks using the shared file provided as a get parameter)
pub struct BlockCache {
    logger: Arc<dyn LoggerAndTracer>,
    cache: Cache<(String, u64), Result<Arc<Vec<u8>>, Arc<Error>>>, // Key = (file_path, block_handle)
}

impl BlockCache {
    /// Create a new Block Cache
    fn new(logger: Arc<dyn LoggerAndTracer>, options: &DatabaseOptions) -> Self {
        let cache_size = options.block_cache_size.to_bytes() as u64;
        let cache = Cache::builder()
            .max_capacity(cache_size)
            .weigher(|_, block: &Result<Arc<Vec<u8>>, Arc<Error>>| {
                match block {
                    Ok(block) => block.len() as u32,
                    Err(_) => 1, // Errors take minimal space
                }
            })
            .build();

        logger.info(format_args!("BlockCache initialized with {} bytes", cache_size));

        Self { logger, cache }
    }

    /// Retrieve the specified block, loading it from disk if necessary
    pub fn get(&self,
               compressor: &Arc<dyn Compressor>,
               checksum_strategy: &Arc<dyn ChecksumStrategy>,
               file: &SharedFile,
               block_handle: &BlockHandle
    ) -> Result<Arc<Vec<u8>>, Error> {

        let key = (file.path.to_string_lossy().into_owned(), block_handle.offset);

        // Fetch or insert the block in a thread-safe manner
        let block = self.cache.get_with(key.clone(), || {
            let path = file.path.to_string_lossy();
            let start = Instant::now();
            self.logger.event(format_args!("event: loading and decompressing sstable block start, path={}, handle={}", path, block_handle));

            // Read, decompress, and validate the block
            let uncompressed_block = self.read_and_decompress(compressor, checksum_strategy, file, block_handle);

            if let Err(e) = uncompressed_block {
                self.logger.error(format_args!("Error loading block from {} at offset {}: {}", path, block_handle.offset, e));
                return Err(Arc::new(e))
            }

            let uncompressed_block = uncompressed_block?;
            let duration = start.elapsed();
            self.logger.event(format_args!("event: loading and decompressing stable block end, size={}, duration={}ms", uncompressed_block.len(), duration.as_millis()));

            Ok(Arc::new(uncompressed_block))
        });

        block.map_err(|arc_err| Error::new(arc_err.kind(), arc_err.to_string()))  // Clone so each thread gets its own Result<Arc<Vec<u8>>>
    }

    fn read_and_decompress(&self,
                           compressor: &Arc<dyn Compressor>,
                           checksum_strategy: &Arc<dyn ChecksumStrategy>,
                           file: &SharedFile,
                           block_handle: &BlockHandle
    ) -> Result<Vec<u8>, Error> {

        let compressed_block = file.read_block(block_handle.offset, block_handle.size as usize)?;
        let mut uncompressed_block = compressor.decompress(&compressed_block)?;
        let checksum_offset = uncompressed_block.len() - checksum_strategy.checksum_size();
        checksum_strategy.verify_checksum(&uncompressed_block[..checksum_offset], &uncompressed_block[checksum_offset..])?;
        uncompressed_block.truncate(checksum_offset);
        Ok(uncompressed_block)
    }
}