use moka::sync::Cache;
use std::sync::Arc;
use std::io::Error;
use std::path::Path;
use tracing::{debug, error, info};
use crate::options::options::DatabaseOptions;
use crate::storage::sstable::sstable_reader::SSTableReader;

/// SSTable reader cache (an LRU cache).
pub struct SSTableCache {
    cache: Cache<String, Result<Arc<SSTableReader>, Arc<Error>>>, // Key = file_path
}

impl SSTableCache {

    /// Create a new SSTableCache
    pub fn new(options: &DatabaseOptions) -> Self {
        let cache_size = options.max_open_files() as u64;
        let cache = Cache::new(cache_size);
        info!("SSTableReaderCache initialized with a capacity of {} files", cache_size);

        Self { cache }
    }

    /// Retrieve the specified SSTableReader, creating it by reading the file on disk if necessary
    pub fn get(&self, file: &Path) -> Result<Arc<SSTableReader>, Error> {

        let key = file.to_string_lossy().into_owned();

        // Fetch or insert the sstable reader in a thread-safe manner
        let sstable_reader= self.cache.get_with(key.clone(), || {
            debug!("Loading sstable reader from {}", &key);

            let reader = SSTableReader::open(&file);

            if let Err(e) = reader {
                error!("Error loading sstable reader from {}:: {}", &key, e);
                return Err(Arc::new(e))
            }

            Ok(Arc::new(reader?))
        });

        sstable_reader.map_err(|arc_err| Error::new(arc_err.kind(), arc_err.to_string()))  // Clone so each thread gets its own Result<Arc<Vec<u8>>>
    }
}