use moka::sync::Cache;
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
    sync::{Arc, RwLock},
};
use std::io::Write;

#[derive(Clone, Debug)]
pub struct SharedFile {
    file: Arc<RwLock<File>>,
}

/// File Descriptor Cache with LRU-based eviction
pub struct FileDescriptorCache {
    cache: Cache<String, SharedFile>,
}

impl FileDescriptorCache {
    /// Create a new FD cache with a given max size
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Cache::new(capacity as u64),
        }
    }

    /// Get or open an FD, using the cache if possible
    pub fn get_or_open(&self, path: &str) -> io::Result<SharedFile> {
        // Try getting an FD from the cache
        if let Some(file) = self.cache.get(&path.to_string()) {
            return Ok(file);
        }

        // Open a new FD and cache it
        let file = File::open(Path::new(path))?;
        let shared_file = SharedFile{ file: Arc::new(RwLock::new(file))};
        self.cache.insert(path.to_string(), shared_file.clone());

        Ok(shared_file)
    }

    /// Read a block of data from a cached FD (Opens a new FD if locked on Windows)
    pub fn read_block(&self, path: &str, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0; size];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let shared_file = self.get_or_open(path)?;
            let file = shared_file.file.read().unwrap(); // Read lock (non-blocking)
            file.read_at(&mut buffer, offset)?;
        }

        #[cfg(windows)]
        {
            let shared_file = self.get_or_open(path)?;

            // Try getting a write lock (since seek modifies state)
            let file_guard = shared_file.try_write();

            if let Ok(mut file) = file_guard {
                // FD is free → Use cached FD
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut buffer)?;
            } else {
                // FD is locked → Open a new FD for this read (no caching)
                let mut temp_file = File::open(Path::new(path))?;
                temp_file.seek(SeekFrom::Start(offset))?;
                temp_file.read_exact(&mut buffer)?;
            }
        }

        Ok(buffer)
    }

    pub fn read_length(&self, path: &str) -> io::Result<u64> {
        let shared_file = self.get_or_open(path)?;
        let file = shared_file.file.read().unwrap();
        Ok(file.metadata()?.len())
    }
}

impl Write for SharedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self.file.write().unwrap(); // Acquire write lock
        file.write(buf) // Delegate to File
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut file = self.file.write().unwrap();
        file.flush()
    }
}