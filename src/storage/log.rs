use std::{fmt, mem, result};
use std::fs::{File, OpenOptions};
use std::io::{Result, ErrorKind, Read, Write, Error};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crc32fast::Hasher;
use crate::io::buffer::Buffer;
use crate::io::{file_name_as_str, ZeroCopy};
use crate::storage::files::DbFile;

/// The size ot the block used in the log file. The 4KB block optimize write efficiency by
/// aligning with disk block sizes.
const BUFFER_SIZE_IN_BYTES: usize = 4096;

pub struct Log {

    directory: PathBuf, // The path to the log directory
    file_creator: Arc<dyn LogFileCreator>,
    current_file: LogFile,
    buffer: Buffer,
}

impl Log {

    pub fn new(path: &Path, file: DbFile, file_creator: Arc<dyn LogFileCreator>) -> Result<Log> {

        let directory = path.to_path_buf();
        let current_file = file_creator.new_log(directory.clone(), file)?;

        Ok(Log {
            directory,
            file_creator,
            current_file,
            buffer: Buffer::with_capacity(BUFFER_SIZE_IN_BYTES),
        })
    }

    pub fn load_from(file_path: &Path, file_creator: Arc<dyn LogFileCreator>) -> Result<Log> {

        let file = DbFile::new(&file_path).ok_or(Error::new(ErrorKind::InvalidData, "Unknown file type"))?;
        let db_path = file_path.parent().ok_or(Error::new(ErrorKind::InvalidData, "Cannot retrieve the database directory"))?
            .to_path_buf();

        let current_file = LogFile::load_from(file_path, file.id)?;

        Ok(Log {
            directory: db_path,
            file_creator,
            current_file,
            buffer: Buffer::with_capacity(BUFFER_SIZE_IN_BYTES),
        })
    }

    /// Returns the computed size of the current log file. That number includes only the bytes that
    /// have been sync to disk.
    pub fn log_file_size(&self) -> u64 {
        self.current_file.file_size
    }

    /// Returns the path to the current log file.
    pub fn filename(&self) -> Option<String> {
        file_name_as_str(&self.current_file.path).map(|s| s.to_string())
    }

    pub fn rotate(&mut self, new_log: DbFile) -> Result<(PathBuf, PathBuf)> {
        // Before syncing to the disk we want to pad the buffer to ensure that we fill the last block.
        self.buffer.pad();
        self.sync()?;

        let new_file = self.file_creator.new_log(self.directory.clone(), new_log)?;
        let old_file = mem::replace(&mut self.current_file, new_file);
        Ok((self.current_file.path.clone(), old_file.path.clone()))
    }

    pub fn append(&mut self, data: &[u8]) -> Result<usize> {

        let mut bytes_written = 0;
        let len = data.len();

        bytes_written += self.flush_buffer_if_needed(len)?;

        // Write record size
        let size = data.len() as u32;
        let size_as_bytes = size.to_be_bytes();

        bytes_written += self.append_to_buffer(&size_as_bytes)?;

        // Write the size CRC
        let size_crc = compute_crc32(&size_as_bytes);
        bytes_written += self.append_to_buffer(&size_crc)?;

        // Write record data
        bytes_written += self.append_to_buffer(&data)?;

        // Write the record data CRC
        let data_crc = compute_crc32(&data);
        bytes_written += self.append_to_buffer(&data_crc)?;

        Ok(bytes_written)
    }

    fn append_to_buffer(&mut self, bytes : &[u8]) -> Result<usize> {

        let to_write = bytes.len();
        let mut written = 0;
        let mut writable = self.buffer.writable_bytes();
        while writable < (to_write - written) {
            self.buffer.write_slice(&bytes[written..written + writable]);
            self.flush_buffer()?;
            written += writable;
            writable = self.buffer.writable_bytes();
        }
        self.buffer.write_slice(&bytes[written..]);
        Ok(to_write)
    }

    /// Write the buffer to the underlying file if the there are not enough space available in
    /// the buffer to add the specified number of bytes.
    fn flush_buffer_if_needed(&mut self, size: usize) -> Result<usize> {

        let remaining = self.buffer.writable_bytes();
        if remaining < size {
            self.buffer.pad();
            self.flush_buffer()?;
            return Ok(remaining)
        }
        Ok(0)
    }

    /// Write the buffer content to the underlying file.
    fn flush_buffer(&mut self) -> Result<()> {
        self.current_file.write_all(&self.buffer.read_slice(self.buffer.readable_bytes()))?;
        // If we reached the end of the buffer we want to reset it as our block is completed.
        // If not we can still write to it until the block is full.
        if self.buffer.writable_bytes() == 0 {
            self.buffer.clear();
        }
        Ok(())
    }

    /// Sync data to the disk. A sync does not pad the content of the current buffer to the block
    /// size.
    pub fn sync(&mut self) -> Result<()> {
        self.flush_buffer()?;
        self.current_file.sync()?;
        Ok(())
    }

    pub fn read_log_file(
        path: PathBuf,
        header_size: usize
    ) -> result::Result<(Vec<u8>, impl Iterator<Item=result::Result<Vec<u8>, LogReplayError>>), LogReplayError > {

        let mut file = File::open(path.clone())?;

        let mut block = vec![0u8; BUFFER_SIZE_IN_BYTES];
        file.read_exact(&mut block).map_err( |_e| LogReplayError::Corruption {
            record_offset: 0,
            reason: format!("Not enough bytes to read {} header.", path.display()),
        })?;

        let mut header = vec![0; header_size];
        header.copy_from_slice(&block[..header_size]);

        let expected_crc32 = compute_crc32(&header);
        let actual_crc32 = &block[header_size..header_size + 4];

        if actual_crc32 != expected_crc32 {
            return Err(LogReplayError::Corruption {
                record_offset: 0,
                reason: format!("Invalid checksum found in {} header.", path.display()),
            })
        }

        let log_iter = LogIterator::new(path, file);

        Ok((header, log_iter))
    }
}

pub trait LogFileCreator: Send + Sync  {

    fn new_log(&self, directory_path: PathBuf, db_file: DbFile) -> Result<LogFile> {

        let path = directory_path.join(db_file.filename());

        let file = OpenOptions::new()
            .write(true)
            .append(true) // Append instead of overwrite
            .create(true) // Create if it doesn't exist
            .open(path.clone())?;


        let header = self.header(db_file.id);
        let crc = compute_crc32(&header);
        let mut buffer = Buffer::with_capacity(BUFFER_SIZE_IN_BYTES);
        buffer.write_slice(&header);
        buffer.write_slice(&crc);
        buffer.pad();

        let mut log_file = LogFile{ id: db_file.id, path, file, pending_bytes: 0, file_size: 0 };
        log_file.write_all(buffer.as_slice())?;
        log_file.sync()?;

        Ok(log_file)
    }

    fn header(&self, id: u64) -> Vec<u8>;
}

impl Drop for Log {
    fn drop(&mut self) {
        self.buffer.pad();
        if let Err(e) = self.sync() {
            eprintln!("Failed to sync log during drop: {}", e);
            // Typically, errors in Drop cannot propagate. Log and handle appropriately.
        }
    }
}

pub struct LogFile {
    id: u64,
    path: PathBuf,
    file: File,
    pending_bytes: u64,
    file_size: u64,
}

impl LogFile {

    fn load_from(path: &Path, id: u64) -> Result<LogFile> {

        let file = OpenOptions::new()
            .write(true)
            .append(true) // Append instead of overwrite
            .create(true) // Create if it doesn't exist
            .open(path)?;

        let file_size = file.metadata()?.len();

        Ok(LogFile{
            id,
            path: path.to_path_buf(),
            file,
            pending_bytes: 0,
            file_size,
        })

    }
    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
        self.pending_bytes += bytes.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        self.file_size += self.pending_bytes;
        self.pending_bytes = 0;
        Ok(())
    }
}

pub struct LogIterator {
    path: PathBuf,
    reader: File,
    buffer: Buffer,
    position: u64, // The record start position.
}

impl LogIterator {
    fn new(path: PathBuf, reader: File) -> Self {
        let buffer = Buffer::with_capacity(BUFFER_SIZE_IN_BYTES);
        Self {
            path,
            reader,
            buffer,
            position: BUFFER_SIZE_IN_BYTES as u64,
        }
    }

    fn refill_buffer(&mut self) -> Result<bool> {
        let remaining = self.buffer.readable_bytes();
        self.buffer.fill(&mut self.reader)?;
        // Update the position with the skipped bytes
        self.position += remaining as u64;
        Ok(self.buffer.readable_bytes() > 0)
    }
}

impl Iterator for LogIterator {
    type Item = result::Result<Vec<u8>, LogReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // There should be either enough readable bytes for the size + its CRC or we need to move to the
            // next block
            if self.buffer.readable_bytes() < 8 {
                match self.refill_buffer() {
                    Ok(false) => return None, // EOF reached
                    Ok(true) => continue,
                    Err(e) => return Some(Err(e.into())), // propagate error
                }
            }

            let size_as_bytes = self.buffer.read_slice(4);

            // If all the size bytes are zero it means that it is the block padding and that we
            // need to move to the next block
            if size_as_bytes == &[0; 4] {
                match self.refill_buffer() {
                    Ok(false) => return None, // EOF reached
                    Ok(true) => {
                        // We need to account for the 4 bytes that have been read.
                        // The skipping will be accounted for by the refill_buffer method
                        self.position += 4;
                        continue
                    },
                    Err(e) => return Some(Err(e.into())), // propagate error
                }
            }

            let size = size_as_bytes.read_u32_be(0) as usize;

            let expected_crc = compute_crc32(size_as_bytes);
            let actual_crc = self.buffer.read_slice(4);

            if expected_crc != actual_crc {
                return Some(Err(LogReplayError::Corruption {
                    record_offset: self.position,
                    reason: format!("Invalid size checksum found in {}. Stopping replay.",
                                    &self.path.to_string_lossy()),
                }))
            }

            let mut data = vec![0u8; size];
            let mut offset = 0;

            while offset < size {
                if self.buffer.readable_bytes() == 0 {
                    match self.refill_buffer() {
                        Ok(false) =>  {
                            return Some(Err(LogReplayError::Corruption {
                                record_offset: self.position,
                                reason: format!("Reached an unexpected end of file in {} while reading data. Stopping replay.",
                                                &self.path.to_string_lossy()),
                            }))
                        },
                        Ok(true) => continue,
                        Err(e) => return Some(Err(e.into())), // propagate error
                    }
                }
                let available = (size - offset).min(self.buffer.readable_bytes());
                data[offset..offset + available].copy_from_slice(&self.buffer.read_slice(available));
                offset += available;
            }

            if self.buffer.readable_bytes() < 4 {
                match self.refill_buffer() {
                    Ok(false) =>  {
                        return Some(Err(LogReplayError::Corruption {
                            record_offset: self.position,
                            reason: format!("Reached an unexpected end of file in {} while reading data crc. Stopping replay.",
                                            &self.path.to_string_lossy()),
                        }))
                    },
                    Ok(true) => continue,
                    Err(e) => return Some(Err(e.into())), // propagate error
                }
            }

            let expected_crc = compute_crc32(&data);
            let actual_crc = self.buffer.read_slice(4);
            if expected_crc != actual_crc {
                return Some(Err(LogReplayError::Corruption {
                    record_offset: self.position,
                    reason: format!("Invalid data checksum found in {}. Stopping replay.",
                                    &self.path.to_string_lossy()),
                }))
            }

            // Update the position with the record total record size:
            // size (4 bytes) + size crc (4 bytes) + data (size) + data crc (4 bytes)
            self.position += 4 + 4 +  size as u64 + 4;
            return Some(Ok(data));
        }
    }
}

#[derive(Debug)]
pub enum LogReplayError {
    Io(Error),
    Corruption { record_offset: u64, reason: String },
}

impl fmt::Display for LogReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogReplayError::Io(err) => write!(f, "I/O error: {}", err),
            LogReplayError::Corruption { record_offset, reason } => {
                write!(f, "Corruption at offset {}: {}", record_offset, reason)
            }
        }
    }
}

impl From<Error> for LogReplayError {
    fn from(e: Error) -> Self {
        LogReplayError::Io(e)
    }
}

impl From<LogReplayError> for Error {
    fn from(e: LogReplayError) -> Self {
        match e {
            LogReplayError::Io(inner) => inner,
            LogReplayError::Corruption { record_offset: offset, reason } => {
                Error::new(ErrorKind::InvalidData, format!("Corruption at offset {}: {}", offset, reason))
            }
        }
    }
}

fn compute_crc32(data: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(&data);
    let checksum = hasher.finalize();
    checksum.to_le_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::sync::Arc;
    use crate::io::truncate_file;

    struct MockLogFileCreator;

    impl LogFileCreator for MockLogFileCreator {

        fn header(&self, _id: u64) -> Vec<u8> {
            Vec::from(b"HEADER")
        }
    }

    #[test]
    fn test_drop() {
        let dir = tempdir().unwrap();

        let path = dir.path();

        let file_creator = Arc::new(MockLogFileCreator);
        let mut log = Log::new(path, DbFile::new_write_ahead_log(1), file_creator).unwrap();

        let data = &vec![1; 250];
        log.append(data).unwrap();

        drop(log);

        let log_file_path = path.join("000001.log");
        assert!(log_file_path.exists());

        let metadata = std::fs::metadata(log_file_path.clone()).unwrap();
        assert_eq!(metadata.len(), 2 * BUFFER_SIZE_IN_BYTES as u64); // 2 blocks should have been written (header + data)

        let (header, entries) = Log::read_log_file(log_file_path, "HEADER".len()).unwrap();

        assert_eq!(header, b"HEADER");

        let entries: Vec<Vec<u8>> = entries.map(|r| r.unwrap()).collect();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], vec![1; 250]);
    }

    #[test]
    fn test_log_rotation() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let file_creator = Arc::new(MockLogFileCreator);
        let mut log = Log::new(path, DbFile::new_write_ahead_log(6), file_creator.clone()).unwrap();

        log.append(&vec![1; 250]).unwrap();
        assert_eq!(log.rotate(DbFile::new_write_ahead_log(7)).unwrap(), (path.join("000007.log"), path.join("000006.log")));
        log.append(&vec![2; 360]).unwrap();

        let first_log = path.join("000006.log");
        let second_log = path.join("000007.log");

        assert!(first_log.exists());
        let metadata = std::fs::metadata(first_log).unwrap();
        assert_eq!(metadata.len(), 2 * BUFFER_SIZE_IN_BYTES as u64); // 2 blocks should have been written (header + data)

        assert!(second_log.exists());
        let metadata = std::fs::metadata(second_log.clone()).unwrap();
        assert_eq!(metadata.len(), BUFFER_SIZE_IN_BYTES as u64); // Only the Header block should have been written to disk at that point.

        log.sync().unwrap(); // It should flush the buffer and sync everything to disk
        let metadata = std::fs::metadata(second_log.clone()).unwrap();
        assert_eq!(metadata.len(),  BUFFER_SIZE_IN_BYTES as u64 + 372);
    }

    #[test]
    fn test_log_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let file_creator = Arc::new(MockLogFileCreator);
        let mut log = Log::new(path, DbFile::new_write_ahead_log(9), file_creator.clone()).unwrap();

        assert_eq!(log.append(&vec![1; 250]).unwrap(), 262);
        assert_eq!(log.append(&vec![2; 360]).unwrap(), 372);
        assert_eq!(log.append(&vec![3; 690]).unwrap(), 702);
        assert_eq!(log.append(&vec![4; 1024]).unwrap(), 1036);
        assert_eq!(log.append(&vec![5; 1024]).unwrap(), 1036); // buffer remaining 688 next one will not fit in
        assert_eq!(log.append(&vec![6; 1024]).unwrap(), 688 + 1036);
        assert_eq!(log.append(&vec![7; 3048]).unwrap(), 3060); // Fill perfectly the remaining part of the block
        assert_eq!(log.append(&vec![8; 4084]).unwrap(), 4096); // Fill the next block fully
        assert_eq!(log.append(&vec![9; 9000]).unwrap(), 9012); // Fill more than 2 blocks
        log.sync().unwrap();

        let log_file_path = path.join("000009.log");
        assert!(log_file_path.exists());
        let (header, entries) = Log::read_log_file(log_file_path, "HEADER".len()).unwrap();

        assert_eq!(header, b"HEADER");

        let entries: Vec<Vec<u8>> = entries.map(|r| r.unwrap()).collect();

        assert_eq!(entries.len(), 9);
        assert_eq!(entries[0], vec![1; 250]);
        assert_eq!(entries[1], vec![2; 360]);
        assert_eq!(entries[2], vec![3; 690]);
        assert_eq!(entries[3], vec![4; 1024]);
        assert_eq!(entries[4], vec![5; 1024]);
        assert_eq!(entries[5], vec![6; 1024]);
        assert_eq!(entries[6], vec![7; 3048]);
        assert_eq!(entries[7], vec![8; 4084]);
        assert_eq!(entries[8], vec![9; 9000]);
    }

    #[test]
    fn test_replay_stop_checksum_mismatch() {

        let dir = tempdir().unwrap();
        let path = dir.path();

        let file_creator = Arc::new(MockLogFileCreator);
        let mut log = Log::new(path, DbFile::new_write_ahead_log(10), file_creator.clone()).unwrap();

        log.append(&vec![1; 250]).unwrap();
        log.sync().unwrap();

        let log_file_path = path.join("000010.log");

        // Append invalid entry
        let mut file = OpenOptions::new()
            .write(true)
            .append(true) // Append instead of overwrite
            .create(true) // Create if it doesn't exist
            .open(log_file_path.clone()).unwrap();

        file.write_all(&vec![0, 0, 0, 10, 0, 0, 0, 0, 3, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Replay
        assert!(log_file_path.exists());
        let (header, mut entries) = Log::read_log_file(log_file_path.clone(), "HEADER".len()).unwrap();

        assert_eq!(header, b"HEADER");

        assert_eq!(entries.next().unwrap().unwrap(), vec![1; 250]);
        let rs = entries.next().unwrap();
        let expected_offset = 4096 + 4 + 4 + 250 + 4;
        match rs {
            Err(LogReplayError::Corruption { record_offset: offset, reason: _ }) => {
                assert_eq!(expected_offset, offset);
            }
            _ => panic!("Unexpected result"),
        }

        // Truncate the file and check that it can be safely replayed after
        truncate_file(&log_file_path, expected_offset as u64).unwrap();
    }

    #[test]
    fn test_replay_stop_checksum_mismatch_after_padding() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let file_creator = Arc::new(MockLogFileCreator);
        let mut log = Log::new(path, DbFile::new_write_ahead_log(11), file_creator.clone()).unwrap();

        let mut record_offset = 4096;

        record_offset += log.append(&vec![1; 250]).unwrap();
        record_offset += log.append(&vec![2; 360]).unwrap();
        record_offset += log.append(&vec![3; 690]).unwrap();
        record_offset += log.append(&vec![4; 1024]).unwrap();
        record_offset += log.append(&vec![5; 1024]).unwrap(); // buffer remaining 688 next one will not fit in
        record_offset += log.append(&vec![6; 1024]).unwrap();
        log.sync().unwrap();

        let log_file_path = path.join("000011.log");

        // Append invalid entry
        let mut file = OpenOptions::new()
            .write(true)
            .append(true) // Append instead of overwrite
            .create(true) // Create if it doesn't exist
            .open(log_file_path.clone()).unwrap();

        let expected_offset = record_offset as u64;

        assert_eq!(expected_offset, file.metadata().unwrap().len());

        file.write_all(&vec![0, 0, 0, 10, 0, 0, 0, 0, 3, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let (header, mut entries) = Log::read_log_file(log_file_path, "HEADER".len()).unwrap();

        assert_eq!(header, b"HEADER");

        assert_eq!(entries.next().unwrap().unwrap(), vec![1; 250]);
        assert_eq!(entries.next().unwrap().unwrap(), vec![2; 360]);
        assert_eq!(entries.next().unwrap().unwrap(), vec![3; 690]);
        assert_eq!(entries.next().unwrap().unwrap(), vec![4; 1024]);
        assert_eq!(entries.next().unwrap().unwrap(), vec![5; 1024]);
        assert_eq!(entries.next().unwrap().unwrap(), vec![6; 1024]);

        let rs = entries.next().unwrap();
        match rs {
            Err(LogReplayError::Corruption { record_offset: offset, reason: _ }) => {
                assert_eq!(expected_offset, offset);
            }
            _ => panic!("Unexpected result"),
        }
    }

    #[test]
    fn test_replay_stop_invalid_header() {

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let log_file_path = path.join("000012.log");

        // Append invalid header
        let mut file = OpenOptions::new()
            .write(true)
            .append(true) // Append instead of overwrite
            .create(true) // Create if it doesn't exist
            .open(log_file_path.clone()).unwrap();

        file.write_all(&vec![0, 0, 0, 10, 0, 0, 0, 0, 3, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Replay
        assert!(log_file_path.exists());
        let rs = Log::read_log_file(log_file_path, 64);
        match rs {
            Err(LogReplayError::Corruption { record_offset: offset, reason : _ }) => {
                assert_eq!(0, offset);
            }
            _ => panic!("Unexpected result"),
        }
    }

    #[test]
    fn test_log_load_from() {

        // Setup
        let dir = tempdir().unwrap();
        let path = dir.path();
        let file_creator = Arc::new(MockLogFileCreator);

        // Create the original Log
        let mut log = Log::new(path, DbFile::new_write_ahead_log(42), file_creator.clone()).unwrap();
        log.append(b"some test data").unwrap();
        log.sync().unwrap();

        let original_path = log.current_file.path.clone();
        let original_file_size = log.current_file.file_size;
        let original_id = log.current_file.id;

        // Now reload it
        let reloaded_log = Log::load_from(&original_path, file_creator.clone()).unwrap();

        // Validate
        assert_eq!(reloaded_log.current_file.id, original_id);
        assert_eq!(reloaded_log.current_file.path, original_path);
        assert_eq!(reloaded_log.current_file.file_size, original_file_size);
    }
}
