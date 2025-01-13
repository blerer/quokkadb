// use crc32fast::Hasher;
// use regex::Regex;
// use std::fs;
// use std::fs::File;
// use std::io;
// use std::io::{Bytes, Error, Write};
// use std::path::{Path, PathBuf};
// use crate::write_batch::WriteBatch;
//
// pub struct WriteAheadLog {
//
//     path: Path,
//     active_file: WriteAheadLogFile,
//
// }
//
// impl WriteAheadLog {
//
//     pub fn new(path: &Path) -> WriteAheadLog {
//
//         let log_files = Self::load_existing_files(path);
//
//         if log_files.is_empty() {
//
//         }
//         // Return WriteAheadLog
//     }
//
//     pub fn log(&mut self, write_batch: WriteBatch) -> io::Result<WriteBatch> {
//         self.active_file.write(write_batch)
//     }
//
//     fn load_existing_files(path: &Path) -> Vec<WriteAheadLogFile> {
//
//         let mut log_files: Vec<WriteAheadLogFile> = Vec::new();
//
//         let entries = fs::read_dir(path).expect(&format!("The wal directory {} could not be read", &path));
//         for entry in entries {
//             let entry = entry.unwrap();
//             let path = entry.path();
//             if WriteAheadLogFile::is_write_ahead_log_file(&path) {
//
//             }
//         }
//         log_files
//     }
//
// }
//
//
//
// struct WriteAheadLogFile {
//
//     path: PathBuf,
//     file: File,
//     buffer: [u8],
//     buffer_offset: u32,
// }
//
// impl WriteAheadLogFile {
//
//     /// The Regex pattern used to identify log files.
//     /// Log files are named as <number>.log where the number that reflect the log files sequence
//     /// is padded to 6 digits. To ensure that file names are lexicographically ordered.
//     const LOG_FILE_PATTERN: &'static str = r"^\d{6}\.log$";
//
//     /// The size ot the block used in the wal file. The 4KB block optimize write efficiency by
//     /// aligning with disk block sizes.
//     const BUFFER_SIZE_IN_BYTES: u32 = 4096;
//
//     /// The size of the header of a batch (size + size CRC  Record size + CRC32)
//     const BATCH_HEADER_IN_BYTES: u32 = 64 + 32 + 64;
//
//     /// Check if the path is the one of a log file
//     fn is_write_ahead_log_file(path: &Path) -> bool {
//
//         if !path.is_file() {
//            return false; // Early return if the path is not a file
//         }
//
//         let file_name = match path.file_name().and_then(|os_str| os_str.to_str()) {
//             Some(name) => name,
//             None => return false, // Return false if the file name is invalid
//         };
//
//         let regexp = Regex::new(Self::LOG_FILE_PATTERN).unwrap(); // We trust the pattern
//         regexp.is_match(file_name)
//     }
//
//     fn write(&mut self, batch: WriteBatch) -> io::Result<WriteBatch> {
//
//         let bytes = batch.to_bytes();
//
//         // Write data size
//         let size = bytes.len() as u32;
//         let size_as_bytes = size.to_ne_bytes();
//
//         self.append_to_buffer(size_as_bytes)?;
//
//         // Write the size CRC
//         let size_crc = crc32fast::hash(&size_as_bytes);
//         self.append_to_buffer(size_crc)?;
//
//         // Write batch data
//         self.append_to_buffer(bytes)?;
//
//         // Write the batch data CRC
//         let data_crc = crc32fast::hash(&bytes);
//         self.append_to_buffer(data_crc)?;
//
//         Ok(batch)
//     }
//
//     fn append_to_buffer(&mut self, bytes : &[u8]) -> Result<(), io::Error> {
//
//         let size = bytes.len() as u32;
//         self.flush_buffer_if_needed(size)?;
//         self.buffer[self.buffer_offset .. self.buffer_offset + size].copy_from_slice(bytes);
//         self.buffer_offset += size;
//     }
//
//     /// Write the buffer to the underlying file if the there are not enough space available in
//     /// the buffer to add the specified number of bytes.
//     fn flush_buffer_if_needed(&mut self, size: u32) -> Result<(), Error> {
//
//         if (self.buffer_offset + size) > Self::BUFFER_SIZE_IN_BYTES {
//             self.flush_buffer()?;
//         }
//         Ok(())
//     }
//
//     /// Write the buffer to the underlying file.
//     fn flush_buffer(&mut self) -> Result<(), Error> {
//         self.file.write_all(&self.buffer[0..self.buffer_offset])?;
//         self.buffer_offset = 0;
//         Ok(())
//     }
// }
