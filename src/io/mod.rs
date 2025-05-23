pub mod buffer;
pub mod byte_reader;
pub mod byte_writer;
pub mod checksum;
pub mod compressor;
pub mod varint;

use crate::obs::logger::{LogLevel, LoggerAndTracer};
use std::fs::{File, OpenOptions};
use std::io::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, ptr};
use crate::warn;

/// A trait for reading little-endian integers directly from byte slices
/// without additional allocations. These methods perform **zero-copy**
/// reads using `ptr::read_unaligned()`.
pub trait ZeroCopy {
    /// Reads a 32-bit little-endian unsigned integer (`u32`) from the given offset.
    ///
    /// # Panics
    /// - Panics if the offset is **out of bounds**.
    fn read_u32_le(&self, offset: usize) -> u32;

    /// Reads a 32-bit big-endian unsigned integer (`u32`) from the given offset.
    ///
    /// # Panics
    /// - Panics if the offset is **out of bounds**.
    fn read_u32_be(&self, offset: usize) -> u32;

    /// Reads a 64-bit little-endian unsigned integer (`u64`) from the given offset.
    ///
    /// # Panics
    /// - Panics if the offset is **out of bounds**.
    fn read_u64_le(&self, offset: usize) -> u64;

    /// Reads a 64-bit big-endian unsigned integer (`u64`) from the given offset.
    ///
    /// # Panics
    /// - Panics if the offset is **out of bounds**.
    fn read_u64_be(&self, offset: usize) -> u64;

    /// Reads a 32-bit little-endian signed integer (`i32`) from the given offset.
    ///
    /// # Panics
    /// - Panics if the offset is **out of bounds**.
    fn read_i32_le(&self, offset: usize) -> i32;

    /// Reads a 64-bit little-endian signed integer (`i64`) from the given offset.
    ///
    /// # Panics
    /// - Panics if the offset is **out of bounds**.
    fn read_i64_le(&self, offset: usize) -> i64;
}

impl ZeroCopy for [u8] {
    #[inline(always)]
    fn read_u32_le(&self, offset: usize) -> u32 {
        assert!(
            offset + 4 <= self.len(),
            "Offset out of bounds: cannot read u32"
        );
        unsafe { ptr::read_unaligned(self.as_ptr().add(offset) as *const u32) }.to_le()
    }

    #[inline(always)]
    fn read_u32_be(&self, offset: usize) -> u32 {
        assert!(
            offset + 4 <= self.len(),
            "Offset out of bounds: cannot read u32"
        );
        unsafe { ptr::read_unaligned(self.as_ptr().add(offset) as *const u32) }.to_be()
    }

    #[inline(always)]
    fn read_u64_le(&self, offset: usize) -> u64 {
        assert!(
            offset + 8 <= self.len(),
            "Offset out of bounds: cannot read u64"
        );
        unsafe { ptr::read_unaligned(self.as_ptr().add(offset) as *const u64) }.to_le()
    }

    #[inline(always)]
    fn read_u64_be(&self, offset: usize) -> u64 {
        assert!(
            offset + 8 <= self.len(),
            "Offset out of bounds: cannot read u64"
        );
        unsafe { ptr::read_unaligned(self.as_ptr().add(offset) as *const u64) }.to_be()
    }

    #[inline(always)]
    fn read_i32_le(&self, offset: usize) -> i32 {
        assert!(
            offset + 4 <= self.len(),
            "Offset out of bounds: cannot read i32"
        );
        unsafe { ptr::read_unaligned(self.as_ptr().add(offset) as *const i32) }.to_le()
    }

    #[inline(always)]
    fn read_i64_le(&self, offset: usize) -> i64 {
        assert!(
            offset + 8 <= self.len(),
            "Offset out of bounds: cannot read i64"
        );
        unsafe { ptr::read_unaligned(self.as_ptr().add(offset) as *const i64) }.to_le()
    }
}

impl ZeroCopy for Vec<u8> {
    #[inline(always)]
    fn read_u32_le(&self, offset: usize) -> u32 {
        self.as_slice().read_u32_le(offset)
    }

    #[inline(always)]
    fn read_u32_be(&self, offset: usize) -> u32 {
        self.as_slice().read_u32_be(offset)
    }

    #[inline(always)]
    fn read_u64_le(&self, offset: usize) -> u64 {
        self.as_slice().read_u64_le(offset)
    }

    #[inline(always)]
    fn read_u64_be(&self, offset: usize) -> u64 {
        self.as_slice().read_u64_be(offset)
    }

    #[inline(always)]
    fn read_i32_le(&self, offset: usize) -> i32 {
        self.as_slice().read_i32_le(offset)
    }

    #[inline(always)]
    fn read_i64_le(&self, offset: usize) -> i64 {
        self.as_slice().read_i64_le(offset)
    }
}

/// Extracts the file name component from a path and returns it as a string slice (`&str`).
///
/// # Arguments
///
/// * `path` - A path-like type.
///
/// # Returns
///
/// * `Some(&str)` containing the file name if it exists and is valid UTF-8.
/// * `None` if the path has no file name component or if the name is not valid UTF-8.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use quokkadb::io::file_name_as_str;
///
/// assert_eq!(file_name_as_str(Path::new("/foo/bar.txt")), Some("bar.txt"));
/// assert_eq!(file_name_as_str(Path::new("/foo/")), Some("foo"));
/// ```
pub fn file_name_as_str(path: &Path) -> Option<&str> {
    path.file_name()?.to_str()
}

/// Syncs a directory to ensure that all file system operations (e.g., renames, file creations)
/// performed in that directory are durably persisted to disk.
///
/// This is especially important after performing atomic operations like renaming or deleting files,
/// where syncing the directory ensures that the metadata changes survive a crash.
///
/// # Arguments
///
/// * `dir_path` - A path-like reference to the directory to be synced.
///
/// # Returns
///
/// * `Ok(())` if the directory was successfully synced.
/// * An error if opening or syncing the directory fails.
pub fn sync_dir<P: AsRef<Path>>(dir_path: P) -> Result<()> {
    let path = dir_path.as_ref();
    let dir = File::open(path)?;
    dir.sync_all()
}

/// Marks a given file as corrupted by renaming it to add a `.corrupted` extension.
///
/// After renaming, the parent directory is synced to ensure that the rename is durable.
///
/// # Arguments
/// * `logger` - The logger
/// * `file_path` - The path to the file to mark as corrupted.
///
/// # Behavior
/// * If the file has an extension (e.g., `000123.log`), it becomes `000123.log.corrupted`.
/// * If the file has no extension (e.g., `000123`), it becomes `000123.corrupted`.
/// * If the rename fails or the sync fails, an `io::Error` is returned.
pub fn mark_file_as_corrupted(logger: Arc<dyn LoggerAndTracer>, file_path: &Path) -> Result<()> {
    let corrupted_path = compute_corrupted_path(file_path);

    fs::rename(file_path, &corrupted_path)?;

    if let Some(parent) = corrupted_path.parent() {
        sync_dir(parent)?;
    }

    warn!(logger, "Marked WAL file as corrupted: {:?} -> {:?}", file_path, corrupted_path);

    Ok(())
}

fn compute_corrupted_path(file_path: &Path) -> PathBuf {
    file_path.with_extension(match file_path.extension() {
        Some(ext) => {
            let mut ext_str = ext.to_os_string();
            ext_str.push(".corrupted");
            ext_str
        }
        None => "corrupted".into(),
    })
}

/// Truncates a file to the specified size.
///
/// After truncation, the file is synced to ensure durability.
///
/// # Arguments
/// * `file_path` - Path to the file to truncate.
/// * `size` - New desired size in bytes.
///
/// # Errors
/// Returns an `io::Error` if opening, truncating, or syncing the file fails.
pub fn truncate_file(file_path: &Path, size: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(file_path)?;

    file.set_len(size)?; // Truncate the file
    file.sync_all()?; // Ensure metadata and contents are flushed

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::io::{file_name_as_str, mark_file_as_corrupted, truncate_file};
    use crate::obs::logger;
    use std::fs;
    use std::io::ErrorKind;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    mod zero_copy {
        use crate::io::ZeroCopy;
        #[test]
        fn test_read_u32_le_vec() {
            let mut data: Vec<u8> = Vec::new();
            data.extend(0_u32.to_le_bytes());
            data.extend(12_u32.to_le_bytes());
            data.extend(124_u32.to_le_bytes());
            data.extend(u32::MAX.to_le_bytes());
            assert_eq!(data.read_u32_le(0), 0);
            assert_eq!(data.read_u32_le(4), 12);
            assert_eq!(data.read_u32_le(8), 124);
            assert_eq!(data.read_u32_le(12), u32::MAX);
        }

        #[test]
        fn test_read_u32_be_vec() {
            let mut data: Vec<u8> = Vec::new();
            data.extend(0_u32.to_be_bytes());
            data.extend(12_u32.to_be_bytes());
            data.extend(124_u32.to_be_bytes());
            data.extend(u32::MAX.to_be_bytes());
            assert_eq!(data.read_u32_be(0), 0);
            assert_eq!(data.read_u32_be(4), 12);
            assert_eq!(data.read_u32_be(8), 124);
            assert_eq!(data.read_u32_be(12), u32::MAX);
        }

        #[test]
        fn test_read_i32_le_vec() {
            let mut data: Vec<u8> = Vec::new();

            data.extend(i32::MIN.to_le_bytes());
            data.extend((-12_i32).to_le_bytes());
            data.extend((-124_i32).to_le_bytes());
            data.extend(0_i32.to_le_bytes());
            data.extend(12_i32.to_le_bytes());
            data.extend(124_i32.to_le_bytes());
            data.extend(i32::MAX.to_le_bytes());
            assert_eq!(data.read_i32_le(0), i32::MIN);
            assert_eq!(data.read_i32_le(4), -12);
            assert_eq!(data.read_i32_le(8), -124);
            assert_eq!(data.read_i32_le(12), 0);
            assert_eq!(data.read_i32_le(16), 12);
            assert_eq!(data.read_i32_le(20), 124);
            assert_eq!(data.read_i32_le(24), i32::MAX);
        }

        #[test]
        fn test_read_u64_le_vec() {
            let mut data: Vec<u8> = Vec::new();
            data.extend(0_u64.to_le_bytes());
            data.extend(12_u64.to_le_bytes());
            data.extend(124_u64.to_le_bytes());
            data.extend(u64::MAX.to_le_bytes());
            assert_eq!(data.read_u64_le(0), 0);
            assert_eq!(data.read_u64_le(8), 12);
            assert_eq!(data.read_u64_le(16), 124);
            assert_eq!(data.read_u64_le(24), u64::MAX);
        }

        #[test]
        fn test_read_u64_be_vec() {
            let mut data: Vec<u8> = Vec::new();
            data.extend(0_u64.to_be_bytes());
            data.extend(12_u64.to_be_bytes());
            data.extend(124_u64.to_be_bytes());
            data.extend(u64::MAX.to_be_bytes());
            assert_eq!(data.read_u64_be(0), 0);
            assert_eq!(data.read_u64_be(8), 12);
            assert_eq!(data.read_u64_be(16), 124);
            assert_eq!(data.read_u64_be(24), u64::MAX);
        }

        #[test]
        fn test_read_i64_le_vec() {
            let mut data: Vec<u8> = Vec::new();

            data.extend(&i64::MIN.to_le_bytes());
            data.extend((-12_i64).to_le_bytes());
            data.extend((-124_i64).to_le_bytes());
            data.extend(0_i64.to_le_bytes());
            data.extend(12_i64.to_le_bytes());
            data.extend(124_i64.to_le_bytes());
            data.extend(&i64::MAX.to_le_bytes());
            assert_eq!(data.read_i64_le(0), i64::MIN);
            assert_eq!(data.read_i64_le(8), -12);
            assert_eq!(data.read_i64_le(16), -124);
            assert_eq!(data.read_i64_le(24), 0);
            assert_eq!(data.read_i64_le(32), 12);
            assert_eq!(data.read_i64_le(40), 124);
            assert_eq!(data.read_i64_le(48), i64::MAX);
        }
    }

    #[test]
    fn test_file_name_from_path() {
        let path = Path::new("/another/dir/example.rs");
        assert_eq!(file_name_as_str(path), Some("example.rs"));
    }

    #[test]
    fn test_file_name_rom_pathbuf() {
        let path = PathBuf::from("/path/to/note.md");
        assert_eq!(file_name_as_str(&path), Some("note.md"));
    }

    #[test]
    fn test_file_name_for_directory() {
        let path = Path::new("/only/dir/");
        assert_eq!(file_name_as_str(path), Some("dir"));
    }

    #[test]
    fn test_mark_file_as_corrupted_with_extension() {
        let logger = logger::test_instance();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");

        fs::write(&file_path, b"test data").unwrap();

        mark_file_as_corrupted(logger, &file_path).expect("Failed to mark file as corrupted");

        let expected = dir.path().join("test.log.corrupted");
        assert!(expected.exists());
        assert!(!file_path.exists());

        let contents = fs::read(&expected).unwrap();
        assert_eq!(contents, b"test data");
    }

    #[test]
    fn test_mark_file_as_corrupted_without_extension() {
        let logger = logger::test_instance();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");

        fs::write(&file_path, b"some content").unwrap();

        mark_file_as_corrupted(logger, &file_path).expect("Failed to mark file as corrupted");

        let expected = dir.path().join("testfile.corrupted");
        assert!(expected.exists());
        assert!(!file_path.exists());

        let contents = fs::read(&expected).unwrap();
        assert_eq!(contents, b"some content");
    }

    #[test]
    fn test_mark_file_as_corrupted_non_existent_file() {
        let logger = logger::test_instance();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("does_not_exist.log");

        let result = mark_file_as_corrupted(logger, &file_path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_truncate_file_shrink() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("shrink_test.log");

        // Create a file with 100 bytes
        fs::write(&file_path, vec![0xAB; 100]).unwrap();
        assert_eq!(fs::metadata(&file_path).unwrap().len(), 100);

        // Truncate to 50 bytes
        truncate_file(&file_path, 50).unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        assert_eq!(metadata.len(), 50);

        let contents = fs::read(&file_path).unwrap();
        assert_eq!(contents.len(), 50);
    }

    #[test]
    fn test_truncate_file_expand() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("expand_test.log");

        // Create a file with 10 bytes
        fs::write(&file_path, b"0123456789").unwrap();
        assert_eq!(fs::metadata(&file_path).unwrap().len(), 10);

        // Expand to 20 bytes
        truncate_file(&file_path, 20).unwrap();

        let metadata = fs::metadata(&file_path).unwrap();
        assert_eq!(metadata.len(), 20);

        let contents = fs::read(&file_path).unwrap();
        assert_eq!(&contents[..10], b"0123456789");
        assert!(contents[10..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_truncate_nonexistent_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("missing.log");

        let result = truncate_file(&file_path, 100);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
    }
}
