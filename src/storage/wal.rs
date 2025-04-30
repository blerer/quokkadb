use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::result;
use std::sync::Arc;
use crate::io::byte_reader::ByteReader;
use crate::io::ZeroCopy;
use crate::options::options::DatabaseOptions;
use crate::storage::files::DbFile;
use crate::storage::log::{Log, LogFileCreator, LogReplayError};
use crate::storage::write_batch::WriteBatch;

/// The current version of the write-ahead log format.
const WAL_VERSION: u32 = 1;

const MAGIC_NUMBER: u32 = 0x51756F6B; //"Quok" in ASCII hex

pub struct WriteAheadLog {
    log: Log,
    wal_bytes_per_sync: usize,
    pending_bytes: usize,
}

impl WriteAheadLog {

    pub fn new(path: &Path, options: &DatabaseOptions, id: u64) -> Result<Self> {

        let log = Log::new(path, DbFile::new_write_ahead_log(id), Arc::new(WriteAheadLogFileCreator{}))?;

        Ok(WriteAheadLog {
            log,
            wal_bytes_per_sync: options.wal_bytes_per_sync().to_bytes(),
            pending_bytes: 0,
        })
    }
    pub fn load_from(file_path: PathBuf, options: &DatabaseOptions) -> Result<Self> {

        let log = Log::load_from(&file_path, Arc::new(WriteAheadLogFileCreator{}))?;

        Ok(WriteAheadLog {
            log,
            wal_bytes_per_sync: options.wal_bytes_per_sync().to_bytes(),
            pending_bytes: 0,
        })
    }

    pub fn append(&mut self, seq: u64, batch: &WriteBatch) -> Result<()> {

        let record = batch.to_wal_record(seq);
        self.pending_bytes += self.log.append(&record)?;

        self.sync_if_needed()?;

        Ok(())
    }

    pub fn rotate(&mut self, new_log_id: u64) -> Result<()> {
        self.log.rotate(DbFile::new_write_ahead_log(new_log_id))?;
        self.pending_bytes = 0;
        Ok(())
    }

    fn sync_if_needed(&mut self) -> Result<()> {
        if self.pending_bytes >= self.wal_bytes_per_sync {
            return self.sync()
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.log.sync()
    }

    pub fn replay(
        wal_file: &Path,
        last_sequence_number: u64
    ) -> result::Result<impl Iterator<Item = result::Result<(u64, WriteBatch), LogReplayError>>, LogReplayError> {

        const HEADER_SIZE: usize = 4 + 4 + 8; // MAGIC (u32) + VERSION (u32) + ID (u64)

        let (header, iter) = Log::read_log_file(wal_file.to_path_buf(), HEADER_SIZE)?;
        let reader = ByteReader::new(&header);

        let magic = reader.read_u32_be()?;
        if magic != MAGIC_NUMBER {
            return Err(LogReplayError::Corruption { record_offset: 0, reason: "Invalid wal magic number".to_string()});
        }

        let _version = reader.read_u32_be()?; // Could be needed one day.
        let _id = reader.read_u64_be()?;

        let filtered = iter.filter_map(move |rs|
            match rs {
                Ok(bytes) => {
                    let seq = bytes[..8].read_u64_be(0);
                    if seq > last_sequence_number {
                        match WriteBatch::from_wal_record(&bytes[8..]) {
                            Ok(batch) => Some(Ok((seq, batch))),
                            Err(e) => Some(Err(e.into())),
                        }
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(e)),
        });

        Ok(filtered)
    }
}

struct WriteAheadLogFileCreator { }

impl LogFileCreator for WriteAheadLogFileCreator {

    fn header(&self, id: u64) -> Vec<u8> {
        let mut vec = Vec::with_capacity(4 + 4 + 8);
        vec.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
        vec.extend_from_slice(&WAL_VERSION.to_be_bytes());
        vec.extend_from_slice(&id.to_be_bytes());
        vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::options::options::DatabaseOptions;
    use crate::storage::operation::Operation;

    #[test]
    fn test_replay_write_ahead_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = DatabaseOptions::default();

        let mut wal = WriteAheadLog::new(&path, &options, 1).unwrap();

        let batches = vec![
            (10, WriteBatch::new(vec![Operation::new_put(1, 1, b"a".to_vec(), b"1".to_vec())])),
            (20, WriteBatch::new(vec![Operation::new_put(1, 1, b"b".to_vec(), b"2".to_vec())])),
            (30, WriteBatch::new(vec![Operation::new_delete(1, 1, b"a".to_vec())])),
        ];

        for (seq, batch) in &batches {
            wal.append(*seq, batch).unwrap();
        }

        wal.sync().unwrap();

        let wal_file = path.join("000001.log");
        assert!(wal_file.exists());

        let replayed: Vec<_> = WriteAheadLog::replay(&wal_file, 15)
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].0, 20);
        assert_eq!(replayed[1].0, 30);

        assert_eq!(
            replayed[0].1.operations()[0],
            Operation::new_put(1, 1, b"b".to_vec(), b"2".to_vec())
        );

        assert_eq!(
            replayed[1].1.operations()[0],
            Operation::new_delete(1, 1, b"a".to_vec())
        );
    }

    #[test]
    fn test_replay_with_no_matches() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = DatabaseOptions::default();

        let mut wal = WriteAheadLog::new(&path, &options, 2).unwrap();

        wal.append(10, &WriteBatch::new(vec![Operation::new_put(1, 1, b"x".to_vec(), b"9".to_vec())]))
            .unwrap();

        wal.sync().unwrap();

        let wal_file = path.join("000002.log");

        let replayed: Vec<_> = WriteAheadLog::replay(&wal_file, 10)
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert!(replayed.is_empty());
    }
}
