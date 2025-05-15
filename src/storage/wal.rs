use std::collections::VecDeque;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::Arc;
use crate::io::byte_reader::ByteReader;
use crate::io::ZeroCopy;
use crate::options::options::DatabaseOptions;
use crate::statistics::ServerStatistics;
use crate::storage::files::DbFile;
use crate::storage::log::{Log, LogFileCreator, LogObserver, LogReplayError};
use crate::storage::write_batch::WriteBatch;

/// The current version of the write-ahead log format.
const WAL_VERSION: u32 = 1;

const MAGIC_NUMBER: u32 = 0x51756F6B; //"Quok" in ASCII hex

pub struct WriteAheadLog {
    /// The underlying log file manager
    log: Log<WalFileCreator>,
    /// The n
    wal_bytes_per_sync: usize,
    /// The bytes not synced to disk yet
    pending_bytes: usize,
    /// The wal files that have been rotated but are still active
    rotated_log_files: VecDeque<(u64, PathBuf)>,
    /// The server statistics
    stats: Arc<ServerStatistics>
}

impl WriteAheadLog {

    pub fn new(options: &DatabaseOptions, stats: Arc<ServerStatistics>, directory: &Path, number: u64) -> Result<Self> {

        let log = Log::new(directory, DbFile::new_write_ahead_log(number), Arc::new(WalFileCreator {}), WalObserver::new(stats.clone()))?;
        Self::new_internal(options, stats, log, VecDeque::new())
    }

    pub fn load_from(
        options: &DatabaseOptions,
        stats: Arc<ServerStatistics>,
        file_path: &Path,
        rotated_log_files: VecDeque<(u64, PathBuf)>
    ) -> Result<Self> {

        let log = Log::load_from(&file_path, Arc::new(WalFileCreator {}), WalObserver::new(stats.clone()))?;
        Self::new_internal(options, stats, log, rotated_log_files)
    }

    pub fn new_after_corruption(
        options: &DatabaseOptions,
        stats: Arc<ServerStatistics>,
        directory: &Path,
        number: u64,
        rotated_log_files: VecDeque<(u64, PathBuf)>
    ) -> Result<Self> {

        let log = Log::new(directory, DbFile::new_write_ahead_log(number), Arc::new(WalFileCreator {}), WalObserver::new(stats.clone()))?;
        Self::new_internal(options, stats, log, rotated_log_files)
    }

    fn new_internal(
        options: &DatabaseOptions,
        stats: Arc<ServerStatistics>,
        log: Log<WalFileCreator>,
        rotated_log_files: VecDeque<(u64, PathBuf)>
    ) -> Result<Self> {
        let amount = 1 + rotated_log_files.len() as u64;
        stats.wal_files.inc(amount);

        // We need to add rotated files to the total log size
        let mut rotated_files_size = 0;
        for rotated_log_file in &rotated_log_files {
            rotated_files_size += Self::file_size(&rotated_log_file.1)?;
        }

        stats.wal_total_bytes.inc(rotated_files_size);

        Ok(WriteAheadLog {
            log,
            wal_bytes_per_sync: options.wal_bytes_per_sync.to_bytes(),
            pending_bytes: 0,
            rotated_log_files,
            stats,
        })
    }

    pub fn append(&mut self, seq: u64, batch: &WriteBatch) -> Result<()> {

        let record = batch.to_wal_record(seq);
        let bytes = self.log.append(&record)?;
        self.pending_bytes += bytes;

        self.sync_if_needed()?;

        Ok(())
    }

    pub fn rotate(&mut self, new_log_number: u64) -> Result<()> {
        let (_new_path, old_path) = self.log.rotate(DbFile::new_write_ahead_log(new_log_number))?;
        self.pending_bytes = 0;

        let self1 = &self.stats;
        self1.wal_files.inc(1);

        let number = DbFile::new(&old_path).unwrap().number;
        self.rotated_log_files.push_back((number, old_path));

        Ok(())
    }

    pub fn drain_obsolete_logs(&mut self, oldest_unflushed_log_number: u64) -> Result<Vec<PathBuf>> {
        let mut obsoletes = Vec::new();
        while let Some(active_log) = self.rotated_log_files.front() {
            if  active_log.0 < oldest_unflushed_log_number {
                let path = self.rotated_log_files.pop_front().unwrap().1;
                let self1 = &self.stats;
                let bytes = Self::file_size(&path)?;
                self1.wal_total_bytes.dec(bytes);
                obsoletes.push(path);
            } else {
                break;
            }
        }
        let self1 = &self.stats;
        let amount = obsoletes.len() as u64;
        self1.wal_files.dec(amount);
        Ok(obsoletes)
    }

    fn file_size(path: &PathBuf) -> Result<u64> {
        Ok(std::fs::metadata(&path)?.len())
    }

    fn sync_if_needed(&mut self) -> Result<()> {
        if self.pending_bytes >= self.wal_bytes_per_sync {
            self.sync()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.log.sync()?;
        self.pending_bytes = 0;
        Ok(())
    }

    pub fn replay(
        wal_file: &Path
    ) -> result::Result<impl Iterator<Item = result::Result<(u64, WriteBatch), LogReplayError>>, LogReplayError> {

        const HEADER_SIZE: usize = 4 + 4 + 8; // MAGIC (u32) + VERSION (u32) + ID (u64)

        let (header, iter) = Log::<WalFileCreator>::read_log_file(wal_file.to_path_buf(), HEADER_SIZE)?;
        let reader = ByteReader::new(&header);

        let magic = reader.read_u32_be()?;
        if magic != MAGIC_NUMBER {
            return Err(LogReplayError::Corruption { record_offset: 0, reason: "Invalid wal magic number".to_string()});
        }

        let _version = reader.read_u32_be()?; // Could be needed one day.
        let _id = reader.read_u64_be()?;

        let records = iter.map(move |rs|
            match rs {
                Ok(bytes) => {
                    let seq = bytes[..8].read_u64_be(0);
                    match WriteBatch::from_wal_record(&bytes[8..]) {
                        Ok(batch) => Ok((seq, batch)),
                        Err(e) => Err(e.into()),
                    }
                }
                Err(e) => Err(e),
        });

        Ok(records)
    }
}

struct WalFileCreator { }

impl LogFileCreator for WalFileCreator {
    type Observer = WalObserver;

    fn header(&self, id: u64) -> Vec<u8> {
        let mut vec = Vec::with_capacity(4 + 4 + 8);
        vec.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
        vec.extend_from_slice(&WAL_VERSION.to_be_bytes());
        vec.extend_from_slice(&id.to_be_bytes());
        vec
    }
}

struct WalObserver {
    stats: Arc<ServerStatistics>,
}

impl WalObserver {
    fn new(stats: Arc<ServerStatistics>) -> Arc<Self> {
        Arc::new(Self { stats })
    }
}

impl LogObserver for WalObserver {
    fn on_load(&self, bytes: u64) {
        self.stats.wal_total_bytes.inc(bytes);
    }

    fn on_buffered(&self, bytes: u64) {
        self.stats.wal_bytes_buffered.inc(bytes);
    }

    fn on_bytes_written(&self, bytes: u64) {
        self.stats.wal_bytes_written.inc(bytes);
        self.stats.wal_bytes_buffered.dec(bytes);
    }

    fn on_sync(&self, bytes: u64) {
        self.stats.wal_syncs.inc(1);
        self.stats.wal_total_bytes.inc(bytes);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::options::options::DatabaseOptions;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::storage::operation::Operation;

    #[test]
    fn test_replay_write_ahead_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = DatabaseOptions::default();
        let stats = ServerStatistics::new();

        let mut wal = WriteAheadLog::new(&options, stats, &path, 1).unwrap();

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

        let len = wal_file.metadata().unwrap().len();

        // metric check:
        assert_eq!(wal.stats.wal_syncs.get(), 2);
        assert_eq!(wal.stats.wal_bytes_buffered.get(), 0);
        assert_eq!(wal.stats.wal_bytes_written.get(), len);
        assert_eq!(wal.stats.wal_total_bytes.get(), len);
        assert_eq!(wal.stats.wal_files.get(), 1);

        let replayed: Vec<_> = WriteAheadLog::replay(&wal_file)
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0].0, 10);
        assert_eq!(replayed[1].0, 20);
        assert_eq!(replayed[2].0, 30);

        assert_eq!(
            replayed[0].1.operations()[0],
            Operation::new_put(1, 1, b"a".to_vec(), b"1".to_vec())
        );

        assert_eq!(
            replayed[1].1.operations()[0],
            Operation::new_put(1, 1, b"b".to_vec(), b"2".to_vec())
        );

        assert_eq!(
            replayed[2].1.operations()[0],
            Operation::new_delete(1, 1, b"a".to_vec())
        );
    }

    #[test]
    fn test_rotate_and_drain_obsoletes_logs_with_replay_check() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = DatabaseOptions::default();
        let stats = ServerStatistics::new();

        let mut wal = WriteAheadLog::new(&options, stats, &path, 1).unwrap();

        // Append and rotate to log 2
        wal.append(1, &WriteBatch::new(vec![Operation::new_put(1, 1, b"a".to_vec(), b"1".to_vec())]))
            .unwrap();
        wal.rotate(2).unwrap();

        // Append and rotate to log 3
        wal.append(2, &WriteBatch::new(vec![Operation::new_put(1, 1, b"b".to_vec(), b"2".to_vec())]))
            .unwrap();
        wal.rotate(3).unwrap();

        // Validate replay of rotated files
        for log_num in [1, 2] {
            let log_path = path.join(format!("{:06}.log", log_num));
            assert!(log_path.exists(), "Expected {} to exist", log_path.display());

            let replayed: Vec<_> = WriteAheadLog::replay(&log_path)
                .unwrap()
                .map(|res| res.unwrap())
                .collect();

            assert_eq!(replayed.len(), 1, "Expected one record in log {}", log_num);
            let (seq, batch) = &replayed[0];
            let expected_key = if *seq == 1 { b"a" } else { b"b" };
            let expected_val = if *seq == 1 { b"1" } else { b"2" };

            assert_eq!(batch.operations().len(), 1);
            assert_eq!(batch.operations()[0], Operation::new_put(1, 1, expected_key.to_vec(), expected_val.to_vec()));
        }

        let self1 = &wal.stats;
        assert_eq!(self1.wal_files.get(), 3);

        // Obsolete logs: 1 and 2 (since current is 3)
        let obsolete = wal.drain_obsolete_logs(3).unwrap();
        assert_eq!(obsolete.len(), 2);
        assert!(obsolete[0].ends_with("000001.log"));
        assert!(obsolete[1].ends_with("000002.log"));

        // Should be empty if called again
        let empty = wal.drain_obsolete_logs(3).unwrap();
        assert!(empty.is_empty());

        // metric check:
        let self1 = &wal.stats;
        assert_eq!(self1.wal_files.get(), 1); // only one active log left
        let self2 = &wal.stats;
        assert!(self2.wal_total_bytes.get() > 0);
    }

    #[test]
    fn test_sync_triggered_by_pending_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut options = DatabaseOptions::default();
        options.wal_bytes_per_sync = StorageQuantity::new(1, StorageUnit::Bytes); // force sync after any write
        let stats = ServerStatistics::new();

        let mut wal = WriteAheadLog::new(&options, stats, path, 1).unwrap();

        let mut batch = Vec::new();
        for i in 0..5 {
            batch.push(new_operation(i));
        }

        wal.append(1, &WriteBatch::new(batch)).unwrap();

        // Should have triggered sync
        assert_eq!(wal.pending_bytes, 0);

        // metric check:
        assert_eq!(wal.stats.wal_syncs.get(), 2); // Header + triggered sync
    }

    #[test]
    fn test_multiple_appends_and_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = DatabaseOptions::default();
        let stats = ServerStatistics::new();

        let mut wal = WriteAheadLog::new(&options, stats, path, 7).unwrap();
        for i in 0..3 {
            wal.append(100 + i as u64, &WriteBatch::new(vec![new_operation(i)])).unwrap();
        }
        wal.rotate(8).unwrap();

        // metrics check
        assert_eq!(wal.stats.wal_files.get(), 2); // one rotated, one active
        assert_eq!(wal.stats.wal_syncs.get(), 3);
        assert_eq!(wal.stats.wal_bytes_buffered.get(), 0);
        assert_eq!(wal.stats.wal_bytes_written.get(), wal.stats.wal_total_bytes.get());

        let log_file = path.join("000007.log");
        let replayed: Vec<_> = WriteAheadLog::replay(&log_file).unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0], (100, WriteBatch::new(vec![new_operation(0)])));
        assert_eq!(replayed[1], (101, WriteBatch::new(vec![new_operation(1)])));
        assert_eq!(replayed[2], (102, WriteBatch::new(vec![new_operation(2)])));
    }

    #[test]
    fn test_rotate_empty_log_still_replayable() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = DatabaseOptions::default();
        let stats = ServerStatistics::new();

        let mut wal = WriteAheadLog::new(&options, stats, path, 11).unwrap();
        wal.rotate(12).unwrap();

        let log_file = path.join("000011.log");
        assert!(log_file.exists());

        let replayed: Vec<_> = WriteAheadLog::replay(&log_file).unwrap().collect();
        assert!(replayed.is_empty());

        // metric check:
        let self1 = &wal.stats;
        assert!(self1.wal_bytes_written.get() > 0);
        let self1 = &wal.stats;
        assert_eq!(self1.wal_files.get(), 2); // one rotated, one active
    }

    #[test]
    fn test_replay_corrupted_log_should_error() {
        use std::fs::OpenOptions;
        use std::io::Write;

        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = DatabaseOptions::default();
        let stats = ServerStatistics::new();

        let mut wal = WriteAheadLog::new(&options, stats, path, 13).unwrap();
        wal.append(1, &WriteBatch::new(vec![
            Operation::new_put(1, 1, b"x".to_vec(), b"1".to_vec())
        ])).unwrap();
        wal.rotate(14).unwrap();

        let log_file = path.join("000013.log");

        // Truncate or corrupt the file
        let mut file = OpenOptions::new().write(true).open(&log_file).unwrap();
        file.set_len(20).unwrap(); // Invalid size
        file.flush().unwrap();

        let result = WriteAheadLog::replay(&log_file);
        assert!(result.is_err(), "Expected replay to fail due to corruption");

        // metric check:
        let self1 = &wal.stats;
        assert_eq!(self1.wal_files.get(), 2); // old + new
    }

    fn new_operation(i: u8) -> Operation {
        Operation::new_put(1, 0, vec![i], vec![i + 1])
    }
}
