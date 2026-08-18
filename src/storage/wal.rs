use crate::io::byte_reader::ByteReader;
use crate::io::ZeroCopy;
use crate::obs::metrics::{self, AtomicGauge, Counter, MetricRegistry};
use crate::options::options::Options;
use crate::storage::append_log::{AppendLog, LogFileCreator, LogObserver, LogReplayError};
use crate::storage::files::DbFile;
use crate::storage::write_batch::WriteBatch;
use std::collections::VecDeque;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info_span, trace_span};

/// The current version of the write-ahead log format.
const WAL_VERSION: u32 = 1;

const MAGIC_NUMBER: u32 = 0x51756F6B; //"Quok" in ASCII hex

pub struct WriteAheadLog {
    /// The wal metrics
    metrics: Metrics,
    /// Number of bytes that has to be written before the wal will issue a fsync
    wal_bytes_per_sync: usize,
    /// The underlying append only log file manager
    append_log: AppendLog<WalFileCreator>,
    /// The bytes not synced to disk yet
    pending_bytes: usize,
    /// The wal files that have been rotated but are still active
    rotated_log_files: VecDeque<(u64, PathBuf)>,
}

impl WriteAheadLog {
    pub fn new(
        metric_registry: &mut MetricRegistry,
        options: &Options,
        directory: &Path,
        number: u64,
    ) -> Result<Self> {
        let metrics = Metrics::new();
        let append_log = AppendLog::new(
            directory,
            DbFile::new_write_ahead_log(number),
            Arc::new(WalFileCreator {}),
            WalObserver::new(metrics.clone()),
        )?;
        metrics.register_to(metric_registry);
        Self::new_internal(metrics, options, append_log, VecDeque::new())
    }

    pub fn load_from(
        metric_registry: &mut MetricRegistry,
        options: &Options,
        file_path: &Path,
        rotated_log_files: VecDeque<(u64, PathBuf)>,
    ) -> Result<Self> {
        let metrics = Metrics::new();
        let append_log = AppendLog::load_from(
            &file_path,
            Arc::new(WalFileCreator {}),
            WalObserver::new(metrics.clone()),
        )?;
        metrics.register_to(metric_registry);
        Self::new_internal(metrics, options, append_log, rotated_log_files)
    }

    pub fn new_after_corruption(
        metric_registry: &mut MetricRegistry,
        options: &Options,
        directory: &Path,
        number: u64,
        rotated_log_files: VecDeque<(u64, PathBuf)>,
    ) -> Result<Self> {
        let metrics = Metrics::new();
        let append_log = AppendLog::new(
            directory,
            DbFile::new_write_ahead_log(number),
            Arc::new(WalFileCreator {}),
            WalObserver::new(metrics.clone()),
        )?;
        metrics.register_to(metric_registry);
        Self::new_internal(metrics, options, append_log, rotated_log_files)
    }

    fn new_internal(
        metrics: Metrics,
        options: &Options,
        append_log: AppendLog<WalFileCreator>,
        rotated_log_files: VecDeque<(u64, PathBuf)>,
    ) -> Result<Self> {
        let amount = 1 + rotated_log_files.len() as u64;
        metrics.files.inc_by(amount);

        // We need to add rotated files to the total log size
        let mut rotated_files_size = 0;
        for rotated_log_file in &rotated_log_files {
            rotated_files_size += Self::file_size(&rotated_log_file.1)?;
        }

        metrics.total_bytes.inc_by(rotated_files_size);

        tracing::info!(
            path = %append_log.file_path().display(),
            "WAL initialized"
        );

        Ok(WriteAheadLog {
            metrics,
            append_log,
            wal_bytes_per_sync: options.wal_bytes_per_sync().to_bytes(),
            pending_bytes: 0,
            rotated_log_files,
        })
    }

    pub fn append(&mut self, seq: u64, batch: &WriteBatch) -> Result<()> {
        let _span = trace_span!("wal.append", seq, batch_size = batch.len()).entered();

        let record = batch.to_wal_record(seq);
        let bytes = self.append_log.append(&record)?;
        self.pending_bytes += bytes;

        self.sync_if_needed()?;

        tracing::trace!(seq, batch_size = batch.len(), bytes, "wal append done");

        Ok(())
    }

    pub fn rotate(&mut self, new_log_number: u64) -> Result<()> {
        let wal_file = DbFile::new_write_ahead_log(new_log_number);
        let old_filename = self.append_log.filename().unwrap().to_string();
        let new_filename = wal_file.filename().to_string();
        let _span = info_span!(
            "wal.rotate",
            old_filename = %old_filename,
            new_filename = %new_filename
        )
        .entered();
        tracing::info!(
            old_filename = %old_filename,
            new_filename = %new_filename,
            "Rotating WAL file"
        );
        let (new_path, old_path) = self.append_log.rotate(wal_file)?;
        self.pending_bytes = 0;
        self.metrics.files.inc();

        tracing::trace!(
            new_path = %new_path.display(),
            "wal rotation done"
        );

        let number = DbFile::new(&old_path).unwrap().number;
        self.rotated_log_files.push_back((number, old_path));

        Ok(())
    }

    pub fn drain_obsolete_logs(
        &mut self,
        oldest_unflushed_log_number: u64,
    ) -> Result<Vec<PathBuf>> {
        let mut obsoletes = Vec::new();
        while let Some(active_log) = self.rotated_log_files.front() {
            if active_log.0 < oldest_unflushed_log_number {
                let path = self.rotated_log_files.pop_front().unwrap().1;
                let bytes = Self::file_size(&path)?;
                self.metrics.total_bytes.dec_by(bytes);
                obsoletes.push(path);
            } else {
                break;
            }
        }
        let amount = obsoletes.len() as u64;
        self.metrics.files.dec_by(amount);
        Ok(obsoletes)
    }

    fn file_size(path: &PathBuf) -> Result<u64> {
        Ok(std::fs::metadata(&path)?.len())
    }

    fn sync_if_needed(&mut self) -> Result<()> {
        if self.pending_bytes >= self.wal_bytes_per_sync {
            tracing::debug!(
                pending_bytes = self.pending_bytes,
                wal_bytes_per_sync = self.wal_bytes_per_sync,
                "WAL sync condition met"
            );
            self.sync()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        let _span =
            trace_span!("wal.sync", path = %self.append_log.file_path().display()).entered();
        let start = Instant::now();
        self.append_log.sync()?;
        let duration = start.elapsed();
        tracing::trace!(
            duration_micros = duration.as_micros(),
            path = %self.append_log.file_path().display(),
            "wal sync done"
        );
        self.pending_bytes = 0;
        Ok(())
    }

    pub fn replay(
        wal_file: &Path,
    ) -> result::Result<
        impl Iterator<Item = result::Result<(u64, WriteBatch), LogReplayError>>,
        LogReplayError,
    > {
        const HEADER_SIZE: usize = 4 + 4 + 8; // MAGIC (u32) + VERSION (u32) + ID (u64)

        let (header, iter) =
            AppendLog::<WalFileCreator>::read_log_file(wal_file.to_path_buf(), HEADER_SIZE)?;
        let reader = ByteReader::new(&header);

        let magic = reader.read_u32_be()?;
        if magic != MAGIC_NUMBER {
            return Err(LogReplayError::Corruption {
                record_offset: 0,
                reason: "Invalid wal magic number".to_string(),
            });
        }

        let _version = reader.read_u32_be()?; // Could be needed one day.
        let _id = reader.read_u64_be()?;

        let records = iter.map(move |rs| match rs {
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

    #[cfg(test)]
    pub fn return_error_on_append(&self, value: bool) {
        self.append_log.return_error_on_append(value);
    }

    #[cfg(test)]
    pub fn return_error_on_rotate(&self, value: bool) {
        self.append_log.return_error_on_rotate(value);
    }
}

struct WalFileCreator {}

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
    metrics: Metrics,
}

impl WalObserver {
    fn new(metrics: Metrics) -> Arc<Self> {
        Arc::new(Self { metrics })
    }
}

impl LogObserver for WalObserver {
    fn on_load(&self, bytes: u64) {
        self.metrics.total_bytes.inc_by(bytes);
    }

    fn on_buffered(&self, bytes: u64) {
        self.metrics.bytes_buffered.inc_by(bytes);
    }

    fn on_bytes_written(&self, bytes: u64) {
        self.metrics.bytes_written.inc_by(bytes);
        self.metrics.bytes_buffered.dec_by(bytes);
    }

    fn on_sync(&self, bytes: u64) {
        self.metrics.syncs.inc();
        self.metrics.total_bytes.inc_by(bytes);
    }
}

#[derive(Clone)]
struct Metrics {
    /// Number of WAL files currently present
    pub files: Arc<AtomicGauge>,

    /// Total size (in bytes) of all WAL files
    pub total_bytes: Arc<AtomicGauge>,

    /// Number of times the WAL has been explicitly synced to disk
    pub syncs: Arc<Counter>,

    /// Number of bytes currently buffered by the wal
    pub bytes_buffered: Arc<AtomicGauge>,

    /// The total number of bytes successfully written to the WAL file(s)
    /// This number refers to bytes written to the file descriptor (i.e., the OS buffer), not
    /// necessarily flushed to disk.
    pub bytes_written: Arc<Counter>,
}

impl Metrics {
    fn new() -> Metrics {
        Self {
            files: AtomicGauge::new(),
            total_bytes: AtomicGauge::new(),
            syncs: Counter::new(),
            bytes_buffered: AtomicGauge::new(),
            bytes_written: Counter::new(),
        }
    }

    fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_gauge(metrics::names::wal::FILES, self.files.clone())
            .register_gauge(metrics::names::wal::TOTAL_BYTES, self.total_bytes.clone())
            .register_counter(metrics::names::wal::SYNCS, self.syncs.clone())
            .register_gauge(
                metrics::names::wal::BYTES_BUFFERED,
                self.bytes_buffered.clone(),
            )
            .register_counter(
                metrics::names::wal::BYTES_WRITTEN,
                self.bytes_written.clone(),
            );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::obs::metrics::Gauge;
    use crate::options::storage_quantity::{StorageQuantity, StorageUnit};
    use crate::storage::count_stats::{CountStats, CountStatsKey};
    use crate::storage::operation::Operation;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn write_batch(operations: Vec<Operation>) -> WriteBatch {
        WriteBatch::new(operations, CountStats::default())
    }

    #[test]
    fn test_replay_write_ahead_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Options::default();
        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, &path, 1).unwrap();

        let batches = vec![
            (
                10,
                write_batch(vec![Operation::new_put(1, 1, b"a".to_vec(), b"1".to_vec())]),
            ),
            (
                20,
                write_batch(vec![Operation::new_put(1, 1, b"b".to_vec(), b"2".to_vec())]),
            ),
            (
                30,
                write_batch(vec![Operation::new_delete(1, 1, b"a".to_vec())]),
            ),
        ];

        for (seq, batch) in &batches {
            wal.append(*seq, batch).unwrap();
        }

        wal.sync().unwrap();

        let wal_file = path.join("000001.log");
        assert!(wal_file.exists());

        let len = wal_file.metadata().unwrap().len();

        // metric check:
        assert_eq!(wal.metrics.syncs.get(), 2);
        assert_eq!(wal.metrics.bytes_buffered.get(), 0);
        assert_eq!(wal.metrics.bytes_written.get(), len);
        assert_eq!(wal.metrics.total_bytes.get(), len);
        assert_eq!(wal.metrics.files.get(), 1);

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

        assert_eq!(replayed[0].1.count_stats(), &CountStats::default());
    }

    #[test]
    fn test_replay_write_ahead_log_preserves_count_stats() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Options::default();
        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, &path, 1).unwrap();

        let batch = WriteBatch::new(
            vec![
                Operation::new_put(7, 0, b"doc".to_vec(), b"value".to_vec()),
                Operation::new_put(7, 1, b"idx".to_vec(), b"doc".to_vec()),
            ],
            CountStats::new(BTreeMap::from([
                (CountStatsKey::Collection(7), 1),
                (
                    CountStatsKey::Index {
                        collection: 7,
                        index: 1,
                    },
                    1,
                ),
            ])),
        );

        wal.append(10, &batch).unwrap();
        wal.sync().unwrap();

        let wal_file = path.join("000001.log");
        let replayed: Vec<_> = WriteAheadLog::replay(&wal_file)
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].0, 10);
        assert_eq!(replayed[0].1, batch);
    }

    #[test]
    fn test_rotate_and_drain_obsoletes_logs_with_replay_check() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let options = Options::default();

        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, &path, 1).unwrap();

        // Append and rotate to log 2
        wal.append(
            1,
            &write_batch(vec![Operation::new_put(1, 1, b"a".to_vec(), b"1".to_vec())]),
        )
        .unwrap();
        wal.rotate(2).unwrap();

        // Append and rotate to log 3
        wal.append(
            2,
            &write_batch(vec![Operation::new_put(1, 1, b"b".to_vec(), b"2".to_vec())]),
        )
        .unwrap();
        wal.rotate(3).unwrap();

        // Validate replay of rotated files
        for log_num in [1, 2] {
            let log_path = path.join(format!("{:06}.log", log_num));
            assert!(
                log_path.exists(),
                "Expected {} to exist",
                log_path.display()
            );

            let replayed: Vec<_> = WriteAheadLog::replay(&log_path)
                .unwrap()
                .map(|res| res.unwrap())
                .collect();

            assert_eq!(replayed.len(), 1, "Expected one record in log {}", log_num);
            let (seq, batch) = &replayed[0];
            let expected_key = if *seq == 1 { b"a" } else { b"b" };
            let expected_val = if *seq == 1 { b"1" } else { b"2" };

            assert_eq!(batch.operations().len(), 1);
            assert_eq!(
                batch.operations()[0],
                Operation::new_put(1, 1, expected_key.to_vec(), expected_val.to_vec())
            );
        }

        assert_eq!(wal.metrics.files.get(), 3);

        // Obsolete logs: 1 and 2 (since current is 3)
        let obsolete = wal.drain_obsolete_logs(3).unwrap();
        assert_eq!(obsolete.len(), 2);
        assert!(obsolete[0].ends_with("000001.log"));
        assert!(obsolete[1].ends_with("000002.log"));

        // Should be empty if called again
        let empty = wal.drain_obsolete_logs(3).unwrap();
        assert!(empty.is_empty());

        // metric check:
        assert_eq!(wal.metrics.files.get(), 1); // only one active log left
        assert!(wal.metrics.total_bytes.get() > 0);
    }

    #[test]
    fn test_sync_triggered_by_pending_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options =
            Options::default().with_wal_bytes_per_sync(StorageQuantity::new(1, StorageUnit::Bytes)); // force sync after any write

        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, path, 1).unwrap();

        let mut batch = Vec::new();
        for i in 0..5 {
            batch.push(new_operation(i));
        }

        wal.append(1, &write_batch(batch)).unwrap();

        // Should have triggered sync
        assert_eq!(wal.pending_bytes, 0);

        // metric check:
        assert_eq!(wal.metrics.syncs.get(), 2); // Header + triggered sync
    }

    #[test]
    fn test_multiple_appends_and_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::default();

        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, path, 7).unwrap();
        for i in 0..3 {
            wal.append(100 + i as u64, &write_batch(vec![new_operation(i)]))
                .unwrap();
        }
        wal.rotate(8).unwrap();

        // metrics check
        assert_eq!(wal.metrics.files.get(), 2); // one rotated, one active
        assert_eq!(wal.metrics.syncs.get(), 3);
        assert_eq!(wal.metrics.bytes_buffered.get(), 0);
        assert_eq!(
            wal.metrics.bytes_written.get(),
            wal.metrics.total_bytes.get()
        );

        let log_file = path.join("000007.log");
        let replayed: Vec<_> = WriteAheadLog::replay(&log_file)
            .unwrap()
            .map(|res| res.unwrap())
            .collect();

        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0], (100, write_batch(vec![new_operation(0)])));
        assert_eq!(replayed[1], (101, write_batch(vec![new_operation(1)])));
        assert_eq!(replayed[2], (102, write_batch(vec![new_operation(2)])));
    }

    #[test]
    fn test_rotate_empty_log_still_replayable() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::default();

        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, path, 11).unwrap();
        wal.rotate(12).unwrap();

        let log_file = path.join("000011.log");
        assert!(log_file.exists());

        let replayed: Vec<_> = WriteAheadLog::replay(&log_file).unwrap().collect();
        assert!(replayed.is_empty());

        // metric check:
        assert!(wal.metrics.bytes_written.get() > 0);
        assert_eq!(wal.metrics.files.get(), 2); // one rotated, one active
    }

    #[test]
    fn test_replay_corrupted_log_should_error() {
        use std::fs::OpenOptions;
        use std::io::Write;

        let dir = tempdir().unwrap();
        let path = dir.path();
        let options = Options::default();

        let mut wal =
            WriteAheadLog::new(&mut MetricRegistry::default(), &options, path, 13).unwrap();
        wal.append(
            1,
            &write_batch(vec![Operation::new_put(1, 1, b"x".to_vec(), b"1".to_vec())]),
        )
        .unwrap();
        wal.rotate(14).unwrap();

        let log_file = path.join("000013.log");

        // Truncate or corrupt the file
        let mut file = OpenOptions::new().write(true).open(&log_file).unwrap();
        file.set_len(20).unwrap(); // Invalid size
        file.flush().unwrap();

        let result = WriteAheadLog::replay(&log_file);
        assert!(result.is_err(), "Expected replay to fail due to corruption");

        // metric check:
        assert_eq!(wal.metrics.files.get(), 2); // old + new
    }

    fn new_operation(i: u8) -> Operation {
        Operation::new_put(1, 0, vec![i], vec![i + 1])
    }
}
