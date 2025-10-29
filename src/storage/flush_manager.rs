use crate::obs::logger::{LogLevel, LoggerAndTracer};
use crate::obs::metrics::{Counter, Histogram, MetricRegistry};
use crate::options::options::Options;
use crate::storage::callback::Callback;
use crate::storage::files::DbFile;
use crate::storage::lsm_version::SSTableMetadata;
use crate::storage::memtable::Memtable;
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::storage_engine::SSTableOperation;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Condvar, Mutex,
    },
    thread::self,
    time::Duration,
};
use crate::{error, event, info};

/// Represents a task to be handled by the `FlushManager`.
///
/// `FlushTask` can be either:
/// - `Flush`: Requests flushing a memtable to an SSTable file. Contains the target SSTable file,
///   the memtable to flush, and an optional callback to be invoked upon completion.
/// - `Sync`: Used to notify the caller when all flushes scheduled before this point have completed.
///   When a `Sync` task is enqueued, the `FlushManager` ensures all prior `Flush` tasks are finished
///   before invoking the optional callback.
pub enum FlushTask {
    /// Flush a memtable to an SSTable file.
    ///
    /// - `sst_file`: The target SSTable file for the flush.
    /// - `memtable`: The memtable to be flushed.
    /// - `callback`: the callback to be called with the result of the flush operation.
    Flush {
        sst_file: DbFile,
        memtable: Arc<Memtable>,
        callback: Arc<Callback<Result<SSTableOperation>>>,
    },
    /// Notify when all previously scheduled flushes are complete.
    ///
    /// - `callback`: the callback to be called when the sync is complete.
    Sync {
        callback: Arc<Callback<Result<()>>>,
    },
}

/// Manages background flushing of memtables to SSTable files.
///
/// The `FlushManager` runs a dedicated background thread that processes
/// `FlushTask`s, which can either flush a memtable to disk or notify when all
/// previously scheduled flushes are complete. Tasks are enqueued via the `enqueue`
/// method. Metrics for flush operations are collected and registered.
///
/// In test builds, the flush thread can be paused and resumed for deterministic testing.
pub struct FlushManager {
    sender: SyncSender<FlushTask>,
    sync_control: Option<Arc<(Mutex<bool>, Condvar)>>,
}

impl FlushManager {

    /// Creates a new `FlushManager` and starts its background thread.
    ///
    /// - `logger`: Logger for events and errors.
    /// - `metric_registry`: Registry for flush metrics.
    /// - `options`: Database options.
    /// - `db_dir`: Path to the database directory.
    /// - `sst_cache`: Cache for SSTable files.
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: Arc<Options>,
        db_dir: &Path,
        sst_cache: Arc<SSTableCache>,
    ) -> Result<Self> {
        let (sender, receiver): (SyncSender<FlushTask>, Receiver<FlushTask>) = sync_channel(32);
        let db_dir = db_dir.to_path_buf();
        let log = logger.clone();
        let metrics = Metrics::new();
        metrics.register_to(metric_registry);

        #[cfg(test)]
        let sync_control = Some(Arc::new((Mutex::new(false), Condvar::new())));
        #[cfg(not(test))]
        let sync_control: Option<Arc<(Mutex<bool>, Condvar)>> = None;

        let sync_control_for_thread = sync_control.clone();

        let _thread = {
            thread::Builder::new()
                .name("flush_manager".to_string())
                .spawn(move || {
                while let Ok(task) = receiver.recv() {
                    if let Some(sync_control) = &sync_control_for_thread {
                        let (lock, cvar) = &**sync_control;
                        let mut paused = lock.lock().unwrap();
                        while *paused {
                            paused = cvar.wait(paused).unwrap();
                        }
                    }

                    match task {
                        FlushTask::Sync { callback } => {
                            event!(log, "flush_sync start");
                            callback.call(Ok(()));
                            event!(log, "flush_sync done");
                        }
                        FlushTask::Flush { sst_file, memtable, callback } => {
                            event!(log, "flush start, memtable={}", memtable.log_number);

                            let result = Self::flush(
                                log.clone(),
                                &metrics,
                                sst_cache.clone(),
                                &options,
                                &memtable,
                                &db_dir,
                                &sst_file,
                            );

                            match result {
                                Ok(sst) => {
                                    let operation = SSTableOperation::Flush {
                                        log_number: memtable.log_number,
                                        flushed: Arc::new(sst),
                                    };
                                    callback.call(Ok(operation));
                                }
                                Err(e) => {
                                    error!(log, "Flush failed: {}", e);
                                    callback.call(Err(e));

                                    thread::sleep(Duration::from_secs(1));
                                }
                            }
                        }
                    }
                }
            })
        }?;

        info!(logger, "FlushManager initialized");

        Ok(Self {
            sender,
            sync_control,
        })
    }

    fn flush(
        logger: Arc<dyn LoggerAndTracer>,
        metrics: &Metrics,
        sst_cache: Arc<SSTableCache>,
        options: &Arc<Options>,
        memtable: &Arc<Memtable>,
        db_dir: &PathBuf,
        sst_file: &DbFile,
    ) -> Result<SSTableMetadata> {
        let start = Instant::now();
        let memtable_size = memtable.size();
        let sst = memtable.flush(&db_dir, &sst_file, &options)?;
        let duration = start.elapsed();
        metrics.memtable_size.record(memtable_size as u64);
        metrics.count.inc();
        metrics.duration.record(duration.as_micros() as u64);

        let throughput = sst.size as f64 / duration.as_secs_f64();
        metrics.write_throughput.record(throughput as u64);

        info!(logger, "Memtable flushed, memtable={}, sst={}", memtable.log_number, sst_file.number);
        event!(logger, "flush done, memtable={}, sst={}, duration={}µs",
            memtable.log_number,
            sst_file.number,
            duration.as_micros()
        );

        // load the new sst in the cache to validate that everything when well and made it available
        // straightaway when the memtable is dropped
        let sst_path = &db_dir.join(sst_file.filename());
        sst_cache.get(sst_path)?;
        Ok(sst)
    }

    /// Enqueues a `FlushTask` to be processed by the background thread.
    ///
    /// Returns an error if the task cannot be enqueued.
    pub fn enqueue(&self, task: FlushTask) -> Result<()> {
        self.sender
            .try_send(task)
            .map_err(|e| Error::new(ErrorKind::Other, e))
    }
}

#[cfg(test)]
impl FlushManager {

    /// (Test only) Pauses the flush thread, blocking task processing.
    pub fn pause(&self) {
        if let Some(sync_control) = &self.sync_control {
            let (lock, _) = &**sync_control;
            let mut paused = lock.lock().unwrap();
            *paused = true;
        }
    }

    /// (Test only) Resumes the flush thread if it was paused.
    pub fn resume(&self) {
        if let Some(sync_control) = &self.sync_control {
            let (lock, cvar) = &**sync_control;
            let mut paused = lock.lock().unwrap();
            *paused = false;
            cvar.notify_one();
        }
    }
}

#[allow(dead_code)]
struct Metrics {
    /// Number of flush operations performed.
    count: Arc<Counter>,
    /// Histogram of flush durations (in microseconds).
    duration: Arc<Histogram>,
    /// Histogram tracking the throughput during flushes (in bytes/sec).
    write_throughput: Arc<Histogram>,
    /// Histogram of the size of the memtable flushed (in bytes).
    memtable_size: Arc<Histogram>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            count: Counter::new(),
            duration: Histogram::new_time_histogram(),
            write_throughput: Histogram::new_throughput_histogram(),
            memtable_size: Histogram::new_size_histogram(),
        }
    }

    fn register_to(&self, metric_registry: &mut MetricRegistry) {
        metric_registry
            .register_counter("flush_count", self.count.clone())
            .register_histogram("flush_duration", self.duration.clone())
            .register_histogram("flush_write_throughput", self.write_throughput.clone())
            .register_histogram("flush_memtable_size", self.memtable_size.clone());
    }
}
