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
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use crate::{error, event, info};

pub struct FlushTask {
    pub sst_file: DbFile,
    pub memtable: Arc<Memtable>,
    pub callback: Option<Arc<dyn Callback<Result<SSTableOperation>>>>,
}

pub struct FlushManager {
    sender: SyncSender<FlushTask>,
    thread: Option<JoinHandle<()>>,
}

impl FlushManager {
    pub fn new(
        logger: Arc<dyn LoggerAndTracer>,
        metric_registry: &mut MetricRegistry,
        options: Arc<Options>,
        db_dir: &Path,
        sst_cache: Arc<SSTableCache>,
    ) -> Self {
        let (sender, receiver): (SyncSender<FlushTask>, Receiver<FlushTask>) = sync_channel(32);
        let db_dir = db_dir.to_path_buf();
        let log = logger.clone();
        let metrics = Metrics::new();
        metrics.register_to(metric_registry);
        let thread = {
            thread::spawn(move || {
                while let Ok(task) = receiver.recv() {
                    let FlushTask {
                        sst_file,
                        memtable,
                        callback,
                    } = task;
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
                            if let Some(callback) = callback {
                                let _ = callback.call(Ok(operation));
                            }
                        }
                        Err(e) => {
                            error!(log, "Flush failed: {}", e);
                            if let Some(callback) = callback {
                                let _ = callback.call(Err(e));
                            }
                            thread::sleep(Duration::from_secs(1));
                        }
                    }
                }
            })
        };

        info!(logger, "FlushManager initialized");

        Self {
            sender,
            thread: Some(thread),
        }
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

    pub fn enqueue(&self, task: FlushTask) -> Result<()> {
        self.sender
            .try_send(task)
            .map_err(|e| Error::new(ErrorKind::Other, e))
    }
}

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
