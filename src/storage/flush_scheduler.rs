use std::{
    sync::{mpsc::{sync_channel, SyncSender, Receiver}, Arc},
    thread::{self, JoinHandle},
    time::Duration,
};
use std::path::Path;
use tracing::error;
use std::io::{Error, ErrorKind, Result};
use crate::options::options::Options;
use crate::storage::callback::Callback;
use crate::storage::memtable::Memtable;
use crate::storage::files::DbFile;
use crate::storage::sstable::sstable_cache::SSTableCache;
use crate::storage::storage_engine::SSTableOperation;

pub struct FlushTask {
    pub db_file: DbFile,
    pub memtable: Arc<Memtable>,
    pub callback: Option<Arc<dyn Callback<Result<SSTableOperation>>>>,
}

pub struct FlushScheduler {
    sender: SyncSender<FlushTask>,
    thread: Option<JoinHandle<()>>,
}

impl FlushScheduler {
    pub fn new(db_dir: &Path, options: Arc<Options>, sst_cache: Arc<SSTableCache>) -> Self {
        let (sender, receiver): (SyncSender<FlushTask>, Receiver<FlushTask>) = sync_channel(32);
        let db_dir = db_dir.to_path_buf();
        let thread = {

            thread::spawn(move || {
                while let Ok(task) = receiver.recv() {
                    let FlushTask { db_file, memtable, callback } = task;
                    let sst_cache = sst_cache.clone();
                    let result = memtable.flush(sst_cache, &db_dir, &db_file, &options);

                    match result {
                        Ok(sst) => {
                            let operation = SSTableOperation::Flush { log_number: memtable.log_number, flushed: Arc::new(sst)};
                            if let Some(callback) = callback {
                                let _ = callback.call(Ok(operation));
                            }
                        }
                        Err(e) => {
                            error!("Flush failed: {}", e);
                            if let Some(callback) = callback {
                                let _ = callback.call(Err(e));
                            }
                            thread::sleep(Duration::from_secs(1));
                        }
                    }
                }
            })
        };

        Self {
            sender,
            thread: Some(thread),
        }
    }

    pub fn enqueue(&self, task: FlushTask) -> Result<()> {
        self.sender.try_send(task).map_err(|e| Error::new(ErrorKind::Other, e))
    }
}