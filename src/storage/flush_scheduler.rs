use std::{
    sync::{mpsc::{sync_channel, SyncSender, Receiver}, Arc},
    thread::{self, JoinHandle},
    time::Duration,
};
use std::path::Path;
use tracing::error;
use std::io::{Error, ErrorKind, Result};
use crate::io::fd_cache::FileDescriptorCache;
use crate::options::options::Options;
use crate::storage::callback::Callback;
use crate::storage::memtable::Memtable;
use crate::storage::files::DbFile;
use crate::storage::lsm_tree::LsmTreeEdit;

pub struct FlushTask {
    pub db_file: DbFile,
    pub memtable: Arc<Memtable>,
    pub callback: Option<Arc<dyn Callback<Result<LsmTreeEdit>>>>,
}

pub struct FlushScheduler {
    sender: SyncSender<FlushTask>,
    thread: Option<JoinHandle<()>>,
}

impl FlushScheduler {
    pub fn new(db_dir: &Path, options: Arc<Options>, fd_cache: Arc<FileDescriptorCache>) -> Self {
        let (sender, receiver): (SyncSender<FlushTask>, Receiver<FlushTask>) = sync_channel(32);
        let db_dir = db_dir.to_path_buf();
        let thread = {

            thread::spawn(move || {
                while let Ok(task) = receiver.recv() {
                    let FlushTask { db_file, memtable, callback } = task;
                    let fd_cache = fd_cache.clone();
                    let result = memtable.flush(fd_cache, &db_dir, &db_file, &options);

                    match result {
                        Ok(sst) => {
                            let edit = LsmTreeEdit::Flush { log_number: memtable.log_number, sst: Arc::new(sst)};
                            if let Some(callback) = callback {
                                let _ = callback.call(Ok(edit));
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