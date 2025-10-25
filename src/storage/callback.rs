use crate::obs::logger::{LogLevel, LoggerAndTracer};
use std::io::{Error, ErrorKind, Result};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use crate::error;

pub trait Callback<T>: Sync + Send {
    fn call(&self, value: T);
}

pub struct AsyncCallback<T> {
    logger: Arc<dyn LoggerAndTracer>,
    fun: Box<dyn Fn(T) -> Result<()> + Send + Sync>,
}

impl<T: Send + 'static> AsyncCallback<T> {
    pub fn new<F>(logger: Arc<dyn LoggerAndTracer>, f: F) -> Self
    where
        F: Fn(T) -> Result<()> + Send + Sync + 'static,
    {
        Self {
            logger,
            fun: Box::new(f),
        }
    }

    pub fn call(&self, value: T) {
        if let Err(err) = (self.fun)(value) {
            error!(self.logger, "AsyncCallback function returned an error: {}", err);
        }
    }
}

use std::marker::PhantomData;

pub struct BlockingCallback<T, F> {
    sender: SyncSender<Result<()>>,
    receiver: Arc<Mutex<Option<Receiver<Result<()>>>>>,
    fun: F,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync, F: Fn(T) -> Result<()> + Send + Sync + 'static> Callback<T>
    for BlockingCallback<T, F>
{
    fn call(&self, value: T) {
        let result = (self.fun)(value);
        self.sender.send(result).unwrap();
    }
}

impl<T, F> BlockingCallback<T, F>
where
    F: Fn(T) -> Result<()> + Send + Sync + 'static,
{
    pub fn new(f: F) -> Self {
        let (sender, receiver): (SyncSender<Result<()>>, Receiver<Result<()>>) = sync_channel(1);

        Self {
            sender,
            receiver: Arc::new(Mutex::new(Some(receiver))),
            fun: f,
            _phantom: PhantomData,
        }
    }

    pub fn await_blocking(&self) -> Result<()> {
        if let Some(receiver) = self.receiver.lock().unwrap().take() {
            match receiver.recv() {
                Ok(result) => result,
                Err(error) => Err(Error::new(ErrorKind::Interrupted, error)),
            }
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Unsupported await_blocking call to an already exhausted callback",
            ))
        }
    }
}
