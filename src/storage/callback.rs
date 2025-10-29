use crate::obs::logger::{LogLevel, LoggerAndTracer};
use std::io::{Error, ErrorKind, Result};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use crate::error;

pub enum Callback<T> {
    Async(AsyncCallback<T>),
    Blocking(BlockingCallback<T>),
}

impl<T> Callback<T> {
    pub fn new_async<F>(logger: Arc<dyn LoggerAndTracer>, f: F) -> Arc<Self>
    where
        F: Fn(T) -> Result<()> + Send + Sync + 'static,
        T: Send + 'static,
    {
        Arc::new(Callback::Async(AsyncCallback::new(logger, f)))
    }

    pub fn new_blocking(f: Box<dyn Fn(T) -> Result<()> + Send + Sync>) -> Arc<Self> {
        Arc::new(Callback::Blocking(BlockingCallback::new(f)))
    }

    pub fn call(&self, value: T)
    where
        T: Send + 'static,
    {
        match self {
            Callback::Async(async_cb) => {
                async_cb.call(value);
            }
            Callback::Blocking(blocking_cb) => {
                blocking_cb.call(value);
            }
        }
    }

    pub fn is_blocking(&self) -> bool {
        matches!(self, Callback::Blocking(_))
    }

    pub fn await_blocking(&self) -> Result<()> {
        match self {
            Callback::Blocking(blocking_cb) => blocking_cb.await_blocking(),
            _ => Err(Error::new(
                ErrorKind::Other,
                "Unsupported await_blocking call to an async callback",
            )),
        }
    }
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

pub struct BlockingCallback<T> {
    sender: SyncSender<Result<()>>,
    receiver: Arc<Mutex<Option<Receiver<Result<()>>>>>,
    fun: Box<dyn Fn(T) -> Result<()> + Send + Sync>,
    _phantom: PhantomData<T>,
}

impl<T> BlockingCallback<T>
{
    pub fn new(f: Box<dyn Fn(T) -> Result<()> + Send + Sync>) -> Self {
        let (sender, receiver): (SyncSender<Result<()>>, Receiver<Result<()>>) = sync_channel(1);

        Self {
            sender,
            receiver: Arc::new(Mutex::new(Some(receiver))),
            fun: f,
            _phantom: PhantomData,
        }
    }

    fn call(&self, value: T) {
        let result = (self.fun)(value);
        self.sender.send(result).unwrap();
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
