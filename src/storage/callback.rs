use std::io::{Error, ErrorKind, Result};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use std::thread::JoinHandle;
use crate::obs::logger::LoggerAndTracer;

pub trait Callback<T>: Sync + Send {
    fn call(&self, value: T);
}

pub struct AsyncCallback<T> {
    sender: SyncSender<T>,
    thread: Option<JoinHandle<()>>,
}

impl<T: Send + Sync> AsyncCallback<T> {
    pub fn new<F>(logger:Arc<dyn LoggerAndTracer>, f: F) -> Self
    where
    F: Fn(T) -> Result<()> + Send + 'static,
    T: Send + Sync + 'static,
    {
        let (sender, receiver): (SyncSender<T>, Receiver<T>) = sync_channel(32);

        let thread = {

            thread::spawn(move || {
                while let Ok(result) = receiver.recv() {
                    let result = f(result);
                    if let Err(err) = result {
                        logger.error(format_args!("AsyncCallback function returned an error: {}", err));
                    }
                }
            })
        };

        Self {
            sender,
            thread: Some(thread),
        }
    }

    pub fn call(&self, value: T) {
        self.sender.send(value).unwrap();
    }
}

pub struct BlockingCallback<T, F>
{
    sender: SyncSender<T>,
    receiver: Arc<Mutex<Option<Receiver<T>>>>,
    fun: F,
}

impl<T: Send + Sync, F: Fn(T) -> Result<()> + Send + Sync + 'static,> Callback<T> for BlockingCallback<T, F> {
    fn call(&self, value: T) {
        self.sender.send(value).unwrap();
    }
}

impl<T: Send + Sync, F: Fn(T) -> Result<()> + Send + 'static,> BlockingCallback<T, F> {

    pub fn new(f: F) -> Self
    {
        let (sender, receiver): (SyncSender<T>, Receiver<T>) = sync_channel(1);

        Self {
            sender,
            receiver: Arc::new(Mutex::new(Some(receiver))),
            fun: f,
        }
    }

    pub fn await_blocking(&self) -> Result<()> {
        if let Some(receiver) = self.receiver.lock().unwrap().take() {
            match receiver.recv() {
                Ok(result) => {
                    return (self.fun)(result)
                },
                Err(error) => {
                    return Err(Error::new(ErrorKind::Interrupted, error))
                },
            }
        }
        Err(Error::new(ErrorKind::Other, "Unsupported await_blocking call to an already exhausted callback"))
    }
}