use std::fmt::Arguments;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Severity levels for log messages.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Fine-grained, low-level diagnostic messages.
    Debug,
    /// Informational messages about normal operation.
    Info,
    /// Warnings about unexpected but non-fatal behavior.
    Warn,
    /// Errors that may require attention.
    Error,
}

/// Interface for structured logging and event tracing.
///
/// This trait supports level-based logging with efficient formatting
/// using `std::fmt::Arguments`, and provides methods for emitting structured
/// trace events and checking logging/tracing status.
pub trait LoggerAndTracer: Send + Sync {
    /// Logs a formatted message at the specified level.
    fn log(&self, level: LogLevel, msg: Arguments);

    /// Convenience method for `Info` level logging.
    fn info(&self, msg: Arguments) {
        self.log(LogLevel::Info, msg);
    }

    /// Convenience method for `Warn` level logging.
    fn warn(&self, msg: Arguments) {
        self.log(LogLevel::Warn, msg);
    }

    /// Convenience method for `Error` level logging.
    fn error(&self, msg: Arguments) {
        self.log(LogLevel::Error, msg);
    }

    /// Convenience method for `Debug` level logging.
    fn debug(&self, msg: Arguments) {
        self.log(LogLevel::Debug, msg);
    }

    /// Emits a trace event message. Format should follow:
    ///
    /// `event: <action>, key1=value1, key2=value2`
    ///
    /// Example:
    /// `event: flush start, level=0, reason=log count`
    fn event(&self, event: Arguments);

    /// Returns `true` if tracing events are enabled.
    fn is_tracing_enabled(&self) -> bool;

    /// Returns `true` if the given log level is currently enabled.
    fn level_enabled(&self, level: LogLevel) -> bool;
}

/// A simple logger that prints messages to stdout with timestamps and thread IDs.
pub struct StdoutLogger {
    /// Minimum log level to emit.
    pub min_level: LogLevel,
    /// Whether structured trace events are enabled.
    pub tracing_enabled: bool,
}

impl StdoutLogger {
    pub fn new(min_level: LogLevel, tracing_enabled: bool) -> Arc<Self> {
        Arc::new(StdoutLogger { tracing_enabled, min_level })
    }

    /// Returns current timestamp in microseconds since UNIX_EPOCH.
    fn now_micros() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    }
}

impl LoggerAndTracer for StdoutLogger {
    fn log(&self, level: LogLevel, msg: Arguments) {
        if self.level_enabled(level) {
            let timestamp = Self::now_micros();
            let thread_id = std::thread::current().id();
            println!("[{:?}] [{}] [thread={:?}] {}", level, timestamp, thread_id, msg);
        }
    }

    fn event(&self, event: Arguments) {
        if self.tracing_enabled {
            let timestamp = Self::now_micros();
            let thread_id = std::thread::current().id();
            println!("[TRACE] [{}] [thread={:?}] {}", timestamp, thread_id, event);
        }
    }

    fn is_tracing_enabled(&self) -> bool {
        self.tracing_enabled
    }

    fn level_enabled(&self, level: LogLevel) -> bool {
        level >= self.min_level
    }
}

#[cfg(test)]
pub fn test_instance() -> Arc<dyn LoggerAndTracer> {
    use crate::obs::logger::{LogLevel, StdoutLogger};
    StdoutLogger::new(LogLevel::Debug, true)
}