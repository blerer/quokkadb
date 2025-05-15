use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A `Statistics` struct for tracking server performance metrics.
#[derive(Debug)]
pub struct ServerStatistics {
    /// Tracks the number of Memtable hits (reads satisfied by the MemTable).
    pub memtable_hit: Counter,

    /// Tracks the number of Memtable misses (reads not found in the MemTable).
    pub memtable_miss: Counter,

    /// Tracks the total number of inserts (PUT operations) into the MemTable.
    pub memtable_total_inserts: Counter,

    /// Tracks the total number of delete operations applied to the MemTable.
    pub memtable_total_deletes: Counter,

    /// The ratio of MemTable hits to the total number of lookups (hits + misses)
    pub mem_table_hit_ratio: Gauge,

    /// Number of WAL files currently present
    pub wal_files: Counter,

    /// Total size (in bytes) of all WAL files
    pub wal_total_bytes: Counter,

    /// Number of times the WAL has been explicitly synced to disk
    pub wal_syncs: Counter,

    /// Number of bytes currently buffered by the wal
    pub wal_bytes_buffered: Counter,

    /// The total number of bytes successfully written to the WAL file(s)
    /// This number refers to bytes written to the file descriptor (i.e., the OS buffer), not
    /// necessarily flushed to disk.
    pub wal_bytes_written: Counter,

    /// The number of time the Manifest has been rewritten
    pub manifest_rewrite: Counter,

    /// The number of edit written to the manifest (including the snapshot)
    pub manifest_writes: Counter,

    /// The number of bytes written to the manifest
    pub manifest_bytes_written: Counter,

    /// The current manifest size
    pub manifest_size: Counter,
}

impl ServerStatistics {
    /// Creates a new `ServerStatistics` instance.
    ///
    /// # Returns
    /// A thread-safe, reference-counted `ServerStatistics` instance.
    pub fn new() -> Arc<Self> {

        let memtable_hit= Counter::new("memtable_hit");
        let memtable_miss= Counter::new("memtable_miss");

        Arc::new(Self {
            memtable_hit: memtable_hit.clone(),
            memtable_miss: memtable_miss.clone(),
            memtable_total_inserts: Counter::new("memtable_total_inserts"),
            memtable_total_deletes: Counter::new("memtable_total_deletes"),
            mem_table_hit_ratio: Gauge::new( "mem_table_hit_ratio",Box::new({
                let hits = memtable_hit;
                let misses = memtable_miss;
                move || {
                    let h = hits.get() as f64;
                    let m = misses.get() as f64;
                    if h + m == 0.0 { 0.0 } else { h / (h + m) }
                }
            })),
            wal_files: Counter::new("wal_files"),
            wal_total_bytes: Counter::new("wal_total_bytes"),
            wal_syncs: Counter::new("wal_syncs"),
            wal_bytes_buffered: Counter::new("wal_bytes_buffered"),
            wal_bytes_written: Counter::new("wal_bytes_written"),
            manifest_writes: Counter::new("manifest_writes"),
            manifest_size: Counter::new("manifest_size"),
            manifest_bytes_written: Counter::new("manifest_bytes_written"),
            manifest_rewrite: Counter::new("manifest_rewrite"),
        })
    }
}

/// A common trait implemented by all metric types (e.g., `Counter`, `Gauge`).
///
/// This trait provides a unified interface for exposing and exporting metric values,
/// typically for monitoring or debugging purposes.
///
/// All metrics must provide:
/// - a name (`&str`) for identification
/// - a floating-point representation of their current value (`f64`)
///
/// # Examples
///
/// ```
/// use quokkadb::statistics::Metric;
/// fn print_metric(metric: &dyn Metric) {
///     println!("{} = {}", metric.name(), metric.as_f64());
/// }
/// ```
pub trait Metric: Send + Sync {
    fn name(&self) -> &str;
    fn as_f64(&self) -> f64;
}

/// A `Counter` tracks a monotonically increasing (or manually decreasing) 64-bit unsigned value,
/// typically used for metrics like event counts, byte totals, or operation tallies.
///
/// Internally backed by an `AtomicU64`, it is safe for concurrent updates.
///
/// # Examples
///
/// ```
/// use quokkadb::statistics::Counter;
/// let counter = Counter::new("requests");
/// counter.inc(1);
/// println!("Request count: {}", counter.get());
/// ```
#[derive(Debug, Clone)]
pub struct Counter {
    name: String,
    atomic: Arc<AtomicU64>,
}

impl Counter {

    /// Creates a new counter with the given name.
    pub fn new(name: &str) -> Self {
        Counter { name: name.to_string(), atomic: Arc::new(AtomicU64::new(0)) }
    }

    /// Returns the current value of the counter.
    pub fn get(&self) -> u64 {
        self.atomic.load(Ordering::Relaxed)
    }

    /// Increments the counter by the given amount.
    pub fn inc(&self, amount: u64)  {
        self.atomic.fetch_add(amount, Ordering::Relaxed);
    }

    /// Decrements the counter by the given amount.
    pub fn dec(&self, amount: u64) {
        self.atomic.fetch_sub(amount, Ordering::Relaxed);
    }
}

impl Metric for Counter {
    fn name(&self) -> &str {
        &self.name
    }
    fn as_f64(&self) -> f64 {
        self.get() as f64
    }
}

/// A `Gauge` is a floating-point metric computed on demand using a closure.
/// It is useful for tracking derived values such as ratios, averages, or memory usage snapshots.
///
/// Gauges are evaluated at the time of access, and are not internally updated or stored.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use quokkadb::statistics::{Counter, Gauge};
/// let a = Arc::new(Counter::new("a"));
/// let b = Arc::new(Counter::new("b"));
/// let ratio = Gauge::new("hit_ratio", Box::new({
///     let a = a.clone();
///     let b = b.clone();
///     move || {
///         let total = a.get() + b.get();
///         if total == 0 { 0.0 } else { a.get() as f64 / total as f64 }
///     }
/// }));
/// println!("Hit ratio: {}", ratio.value());
/// ```
pub struct Gauge {
    /// Returns the name of the metric (e.g., `"cache_hits"` or `"wal_bytes_written"`).
    name: String,

    /// Returns the current value of the metric as an `f64`.
    /// For integer-based metrics (e.g., `Counter`), this is a cast.
    compute: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl Gauge {

    /// Creates a new gauge with a name and a closure to compute its value.
    pub fn new(name: &str, compute: Box<dyn Fn() -> f64 + Send + Sync>) -> Self {
        Self { name: name.to_string(), compute }
    }

    /// Returns the current computed value of the gauge.
    pub fn value(&self) -> f64 {
        (self.compute)()
    }
}

impl Metric for Gauge {
    fn name(&self) -> &str {
        &self.name
    }
    fn as_f64(&self) -> f64 {
        self.value()
    }
}

impl std::fmt::Debug for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gauge")
            .field("name", &self.name)
            .field("value", &self.value())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_table_hit_ratio() {
        let stats = ServerStatistics::new();

        // Initially: 0 hits, 0 misses → ratio should be 0.0
        assert_eq!(stats.mem_table_hit_ratio.value(), 0.0);

        // Add 3 hits, 1 miss
        stats.memtable_hit.inc(3);
        stats.memtable_miss.inc(1);

        // Ratio = 3 / (3 + 1) = 0.75
        let ratio = stats.mem_table_hit_ratio.value();
        assert!((ratio - 0.75).abs() < f64::EPSILON);

        // Add 1 more miss → total: 3 hits, 2 misses → ratio = 0.6
        stats.memtable_miss.inc(1);
        let ratio = stats.mem_table_hit_ratio.value();
        assert!((ratio - 0.6).abs() < f64::EPSILON);
    }

    fn test_clone_counter() {
        let counter = Counter::new("counter");
        counter.inc(3);
        assert_eq!(counter.get(), 3);
        let clone = counter.clone();
        assert_eq!(clone.get(), 3);
        clone.dec(2);
        assert_eq!(clone.get(), 1);
        assert_eq!(counter.get(), 1);
    }
}