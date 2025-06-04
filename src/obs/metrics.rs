use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct MetricRegistry {
    counters: BTreeMap<String, Arc<Counter>>,
    gauges: BTreeMap<String, Arc<dyn Gauge>>,
    histograms: BTreeMap<String, Arc<Histogram>>,
    computed: BTreeMap<String, Arc<dyn Computed>>,
}

impl MetricRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_counter(&mut self, name: &str, counter: Arc<Counter>) -> &mut Self {
        self.counters.insert(name.to_string(), counter);
        self
    }

    pub fn register_gauge(&mut self, name: &str, gauge: Arc<dyn Gauge>) -> &mut Self {
        self.gauges.insert(name.to_string(), gauge);
        self
    }

    pub fn register_computed(&mut self, name: &str, computed: Arc<dyn Computed>) -> &mut Self {
        self.computed.insert(name.to_string(), computed);
        self
    }

    pub fn register_histogram(&mut self, name: &str, histogram: Arc<Histogram>) -> &mut Self {
        self.histograms.insert(name.to_string(), histogram);
        self
    }

    pub fn get_counter(&self, name: &str) -> Option<Arc<Counter>> {
        self.counters.get(name).cloned()
    }

    pub fn get_gauge(&self, name: &str) -> Option<Arc<dyn Gauge>> {
        self.gauges.get(name).cloned()
    }

    pub fn get_computed(&self, name: &str) -> Option<Arc<dyn Computed>> {
        self.computed.get(name).cloned()
    }

    pub fn get_histogram(&self, name: &str) -> Option<Arc<Histogram>> {
        self.histograms.get(name).cloned()
    }
}

#[derive(Default)]
pub struct Counter {
    atomic: AtomicU64,
}

impl Counter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns the current value of the counter.
    pub fn get(&self) -> u64 {
        self.atomic.load(Ordering::Relaxed)
    }

    /// Increments the counter by one.
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Increments the counter by the given amount.
    pub fn inc_by(&self, amount: u64) {
        self.atomic.fetch_add(amount, Ordering::Relaxed);
    }
}

pub trait Gauge: Send + Sync {
    /// Returns the current value of the gauge as an `u64`.
    fn get(&self) -> u64;
}

pub struct DerivedGauge {
    /// Returns the current value of the metric as an `f64`.
    /// For integer-based metrics (e.g., `Counter`), this is a cast.
    compute: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl DerivedGauge {
    pub fn new(compute: Arc<dyn Fn() -> u64 + Send + Sync>) -> Arc<Self> {
        Arc::new(Self { compute })
    }
}

impl Gauge for DerivedGauge {

    /// Returns the current computed value of the gauge.
    fn get(&self) -> u64 {
        (self.compute)()
    }
}

#[derive(Default)]
pub struct AtomicGauge {
    atomic: Arc<AtomicU64>,
}

impl AtomicGauge {
    pub fn new() -> Arc<Self> {
        Arc::new(AtomicGauge::default())
    }

    pub fn inc(&self) {
        self.inc_by(1);
    }

    pub fn inc_by(&self, amount: u64) {
        self.atomic.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.dec_by(1);
    }

    pub fn dec_by(&self, amount: u64) {
        self.atomic.fetch_sub(amount, Ordering::Relaxed);
    }
}

impl Gauge for AtomicGauge {
    /// Returns the current value of the atomic gauge.
    fn get(&self) -> u64 {
        self.atomic.load(Ordering::Relaxed)
    }
}

pub trait Computed: Send + Sync {
    /// Returns the current computed value as an `f64`.
    fn get(&self) -> f64;
}

pub struct HitRatio {
    hit_counter: Arc<Counter>,
    miss_counter: Arc<Counter>,
}

impl HitRatio {
    pub fn new(hit_counter: Arc<Counter>, miss_counter: Arc<Counter>) -> Arc<Self> {
        Arc::new(Self {
            hit_counter,
            miss_counter,
        })
    }
}

impl Computed for HitRatio {
    /// Returns the current hit ratio as an `f64`.
    fn get(&self) -> f64 {
        let h = self.hit_counter.get() as f64;
        let m = self.miss_counter.get() as f64;
        if h + m == 0.0 {
            0.0
        } else {
            h / (h + m)
        }
    }
}

/// A thread-safe histogram implementation using fixed bucket boundaries.
///
/// Buckets are expected to be sorted and non-overlapping. This structure tracks:
/// - Count of samples per bucket
/// - Total count and sum
/// - Min, max, mean, standard deviation
/// - Quantile estimates based on bucketed data
pub struct Histogram {
    buckets: Vec<(u64, AtomicU64)>, // (upper_bound, count)
    count: AtomicU64,
    sum: AtomicU64,
    sum_of_squares: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

impl Histogram {
    pub fn new_time_histogram() -> Arc<Self> {
        Self::new(&time_buckets())
    }

    pub fn new_size_histogram() -> Arc<Self> {
        Self::new(&size_buckets())
    }

    pub fn new_throughput_histogram() -> Arc<Self> {
        Self::new(&throughput_buckets())
    }

    /// Creates a new histogram with the specified upper bounds for each bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_bounds` - A slice of upper bounds for the histogram buckets.
    pub fn new(bucket_bounds: &[u64]) -> Arc<Self> {
        let buckets = bucket_bounds
            .iter()
            .map(|&b| (b, AtomicU64::new(0)))
            .collect();

        Arc::new(Self {
            buckets,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            sum_of_squares: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        })
    }

    /// Records a new value into the histogram.
    ///
    /// Finds the first bucket with an upper bound ≥ `value` and increments its count.
    /// Also updates total count, sum, sum of squares, and min/max.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to record (e.g., duration in microseconds).
    pub fn record(&self, value: u64) {
        let mut i = match self.buckets.binary_search_by_key(&value, |(b, _)| *b) {
            Ok(i) | Err(i) => i,
        };

        if i >= self.buckets.len() {
            i = self.buckets.len() - 1;
        }

        self.buckets[i].1.fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.sum_of_squares
            .fetch_add(value * value, Ordering::Relaxed);

        // min and max: relaxed atomic min/max
        self.min.fetch_min(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    /// Produces a snapshot of the current histogram state.
    ///
    /// Returns a `HistogramSnapshot` containing computed metrics like count,
    /// mean, standard deviation, and per-bucket counts.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let sum_of_squares = self.sum_of_squares.load(Ordering::Relaxed);
        let min = self.min.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);

        let mean = if count > 0 {
            sum as f64 / count as f64
        } else {
            0.0
        };

        let variance = if count > 1 {
            let mean_sq = mean * mean;
            let avg_sq = sum_of_squares as f64 / count as f64;
            (avg_sq - mean_sq).max(0.0)
        } else {
            0.0
        };

        let stddev = variance.sqrt();

        let mut counts = vec![];
        for (bound, c) in &self.buckets {
            let v = c.load(Ordering::Relaxed);
            counts.push((*bound, v));
        }

        HistogramSnapshot {
            count,
            sum,
            min,
            max,
            mean,
            stddev,
            buckets: counts,
        }
    }

    /// Estimates the value at a given quantile using cumulative bucket counts.
    ///
    /// # Arguments
    ///
    /// * `quantile` - A value between 0.0 and 1.0 (e.g., 0.99 for p99).
    ///
    /// # Returns
    ///
    /// The upper bound of the first bucket meeting the quantile.
    pub fn estimate_quantile(&self, quantile: f64) -> Option<u64> {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }

        let target = (quantile * total as f64).ceil() as u64;
        let mut cumulative = 0;

        for (bound, c) in &self.buckets {
            cumulative += c.load(Ordering::Relaxed);
            if cumulative >= target {
                return Some(*bound);
            }
        }
        self.buckets.last().map(|(b, _)| *b)
    }
}

/// Represents an immutable snapshot of a histogram's state for reporting or display.
pub struct HistogramSnapshot {
    /// Number of recorded values.
    pub count: u64,
    /// Sum of all recorded values.
    pub sum: u64,
    /// Minimum value recorded.
    pub min: u64,
    /// Maximum value recorded.
    pub max: u64,
    /// Average value.
    pub mean: f64,
    /// Standard deviation of recorded values.
    pub stddev: f64,
    /// Buckets: each is (upper_bound, count).
    pub buckets: Vec<(u64, u64)>,
}

/// Generates exponential bucket upper-bounds in microseconds for latency/duration histograms.
///
/// Used for metrics like `flush.duration`. Buckets range from 1µs to 8s (8_388_608 µs).
///
/// # Returns
///
/// A vector of bucket upper bounds in microseconds: `[1, 2, 4, ..., 8_388_608]`
pub fn time_buckets() -> Vec<u64> {
    let mut buckets = vec![];
    let mut v = 1;
    while v <= 8_000_000 {
        buckets.push(v);
        v *= 2;
    }
    buckets
}

/// Generates exponential throughput bucket upper bounds for write performance histograms.
///
/// Used for metrics like `flush.write-throughput`. Buckets are in bytes/sec, from 1 KB/s to 1 GB/s.
///
/// # Returns
///
/// A vector of bucket upper bounds in bytes/sec: `[1<<10, 1<<11, ..., 1<<30]`
pub fn throughput_buckets() -> Vec<u64> {
    let mut buckets = vec![];
    let mut v = 1 << 10; // Start at 1 KB/s
    while v <= 1 << 30 {
        // Up to 1 GB/s
        buckets.push(v);
        v *= 2;
    }
    buckets
}

/// Generates exponential size bucket upper bounds for metrics measuring data sizes.
///
/// Used for metrics like `flush.memtable.size`. Buckets are in bytes, from 1 KB to 1 GB.
///
/// # Returns
///
/// A vector of bucket upper bounds in bytes: `[1<<10, 1<<11, ..., 1<<30]`
pub fn size_buckets() -> Vec<u64> {
    let mut buckets = vec![];
    let mut v = 1 << 10; // 1 KB
    while v <= 1 << 30 {
        // 1 GB
        buckets.push(v);
        v *= 2;
    }
    buckets
}

#[cfg(test)]
pub fn assert_counter_eq(registry: &MetricRegistry, name: &str, expected: u64) {
    if let Some(counter) = registry.get_counter(name) {
        assert_eq!(counter.get(), expected, "Counter '{}' mismatch", name);
    } else {
        panic!("Counter '{}' not found in registry", name);
    }
}

#[cfg(test)]
pub fn assert_gauge_eq(registry: &MetricRegistry, name: &str, expected: u64) {
    if let Some(gauge) = registry.get_gauge(name) {
        assert_eq!(gauge.get(), expected, "Gauge '{}' mismatch", name);
    } else {
        panic!("Gauge '{}' not found in registry", name);
    }
}

#[cfg(test)]
mod tests {

    mod hit_ratio {
        use crate::obs::metrics::{Computed, Counter};
        use crate::obs::metrics::HitRatio;

        #[test]
        fn test_mem_table_hit_ratio() {
            let hits = Counter::new();
            let misses = Counter::new();
            let ratio = HitRatio::new(hits.clone(), misses.clone());

            // Initially: 0 hits, 0 misses → ratio should be 0.0
            assert_eq!(ratio.get(), 0.0);

            // Add 3 hits, 1 miss
            hits.inc_by(3);
            misses.inc();

            // Ratio = 3 / (3 + 1) = 0.75
            assert!((ratio.get() - 0.75).abs() < f64::EPSILON);

            // Add 1 more miss → total: 3 hits, 2 misses → ratio = 0.6
            misses.inc();
            assert!((ratio.get() - 0.6).abs() < f64::EPSILON);
        }
    }

    mod histogram {
        use crate::obs::metrics::Histogram;
        use std::sync::Arc;

        fn make_histogram() -> Arc<Histogram> {
            Histogram::new(&[1, 2, 4, 8, 16, 32, 64])
        }

        #[test]
        fn test_basic_stats() {
            let hist = make_histogram();
            let values = [1, 2, 3, 4, 5];

            for v in values {
                hist.record(v);
            }

            let snap = hist.snapshot();
            assert_eq!(snap.count, 5);
            assert_eq!(snap.min, 1);
            assert_eq!(snap.max, 5);
            assert_eq!(snap.sum, 15);
            assert!((snap.mean - 3.0).abs() < 1e-6);
            assert!((snap.stddev - 1.4142).abs() < 0.01); // ≈ sqrt(2)
        }

        #[test]
        fn test_bucket_counts() {
            let hist = make_histogram();
            hist.record(1);
            hist.record(2);
            hist.record(5);
            hist.record(33);

            let snap = hist.snapshot();
            let buckets = snap.buckets;

            let expected = vec![
                (1, 1), // 1
                (2, 1), // 2
                (4, 0), // none
                (8, 1), // 5
                (16, 0),
                (32, 0),
                (64, 1), // 33
            ];
            assert_eq!(buckets, expected);
        }

        #[test]
        fn test_quantiles() {
            let hist = make_histogram();
            for v in [1, 2, 3, 4, 5, 6, 7] {
                hist.record(v);
            }

            assert_eq!(hist.estimate_quantile(0.0), Some(1));
            assert_eq!(hist.estimate_quantile(0.5), Some(4)); // median
            assert_eq!(hist.estimate_quantile(1.0), Some(8));
        }
    }
}
