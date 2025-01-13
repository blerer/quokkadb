use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::storage::operation::OperationType;

/// A `Statistics` struct for tracking collection performance metrics.
#[derive(Default, Debug)]
pub struct CollectionStatistics {
    /// Tracks the number of Memtable hits (reads satisfied by the MemTable).
    pub memtable_hit: AtomicU64,

    /// Tracks the number of Memtable misses (reads not found in the MemTable).
    pub memtable_miss: AtomicU64,

    /// Tracks the total number of inserts (PUT operations) into the MemTable.
    pub memtable_total_inserts: AtomicU64,

    /// Tracks the total number of delete operations applied to the MemTable.
    pub memtable_total_deletes: AtomicU64,
}

impl CollectionStatistics {
    /// Creates a new `Statistics` instance.
    ///
    /// # Returns
    /// A thread-safe, reference-counted `Statistics` instance.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Increments the `mem_table_hit` counter by 1.
    ///
    /// This should be called whenever a read operation is satisfied by the MemTable.
    pub fn increment_mem_table_hit(&self) {
        self.memtable_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the `mem_table_miss` counter by 1.
    ///
    /// This should be called whenever a read operation is not found in the MemTable.
    pub fn increment_mem_table_miss(&self) {
        self.memtable_miss.fetch_add(1, Ordering::Relaxed);
    }

    /// Update the memtable operation counters.
    ///
    /// # Arguments
    /// - `operation`: The type of the operation that has been performed
    pub fn add_memtable_operation(&self, operation: OperationType) {
        match operation {
            OperationType::Put => { self.memtable_total_inserts.fetch_add(1, Ordering::Relaxed); }
            OperationType::Delete => {self.memtable_total_deletes.fetch_add(1, Ordering::Relaxed); }
            _ => panic!("Unexpected operation type {:?}", operation),
        }
    }

    /// Retrieves the current value of `mem_table_hit`.
    ///
    /// # Returns
    /// The total number of MemTable hits as a `u64`.
    pub fn get_mem_table_hit(&self) -> u64 {
        self.memtable_hit.load(Ordering::Relaxed)
    }

    /// Retrieves the current value of `mem_table_miss`.
    ///
    /// # Returns
    /// The total number of MemTable misses as a `u64`.
    pub fn get_mem_table_miss(&self) -> u64 {
        self.memtable_miss.load(Ordering::Relaxed)
    }

    /// Retrieves the ratio of MemTable hits to the total number of lookups (hits + misses)
    ///
    /// # Returns
    /// The ratio of Memtable hits to the total number of lookups as a `f64`.
    pub fn get_mem_table_hit_ratio(&self) -> f64 {
        let misses = self.memtable_miss.load(Ordering::Relaxed) as f64;
        let hits = self.memtable_hit.load(Ordering::Relaxed) as f64;

        hits / (hits + misses)
    }

    /// Retrieves the total number of insert operations in the MemTable.
    ///
    /// # Returns
    /// The total number of inserts as a `u64`.
    pub fn get_memtable_total_inserts(&self) -> u64 {
        self.memtable_total_inserts.load(Ordering::Relaxed)
    }

    /// Retrieves the total number of delete operations in the MemTable.
    ///
    /// # Returns
    /// The total number of deletes as a `u64`.
    pub fn get_memtable_total_deletes(&self) -> u64 {
        self.memtable_total_deletes.load(Ordering::Relaxed)
    }
}