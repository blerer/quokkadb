use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) trait SizeEstimate {
    /// Estimates additional heap memory reachable from this value.
    ///
    /// Shared allocations are treated as exclusively owned and may therefore
    /// be counted multiple times. This is acceptable for our use case, as we only need a rough estimate of the total memory usage.
    fn estimated_heap_size(&self) -> usize;
}

// Due to Arc internals and sharing, we chose to use an approximation, that does not prevent
// double-counting inline fields but is enough for our needs.
impl<T: SizeEstimate> SizeEstimate for Arc<T> {
    fn estimated_heap_size(&self) -> usize {
        // Approximation: Arc allocation contains two counters + T.
        2 * size_of::<usize>() + size_of::<T>() + self.as_ref().estimated_heap_size()
    }
}

impl<T: SizeEstimate> SizeEstimate for Option<T> {
    fn estimated_heap_size(&self) -> usize {
        self.as_ref().map_or(0, SizeEstimate::estimated_heap_size)
    }
}

impl<T: SizeEstimate> SizeEstimate for Vec<T> {
    fn estimated_heap_size(&self) -> usize {
        self.capacity() * size_of::<T>()
            + self
                .iter()
                .map(SizeEstimate::estimated_heap_size)
                .sum::<usize>()
    }
}

impl SizeEstimate for String {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl SizeEstimate for u8 {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl SizeEstimate for i32 {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl SizeEstimate for usize {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl<K: SizeEstimate, V: SizeEstimate> SizeEstimate for BTreeMap<K, V> {
    fn estimated_heap_size(&self) -> usize {
        self.iter()
            .map(|(key, value)| {
                size_of::<K>()
                    + key.estimated_heap_size()
                    + size_of::<V>()
                    + value.estimated_heap_size()
            })
            .sum()
        // We underestimate, because we do not count node metadata, pointers, unused slots,
        // allocator overhead, etc.
    }
}
