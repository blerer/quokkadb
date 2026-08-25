use crossbeam_skiplist::SkipMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Tracks active snapshot leases and exposes the oldest live snapshot sequence.
pub(crate) struct SnapshotManager {
    snapshots: SkipMap<(u64, u64), ()>,
    next_id: AtomicU64,
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            snapshots: SkipMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    pub fn oldest_active_snapshot(&self) -> Option<u64> {
        self.snapshots.front().map(|entry| entry.key().0)
    }

    pub fn acquire(self: &Arc<Self>, sequence: u64) -> Snapshot {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.snapshots.insert((sequence, id), ());

        Snapshot {
            inner: Arc::new(SnapshotLease {
                sequence,
                id,
                manager: self.clone(),
            }),
        }
    }
}

struct SnapshotLease {
    sequence: u64,
    id: u64,
    manager: Arc<SnapshotManager>,
}

impl Drop for SnapshotLease {
    fn drop(&mut self) {
        self.manager.snapshots.remove(&(self.sequence, self.id));
    }
}

/// A cloneable snapshot lease. The lease remains active until the last clone is dropped.
#[derive(Clone)]
pub(crate) struct Snapshot {
    inner: Arc<SnapshotLease>,
}

impl Snapshot {
    pub fn sequence(&self) -> u64 {
        self.inner.sequence
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("sequence", &self.sequence())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oldest_active_snapshot_is_none_when_empty() {
        let manager = SnapshotManager::new();
        assert_eq!(manager.oldest_active_snapshot(), None);
    }

    #[test]
    fn acquire_and_release_tracks_oldest_snapshot() {
        let manager = Arc::new(SnapshotManager::new());

        let snapshot_3 = manager.acquire(3);
        assert_eq!(manager.oldest_active_snapshot(), Some(3));

        let snapshot_7 = manager.acquire(7);
        assert_eq!(snapshot_7.sequence(), 7);
        assert_eq!(manager.oldest_active_snapshot(), Some(3));

        let snapshot_9 = manager.acquire(9);
        assert_eq!(manager.oldest_active_snapshot(), Some(3));

        drop(snapshot_3);
        assert_eq!(manager.oldest_active_snapshot(), Some(7));

        drop(snapshot_7);
        assert_eq!(manager.oldest_active_snapshot(), Some(9));

        drop(snapshot_9);
        assert_eq!(manager.oldest_active_snapshot(), None);
    }

    #[test]
    fn cloned_snapshot_releases_only_after_last_drop() {
        let manager = Arc::new(SnapshotManager::new());

        let snapshot = manager.acquire(11);
        let snapshot_clone = snapshot.clone();
        assert_eq!(manager.oldest_active_snapshot(), Some(11));

        drop(snapshot);
        assert_eq!(manager.oldest_active_snapshot(), Some(11));

        drop(snapshot_clone);
        assert_eq!(manager.oldest_active_snapshot(), None);
    }

    #[test]
    fn multiple_snapshots_at_same_sequence_share_refcount() {
        let manager = Arc::new(SnapshotManager::new());

        let snapshot_a = manager.acquire(5);
        let snapshot_b = manager.acquire(5);
        assert_eq!(manager.oldest_active_snapshot(), Some(5));

        drop(snapshot_a);
        assert_eq!(manager.oldest_active_snapshot(), Some(5));

        drop(snapshot_b);
        assert_eq!(manager.oldest_active_snapshot(), None);
    }
}
