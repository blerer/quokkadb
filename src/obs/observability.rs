use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info_span, Span};

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct Observability {
    instance_id: u64,
    instance_span: Span,
}

impl Observability {
    pub(crate) fn new(path: &Path) -> Arc<Self> {
        let instance_id = NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed);
        let instance_span = info_span!(
            target: "quokkadb::instance",
            "quokkadb.instance",
            id = instance_id,
            path = %path.display(),
        );

        Arc::new(Self {
            instance_id,
            instance_span,
        })
    }

    pub(crate) fn instance_id(&self) -> u64 {
        self.instance_id
    }

    pub(crate) fn instance_span(&self) -> &Span {
        &self.instance_span
    }
}

#[cfg(test)]
mod tests {
    use super::Observability;
    use std::path::Path;

    #[test]
    fn assigns_unique_instance_ids() {
        let first = Observability::new(Path::new("/tmp/quokkadb-first"));
        let second = Observability::new(Path::new("/tmp/quokkadb-second"));

        assert!(first.instance_id() < second.instance_id());
    }
}
