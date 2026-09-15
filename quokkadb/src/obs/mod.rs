#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use tracing_subscriber::EnvFilter;

pub(crate) mod metrics;
pub(crate) mod observability;

#[cfg(test)]
#[allow(dead_code)]
/// Utility to initialize tracing for tests. When we need if for debugging reasons.
pub(crate) fn init_tracing() {
    static TEST_TRACING: OnceLock<()> = OnceLock::new();

    TEST_TRACING.get_or_init(|| {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("quokkadb=trace"));

        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_test_writer()
            .try_init();
    });
}
