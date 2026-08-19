use quokkadb::QuokkaDB;
use std::path::Path;
use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;

pub fn init_tracing() {
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

pub fn open_db(path: &Path) -> QuokkaDB {
    init_tracing();
    QuokkaDB::open(path).unwrap()
}
