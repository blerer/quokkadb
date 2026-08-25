use crate::error::Result;
use crate::QuokkaDB;

/// Internal test-only controls for driving storage state in integration tests.
#[doc(hidden)]
#[derive(Clone)]
pub struct TestControl {
    db: QuokkaDB,
}

impl TestControl {
    pub fn flush(&self) -> Result<()> {
        self.db.db_impl.storage_engine.flush()?;
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        self.db.db_impl.storage_engine.compact()?;
        Ok(())
    }

    pub fn disable_auto_compaction(&self) {
        self.db.db_impl.storage_engine.disable_auto_compaction();
    }
}

impl QuokkaDB {
    #[doc(hidden)]
    pub fn test_control(&self) -> TestControl {
        TestControl { db: self.clone() }
    }
}
