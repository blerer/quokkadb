extern crate core;

pub mod collection;
pub mod error;
mod io;
mod obs;
pub mod options;
mod query;
mod storage;
mod util;

use crate::error::Error;
use std::path::Path;
use std::sync::Arc;
use crate::collection::Collection;
use crate::obs::logger::NoOpLogger;
use crate::obs::metrics::MetricRegistry;
use crate::options::options::Options;
use crate::query::optimizer::Optimizer;
use crate::storage::storage_engine::StorageEngine;

pub struct QuokkaDB {
    options: Arc<Options>,
    db_impl: Arc<DbImpl>,

}

impl QuokkaDB {
    pub fn open(path: &Path) -> error::Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    pub fn open_with_options(path: &Path, options: Options) -> error::Result<Self> {

        let options = Arc::new(options);
        let logger = Arc::new(NoOpLogger::default());
        let mut metric_registry = MetricRegistry::new();
        let storage_engine = StorageEngine::new(logger, &mut metric_registry, options.clone(), path)?;
        let optimizer = Arc::new(Optimizer::new(vec![])); // Add normalization rules as needed
        let db_impl = Arc::new(DbImpl {
            optimizer,
            storage_engine,
        });

        Ok(QuokkaDB {
            options,
            db_impl,
        })
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(self.db_impl.clone(), name.to_string())
    }
}

struct DbImpl {
    optimizer: Arc<Optimizer>,
    storage_engine: Arc<StorageEngine>,
}

impl DbImpl {
    pub fn create_collection_if_not_exists(self: &Arc<Self>, name: &str) -> error::Result<u32> {
        Ok(self.storage_engine.create_collection_if_not_exists(name)?)
    }
}
