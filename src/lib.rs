extern crate core;

pub mod collection;
pub mod error;
pub mod io;
pub mod obs;
mod options;
mod query;
pub mod storage;
pub mod util;

use crate::error::Error;
use std::path::Path;
use crate::collection::Collection;
use crate::options::options::Options;

pub struct QuokkaDB {
    options: Options,

}

impl QuokkaDB {
    pub fn open(path: &Path) -> Self {
        QuokkaDB {
            options: Options::default(),
        }
    }

    pub fn open_with_options(path: &Path, options: Options) -> Self {
        QuokkaDB { options }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn collection(&self, name: &str) -> Collection {
        Collection::new(name.to_string())
    }
}
