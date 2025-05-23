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

pub struct QuokkaDB {
    options: Options,
}

impl QuokkaDB {
    pub fn open(path: &Path) -> Self {
        QuokkaDB {
            options: Options::default_options(),
        }
    }

    pub fn open_with_options(path: &Path, options: Options) -> Self {
        QuokkaDB { options }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn collection<'a>(&self, name: &'a str) -> Collection<'a> {
        Collection { name }
    }
}

pub struct Options {}

impl Options {
    pub fn default_options() -> Options {
        Options {}
    }
}

pub struct Collection<'a> {
    name: &'a str,
}

impl<'a> Collection<'a> {
    // fn insert_one(&self, document: impl Serialize) -> Result<ObjectId> {
    //
    // }
}

pub type Result<T> = std::result::Result<T, Error>;
