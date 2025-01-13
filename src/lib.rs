pub mod error;
mod options;
mod util;
mod storage;
mod statistics;

use std::path::Path;
use serde::Serialize;
use bson::oid::ObjectId;
use crate::error::Error;

pub struct QuokkaDB {
    options: Options,
}

impl QuokkaDB {

    pub fn open (path: &Path) -> Self {
        QuokkaDB {
            options: Options::default_options(),
        }
    }

    pub fn open_with_options (path: &Path, options: Options) -> Self {
        QuokkaDB {
            options,
        }
    }
    
    pub fn options(&self) -> &Options {
        &self.options   
    }

    pub fn collection<'a>(&self, name: &'a str) -> Collection<'a> {
        Collection {
            name,
        }
    }
}

pub struct Options {

}

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

