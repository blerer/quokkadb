# Getting Started

Use QuokkaDB to keep application data in the same process as your Rust application. This chapter will take a new user from adding the crate to reading back a stored model.

The common path uses a typed collection. Define a serializable Rust type, derive `QuokkaDocument`, open a directory, and create the collection when it is first written.

```rust
use quokkadb::error::Result;
use quokkadb::{QuokkaDB, QuokkaDocument};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, QuokkaDocument)]
struct Task {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    title: String,
    done: bool,
}

fn main() -> Result<()> {
    let db = QuokkaDB::open(Path::new("./data"))?;
    let tasks = db.typed_collection::<Task>("tasks").create_if_missing();

    tasks.insert_one(Task {
        id: 1,
        title: "Ship the release".into(),
        done: false,
    })?;

    let task = tasks
        .find_one(|task| task.done.eq(false))?
        .expect("the inserted task exists");

    assert_eq!(task.title, "Ship the release");
    Ok(())
}
```

## Planned topics

- Installation and the dependencies required by typed models.
- Opening a database and choosing its data directory.
- Defining typed models and their `_id` fields.
- Creating collections, inserting documents, and reading them back.
- Choosing between the typed API and the BSON document API.
- Where to go next for queries, updates, and indexes.
