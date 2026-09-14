# Getting Started

Use QuokkaDB to persist your application's data in the same process as your Rust application. In a few steps, you will store a plant and read it back.

The common path uses a typed collection. Define a Rust type for your data, derive `QuokkaDocument`, open a directory, and create the collection when it is first written.

## Install

Add QuokkaDB and Serde to your `Cargo.toml`:

```toml
[dependencies]
quokkadb = { git = "https://github.com/blerer/quokkadb" }
serde = { version = "1", features = ["derive"] }
```

## Store and query a plant

This complete program opens a database in `./data`, stores a `Plant`, and finds plants that need water.

```rust
use quokkadb::error::Result;
use quokkadb::{QuokkaDB, QuokkaDocument};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, QuokkaDocument)]
struct Plant {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    needs_water: bool,
}

fn main() -> Result<()> {
    let db = QuokkaDB::open(Path::new("./data"))?;
    let plants = db
        .typed_collection::<Plant>("plants")
        .create_if_missing();

    plants.insert_one(Plant {
        id: 1,
        name: "Monstera".into(),
        needs_water: true,
    })?;

    let plant = plants
        .find_one(|plant| plant.needs_water.eq(true))?
        .expect("the inserted plant exists");

    assert_eq!(plant.name, "Monstera");
    Ok(())
}
```

## Next steps

Continue with [Manage collections](guides/manage-collections.md) when you need to choose how document IDs are created. Read the [Guides](guides.md) for common tasks, [Concepts](concepts.md) for the data model and guarantees, and [Operations](operations.md) for sharing, running, and observing a database.
