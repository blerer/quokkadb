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

The `Plant` type is your application model. `QuokkaDocument` makes it usable with a typed collection, while Serde reads and writes its stored form.

`QuokkaDB::open` creates or reopens the database at the path you provide. Reopen the same directory when the application starts again to access its stored data.

Each document has an `_id`. The `#[quokka(id)]` attribute identifies the model field used for it, and `#[serde(rename = "_id")]` stores this example's `id` field under that name. This example provides its own ID.

`create_if_missing` makes the `plants` collection available on its first use. `find_one` returns `Result<Option<Plant>>`: an error if the operation fails, `None` when no plant matches, or the matching plant.

## Use a database from another thread

`QuokkaDB` implements `Clone`. Clones share the same database instance, so a worker can use a clone without opening the database again. The instance stays active while at least one handle remains.

```rust
let worker_db = db.clone();
let worker = std::thread::spawn(move || {
    let plants = worker_db.typed_collection::<Plant>("plants");
    plants.insert_one(Plant {
        id: 2,
        name: "Spider plant".into(),
        needs_water: false,
    })
});

let inserted = worker
    .join()
    .expect("worker thread must not panic")?;
assert_eq!(inserted.inserted_id, 2);
```

## Next steps

Use the BSON document API when your application works with dynamic data instead of a fixed Rust type. Continue with the [Guides](guides.md) for common tasks, [Concepts](concepts.md) for the data model and guarantees, or [API Reference](api-reference.md) for the public Rust API.
