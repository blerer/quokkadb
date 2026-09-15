<p style="text-align: left;">
  <img src="assets/logo.png" alt="QuokkaDB Logo" width="300"/>
</p>

# QuokkaDB

**The embedded application database.**

QuokkaDB is an embedded application database for **Rust**, with Node.js/TypeScript support planned. It lets you persist your application models directly, without running or managing a database server.

## Persist your models, not your mappings

Applications are built from nested objects, collections, and evolving state. **QuokkaDB stores that structure directly**, without decomposing your models into tables, joins, and mapping layers.

## Persistence without infrastructure

- **No server** — QuokkaDB runs inside your application.
- **Single directory** — all database files live in one place.
- **No setup** — open a path and start storing data.
- **Flexible schema** — evolve your documents with your application models.
- **No ORM** — persist nested application models directly.
- **Small and bounded** — designed for predictable application-level resource usage.
- **Real database semantics** — durability, indexes, concurrent access, atomic updates, and crash recovery.

QuokkaDB is designed to make persistence a boring part of building an application.

## Quick start

Add QuokkaDB and the dependencies used by the examples to your `Cargo.toml`:

```toml
[dependencies]
quokkadb = { git = "https://github.com/blerer/quokkadb" }
bson = "3"
serde = { version = "1", features = ["derive"] }
```

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
        .expect("inserted plant must exist");

    assert_eq!(plant.name, "Monstera");
    Ok(())
}
```

`QuokkaDB` implements `Clone`. Clones share the same database instance, so you can pass a clone to another thread when your application needs it.

For dynamic data or lower-level access, QuokkaDB also provides a BSON document API.

```rust
use bson::doc;
use quokkadb::error::Result;
use quokkadb::QuokkaDB;
use std::path::Path;

fn main() -> Result<()> {
    let db = QuokkaDB::open(Path::new("./data"))?;
    let plants = db.collection("plants").create_if_missing();

    plants.insert_one(doc! {
        "_id": 1,
        "name": "Monstera",
        "needs_water": true,
    })?;

    let plant = plants
        .find_one(doc! { "needs_water": true })?
        .expect("inserted plant must exist");

    assert_eq!(
        plant.get_str("name").expect("name must be a string"),
        "Monstera"
    );
    Ok(())
}
```

## Documentation

[Read the QuokkaDB documentation.](docs/README.md)

The documentation covers the document and typed APIs, queries, indexes, configuration, database internals, and current limitations.

## Contributing

QuokkaDB is still evolving, and real-world feedback is especially valuable.

If you try it, I’d love to hear what works, what feels awkward, and which workloads matter to you. Bug reports, small fixes, documentation improvements, and larger contributions are all welcome.

Read the [Project page](docs/project.md) for stability expectations and contribution guidance.

## License

Apache License 2.0.
