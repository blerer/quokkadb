# QuokkaDB

QuokkaDB is an embedded application database for Rust applications. It stores
your application models in a directory owned by the application, without a
separate database server or mapping layer.

Use QuokkaDB when your application needs local, durable persistence for typed
Rust models, nested documents, indexes, and document queries. Start with the
typed API when your data has a Rust type; use the document API for dynamic BSON
documents.

## Quick start

Add QuokkaDB and Serde to your application:

```toml
[dependencies]
quokkadb = "0.2.0"
serde = { version = "1", features = ["derive"] }
```

Define a model, open a directory, and use a typed collection:

```rust
use quokkadb::{QuokkaDB, QuokkaDocument, QuokkaId};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: QuokkaId,
    name: String,
}

fn main() -> quokkadb::Result<()> {
let db = QuokkaDB::open("./data")?;
let users = db.typed_collection::<User>("users").create_if_missing();
users.insert_one(User {
    id: QuokkaId::new(),
    name: "Ada".into(),
})?;
let user = users.find_one(|user| user.name.eq("Ada"))?;
assert!(user.is_some());
Ok(())
}
```

The model must have one concrete field whose Serde name is `_id`. QuokkaDB
creates the database directory when it is opened, and cloned database handles
share the same embedded database instance.

## Learn more

- [Getting started](https://github.com/blerer/quokkadb/blob/main/docs/getting-started.md)
- [Features and current limitations](https://github.com/blerer/quokkadb/blob/main/docs/features.md)
- [API reference](https://github.com/blerer/quokkadb/blob/main/docs/api-reference.md)
- [Full documentation](https://github.com/blerer/quokkadb/tree/main/docs)

The generated API documentation is available at <https://docs.rs/quokkadb>.
