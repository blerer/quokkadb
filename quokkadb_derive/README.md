# quokkadb_derive

This crate provides the derive macros used by QuokkaDB's typed API. It is
published as proc-macro support for [`quokkadb`](https://crates.io/crates/quokkadb);
application code should depend on `quokkadb`, which re-exports these macros.

```toml
[dependencies]
quokkadb = "0.2.0"
serde = { version = "1", features = ["derive"] }
```

```rust
use quokkadb::{QuokkaDocument, QuokkaId};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: QuokkaId,
    name: String,
}
```

The derives expose typed filters, updates, projections, sorting, and indexes.
`QuokkaDocument` requires named, non-generic fields and exactly one concrete
field serialized as `_id`. `QuokkaType` is for nested structs and supported
externally tagged enum payloads, while `QuokkaScalar` marks a custom Serde
value as a typed scalar without implementing Serde itself.

See the [typed API reference](https://github.com/blerer/quokkadb/blob/main/docs/api-reference.md)
for supported Serde attributes and enum constraints.
