# Concepts

QuokkaDB runs in your Rust application's process. This page explains how application models map to documents, how collections create IDs, and which consistency and durability guarantees apply to each write.

## Documents and typed models

A document is a BSON value with named fields. Documents can contain nested documents, arrays, and the BSON value types supported by the `bson` crate.

The document API works directly with dynamic BSON documents. It is useful when the shape of the data is only known at runtime.

```rust
use bson::doc;

let plants = db.collection("plants").create_if_missing();

plants.insert_one(doc! {
    "_id": 1,
    "name": "Monstera",
    "needs_water": true,
})?;
```

Most applications should use the typed API. A typed collection serializes a Rust model with Serde and gives its fields typed query, update, sort, and index expressions.

```rust
#[derive(serde::Serialize, serde::Deserialize, quokkadb::QuokkaDocument)]
struct Plant {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    needs_water: bool,
}

let plants = db
    .typed_collection::<Plant>("plants")
    .create_if_missing();

let plant = plants.find_one(|plant| plant.needs_water.eq(true))?;
```

The Rust type describes how the application reads and writes documents; it does not add a database-wide schema constraint. Code using the document API can write a document with a different shape to the same collection. A typed read of an incompatible document returns a deserialization error. Keep one document shape per typed collection, or use the document API when the shape is intentionally flexible.

## Collections

A collection is a named group of documents. A collection handle does not create the collection by itself. Operations through a missing collection return an error by default, which helps detect misspelled names and missing setup.

Call `create_if_missing` when a collection may be created by its first write. Reads through that handle return no documents until it exists.

```rust
let plants = db.collection("plants").create_if_missing();
```

Create a collection explicitly when its setup is part of application initialization, or when you need to choose an ID strategy. Collections can be listed, renamed, and dropped. See [Manage collections](guides/manage-collections.md) for those operations.

## IDs

Every stored document has an `_id` field. It identifies the document within its collection and must be unique.

For a document collection, each collection has one of three ID-creation strategies:

| Strategy | Behavior |
| --- | --- |
| `Mixed` | Accept a supplied `_id`, or generate one when it is absent. This is the default. |
| `Manual` | Require the application to supply `_id`. |
| `Generated` | Generate every `_id` and reject documents that supply one. |

Typed models always supply their own ID. A `QuokkaDocument` model has exactly one non-optional field marked `#[quokka(id)]`, usually serialized as `_id` with `#[serde(rename = "_id")]`.

When an application needs to create IDs before insertion, `QuokkaId::new()` produces a globally ordered Sonyflake ID. It serializes as a BSON `Int64`, so it can be used with the document API. It is not currently available as a typed model ID.

## Queries, updates, and indexes

Use filters to select documents, then optionally project fields, sort, skip, and limit the result. `find_one` returns at most one document; `find` returns an iterator that produces matching documents as it is consumed.

The typed API expresses paths through model fields:

```rust
let thirsty = plants
    .find(|plant| plant.needs_water.eq(true))
    .sort(|plant| plant.name.asc())
    .limit(20)
    .execute()?;
```

The document API expresses the same ideas with BSON filter, projection, and sort documents. See [Find queries](guides/find-queries.md) for examples of both APIs.

An index stores ordered values for one field or an ordered sequence of fields. Create one for filters and sorts that are frequent in your application. Indexes can reduce query work, but every insert, update, and delete must also maintain them. Compound index field order matters: an index on `needs_water` followed by `name` is designed for queries and sorts that begin with `needs_water`.

See [Indexes](guides/indexes.md) for index creation and management.

## Concurrent access and atomic writes

`QuokkaDB` implements `Clone`. Its clones share one opened database instance, so application threads can use a clone without reopening the database directory.

Each query runs against a consistent snapshot taken when it starts. A query iterator continues to see that snapshot while it is consumed, even if another thread writes newer data.

Each write operation is atomic. A reader sees the state before the write or the state after it, including operations that affect several matching documents such as `update_many` and `delete_many`. QuokkaDB checks for conflicting concurrent changes before committing these operations; if a conflict prevents the write, it returns an error without applying a partial result. Retry the operation when the application can safely do so.

Multi-operation transactions are not currently supported. If an application invariant spans separate database calls, keep that invariant in application logic or redesign it around one write operation.

## Durability and recovery

QuokkaDB records writes for recovery in the database directory. Reopen the same directory after a clean restart or crash to recover stored data.

The database-level `WalDurability` setting determines what an acknowledged write means:

| Setting | Guarantee when a write returns successfully |
| --- | --- |
| `Durable` | The write is durable before the call returns. This is the default. |
| `ProcessSafe` | The write survives a process crash, but a recent write may be lost after an OS crash or power loss before the next periodic sync. |
| `Buffered` | The write may still be in QuokkaDB's buffer and can be lost in a normal process crash. |

When the database uses `ProcessSafe` or `Buffered`, call `sync()` on an individual write builder when that operation must be durable before it returns.

```rust
plants
    .insert_one_with(Plant {
        id: 1,
        name: "Monstera".into(),
        needs_water: true,
    })?
    .sync()
    .execute()?;
```

`sync()` makes the write's recovery record durable. It does not wait for later storage maintenance work, and it does not change the default setting for other writes. See [Durable writes](guides/durable-writes.md) and [Options](guides/options.md) to choose the right behavior for your application.

The on-disk format may change before the first stable release. Treat a QuokkaDB directory as data owned by the application and plan upgrades around the project's release notes.
