# Features

QuokkaDB is an embedded application database for Rust. It stores application data in a local directory and provides document queries, updates, indexes, and durable writes without a separate database server.

## Current support

| Area | Status | What is available today |
| --- | --- | --- |
| Data access | Supported | Typed Rust models and direct BSON document collections. |
| Collections | Supported | Create, list, rename, and drop collections; choose generated, manual, or mixed `_id` creation. |
| Queries | Supported | Filters, projections, sorting, limits, skips, and typed field access for nested values, arrays, and maps. |
| Writes | Supported | Insert, update, replace, delete, find-and-modify, upserts, and array updates. |
| Indexes | Supported | Ascending, descending, and compound indexes. |
| Persistence | Supported | On-disk storage, configurable write-ahead-log durability, and recovery when reopening a database. |
| Concurrent access | Supported | Clone a database handle and use it from application threads. |
| Observability | Supported | Tracing instrumentation and an in-process metrics API. |
| Transactions | Unsupported | Multi-operation transactions are not currently exposed by the public API. |

## Store application data

Use a typed collection when your application already has a Rust type. Derive `QuokkaDocument` and query its fields with Rust expressions.

```rust
let thirsty: Vec<Plant> = plants
    .find(|plant| plant.needs_water.eq(true))
    .sort(|plant| plant.name.asc())
    .execute()?
    .collect::<quokkadb::error::Result<_>>()?;
```

Use the document API when the data is dynamic or you need direct BSON access. It supports the same collection operations with BSON query documents.

```rust
use bson::doc;

let thirsty = plants
    .find(doc! { "needs_water": true })
    .sort(doc! { "name": 1 })
    .execute()?
    .collect::<quokkadb::error::Result<Vec<_>>>()?;
```

See [Getting Started](getting-started.md) for a complete typed example and [Guides](guides.md) for task-focused documentation.

## Query and shape results

Queries can match scalar, nested, optional, array, and map values. Combine filters with logical operators, choose fields to include or exclude, sort results, and page through them with `skip` and `limit`.

The typed API generates field access from your model. The document API accepts BSON filter, projection, and sort documents. Use the API that matches the data your application has.

## Change data

Insert one or many documents. Update or replace one or many matches, delete documents, or combine a read with a change through find-and-modify operations.

Updates support scalar changes, nested paths, array changes, positional and filtered array updates, and upserts. Typed updates use model fields; document updates use BSON update documents.

## Use indexes

Create an index when your application frequently filters or sorts on a field. QuokkaDB supports ascending, descending, and compound indexes. You can name indexes, inspect the active indexes on a collection, and remove indexes that are no longer needed.

## Persist and operate the database

Open QuokkaDB on a directory owned by your application. Reopening that directory recovers its stored data. The default configuration uses durable writes; `Options` can select another write-ahead-log durability mode, and write builders can force an individual write to synchronize before it returns.

`QuokkaDB` implements `Clone`, so application threads can share an already opened database instance. Use `metrics()` for in-process cache, storage, compaction, and query metrics. QuokkaDB also emits tracing instrumentation for database activity.

Read [Concepts](concepts.md) for the data and durability model, [Operations](operations.md) for configuration and observability, and [API Reference](api-reference.md) for the available Rust types and methods.

## Current limits

Multi-operation transactions are not currently supported. The on-disk format may change before the first stable release.
