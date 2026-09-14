# Features

QuokkaDB is an embedded application database for Rust. It stores application data in a local directory without a separate database server.

This page shows what QuokkaDB supports today and where current limitations apply.

**Supported** means the capability is directly supported by that API. **Partial** means it has an important semantic or API limitation, stated in the note. **Not supported** means it is unavailable through that API.

## Querying

| Capability | Typed API | Document API | Notes |
| --- | --- | --- | --- |
| Equality and ranges | Supported | Supported | Equality, inequality, and `$gt`, `$gte`, `$lt`, and `$lte` are available. Comparisons use BSON value ordering. |
| Set membership | Not supported | Supported | The document API supports `$in` and `$nin`. |
| Logical operators | Partial | Supported | Typed filters compose with `and` and `or`. The document API also supports `$nor` and `$not`. |
| Field existence and BSON type | Partial | Supported | Typed optional fields support existence checks. The document API also supports `$exists` and `$type`. |
| Nested fields and maps | Supported | Supported | Typed fields follow embedded Rust types and string-keyed maps. Document queries use dotted paths. |
| Arrays | Partial | Supported | Typed fields support equality, length, `$all`, fixed indexes, and matching embedded elements. The document API also supports `$size` and `$elemMatch`. |
| Sorting and pagination | Supported | Supported | Ascending and descending sorts, compound sorts, `skip`, and `limit` are available. |
| `$regex` | Not supported | Not supported | BSON regular-expression values can be stored, but regular-expression matching is not available. |
| `$text`, geospatial, `$expr`, and `$where` queries | Not supported | Not supported | Full-text, geospatial, expression, and JavaScript query operators are not available. |

See [Find queries](guides/find-queries.md) for typed and BSON examples.

## Projections

| Capability | Typed API | Document API | Notes |
| --- | --- | --- | --- |
| Include and exclude fields | Supported | Supported | Typed projections use model fields and deserialize into the requested result type. The document API uses BSON projection documents. |
| Select individual fields | Supported | Partial | Typed `select` returns the chosen typed value or tuple. Document projections return a BSON document. |
| Array `$slice` and projection `$elemMatch` | Not supported | Supported | These projection operators are available only in BSON projection documents. |
| Positional `$` projection | Not supported | Not supported | Positional projection paths are rejected. |

## Write operations

| Capability | Typed API | Document API | Notes |
| --- | --- | --- | --- |
| Insert one or many documents | Supported | Supported | Collections support one-at-a-time and batch inserts. |
| Replace and delete matching documents | Supported | Supported | `replace_one`, `delete_one`, and `delete_many` are available. |
| Upserts | Supported | Supported | Update, replacement, and find-and-modify builders can insert when no document matches. |
| Find and modify | Supported | Supported | Find-one-and-update, replace, and delete operations can return the affected document. |

## Update operators

| Capability | Typed API | Document API | Notes |
| --- | --- | --- | --- |
| Field assignment and conditional replacement | Supported | Supported | `$set`, `$setOnInsert`, `$unset`, `$min`, and `$max` are available. |
| Numeric, temporal, and bitwise changes | Supported | Supported | `$inc`, `$mul`, `$currentDate`, and `$bit` are available when the typed field type permits the operation. |
| `$rename` | Not supported | Partial | The document API supports renaming document fields, but not paths through arrays or positional paths. |
| Array add, append, remove, and deduplicate | Supported | Supported | `$addToSet`, `$push`, `$pop`, `$pull`, and `$pullAll` are available. `$push` supports `$each`, `$position`, `$slice`, and `$sort`. |
| All and filtered array-element updates | Partial | Partial | Typed updates can target fixed array indexes. The document API also supports `$[]` and `$[identifier]` with `array_filters`; the first-match positional `$` operator is unavailable. |
| Update pipelines | Not supported | Not supported | Updates use modifier documents or typed update builders; aggregation-style update pipelines are unavailable. |

See [Update data](guides/update-data.md) for common update patterns and the [API Reference](api-reference.md) for builder options.

## Indexes

| Capability | Typed API | Document API | Notes |
| --- | --- | --- | --- |
| Ordered single-field indexes | Supported | Supported | Ascending and descending regular indexes are available. |
| Ordered compound indexes | Supported | Supported | Field order matters for filters and sorts; an index can satisfy a compatible leading prefix. |
| Index-backed filters and sorts | Partial | Partial | The optimizer can use compatible equality/range filters and sort order, but not every query shape is indexable. |
| Multikey or array-path indexes | Not supported | Not supported | An indexed path cannot contain an array element. QuokkaDB does not create multikey indexes. |
| Unique, sparse, partial, text, geospatial, wildcard, and TTL indexes | Not supported | Not supported | Regular ordered indexes are the only public index type. |

See [Indexes](guides/indexes.md) for creation and lifecycle operations.

## Guarantees and operations

| Capability | Typed API | Document API | Notes |
| --- | --- | --- | --- |
| Consistent query snapshots | Supported | Supported | A query sees the state at its start while its iterator is consumed. |
| Atomic write operations | Supported | Supported | Each operation, including `update_many` and `delete_many`, commits entirely or returns an error without a partial result. Conflicting concurrent writes can return an error and should be retried when safe. |
| Concurrent access in one application | Supported | Supported | Clone an already-opened `QuokkaDB` handle and share it across application threads. |
| One-process directory ownership | Supported | Supported | One application process owns a database directory. Do not open or modify that directory from another process. |
| Durable writes and recovery | Partial | Partial | `Durable` is the default. `ProcessSafe` and `Buffered` trade crash durability for throughput; `sync()` makes an individual write durable before it returns. Reopening the directory recovers acknowledged writes according to that setting. |
| In-process metrics and tracing | Supported | Supported | `metrics()` exposes in-process measurements and QuokkaDB emits `tracing` instrumentation. |

Read [Concepts](concepts.md) for consistency and durability semantics, and [Operations](operations.md) for configuration, recovery, and observability.

## Not supported

| Capability | Notes |
| --- | --- |
| Multi-operation transactions | There is no public transaction API. Keep invariants within one write operation or enforce them in application logic. |
| Aggregation pipelines | There is no `aggregate` API or aggregation pipeline execution. |
| Backup and restore | No supported backup or restore procedure exists. Copying live individual database files is not a supported backup method. |
| Upgrade and downgrade procedures | The on-disk format may change before 1.0; no supported migration, upgrade, or downgrade procedure exists. |
| Multi-process access | A database directory is owned by one process; cross-process coordination is unavailable. |
| Remote/server access | QuokkaDB runs in the application process and does not provide a database server or network protocol. |
| Change streams | There is no change-stream or watch API. |

## Store application data

Use a typed collection when your application already has a Rust type. Derive `QuokkaDocument` and query its fields with Rust expressions.

```rust
let thirsty: Vec<Plant> = plants
    .find(|plant| plant.needs_water.eq(true))
    .sort(|plant| plant.name.asc())
    .execute()?
    .collect::<quokkadb::error::Result<_>>()?;
```

Use the document API when the data is dynamic or you need direct BSON access.

```rust
use bson::doc;

let thirsty = plants
    .find(doc! { "needs_water": true })
    .sort(doc! { "name": 1 })
    .execute()?
    .collect::<quokkadb::error::Result<Vec<_>>>()?;
```

See [Getting Started](getting-started.md) for a complete typed example and [Guides](guides.md) for task-focused documentation.
