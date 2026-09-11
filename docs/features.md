# Features

QuokkaDB provides document persistence, queries, updates, indexes, and durable storage inside a Rust application. This chapter will make the current support level easy to assess before adopting the database.

## Current support

| Area | Status | What is available today |
| --- | --- | --- |
| Data access | Supported | Typed Rust models and direct BSON document collections. |
| Collection management | Supported | Create, list, rename, and drop collections. |
| Reads and writes | Supported | Insert, find, update, replace, delete, projections, sorting, limits, skips, and find-and-modify operations. |
| Indexes | Supported | Ascending, descending, and compound indexes. |
| Persistence | Supported | On-disk storage, write-ahead logging, and recovery when a database is reopened. |
| Observability | Supported | Tracing events and in-process metrics. |
| Transactions | Unsupported | Multi-operation transactions are not currently exposed by the public API. |

## Planned topics

- Typed models and the BSON document API.
- Query, update, projection, and sort capabilities.
- Indexes and the workloads they help.
- Durability and crash recovery guarantees.
- Concurrency and atomicity expectations.
- Current limitations and compatibility expectations before the first stable release.
