# Operations

QuokkaDB runs inside the application that opens it. Operating it means choosing a durable directory, selecting configuration when the application starts, and exposing the signals that help you understand its workload.

This page covers the operational boundary. Read [Concepts](concepts.md) for data and durability guarantees, and [Options](guides/options.md) for the complete configuration API.

## Own the database directory

Open QuokkaDB on a persistent directory owned by the application. Use a path that survives application restarts and deployments, such as an application data directory or a mounted persistent volume.

```rust
use quokkadb::QuokkaDB;
use std::path::Path;

let db = QuokkaDB::open(Path::new("/var/lib/my-app/quokkadb"))?;
```

Keep the directory separate from temporary files, build output, and application caches. QuokkaDB manages the files inside it as one database; do not edit, remove, rename, or move individual files while the application is running.

Give the application identity read and write access to the directory, and restrict access from unrelated users and processes. The documented sharing model is cloned `QuokkaDB` handles in one application process. Treat one process as the directory owner.

The `quokka` CLI is a separate process and therefore also needs exclusive ownership. Stop the application before importing from or exporting to its database directory, wait for it to exit, run the CLI command, then restart the application. See [Import and export data](guides/import-export.md) for the CLI workflow.

## Start, share, and stop the database

Open the same directory whenever the application starts. QuokkaDB recovers stored data during opening, then its collection handles are ready to use.

`QuokkaDB` implements `Clone`. Clones share the already opened database, so pass a clone to worker threads instead of opening the directory again.

```rust
let worker_db = db.clone();
let worker = std::thread::spawn(move || {
    let plants = worker_db.typed_collection::<Plant>("plants");
    plants.find_one(|plant| plant.needs_water.eq(true))
});
```

There is no public `shutdown` method. Keep database and collection handles alive until application work has finished. When the shared database state is dropped, QuokkaDB flushes pending storage work and stops its background work. A clean application shutdown therefore gives the database time to finish dropping its handles.

## Choose configuration for the workload

Select `Options` before opening the database. The configuration cannot change while the database is running, so restart the application to apply a new value.

```rust
use quokkadb::options::options::{Options, WalDurability};
use quokkadb::options::storage_quantity::{StorageQuantity, StorageUnit};
use quokkadb::QuokkaDB;
use std::path::Path;

let options = Options::optimized()
    .with_block_cache_size(StorageQuantity::new(64, StorageUnit::Mebibytes))
    .with_wal_durability(WalDurability::Durable);

let db = QuokkaDB::open_with_options(Path::new("/var/lib/my-app/quokkadb"), options)?;
```

Start with the default unless a measured workload points to a specific constraint.

| Situation | Start with | Settings to consider |
| --- | --- | --- |
| Small or unknown workload | `Options::default()` or `Options::lightweight()` | Keep the defaults until metrics show a constraint. |
| More memory and I/O capacity for general work | `Options::optimized()` | `block_cache_size`, `query_cache_size`, `max_open_files`, and `compaction_threads`. |
| Read-heavy workload where a periodic durability window is acceptable | `Options::high_query_load()` | Its WAL mode is `ProcessSafe`; use `sync()` for writes that must be durable before they return. |
| Bounded memory or file-descriptor budget | Any profile | `block_cache_size`, `query_cache_size`, and `max_open_files`. |
| Sustained write volume or compaction backlog | Any profile | `compaction_threads`, `file_write_buffer_size`, and the compaction size settings. |

The default, lightweight, and optimized profiles use `WalDurability::Durable`. The high-query-load profile uses `WalDurability::ProcessSafe`. Profiles are starting configurations, not performance guarantees.

Options are validated by `open_with_options`. Use `db.options()` to inspect the values selected at startup. The [API Reference](api-reference.md#configuration) lists every option builder and validation rule.

### Choose durability deliberately

`WalDurability` controls what a successful write acknowledgement means:

| Setting | Meaning |
| --- | --- |
| `Durable` | The write is durable before it returns. |
| `ProcessSafe` | The write survives a process crash, but a recent write can be lost after an operating-system crash or power loss before the next periodic sync. |
| `Buffered` | The write can still be in QuokkaDB's userspace buffer and can be lost in a normal process crash. |

When the database uses `ProcessSafe` or `Buffered`, call `sync()` on a write builder when that one operation must be durable before it returns.

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

`sync()` does not change the configured durability for later operations, and it does not wait for later storage maintenance. See [Durable writes](guides/durable-writes.md) for the full pattern.

## Read metrics in the application

`db.metrics()` exposes in-process counters, gauges, and histograms. Read them from the component where the application already emits health or telemetry data.

```rust
let metrics = db.metrics();

let block_cache_hit_ratio = metrics.block_cache().hit_ratio();
let reads = metrics.executor().read_queries();
let writes = metrics.executor().write_queries();
let wal_bytes_buffered = metrics.wal().bytes_buffered();
```

| Operational question | Metrics to inspect | What the values indicate |
| --- | --- | --- |
| Is cache capacity helping reads? | `block_cache().hit_ratio()`, `hits()`, `misses()`, `evictions()` | A low hit ratio or frequent evictions can justify investigating cache capacity and the working set. |
| Are queries using available indexes? | `executor().collection_scans()`, `index_scans()`, `point_searches()` | Compare scan counts with the filters and sorts the application performs. Add or revise an index only when the query pattern warrants it. |
| Is sorting becoming expensive? | `in_memory_sorts()`, `external_merge_sorts()`, `top_k_sorts()` | Correlate sort activity with query shape, limits, and index coverage. |
| Is durable-write work accumulating? | `wal().bytes_buffered()`, `wal().syncs()`, `wal().total_bytes()` | Buffered WAL bytes and sync activity show the selected durability behavior and write pressure. |
| Is storage growing or flushing frequently? | `storage().total_sstable_size()`, `memtable_size()`, `memtable_total_size()`, `flush().count()` | Use these values to understand on-disk growth and memory waiting to be flushed. |
| Is compaction keeping up? | `compaction().active_at_level(level)`, `score_at_level(level)`, `jobs_picked()`, `jobs_skipped_level_compacting()` | A sustained high score or active work can identify a compaction capacity constraint. |

Histogram accessors such as `flush().duration()` and `executor().read_query_duration()` provide `count`, `mean`, `quantile`, and `buckets`. Use them to compare latency before and after a configuration change. The [API Reference](api-reference.md#results-errors-ids-and-metrics) lists every metric group.

## Integrate tracing

QuokkaDB emits events and spans through the `tracing` crate. Configure a tracing subscriber in the application before opening the database, then choose a filter appropriate for the environment.

Use `debug` to see database opening, background work, recovery, and lifecycle events. Use `trace` only while investigating a specific issue: it includes low-level query, cache, write, flush, and compaction activity and can produce a large volume of telemetry.

QuokkaDB events are emitted under the `quokkadb` target. A typical filter enables `quokkadb=debug` in a staging or diagnostic environment, then sends structured events to the application's existing logging or telemetry pipeline.

## Restart and recovery

Reopen the same database directory after a clean restart or a crash. QuokkaDB uses its persisted recovery records to restore acknowledged writes according to the selected `WalDurability` setting.

If opening the database returns an error, do not attempt to repair individual files while the application is running. Preserve the directory and error details, then investigate the underlying filesystem, permissions, available storage, and reported recovery error. The public `Error` type distinguishes I/O, log-corruption, invalid-option, and other failure categories.

See [Concepts](concepts.md#durability-and-recovery) for the recovery and durability guarantees.

## Backup, restore, and upgrades

QuokkaDB does not currently document a supported backup or restore procedure. Do not treat copying individual live files as a supported backup method.

The on-disk format may change before the first stable release. QuokkaDB also does not currently document a supported upgrade or downgrade procedure. Plan application upgrades around the project's release notes and retain the source data required by your application until a supported migration path is available.
