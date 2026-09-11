# Options

Choose database options when you open QuokkaDB. Start with the default configuration unless you have a clear workload requirement, then select a profile or change the settings that address that requirement.

## Open with options

Use `open_with_options` to choose a profile and override a focused setting.

```rust
use quokkadb::options::options::{Options, WalDurability};
use quokkadb::options::storage_quantity::{StorageQuantity, StorageUnit};
use quokkadb::QuokkaDB;
use std::path::Path;

let options = Options::optimized()
    .with_block_cache_size(StorageQuantity::new(64, StorageUnit::Mebibytes))
    .with_wal_durability(WalDurability::Durable);

let db = QuokkaDB::open_with_options(Path::new("./data"), options)?;
```

`Options::default`, `Options::lightweight`, `Options::optimized`, and `Options::high_query_load` are starting configurations. They are not workload guarantees; use them as a baseline and change settings only when your application's needs are known.

The default, lightweight, and optimized configurations use `WalDurability::Durable`. The high-query-load configuration uses `WalDurability::ProcessSafe`.

Options are validated when the database opens. The resulting database exposes its selected options through `db.options()` but does not change them while it is running.

## Choose durability deliberately

`WalDurability` controls how acknowledged writes are propagated:

- `Durable` makes an acknowledged write durable before it returns.
- `ProcessSafe` preserves acknowledged writes across a process crash, but recent writes can still be lost after an operating-system crash or power loss before the next periodic sync.
- `Buffered` can leave acknowledged writes in QuokkaDB's userspace buffer, where a normal process crash can lose them.

Use [Durable writes](durable-writes.md) to synchronize one write without changing the database default.

## Tune only the settings you need

The option builders cover cache and resource limits, write-ahead-log behavior, compaction capacity, and storage-format settings. Cache sizes, `max_open_files`, and `compaction_threads` are common resource controls. The [API Reference](../api-reference.md) lists every builder and its validation rules.

Read [Operations](../operations.md) for database-directory management, metrics, and workload-oriented tuning guidance.
