# Durable writes

Ordinary writes are already durable with `Options::default()`, `Options::lightweight()`, and `Options::optimized()`. Those configurations use `WalDurability::Durable`, so calling `sync()` is not needed for their writes.

Use `sync()` when you configured the database with `ProcessSafe` or `Buffered` durability and one operation must be durable before it returns. `Options::high_query_load()` uses `ProcessSafe`, so it is one profile where this override can be useful.

Related: [Options](options.md) · [Operations — Choose durability deliberately](../operations.md#choose-durability-deliberately) · [Concepts — Durability and recovery](../concepts.md#durability-and-recovery).

## Synchronize one write

Use the operation builder, call `sync`, then execute it.

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

`sync()` makes this write durable before `execute()` returns. It overrides the [database-level write-ahead-log durability setting](../operations.md#choose-durability-deliberately) for this operation only; it does not change the setting for later writes.

The same pattern is available for inserts, updates, replacements, deletes, and find-and-modify writes in both the typed and document APIs.

## Next

- [Options](options.md)
- [Operations](../operations.md)
- [Update data](update-data.md)
