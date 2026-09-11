# Update data

Use an update when you want to change selected fields without replacing the rest of a model. This guide assumes the `Plant` model and `plants` typed collection from [Getting Started](../getting-started.md).

## Update matching models

Pass one closure to choose models and another to describe the change.

```rust
let result = plants.update_one(
    |plant| plant.id.eq(1_u64),
    |plant| plant.needs_water.set(false),
)?;

assert_eq!(result.matched_count, 1);
assert_eq!(result.modified_count, 1);
```

Use `update_many` for every matching model. Typed fields provide updates appropriate to their type, including scalar changes, numeric changes, optional fields, and array operations.

```rust
plants.update_many(
    |plant| plant.needs_water.eq(true),
    |plant| plant.needs_water.set(false),
)?;
```

Use an operation builder when an update should insert a model when no match exists.

```rust
let result = plants
    .update_one_with(
        |plant| plant.id.eq(2_u64),
        |plant| plant.needs_water.set(true),
    )
    .upsert(true)
    .execute()?;
```

`replace_one`, `delete_one`, `delete_many`, and the `find_one_and_*` methods cover full replacements, removals, and operations that return the affected model. Array updates support adding, removing, and changing array values; use the [API Reference](../api-reference.md) for positional and filtered-array operations.

## Use BSON update documents

The document API accepts Mongo-like update documents.

```rust
use bson::doc;

documents.update_one(
    doc! { "_id": 1 },
    doc! { "$set": { "needs_water": false } },
)?;
```

Use [Durable writes](durable-writes.md) when a specific write must be durable before your application continues.
