# Update data

Use an update when you want to change selected fields without replacing the rest of a model. This guide starts with the `Plant` model and `plants` typed collection from [Getting Started](../getting-started.md).

The examples extend `Plant` with `height_cm: i32`, `last_watered: Option<bson::DateTime>`, and `tags: Vec<String>`.

Related: [Find queries](find-queries.md) · [Features — Update operators](../features.md#update-operators).

## Make normal typed updates

Pass one closure to choose models and another to describe the change. Update expressions use `and` to combine changes into one operation.

```rust
let result = plants.update_one(
    |plant| plant.id.eq(1_u64),
    |plant| {
        plant
            .needs_water
            .set(false)
            .and(plant.last_watered.unset())
            .and(plant.height_cm.inc(5))
    },
)?;

assert_eq!(result.matched_count, 1);
assert_eq!(result.modified_count, 1);
```

Scalar fields support `set`, `min`, and `max`. Numeric fields also support `inc` and `mul`; optional fields provide `unset`.

Use `update_many` when every match should change. The operation either applies to all selected documents or returns an error without a partial result. See [Concepts](../concepts.md#concurrent-access-and-atomic-writes) for the atomicity and conflict rules.

```rust
plants.update_many(
    |plant| plant.needs_water.eq(true),
    |plant| plant.height_cm.mul(2),
)?;
```

## Change arrays

Array methods use the same names you will see in the API: `add_to_set` adds a value only when it is absent, `push` appends, and `pull` removes matching values.

```rust
plants.update_one(
    |plant| plant.id.eq(1_u64),
    |plant| {
        plant
            .tags
            .add_to_set("indoor")
            .and(plant.tags.push("needs-repotting"))
            .and(plant.tags.pull("temporary"))
    },
)?;
```

Use `add_to_set_each`, `push_each`, `push_each_with`, `pop_first`, `pop_last`, and `pull_all` when the task needs their respective batch, ordering, or removal behavior. Arrays of embedded models also provide `pull_where`.

## Choose operation behavior

Use an operation builder for options such as upsert. `set_on_insert` changes a field only when the upsert creates a document; set every required field for the model in that case.

```rust
let result = plants
    .update_one_with(
        |plant| plant.id.eq(2_u64),
        |plant| {
            plant
                .name
                .set_on_insert("Pothos")
                .and(plant.needs_water.set_on_insert(true))
                .and(plant.height_cm.set_on_insert(20))
        },
    )
    .upsert(true)
    .execute()?;
```

Find-and-modify operations return the affected model. They return the value from before the change by default; choose `ReturnDocument::After` when the caller needs the changed value.

```rust
use quokkadb::document::ReturnDocument;

let updated = plants
    .find_one_and_update_with(
        |plant| plant.id.eq(1_u64),
        |plant| plant.needs_water.set(false),
    )
    .return_document(ReturnDocument::After)
    .execute()?;
```

`replace_one`, `delete_one`, `delete_many`, `find_one_and_replace`, and `find_one_and_delete` cover full replacements and removals. Use [Durable writes](durable-writes.md) when an individual write must be durable before the application continues.

## Use advanced BSON updates when needed

The document API is the escape hatch for update capabilities without a typed equivalent. It supports `$[]` for all array elements and `$[identifier]` with `array_filters` for selected elements.

```rust
use bson::doc;

documents
    .update_many_with(
        doc! { "needs_water": true },
        doc! { "$set": { "care_steps.$[step].done": true } },
    )
    .array_filters(vec![doc! { "step.kind": "water" }])
    .execute()?;
```

`$rename` changes a document field.

```rust
documents.update_one(
    doc! { "_id": 1 },
    doc! { "$rename": { "last_watered": "last_checked" } },
)?;
```

The first-match positional `$` operator and aggregation-style update pipelines are not available. Read [Features](../features.md#update-operators) for the complete update support matrix.

## Next

- [Find queries](find-queries.md)
- [Durable writes](durable-writes.md)
- [API Reference](../api-reference.md)
