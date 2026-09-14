# Indexes

Create an index for a filter or sort that occurs often enough to justify extra work on writes and extra stored data. Start from the query pattern, not from the fields that happen to exist in a model.

The range example extends the `Plant` model from [Getting Started](../getting-started.md) with `height_cm: i32`.

## Create an index for a query pattern

Suppose a screen finds thirsty plants and orders them by name. Create the compound index in that field order.

```rust
let index_name = plants.create_index(|plant| {
    plant
        .needs_water
        .index_asc()
        .then(plant.name.index_asc())
})?;
```

Use `create_index_with` to assign a stable name. `list_indexes` shows active indexes, and `drop_index` removes one that no longer matches the application workload.

```rust
let index_name = plants
    .create_index_with(|plant| plant.name.index_asc())
    .name("plant_name")
    .execute()?;

let indexes = plants.list_indexes()?;
plants.drop_index(&index_name)?;
```

The document API uses the same field order in a BSON key specification. Use `1` for ascending and `-1` for descending order.

```rust
use bson::doc;

let index_name = documents.create_index(doc! {
    "needs_water": 1,
    "name": 1,
})?;
```

## Put leading fields first

For filters, an index scan starts with the leading indexed fields. An equality condition on each leading field can be followed by a range on the next field. For example, an index on `needs_water`, then `height_cm`, fits a query that fixes `needs_water` and filters a height range.

```rust
let index_name = plants.create_index(|plant| {
    plant
        .needs_water
        .index_asc()
        .then(plant.height_cm.index_asc())
})?;

let tall_thirsty = plants
    .find(|plant| plant.needs_water.eq(true).and(plant.height_cm.gte(100)))
    .execute()?;
```

A query that filters only `height_cm` does not have the leading `needs_water` condition, so this index does not provide the same targeted scan. Choose compound fields from the filters your application actually performs.

## Match sort order

An index can provide a sort when the requested sort fields match an indexed leading prefix in the same direction, or when the complete requested order is reversed. Other sort shapes still work, but QuokkaDB sorts the matching results itself.

For example, an index on `needs_water` ascending and `name` ascending can provide that same two-field order or both fields descending. It does not provide an order by `name` alone.

An index that looks structurally useful is not guaranteed to be chosen. Use metrics after representative application work to see what the executor did.

```rust
let executor = db.metrics().executor();
let index_scans = executor.index_scans();
let collection_scans = executor.collection_scans();
let sorts = executor.in_memory_sorts()
    + executor.external_merge_sorts()
    + executor.top_k_sorts();
```

Compare those values with the filters and sorts your application runs. See [Operations](../operations.md#read-metrics-in-the-application) for the available metrics and how to interpret them.

## When not to add an index

Every index adds write work and occupies storage. Do not index every field. Add an index when an observed filter or sort is important to the application, then keep it only while that query pattern remains useful.

## Current limits

QuokkaDB supports ordered ascending, descending, and compound indexes. An indexed path cannot include an array element, so multikey and array-path indexes are not available.

Unique, sparse, partial, text, geospatial, wildcard, and TTL indexes are not available. Read [Features](../features.md#indexes) for the complete support matrix.
