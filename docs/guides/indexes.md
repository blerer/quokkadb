# Indexes

Create an index for a filter or sort that occurs often enough to justify extra work on writes and extra stored data. Start from the query pattern, not from the fields that happen to exist in a model.

The range example extends the `Plant` model from [Getting Started](../getting-started.md) with `height_cm: i32`.

Related: [Find queries](find-queries.md) · [Features — Indexes](../features.md#indexes).

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

`create_index` completes the initial build before it returns. QuokkaDB takes a
snapshot of the collection when it registers the index, scans the documents in
that snapshot, and publishes the index only after all of its entries have been
written. The optimizer ignores the index until publication, and writes to the
collection are rejected until the build has completed. After publication, normal
inserts, updates, and deletes maintain the index.

The scan is split into batches controlled by
`Options::with_index_build_batch_size`. Each batch writes its index entries and
the checkpoint for the last processed primary key together. If the process
stops during a build, opening the database resumes the build from its
checkpoint before `QuokkaDB::open` returns. A failed build is removed and the
creation call returns the error.

Use `create_index_with` to assign a stable name. `list_indexes` shows active indexes, and `drop_index` removes one that no longer matches the application workload.

```rust
let index_name = plants
    .create_index_with(|plant| plant.name.index_asc())
    .name("plant_name")
    .execute()?;

let indexes = plants.list_indexes()?;
plants.drop_index(&index_name)?;
```

## Use hints only when needed

Let the optimizer choose the access path by default. When a query behaves unexpectedly, use `explain()` and executor metrics to inspect the selected plan before forcing one. Add a named-index hint only when the optimizer repeatedly chooses an unsuitable path for a known workload.

Use `.hint_collection_scan()` only when deliberately avoiding secondary indexes. Hints couple application code to an index name and to the query shapes that the index can serve, so changes to index definitions or names may require corresponding code changes. Hints are strict: a missing, unavailable, or unusable named index returns an error instead of falling back.

Use the returned name to force an index for a query when you need a particular access path.

```rust
let plants = plants
    .find(|plant| plant.name.eq("Fern"))
    .hint(&index_name)
    .execute_collect()?;
```

The hint is also available on update, replace, delete, and find-and-modify builders. Dropping and recreating an index with the same name resolves the name to the current index definition when the operation is planned.

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

An index can provide a sort when the requested fields match a leading prefix of the index, either in index order or with every direction reversed. A multikey field cannot provide document sort order for a requested prefix that includes that field. Other sort shapes still work, but QuokkaDB sorts the matching results itself.

For example, an index on `needs_water` ascending and `name` ascending can provide that same two-field order or both fields descending. It does not provide an order by `name` alone.

An index that looks structurally useful is not guaranteed to be chosen. Use metrics after representative application work to see what the executor did.

```rust
let executor = db.metrics().executor();
let index_scans = executor.index_scans();
let collection_scans = executor.collection_scans();
let sorts = executor.external_merge_sorts() + executor.top_k_sorts();
let spill_operations = executor.external_merge_spills();
let spill_bytes = executor.external_merge_spill_bytes();
```

Compare those values with the filters and sorts your application runs. See [Operations](../operations.md#read-metrics-in-the-application) for the available metrics and how to interpret them.

## Index array values

Assuming the model has `tags: Vec<String>` and `orders: Vec<Order>` fields, typed
scalar arrays can be indexed directly. QuokkaDB creates one index entry for each
array member.

```rust
let tag_index = plants.create_index(|plant| plant.tags.index_asc())?;
```

For an array of embedded models, use `each` to index a field across all elements:

```rust
let order_index = shops.create_index(|shop| {
    shop.orders.each(|order| order.total.index_asc())
})?;
```

The embedded-array example indexes `orders.total`. Query the array with the existing typed `any` predicate:

```rust
let high_value_orders = shops
    .find(|shop| shop.orders.any(|order| order.total.gte(100)))
    .execute_collect()?;
```

An array-valued path produces one index entry for each array member. A query still returns a document only once, even when several members match; deduplication happens before `skip` and `limit` are applied.

For an embedded array, `any` keeps predicates correlated to one array element. Separate predicates on dotted paths can match different elements, so use `any` when all conditions must hold for the same element. The document API uses `$elemMatch` for the same purpose.

The document API uses the same multikey behavior with BSON key specifications:

```rust
let tag_index = documents.create_index(doc! { "tags": 1 })?;
let order_total_index = documents.create_index(doc! { "orders.total": 1 })?;
```

In a compound index, scalar fields can precede or follow a multikey field. Without correlation evidence, QuokkaDB does not use a second multikey field as an independent index bound; its predicate remains available for document filtering. A compound index also cannot expand two independent array paths for the same document. For example, indexing both `tags` and `orders.total` fails when a document contains both arrays.

Use `at(index)` for a fixed array position in a filter or projection, not in an index definition. Numeric positions such as `orders.0.total` are not valid index paths.

## When not to add an index

Every index adds write work and occupies storage. Do not index every field. Add an index when an observed filter or sort is important to the application, then keep it only while that query pattern remains useful.

## Current limits

QuokkaDB supports ordered ascending, descending, compound, and multikey indexes. Fixed numeric array positions are not valid index paths. Unique, sparse, partial, text, geospatial, wildcard, and TTL indexes are not available. Read [Features](../features.md#indexes) for the complete support matrix.

## Next

- [Find queries](find-queries.md)
- [Operations](../operations.md)
- [API Reference](../api-reference.md)
