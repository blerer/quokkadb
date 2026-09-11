# Indexes

Create an index when your application frequently filters or sorts on the same fields. An index can make those queries more efficient, but it also adds work when documents are written.

## Create an index for a typed collection

The typed API builds an index from model fields. This compound index supports queries that use `needs_water` and `name` in that order.

```rust
let index_name = plants.create_index(|plant| {
    plant
        .needs_water
        .index_asc()
        .then(plant.name.index_asc())
})?;
```

Use `create_index_with` when you need to assign a name. Use `list_indexes` to inspect active indexes and `drop_index` to remove one by name.

```rust
let index_name = plants
    .create_index_with(|plant| plant.name.index_asc())
    .name("plant_name")
    .execute()?;

let indexes = plants.list_indexes()?;
plants.drop_index(&index_name)?;
```

Start with the filters and sorts your application actually performs. Add an index for those paths rather than indexing every field.

## Use BSON key specifications

The document API accepts the same field order as a BSON key specification. Use `1` for ascending and `-1` for descending order.

```rust
use bson::doc;

let index_name = documents.create_index(doc! {
    "needs_water": 1,
    "name": 1,
})?;
```

See [Find queries](find-queries.md) for the filters and sorts that indexes can support.
