# Find queries

Use `find` when you need several matching models and `find_one` when you need at most one. This guide assumes the `Plant` model and `plants` typed collection from [Getting Started](../getting-started.md).

## Find matching models

Filter fields with typed expressions, then sort and limit the result set before executing the query.

```rust
let thirsty: Vec<Plant> = plants
    .find(|plant| plant.needs_water.eq(true).and(plant.name.ne("Cactus")))
    .sort(|plant| plant.name.asc())
    .skip(0)
    .limit(20)
    .execute()?
    .collect::<quokkadb::error::Result<_>>()?;
```

Use `find_one` when a single matching model is enough. It returns `Result<Option<Plant>>`: `None` means that no model matched the filter.

```rust
let plant = plants.find_one(|plant| plant.id.eq(1_u64))?;
```

Combine filters with `and`, `or`, `not`, and `nor`. Typed fields support equality and comparisons where their value type permits them. Nested values, optional fields, arrays, and maps also expose typed query fields; see the [API Reference](../api-reference.md) for their available operations.

Use `in_values` or `nin` when a scalar field must match or exclude several values.

```rust
let selected = plants
    .find(|plant| plant.id.in_values([1_u64, 4, 9]).and(plant.needs_water.nin([false])))
    .execute()?;
```

## Return only the data you need

Use `include`, `exclude`, or `select` on a typed find builder to change the returned shape. `include` and `exclude` deserialize the selected document into another Rust type, while `select` returns the chosen field or fields directly.

Apply `sort`, `skip`, and `limit` before `execute` when a view needs ordered or paged results.

## Use BSON query documents

The document API accepts BSON filter, projection, and sort documents. Use it when the query or result shape is dynamic.

```rust
use bson::doc;

let thirsty = documents
    .find(doc! {
        "needs_water": true,
        "name": { "$ne": "Cactus" },
    })
    .projection(doc! { "name": 1, "_id": 0 })
    .sort(doc! { "name": 1 })
    .skip(0)
    .limit(20)
    .execute()?
    .collect::<quokkadb::error::Result<Vec<_>>>()?;
```

Continue with [Indexes](indexes.md) when a filter or sort becomes a frequent part of your application.
