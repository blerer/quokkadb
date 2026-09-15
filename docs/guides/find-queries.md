# Find queries

Use `find` when you need several matching models and `find_one` when you need at most one. This guide starts with the `Plant` model and `plants` typed collection from [Getting Started](../getting-started.md).

The examples extend `Plant` with `height_cm: i32`, `location` with a `room` field, `tags: Vec<String>`, and `care: BTreeMap<String, String>`.

Related: [Features — Querying](../features.md#querying) · [Indexes](indexes.md).

## Find matching models

Build a filter from typed fields, then execute the query. `find_one` returns `Result<Option<Plant>>`, so `None` means that no model matched.

```rust
let plant = plants
    .find_one(|plant| plant.height_cm.gte(100).and(plant.needs_water.eq(true)))?
    .expect("a tall thirsty plant exists");
```

Use `find_all` when every model in the collection is needed. It returns the usual query builder, so you can still sort, project, and paginate the scan.

```rust
let plants = plants
    .find_all()
    .sort(|plant| plant.id.asc())
    .execute_collect()?;
```

Scalar fields provide `eq`, `ne`, `gt`, `gte`, `lt`, and `lte`. Use `in_values` and `nin` when a value must match or exclude a set.

```rust
let selected = plants
    .find(|plant| plant.id.in_values([1_u64, 4, 9]).and(plant.name.nin(["Cactus"])))
    .execute()?;
```

Combine typed filters with `and`, `or`, and `nor`. Import `not` when a whole filter should be negated.

```rust
use quokkadb::not;

let candidates = plants
    .find(|plant| {
        not(plant.needs_water.eq(false)).and(
            plant.location.room.eq("living-room").or(plant.height_cm.gt(150)),
        )
    })
    .execute()?;
```

## Query nested values, arrays, and maps

Derived embedded types expose their fields through the parent field. String-keyed maps use `key`, and arrays provide operations that describe the matching rule.

```rust
let indoor_plants = plants
    .find(|plant| {
        plant
            .location
            .room
            .eq("living-room")
            .and(plant.care.key("light").eq("indirect"))
            .and(plant.tags.any_eq("low-maintenance"))
    })
    .execute()?;
```

Use `all` when every listed value must occur and `len_eq` when the array must have an exact length. `any_where` applies a typed predicate to scalar array elements. Arrays of embedded models use `any` to match their fields.

```rust
let tagged = plants
    .find(|plant| {
        plant
            .tags
            .all(["indoor", "low-maintenance"])
            .and(plant.tags.len_eq(2))
            .and(plant.tags.any_where(|tag| tag.eq("indoor").or(tag.eq("office"))))
    })
    .execute()?;
```

## Return only the data you need

`include` and `exclude` shape the BSON document before QuokkaDB deserializes it. Give them a result type that can deserialize the projected fields.

```rust
#[derive(serde::Deserialize)]
struct PlantCard {
    name: String,
    height_cm: i32,
}

let cards: Vec<PlantCard> = plants
    .find(|plant| plant.needs_water.eq(true))
    .include(|plant| (plant.name, plant.height_cm))
    .execute_collect()?;
```

`select` changes the typed result shape instead. It returns the selected value or tuple directly, so no projected model type is needed.

```rust
let names: Vec<String> = plants
    .find(|plant| plant.needs_water.eq(true))
    .select(|plant| plant.name)
    .execute_collect()?;
```

## Sort and paginate

Sort before using `skip` and `limit`. Stable pagination requires a deterministic sort, so add a unique tie-breaker after fields that can have duplicate values.

```rust
let page: Vec<Plant> = plants
    .find(|plant| plant.needs_water.eq(true))
    .sort(|plant| plant.name.asc().then(plant.id.asc()))
    .skip(20)
    .limit(20)
    .execute_collect()?;
```

## Use BSON query documents when they add value

Use the document API when data or filter shape is dynamic. BSON documents also expose document-only query syntax such as `$type` and direct `$elemMatch` expressions.

```rust
use bson::doc;

let candidates = documents
    .find(doc! {
        "$nor": [
            { "care.light": { "$nin": ["indirect", "shade"] } },
            { "height_cm": { "$lt": 100 } },
        ],
        "tags": { "$elemMatch": { "$in": ["indoor", "office"] } },
    })
    .projection(doc! { "name": 1, "height_cm": 1, "_id": 0 })
    .sort(doc! { "name": 1 })
    .execute()?;
```

Read [Features](../features.md#querying) for the complete support matrix, including unavailable query families such as `$regex`, text, geospatial, `$expr`, and `$where`.

## Next

- [Update data](update-data.md)
- [Indexes](indexes.md)
- [API Reference](../api-reference.md)
