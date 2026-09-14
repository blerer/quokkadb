# API Reference

This page maps QuokkaDB's public Rust API to common application tasks. Use a [`TypedCollection<T>`](#typed-collections) when a collection has a Rust model. Use a [`Collection`](#document-collections) when the document shape is dynamic or your application needs BSON query documents.

The [Guides](guides.md) explain complete workflows. This reference describes the types, methods, return values, and builder options available for those workflows.

## Database and collections

`QuokkaDB` owns an opened database directory. It implements `Clone`; clones share the same database instance and can be used from application threads.

```rust
use quokkadb::error::Result;
use quokkadb::QuokkaDB;
use std::path::Path;

fn open_database() -> Result<QuokkaDB> {
    QuokkaDB::open(Path::new("./data"))
}
```

| Method | Returns | Purpose |
| --- | --- | --- |
| `QuokkaDB::open(path)` | `Result<QuokkaDB>` | Opens or creates a database with `Options::default()`. |
| `QuokkaDB::open_with_options(path, options)` | `Result<QuokkaDB>` | Opens a database with validated configuration. |
| `db.options()` | `&Options` | Reads the configuration selected when the database opened. |
| `db.collection(name)` | `Collection` | Gets a document-API collection handle. |
| `db.typed_collection::<T>(name)` | `TypedCollection<T>` | Gets a typed collection handle for `T`. |
| `db.create_collection(name)` | `Result<()>` | Creates a collection with the default `Mixed` ID strategy. |
| `db.create_collection_with(name)` | `CreateCollection` | Creates a collection builder. |
| `db.list_collections()` | `Vec<CollectionInfo>` | Lists collection ID, name, creation time, and ID strategy. |
| `db.metrics()` | `Metrics` | Gets the in-process metrics facade. |

`CreateCollection::id_creation_strategy(strategy)` selects how the document API handles missing `_id` values. Call `execute()` to create the collection.

| `IdCreationStrategy` | Behavior |
| --- | --- |
| `Mixed` | Accept a supplied `_id` or generate one when it is absent. This is the default. |
| `Manual` | Require every inserted document to provide `_id`. |
| `Generated` | Generate every `_id` and reject a supplied `_id`. |

`CollectionInfo` exposes `id`, `name`, `created_at`, and `id_creation_strategy`. See [Manage collections](guides/manage-collections.md) for lifecycle examples and [Concepts](concepts.md#ids) for ID behavior.

## Typed collections

`TypedCollection<T>` stores a Serde-serializable Rust model and produces typed field expressions from it. Derive `QuokkaDocument` for the root collection model. Mark exactly one concrete, non-optional ID field with `#[quokka(id)]`.

```rust
use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, QuokkaDocument)]
struct Plant {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    needs_water: bool,
}

let plants = db
    .typed_collection::<Plant>("plants")
    .create_if_missing();
```

Use `QuokkaType` for an embedded Serde type that should expose typed nested fields but is not a collection document. The derive macro supplies the `Fields<T>` type used by closures passed to `find`, `update_one`, `sort`, and `create_index`.

### Collection lifecycle and indexes

| Method | Returns | Purpose |
| --- | --- | --- |
| `create_if_missing()` | `TypedCollection<T>` | Creates the collection on its first write; reads return no results until then. |
| `create_index(fields)` | `Result<String>` | Creates an index from typed fields. |
| `create_index_with(fields)` | `TypedCreateIndex<T>` | Builds an index; use `name(name)` before `execute()`. |
| `list_indexes()` | `Result<Vec<IndexInfo>>` | Lists active indexes. |
| `drop_index(name)` | `Result<()>` | Removes an index by name. |
| `rename(name)` | `Result<TypedCollection<T>>` | Renames the collection and returns a handle with the new name. |
| `drop_collection()` | `Result<()>` | Drops the collection. |
| `estimated_document_count()` | `Result<u64>` | Returns the storage estimate, not an exact filtered count. |

An index closure returns `Index<T>`. Start an index with `field.index_asc()` or `field.index_desc()`, then use `then` to append fields.

```rust
let index_name = plants.create_index(|plant| {
    plant
        .needs_water
        .index_asc()
        .then(plant.name.index_asc())
})?;
```

### Reads and result shaping

| Method | Returns | Purpose |
| --- | --- | --- |
| `find(filter)` | `TypedFind<T>` | Builds a query for all matching models. |
| `find_one(filter)` | `Result<Option<T>>` | Returns at most one matching model. |
| `find_one_with(filter)` | `TypedFindOne<T>` | Builds a one-model query with options. |

`TypedFind` supports `sort`, `skip`, `limit`, and `execute`. `execute()` returns `TypedQueryOutput<R>`, an iterator of `Result<R>` values. Collect it with `collect::<quokkadb::error::Result<Vec<_>>>()` when the application needs all results.

`TypedFind`, `TypedFindOne`, and typed find-and-modify builders provide these projection methods:

| Method | Result shape |
| --- | --- |
| `include(fields)` | Deserializes included fields into a chosen `NewResult` type. `_id` remains included. |
| `include_without_id(fields)` | Deserializes included fields after omitting `_id`. |
| `exclude(fields)` | Deserializes the remaining fields into a chosen `NewResult` type. |
| `select(fields)` | Returns one selected value or a tuple of selected values. |

For example, use `select` when a view needs only names:

```rust
let names = plants
    .find(|plant| plant.needs_water.eq(true))
    .select(|plant| plant.name)
    .execute()?;
```

### Writes

The direct methods execute immediately. The `_with` variants return builders for operation-specific options, then run when `execute()` is called.

| Operation | Direct method | Builder and options | Result |
| --- | --- | --- | --- |
| Insert one | `insert_one(document)` | `insert_one_with(document).sync()` | `TypedInsertOneResult<T>` with `inserted_id: T::Id` |
| Insert many | `insert_many(documents)` | `insert_many_with(documents).sync()` | `TypedInsertManyResult<T>` with `inserted_ids: Vec<T::Id>` |
| Update one | `update_one(filter, update)` | `update_one_with(...).sort(...).upsert(bool).sync()` | `UpdateResult` |
| Update many | `update_many(filter, update)` | `update_many_with(...).upsert(bool).sync()` | `UpdateResult` |
| Replace one | `replace_one(filter, replacement)` | `replace_one_with(...).sort(...).upsert(bool).sync()` | `UpdateResult` |
| Delete one | `delete_one(filter)` | `delete_one_with(...).sort(...).sync()` | `DeleteResult` |
| Delete many | `delete_many(filter)` | `delete_many_with(...).sync()` | `DeleteResult` |
| Find and update | `find_one_and_update(filter, update)` | `find_one_and_update_with(...).sort(...).upsert(bool).return_document(...).sync()` | `Result<Option<T>>` |
| Find and replace | `find_one_and_replace(filter, replacement)` | `find_one_and_replace_with(...).sort(...).upsert(bool).return_document(...).sync()` | `Result<Option<T>>` |
| Find and delete | `find_one_and_delete(filter)` | `find_one_and_delete_with(...).sort(...).sync()` | `Result<Option<T>>` |

`UpdateResult` exposes `matched_count`, `modified_count`, and `upserted_id`. `DeleteResult` exposes `deleted_count`. `ReturnDocument::Before` is the default for find-and-update and find-and-replace; pass `ReturnDocument::After` to return the changed model.

```rust
let result = plants
    .update_one_with(
        |plant| plant.id.eq(1_u64),
        |plant| plant.needs_water.set(false),
    )
    .upsert(true)
    .execute()?;
```

`sync()` forces that write to become durable before `execute()` returns. It overrides the database durability setting only for that operation. See [Durable writes](guides/durable-writes.md).

## Typed fields and expressions

Typed field methods build `Filter<T>`, `Update<T>`, `Sort<T>`, `Index<T>`, and selections. Combine compatible values with `and`, `or`, `not`, `nor`, and `then` as appropriate.

| Field kind | Query and navigation methods | Update and ordering methods |
| --- | --- | --- |
| Scalar `Field<T, V>` | `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in_values`, `nin` | `set`, `set_on_insert`, `min`, `max`, `asc`, `desc`, `index_asc`, `index_desc` |
| Numeric scalar | All scalar methods | `inc`, `mul`; `i32` and `i64` also provide `bit(and, or, xor)` |
| BSON date or timestamp | All applicable scalar methods | `current_date` for `bson::DateTime`; `current_timestamp` for `bson::Timestamp` |
| Optional `Option<V>` | The inner field's operations plus `exists` | `unset` plus the inner field's supported updates |
| Embedded `QuokkaType` | Its generated nested fields; object equality methods | Whole-object scalar-style updates where applicable |
| Array `Vec<T>` | `at(index)`, `len_eq`, `any_eq`, `all`; embedded models also provide `any(predicate)` | Array-level `set` and ordering methods; `add_to_set`, `add_to_set_each`, `push`, `push_each`, `push_each_with`, `pop_first`, `pop_last`, `pull`, `pull_all`; embedded models also provide `pull_where` |
| Map `BTreeMap<String, V>` | `key(name)` to access a value field | Map-level scalar-style updates and ordering methods |

`PushOptions::new()` configures `push_each`: use `position`, `slice`, `sort_ascending`, or `sort_descending`. For arrays of embedded `QuokkaType` values, `sort_by` accepts a typed sort closure.

Typed filters and updates follow BSON comparison behavior. See [Find queries](guides/find-queries.md) and [Update data](guides/update-data.md) for task-oriented examples.

`Filter<T>` combines conditions with `and`, `or`, `not`, and `nor`. `Update<T>`, `Sort<T>`, and `Index<T>` combine compatible expressions with `then`. `TypedSelection<T>` is the trait implemented by a field or tuple of fields accepted by `include`, `exclude`, and `select`.

The derive macros implement `QueryFieldType` for the model shapes they generate. For a custom BSON leaf type, implement `QuokkaScalar`; add `NumericValue` when it supports `inc` and `mul`, or `BitwiseValue` when it supports `bit`. A type must not implement both `QuokkaScalar` and `QuokkaType`. `TypedPath` and `TypedQueryField` are public building blocks used by the generated field API; application code normally uses derived fields instead of constructing them directly.

## Document collections

`Collection` accepts values implementing `Serialize` for inserts and uses `bson::Document` for filters, update documents, projections, sorts, replacements, array filters, and indexes.

```rust
use bson::doc;

let plants = db.collection("plants").create_if_missing();

let plants_to_water = plants
    .find(doc! { "needs_water": true })
    .sort(doc! { "name": 1 })
    .limit(20)
    .execute()?;
```

### Collection lifecycle and indexes

`Collection` exposes the same lifecycle methods as `TypedCollection<T>`: `create_if_missing`, `list_indexes`, `drop_index`, `rename`, `drop_collection`, and `estimated_document_count`.

Use `create_index(keys)` for a BSON key specification, or `create_index_with(keys).name(name).execute()` to choose its name. A key value of `1` is ascending and `-1` is descending.

```rust
let index_name = plants.create_index(doc! {
    "needs_water": 1,
    "name": 1,
})?;
```

`IndexInfo` exposes an index `id`, `name`, and ordered `fields`. Each `IndexFieldInfo` has its BSON `path` and an `IndexDirection` of `Ascending` or `Descending`.

### Reads and writes

| Operation | Direct method | Builder and options | Result |
| --- | --- | --- | --- |
| Find many | `find(filter)` | `projection`, `sort`, `skip`, `limit`, `execute` | `QueryOutput`, an iterator of `Result<Document>` |
| Find one | `find_one(filter)` | `find_one_with(filter).projection(...).sort(...).execute()` | `Result<Option<Document>>` |
| Insert one | `insert_one(document)` | `insert_one_with(document).sync()` | `InsertOneResult` with `inserted_id: Bson` |
| Insert many | `insert_many(documents)` | `insert_many_with(documents).sync()` | `InsertManyResult` with `inserted_ids: Vec<Bson>` |
| Update one | `update_one(filter, update)` | `update_one_with(...).array_filters(...).sort(...).upsert(bool).sync()` | `UpdateResult` |
| Update many | `update_many(filter, update)` | `update_many_with(...).array_filters(...).upsert(bool).sync()` | `UpdateResult` |
| Replace one | `replace_one(filter, replacement)` | `replace_one_with(...).sort(...).upsert(bool).sync()` | `UpdateResult` |
| Delete one | `delete_one(filter)` | `delete_one_with(...).sort(...).sync()` | `DeleteResult` |
| Delete many | `delete_many(filter)` | `delete_many_with(...).sync()` | `DeleteResult` |
| Find and update | `find_one_and_update(filter, update)` | `find_one_and_update_with(...).projection(...).sort(...).upsert(bool).return_document(...).sync()` | `Result<Option<Document>>` |
| Find and replace | `find_one_and_replace(filter, replacement)` | `find_one_and_replace_with(...).projection(...).sort(...).upsert(bool).return_document(...).sync()` | `Result<Option<Document>>` |
| Find and delete | `find_one_and_delete(filter)` | `find_one_and_delete_with(...).projection(...).sort(...).sync()` | `Result<Option<Document>>` |

`array_filters` accepts `Vec<Document>` for filtered positional array updates. `projection` changes the returned document; it does not change which document is updated or deleted. See [Update data](guides/update-data.md) for Mongo-like update documents.

## Configuration

Import options from their public modules:

```rust
use quokkadb::options::options::{Options, WalDurability};
use quokkadb::options::storage_quantity::{StorageQuantity, StorageUnit};
```

`Options::default()`, `Options::lightweight()`, `Options::optimized()`, and `Options::high_query_load()` create profiles. Each `with_*` method consumes and returns `Options`, so settings can be chained before `QuokkaDB::open_with_options`.

| Option builder group | Methods |
| --- | --- |
| Resource and cache limits | `with_file_write_buffer_size`, `with_max_open_files`, `with_block_cache_size`, `with_query_cache_size` |
| WAL behavior | `with_wal_durability`, `with_wal_bytes_per_sync`, `with_max_manifest_file_size` |
| Compaction | `with_max_levels`, `with_level0_file_num_compaction_trigger`, `with_max_bytes_for_level_base`, `with_max_bytes_for_level_multiplier`, `with_compaction_threads`, `with_max_target_file_size` |
| Storage format | `with_block_size`, `with_restart_interval`, `with_bloom_fpr` |

Each builder has a corresponding accessor without `with_`, such as `block_cache_size()` or `wal_durability()`. `validate()` checks an `Options` value directly; opening a database validates it automatically.

`Options` also exposes `with_compressor` and `compressor_type`, but their `CompressorType` is not currently re-exported through a public module. Downstream applications cannot construct a compressor value through the public API, so use the configured default compressor until that type is exposed.

`StorageQuantity::new(value, unit)` expresses a size. Use `to_bytes()` for bytes and `convert_to(unit)` for a converted, truncated quantity. `StorageUnit` has `Bytes`, `Kibibytes`, `Mebibytes`, and `Gibibytes` variants.

| `WalDurability` | Acknowledged-write behavior |
| --- | --- |
| `Durable` | Durable before the write returns. |
| `ProcessSafe` | Survives a process crash; a recent write can still be lost after an OS crash or power loss before a periodic sync. |
| `Buffered` | May remain in QuokkaDB's userspace buffer and can be lost in a process crash. |

Read [Options](guides/options.md) before changing storage or compaction settings.

## Results, errors, IDs, and metrics

Operations return `quokkadb::error::Result<T>`, an alias for `Result<T, Error>`. `Error` distinguishes I/O, BSON and deserialization failures, invalid requests or options, version conflicts, collection and index lookup failures, and recovery errors. Handle errors at the operation boundary; callers can inspect an `Error` variant when different application responses are needed.

`QuokkaId::new()` creates a Sonyflake ID for an application-generated document `_id`; `QuokkaId::default()` creates one too. It serializes as BSON `Int64`. `as_u64()` returns its numeric value, and `TryFrom<u64>` rejects values that do not fit BSON `Int64`.

`db.metrics()` returns grouped runtime counters and gauges:

| Metric group | Accessor | Available values |
| --- | --- | --- |
| Block cache | `block_cache()` | `size`, `hits`, `misses`, `hit_ratio`, `evictions` |
| SSTable cache | `sstable_cache()` | `open_count`, `hits`, `misses`, `hit_ratio` |
| Query cache | `query_cache()` | `size`, `hits`, `misses`, `hit_ratio` |
| Write-ahead log | `wal()` | `files`, `total_bytes`, `syncs`, `bytes_buffered`, `bytes_written` |
| Flushes | `flush()` | `count`, `duration`, `write_throughput`, `memtable_size` |
| Manifest | `manifest()` | `rewrite`, `writes`, `size`, `bytes_written` |
| Storage | `storage()` | `sstable_count`, `sstable_count_at_level`, `total_sstable_size`, `sstable_size_at_level`, `memtable_size`, `memtable_total_size`, `memtable_count` |
| Compaction | `compaction()` | `jobs_picked`, `jobs_picked_full`, `jobs_picked_partial`, `jobs_skipped_level_compacting`, `jobs_skipped_range_overlap`, `input_files_count`, `picked_at_level`, `score_at_level`, `active_at_level` |
| Query execution | `executor()` | `read_queries`, `write_queries`, `read_query_duration`, `write_query_duration`, `rows_returned`, `documents_written`, `collection_scans`, `index_scans`, `point_searches`, `multi_point_searches`, `in_memory_sorts`, `external_merge_sorts`, `top_k_sorts` |

Duration, throughput, size, and input-count accessors that return `HistogramMetrics` provide `count`, `sum`, `min`, `max`, `mean`, `stddev`, `quantile`, and `buckets`. See [Operations](operations.md) for observability guidance.
