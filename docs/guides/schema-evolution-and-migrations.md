# Schema evolution & migrations

QuokkaDB lets an application change its Rust models over time, but different changes have different owners. Serde handles compatible model changes, QuokkaDB handles collection and index metadata, and application code must transform documents when their meaning or structure changes.

Related: [Concepts — Documents and typed models](../concepts.md#documents-and-typed-models) · [Update data](update-data.md) · [Manage collections](manage-collections.md).

## Start with a compatible model change

Suppose the first version of an application stored only an ID and a name:

```rust
#[derive(serde::Serialize, serde::Deserialize, quokkadb::QuokkaDocument)]
struct Plant {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
}
```

The application can later add fields without rewriting every old document:

```rust
#[derive(serde::Serialize, serde::Deserialize, quokkadb::QuokkaDocument)]
struct Plant {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    #[serde(default)]
    needs_water: bool,
    notes: Option<String>,
}
```

When this model reads an old document, `needs_water` becomes `false` through Serde's default and `notes` becomes `None`. The old BSON document stays unchanged until the application writes it again.

## Three kinds of change

### Transparent evolution: Serde handles it

Use optional fields, Serde defaults, and stable persisted names when old and new documents still represent the same model. These changes affect how the application reads and writes a document; they do not automatically rewrite stored BSON.

Serde ignores unknown fields by default, so removing a Rust field does not require an immediate data rewrite. A required new field without a default is different: old documents fail to deserialize until they are migrated or the field receives a default.

### Database metadata changes: QuokkaDB handles them

Collections and indexes are database metadata. Use the collection APIs to rename a collection, add or drop an index, or replace an index definition. These operations do not change the document shape.

```rust
let renamed_plants = plants.rename("houseplants")?;

let index_name = renamed_plants.create_index(|plant| plant.name.index_asc())?;
renamed_plants.drop_index(&index_name)?;
```

Collection renames preserve the collection's documents and indexes; use the handle returned by `rename`. To change an index definition, drop the old index first and create the new one. Index creation is a metadata operation and does not serve as a document migration; see [Indexes](indexes.md) for the supported index shapes and lifecycle API.

### Data migrations: the application transforms documents

A data migration is required when the new model gives existing values a different meaning, type, or structure. Run an explicit update or read-transform-write operation, and make it safe to retry if the process stops partway through.

For a simple field rename, the document API supports `$rename`:

```rust
use bson::doc;

documents.update_many(
    doc! {},
    doc! { "$rename": { "legacy_name": "name" } },
)?;
```

Use an application read-transform-replace loop for type conversions and structural changes. `update_many` commits matching documents individually, so a migration must tolerate stopping partway through and be safe to retry. QuokkaDB does not provide a multi-operation transaction or a general migration runner. Validate the result before removing compatibility code.

## Compatibility table

| Change | Supported? | How |
| --- | --- | --- |
| Add optional field | ✅ | No migration required; a missing field deserializes as `Option::None`. |
| Add field with default | ✅ | Use Serde's `#[serde(default)]`; old documents read with the default and gain the field when rewritten. |
| Remove field | ✅ | Old BSON fields are ignored by Serde by default. |
| Rename Rust field | ✅ | Keep the persisted name with `#[serde(rename = "old_name")]`; BSON remains unchanged. |
| Rename persisted field | ⚠️ | Run an explicit `$rename` migration. |
| Change field type | ⚠️ | Transform existing values explicitly before the new typed model reads them. |
| Change document structure | ⚠️ | Use an application migration to construct the new structure. |
| Change `_id` type or value | ⚠️/❌ | `_id` identifies the document; treat this as an identity migration that creates new identities and removes or replaces the old documents. |
| Add index | ✅ | Call `create_index`; QuokkaDB manages the index metadata and maintains the index for database writes. This does not change document schema. |
| Drop index | ✅ | Call `drop_index`; this is a metadata operation. |
| Change index definition | ✅ | Drop the old index and create the replacement definition. |
| Rename collection | ✅ | Call `collection.rename("new_name")`; QuokkaDB preserves the collection's documents and indexes and returns a handle for the new name. |
| Transform all existing documents | ⚠️ | Run an explicit `update_many` or application read-transform-write migration. |

## Keep database upgrades separate

This page covers application models and documents. QuokkaDB is pre-1.0, and its on-disk format may change; there is currently no supported database-directory upgrade or downgrade procedure. Plan those upgrades around the project's release notes. See [Features](../features.md#not-supported) and [Project](../project.md#current-development-stage) for the current limits.

## Next

- [Update data](update-data.md)
- [Indexes](indexes.md)
- [Manage collections](manage-collections.md)
