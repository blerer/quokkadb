# Manage collections

A collection is a named group of models or documents. Use a typed collection when the collection has one Rust model type, or a document collection for dynamic BSON data.

## Create a collection when it is first used

Collection handles are strict by default: an operation on a missing collection returns an error. Call `create_if_missing` when a collection should be created by its first write. Queries through that handle return no results while the collection is still missing.

```rust
let plants = db
    .typed_collection::<Plant>("plants")
    .create_if_missing();
```

Create a collection explicitly when you need to choose its ID strategy.

```rust
use quokkadb::IdCreationStrategy;

db.create_collection_with("plants")
    .id_creation_strategy(IdCreationStrategy::Manual)
    .execute()?;
```

`Generated` creates every document ID in the database and rejects a supplied `_id`. `Manual` requires an application-provided ID. `Mixed` accepts a supplied `_id` or generates one when it is missing; it is the default.

These strategies are primarily for the document API, where a document can omit `_id`. A typed `QuokkaDocument` model must declare exactly one concrete `#[quokka(id)]` field; the ID cannot be an `Option`, so each typed model provides an ID before insertion. `QuokkaId` is not yet available as a typed model ID. See [Getting Started](../getting-started.md#generate-an-id-in-your-application) for application-generated IDs.

## Inspect and change collections

List collection metadata from the database. Rename and drop collections through either a typed or document collection handle.

```rust
let collections = db.list_collections();
let houseplants = plants.rename("houseplants")?;
let count = houseplants.estimated_document_count()?;
houseplants.drop_collection()?;
```

`estimated_document_count` uses storage count statistics and is useful when an exact query count is not required.

The document API follows the same lifecycle methods after calling `db.collection("plants")`.
