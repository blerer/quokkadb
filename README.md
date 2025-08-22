# QuokkaDB

A high-performance, document-oriented database engine written in Rust.

## Introduction

QuokkaDB is a modern database built from the ground up in Rust, designed for performance, reliability, and ease of use. It uses a Log-Structured Merge-Tree (LSM-Tree) storage engine, making it highly efficient for write-heavy workloads. Documents are stored in BSON format, providing a rich and flexible data model.

## Features

- **Document-Oriented**: Store and query flexible BSON documents.
- **LSM-Tree Storage Engine**: Optimized for high write throughput, with Memtables, SSTables, and a Write-Ahead Log (WAL) for durability.
- **Rich Query API**: Fluent API for finding documents, with support for projections, sorting, skipping, and limiting results.
- **Query Optimization**: A cost-based query optimizer with a set of normalization rules to ensure efficient query execution.
- **Pluggable Compression**: Supports Snappy, LZ4, and no-op compression for SSTable blocks to save space.
- **Configurable**: Fine-tune performance with options for cache sizes, block sizes, and more.
- **Monitoring**: Built-in metrics for observing database performance.

## Project Status & Roadmap

QuokkaDB is a work in progress and not yet ready for production use. The future roadmap includes:

- **Query Optimization**: Completing the query optimization process.
- **Compaction**: Implementing LSM-Tree compaction.
- **Update & Delete**: Adding full support for update and delete operations.
- **Indexing**: Introducing secondary indexing capabilities.
- **Transactions**: Implementing support for ACID transactions.

## Usage

Here's a quick example of how to use QuokkaDB.

First, add QuokkaDB to your `Cargo.toml`:

```toml
[dependencies]
# The crate name and version are assumed
quokkadb = "0.1.0"
bson = "2.0"
```

Then, you can use it in your code:

```rust
use quokkadb::{QuokkaDB, Document};
use bson::doc;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open the database
    let db = QuokkaDB::open(Path::new("/tmp/quokkadb"))?;

    // Get a collection. This will create it if it doesn't exist.
    let collection = db.collection("my_collection");

    // The API for inserting documents is not available in the provided code snippets.
    // The following is an assumption of how it might work.
    // let doc_to_insert = doc! {
    //     "name": "Quokka",
    //     "continent": "Australia",
    //     "cuteness_level": 9001,
    // };
    // collection.insert_one(doc_to_insert)?;

    // Find documents
    let filter = doc! { "cuteness_level": { "$gt": 9000 } };
    let results = collection.find(filter)
        .projection(doc! { "name": 1, "cuteness_level": 1, "_id": 0 })
        .sort(doc! { "cuteness_level": -1 })
        .limit(10)
        .execute()?;

    for result in results {
        let doc = result?;
        println!("{}", doc);
    }

    Ok(())
}
```

## Architecture

QuokkaDB has a layered architecture:

- **Query API**: A user-friendly, fluent API for interacting with the database.
- **Query Engine**:
    - **Parser**: Parses BSON queries into an abstract syntax tree (AST) of expressions.
    - **Logical Planner**: Builds a logical plan from the AST.
    - **Optimizer**: Applies a series of normalization rules to the logical plan to create an optimized plan. It includes a cost-based estimator to choose the best physical plan.
    - **Physical Planner**: Converts the optimized logical plan into a physical execution plan.
    - **Executor**: Executes the physical plan, fetching data from the storage engine.
- **Storage Engine**:
    - **LSM-Tree**: The core storage structure, composed of:
        - **Memtable**: An in-memory skip-list for fast writes.
        - **SSTables**: Immutable, sorted files on disk for persistent storage.
        - **Write-Ahead Log (WAL)**: Ensures durability of writes before they are flushed to SSTables.
    - **Block Cache**: Caches SSTable blocks in memory to speed up reads.
    - **Concurrency Control**: Manages concurrent reads and writes.

## Building

```bash
# Clone the repository
git clone https://github.com/your-username/quokkadb.git
cd quokkadb

# Build
cargo build --release

# Run tests
cargo test
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
