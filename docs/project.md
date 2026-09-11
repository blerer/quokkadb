# Project

QuokkaDB is an evolving embedded application database for Rust. It aims to let applications persist their models without running separate database infrastructure. This page explains the current stability expectations and how to contribute.

## Current development stage

QuokkaDB is pre-1.0. Its public API and on-disk format may change before the first stable release. Plan upgrades around the project's release notes, and do not assume that a database directory can be read by a later or earlier version without an announced migration path.

The current release supports document and typed Rust-model APIs, queries, updates, indexes, durable writes, concurrent access within an application, and in-process observability. Multi-operation transactions are not currently exposed. There is no supported backup, restore, upgrade, or downgrade procedure yet.

Read [Features](features.md) for the supported surface and [Operations](operations.md) for durability, recovery, and current data-management limits.

## Set up a development checkout

Use a Rust toolchain that supports the Rust 2024 edition. Clone the repository, enter it, and run the test suite.

```sh
git clone https://github.com/blerer/quokkadb.git
cd quokkadb
cargo test
```

The test suite includes unit tests beside implementation modules, integration tests in `tests/`, restart coverage, document and typed API behavior, and compile-fail tests for invalid typed API usage.

Run a focused test while developing a narrow change:

```sh
cargo test --test typed_insert
```

Some storage-layout test helpers require the internal test feature. Use it when a test needs to force a flush or compaction state:

```sh
cargo test --features internal-testing
```

Run `cargo fmt` after changing Rust code. Documentation-only changes do not need Rust formatting. Run the full `cargo test` suite before submitting a change that affects behavior.

## Make a focused change

Keep changes within the existing module boundaries. Add behavior behind established public entry points such as `QuokkaDB` and `Collection` unless a new public type is necessary.

Add focused semantic tests close to the code they cover. Use concrete BSON documents and exact errors when they clarify behavior. Keep small unit tests in the module; place larger tests in the module's test child or in `tests/` when they exercise the public API. Update documentation whenever a user-visible behavior, guarantee, limitation, or public API changes.

For broad or structural changes, describe the design before implementation. Include the trade-offs, the assumptions it introduces, and the future costs it creates. Preserve existing comments and documentation unless they are incorrect or misleading.

Read the [documentation style guide](STYLE.md) before changing user documentation. The [API Reference](api-reference.md) maps the public API, while the [Guides](guides.md) explain common tasks.

## Report and discuss work

Use the [GitHub issue tracker](https://github.com/blerer/quokkadb/issues) to report a bug. Include the QuokkaDB revision, Rust version, operating system, a minimal reproduction, the expected behavior, the actual behavior, and any relevant error or tracing output.

Open an issue before implementing a larger feature or structural change. It gives maintainers and contributors a place to agree on the user-facing behavior, design, and compatibility impact before the code becomes expensive to revise.

Small fixes, tests, and documentation improvements can be proposed directly as focused pull requests. Keep a pull request limited to one coherent change, include relevant tests, and explain any user-visible behavior or documentation updates in its description.

## License

QuokkaDB is licensed under the [Apache License 2.0](../LICENSE).
