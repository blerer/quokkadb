# Changelog

All notable changes to QuokkaDB are documented here.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project is currently pre-1.0. Public APIs and the on-disk format may
change before the first stable release.

## [Unreleased]

### Added

- Added the public `quokkadb::typed` module as the main entry point for typed
  model fields, filters, updates, projections, sorting, indexes, and typed
  collection builders.
- Added comprehensive Rustdoc examples and API documentation for the database,
  document API, typed API, options, errors, results, and derive macros.
- Added package documentation for the `quokkadb`, `quokkadb_derive`, and
  `quokkadb-cli` crates.

### Changed

- Reorganized the public API into focused modules while keeping common handles
  and operation types available at the crate root.
- Re-exported `Error`, `Result`, database options, storage quantity types, and
  `ReturnDocument` from the crate root.
- Moved detailed types to their owning modules. Update explicit imports as
  follows:

  | Previous path | Current path |
  | --- | --- |
  | `quokkadb::error::Result` | `quokkadb::Result` |
  | `quokkadb::Find` | `quokkadb::collection::Find` |
  | `quokkadb::PushOptions` | `quokkadb::typed::PushOptions` |
  | `quokkadb::Metrics` | `quokkadb::metrics::Metrics` |
  | `quokkadb::ExplainPlan` | `quokkadb::explain::ExplainPlan` |
  | `quokkadb::document::ReturnDocument` | `quokkadb::ReturnDocument` |
  | `quokkadb::options::options::{...}` | `quokkadb::options::{...}` |

- Improved public API documentation and examples throughout the README files
  and user guides.

[Unreleased]: https://github.com/blerer/quokkadb/compare/v0.1.0...HEAD
