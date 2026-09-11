# QuokkaDB Documentation Style Guide

This guide defines how QuokkaDB documentation should be written and organized.

The goal is to make QuokkaDB feel simple to understand and use while remaining precise about what the database does and guarantees.

## 1. Positioning

QuokkaDB is an **application database**.

Its core proposition is:

> **Persist your application models without database infrastructure.**

QuokkaDB runs inside the application and provides document storage, queries, updates, indexes, and persistence without requiring a separate database server.

When introducing QuokkaDB, emphasize:

- application persistence
- no database infrastructure
- embedded operation
- working directly with application models
- Mongo-like document queries and updates
- real database capabilities such as indexes and durable storage

Do not primarily position QuokkaDB as:

- an LSM-tree implementation
- a BSON store
- a key-value store
- a MongoDB replacement
- an ORM or ODM

Those concepts may be useful when explaining specific features, but they are not the reason users should care about QuokkaDB.

## 2. Write from the user's goal

Start with what the user wants to accomplish, not with QuokkaDB's internal abstraction.

Prefer:

> Use a typed collection to store and query your Rust types directly.

Over:

> `TypedCollection<T>` is a generic abstraction over a BSON document collection.

Prefer:

> Create an index when queries frequently filter or sort on a field.

Over:

> QuokkaDB exposes secondary indexes through the `Index` API.

Implementation details should follow the user-facing explanation when they are useful.

## 3. Show the simplest useful example early

Most documentation pages should show code shortly after introducing the task or concept.

Prefer this order:

**Goal → minimal example → explanation → details and edge cases**

Do not explain every option before showing how the common case works.

Examples should represent realistic application code rather than demonstrate APIs in isolation.

## 4. Prefer the typed API for application examples

When both APIs can express the same operation, introductory documentation should normally use the typed API.

The typed API best demonstrates QuokkaDB's goal of persisting application models directly.

Use the document API when:

- explaining document-specific functionality
- demonstrating dynamic data
- the typed API cannot express the operation
- showing an escape hatch from the typed API
- documenting the document API itself

Do not make BSON an unnecessary part of the user's mental model when working with typed application data.

## 5. Keep the writing simple

Use short sentences and short paragraphs.

Prefer ordinary database terminology over QuokkaDB-specific terminology.

Avoid unnecessary abstractions and formal definitions when a concrete explanation is sufficient.

Prefer:

> `find_one` returns the first matching document.

Over:

> The `find_one` operation executes a query against the collection and materializes the first matching result.

Do not make simple operations sound complicated.

## 6. Be precise

Simplicity must not come at the expense of correctness.

Clearly distinguish between:

- guarantees and typical behavior
- supported and unsupported features
- current functionality and planned functionality
- durable and non-durable operations
- typed API restrictions and underlying document capabilities

Avoid vague claims such as:

> QuokkaDB provides robust consistency.

Instead describe the actual guarantee.

When behavior differs from MongoDB, Rust conventions, or another likely user expectation, call it out explicitly.

## 7. Be open about limitations

QuokkaDB is a young project. Do not hide that fact, but do not repeatedly apologize for it.

Use direct statements:

> Transactions are not currently supported.

> The on-disk format may change before the first stable release.

Avoid defensive language such as:

> Unfortunately, QuokkaDB does not yet support...

Do not describe planned functionality as though it already exists.

## 8. Avoid marketing language in technical documentation

The README and documentation landing page can explain why QuokkaDB exists.

Technical documentation should concentrate on helping users understand and use the database.

Avoid words such as:

- blazing fast
- revolutionary
- powerful
- cutting-edge
- enterprise-grade
- seamless
- next-generation

Prefer concrete properties and measurable behavior.

## 9. Keep implementation details in their place

Users should not need to understand the storage engine to use QuokkaDB.

Do not introduce WALs, SSTables, sequence numbers, compaction, internal keys, or LSM internals unless they help answer the question addressed by the page.

For example, a durability page may explain the WAL because it is relevant to the guarantees being described.

A page explaining `insert_one` should not.

## 10. Organize documentation by user intent

The main documentation should be organized around:

### Getting Started

Help a new user go from nothing to a working persistent application quickly.

### Features

Answer:

> Can QuokkaDB do what my application needs?

Provide a clear overview of supported functionality and limitations.

### Guides

Answer practical questions:

> How do I create an index?

> How do I query a nested field?

> How do I update an array?

Guides should be task-oriented.

### Concepts

Explain important ideas users need to understand:

- documents and models
- collections
- IDs
- indexes
- queries
- consistency
- durability

### API Reference

Document the exact Rust API and available options.

Reference documentation should not replace guides or conceptual documentation.

## 11. Make feature support easy to discover

A potential user should not have to search through API documentation to determine whether QuokkaDB supports something.

Maintain a clear feature/support overview.

For important features, link from the overview to detailed documentation.

Be explicit about:

- supported
- partially supported
- unsupported
- planned

## 12. Explain the common case first

Document the path most applications should use before alternatives and advanced options.

For example:

1. Show a typed model.
2. Store it.
3. Query it.
4. Explain additional query operators.
5. Introduce the document API only if needed.

Do not make advanced flexibility obscure the simple path.

## 13. Use progressive disclosure

A reader should be able to stop reading once they have enough information.

Start with the minimum needed to use a feature correctly.

Then cover:

- additional options
- semantics
- edge cases
- performance considerations
- implementation details, when relevant

Avoid front-loading every detail.

## 14. Use consistent terminology

Prefer these concepts consistently:

- **application database** — when describing QuokkaDB's role
- **embedded** — when describing how QuokkaDB runs
- **model** or **Rust type** — for typed application data
- **document** — when discussing the underlying document model
- **typed API** — for model-oriented Rust access
- **document API** — for direct BSON/document access
- **collection** — for a collection of stored documents/models

Do not casually introduce synonyms for established concepts.

## 15. Documentation should reinforce simplicity

The documentation itself is part of the QuokkaDB experience.

A database presented as simple should have documentation that feels simple.

A reader should quickly be able to answer:

1. What is QuokkaDB?
2. Why would I use it?
3. Does it support what I need?
4. How do I get started?
5. What are its current limitations?

If answering one of these questions requires understanding QuokkaDB internals, the documentation probably needs to be simplified.

## Guiding principle

When deciding how to explain something, start from:

> **Your application has data. QuokkaDB makes it persistent.**

Then introduce only the concepts the user needs to accomplish that goal.