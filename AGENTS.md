# Codebase Style Guide

This guide captures conventions specific to this repository. It focuses on patterns, APIs, and idioms that are non-obvious or leverage custom/new features.

## Project goal

Lightweight embedded document database in Rust with a Mongo-like API and a simple LSM-based storage backend.

## Rust style

- Use clear ownership boundaries.
- Avoid unnecessary allocation.
- Prefer small structs with explicit responsibilities.

## Agent behavior

- Preserve existing style and module boundaries.
- Avoid large rewrites unless requested.
- For broad or structural changes, propose the design before editing.
- Add focused tests that match the risk of the change.
- Run formatting after code changes before finishing the task.
- Explain trade-offs, not just implementation steps.

When proposing a structural change, call out future costs explicitly:
- what becomes harder after this change
- which abstractions may be premature
- what assumptions are being baked in

## Tree-shaped IRs: ownership, traversal, and rewriting

- Tree nodes (Expr, ProjectionExpr, LogicalPlan) are Arc-based immutable structures.
  - Implement TreeNode with:
    - children() returning cloned Arcs to child nodes.
    - with_new_children(...) reconstructing the node from provided children.
      - For leaves or unchanged structures, return self to preserve Arc sharing.
      - For keyed children (e.g., BTreeMap), preserve key order and assert child count matches: assert_eq!(old.len(), children.len()).
      - When remapping maps, reuse existing keys (cloned) and zip with new children.
  - For Interval nodes, preserve bound inclusivity/exclusivity when replacing children by matching on existing Bound variants and pulling from the iterator accordingly.

- Transformations over trees are done with generic top-down and bottom-up traversals:
  - Use transform_up(...) and transform_down(...) from TreeNode to implement expression rewrites.
  - When rewriting LogicalPlan, make sure to also rewrite nested Expr within ProjectionExpr (e.g., inside ElemMatch).

- For mutable tree edits that are designed around unique Arc ownership (notably ProjectionExpr add/remove), require unique ownership via Arc::get_mut and panic if the strong count is not 1. Do not generalize this to unrelated Arc-based state.

## Binary serialization protocol (Serializable)

- All IRs implement a custom Serializable trait. Encoding is tag-based and positionally defined:
  - Write a leading u8 “tag”, then variant-specific payload.
  - Use varint methods for integers (indices, lengths, sizes) and length-prefixed slices for raw BSON blobs.
  - Use BTreeMap for children to ensure deterministic, stable serialization order.
  - Option, Arc, Vec, BTreeMap leverage blanket Serializable impls.

- Tags are stable, explicit numbers. Always return InvalidData for malformed serialized input. Do not silently ignore or skip.

- Disallow serializing Expr::Literal. Serialization before parameterization is a logic error and must panic.

- LogicalPlan hashing for caching: compute_hash() serializes to bytes and hashes with murmur_hash64a using a fixed seed (HASH_SEED). Any semantic change requiring cache invalidation should change the serialization or the seed.

## Parameterization and placeholders

- Cached/read plans must be parameterized; direct write-like plans may remain non-parameterized:
  - Replace literals with Expr::Placeholder(u32) and collect actual values into Parameters in creation/normalization phases.
  - Executor binding functions accept only placeholders. Passing non-placeholder expressions is a logic error and must panic.

- Placeholder binding:
  - For key/range scans, bind placeholders to bytes via BsonKey::try_into_key().
  - For document-level filters and projections, bind placeholders to BsonValue(s).

## Logical normalization and boolean algebra

- Negation is structural:
  - Expr::negate() pushes NOT inwards (De Morgan on And/Or, flips ComparisonOperator, toggles Exists/Type/Size).
  - Special-case AlwaysTrue/AlwaysFalse and boolean literals to avoid wrapping with Not.
  - FieldFilters with multiple filters apply De Morgan by distributing and re-wrapping with Or as needed.

## Projection structure and path typing

- ProjectionExpr is a typed tree:
  - Fields node expects only field-name components; ArrayElements node expects only array-index components.
  - Adding/removing uses typed validation; mismatches return Error::InvalidRequest with human-friendly messages (format_path).

- add_expr/remove_expr:
  - Build out the hierarchy by inserting intermediate Fields or ArrayElements nodes as required.
  - Prune empty nodes after removal.
  - For invalid replacements (attempting to insert under terminal nodes), return InvalidRequest.

- children() for leaf nodes returns a lazily initialized, shared empty BTreeMap (static EMPTY) to avoid allocations; this enables generic tree algorithms over heterogeneous nodes.

## BSON value semantics

- BsonValue/BsonValueRef enforce Mongo-like equality, ordering, and hashing:
  - Equality across numeric types (e.g., 5_i32 == 5_i64 == 5.0_f64); NaN compares equal to NaN.
  - Hash is consistent with equality.
  - Provide Into/From conversions for common sources (i32/i64/f64/&str/bool/Bson/Document/Vec/BTreeSet) to streamline literal creation.

- BsonKey trait abstracts “document key” encoding to storage keys (Vec<u8>).

- Macro bson_value! wraps bson::bson! to produce the local BsonValue newtype for consistency.

## Executor conventions

- Execution entry points:
  - execute_direct: permitted only for non-parameterized, write-like plans (e.g., inserts). Returns a one-item iterator of an acknowledgment document.
  - execute_cached: all read/compute plans; must be fully parameterized. Will panic on plans that should have been executed directly.

- _id handling in insert:
  - prepend_id_if_needed inspects BSON bytes via RawDocument and prepends an ObjectId if absent using a custom prepend function. This preserves field ordering and avoids full deserialization.

- Storage scan handling:
  - Ignore Delete tombstones by decoding operation type from the key.
  - Most filtering/projection work still happens after BSON deserialization to Document, though some raw BSON helpers exist. Follow the current codepath rather than assuming full byte-level execution.

- Iterators over results:
  - QueryOutput is Box<dyn Iterator<Item = Result<Document>>>.
  - Plan nodes build pipelines by mapping/filtering over upstream iterators.
  - Limit node composes via skip and take; do not eagerly collect unless absolutely necessary (sorting strategies handle materialization as needed).

## Error and panic strategy

- End-user input issues (e.g., invalid projection paths/specs) return crate::error::Error::InvalidRequest with precise, user-facing messages.

- Internal invariants use assert!/panic! with clear messages:
  - child count mismatches, unexpected operation types, non-parameterized plans in execute_cached, serializing Expr::Literal, attempting mutable access without unique Arc ownership in ProjectionExpr edits.

- For Serializable readers, always return io::Error(InvalidData) for malformed serialized input.

## Path components and formatting

- Paths are vectors of PathComponent (FieldName(String) | ArrayElement(usize)).
  - Ordering and comparisons are well-defined to allow BTreeMap keys.
  - fmt::Display prints raw names/indices; format_path joins with '.' (numeric indices included as-is) to match user-facing path syntax.

- get_path_value(Document, path) traverses strictly: documents by field name, arrays by index; any mismatch yields None (no implicit array/document coercion).

## Module and visibility layout

- Keep the public API surface small. Prefer adding behavior behind existing public entry points (`QuokkaDB`, `Collection`) and keep query/storage/io internals crate-private unless there is a strong reason not to.
- Use pub(crate) liberally to expose only what is needed across crates while keeping implementation details private.

- Test-only helpers live behind #[cfg(test)] modules and can re-export special constructors or DSLs (expr_fn) to keep production code clean.

## Query pipeline

- Preserve the existing flow: parse BSON input into LogicalPlan/Expr, normalize, parameterize, optimize into PhysicalPlan, then execute.

## Concurrency model

- Prefer the existing blocking/threaded model over async abstractions:
  - shared ownership via Arc
  - coordination via Mutex/Condvar/atomics/channels
  - read-mostly snapshot swapping via ArcSwap
  - background work via dedicated threads

## Test style

- Keep tests close to the module they cover. Prefer small semantic tests with concrete BSON documents, exact error messages where relevant, round-trip serialization checks, and focused helper DSLs/utilities.

## Sorting strategies

- Provide multiple sort plans (in-memory, external merge, and top-k heap) all using shared comparison semantics:
  - SortField pairs an Expr path with SortOrder.
  - Use Arc<Vec<SortField>> to pass sort specs through plans.
  - Tests assert identical ordering across strategies; implementation can vary by resource constraints.

## Miscellaneous idioms

- Use BTreeMap/BTreeSet (not HashMap/HashSet) for deterministic ordering in serialization and predictable behavior in tests.

- When a node expects exactly one child (e.g., Expr::All), assert this invariant at construction/wrapping sites.

- Prefer concise helper constructors on types (e.g., SortField::asc/desc) and builder patterns (LogicalPlanBuilder) for readability and composability.
