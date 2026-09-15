# Instructions for agents modifying QuokkaDB documentation

This file governs documentation changes. It is not user-facing documentation.

## Product framing

Present QuokkaDB as an **application database**: it persists Rust application models without a separate database server.

Emphasize embedded operation, typed models, document queries and updates, indexes, and durable storage. Do not primarily position QuokkaDB as an LSM tree, BSON store, key-value store, MongoDB replacement, ORM, or ODM.

## Writing rules

- Start from the user's goal, not an internal abstraction.
- Use the order: **goal → minimal example → explanation → details and limitations**.
- Keep sentences, paragraphs, and examples short.
- Prefer ordinary database terminology and concrete behavior over marketing language.
- Introduce storage internals only when they help explain user-visible behavior.
- State current limitations directly. Never present planned or internal functionality as supported.

## APIs and examples

- Teach application tasks through the **typed API** first.
- Show the **document API** only when it adds capability, materially changes the syntax, handles dynamic data, or is the subject of the page.
- Use realistic, task-oriented examples.
- Explain the common case before options and edge cases.

## Accuracy

- Verify examples and capability claims against the public API and tests.
- Distinguish guarantees from typical behavior and durable modes from weaker modes.
- Distinguish typed API support from document API support. A raw BSON escape hatch does not make a capability natively supported by the typed API.
- Call out behavior that differs from likely MongoDB or Rust expectations.

## Page responsibilities

- `README.md` and `docs/README.md`: explain what QuokkaDB is and direct readers to the right page.
- `docs/getting-started.md`: provide the shortest path to a working persistent application.
- `docs/features.md`: authoritative source for supported, partial, and unsupported capabilities.
- `docs/guides/`: typed-first instructions for completing tasks.
- `docs/concepts.md`: semantics and guarantees.
- `docs/operations.md`: durability, recovery, observability, tuning, and operational constraints.
- `docs/api-reference.md` and Rustdoc: exact APIs and options.

Link to the authoritative page instead of repeating its content. Add contextual links when readers are likely to need a related capability, concept, or constraint.

## Terminology

Use these terms consistently: **application database**, **embedded**, **model** or **Rust type**, **document**, **typed API**, **document API**, and **collection**.

## Final check

Before finishing a documentation change, verify that:

- the simplest useful example appears early;
- links and Markdown render correctly;
- limitations are easy to find;
- duplicated explanations have been removed;
- the page remains useful when read on its own.
