# quokkadb-cli

`quokkadb-cli` provides the `quokka` command for moving collection data between
an embedded QuokkaDB database and JSONL or CSV files. It is useful for imports,
exports, migrations, backups, and inspecting application data without writing
a separate conversion program.

The CLI opens the database directory directly. Stop the application that owns
the directory before running a command, then start the application again when
the command finishes.

## Install

When using the repository source:

```sh
cargo install --path quokkadb-cli
```

If the package is available from your configured Cargo registry, install the
published binary with:

```sh
cargo install quokkadb-cli
```

## Quick start

Import one Extended JSON document per line into a collection:

```sh
quokka import \
  --db ./data/quokkadb \
  --collection users \
  users.jsonl
```

Export the collection as JSONL:

```sh
quokka export \
  --db ./data/quokkadb \
  --collection users \
  users.jsonl
```

The `--db` option selects the database directory, `--collection` selects the
collection, and the final argument is the input or output path. Use `-` for
standard input or output. JSONL is the default format and is inferred from
`.jsonl` and `.ndjson` paths; `.csv` paths select CSV automatically.

## Formats and limitations

- JSONL uses canonical Extended JSON, preserving BSON values and nested
  documents when data is exported and imported again.
- CSV requires a header row and represents top-level fields. Import cells are
  strings by default; use `--csv-types` or `--infer-types` for scalar values.
- CSV export supports scalar values, ObjectIds, and RFC 3339 datetimes. Arrays,
  embedded documents, and other nested BSON values cannot be exported as CSV.
- CSV export collects the collection in memory to determine its complete
  header, so JSONL is preferable for large or deeply nested collections.
- Use `--batch-size` to control import write batches and `--ignore-errors` to
  continue after malformed or rejected input rows.

See the [import and export guide](https://github.com/blerer/quokkadb/blob/main/docs/guides/import-export.md)
for CSV examples, standard-stream usage, and format details.
