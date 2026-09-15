# Import and export data

Use the `quokka` command-line tool to move a collection between JSONL or CSV files and a QuokkaDB database directory.

Related: [Operations — Own the database directory](../operations.md#own-the-database-directory) · [Manage collections](manage-collections.md).

## Stop the application first

The CLI opens the database directory directly. Stop the application that owns the directory before running `quokka`, then start it again after the command completes. One process must be the database directory owner at a time.

## Install the CLI

From the QuokkaDB workspace, install the `quokka` executable with Cargo:

```sh
cargo install --path quokkadb-cli
```

Cargo installs `quokka` into its binary directory, which must be on your `PATH`. While developing QuokkaDB, run the workspace binary without installing it:

```sh
cargo run -p quokkadb-cli -- import --help
```

## Import JSONL

JSONL contains one JSON document per line. Import a file into a collection with its database directory and collection name.

```sh
quokka import \
  --db ./data/quokkadb \
  --collection users \
  users.jsonl
```

Use Extended JSON for BSON values that ordinary JSON cannot represent. For example, this preserves a 64-bit integer ID:

```json
{"_id":{"$numberLong":"42"},"name":"Ada"}
{"_id":{"$numberLong":"43"},"name":"Grace"}
```

The command creates the database directory and collection when they do not yet exist. It reports malformed or rejected rows on standard error. Add `--ignore-errors` to continue after those rows, and use `--batch-size` to choose how many documents each write operation contains.

```sh
quokka import --db ./data/quokkadb --collection users \
  --batch-size 500 --ignore-errors users.jsonl
```

## Export JSONL

Export the collection as JSONL. With no output path, JSONL is written to standard output.

```sh
quokka export --db ./data/quokkadb --collection users > users.jsonl
```

JSONL export writes canonical Extended JSON, so BSON types are preserved when the file is imported again.

## Import CSV

CSV files require a header row. Each header becomes a top-level document field. Empty cells do not create a field.

```csv
name,age,active
Ada,36,true
Grace,29,false
```

By default, nonempty CSV cells are strings. Use `--csv-types` for known columns or `--infer-types` for booleans and numbers.

```sh
quokka import \
  --db ./data/quokkadb \
  --collection users \
  --format csv \
  --csv-types age:int32,active:bool \
  users.csv
```

Supported explicit types are `string`, `bool`, `int32`, `int64`, `double`, and `datetime`. Datetimes use RFC 3339 format.

## Export CSV

Choose CSV explicitly or use a `.csv` output path.

```sh
quokka export --db ./data/quokkadb --collection users users.csv
```

CSV export creates a sorted header from all top-level fields in the collection. Missing and null fields become empty cells. It supports scalar values, ObjectIds, and RFC 3339 datetimes. Arrays, embedded documents, and other nested BSON values cannot be represented as CSV cells and cause export to fail.

CSV needs its complete header before it can write records, so CSV export collects the collection in memory. Prefer JSONL for large collections or when preserving nested BSON values.

## Standard input and output

Use `-` to read from standard input or write to standard output. JSONL is the default format for `-`; specify `--format csv` when piping CSV.

```sh
cat users.csv | quokka import --db ./data/quokkadb --collection users --format csv -
quokka export --db ./data/quokkadb --collection users --format csv - > users.csv
```

## Next

- [Manage collections](manage-collections.md)
- [Operations](../operations.md)
- [Concepts](../concepts.md)
