use clap::{Parser, Subcommand, ValueEnum};
use quokkadb::collection::Collection;
use quokkadb::{QuokkaDB, error};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::{collections::BTreeMap, str::FromStr};

#[derive(Debug, Parser)]
#[command(
    name = "quokka",
    version,
    about = "Command-line tools for QuokkaDB",
    after_help = "Input and output:\n  Use - for standard input or standard output.\n\nFormats:\n  --format is inferred from .jsonl, .ndjson, or .csv when possible.\n  JSONL is used when a path has no recognized extension, including -.\n\nExit status:\n  0  Command completed successfully.\n  1  An operational error occurred.\n  2  Command-line usage was invalid."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Import documents into a collection.
    Import(ImportArgs),
    /// Export a collection's documents.
    Export(ExportArgs),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum DataFormat {
    /// One Extended JSON document per line.
    Jsonl,
    /// A flat, header-based comma-separated values file.
    Csv,
}

impl DataFormat {
    fn infer(path: &Path) -> Option<Self> {
        match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
            "jsonl" | "ndjson" => Some(Self::Jsonl),
            "csv" => Some(Self::Csv),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CsvType {
    String,
    Bool,
    Int32,
    Int64,
    Double,
    DateTime,
}

impl FromStr for CsvType {
    type Err = CliError;

    fn from_str(value: &str) -> CliResult<Self> {
        match value {
            "string" => Ok(Self::String),
            "bool" => Ok(Self::Bool),
            "int32" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "double" => Ok(Self::Double),
            "datetime" => Ok(Self::DateTime),
            _ => Err(CliError::new(format!(
                "unsupported CSV type {value:?}; use string, bool, int32, int64, double, or datetime"
            ))),
        }
    }
}

#[derive(Debug, clap::Args)]
struct CollectionArgs {
    /// Path to the database directory.
    #[arg(long, value_name = "PATH")]
    db: PathBuf,

    /// Name of the collection.
    #[arg(long, value_name = "NAME")]
    collection: String,
}

#[derive(Debug, clap::Args)]
struct ImportArgs {
    #[command(flatten)]
    collection: CollectionArgs,

    /// Input format. Inferred from INPUT when omitted.
    #[arg(long, value_enum)]
    format: Option<DataFormat>,

    /// Maximum number of documents written in one database operation.
    #[arg(long, default_value_t = 1_000, value_parser = parse_batch_size)]
    batch_size: usize,

    /// Continue after a malformed or rejected input row and report it on standard error.
    #[arg(long)]
    ignore_errors: bool,

    /// Infer booleans and numeric values in CSV cells. Explicit --csv-types entries take priority.
    #[arg(long)]
    infer_types: bool,

    /// Comma-separated CSV field types, for example age:int32,active:bool.
    #[arg(long, value_name = "FIELD:TYPE,...")]
    csv_types: Option<String>,

    /// Source file, or - for standard input.
    #[arg(value_name = "INPUT")]
    input: PathBuf,
}

impl ImportArgs {
    fn format(&self) -> DataFormat {
        self.format
            .or_else(|| DataFormat::infer(&self.input))
            .unwrap_or(DataFormat::Jsonl)
    }
}

#[derive(Debug, clap::Args)]
struct ExportArgs {
    #[command(flatten)]
    collection: CollectionArgs,

    /// Output format. Inferred from OUTPUT when omitted.
    #[arg(long, value_enum)]
    format: Option<DataFormat>,

    /// Destination file, or - for standard output.
    #[arg(default_value = "-", value_name = "OUTPUT")]
    output: PathBuf,
}

impl ExportArgs {
    fn format(&self) -> DataFormat {
        self.format
            .or_else(|| DataFormat::infer(&self.output))
            .unwrap_or(DataFormat::Jsonl)
    }
}

fn parse_batch_size(value: &str) -> Result<usize, String> {
    let batch_size = value
        .parse::<usize>()
        .map_err(|_| "must be a positive integer".to_string())?;

    if batch_size == 0 {
        return Err("must be greater than zero".to_string());
    }

    Ok(batch_size)
}

fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli) {
        eprintln!("quokka: {error}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> CliResult<()> {
    match cli.command {
        Commands::Import(args) => import(args),
        Commands::Export(args) => export(args),
    }
}

#[derive(Debug)]
struct CliError(String);

impl CliError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for CliError {}

impl From<io::Error> for CliError {
    fn from(error: io::Error) -> Self {
        Self::new(error.to_string())
    }
}

type CliResult<T> = Result<T, CliError>;

#[derive(Debug, Default, PartialEq, Eq)]
struct ImportSummary {
    imported: u64,
    rejected: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ExportSummary {
    exported: u64,
}

struct PendingDocument {
    row: InputRow,
    document: bson::Document,
}

#[derive(Clone, Copy)]
enum InputRow {
    Jsonl(u64),
    Csv(u64),
}

impl fmt::Display for InputRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Jsonl(number) => write!(formatter, "line {number}"),
            Self::Csv(number) => write!(formatter, "CSV row {number}"),
        }
    }
}

fn import(args: ImportArgs) -> CliResult<()> {
    let db = open_import_database(&args.collection.db)?;
    let collection = db
        .collection(&args.collection.collection)
        .create_if_missing();
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    let input = open_input(&args.input)?;
    let summary = {
        let mut batcher = ImportBatcher::new(
            &collection,
            args.batch_size,
            args.ignore_errors,
            &mut stderr,
        );

        match args.format() {
            DataFormat::Jsonl => import_jsonl_from_reader(input, &mut batcher)?,
            DataFormat::Csv => import_csv_from_reader(input, &args, &mut batcher)?,
        }
        batcher.finish()?
    };

    write_import_summary(&mut stderr, &summary)?;
    Ok(())
}

fn import_csv_from_reader<R: io::Read, W: Write>(
    reader: R,
    args: &ImportArgs,
    batcher: &mut ImportBatcher<'_, '_, W>,
) -> CliResult<()> {
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    let headers = csv
        .headers()
        .map_err(|error| CliError::new(format!("could not read CSV headers: {error}")))?
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    validate_csv_headers(&headers)?;
    let types = parse_csv_types(args.csv_types.as_deref(), &headers)?;

    for (index, record) in csv.into_records().enumerate() {
        let row = InputRow::Csv(index as u64 + 2);
        let document = record
            .map_err(|error| CliError::new(error.to_string()))
            .and_then(|record| csv_record_to_document(&headers, &record, &types, args.infer_types));
        handle_row(row, document, batcher)?;
    }

    Ok(())
}

fn validate_csv_headers(headers: &[String]) -> CliResult<()> {
    let mut seen = std::collections::BTreeSet::new();
    for header in headers {
        if header.is_empty() {
            return Err(CliError::new("CSV header names must not be empty"));
        }
        if !seen.insert(header) {
            return Err(CliError::new(format!(
                "CSV header {header:?} appears more than once"
            )));
        }
    }
    Ok(())
}

fn parse_csv_types(
    specification: Option<&str>,
    headers: &[String],
) -> CliResult<BTreeMap<String, CsvType>> {
    let Some(specification) = specification else {
        return Ok(BTreeMap::new());
    };

    let mut types = BTreeMap::new();
    for entry in specification.split(',') {
        let (field, type_name) = entry.split_once(':').ok_or_else(|| {
            CliError::new(format!("invalid CSV type {entry:?}; expected field:type"))
        })?;
        if field.is_empty() || type_name.is_empty() {
            return Err(CliError::new(format!(
                "invalid CSV type {entry:?}; expected field:type"
            )));
        }
        if !headers.iter().any(|header| header == field) {
            return Err(CliError::new(format!(
                "CSV type specifies unknown field {field:?}"
            )));
        }
        if types.insert(field.to_owned(), type_name.parse()?).is_some() {
            return Err(CliError::new(format!(
                "CSV type specifies field {field:?} more than once"
            )));
        }
    }
    Ok(types)
}

fn csv_record_to_document(
    headers: &[String],
    record: &csv::StringRecord,
    types: &BTreeMap<String, CsvType>,
    infer_types: bool,
) -> CliResult<bson::Document> {
    let mut document = bson::Document::new();
    for (header, cell) in headers.iter().zip(record.iter()) {
        if cell.is_empty() {
            continue;
        }

        let value = match types.get(header) {
            Some(value_type) => parse_csv_cell(cell, *value_type)?,
            None if infer_types => infer_csv_cell(cell),
            None => bson::Bson::String(cell.to_owned()),
        };
        document.insert(header, value);
    }
    Ok(document)
}

fn parse_csv_cell(cell: &str, value_type: CsvType) -> CliResult<bson::Bson> {
    match value_type {
        CsvType::String => Ok(bson::Bson::String(cell.to_owned())),
        CsvType::Bool => cell
            .parse::<bool>()
            .map(bson::Bson::Boolean)
            .map_err(|_| CliError::new(format!("expected bool, got {cell:?}"))),
        CsvType::Int32 => cell
            .parse::<i32>()
            .map(bson::Bson::Int32)
            .map_err(|_| CliError::new(format!("expected int32, got {cell:?}"))),
        CsvType::Int64 => cell
            .parse::<i64>()
            .map(bson::Bson::Int64)
            .map_err(|_| CliError::new(format!("expected int64, got {cell:?}"))),
        CsvType::Double => cell
            .parse::<f64>()
            .map(bson::Bson::Double)
            .map_err(|_| CliError::new(format!("expected double, got {cell:?}"))),
        CsvType::DateTime => bson::DateTime::parse_rfc3339_str(cell)
            .map(bson::Bson::DateTime)
            .map_err(|_| CliError::new(format!("expected RFC 3339 datetime, got {cell:?}"))),
    }
}

fn infer_csv_cell(cell: &str) -> bson::Bson {
    if let Ok(value) = cell.parse::<bool>() {
        return bson::Bson::Boolean(value);
    }
    if let Ok(value) = cell.parse::<i32>() {
        return bson::Bson::Int32(value);
    }
    if let Ok(value) = cell.parse::<i64>() {
        return bson::Bson::Int64(value);
    }
    if let Ok(value) = cell.parse::<f64>() {
        return bson::Bson::Double(value);
    }
    bson::Bson::String(cell.to_owned())
}

fn import_jsonl_from_reader<R: BufRead, W: Write>(
    reader: R,
    batcher: &mut ImportBatcher<'_, '_, W>,
) -> CliResult<()> {
    for (index, line) in reader.lines().enumerate() {
        let line_number = index as u64 + 1;
        let line = line.map_err(|error| {
            CliError::new(format!("could not read line {line_number}: {error}"))
        })?;
        if line.trim().is_empty() {
            continue;
        }

        handle_row(
            InputRow::Jsonl(line_number),
            parse_jsonl_document(&line),
            batcher,
        )?;
    }

    Ok(())
}

fn parse_jsonl_document(line: &str) -> CliResult<bson::Document> {
    let value: serde_json::Value = serde_json::from_str(line)
        .map_err(|error| CliError::new(format!("invalid JSON: {error}")))?;
    let serde_json::Value::Object(document) = value else {
        return Err(CliError::new("a JSONL row must be a JSON object"));
    };

    bson::Document::try_from(document)
        .map_err(|error| CliError::new(format!("invalid Extended JSON: {error}")))
}

fn handle_row<W: Write>(
    row: InputRow,
    result: CliResult<bson::Document>,
    batcher: &mut ImportBatcher<'_, '_, W>,
) -> CliResult<()> {
    match result {
        Ok(document) => batcher.push(row, document),
        Err(error) => batcher.reject(row, error.to_string()),
    }
}

struct ImportBatcher<'collection, 'errors, W: Write> {
    collection: &'collection Collection,
    batch_size: usize,
    ignore_errors: bool,
    errors: &'errors mut W,
    batch: Vec<PendingDocument>,
    summary: ImportSummary,
}

impl<'collection, 'errors, W: Write> ImportBatcher<'collection, 'errors, W> {
    fn new(
        collection: &'collection Collection,
        batch_size: usize,
        ignore_errors: bool,
        errors: &'errors mut W,
    ) -> Self {
        Self {
            collection,
            batch_size,
            ignore_errors,
            errors,
            batch: Vec::with_capacity(batch_size),
            summary: ImportSummary::default(),
        }
    }

    fn push(&mut self, row: InputRow, document: bson::Document) -> CliResult<()> {
        self.batch.push(PendingDocument { row, document });
        if self.batch.len() == self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn reject(&mut self, row: InputRow, message: String) -> CliResult<()> {
        if !self.ignore_errors {
            return Err(CliError::new(format!("{row}: {message}")));
        }

        report_document_error(self.errors, row, &message)?;
        self.summary.rejected += 1;
        Ok(())
    }

    fn finish(mut self) -> CliResult<ImportSummary> {
        self.flush()?;
        Ok(self.summary)
    }

    fn flush(&mut self) -> CliResult<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        match self
            .collection
            .insert_many(self.batch.iter().map(|pending| &pending.document))
        {
            Ok(result) => {
                self.summary.imported += result.inserted_ids.len() as u64;
                self.batch.clear();
                Ok(())
            }
            Err(_error) if self.ignore_errors => {
                for pending in self.batch.drain(..) {
                    match self.collection.insert_one(&pending.document) {
                        Ok(_) => self.summary.imported += 1,
                        Err(error) => {
                            report_document_error(self.errors, pending.row, &error.to_string())?;
                            self.summary.rejected += 1;
                        }
                    }
                }
                Ok(())
            }
            Err(error) => {
                let first_row = self.batch.first().expect("batch is not empty").row;
                let last_row = self.batch.last().expect("batch is not empty").row;
                Err(CliError::new(format!(
                    "could not import documents from {first_row} through {last_row}: {error}"
                )))
            }
        }
    }
}

fn report_document_error<W: Write>(errors: &mut W, row: InputRow, message: &str) -> CliResult<()> {
    writeln!(errors, "quokka: {row}: {message}")?;
    Ok(())
}

fn write_import_summary<W: Write>(writer: &mut W, summary: &ImportSummary) -> CliResult<()> {
    if summary.rejected == 0 {
        writeln!(writer, "quokka: imported {} document(s)", summary.imported)?;
    } else {
        writeln!(
            writer,
            "quokka: imported {} document(s); rejected {}",
            summary.imported, summary.rejected
        )?;
    }
    Ok(())
}

fn export(args: ExportArgs) -> CliResult<()> {
    let db = open_database(&args.collection.db)?;
    let collection = db.collection(&args.collection.collection);
    let format = args.format();
    let output = open_output(&args.output)?;
    let summary = match format {
        DataFormat::Jsonl => export_jsonl_to_writer(&collection, output)?,
        DataFormat::Csv => export_csv_to_writer(&collection, output)?,
    };

    eprintln!("quokka: exported {} document(s)", summary.exported);
    Ok(())
}

fn export_jsonl_to_writer<W: Write>(
    collection: &Collection,
    mut writer: W,
) -> CliResult<ExportSummary> {
    let output = collection
        .find(bson::Document::new())
        .execute()
        .map_err(quokkadb_error)?;
    let mut summary = ExportSummary::default();

    for document in output {
        let document = document.map_err(quokkadb_error)?;
        let value = bson::Bson::Document(document).into_canonical_extjson();
        serde_json::to_writer(&mut writer, &value)
            .map_err(|error| CliError::new(format!("could not write JSONL output: {error}")))?;
        writer.write_all(b"\n")?;
        summary.exported += 1;
    }

    writer.flush()?;
    Ok(summary)
}

fn open_output(path: &Path) -> CliResult<Box<dyn Write>> {
    if is_standard_stream(path) {
        return Ok(Box::new(io::stdout()));
    }

    let output = File::create(path)
        .map_err(|error| CliError::new(format!("could not create {}: {error}", path.display())))?;
    Ok(Box::new(output))
}

fn export_csv_to_writer<W: Write>(collection: &Collection, writer: W) -> CliResult<ExportSummary> {
    let documents = collection
        .find(bson::Document::new())
        .execute_collect()
        .map_err(quokkadb_error)?;
    let headers = documents
        .iter()
        .flat_map(|document| document.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let mut csv = csv::Writer::from_writer(writer);
    if !headers.is_empty() {
        csv.write_record(&headers)
            .map_err(|error| CliError::new(format!("could not write CSV header: {error}")))?;
    }

    for document in &documents {
        let record = headers
            .iter()
            .map(|header| match document.get(header) {
                Some(value) => csv_cell(value),
                None => Ok(String::new()),
            })
            .collect::<CliResult<Vec<_>>>()?;
        csv.write_record(&record)
            .map_err(|error| CliError::new(format!("could not write CSV row: {error}")))?;
    }
    csv.flush()
        .map_err(|error| CliError::new(format!("could not write CSV output: {error}")))?;

    Ok(ExportSummary {
        exported: documents.len() as u64,
    })
}

fn csv_cell(value: &bson::Bson) -> CliResult<String> {
    match value {
        bson::Bson::String(value)
        | bson::Bson::Symbol(value)
        | bson::Bson::JavaScriptCode(value) => Ok(value.clone()),
        bson::Bson::Boolean(value) => Ok(value.to_string()),
        bson::Bson::Int32(value) => Ok(value.to_string()),
        bson::Bson::Int64(value) => Ok(value.to_string()),
        bson::Bson::Double(value) => Ok(value.to_string()),
        bson::Bson::Decimal128(value) => Ok(value.to_string()),
        bson::Bson::ObjectId(value) => Ok(value.to_hex()),
        bson::Bson::DateTime(value) => value
            .try_to_rfc3339_string()
            .map_err(|error| CliError::new(format!("could not format datetime: {error}"))),
        bson::Bson::Null | bson::Bson::Undefined => Ok(String::new()),
        _ => Err(CliError::new(format!(
            "CSV export does not support {:?} values",
            value.element_type()
        ))),
    }
}

fn open_database(path: &Path) -> CliResult<QuokkaDB> {
    QuokkaDB::open(path).map_err(quokkadb_error)
}

fn open_import_database(path: &Path) -> CliResult<QuokkaDB> {
    fs::create_dir_all(path).map_err(|error| {
        CliError::new(format!(
            "could not create database directory {}: {error}",
            path.display()
        ))
    })?;
    open_database(path)
}

fn open_input(path: &Path) -> CliResult<Box<dyn BufRead>> {
    if is_standard_stream(path) {
        return Ok(Box::new(BufReader::new(io::stdin())));
    }

    let input = File::open(path)
        .map_err(|error| CliError::new(format!("could not open {}: {error}", path.display())))?;
    Ok(Box::new(BufReader::new(input)))
}

fn quokkadb_error(error: error::Error) -> CliError {
    CliError::new(error.to_string())
}

fn is_standard_stream(path: &Path) -> bool {
    path == Path::new("-")
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use std::io::Cursor;
    use tempfile::tempdir;

    fn import_args(database: PathBuf, batch_size: usize, ignore_errors: bool) -> ImportArgs {
        ImportArgs {
            collection: CollectionArgs {
                db: database,
                collection: "users".to_string(),
            },
            format: Some(DataFormat::Jsonl),
            batch_size,
            ignore_errors,
            infer_types: false,
            csv_types: None,
            input: PathBuf::from("users.jsonl"),
        }
    }

    fn import_jsonl_for_test<R: BufRead, W: Write>(
        reader: R,
        collection: &Collection,
        args: &ImportArgs,
        errors: &mut W,
    ) -> CliResult<ImportSummary> {
        let mut batcher =
            ImportBatcher::new(collection, args.batch_size, args.ignore_errors, errors);
        import_jsonl_from_reader(reader, &mut batcher)?;
        batcher.finish()
    }

    fn import_csv_for_test<R: io::Read, W: Write>(
        reader: R,
        collection: &Collection,
        args: &ImportArgs,
        errors: &mut W,
    ) -> CliResult<ImportSummary> {
        let mut batcher =
            ImportBatcher::new(collection, args.batch_size, args.ignore_errors, errors);
        import_csv_from_reader(reader, args, &mut batcher)?;
        batcher.finish()
    }

    #[test]
    fn command_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn import_accepts_database_collection_and_input() {
        let cli = Cli::try_parse_from([
            "quokka",
            "import",
            "--db",
            "./data/quokka.db",
            "--collection",
            "users",
            "users.jsonl",
        ])
        .unwrap();

        let Commands::Import(args) = cli.command else {
            panic!("expected import command");
        };
        assert_eq!(args.collection.db, PathBuf::from("./data/quokka.db"));
        assert_eq!(args.collection.collection, "users");
        assert_eq!(args.input, PathBuf::from("users.jsonl"));
        assert_eq!(args.format(), DataFormat::Jsonl);
        assert_eq!(args.batch_size, 1_000);
        assert!(!args.ignore_errors);
    }

    #[test]
    fn import_accepts_csv_options_and_standard_input() {
        let cli = Cli::try_parse_from([
            "quokka",
            "import",
            "--db",
            "./data/quokka.db",
            "--collection",
            "users",
            "--format",
            "csv",
            "--batch-size",
            "500",
            "--ignore-errors",
            "--infer-types",
            "--csv-types",
            "age:int32,active:bool",
            "-",
        ])
        .unwrap();

        let Commands::Import(args) = cli.command else {
            panic!("expected import command");
        };
        assert_eq!(args.format(), DataFormat::Csv);
        assert_eq!(args.batch_size, 500);
        assert!(args.ignore_errors);
        assert!(args.infer_types);
        assert_eq!(args.csv_types.as_deref(), Some("age:int32,active:bool"));
        assert_eq!(args.input, PathBuf::from("-"));
    }

    #[test]
    fn import_rejects_a_zero_batch_size() {
        let result = Cli::try_parse_from([
            "quokka",
            "import",
            "--db",
            "./data/quokka.db",
            "--collection",
            "users",
            "--batch-size",
            "0",
            "users.jsonl",
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn export_accepts_database_collection_and_defaults_to_standard_output() {
        let cli = Cli::try_parse_from([
            "quokka",
            "export",
            "--db",
            "./data/quokka.db",
            "--collection",
            "users",
        ])
        .unwrap();

        let Commands::Export(args) = cli.command else {
            panic!("expected export command");
        };
        assert_eq!(args.collection.db, PathBuf::from("./data/quokka.db"));
        assert_eq!(args.collection.collection, "users");
        assert_eq!(args.output, PathBuf::from("-"));
        assert_eq!(args.format(), DataFormat::Jsonl);
    }

    #[test]
    fn export_infers_csv_format_from_its_output_path() {
        let cli = Cli::try_parse_from([
            "quokka",
            "export",
            "--db",
            "./data/quokka.db",
            "--collection",
            "users",
            "users.csv",
        ])
        .unwrap();

        let Commands::Export(args) = cli.command else {
            panic!("expected export command");
        };
        assert_eq!(args.output, PathBuf::from("users.csv"));
        assert_eq!(args.format(), DataFormat::Csv);
    }

    #[test]
    fn jsonl_import_batches_documents_and_exports_canonical_extended_json() {
        let directory = tempdir().unwrap();
        let database = QuokkaDB::open(directory.path()).unwrap();
        let collection = database.collection("users").create_if_missing();
        let args = import_args(directory.path().to_path_buf(), 2, false);
        let mut errors = Vec::new();

        let summary = import_jsonl_for_test(
            Cursor::new(
                "{\"_id\":{\"$numberLong\":\"9\"},\"name\":\"Ada\"}\n\n{\"_id\":{\"$numberLong\":\"10\"},\"name\":\"Grace\"}\n",
            ),
            &collection,
            &args,
            &mut errors,
        )
        .unwrap();

        assert_eq!(
            summary,
            ImportSummary {
                imported: 2,
                rejected: 0
            }
        );
        assert!(errors.is_empty());

        let mut output = Vec::new();
        let summary = export_jsonl_to_writer(&collection, &mut output).unwrap();

        assert_eq!(summary, ExportSummary { exported: 2 });
        let lines = String::from_utf8(output).unwrap();
        assert!(lines.contains("\"$numberLong\":\"9\""));
        assert!(lines.contains("\"$numberLong\":\"10\""));
        assert!(lines.contains("\"name\":\"Ada\""));
        assert!(lines.contains("\"name\":\"Grace\""));
    }

    #[test]
    fn jsonl_import_ignores_invalid_and_rejected_rows() {
        let directory = tempdir().unwrap();
        let database = QuokkaDB::open(directory.path()).unwrap();
        let collection = database.collection("users").create_if_missing();
        collection
            .insert_one(bson::doc! { "_id": 2, "name": "existing" })
            .unwrap();
        let args = import_args(directory.path().to_path_buf(), 2, true);
        let mut errors = Vec::new();

        let summary = import_jsonl_for_test(
            Cursor::new(
                "{\"_id\":1,\"name\":\"Ada\"}\nnot json\n{\"_id\":2,\"name\":\"duplicate\"}\n",
            ),
            &collection,
            &args,
            &mut errors,
        )
        .unwrap();

        assert_eq!(
            summary,
            ImportSummary {
                imported: 1,
                rejected: 2
            }
        );
        let errors = String::from_utf8(errors).unwrap();
        assert!(errors.contains("line 2: invalid JSON"));
        assert!(errors.contains("line 3:"));
        assert_eq!(collection.estimated_document_count().unwrap(), 2);
    }

    #[test]
    fn jsonl_commands_import_and_export_files() {
        let directory = tempdir().unwrap();
        let database = directory.path().join("database");
        let input = directory.path().join("users.jsonl");
        let output = directory.path().join("export.jsonl");
        fs::write(
            &input,
            "{\"_id\":{\"$numberLong\":\"42\"},\"name\":\"Ada\"}\n",
        )
        .unwrap();

        import(ImportArgs {
            input,
            ..import_args(database.clone(), 1_000, false)
        })
        .unwrap();
        export(ExportArgs {
            collection: CollectionArgs {
                db: database,
                collection: "users".to_string(),
            },
            format: Some(DataFormat::Jsonl),
            output: output.clone(),
        })
        .unwrap();

        let exported = fs::read_to_string(output).unwrap();
        assert_eq!(
            exported,
            "{\"_id\":{\"$numberLong\":\"42\"},\"name\":\"Ada\"}\n"
        );
    }

    #[test]
    fn csv_export_writes_sorted_headers_and_scalar_values() {
        let directory = tempdir().unwrap();
        let database = QuokkaDB::open(directory.path()).unwrap();
        let collection = database.collection("users").create_if_missing();
        let joined = bson::DateTime::parse_rfc3339_str("2024-01-02T03:04:05Z").unwrap();
        collection
            .insert_many([
                &bson::doc! { "_id": 1, "name": "Ada, Lovelace", "age": 36, "active": true },
                &bson::doc! { "_id": 2, "name": "Grace", "joined": joined },
            ])
            .unwrap();

        let mut output = Vec::new();
        let summary = export_csv_to_writer(&collection, &mut output).unwrap();

        assert_eq!(summary, ExportSummary { exported: 2 });
        let mut csv = csv::Reader::from_reader(output.as_slice());
        assert_eq!(
            csv.headers().unwrap(),
            &csv::StringRecord::from(vec!["_id", "active", "age", "joined", "name"])
        );
        let records = csv.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records[0].get(0), Some("1"));
        assert_eq!(records[0].get(1), Some("true"));
        assert_eq!(records[0].get(2), Some("36"));
        assert_eq!(records[0].get(3), Some(""));
        assert_eq!(records[0].get(4), Some("Ada, Lovelace"));
        assert_eq!(records[1].get(0), Some("2"));
        assert_eq!(records[1].get(1), Some(""));
        assert_eq!(records[1].get(2), Some(""));
        assert_eq!(records[1].get(4), Some("Grace"));
        assert_eq!(
            bson::DateTime::parse_rfc3339_str(records[1].get(3).unwrap()).unwrap(),
            joined
        );
    }

    #[test]
    fn csv_export_rejects_nested_values() {
        let directory = tempdir().unwrap();
        let database = QuokkaDB::open(directory.path()).unwrap();
        let collection = database.collection("users").create_if_missing();
        collection
            .insert_one(bson::doc! { "_id": 1, "tags": ["rust"] })
            .unwrap();

        let error = export_csv_to_writer(&collection, Vec::new()).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("CSV export does not support Array values")
        );
    }

    #[test]
    fn csv_import_supports_quoted_cells_and_explicit_types() {
        let directory = tempdir().unwrap();
        let database = QuokkaDB::open(directory.path()).unwrap();
        let collection = database.collection("users").create_if_missing();
        let args = ImportArgs {
            format: Some(DataFormat::Csv),
            csv_types: Some("age:int32,active:bool,joined:datetime".to_string()),
            ..import_args(directory.path().to_path_buf(), 1_000, false)
        };
        let mut errors = Vec::new();

        let summary = import_csv_for_test(
            Cursor::new(
                "name,age,active,joined,nickname\n\"Ada, Lovelace\",36,true,2024-01-02T03:04:05Z,\n",
            ),
            &collection,
            &args,
            &mut errors,
        )
        .unwrap();

        assert_eq!(
            summary,
            ImportSummary {
                imported: 1,
                rejected: 0
            }
        );
        assert!(errors.is_empty());
        let document = collection
            .find(bson::Document::new())
            .execute_collect()
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(document.get_str("name").unwrap(), "Ada, Lovelace");
        assert!(matches!(document.get("age"), Some(bson::Bson::Int32(36))));
        assert!(matches!(
            document.get("active"),
            Some(bson::Bson::Boolean(true))
        ));
        assert!(matches!(
            document.get("joined"),
            Some(bson::Bson::DateTime(_))
        ));
        assert!(!document.contains_key("nickname"));
    }

    #[test]
    fn csv_import_infers_scalar_types_and_ignores_invalid_typed_rows() {
        let directory = tempdir().unwrap();
        let database = QuokkaDB::open(directory.path()).unwrap();
        let collection = database.collection("users").create_if_missing();
        let args = ImportArgs {
            format: Some(DataFormat::Csv),
            infer_types: true,
            csv_types: Some("age:int32".to_string()),
            ..import_args(directory.path().to_path_buf(), 2, true)
        };
        let mut errors = Vec::new();

        let summary = import_csv_for_test(
            Cursor::new("name,age,active,score\nAda,not-a-number,true,3.5\nGrace,42,false,7\n"),
            &collection,
            &args,
            &mut errors,
        )
        .unwrap();

        assert_eq!(
            summary,
            ImportSummary {
                imported: 1,
                rejected: 1
            }
        );
        assert!(
            String::from_utf8(errors)
                .unwrap()
                .contains("CSV row 2: expected int32")
        );
        let document = collection
            .find(bson::Document::new())
            .execute_collect()
            .unwrap();
        assert_eq!(document.len(), 1);
        let document = &document[0];
        assert!(matches!(document.get("age"), Some(bson::Bson::Int32(42))));
        assert!(matches!(
            document.get("active"),
            Some(bson::Bson::Boolean(false))
        ));
        assert!(matches!(document.get("score"), Some(bson::Bson::Int32(7))));
    }

    #[test]
    fn csv_types_reject_unknown_headers() {
        let error = parse_csv_types(Some("missing:int32"), &["name".to_string()]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "CSV type specifies unknown field \"missing\""
        );
    }
}
