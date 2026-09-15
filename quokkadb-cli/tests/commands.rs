use std::fs;
use std::process::Command;
use tempfile::tempdir;

fn quokka() -> Command {
    Command::new(env!("CARGO_BIN_EXE_quokka"))
}

#[test]
fn imports_and_exports_jsonl_files() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("database");
    let input = directory.path().join("users.jsonl");
    let output = directory.path().join("users-export.jsonl");
    fs::write(
        &input,
        "{\"_id\":{\"$numberLong\":\"42\"},\"name\":\"Ada\"}\n",
    )
    .unwrap();

    let import = quokka()
        .args(["import", "--db"])
        .arg(&database)
        .args(["--collection", "users"])
        .arg(&input)
        .output()
        .unwrap();
    assert!(import.status.success(), "{import:?}");
    assert!(String::from_utf8_lossy(&import.stderr).contains("imported 1 document"));

    let export = quokka()
        .args(["export", "--db"])
        .arg(&database)
        .args(["--collection", "users"])
        .arg(&output)
        .output()
        .unwrap();
    assert!(export.status.success(), "{export:?}");
    assert_eq!(
        fs::read_to_string(output).unwrap(),
        "{\"_id\":{\"$numberLong\":\"42\"},\"name\":\"Ada\"}\n"
    );
}

#[test]
fn imports_and_exports_csv_files() {
    let directory = tempdir().unwrap();
    let database = directory.path().join("database");
    let input = directory.path().join("users.csv");
    let output = directory.path().join("users-export.csv");
    fs::write(&input, "_id,name,age\n1,\"Ada, Lovelace\",36\n").unwrap();

    let import = quokka()
        .args(["import", "--db"])
        .arg(&database)
        .args([
            "--collection",
            "users",
            "--format",
            "csv",
            "--csv-types",
            "_id:int32,age:int32",
        ])
        .arg(&input)
        .output()
        .unwrap();
    assert!(import.status.success(), "{import:?}");

    let export = quokka()
        .args(["export", "--db"])
        .arg(&database)
        .args(["--collection", "users"])
        .arg(&output)
        .output()
        .unwrap();
    assert!(export.status.success(), "{export:?}");

    let mut csv = csv::Reader::from_path(output).unwrap();
    assert_eq!(
        csv.headers().unwrap(),
        &csv::StringRecord::from(vec!["_id", "age", "name"])
    );
    let record = csv.records().next().unwrap().unwrap();
    assert_eq!(record.get(0), Some("1"));
    assert_eq!(record.get(1), Some("36"));
    assert_eq!(record.get(2), Some("Ada, Lovelace"));
}
