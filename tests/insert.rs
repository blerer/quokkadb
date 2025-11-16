use bson::doc;
use quokkadb::{error::Error, QuokkaDB};
use tempfile::tempdir;

fn setup() -> (tempfile::TempDir, QuokkaDB) {
    let dir = tempdir().unwrap();
    let db = QuokkaDB::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn insert_duplicate_id_is_version_conflict() {
    let (_dir, db) = setup();
    let collection = db.collection("test");

    let doc = doc! {"_id": 365, "x": 1};

    // First insert should succeed.
    collection.insert_one(doc.clone()).unwrap();

    // Second insert with the same _id should fail with VersionConflict.
    let result = collection.insert_one(doc);
    let error = result.err().unwrap();
    assert!(matches!(error, Error::InvalidRequest(_)));
    assert_eq!(
        format!("{}", error),
        "Duplicate key error. dup key: { _id: 365 }"
    );
}
