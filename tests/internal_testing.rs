#![cfg(feature = "internal-testing")]

mod common;

use tempfile::tempdir;

#[test]
fn test_control_can_drive_flush_and_compaction() {
    let dir = tempdir().unwrap();
    let db = common::open_db(dir.path());
    let control = db.test_control();

    db.create_collection("users").unwrap();
    let users = db.collection("users");

    control.disable_auto_compaction();

    users
        .insert_one(bson::doc! { "_id": 1, "name": "Ada" })
        .unwrap();
    control.flush().unwrap();
    control.compact().unwrap();

    let doc = users.find_one(bson::doc! { "_id": 1 }).unwrap().unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Ada");
}
