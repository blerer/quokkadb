use bson::{doc, Document};
use quokkadb::QuokkaDB;
use std::path::Path;
use tempfile::tempdir;

fn get_db(path: &Path) -> QuokkaDB {
    QuokkaDB::open(path).unwrap()
}

#[test]
fn test_restart_with_multiple_collections() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path();

    {
        let db = get_db(db_path);
        let collection1 = db.collection("collection1");
        let collection2 = db.collection("collection2");

        collection1
            .insert_many(vec![
                doc! { "name": "Alice", "age": 30 },
                doc! { "name": "Bob", "age": 25 },
            ])
            .unwrap();

        collection2
            .insert_many(vec![
                doc! { "city": "New York", "population": 8_400_000 },
                doc! { "city": "London", "population": 8_900_000 },
            ])
            .unwrap();
    } // db is dropped here, and data should be flushed to disk

    // Re-open the database
    let db = get_db(db_path);
    let collection1 = db.collection("collection1");
    let collection2 = db.collection("collection2");

    let mut results1 = collection1
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|d| d.unwrap())
        .collect::<Vec<Document>>();
    assert_eq!(results1.len(), 2);

    // Order is not guaranteed, so we sort before asserting
    results1.sort_by_key(|d| d["name"].as_str().unwrap().to_string());
    assert_eq!(results1[0]["name"], "Alice".into());
    assert_eq!(results1[0]["age"], 30.into());
    assert_eq!(results1[1]["name"], "Bob".into());
    assert_eq!(results1[1]["age"], 25.into());

    let mut results2 = collection2
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|d| d.unwrap())
        .collect::<Vec<Document>>();
    assert_eq!(results2.len(), 2);

    // Order is not guaranteed, so we sort before asserting
    results2.sort_by_key(|d| d["city"].as_str().unwrap().to_string());
    assert_eq!(results2[0]["city"], "London".into());
    assert_eq!(results2[0]["population"], 8_900_000.into());
    assert_eq!(results2[1]["city"], "New York".into());
    assert_eq!(results2[1]["population"], 8_400_000.into());
}

#[test]
fn test_restart_after_parallel_operations_from_clones() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path();

    const THREADS: usize = 4;
    const INSERTS_PER_THREAD: usize = 50;

    {
        let db = get_db(db_path);
        let collection_name = "clone_parallel_ops";
        let collection = db.collection(collection_name);

        // Seed document so the collection exists, and validate basic write path.
        collection
            .insert_one(doc! { "type": "seed", "v": 0_i32 })
            .unwrap();

        std::thread::scope(|s| {
            for t in 0..THREADS {
                let db_clone = db.clone();

                s.spawn(move || {
                    let coll = db_clone.collection(collection_name);

                    for i in 0..INSERTS_PER_THREAD {
                        coll.insert_one(doc! {
                            "type": "parallel",
                            "thread": t as i32,
                            "i": i as i32,
                        })
                        .unwrap();
                    }

                    // Dropping some clones at different times should not shut down the DB
                    // as long as at least one clone is still alive.
                    drop(db_clone);
                });
            }

            // Drop one handle while other threads are still using their clones.
            // This must not shut down the underlying DbImpl because other clones still exist.
            drop(db);
        });
        // All clones dropped here (end of scope), DbImpl::drop triggers shutdown+flush.
    }

    // Restart
    let db = get_db(db_path);
    let collection = db.collection("clone_parallel_ops");

    let docs = collection
        .find(doc! {})
        .execute()
        .unwrap()
        .map(|d| d.unwrap())
        .collect::<Vec<Document>>();

    // 1 seed + THREADS * INSERTS_PER_THREAD parallel inserts
    assert_eq!(docs.len(), 1 + THREADS * INSERTS_PER_THREAD);

    let seed_count = docs
        .iter()
        .filter(|d| d.get_str("type").ok() == Some("seed"))
        .count();
    assert_eq!(seed_count, 1);

    let parallel_docs = docs
        .iter()
        .filter(|d| d.get_str("type").ok() == Some("parallel"))
        .collect::<Vec<_>>();
    assert_eq!(parallel_docs.len(), THREADS * INSERTS_PER_THREAD);

    // Verify we have exactly one doc for each (thread, i) pair.
    let mut seen = std::collections::BTreeSet::new();
    for d in parallel_docs {
        let thread = d.get_i32("thread").unwrap() as usize;
        let i = d.get_i32("i").unwrap() as usize;
        assert!(thread < THREADS);
        assert!(i < INSERTS_PER_THREAD);
        assert!(seen.insert((thread, i)), "duplicate doc for (thread={}, i={})", thread, i);
    }
    assert_eq!(seen.len(), THREADS * INSERTS_PER_THREAD);
}
