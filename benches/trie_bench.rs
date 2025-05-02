use std::sync::Arc;
use bson::oid::ObjectId;
use quokkadb::util::bson_utils::BsonKey;
use quokkadb::storage::internal_key::encode_internal_key;
use quokkadb::storage::operation::OperationType;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use crossbeam_skiplist::SkipMap;

use quokkadb::storage::trie::Trie;

use pprof::ProfilerGuard;


/// Generate `n` realistic internal database keys.
pub fn generate_realistic_keys(n: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut keys = Vec::with_capacity(n);

    for seq in 0..n as u64 {
        let collection_id = rng.gen_range(0..10);
        let index_id = 0;

        let oid = ObjectId::new();
        let bson_key = bson::Bson::ObjectId(oid).try_into_key().expect("Failed to encode BSON key");

        let key = encode_internal_key(
            collection_id,
            index_id,
            &bson_key,
            seq,
            OperationType::Put,
        );

        keys.push(key);
    }

    keys
}

fn bench_insert_trie(c: &mut Criterion) {

    let guard = pprof::ProfilerGuard::new(100).unwrap();

    let keys = generate_realistic_keys(10_000);
    c.bench_function("insert_trie", |b| {
        b.iter(|| {
            let trie = Trie::new();
            for key in &keys {
                trie.insert(&key, &key);
            }
        });
    });

    if let Ok(report) = guard.report().build() {
        let file = std::fs::File::create("flamegraph.svg").unwrap();
        report.flamegraph(file).unwrap();
    }
}

fn bench_insert_skipmap(c: &mut Criterion) {
    let keys = generate_realistic_keys(10_000);
    c.bench_function("insert_skipmap", |b| {
        b.iter(|| {
            let map: SkipMap<Arc<[u8]>, Arc<[u8]>> = SkipMap::new();
            for key in &keys {
                let key: Arc<[u8]> = Arc::from(&key[..]);
                map.insert(key.clone(), key);
            }
        });
    });
}

criterion_group!(benches, bench_insert_trie, bench_insert_skipmap);
criterion_main!(benches);