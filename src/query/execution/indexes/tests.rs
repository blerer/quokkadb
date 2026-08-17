use super::{DocumentKeySource, Index, IndexKeyValue, Indexes};
use crate::io::varint;
use crate::query::physical_plan::IndexScanRangeExpr;
use crate::query::{BsonValue, Expr, Parameters};
use crate::storage::catalog::{
    CollectionMetadata, CollectionOptions, IndexDefinition, IndexDirection, IndexMetadata,
    IndexPath, OrderedIndexField,
};
use crate::storage::count_stats::{CountStatsBuilder, CountStatsKey};
use crate::storage::operation::OperationType;
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;
use bson::{
    doc, oid::ObjectId, raw::RawDocumentBuf, spec::BinarySubtype, Binary, Bson, DateTime,
    Decimal128, Document, Timestamp,
};
use std::collections::BTreeMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_index(fields: Vec<(&str, IndexDirection)>) -> Index {
    let metadata = make_index_metadata(1, fields);
    Index::from(42, &metadata)
}

fn extract_and_decode(index: &Index, doc: &Document) -> Document {
    let doc_id = {
        let id_val = doc.get("_id").expect("document must have _id");
        id_val
            .try_into_typed_key()
            .expect("_id must be convertible to key")
    };

    let key_source = DocumentKeySource::BsonDocument(doc);
    let entry = index
        .extract_index_entry(&key_source, &doc_id)
        .expect("extract should succeed");
    index
        .decode_index_entry(entry)
        .expect("decode should succeed")
}

fn extract_key(index: &Index, doc: &Document) -> Vec<u8> {
    let doc_id = {
        let id_val = doc.get("_id").expect("document must have _id");
        id_val
            .try_into_typed_key()
            .expect("_id must be convertible to key")
    };

    let key_source = DocumentKeySource::BsonDocument(doc);
    index
        .extract_index_entry(&key_source, &doc_id)
        .expect("extract should succeed")
        .key
}

fn make_collection(indexes: Vec<IndexMetadata>) -> CollectionMetadata {
    let next_index_id = indexes.iter().map(|index| index.id).max().unwrap_or(0) + 1;
    let indexes = indexes
        .into_iter()
        .map(|index| (index.id, Arc::new(index)))
        .collect::<BTreeMap<_, _>>();
    let mut collection = CollectionMetadata::new(42, "test", 1, CollectionOptions::default());
    collection.next_index_id = next_index_id;
    collection.indexes = indexes;
    collection
}

fn make_index_metadata(id: u32, fields: Vec<(&str, IndexDirection)>) -> IndexMetadata {
    let ordered_fields = fields
        .into_iter()
        .map(|(name, direction)| OrderedIndexField {
            path: IndexPath {
                components: name.split('.').map(|s| s.to_string()).collect(),
            },
            direction,
        })
        .collect();
    IndexMetadata::new(id, IndexDefinition::Regular(ordered_fields), 1)
}

fn supported_id_values() -> Vec<(&'static str, Bson)> {
    vec![
        ("min_key", Bson::MinKey),
        ("null", Bson::Null),
        ("max_key", Bson::MaxKey),
        ("bool_false", Bson::Boolean(false)),
        ("bool_true", Bson::Boolean(true)),
        ("string", Bson::String("user-42".to_string())),
        (
            "object_id",
            Bson::ObjectId(ObjectId::from_bytes([
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            ])),
        ),
        (
            "datetime",
            Bson::DateTime(DateTime::from_millis(1_735_689_600_123)),
        ),
        (
            "timestamp",
            Bson::Timestamp(Timestamp {
                time: 1234,
                increment: 7,
            }),
        ),
        (
            "binary",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0, 1, 2, 255],
            }),
        ),
        ("double", Bson::Double(3.25)),
        ("int32", Bson::Int32(42)),
        ("int64", Bson::Int64(1_000_000_000_000)),
        (
            "decimal128",
            Bson::Decimal128("12.50".parse::<Decimal128>().unwrap()),
        ),
        ("document_simple", Bson::Document(doc! { "k": "v" })),
        (
            "document_nested",
            Bson::Document(doc! { "outer": { "inner": 9_i32 }, "flag": true }),
        ),
        (
            "array_scalar",
            Bson::Array(vec![Bson::Int32(1), Bson::String("two".to_string())]),
        ),
        (
            "array_nested",
            Bson::Array(vec![
                Bson::Document(doc! { "x": 1_i32 }),
                Bson::Array(vec![Bson::Boolean(true), Bson::Null]),
            ]),
        ),
    ]
}

fn assert_id_round_trip(label: &str, actual: &Bson, expected: &Bson) {
    match (actual, expected) {
        (Bson::Decimal128(actual), Bson::Decimal128(expected)) => {
            assert_eq!(
                Bson::Decimal128(*actual).try_into_key().unwrap(),
                Bson::Decimal128(*expected).try_into_key().unwrap(),
                "failed id round trip for {label}"
            );
        }
        _ => assert_eq!(actual, expected, "failed id round trip for {label}"),
    }
}

// ---------------------------------------------------------------------------
// Basic single-field ascending
// ---------------------------------------------------------------------------

#[test]
fn single_field_ascending_round_trip_string() {
    let index = make_index(vec![("name", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "name": "alice" };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(decoded, doc! {"_id": id, "name": "alice"});
}

#[test]
fn single_field_ascending_round_trip_int32() {
    let index = make_index(vec![("score", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "score": 42_i32 };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(decoded, doc! { "_id": id, "score": 42_i32 });
}

#[test]
fn single_field_ascending_round_trip_int64() {
    let index = make_index(vec![("counter", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "counter": Bson::Int64(1_000_000_000_000_i64) };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(
        decoded,
        doc! { "_id": id, "counter": Bson::Int64(1_000_000_000_000_i64) }
    );
}

#[test]
fn single_field_ascending_round_trip_double() {
    let index = make_index(vec![("ratio", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "ratio": 3.14_f64 };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(decoded, doc! { "_id": id, "ratio": 3.14_f64 });
}

#[test]
fn single_field_ascending_round_trip_boolean() {
    let index = make_index(vec![("active", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "active": true };

    let decoded = extract_and_decode(&index, &document);

    assert_eq!(decoded, doc! { "_id": id, "active": true });
}

#[test]
fn missing_indexed_field_is_encoded_as_null() {
    let index = make_index(vec![("missing", IndexDirection::Ascending)]);
    let document = doc! { "_id": 56i64, "present": 1_i32 };

    let decoded = extract_and_decode(&index, &document);

    assert_eq!(decoded, doc! { "_id": 56i64, "missing": Bson::Null });
}

// ---------------------------------------------------------------------------
// Single-field descending
// ---------------------------------------------------------------------------

#[test]
fn single_field_descending_round_trip_int32() {
    let index = make_index(vec![("score", IndexDirection::Descending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "score": -7_i32 };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(decoded, doc! { "_id": id, "score": -7_i32 });
}

#[test]
fn single_field_descending_round_trip_string() {
    let index = make_index(vec![("tag", IndexDirection::Descending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "tag": "zulu" };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(decoded, doc! { "_id": id, "tag": "zulu" });
}

#[test]
fn descending_key_sorts_opposite_to_ascending() {
    // Produce two entries with ascending and two with descending index.
    // The descending entries should sort in reverse order of the values.
    let asc_index = make_index(vec![("v", IndexDirection::Ascending)]);
    let desc_index = make_index(vec![("v", IndexDirection::Descending)]);

    let id1 = 1i64;
    let id2 = 2i64;
    let doc_lo = doc! { "_id": id1, "v": 1_i32 };
    let doc_hi = doc! { "_id": id2, "v": 2_i32 };

    let asc_key_lo = {
        let ks = DocumentKeySource::BsonDocument(&doc_lo);
        let id_key = Indexes::extract_id_key(&ks).unwrap();
        asc_index.extract_index_entry(&ks, &id_key).unwrap().key
    };
    let asc_key_hi = {
        let ks = DocumentKeySource::BsonDocument(&doc_hi);
        let id_key = Indexes::extract_id_key(&ks).unwrap();
        asc_index.extract_index_entry(&ks, &id_key).unwrap().key
    };
    let desc_key_lo = {
        let ks = DocumentKeySource::BsonDocument(&doc_lo);
        let id_key = Indexes::extract_id_key(&ks).unwrap();
        desc_index.extract_index_entry(&ks, &id_key).unwrap().key
    };
    let desc_key_hi = {
        let ks = DocumentKeySource::BsonDocument(&doc_hi);
        let id_key = Indexes::extract_id_key(&ks).unwrap();
        desc_index.extract_index_entry(&ks, &id_key).unwrap().key
    };

    assert!(
        asc_key_lo < asc_key_hi,
        "ascending: lo should sort before hi"
    );
    assert!(
        desc_key_lo > desc_key_hi,
        "descending: lo should sort after hi"
    );
}

// ---------------------------------------------------------------------------
// Multi-field indexes
// ---------------------------------------------------------------------------

#[test]
fn multi_field_ascending_round_trip() {
    let index = make_index(vec![
        ("last", IndexDirection::Ascending),
        ("first", IndexDirection::Ascending),
    ]);
    let id = 56i64;
    let document = doc! { "_id": id, "last": "smith", "first": "john" };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(
        &decoded,
        &doc! { "_id": id, "last": "smith", "first": "john" }
    );
}

#[test]
fn multi_field_mixed_directions_round_trip() {
    let index = make_index(vec![
        ("category", IndexDirection::Ascending),
        ("rank", IndexDirection::Descending),
    ]);
    let id = 56i64;
    let document = doc! { "_id": id, "category": "books", "rank": 99_i32 };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(
        decoded,
        doc! { "_id": id, "category": "books", "rank": 99_i32 }
    );
}

#[test]
fn multi_field_three_fields_round_trip() {
    let index = make_index(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Descending),
        ("c", IndexDirection::Ascending),
    ]);
    let id = 56i64;
    let document =
        doc! { "_id": id, "a": "hello", "b": -5_i32, "c": Bson::Int64(999), "d": "world" };

    let decoded = extract_and_decode(&index, &document);
    assert_eq!(
        decoded,
        doc! { "_id": id, "a": "hello", "b": -5_i32, "c": Bson::Int64(999) }
    );
}

// ---------------------------------------------------------------------------
// Value layout: _id length and key lengths are stored correctly
// ---------------------------------------------------------------------------
#[test]
fn supported_id_types_round_trip_using_stored_id_key_type() {
    let index = make_index(vec![("name", IndexDirection::Ascending)]);
    for (label, id_value) in supported_id_values() {
        let document = doc! { "_id": id_value.clone(), "name": "alice" };

        let decoded = extract_and_decode(&index, &document);

        assert_eq!(decoded.get_str("name").unwrap(), "alice");
        assert_id_round_trip(label, decoded.get("_id").unwrap(), &id_value);
    }
}

#[test]
fn value_layout_id_length_matches_key_suffix() {
    let index = make_index(vec![("x", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "x": 1_i32 };

    let id_key = { Bson::Int64(id).try_into_typed_key().unwrap() };
    let key_source = DocumentKeySource::BsonDocument(&document);
    let entry = index.extract_index_entry(&key_source, &id_key).unwrap();

    // Skip the u32 value_len prefix.
    let (id_len, _) = varint::read_u32(&entry.value, 4);
    let id_len = id_len as usize;

    // The last `id_len` bytes of the key should equal the id_key bytes.
    let key_suffix = &entry.key[entry.key.len() - id_len..];
    assert_eq!(key_suffix, id_key.key.as_slice());
}

// ---------------------------------------------------------------------------
// Nested field paths
// ---------------------------------------------------------------------------

#[test]
fn nested_field_round_trip() {
    let index = make_index(vec![("address.city", IndexDirection::Ascending)]);
    let id = 1i64;
    let document = doc! {
        "_id": id,
        "address": { "city": "London" }
    };

    let decoded = extract_and_decode(&index, &document);

    // The decoded document should have a nested structure, not a flat dot-key.
    assert_eq!(
        decoded,
        doc! {
            "_id": id,
            "address": { "city": "London" }
        }
    );
}

#[test]
fn value_layout_prefix_and_id_key_type_bytes_are_stored() {
    let index = make_index(vec![("x", IndexDirection::Ascending)]);
    let id = "user-42";
    let id_key = Bson::String(id.to_string()).try_into_typed_key().unwrap();
    let key_source = DocumentKeySource::BsonDocument(&doc! { "_id": id, "x": 1_i32 });
    let entry = index.extract_index_entry(&key_source, &id_key).unwrap();

    let stored_len = u32::from_le_bytes(entry.value[0..4].try_into().unwrap()) as usize;
    assert_eq!(stored_len, entry.value.len());

    let (id_len, offset) = varint::read_u32(&entry.value, 4);
    assert_eq!(id_len as usize, id_key.key.len());

    let (id_key_type_len, offset) = varint::read_u32(&entry.value, offset);
    assert_eq!(id_key_type_len as usize, id_key.key_type.len());
    assert_eq!(
        &entry.value[offset..offset + id_key.key_type.len()],
        id_key.key_type.as_slice()
    );
}

#[test]
fn append_put_ops_emits_only_active_indexes() {
    let active = IndexMetadata::new(
        1,
        IndexDefinition::Regular(vec![OrderedIndexField {
            path: IndexPath {
                components: vec!["name".to_string()],
            },
            direction: IndexDirection::Ascending,
        }]),
        1,
    );
    let mut dropped = IndexMetadata::new(
        2,
        IndexDefinition::Regular(vec![OrderedIndexField {
            path: IndexPath {
                components: vec!["age".to_string()],
            },
            direction: IndexDirection::Ascending,
        }]),
        1,
    );
    dropped.dropped_at = Some(2);
    let collection = make_collection(vec![active, dropped]);
    let indices = Indexes::from_collection(&collection);
    let document = doc! { "_id": 7i64, "name": "alice", "age": 30_i32 };
    let mut operations = Vec::new();
    let mut count_stats = CountStatsBuilder::new();

    indices
        .append_put_ops(&mut operations, &document, &mut count_stats)
        .unwrap();

    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].operation_type, OperationType::Put);
    assert_eq!(operations[0].collection, 42);
    assert_eq!(operations[0].index, 1);
    assert!(!operations[0].value().is_empty());
    let count_stats = count_stats.build();
    assert_eq!(
        count_stats.count_stat(&CountStatsKey::Index {
            collection: 42,
            index: 1,
        }),
        Some(1)
    );
    assert_eq!(
        count_stats.count_stat(&CountStatsKey::Index {
            collection: 42,
            index: 2,
        }),
        None
    );
}

#[test]
fn append_delete_ops_emits_delete_operation_with_empty_value() {
    let collection = make_collection(vec![make_index_metadata(
        1,
        vec![("name", IndexDirection::Ascending)],
    )]);
    let indices = Indexes::from_collection(&collection);
    let document = doc! { "_id": 7i64, "name": "alice" };
    let mut operations = Vec::new();
    let mut count_stats = CountStatsBuilder::new();

    indices
        .append_delete_ops(&mut operations, &document, &mut count_stats)
        .unwrap();

    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].operation_type, OperationType::Delete);
    assert_eq!(operations[0].collection, 42);
    assert_eq!(operations[0].index, 1);
    assert!(operations[0].value().is_empty());
    let count_stats = count_stats.build();
    assert_eq!(
        count_stats.count_stat(&CountStatsKey::Index {
            collection: 42,
            index: 1,
        }),
        Some(-1)
    );
}

#[test]
fn append_put_ops_raw_matches_bson_document_variant() {
    let collection = make_collection(vec![make_index_metadata(
        1,
        vec![("address.city", IndexDirection::Ascending)],
    )]);
    let indices = Indexes::from_collection(&collection);
    let document = doc! {
        "_id": 7i64,
        "address": { "city": "London" }
    };
    let raw = RawDocumentBuf::from_bytes(document.to_vec().unwrap()).unwrap();
    let mut bson_ops = Vec::new();
    let mut raw_ops = Vec::new();
    let mut bson_count_stats = CountStatsBuilder::new();
    let mut raw_count_stats = CountStatsBuilder::new();

    indices
        .append_put_ops(&mut bson_ops, &document, &mut bson_count_stats)
        .unwrap();
    indices
        .append_put_ops_raw(&mut raw_ops, raw.as_ref(), &mut raw_count_stats)
        .unwrap();

    assert_eq!(bson_ops.len(), 1);
    assert_eq!(raw_ops.len(), 1);
    assert_eq!(bson_ops[0], raw_ops[0]);
    assert_eq!(bson_count_stats.build(), raw_count_stats.build());
}

#[test]
fn bind_range_expr_empty_is_unbounded() {
    let index = make_index(vec![("a", IndexDirection::Ascending)]);

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: None,
            },
            &Parameters::empty(),
        )
        .unwrap();

    assert_eq!(range, Interval::all());
}

#[test]
fn bind_range_expr_single_field_equality_contains_matching_keys_only() {
    let index = make_index(vec![("a", IndexDirection::Ascending)]);
    let mut parameters = Parameters::new();
    let bound = parameters.collect_parameter(10_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![bound],
                tail: None,
            },
            &parameters,
        )
        .unwrap();

    let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32 });
    let lower = extract_key(&index, &doc! { "_id": 2_i64, "a": 9_i32 });
    let higher = extract_key(&index, &doc! { "_id": 3_i64, "a": 11_i32 });

    assert!(range.contains(&matching));
    assert!(!range.contains(&lower));
    assert!(!range.contains(&higher));
}

#[test]
fn bind_range_expr_compound_full_equality_contains_only_matching_prefix() {
    let index = make_index(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Descending),
    ]);
    let mut parameters = Parameters::new();
    let a = parameters.collect_parameter(10_i32.into());
    let b = parameters.collect_parameter(20_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![a, b],
                tail: None,
            },
            &parameters,
        )
        .unwrap();

    let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 20_i32 });
    let wrong_a = extract_key(&index, &doc! { "_id": 2_i64, "a": 9_i32, "b": 20_i32 });
    let wrong_b = extract_key(&index, &doc! { "_id": 3_i64, "a": 10_i32, "b": 19_i32 });

    assert!(range.contains(&matching));
    assert!(!range.contains(&wrong_a));
    assert!(!range.contains(&wrong_b));
}

#[test]
fn bind_range_expr_compound_prefix_plus_tail_range_contains_expected_keys() {
    let index = make_index(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Descending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(10_i32.into());
    let tail = parameters.collect_parameter(5_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::less_than(tail)),
            },
            &parameters,
        )
        .unwrap();

    let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 4_i32 });
    let wrong_prefix = extract_key(&index, &doc! { "_id": 2_i64, "a": 9_i32, "b": 4_i32 });
    let wrong_tail = extract_key(&index, &doc! { "_id": 3_i64, "a": 10_i32, "b": 6_i32 });

    assert!(range.contains(&matching));
    assert!(!range.contains(&wrong_prefix));
    assert!(!range.contains(&wrong_tail));
}

#[test]
fn bind_range_expr_tail_inclusive_and_exclusive_bounds_are_respected() {
    let index = make_index(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Ascending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(10_i32.into());
    let included = parameters.collect_parameter(5_i32.into());
    let excluded = parameters.collect_parameter(8_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::new(
                    std::ops::Bound::Included(included),
                    std::ops::Bound::Excluded(excluded),
                )),
            },
            &parameters,
        )
        .unwrap();

    let lower_included = extract_key(&index, &doc! { "_id": 1_i64, "a": 10_i32, "b": 5_i32 });
    let middle = extract_key(&index, &doc! { "_id": 2_i64, "a": 10_i32, "b": 7_i32 });
    let upper_excluded = extract_key(&index, &doc! { "_id": 3_i64, "a": 10_i32, "b": 8_i32 });

    assert!(range.contains(&lower_included));
    assert!(range.contains(&middle));
    assert!(!range.contains(&upper_excluded));
}

#[test]
fn bind_range_expr_descending_single_field_swaps_interval_orientation() {
    let index = make_index(vec![("a", IndexDirection::Descending)]);
    let mut parameters = Parameters::new();
    let lower = parameters.collect_parameter(5_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: Some(Interval::greater_than(lower)),
            },
            &parameters,
        )
        .unwrap();

    let above = extract_key(&index, &doc! { "_id": 1_i64, "a": 6_i32 });
    let boundary = extract_key(&index, &doc! { "_id": 2_i64, "a": 5_i32 });
    let below = extract_key(&index, &doc! { "_id": 3_i64, "a": 4_i32 });

    assert!(range.contains(&above));
    assert!(!range.contains(&boundary));
    assert!(!range.contains(&below));
}

#[test]
fn bind_range_expr_unbounded_tail_stays_within_equality_prefix() {
    let index = make_index(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Ascending),
        ("c", IndexDirection::Descending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(10_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::all()),
            },
            &parameters,
        )
        .unwrap();

    let matching_1 = extract_key(
        &index,
        &doc! { "_id": 1_i64, "a": 10_i32, "b": 0_i32, "c": 9_i32 },
    );
    let matching_2 = extract_key(
        &index,
        &doc! { "_id": 2_i64, "a": 10_i32, "b": 99_i32, "c": -1_i32 },
    );
    let wrong_prefix = extract_key(
        &index,
        &doc! { "_id": 3_i64, "a": 11_i32, "b": 0_i32, "c": 9_i32 },
    );

    assert!(range.contains(&matching_1));
    assert!(range.contains(&matching_2));
    assert!(!range.contains(&wrong_prefix));
}

#[test]
fn bind_range_expr_allows_literal_tail_sentinels() {
    let index = make_index(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Ascending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(7_i32.into());

    let range = index
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::closed(
                    Arc::new(Expr::Literal(BsonValue(Bson::MinKey))),
                    Arc::new(Expr::Literal(BsonValue(Bson::MaxKey))),
                )),
            },
            &parameters,
        )
        .unwrap();

    let matching = extract_key(&index, &doc! { "_id": 1_i64, "a": 7_i32, "b": 123_i32 });
    let wrong_prefix = extract_key(&index, &doc! { "_id": 2_i64, "a": 8_i32, "b": 123_i32 });

    assert!(range.contains(&matching));
    assert!(!range.contains(&wrong_prefix));
}

#[test]
fn decode_index_entry_returns_error_for_truncated_value() {
    let index = make_index(vec![("name", IndexDirection::Ascending)]);
    let document = doc! { "_id": 7i64, "name": "alice" };
    let key_source = DocumentKeySource::BsonDocument(&document);
    let id_key = Indexes::extract_id_key(&key_source).unwrap();
    let mut entry = index.extract_index_entry(&key_source, &id_key).unwrap();
    entry.value.truncate(4);

    let result = index.decode_index_entry(entry);

    assert!(result.is_err());
}

#[test]
fn decode_index_entry_returns_error_for_invalid_id_key_type_length() {
    let index = make_index(vec![("name", IndexDirection::Ascending)]);
    let entry = IndexKeyValue {
        key: vec![1, 2, 3],
        value: vec![6, 0, 0, 0, 3, 10],
    };

    let result = index.decode_index_entry(entry);

    assert!(result.is_err());
}
