use super::super::IndexKeyValue;
use super::IndexReadCodec;
use crate::io::varint;
use crate::query::QueryEndpoint;
use crate::query::execution::indexes::indexes::{Index, IndexSource};
use crate::query::execution::indexes::test_utils::{make_index_metadata, to_raw, to_typed_key};
use crate::query::expr::Expr;
use crate::query::physical_plan::IndexScanRangeExpr;
use crate::query::{BsonValue, Parameters};
use crate::storage::catalog::IndexDirection;
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;
use bson::{
    Binary, Bson, DateTime, Decimal128, Document, Timestamp, doc, oid::ObjectId,
    spec::BinarySubtype,
};
use std::sync::Arc;

#[test]
fn reader_codec_round_trip() {
    let fields = vec![
        ("address.city", IndexDirection::Ascending),
        ("rank", IndexDirection::Descending),
    ];
    let metadata = make_index_metadata(1, fields);
    let writer = Index::from(42, &metadata);
    let decoder = IndexReadCodec::from(&metadata);
    let document = doc! {
        "_id": 7_i64,
        "address": {"city": "London"},
        "rank": 3_i32,
    };
    let source = IndexSource::from_document(&document);
    let id = to_typed_key(document.get("_id").unwrap().clone());
    let entry = writer
        .generate_entries(source, &id)
        .unwrap()
        .entries
        .remove(0);

    assert_eq!(decoder.decode(entry).unwrap(), document);
}

#[test]
fn array_expansion_is_local_to_each_shared_array_element() {
    let (index, decoder) = make_index_and_read_codec(vec![
        ("items.x", IndexDirection::Ascending),
        ("items.y", IndexDirection::Ascending),
        ("tenant", IndexDirection::Ascending),
    ]);
    let document = doc! {
        "_id": 1, "tenant": "T",
        "items": [
            {"x": [1, 2], "y": 10},
            {"x": 3, "y": [20, 30]}
        ]
    };
    let raw = to_raw(&document);
    let id = to_typed_key(1);
    for source in [
        IndexSource::from_document(&document),
        IndexSource::from_raw_document(&raw),
    ] {
        let actual: Vec<_> = index
            .generate_entries(source, &id)
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| decode_index_entry(&decoder, entry).unwrap())
            .collect();
        assert_eq!(
            actual,
            vec![
                doc! {"_id": 1, "tenant": "T", "items": {"x": 1, "y": 10}},
                doc! {"_id": 1, "tenant": "T", "items": {"x": 2, "y": 10}},
                doc! {"_id": 1, "tenant": "T", "items": {"x": 3, "y": 20}},
                doc! {"_id": 1, "tenant": "T", "items": {"x": 3, "y": 30}},
            ]
        );
    }
}

#[test]
fn tree_extraction_preserves_key_order_and_nested_array_correlation() {
    // Index order deliberately differs from the tree's field-name order.
    let (index, decoder) = make_index_and_read_codec(vec![
        ("items.parts.y", IndexDirection::Descending),
        ("tenant", IndexDirection::Ascending),
        ("items.parts.x", IndexDirection::Ascending),
    ]);
    let document = doc! {
        "_id": 1, "tenant": "T",
        "items": [
            {"parts": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]},
            {"parts": []},
            {"parts": [{"x": [7, 8], "y": 6}]}
        ]
    };
    let raw = to_raw(&document);
    let id = to_typed_key(1);
    let decoded_entries = index
        .generate_entries(IndexSource::from_document(&document), &id)
        .unwrap()
        .entries;
    let raw_entries = index
        .generate_entries(IndexSource::from_raw_document(&raw), &id)
        .unwrap()
        .entries;
    let decoded_bytes: Vec<_> = decoded_entries
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect();
    let decoded_documents: Vec<_> = decoded_entries
        .into_iter()
        .map(|entry| decode_index_entry(&decoder, entry).unwrap())
        .collect();
    assert_eq!(
        decoded_documents,
        vec![
            doc! {"_id": 1, "tenant": "T", "items": {"parts": {"x": 7, "y": 6}}},
            doc! {"_id": 1, "tenant": "T", "items": {"parts": {"x": 8, "y": 6}}},
            doc! {"_id": 1, "tenant": "T", "items": {"parts": {"x": 3, "y": 4}}},
            doc! {"_id": 1, "tenant": "T", "items": {"parts": {"x": 1, "y": 2}}},
            doc! {"_id": 1, "tenant": "T", "items": {"parts": {"x": null, "y": null}}},
        ]
    );
    assert_eq!(
        decoded_bytes,
        raw_entries
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn correlated_generation_preserves_pairs_and_missing_values() {
    let (index, decoder) = make_index_and_read_codec(vec![
        ("items.sku", IndexDirection::Ascending),
        ("items.price", IndexDirection::Descending),
    ]);
    let document = doc! {"_id": 1, "items": [{"sku": "A", "price": 10}, {"sku": "B", "price": 20}, {"sku": "C"}]};
    let id = to_typed_key(document.get("_id").unwrap().clone());
    let source = IndexSource::from_document(&document);
    let entries = index.generate_entries(source, &id).unwrap().entries;
    let decoded: Vec<_> = entries
        .into_iter()
        .map(|entry| decode_index_entry(&decoder, entry).unwrap())
        .collect();
    assert_eq!(
        decoded,
        vec![
            doc! {"items": {"sku": "A", "price": 10}, "_id": 1},
            doc! {"items": {"sku": "B", "price": 20}, "_id": 1},
            doc! {"items": {"sku": "C", "price": null}, "_id": 1}
        ]
    );
}

#[test]
fn correlated_nested_arrays_and_empty_paths() {
    let (index, decoder) = make_index_and_read_codec(vec![
        ("items.parts.x", IndexDirection::Ascending),
        ("items.parts.y", IndexDirection::Ascending),
    ]);
    let document =
        doc! {"_id": 1, "items": [{"parts": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]}, {"parts": []}]};
    let id = to_typed_key(document.get("_id").unwrap().clone());
    let source = IndexSource::from_document(&document);
    let decoded: Vec<_> = index
        .generate_entries(source, &id)
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| decode_index_entry(&decoder, entry).unwrap())
        .collect();
    assert_eq!(
        decoded,
        vec![
            doc! {"items": {"parts": {"x": null, "y": null}}, "_id": 1},
            doc! {"items": {"parts": {"x": 1, "y": 2}}, "_id": 1},
            doc! {"items": {"parts": {"x": 3, "y": 4}}, "_id": 1}
        ]
    );
    let (terminal, terminal_decoder) =
        make_index_and_read_codec(vec![("items", IndexDirection::Ascending)]);
    for document in [doc! {"_id": 1, "items": []}, doc! {"_id": 1}] {
        let source = IndexSource::from_document(&document);
        let decoded = decode_index_entry(
            &terminal_decoder,
            terminal
                .generate_entries(source, &id)
                .unwrap()
                .entries
                .remove(0),
        )
        .unwrap();
        assert_eq!(
            decoded.get("items"),
            Some(document.get("items").unwrap_or(&Bson::Null))
        );
    }
}

#[test]
fn single_field_ascending_round_trip_string() {
    let (index, decoder) = make_index_and_read_codec(vec![("name", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "name": "alice" };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(decoded, doc! {"_id": id, "name": "alice"});
}

#[test]
fn single_field_ascending_round_trip_int32() {
    let (index, decoder) = make_index_and_read_codec(vec![("score", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "score": 42_i32 };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(decoded, doc! { "_id": id, "score": 42_i32 });
}

#[test]
fn single_field_ascending_round_trip_int64() {
    let (index, decoder) = make_index_and_read_codec(vec![("counter", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "counter": Bson::Int64(1_000_000_000_000_i64) };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(
        decoded,
        doc! { "_id": id, "counter": Bson::Int64(1_000_000_000_000_i64) }
    );
}

#[test]
fn single_field_ascending_round_trip_double() {
    let (index, decoder) = make_index_and_read_codec(vec![("ratio", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "ratio": 3.14_f64 };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(decoded, doc! { "_id": id, "ratio": 3.14_f64 });
}

#[test]
fn single_field_ascending_round_trip_boolean() {
    let (index, decoder) = make_index_and_read_codec(vec![("active", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "active": true };

    let decoded = extract_and_decode(&index, &decoder, &document);

    assert_eq!(decoded, doc! { "_id": id, "active": true });
}

#[test]
fn missing_indexed_field_is_encoded_as_null() {
    let (index, decoder) = make_index_and_read_codec(vec![("missing", IndexDirection::Ascending)]);
    let document = doc! { "_id": 56i64, "present": 1_i32 };

    let decoded = extract_and_decode(&index, &decoder, &document);

    assert_eq!(decoded, doc! { "_id": 56i64, "missing": Bson::Null });
}

#[test]
fn single_field_descending_round_trip_int32() {
    let (index, decoder) = make_index_and_read_codec(vec![("score", IndexDirection::Descending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "score": -7_i32 };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(decoded, doc! { "_id": id, "score": -7_i32 });
}

#[test]
fn single_field_descending_round_trip_string() {
    let (index, decoder) = make_index_and_read_codec(vec![("tag", IndexDirection::Descending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "tag": "zulu" };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(decoded, doc! { "_id": id, "tag": "zulu" });
}

#[test]
fn multi_field_ascending_round_trip() {
    let (index, decoder) = make_index_and_read_codec(vec![
        ("last", IndexDirection::Ascending),
        ("first", IndexDirection::Ascending),
    ]);
    let id = 56i64;
    let document = doc! { "_id": id, "last": "smith", "first": "john" };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(
        &decoded,
        &doc! { "_id": id, "last": "smith", "first": "john" }
    );
}

#[test]
fn multi_field_mixed_directions_round_trip() {
    let (index, decoder) = make_index_and_read_codec(vec![
        ("category", IndexDirection::Ascending),
        ("rank", IndexDirection::Descending),
    ]);
    let id = 56i64;
    let document = doc! { "_id": id, "category": "books", "rank": 99_i32 };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(
        decoded,
        doc! { "_id": id, "category": "books", "rank": 99_i32 }
    );
}

#[test]
fn multi_field_three_fields_round_trip() {
    let (index, decoder) = make_index_and_read_codec(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Descending),
        ("c", IndexDirection::Ascending),
    ]);
    let id = 56i64;
    let document =
        doc! { "_id": id, "a": "hello", "b": -5_i32, "c": Bson::Int64(999), "d": "world" };

    let decoded = extract_and_decode(&index, &decoder, &document);
    assert_eq!(
        decoded,
        doc! { "_id": id, "a": "hello", "b": -5_i32, "c": Bson::Int64(999) }
    );
}

#[test]
fn supported_id_types_round_trip_using_stored_id_key_type() {
    let (index, decoder) = make_index_and_read_codec(vec![("name", IndexDirection::Ascending)]);
    for (label, id_value) in supported_id_values() {
        let document = doc! { "_id": id_value.clone(), "name": "alice" };

        let decoded = extract_and_decode(&index, &decoder, &document);

        assert_eq!(decoded.get_str("name").unwrap(), "alice");
        assert_id_round_trip(label, decoded.get("_id").unwrap(), &id_value);
    }
}

#[test]
fn value_layout_id_length_matches_key_suffix() {
    let (index, _) = make_index_and_read_codec(vec![("x", IndexDirection::Ascending)]);
    let id = 56i64;
    let document = doc! { "_id": id, "x": 1_i32 };

    let id_key = { Bson::Int64(id).try_into_typed_key().unwrap() };
    let key_source = IndexSource::from_document(&document);
    let entry = index
        .generate_entries(key_source, &id_key)
        .unwrap()
        .entries
        .remove(0);

    let (id_len, _) = varint::read_u32(&entry.value, 0);
    let id_len = id_len as usize;

    // The last `id_len` bytes of the key should equal the id_key bytes.
    let key_suffix = &entry.key[entry.key.len() - id_len..];
    assert_eq!(key_suffix, id_key.key.as_slice());
}

#[test]
fn nested_field_round_trip() {
    let (index, decoder) =
        make_index_and_read_codec(vec![("address.city", IndexDirection::Ascending)]);
    let id = 1i64;
    let document = doc! {
        "_id": id,
        "address": { "city": "London" }
    };

    let decoded = extract_and_decode(&index, &decoder, &document);

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
fn value_layout_id_key_type_bytes_are_stored() {
    let (index, _) = make_index_and_read_codec(vec![("x", IndexDirection::Ascending)]);
    let id = "user-42";
    let id_key = Bson::String(id.to_string()).try_into_typed_key().unwrap();
    let document = doc! { "_id": id, "x": 1_i32 };
    let key_source = IndexSource::from_document(&document);
    let entry = index
        .generate_entries(key_source, &id_key)
        .unwrap()
        .entries
        .remove(0);

    let (id_len, offset) = varint::read_u32(&entry.value, 0);
    assert_eq!(id_len as usize, id_key.key.len());

    let (id_key_type_len, offset) = varint::read_u32(&entry.value, offset);
    assert_eq!(id_key_type_len as usize, id_key.key_type.len());
    assert_eq!(
        &entry.value[offset..offset + id_key.key_type.len()],
        id_key.key_type.as_slice()
    );
}

#[test]
fn bind_range_expr_empty_is_unbounded() {
    let (_, range_codec) = make_index_and_read_codec(vec![("a", IndexDirection::Ascending)]);

    let range = range_codec
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
    let (index, range_codec) = make_index_and_read_codec(vec![("a", IndexDirection::Ascending)]);
    let mut parameters = Parameters::new();
    let bound = parameters.collect_parameter(10_i32.into());

    let range = range_codec
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
    let (index, range_codec) = make_index_and_read_codec(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Descending),
    ]);
    let mut parameters = Parameters::new();
    let a = parameters.collect_parameter(10_i32.into());
    let b = parameters.collect_parameter(20_i32.into());

    let range = range_codec
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
    let (index, range_codec) = make_index_and_read_codec(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Descending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(10_i32.into());
    let tail = parameters.collect_parameter(5_i32.into());

    let range = range_codec
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::less_than(QueryEndpoint::Value(tail))),
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
    let (index, range_codec) = make_index_and_read_codec(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Ascending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(10_i32.into());
    let included = parameters.collect_parameter(5_i32.into());
    let excluded = parameters.collect_parameter(8_i32.into());

    let range = range_codec
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::new(
                    std::ops::Bound::Included(QueryEndpoint::Value(included)),
                    std::ops::Bound::Excluded(QueryEndpoint::Value(excluded)),
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
    let (index, range_codec) = make_index_and_read_codec(vec![("a", IndexDirection::Descending)]);
    let mut parameters = Parameters::new();
    let lower = parameters.collect_parameter(5_i32.into());

    let range = range_codec
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: Some(Interval::greater_than(QueryEndpoint::Value(lower))),
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
    let (index, range_codec) = make_index_and_read_codec(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Ascending),
        ("c", IndexDirection::Descending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(10_i32.into());

    let range = range_codec
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
    let (index, range_codec) = make_index_and_read_codec(vec![
        ("a", IndexDirection::Ascending),
        ("b", IndexDirection::Ascending),
    ]);
    let mut parameters = Parameters::new();
    let prefix = parameters.collect_parameter(7_i32.into());

    let range = range_codec
        .bind_range_expr(
            &IndexScanRangeExpr {
                equal_prefix: vec![prefix],
                tail: Some(Interval::closed(
                    QueryEndpoint::Value(Arc::new(Expr::Literal(BsonValue(Bson::MinKey)))),
                    QueryEndpoint::Value(Arc::new(Expr::Literal(BsonValue(Bson::MaxKey)))),
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
    let (index, decoder) = make_index_and_read_codec(vec![("name", IndexDirection::Ascending)]);
    let document = doc! { "_id": 7i64, "name": "alice" };
    let key_source = IndexSource::from_document(&document);
    let id_key = to_typed_key(document.get("_id").unwrap().clone());
    let mut entry = index
        .generate_entries(key_source, &id_key)
        .unwrap()
        .entries
        .remove(0);
    entry.value.truncate(1);

    let result = decode_index_entry(&decoder, entry);

    assert!(result.is_err());
}

#[test]
fn decode_index_entry_returns_error_for_invalid_id_key_type_length() {
    let (_, decoder) = make_index_and_read_codec(vec![("name", IndexDirection::Ascending)]);
    let entry = IndexKeyValue {
        key: vec![1, 2, 3],
        value: vec![3, 10],
    };

    let result = decode_index_entry(&decoder, entry);

    assert!(result.is_err());
}

fn decode_index_entry(decoder: &IndexReadCodec, entry: IndexKeyValue) -> std::io::Result<Document> {
    decoder.decode(entry)
}

fn make_index_and_read_codec(fields: Vec<(&str, IndexDirection)>) -> (Index, IndexReadCodec) {
    let metadata = make_index_metadata(1, fields);
    (Index::from(42, &metadata), IndexReadCodec::from(&metadata))
}

fn extract_and_decode(index: &Index, decoder: &IndexReadCodec, doc: &Document) -> Document {
    let doc_id = {
        let id_val = doc.get("_id").expect("document must have _id");
        to_typed_key(id_val.clone())
    };

    let key_source = IndexSource::from_document(doc);
    let entry = index
        .generate_entries(key_source, &doc_id)
        .expect("extract should succeed")
        .entries
        .remove(0);
    decode_index_entry(decoder, entry).expect("decode should succeed")
}

fn extract_key(index: &Index, doc: &Document) -> Vec<u8> {
    let doc_id = {
        let id_val = doc.get("_id").expect("document must have _id");
        to_typed_key(id_val.clone())
    };

    let key_source = IndexSource::from_document(doc);
    index
        .generate_entries(key_source, &doc_id)
        .expect("extract should succeed")
        .entries
        .remove(0)
        .key
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
    ]
}

fn assert_id_round_trip(label: &str, actual: &Bson, expected: &Bson) {
    match (actual, expected) {
        (Bson::Decimal128(actual), Bson::Decimal128(expected)) => {
            assert_eq!(
                to_typed_key(Bson::Decimal128(*actual)).key,
                to_typed_key(Bson::Decimal128(*expected)).key,
                "failed id round trip for {label}"
            );
        }
        _ => assert_eq!(actual, expected, "failed id round trip for {label}"),
    }
}
