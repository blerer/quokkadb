mod common;

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, DateTime, Decimal128, Document, Timestamp, doc, oid::ObjectId};
use std::str::FromStr;
use tempfile::TempDir;

fn ordered_values() -> Vec<(&'static str, Bson)> {
    vec![
        ("min_key", Bson::MinKey),
        ("null", Bson::Null),
        ("number_nan", Bson::Double(f64::NAN)),
        ("number_negative_infinity", Bson::Double(f64::NEG_INFINITY)),
        ("number_int32_negative", Bson::Int32(-7)),
        ("number_int64_negative", Bson::Int64(-1)),
        ("number_double_negative", Bson::Double(-0.5)),
        (
            "number_decimal_negative",
            Bson::Decimal128(Decimal128::from_str("-0.25").unwrap()),
        ),
        ("number_zero", Bson::Int32(0)),
        ("number_int32", Bson::Int32(7)),
        ("number_int64", Bson::Int64(8)),
        ("number_double", Bson::Double(9.5)),
        (
            "number_decimal",
            Bson::Decimal128(Decimal128::from_str("10.25").unwrap()),
        ),
        (
            "number_int64_precision_below",
            Bson::Int64(9_007_199_254_740_991),
        ),
        (
            "number_double_precision_boundary",
            Bson::Double(9_007_199_254_740_992.0),
        ),
        (
            "number_int64_precision_above",
            Bson::Int64(9_007_199_254_740_993),
        ),
        (
            "number_double_precision_above",
            Bson::Double(9_007_199_254_740_994.0),
        ),
        ("number_positive_infinity", Bson::Double(f64::INFINITY)),
        ("string_empty", Bson::String(String::new())),
        ("string_alpha", Bson::String("alpha".into())),
        ("string_value", Bson::String("value".into())),
        ("string_zeta", Bson::String("zeta".into())),
        ("document_a_then_b", Bson::Document(doc! { "a": 1, "b": 2 })),
        ("document_b_then_a", Bson::Document(doc! { "b": 2, "a": 1 })),
        ("document_1", Bson::Document(doc! { "field": 1 })),
        ("document_2", Bson::Document(doc! { "field": 2 })),
        (
            "document_nested_a_then_b",
            Bson::Document(doc! { "nested": { "a": 1, "b": 2 } }),
        ),
        (
            "document_nested_b_then_a",
            Bson::Document(doc! { "nested": { "b": 2, "a": 1 } }),
        ),
        ("document_3", Bson::Document(doc! { "other": 0 })),
        (
            "binary_empty",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![],
            }),
        ),
        (
            "binary_1",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1],
            }),
        ),
        (
            "binary_1_2",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2],
            }),
        ),
        (
            "binary_2",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![2],
            }),
        ),
        (
            "binary_function_1",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Function,
                bytes: vec![1],
            }),
        ),
        (
            "object_id_1",
            Bson::ObjectId(ObjectId::parse_str("000000000000000000000001").unwrap()),
        ),
        (
            "object_id_2",
            Bson::ObjectId(ObjectId::parse_str("000000000000000000000002").unwrap()),
        ),
        (
            "object_id_3",
            Bson::ObjectId(ObjectId::parse_str("000000000000000000000003").unwrap()),
        ),
        ("boolean_false", Bson::Boolean(false)),
        ("boolean_true", Bson::Boolean(true)),
        ("date_1", Bson::DateTime(DateTime::from_millis(1))),
        ("date_2", Bson::DateTime(DateTime::from_millis(2))),
        ("date_3", Bson::DateTime(DateTime::from_millis(3))),
        (
            "timestamp_1",
            Bson::Timestamp(Timestamp {
                time: 1,
                increment: 0,
            }),
        ),
        (
            "timestamp_2",
            Bson::Timestamp(Timestamp {
                time: 1,
                increment: 1,
            }),
        ),
        (
            "timestamp_3",
            Bson::Timestamp(Timestamp {
                time: 2,
                increment: 0,
            }),
        ),
        ("max_key", Bson::MaxKey),
    ]
}

fn shuffled_values() -> Vec<(&'static str, Bson)> {
    let ordered = ordered_values();
    let mut shuffled = Vec::with_capacity(ordered.len());
    let (mut left, mut right) = (0, ordered.len());

    while left < right {
        right -= 1;
        shuffled.push(ordered[right].clone());
        if left < right {
            shuffled.push(ordered[left].clone());
            left += 1;
        }
    }

    shuffled
}

fn setup_collection() -> (TempDir, quokkadb::Collection) {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("values").create_if_missing();
    collection
        .insert_many(shuffled_values().into_iter().map(|(name, value)| {
            doc! {
                "_id": value.clone(),
                "indexed_value": value.clone(),
                "filtered_value": value,
                "name": name,
            }
        }))
        .unwrap();
    (directory, collection)
}

fn names(documents: &[Document]) -> Vec<String> {
    documents
        .iter()
        .map(|document| document.get_str("name").unwrap().to_owned())
        .collect()
}

fn comparison_filter(field: &str, operator: &str, value: &Bson) -> Document {
    doc! { (field): { (operator): value.clone() } }
}

#[test]
fn index_key_order_matches_comparator_order_in_both_directions() {
    let (_directory, collection) = setup_collection();
    let index_name = collection
        .create_index(doc! { "indexed_value": 1 })
        .unwrap();

    for sort_direction in [1, -1] {
        let mut ordered_names: Vec<_> = ordered_values()
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect();

        if sort_direction == -1 {
            ordered_names.reverse();
        }

        let comparator_order = names(
            &collection
                .find(Document::new())
                .hint_collection_scan()
                .sort(doc! { "indexed_value": sort_direction })
                .execute_collect()
                .unwrap(),
        );

        assert_eq!(
            comparator_order, ordered_names,
            "comparator order should match ordered values"
        );

        let index_order = names(
            &collection
                .find(Document::new())
                .hint(index_name.clone())
                .sort(doc! { "indexed_value": sort_direction })
                .execute_collect()
                .unwrap(),
        );

        assert_eq!(
            comparator_order, index_order,
            "index order should match comparator order"
        );
    }
}

#[test]
fn indexed_and_non_indexed_comparison_results_match_for_every_value_pair() {
    let (_directory, collection) = setup_collection();
    let index_name = collection
        .create_index(doc! { "indexed_value": 1 })
        .unwrap();
    let values = ordered_values();
    let operators = ["$eq", "$lt", "$lte", "$gt", "$gte"];

    for operator in operators {
        for (bound_index, (bound_name, bound_value)) in values.iter().enumerate() {
            let indexed_names = names(
                &collection
                    .find(comparison_filter("indexed_value", operator, bound_value))
                    .hint(index_name.clone())
                    .sort(doc! { "name": 1 })
                    .execute_collect()
                    .unwrap(),
            );

            let scan_names = names(
                &collection
                    .find(comparison_filter("filtered_value", operator, bound_value))
                    .hint_collection_scan()
                    .sort(doc! { "name": 1 })
                    .execute_collect()
                    .unwrap(),
            );

            assert_eq!(
                indexed_names, scan_names,
                "indexed and scanned result sets differ for {operator} {bound_name}"
            );

            for (value_index, (value_name, _)) in values.iter().enumerate() {
                let expected_match = match operator {
                    "$eq" => value_index == bound_index,
                    "$lt" => value_index < bound_index,
                    "$lte" => value_index <= bound_index,
                    "$gt" => value_index > bound_index,
                    "$gte" => value_index >= bound_index,
                    _ => unreachable!(),
                };
                assert_eq!(
                    indexed_names.contains(&value_name.to_string()),
                    expected_match,
                    "comparison result disagrees with ordered values for pair ({value_name}, {bound_name}) with {operator}"
                );
            }
        }
    }
}

#[test]
fn sorting_uses_within_type_order_for_documents_and_arrays() {
    let (_directory, collection) = setup_collection();

    let document_names = ["document_1", "document_2", "document_3"];
    let documents = collection
        .find(doc! { "name": { "$in": document_names } })
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap();
    assert_eq!(names(&documents), document_names);

    collection
        .insert_many([
            doc! { "_id": 100, "sort_value": [1], "name": "array_1" },
            doc! { "_id": 101, "sort_value": [1, 2], "name": "array_1_2" },
            doc! { "_id": 102, "sort_value": [2], "name": "array_2" },
        ])
        .unwrap();
    let array_names = ["array_1", "array_1_2", "array_2"];
    let documents = collection
        .find(doc! { "name": { "$in": array_names } })
        .sort(doc! { "sort_value": 1 })
        .execute_collect()
        .unwrap();
    assert_eq!(names(&documents), array_names);
}

#[test]
fn bson_type_equality_does_not_match_a_different_type() {
    let (_directory, collection) = setup_collection();

    let string_match = collection
        .find(doc! { "_id": "value" })
        .execute_collect()
        .unwrap();
    assert_eq!(names(&string_match), ["string_value"]);

    let number_match = collection
        .find(doc! { "filtered_value": 7.0_f64 })
        .execute_collect()
        .unwrap();
    assert_eq!(names(&number_match), ["number_int32"]);
}

fn assert_numeric_predicate_results(
    collection: &quokkadb::Collection,
    index_name: &str,
    operator: &str,
    query_values: &[(&str, Bson)],
    matching_values: &[(&str, Bson)],
) {
    let mut expected_names: Vec<_> = matching_values
        .iter()
        .map(|(name, _)| name.to_string())
        .collect();
    expected_names.sort();

    for (query_name, query_value) in query_values {
        let indexed_names = names(
            &collection
                .find(comparison_filter("indexed_value", operator, query_value))
                .hint(index_name.to_owned())
                .sort(doc! { "name": 1 })
                .execute_collect()
                .unwrap(),
        );
        let scanned_names = names(
            &collection
                .find(comparison_filter("filtered_value", operator, query_value))
                .hint_collection_scan()
                .sort(doc! { "name": 1 })
                .execute_collect()
                .unwrap(),
        );

        assert_eq!(
            indexed_names, expected_names,
            "indexed {operator} comparison for {query_name}"
        );
        assert_eq!(
            scanned_names, expected_names,
            "scanned {operator} comparison for {query_name}"
        );
    }
}

#[test]
fn numerically_equal_bson_representations_match_indexed_and_scanned_equality() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("equal_numbers").create_if_missing();
    let values = [
        ("int32", Bson::Int32(7)),
        ("int64", Bson::Int64(7)),
        ("double", Bson::Double(7.0)),
        (
            "decimal",
            Bson::Decimal128(Decimal128::from_str("7").unwrap()),
        ),
    ];
    let precision_values = [
        ("precision_int64", Bson::Int64(9_007_199_254_740_992)),
        ("precision_double", Bson::Double(9_007_199_254_740_992.0)),
    ];
    collection
        .insert_many(values.iter().chain(&precision_values).enumerate().map(
            |(id, (name, value))| {
                doc! {
                    "_id": id as i32,
                    "indexed_value": value.clone(),
                    "filtered_value": value.clone(),
                    "name": name,
                }
            },
        ))
        .unwrap();
    let index_name = collection
        .create_index(doc! { "indexed_value": 1 })
        .unwrap();
    let all_values: Vec<_> = values.iter().chain(&precision_values).cloned().collect();
    assert_numeric_predicate_results(&collection, &index_name, "$eq", &values, &values);
    assert_numeric_predicate_results(
        &collection,
        &index_name,
        "$eq",
        &precision_values,
        &precision_values,
    );

    // 7 is below the precision-boundary values; each representation in a
    // group should produce the same range result.
    assert_numeric_predicate_results(&collection, &index_name, "$lt", &values, &[]);
    assert_numeric_predicate_results(&collection, &index_name, "$lt", &precision_values, &values);
    assert_numeric_predicate_results(&collection, &index_name, "$lte", &values, &values);
    assert_numeric_predicate_results(
        &collection,
        &index_name,
        "$lte",
        &precision_values,
        &all_values,
    );
    assert_numeric_predicate_results(&collection, &index_name, "$gt", &values, &precision_values);
    assert_numeric_predicate_results(&collection, &index_name, "$gt", &precision_values, &[]);
    assert_numeric_predicate_results(&collection, &index_name, "$gte", &values, &all_values);
    assert_numeric_predicate_results(
        &collection,
        &index_name,
        "$gte",
        &precision_values,
        &precision_values,
    );
}

#[test]
fn regex_values_are_ordered_during_document_filtering() {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    let collection = db.collection("regex_values").create_if_missing();
    let regex = new_regexp("alpha", "");
    collection
        .insert_many([
            doc! { "_id": 1, "value": Bson::Timestamp(Timestamp { time: 1, increment: 0 }), "name": "timestamp" },
            doc! { "_id": 2, "value": regex.clone(), "name": "regex_a" },
            doc! { "_id": 3, "value": new_regexp("middle", ""), "name": "regex_middle" },
            doc! { "_id": 4, "value": new_regexp("zeta", ""), "name": "regex_zeta" },
            doc! { "_id": 5, "value": Bson::MaxKey, "name": "max_key" },
        ])
        .unwrap();

    let documents = collection
        .find(doc! { "value": { "$gte": regex, "$lte": new_regexp("zeta", "") } })
        .sort(doc! { "_id": 1 })
        .execute_collect()
        .unwrap();
    assert_eq!(names(&documents), ["regex_a", "regex_middle", "regex_zeta"]);
}

fn new_regexp(pattern: &str, options: &str) -> Bson {
    Bson::RegularExpression(bson::Regex {
        pattern: bson::raw::CString::try_from(pattern).unwrap(),
        options: bson::raw::CString::try_from(options).unwrap(),
    })
}
