use super::*;
use crate::query::update_fn::{
    add_to_set_each, add_to_set_single, bit, current_date, inc, max, min, mul, pop, pull_all,
    pull_eq, pull_matches, rename, set, unset, update,
};
use bson::doc;
use std::sync::Arc;

fn field(s: &str) -> UpdatePathComponent {
    UpdatePathComponent::FieldName(s.to_string())
}

fn index(i: usize) -> UpdatePathComponent {
    UpdatePathComponent::ArrayElement(i)
}

#[test]
fn test_set_simple() {
    let update_expr = update([set([field("a")], 10)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 10 });

    let doc = doc! { "a": 1 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 10 });
}

#[test]
fn test_set_nested() {
    let update_expr = update([set([field("a"), field("b")], 10)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "b": 10 } });

    let doc = doc! { "a": { "b": 1 } };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "b": 10 } });

    let doc = doc! { "a": {} };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "b": 10 } });
}

#[test]
fn test_set_nested_within_array() {
    let update_expr = update([set([field("a"), index(2), field("b")], 30)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": [{"b" : 0},  {"b" : 10}, { "b": 20 }]};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [{"b" : 0},  {"b" : 10}, { "b": 30 }]}
    );

    let doc = doc! { "a": []};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [Bson::Null,  Bson::Null, { "b": 30 }]}
    );

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [Bson::Null,  Bson::Null, { "b": 30 }]}
    );
}

#[test]
fn test_set_array_nested_within_array() {
    let update_expr = update([set([field("a"), index(2), index(1)], 30)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [[1, 2, 3],  [4, 5, 6], [7, 30, 9]]}
    );

    let doc = doc! { "a": [[1, 2, 3], [4, 5, 6], [7]]};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": [[1, 2, 3],  [4, 5, 6], [7, 30]]});

    let doc = doc! { "a": [[1, 2, 3], [4, 5, 6]]};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [[1, 2, 3],  [4, 5, 6], [Bson::Null, 30]]}
    );

    let doc = doc! { "a": []};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [Bson::Null,  Bson::Null, [Bson::Null, 30]]}
    );

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "a": [Bson::Null,  Bson::Null, [Bson::Null, 30]]}
    );
}

#[test]
fn test_set_array() {
    let update_expr = update([set([field("a"), index(0)], 10)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": [] };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": [10] });

    let doc = doc! { "a": [1] };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": [10] });

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": [10] });
}

#[test]
fn test_set_array_out_of_bounds() {
    let update_expr = update([set([field("a"), index(2)], 10)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": [1] };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": [1, Bson::Null, 10] });
}

#[test]
fn test_unset_simple() {
    let update_expr = update([unset([field("a")])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 1, "b": 2 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "b": 2 });

    let doc = doc! { "b": 2 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "b": 2 });
}

#[test]
fn test_unset_nested() {
    let update_expr = update([unset([field("a"), field("b")])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": { "b": 1, "c": 2}, "d": 3 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "c": 2 }, "d": 3 });
}

#[test]
fn test_unset_array_element() {
    let update_expr = update([unset([field("a"), index(1)])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": [1, 2, 3] };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": [1, Bson::Null, 3] });
}

#[test]
fn test_inc_simple() {
    let update_expr = update([inc([field("a")], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 10, "b": 2 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 15, "b": 2 });
}

#[test]
fn test_inc_non_existent_field() {
    let update_expr = update([inc([field("a")], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "b": 2 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "b": 2, "a": 5 });
}

#[test]
fn test_inc_nested() {
    let update_expr = update([inc([field("a"), field("b")], -3)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": { "b": 5 } };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "b": 2 } });
}

#[test]
fn test_inc_wrong_type() {
    let update_expr = update([inc([field("a")], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": "hello" };
    let result = updater(doc);
    assert!(result.is_err());
}

#[test]
fn test_rename_simple() {
    let update_expr = update([rename([field("a")], [field("c")])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 1, "b": 2 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "c": 1, "b": 2 });
}

#[test]
fn test_rename_non_existent() {
    let update_expr = update([rename([field("a")], [field("c")])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "b": 2 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "b": 2 });
}

#[test]
fn test_rename_nested() {
    let update_expr = update([rename([field("a"), field("b")], [field("a"), field("c")])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": { "b": 1 }, "d": 4 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "c": 1 }, "d": 4 });
}

#[test]
fn test_multiple_ops() {
    let update_expr = update([
        set([field("a")], 20),
        unset([field("b")]),
        inc([field("c")], 1),
    ]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 1, "b": 2, "c": 3 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 20, "c": 4 });
}

// -----------------------------
// Arithmetic: $mul, $min, $max
// -----------------------------
#[test]
fn test_mul_simple() {
    let update_expr = update([mul([field("a")], 2)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 10 };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 20 });
}

#[test]
fn test_mul_missing_field_defaults_to_zero() {
    // missing 'a' treated as 0; 0 * 5 = 0
    let update_expr = update([mul([field("a")], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! {};
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 0 });
}

#[test]
fn test_mul_wrong_type_errors() {
    let update_expr = update([mul([field("a")], 3)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": "oops" };
    let result = updater(doc);
    assert!(result.is_err());
}

#[test]
fn test_min_updates_when_lower() {
    let update_expr = update([min([field("a")], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 10 };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 5 });
}

#[test]
fn test_min_noop_when_not_lower() {
    let update_expr = update([min([field("a")], 20)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 10 };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 10 });
}

#[test]
fn test_min_sets_when_missing() {
    let update_expr = update([min([field("a")], 7)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! {};
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 7 });
}

#[test]
fn test_max_updates_when_higher() {
    let update_expr = update([max([field("a")], 20)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 10 };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 20 });
}

#[test]
fn test_max_noop_when_not_higher() {
    let update_expr = update([max([field("a")], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "a": 10 };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 10 });
}

#[test]
fn test_max_sets_when_missing() {
    let update_expr = update([max([field("a")], 21)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! {};
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "a": 21 });
}

// -----------------------------
// $currentDate
// -----------------------------
use crate::query::update::CurrentDateType;

#[test]
fn test_current_date_boolean_true_sets_date() {
    let update_expr = update([current_date([field("a")], CurrentDateType::Date)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! {};
    let updated = updater(doc).unwrap();
    assert!(matches!(updated.get("a"), Some(Bson::DateTime(_))));
}

#[test]
fn test_current_date_type_date_nested_creates_path() {
    let update_expr = update([current_date(
        [field("a"), field("b")],
        CurrentDateType::Date,
    )]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    match updated.get("a") {
        Some(Bson::Document(inner)) => {
            assert!(matches!(inner.get("b"), Some(Bson::DateTime(_))))
        }
        other => panic!("expected document at 'a', got {:?}", other),
    }
}

#[test]
fn test_current_date_type_timestamp() {
    let update_expr = update([current_date([field("ts")], CurrentDateType::Timestamp)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert!(matches!(updated.get("ts"), Some(Bson::Timestamp(_))));
}

// -----------------------------
// $addToSet
// -----------------------------

#[test]
fn test_add_to_set_single() {
    let update_expr = update([add_to_set_single([field("a")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    // missing -> create array with [1]
    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": [1] });

    // existing without value -> append
    let updated = updater(doc! { "a": [2] }).unwrap();
    assert_eq!(updated, doc! { "a": [2, 1] });

    // existing with value -> no duplicate
    let updated = updater(doc! { "a": [1, 2] }).unwrap();
    assert_eq!(updated, doc! { "a": [1, 2] });
}

#[test]
fn test_add_to_set_each_dedup_and_order() {
    let update_expr = update([add_to_set_each([field("a")], [1, 2, 2])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [2] }).unwrap();
    // 2 already exists, 1 is appended once
    assert_eq!(updated, doc! { "a": [2, 1] });
}

#[test]
fn test_add_to_set_on_non_array_errors() {
    let update_expr = update([add_to_set_single([field("a")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let result = updater(doc! { "a": "oops" });
    assert!(result.is_err());
}

#[test]
fn test_add_to_set_creates_missing_nested_document() {
    let update_expr = update([add_to_set_single([field("a"), field("b")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": { "b": [1] } });
}

#[test]
fn test_add_to_set_creates_missing_deeply_nested_path() {
    let update_expr = update([add_to_set_single([field("a"), field("b"), field("c")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": { "b": { "c": [1] } } });
}

#[test]
fn test_add_to_set_creates_missing_array_element_in_path() {
    let update_expr = update([add_to_set_single([field("a"), index(1), field("b")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [] }).unwrap();
    assert_eq!(updated, doc! { "a": [Bson::Null, { "b": [1] }] });
}

#[test]
fn test_add_to_set_each_creates_missing_nested_path() {
    let update_expr = update([add_to_set_each([field("x"), field("y")], [1, 2, 3])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "x": { "y": [1, 2, 3] } });
}

// -----------------------------
// $push
// -----------------------------
use crate::query::update_fn::{by_fields_sort, push_each_spec, push_single, push_spec};

#[test]
fn test_push_simple_and_create_array() {
    let update_expr = update([push_single([field("a")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    // create
    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": [1] });

    // append
    let updated = updater(doc! { "a": [1] }).unwrap();
    assert_eq!(updated, doc! { "a": [1, 1] });
}

#[test]
fn test_push_literal_document_with_modifier_like_key() {
    let update_expr = update([push_single(
        [field("items")],
        doc! { "name": "item1", "$slice": 5 },
    )]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "items": [] }).unwrap();
    assert_eq!(
        updated,
        doc! { "items": [ { "name": "item1", "$slice": 5 } ] }
    );
}

#[test]
fn test_push_with_position_each() {
    let spec = push_each_spec([10, 11], Some(1), None, None);
    let update_expr = update([push_spec([field("a")], spec)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [1, 2, 3] }).unwrap();
    assert_eq!(updated, doc! { "a": [1, 10, 11, 2, 3] });
}

#[test]
fn test_push_with_negative_position_each() {
    let spec = push_each_spec([10, 11], Some(-2), None, None);
    let update_expr = update([push_spec([field("a")], spec)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [1, 2, 3] }).unwrap();
    assert_eq!(updated, doc! { "a": [1, 10, 11, 2, 3] });
}

#[test]
fn test_push_with_negative_position_greater_then_array_length_each() {
    let spec = push_each_spec([10, 11], Some(-5), None, None);
    let update_expr = update([push_spec([field("a")], spec)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [1, 2, 3] }).unwrap();
    assert_eq!(updated, doc! { "a": [10, 11, 1, 2, 3] });
}

#[test]
fn test_push_with_sort_and_slice() {
    // Start with one quiz, push two, then sort by score desc and slice -2 (keep last two)
    let spec = push_each_spec(
        [doc! { "wk": 5, "score": 8 }, doc! { "wk": 6, "score": 7 }],
        None,
        Some(-2),
        Some(by_fields_sort(std::collections::BTreeMap::from([(
            "score".into(),
            -1,
        )]))),
    );
    let update_expr = update([push_spec([field("quizzes")], spec)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "quizzes": [ { "wk": 4, "score": 9 } ] }).unwrap();
    assert_eq!(
        updated,
        doc! { "quizzes": [ { "wk": 5, "score": 8 }, { "wk": 6, "score": 7 } ] }
    );
}

#[test]
fn test_push_on_non_array_errors() {
    let update_expr = update([push_single([field("a")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let result = updater(doc! { "a": "oops" });
    assert!(result.is_err());
}

#[test]
fn test_push_creates_missing_nested_document() {
    let update_expr = update([push_single([field("a"), field("b")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": { "b": [1] } });
}

#[test]
fn test_push_creates_missing_deeply_nested_path() {
    let update_expr = update([push_single([field("a"), field("b"), field("c")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": { "b": { "c": [1] } } });
}

#[test]
fn test_push_creates_missing_array_element_in_path() {
    let update_expr = update([push_single([field("a"), index(1), field("b")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [] }).unwrap();
    assert_eq!(updated, doc! { "a": [Bson::Null, { "b": [1] }] });
}

#[test]
fn test_push_creates_missing_nested_array_in_path() {
    let update_expr = update([push_single([field("a"), index(0), index(0)], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": [ [[1]] ] });
}

#[test]
fn test_push_creates_nested_path() {
    let update_expr = update([push_single([field("metadata"), field("history")], "event1")]);

    let updater = to_updater(&update_expr, false).unwrap();
    let updated = updater(doc! { "_id": 1,
    "item": "journal",
    "dim_cm": [ 14, 21 ] })
    .unwrap();

    assert_eq!(
        updated,
        doc! { "_id": 1,
        "item": "journal",
        "dim_cm": [ 14, 21 ],
        "metadata": { "history": ["event1"] } }
    );
}

// -----------------------------
// $pop
// -----------------------------
use crate::query::update::PopFrom;

#[test]
fn test_pop_first_and_last() {
    let update_expr_first = update([pop([field("a")], PopFrom::First)]);
    let update_expr_last = update([pop([field("a")], PopFrom::Last)]);
    let up_first = to_updater(&update_expr_first, false).unwrap();
    let up_last = to_updater(&update_expr_last, false).unwrap();

    let updated = up_first(doc! { "a": [1, 2, 3] }).unwrap();
    assert_eq!(updated, doc! { "a": [2, 3] });

    let updated = up_last(doc! { "a": [1, 2, 3] }).unwrap();
    assert_eq!(updated, doc! { "a": [1, 2] });
}

#[test]
fn test_pop_from_empty_or_missing_is_noop() {
    let update_expr = update([pop([field("a")], PopFrom::First)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": [] }).unwrap();
    assert_eq!(updated, doc! { "a": [] });

    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! {});
}

#[test]
fn test_pop_on_non_array_errors() {
    let update_expr = update([pop([field("a")], PopFrom::First)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let result = updater(doc! { "a": "oops" });
    assert!(result.is_err());
}

// -----------------------------
// $pull and $pullAll
// -----------------------------
use crate::query::expr_fn as ef;

#[test]
fn test_pull_equals_scalar() {
    let update_expr = update([pull_eq([field("a")], "x")]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": ["x", "y", "x"] }).unwrap();
    assert_eq!(updated, doc! { "a": ["y"] });
}

#[test]
fn test_pull_matches_operator_only() {
    let update_expr = update([pull_matches([field("scores")], ef::gte(ef::lit(80)))]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "scores": [70, 80, 90] }).unwrap();
    assert_eq!(updated, doc! { "scores": [70] });
}

#[test]
fn test_pull_matches_nested_document() {
    // pull documents matching { score: 8, wk: 5 }
    let criterion = ef::and([
        ef::field_filters(ef::field(["score"]), [ef::eq(ef::lit(8))]),
        ef::field_filters(ef::field(["wk"]), [ef::eq(ef::lit(5))]),
    ]);

    let update_expr = update([pull_matches([field("quizzes")], criterion)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated =
        updater(doc! { "quizzes": [ { "score": 8, "wk": 5 }, { "score": 7, "wk": 6 } ] }).unwrap();
    assert_eq!(updated, doc! { "quizzes": [ { "score": 7, "wk": 6 } ] });
}

#[test]
fn test_pull_all_removes_all_instances() {
    let update_expr = update([pull_all([field("a")], ["x", "z"])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let updated = updater(doc! { "a": ["x", "y", "x", "w"] }).unwrap();
    assert_eq!(updated, doc! { "a": ["y", "w"] });
}

#[test]
fn test_pull_on_non_array_errors() {
    let update_expr = update([pull_eq([field("a")], 1)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let result = updater(doc! { "a": "oops" });
    assert!(result.is_err());
}

// -----------------------------
// $bit
// -----------------------------
#[test]
fn test_bit_and_or_xor() {
    // AND
    let and_expr = update([bit([field("a")], Some(0b1010), None, None)]);
    let updater_and = to_updater(&and_expr, false).unwrap();
    let updated = updater_and(doc! { "a": 0b1100 }).unwrap();
    assert_eq!(updated, doc! { "a": 0b1000 });

    // OR
    let or_expr = update([bit([field("a")], None, Some(0b0001), None)]);
    let updater_or = to_updater(&or_expr, false).unwrap();
    let updated = updater_or(doc! { "a": 0b1000 }).unwrap();
    assert_eq!(updated, doc! { "a": 0b1001 });

    // XOR
    let xor_expr = update([bit([field("a")], None, None, Some(0b0011))]);
    let updater_xor = to_updater(&xor_expr, false).unwrap();
    let updated = updater_xor(doc! { "a": 0b1001 }).unwrap();
    assert_eq!(updated, doc! { "a": 0b1001 ^ 0b0011 });
}

#[test]
fn test_bit_on_missing_defaults_to_zero() {
    // OR 0b0010 to missing field => 0b0010
    let update_expr = update([bit([field("a")], None, Some(0b0010), None)]);
    let updater = to_updater(&update_expr, false).unwrap();
    let updated = updater(doc! {}).unwrap();
    assert_eq!(updated, doc! { "a": 0b0010 });
}

#[test]
fn test_bit_on_non_integer_errors() {
    let update_expr = update([bit([field("a")], Some(1), None, None)]);
    let updater = to_updater(&update_expr, false).unwrap();
    let result = updater(doc! { "a": "oops" });
    assert!(result.is_err());
}

// -----------------------------
// Positional operators: $[] and $[id] with arrayFilters
// -----------------------------
use crate::query::update_fn::{all, filter, update_with_filters};

#[test]
fn test_positional_all_elements_set_field() {
    // $set: { "grades.$[].mean": 100 }
    let update_expr = update([set([field("grades"), all(), field("mean")], 100)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "grades": [ { "mean": 70 }, { "mean": 80 } ] };
    let updated = updater(doc).unwrap();
    assert_eq!(
        updated,
        doc! { "grades": [ { "mean": 100 }, { "mean": 100 } ] }
    );
}

#[test]
fn test_filtered_positional_with_array_filters() {
    // $set: { "grades.$[elem].mean": 100 }, arrayFilters: [{ "elem.grade": { "$gte": 85 } }]
    let ops = [set([field("grades"), filter("elem"), field("mean")], 100)];
    let filters = [(
        "elem".to_string(),
        ef::field_filters(ef::field(["grade"]), [ef::gte(ef::lit(85))]),
    )];
    let update_expr = Arc::new(update_with_filters(ops, filters));
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "grades": [ { "grade": 90, "mean": 70 }, { "grade": 80, "mean": 60 } ] };
    let updated = updater(doc).unwrap();
    assert_eq!(
        updated,
        doc! { "grades": [ { "grade": 90, "mean": 100 }, { "grade": 80, "mean": 60 } ] }
    );
}

#[test]
fn test_positional_inc_on_scalar_array() {
    // $inc: { "scores.$[]": 5 }
    let update_expr = update([inc([field("scores"), all()], 5)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "scores": [10, 20, 30] };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "scores": [15, 25, 35] });
}

#[test]
fn test_positional_unset_field() {
    // $unset: { "items.$[].b": "" }
    let update_expr = update([unset([field("items"), all(), field("b")])]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "items": [ { "a": 1, "b": 2 }, { "a": 3, "b": 4 } ] };
    let updated = updater(doc).unwrap();
    assert_eq!(updated, doc! { "items": [ { "a": 1 }, { "a": 3 } ] });
}

#[test]
fn test_positional_push_to_nested_array() {
    // $push: { "grades.$[].scores": 100 }
    use crate::query::update_fn::push_single;
    let update_expr = update([push_single([field("grades"), all(), field("scores")], 100)]);
    let updater = to_updater(&update_expr, false).unwrap();
    let doc = doc! { "grades": [ { "scores": [70] }, { "scores": [80] } ] };
    let updated = updater(doc).unwrap();
    assert_eq!(
        updated,
        doc! { "grades": [ { "scores": [70, 100] }, { "scores": [80, 100] } ] }
    );
}

#[test]
fn test_nested_positional_operators_set() {
    // $set: { "schools.$[].classes.$[].students.$[].passed": true }
    let update_expr = update([set(
        [
            field("schools"),
            all(),
            field("classes"),
            all(),
            field("students"),
            all(),
            field("passed"),
        ],
        true,
    )]);
    let updater = to_updater(&update_expr, false).unwrap();
    let doc = doc! {
        "schools": [
            { "classes": [
                { "students": [ {"name": "A"}, {"name": "B"} ] },
                { "students": [ {"name": "C"} ] }
            ] },
            { "classes": [
                { "students": [ {"name": "D"} ] }
            ] }
        ]
    };
    let updated = updater(doc).unwrap();
    assert_eq!(
        updated,
        doc! {
            "schools": [
                { "classes": [
                    { "students": [ {"name": "A", "passed": true}, {"name": "B", "passed": true} ] },
                    { "students": [ {"name": "C", "passed": true} ] }
                ] },
                { "classes": [
                    { "students": [ {"name": "D", "passed": true} ] }
                ] }
            ]
        }
    );
}

#[test]
fn test_positional_on_empty_array_is_noop() {
    let update_expr = update([set([field("grades"), all(), field("score")], 100)]);
    let updater = to_updater(&update_expr, false).unwrap();
    let doc = doc! { "grades": [] };
    let original_doc = doc.clone();
    let updated = updater(doc).unwrap();
    assert_eq!(updated, original_doc);
}

#[test]
fn test_positional_on_missing_field_is_noop() {
    let update_expr = update([set([field("grades"), all(), field("score")], 100)]);
    let updater = to_updater(&update_expr, false).unwrap();
    let doc = doc! { "name": "test" };
    let original_doc = doc.clone();
    let updated = updater(doc).unwrap();
    assert_eq!(updated, original_doc);
}

#[test]
fn test_positional_on_non_array_field_errors() {
    let update_expr = update([set([field("grades"), all(), field("score")], 100)]);
    let updater = to_updater(&update_expr, false).unwrap();
    let doc = doc! { "grades": { "not": "an array" } };
    let result = updater(doc);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Cannot apply positional operator ('$[]') to a document"
    );
}

// -----------------------------
// $setOnInsert
// -----------------------------
use crate::query::update_fn::set_on_insert;

#[test]
fn test_set_on_insert_applies_on_insert() {
    let update_expr = update([set_on_insert([field("a")], 10)]);
    let updater = to_updater(&update_expr, true).unwrap();

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 10 });
}

#[test]
fn test_set_on_insert_ignored_on_update() {
    let update_expr = update([set_on_insert([field("a")], 10)]);
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "b": 5 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "b": 5 });
}

#[test]
fn test_set_on_insert_with_existing_field_on_insert() {
    let update_expr = update([set_on_insert([field("a")], 20)]);
    let updater = to_updater(&update_expr, true).unwrap();

    let doc = doc! { "a": 10 };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 20 });
}

#[test]
fn test_set_on_insert_nested_path() {
    let update_expr = update([set_on_insert([field("a"), field("b")], "nested")]);
    let updater = to_updater(&update_expr, true).unwrap();

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": { "b": "nested" } });
}

#[test]
fn test_set_on_insert_combined_with_set() {
    let update_expr = update([
        set([field("updated")], true),
        set_on_insert([field("created_at")], "2024-01-01"),
    ]);

    // On insert: both $set and $setOnInsert apply
    let updater_insert = to_updater(&update_expr, true).unwrap();
    let doc = doc! {};
    let updated_doc = updater_insert(doc).unwrap();
    assert_eq!(
        updated_doc,
        doc! { "updated": true, "created_at": "2024-01-01" }
    );

    // On update: only $set applies
    let updater_update = to_updater(&update_expr, false).unwrap();
    let doc = doc! { "existing": 1 };
    let updated_doc = updater_update(doc).unwrap();
    assert_eq!(updated_doc, doc! { "existing": 1, "updated": true });
}

#[test]
fn test_set_on_insert_array_element() {
    let update_expr = update([set_on_insert([field("arr"), index(1)], 100)]);
    let updater = to_updater(&update_expr, true).unwrap();

    let doc = doc! { "arr": [0] };
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "arr": [0, 100] });
}

#[test]
fn test_set_on_insert_creates_array_on_insert() {
    let update_expr = update([set_on_insert([field("arr"), index(0)], "first")]);
    let updater = to_updater(&update_expr, true).unwrap();

    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "arr": ["first"] });
}

#[test]
fn test_multiple_set_on_insert_operations() {
    let update_expr = update([
        set_on_insert([field("a")], 1),
        set_on_insert([field("b")], 2),
        set_on_insert([field("c")], 3),
    ]);

    let updater = to_updater(&update_expr, true).unwrap();
    let doc = doc! {};
    let updated_doc = updater(doc).unwrap();
    assert_eq!(updated_doc, doc! { "a": 1, "b": 2, "c": 3 });

    let updater_no_insert = to_updater(&update_expr, false).unwrap();
    let doc = doc! { "x": 99 };
    let updated_doc = updater_no_insert(doc).unwrap();
    assert_eq!(updated_doc, doc! { "x": 99 });
}

#[test]
fn test_filtered_positional_on_heterogeneous_array() {
    // only update documents where grade is >= 85
    let ops = [set([field("grades"), filter("elem"), field("mean")], 100)];
    let filters = [(
        "elem".to_string(),
        ef::field_filters(ef::field(["grade"]), [ef::gte(ef::lit(85))]),
    )];
    let update_expr = Arc::new(update_with_filters(ops, filters));
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "grades": [
        { "grade": 90, "mean": 70 },
        "not a document",
        { "grade": 80, "mean": 60 },
        Bson::Null
    ]};
    let updated = updater(doc).unwrap();
    assert_eq!(
        updated,
        doc! { "grades": [
            { "grade": 90, "mean": 100 },
            "not a document",
            { "grade": 80, "mean": 60 },
            Bson::Null
        ]}
    );
}

#[test]
fn test_filtered_positional_with_no_matches_is_noop() {
    let ops = [set([field("grades"), filter("elem"), field("mean")], 100)];
    let filters = [(
        "elem".to_string(),
        ef::field_filters(ef::field(["grade"]), [ef::gte(ef::lit(95))]),
    )];
    let update_expr = Arc::new(update_with_filters(ops, filters));
    let updater = to_updater(&update_expr, false).unwrap();

    let doc = doc! { "grades": [ { "grade": 90, "mean": 70 }, { "grade": 80, "mean": 60 } ] };
    let original_doc = doc.clone();
    let updated = updater(doc).unwrap();
    assert_eq!(updated, original_doc);
}
