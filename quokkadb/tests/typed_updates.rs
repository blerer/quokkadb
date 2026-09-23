mod common;

use quokkadb::collection::ReturnDocument;
use quokkadb::{Filter, PushOptions, QuokkaDB, QuokkaDocument, QuokkaType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    age: i32,
    active: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
struct UserName {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
struct Score {
    label: String,
    rank: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
struct Profile {
    name: String,
    score: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct UpdateFeatures {
    #[serde(rename = "_id")]
    id: u64,
    total: i32,
    flags: i64,
    created_at: bson::DateTime,
    updated_at: bson::Timestamp,
    nickname: Option<String>,
    profile: Profile,
    metrics: BTreeMap<String, i32>,
    ratio: f64,
    tags: Vec<String>,
    scores: Vec<Score>,
}

fn users() -> Vec<User> {
    vec![
        User {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
            active: true,
        },
        User {
            id: 2,
            name: "Bob".to_string(),
            age: 40,
            active: true,
        },
        User {
            id: 3,
            name: "Cara".to_string(),
            age: 20,
            active: false,
        },
    ]
}

fn setup() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db.typed_collection::<User>("users").create_if_missing();
    collection.insert_many(users()).unwrap();
    (dir, db)
}

#[test]
fn typed_update_one_composes_updates_and_honors_sort() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let result = collection
        .update_one_with(
            |user| user.active.eq(true),
            |user| user.name.set("Robert").and(user.age.inc(1)),
        )
        .sort(|user| user.age.desc())
        .sync()
        .execute()
        .unwrap();

    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);
    assert_eq!(
        collection.find_one(|user| user.id.eq(2_u64)).unwrap(),
        Some(User {
            id: 2,
            name: "Robert".to_string(),
            age: 41,
            active: true,
        })
    );
    assert_eq!(
        collection
            .update_one(|user| user.id.eq(99_u64), |user| user.age.inc(1))
            .unwrap()
            .matched_count,
        0
    );
}

#[test]
fn typed_update_many_and_upsert_report_write_results() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let result = collection
        .update_many(|user| user.active.eq(true), |user| user.age.inc(5))
        .unwrap();
    assert_eq!(result.matched_count, 2);
    assert_eq!(result.modified_count, 2);

    let result = collection
        .update_one_with(
            |user| user.id.eq(4_u64),
            |user| {
                user.name
                    .set("Dora")
                    .and(user.age.set(25))
                    .and(user.active.set(true))
            },
        )
        .upsert(true)
        .execute()
        .unwrap();
    assert_eq!(result.matched_count, 0);
    assert_eq!(result.modified_count, 0);
    assert_eq!(result.upserted_id, Some(4_i64.into()));
    assert_eq!(
        collection
            .find_one(|user| user.id.eq(4_u64))
            .unwrap()
            .unwrap(),
        User {
            id: 4,
            name: "Dora".to_string(),
            age: 25,
            active: true,
        }
    );
}

#[test]
fn typed_filter_all_updates_every_document() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let result = collection
        .update_many(|_| Filter::all(), |user| user.active.set(false))
        .unwrap();

    assert_eq!(result.matched_count, 3);
    assert_eq!(result.modified_count, 3);
    assert_eq!(
        collection
            .find_all()
            .sort(|user| user.id.asc())
            .select(|user| user.active)
            .execute_collect()
            .unwrap(),
        vec![false, false, false]
    );
}

#[test]
fn typed_find_one_and_update_projects_the_requested_return_document() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let updated: UserName = collection
        .find_one_and_update_with(|user| user.active.eq(true), |user| user.name.set("Updated"))
        .sort(|user| user.age.desc())
        .return_document(ReturnDocument::After)
        .include_without_id(|user| user.name)
        .execute()
        .unwrap()
        .unwrap();

    assert_eq!(
        updated,
        UserName {
            name: "Updated".to_string()
        }
    );
    assert_eq!(
        collection
            .find_one_and_update(|user| user.id.eq(99_u64), |user| user.age.inc(1))
            .unwrap(),
        None
    );

    let upserted = collection
        .find_one_and_update_with(
            |user| user.id.eq(4_u64),
            |user| {
                user.name
                    .set("Dora")
                    .and(user.age.set(25))
                    .and(user.active.set(true))
            },
        )
        .upsert(true)
        .return_document(ReturnDocument::After)
        .execute()
        .unwrap()
        .unwrap();
    assert_eq!(
        upserted,
        User {
            id: 4,
            name: "Dora".to_string(),
            age: 25,
            active: true,
        }
    );
}

#[test]
fn typed_find_one_and_update_returns_the_previous_document_by_default() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let previous = collection
        .find_one_and_update(|user| user.id.eq(1_u64), |user| user.age.inc(1))
        .unwrap()
        .unwrap();

    assert_eq!(previous.age, 30);
    assert_eq!(
        collection
            .find_one(|user| user.id.eq(1_u64))
            .unwrap()
            .unwrap()
            .age,
        31
    );
}

#[test]
fn typed_replacements_return_and_persist_typed_documents() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let result = collection
        .replace_one_with(
            |user| user.id.eq(1_u64),
            User {
                id: 1,
                name: "Alicia".to_string(),
                age: 31,
                active: false,
            },
        )
        .unwrap()
        .sync()
        .execute()
        .unwrap();
    assert_eq!(result.matched_count, 1);

    let previous = collection
        .find_one_and_replace(
            |user| user.id.eq(2_u64),
            User {
                id: 2,
                name: "Bobby".to_string(),
                age: 41,
                active: false,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(previous.name, "Bob");

    let replacement = collection
        .find_one_and_replace_with(
            |user| user.id.eq(3_u64),
            User {
                id: 3,
                name: "Carla".to_string(),
                age: 21,
                active: true,
            },
        )
        .unwrap()
        .return_document(ReturnDocument::After)
        .select(|user| (user.name, user.age))
        .execute()
        .unwrap();
    assert_eq!(replacement, Some(("Carla".to_string(), 21)));

    let upserted = collection
        .find_one_and_replace_with(
            |user| user.id.eq(4_u64),
            User {
                id: 4,
                name: "Dora".to_string(),
                age: 25,
                active: true,
            },
        )
        .unwrap()
        .upsert(true)
        .return_document(ReturnDocument::After)
        .execute()
        .unwrap()
        .unwrap();
    assert_eq!(
        upserted,
        User {
            id: 4,
            name: "Dora".to_string(),
            age: 25,
            active: true,
        }
    );
}

#[test]
fn typed_updates_support_scalar_temporal_and_bitwise_operators() {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db
        .typed_collection::<UpdateFeatures>("update_features")
        .create_if_missing();
    collection
        .insert_one(UpdateFeatures {
            id: 1,
            total: 6,
            flags: 0b1100,
            created_at: bson::DateTime::from_millis(0),
            updated_at: bson::Timestamp {
                time: 0,
                increment: 0,
            },
            nickname: Some("initial".to_string()),
            profile: Profile {
                name: "initial".to_string(),
                score: 1,
            },
            metrics: BTreeMap::from([("views".to_string(), 1)]),
            ratio: 1.5,
            tags: vec!["red".to_string(), "blue".to_string()],
            scores: vec![],
        })
        .unwrap();

    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .total
                    .mul(3)
                    .and(feature.flags.bit(Some(0b1010), Some(0b0001), None))
                    .and(feature.created_at.current_date())
                    .and(feature.updated_at.current_timestamp())
            },
        )
        .unwrap();

    let updated = collection
        .find_one(|feature| feature.id.eq(1_u64))
        .unwrap()
        .unwrap();
    assert_eq!(updated.total, 18);
    assert_eq!(updated.flags, 0b1001);
    assert!(updated.created_at.timestamp_millis() > 0);
    assert!(updated.updated_at.time > 0);

    collection
        .update_one_with(
            |feature| feature.id.eq(2_u64),
            |feature| {
                feature
                    .total
                    .set(1)
                    .and(feature.flags.set(0_i64))
                    .and(feature.tags.set(vec!["new".to_string()]))
                    .and(feature.scores.set(vec![]))
                    .and(feature.profile.set(Profile {
                        name: "new".to_string(),
                        score: 0,
                    }))
                    .and(feature.metrics.set(BTreeMap::new()))
                    .and(feature.ratio.set(0.0))
                    .and(
                        feature
                            .created_at
                            .set_on_insert(bson::DateTime::from_millis(5)),
                    )
                    .and(feature.updated_at.set_on_insert(bson::Timestamp {
                        time: 1,
                        increment: 0,
                    }))
            },
        )
        .upsert(true)
        .execute()
        .unwrap();

    assert_eq!(
        collection
            .find_one(|feature| feature.id.eq(2_u64))
            .unwrap()
            .unwrap()
            .created_at,
        bson::DateTime::from_millis(5)
    );
}

#[test]
fn typed_updates_cover_conditional_optional_and_upsert_edge_cases() {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db
        .typed_collection::<UpdateFeatures>("update_features")
        .create_if_missing();
    collection
        .insert_one(UpdateFeatures {
            id: 1,
            total: 10,
            flags: 5,
            created_at: bson::DateTime::from_millis(0),
            updated_at: bson::Timestamp {
                time: 0,
                increment: 0,
            },
            nickname: Some("initial".to_string()),
            profile: Profile {
                name: "initial".to_string(),
                score: 1,
            },
            metrics: BTreeMap::from([("views".to_string(), 1)]),
            ratio: 1.5,
            tags: vec![],
            scores: vec![],
        })
        .unwrap();

    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .total
                    .min(8)
                    .and(feature.flags.max(8_i64))
                    .and(feature.nickname.unset())
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature.total.min(12).and(feature.flags.max(3_i64)).and(
                    feature
                        .created_at
                        .set_on_insert(bson::DateTime::from_millis(99)),
                )
            },
        )
        .unwrap();

    let existing = collection
        .find_one(|feature| feature.id.eq(1_u64))
        .unwrap()
        .unwrap();
    assert_eq!(existing.total, 8);
    assert_eq!(existing.flags, 8);
    assert_eq!(existing.nickname, None);
    assert_eq!(existing.created_at, bson::DateTime::from_millis(0));

    let no_upsert = collection
        .update_one(
            |feature| feature.id.eq(99_u64),
            |feature| feature.total.set_on_insert(1),
        )
        .unwrap();
    assert_eq!(no_upsert.matched_count, 0);
    assert_eq!(
        collection
            .find_one(|feature| feature.id.eq(99_u64))
            .unwrap(),
        None
    );

    let upsert = collection
        .update_many_with(
            |feature| feature.id.eq(2_u64),
            |feature| {
                feature
                    .total
                    .set_on_insert(1)
                    .and(feature.flags.set_on_insert(2_i64))
                    .and(
                        feature
                            .created_at
                            .set_on_insert(bson::DateTime::from_millis(3)),
                    )
                    .and(feature.updated_at.set_on_insert(bson::Timestamp {
                        time: 4,
                        increment: 0,
                    }))
                    .and(feature.tags.set_on_insert(vec!["new".to_string()]))
                    .and(feature.scores.set_on_insert(vec![]))
                    .and(feature.profile.set_on_insert(Profile {
                        name: "new".to_string(),
                        score: 0,
                    }))
                    .and(feature.metrics.set_on_insert(BTreeMap::new()))
                    .and(feature.ratio.set_on_insert(0.0))
            },
        )
        .upsert(true)
        .execute()
        .unwrap();
    assert_eq!(upsert.matched_count, 0);
    assert_eq!(upsert.upserted_id, Some(2_i64.into()));
    assert_eq!(
        collection
            .find_one(|feature| feature.id.eq(2_u64))
            .unwrap()
            .unwrap()
            .tags,
        vec!["new"]
    );
}

#[test]
fn typed_updates_cover_nested_map_indexed_and_numeric_paths() {
    fn assert_numeric<T: quokkadb::NumericValue>() {}
    assert_numeric::<bson::Decimal128>();

    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db
        .typed_collection::<UpdateFeatures>("update_features")
        .create_if_missing();
    collection
        .insert_one(UpdateFeatures {
            id: 1,
            total: 0b1100,
            flags: 0,
            created_at: bson::DateTime::from_millis(0),
            updated_at: bson::Timestamp {
                time: 0,
                increment: 0,
            },
            nickname: None,
            profile: Profile {
                name: "initial".to_string(),
                score: 1,
            },
            metrics: BTreeMap::from([("views".to_string(), 1)]),
            ratio: 1.5,
            tags: vec!["first".to_string()],
            scores: vec![],
        })
        .unwrap();

    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .tags
                    .at(0)
                    .set("updated")
                    .and(feature.profile.name.set("renamed"))
                    .and(feature.metrics.key("views").inc(2))
                    .and(feature.ratio.inc(0.5).and(feature.ratio.mul(2.0)))
                    .and(feature.total.bit(None, None, Some(0b0011)))
                    .and(feature.nickname.set("present"))
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |_| UpdateFeatures::update(|feature| feature.profile.score.inc(2)),
        )
        .unwrap();

    let updated = collection
        .find_one(|feature| feature.id.eq(1_u64))
        .unwrap()
        .unwrap();
    assert_eq!(updated.tags, vec!["updated"]);
    assert_eq!(updated.profile.name, "renamed");
    assert_eq!(updated.profile.score, 3);
    assert_eq!(updated.metrics, BTreeMap::from([("views".to_string(), 3)]));
    assert_eq!(updated.ratio, 4.0);
    assert_eq!(updated.total, 0b1111);
    assert_eq!(updated.nickname.as_deref(), Some("present"));
}

#[test]
fn typed_updates_support_array_operators_and_push_modifiers() {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db
        .typed_collection::<UpdateFeatures>("update_features")
        .create_if_missing();
    collection
        .insert_one(UpdateFeatures {
            id: 1,
            total: 0,
            flags: 0,
            created_at: bson::DateTime::from_millis(0),
            updated_at: bson::Timestamp {
                time: 0,
                increment: 0,
            },
            nickname: Some("initial".to_string()),
            profile: Profile {
                name: "initial".to_string(),
                score: 1,
            },
            metrics: BTreeMap::from([("views".to_string(), 1)]),
            ratio: 1.5,
            tags: vec!["red".to_string(), "blue".to_string()],
            scores: vec![
                Score {
                    label: "old".to_string(),
                    rank: 2,
                },
                Score {
                    label: "low".to_string(),
                    rank: 1,
                },
            ],
        })
        .unwrap();

    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .tags
                    .add_to_set("red")
                    .and(feature.tags.add_to_set_each(["green", "blue"]))
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| feature.tags.push("orange"),
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature.tags.push_each(
                    ["yellow", "amber"],
                    PushOptions::new().position(0).slice(4).sort_ascending(),
                )
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| feature.tags.pop_first().and(feature.tags.pop_last()),
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .tags
                    .pull("green")
                    .and(feature.tags.pull_all(["blue"]))
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature.scores.push_each_with(
                    [
                        Score {
                            label: "high".to_string(),
                            rank: 3,
                        },
                        Score {
                            label: "middle".to_string(),
                            rank: 2,
                        },
                    ],
                    |options| {
                        options
                            .position(0)
                            .slice(3)
                            .sort_by(|score| score.rank.desc())
                    },
                )
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| feature.scores.pull_where(|score| score.rank.lt(2)),
        )
        .unwrap();

    let updated = collection
        .find_one(|feature| feature.id.eq(1_u64))
        .unwrap()
        .unwrap();
    assert!(updated.tags.is_empty());
    assert_eq!(
        updated.scores,
        vec![
            Score {
                label: "high".to_string(),
                rank: 3,
            },
            Score {
                label: "middle".to_string(),
                rank: 2,
            },
            Score {
                label: "old".to_string(),
                rank: 2,
            },
        ]
    );

    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| feature.tags.pop_first(),
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .tags
                    .push_each(["a", "c", "b"], PushOptions::new().sort_descending())
            },
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| feature.tags.pull("missing"),
        )
        .unwrap();
    collection
        .update_one(
            |feature| feature.id.eq(1_u64),
            |feature| {
                feature
                    .tags
                    .pull_all(["missing"])
                    .and(feature.tags.add_to_set_each(std::iter::empty::<String>()))
            },
        )
        .unwrap();

    assert_eq!(
        collection
            .find_one(|feature| feature.id.eq(1_u64))
            .unwrap()
            .unwrap()
            .tags,
        vec!["c", "b", "a"]
    );
}
