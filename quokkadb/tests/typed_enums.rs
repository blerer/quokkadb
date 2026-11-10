mod common;

use bson::doc;
use quokkadb::{QuokkaDB, QuokkaDocument, QuokkaScalar, QuokkaType};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
#[serde(rename_all = "snake_case")]
enum Shape {
    Pending,
    Circle {
        #[serde(rename = "radius_cm")]
        radius: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
    Rectangle {
        width: f64,
        height: f64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
struct Person {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
enum Value {
    Integer(i64),
    Person(Person),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
enum NestedValue {
    Empty,
    Measured { amount: f64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
enum NestedShape {
    Empty,
    WithValue { value: NestedValue },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, QuokkaScalar)]
#[serde(rename_all = "snake_case")]
enum Status {
    Active,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct Page {
    #[serde(rename = "_id")]
    id: u64,
    shape: Shape,
    value: Value,
    status: Status,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct NestedPage {
    #[serde(rename = "_id")]
    id: u64,
    shape: NestedShape,
}

fn setup() -> (TempDir, QuokkaDB) {
    let directory = TempDir::new().unwrap();
    let db = common::open_db(directory.path());
    (directory, db)
}

fn circle(id: u64, radius: f64) -> Page {
    Page {
        id,
        shape: Shape::Circle { radius, note: None },
        value: Value::Integer(42),
        status: Status::Active,
    }
}

#[test]
fn typed_enums_use_serde_values_and_variant_paths() {
    let (_directory, db) = setup();
    let collection = db.typed_collection::<Page>("pages").create_if_missing();
    collection
        .insert_many([
            circle(1, 5.0),
            Page {
                id: 2,
                shape: Shape::Rectangle {
                    width: 10.0,
                    height: 20.0,
                },
                value: Value::Person(Person {
                    name: "Alice".to_string(),
                }),
                status: Status::Active,
            },
            Page {
                id: 3,
                shape: Shape::Pending,
                value: Value::Integer(7),
                status: Status::Disabled,
            },
        ])
        .unwrap();

    let stored = db
        .collection("pages")
        .find_one(doc! { "_id": 1_i64 })
        .unwrap()
        .unwrap();
    assert_eq!(
        stored,
        doc! {
            "_id": 1_i64,
            "shape": { "circle": { "radius_cm": 5.0_f64 } },
            "value": { "Integer": 42_i64 },
            "status": "active",
        }
    );

    let circle_radii: Vec<Option<f64>> = collection
        .find_all()
        .sort(|page| page.id.asc())
        .select(|page| page.shape.circle.radius.clone())
        .execute_collect()
        .unwrap();
    assert_eq!(circle_radii, vec![Some(5.0), None, None]);

    let integer_values: Vec<Option<i64>> = collection
        .find_all()
        .sort(|page| page.id.asc())
        .select(|page| page.value.integer.clone())
        .execute_collect()
        .unwrap();
    assert_eq!(integer_values, vec![Some(42), None, Some(7)]);

    let notes: Vec<Option<String>> = collection
        .find_all()
        .sort(|page| page.id.asc())
        .select(|page| page.shape.circle.note.clone())
        .execute_collect()
        .unwrap();
    assert_eq!(notes, vec![None, None, None]);

    let circle_ids: Vec<u64> = collection
        .find(|page| page.shape.circle.radius.gt(4.0))
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert_eq!(circle_ids, vec![1]);

    let rectangle_ids: Vec<u64> = collection
        .find(|page| page.shape.rectangle.width.eq(10.0))
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert_eq!(rectangle_ids, vec![2]);

    let integer_ids: Vec<u64> = collection
        .find(|page| page.value.integer.eq(42_i64))
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert_eq!(integer_ids, vec![1]);

    let person_ids: Vec<u64> = collection
        .find(|page| page.value.person.name.eq("Alice"))
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert_eq!(person_ids, vec![2]);

    let active_ids: Vec<u64> = collection
        .find(|page| page.status.eq(Status::Active))
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert_eq!(active_ids, vec![1, 2]);

    let pending_ids: Vec<u64> = collection
        .find(|page| page.shape.eq(Shape::Pending))
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert_eq!(pending_ids, vec![3]);

    let omitted_note_ids: Vec<u64> = collection
        .find(|page| page.shape.circle.note.exists())
        .select(|page| page.id)
        .execute_collect()
        .unwrap();
    assert!(omitted_note_ids.is_empty());
}

#[test]
fn nested_enum_selections_flatten_variant_presence() {
    let (_directory, db) = setup();
    let collection = db
        .typed_collection::<NestedPage>("nested_pages")
        .create_if_missing();
    collection
        .insert_many([
            NestedPage {
                id: 1,
                shape: NestedShape::WithValue {
                    value: NestedValue::Measured { amount: 2.5 },
                },
            },
            NestedPage {
                id: 2,
                shape: NestedShape::WithValue {
                    value: NestedValue::Empty,
                },
            },
            NestedPage {
                id: 3,
                shape: NestedShape::Empty,
            },
        ])
        .unwrap();

    let amounts: Vec<Option<f64>> = collection
        .find_all()
        .sort(|page| page.id.asc())
        .select(|page| page.shape.with_value.value.measured.amount.clone())
        .execute_collect()
        .unwrap();
    assert_eq!(amounts, vec![Some(2.5), None, None]);
}

#[test]
fn typed_enum_variant_index_entries_follow_replacements() {
    let (_directory, db) = setup();
    let collection = db.typed_collection::<Page>("pages").create_if_missing();
    collection.insert_one(circle(1, 5.0)).unwrap();

    let circle_index = collection
        .create_index(|page| page.shape.circle.radius.index_asc())
        .unwrap();
    let rectangle_index = collection
        .create_index(|page| page.shape.rectangle.width.index_asc())
        .unwrap();

    assert_eq!(
        collection
            .find(|page| page.shape.circle.radius.eq(5.0))
            .hint(circle_index.clone())
            .select(|page| page.id)
            .execute_collect()
            .unwrap(),
        vec![1]
    );

    collection
        .replace_one(
            |page| page.id.eq(1_u64),
            Page {
                id: 1,
                shape: Shape::Rectangle {
                    width: 99.0,
                    height: 20.0,
                },
                value: Value::Integer(42),
                status: Status::Active,
            },
        )
        .unwrap();

    assert!(
        collection
            .find(|page| page.shape.circle.radius.eq(5.0))
            .hint(circle_index.clone())
            .execute_collect()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        collection
            .find(|page| page.shape.rectangle.width.eq(99.0))
            .hint(rectangle_index.clone())
            .select(|page| page.id)
            .execute_collect()
            .unwrap(),
        vec![1]
    );

    collection
        .replace_one(|page| page.id.eq(1_u64), circle(1, 7.0))
        .unwrap();

    assert!(
        collection
            .find(|page| page.shape.rectangle.width.eq(99.0))
            .hint(rectangle_index)
            .execute_collect()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        collection
            .find(|page| page.shape.circle.radius.eq(7.0))
            .hint(circle_index)
            .select(|page| page.id)
            .execute_collect()
            .unwrap(),
        vec![1]
    );
}
