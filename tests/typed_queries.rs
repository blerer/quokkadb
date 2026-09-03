mod common;

use bson::doc;
use quokkadb::error::Error;
use quokkadb::{QuokkaDB, QuokkaDocument, QuokkaType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tempfile::TempDir;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    age: i32,
    active: bool,
}

#[derive(Debug, PartialEq, Deserialize)]
struct UserSummary {
    name: String,
    age: i32,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserSummaryWithoutId {
    name: String,
    age: i32,
}

#[derive(Debug, PartialEq, Deserialize)]
struct PublicUser {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    age: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct OptionalUser {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    nickname: Option<String>,
    address: Option<Address>,
}

// These types are the baseline for nested typed-query support. They deliberately
// use Serde-only derives: embedded values are not collections and have no `_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
struct Address {
    city: String,
    postal_code: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaType)]
struct Order {
    total: i64,
    shipping: Address,
}

#[derive(Debug, PartialEq, Deserialize)]
struct AddressOnly {
    address: Address,
}

#[derive(Debug, PartialEq, Deserialize)]
struct IndexedOrders {
    orders: BTreeMap<String, Order>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, QuokkaDocument)]
struct NestedUser {
    #[quokka(id)]
    #[serde(rename = "_id")]
    id: u64,
    address: Address,
    orders: Vec<Order>,
    tags: Vec<String>,
    settings: BTreeMap<String, String>,
}

fn setup() -> (TempDir, QuokkaDB) {
    let dir = TempDir::new().unwrap();
    let db = common::open_db(dir.path());
    let collection = db.typed_collection::<User>("users").create_if_missing();
    collection
        .insert_many([
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
        ])
        .unwrap();
    (dir, db)
}

#[test]
fn typed_find_filters_documents() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let users: Vec<User> = collection
        .find(|user| user.active.eq(true).and(user.age.gte(35)))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        users,
        vec![User {
            id: 2,
            name: "Bob".to_string(),
            age: 40,
            active: true,
        }]
    );
}

#[test]
fn typed_find_sorts_and_paginates_documents() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let users: Vec<_> = collection
        .find(|user| user.age.gte(20))
        .sort(|user| user.age.desc())
        .skip(1)
        .limit(1)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(users[0].id, 1);
}

#[test]
fn typed_find_supports_composed_sorts() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let ids: Vec<u64> = collection
        .find(|user| user.age.gte(20))
        .sort(|user| user.active.desc().then(user.age.asc()))
        .select(|user| user.id)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn typed_find_one_supports_sort_and_returns_none_when_no_document_matches() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let youngest_active = collection
        .find_one_with(|user| user.active.eq(true))
        .sort(|user| user.age.asc())
        .execute()
        .unwrap()
        .unwrap();
    assert_eq!(youngest_active.id, 1);

    assert!(
        collection
            .find_one(|user| user.name.eq("Dora"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn typed_find_reports_document_deserialization_errors() {
    let (_dir, db) = setup();
    let collection = db.collection("users");
    collection
        .insert_one(doc! {
            "_id": 4_i64,
            "name": "Dora",
            "age": "not-a-number",
            "active": true,
        })
        .unwrap();

    let mut output = db
        .typed_collection::<User>("users")
        .find(|user| user.id.eq(4_u64))
        .execute()
        .unwrap();

    assert!(matches!(output.next().unwrap(), Err(Error::BsonError(_))));
}

#[test]
fn typed_find_projects_into_another_struct() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let summaries: Vec<UserSummary> = collection
        .find(|user| user.active.eq(true))
        .sort(|user| user.id.asc())
        .include(|user| (user.name, user.age))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        summaries,
        vec![
            UserSummary {
                name: "Alice".to_string(),
                age: 30,
            },
            UserSummary {
                name: "Bob".to_string(),
                age: 40,
            },
        ]
    );
}

#[test]
fn typed_find_include_preserves_the_implicit_id() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let users: Vec<PublicUser> = collection
        .find(|user| user.id.eq(1_u64))
        .include(|user| (user.name, user.age))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        users,
        vec![PublicUser {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
        }]
    );
}

#[test]
fn typed_find_can_include_fields_without_the_implicit_id() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let summaries: Vec<UserSummaryWithoutId> = collection
        .find(|user| user.id.eq(1_u64))
        .include_without_id(|user| (user.name, user.age))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        summaries,
        vec![UserSummaryWithoutId {
            name: "Alice".to_string(),
            age: 30,
        }]
    );
}

#[test]
fn typed_find_one_supports_inclusion_projections() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let user: PublicUser = collection
        .find_one_with(|user| user.active.eq(true))
        .include(|user| (user.name, user.age))
        .sort(|user| user.age.asc())
        .execute()
        .unwrap()
        .unwrap();

    assert_eq!(
        user,
        PublicUser {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
        }
    );
}

#[test]
fn typed_find_one_can_include_fields_without_the_implicit_id() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let user: UserSummaryWithoutId = collection
        .find_one_with(|user| user.id.eq(1_u64))
        .include_without_id(|user| (user.name, user.age))
        .execute()
        .unwrap()
        .unwrap();

    assert_eq!(
        user,
        UserSummaryWithoutId {
            name: "Alice".to_string(),
            age: 30,
        }
    );
}

#[test]
fn typed_find_one_supports_exclusion_projections() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let user = collection.find_one(|user| user.id.eq(1_u64)).unwrap();
    assert_eq!(user.unwrap().active, true);

    let public_user: PublicUser = collection
        .find_one_with(|user| user.id.eq(1_u64))
        .exclude(|user| user.active)
        .execute()
        .unwrap()
        .unwrap();

    assert_eq!(
        public_user,
        PublicUser {
            id: 1,
            name: "Alice".to_string(),
            age: 30,
        }
    );
}

#[test]
fn typed_find_supports_exclusion_projections() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let users: Vec<PublicUser> = collection
        .find(|user| user.active.eq(true))
        .sort(|user| user.id.asc())
        .exclude(|user| user.active)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        users,
        vec![
            PublicUser {
                id: 1,
                name: "Alice".to_string(),
                age: 30,
            },
            PublicUser {
                id: 2,
                name: "Bob".to_string(),
                age: 40,
            },
        ]
    );
}

#[test]
fn typed_find_supports_pagination_after_a_projection() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let users: Vec<User> = collection
        .find(|user| user.age.gte(20))
        .include(|user| (user.id, user.name, user.age, user.active))
        .sort(|user| user.age.desc())
        .skip(1)
        .limit(1)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(users[0].id, 1);
}

#[test]
fn typed_find_selects_tuples_by_the_declared_field_order() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let values: Vec<(i32, String)> = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| (user.age, user.name))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(values, vec![(30, "Alice".to_string())]);
}

#[test]
fn typed_find_one_selects_tuples_by_the_declared_field_order() {
    let (_dir, db) = setup();
    let collection = db.typed_collection::<User>("users");

    let value = collection
        .find_one_with(|user| user.active.eq(true))
        .sort(|user| user.age.asc())
        .select(|user| (user.age, user.name))
        .execute()
        .unwrap();

    assert_eq!(value, Some((30, "Alice".to_string())));
}

#[test]
fn typed_select_reports_decoding_errors() {
    let (_dir, db) = setup();
    db.collection("users")
        .insert_one(doc! {
            "_id": 4_i64,
            "name": "Dora",
            "age": "not-a-number",
            "active": true,
        })
        .unwrap();

    let mut output = db
        .typed_collection::<User>("users")
        .find(|user| user.id.eq(4_u64))
        .select(|user| user.age)
        .execute()
        .unwrap();

    assert!(matches!(output.next().unwrap(), Err(Error::BsonError(_))));
}

#[test]
fn typed_select_decodes_missing_optional_fields_as_none() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<OptionalUser>("optional_users")
        .create_if_missing();
    collection
        .insert_one(OptionalUser {
            id: 1,
            nickname: Some("Al".to_string()),
            address: None,
        })
        .unwrap();
    db.collection("optional_users")
        .insert_one(doc! { "_id": 2_i64 })
        .unwrap();

    let nicknames: Vec<Option<String>> = collection
        .find(|user| user.id.gte(1_u64))
        .sort(|user| user.id.asc())
        .select(|user| user.nickname)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(nicknames, vec![Some("Al".to_string()), None]);
}

#[test]
fn typed_select_reports_missing_required_fields() {
    let (_dir, db) = setup();
    db.collection("users")
        .insert_one(doc! { "_id": 4_i64 })
        .unwrap();

    let mut output = db
        .typed_collection::<User>("users")
        .find(|user| user.id.eq(4_u64))
        .select(|user| user.age)
        .execute()
        .unwrap();

    assert!(matches!(
        output.next().unwrap(),
        Err(Error::DeserializationError(message)) if message == "Selected field 'age' is missing"
    ));
}

#[test]
fn typed_select_decodes_optional_embedded_values() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<OptionalUser>("optional_users")
        .create_if_missing();
    let zurich = Address {
        city: "Zurich".to_string(),
        postal_code: "8001".to_string(),
    };
    collection
        .insert_one(OptionalUser {
            id: 1,
            nickname: None,
            address: Some(zurich.clone()),
        })
        .unwrap();
    db.collection("optional_users")
        .insert_many([
            doc! { "_id": 2_i64, "address": bson::Bson::Null },
            doc! { "_id": 3_i64 },
        ])
        .unwrap();

    let addresses: Vec<Option<Address>> = collection
        .find(|user| user.id.gte(1_u64))
        .sort(|user| user.id.asc())
        .select(|user| user.address)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(addresses, vec![Some(zurich), None, None]);
}

#[test]
fn typed_optional_fields_support_exists_and_nested_filters() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<OptionalUser>("optional_users")
        .create_if_missing();
    collection
        .insert_many([
            OptionalUser {
                id: 1,
                nickname: Some("Al".to_string()),
                address: Some(Address {
                    city: "Zurich".to_string(),
                    postal_code: "8001".to_string(),
                }),
            },
            OptionalUser {
                id: 2,
                nickname: None,
                address: None,
            },
        ])
        .unwrap();
    db.collection("optional_users")
        .insert_one(doc! { "_id": 3_i64 })
        .unwrap();

    let nickname_ids: Vec<u64> = collection
        .find(|user| user.nickname.exists())
        .sort(|user| user.id.asc())
        .select(|user| user.id)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(nickname_ids, vec![1, 2]);

    let address_ids: Vec<u64> = collection
        .find(|user| user.address.city.eq("Zurich"))
        .select(|user| user.id)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(address_ids, vec![1]);
}

#[test]
fn typed_api_round_trips_embedded_values_and_collections() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    let settings = BTreeMap::from([
        ("notifications".to_string(), "enabled".to_string()),
        ("theme".to_string(), "dark".to_string()),
    ]);
    let user = NestedUser {
        id: 1,
        address: Address {
            city: "Zurich".to_string(),
            postal_code: "8001".to_string(),
        },
        orders: vec![Order {
            total: 125,
            shipping: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
        }],
        tags: vec!["database".to_string(), "rust".to_string()],
        settings: settings.clone(),
    };

    collection.insert_one(user.clone()).unwrap();

    let users: Vec<NestedUser> = collection
        .find(|nested_user| nested_user.id.eq(1_u64))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(users, vec![user]);

    let selected: Vec<(Vec<Order>, BTreeMap<String, String>)> = collection
        .find(|nested_user| nested_user.id.eq(1_u64))
        .select(|nested_user| (nested_user.orders, nested_user.settings))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(selected, vec![(users[0].orders.clone(), settings)]);
}

#[test]
fn typed_find_filters_on_an_embedded_field_without_annotations() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![],
            tags: vec![],
            settings: BTreeMap::new(),
        })
        .unwrap();

    let users: Vec<_> = collection
        .find(|user| user.address.city.eq("Zurich"))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(users.len(), 1);
}

#[test]
fn typed_find_projects_and_selects_nested_fields() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![],
            tags: vec![],
            settings: BTreeMap::new(),
        })
        .unwrap();

    let projected: Vec<AddressOnly> = collection
        .find(|user| user.id.eq(1_u64))
        .include(|user| user.address)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(projected[0].address.city, "Zurich");
    assert_eq!(projected[0].address.postal_code, "8001");

    let addresses: Vec<Address> = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.address)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        addresses,
        vec![Address {
            city: "Zurich".to_string(),
            postal_code: "8001".to_string(),
        }]
    );

    let cities: Vec<String> = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.address.city.clone())
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(cities, vec!["Zurich"]);
}

#[test]
fn typed_find_filters_collections_through_typed_array_and_map_accessors() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![],
            tags: vec!["rust".to_string(), "database".to_string()],
            settings: BTreeMap::from([("theme".to_string(), "dark".to_string())]),
        })
        .unwrap();

    let users: Vec<_> = collection
        .find(|user| {
            user.tags
                .any_eq("rust")
                .and(user.settings.key("theme").eq("dark"))
        })
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(users.len(), 1);
}

#[test]
fn typed_find_filters_arrays_by_all_values_and_length() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    let user = |id, tags: Vec<&str>| NestedUser {
        id,
        address: Address {
            city: "Zurich".to_string(),
            postal_code: "8001".to_string(),
        },
        orders: vec![],
        tags: tags.into_iter().map(str::to_string).collect(),
        settings: BTreeMap::new(),
    };
    collection
        .insert_many([
            user(1, vec!["rust", "database"]),
            user(2, vec!["rust", "database", "storage"]),
            user(3, vec!["rust"]),
        ])
        .unwrap();

    let all_and_two: Vec<u64> = collection
        .find(|user| user.tags.all(["rust", "database"]).and(user.tags.len_eq(2)))
        .select(|user| user.id)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(all_and_two, vec![1]);

    let two_elements: Vec<u64> = collection
        .find(|user| user.tags.len_eq(2))
        .select(|user| user.id)
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(two_elements, vec![1]);
}

#[test]
fn typed_selects_map_entries_and_array_elements() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    let first_order = Order {
        total: 125,
        shipping: Address {
            city: "Bern".to_string(),
            postal_code: "3000".to_string(),
        },
    };
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![first_order.clone()],
            tags: vec!["rust".to_string()],
            settings: BTreeMap::from([("theme".to_string(), "dark".to_string())]),
        })
        .unwrap();

    let themes: Vec<String> = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.settings.key("theme"))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(themes, vec!["dark"]);

    let tags: Vec<String> = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.tags.at(0))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(tags, vec!["rust"]);

    let orders: Vec<Order> = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.orders.at(0))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(orders, vec![first_order]);
}

#[test]
fn typed_select_reports_missing_array_elements() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![Order {
                total: 125,
                shipping: Address {
                    city: "Bern".to_string(),
                    postal_code: "3000".to_string(),
                },
            }],
            tags: vec!["rust".to_string()],
            settings: BTreeMap::new(),
        })
        .unwrap();

    let mut tags = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.tags.at(1))
        .execute()
        .unwrap();
    assert!(matches!(
        tags.next().unwrap(),
        Err(Error::DeserializationError(message)) if message == "Selected field 'tags.1' is missing"
    ));

    let mut orders = collection
        .find(|user| user.id.eq(1_u64))
        .select(|user| user.orders.at(1))
        .execute()
        .unwrap();
    assert!(matches!(
        orders.next().unwrap(),
        Err(Error::DeserializationError(message)) if message == "Selected field 'orders.1' is missing"
    ));
}

#[test]
fn typed_find_filters_array_indexes_and_embedded_array_elements() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![Order {
                total: 125,
                shipping: Address {
                    city: "Bern".to_string(),
                    postal_code: "3000".to_string(),
                },
            }],
            tags: vec!["rust".to_string()],
            settings: BTreeMap::new(),
        })
        .unwrap();

    let users: Vec<_> = collection
        .find(|user| {
            user.tags.at(0).eq("rust").and(
                user.orders
                    .any(|order| order.total.gte(100).and(order.shipping.city.eq("Bern"))),
            )
        })
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(users.len(), 1);
}

#[test]
fn typed_include_projects_an_array_index_as_an_index_keyed_document() {
    let (_dir, db) = setup();
    let collection = db
        .typed_collection::<NestedUser>("nested_users")
        .create_if_missing();
    collection
        .insert_one(NestedUser {
            id: 1,
            address: Address {
                city: "Zurich".to_string(),
                postal_code: "8001".to_string(),
            },
            orders: vec![
                Order {
                    total: 125,
                    shipping: Address {
                        city: "Bern".to_string(),
                        postal_code: "3000".to_string(),
                    },
                },
                Order {
                    total: 75,
                    shipping: Address {
                        city: "Basel".to_string(),
                        postal_code: "4000".to_string(),
                    },
                },
            ],
            tags: vec![],
            settings: BTreeMap::new(),
        })
        .unwrap();

    let projected: Vec<IndexedOrders> = collection
        .find(|user| user.id.eq(1_u64))
        .include(|user| user.orders.at(0))
        .execute()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(
        projected,
        vec![IndexedOrders {
            orders: BTreeMap::from([(
                "0".to_string(),
                Order {
                    total: 125,
                    shipping: Address {
                        city: "Bern".to_string(),
                        postal_code: "3000".to_string(),
                    },
                },
            )]),
        }]
    );
}
