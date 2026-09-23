use quokkadb::{QuokkaDB, QuokkaDocument};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
    active: bool,
}

fn mixed_projection_modes(db: &QuokkaDB) {
    let _ = db
        .typed_collection::<User>("users")
        .find(|user| user.active.eq(true))
        .include(|user| user.name)
        .exclude(|user| user.active);
}

fn reversed_mixed_projection_modes(db: &QuokkaDB) {
    let _ = db
        .typed_collection::<User>("users")
        .find(|user| user.active.eq(true))
        .exclude(|user| user.active)
        .include(|user| user.name);
}

fn mixed_find_one_projection_modes(db: &QuokkaDB) {
    let _ = db
        .typed_collection::<User>("users")
        .find_one_with(|user| user.active.eq(true))
        .include(|user| user.name)
        .exclude(|user| user.active);
}

fn mixed_include_without_id_projection_modes(db: &QuokkaDB) {
    let _ = db
        .typed_collection::<User>("users")
        .find(|user| user.active.eq(true))
        .include_without_id(|user| user.name)
        .exclude(|user| user.active);
}

fn mixed_select_projection_modes(db: &QuokkaDB) {
    let _ = db
        .typed_collection::<User>("users")
        .find(|user| user.active.eq(true))
        .select(|user| user.name)
        .include(|user| user.name);
}

fn reversed_mixed_find_one_projection_modes(db: &QuokkaDB) {
    let _ = db
        .typed_collection::<User>("users")
        .find_one_with(|user| user.active.eq(true))
        .exclude(|user| user.active)
        .include(|user| user.name);
}

fn main() {}
