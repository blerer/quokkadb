use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    tags: Vec<String>,
}

fn main() {
    User::root_fields().tags.any_eq(42);
}
