use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
}

fn main() {
    User::root_fields().name.exists();
    User::update(|user| user.name.unset());
}
