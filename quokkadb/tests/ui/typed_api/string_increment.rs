use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    name: String,
}

fn main() {
    User::update(|user| user.name.inc(1));
}
