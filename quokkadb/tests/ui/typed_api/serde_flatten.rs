use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    #[serde(flatten)]
    profile: Profile,
}

#[derive(Serialize, Deserialize)]
struct Profile {
    name: String,
}

fn main() {}
