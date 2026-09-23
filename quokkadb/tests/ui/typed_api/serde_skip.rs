use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    #[serde(skip)]
    internal: String,
}

fn main() {
    let _ = User::root_fields().internal.exists();
}
