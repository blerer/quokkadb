use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[quokka(id)]
    id: u64,
    #[serde(skip_deserializing)]
    internal: String,
}

fn main() {}
