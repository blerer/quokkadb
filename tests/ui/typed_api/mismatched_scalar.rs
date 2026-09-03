use quokkadb::QuokkaDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[quokka(id)]
    id: u64,
    age: i32,
}

fn main() {
    User::root_fields().age.eq("old");
}
