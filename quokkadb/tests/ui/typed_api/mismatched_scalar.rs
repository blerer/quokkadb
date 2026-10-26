use quokkadb::{QuokkaDocument, QuokkaScalar};
use serde::{Deserialize, Serialize};

// Keep the diagnostic independent of the platform-specific `From<T>` impls for `i32`.
#[derive(Serialize, Deserialize)]
struct Age(i32);

impl QuokkaScalar for Age {}

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    age: Age,
}

fn main() {
    User::root_fields().age.eq("old");
}
