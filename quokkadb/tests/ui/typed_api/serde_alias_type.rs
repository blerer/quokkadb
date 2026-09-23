use quokkadb::QuokkaType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, QuokkaType)]
struct Profile {
    #[serde(alias = "legacy_name")]
    name: String,
}

fn main() {}
