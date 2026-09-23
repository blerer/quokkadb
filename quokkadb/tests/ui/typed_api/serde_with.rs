use quokkadb::QuokkaDocument;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

mod codec {
    use super::*;

    pub fn serialize<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)
    }
}

#[derive(Serialize, Deserialize, QuokkaDocument)]
struct User {
    #[serde(rename = "_id")]
    id: u64,
    #[serde(with = "codec")]
    name: String,
}

fn main() {}
