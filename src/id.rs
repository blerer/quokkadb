use serde::de::{Deserialize, Deserializer, Error as _};
use serde::ser::{Serialize, Serializer};
use sonyflake::Sonyflake;
use std::num::TryFromIntError;
use std::sync::{LazyLock, Mutex};

static ID_GENERATOR: LazyLock<Mutex<Sonyflake>> = LazyLock::new(|| {
    Mutex::new(
        Sonyflake::builder()
            .machine_id(&|| Ok(0))
            .finalize()
            .expect("Sonyflake ID generator configuration must be valid"),
    )
});

/// A Sonyflake-generated identifier for a QuokkaDB document.
///
/// It serializes as a BSON `Int64` so it can be stored directly in a document's
/// `_id` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuokkaId(u64);

impl QuokkaId {
    /// Creates a new globally ordered Sonyflake identifier.
    pub fn new() -> Self {
        let id = ID_GENERATOR
            .lock()
            .expect("Sonyflake ID generator lock must not be poisoned")
            .next_id()
            .expect("Sonyflake ID generation must not overflow")
            .to_u64();

        Self(id)
    }

    /// Returns the underlying Sonyflake value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Default for QuokkaId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<u64> for QuokkaId {
    type Error = TryFromIntError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        i64::try_from(value)?;
        Ok(Self(value))
    }
}

impl From<QuokkaId> for u64 {
    fn from(value: QuokkaId) -> Self {
        value.0
    }
}

impl Serialize for QuokkaId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(
            i64::try_from(self.0).expect("QuokkaId values must fit into a BSON Int64"),
        )
    }
}

impl<'de> Deserialize<'de> for QuokkaId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i64::deserialize(deserializer)?;
        let value = u64::try_from(value)
            .map_err(|_| D::Error::custom("QuokkaId must be a non-negative BSON Int64"))?;
        Ok(Self(value))
    }
}

#[cfg(test)]
mod tests {
    use super::QuokkaId;
    use bson::{Bson, doc, serialize_to_document};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize)]
    struct DocumentWithId {
        #[serde(rename = "_id")]
        id: QuokkaId,
    }

    #[test]
    fn serializes_as_bson_int64_and_round_trips() {
        let document = DocumentWithId {
            id: QuokkaId::new(),
        };

        let serialized = serialize_to_document(&document).unwrap();
        assert_eq!(
            serialized.get_i64("_id").unwrap(),
            document.id.as_u64() as i64
        );
        assert!(matches!(serialized.get("_id"), Some(Bson::Int64(_))));

        let deserialized: DocumentWithId = bson::deserialize_from_document(serialized).unwrap();
        assert_eq!(deserialized.id, document.id);
    }

    #[test]
    fn rejects_negative_values() {
        let error =
            bson::deserialize_from_document::<DocumentWithId>(doc! { "_id": -1_i64 }).unwrap_err();

        assert!(error.to_string().contains("non-negative BSON Int64"));
    }
}
