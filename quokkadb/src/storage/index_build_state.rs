use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::storage::FIRST_USER_COLLECTION_ID;
use std::io::Result;

mod cursor_tags {
    pub(super) const ABSENT: u8 = 0;
    pub(super) const PRESENT: u8 = 1;
}

/// Identifies one index build in the internal state collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexBuildKey {
    pub(crate) collection_id: u32,
    pub(crate) index_id: u32,
}

impl IndexBuildKey {
    pub(crate) fn new(collection_id: u32, index_id: u32) -> Self {
        assert!(
            collection_id >= FIRST_USER_COLLECTION_ID,
            "Index builds must target user collections"
        );
        assert!(
            index_id > 0,
            "Index build IDs must not use the collection data index"
        );
        Self {
            collection_id,
            index_id,
        }
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        self.write_to(&mut writer, 0);
        writer.take_buffer()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let reader = ByteReader::new(bytes);
        let key = Self::read_from(&reader, 0)?;
        assert!(!reader.has_remaining());
        Ok(key)
    }
}

impl Serializable for IndexBuildKey {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, _version: u32) -> Result<Self> {
        let collection_id = reader.read_varint_u32()?;
        let index_id = reader.read_varint_u32()?;

        Ok(Self::new(collection_id, index_id))
    }

    fn write_to(&self, writer: &mut ByteWriter, _version: u32) {
        writer
            .write_varint_u32(self.collection_id)
            .write_varint_u32(self.index_id);
    }
}

/// The lifecycle state persisted for one index build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexBuildState {
    pub(crate) last_processed_primary_key: Option<Vec<u8>>,
    pub(crate) processed_document_count: u64,
    pub(crate) indexed_entry_count: u64,
}

impl IndexBuildState {
    pub(crate) fn new() -> Self {
        Self {
            last_processed_primary_key: None,
            processed_document_count: 0,
            indexed_entry_count: 0,
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut writer = ByteWriter::new();
        self.write_to(&mut writer, 0);
        writer.take_buffer()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let reader = ByteReader::new(bytes);
        let state = Self::read_from(&reader, 0)?;
        assert!(!reader.has_remaining());
        Ok(state)
    }
}

impl Serializable for IndexBuildState {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        Ok(Self {
            last_processed_primary_key: Option::<Vec<u8>>::read_from(reader, version)?,
            processed_document_count: u64::read_from(reader, version)?,
            indexed_entry_count: u64::read_from(reader, version)?,
        })
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        self.last_processed_primary_key.write_to(writer, version);
        self.processed_document_count.write_to(writer, version);
        self.indexed_entry_count.write_to(writer, version);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::serializable::check_serialization_round_trip;

    #[test]
    fn index_build_key_round_trips() {
        let key = IndexBuildKey::new(FIRST_USER_COLLECTION_ID + 3, 7);
        let encoded = key.encode();

        assert_eq!(IndexBuildKey::decode(&encoded).unwrap(), key);
        check_serialization_round_trip(key, 0);
    }

    #[test]
    fn index_build_state_round_trips() {
        let state = IndexBuildState {
            last_processed_primary_key: Some(vec![1, 2, 3]),
            processed_document_count: 12,
            indexed_entry_count: 14,
        };

        let encoded = state.encode();
        assert_eq!(IndexBuildState::decode(&encoded).unwrap(), state);
        check_serialization_round_trip(state, 0);
    }

    #[test]
    fn index_build_state_starts_at_the_beginning() {
        assert_eq!(
            IndexBuildState::new(),
            IndexBuildState {
                last_processed_primary_key: None,
                processed_document_count: 0,
                indexed_entry_count: 0,
            }
        );
    }
}
