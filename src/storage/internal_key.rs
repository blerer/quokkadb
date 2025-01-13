use std::convert::TryFrom;
use crate::io::ZeroCopy;
use crate::storage::operation::OperationType;

/// The key used by the storage internally are called internal key and are composed of
/// the collection ID (4 bytes big-endian), the index ID (4 bytes big-endian), the user key,
/// the sequence number (7 bytes big-endian) and the operation type (1 byte).
/// The part including the collection ID, the index ID and the user key is called the record key.

/// Maximum sequence number as the sequence must fit in 56 bits.
pub const MAX_SEQUENCE_NUMBER: u64 = (1 << 56) - 1;

/// Encodes an internal key into raw bytes that sort correctly.
///
///  The internal key
/// Format: `[collection (u32)][index (u32)][user_key][!sequence_number (big-endian u56)][operation_type (u8)]`
pub fn encode_internal_key(
    collection: u32,
    index: u32,
    user_key: &[u8],
    sequence_number: u64,
    value_type: OperationType,
) -> Vec<u8> {

    assert!(sequence_number <= MAX_SEQUENCE_NUMBER, "Sequence number must fit in 56 bits");

    let inverted_seq = !sequence_number;
    let mut key = Vec::with_capacity(4 + 4 + user_key.len() + 8);

    // Appends the record key (collection + index + user key)
    append_record_key(&mut key, &collection, &index, user_key);

    // Encode inverted sequence number in big-endian format (7 bytes)
    key.extend_from_slice(&(inverted_seq.to_be_bytes()[1..]));


    // Append the operation type as the last byte
    key.push(u8::from(value_type));

    key
}

fn append_record_key(key: &mut Vec<u8>, collection: &u32, index: &u32, user_key: &[u8]) {
    // Append collection (big-endian)
    key.extend_from_slice(&collection.to_be_bytes());

    // Append index (big-endian)
    key.extend_from_slice(&index.to_be_bytes());

    // Append user_key
    key.extend_from_slice(user_key);
}

pub fn encode_record_key(
    collection: u32,
    index: u32,
    user_key: &[u8]) -> Vec<u8> {

    let mut key = Vec::with_capacity(4 + 4 + user_key.len());
    append_record_key(&mut key, &collection, &index, user_key);
    key
}

pub fn extract_collection(internal_key: &[u8]) -> u32 {
    assert!(internal_key.len() >= 8, "Invalid internal key length");
    internal_key.read_u32_be(0)
}

pub fn extract_index(internal_key: &[u8]) -> u32 {
    assert!(internal_key.len() >= 8, "Invalid internal key length");
    internal_key.read_u32_be(4)
}

/// Extracts the user key from a compound key.
pub fn extract_user_key(internal_key: &[u8]) -> &[u8] {
    assert!(internal_key.len() >= 16, "Invalid internal key length");
    &internal_key[8..internal_key.len() - 8]
}

/// Extracts the original sequence number from an internal key.
pub fn extract_sequence_number(internal_key: &[u8]) -> u64 {
    assert!(internal_key.len() >= 8, "Invalid internal key length");

    let mut seq_bytes = [u8::MAX; 8];
    seq_bytes[1..].copy_from_slice(&internal_key[internal_key.len() - 8..internal_key.len() - 1]);

    let inverted_seq = seq_bytes.read_u64_be(0);
    !inverted_seq // Reverse the inversion to get the actual sequence number
}

/// Extracts the operation type from an internal key.
pub fn extract_operation_type(internal_key: &[u8]) -> OperationType {
    assert!(internal_key.len() >= 8, "Invalid internal key length");
    OperationType::try_from(internal_key[internal_key.len() - 1]).unwrap()
}

#[cfg(test)]
mod tests {
    use bson::Bson;
    use bson::oid::ObjectId;
    use super::*;
    use crate::storage::operation::OperationType;
    use crate::util::bson_utils::BsonKey;

    #[test]
    fn test_internal_key_encoding_decoding() {
        let collection = 32;
        let index = 0;
        let user_key =  Bson::ObjectId(ObjectId::new()).try_into_key().unwrap();
        let seq = 42;
        let op_type = OperationType::Put;

        let encoded = encode_internal_key(collection, index, &user_key, seq, op_type);

        assert_eq!(extract_collection(&encoded), collection);
        assert_eq!(extract_index(&encoded), index);
        assert_eq!(extract_user_key(&encoded), user_key);
        assert_eq!(extract_sequence_number(&encoded), seq);
        assert_eq!(extract_operation_type(&encoded), op_type);
    }

    #[test]
    fn test_internal_key_ordering() {
        let collection = 32;
        let index = 0;
        let id1 =  Bson::Int32(1).try_into_key().unwrap();
        let id2 =  Bson::Int32(2).try_into_key().unwrap();

        assert!(id1 < id2);

        let key1 = encode_internal_key(collection, index, &id1, 100, OperationType::Put);
        let key2 = encode_internal_key(collection, index, &id1, 200, OperationType::Put);
        let key3 = encode_internal_key(collection, index, &id1, 200, OperationType::Delete);
        let key4 = encode_internal_key(collection, index, &id2, 100, OperationType::Put);

        assert!(key2 < key1); // Higher sequence number sorts first
        assert!(key3 < key2); // DELETE sorts before PUT if sequence is the same
        assert!(key3 < key4);
    }
}