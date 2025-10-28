use crate::io::{varint, ZeroCopy};
use crate::storage::operation::OperationType;
use std::convert::TryFrom;
use std::ops::{Bound, RangeBounds, Bound::Unbounded};
use std::rc::Rc;
use crate::storage::Direction;
use crate::util::interval::Interval;
use crate::util::bson_utils::BsonKey;
use bson::Bson;

/// The key used by the storage internally are called internal key and are composed of
/// the collection ID (varint-encoded u32), the index ID (varint-encoded u32), the user key,
/// the sequence number (7 bytes big-endian) and the operation type (1 byte).
/// The part including the collection ID, the index ID and the user key is called the record key.

/// Maximum sequence number as the sequence must fit in 56 bits.
pub const MAX_SEQUENCE_NUMBER: u64 = (1 << 56) - 1;

/// Encodes an internal key into raw bytes that sort correctly.
///
/// The internal key format is: `[collection (varint u32)][index (varint u32)][user_key][!sequence_number (big-endian u56)][operation_type (u8)]`
pub fn encode_internal_key(
    record_key: &[u8],
    sequence_number: u64,
    value_type: OperationType,
) -> Vec<u8> {
    assert!(
        sequence_number <= MAX_SEQUENCE_NUMBER,
        "Sequence number must fit in 56 bits"
    );

    let inverted_seq = !sequence_number;
    let mut key = Vec::with_capacity(record_key.len() + 8);

    // Appends the record key (collection + index + user key)
    key.extend_from_slice(record_key);

    // Encode inverted sequence number in big-endian format (7 bytes)
    key.extend_from_slice(&(inverted_seq.to_be_bytes()[1..]));

    // Append the operation type as the last byte
    key.push(u8::from(value_type));

    key
}

fn append_record_key(key: &mut Vec<u8>, collection: u32, index: u32, user_key: &[u8]) {
    // Append collection (varint-encoded)
    varint::write_u32(collection, key);

    // Append index (varint-encoded)
    varint::write_u32(index, key);

    // Append user_key
    key.extend_from_slice(user_key);
}

pub fn encode_record_key(collection: u32, index: u32, user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        varint::compute_u32_vint_size(collection)
            + varint::compute_u32_vint_size(index)
            + user_key.len(),
    );
    append_record_key(&mut key, collection, index, user_key);
    key
}

/// Decodes a record key into its components.
pub fn decode_record_key(record_key: &[u8]) -> (u32, u32, &[u8]) {
    let (collection, offset) = varint::read_u32(record_key, 0);
    let (index, offset) = varint::read_u32(record_key, offset);
    (collection, index, &record_key[offset..])
}

/// Extracts the record key from an internal key.
pub fn extract_record_key(internal_key: &[u8]) -> &[u8] {
    // An internal key must have at least the MVCC suffix (8 bytes), a 1-byte collection ID,
    // and a 1-byte index ID.
    assert!(internal_key.len() >= 10, "Invalid internal key length");
    &internal_key[..internal_key.len() - 8]
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

/// Represents a bound for an internal key, which can either be bounded by a specific key or unbounded.
/// There is no excluding or including bound as this property is enforced by the value of the internal key.
/// For example to exclude a key the sequence number is set to `u64::MIN` and the operator type to OperatorType::MinKey.
#[derive(Clone, Debug, PartialEq)]
pub struct InternalKeyBound (pub Vec<u8>);

impl InternalKeyBound {
    pub fn is_less_than(&self, internal_key: &[u8]) -> bool {
        &self.0[..] < internal_key
    }

    pub fn is_greater_than(&self, internal_key: &[u8]) -> bool {
        &self.0[..] > internal_key
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Represents a range of internal keys, which can be used for range scans.
#[derive(Debug, PartialEq)]
pub struct InternalKeyRange {
    pub start: InternalKeyBound,
    pub end: InternalKeyBound,
}

impl InternalKeyRange {
    pub fn new(start: InternalKeyBound, end: InternalKeyBound) -> Self {
        InternalKeyRange { start, end }
    }

    pub fn start_bound(&self) -> &InternalKeyBound {
        &self.start
    }

    pub fn end_bound(&self) -> &InternalKeyBound  {
        &self.end
    }

    pub fn contains(&self, internal_key: &[u8]) -> bool {
        !self.start.is_greater_than(internal_key) && !self.end.is_less_than(internal_key)
    }
}

/// Encode a range over record keys (collection/index/user_key only).
///
/// This function builds a byte range for the record-key prefix:
/// `[collection (varint u32)][index (varint u32)][user_key bytes …]`.
///
/// - Unbounded bounds are tightened to the target `(collection, index)` by encoding the
///   user key with `Bson::MinKey` (for the start) and `Bson::MaxKey` (for the end).
///   This guarantees that even "unbounded" scans remain confined within the given
///   collection and index and never bleed into adjacent prefixes.
///
/// - Note: This range does not include the MVCC suffix (`!sequence (u56)` + `op (u8)`).
///   It is intended for record-key-only interval computations and as a helper to
///   compose internal-key ranges where the MVCC portion is appended separately.
pub fn encode_record_key_range<R>(
    collection: u32,
    index: u32,
    range: &R
) -> Interval<Vec<u8>>
where
    R: RangeBounds<Vec<u8>>,
{
    let start_bound = match range.start_bound() {
        Bound::Included(key) => Bound::Included(encode_record_key(collection, index, key)),
        Bound::Excluded(key) => Bound::Excluded(encode_record_key(collection, index, key)),
        Unbounded => Bound::Included(encode_record_key(collection, index, &Bson::MinKey.try_into_key().unwrap())),
    };

    let end_bound = match range.end_bound() {
        Bound::Included(key) => Bound::Included(encode_record_key(collection, index, key)),
        Bound::Excluded(key) => Bound::Excluded(encode_record_key(collection, index, key)),
        Unbounded => Bound::Included(encode_record_key(collection, index, &Bson::MaxKey.try_into_key().unwrap())),
    };

    Interval::new(start_bound, end_bound)
}

/// Creates a range of internal keys to use for range scan queries.
///
/// Internal-key format:
/// `[record_key ...][!sequence (big-endian u56)][operation_type (u8)]`, where
/// `record_key = [collection (varint u32)][index (varint u32)][user_key …]`.
///
/// Important behavior and rationale:
/// - Unbounded user-key bounds are tightened using `Bson::MinKey` / `Bson::MaxKey`
///   for the user_key, which constrains scans to the given `(collection, index)`
///   and avoids sweeping unrelated data.
///
/// - Inclusive/exclusive semantics are modeled purely by the MVCC suffix:
///   • Start, Bound::Included, Direction::Forward:
///       `(seq = snapshot, op = Delete)` ensures we do not include versions created
///       after `snapshot` for the starting user_key while allowing all older ones.
///   • Start, Bound::Included, Direction::Reverse:
///       `(seq = MAX_SEQUENCE_NUMBER, op = MaxKey)` acts as the absolute minimal
///       internal key for that user_key, ensuring all its versions are included.
///       MVCC visibility (choosing the first version <= snapshot) is enforced
///      during iteration, not in the range bound itself. This avoids overshooting
///       block/restart boundaries when seeking backwards.
///   • Start, Bound::Excluded:
///       `(seq = 0, op = MinKey)` is the absolute maximum internal key for that
///       user_key; since the range uses `key >= start`, this effectively excludes
///       that key entirely.
///   • End, Bound::Included:
///       `(seq = 0, op = MinKey)` is the absolute maximum internal key for the end
///       user_key; combined with `key <= end`, this includes all versions of it.
///   • End, Bound::Excluded:
///       `(seq = MAX_SEQUENCE_NUMBER, op = MaxKey)` is the absolute minimal internal
///       key for that user_key; used with `key <= end`, it excludes that key entirely.
///
/// - OperationType::MinKey/MaxKey are sentinel types used only for constructing
///   range bounds. They are not produced by writers and should not appear in persisted
///   entries; they exist to make lexicographic range math precise and efficient.
///
/// The return value is a reference-counted `InternalKeyRange` because such ranges are
/// often cloned to build the iterators involved in range scan queries.
pub fn encode_internal_key_range<R>(
    collection: u32,
    index: u32,
    range: &R,
    snapshot: u64,
    direction: Direction
) -> Rc<InternalKeyRange>
where
    R: RangeBounds<Vec<u8>>,
{
    // Convert the start and end bounds
    let start = match range.start_bound() {
        Bound::Included(key) => {
            match direction {
                Direction::Forward => InternalKeyBound(encode_internal_key(
                    &encode_record_key(collection, index, &key),
                    snapshot,
                    // Delete is the lowest OperationType that exists from the write
                    // perspective. Using it instead of MaxKey can lead to more efficient seek when
                    // the delete operation is at an SSTable block restart point.
                    OperationType::Delete,
                )),
                Direction::Reverse => InternalKeyBound(encode_internal_key(
                    &encode_record_key(collection, index, &key),
                    MAX_SEQUENCE_NUMBER,
                    OperationType::MaxKey,
                )),
            }
        },
        Bound::Excluded(key) => InternalKeyBound(encode_internal_key(
            &encode_record_key(collection, index, &key),
            u64::MIN,
            OperationType::MinKey,
        )),
        Unbounded => {
            let key = Bson::MinKey.try_into_key().unwrap();
            InternalKeyBound(encode_internal_key(
                &encode_record_key(collection, index, &key),
                MAX_SEQUENCE_NUMBER,
                OperationType::MaxKey,
            ))
        }
    };

    let end = match range.end_bound() {
        Bound::Included(key) => {
            InternalKeyBound(encode_internal_key(
                &encode_record_key(collection, index, &key),
                u64::MIN,
                OperationType::MinKey,
            ))
        },
        Bound::Excluded(key) => InternalKeyBound(encode_internal_key(
            &encode_record_key(collection, index, &key),
            MAX_SEQUENCE_NUMBER,
            OperationType::MaxKey,
        )),
        Unbounded => {
            let key = Bson::MaxKey.try_into_key().unwrap();
            InternalKeyBound(encode_internal_key(
                &encode_record_key(collection, index, &key),
                u64::MIN,
                OperationType::MinKey,
            ))
        }
    };
    Rc::new(InternalKeyRange::new(start, end))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::operation::OperationType;
    use crate::util::bson_utils::BsonKey;
    use bson::oid::ObjectId;
    use bson::Bson;

    #[test]
    fn test_internal_key_encoding_decoding() {
        let collection = 32;
        let index = 0;
        let user_key = Bson::ObjectId(ObjectId::new()).try_into_key().unwrap();
        let seq = 42;
        let op_type = OperationType::Put;
        let record_key = encode_record_key(collection, index, &user_key);

        let encoded = encode_internal_key(&record_key, seq, op_type);

        assert_eq!(extract_sequence_number(&encoded), seq);
        assert_eq!(extract_operation_type(&encoded), op_type);
        assert_eq!(extract_record_key(&encoded), record_key.as_slice());

        let (decoded_collection, decoded_index, decoded_user_key) =
            decode_record_key(&record_key);
        assert_eq!(decoded_collection, collection);
        assert_eq!(decoded_index, index);
        assert_eq!(decoded_user_key, user_key.as_slice());
    }

    #[test]
    fn test_record_key_encoding_decoding_large_ids() {
        let collection = u32::MAX;
        let index = 16_384; // 3 bytes varint
        let user_key = Bson::Int64(12345).try_into_key().unwrap();

        let record_key = encode_record_key(collection, index, &user_key);
        let (decoded_collection, decoded_index, decoded_user_key) =
            decode_record_key(&record_key);

        assert_eq!(decoded_collection, collection);
        assert_eq!(decoded_index, index);
        assert_eq!(decoded_user_key, user_key.as_slice());
    }

    #[test]
    fn test_internal_key_ordering() {
        let collection = 32;
        let index = 0;
        let id1 = Bson::Int32(1).try_into_key().unwrap();
        let id2 = Bson::Int32(2).try_into_key().unwrap();

        assert!(id1 < id2);

        let record_key_1 = encode_record_key(collection, index, &id1);
        let record_key_2 = encode_record_key(collection, index, &id2);

        let key1 = encode_internal_key(&record_key_1, 100, OperationType::Put);
        let key2 = encode_internal_key(&record_key_1, 200, OperationType::Put);
        let key3 = encode_internal_key(&record_key_1, 200, OperationType::Delete);
        let key4 = encode_internal_key(&record_key_2, 100, OperationType::Put);

        assert!(key2 < key1); // Higher sequence number sorts first
        assert!(key3 < key2); // DELETE sorts before PUT if sequence is the same
        assert!(key3 < key4);
    }

    #[test]
    fn test_encode_internal_key_range_directions() {
        let collection = 1;
        let index = 2;
        let snapshot = 100;

        let key_before = Bson::Int32(5).try_into_key().unwrap();
        let key_a = Bson::Int32(10).try_into_key().unwrap();
        let key_inside = Bson::Int32(15).try_into_key().unwrap();
        let key_b = Bson::Int32(20).try_into_key().unwrap();
        let key_after = Bson::Int32(25).try_into_key().unwrap();

        let record_key = |user_key: &Vec<u8>| encode_record_key(collection, index, user_key);

        let internal_key = |user_key: &Vec<u8>, seq: u64, op: OperationType| {
            encode_internal_key(&record_key(user_key), seq, op)
        };

        // --- FORWARD SCAN ---

        // Range: [a, b] (inclusive)
        let range = encode_internal_key_range(
            collection,
            index,
            &(key_a.clone()..=key_b.clone()),
            snapshot,
            Direction::Forward,
        );

        assert!(!range.contains(&internal_key(&key_before, snapshot, OperationType::Put)));
        assert!(!range.contains(&internal_key(&key_a, snapshot + 1, OperationType::Put)),
            "Forward [a,b]: key 'a' with seq > snapshot should be outside the seek range");
        assert!(range.contains(&internal_key(&key_a, snapshot, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_a, snapshot - 1, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_inside, MAX_SEQUENCE_NUMBER, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_inside, 0, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_b, MAX_SEQUENCE_NUMBER, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_b, 0, OperationType::Put)));
        assert!(!range.contains(&internal_key(&key_after, snapshot, OperationType::Put)));

        // Range: (a, b) (exclusive)
        let range = encode_internal_key_range(
            collection,
            index,
            &Interval::open(key_a.clone(), key_b.clone()),
            snapshot,
            Direction::Forward,
        );

        assert!(!range.contains(&internal_key(&key_a, 0, OperationType::Put)), "Forward (a,b): key 'a' should be fully excluded");
        assert!(range.contains(&internal_key(&key_inside, snapshot, OperationType::Put)));
        assert!(!range.contains(&internal_key(&key_b, MAX_SEQUENCE_NUMBER, OperationType::Put)), "Forward (a,b): key 'b' should be fully excluded");

        // --- REVERSE SCAN ---

        // Range: [a, b] (inclusive)
        let range = encode_internal_key_range(
            collection,
            index,
            &(key_a.clone()..=key_b.clone()),
            snapshot,
            Direction::Reverse,
        );

        assert!(!range.contains(&internal_key(&key_before, snapshot, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_a, MAX_SEQUENCE_NUMBER, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_a, 0, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_inside, snapshot, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_b, MAX_SEQUENCE_NUMBER, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_b, 0, OperationType::Put)));
        assert!(!range.contains(&internal_key(&key_after, snapshot, OperationType::Put)));

        // --- UNBOUNDED SCAN ---
        let range = encode_internal_key_range(collection, index, &.., snapshot, Direction::Forward);
        assert!(range.contains(&internal_key(&key_a, snapshot, OperationType::Put)));
        assert!(range.contains(&internal_key(&key_b, snapshot, OperationType::Put)));

        let other_collection_key =
            encode_internal_key(&encode_record_key(collection + 1, index, &key_a), snapshot, OperationType::Put);
        assert!(!range.contains(&other_collection_key));

        let other_index_key =
            encode_internal_key(&encode_record_key(collection, index + 1, &key_a), snapshot, OperationType::Put);
        assert!(!range.contains(&other_index_key));
    }

    #[test]
    fn test_internal_key_range_collection_and_index_isolation() {
        let collection_1 = 10;
        let collection_2 = 11;
        let index_1 = 1;
        let index_2 = 2;
        let snapshot = 100;
        let user_key = Bson::Int32(42).try_into_key().unwrap();

        // --- Test Collection Isolation ---

        // Create an unbounded range for collection_1, index_1
        let range_c1 =
            encode_internal_key_range(collection_1, index_1, &.., snapshot, Direction::Forward);

        // Key for a document in collection_1, index_1
        let key_in_c1 = encode_internal_key(
            &encode_record_key(collection_1, index_1, &user_key),
            snapshot,
            OperationType::Put,
        );

        // Key for a document in collection_2, index_1 (same user key and index)
        let key_in_c2 = encode_internal_key(
            &encode_record_key(collection_2, index_1, &user_key),
            snapshot,
            OperationType::Put,
        );

        assert!(
            range_c1.contains(&key_in_c1),
            "Range for collection 1 should contain its own key"
        );
        assert!(
            !range_c1.contains(&key_in_c2),
            "Range for collection 1 should NOT contain a key from collection 2"
        );

        // --- Test Index Isolation ---

        // Create an unbounded range for collection_1, index_1
        let range_i1 =
            encode_internal_key_range(collection_1, index_1, &.., snapshot, Direction::Forward);

        // Key for a document in collection_1, index_2
        let key_in_i2 = encode_internal_key(
            &encode_record_key(collection_1, index_2, &user_key),
            snapshot,
            OperationType::Put,
        );

        assert!(
            range_i1.contains(&key_in_c1), // re-using key from collection_1, index_1
            "Range for index 1 should contain its own key"
        );
        assert!(
            !range_i1.contains(&key_in_i2),
            "Range for index 1 should NOT contain a key from index 2"
        );
    }
}
