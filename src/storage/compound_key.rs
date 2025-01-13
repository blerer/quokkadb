use crate::storage::operation::OperationType;


/// A `CompoundKey` represents a combination of a user key and metadata
/// (sequence number and value type) for storage in the skip list.
///
/// This key is designed to maintain lexicographical ordering while encoding
/// additional metadata within the key itself, such as:
/// - `sequence_number`: A monotonically increasing number for ordering versions.
/// - `value_type`: Indicates the type of operation (e.g., PUT, DELETE).
///
/// The combined structure ensures efficient and consistent handling of operations
/// in the database.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct CompoundKey {
    /// The user-provided key (arbitrary binary data).
    pub user_key: Vec<u8>,

    /// A 64-bit integer combining the sequence number (high 56 bits)
    /// and the value type (low 8 bits).
    sequence_number: u64,
}
impl CompoundKey {
    /// Creates a new `CompoundKey`.
    ///
    /// # Arguments
    /// - `user_key`: The binary user key as a `Vec<u8>`.
    /// - `sequence_number`: A monotonically increasing 56-bit sequence number.
    /// - `value_type`: An 8-bit value indicating the operation type (e.g., PUT, DELETE).
    ///
    /// # Returns
    /// A new `CompoundKey` instance.
    pub fn new(user_key: Vec<u8>, sequence_number: u64, value_type: OperationType) -> Self {
        let combined_seq = (sequence_number << 8) | u8::from(value_type) as u64;
        CompoundKey {
            user_key,
            sequence_number: combined_seq,
        }
    }

    /// Extracts the sequence number from the compound key.
    ///
    /// # Returns
    /// The 56-bit sequence number as a `u64`.
    pub fn extract_sequence_number(&self) -> u64 {
        self.sequence_number >> 8
    }

    /// Extracts the value type from the compound key.
    ///
    /// # Returns
    /// The 8-bit value type as a `u8`.
    pub fn extract_value_type(&self) -> OperationType {
        OperationType::try_from((self.sequence_number & 0xFF) as u8).unwrap()
    }

    /// Serializes the `CompoundKey` into a `Vec<u8>`.
    ///
    /// The format is:
    /// [user_key bytes][sequence_number (u64, little-endian)]
    ///
    /// # Returns
    /// A serialized representation of the `CompoundKey` as a `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut serialized = self.user_key.clone();
        serialized.extend(&self.sequence_number.to_le_bytes());
        serialized
    }

    /// Deserializes a `&[u8]` into a `CompoundKey`.
    ///
    /// Assumes the last 8 bytes represent the sequence number and value type.
    ///
    /// # Arguments
    /// - `data`: A serialized `CompoundKey` as a `&[u8]`.
    ///
    /// # Returns
    /// A `CompoundKey` instance if deserialization succeeds.
    ///
    /// # Panics
    /// Panics if `data` is shorter than 8 bytes.
    pub fn from_vec(data: &[u8]) -> Self {
        assert!(data.len() >= 8, "Data too short to contain a CompoundKey");
        let (user_key, sequence_bytes) = data.split_at(data.len() - 8);
        let sequence_number = u64::from_le_bytes(sequence_bytes.try_into().unwrap());

        Self {
            user_key: user_key.to_vec(),
            sequence_number,
        }
    }
}

/// The Keys in the memtable are primarily sorted by the user key in lexicographical order.
/// Within a given user key, entries are then sorted in decreasing sequence number order.
/// If there are multiple entries with the same user key and sequence number,
/// the operation type serves as a tiebreaker.
///
/// This ordering ensure that all operations related to a specific user key are grouped together
/// and that the most recent write for a key will appear first in this grouping.
/// DELETE operations take precedence over PUT to ensure proper handling of deletions.
impl Ord for CompoundKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First, compare user keys lexicographically.
        match self.user_key.cmp(&other.user_key) {
            std::cmp::Ordering::Equal => {
                // For equal user keys, compare sequence numbers in descending order.
                other.sequence_number.cmp(&self.sequence_number)
            }
            other => other,
        }
    }
}

impl PartialOrd for CompoundKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::operation::OperationType;
    use super::*;

    #[test]
    fn test_compound_key_creation() {
        let key = vec![1, 2, 3];
        let sequence_number = 42;

        let compound_key = CompoundKey::new(key.clone(), sequence_number, OperationType::Put);

        assert_eq!(compound_key.user_key, key);
        assert_eq!(compound_key.extract_sequence_number(), sequence_number);
        assert_eq!(compound_key.extract_value_type(), OperationType::Put);
    }

    #[test]
    fn test_compound_key_with_different_value_types() {
        let key = vec![4, 5, 6];
        let sequence_number = 100;

        let put_key = CompoundKey::new(key.clone(), sequence_number, OperationType::Put);
        let delete_key = CompoundKey::new(key.clone(), sequence_number, OperationType::Delete);

        assert_eq!(put_key.extract_value_type(), OperationType::Put);
        assert_eq!(delete_key.extract_value_type(), OperationType::Delete);

        // DELETE should come before PUT when sequence numbers are the same
        assert!(delete_key < put_key);
    }

    #[test]
    fn test_compound_key_ordering_with_same_sequence_numbers() {
        let key_a = vec![10];
        let key_b = vec![20];

        let key_a_put = CompoundKey::new(key_a.clone(), 1, OperationType::Put);
        let key_a_delete = CompoundKey::new(key_a.clone(), 1, OperationType::Delete);
        let key_b_put = CompoundKey::new(key_b.clone(), 1, OperationType::Put);

        // Ordering by user key first
        assert!(key_a_put < key_b_put);
        assert!(key_a_delete < key_b_put);

        // Within the same sequence number, ordering by value type
        assert!(key_a_delete < key_a_put);
    }

    #[test]
    fn test_compound_key_ordering_with_sequence_numbers() {
        let key = vec![1, 2, 3];

        let key_seq1 = CompoundKey::new(key.clone(), 1, OperationType::Put); // Sequence number 1
        let key_seq2 = CompoundKey::new(key.clone(), 2, OperationType::Put); // Sequence number 2
        let key_seq3 = CompoundKey::new(key.clone(), 3, OperationType::Put); // Sequence number 3

        // Higher sequence numbers should come first
        assert!(key_seq3 < key_seq2);
        assert!(key_seq2 < key_seq1);
    }

    #[test]
    fn test_compound_key_full_ordering_logic() {
        let key_a = vec![10];
        let key_b = vec![20];

        let key_a_early_put = CompoundKey::new(key_a.clone(), 1, OperationType::Put);
        let key_a_later_put = CompoundKey::new(key_a.clone(), 2, OperationType::Put);
        let key_a_later_delete = CompoundKey::new(key_a.clone(), 2, OperationType::Delete);

        let key_b_early_put = CompoundKey::new(key_b.clone(), 1, OperationType::Put);

        // User key ordering
        assert!(key_a_early_put < key_b_early_put);
        assert!(key_a_later_put < key_b_early_put);

        // Sequence number ordering
        assert!(key_a_later_put < key_a_early_put);

        // Value type ordering
        assert!(key_a_later_delete < key_a_later_put);
    }

    #[test]
    fn test_compound_key_mixed_user_key_ordering() {
        let key_a = vec![1];
        let key_b = vec![2];

        let key_a_seq1_put = CompoundKey::new(key_a.clone(), 1, OperationType::Put);
        let key_b_seq1_put = CompoundKey::new(key_b.clone(), 1, OperationType::Put);
        let key_a_seq2_put = CompoundKey::new(key_a.clone(), 2, OperationType::Put);

        // Different user keys are ordered lexicographically
        assert!(key_a_seq1_put < key_b_seq1_put);

        // Same user key, sequence number determines order
        assert!(key_a_seq2_put < key_a_seq1_put);
    }

    #[test]
    fn test_compound_key_equality() {
        let key = vec![11, 12, 13];
        let sequence_number = 99;

        let compound_key1 = CompoundKey::new(key.clone(), sequence_number, OperationType::Delete);
        let compound_key2 = CompoundKey::new(key.clone(), sequence_number, OperationType::Delete);

        assert_eq!(compound_key1, compound_key2);
    }

    #[test]
    fn test_compound_key_to_vec() {
        let key = vec![1, 2, 3, 4];
        let sequence_number = 42;
        let compound_key = CompoundKey::new(key.clone(), sequence_number, OperationType::Put);

        let deserialized = CompoundKey::from_vec(&compound_key.to_vec());

        assert_eq!(deserialized.user_key, key);
        assert_eq!(deserialized.extract_sequence_number(), sequence_number);
        assert_eq!(deserialized.extract_value_type(), OperationType::Put);
    }
}