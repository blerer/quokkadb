use std::f64::consts::LN_2;
use crate::util::murmur_hash64::murmur_hash64a;

/// A Bloom Filter is a probabilistic data structure that efficiently tests for the existence of an element.
/// It may yield false positives but guarantees no false negatives. This implementation uses MurmurHash3 for hashing.
///
/// # Parameters
/// - `bit_array`: The underlying storage for the filter's bits, implemented as a `Vec<u8>`.
/// - `size`: Total number of bits in the filter.
/// - `hash_count`: Number of hash functions used for each item.
#[derive(Default)]
pub struct BloomFilter {
    bit_array: Vec<u8>,
    size: usize,
    hash_count: usize,
}

impl BloomFilter {
    /// Creates a new BloomFilter with dynamically calculated size and hash functions.
    ///
    /// # Arguments
    /// - `expected_items`: The estimated number of items to be stored in the filter.
    /// - `false_positive_rate`: The desired false positive probability (e.g., 0.01 for 1%).
    ///
    /// # Explanation
    /// 1. **Filter Size (`size`)**:
    ///    - Formula: `size = -(expected_items * ln(false_positive_rate)) / (ln(2))²`
    ///    - This determines the total number of bits needed to achieve the given false positive rate for the expected number of items.
    ///    - It uses the mathematical principle of minimizing false positives in Bloom Filters.
    ///    - The result is rounded up using ceil to ensure there are enough bits.
    ///
    /// 2. **Hash Function Count (`hash_count`)**:
    ///    - Formula: `hash_count = (size / expected_items) * ln(2)`
    ///    - This calculates the optimal number of hash functions to minimize false positives.
    ///    - It leverages the natural logarithm of 2 for efficiency.
    ///
    /// 3. **Byte Array Size (`byte_size`)**:
    ///    - Formula: `byte_size = size / 8`
    ///    - Converts the bit size to bytes for efficient storage in a `Vec<u8>`.
    ///
    /// # Returns
    /// A `BloomFilter` instance with an appropriately sized bit array and number of hash functions.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let ln2_squared = LN_2.powi(2);
        let size = -((expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let hash_count = ((size as f64 / expected_items as f64) * LN_2).ceil() as usize;
        let byte_size = size >> 3;
        Self {
            bit_array: vec![0; byte_size],
            size,
            hash_count,
        }
    }

    /// Adds an item to the Bloom Filter.
    ///
    /// # Arguments
    /// - `item`: A reference to a `Vec<u8>` representing the item to be added.
    ///
    /// The item is hashed using double hashing to determine the bits to set.
    pub fn add(&mut self, item: &[u8]) {
        let (hash1, hash2) = self.double_hash(item);
        for i in 0..self.hash_count {
            let index = (hash1.wrapping_add(i as u64 * hash2) % self.size as u64) as usize;
            self.set_bit(index);
        }
    }

    /// Checks if an item is possibly in the Bloom Filter.
    ///
    /// # Arguments
    /// - `item`: A reference to a `Vec<u8>` representing the item to check.
    ///
    /// # Returns
    /// - `true` if the item is possibly in the filter (with a false positive probability).
    /// - `false` if the item is definitely not in the filter.
    pub fn contains(&self, item: &Vec<u8>) -> bool {
        let (hash1, hash2) = self.double_hash(item);
        for i in 0..self.hash_count {
            let index = (hash1.wrapping_add(i as u64 * hash2) % self.size as u64) as usize;
            if !self.get_bit(index) {
                return false;
            }
        }
        true
    }

    /// Uses double hashing to generate two hash values for an item.
    ///
    /// # Arguments
    /// - `item`: A reference to a `Vec<u8>` representing the item to hash.
    ///
    /// # Returns
    /// A tuple `(hash1, hash2)` where both are `u64` values.
    fn double_hash(&self, item: &[u8]) -> (u64, u64) {
        let hash1 = murmur_hash64a(item, 0);
        let hash2 = murmur_hash64a(item, hash1);
        (hash1, hash2)
    }

    /// Sets a bit in the bit array at the specified index.
    ///
    /// # Arguments
    /// - `index`: The bit index to set.
    fn set_bit(&mut self, index: usize) {
        let byte_index = index >> 3;
        let bit_index = index & 7;
        self.bit_array[byte_index] |= 1 << bit_index;
    }

    /// Gets the value of a bit in the bit array at the specified index.
    ///
    /// # Arguments
    /// - `index`: The bit index to check.
    ///
    /// # Returns
    /// - `true` if the bit is set.
    /// - `false` if the bit is not set.
    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index >> 3;
        let bit_index = index & 7;
        (self.bit_array[byte_index] & (1 << bit_index)) != 0
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.bit_array
    }
}
