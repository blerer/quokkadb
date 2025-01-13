use std::f64::consts::LN_2;
use std::io::Error;
use crate::io::byte_reader::ByteReader;
use crate::util::murmur_hash64::murmur_hash64a;
use crate::io::varint;

/// A Bloom Filter is a probabilistic data structure that efficiently tests for the existence of an element.
/// It may yield false positives but guarantees no false negatives. This implementation uses MurmurHash3 for hashing.
///
/// The implementation supports a writable (mutable) mode used during SSTable construction
/// and a read-only mode used on SSTable reads. The read-only mode use zero-copy when data is
/// loaded from an SST block.
///
/// # Parameters
/// - `bit_array`: The underlying storage for the filter's bits.
/// - `size`: Total number of bits in the filter.
/// - `hash_count`: Number of hash functions used for each item.
pub enum BloomFilter<'a> {
    Writable {
        bit_array: Vec<u8>,  // Used during MemTable flush
        size: usize,
        hash_count: usize,
    },
    ReadOnly {
        bit_array: &'a [u8],  // Read directly from SST block (zero-copy)
        size: usize,
        hash_count: usize,
    },
}

impl<'a> BloomFilter<'a> {
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
        BloomFilter::Writable {
            bit_array: vec![0; byte_size],
            size,
            hash_count,
        }
    }

    /// Adds an item to the Bloom filter (only in `Writable` mode).
    ///
    /// # Arguments
    /// - `item`: A reference to a `Vec<u8>` representing the item to be added.
    ///
    /// The item is hashed using double hashing to determine the bits to set.
    pub fn add(&mut self, item: &[u8]) {
        if let BloomFilter::Writable { bit_array, size, hash_count } = self {
            let (hash1, hash2) = Self::double_hash(item);
            let normalized_hash2 = hash2 % *size as u64;
            for i in 0..*hash_count {
                let index = (hash1.wrapping_add(i as u64 * normalized_hash2) % *size as u64) as usize;
                Self::set_bit(bit_array, index);
            }
        } else {
            panic!("Cannot modify a read-only BloomFilter");
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
    pub fn contains(&self, item: &[u8]) -> bool {
        let (hash1, hash2) = Self::double_hash(item);
        let normalized_hash2 = hash2 % self.size() as u64;

        for i in 0..self.hash_count() {
            let index = (hash1.wrapping_add(i as u64 * normalized_hash2) % self.size() as u64) as usize;
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
    fn double_hash(item: &[u8]) -> (u64, u64) {
        let hash1 = murmur_hash64a(item, 0);
        let hash2 = murmur_hash64a(item, hash1) | 1; // Ensure hash2 is odd
        (hash1, hash2)
    }

   /// Helper function to set a bit in the bit array.
    fn set_bit(bit_array: &mut [u8], index: usize) {
        let byte_index = index >> 3;
        let bit_index = index & 7;
        bit_array[byte_index] |= 1 << bit_index;
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
        let (bit_array, _size) = match self {
            BloomFilter::Writable { bit_array, size, .. } => (bit_array.as_slice(), *size),
            BloomFilter::ReadOnly { bit_array, size, .. } => (*bit_array, *size),
        };

        let byte_index = index >> 3;
        let bit_index = index & 7;
        (bit_array[byte_index] & (1 << bit_index)) != 0
    }

    /// Returns the filter size.
    fn size(&self) -> usize {
        match self {
            BloomFilter::Writable { size, .. } => *size,
            BloomFilter::ReadOnly { size, .. } => *size,
        }
    }

    /// Returns the number of hash functions used.
    fn hash_count(&self) -> usize {
        match self {
            BloomFilter::Writable { hash_count, .. } => *hash_count,
            BloomFilter::ReadOnly { hash_count, .. } => *hash_count,
        }
    }

    /// Converts the Bloom filter to an SSTable block format (serializing).
    pub fn to_block(&self) -> Vec<u8> {
        match self {
            BloomFilter::Writable { bit_array, size, hash_count } => {
                let mut buffer = Vec::new();
                varint::write_u64(*hash_count as u64, &mut buffer);
                varint::write_u64(*size as u64, &mut buffer);
                buffer.extend_from_slice(bit_array);
                buffer
            }
            _ => panic!("Cannot serialize a read-only BloomFilter"),
        }
    }

    /// Loads a Bloom filter from an SSTable block (zero-copy).
    pub fn from_block(block: &'a [u8]) -> Result<Self, Error> {
        let reader = ByteReader::new(block);
        let hash_count = reader.read_varint_u64()? as usize;
        let size = reader.read_varint_u64()? as usize;
        let bit_array = &block[reader.position()..]; // The remaining block is the bit array

        Ok(BloomFilter::ReadOnly {
            bit_array,
            size,
            hash_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        bloom.add(b"key1");
        bloom.add(b"key2");

        assert!(bloom.contains(b"key1"));
        assert!(bloom.contains(b"key2"));
        assert!(!bloom.contains(b"unknown"));

        // Convert to SST block format
        let block = bloom.to_block();

        // Load as a read-only filter from SST block
        let read_only_bloom = BloomFilter::from_block(&block).unwrap();

        assert!(read_only_bloom.contains(b"key1"));
        assert!(read_only_bloom.contains(b"key2"));
        assert!(!read_only_bloom.contains(b"unknown"));
    }
}


