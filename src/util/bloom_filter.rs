use crate::io::byte_reader::ByteReader;
use crate::io::varint;
use crate::io::varint::compute_u64_vint_size;
use crate::util::murmur_hash64::murmur_hash64a;
use std::f64::consts::LN_2;
use std::io::Error;

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
        bit_array: Vec<u8>, // Used during MemTable flush / compaction output build
        size: usize,
        hash_count: usize,
    },
    ReadOnly {
        bit_array: &'a [u8], // Read directly from SST block (zero-copy)
        size: usize,
        hash_count: usize,
    },
}

impl<'a> BloomFilter<'a> {
    /// Compute Bloom filter parameters.
    ///
    /// Returns: (size_in_bits, hash_count, size_in_bytes)
    fn compute_params(expected_items: usize, false_positive_rate: f64) -> (usize, usize, usize) {
        assert!(expected_items > 0, "expected_items must be > 0");
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "false_positive_rate must be in (0, 1)"
        );

        let ln2_squared = LN_2.powi(2);
        let computed_size =
            -((expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let size = computed_size.max(1);

        let hash_count = (((size as f64 / expected_items as f64) * LN_2).ceil() as usize).max(1);
        let byte_size = (size + 7) >> 3;
        (size, hash_count, byte_size)
    }

    /// Cheap estimate of the serialized Bloom filter block size (uncompressed).
    ///
    /// The serialized format is:
    /// varint(hash_count) + varint(size_in_bits) + bit_array_bytes
    pub fn estimated_block_size_in_bytes(&self) -> usize {
        match self {
            BloomFilter::Writable {
                bit_array,
                size,
                hash_count,
            } => {
                compute_u64_vint_size(*hash_count as u64)
                    + compute_u64_vint_size(*size as u64)
                    + bit_array.len()
            }
            BloomFilter::ReadOnly { .. } => {
                panic!("Cannot estimate block size for a read-only BloomFilter")
            }
        }
    }

    /// Creates a new BloomFilter with dynamically calculated size and hash functions.
    ///
    /// # Arguments
    /// - `expected_items`: The estimated number of items to be stored in the filter.
    /// - `false_positive_rate`: The desired false positive probability (e.g., 0.01 for 1%).
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let (size, hash_count, byte_size) =
            Self::compute_params(expected_items, false_positive_rate);
        BloomFilter::Writable {
            bit_array: vec![0; byte_size],
            size,
            hash_count,
        }
    }

    /// Adds an item to the Bloom filter (only in `Writable` mode).
    pub fn add(&mut self, item: &[u8]) {
        let (hash1, hash2) = Self::double_hash(item);
        self.add_hashes(hash1, hash2);
    }

    /// Adds an already-hashed item (only in `Writable` mode).
    ///
    /// This is useful for "two-phase" building where you buffer hashes during compaction,
    /// and only allocate the bit-array once the final number of keys is known.
    fn add_hashes(&mut self, hash1: u64, hash2: u64) {
        if let BloomFilter::Writable {
            bit_array,
            size,
            hash_count,
        } = self
        {
            let normalized_hash2 = (hash2 % *size as u64) | 1; // Ensure step is at least 1
            for i in 0..*hash_count {
                let index = (hash1.wrapping_add((i as u64).wrapping_mul(normalized_hash2))
                    % *size as u64) as usize;
                Self::set_bit(bit_array, index);
            }
        } else {
            panic!("Cannot modify a read-only BloomFilter");
        }
    }

    /// Checks if an item is possibly in the Bloom Filter.
    pub fn contains(&self, item: &[u8]) -> bool {
        let (hash1, hash2) = Self::double_hash(item);
        let normalized_hash2 = (hash2 % self.size() as u64) | 1; // Ensure step is at least 1

        for i in 0..self.hash_count() {
            let index = (hash1.wrapping_add((i as u64).wrapping_mul(normalized_hash2))
                % self.size() as u64) as usize;
            if !self.get_bit(index) {
                return false;
            }
        }
        true
    }

    /// Uses double hashing to generate two hash values for an item.
    fn double_hash(item: &[u8]) -> (u64, u64) {
        let hash1 = murmur_hash64a(item, 0);
        let hash2 = murmur_hash64a(item, hash1);
        (hash1, hash2)
    }

    /// Helper function to set a bit in the bit array.
    fn set_bit(bit_array: &mut [u8], index: usize) {
        let byte_index = index >> 3;
        let bit_index = index & 7;
        bit_array[byte_index] |= 1 << bit_index;
    }

    /// Gets the value of a bit in the bit array at the specified index.
    fn get_bit(&self, index: usize) -> bool {
        let bit_array = match self {
            BloomFilter::Writable { bit_array, .. } => bit_array.as_slice(),
            BloomFilter::ReadOnly { bit_array, .. } => *bit_array,
        };

        let byte_index = index >> 3;
        let bit_index = index & 7;
        (bit_array[byte_index] & (1 << bit_index)) != 0
    }

    /// Returns the filter size in bits.
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
            BloomFilter::Writable {
                bit_array,
                size,
                hash_count,
            } => {
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

/// Two-phase Bloom filter builder for cases where the final number of keys is unknown up-front
/// (e.g. compaction output). It buffers only (hash1, hash2) pairs, then allocates the bit-array
/// when `build()` is called.
pub struct BloomFilterBuilder {
    false_positive_rate: f64,
    hashes: Vec<(u64, u64)>,
}

impl BloomFilterBuilder {
    pub fn new(false_positive_rate: f64) -> Self {
        Self {
            false_positive_rate,
            hashes: Vec::new(),
        }
    }

    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate
    }

    pub fn with_capacity(false_positive_rate: f64, capacity: usize) -> Self {
        Self {
            false_positive_rate,
            hashes: Vec::with_capacity(capacity),
        }
    }

    /// Cheap estimate of the serialized Bloom filter block size (uncompressed) that `build()` will
    /// produce.
    ///
    /// The serialized format is:
    /// varint(hash_count) + varint(size_in_bits) + bit_array_bytes
    pub fn estimated_block_size_in_bytes(&self) -> usize {
        let expected_items = self.len();
        if expected_items == 0 {
            return 0;
        }

        let (size_bits, hash_count, byte_size) =
            BloomFilter::compute_params(expected_items, self.false_positive_rate);

        compute_u64_vint_size(hash_count as u64)
            + compute_u64_vint_size(size_bits as u64)
            + byte_size
    }

    pub fn add(&mut self, item: &[u8]) {
        self.hashes.push(BloomFilter::double_hash(item));
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub fn build(&self) -> BloomFilter<'static> {
        assert!(
            !self.hashes.is_empty(),
            "cannot finish an empty BloomFilterBuilder"
        );

        let expected_items = self.hashes.len();
        let (size, hash_count, byte_size) =
            BloomFilter::compute_params(expected_items, self.false_positive_rate);

        let mut filter = BloomFilter::Writable {
            bit_array: vec![0; byte_size],
            size,
            hash_count,
        };

        for (h1, h2) in &self.hashes {
            filter.add_hashes(*h1, *h2);
        }

        filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_filter() {
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

    #[test]
    fn with_small_nbr_of_items() {
        let mut bloom = BloomFilter::new(4, 0.01);
        bloom.add(b"key1");
        bloom.add(b"key2");
        bloom.add(b"key3");
        bloom.add(b"key4");

        assert!(bloom.contains(b"key1"));
        assert!(bloom.contains(b"key2"));
        assert!(bloom.contains(b"key3"));
        assert!(bloom.contains(b"key4"));
        assert!(!bloom.contains(b"unknown"));

        // Convert to SST block format
        let block = bloom.to_block();

        // Load as a read-only filter from SST block
        let read_only_bloom = BloomFilter::from_block(&block).unwrap();

        assert!(read_only_bloom.contains(b"key1"));
        assert!(read_only_bloom.contains(b"key2"));
        assert!(read_only_bloom.contains(b"key3"));
        assert!(read_only_bloom.contains(b"key4"));
        assert!(!read_only_bloom.contains(b"unknown"));
    }

    #[test]
    fn builder_can_finish_without_expected_items() {
        let mut b = BloomFilterBuilder::new(0.01);
        b.add(b"key1");
        b.add(b"key2");
        b.add(b"key3");

        let bloom = b.build();
        assert!(bloom.contains(b"key1"));
        assert!(bloom.contains(b"key2"));
        assert!(bloom.contains(b"key3"));
        assert!(!bloom.contains(b"unknown"));

        let block = bloom.to_block();
        let ro = BloomFilter::from_block(&block).unwrap();
        assert!(ro.contains(b"key1"));
        assert!(ro.contains(b"key2"));
        assert!(ro.contains(b"key3"));
        assert!(!ro.contains(b"unknown"));
    }

    #[test]
    fn zero_expected_items_should_panic() {
        let result = std::panic::catch_unwind(|| {
            let _ = BloomFilter::new(0, 0.01);
        });
        assert!(result.is_err(), "Should panic for zero expected items");
    }

    #[test]
    fn high_false_positive_should_not_panic() {
        let mut filter = BloomFilter::new(1, 0.99);
        let item = b"high-fpr";
        filter.add(item);
        assert!(filter.contains(item));
    }

    #[test]
    fn low_false_positive_should_work() {
        let mut filter = BloomFilter::new(10, 0.0001);
        let item = b"low-fpr";
        filter.add(item);
        assert!(filter.contains(item));
    }

    #[test]
    fn should_handle_very_small_alloc() {
        let mut filter = BloomFilter::new(1, 0.9);
        filter.add(b"x");
        assert!(filter.contains(b"x"));
    }

    #[test]
    fn fpr_zero_should_panic() {
        let result = std::panic::catch_unwind(|| {
            let _ = BloomFilter::new(100, 0.0);
        });
        assert!(
            result.is_err(),
            "Should panic for false_positive_rate = 0.0"
        );
    }

    #[test]
    fn fpr_one_should_panic() {
        let result = std::panic::catch_unwind(|| {
            let _ = BloomFilter::new(100, 1.0);
        });
        assert!(
            result.is_err(),
            "Should panic for false_positive_rate = 1.0"
        );
    }

    #[test]
    fn fpr_negative_should_panic() {
        let result = std::panic::catch_unwind(|| {
            let _ = BloomFilter::new(100, -0.01);
        });
        assert!(
            result.is_err(),
            "Should panic for negative false_positive_rate"
        );
    }

    #[test]
    fn fpr_greater_than_one_should_panic() {
        let result = std::panic::catch_unwind(|| {
            let _ = BloomFilter::new(100, 1.5);
        });
        assert!(
            result.is_err(),
            "Should panic for false_positive_rate > 1.0"
        );
    }

    #[test]
    fn serialize_readonly_should_panic() {
        let mut writable = BloomFilter::new(10, 0.01);
        writable.add(b"key");
        let block = writable.to_block();
        let readonly = BloomFilter::from_block(&block).unwrap();

        let result = std::panic::catch_unwind(move || readonly.to_block());
        assert!(
            result.is_err(),
            "Should panic when serializing a read-only filter"
        );
    }

    #[test]
    fn empty_builder_finish_should_panic() {
        let result = std::panic::catch_unwind(|| {
            let builder = BloomFilterBuilder::new(0.01);
            builder.build()
        });
        assert!(
            result.is_err(),
            "Should panic when finishing an empty builder"
        );
    }

    // --- Statistical FPR Validation ---

    #[test]
    fn statistical_false_positive_rate_validation() {
        let expected_items = 10_000;
        let target_fpr = 0.01; // 1%
        let mut bloom = BloomFilter::new(expected_items, target_fpr);

        // Insert keys
        for i in 0..expected_items {
            let key = format!("inserted_key_{}", i);
            bloom.add(key.as_bytes());
        }

        // Verify all inserted keys are found (no false negatives)
        for i in 0..expected_items {
            let key = format!("inserted_key_{}", i);
            assert!(
                bloom.contains(key.as_bytes()),
                "False negative detected for key: {}",
                key
            );
        }

        // Test for false positives with keys that were NOT inserted
        let test_count = 100_000;
        let mut false_positives = 0;
        for i in 0..test_count {
            let key = format!("not_inserted_key_{}", i);
            if bloom.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let observed_fpr = false_positives as f64 / test_count as f64;

        // Allow some statistical tolerance (3x the target FPR)
        // This is generous to avoid flaky tests while still catching gross errors
        let max_acceptable_fpr = target_fpr * 3.0;
        assert!(
            observed_fpr <= max_acceptable_fpr,
            "Observed FPR ({:.4}) exceeds acceptable threshold ({:.4}). False positives: {}/{}",
            observed_fpr,
            max_acceptable_fpr,
            false_positives,
            test_count
        );
    }

    // --- Binary/Edge-Case Keys ---

    #[test]
    fn empty_key() {
        let mut bloom = BloomFilter::new(10, 0.01);
        bloom.add(b"");
        assert!(bloom.contains(b""));
        assert!(!bloom.contains(b"nonempty"));
    }

    #[test]
    fn binary_keys_with_null_bytes() {
        let mut bloom = BloomFilter::new(10, 0.01);

        let key1 = b"hello\x00world";
        let key2 = b"hello\x00other";
        let key3 = b"\x00\x00\x00";
        let key4 = b"\x00";

        bloom.add(key1);
        bloom.add(key2);
        bloom.add(key3);
        bloom.add(key4);

        assert!(bloom.contains(key1));
        assert!(bloom.contains(key2));
        assert!(bloom.contains(key3));
        assert!(bloom.contains(key4));
    }

    #[test]
    fn very_long_key() {
        let mut bloom = BloomFilter::new(10, 0.01);

        // 1 MB key
        let long_key: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        bloom.add(&long_key);

        assert!(bloom.contains(&long_key));
        assert!(!bloom.contains(b"short"));
    }

    #[test]
    fn duplicate_keys() {
        let mut bloom = BloomFilter::new(10, 0.01);

        // Adding the same key multiple times should work
        bloom.add(b"duplicate");
        bloom.add(b"duplicate");
        bloom.add(b"duplicate");

        assert!(bloom.contains(b"duplicate"));
    }

    // --- Builder Tests ---

    #[test]
    fn builder_with_capacity() {
        let mut builder = BloomFilterBuilder::with_capacity(0.01, 100);

        for i in 0..50 {
            let key = format!("key_{}", i);
            builder.add(key.as_bytes());
        }

        assert_eq!(builder.len(), 50);
        assert!(!builder.is_empty());

        let bloom = builder.build();

        for i in 0..50 {
            let key = format!("key_{}", i);
            assert!(bloom.contains(key.as_bytes()));
        }
    }

    #[test]
    fn builder_len_and_is_empty() {
        let mut builder = BloomFilterBuilder::new(0.01);

        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.add(b"first");
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);

        builder.add(b"second");
        assert_eq!(builder.len(), 2);
    }
}
