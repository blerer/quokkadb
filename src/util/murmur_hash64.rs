/// MurmurHash64A implementation as a standalone function.
///
/// MurmurHash is a high-performance non-cryptographic hash function.
/// This specific version (MurmurHash64A) is designed for 64-bit systems
/// and provides a 64-bit hash output. It is known for its speed and
/// uniform distribution, making it suitable for hash tables and Bloom filters.
///
/// # Algorithm
/// 1. Initializes the hash value using the seed XORed with the data length multiplied by a constant.
/// 2. Processes the input data in 8-byte chunks, performing bitwise operations and multiplications
///    with a constant for mixing.
/// 3. Handles any remaining bytes at the end of the input.
/// 4. Finalizes the hash value with additional mixing to ensure good distribution.


/// Computes the 64-bit MurmurHash for the given input data.
///
/// # Parameters
/// - `data`: A byte slice (`&[u8]`) containing the data to hash.
/// - `seed`: The seed value used for initializing the hash.
///
/// # Returns
/// - A 64-bit hash value as `u64`.
pub fn murmur_hash64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    // Initialize the hash value
    let mut h = seed ^ ((data.len() as u64).wrapping_mul(M));

    // Process 8-byte chunks
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let mut k = u64::from_le_bytes(chunk.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Handle remaining bytes
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut tail = 0u64;
        for (i, &byte) in remainder.iter().enumerate() {
            tail |= (byte as u64) << (i * 8);
        }
        h ^= tail;
        h = h.wrapping_mul(M);
    }

    // Finalize the hash value
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_with_known_hashes() {
        let input: [&[u8]; 17] = [
            b"\xed\x53\xc4\xa5\x3b\x1b\xbd\xc2\x52\x7d\xc3\xef\x53\x5f\xae\x3b",
            b"\x21\x65\x59\x4e\xd8\x12\xf9\x05\x80\xe9\x1e\xed\xe4\x56\xbb",
            b"\x2b\x02\xb1\xd0\x3d\xce\x31\x3d\x97\xc4\x91\x0d\xf7\x17",
            b"\x8e\xa7\x9a\x02\xe8\xb9\x6a\xda\x92\xad\xe9\x2d\x21",
            b"\xa9\x6d\xea\x77\x06\xce\x1b\x85\x48\x27\x4c\xfe",
            b"\xec\x93\xa0\x12\x60\xee\xc8\x0a\xc5\x90\x62",
            b"\x55\x6d\x93\x66\x14\x6d\xdf\x00\x58\x99",
            b"\x3c\x72\x20\x1f\xd2\x59\x19\xdb\xa1",
            b"\x23\xa8\xb1\x87\x55\xf7\x8a\x4b",
            b"\xe2\x42\x1c\x2d\xc1\xe4\x3e",
            b"\x66\xa6\xb5\x5a\x74\xd9",
            b"\xe8\x76\xa8\x90\x76",
            b"\xeb\x25\x3f\x87",
            b"\x37\xa0\xa9",
            b"\x5b\x5d",
            b"\x7e",
            b"",
        ];
        let results: [u64; 17] = [
            0x4987cb15118a83d9, 0x28e2a79e3f0394d9, 0x8f4600d786fc5c05,
            0xa09b27fea4b54af3, 0x25f34447525bfd1e, 0x32fad4c21379c7bf,
            0x4b30b99a9d931921, 0x4e5dab004f936cdb, 0x06825c27bc96cf40,
            0xff4bf2f8a4823905, 0x7f7e950c064e6367, 0x821ade90caaa5889,
            0x6d28c915d791686a, 0x9c32649372163ba2, 0xd66ae956c14d5212,
            0x38ed30ee5161200f, 0x9bfae0a4e613fc3c,
        ];

        for (i, &data) in input.iter().enumerate() {
            assert_eq!(murmur_hash64a(data, 0xe17a1465), results[i]);
        }

        let results: [u64; 17] = [
            0x0822b1481a92e97b, 0xf8a9223fef0822dd, 0x4b49e56affae3a89,
            0xc970296e32e1d1c1, 0xe2f9f88789f1b08f, 0x2b0459d9b4c10c61,
            0x377e97ea9197ee89, 0xd2ccad460751e0e7, 0xff162ca8d6da8c47,
            0xf12e051405769857, 0xdabba41293d5b035, 0xacf326b0bb690d0e,
            0x0617f431bc1a8e04, 0x15b81f28d576e1b2, 0x28c1fe59e4f8e5ba,
            0x694dd315c9354ca9, 0xa97052a8f088ae6c
        ];

        for (i, &data) in input.iter().enumerate() {
            assert_eq!(murmur_hash64a(data, 0x344d1f5c), results[i]);
        }
    }
}