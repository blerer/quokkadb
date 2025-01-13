use std::io::{Error, ErrorKind};

/// # Variable-Length Integer Serialization
///
/// This module implements variable-length integer (varint) serialization and deserialization for `u64` values.
/// This format is the one used by Apache Cassandra for VInt encoding (variable-length integer).
///
/// ## Serialization Format
///
/// The varint format encodes integers into a variable number of bytes, optimizing for small numbers by using fewer bytes:
///
/// - **1 Byte**: Values in the range [0, 127] are encoded in a single byte, as-is.
/// - **2 to 9 Bytes**: Larger values are encoded across multiple bytes, with the first byte indicating the number of extra bytes required:
///     - The first byte reserves its leading `N` bits (where `N` equals the number of extra bytes) as a prefix to indicate how many additional bytes follow.
///     - The remaining bits in the first byte and the subsequent bytes store the actual value in big-endian order.
/// - **9 Bytes**: Values larger than 2^56 (or `u64::MAX`) are encoded with a fixed-size 9-byte format.
///
/// ### Example Encodings
///
/// - `0x00`: Encodes the integer `0`.
/// - `0x7F`: Encodes the integer `127`.
/// - `0x80 0x80`: Encodes the integer `128`.
/// - `0xFF <8-byte BE>`: Encodes any 64-bit value in a fixed 9-byte format.
///
/// ## Functions
///
/// - `write_u64`: Serializes a `u64` value into a `Vec<u8>` using the varint format.
/// - `read_u64`: Deserializes a `u64` value from a slice of bytes encoded in the varint format.
///
/// ## Error Handling
///
/// - `write_u64` returns an error if the size of the encoded varint exceeds 9 bytes, which is invalid for `u64` values.
/// - `read_u64` checks for input size mismatches and truncated inputs, returning appropriate errors.
///
/// ## Notes
///
/// - The encoding prioritizes compactness for small values while maintaining efficiency for larger ones.
/// - Utility functions such as `compute_u64_vint_size` help calculate the number of bytes required for a given value.
///

pub fn write_u64(x: u64, output: &mut Vec<u8>) -> Result<(), Error> {
    let size = compute_u64_vint_size(x);
    if size == 1 {
        output.push(x as u8);
    } else if size < 9 {
        // Create a buffer with the given size
        let mut encoding_space = vec![0u8; size as usize];
        let mut value = x; // Make the value mutable for bitwise operations
        let extra_bytes = size - 1;

        // Fill the buffer from the most significant byte to the least significant
        for i in (0..=extra_bytes as usize).rev() {
            encoding_space[i] = value as u8;
            value >>= 8;
        }
        // Encode the number of extra bytes into the first byte
        encoding_space[0] |= encode_extra_bytes_to_read(extra_bytes);
        output.extend(encoding_space);

    } else if size == 9 {
        output.push(0xFF);
        output.extend_from_slice(&x.to_be_bytes());
    } else {
        return Err(Error::new(ErrorKind::Other, "Invalid vint size"))
    }
    Ok(())
}

pub fn write_i64(x: i64, output: &mut Vec<u8>) -> Result<(), Error> {
    write_u64(encode_zigzag_64(x), output)
}

pub fn read_u64(input: &[u8]) -> Result<u64, Error> {
    if input.is_empty() {
        return Err(Error::new(ErrorKind::Other, "Input is empty"));
    }

    let first_byte = input[0];

    let size = number_of_extra_bytes_to_read(&first_byte);
    if input.len() < size as usize {
        return Err(Error::new(ErrorKind::Other, "Input too short for the specified size"));
    }
    let mask = first_byte_value_mask(size) as u64;

    let mut retval = first_byte as u64 & mask;
    for i in 1..=size as usize {
        retval <<= 8;
        retval |= input[i] as u64;
    }
    Ok(retval)
}

pub fn read_i64(input: &[u8]) -> Result<i64, Error> {
    read_u64(input).map(|u| decode_zigzag_64(u))
}

/// Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
/// into values that can be efficiently encoded with varint.  (Otherwise,
/// negative values must be sign-extended to 64 bits to be varint encoded,
/// thus always taking 10 bytes on the wire.)
fn decode_zigzag_64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
/// into values that can be efficiently encoded with varint.  (Otherwise,
/// negative values must be sign-extended to 64 bits to be varint encoded,
/// thus always taking 10 bytes on the wire.)
fn encode_zigzag_64(n: i64) -> u64 {
    ((n << 1) as u64) ^ ((n >> 63) as u64)
}
fn number_of_extra_bytes_to_read(first_byte: &u8) -> u8 {
    first_byte.leading_ones() as u8
}

fn compute_u64_vint_size(x: u64) -> u8 {
    let magnitude = (x | 1).leading_zeros();
    // the formula below is hand-picked to match the original 9 - ((magnitude - 1) / 7)
    ((639 - magnitude * 9) >> 6) as u8
}

// & this with the first byte to give the value part for a given extraBytesToRead encoded in the byte
fn first_byte_value_mask(extra_bytes_to_read: u8) -> u8 {
    // by including the known 0bit in the mask, we can use this for encodeExtraBytesToRead
    if extra_bytes_to_read >= 8 {
        0
    } else {
        0xffu8 >> extra_bytes_to_read
    }
}

fn encode_extra_bytes_to_read(extra_bytes_to_read: u8) -> u8 {
    // because we have an extra bit in the value mask, we just need to invert it
    !first_byte_value_mask(extra_bytes_to_read)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_u64_vint_size() {
        assert_eq!(compute_u64_vint_size(0), 1);
        assert_eq!(compute_u64_vint_size(127), 1);
        assert_eq!(compute_u64_vint_size(128), 2);
        assert_eq!(compute_u64_vint_size(16_383), 2);
        assert_eq!(compute_u64_vint_size(16_384), 3);
        assert_eq!(compute_u64_vint_size(u64::MAX), 9);
    }

    #[test]
    fn test_first_byte_value_mask() {
        assert_eq!(first_byte_value_mask(0), 0xFF);
        assert_eq!(first_byte_value_mask(1), 0x7F);
        assert_eq!(first_byte_value_mask(2), 0x3F);
        assert_eq!(first_byte_value_mask(7), 0x01);
    }

    #[test]
    fn test_encode_extra_bytes_to_read() {
        assert_eq!(encode_extra_bytes_to_read(0), 0);
        assert_eq!(encode_extra_bytes_to_read(1), 0x80);
        assert_eq!(encode_extra_bytes_to_read(2), 0xC0);
        assert_eq!(encode_extra_bytes_to_read(3), 0xE0);
        assert_eq!(encode_extra_bytes_to_read(7), 0xFE);
    }

    #[test]
    fn test_number_of_extra_bytes_to_read() {
        assert_eq!(number_of_extra_bytes_to_read(&0x00), 0);
        assert_eq!(number_of_extra_bytes_to_read(&0xC0), 2);
        assert_eq!(number_of_extra_bytes_to_read(&0x80), 1);
        assert_eq!(number_of_extra_bytes_to_read(&0xE0), 3);
        assert_eq!(number_of_extra_bytes_to_read(&0xFE), 7);
        assert_eq!(number_of_extra_bytes_to_read(&0xFF), 8);
    }

    #[test]
    fn test_write_and_read_u64() {

        let mut output = Vec::new();

        let test_values = vec![
            0u64,
            127u64,
            128u64,
            16_383u64,
            16_384u64,
            u64::MAX,
            u64::MAX - 1,
        ];


        for &value in &test_values {
            output.clear();

            // Test write_u64
            write_u64(value, &mut output).expect("write_u64 failed");

            println!("-> {:?}", output);

            // Test read_u64
            let decoded = read_u64(&output).expect("read_u64 failed");

            let binary = format!("{:b}", value);
            println!("Binary representation: {}", binary);

            let binary2 = format!("{:b}", decoded);
            println!("Binary representation: {}", binary2);

            assert_eq!(value, decoded, "Value mismatch for {value}");
        }
    }

    #[test]
    fn test_write_and_read_i64() {
        let mut output = Vec::new();

        let test_values = vec![
            0i64,
            -0i64,
            127i64,
            -127i64,
            128i64,
            -128i64,
            16_383i64,
            -16_384i64,
            16_384i64,
            -16_384i64,
            i64::MIN,
            i64::MAX,
        ];

        for &value in &test_values {
            output.clear();

            // Test write_u64
            write_i64(value, &mut output).expect("write_i64 failed");

            println!("-> {:?}", encode_zigzag_64(value));

            // Test read_u64
            let decoded = read_i64(&output).expect("read_i64 failed");

            assert_eq!(value, decoded, "Value mismatch for {value}");
        }
    }

    #[test]
    fn test_encode_decode() {

        let test_values = vec![
            0i64,
            -0i64,
            127i64,
            -127i64,
            128i64,
            -128i64,
            16_383i64,
            -16_384i64,
            16_384i64,
            -16_384i64,
            i64::MIN,
            i64::MAX,
        ];

        for &value in &test_values {
            assert_eq!(value, decode_zigzag_64(encode_zigzag_64(value)), "Value mismatch for {value}");
        }
    }

    #[test]
    fn test_invalid_vint_size() {
        let mut output = Vec::new();
        assert!(write_u64(u64::MAX, &mut output).is_ok());
        assert!(read_u64(&[]).is_err()); // Empty input

        let invalid_input = vec![0xFF, 0x00, 0x00, 0x00];
        assert!(read_u64(&invalid_input).is_err()); // Truncated input
    }
}