use crate::io::byte_reader::ByteReader;
use crate::io::ZeroCopy;
use crate::storage::internal_key::encode_record_key;
use bson::oid::ObjectId;
use bson::{to_vec, Bson, Document};
use std::io::{Error, ErrorKind, Result};

pub fn as_key_value(doc: &Document) -> bson::ser::Result<(Vec<u8>, Vec<u8>)> {
    let id = doc.get("id").unwrap();
    let user_key = id.try_into_key()?;
    let value = to_vec(&doc)?;
    Ok((user_key, value))
}

fn read_cstring<'a>(reader: &'a ByteReader<'a>) -> Result<&'a [u8]> {
    let end = reader.find_next_by(|b| b == 0);
    if let Some(end) = end {
        Ok(reader.read_fixed_slice(end + 1)?)
    } else {
        Err(invalid_input("End of CString could not be found"))
    }
}

fn skip_cstring<'a>(reader: &'a ByteReader<'a>) -> Result<()> {
    let end = reader.find_next_by(|b| b == 0);
    if let Some(end) = end {
        reader.skip(end + 1)
    } else {
        Err(invalid_input("End of CString could not be found"))
    }
}

fn document_length(data: &[u8], offset: usize) -> Result<usize> {
    if data.len() - offset < 5 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Not enough bytes for a BSON document",
        ));
    }
    Ok(data.read_i32_le(offset) as usize)
}

pub fn find_bson_field_value<'a>(
    reader: &'a ByteReader<'a>,
    field_name: &'a [u8],
) -> Result<Option<&'a [u8]>> {
    // Read document length (first 4 bytes)
    let doc_length = reader.read_i32_le()? as usize;

    if doc_length > reader.remaining() + 4 {
        return Err(invalid_input("Invalid BSON document length"));
    }

    while reader.has_remaining() {
        // Read field type (1 byte)
        let bson_type = match reader.read_u8() {
            Ok(t) => t,
            Err(_) => return Ok(None), // End of document
        };

        // Read field name (CString)
        let start_pos = reader.position();
        let field_bytes = match read_cstring(reader) {
            Ok(bytes) => bytes,
            Err(_) => return Err(invalid_input("Invalid BSON field name")),
        };

        // Check if the field name matches
        if field_bytes == field_name {
            // Extract the BSON value
            return extract_bson_value(bson_type, reader).map(Some);
        }

        // Skip over the field name (CString includes null byte)
        let new_pos = start_pos + field_bytes.len() + 1;
        reader.seek(new_pos)?;

        // Skip value bytes (using extract_bson_value to determine length, but ignore the value)
        let _ = skip_value(bson_type, reader)?;
    }

    Ok(None) // Field not found
}

pub fn extract_bson_value<'a>(bson_type: u8, reader: &'a ByteReader<'a>) -> Result<&'a [u8]> {
    match bson_type {
        0x01 => {
            // Double (8 bytes)
            reader.read_fixed_slice(8)
        }
        0x02 => {
            // String (length-prefixed + null terminator)
            let str_len = reader.read_i32_le()? as usize;
            reader.read_fixed_slice(4 + str_len)
        }
        0x03 | 0x04 => {
            // Embedded Document or Array (length-prefixed)
            let doc_len = reader.read_i32_le()? as usize;
            reader.read_fixed_slice(4 + doc_len)
        }
        0x05 => {
            // Binary (length-prefixed + subtype + data)
            let bin_len = reader.read_i32_le()? as usize;
            reader.read_fixed_slice(4 + 1 + bin_len) // 4-byte length + subtype + data
        }
        0x07 => {
            // ObjectId (12 bytes)
            reader.read_fixed_slice(12)
        }
        0x08 => {
            // Boolean (1 byte)
            reader.read_fixed_slice(1)
        }
        0x09 => {
            // UTC Datetime (8 bytes)
            reader.read_fixed_slice(8)
        }
        0x0A => {
            // Null
            Ok(&[])
        }
        0x0B => {
            // Regular Expression (cstring + cstring)
            let start = reader.position();
            let mut end = reader.find_next_by(|b| b == 0);
            if let Some(end) = end {
                reader.seek(end + 1)?;
            } else {
                return Err(invalid_input("End of first CString could not be found"));
            }
            end = reader.find_next_by(|b| b == 0);
            if let Some(end) = end {
                reader.seek(start)?;
                reader.read_fixed_slice(end + 1)
            } else {
                Err(invalid_input("End of second CString could not be found"))
            }
        }
        0x0D => {
            // JavaScript Code (length-prefixed string)
            let str_len = reader.read_i32_le()? as usize;
            reader.read_fixed_slice(4 + str_len)
        }
        0x0F => {
            // JavaScript Code w/ Scope (length-prefixed + string + document)
            let total_len = reader.read_i32_le()? as usize;
            reader.read_fixed_slice(total_len)
        }
        0x10 => {
            // Int32 (4 bytes)
            reader.read_fixed_slice(4)
        }
        0x11 => {
            // Timestamp (8 bytes: increment + timestamp)
            reader.read_fixed_slice(8)
        }
        0x12 => {
            // Int64 (8 bytes)
            reader.read_fixed_slice(8)
        }
        0x13 => {
            // Decimal128 (16 bytes)
            reader.read_fixed_slice(16)
        }
        _ => Err(invalid_input("Unsupported BSON type")),
    }
}

pub fn skip_value<'a>(bson_type: u8, reader: &'a ByteReader<'a>) -> Result<()> {
    match bson_type {
        0x01 => reader.skip(8), // Double (8 bytes)
        0x02 => {
            // String (length-prefixed)
            let str_len = reader.read_i32_le()? as usize;
            reader.skip(str_len)
        }
        0x03 | 0x04 => {
            // Document or Array (length-prefixed)
            let doc_len = reader.read_i32_le()? as usize;
            reader.skip(doc_len - 4) // Already read 4 bytes
        }
        0x05 => {
            // Binary (length-prefixed + subtype + data)
            let bin_len = reader.read_i32_le()? as usize;
            reader.skip(bin_len + 1) // 4-byte length + subtype
        }
        0x07 => reader.skip(12), // ObjectId (12 bytes)
        0x08 => reader.skip(1),  // Boolean (1 byte)
        0x09 => reader.skip(8),  // UTC Datetime (8 bytes)
        0x0A => Ok(()),          // Null (0 bytes)
        0x0B => {
            // Regular Expression (CString + CString)
            skip_cstring(reader)?; // Skip pattern
            skip_cstring(reader) // Skip options
        }
        0x0D => {
            // JavaScript Code (length-prefixed string)
            let str_len = reader.read_i32_le()? as usize;
            reader.skip(str_len)
        }
        0x0F => {
            // JavaScript Code w/ Scope (length + script + doc)
            let total_len = reader.read_i32_le()? as usize;
            reader.skip(total_len - 4) // Already read 4 bytes
        }
        0x10 => reader.skip(4),  // Int32 (4 bytes)
        0x11 => reader.skip(8),  // Timestamp (8 bytes)
        0x12 => reader.skip(8),  // Int64 (8 bytes)
        0x13 => reader.skip(16), // Decimal128 (16 bytes)
        _ => Err(Error::new(ErrorKind::InvalidData, "Unsupported BSON type")),
    }
}

fn invalid_input(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, message)
}

/// Trait for converting BSON values into sortable byte keys
pub trait BsonKey {
    fn try_into_key(&self) -> Result<Vec<u8>>;
}

/// Implement `BsonKey` for `Bson`
impl BsonKey for Bson {
    fn try_into_key(&self) -> Result<Vec<u8>> {
        let mut key = Vec::new();

        match self {
            Bson::MinKey => key.push(0x00), // MinKey -> Lowest possible byte

            Bson::Null => key.push(0x10),

            Bson::Int32(n) => {
                key.push(0x20);
                let mut bytes = n.to_be_bytes();
                bytes[0] ^= 0x80; // Flip sign bit for sorting
                key.extend_from_slice(&bytes);
            }
            Bson::Int64(n) => {
                key.push(0x21);
                let mut bytes = n.to_be_bytes();
                bytes[0] ^= 0x80;
                key.extend_from_slice(&bytes);
            }
            Bson::Double(n) => {
                key.push(0x22);
                let mut bytes = n.to_bits().to_be_bytes();

                if n.is_sign_positive() {
                    bytes[0] ^= 0x80; // Flip sign bit for positives
                } else {
                    for byte in bytes.iter_mut() {
                        *byte ^= 0xFF; // Flip all bits for negatives
                    }
                }
                key.extend_from_slice(&bytes);
            }
            Bson::Decimal128(d) => {
                key.push(0x23); // BSON type prefix for Decimal128

                let bytes = d.bytes(); // Get raw 16-byte representation (Little-Endian)

                // **Step 1: Swap low and high parts (since BSON stores them as little-endian)**
                let mut swapped_bytes = [0u8; 16];
                swapped_bytes[..8].copy_from_slice(&bytes[8..]); // Move HIGH to LOW position
                swapped_bytes[8..].copy_from_slice(&bytes[..8]); // Move LOW to HIGH position

                // **Step 2: Reverse bytes within each 64-bit part (convert to big-endian)**
                swapped_bytes[..8].reverse(); // Reverse first 8 bytes (new HIGH)
                swapped_bytes[8..].reverse(); // Reverse last 8 bytes (new LOW)

                // **Example: Original BSON Decimal128 (Little-Endian)**
                // Suppose we have the decimal value `-1000.12345`
                // Stored in BSON as: `[57, 17, 246, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 54, 176]`
                // After swapping & reversing:
                // `[176, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 246, 17, 57]`

                // **Step 3: Extract and Adjust Exponent (E)**
                let exponent =
                    (((swapped_bytes[0] & 0b0011_1111) as u16) << 8) | (swapped_bytes[1] as u16);
                // The exponent is stored in the **first 14 bits** of the high 64-bit part.
                // We extract it using bitwise operations.

                // **Step 4: Check if Coefficient is Zero (`C == 0`)**
                let is_zero = swapped_bytes[2..16].iter().all(|&b| b == 0);
                let is_negative = swapped_bytes[0] & 0x80 != 0;

                if is_zero {
                    // Ensure exponent is the smallest possible
                    swapped_bytes[0] = if is_negative { 0xFF } else { 0x00 }; // Ensure `-0.0` sorts before `0.0`
                    swapped_bytes[1] = 0x00;
                } else {
                    // **Step 5: Normalize Coefficient (Mantissa)**
                    // The coefficient (C) is an explicit integer in Decimal128.
                    // To ensure proper ordering, we need to shift it so that the most significant bit is `1`.
                    let mut leading_bit_position = 0;
                    for i in 2..16 {
                        if swapped_bytes[i] != 0 {
                            leading_bit_position = i;
                            break;
                        }
                    }

                    // **Adjust exponent based on shift**
                    let shift_amount = leading_bit_position * 8; // Convert byte index to bit shift
                    let new_exponent = exponent.saturating_sub(shift_amount as u16);

                    // **Store the adjusted exponent back**
                    swapped_bytes[0] =
                        ((new_exponent >> 8) & 0x3F) as u8 | (swapped_bytes[0] & 0x80);
                    swapped_bytes[1] = (new_exponent & 0xFF) as u8;
                }

                // **Step 6: Handle Sign Bit for Sorting**
                if swapped_bytes[0] & 0x80 == 0 {
                    // **Positive numbers: Flip only the sign bit**
                    swapped_bytes[0] ^= 0x80;
                } else {
                    // **Negative numbers: Flip exponent + coefficient (two’s complement-like)**
                    for i in 0..16 {
                        swapped_bytes[i] ^= 0xFF;
                    }
                }

                // **Step 7: Append sorted bytes to the key**
                key.extend_from_slice(&swapped_bytes);
            }
            Bson::String(s) => {
                key.push(0x30);
                key.extend_from_slice(s.as_bytes());
                key.push(0x00); // Null terminator
            }
            Bson::Binary(bin) => {
                key.push(0x40);
                key.extend_from_slice(&bin.bytes);
            }
            Bson::ObjectId(oid) => {
                key.push(0x50);
                key.extend_from_slice(&oid.bytes());
            }
            Bson::Boolean(b) => {
                key.push(if *b { 0x61 } else { 0x60 }); // Boolean (false = 0x60, true = 0x61)
            }
            Bson::DateTime(dt) => {
                key.push(0x70);
                key.extend_from_slice(&dt.timestamp_millis().to_be_bytes());
            }
            Bson::Timestamp(ts) => {
                key.push(0x80);
                key.extend_from_slice(&ts.time.to_be_bytes());
                key.extend_from_slice(&ts.increment.to_be_bytes());
            }
            Bson::MaxKey => key.push(0xFF), // MaxKey -> Highest possible byte

            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("Unsupported BSON type: {:?}", self),
                ))
            }
        }

        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::util::bson_utils::BsonKey;
    use bson::{oid::ObjectId, Binary, Bson, DateTime, Decimal128, Timestamp};
    use std::str::FromStr;

    /// Helper function to check lexicographic ordering
    fn assert_ordering(a: &Bson, b: &Bson) {
        let key_a = a.try_into_key().expect("Failed to encode BSON A");
        let key_b = b.try_into_key().expect("Failed to encode BSON B");
        assert!(key_a < key_b, "Expected {:?} < {:?}", a, b);
    }

    /// **Test MinKey & MaxKey** (absolute ordering boundaries)
    #[test]
    fn test_minkey_maxkey() {
        assert_ordering(&Bson::MinKey, &Bson::Null);
        assert_ordering(&Bson::Int32(0), &Bson::MaxKey);
    }

    /// **Test Integer Sorting**
    #[test]
    fn test_int32_ordering() {
        assert_ordering(&Bson::Int32(-1000), &Bson::Int32(-1));
        assert_ordering(&Bson::Int32(-1), &Bson::Int32(0));
        assert_ordering(&Bson::Int32(0), &Bson::Int32(1));
        assert_ordering(&Bson::Int32(1), &Bson::Int32(1000));
    }

    #[test]
    fn test_int64_ordering() {
        assert_ordering(&Bson::Int64(-1_000_000_000), &Bson::Int64(-1));
        assert_ordering(&Bson::Int64(-1), &Bson::Int64(0));
        assert_ordering(&Bson::Int64(0), &Bson::Int64(1));
        assert_ordering(&Bson::Int64(1), &Bson::Int64(1_000_000_000));
    }

    /// **Test Double Precision Floating-Point Sorting**
    #[test]
    fn test_double_ordering() {
        assert_ordering(&Bson::Double(-1000.0), &Bson::Double(-1.0));
        assert_ordering(&Bson::Double(-1.0), &Bson::Double(-0.1));
        assert_ordering(&Bson::Double(-0.1), &Bson::Double(0.0));
        assert_ordering(&Bson::Double(0.0), &Bson::Double(0.1));
        assert_ordering(&Bson::Double(0.1), &Bson::Double(1.0));
        assert_ordering(&Bson::Double(1.0), &Bson::Double(1000.0));
    }

    /// **Test Special Double Cases**
    #[test]
    fn test_double_edge_cases() {
        assert_ordering(&Bson::Double(-1.0), &Bson::Double(1.0));
        assert_ordering(&Bson::Double(-0.0), &Bson::Double(0.0));
        assert_ordering(&Bson::Double(1.0e-10), &Bson::Double(1.0));
        assert_ordering(&Bson::Double(1.0), &Bson::Double(1.0e10));
    }

    /// **Test Decimal128 Sorting**
    #[test]
    fn test_decimal128_ordering() {
        let dec1 = Decimal128::from_str("-1000.12345").unwrap();
        let dec2 = Decimal128::from_str("-1.0").unwrap();
        let dec3 = Decimal128::from_str("0.0").unwrap();
        let dec4 = Decimal128::from_str("1.0").unwrap();
        let dec5 = Decimal128::from_str("1000.12345").unwrap();

        assert_decimal_ordering(&dec1, &dec2);
        assert_decimal_ordering(&dec2, &dec3);
        assert_decimal_ordering(&dec3, &dec4);
        assert_decimal_ordering(&dec4, &dec5);
    }

    fn assert_decimal_ordering(a: &Decimal128, b: &Decimal128) {
        let key_a = &Bson::Decimal128(*a).try_into_key().unwrap();
        let key_b = &Bson::Decimal128(*b).try_into_key().unwrap();
        assert!(
            key_a < key_b,
            "Expected {:?} < {:?}, but got {:?} >= {:?}",
            a,
            b,
            key_a,
            key_b
        );
    }

    /// Tests that leading zeros in the coefficient (`C`) do not break sorting.
    #[test]
    fn test_decimal128_leading_zero_normalization() {
        let dec1 = Decimal128::from_str("1.000000000000000000000000000000000").unwrap();
        let dec2 = Decimal128::from_str("1.000000000000000000000000000000001").unwrap();

        // `dec1` and `dec2` should be sorted correctly, even if `dec1` has trailing zeroes.
        assert_decimal_ordering(&dec1, &dec2);
    }

    /// Tests sorting when numbers have different exponent values.
    #[test]
    fn test_decimal128_exponent_adjustment() {
        let dec1 = Decimal128::from_str("1.0").unwrap();
        let dec2 = Decimal128::from_str("10.0").unwrap();
        let dec3 = Decimal128::from_str("100.0").unwrap();

        // Ensure proper ordering even when the exponent is different.
        assert_decimal_ordering(&dec1, &dec2);
        assert_decimal_ordering(&dec2, &dec3);
    }

    /// Tests very large and very small values to check that sorting handles extreme exponent shifts correctly.
    #[test]
    fn test_decimal128_extreme_values() {
        let min_value = Decimal128::from_str("1E-6176").unwrap();
        let max_value = Decimal128::from_str("9.999999999999999999999999999999999E6111").unwrap();

        // Ensure smallest value sorts before largest value.
        assert_decimal_ordering(&min_value, &max_value);
    }

    /// Tests sorting of numbers with small and large coefficients but similar exponents.
    #[test]
    fn test_decimal128_large_vs_small_coefficients() {
        let small_coeff = Decimal128::from_str("0.00000000000000000001").unwrap();
        let large_coeff = Decimal128::from_str("10000000000000000000.0").unwrap();

        assert_decimal_ordering(&small_coeff, &large_coeff);
    }

    /// Tests correct sorting of zero (`0.0`) and negative zero (`-0.0`).
    #[test]
    fn test_decimal128_zero_ordering() {
        let dec_zero = Decimal128::from_str("0.0").unwrap();
        let dec_one = Decimal128::from_str("1.0").unwrap();
        let dec_neg_zero = Decimal128::from_str("-0.0").unwrap();

        assert_decimal_ordering(&dec_neg_zero, &dec_zero);
        assert_decimal_ordering(&dec_zero, &dec_one);
    }

    /// Tests sorting of negative and positive numbers across exponent ranges.
    #[test]
    fn test_decimal128_negative_vs_positive() {
        let neg_small = Decimal128::from_str("-1.0").unwrap();
        let neg_large = Decimal128::from_str("-1000.0").unwrap();
        let pos_small = Decimal128::from_str("1.0").unwrap();
        let pos_large = Decimal128::from_str("1000.0").unwrap();

        // Ensure negative numbers sort before positive numbers.
        assert_decimal_ordering(&neg_large, &neg_small);
        assert_decimal_ordering(&neg_small, &pos_small);
        assert_decimal_ordering(&pos_small, &pos_large);
    }

    /// **Test String Ordering**
    #[test]
    fn test_string_ordering() {
        assert_ordering(
            &Bson::String("abc".to_string()),
            &Bson::String("abd".to_string()),
        );
        assert_ordering(
            &Bson::String("".to_string()),
            &Bson::String("a".to_string()),
        );
        assert_ordering(
            &Bson::String("a".to_string()),
            &Bson::String("aa".to_string()),
        );
    }

    /// **Test Boolean Ordering**
    #[test]
    fn test_boolean_ordering() {
        assert_ordering(&Bson::Boolean(false), &Bson::Boolean(true));
    }

    /// **Test ObjectId Ordering**
    #[test]
    fn test_objectid_ordering() {
        let oid1 = ObjectId::parse_str("000000000000000000000000").unwrap();
        let oid2 = ObjectId::parse_str("ffffffffffffffffffffffff").unwrap();
        assert_ordering(&Bson::ObjectId(oid1), &Bson::ObjectId(oid2));
    }

    /// **Test Binary Sorting**
    #[test]
    fn test_binary_ordering() {
        let bin1 = Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![1, 2, 3],
        };
        let bin2 = Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![1, 2, 4],
        };
        assert_ordering(&Bson::Binary(bin1), &Bson::Binary(bin2));
    }

    /// **Test DateTime Ordering**
    #[test]
    fn test_datetime_ordering() {
        let dt1 = DateTime::from_millis(1609459200000); // 2021-01-01 00:00:00 UTC
        let dt2 = DateTime::from_millis(1640995200000); // 2022-01-01 00:00:00 UTC
        assert_ordering(&Bson::DateTime(dt1), &Bson::DateTime(dt2));
    }

    /// **Test Timestamp Ordering**
    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Timestamp {
            time: 1000,
            increment: 1,
        };
        let ts2 = Timestamp {
            time: 2000,
            increment: 1,
        };
        assert_ordering(&Bson::Timestamp(ts1), &Bson::Timestamp(ts2));
    }
}
