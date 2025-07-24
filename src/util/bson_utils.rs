use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use crate::io::ZeroCopy;
use bson::{to_vec, Bson};
use std::io::{Error, ErrorKind, Result};
use bson::spec::BinarySubtype;

pub fn prepend_field(doc: &mut Vec<u8>, key: &str, value: &Bson) -> Result<()> {
    let raw_field = make_raw_bson_element(key, value)?;
    // Prepend the field to the raw BSON data
    prepend_raw_bson_field(doc, &raw_field)?;
    Ok(())
}

fn invalid_data(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

/// Build the raw bytes that make up a **single BSON element**
/// (`<type-byte><cstring key><value bytes>`), given a key and a `Bson` value.
pub fn make_raw_bson_element(key: &str, value: &Bson) -> Result<Vec<u8>> {

    // Serialize a 1-field document and slice away the outer framing:
    // [i32 size][element …][0x00 terminator]
    let mut doc = bson::Document::new();
    doc.insert(key, value.clone());

    let mut buf = to_vec(&doc).map_err(|e| invalid_data(&e.to_string()))?; // – full document bytes
    buf.pop();                          // drop trailing 0
    Ok(buf.split_off(4))             // drop 4-byte size header → element bytes
}

/// Insert a raw BSON element as the very first field of a serialized BSON
/// document *without* deserializing either side.
///
/// * `doc`   – mutable buffer containing a BSON document
/// * `field` – a complete BSON element (`type-byte || cstring key || value`)
///
/// Returns `Ok(())` on success, or an error if the input is not valid.
pub fn prepend_raw_bson_field(doc: &mut Vec<u8>, field: &[u8]) -> Result<()> {

    if doc.len() < 5 || *doc.last().unwrap() != 0 {
        return Err(invalid_data("invalid BSON document"));
    }

    // Original size (first 4 bytes = little-endian i32)
    let orig_size = doc.read_i32_le(0) as usize;
    if orig_size != doc.len() {
        return Err(invalid_data("size header does not match document length"));
    }

    // Compute the new size and write it back
    let new_size = orig_size
        .checked_add(field.len())
        .ok_or(invalid_data("document would exceed 32-bit size limit"))?;

    doc[0..4].copy_from_slice(&(new_size as i32).to_le_bytes());

    // Splice the new field just after the size header (index 4)
    doc.splice(4..4, field.iter().cloned());

    Ok(())
}

/// Compare two [`Bson`] values using **MongoDB’s canonical sort order**.
///
/// MongoDB orders BSON values in two steps:
///
/// 1. **Type rank** – A fixed ranking by BSON type
///    `MinKey < Null < Numbers < String < Document < Array < Binary < ObjectId`
///    `< Boolean < DateTime < Timestamp < RegularExpression < MaxKey`.
///
/// 2. **Within-type comparison** – If the two values share the same type,
///    comparison falls back to a rule specific to that type:
///    * numbers by numeric value (handling cross-family comparisons)
///    * strings lexicographically (UTF-8)
///    * documents by key, then by value (prefix wins)
///    * arrays element-by-element (prefix wins)
///    * binary by subtype first, then bytes, etc.
///
/// Deprecated BSON variants (`Undefined`, `Symbol`, `DBPointer`, …) are not supported.
///
/// Returns [`Ordering::Less`], [`Ordering::Equal`], or [`Ordering::Greater`]
/// exactly as mandated by the MongoDB wire protocol.
pub fn cmp_bson(a: &Bson, b: &Bson) -> Ordering {
    use Bson::*;

    // 1. Type ranking (lower ⇒ smaller)
    fn rank(v: &Bson) -> u8 {
        match v {
            MinKey                                                   => 0,
            Null                                                     => 1,
            Double(_)|Int32(_)|Int64(_)|Decimal128(_)                => 2,
            String(_)                                                => 3,
            Document(_)                                              => 4,
            Array(_)                                                 => 5,
            Binary(_)                                                => 6,
            ObjectId(_)                                              => 7,
            Boolean(_)                                               => 8,
            DateTime(_)                                              => 9,
            Timestamp(_)                                             => 10,
            RegularExpression(_)                                     => 11,
            MaxKey                                                   => 12,
            _ /* legacy variants */                                  => panic!(
                "Unsupported BSON type for comparison: {:?}. Use only supported types.",
                v
            ),
        }
    }

    // Different BSON kinds → compare by rank.
    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }

    // Same kind → value-specific comparison.
    match (a, b) {
        // — numeric family —
        (Double(x), Double(y))           => x.partial_cmp(y).unwrap_or(Ordering::Greater),
        (Int32(x),  Int32(y))            => x.cmp(y),
        (Int64(x),  Int64(y))            => x.cmp(y),
        (Decimal128(x), Decimal128(y))   => x.to_string().cmp(&y.to_string()),

        // cross-numeric
        (Int32(x),  Double(y))           => (*x as f64).partial_cmp(y).unwrap(),
        (Int64(x),  Double(y))           => (*x as f64).partial_cmp(y).unwrap(),
        (Double(x), Int32(y))            => x.partial_cmp(&(*y as f64)).unwrap(),
        (Double(x), Int64(y))            => x.partial_cmp(&(*y as f64)).unwrap(),
        (Int32(x),  Int64(y))            => (*x as i64).cmp(y),
        (Int64(x),  Int32(y))            => x.cmp(&(*y as i64)),

        // — simple scalars —
        (String(x), String(y))           => x.cmp(y),
        (Boolean(x), Boolean(y))         => x.cmp(y),
        (DateTime(x), DateTime(y))       => x.cmp(y),
        (ObjectId(x), ObjectId(y))       => x.bytes().cmp(&y.bytes()),
        (Timestamp(x), Timestamp(y))     => (x.time, x.increment).cmp(&(y.time, y.increment)),

        // — binary —
        (Binary(x), Binary(y)) => match subtype_code(x.subtype).cmp(&subtype_code(y.subtype)) {
            Ordering::Equal => x.bytes.cmp(&y.bytes),
            other           => other,
        },

        // — regex —
        (RegularExpression(x), RegularExpression(y)) => match x.pattern.cmp(&y.pattern) {
            Ordering::Equal => x.options.cmp(&y.options),
            other           => other,
        },

        // — compound types —
        (Array(av), Array(bv)) => {
            for (ai, bi) in av.iter().zip(bv.iter()) {
                let ord = cmp_bson(ai, bi);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            av.len().cmp(&bv.len())
        }
        (Document(ad), Document(bd)) => {
            for ((ak, av), (bk, bv)) in ad.iter().zip(bd.iter()) {
                match ak.cmp(bk) {
                    Ordering::Equal => {
                        let ord = cmp_bson(av, bv);
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    other => return other,
                }
            }
            ad.len().cmp(&bd.len())
        }

        // identical MinKey / MaxKey / Null, etc.
        _ => Ordering::Equal,
    }
}

pub fn bson_eq(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        // Handle NaN correctly (MongoDB: NaN == NaN)
        (Bson::Double(x), Bson::Double(y)) if x.is_nan() && y.is_nan() => true,
        // Exact match for same BSON type
        (Bson::Int32(x), Bson::Int32(y)) => x == y,
        (Bson::Int64(x), Bson::Int64(y)) => x == y,
        (Bson::Double(x), Bson::Double(y)) => (x - y).abs() < f64::EPSILON,

        // Normalize and compare mixed numeric types
        (Bson::Int32(x), Bson::Int64(y)) => *x as i64 == *y,
        (Bson::Int32(x), Bson::Double(y)) => (*x as f64 - *y).abs() < f64::EPSILON,
        (Bson::Int64(x), Bson::Double(y)) => (*x as f64 - *y).abs() < f64::EPSILON,
        (Bson::Int64(x), Bson::Int32(y)) => *x == *y as i64,
        (Bson::Double(x), Bson::Int32(y)) => (*x - *y as f64).abs() < f64::EPSILON,
        (Bson::Double(x), Bson::Int64(y)) => (*x - *y as f64).abs() < f64::EPSILON,

        // Compare objects/maps in a canonical order
        (Bson::Document(a), Bson::Document(b)) => {
            let a_sorted: BTreeMap<_, _> = a.iter().collect();
            let b_sorted: BTreeMap<_, _> = b.iter().collect();
            a_sorted == b_sorted
        }

        // Compare arrays element-wise
        (Bson::Array(a), Bson::Array(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(x, y)| bson_eq(x, y))
        }

        // Compare regex patterns and options
        (Bson::RegularExpression(a), Bson::RegularExpression(b)) => {
            a.pattern == b.pattern && a.options == b.options
        }

        // Default strict equality for other types
        _ => a == b,
    }
}
pub fn bson_hash<H: Hasher>(bson: &Bson, state: &mut H) {
    match bson {
        // Normalize NaN to a fixed value
        Bson::Int32(x) => x.hash(state),
        Bson::Int64(x) => x.hash(state),
        Bson::Double(x) => {
            if x.is_nan() {
                0x7FF8_0000_0000_0000u64.hash(state)
            } else {
                x.to_bits().hash(state)
            }
        } // Normalize floating point hashing
        Bson::String(s) => s.hash(state),
        Bson::Boolean(b) => b.hash(state),

        // Hash BSON arrays
        Bson::Array(arr) => {
            for elem in arr {
                bson_hash(elem, state);
            }
        }

        // Hash BSON documents (sorted order)
        Bson::Document(doc) => {
            let sorted: BTreeMap<_, _> = doc.iter().collect();
            for (key, value) in sorted {
                key.hash(state);
                bson_hash(value, state);
            }
        }

        Bson::RegularExpression(regex) => {
            regex.pattern.hash(state);
            regex.options.hash(state);
        }

        _ => (),
    }
}
fn subtype_code(s: BinarySubtype) -> u8 { s.into() } // From<BinarySubtype> for u8 exists

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

    mod prepend_raw_bson_field {
        use bson::{doc, to_vec, Bson};
        use crate::util::bson_utils::{make_raw_bson_element, prepend_raw_bson_field};

        #[test]
        fn inserts_string_field_as_first() {
            // original document { a: 1, b: true }
            let mut doc_buf = to_vec(&doc! { "a": 1_i32, "b": true }).unwrap();

            // raw element for { x: "bar" } built with the helper
            let field = make_raw_bson_element("x", &Bson::String("bar".into())).unwrap();

            prepend_raw_bson_field(&mut doc_buf, &field).unwrap();

            // expect { x: "bar", a: 1, b: true }
            assert_eq!(
                doc_buf,
                to_vec(&doc! { "x": "bar", "a": 1_i32, "b": true }).unwrap()
            );
        }

        #[test]
        fn rejects_length_mismatch() {
            // corrupt size header
            let mut bad = to_vec(&doc! { "a": 1_i32 }).unwrap();
            bad[0..4].copy_from_slice(&0_i32.to_le_bytes());

            let field = make_raw_bson_element("x", &Bson::Int32(0)).unwrap();
            assert!(prepend_raw_bson_field(&mut bad, &field).is_err());
        }

        #[test]
        fn rejects_missing_terminator() {
            let mut bad = to_vec(&doc! { "a": 1_i32 }).unwrap();
            bad.pop(); // drop trailing 0x00

            let field = make_raw_bson_element("x", &Bson::String("y".into())).unwrap();
            assert!(prepend_raw_bson_field(&mut bad, &field).is_err());
        }
    }

    mod bson_key {
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
        fn minkey_maxkey() {
            assert_ordering(&Bson::MinKey, &Bson::Null);
            assert_ordering(&Bson::Int32(0), &Bson::MaxKey);
        }

        /// **Test Integer Sorting**
        #[test]
        fn int32_ordering() {
            assert_ordering(&Bson::Int32(-1000), &Bson::Int32(-1));
            assert_ordering(&Bson::Int32(-1), &Bson::Int32(0));
            assert_ordering(&Bson::Int32(0), &Bson::Int32(1));
            assert_ordering(&Bson::Int32(1), &Bson::Int32(1000));
        }

        #[test]
        fn int64_ordering() {
            assert_ordering(&Bson::Int64(-1_000_000_000), &Bson::Int64(-1));
            assert_ordering(&Bson::Int64(-1), &Bson::Int64(0));
            assert_ordering(&Bson::Int64(0), &Bson::Int64(1));
            assert_ordering(&Bson::Int64(1), &Bson::Int64(1_000_000_000));
        }

        /// **Test Double Precision Floating-Point Sorting**
        #[test]
        fn double_ordering() {
            assert_ordering(&Bson::Double(-1000.0), &Bson::Double(-1.0));
            assert_ordering(&Bson::Double(-1.0), &Bson::Double(-0.1));
            assert_ordering(&Bson::Double(-0.1), &Bson::Double(0.0));
            assert_ordering(&Bson::Double(0.0), &Bson::Double(0.1));
            assert_ordering(&Bson::Double(0.1), &Bson::Double(1.0));
            assert_ordering(&Bson::Double(1.0), &Bson::Double(1000.0));
        }

        /// **Test Special Double Cases**
        #[test]
        fn double_edge_cases() {
            assert_ordering(&Bson::Double(-1.0), &Bson::Double(1.0));
            assert_ordering(&Bson::Double(-0.0), &Bson::Double(0.0));
            assert_ordering(&Bson::Double(1.0e-10), &Bson::Double(1.0));
            assert_ordering(&Bson::Double(1.0), &Bson::Double(1.0e10));
        }

        /// **Test Decimal128 Sorting**
        #[test]
        fn decimal128_ordering() {
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
        fn decimal128_leading_zero_normalization() {
            let dec1 = Decimal128::from_str("1.000000000000000000000000000000000").unwrap();
            let dec2 = Decimal128::from_str("1.000000000000000000000000000000001").unwrap();

            // `dec1` and `dec2` should be sorted correctly, even if `dec1` has trailing zeroes.
            assert_decimal_ordering(&dec1, &dec2);
        }

        /// Tests sorting when numbers have different exponent values.
        #[test]
        fn decimal128_exponent_adjustment() {
            let dec1 = Decimal128::from_str("1.0").unwrap();
            let dec2 = Decimal128::from_str("10.0").unwrap();
            let dec3 = Decimal128::from_str("100.0").unwrap();

            // Ensure proper ordering even when the exponent is different.
            assert_decimal_ordering(&dec1, &dec2);
            assert_decimal_ordering(&dec2, &dec3);
        }

        /// Tests very large and very small values to check that sorting handles extreme exponent shifts correctly.
        #[test]
        fn decimal128_extreme_values() {
            let min_value = Decimal128::from_str("1E-6176").unwrap();
            let max_value = Decimal128::from_str("9.999999999999999999999999999999999E6111").unwrap();

            // Ensure smallest value sorts before largest value.
            assert_decimal_ordering(&min_value, &max_value);
        }

        /// Tests sorting of numbers with small and large coefficients but similar exponents.
        #[test]
        fn decimal128_large_vs_small_coefficients() {
            let small_coeff = Decimal128::from_str("0.00000000000000000001").unwrap();
            let large_coeff = Decimal128::from_str("10000000000000000000.0").unwrap();

            assert_decimal_ordering(&small_coeff, &large_coeff);
        }

        /// Tests correct sorting of zero (`0.0`) and negative zero (`-0.0`).
        #[test]
        fn decimal128_zero_ordering() {
            let dec_zero = Decimal128::from_str("0.0").unwrap();
            let dec_one = Decimal128::from_str("1.0").unwrap();
            let dec_neg_zero = Decimal128::from_str("-0.0").unwrap();

            assert_decimal_ordering(&dec_neg_zero, &dec_zero);
            assert_decimal_ordering(&dec_zero, &dec_one);
        }

        /// Tests sorting of negative and positive numbers across exponent ranges.
        #[test]
        fn decimal128_negative_vs_positive() {
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
        fn string_ordering() {
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
        fn boolean_ordering() {
            assert_ordering(&Bson::Boolean(false), &Bson::Boolean(true));
        }

        /// **Test ObjectId Ordering**
        #[test]
        fn objectid_ordering() {
            let oid1 = ObjectId::parse_str("000000000000000000000000").unwrap();
            let oid2 = ObjectId::parse_str("ffffffffffffffffffffffff").unwrap();
            assert_ordering(&Bson::ObjectId(oid1), &Bson::ObjectId(oid2));
        }

        /// **Test Binary Sorting**
        #[test]
        fn binary_ordering() {
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
        fn datetime_ordering() {
            let dt1 = DateTime::from_millis(1609459200000); // 2021-01-01 00:00:00 UTC
            let dt2 = DateTime::from_millis(1640995200000); // 2022-01-01 00:00:00 UTC
            assert_ordering(&Bson::DateTime(dt1), &Bson::DateTime(dt2));
        }

        /// **Test Timestamp Ordering**
        #[test]
        fn timestamp_ordering() {
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

    mod bson_cmp {
        use bson::{doc, Binary, Bson, DateTime, oid::ObjectId, spec::BinarySubtype};
        use std::cmp::Ordering::*;
        use crate::util::bson_utils;

        #[test]
        fn test_type_rank() {
            assert_eq!(bson_utils::cmp_bson(&Bson::MinKey, &Bson::Null), Less);
            assert_eq!(bson_utils::cmp_bson(&Bson::Null, &Bson::Int32(0)), Less);
            assert_eq!(bson_utils::cmp_bson(&Bson::Int32(0), &Bson::String("x".into())), Less);
        }

        #[test]
        fn test_numeric_family() {
            assert_eq!(bson_utils::cmp_bson(&Bson::Int32(5), &Bson::Int64(5)), Equal);
            assert_eq!(bson_utils::cmp_bson(&Bson::Double(3.1), &Bson::Int32(4)), Less);
        }

        #[test]
        fn test_string_vs_string() {
            assert_eq!(bson_utils::cmp_bson(&Bson::String("apple".into()), &Bson::String("banana".into())), Less);
        }

        #[test]
        fn test_object_id_order() {
            let a = ObjectId::parse_str("000000000000000000000000").unwrap();
            let b = ObjectId::parse_str("ffffffffffffffffffffffff").unwrap();
            assert_eq!(bson_utils::cmp_bson(&Bson::ObjectId(a), &Bson::ObjectId(b)), Less);
        }

        #[test]
        fn test_binary_subtype_then_bytes() {
            let x = Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] };
            let y = Binary { subtype: BinarySubtype::Uuid,    bytes: vec![0] };
            assert_eq!(bson_utils::cmp_bson(&Bson::Binary(x.clone()), &Bson::Binary(y.clone())), Less);
            // same subtype → fall back to bytes
            let z = Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 3] };
            assert_eq!(bson_utils::cmp_bson(&Bson::Binary(x), &Bson::Binary(z)), Less);
        }

        #[test]
        fn test_array_prefix_rule() {
            let a = Bson::Array(vec![Bson::Int32(1)]);
            let b = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
            assert_eq!(bson_utils::cmp_bson(&a, &b), Less);
        }

        #[test]
        fn test_document_key_and_value() {
            let a = Bson::Document(doc! { "a": 1 });
            let b = Bson::Document(doc! { "b": 1 });
            let c = Bson::Document(doc! { "a": 2 });

            assert_eq!(bson_utils::cmp_bson(&a, &b), Less);   // key comparison
            assert_eq!(bson_utils::cmp_bson(&a, &c), Less);   // same key, value comparison
        }

        #[test]
        fn test_datetime() {
            let t1 = DateTime::from_millis(1_000);
            let t2 = DateTime::from_millis(2_000);
            assert_eq!(bson_utils::cmp_bson(&Bson::DateTime(t1), &Bson::DateTime(t2)), Less);
        }
    }
}
