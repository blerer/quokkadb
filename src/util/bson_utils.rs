use crate::io::byte_reader::ByteReader;
use crate::io::{invalid_data, unexpected_eof, varint, ZeroCopy};
use bson::spec::BinarySubtype;
use bson::{serialize_to_vec, Bson};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::string::String;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BsonArithmeticError {
    LhsNotNumeric,
    RhsNotNumeric,
    LhsNotInteger,
    Overflow,
}

pub fn prepend_field(doc: &mut Vec<u8>, key: &str, value: &Bson) -> std::io::Result<()> {
    let raw_field = make_raw_bson_element(key, value)?;
    prepend_raw_bson_field(doc, &raw_field)?;
    Ok(())
}

/// Build the raw bytes that make up a **single BSON element**
/// (`<type-byte><cstring key><value bytes>`), given a key and a `Bson` value.
pub fn make_raw_bson_element(key: &str, value: &Bson) -> std::io::Result<Vec<u8>> {
    let mut doc = bson::Document::new();
    doc.insert(key, value.clone());

    let mut buf = serialize_to_vec(&doc).map_err(|e| invalid_data(&e.to_string()))?;
    buf.pop();
    Ok(buf.split_off(4))
}

/// Insert a raw BSON element as the very first field of a serialized BSON
/// document *without* deserializing either side.
pub fn prepend_raw_bson_field(doc: &mut Vec<u8>, field: &[u8]) -> std::io::Result<()> {
    if doc.len() < 5 || *doc.last().unwrap() != 0 {
        return Err(invalid_data("invalid BSON document"));
    }

    let orig_size = doc.read_i32_le(0) as usize;
    if orig_size != doc.len() {
        return Err(invalid_data("size header does not match document length"));
    }

    let new_size = orig_size
        .checked_add(field.len())
        .ok_or(invalid_data("document would exceed 32-bit size limit"))?;

    doc[0..4].copy_from_slice(&(new_size as i32).to_le_bytes());
    doc.splice(4..4, field.iter().cloned());

    Ok(())
}

/// Compare two [`Bson`] values using **MongoDB's canonical sort order**.
pub fn cmp_bson(a: &Bson, b: &Bson) -> Ordering {
    use Bson::*;

    fn rank(v: &Bson) -> u8 {
        match v {
            MinKey => 0,
            Null => 1,
            Double(_) | Int32(_) | Int64(_) | Decimal128(_) => 2,
            String(_) => 3,
            Document(_) => 4,
            Array(_) => 5,
            Binary(_) => 6,
            ObjectId(_) => 7,
            Boolean(_) => 8,
            DateTime(_) => 9,
            Timestamp(_) => 10,
            RegularExpression(_) => 11,
            MaxKey => 12,
            _ => panic!(
                "Unsupported BSON type for comparison: {:?}. Use only supported types.",
                v
            ),
        }
    }

    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }

    if ra == 2 {
        if let Some(ord) = numeric_cmp(a, b) {
            return ord;
        }
    }

    match (a, b) {
        (String(x), String(y)) => x.cmp(y),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        (DateTime(x), DateTime(y)) => x.cmp(y),
        (ObjectId(x), ObjectId(y)) => x.bytes().cmp(&y.bytes()),
        (Timestamp(x), Timestamp(y)) => (x.time, x.increment).cmp(&(y.time, y.increment)),
        (Binary(x), Binary(y)) => match subtype_code(x.subtype).cmp(&subtype_code(y.subtype)) {
            Ordering::Equal => x.bytes.cmp(&y.bytes),
            other => other,
        },
        (RegularExpression(x), RegularExpression(y)) => match x.pattern.cmp(&y.pattern) {
            Ordering::Equal => x.options.cmp(&y.options),
            other => other,
        },
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
        _ => Ordering::Equal,
    }
}

pub fn bson_eq(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) if x.is_nan() && y.is_nan() => true,
        (Bson::Int32(x), Bson::Int32(y)) => x == y,
        (Bson::Int64(x), Bson::Int64(y)) => x == y,
        (Bson::Double(x), Bson::Double(y)) => (x - y).abs() < f64::EPSILON,

        (Bson::Int32(x), Bson::Int64(y)) => *x as i64 == *y,
        (Bson::Int32(x), Bson::Double(y)) => (*x as f64 - *y).abs() < f64::EPSILON,
        (Bson::Int64(x), Bson::Double(y)) => (*x as f64 - *y).abs() < f64::EPSILON,
        (Bson::Int64(x), Bson::Int32(y)) => *x == *y as i64,
        (Bson::Double(x), Bson::Int32(y)) => (*x - *y as f64).abs() < f64::EPSILON,
        (Bson::Double(x), Bson::Int64(y)) => (*x - *y as f64).abs() < f64::EPSILON,

        (Bson::Document(a), Bson::Document(b)) => {
            let a_sorted: BTreeMap<_, _> = a.iter().collect();
            let b_sorted: BTreeMap<_, _> = b.iter().collect();
            a_sorted == b_sorted
        }

        (Bson::Array(a), Bson::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| bson_eq(x, y))
        }

        (Bson::RegularExpression(a), Bson::RegularExpression(b)) => {
            a.pattern == b.pattern && a.options == b.options
        }

        _ => a == b,
    }
}

pub fn bson_hash<H: Hasher>(bson: &Bson, state: &mut H) {
    match bson {
        Bson::Int32(x) => (*x as i64).hash(state),
        Bson::Int64(x) => x.hash(state),
        Bson::Double(x) => {
            if x.is_nan() {
                // All NaN forms collapse to one canonical hash.
                0x7FF8_0000_0000_0000u64.hash(state)
            } else if *x == 0.0 {
                // +0.0 and -0.0 are equal to integer 0; hash as i64.
                0i64.hash(state)
            } else {
                let trunc = *x as i64;
                if trunc as f64 == *x {
                    // Whole-number double: hash as i64 so it agrees with Int32/Int64.
                    trunc.hash(state)
                } else {
                    // Fractional value: no integer can equal this, so raw bits are fine.
                    // No need to handle -0.0 here; already handled above.
                    x.to_bits().hash(state)
                }
            }
        }
        Bson::String(s) => s.hash(state),
        Bson::Boolean(b) => b.hash(state),

        Bson::Array(arr) => {
            for elem in arr {
                bson_hash(elem, state);
            }
        }

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

pub fn add_numeric(existing: Option<&Bson>, amount: &Bson) -> Result<Bson, BsonArithmeticError> {
    use bson::Bson::*;
    use BsonArithmeticError::*;

    enum Num {
        I64(i64),
        F64(f64),
    }

    let amt = match amount {
        Int32(n) => Num::I64(*n as i64),
        Int64(n) => Num::I64(*n),
        Double(f) => Num::F64(*f),
        _ => return Err(RhsNotNumeric),
    };

    match existing {
        None => Ok(amount.clone()),
        Some(Int32(a)) => match amt {
            Num::I64(b) => {
                let sum = (*a as i64).checked_add(b).ok_or(Overflow)?;
                if sum >= i32::MIN as i64 && sum <= i32::MAX as i64 {
                    Ok(Int32(sum as i32))
                } else {
                    Ok(Int64(sum))
                }
            }
            Num::F64(b) => Ok(Double((*a as f64) + b)),
        },
        Some(Int64(a)) => match amt {
            Num::I64(b) => {
                let sum = (*a).checked_add(b).ok_or(Overflow)?;
                Ok(Int64(sum))
            }
            Num::F64(b) => Ok(Double((*a as f64) + b)),
        },
        Some(Double(a)) => match amt {
            Num::I64(b) => Ok(Double(*a + (b as f64))),
            Num::F64(b) => Ok(Double(*a + b)),
        },
        Some(_) => Err(LhsNotNumeric),
    }
}

pub fn multiply_numeric(
    existing: Option<&Bson>,
    factor: &Bson,
) -> Result<Bson, BsonArithmeticError> {
    use bson::Bson::*;
    use BsonArithmeticError::*;

    enum Num {
        I64(i64),
        F64(f64),
    }

    let fact = match factor {
        Int32(n) => Num::I64(*n as i64),
        Int64(n) => Num::I64(*n),
        Double(f) => Num::F64(*f),
        _ => return Err(RhsNotNumeric),
    };

    match existing {
        None => Ok(match factor {
            Int32(_) => Int32(0),
            Int64(_) => Int64(0),
            Double(_) => Double(0.0),
            _ => unreachable!(),
        }),
        Some(Int32(a)) => match fact {
            Num::I64(b) => {
                let prod = (*a as i64).checked_mul(b).ok_or(Overflow)?;
                if prod >= i32::MIN as i64 && prod <= i32::MAX as i64 {
                    Ok(Int32(prod as i32))
                } else {
                    Ok(Int64(prod))
                }
            }
            Num::F64(b) => Ok(Double((*a as f64) * b)),
        },
        Some(Int64(a)) => match fact {
            Num::I64(b) => {
                let prod = (*a).checked_mul(b).ok_or(Overflow)?;
                Ok(Int64(prod))
            }
            Num::F64(b) => Ok(Double((*a as f64) * b)),
        },
        Some(Double(a)) => match fact {
            Num::I64(b) => Ok(Double(*a * (b as f64))),
            Num::F64(b) => Ok(Double(*a * b)),
        },
        Some(_) => Err(LhsNotNumeric),
    }
}

pub fn perform_bitwise_op(
    existing: Option<&Bson>,
    and: Option<i64>,
    or: Option<i64>,
    xor: Option<i64>,
) -> Result<Bson, BsonArithmeticError> {
    use BsonArithmeticError::*;
    if and.is_none() && or.is_none() && xor.is_none() {
        return Ok(existing.cloned().unwrap_or(Bson::Int64(0)));
    }

    let mut num = match existing {
        None => 0i64,
        Some(Bson::Int32(i)) => *i as i64,
        Some(Bson::Int64(i)) => *i,
        Some(_) => return Err(LhsNotInteger),
    };

    if let Some(val) = and {
        num &= val;
    }
    if let Some(val) = or {
        num |= val;
    }
    if let Some(val) = xor {
        num ^= val;
    }

    if matches!(existing, Some(Bson::Int64(_))) {
        return Ok(Bson::Int64(num));
    }

    if num >= i32::MIN as i64 && num <= i32::MAX as i64 {
        Ok(Bson::Int32(num as i32))
    } else {
        Ok(Bson::Int64(num))
    }
}

fn subtype_code(s: BinarySubtype) -> u8 {
    s.into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecimalFiniteMetadata {
    original_negative: bool,
    original_exp10: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NumericClass {
    NaN,
    NegInf,
    Finite {
        negative: bool,
        digits: Vec<u8>,
        exp10: i32,
        decimal_metadata: Option<DecimalFiniteMetadata>,
    },
    PosInf,
}

fn is_zero_digits(digits: &[u8]) -> bool {
    digits.len() == 1 && digits[0] == b'0'
}

fn normalize_digits_and_exp(mut digits: Vec<u8>, mut exp10: i32) -> (Vec<u8>, i32) {
    debug_assert!(digits.iter().all(|d| d.is_ascii_digit()));

    if digits.is_empty() {
        return (vec![b'0'], 0);
    }

    let first_non_zero = digits.iter().position(|&d| d != b'0');
    match first_non_zero {
        None => return (vec![b'0'], 0),
        Some(0) => {}
        Some(idx) => {
            digits.drain(0..idx);
        }
    }

    while digits.len() > 1 && digits.last() == Some(&b'0') {
        digits.pop();
        exp10 += 1;
    }

    debug_assert!(!digits.is_empty());
    debug_assert!(digits.iter().all(|d| d.is_ascii_digit()));
    (digits, exp10)
}

fn make_finite_numeric_class(negative: bool, digits: Vec<u8>, exp10: i32) -> NumericClass {
    let (digits, exp10) = normalize_digits_and_exp(digits, exp10);
    let negative = negative && !is_zero_digits(&digits);
    NumericClass::Finite {
        negative,
        digits,
        exp10,
        decimal_metadata: None,
    }
}

fn make_decimal_finite_numeric_class(negative: bool, digits: Vec<u8>, exp10: i32) -> NumericClass {
    let (normalized_digits, normalized_exp10) = normalize_digits_and_exp(digits, exp10);
    let normalized_negative = negative && !is_zero_digits(&normalized_digits);
    NumericClass::Finite {
        negative: normalized_negative,
        digits: normalized_digits,
        exp10: normalized_exp10,
        decimal_metadata: Some(DecimalFiniteMetadata {
            original_negative: negative,
            original_exp10: exp10,
        }),
    }
}

fn parse_scientific_to_finite_raw(s: &str) -> Option<(bool, Vec<u8>, i32)> {
    let mut bytes = s.trim().as_bytes();
    let mut negative = false;

    if let Some(&b'-') = bytes.first() {
        negative = true;
        bytes = &bytes[1..];
    } else if let Some(&b'+') = bytes.first() {
        bytes = &bytes[1..];
    }

    let mut e_idx = None;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'e' || b == b'E' {
            e_idx = Some(i);
            break;
        }
    }

    let (mantissa, exp_part) = if let Some(idx) = e_idx {
        (&bytes[..idx], Some(&bytes[idx + 1..]))
    } else {
        (bytes, None)
    };

    let mut exp10 = if let Some(exp) = exp_part {
        if exp.is_empty() {
            return None;
        }
        let mut i = 0usize;
        let mut exp_negative = false;
        if exp[0] == b'+' {
            i = 1;
        } else if exp[0] == b'-' {
            exp_negative = true;
            i = 1;
        }
        if i >= exp.len() {
            return None;
        }
        let mut value: i32 = 0;
        while i < exp.len() {
            let b = exp[i];
            if !b.is_ascii_digit() {
                return None;
            }
            let digit = (b - b'0') as i32;
            value = value.checked_mul(10)?.checked_add(digit)?;
            i += 1;
        }
        if exp_negative {
            -value
        } else {
            value
        }
    } else {
        0
    };

    let mut digits = Vec::with_capacity(mantissa.len());
    let mut dot_idx = None;
    for (i, &b) in mantissa.iter().enumerate() {
        if b == b'.' {
            dot_idx = Some(i);
            break;
        }
    }

    if let Some(dot) = dot_idx {
        let int_part = &mantissa[..dot];
        let frac_part = &mantissa[dot + 1..];

        for &b in int_part {
            if b.is_ascii_digit() {
                digits.push(b);
            } else {
                return None;
            }
        }
        for &b in frac_part {
            if b.is_ascii_digit() {
                digits.push(b);
            } else {
                return None;
            }
        }

        exp10 = exp10.checked_sub(frac_part.len() as i32)?;
    } else {
        for &b in mantissa {
            if b.is_ascii_digit() {
                digits.push(b);
            } else {
                return None;
            }
        }
    }

    Some((negative, digits, exp10))
}

fn canonical_numeric(v: &Bson) -> Option<NumericClass> {
    match v {
        Bson::Int32(n) => {
            if *n == 0 {
                Some(make_finite_numeric_class(false, vec![b'0'], 0))
            } else {
                let negative = *n < 0;
                let mut mag = if negative { -(*n as i64) } else { *n as i64 };
                let mut rev = Vec::new();
                while mag > 0 {
                    rev.push((mag % 10) as u8 + b'0');
                    mag /= 10;
                }
                rev.reverse();
                Some(make_finite_numeric_class(negative, rev, 0))
            }
        }
        Bson::Int64(n) => {
            if *n == 0 {
                Some(make_finite_numeric_class(false, vec![b'0'], 0))
            } else {
                let negative = *n < 0;
                let mut mag: u64 = if negative {
                    if *n == i64::MIN {
                        (i64::MAX as u64) + 1
                    } else {
                        (-*n) as u64
                    }
                } else {
                    *n as u64
                };

                let mut rev = Vec::new();
                while mag > 0 {
                    rev.push((mag % 10) as u8 + b'0');
                    mag /= 10;
                }
                rev.reverse();

                Some(make_finite_numeric_class(negative, rev, 0))
            }
        }
        Bson::Double(f) => {
            if f.is_nan() {
                Some(NumericClass::NaN)
            } else if *f == f64::NEG_INFINITY {
                Some(NumericClass::NegInf)
            } else if *f == f64::INFINITY {
                Some(NumericClass::PosInf)
            } else if *f == 0.0 {
                Some(make_finite_numeric_class(false, vec![b'0'], 0))
            } else {
                let s = format!("{:.17e}", *f);
                let (negative, digits, exp10) = parse_scientific_to_finite_raw(&s)?;
                Some(make_finite_numeric_class(negative, digits, exp10))
            }
        }
        Bson::Decimal128(d) => {
            let s = d.to_string();
            let lower = s.to_ascii_lowercase();
            if lower == "nan" || lower == "-nan" || lower == "+nan" {
                return Some(NumericClass::NaN);
            }
            if lower == "infinity" || lower == "+infinity" || lower == "inf" || lower == "+inf" {
                return Some(NumericClass::PosInf);
            }
            if lower == "-infinity" || lower == "-inf" {
                return Some(NumericClass::NegInf);
            }

            let (negative, digits, exp10) = parse_scientific_to_finite_raw(&s)?;
            Some(make_decimal_finite_numeric_class(negative, digits, exp10))
        }
        _ => None,
    }
}

fn cmp_finite_mag(a_digits: &[u8], a_exp10: i32, b_digits: &[u8], b_exp10: i32) -> Ordering {
    let a_mag = a_digits.len() as i64 + a_exp10 as i64;
    let b_mag = b_digits.len() as i64 + b_exp10 as i64;
    if a_mag != b_mag {
        return a_mag.cmp(&b_mag);
    }

    let mut ai = 0usize;
    let mut bi = 0usize;

    while ai < a_digits.len() || bi < b_digits.len() {
        let ad = if ai < a_digits.len() {
            a_digits[ai]
        } else {
            b'0'
        };
        let bd = if bi < b_digits.len() {
            b_digits[bi]
        } else {
            b'0'
        };

        if ad != bd {
            return ad.cmp(&bd);
        }
        ai += 1;
        bi += 1;
    }

    Ordering::Equal
}

fn cmp_numeric_class(a: &NumericClass, b: &NumericClass) -> Ordering {
    use NumericClass::*;

    let rank = |n: &NumericClass| match n {
        NaN => 0u8,
        NegInf => 1u8,
        Finite { negative: true, .. } => 2u8,
        Finite {
            negative: false,
            digits,
            ..
        } if is_zero_digits(digits) => 3u8,
        Finite {
            negative: false, ..
        } => 4u8,
        PosInf => 5u8,
    };

    let ra = rank(a);
    let rb = rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }

    match (a, b) {
        (
            Finite {
                negative: an,
                digits: ad,
                exp10: ae,
                ..
            },
            Finite {
                negative: _,
                digits: bd,
                exp10: be,
                ..
            },
        ) => {
            if is_zero_digits(ad) && is_zero_digits(bd) {
                Ordering::Equal
            } else if *an {
                cmp_finite_mag(ad, *ae, bd, *be).reverse()
            } else {
                cmp_finite_mag(ad, *ae, bd, *be)
            }
        }
        _ => Ordering::Equal,
    }
}

fn numeric_cmp(a: &Bson, b: &Bson) -> Option<Ordering> {
    use Bson::*;

    // Helper: compare an i64 to a f64 without loss of precision.
    //
    // Strategy:
    //  1. If the f64 is NaN, -Inf, or +Inf handle immediately.
    //  2. If the f64 is outside the representable i64 range the ordering is
    //     determined by the sign of the f64 alone.
    //  3. Otherwise, truncate the f64 to i64 and compare; if equal, the
    //     fractional part of the f64 (if any) breaks the tie: a positive
    //     fraction means f64 > i64, a negative fraction means f64 < i64.
    fn cmp_i64_f64(i: i64, f: f64) -> Ordering {
        if f.is_nan() {
            // NaN sorts below everything (MongoDB convention).
            return Ordering::Greater;
        }
        if f == f64::NEG_INFINITY {
            return Ordering::Greater;
        }
        if f == f64::INFINITY {
            return Ordering::Less;
        }
        // i64::MIN and i64::MAX rounded to f64.
        const I64_MIN_F: f64 = i64::MIN as f64; // exactly representable
        const I64_MAX_F: f64 = i64::MAX as f64; // rounds up to 2^63
        if f < I64_MIN_F {
            return Ordering::Greater;
        }
        if f >= I64_MAX_F {
            // f64 value >= 2^63 is larger than any i64.
            return Ordering::Less;
        }
        let f_trunc = f as i64;
        match i.cmp(&f_trunc) {
            Ordering::Equal => {
                // The integer parts are equal; the fractional part decides.
                // f - f_trunc is in (-1, 1); its sign tells us whether f is
                // above or below the integer.
                let frac = f - f_trunc as f64;
                if frac > 0.0 {
                    Ordering::Less // i < f
                } else if frac < 0.0 {
                    Ordering::Greater // i > f  (f is between f_trunc-1 and f_trunc)
                } else {
                    Ordering::Equal
                }
            }
            other => other,
        }
    }

    // Helper: MongoDB NaN/Inf total order for f64 pairs.
    fn cmp_f64_total(a: f64, b: f64) -> Ordering {
        // NaN < -Inf < finite < +Inf  (MongoDB order)
        match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        }
    }

    match (a, b) {
        // ---- Int32 vs Int32 ----
        (Int32(x), Int32(y)) => Some(x.cmp(y)),

        // ---- Int64 vs Int64 ----
        (Int64(x), Int64(y)) => Some(x.cmp(y)),

        // ---- Int32 <-> Int64 ----
        (Int32(x), Int64(y)) => Some((*x as i64).cmp(y)),
        (Int64(x), Int32(y)) => Some(x.cmp(&(*y as i64))),

        // ---- Double vs Double ----
        (Double(x), Double(y)) => Some(cmp_f64_total(*x, *y)),

        // ---- Int32 <-> Double ----
        (Int32(x), Double(y)) => Some(cmp_i64_f64(*x as i64, *y)),
        (Double(x), Int32(y)) => Some(cmp_i64_f64(*y as i64, *x).reverse()),

        // ---- Int64 <-> Double ----
        (Int64(x), Double(y)) => Some(cmp_i64_f64(*x, *y)),
        (Double(x), Int64(y)) => Some(cmp_i64_f64(*y, *x).reverse()),

        // ---- Any Decimal128 operand: fall back to canonical parsing ----
        _ => {
            let ca = canonical_numeric(a)?;
            let cb = canonical_numeric(b)?;
            Some(cmp_numeric_class(&ca, &cb))
        }
    }
}

mod key_code {
    pub const TYPE_MIN_KEY: u8 = 0x00;
    pub const TYPE_NULL: u8 = 0x10;
    pub const TYPE_NUMBER: u8 = 0x20;
    pub const TYPE_STRING: u8 = 0x30;
    pub const TYPE_DOCUMENT: u8 = 0x34;
    pub const TYPE_ARRAY: u8 = 0x38;
    pub const TYPE_BINARY: u8 = 0x40;
    pub const TYPE_OBJECT_ID: u8 = 0x50;
    pub const TYPE_BOOL_FALSE: u8 = 0x60;
    pub const TYPE_BOOL_TRUE: u8 = 0x61;
    pub const TYPE_DATETIME: u8 = 0x70;
    pub const TYPE_TIMESTAMP: u8 = 0x80;
    pub const TYPE_MAX_KEY: u8 = 0xFF;
}

/// Subtype tags written immediately after the `TYPE_NUMBER` (0x20) byte.
///
/// The tags are ordered so that their numeric values sort correctly as raw bytes:
///
/// ```text
/// 0x00  NaN        — sorts before everything else (MongoDB convention)
/// 0x10  -Infinity
/// 0x20  negative finite  (followed by biased exponent + inverted digits + 0x00)
/// 0x30  zero       (all zero forms collapse here)
/// 0x40  positive finite  (followed by biased exponent + digits + 0x00)
/// 0x50  +Infinity
/// ```
mod numeric_code {
    pub const NUM_NAN: u8 = 0x00;
    pub const NUM_NEG_INF: u8 = 0x10;
    pub const NUM_NEG_FINITE: u8 = 0x20;
    pub const NUM_ZERO: u8 = 0x30;
    pub const NUM_POS_FINITE: u8 = 0x40;
    pub const NUM_POS_INF: u8 = 0x50;
}

/// Write a `u32` in big-endian order.  Big-endian byte order ensures that the
/// encoded exponent compares correctly as a raw byte slice.
fn encode_exp_u32(v: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Write the ASCII digit bytes of `digits` with each digit *inverted*
/// (`'9' - digit`).
///
/// Inverting the digits of a negative-finite number makes larger absolute
/// values produce *smaller* byte sequences, so that the raw-byte ordering of
/// the encoded key matches the true numeric ordering (more-negative → smaller
/// key).
fn extend_inverted_ascii_digits(digits: &[u8], out: &mut Vec<u8>) {
    out.reserve(digits.len());
    for &digit in digits {
        debug_assert!(digit.is_ascii_digit());
        out.push(b'9' - (digit - b'0'));
    }
}

/// Encode a [`NumericClass`] into its sort-key payload and append it to `out`.
///
/// # Layout
///
/// ```text
/// NaN / ±Infinity / zero
///   [class_tag]          — 1 byte, no further payload
///
/// Positive finite  (value = d × 10^exp10, d > 0)
///   [NUM_POS_FINITE]
///   [biased_exp u32 BE]  — 4 bytes: (magnitude + i32::MAX), where
///                          magnitude = floor(log10(|value|)) + 1
///                          = digits.len() + exp10
///   [digit bytes ASCII]  — the significant digits, no dot
///   [0x00]               — NUL terminator
///
/// Negative finite  (value = -d × 10^exp10, d > 0)
///   [NUM_NEG_FINITE]
///   [biased_exp u32 BE]  — 4 bytes: (i32::MAX - magnitude), i.e. the
///                          positive exponent is *subtracted* from the bias
///                          so that a larger magnitude → smaller exponent
///                          bytes → smaller key (more-negative value)
///   [inverted digits]    — each digit byte is replaced by ('9' - digit),
///                          so larger digits → smaller bytes → smaller key
///   [0x00]               — NUL terminator
/// ```
///
/// ## Why this works
///
/// For **positive** numbers the biased exponent grows with the value, and the
/// digit string is a left-to-right decimal representation, so the resulting
/// byte sequence has the same total ordering as the real number line.
///
/// For **negative** numbers both the exponent and the digit bytes are
/// complemented, turning the "larger absolute value is more negative" rule
/// into "larger absolute value → smaller bytes", which again yields the
/// correct total order.
///
/// The `i32::MAX` bias keeps the `u32` exponent in the range `[0, 2×i32::MAX]`
/// for all representable magnitudes, avoiding wrap-around.
fn encode_numeric_key_payload(cn: NumericClass, out: &mut Vec<u8>) {
    match cn {
        NumericClass::NaN => out.push(numeric_code::NUM_NAN),
        NumericClass::NegInf => out.push(numeric_code::NUM_NEG_INF),
        NumericClass::PosInf => out.push(numeric_code::NUM_POS_INF),
        NumericClass::Finite {
            negative,
            digits,
            exp10,
            ..
        } => {
            if is_zero_digits(&digits) {
                out.push(numeric_code::NUM_ZERO);
                return;
            }

            // magnitude = floor(log10(|value|)) + 1 = number of digits before
            // the decimal point in the un-scaled integer representation.
            let magnitude = (digits.len() as i32 + exp10) as i64;

            if negative {
                out.push(numeric_code::NUM_NEG_FINITE);
                // Bias: subtract magnitude so that a *larger* absolute value
                // produces a *smaller* exponent word → smaller key bytes.
                encode_exp_u32((i32::MAX as i64 - magnitude) as u32, out);
                // Complement digits so that larger digit values → smaller bytes.
                extend_inverted_ascii_digits(&digits, out);
            } else {
                out.push(numeric_code::NUM_POS_FINITE);
                // Bias: add magnitude so that a *larger* value produces a
                // *larger* exponent word → larger key bytes.
                encode_exp_u32((magnitude + i32::MAX as i64) as u32, out);
                out.extend_from_slice(&digits);
            }

            out.push(0x00);
        }
    }
}

// ---------------------------------------------------------------------------
// Binary payload escaping helpers
// ---------------------------------------------------------------------------

/// Encode `bytes` with escape sequences and append a `0x00` terminator.
///
/// Escape rules (preserves lexicographic order on the original bytes):
/// * `0x01` → `[0x01, 0x01]`
/// * `0x00` → `[0x01, 0x00]`
///
/// A bare `0x00` (not preceded by `0x01`) acts as the terminator.
fn encode_binary_payload(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        match b {
            0x00 => {
                out.push(0x01);
                out.push(0x00);
            }
            0x01 => {
                out.push(0x01);
                out.push(0x01);
            }
            _ => {
                out.push(b);
            }
        }
    }
    out.push(0x00); // terminator
}

/// Decode an escaped binary payload from `reader`, consuming up to and including
/// the `0x00` terminator.  Returns the unescaped bytes.
fn decode_binary_payload(reader: &ByteReader<&[u8]>) -> std::io::Result<Vec<u8>> {
    let mut result = Vec::new();
    loop {
        let b = reader.read_u8()?;
        match b {
            0x00 => break, // bare 0x00 is the terminator
            0x01 => {
                let next = reader.read_u8()?;
                result.push(next); // 0x01 -> next (either 0x00 or 0x01)
            }
            _ => result.push(b),
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// KeyType — compact binary encoding of the BSON type metadata needed to decode
//           a sort key back to a Bson value.
// ---------------------------------------------------------------------------

/// Custom type-code constants for the `KeyType` serialized format.
///
/// Scalar types occupy exactly **1 byte**.
/// `Double` is split into two codes so that the sign of `-0.0` / `-NaN` is
/// captured implicitly, without a separate `is_negative` flag.
/// Container types (`Document`, `Array`) are followed the concatenated child `KeyType` bytes.
///
/// ```text
/// Scalar layout : [code]
/// Container layout: [code]([child_key_type_bytes]...)
/// ```
mod key_type_code {
    pub const MIN_KEY: u8 = 0x01;
    pub const NULL: u8 = 0x02;
    pub const POS_DOUBLE: u8 = 0x03;
    pub const NEG_DOUBLE: u8 = 0x04;
    pub const INT32: u8 = 0x05;
    pub const INT64: u8 = 0x06;
    pub const DECIMAL128: u8 = 0x07;
    pub const STRING: u8 = 0x08;
    pub const BINARY: u8 = 0x09;
    pub const OBJECT_ID: u8 = 0x0A;
    pub const BOOLEAN: u8 = 0x0B;
    pub const DATETIME: u8 = 0x0C;
    pub const TIMESTAMP: u8 = 0x0D;
    pub const DOCUMENT: u8 = 0x0E;
    pub const ARRAY: u8 = 0x0F;
    pub const MAX_KEY: u8 = 0x10;
}

mod decimal128_meta_code {
    pub const CANONICAL: u8 = 0x00;
    pub const FINITE: u8 = 0x01;
    pub const NAN_POSITIVE: u8 = 0x02;
    pub const NAN_NEGATIVE: u8 = 0x03;
}

enum Decimal128Metadata {
    Canonical,
    Finite { negative: bool, exp10_orig: i32 },
    NaN { negative: bool },
}

fn decimal128_is_negative(value: &bson::Decimal128) -> bool {
    value.bytes()[15] & 0x80 != 0
}

fn encode_decimal128_metadata(
    value: &bson::Decimal128,
    cn: &NumericClass,
    key_type: &mut Vec<u8>,
) -> std::io::Result<()> {
    key_type.push(key_type_code::DECIMAL128);
    match cn {
        NumericClass::Finite {
            decimal_metadata: Some(metadata),
            ..
        } => {
            key_type.push(decimal128_meta_code::FINITE);
            key_type.push(if metadata.original_negative { 1 } else { 0 });
            varint::write_i32(metadata.original_exp10, key_type);
            Ok(())
        }
        NumericClass::NaN => {
            let negative = decimal128_is_negative(value);
            key_type.push(if negative {
                decimal128_meta_code::NAN_NEGATIVE
            } else {
                decimal128_meta_code::NAN_POSITIVE
            });
            Ok(())
        }
        _ => {
            key_type.push(decimal128_meta_code::CANONICAL);
            Ok(())
        }
    }
}

fn decode_decimal128_metadata(key_type: &ByteReader<&[u8]>) -> std::io::Result<Decimal128Metadata> {
    let mode = key_type.read_u8()?;
    match mode {
        decimal128_meta_code::CANONICAL => Ok(Decimal128Metadata::Canonical),
        decimal128_meta_code::FINITE => {
            let negative = match key_type.read_u8()? {
                0 => false,
                1 => true,
                _ => return Err(invalid_data("invalid Decimal128 sign metadata")),
            };
            let exp10_orig = key_type.read_varint_i32()?;
            Ok(Decimal128Metadata::Finite {
                negative,
                exp10_orig,
            })
        }
        decimal128_meta_code::NAN_POSITIVE => Ok(Decimal128Metadata::NaN { negative: false }),
        decimal128_meta_code::NAN_NEGATIVE => Ok(Decimal128Metadata::NaN { negative: true }),
        _ => Err(invalid_data("invalid Decimal128 metadata mode")),
    }
}

// ---------------------------------------------------------------------------
// Encoding — produces (encoded key, encoded key type) simultaneously
// ---------------------------------------------------------------------------

/// A sort key together with the type metadata needed to decode it back into a [`Bson`] value.
///
/// Produced by [`BsonKey::try_into_typed_key`].  Keeping the two byte vectors in a named
/// struct prevents accidentally swapping `key` and `key_type` at call sites.
#[derive(Debug, PartialEq)]
pub struct TypedKey {
    /// The encoded, byte-comparable sort key.
    pub key: Vec<u8>,
    /// The type metadata bytes that allow [`decode_bson_from_key`] to reconstruct the
    /// original [`Bson`] value from `key`.
    pub key_type: Vec<u8>,
}

/// Trait for converting BSON values into sortable byte keys.
pub trait BsonKey {
    /// Encode this value into a sort key.
    fn try_into_key(&self) -> std::io::Result<Vec<u8>>;

    /// Encode this value into a sort key **and** its type metadata
    /// needed to decode the key back into a [`Bson`] value later.
    fn try_into_typed_key(&self) -> std::io::Result<TypedKey>;
}

impl BsonKey for Bson {
    fn try_into_key(&self) -> std::io::Result<Vec<u8>> {
        Ok(self.try_into_typed_key()?.key)
    }

    fn try_into_typed_key(&self) -> std::io::Result<TypedKey> {
        let mut key = Vec::new();
        let mut key_type = Vec::new();
        encode_bson_into_key_typed(self, &mut key, &mut key_type)?;
        Ok(TypedKey { key, key_type })
    }
}

/// Encode a `Bson` value into `out` and return the [`KeyType`] describing the original type.
fn encode_bson_into_key_typed(
    value: &Bson,
    key: &mut Vec<u8>,
    key_type: &mut Vec<u8>,
) -> std::io::Result<()> {
    match value {
        Bson::MinKey => {
            key.push(key_code::TYPE_MIN_KEY);
            key_type.push(key_type_code::MIN_KEY);
        }
        Bson::Null => {
            key.push(key_code::TYPE_NULL);
            key_type.push(key_type_code::NULL);
        }
        Bson::MaxKey => {
            key.push(key_code::TYPE_MAX_KEY);
            key_type.push(key_type_code::MAX_KEY);
        }

        Bson::Boolean(b) => {
            key.push(if *b {
                key_code::TYPE_BOOL_TRUE
            } else {
                key_code::TYPE_BOOL_FALSE
            });
            key_type.push(key_type_code::BOOLEAN);
        }

        Bson::String(s) => {
            key.push(key_code::TYPE_STRING);
            key.extend_from_slice(s.as_bytes());
            key.push(0x00);
            key_type.push(key_type_code::STRING);
        }

        Bson::ObjectId(oid) => {
            key.push(key_code::TYPE_OBJECT_ID);
            key.extend_from_slice(&oid.bytes());
            key_type.push(key_type_code::OBJECT_ID);
        }

        Bson::DateTime(dt) => {
            key.push(key_code::TYPE_DATETIME);
            let mut bytes = dt.timestamp_millis().to_be_bytes();
            bytes[0] ^= 0x80;
            key.extend_from_slice(&bytes);
            key_type.push(key_type_code::DATETIME);
        }

        Bson::Timestamp(ts) => {
            key.push(key_code::TYPE_TIMESTAMP);
            key.extend_from_slice(&ts.time.to_be_bytes());
            key.extend_from_slice(&ts.increment.to_be_bytes());
            key_type.push(key_type_code::TIMESTAMP);
        }

        Bson::Binary(bin) => {
            key.push(key_code::TYPE_BINARY);
            key.push(subtype_code(bin.subtype));
            encode_binary_payload(&bin.bytes, key);
            key_type.push(key_type_code::BINARY);
        }

        Bson::Double(f) => {
            key.push(key_code::TYPE_NUMBER);
            let cn = canonical_numeric(value).ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "Failed to canonicalize Double")
            })?;
            let is_negative = f.is_sign_negative();
            encode_numeric_key_payload(cn, key);
            key_type.push(if is_negative {
                key_type_code::NEG_DOUBLE
            } else {
                key_type_code::POS_DOUBLE
            });
        }

        Bson::Int32(_) => {
            key.push(key_code::TYPE_NUMBER);
            encode_numeric_key_payload(canonical_numeric(value).unwrap(), key);
            key_type.push(key_type_code::INT32);
        }

        Bson::Int64(_) => {
            key.push(key_code::TYPE_NUMBER);
            encode_numeric_key_payload(canonical_numeric(value).unwrap(), key);
            key_type.push(key_type_code::INT64);
        }

        Bson::Decimal128(d) => {
            let cn = canonical_numeric(value).ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "Failed to canonicalize Decimal128")
            })?;
            key.push(key_code::TYPE_NUMBER);
            encode_numeric_key_payload(cn.clone(), key);
            encode_decimal128_metadata(d, &cn, key_type)?;
        }

        Bson::Document(doc) => {
            key.push(key_code::TYPE_DOCUMENT);
            key_type.push(key_type_code::DOCUMENT);
            let mut sub_types = Vec::with_capacity(doc.len());
            for (field_name, field_value) in doc {
                key.push(0x01);
                key.extend_from_slice(field_name.as_bytes());
                key.push(0x00);
                let field_kt = encode_bson_into_key_typed(field_value, key, key_type)?;
                sub_types.push(field_kt);
            }
            key.push(0x00);
        }

        Bson::Array(arr) => {
            key.push(key_code::TYPE_ARRAY);
            key_type.push(key_type_code::ARRAY);
            let mut sub_types = Vec::with_capacity(arr.len());
            for elem in arr {
                key.push(0x01);
                let elem_kt = encode_bson_into_key_typed(elem, key, key_type)?;
                sub_types.push(elem_kt);
            }
            key.push(0x00);
        }

        _ => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Unsupported BSON type: {:?}", value),
            ))
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Decoding — reconstruct Bson from key bytes + KeyType
// ---------------------------------------------------------------------------

/// Decode a key produced by [`BsonKey::try_into_typed_key`] back into a [`Bson`] value.
///
/// * `key`      – the raw key bytes
/// * `key_type` – the raw type bytes
pub fn decode_bson_from_key(key: &TypedKey) -> std::io::Result<Bson> {
    let key_reader = ByteReader::new(key.key.as_slice());
    let key_type_reader = ByteReader::new(key.key_type.as_slice());
    decode_key_inner(&key_reader, &key_type_reader)
}

/// Decode one BSON value by jointly advancing pre-built `ByteReader`s.
///
/// This variant is used when the key and key_type readers are shared across
/// multiple successive decode calls (e.g. decoding multi-field index entries)
/// so that the readers' positions advance correctly for each field.
pub fn decode_bson_from_key_readers(
    key_reader: &ByteReader<&[u8]>,
    key_type_reader: &ByteReader<&[u8]>,
) -> std::io::Result<Bson> {
    decode_key_inner(key_reader, key_type_reader)
}

fn decode_key_inner(
    key: &ByteReader<&[u8]>,
    key_type: &ByteReader<&[u8]>,
) -> std::io::Result<Bson> {
    if !key.has_remaining() {
        return Err(unexpected_eof("key must not be empty"));
    }
    if !key_type.has_remaining() {
        return Err(unexpected_eof("key_type must not be empty"));
    }
    let key_code = key.read_u8()?;
    let key_type_code = key_type.read_u8()?;

    match key_type_code {
        // ---- trivial single-byte types ----
        key_type_code::MIN_KEY => {
            if key_code != key_code::TYPE_MIN_KEY {
                return Err(invalid_data("MIN_KEY type code does not match key bytes"));
            }
            Ok(Bson::MinKey)
        }
        key_type_code::NULL => {
            if key_code != key_code::TYPE_NULL {
                return Err(invalid_data("NULL type code does not match key bytes"));
            }
            Ok(Bson::Null)
        }
        key_type_code::MAX_KEY => {
            if key_code != key_code::TYPE_MAX_KEY {
                return Err(invalid_data("MAX_KEY type code does not match key bytes"));
            }
            Ok(Bson::MaxKey)
        }

        // ---- boolean ----
        key_type_code::BOOLEAN => {
            if key_code != key_code::TYPE_BOOL_FALSE && key_code != key_code::TYPE_BOOL_TRUE {
                return Err(invalid_data("BOOLEAN type code does not match key bytes"));
            }
            Ok(Bson::Boolean(key_code == key_code::TYPE_BOOL_TRUE))
        }

        // ---- string ----
        key_type_code::STRING => {
            if key_code != key_code::TYPE_STRING {
                return Err(invalid_data("STRING type code does not match key bytes"));
            }
            // layout: [0x30][utf-8 bytes][0x00]
            let nul_abs = key
                .find_next_by(|b| b == 0x00)
                .ok_or_else(|| invalid_data("unterminated string in key"))?;
            let nul_rel = nul_abs - key.position();
            let s_bytes = key.read_fixed_slice(nul_rel)?;
            let s = std::str::from_utf8(s_bytes).map_err(|e| invalid_data(e))?;
            let result = Bson::String(s.to_owned());
            key.skip(1)?; // consume NUL terminator
            Ok(result)
        }

        // ---- ObjectId ----
        key_type_code::OBJECT_ID => {
            if key_code != key_code::TYPE_OBJECT_ID {
                return Err(invalid_data("OBJECT_ID type code does not match key bytes"));
            }
            // layout: [0x50][12 bytes]
            let bytes = key.read_fixed_slice(12)?;
            let oid = bson::oid::ObjectId::from_bytes(
                bytes
                    .try_into()
                    .map_err(|_| invalid_data("invalid ObjectId length in key"))?,
            );
            Ok(Bson::ObjectId(oid))
        }

        // ---- DateTime ----
        key_type_code::DATETIME => {
            if key_code != key_code::TYPE_DATETIME {
                return Err(invalid_data("DATETIME type code does not match key bytes"));
            }
            // layout: [0x70][8 bytes big-endian i64 with sign bit flipped]
            let raw = key.read_fixed_slice(8)?;
            let mut bytes: [u8; 8] = raw
                .try_into()
                .map_err(|_| invalid_data("invalid DateTime payload length in key"))?;
            bytes[0] ^= 0x80;
            let millis = i64::from_be_bytes(bytes);
            Ok(Bson::DateTime(bson::DateTime::from_millis(millis)))
        }

        // ---- Timestamp ----
        key_type_code::TIMESTAMP => {
            if key_code != key_code::TYPE_TIMESTAMP {
                return Err(invalid_data("TIMESTAMP type code does not match key bytes"));
            }
            // layout: [0x80][time u32 BE][increment u32 BE]
            let time = key.read_u32_be()?;
            let increment = key.read_u32_be()?;
            Ok(Bson::Timestamp(bson::Timestamp { time, increment }))
        }

        // ---- Binary ----
        key_type_code::BINARY => {
            if key_code != key_code::TYPE_BINARY {
                return Err(invalid_data("BINARY type code does not match key bytes"));
            }
            // layout: [0x40][subtype byte][escaped payload bytes][0x00 terminator]
            let subtype = BinarySubtype::from(key.read_u8()?);
            let bytes = decode_binary_payload(key)?;
            Ok(Bson::Binary(bson::Binary { subtype, bytes }))
        }

        // ---- numeric types ----
        code @ (key_type_code::INT32
        | key_type_code::INT64
        | key_type_code::POS_DOUBLE
        | key_type_code::NEG_DOUBLE
        | key_type_code::DECIMAL128) => {
            if key_code != key_code::TYPE_NUMBER {
                return Err(invalid_data("numeric type code does not match key bytes"));
            }
            decode_numeric_key_payload(key, key_type, code)
        }

        // ---- Document ----
        key_type_code::DOCUMENT => {
            if key_code != key_code::TYPE_DOCUMENT {
                return Err(invalid_data("DOCUMENT type code does not match key bytes"));
            }
            // layout: [0x34][( 0x01 [field-name NUL] [value-key] )* 0x00]
            let mut doc = bson::Document::new();

            loop {
                if !key.has_remaining() {
                    return Err(invalid_data("unterminated document in key"));
                }
                let marker = key.read_u8()?;
                if marker == 0x00 {
                    break;
                }
                if marker != 0x01 {
                    return Err(invalid_data(
                        "expected 0x01 field-present marker in document key",
                    ));
                }

                // field name: NUL-terminated
                let nul_abs = key
                    .find_next_by(|b| b == 0x00)
                    .ok_or_else(|| invalid_data("unterminated field name in document key"))?;
                let nul_rel = nul_abs - key.position();
                let name_bytes = key.read_fixed_slice(nul_rel)?;
                let field_name = std::str::from_utf8(name_bytes)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
                    .to_owned();
                key.skip(1)?; // consume NUL terminator

                let value_bson = decode_key_inner(key, key_type)?;
                doc.insert(field_name, value_bson);
            }

            Ok(Bson::Document(doc))
        }

        // ---- Array ----
        key_type_code::ARRAY => {
            if key_code != key_code::TYPE_ARRAY {
                return Err(invalid_data("ARRAY type code does not match key bytes"));
            }
            // layout: [0x38][( 0x01 [elem-key] )* 0x00]
            let mut arr = Vec::new();

            loop {
                if !key.has_remaining() {
                    return Err(invalid_data("unterminated array in key"));
                }
                let marker = key.read_u8()?;
                if marker == 0x00 {
                    break;
                }
                if marker != 0x01 {
                    return Err(invalid_data(
                        "expected 0x01 element-present marker in array key",
                    ));
                }

                let elem_bson = decode_key_inner(key, key_type)?;
                arr.push(elem_bson);
            }

            Ok(Bson::Array(arr))
        }

        other => Err(invalid_data(&format!(
            "unknown KeyType code 0x{other:02X} in decode_key_inner"
        ))),
    }
}

/// Decode the numeric payload (the bytes *after* the `0x20` type tag).
/// `code` is the `key_type_code` constant for the numeric type.
/// Advances `reader` past the consumed payload bytes.
fn decode_numeric_key_payload(
    reader: &ByteReader<&[u8]>,
    key_type: &ByteReader<&[u8]>,
    code: u8,
) -> std::io::Result<Bson> {
    if !reader.has_remaining() {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "numeric payload must not be empty",
        ));
    }

    let class_tag = reader.read_u8()?;

    match class_tag {
        numeric_code::NUM_NAN => match code {
            key_type_code::POS_DOUBLE | key_type_code::NEG_DOUBLE => {
                let nan = if code == key_type_code::NEG_DOUBLE {
                    -f64::NAN
                } else {
                    f64::NAN
                };
                Ok(Bson::Double(nan))
            }
            key_type_code::DECIMAL128 => {
                let decimal_str = match decode_decimal128_metadata(key_type)? {
                    Decimal128Metadata::Canonical => "NaN",
                    Decimal128Metadata::NaN { negative: true } => "-NaN",
                    Decimal128Metadata::NaN { negative: false } => "NaN",
                    Decimal128Metadata::Finite { .. } => {
                        return Err(invalid_data(
                            "unexpected finite Decimal128 metadata for NaN",
                        ));
                    }
                };
                Ok(Bson::Decimal128(
                    bson::Decimal128::from_str(decimal_str).map_err(|e| invalid_data(e))?,
                ))
            }
            _ => Err(invalid_data(&format!(
                "NaN key payload for unsupported type code 0x{code:02X}"
            ))),
        },

        numeric_code::NUM_NEG_INF => match code {
            key_type_code::POS_DOUBLE | key_type_code::NEG_DOUBLE => {
                Ok(Bson::Double(f64::NEG_INFINITY))
            }
            key_type_code::DECIMAL128 => {
                match decode_decimal128_metadata(key_type)? {
                    Decimal128Metadata::Canonical => {}
                    Decimal128Metadata::NaN { .. } => {
                        return Err(invalid_data(
                            "unexpected NaN Decimal128 metadata for -Infinity",
                        ));
                    }
                    Decimal128Metadata::Finite { .. } => {
                        return Err(invalid_data(
                            "unexpected finite Decimal128 metadata for -Infinity",
                        ));
                    }
                }
                Ok(Bson::Decimal128(
                    bson::Decimal128::from_str("-Infinity")
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
                ))
            }
            _ => Err(invalid_data(&format!(
                "NEG_INF payload for non-float type code 0x{code:02X}"
            ))),
        },

        numeric_code::NUM_POS_INF => match code {
            key_type_code::POS_DOUBLE | key_type_code::NEG_DOUBLE => {
                Ok(Bson::Double(f64::INFINITY))
            }
            key_type_code::DECIMAL128 => {
                match decode_decimal128_metadata(key_type)? {
                    Decimal128Metadata::Canonical => {}
                    Decimal128Metadata::NaN { .. } => {
                        return Err(invalid_data(
                            "unexpected NaN Decimal128 metadata for Infinity",
                        ));
                    }
                    Decimal128Metadata::Finite { .. } => {
                        return Err(invalid_data(
                            "unexpected finite Decimal128 metadata for Infinity",
                        ));
                    }
                }
                Ok(Bson::Decimal128(
                    bson::Decimal128::from_str("Infinity").map_err(|e| invalid_data(e))?,
                ))
            }
            _ => Err(invalid_data(&format!(
                "POS_INF payload for non-float type code 0x{code:02X}"
            ))),
        },

        numeric_code::NUM_ZERO => match code {
            key_type_code::INT32 => Ok(Bson::Int32(0)),
            key_type_code::INT64 => Ok(Bson::Int64(0)),
            key_type_code::POS_DOUBLE => Ok(Bson::Double(0.0_f64)),
            key_type_code::NEG_DOUBLE => Ok(Bson::Double(-0.0_f64)),
            key_type_code::DECIMAL128 => {
                let decimal_str = match decode_decimal128_metadata(key_type)? {
                    Decimal128Metadata::Finite {
                        negative,
                        exp10_orig,
                    } => build_decimal_string_with_original_exp(negative, &[b'0'], 0, exp10_orig)?,
                    Decimal128Metadata::Canonical => "0".to_string(),
                    Decimal128Metadata::NaN { .. } => {
                        return Err(invalid_data("unexpected NaN Decimal128 metadata for zero"));
                    }
                };
                Ok(Bson::Decimal128(
                    bson::Decimal128::from_str(&decimal_str).map_err(|e| invalid_data(e))?,
                ))
            }
            _ => Err(invalid_data(&format!(
                "ZERO payload for unknown numeric type code 0x{code:02X}"
            ))),
        },

        numeric_code::NUM_NEG_FINITE | numeric_code::NUM_POS_FINITE => {
            // layout: [class][exp_u32 BE (4 bytes)][digit bytes ASCII][0x00 terminator]
            let negative = class_tag == numeric_code::NUM_NEG_FINITE;

            let raw_exp = reader.read_u32_be()?;
            let magnitude: i64 = if negative {
                i32::MAX as i64 - raw_exp as i64
            } else {
                raw_exp as i64 - i32::MAX as i64
            };

            // Digits run until the 0x00 terminator
            let nul_abs = reader
                .find_next_by(|b| b == 0x00)
                .ok_or_else(|| invalid_data("unterminated numeric payload"))?;
            let nul_rel = nul_abs - reader.position();
            let raw_digits = reader.read_fixed_slice(nul_rel)?;

            // For negative finite values the digits were inverted; undo that.
            let digits: Vec<u8> = if negative {
                raw_digits.iter().map(|&b| b'9' - (b - b'0')).collect()
            } else {
                raw_digits.to_vec()
            };

            if digits.is_empty() {
                return Err(invalid_data("numeric payload is missing digits"));
            }
            if !digits.iter().all(|d| d.is_ascii_digit()) {
                return Err(invalid_data("numeric payload contains non-digit bytes"));
            }

            reader.skip(1)?; // consume NUL terminator

            // magnitude = digits.len() + exp10  ⟹  exp10 = magnitude - digits.len()
            let exp10 = magnitude - digits.len() as i64;

            match code {
                key_type_code::INT32 => {
                    let v = reconstruct_integer(&digits, exp10, negative)?;
                    let int32 = i32::try_from(v)
                        .map_err(|_| invalid_data("numeric payload is out of range for Int32"))?;
                    Ok(Bson::Int32(int32))
                }
                key_type_code::INT64 => {
                    let v = reconstruct_integer(&digits, exp10, negative)?;
                    Ok(Bson::Int64(v))
                }
                key_type_code::POS_DOUBLE | key_type_code::NEG_DOUBLE => {
                    let decimal_str = build_decimal_string(negative, &digits, exp10);
                    let v: f64 = decimal_str.parse::<f64>().map_err(|e| invalid_data(e))?;
                    Ok(Bson::Double(v))
                }
                key_type_code::DECIMAL128 => {
                    let decimal_str = match decode_decimal128_metadata(key_type)? {
                        Decimal128Metadata::Finite {
                            negative: negative_orig,
                            exp10_orig,
                        } => build_decimal_string_with_original_exp(
                            negative_orig,
                            &digits,
                            exp10,
                            exp10_orig,
                        )?,
                        Decimal128Metadata::Canonical => {
                            build_decimal_string(negative, &digits, exp10)
                        }
                        Decimal128Metadata::NaN { .. } => {
                            return Err(invalid_data(
                                "unexpected NaN Decimal128 metadata for finite value",
                            ));
                        }
                    };
                    let d =
                        bson::Decimal128::from_str(&decimal_str).map_err(|e| invalid_data(e))?;
                    Ok(Bson::Decimal128(d))
                }
                _ => Err(invalid_data(&format!(
                    "finite numeric payload for unknown type code 0x{code:02X}"
                ))),
            }
        }

        other => Err(invalid_data(&format!(
            "unknown numeric class tag 0x{other:02X}"
        ))),
    }
}

/// Reconstruct an integer value directly from its digit bytes and base-10 exponent.
///
/// `digits` are ASCII digit bytes, `exp10` is the power of ten by which the digit
/// integer is scaled (always `>= 0` for integers stored in a key), and `negative`
/// flips the sign of the result.
///
/// Accumulates through `u64` to correctly handle `i64::MIN` whose absolute value
/// (`9223372036854775808`) exceeds `i64::MAX`.
///
/// Panics if `exp10 < 0` (would imply a fractional value, which cannot be Int32/Int64).
fn reconstruct_integer(digits: &[u8], exp10: i64, negative: bool) -> std::io::Result<i64> {
    if exp10 < 0 {
        return Err(invalid_data(&format!(
            "exp10 must be >= 0 for integer reconstruction, got {exp10}"
        )));
    }
    let mut value: u64 = 0;
    for &b in digits {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as u64))
            .ok_or_else(|| invalid_data("integer payload overflows u64"))?;
    }
    for _ in 0..exp10 {
        value = value
            .checked_mul(10)
            .ok_or_else(|| invalid_data("integer payload overflows u64"))?;
    }
    if negative {
        // value == i64::MAX + 1 is i64::MIN; handle without wrapping arithmetic.
        if value > i64::MAX as u64 + 1 {
            return Err(invalid_data(
                "integer payload is out of range for signed values",
            ));
        }
        Ok(value.wrapping_neg() as i64)
    } else {
        if value > i64::MAX as u64 {
            return Err(invalid_data(
                "integer payload is out of range for signed values",
            ));
        }
        Ok(value as i64)
    }
}

/// Build a decimal string `±digits × 10^exp10` suitable for `parse::<f64>()` and
/// `Decimal128::from_str()`.
fn build_decimal_string(negative: bool, digits: &[u8], exp10: i64) -> String {
    let digit_chars: String = digits.iter().map(|&b| b as char).collect();
    let sign = if negative { "-" } else { "" };
    let adjusted_exp = exp10 + digits.len() as i64 - 1;
    let (first, rest) = digit_chars.split_at(1);
    if rest.is_empty() {
        format!("{}{}e{}", sign, first, adjusted_exp)
    } else {
        format!("{}{}.{}e{}", sign, first, rest, adjusted_exp)
    }
}

fn build_decimal_string_with_original_exp(
    negative: bool,
    digits_norm: &[u8],
    exp10_norm: i64,
    exp10_orig: i32,
) -> std::io::Result<String> {
    let trailing_zeros = exp10_norm - exp10_orig as i64;
    if trailing_zeros < 0 {
        return Err(invalid_data(
            "Decimal128 original exponent exceeds normalized exponent",
        ));
    }

    let mut digits = digits_norm.to_vec();
    digits.extend(std::iter::repeat_n(b'0', trailing_zeros as usize));
    Ok(build_decimal_string(negative, &digits, exp10_orig as i64))
}

#[cfg(test)]
mod tests {

    mod prepend_raw_bson_field {
        use crate::util::bson_utils::{make_raw_bson_element, prepend_raw_bson_field};
        use bson::{doc, Bson};

        #[test]
        fn inserts_string_field_as_first() {
            let mut doc_buf = doc! { "a": 1_i32, "b": true }.to_vec().unwrap();
            let field = make_raw_bson_element("x", &Bson::String("bar".into())).unwrap();
            prepend_raw_bson_field(&mut doc_buf, &field).unwrap();
            assert_eq!(
                doc_buf,
                doc! { "x": "bar", "a": 1_i32, "b": true }.to_vec().unwrap()
            );
        }

        #[test]
        fn rejects_length_mismatch() {
            let mut bad = doc! { "a": 1_i32 }.to_vec().unwrap();
            bad[0..4].copy_from_slice(&0_i32.to_le_bytes());
            let field = make_raw_bson_element("x", &Bson::Int32(0)).unwrap();
            assert!(prepend_raw_bson_field(&mut bad, &field).is_err());
        }

        #[test]
        fn rejects_missing_terminator() {
            let mut bad = doc! { "a": 1_i32 }.to_vec().unwrap();
            bad.pop();
            let field = make_raw_bson_element("x", &Bson::String("y".into())).unwrap();
            assert!(prepend_raw_bson_field(&mut bad, &field).is_err());
        }
    }

    mod bson_key {
        use crate::util::bson_utils::BsonKey;
        use bson::{oid::ObjectId, Binary, Bson, DateTime, Decimal128, Timestamp};
        use std::str::FromStr;
        use bson::raw::CString;

        fn assert_ordering(a: &Bson, b: &Bson) {
            let key_a = a.try_into_key().expect("Failed to encode BSON A");
            let key_b = b.try_into_key().expect("Failed to encode BSON B");
            assert!(key_a < key_b, "Expected {:?} < {:?}", a, b);
        }

        #[test]
        fn minkey_maxkey() {
            assert_ordering(&Bson::MinKey, &Bson::Null);
            assert_ordering(&Bson::Int32(0), &Bson::MaxKey);
        }

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

        #[test]
        fn double_ordering() {
            assert_ordering(&Bson::Double(-1000.0), &Bson::Double(-1.0));
            assert_ordering(&Bson::Double(-1.0), &Bson::Double(-0.1));
            assert_ordering(&Bson::Double(-0.1), &Bson::Double(0.0));
            assert_ordering(&Bson::Double(0.0), &Bson::Double(0.1));
            assert_ordering(&Bson::Double(0.1), &Bson::Double(1.0));
            assert_ordering(&Bson::Double(1.0), &Bson::Double(1000.0));
        }

        #[test]
        fn double_edge_cases() {
            assert_ordering(&Bson::Double(-1.0), &Bson::Double(1.0));

            let neg_zero_key = Bson::Double(-0.0)
                .try_into_key()
                .expect("Failed to encode -0.0");
            let pos_zero_key = Bson::Double(0.0)
                .try_into_key()
                .expect("Failed to encode 0.0");
            assert_eq!(
                neg_zero_key, pos_zero_key,
                "Expected Double(-0.0) and Double(0.0) to map to one canonical zero key"
            );

            assert_ordering(&Bson::Double(1.0e-10), &Bson::Double(1.0));
            assert_ordering(&Bson::Double(1.0), &Bson::Double(1.0e10));
        }

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

        fn assert_decimal_eq(a: &Decimal128, b: &Decimal128) {
            let key_a = &Bson::Decimal128(*a).try_into_key().unwrap();
            let key_b = &Bson::Decimal128(*b).try_into_key().unwrap();
            assert_eq!(
                key_a, key_b,
                "Expected {:?} = {:?}, but got {:?} != {:?}",
                a, b, key_a, key_b
            );
        }

        #[test]
        fn decimal128_leading_zero_normalization() {
            let dec1 = Decimal128::from_str("1.000000000000000000000000000000000").unwrap();
            let dec2 = Decimal128::from_str("1.000000000000000000000000000000001").unwrap();
            assert_decimal_ordering(&dec1, &dec2);
        }

        #[test]
        fn decimal128_exponent_adjustment() {
            let dec1 = Decimal128::from_str("1.0").unwrap();
            let dec2 = Decimal128::from_str("10.0").unwrap();
            let dec3 = Decimal128::from_str("100.0").unwrap();
            assert_decimal_ordering(&dec1, &dec2);
            assert_decimal_ordering(&dec2, &dec3);
        }

        #[test]
        fn decimal128_extreme_values() {
            let min_value = Decimal128::from_str("1E-6176").unwrap();
            let max_value =
                Decimal128::from_str("9.999999999999999999999999999999999E6111").unwrap();
            assert_decimal_ordering(&min_value, &max_value);
        }

        #[test]
        fn decimal128_large_vs_small_coefficients() {
            let small_coeff = Decimal128::from_str("0.00000000000000000001").unwrap();
            let large_coeff = Decimal128::from_str("10000000000000000000.0").unwrap();
            assert_decimal_ordering(&small_coeff, &large_coeff);
        }

        #[test]
        fn decimal128_zero_ordering() {
            let dec_zero = Decimal128::from_str("0.0").unwrap();
            let dec_neg_zero = Decimal128::from_str("-0.0").unwrap();
            assert_decimal_eq(&dec_neg_zero, &dec_zero);
            assert_decimal_eq(&dec_zero, &Decimal128::from_str("0").unwrap());
        }

        #[test]
        fn numeric_cross_type_zero_canonicalization() {
            let dec_zero = Decimal128::from_str("0").unwrap();
            let dec_neg_zero = Decimal128::from_str("-0").unwrap();

            let int32_zero = Bson::Int32(0);
            let int64_zero = Bson::Int64(0);
            let dbl_zero = Bson::Double(0.0);
            let dbl_neg_zero = Bson::Double(-0.0);
            let dec_zero_bson = Bson::Decimal128(dec_zero);
            let dec_neg_zero_bson = Bson::Decimal128(dec_neg_zero);

            let keys = vec![
                int32_zero.try_into_key().unwrap(),
                int64_zero.try_into_key().unwrap(),
                dbl_zero.try_into_key().unwrap(),
                dbl_neg_zero.try_into_key().unwrap(),
                dec_zero_bson.try_into_key().unwrap(),
                dec_neg_zero_bson.try_into_key().unwrap(),
            ];

            for i in 0..keys.len() {
                for j in 0..keys.len() {
                    assert_eq!(keys[i], keys[j], "zero keys differed at ({i},{j})");
                }
            }
        }

        #[test]
        fn numeric_special_values_nan_and_infinity_policy() {
            let nan = Bson::Double(f64::NAN);
            let neg_nan = Bson::Double(-f64::NAN);
            let neg_inf = Bson::Double(f64::NEG_INFINITY);
            let pos_inf = Bson::Double(f64::INFINITY);
            let zero = Bson::Double(0.0);

            let nan_key = nan.try_into_key().unwrap();
            let neg_nan_key = neg_nan.try_into_key().unwrap();
            let neg_inf_key = neg_inf.try_into_key().unwrap();
            let pos_inf_key = pos_inf.try_into_key().unwrap();
            let zero_key = zero.try_into_key().unwrap();

            assert_eq!(nan_key, neg_nan_key);
            assert!(nan_key < neg_inf_key);
            assert!(neg_inf_key < zero_key);
            assert!(zero_key < pos_inf_key);
        }

        #[test]
        fn numeric_cross_type_equivalent_values_share_key() {
            let cases = vec![
                (
                    Bson::Int32(5),
                    Bson::Decimal128(Decimal128::from_str("5").unwrap()),
                ),
                (
                    Bson::Int64(-42),
                    Bson::Decimal128(Decimal128::from_str("-42").unwrap()),
                ),
                (
                    Bson::Double(1.25),
                    Bson::Decimal128(Decimal128::from_str("1.25").unwrap()),
                ),
                (
                    Bson::Double(1000.0),
                    Bson::Decimal128(Decimal128::from_str("1e3").unwrap()),
                ),
            ];

            for (a, b) in cases {
                let ka = a.try_into_key().unwrap();
                let kb = b.try_into_key().unwrap();
                assert_eq!(ka, kb, "expected equal keys for {:?} and {:?}", a, b);
            }
        }

        #[test]
        fn numeric_boundary_mixed_type_key_ordering() {
            let ordered = vec![
                Bson::Double(f64::NAN),
                Bson::Double(f64::NEG_INFINITY),
                Bson::Decimal128(Decimal128::from_str("-9223372036854775809").unwrap()),
                Bson::Int64(i64::MIN),
                Bson::Int32(i32::MIN),
                Bson::Double(-0.0),
                Bson::Int32(0),
                Bson::Decimal128(Decimal128::from_str("-0").unwrap()),
                Bson::Int64(0),
                Bson::Decimal128(Decimal128::from_str("0").unwrap()),
                Bson::Int32(i32::MAX),
                Bson::Int64(i64::MAX),
                Bson::Decimal128(Decimal128::from_str("9223372036854775808").unwrap()),
                Bson::Double(f64::INFINITY),
            ];

            for i in 0..ordered.len() {
                for j in (i + 1)..ordered.len() {
                    let ki = ordered[i].try_into_key().unwrap();
                    let kj = ordered[j].try_into_key().unwrap();
                    assert!(
                        ki <= kj,
                        "expected key {:?} <= {:?}",
                        ordered[i],
                        ordered[j]
                    );
                }
            }
        }

        #[test]
        fn decimal128_negative_vs_positive() {
            let neg_small = Decimal128::from_str("-1.0").unwrap();
            let neg_large = Decimal128::from_str("-1000.0").unwrap();
            let pos_small = Decimal128::from_str("1.0").unwrap();
            let pos_large = Decimal128::from_str("1000.0").unwrap();

            assert_decimal_ordering(&neg_large, &neg_small);
            assert_decimal_ordering(&neg_small, &pos_small);
            assert_decimal_ordering(&pos_small, &pos_large);
        }

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

        #[test]
        fn boolean_ordering() {
            assert_ordering(&Bson::Boolean(false), &Bson::Boolean(true));
        }

        #[test]
        fn objectid_ordering() {
            let oid1 = ObjectId::parse_str("000000000000000000000000").unwrap();
            let oid2 = ObjectId::parse_str("ffffffffffffffffffffffff").unwrap();
            assert_ordering(&Bson::ObjectId(oid1), &Bson::ObjectId(oid2));
        }

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

        #[test]
        fn binary_ordering_escape_preserves_lexicographic_order() {
            let b0 = Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![0x00],
            });
            let b1 = Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![0x01],
            });
            let b2 = Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![0x02],
            });
            let b3 = Bson::Binary(Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![0xFF],
            });

            assert_ordering(&b0, &b1);
            assert_ordering(&b1, &b2);
            assert_ordering(&b2, &b3);
        }

        #[test]
        fn datetime_ordering() {
            let dt1 = DateTime::from_millis(1609459200000);
            let dt2 = DateTime::from_millis(1640995200000);
            assert_ordering(&Bson::DateTime(dt1), &Bson::DateTime(dt2));
        }

        #[test]
        fn datetime_pre_epoch_ordering() {
            let dt_neg = DateTime::from_millis(-86_400_000);
            let dt_zero = DateTime::from_millis(0);
            let dt_pos = DateTime::from_millis(86_400_000);

            assert_ordering(&Bson::DateTime(dt_neg), &Bson::DateTime(dt_zero));
            assert_ordering(&Bson::DateTime(dt_zero), &Bson::DateTime(dt_pos));
            assert_ordering(&Bson::DateTime(dt_neg), &Bson::DateTime(dt_pos));
        }

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

        #[test]
        fn unsupported_types_return_err_not_panic() {
            let js_result = Bson::JavaScriptCode("x".into()).try_into_key();
            assert!(js_result.is_err());

            let symbol_result = Bson::Symbol("x".into()).try_into_key();
            assert!(symbol_result.is_err());

            let regex_result = Bson::RegularExpression(bson::Regex {
                pattern: CString::try_from("x").unwrap(),
                options: CString::try_from("").unwrap(),
            })
            .try_into_key();
            assert!(regex_result.is_err());
        }
    }

    mod bson_cmp {
        use crate::util::bson_utils;
        use bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Decimal128};
        use std::cmp::Ordering::*;
        use std::str::FromStr;

        #[test]
        fn test_type_rank() {
            assert_eq!(bson_utils::cmp_bson(&Bson::MinKey, &Bson::Null), Less);
            assert_eq!(bson_utils::cmp_bson(&Bson::Null, &Bson::Int32(0)), Less);
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Int32(0), &Bson::String("x".into())),
                Less
            );
        }

        #[test]
        fn test_numeric_family() {
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Int32(5), &Bson::Int64(5)),
                Equal
            );
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Double(3.1), &Bson::Int32(4)),
                Less
            );
        }

        #[test]
        fn test_numeric_cross_type_equality_and_boundaries() {
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Int32(0), &Bson::Double(-0.0)),
                Equal
            );
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Int64(5), &Bson::Double(5.0)),
                Equal
            );
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Int32(i32::MAX), &Bson::Int64(i32::MAX as i64 + 1)),
                Less
            );
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Int64(i64::MIN), &Bson::Double(-9.22e18)),
                Less
            );
        }

        #[test]
        fn test_numeric_nan_inf_policy() {
            let nan = Bson::Double(f64::NAN);
            let neg_nan = Bson::Double(-f64::NAN);
            let neg_inf = Bson::Double(f64::NEG_INFINITY);
            let pos_inf = Bson::Double(f64::INFINITY);
            let zero = Bson::Int32(0);

            assert_eq!(bson_utils::cmp_bson(&nan, &neg_nan), Equal);
            assert_eq!(bson_utils::cmp_bson(&nan, &neg_inf), Less);
            assert_eq!(bson_utils::cmp_bson(&neg_inf, &zero), Less);
            assert_eq!(bson_utils::cmp_bson(&zero, &pos_inf), Less);
        }

        #[test]
        fn test_numeric_cross_type_with_decimal128_and_zero_forms() {
            let dec_five = Bson::Decimal128(Decimal128::from_str("5").unwrap());
            let dec_neg = Bson::Decimal128(Decimal128::from_str("-42").unwrap());
            let dec_zero = Bson::Decimal128(Decimal128::from_str("0").unwrap());
            let dec_neg_zero = Bson::Decimal128(Decimal128::from_str("-0").unwrap());

            assert_eq!(bson_utils::cmp_bson(&Bson::Int32(5), &dec_five), Equal);
            assert_eq!(bson_utils::cmp_bson(&Bson::Int64(-42), &dec_neg), Equal);

            assert_eq!(bson_utils::cmp_bson(&Bson::Int32(0), &dec_zero), Equal);
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Double(-0.0), &dec_neg_zero),
                Equal
            );
            assert_eq!(bson_utils::cmp_bson(&dec_zero, &dec_neg_zero), Equal);

            assert_eq!(
                bson_utils::cmp_bson(
                    &Bson::Decimal128(Decimal128::from_str("5.000").unwrap()),
                    &Bson::Double(5.0)
                ),
                Equal
            );
            assert_eq!(
                bson_utils::cmp_bson(
                    &Bson::Decimal128(Decimal128::from_str("5.0001").unwrap()),
                    &Bson::Double(5.0)
                ),
                Greater
            );
        }

        #[test]
        fn test_numeric_total_policy_samples() {
            let ordered = vec![
                vec![Bson::Double(f64::NAN)],
                vec![Bson::Double(f64::NEG_INFINITY)],
                vec![Bson::Decimal128(Decimal128::from_str("-1E100").unwrap())],
                vec![Bson::Int64(-1)],
                vec![
                    Bson::Double(-0.0),
                    Bson::Int32(0),
                    Bson::Decimal128(Decimal128::from_str("0").unwrap()),
                ],
                vec![Bson::Double(1.5)],
                vec![Bson::Decimal128(Decimal128::from_str("2").unwrap())],
                vec![Bson::Double(f64::INFINITY)],
            ];

            for (i, group_a) in ordered.iter().enumerate() {
                for (j, group_b) in ordered.iter().enumerate() {
                    let expected = i.cmp(&j);
                    for lhs in group_a {
                        for rhs in group_b {
                            assert_eq!(
                                bson_utils::cmp_bson(lhs, rhs),
                                expected,
                                "unexpected ordering for lhs={:?}, rhs={:?}",
                                lhs,
                                rhs
                            );
                        }
                    }
                }
            }
        }

        #[test]
        fn test_string_vs_string() {
            assert_eq!(
                bson_utils::cmp_bson(
                    &Bson::String("apple".into()),
                    &Bson::String("banana".into())
                ),
                Less
            );
        }

        #[test]
        fn test_object_id_order() {
            let a = ObjectId::parse_str("000000000000000000000000").unwrap();
            let b = ObjectId::parse_str("ffffffffffffffffffffffff").unwrap();
            assert_eq!(
                bson_utils::cmp_bson(&Bson::ObjectId(a), &Bson::ObjectId(b)),
                Less
            );
        }

        #[test]
        fn test_binary_subtype_then_bytes() {
            let x = Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2],
            };
            let y = Binary {
                subtype: BinarySubtype::Uuid,
                bytes: vec![0],
            };
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Binary(x.clone()), &Bson::Binary(y.clone())),
                Less
            );
            let z = Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 3],
            };
            assert_eq!(
                bson_utils::cmp_bson(&Bson::Binary(x), &Bson::Binary(z)),
                Less
            );
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

            assert_eq!(bson_utils::cmp_bson(&a, &b), Less);
            assert_eq!(bson_utils::cmp_bson(&a, &c), Less);
        }

        #[test]
        fn test_datetime() {
            let t1 = DateTime::from_millis(1_000);
            let t2 = DateTime::from_millis(2_000);
            assert_eq!(
                bson_utils::cmp_bson(&Bson::DateTime(t1), &Bson::DateTime(t2)),
                Less
            );
        }

        #[test]
        fn test_same_type_boundary_equality() {
            assert_eq!(bson_utils::cmp_bson(&Bson::MinKey, &Bson::MinKey), Equal);
            assert_eq!(bson_utils::cmp_bson(&Bson::MaxKey, &Bson::MaxKey), Equal);
            assert_eq!(bson_utils::cmp_bson(&Bson::Null, &Bson::Null), Equal);
        }

        #[test]
        fn test_timestamp_ordering_by_increment() {
            let a = Bson::Timestamp(bson::Timestamp {
                time: 1,
                increment: 1,
            });
            let b = Bson::Timestamp(bson::Timestamp {
                time: 1,
                increment: 2,
            });
            assert_eq!(bson_utils::cmp_bson(&a, &b), Less);
        }

        #[test]
        fn test_nested_numeric_cross_type_inside_document() {
            let a = Bson::Document(doc! { "a": 5_i32 });
            let b = Bson::Document(doc! { "a": Bson::Int64(5) });
            let c = Bson::Document(doc! { "a": Bson::Int64(6) });

            assert_eq!(bson_utils::cmp_bson(&a, &b), Equal);
            assert_eq!(bson_utils::cmp_bson(&a, &c), Less);
        }

        #[test]
        fn test_nested_numeric_cross_type_inside_array() {
            let a = Bson::Array(vec![Bson::Int32(1)]);
            let b = Bson::Array(vec![Bson::Double(1.0)]);
            assert_eq!(bson_utils::cmp_bson(&a, &b), Equal);
        }
    }

    mod bson_key_cmp_agreement {
        use std::cmp::Ordering;
        use std::str::FromStr;

        use bson::{
            oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Decimal128, Timestamp,
        };

        use crate::util::bson_utils::{cmp_bson, BsonKey};

        fn assert_key_cmp_agrees(a: &Bson, b: &Bson) {
            let cmp_result = cmp_bson(a, b);
            let key_a = a.try_into_key().expect("Failed to encode lhs BSON key");
            let key_b = b.try_into_key().expect("Failed to encode rhs BSON key");
            let key_result = key_a.cmp(&key_b);

            assert_eq!(
                cmp_result, key_result,
                "cmp_bson and key ordering mismatch for a={:?}, b={:?}, key_a={:?}, key_b={:?}",
                a, b, key_a, key_b
            );
        }

        fn assert_keys_equal(a: &Bson, b: &Bson) {
            let key_a = a.try_into_key().expect("Failed to encode lhs BSON key");
            let key_b = b.try_into_key().expect("Failed to encode rhs BSON key");

            assert_eq!(
                key_a, key_b,
                "Expected equal keys for a={:?}, b={:?}; key_a={:?}, key_b={:?}",
                a, b, key_a, key_b
            );
        }

        #[test]
        fn numeric_cross_type_equal_values_have_equal_keys() {
            assert_keys_equal(&Bson::Int32(0), &Bson::Int64(0));
            assert_keys_equal(&Bson::Int32(0), &Bson::Double(0.0));
            assert_keys_equal(&Bson::Int64(0), &Bson::Double(0.0));

            assert_keys_equal(&Bson::Int32(5), &Bson::Int64(5));
            assert_keys_equal(&Bson::Int32(5), &Bson::Double(5.0));
            assert_keys_equal(&Bson::Int64(5), &Bson::Double(5.0));

            assert_keys_equal(&Bson::Int32(-42), &Bson::Int64(-42));
            assert_keys_equal(&Bson::Int32(-42), &Bson::Double(-42.0));

            let dec5 = Decimal128::from_str("5").unwrap();
            assert_keys_equal(&Bson::Int32(5), &Bson::Decimal128(dec5));

            let dec_neg42 = Decimal128::from_str("-42").unwrap();
            assert_keys_equal(&Bson::Int64(-42), &Bson::Decimal128(dec_neg42));
        }

        #[test]
        fn numeric_zero_is_canonical() {
            assert_keys_equal(&Bson::Double(-0.0), &Bson::Double(0.0));
            assert_keys_equal(&Bson::Double(0.0), &Bson::Int32(0));
            assert_keys_equal(&Bson::Double(-0.0), &Bson::Int64(0));

            let dec_zero = Decimal128::from_str("0").unwrap();
            let dec_neg_zero = Decimal128::from_str("-0").unwrap();
            assert_keys_equal(&Bson::Decimal128(dec_zero), &Bson::Decimal128(dec_neg_zero));
            assert_keys_equal(&Bson::Double(0.0), &Bson::Decimal128(dec_zero));
        }

        #[test]
        fn numeric_nan_is_canonical_and_lowest() {
            let nan = Bson::Double(f64::NAN);
            let neg_nan = Bson::Double(-f64::NAN);

            assert_keys_equal(&nan, &neg_nan);

            let samples = vec![
                Bson::Double(f64::NEG_INFINITY),
                Bson::Double(-1000.0),
                Bson::Double(-0.0),
                Bson::Double(0.0),
                Bson::Int32(0),
                Bson::Int64(i64::MAX),
                Bson::Double(f64::INFINITY),
            ];

            for sample in samples {
                assert_eq!(cmp_bson(&nan, &sample), Ordering::Less);
                assert_key_cmp_agrees(&nan, &sample);
            }
        }

        #[test]
        fn numeric_mixed_type_ordering_agrees() {
            assert_key_cmp_agrees(&Bson::Int32(-1), &Bson::Double(-0.5));
            assert_key_cmp_agrees(&Bson::Int32(3), &Bson::Double(3.5));
            assert_key_cmp_agrees(&Bson::Double(2.9), &Bson::Int32(3));

            assert_key_cmp_agrees(&Bson::Int32(5), &Bson::Int64(6));
            assert_key_cmp_agrees(&Bson::Int64(-100), &Bson::Int32(-99));

            assert_key_cmp_agrees(&Bson::Double(1e20), &Bson::Int64(i64::MAX));
            assert_key_cmp_agrees(&Bson::Int64(i64::MIN), &Bson::Double(-1e18));

            assert_key_cmp_agrees(&Bson::Double(f64::NEG_INFINITY), &Bson::Int32(i32::MIN));
            assert_key_cmp_agrees(&Bson::Int32(i32::MAX), &Bson::Double(f64::INFINITY));

            let dec = Decimal128::from_str("3.14").unwrap();
            assert_key_cmp_agrees(&Bson::Int32(3), &Bson::Decimal128(dec));
            assert_key_cmp_agrees(&Bson::Decimal128(dec), &Bson::Double(3.15));
        }

        #[test]
        fn numeric_nan_inf_and_zero_agreement() {
            let nan = Bson::Double(f64::NAN);
            let neg_nan = Bson::Double(-f64::NAN);
            let neg_inf = Bson::Double(f64::NEG_INFINITY);
            let pos_inf = Bson::Double(f64::INFINITY);
            let zeroes = vec![
                Bson::Int32(0),
                Bson::Int64(0),
                Bson::Double(0.0),
                Bson::Double(-0.0),
                Bson::Decimal128(Decimal128::from_str("0").unwrap()),
                Bson::Decimal128(Decimal128::from_str("-0").unwrap()),
            ];

            assert_key_cmp_agrees(&nan, &neg_nan);
            assert_key_cmp_agrees(&nan, &neg_inf);
            assert_key_cmp_agrees(&neg_inf, &pos_inf);

            for z in &zeroes {
                assert_key_cmp_agrees(&neg_inf, z);
                assert_key_cmp_agrees(z, &pos_inf);
            }

            for i in 0..zeroes.len() {
                for j in 0..zeroes.len() {
                    assert_key_cmp_agrees(&zeroes[i], &zeroes[j]);
                }
            }
        }

        #[test]
        fn numeric_boundary_ordering_agrees() {
            let pairs: Vec<(Bson, Bson)> = vec![
                (Bson::Int32(i32::MIN), Bson::Int32(i32::MIN + 1)),
                (Bson::Int32(i32::MAX - 1), Bson::Int32(i32::MAX)),
                (Bson::Int64(i64::MIN), Bson::Int64(i64::MIN + 1)),
                (Bson::Int64(i64::MAX - 1), Bson::Int64(i64::MAX)),
                (Bson::Int32(i32::MAX), Bson::Int64(i32::MAX as i64 + 1)),
                (Bson::Int64(i32::MIN as i64 - 1), Bson::Int32(i32::MIN)),
            ];

            for (a, b) in pairs {
                assert_key_cmp_agrees(&a, &b);
            }
        }

        #[test]
        fn numeric_cmp_and_key_agree_on_migration_gate_samples() {
            let values = vec![
                Bson::Double(f64::NAN),
                Bson::Double(f64::NEG_INFINITY),
                Bson::Decimal128(Decimal128::from_str("-9223372036854775809").unwrap()),
                Bson::Int64(i64::MIN),
                Bson::Int32(i32::MIN),
                Bson::Int32(-1),
                Bson::Double(-0.0),
                Bson::Double(0.0),
                Bson::Int32(0),
                Bson::Int64(0),
                Bson::Decimal128(Decimal128::from_str("-0").unwrap()),
                Bson::Decimal128(Decimal128::from_str("0").unwrap()),
                Bson::Double(0.5),
                Bson::Int32(1),
                Bson::Decimal128(Decimal128::from_str("1.0").unwrap()),
                Bson::Int32(i32::MAX),
                Bson::Int64(i64::MAX),
                Bson::Decimal128(Decimal128::from_str("9223372036854775808").unwrap()),
                Bson::Double(f64::INFINITY),
            ];

            for i in 0..values.len() {
                for j in 0..values.len() {
                    assert_key_cmp_agrees(&values[i], &values[j]);
                }
            }
        }

        #[test]
        fn binary_orders_by_subtype_then_bytes() {
            let generic_12 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2],
            });
            let generic_13 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 3],
            });
            let uuid_01 = Bson::Binary(Binary {
                subtype: BinarySubtype::Uuid,
                bytes: vec![0, 1],
            });

            assert_key_cmp_agrees(&generic_12, &generic_13);
            assert_key_cmp_agrees(&generic_12, &uuid_01);
        }

        #[test]
        fn type_precedence_chain_agrees() {
            use bson::doc;

            let values: Vec<Bson> = vec![
                Bson::MinKey,
                Bson::Null,
                Bson::Int32(0),
                Bson::String("".into()),
                Bson::Document(doc! {}),
                Bson::Array(vec![]),
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: vec![],
                }),
                Bson::ObjectId(ObjectId::parse_str("000000000000000000000000").unwrap()),
                Bson::Boolean(false),
                Bson::DateTime(DateTime::from_millis(0)),
                Bson::Timestamp(Timestamp {
                    time: 0,
                    increment: 0,
                }),
                Bson::MaxKey,
            ];

            for i in 0..values.len() {
                for j in (i + 1)..values.len() {
                    assert_key_cmp_agrees(&values[i], &values[j]);
                }
            }
        }

        #[test]
        fn document_ordering_by_key() {
            use bson::doc;
            let a = Bson::Document(doc! { "a": 1 });
            let b = Bson::Document(doc! { "b": 1 });
            assert_key_cmp_agrees(&a, &b);
        }

        #[test]
        fn document_ordering_by_value() {
            use bson::doc;
            let a = Bson::Document(doc! { "a": 1 });
            let b = Bson::Document(doc! { "a": 2 });
            assert_key_cmp_agrees(&a, &b);
        }

        #[test]
        fn document_prefix_ordering() {
            use bson::doc;
            let a = Bson::Document(doc! { "a": 1 });
            let b = Bson::Document(doc! { "a": 1, "b": 2 });
            assert_key_cmp_agrees(&a, &b);
        }

        #[test]
        fn document_empty_sorts_first() {
            use bson::doc;
            let empty = Bson::Document(doc! {});
            let one = Bson::Document(doc! { "a": 0 });
            assert_key_cmp_agrees(&empty, &one);
        }

        #[test]
        fn document_nested() {
            use bson::doc;
            let a = Bson::Document(doc! { "x": { "y": 1 } });
            let b = Bson::Document(doc! { "x": { "y": 2 } });
            assert_key_cmp_agrees(&a, &b);
        }

        #[test]
        fn document_equal_keys() {
            use bson::doc;
            let a = Bson::Document(doc! { "a": 5_i32 });
            let b = Bson::Document(doc! { "a": 5.0_f64 });
            assert_keys_equal(&a, &b);
        }

        #[test]
        fn array_ordering_element_by_element() {
            let a = Bson::Array(vec![Bson::Int32(1)]);
            let b = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
            let c = Bson::Array(vec![Bson::Int32(2)]);

            assert_key_cmp_agrees(&a, &b);
            assert_key_cmp_agrees(&a, &c);
            assert_key_cmp_agrees(&b, &c);
        }

        #[test]
        fn array_empty_sorts_first() {
            let empty = Bson::Array(vec![]);
            let one = Bson::Array(vec![Bson::Int32(0)]);
            assert_key_cmp_agrees(&empty, &one);
        }

        #[test]
        fn array_nested() {
            let a = Bson::Array(vec![Bson::Array(vec![Bson::Int32(1)])]);
            let b = Bson::Array(vec![Bson::Array(vec![Bson::Int32(2)])]);
            assert_key_cmp_agrees(&a, &b);
        }

        #[test]
        fn array_mixed_element_types() {
            let a = Bson::Array(vec![Bson::Int32(1), Bson::String("a".into())]);
            let b = Bson::Array(vec![Bson::Int32(1), Bson::String("b".into())]);
            assert_key_cmp_agrees(&a, &b);
        }

        #[test]
        fn array_cross_type_numeric_elements() {
            let a = Bson::Array(vec![Bson::Int32(5)]);
            let b = Bson::Array(vec![Bson::Double(5.0)]);
            assert_keys_equal(&a, &b);
        }

        #[test]
        fn decimal128_edge_ordering_agrees_with_cmp() {
            let strs = [
                "-9.999999999999999999999999999999999E6111",
                "-1000.12345",
                "-1.0",
                "-1E-6176",
                "0",
                "1E-6176",
                "1.0",
                "1000.12345",
                "9.999999999999999999999999999999999E6111",
            ];

            let values: Vec<Bson> = strs
                .iter()
                .map(|s| Bson::Decimal128(Decimal128::from_str(s).unwrap()))
                .collect();

            for i in 0..values.len() {
                for j in (i + 1)..values.len() {
                    assert_key_cmp_agrees(&values[i], &values[j]);
                }
            }
        }
    }

    mod bson_key_decode {
        use crate::util::bson_utils::{decode_bson_from_key, key_type_code, BsonKey, TypedKey};
        use bson::{
            oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Decimal128, Timestamp,
        };
        use std::io::ErrorKind;
        use std::str::FromStr;

        fn round_trip(value: &Bson) -> Bson {
            let key = value.try_into_typed_key().expect("encode failed");
            decode_bson_from_key(&key).expect("decode failed")
        }

        #[test]
        fn min_max_null() {
            assert_eq!(round_trip(&Bson::MinKey), Bson::MinKey);
            assert_eq!(round_trip(&Bson::MaxKey), Bson::MaxKey);
            assert_eq!(round_trip(&Bson::Null), Bson::Null);
        }

        #[test]
        fn boolean() {
            assert_eq!(round_trip(&Bson::Boolean(false)), Bson::Boolean(false));
            assert_eq!(round_trip(&Bson::Boolean(true)), Bson::Boolean(true));
        }

        #[test]
        fn string() {
            let s = Bson::String("hello world".into());
            assert_eq!(round_trip(&s), s);
            let empty = Bson::String("".into());
            assert_eq!(round_trip(&empty), empty);
        }

        #[test]
        fn object_id() {
            let oid = ObjectId::parse_str("0102030405060708090a0b0c").unwrap();
            let v = Bson::ObjectId(oid);
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn datetime() {
            let dt = Bson::DateTime(DateTime::from_millis(1_700_000_000_000));
            assert_eq!(round_trip(&dt), dt);
        }

        #[test]
        fn datetime_pre_epoch() {
            let dt = Bson::DateTime(DateTime::from_millis(-86_400_000));
            assert_eq!(round_trip(&dt), dt);
        }

        #[test]
        fn timestamp() {
            let ts = Bson::Timestamp(Timestamp {
                time: 12345,
                increment: 67890,
            });
            assert_eq!(round_trip(&ts), ts);
        }

        #[test]
        fn binary() {
            let bin = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2, 3, 4],
            });
            assert_eq!(round_trip(&bin), bin);
        }

        #[test]
        fn binary_with_null_bytes() {
            let bin = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x00, 0x01, 0x00, 0xFF, 0x00],
            });
            assert_eq!(round_trip(&bin), bin);
        }

        #[test]
        fn binary_with_only_null_bytes() {
            let bin = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x00, 0x00, 0x00],
            });
            assert_eq!(round_trip(&bin), bin);
        }

        #[test]
        fn binary_empty_payload_round_trip_and_encode() {
            let bin = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![],
            });

            let encoded = bin.try_into_key();
            assert!(
                encoded.is_ok(),
                "empty binary payload should encode without error"
            );
            assert_eq!(round_trip(&bin), bin);
        }

        #[test]
        fn binary_with_only_0x01_bytes_round_trip() {
            let bin = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x01, 0x01, 0x01],
            });
            assert_eq!(round_trip(&bin), bin);
        }

        #[test]
        fn binary_boundary_escape_sequence_at_end_round_trip() {
            let bin = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x00, 0x01],
            });
            assert_eq!(round_trip(&bin), bin);
        }

        #[test]
        fn binary_ordering_with_null_bytes_preserved() {
            use crate::util::bson_utils::BsonKey;
            let b0 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x00],
            });
            let b1 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x01],
            });
            let b2 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x02],
            });
            let b3 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0xFF],
            });

            let k0 = b0.try_into_key().unwrap();
            let k1 = b1.try_into_key().unwrap();
            let k2 = b2.try_into_key().unwrap();
            let k3 = b3.try_into_key().unwrap();

            assert!(k0 < k1, "encoded [0x00] should sort before [0x01]");
            assert!(k1 < k2, "encoded [0x01] should sort before [0x02]");
            assert!(k2 < k3, "encoded [0x02] should sort before [0xFF]");
        }

        #[test]
        fn binary_inside_document_round_trip() {
            use bson::doc;
            // Binary is non-final: followed by a string field.
            let bin = Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x00, 0x01, 0xFF],
            };
            let v = Bson::Document(doc! {
                "data": Bson::Binary(bin),
                "tag":  "hello"
            });
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn int32_positive() {
            assert_eq!(round_trip(&Bson::Int32(42)), Bson::Int32(42));
        }

        #[test]
        fn int32_negative() {
            assert_eq!(round_trip(&Bson::Int32(-999)), Bson::Int32(-999));
        }

        #[test]
        fn int32_zero() {
            assert_eq!(round_trip(&Bson::Int32(0)), Bson::Int32(0));
        }

        #[test]
        fn int64_boundaries() {
            for n in [0_i64, 1, -1, i64::MIN, i64::MAX, 1_000_000_000_000] {
                assert_eq!(
                    round_trip(&Bson::Int64(n)),
                    Bson::Int64(n),
                    "failed for n={n}"
                );
            }
        }

        #[test]
        fn double_finite() {
            for f in [1.0_f64, -1.0, 3.14, -2.718, 1e100, -1e-10] {
                assert_eq!(
                    round_trip(&Bson::Double(f)),
                    Bson::Double(f),
                    "failed for f={f}"
                );
            }
        }

        #[test]
        fn double_pos_zero() {
            assert_eq!(round_trip(&Bson::Double(0.0)), Bson::Double(0.0));
        }

        #[test]
        fn double_neg_zero() {
            let result = round_trip(&Bson::Double(-0.0));
            if let Bson::Double(f) = result {
                assert!(f.is_sign_negative(), "expected -0.0 but got {f}");
            } else {
                panic!("expected Double, got {:?}", result);
            }
        }

        #[test]
        fn double_pos_infinity() {
            assert_eq!(
                round_trip(&Bson::Double(f64::INFINITY)),
                Bson::Double(f64::INFINITY)
            );
        }

        #[test]
        fn double_neg_infinity() {
            assert_eq!(
                round_trip(&Bson::Double(f64::NEG_INFINITY)),
                Bson::Double(f64::NEG_INFINITY)
            );
        }

        #[test]
        fn double_nan_positive() {
            let result = round_trip(&Bson::Double(f64::NAN));
            if let Bson::Double(f) = result {
                assert!(f.is_nan() && !f.is_sign_negative());
            } else {
                panic!("expected Double NaN");
            }
        }

        #[test]
        fn double_nan_negative() {
            let result = round_trip(&Bson::Double(-f64::NAN));
            if let Bson::Double(f) = result {
                assert!(f.is_nan() && f.is_sign_negative());
            } else {
                panic!("expected Double -NaN");
            }
        }

        #[test]
        fn decimal128_values() {
            for s in [
                "0",
                "-0",
                "1.5",
                "-42",
                "9.999999999999999999999999999999999E6111",
                "-9.999999999999999999999999999999999E6111",
                "1E-6176",
            ] {
                let d = Decimal128::from_str(s).unwrap();
                let v = Bson::Decimal128(d);
                let key = v.try_into_typed_key().expect("encode");
                let decoded = decode_bson_from_key(&key).expect("decode");
                let re_key = decoded.try_into_typed_key().expect("re-encode");
                assert_eq!(key, re_key, "key round-trip failed for {s}");
            }
        }

        #[test]
        fn decimal128_preserves_scale_and_zero_sign() {
            for s in ["12.50", "12.5", "0.00", "-0.00", "1E+3", "1000"] {
                let original = Bson::Decimal128(Decimal128::from_str(s).unwrap());
                let decoded = round_trip(&original);

                match (original, decoded) {
                    (Bson::Decimal128(expected), Bson::Decimal128(actual)) => {
                        assert_eq!(expected.to_string(), actual.to_string(), "failed for {s}");
                    }
                    _ => panic!("expected Decimal128"),
                }
            }
        }

        #[test]
        fn decimal128_nan_positive() {
            let original = Bson::Decimal128(Decimal128::from_str("NaN").unwrap());
            let decoded = round_trip(&original);

            match decoded {
                Bson::Decimal128(actual) => {
                    assert_eq!(actual.bytes(), Decimal128::from_str("NaN").unwrap().bytes());
                }
                _ => panic!("expected Decimal128 NaN"),
            }
        }

        #[test]
        fn decimal128_nan_negative() {
            let original = Bson::Decimal128(Decimal128::from_str("-NaN").unwrap());
            let decoded = round_trip(&original);

            match decoded {
                Bson::Decimal128(actual) => {
                    assert_eq!(
                        actual.bytes(),
                        Decimal128::from_str("-NaN").unwrap().bytes()
                    );
                }
                _ => panic!("expected Decimal128 -NaN"),
            }
        }

        #[test]
        fn decode_returns_error_for_truncated_string_key() {
            let key = TypedKey {
                key: vec![0x30, b'a'],
                key_type: vec![key_type_code::STRING],
            };

            let err = decode_bson_from_key(&key).unwrap_err();

            assert_eq!(err.kind(), ErrorKind::InvalidData);
        }

        #[test]
        fn decode_returns_error_for_mismatched_type_code() {
            let key = TypedKey {
                key: vec![0x30, b'a', 0x00],
                key_type: vec![key_type_code::INT32],
            };

            let err = decode_bson_from_key(&key).unwrap_err();

            assert_eq!(err.kind(), ErrorKind::InvalidData);
        }

        #[test]
        fn document_round_trip() {
            use bson::doc;
            let v = Bson::Document(doc! { "x": 1_i32, "y": "hello" });
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn array_round_trip() {
            let v = Bson::Array(vec![
                Bson::Int32(1),
                Bson::String("a".into()),
                Bson::Boolean(true),
            ]);
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn nested_document_round_trip() {
            use bson::doc;
            let v = Bson::Document(doc! { "outer": { "inner": 42_i32 } });
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn document_with_mixed_numeric_types() {
            use bson::doc;
            // Ensure Int32 and Int64 fields inside a document survive round-trip
            // (they would be indistinguishable without KeyType)
            let v = Bson::Document(doc! {
                "a": 1_i32,
                "b": Bson::Int64(2),
                "c": 3.0_f64,
                "d": Bson::Boolean(true)
            });
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn array_with_mixed_numeric_types() {
            let v = Bson::Array(vec![
                Bson::Int32(1),
                Bson::Int64(2),
                Bson::Double(3.0),
                Bson::String("x".into()),
            ]);
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn nested_array_in_document() {
            use bson::doc;
            let v = Bson::Document(doc! {
                "arr": Bson::Array(vec![Bson::Int32(10), Bson::Int64(20)])
            });
            assert_eq!(round_trip(&v), v);
        }

        #[test]
        fn key_type_carries_correct_element_types() {
            let v = Bson::Array(vec![Bson::Int32(1), Bson::Int64(2), Bson::Double(3.0)]);
            let TypedKey { key_type, .. } = v.try_into_typed_key().unwrap();
            assert_eq!(key_type[0], key_type_code::ARRAY);
            assert_eq!(key_type[1], key_type_code::INT32);
            assert_eq!(key_type[2], key_type_code::INT64);
            assert_eq!(key_type[3], key_type_code::POS_DOUBLE);
        }

        #[test]
        fn key_type_double_neg_zero_is_negative() {
            let TypedKey { key_type, .. } = Bson::Double(-0.0).try_into_typed_key().unwrap();
            assert_eq!(key_type[0], key_type_code::NEG_DOUBLE);
        }

        #[test]
        fn key_type_double_pos_zero_is_not_negative() {
            let TypedKey { key_type, .. } = Bson::Double(0.0).try_into_typed_key().unwrap();
            assert_eq!(key_type[0], key_type_code::POS_DOUBLE);
        }

        #[test]
        fn key_type_double_neg_nan_is_negative() {
            let TypedKey { key_type, .. } = Bson::Double(-f64::NAN).try_into_typed_key().unwrap();
            assert_eq!(key_type[0], key_type_code::NEG_DOUBLE);
        }

        #[test]
        fn key_type_document_layout_for_simple_fields() {
            use bson::doc;
            let v = Bson::Document(doc! { "a": 1_i32, "b": "hello" });
            let TypedKey { key_type, .. } = v.try_into_typed_key().unwrap();

            assert_eq!(key_type[0], key_type_code::DOCUMENT);
            assert_eq!(key_type[1], key_type_code::INT32);
            assert_eq!(key_type[2], key_type_code::STRING);
        }

        #[test]
        fn key_type_document_layout_for_nested_array() {
            use bson::doc;
            let v = Bson::Document(doc! {
                "x": Bson::Array(vec![Bson::Int32(1), Bson::Int64(2)])
            });
            let TypedKey { key_type, .. } = v.try_into_typed_key().unwrap();

            assert_eq!(key_type[0], key_type_code::DOCUMENT);
            assert_eq!(key_type[1], key_type_code::ARRAY);
            assert_eq!(key_type[2], key_type_code::INT32);
            assert_eq!(key_type[3], key_type_code::INT64);
        }
    }

    mod bson_eq {
        use crate::util::bson_utils::bson_eq;
        use bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime};
        use bson::raw::CString;

        #[test]
        fn nan_equality() {
            assert!(bson_eq(&Bson::Double(f64::NAN), &Bson::Double(f64::NAN)));
            assert!(bson_eq(&Bson::Double(f64::NAN), &Bson::Double(-f64::NAN)));
        }

        #[test]
        fn cross_type_numeric_equality() {
            assert!(bson_eq(&Bson::Int32(5), &Bson::Int64(5)));
            assert!(bson_eq(&Bson::Int64(5), &Bson::Double(5.0)));
            assert!(bson_eq(&Bson::Int32(5), &Bson::Double(5.0)));
            assert!(bson_eq(&Bson::Int32(0), &Bson::Double(-0.0)));
            assert!(bson_eq(&Bson::Int64(-42), &Bson::Double(-42.0)));
        }

        #[test]
        fn cross_type_numeric_inequality() {
            assert!(!bson_eq(&Bson::Int32(5), &Bson::Double(5.1)));
            assert!(!bson_eq(&Bson::Int32(1), &Bson::Int64(2)));
        }

        #[test]
        fn document_equality_is_order_insensitive() {
            let a = Bson::Document(doc! { "a": 1, "b": 2 });
            let b = Bson::Document(doc! { "b": 2, "a": 1 });
            assert!(bson_eq(&a, &b));
        }

        #[test]
        fn document_inequality() {
            let base = Bson::Document(doc! { "a": 1, "b": 2 });
            let diff_value = Bson::Document(doc! { "a": 1, "b": 3 });
            let extra_key = Bson::Document(doc! { "a": 1, "b": 2, "c": 3 });

            assert!(!bson_eq(&base, &diff_value));
            assert!(!bson_eq(&base, &extra_key));
        }

        #[test]
        fn array_equality_element_wise_using_bson_eq() {
            let a = Bson::Array(vec![Bson::Int32(1)]);
            let b = Bson::Array(vec![Bson::Double(1.0)]);
            assert!(bson_eq(&a, &b));
        }

        #[test]
        fn array_inequality() {
            let shorter = Bson::Array(vec![Bson::Int32(1)]);
            let longer = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
            let diff_elem = Bson::Array(vec![Bson::Int32(1)]);
            let diff_elem_other = Bson::Array(vec![Bson::Int32(2)]);

            assert!(!bson_eq(&shorter, &longer));
            assert!(!bson_eq(&diff_elem, &diff_elem_other));
        }

        #[test]
        fn regular_expression_equality_and_inequality() {
            let a = Bson::RegularExpression(bson::Regex {
                pattern: CString::try_from("abc").unwrap(),
                options: CString::try_from("im").unwrap(),
            });
            let b = Bson::RegularExpression(bson::Regex {
                pattern: CString::try_from("abc").unwrap(),
                options: CString::try_from("im").unwrap(),
            });
            let c = Bson::RegularExpression(bson::Regex {
                pattern: CString::try_from("abc").unwrap(),
                options: CString::try_from("i").unwrap(),
            });

            assert!(bson_eq(&a, &b));
            assert!(!bson_eq(&a, &c));
        }

        #[test]
        fn fallback_other_types_positive_and_negative_cases() {
            assert!(bson_eq(
                &Bson::String("x".into()),
                &Bson::String("x".into())
            ));
            assert!(!bson_eq(
                &Bson::String("x".into()),
                &Bson::String("y".into())
            ));

            assert!(bson_eq(&Bson::Boolean(true), &Bson::Boolean(true)));
            assert!(!bson_eq(&Bson::Boolean(true), &Bson::Boolean(false)));

            let oid1 = ObjectId::parse_str("000000000000000000000001").unwrap();
            let oid2 = ObjectId::parse_str("000000000000000000000002").unwrap();
            assert!(bson_eq(&Bson::ObjectId(oid1), &Bson::ObjectId(oid1)));
            assert!(!bson_eq(&Bson::ObjectId(oid1), &Bson::ObjectId(oid2)));

            let dt1 = DateTime::from_millis(1_000);
            let dt2 = DateTime::from_millis(2_000);
            assert!(bson_eq(&Bson::DateTime(dt1), &Bson::DateTime(dt1)));
            assert!(!bson_eq(&Bson::DateTime(dt1), &Bson::DateTime(dt2)));

            let bin1 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2, 3],
            });
            let bin2 = Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![1, 2, 4],
            });
            assert!(bson_eq(&bin1, &bin1));
            assert!(!bson_eq(&bin1, &bin2));
        }
    }

    mod bson_hash {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        use bson::{doc, Bson};
        use bson::raw::CString;
        use crate::util::bson_utils::{bson_eq, bson_hash};

        fn hash(value: &Bson) -> u64 {
            let mut hasher = DefaultHasher::new();
            bson_hash(value, &mut hasher);
            hasher.finish()
        }

        fn assert_eq_implies_hash_eq(a: &Bson, b: &Bson) {
            assert!(bson_eq(a, b), "expected bson_eq(a, b) for a={a:?}, b={b:?}");
            assert_eq!(
                hash(a),
                hash(b),
                "expected hash(a) == hash(b) for bson_eq pair a={a:?}, b={b:?}"
            );
        }

        #[test]
        fn nan_forms_hash_equal() {
            assert_eq_implies_hash_eq(&Bson::Double(f64::NAN), &Bson::Double(-f64::NAN));
        }

        #[test]
        fn cross_type_zeros_hash_equal() {
            let values = [
                Bson::Int32(0),
                Bson::Int64(0),
                Bson::Double(0.0),
                Bson::Double(-0.0),
            ];

            for i in 0..values.len() {
                for j in 0..values.len() {
                    assert_eq_implies_hash_eq(&values[i], &values[j]);
                }
            }
        }

        #[test]
        fn cross_type_equal_integers_hash_equal() {
            let values = [Bson::Int32(5), Bson::Int64(5), Bson::Double(5.0)];

            for i in 0..values.len() {
                for j in 0..values.len() {
                    assert_eq_implies_hash_eq(&values[i], &values[j]);
                }
            }
        }

        #[test]
        fn negative_equal_integers_hash_equal() {
            let values = [Bson::Int32(-42), Bson::Int64(-42), Bson::Double(-42.0)];

            for i in 0..values.len() {
                for j in 0..values.len() {
                    assert_eq_implies_hash_eq(&values[i], &values[j]);
                }
            }
        }

        #[test]
        fn document_equal_with_different_insertion_order_hash_equal() {
            let a = Bson::Document(doc! { "a": 1, "b": 2 });
            let b = Bson::Document(doc! { "b": 2, "a": 1 });
            assert_eq_implies_hash_eq(&a, &b);
        }

        #[test]
        fn arrays_with_numerically_equal_elements_hash_equal() {
            let a = Bson::Array(vec![Bson::Int32(5), Bson::Int64(0), Bson::Double(-42.0)]);
            let b = Bson::Array(vec![
                Bson::Double(5.0),
                Bson::Double(-0.0),
                Bson::Int32(-42),
            ]);
            assert_eq_implies_hash_eq(&a, &b);
        }

        #[test]
        fn regex_equal_pattern_and_options_hash_equal() {
            let a = Bson::RegularExpression(bson::Regex {
                pattern: CString::try_from("abc").unwrap(),
                options: CString::try_from("im").unwrap(),
            });
            let b = Bson::RegularExpression(bson::Regex {
                pattern: CString::try_from("abc").unwrap(),
                options: CString::try_from("im").unwrap(),
            });
            assert_eq_implies_hash_eq(&a, &b);
        }
    }

    mod arithmetic {
        use crate::util::bson_utils::{
            add_numeric, multiply_numeric, perform_bitwise_op, BsonArithmeticError,
        };
        use bson::Bson;

        #[test]
        fn test_add_numeric() {
            assert_eq!(
                add_numeric(Some(&Bson::Int32(5)), &Bson::Int32(10)).unwrap(),
                Bson::Int32(15)
            );
            assert_eq!(
                add_numeric(Some(&Bson::Int64(100)), &Bson::Int32(-20)).unwrap(),
                Bson::Int64(80)
            );
            assert_eq!(
                add_numeric(Some(&Bson::Double(3.5)), &Bson::Int32(2)).unwrap(),
                Bson::Double(5.5)
            );

            assert_eq!(
                add_numeric(Some(&Bson::Int32(i32::MAX)), &Bson::Int32(1)).unwrap(),
                Bson::Int64(i32::MAX as i64 + 1)
            );
            assert_eq!(
                add_numeric(Some(&Bson::Int32(10)), &Bson::Double(2.5)).unwrap(),
                Bson::Double(12.5)
            );

            assert_eq!(
                add_numeric(None, &Bson::Int64(42)).unwrap(),
                Bson::Int64(42)
            );

            assert_eq!(
                add_numeric(Some(&Bson::Int64(i64::MAX)), &Bson::Int64(1)),
                Err(BsonArithmeticError::Overflow)
            );
            assert_eq!(
                add_numeric(Some(&Bson::Double(1.5)), &Bson::Double(2.5)).unwrap(),
                Bson::Double(4.0)
            );
            assert_eq!(
                add_numeric(Some(&Bson::Int32(2)), &Bson::Double(1.5)).unwrap(),
                Bson::Double(3.5)
            );

            assert_eq!(
                add_numeric(Some(&Bson::String("text".into())), &Bson::Int32(1)),
                Err(BsonArithmeticError::LhsNotNumeric)
            );
            assert_eq!(
                add_numeric(Some(&Bson::Int32(1)), &Bson::String("text".into())),
                Err(BsonArithmeticError::RhsNotNumeric)
            );
        }

        #[test]
        fn test_multiply_numeric() {
            assert_eq!(
                multiply_numeric(Some(&Bson::Int32(5)), &Bson::Int32(10)).unwrap(),
                Bson::Int32(50)
            );
            assert_eq!(
                multiply_numeric(Some(&Bson::Int64(10)), &Bson::Double(2.5)).unwrap(),
                Bson::Double(25.0)
            );

            assert_eq!(
                multiply_numeric(Some(&Bson::Int32(i32::MAX)), &Bson::Int32(2)).unwrap(),
                Bson::Int64(i32::MAX as i64 * 2)
            );

            assert_eq!(
                multiply_numeric(None, &Bson::Int32(100)).unwrap(),
                Bson::Int32(0)
            );
            assert_eq!(
                multiply_numeric(None, &Bson::Int64(100)).unwrap(),
                Bson::Int64(0)
            );
            assert_eq!(
                multiply_numeric(None, &Bson::Double(100.0)).unwrap(),
                Bson::Double(0.0)
            );

            assert_eq!(
                multiply_numeric(Some(&Bson::Int64(i64::MAX)), &Bson::Int64(2)),
                Err(BsonArithmeticError::Overflow)
            );
            assert_eq!(
                multiply_numeric(Some(&Bson::Double(2.0)), &Bson::Double(3.0)).unwrap(),
                Bson::Double(6.0)
            );
            assert_eq!(
                multiply_numeric(Some(&Bson::Int64(3)), &Bson::Double(2.5)).unwrap(),
                Bson::Double(7.5)
            );

            assert_eq!(
                multiply_numeric(Some(&Bson::String("text".into())), &Bson::Int32(1)),
                Err(BsonArithmeticError::LhsNotNumeric)
            );
            assert_eq!(
                multiply_numeric(Some(&Bson::Int32(1)), &Bson::String("text".into())),
                Err(BsonArithmeticError::RhsNotNumeric)
            );
        }

        #[test]
        fn test_perform_bitwise_op() {
            assert_eq!(
                perform_bitwise_op(Some(&Bson::Int32(0b1100)), Some(0b1010), None, None).unwrap(),
                Bson::Int32(0b1000)
            );

            assert_eq!(
                perform_bitwise_op(Some(&Bson::Int32(0b1000)), None, Some(0b0011), None).unwrap(),
                Bson::Int32(0b1011)
            );

            assert_eq!(
                perform_bitwise_op(Some(&Bson::Int32(0b1011)), None, None, Some(0b1100)).unwrap(),
                Bson::Int32(0b0111)
            );

            assert_eq!(
                perform_bitwise_op(
                    Some(&Bson::Int32(0b11110000)),
                    Some(0b11001100),
                    Some(0b00000011),
                    Some(0b10101010),
                )
                .unwrap(),
                Bson::Int32(((0b11110000 & 0b11001100) | 0b00000011) ^ 0b10101010)
            );

            assert_eq!(
                perform_bitwise_op(None, None, Some(0b1010), None).unwrap(),
                Bson::Int32(0b1010)
            );

            let large_num = i32::MAX as i64 + 10;
            assert_eq!(
                perform_bitwise_op(Some(&Bson::Int64(large_num)), Some(1), None, None).unwrap(),
                Bson::Int64(large_num & 1)
            );
            assert_eq!(
                perform_bitwise_op(
                    Some(&Bson::Int32(i32::MAX - 1)),
                    Some(i32::MAX as i64),
                    None,
                    None,
                )
                .unwrap(),
                Bson::Int32(i32::MAX - 1)
            );

            assert_eq!(
                perform_bitwise_op(None, None, None, None).unwrap(),
                Bson::Int64(0)
            );
            assert_eq!(
                perform_bitwise_op(Some(&Bson::Int64(5)), None, None, Some(3)).unwrap(),
                Bson::Int64(6)
            );
            assert_eq!(
                perform_bitwise_op(Some(&Bson::Int32(-1)), Some(0xFF), None, None).unwrap(),
                Bson::Int32(0xFF)
            );

            assert_eq!(
                perform_bitwise_op(Some(&Bson::Double(1.0)), Some(1), None, None),
                Err(BsonArithmeticError::LhsNotInteger)
            );
        }
    }
}
