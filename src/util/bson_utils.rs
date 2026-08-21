use crate::io::byte_reader::ByteReader;
use crate::io::{invalid_data, unexpected_eof, varint, ZeroCopy};
use bson::spec::BinarySubtype;
use bson::{serialize_to_vec, Bson, Document};
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

pub(crate) fn estimate_document_heap_size(doc: &Document) -> usize {
    doc.len() * size_of::<(String, Bson)>()
        + doc
            .iter()
            .map(|(key, value)| key.capacity() + estimate_bson_heap_size(value))
            .sum::<usize>()
}

pub(crate) fn estimate_bson_heap_size(value: &Bson) -> usize {
    match value {
        Bson::Double(_)
        | Bson::Boolean(_)
        | Bson::Null
        | Bson::Int32(_)
        | Bson::Int64(_)
        | Bson::Timestamp(_)
        | Bson::ObjectId(_)
        | Bson::DateTime(_)
        | Bson::Decimal128(_)
        | Bson::Undefined
        | Bson::MaxKey
        | Bson::MinKey => 0,
        Bson::String(s) => s.capacity(),

        Bson::Array(values) => {
            values.capacity() * size_of::<Bson>()
                + values.iter().map(estimate_bson_heap_size).sum::<usize>()
        }

        Bson::Document(doc) => estimate_document_heap_size(doc),

        Bson::RegularExpression(regex) => regex.pattern.len() + regex.options.len(),

        Bson::Binary(binary) => binary.bytes.capacity(),

        _ => unreachable!("Unsupported BSON type for heap size estimation"),
    }
}

#[cfg(test)]
mod tests;
