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
    use bson::raw::CString;
    use bson::{oid::ObjectId, Binary, Bson, DateTime, Decimal128, Timestamp};
    use std::str::FromStr;

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
        let max_value = Decimal128::from_str("9.999999999999999999999999999999999E6111").unwrap();
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

    use bson::{oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Decimal128, Timestamp};

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
    use bson::{oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime, Decimal128, Timestamp};
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
    use bson::raw::CString;
    use bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Bson, DateTime};

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

    use crate::util::bson_utils::{bson_eq, bson_hash};
    use bson::raw::CString;
    use bson::{doc, Bson};

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
