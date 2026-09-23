#[test]
fn typed_field_capabilities_reject_type_mismatches_and_non_numeric_increment() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/typed_api/serde_alias_document.rs");
    tests.compile_fail("tests/ui/typed_api/serde_alias_type.rs");
    tests.compile_fail("tests/ui/typed_api/array_element.rs");
    tests.compile_fail("tests/ui/typed_api/serde_flatten.rs");
    tests.compile_fail("tests/ui/typed_api/mismatched_scalar.rs");
    tests.compile_fail("tests/ui/typed_api/required_field_presence.rs");
    tests.compile_fail("tests/ui/typed_api/serde_skip.rs");
    tests.compile_fail("tests/ui/typed_api/serde_skip_deserializing.rs");
    tests.compile_fail("tests/ui/typed_api/serde_skip_serializing.rs");
    tests.compile_fail("tests/ui/typed_api/string_increment.rs");
    tests.compile_fail("tests/ui/typed_api/mixed_projection_modes.rs");
    tests.compile_fail("tests/ui/typed_api/serde_with.rs");
}
