use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{ToTokens, format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DeriveInput, Error, Fields, GenericArgument, LitStr, Meta, PathArguments,
    Token, Type, parse_macro_input,
};

#[proc_macro_derive(QuokkaDocument, attributes(quokka))]
pub fn derive_quokka_document(input: TokenStream) -> TokenStream {
    match derive_quokka_document_impl(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derives typed field metadata for an embedded Serde struct.
#[proc_macro_derive(QuokkaType, attributes(quokka))]
pub fn derive_quokka_type(input: TokenStream) -> TokenStream {
    match derive_quokka_type_impl(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_quokka_document_impl(input: DeriveInput) -> Result<proc_macro2::TokenStream, Error> {
    let visibility = input.vis.clone();
    let container_attributes = parse_container_attributes(&input.attrs, DeriveKind::Document)?;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            input.generics,
            "QuokkaDocument derive does not support generic types yet",
        ));
    }

    let struct_name = input.ident;
    let fields_name = format_ident!("{}Fields", struct_name);

    let named_fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(Error::new_spanned(
                    fields,
                    "QuokkaDocument derive requires a struct with named fields",
                ));
            }
            Fields::Unit => {
                return Err(Error::new(
                    Span::call_site(),
                    "QuokkaDocument derive requires at least one named field",
                ));
            }
        },
        Data::Enum(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaDocument derive only supports structs, not enums",
            ));
        }
        Data::Union(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaDocument derive only supports structs, not unions",
            ));
        }
    };

    let mut generated_fields = Vec::new();
    let mut field_initializers = Vec::new();
    let mut id_field_ident = None;
    let mut id_ty = None;

    for field in named_fields {
        let field_ident = field
            .ident
            .clone()
            .ok_or_else(|| Error::new_spanned(&field, "expected named field"))?;
        let field_attributes =
            parse_field_attributes(&field, &container_attributes, DeriveKind::Document)?;
        let is_id = parse_quokka_id(&field)?;
        if is_id && option_inner_type(&field.ty).is_some() {
            return Err(Error::new_spanned(
                &field.ty,
                "QuokkaDocument ID fields cannot use Option; use a concrete ID type",
            ));
        }
        let field_ty = field.ty.clone();
        if is_id {
            if id_field_ident.is_some() {
                return Err(Error::new_spanned(
                    &field,
                    "QuokkaDocument derive supports exactly one #[quokka(id)] field",
                ));
            }
            id_field_ident = Some(field_ident.clone());
            id_ty = Some(field.ty.clone());
        }
        if is_id && field_attributes.omittable {
            return Err(Error::new_spanned(
                &field,
                "QuokkaDocument ID fields cannot use Serde field-skipping attributes",
            ));
        }

        if field_attributes.skip {
            continue;
        }

        let stored_name = field_attributes.stored_name;

        generated_fields.push(quote! {
            pub #field_ident: <#field_ty as ::quokkadb::QueryFieldType>::Field<D>
        });
        field_initializers.push(quote! {
            #field_ident: <#field_ty as ::quokkadb::QueryFieldType>::field(path.clone().field(#stored_name))
        });
    }

    let id_field_ident = id_field_ident.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "QuokkaDocument derive requires exactly one #[quokka(id)] field",
        )
    })?;
    let id_ty = id_ty.expect("id field type should exist when id field is present");

    let tokens = quote! {
        #visibility struct #fields_name<D> {
            #( #generated_fields, )*
        }

        impl ::quokkadb::QuokkaType for #struct_name {
            type Fields<D> = #fields_name<D>;

            fn fields<D>(path: ::quokkadb::TypedPath) -> Self::Fields<D> {
                #fields_name { #( #field_initializers, )* }
            }
        }

        impl ::quokkadb::QuokkaDocument for #struct_name {
            type Id = #id_ty;

            fn id(&self) -> &Self::Id {
                &self.#id_field_ident
            }
        }
    };
    //    eprintln!("{}", quote! { #tokens });
    Ok(tokens)
}

fn derive_quokka_type_impl(input: DeriveInput) -> Result<proc_macro2::TokenStream, Error> {
    let visibility = input.vis.clone();
    let container_attributes = parse_container_attributes(&input.attrs, DeriveKind::Type)?;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            input.generics,
            "QuokkaType derive does not support generic types yet",
        ));
    }

    let struct_name = input.ident;
    let fields_name = format_ident!("{}Fields", struct_name);

    let named_fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            Fields::Unnamed(fields) => {
                return Err(Error::new_spanned(
                    fields,
                    "QuokkaType derive requires a struct with named fields",
                ));
            }
            Fields::Unit => {
                return Err(Error::new(
                    Span::call_site(),
                    "QuokkaType derive requires at least one named field",
                ));
            }
        },
        Data::Enum(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaType derive only supports structs, not enums",
            ));
        }
        Data::Union(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaType derive only supports structs, not unions",
            ));
        }
    };

    let mut generated_fields = Vec::new();
    let mut field_initializers = Vec::new();

    for field in named_fields {
        let field_ident = field
            .ident
            .clone()
            .ok_or_else(|| Error::new_spanned(&field, "expected named field"))?;
        if parse_quokka_id(&field)? {
            return Err(Error::new_spanned(
                &field,
                "QuokkaType fields cannot use #[quokka(id)]",
            ));
        }
        let field_attributes =
            parse_field_attributes(&field, &container_attributes, DeriveKind::Type)?;
        if field_attributes.skip {
            continue;
        }
        let stored_name = field_attributes.stored_name;
        let field_ty = field.ty.clone();

        generated_fields.push(quote! {
            pub #field_ident: <#field_ty as ::quokkadb::QueryFieldType>::Field<D>
        });
        field_initializers.push(quote! {
            #field_ident: <#field_ty as ::quokkadb::QueryFieldType>::field(path.clone().field(#stored_name))
        });
    }

    Ok(quote! {
        #visibility struct #fields_name<D> {
            #( #generated_fields, )*
        }

        impl ::quokkadb::QuokkaType for #struct_name {
            type Fields<D> = #fields_name<D>;

            fn fields<D>(path: ::quokkadb::TypedPath) -> Self::Fields<D> {
                #fields_name { #( #field_initializers, )* }
            }
        }

        impl ::quokkadb::QueryFieldType for #struct_name {
            type Field<D> = ::quokkadb::ObjectField<D, Self>;
            fn field<D>(path: ::quokkadb::TypedPath) -> Self::Field<D> {
                ::quokkadb::ObjectField::from_path(path)
            }
        }
    })
}

#[derive(Clone, Copy)]
enum DeriveKind {
    Document,
    Type,
}

impl DeriveKind {
    fn name(self) -> &'static str {
        match self {
            Self::Document => "QuokkaDocument",
            Self::Type => "QuokkaType",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenameRule {
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
}

impl RenameRule {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "lowercase" => Ok(Self::LowerCase),
            "UPPERCASE" => Ok(Self::UpperCase),
            "PascalCase" => Ok(Self::PascalCase),
            "camelCase" => Ok(Self::CamelCase),
            "snake_case" => Ok(Self::SnakeCase),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnakeCase),
            "kebab-case" => Ok(Self::KebabCase),
            "SCREAMING-KEBAB-CASE" => Ok(Self::ScreamingKebabCase),
            _ => Err(format!(
                "unknown rename rule `rename_all = {value:?}`, expected one of lowercase, UPPERCASE, PascalCase, camelCase, snake_case, SCREAMING_SNAKE_CASE, kebab-case, SCREAMING-KEBAB-CASE"
            )),
        }
    }

    fn apply_to_field(self, field: &str) -> String {
        match self {
            Self::LowerCase => field.to_owned(),
            Self::UpperCase => field.to_ascii_uppercase(),
            Self::PascalCase => {
                let mut pascal = String::new();
                let mut capitalize = true;
                for ch in field.chars() {
                    if ch == '_' {
                        capitalize = true;
                    } else if capitalize {
                        pascal.push(ch.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        pascal.push(ch);
                    }
                }
                pascal
            }
            Self::CamelCase => {
                let pascal = Self::PascalCase.apply_to_field(field);
                let mut chars = pascal.chars();
                match chars.next() {
                    Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
                    None => pascal,
                }
            }
            Self::SnakeCase => field.to_owned(),
            Self::ScreamingSnakeCase => field.to_ascii_uppercase(),
            Self::KebabCase => field.replace('_', "-"),
            Self::ScreamingKebabCase => field.to_ascii_uppercase().replace('_', "-"),
        }
    }
}

#[derive(Default)]
struct ContainerAttributes {
    rename_all: Option<RenameRule>,
}

struct FieldAttributes {
    stored_name: String,
    skip: bool,
    omittable: bool,
}

fn parse_container_attributes(
    attrs: &[Attribute],
    derive_kind: DeriveKind,
) -> Result<ContainerAttributes, Error> {
    let mut parsed = ContainerAttributes::default();

    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                if meta.input.peek(syn::token::Paren) {
                    return Err(meta.error(format!(
                        "{} derive does not support directional #[serde(rename_all)] in typed query metadata",
                        derive_kind.name()
                    )));
                }
                let value: LitStr = meta.value()?.parse()?;
                parsed.rename_all = Some(
                    RenameRule::parse(&value.value())
                        .map_err(|message| Error::new(value.span(), message))?,
                );
                Ok(())
            } else if meta.path.is_ident("default") {
                parse_default_attribute(&meta)?;
                Ok(())
            } else {
                Err(unsupported_attribute(derive_kind, &meta.path))
            }
        })?;
    }

    Ok(parsed)
}

fn parse_field_attributes(
    field: &syn::Field,
    container: &ContainerAttributes,
    derive_kind: DeriveKind,
) -> Result<FieldAttributes, Error> {
    let field_ident = field
        .ident
        .as_ref()
        .ok_or_else(|| Error::new_spanned(field, "expected named field"))?;
    let rust_name = field_ident.to_string();
    let rust_name = rust_name
        .strip_prefix("r#")
        .unwrap_or(&rust_name)
        .to_owned();
    let mut explicit_name = None;
    let mut skip = false;
    let mut omittable = false;

    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if meta.input.peek(syn::token::Paren) {
                    return Err(meta.error(format!(
                        "{} derive does not support directional #[serde(rename)] in typed query metadata",
                        derive_kind.name()
                    )));
                }
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                explicit_name = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("alias") {
                let _: LitStr = meta.value()?.parse()?;
                Err(meta.error(format!(
                    "{} derive does not support #[serde(alias)] in typed query metadata",
                    derive_kind.name()
                )))
            } else if meta.path.is_ident("default") {
                parse_default_attribute(&meta)?;
                Ok(())
            } else if meta.path.is_ident("skip") {
                ensure_flag_attribute(&meta)?;
                skip = true;
                omittable = true;
                Ok(())
            } else if meta.path.is_ident("skip_serializing_if") {
                parse_path_attribute(&meta)?;
                omittable = true;
                Ok(())
            } else {
                Err(unsupported_attribute(derive_kind, &meta.path))
            }
        })?;
    }

    let stored_name = explicit_name.unwrap_or_else(|| {
        container
            .rename_all
            .map(|rule| rule.apply_to_field(&rust_name))
            .unwrap_or(rust_name)
    });

    Ok(FieldAttributes {
        stored_name,
        skip,
        omittable,
    })
}

fn parse_default_attribute(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<(), Error> {
    if meta.input.peek(Token![=]) {
        let value = meta.value()?;
        let path: LitStr = value.parse()?;
        path.parse::<syn::ExprPath>()
            .map(|_| ())
            .map_err(|error| Error::new(path.span(), error.to_string()))
    } else {
        ensure_flag_attribute(meta)
    }
}

fn parse_path_attribute(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<(), Error> {
    let value = meta.value()?;
    let path: LitStr = value.parse()?;
    path.parse::<syn::ExprPath>()
        .map(|_| ())
        .map_err(|error| Error::new(path.span(), error.to_string()))
}

fn ensure_flag_attribute(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<(), Error> {
    if meta.input.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            meta.path.span(),
            "Serde attribute does not accept a value",
        ))
    }
}

fn unsupported_attribute(derive_kind: DeriveKind, path: &syn::Path) -> Error {
    Error::new(
        path.span(),
        format!(
            "{} derive does not support #[serde({})] in typed query metadata",
            derive_kind.name(),
            path.to_token_stream()
        ),
    )
}

fn parse_quokka_id(field: &syn::Field) -> Result<bool, Error> {
    let mut is_id = false;

    for attr in &field.attrs {
        if !attr.path().is_ident("quokka") {
            continue;
        }

        let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
        let metas = parser.parse2(attr.meta.require_list()?.tokens.clone())?;

        for meta in metas {
            match meta {
                Meta::Path(path) if path.is_ident("id") => is_id = true,
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "unsupported #[quokka(...)] attribute, expected #[quokka(id)]",
                    ));
                }
            }
        }
    }

    Ok(is_id)
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let Some(GenericArgument::Type(inner)) = args.args.first() else {
        return None;
    };
    Some(inner)
}

#[cfg(test)]
mod tests {
    use super::{RenameRule, derive_quokka_document_impl, derive_quokka_type_impl};
    use syn::{DeriveInput, parse_quote};

    fn expanded_document(input: DeriveInput) -> String {
        derive_quokka_document_impl(input).unwrap().to_string()
    }

    fn expanded_type(input: DeriveInput) -> String {
        derive_quokka_type_impl(input).unwrap().to_string()
    }

    #[test]
    fn rejects_optional_id_fields() {
        let error = derive_quokka_document_impl(parse_quote! {
            struct User {
                #[quokka(id)]
                id: Option<u64>,
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaDocument ID fields cannot use Option; use a concrete ID type"
        );
    }

    #[test]
    fn accepts_concrete_id_fields() {
        assert!(
            derive_quokka_document_impl(parse_quote! {
                struct User {
                    #[quokka(id)]
                    id: u64,
                }
            })
            .is_ok()
        );
    }

    #[test]
    fn generated_document_metadata_uses_renamed_field_paths() {
        let expanded = expanded_document(parse_quote! {
            struct User {
                #[quokka(id)]
                #[serde(rename = "_id")]
                id: u64,
                #[serde(rename = "display_name")]
                display_name: String,
            }
        });

        assert!(expanded.contains("\"display_name\""));
    }

    #[test]
    fn generated_document_metadata_applies_rename_all() {
        let expanded = expanded_document(parse_quote! {
            #[serde(rename_all = "camelCase")]
            struct User {
                #[quokka(id)]
                #[serde(rename = "_id")]
                id: u64,
                display_name: String,
            }
        });

        assert!(expanded.contains("\"displayName\""));
    }

    #[test]
    fn generated_embedded_metadata_applies_rename_all() {
        let expanded = expanded_type(parse_quote! {
            #[serde(rename_all = "camelCase")]
            struct Profile {
                first_name: String,
            }
        });

        assert!(expanded.contains("\"firstName\""));
    }

    #[test]
    fn supported_rename_all_rules_match_serde_names() {
        for (rule, expected) in [
            ("lowercase", "some_field"),
            ("UPPERCASE", "SOME_FIELD"),
            ("PascalCase", "SomeField"),
            ("camelCase", "someField"),
            ("snake_case", "some_field"),
            ("SCREAMING_SNAKE_CASE", "SOME_FIELD"),
            ("kebab-case", "some-field"),
            ("SCREAMING-KEBAB-CASE", "SOME-FIELD"),
        ] {
            let rule = RenameRule::parse(rule).unwrap();
            assert_eq!(rule.apply_to_field("some_field"), expected);
        }
    }

    #[test]
    fn supported_serde_metadata_is_accepted_and_skipped_fields_are_omitted() {
        let expanded = expanded_document(parse_quote! {
            #[serde(default)]
            struct User {
                #[quokka(id)]
                #[serde(rename = "_id")]
                id: u64,
                #[serde(default)]
                retry_count: i32,
                #[serde(skip)]
                internal: String,
                #[serde(skip_serializing_if = "Option::is_none")]
                note: Option<String>,
            }
        });

        assert!(expanded.contains("\"retry_count\""));
        assert!(expanded.contains("\"note\""));
        assert!(!expanded.contains("internal"));

        let expanded = expanded_type(parse_quote! {
            struct Profile {
                #[serde(skip)]
                internal: String,
                name: String,
            }
        });
        assert!(!expanded.contains("internal"));
    }

    #[test]
    fn rejects_alias_on_documents() {
        assert!(
            derive_quokka_document_impl(parse_quote! {
                struct User {
                    #[quokka(id)]
                    id: u64,
                    #[serde(alias = "legacy_name")]
                    name: String,
                }
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_alias_on_embedded_types() {
        assert!(
            derive_quokka_type_impl(parse_quote! {
                struct Profile {
                    #[serde(alias = "legacy_name")]
                    name: String,
                }
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_directional_serde_renames() {
        assert!(
            derive_quokka_document_impl(parse_quote! {
                #[serde(rename_all(serialize = "camelCase"))]
                struct User {
                    #[quokka(id)]
                    id: u64,
                }
            })
            .is_err()
        );

        assert!(
            derive_quokka_type_impl(parse_quote! {
                struct Profile {
                    #[serde(rename(serialize = "legacy_name"))]
                    name: String,
                }
            })
            .is_err()
        );
    }
}
