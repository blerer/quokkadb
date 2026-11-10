use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote, ToTokens};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Error, Fields, GenericArgument, Ident, LitStr,
    PathArguments, Token, Type, Variant,
};

#[proc_macro_derive(QuokkaDocument)]
pub fn derive_quokka_document(input: TokenStream) -> TokenStream {
    match derive_quokka_document_impl(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derives typed field metadata for an embedded Serde struct or externally tagged enum.
#[proc_macro_derive(QuokkaType)]
pub fn derive_quokka_type(input: TokenStream) -> TokenStream {
    match derive_quokka_type_impl(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derives the marker trait used by typed scalar fields.
#[proc_macro_derive(QuokkaScalar)]
pub fn derive_quokka_scalar(input: TokenStream) -> TokenStream {
    match derive_quokka_scalar_impl(parse_macro_input!(input as DeriveInput)) {
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
    let partial_fields_name = format_ident!("{}PartialFields", struct_name);

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
    let mut generated_partial_fields = Vec::new();
    let mut partial_field_initializers = Vec::new();
    let mut id_field_ident = None;
    let mut id_ty = None;

    for field in named_fields {
        let parsed_field = parse_named_field(&field, &container_attributes, DeriveKind::Document)?;
        let is_id = parsed_field.attributes.stored_name == "_id";
        if is_id && option_inner_type(&parsed_field.ty).is_some() {
            return Err(Error::new_spanned(
                &parsed_field.ty,
                "QuokkaDocument ID fields cannot use Option; use a concrete ID type",
            ));
        }
        if is_id {
            if id_field_ident.is_some() {
                return Err(Error::new_spanned(
                    &field,
                    "QuokkaDocument derive supports exactly one field serialized as `_id`",
                ));
            }
            id_field_ident = Some(parsed_field.ident.clone());
            id_ty = Some(parsed_field.ty.clone());
        }
        if is_id && parsed_field.attributes.omittable {
            return Err(Error::new_spanned(
                &field,
                "QuokkaDocument ID fields cannot use Serde field-skipping attributes",
            ));
        }

        if parsed_field.attributes.skip {
            continue;
        }

        let (field_tokens, initializer) =
            generate_field_tokens(&parsed_field, quote! { path.clone() });
        generated_fields.push(field_tokens);
        field_initializers.push(initializer);
        let (partial_field_tokens, partial_initializer) =
            generate_partial_field_tokens(&parsed_field, quote! { path.clone() });
        generated_partial_fields.push(partial_field_tokens);
        partial_field_initializers.push(partial_initializer);
    }

    let id_field_ident = id_field_ident.ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "QuokkaDocument derive requires exactly one field serialized as `_id`",
        )
    })?;
    let id_ty = id_ty.expect("id field type should exist when id field is present");

    let tokens = quote! {
        #visibility struct #fields_name<D> {
            #( #generated_fields, )*
        }

        #visibility struct #partial_fields_name<D> {
            #( #generated_partial_fields, )*
        }

        impl ::quokkadb::QuokkaType for #struct_name {
            type Fields<D> = #fields_name<D>;
            type PartialFields<D> = #partial_fields_name<D>;

            fn fields<D>(path: ::quokkadb::TypedPath) -> Self::Fields<D> {
                #fields_name { #( #field_initializers, )* }
            }

            fn partial_fields<D>(path: ::quokkadb::TypedPath) -> Self::PartialFields<D> {
                #partial_fields_name { #( #partial_field_initializers, )* }
            }
        }

        impl ::quokkadb::PartialQueryFieldType for #struct_name {
            type Field<D> = ::quokkadb::PartialObjectField<D, Self>;

            fn partial_field<D>(path: ::quokkadb::TypedPath) -> Self::Field<D> {
                ::quokkadb::PartialObjectField::from_path(path)
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

fn derive_quokka_scalar_impl(input: DeriveInput) -> Result<proc_macro2::TokenStream, Error> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            input.generics,
            "QuokkaScalar derive does not support generic types yet",
        ));
    }

    let enum_name = input.ident;
    let variants = match input.data {
        Data::Enum(data) => data.variants,
        Data::Struct(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaScalar derive only supports enums, not structs",
            ));
        }
        Data::Union(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaScalar derive only supports enums, not unions",
            ));
        }
    };

    for variant in variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(Error::new_spanned(
                variant,
                "QuokkaScalar derive only supports unit variants",
            ));
        }
    }

    Ok(quote! {
        impl ::quokkadb::QuokkaScalar for #enum_name {}
    })
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

    let type_name = input.ident;
    let fields_name = format_ident!("{}Fields", type_name);
    let partial_fields_name = format_ident!("{}PartialFields", type_name);

    match input.data {
        Data::Struct(data) => {
            let named_fields = match data.fields {
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
            };

            let mut generated_fields = Vec::new();
            let mut field_initializers = Vec::new();
            let mut generated_partial_fields = Vec::new();
            let mut partial_field_initializers = Vec::new();

            for field in named_fields {
                let parsed_field =
                    parse_named_field(&field, &container_attributes, DeriveKind::Type)?;
                if parsed_field.attributes.skip {
                    continue;
                }

                let (field_tokens, initializer) =
                    generate_field_tokens(&parsed_field, quote! { path.clone() });
                generated_fields.push(field_tokens);
                field_initializers.push(initializer);
                let (partial_field_tokens, partial_initializer) =
                    generate_partial_field_tokens(&parsed_field, quote! { path.clone() });
                generated_partial_fields.push(partial_field_tokens);
                partial_field_initializers.push(partial_initializer);
            }

            Ok(quote! {
                #visibility struct #fields_name<D> {
                    #( #generated_fields, )*
                }

                #visibility struct #partial_fields_name<D> {
                    #( #generated_partial_fields, )*
                }

                impl ::quokkadb::QuokkaType for #type_name {
                    type Fields<D> = #fields_name<D>;
                    type PartialFields<D> = #partial_fields_name<D>;

                    fn fields<D>(path: ::quokkadb::TypedPath) -> Self::Fields<D> {
                        #fields_name { #( #field_initializers, )* }
                    }

                    fn partial_fields<D>(path: ::quokkadb::TypedPath) -> Self::PartialFields<D> {
                        #partial_fields_name { #( #partial_field_initializers, )* }
                    }
                }

                impl ::quokkadb::PartialQueryFieldType for #type_name {
                    type Field<D> = ::quokkadb::PartialObjectField<D, Self>;

                    fn partial_field<D>(path: ::quokkadb::TypedPath) -> Self::Field<D> {
                        ::quokkadb::PartialObjectField::from_path(path)
                    }
                }

                impl ::quokkadb::QueryFieldType for #type_name {
                    type Field<D> = ::quokkadb::ObjectField<D, Self>;
                    fn field<D>(path: ::quokkadb::TypedPath) -> Self::Field<D> {
                        ::quokkadb::ObjectField::from_path(path)
                    }
                }
            })
        }
        Data::Enum(data) => derive_quokka_type_enum(
            visibility,
            type_name,
            fields_name,
            container_attributes,
            data.variants,
        ),
        Data::Union(_) => Err(Error::new(
            Span::call_site(),
            "QuokkaType derive only supports structs and enums, not unions",
        )),
    }
}

fn derive_quokka_type_enum(
    visibility: syn::Visibility,
    type_name: Ident,
    fields_name: Ident,
    container_attributes: ContainerAttributes,
    variants: syn::punctuated::Punctuated<Variant, syn::token::Comma>,
) -> Result<proc_macro2::TokenStream, Error> {
    let mut variant_structs = Vec::new();
    let mut generated_fields = Vec::new();
    let mut field_initializers = Vec::new();
    let mut has_payload_variant = false;

    for variant in variants {
        let variant_attributes =
            parse_variant_attributes(&variant, &container_attributes, DeriveKind::Type)?;
        if variant_attributes.skip {
            continue;
        }

        let variant_ident = variant.ident;
        let variant_accessor = accessor_ident(&variant_ident);
        let variant_name = variant_attributes.stored_name;

        match variant.fields {
            Fields::Unit => {}
            Fields::Named(fields) => {
                let variant_fields_name = format_ident!("{}{}Fields", type_name, variant_ident);
                let mut variant_generated_fields = Vec::new();
                let mut variant_field_initializers = Vec::new();

                for field in fields.named {
                    let parsed_field =
                        parse_named_field(&field, &variant_attributes.fields, DeriveKind::Type)?;
                    if parsed_field.attributes.skip {
                        continue;
                    }
                    let (field_tokens, initializer) = generate_partial_field_tokens(
                        &parsed_field,
                        quote! { path.clone().field(#variant_name) },
                    );
                    variant_generated_fields.push(field_tokens);
                    variant_field_initializers.push(initializer);
                }

                if variant_generated_fields.is_empty() {
                    continue;
                }

                has_payload_variant = true;
                variant_structs.push(quote! {
                    #visibility struct #variant_fields_name<D> {
                        #( #variant_generated_fields, )*
                    }
                });
                generated_fields.push(quote! {
                    pub #variant_accessor: #variant_fields_name<D>
                });
                field_initializers.push(quote! {
                    #variant_accessor: #variant_fields_name {
                        #( #variant_field_initializers, )*
                    }
                });
            }
            Fields::Unnamed(fields) => {
                if fields.unnamed.len() != 1 {
                    return Err(Error::new_spanned(
                        fields,
                        "QuokkaType derive only supports single-field tuple variants",
                    ));
                }
                let field = fields
                    .unnamed
                    .first()
                    .expect("single-field tuple variant should contain a field");
                let field_attributes = parse_tuple_field_attributes(field, DeriveKind::Type)?;
                if field_attributes.skip {
                    continue;
                }
                let field_ty = field.ty.clone();
                has_payload_variant = true;
                generated_fields.push(quote! {
                    pub #variant_accessor: <#field_ty as ::quokkadb::PartialQueryFieldType>::Field<D>
                });
                field_initializers.push(quote! {
                    #variant_accessor: <#field_ty as ::quokkadb::PartialQueryFieldType>::partial_field(
                        path.clone().field(#variant_name)
                    )
                });
            }
        }
    }

    if !has_payload_variant {
        return Err(Error::new(
            Span::call_site(),
            "QuokkaType derive requires at least one queryable payload variant; use QuokkaScalar for unit enums",
        ));
    }

    Ok(quote! {
        #( #variant_structs )*

        #visibility struct #fields_name<D> {
            #( #generated_fields, )*
        }

        impl ::quokkadb::QuokkaType for #type_name {
            type Fields<D> = #fields_name<D>;
            type PartialFields<D> = #fields_name<D>;

            fn fields<D>(path: ::quokkadb::TypedPath) -> Self::Fields<D> {
                #fields_name { #( #field_initializers, )* }
            }

            fn partial_fields<D>(path: ::quokkadb::TypedPath) -> Self::PartialFields<D> {
                #fields_name { #( #field_initializers, )* }
            }
        }

        impl ::quokkadb::PartialQueryFieldType for #type_name {
            type Field<D> = ::quokkadb::PartialObjectField<D, Self>;

            fn partial_field<D>(path: ::quokkadb::TypedPath) -> Self::Field<D> {
                ::quokkadb::PartialObjectField::from_path(path)
            }
        }

        impl ::quokkadb::QueryFieldType for #type_name {
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

#[derive(Clone, Copy)]
enum RenameInput {
    Field,
    Variant,
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

    fn apply(self, name: &str, input: RenameInput) -> String {
        let snake = match input {
            RenameInput::Field => name.to_owned(),
            RenameInput::Variant => to_snake_case(name),
        };
        let pascal = match input {
            RenameInput::Field => to_pascal_case(name),
            RenameInput::Variant => name.to_owned(),
        };

        match self {
            Self::LowerCase => match input {
                RenameInput::Field => name.to_owned(),
                RenameInput::Variant => name.to_ascii_lowercase(),
            },
            Self::UpperCase => name.to_ascii_uppercase(),
            Self::PascalCase => pascal,
            Self::CamelCase => lowercase_first(&pascal),
            Self::SnakeCase => snake,
            Self::ScreamingSnakeCase => snake.to_ascii_uppercase(),
            Self::KebabCase => snake.replace('_', "-"),
            Self::ScreamingKebabCase => snake.to_ascii_uppercase().replace('_', "-"),
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

struct ParsedField {
    ident: Ident,
    ty: Type,
    attributes: FieldAttributes,
}

struct VariantAttributes {
    stored_name: String,
    skip: bool,
    fields: ContainerAttributes,
}

fn parse_named_field(
    field: &syn::Field,
    container: &ContainerAttributes,
    derive_kind: DeriveKind,
) -> Result<ParsedField, Error> {
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| Error::new_spanned(field, "expected named field"))?;
    let attributes = parse_field_attributes(field, container, derive_kind)?;

    Ok(ParsedField {
        ident,
        ty: field.ty.clone(),
        attributes,
    })
}

fn generate_field_tokens(
    field: &ParsedField,
    path: proc_macro2::TokenStream,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let field_ident = &field.ident;
    let field_ty = &field.ty;
    let stored_name = &field.attributes.stored_name;
    let field_path = quote! { #path.field(#stored_name) };

    (
        quote! {
            pub #field_ident: <#field_ty as ::quokkadb::QueryFieldType>::Field<D>
        },
        quote! {
            #field_ident: <#field_ty as ::quokkadb::QueryFieldType>::field(#field_path)
        },
    )
}

fn generate_partial_field_tokens(
    field: &ParsedField,
    path: proc_macro2::TokenStream,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let field_ident = &field.ident;
    let field_ty = &field.ty;
    let stored_name = &field.attributes.stored_name;
    let field_path = quote! { #path.field(#stored_name) };

    (
        quote! {
            pub #field_ident: <#field_ty as ::quokkadb::PartialQueryFieldType>::Field<D>
        },
        quote! {
            #field_ident: <#field_ty as ::quokkadb::PartialQueryFieldType>::partial_field(#field_path)
        },
    )
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
                parsed.rename_all = Some(parse_rename_rule(&meta, derive_kind, "rename_all")?);
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
                explicit_name = Some(parse_rename(&meta, derive_kind, "rename")?);
                Ok(())
            } else if meta.path.is_ident("alias") {
                reject_alias(&meta, derive_kind)
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
            .map(|rule| rule.apply(&rust_name, RenameInput::Field))
            .unwrap_or(rust_name)
    });

    Ok(FieldAttributes {
        stored_name,
        skip,
        omittable,
    })
}

fn parse_variant_attributes(
    variant: &Variant,
    container: &ContainerAttributes,
    derive_kind: DeriveKind,
) -> Result<VariantAttributes, Error> {
    let mut explicit_name = None;
    let mut skip = false;
    let mut fields = ContainerAttributes::default();

    for attr in &variant.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                explicit_name = Some(parse_rename(&meta, derive_kind, "rename")?);
                Ok(())
            } else if meta.path.is_ident("rename_all") {
                fields.rename_all = Some(parse_rename_rule(&meta, derive_kind, "rename_all")?);
                Ok(())
            } else if meta.path.is_ident("alias") {
                reject_alias(&meta, derive_kind)
            } else if meta.path.is_ident("skip") {
                ensure_flag_attribute(&meta)?;
                skip = true;
                Ok(())
            } else {
                Err(unsupported_attribute(derive_kind, &meta.path))
            }
        })?;
    }

    let rust_name = variant.ident.to_string();
    let rust_name = rust_name.strip_prefix("r#").unwrap_or(&rust_name);
    let stored_name = explicit_name.unwrap_or_else(|| {
        container
            .rename_all
            .map(|rule| rule.apply(rust_name, RenameInput::Variant))
            .unwrap_or_else(|| rust_name.to_owned())
    });

    Ok(VariantAttributes {
        stored_name,
        skip,
        fields,
    })
}

fn parse_tuple_field_attributes(
    field: &syn::Field,
    derive_kind: DeriveKind,
) -> Result<FieldAttributes, Error> {
    let mut skip = false;
    let mut omittable = false;

    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
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

    Ok(FieldAttributes {
        stored_name: String::new(),
        skip,
        omittable,
    })
}

fn to_pascal_case(value: &str) -> String {
    let mut pascal = String::new();
    let mut capitalize = true;
    for ch in value.chars() {
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

fn lowercase_first(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn to_snake_case(value: &str) -> String {
    let mut snake = String::new();
    for (index, ch) in value.char_indices() {
        if index > 0 && ch.is_uppercase() {
            snake.push('_');
        }
        snake.extend(ch.to_lowercase());
    }
    snake
}

fn accessor_ident(ident: &Ident) -> Ident {
    let name = ident.to_string();
    let name = name.strip_prefix("r#").unwrap_or(&name);
    let name = to_snake_case(name);
    syn::parse_str(&name).unwrap_or_else(|_| {
        syn::parse_str(&format!("r#{name}"))
            .expect("generated enum accessor should be a valid identifier")
    })
}

fn parse_rename(
    meta: &syn::meta::ParseNestedMeta<'_>,
    derive_kind: DeriveKind,
    attribute: &str,
) -> Result<String, Error> {
    if meta.input.peek(syn::token::Paren) {
        return Err(meta.error(format!(
            "{} derive does not support directional #[serde({attribute})] in typed query metadata",
            derive_kind.name()
        )));
    }
    let value = meta.value()?;
    let name: LitStr = value.parse()?;
    Ok(name.value())
}

fn parse_rename_rule(
    meta: &syn::meta::ParseNestedMeta<'_>,
    derive_kind: DeriveKind,
    attribute: &str,
) -> Result<RenameRule, Error> {
    if meta.input.peek(syn::token::Paren) {
        return Err(meta.error(format!(
            "{} derive does not support directional #[serde({attribute})] in typed query metadata",
            derive_kind.name()
        )));
    }
    let value: LitStr = meta.value()?.parse()?;
    RenameRule::parse(&value.value()).map_err(|message| Error::new(value.span(), message))
}

fn reject_alias(
    meta: &syn::meta::ParseNestedMeta<'_>,
    derive_kind: DeriveKind,
) -> Result<(), Error> {
    let _: LitStr = meta.value()?.parse()?;
    Err(meta.error(format!(
        "{} derive does not support #[serde(alias)] in typed query metadata",
        derive_kind.name()
    )))
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
    use super::{
        derive_quokka_document_impl, derive_quokka_scalar_impl, derive_quokka_type_impl,
        RenameInput, RenameRule,
    };
    use syn::{parse_quote, DeriveInput};

    fn expanded_document(input: DeriveInput) -> String {
        derive_quokka_document_impl(input).unwrap().to_string()
    }

    fn expanded_type(input: DeriveInput) -> String {
        derive_quokka_type_impl(input).unwrap().to_string()
    }

    fn expanded_scalar(input: DeriveInput) -> String {
        derive_quokka_scalar_impl(input).unwrap().to_string()
    }

    #[test]
    fn generated_scalar_metadata_uses_only_the_marker_trait() {
        let expanded = expanded_scalar(parse_quote! {
            #[serde(rename_all = "snake_case")]
            enum Status {
                Active,
                Disabled,
            }
        });

        assert!(expanded.contains("impl :: quokkadb :: QuokkaScalar for Status"));
        assert!(!expanded.contains("Serialize"));
        assert!(!expanded.contains("Deserialize"));
    }

    #[test]
    fn generated_enum_metadata_uses_variant_and_payload_paths() {
        let expanded = expanded_type(parse_quote! {
            #[serde(rename_all = "snake_case")]
            enum Shape {
                Pending,
                Circle { radius: f64 },
                #[serde(rename = "rect")]
                Rectangle { width: f64, height: f64 },
                Integer(i64),
            }
        });

        assert!(expanded.contains("ShapeCircleFields"));
        assert!(expanded.contains("circle"));
        assert!(expanded.contains("radius"));
        assert!(expanded.contains("rect"));
        assert!(expanded.contains("width"));
        assert!(expanded.contains("integer"));
        assert!(!expanded.contains("pending"));
    }

    #[test]
    fn quokka_type_rejects_multi_field_tuple_variants() {
        let error = derive_quokka_type_impl(parse_quote! {
            enum Value {
                Point(f64, f64),
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaType derive only supports single-field tuple variants"
        );
    }

    #[test]
    fn quokka_type_rejects_unit_only_enums() {
        let error = derive_quokka_type_impl(parse_quote! {
            enum Status {
                Active,
                Disabled,
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaType derive requires at least one queryable payload variant; use QuokkaScalar for unit enums"
        );
    }

    #[test]
    fn quokka_scalar_rejects_payload_variants() {
        let error = derive_quokka_scalar_impl(parse_quote! {
            enum Value {
                Integer(i64),
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaScalar derive only supports unit variants"
        );
    }

    #[test]
    fn rejects_optional_id_fields() {
        let error = derive_quokka_document_impl(parse_quote! {
            struct User {
                #[serde(rename = "_id")]
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
        assert!(derive_quokka_document_impl(parse_quote! {
            struct User {
                #[serde(rename = "_id")]
                identifier: u64,
            }
        })
        .is_ok());
    }

    #[test]
    fn rejects_missing_id_fields() {
        let error = derive_quokka_document_impl(parse_quote! {
            struct User {
                id: u64,
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaDocument derive requires exactly one field serialized as `_id`"
        );
    }

    #[test]
    fn rejects_duplicate_id_fields() {
        let error = derive_quokka_document_impl(parse_quote! {
            struct User {
                #[serde(rename = "_id")]
                first_id: u64,
                #[serde(rename = "_id")]
                second_id: u64,
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaDocument derive supports exactly one field serialized as `_id`"
        );
    }

    #[test]
    fn rejects_omittable_id_fields() {
        let error = derive_quokka_document_impl(parse_quote! {
            struct User {
                #[serde(rename = "_id", skip)]
                id: u64,
            }
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "QuokkaDocument ID fields cannot use Serde field-skipping attributes"
        );
    }

    #[test]
    fn generated_document_metadata_uses_renamed_field_paths() {
        let expanded = expanded_document(parse_quote! {
            struct User {
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
            assert_eq!(rule.apply("some_field", RenameInput::Field), expected);
        }

        for (rule, expected) in [
            ("lowercase", "somevariant"),
            ("UPPERCASE", "SOMEVARIANT"),
            ("PascalCase", "SomeVariant"),
            ("camelCase", "someVariant"),
            ("snake_case", "some_variant"),
            ("SCREAMING_SNAKE_CASE", "SOME_VARIANT"),
            ("kebab-case", "some-variant"),
            ("SCREAMING-KEBAB-CASE", "SOME-VARIANT"),
        ] {
            let rule = RenameRule::parse(rule).unwrap();
            assert_eq!(rule.apply("SomeVariant", RenameInput::Variant), expected);
        }
    }

    #[test]
    fn supported_serde_metadata_is_accepted_and_skipped_fields_are_omitted() {
        let expanded = expanded_document(parse_quote! {
            #[serde(default)]
            struct User {
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
        assert!(derive_quokka_document_impl(parse_quote! {
            struct User {
                #[serde(rename = "_id")]
                id: u64,
                #[serde(alias = "legacy_name")]
                name: String,
            }
        })
        .is_err());
    }

    #[test]
    fn rejects_alias_on_embedded_types() {
        assert!(derive_quokka_type_impl(parse_quote! {
            struct Profile {
                #[serde(alias = "legacy_name")]
                name: String,
            }
        })
        .is_err());
    }

    #[test]
    fn rejects_directional_serde_renames() {
        assert!(derive_quokka_document_impl(parse_quote! {
            #[serde(rename_all(serialize = "camelCase"))]
            struct User {
                #[serde(rename = "_id")]
                id: u64,
            }
        })
        .is_err());

        assert!(derive_quokka_type_impl(parse_quote! {
            struct Profile {
                #[serde(rename(serialize = "legacy_name"))]
                name: String,
            }
        })
        .is_err());
    }
}
