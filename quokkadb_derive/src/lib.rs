use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, Data, DeriveInput, Error, Fields, GenericArgument, Meta, PathArguments,
    Token, Type,
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
                ))
            }
            Fields::Unit => {
                return Err(Error::new(
                    Span::call_site(),
                    "QuokkaDocument derive requires at least one named field",
                ))
            }
        },
        Data::Enum(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaDocument derive only supports structs, not enums",
            ))
        }
        Data::Union(_) => {
            return Err(Error::new(
                Span::call_site(),
                "QuokkaDocument derive only supports structs, not unions",
            ))
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
        let rust_name = field_ident.to_string();
        let stored_name = parse_stored_name(&field)?.unwrap_or_else(|| rust_name.clone());
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
        let rust_name = field_ident.to_string();
        let stored_name = parse_stored_name(&field)?.unwrap_or_else(|| rust_name.clone());
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

fn parse_stored_name(field: &syn::Field) -> Result<Option<String>, Error> {
    let mut stored_name = None;

    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                stored_name = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("flatten") {
                Err(meta.error("QuokkaDocument derive does not support #[serde(flatten)] in v1"))
            } else if meta.path.is_ident("skip")
                || meta.path.is_ident("skip_serializing")
                || meta.path.is_ident("skip_deserializing")
            {
                Err(meta.error(
                    "QuokkaDocument derive does not support skipped fields in query metadata",
                ))
            } else {
                Ok(())
            }
        })?;
    }

    Ok(stored_name)
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
                    ))
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
    use super::derive_quokka_document_impl;
    use syn::parse_quote;

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
        assert!(derive_quokka_document_impl(parse_quote! {
            struct User {
                #[quokka(id)]
                id: u64,
            }
        })
        .is_ok());
    }
}
