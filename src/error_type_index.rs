use crate::codegen::OID;
use crate::config::ErrorsConfig;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::ty_index::TypeIndex;
use anyhow::Context;
use heck::ToPascalCase;
use postgres::Client;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::{Deref, DerefMut};

const ERROR_TYPE_INTROSPECTION_QUERY: &str = include_str!("./queries/error_type_introspection.sql");

#[derive(Debug, Clone)]
pub struct ErrorType {
    pub error_name: String,
    pub type_oid: OID,
    /// `comment on type`, carrying `@pgrpc_message` / `@pgrpc_field`.
    pub comment: Option<String>,
    pub fields: Vec<ErrorField>,
}

#[derive(Debug, Clone)]
pub struct ErrorField {
    pub name: String,
    pub type_oid: OID,
    pub postgres_type: String,
    pub position: i32,
    pub not_null: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Default)]
pub struct ErrorTypeIndex(HashMap<String, ErrorType>);

impl Deref for ErrorTypeIndex {
    type Target = HashMap<String, ErrorType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ErrorTypeIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ErrorTypeIndex {
    /// Construct the error type index by introspecting the configured errors schema
    pub fn new(db: &mut Client, errors_config: &ErrorsConfig) -> anyhow::Result<Self> {
        let query_result = db
            .query(ERROR_TYPE_INTROSPECTION_QUERY, &[&errors_config.schema])
            .context("Error type introspection query failed")?;

        let error_types: HashMap<String, ErrorType> = query_result
            .into_iter()
            .map(|row| {
                let error_name: String = row.try_get("error_name")?;
                let type_oid: i32 = row.try_get("type_oid")?;
                let type_oid = type_oid as u32;
                let comment: Option<String> = row.try_get("comment")?;
                let fields_json: Value = row.try_get("fields")?;

                let fields_vec = if fields_json.is_null() {
                    Vec::new()
                } else {
                    serde_json::from_value::<Vec<Value>>(fields_json)?
                };

                let fields: Vec<ErrorField> = fields_vec
                    .into_iter()
                    .filter_map(|field_json: Value| {
                        let name = field_json["name"].as_str();
                        let type_oid = field_json["type_oid"].as_u64().or_else(|| {
                            field_json["type_oid"].as_str().and_then(|s| s.parse().ok())
                        });
                        let postgres_type = field_json["postgres_type"].as_str();
                        let position = field_json["position"].as_i64();
                        let not_null = field_json["not_null"].as_bool();

                        match (name, type_oid, postgres_type, position, not_null) {
                            (Some(n), Some(t), Some(pt), Some(p), Some(nn)) => Some(ErrorField {
                                name: n.to_string(),
                                type_oid: t as u32,
                                postgres_type: pt.to_string(),
                                position: p as i32,
                                not_null: nn,
                                comment: field_json["comment"].as_str().map(|s| s.to_string()),
                            }),
                            _ => None,
                        }
                    })
                    .collect();

                Ok::<_, anyhow::Error>((
                    error_name.clone(),
                    ErrorType {
                        error_name,
                        type_oid,
                        comment,
                        fields,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(Self(error_types))
    }

    /// Get all error type names in the index
    pub fn error_names(&self) -> Vec<String> {
        self.0.keys().cloned().collect()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Collect OIDs of all types referenced by error types
pub fn collect_error_type_oids(
    db: &mut Client,
    errors_config: &ErrorsConfig,
) -> anyhow::Result<Vec<OID>> {
    let error_index = ErrorTypeIndex::new(db, errors_config)?;

    let mut type_oids = HashSet::new();

    // Collect the error type OIDs themselves
    for error_type in error_index.values() {
        type_oids.insert(error_type.type_oid);

        // Also collect field type OIDs
        for field in &error_type.fields {
            type_oids.insert(field.type_oid);
        }
    }

    Ok(type_oids.into_iter().collect())
}

/// Generate error payload struct definitions.
///
/// Returns the structs and the set of schema modules their field types live
/// in, so the caller can emit `use super::<schema>;` for them.
pub fn generate_error_payload_structs(
    error_index: &ErrorTypeIndex,
    ty_index: &TypeIndex,
    config: &crate::config::Config,
) -> (TokenStream, BTreeSet<String>) {
    let mut structs = Vec::new();
    let mut referenced_schemas = BTreeSet::new();

    for error_type in error_index.values() {
        let struct_name = format_ident!("{}", error_type.error_name.to_pascal_case());

        let fields: Vec<TokenStream> = error_type
            .fields
            .iter()
            .map(|field| {
                let field_name = sql_to_rs_ident(&field.name, CaseType::Snake);
                let pg_name = &field.name;
                // A *reference* to the field's Rust type (`i32`, `String`,
                // `super::api::Thing`), not its definition.
                let base_field_type = match ty_index.deref().get(&field.type_oid) {
                    Some(pg_type) => {
                        let (tokens, schemas) = pg_type.to_rust_ident_with_schemas(ty_index.deref());
                        referenced_schemas.extend(schemas);
                        tokens
                    }
                    None => quote! { serde_json::Value },
                };

                let (field_type, is_nullable) = if field.not_null {
                    (base_field_type, false)
                } else {
                    (quote! { Option<#base_field_type> }, true)
                };

                let serde_attr = if is_nullable {
                    quote! { #[serde(rename = #pg_name, default)] }
                } else {
                    quote! { #[serde(rename = #pg_name)] }
                };

                if let Some(comment) = &field.comment {
                    quote! {
                        #[doc = #comment]
                        #serde_attr
                        pub #field_name: #field_type
                    }
                } else {
                    quote! {
                        #serde_attr
                        pub #field_name: #field_type
                    }
                }
            })
            .collect();

        // Get the error schema from config
        let error_schema = config
            .errors
            .as_ref()
            .map(|ec| ec.schema.as_str())
            .unwrap_or("errors");
        
        let deserialize_derive = if config.should_disable_deserialize(error_schema, &struct_name.to_string()) {
            quote! {}
        } else {
            quote! { serde::Deserialize, }
        };

        structs.push(quote! {
            #[derive(Debug, Clone, #deserialize_derive serde::Serialize)]
            pub struct #struct_name {
                #(#fields),*
            }
        });
    }

    (
        quote! {
            #(#structs)*
        },
        referenced_schemas,
    )
}

/// `custom_error_message(type_name)`: the `@pgrpc_message` / `@pgrpc_field`
/// from a payload type's comment, keyed by the JSON `type` name the raise
/// helper emits. Lets `user_message()` cover typed errors with no Rust arm.
pub fn generate_custom_error_message_lookup(error_index: &ErrorTypeIndex) -> TokenStream {
    let mut names: Vec<&String> = error_index.keys().collect();
    names.sort();
    let arms = names.into_iter().filter_map(|name| {
        let error_type = &error_index[name];
        let comment = error_type.comment.as_deref()?;
        let message = crate::annotations::parse_message(comment)?;
        let field = match crate::annotations::parse_field(comment) {
            Some(Some(f)) => quote! { Some(#f) },
            _ => quote! { None },
        };
        let type_name = error_type.error_name.as_str();
        Some(quote! {
            #type_name => Some(super::errors::ConstraintMessage { message: #message, field: #field })
        })
    });
    quote! {
        /// The user-facing message tagged on a typed error's payload type.
        pub fn custom_error_message(type_name: &str) -> Option<super::errors::ConstraintMessage> {
            match type_name {
                #(#arms,)*
                _ => None,
            }
        }
    }
}

/// Generate an enum of all error types for pattern matching
pub fn generate_error_type_enum(error_index: &ErrorTypeIndex) -> TokenStream {
    if error_index.is_empty() {
        return quote! {};
    }

    let variants: Vec<TokenStream> = error_index
        .values()
        .map(|error_type| {
            let variant_name = format_ident!("{}", error_type.error_name.to_pascal_case());
            let struct_name = format_ident!("{}", error_type.error_name.to_pascal_case());
            quote! {
                #variant_name(#struct_name)
            }
        })
        .collect();

    let from_json_arms: Vec<TokenStream> = error_index
        .values()
        .map(|error_type| {
            let error_name_str = &error_type.error_name;
            let variant_name = format_ident!("{}", error_type.error_name.to_pascal_case());
            let struct_name = format_ident!("{}", error_type.error_name.to_pascal_case());

            quote! {
                #error_name_str => {
                    serde_json::from_value::<#struct_name>(json.clone())
                        .ok()
                        .map(CustomError::#variant_name)
                }
            }
        })
        .collect();

    quote! {
        #[derive(Debug, Clone)]
        pub enum CustomError {
            #(#variants),*
        }

        impl CustomError {
            /// Try to parse a custom error from a JSON value
            pub fn from_json(json: &serde_json::Value) -> Option<Self> {
                let type_name = json.get("type")?.as_str()?;

                match type_name {
                    #(#from_json_arms),*
                    _ => None
                }
            }
        }
    }
}
