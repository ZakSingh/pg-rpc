use crate::codegen::OID;
use crate::config::ErrorsConfig;
use crate::db::{
    check_schema_composites, composite_type_oids, load_schema_composites, SchemaComposite,
};
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::ty_index::TypeIndex;
use heck::ToPascalCase;
use postgres::Client;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

/// The composite types of the errors schema, keyed (and therefore iterated,
/// and emitted) by name.
#[derive(Debug, Default)]
pub struct ErrorTypeIndex(BTreeMap<String, SchemaComposite>);

impl Deref for ErrorTypeIndex {
    type Target = BTreeMap<String, SchemaComposite>;

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
        let types = load_schema_composites(db, &errors_config.schema)?;
        check_schema_composites(&types, &errors_config.schema, true)?;
        Ok(Self::from_types(types))
    }

    fn from_types(types: impl IntoIterator<Item = SchemaComposite>) -> Self {
        Self(types.into_iter().map(|t| (t.name.clone(), t)).collect())
    }
}

/// Collect OIDs of all types referenced by error types
pub fn collect_error_type_oids(
    db: &mut Client,
    errors_config: &ErrorsConfig,
) -> anyhow::Result<Vec<OID>> {
    Ok(composite_type_oids(&load_schema_composites(
        db,
        &errors_config.schema,
    )?))
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
    error_payload_structs(error_index, ty_index.deref(), config)
}

fn error_payload_structs(
    error_index: &ErrorTypeIndex,
    types: &BTreeMap<OID, crate::pg_type::PgType>,
    config: &crate::config::Config,
) -> (TokenStream, BTreeSet<String>) {
    let mut structs = Vec::new();
    let mut referenced_schemas = BTreeSet::new();

    // Get the error schema from config
    let error_schema = config
        .errors
        .as_ref()
        .map(|ec| ec.schema.as_str())
        .unwrap_or("errors");

    for error_type in error_index.values() {
        let struct_name = format_ident!("{}", error_type.name.to_pascal_case());

        let fields: Vec<TokenStream> = error_type
            .fields
            .iter()
            .map(|field| {
                let field_name = sql_to_rs_ident(&field.name, CaseType::Snake);
                let pg_name = &field.name;
                // A *reference* to the field's Rust type (`i32`, `String`,
                // `super::api::Thing`), not its definition.
                let base_field_type = match types.get(&field.type_oid) {
                    Some(pg_type) => {
                        let (tokens, schemas) = pg_type.to_rust_ident_with_schemas(types);
                        referenced_schemas.extend(schemas);
                        tokens
                    }
                    None => {
                        log::warn!(
                            "Error type {}.{}: field `{}` has type {} which was not introspected; \
                             generating it as serde_json::Value",
                            error_schema,
                            error_type.name,
                            field.name,
                            field.postgres_type
                        );
                        quote! { serde_json::Value }
                    }
                };

                // Same rule as task payloads and other composites: nullable
                // unless annotated `@pgrpc_not_null`.
                let (field_type, serde_attr) = if error_type.is_nullable(field) {
                    (
                        quote! { Option<#base_field_type> },
                        quote! { #[serde(rename = #pg_name, default)] },
                    )
                } else {
                    (base_field_type, quote! { #[serde(rename = #pg_name)] })
                };

                let doc = field.comment.as_ref().map(|comment| {
                    quote! { #[doc = #comment] }
                });

                quote! {
                    #doc
                    #serde_attr
                    pub #field_name: #field_type
                }
            })
            .collect();

        let deserialize_derive =
            if config.should_disable_deserialize(error_schema, &struct_name.to_string()) {
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
    let arms = error_index.values().filter_map(|error_type| {
        let annotations = crate::annotations::Annotations::of(error_type.comment.as_deref());
        let message = annotations.message?;
        let field = match annotations.field {
            Some(Some(f)) => quote! { Some(#f) },
            _ => quote! { None },
        };
        let type_name = error_type.name.as_str();
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
            let name = format_ident!("{}", error_type.name.to_pascal_case());
            quote! {
                #name(#name)
            }
        })
        .collect();

    let from_json_arms: Vec<TokenStream> = error_index
        .values()
        .map(|error_type| {
            let error_name_str = &error_type.name;
            let name = format_ident!("{}", error_type.name.to_pascal_case());

            quote! {
                #error_name_str => {
                    serde_json::from_value::<#name>(json.clone()).map(|payload| Some(CustomError::#name(payload)))
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
            /// Try to parse a custom error from a JSON value.
            ///
            /// `None` both when the JSON is not a typed error and when it
            /// names a known type but its payload does not match the struct;
            /// use [`Self::try_from_json`] to tell those apart.
            pub fn from_json(json: &serde_json::Value) -> Option<Self> {
                Self::try_from_json(json).ok().flatten()
            }

            /// Parse a custom error from a JSON value.
            ///
            /// `Ok(None)` when the JSON has no `type`, or a `type` that is not
            /// one of the known error types. `Err` when the `type` is known
            /// but the payload does not deserialize into its struct: the
            /// database and the generated code disagree about the error's
            /// shape (e.g. the type changed since the code was generated).
            pub fn try_from_json(json: &serde_json::Value) -> Result<Option<Self>, serde_json::Error> {
                let Some(type_name) = json.get("type").and_then(|t| t.as_str()) else {
                    return Ok(None);
                };

                match type_name {
                    #(#from_json_arms),*
                    _ => Ok(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::db::test_support::{composite, field};
    use crate::pg_type::PgType;

    const TEXT: OID = 25;
    const INT4: OID = 23;

    fn types() -> BTreeMap<OID, PgType> {
        BTreeMap::from([(TEXT, PgType::Text), (INT4, PgType::Int32)])
    }

    fn config() -> Config {
        Config {
            errors: Some(ErrorsConfig {
                schema: "errors".to_string(),
                raise_function: None,
            }),
            ..Default::default()
        }
    }

    /// Token text with all whitespace removed, so assertions do not depend on
    /// how `quote` spaces tokens.
    fn squash(tokens: &TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    fn index() -> ErrorTypeIndex {
        // Deliberately not in name order.
        ErrorTypeIndex::from_types([
            composite(
                "zebra_error",
                3,
                Some("@pgrpc_not_null(stripes)"),
                vec![field("stripes", INT4, None), field("note", TEXT, None)],
            ),
            composite(
                "not_found",
                1,
                None,
                vec![
                    field("resource", TEXT, Some("What was missing @pgrpc_not_null")),
                    field("hint", TEXT, None),
                ],
            ),
            composite("empty_error", 2, None, vec![]),
            composite("mystery", 4, None, vec![field("blob", 999_999, None)]),
        ])
    }

    #[test]
    fn output_is_in_name_order() {
        let index = index();
        let (structs, _) = error_payload_structs(&index, &types(), &config());
        let structs = squash(&structs);
        let enum_tokens = squash(&generate_error_type_enum(&index));

        let positions = |haystack: &str, needles: [&str; 4]| -> Vec<usize> {
            needles
                .iter()
                .map(|n| haystack.find(n).unwrap_or_else(|| panic!("{n} missing")))
                .collect()
        };
        let sorted = |p: Vec<usize>| p.windows(2).all(|w| w[0] < w[1]);

        assert!(sorted(positions(
            &structs,
            [
                "pubstructEmptyError",
                "pubstructMystery",
                "pubstructNotFound",
                "pubstructZebraError"
            ]
        )));
        assert!(sorted(positions(
            &enum_tokens,
            [
                "EmptyError(EmptyError)",
                "Mystery(Mystery)",
                "NotFound(NotFound)",
                "ZebraError(ZebraError)"
            ]
        )));
        assert!(sorted(positions(
            &enum_tokens,
            ["\"empty_error\"=>", "\"mystery\"=>", "\"not_found\"=>", "\"zebra_error\"=>"]
        )));
    }

    #[test]
    fn not_null_annotations_remove_the_option() {
        let (structs, _) = error_payload_structs(&index(), &types(), &config());
        let structs = squash(&structs);

        // Field-level annotation.
        assert!(structs.contains(r#"#[serde(rename="resource")]pubresource:String"#), "{structs}");
        // Type-level `@pgrpc_not_null(stripes)`.
        assert!(structs.contains(r#"#[serde(rename="stripes")]pubstripes:i32"#), "{structs}");
        // Unannotated fields stay optional and default when absent.
        assert!(
            structs.contains(r#"#[serde(rename="hint",default)]pubhint:Option<String>"#),
            "{structs}"
        );
        assert!(
            structs.contains(r#"#[serde(rename="note",default)]pubnote:Option<String>"#),
            "{structs}"
        );
        // The field comment becomes a doc comment.
        assert!(structs.contains(r#"#[doc="Whatwasmissing@pgrpc_not_null"]"#), "{structs}");
    }

    #[test]
    fn zero_field_and_unknown_type_fields_still_generate() {
        let (structs, _) = error_payload_structs(&index(), &types(), &config());
        let structs = squash(&structs);
        assert!(structs.contains("pubstructEmptyError{}"), "{structs}");
        assert!(structs.contains("pubblob:Option<serde_json::Value>"), "{structs}");
    }

    #[test]
    fn disable_deserialize_drops_the_derive() {
        let mut config = config();
        config.disable_deserialize = vec!["errors.NotFound".to_string()];
        let (structs, _) = error_payload_structs(&index(), &types(), &config);
        let structs = squash(&structs);
        assert!(
            structs.contains("#[derive(Debug,Clone,serde::Serialize)]pubstructNotFound"),
            "{structs}"
        );
        assert!(
            structs.contains("#[derive(Debug,Clone,serde::Deserialize,serde::Serialize)]pubstructZebraError"),
            "{structs}"
        );
    }

    #[test]
    fn from_json_keeps_its_signature_and_try_from_json_surfaces_mismatches() {
        let tokens = squash(&generate_error_type_enum(&index()));
        assert!(
            tokens.contains("pubfnfrom_json(json:&serde_json::Value)->Option<Self>"),
            "{tokens}"
        );
        assert!(
            tokens.contains(
                "pubfntry_from_json(json:&serde_json::Value)->Result<Option<Self>,serde_json::Error>"
            ),
            "{tokens}"
        );
        // A known type whose payload fails to deserialize propagates the
        // serde error rather than being turned into "not a typed error".
        assert!(
            tokens.contains(
                "\"not_found\"=>{serde_json::from_value::<NotFound>(json.clone()).map(|payload|Some(CustomError::NotFound(payload)))}"
            ),
            "{tokens}"
        );
        assert!(tokens.contains("_=>Ok(None)"), "{tokens}");
    }

    #[test]
    fn empty_index_generates_no_enum() {
        assert!(generate_error_type_enum(&ErrorTypeIndex::default()).is_empty());
    }

    #[test]
    fn message_lookup_is_in_name_order() {
        let index = ErrorTypeIndex::from_types([
            composite("b_error", 2, Some("@pgrpc_message Second"), vec![]),
            composite("a_error", 1, Some("@pgrpc_field(email)\n@pgrpc_message First"), vec![]),
            composite("no_message", 3, None, vec![]),
        ]);
        let tokens = squash(&generate_custom_error_message_lookup(&index));
        let a = tokens.find("\"a_error\"=>").expect("a_error arm");
        let b = tokens.find("\"b_error\"=>").expect("b_error arm");
        assert!(a < b, "{tokens}");
        assert!(!tokens.contains("no_message"), "{tokens}");
    }
}
