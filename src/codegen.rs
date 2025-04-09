use crate::config::Config;
use crate::fn_index::FunctionIndex;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::pg_fn::PgFn;
use crate::pg_type::PgType;
use crate::ty_index::TypeIndex;
use itertools::Itertools;
use quote::__private::TokenStream;
use quote::quote;
use std::collections::HashMap;

/// Postgres object ID
/// Uniquely identifies database objects
pub type OID = u32;

/// Postgres schema identifier
pub type SchemaName = String;

/// Postgres function identifier
pub type FunctionName = String;

pub trait ToRust {
    fn to_rust(&self, types: &HashMap<OID, PgType>, config: &Config) -> TokenStream;
}

/// Generate the output file
pub fn codegen(
    fn_index: &FunctionIndex,
    ty_index: &TypeIndex,
    config: &Config,
) -> anyhow::Result<String> {
    let warning_ignores = "#![allow(dead_code)]\n#![allow(unused_variables)]\n#![allow(unused_imports)]\n#![allow(unused_mut)]\n\n";
    let type_def_code = codegen_types(&ty_index, config);
    let fn_code = codegen_fns(&fn_index, &ty_index, config);

    let out: String = type_def_code
        .into_iter()
        .chain(fn_code) // Combine both maps into one iterator
        .into_grouping_map()
        .fold(TokenStream::new(), |acc, _, ts| quote! { #acc #ts })
        .iter()
        .map(|(schema, tokens)| {
            let s = sql_to_rs_ident(schema.as_str(), CaseType::Snake);
            prettyplease::unparse(
                &syn::parse2::<syn::File>(quote! {
                    pub mod #s {
                        use postgres_types::private::BytesMut;
                        use postgres_types::{IsNull, ToSql, Type};
                    
                        #tokens
                    }
                })
                .expect("generated code to parse"),
            )
        })
        .collect();

    Ok(warning_ignores.to_string() + &out)
}

fn codegen_types(type_index: &TypeIndex, config: &Config) -> HashMap<SchemaName, TokenStream> {
    type_index
        .values()
        .map(|t| (t.schema(), t.to_rust(type_index, config)))
        .filter(|(_, tokens)| !tokens.is_empty())
        .into_group_map()
        .into_iter()
        .map(|(schema, token_streams)| {
            (
                schema,
                quote! {
                    #(#token_streams)*
                },
            )
        })
        .collect()
}

/// Returns map of schema to token stream containing all fn definitions for that schema
fn codegen_fns(
    fns: &FunctionIndex,
    types: &TypeIndex,
    config: &Config,
) -> HashMap<SchemaName, TokenStream> {
    let schemas: HashMap<&str, Vec<&PgFn>> = fns.values().into_group_map_by(|f| f.schema.as_str());

    schemas
        .into_iter()
        .map(|(schema, fns)| {
            let fns_rs: Vec<TokenStream> =
                fns.into_iter().map(|f| f.to_rust(&types, config)).collect();
            (schema.to_string(), quote! { #(#fns_rs)* })
        })
        .collect()
}
