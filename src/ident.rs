use heck::{ToPascalCase, ToSnakeCase};
use proc_macro2::TokenStream;
use quote::ToTokens;

pub enum CaseType {
    Snake,
    Pascal,
}

/// Converts an arbitrary postgres identifier to a valid Rust identifier with a given
/// case. Will ensure any invalid strings are turned into raw identifiers.
pub fn sql_to_rs_ident(name: &str, case_type: CaseType) -> TokenStream {
    let prefix = if starts_with_number(&name) { "_" } else { "" };
    let name = prefix.to_string()
        + &match case_type {
            CaseType::Snake => name.to_snake_case(),
            CaseType::Pascal => {
                // Preserve _ prefixes (heck removes them for some reason)
                if name.chars().next() == Some('_') {
                    "_".to_string().as_str().to_owned() + name.to_pascal_case().as_str()
                } else {
                    name.to_pascal_case()
                }
            }
        };

    match syn::parse_str::<syn::Ident>(&name) {
        Ok(ident) => ident.into_token_stream(),
        Err(_) => ("r#".to_owned() + &name).parse().unwrap(),
    }
}

pub fn sql_to_rs_string(name: &str, case_type: CaseType) -> String {
    let prefix = if starts_with_number(&name) { "_" } else { "" };
    let name = prefix.to_string()
        + &match case_type {
            CaseType::Snake => name.to_snake_case(),
            CaseType::Pascal => {
                // Preserve _ prefixes (heck removes them for some reason)
                if name.chars().next() == Some('_') {
                    "_".to_string().as_str().to_owned() + name.to_pascal_case().as_str()
                } else {
                    name.to_pascal_case()
                }
            }
        };

    match syn::parse_str::<syn::Ident>(&name) {
        Ok(_) => name,
        Err(_) => "r#".to_owned() + &name,
    }
}

fn starts_with_number(s: &str) -> bool {
    s.chars().next().map(|c| c.is_numeric()).unwrap_or(false)
}
