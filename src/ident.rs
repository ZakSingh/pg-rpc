use heck::{ToPascalCase, ToSnakeCase};
use proc_macro2::TokenStream;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseType {
    Snake,
    Pascal,
}

/// Converts an arbitrary Postgres identifier to a valid Rust identifier in the
/// given case. Never panics: see [`sql_to_rs_string`] for what happens to names
/// that are not identifiers on their own.
pub fn sql_to_rs_ident(name: &str, case_type: CaseType) -> TokenStream {
    ident_tokens(&sql_to_rs_string(name, case_type))
}

/// The tokens of an identifier produced by [`sql_to_rs_string`],
/// [`sanitize_ident`] or [`dedupe_idents`].
pub fn ident_tokens(ident: &str) -> TokenStream {
    ident
        .parse()
        .unwrap_or_else(|_| hex_fallback(ident, CaseType::Snake).parse().unwrap())
}

/// [`sql_to_rs_ident`] as a string. A keyword comes back as a raw identifier
/// (`r#type`).
pub fn sql_to_rs_string(name: &str, case_type: CaseType) -> String {
    let cased = match case_type {
        CaseType::Snake => name.to_snake_case(),
        CaseType::Pascal => {
            // Preserve a leading underscore, which heck drops.
            if name.starts_with('_') {
                format!("_{}", name.to_pascal_case())
            } else {
                name.to_pascal_case()
            }
        }
    };
    sanitize_ident(cased, name, case_type)
}

/// The Rust module a schema's code is generated into, and the path segment
/// other modules use to refer to it.
pub fn schema_mod_ident(schema: &str) -> TokenStream {
    sql_to_rs_ident(schema, CaseType::Snake)
}

/// Turns `candidate` (a case-converted form of the SQL name `original`) into
/// something Rust accepts as an identifier:
///
/// - a leading digit gets a `_` prefix (`1st` -> `_1st`);
/// - a keyword becomes a raw identifier (`type` -> `r#type`), except the four
///   that cannot be raw (`self`, `Self`, `crate`, `super`), which get a `_`
///   suffix;
/// - a name with nothing usable left (`+`, an empty string, `_`) becomes `x`
///   followed by the hex of `original`'s bytes, which is stable and distinct
///   per name.
pub fn sanitize_ident(candidate: String, original: &str, case_type: CaseType) -> String {
    let mut ident = candidate;
    if ident.chars().all(|c| c == '_') {
        ident = hex_fallback(original, case_type);
    }
    if ident.starts_with(|c: char| c.is_numeric()) {
        ident.insert(0, '_');
    }
    if is_ident(&ident) {
        return ident;
    }
    if matches!(ident.as_str(), "self" | "Self" | "crate" | "super") {
        return format!("{ident}_");
    }
    let raw = format!("r#{ident}");
    if is_ident(&raw) {
        return raw;
    }
    // Alphanumeric to heck but not to Rust (a superscript digit, say).
    hex_fallback(original, case_type)
}

fn hex_fallback(original: &str, case_type: CaseType) -> String {
    let prefix = match case_type {
        CaseType::Snake => 'x',
        CaseType::Pascal => 'X',
    };
    let hex: String = original.bytes().map(|b| format!("{b:02x}")).collect();
    format!("{prefix}{hex}")
}

fn is_ident(s: &str) -> bool {
    syn::parse_str::<syn::Ident>(s).is_ok()
}

/// Makes a list of identifiers unique, in order: the first holder of a name
/// keeps it and each later one gets the lowest numeric suffix (`a`, `a2`,
/// `a3`) that no other entry uses. Names that are already distinct are
/// returned unchanged.
pub fn dedupe_idents(idents: Vec<String>) -> Vec<String> {
    let mut taken: BTreeSet<String> = idents.iter().cloned().collect();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    idents
        .into_iter()
        .map(|ident| {
            if seen.insert(ident.clone()) {
                return ident;
            }
            // `r#type2` is not a keyword, so the suffixed name is never raw.
            let base = ident.strip_prefix("r#").unwrap_or(&ident).to_string();
            let unique = (2..)
                .map(|n| format!("{base}{n}"))
                .find(|candidate| !taken.contains(candidate))
                .expect("an unbounded range has an unused suffix");
            taken.insert(unique.clone());
            seen.insert(unique.clone());
            unique
        })
        .collect()
}

/// [`sql_to_rs_string`] for every name, made unique with [`dedupe_idents`]:
/// the identifiers for the variants of one enum or the fields of one struct.
pub fn unique_rs_idents<'a>(
    names: impl IntoIterator<Item = &'a str>,
    case_type: CaseType,
) -> Vec<TokenStream> {
    let idents = names
        .into_iter()
        .map(|name| sql_to_rs_string(name, case_type))
        .collect();
    dedupe_idents(idents)
        .iter()
        .map(|ident| ident_tokens(ident))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use CaseType::{Pascal, Snake};

    #[test]
    fn ordinary_names_are_unchanged() {
        assert_eq!(sql_to_rs_string("user_id", Snake), "user_id");
        assert_eq!(sql_to_rs_string("user_id", Pascal), "UserId");
        assert_eq!(sql_to_rs_string("my-schema", Snake), "my_schema");
        assert_eq!(sql_to_rs_string("type", Snake), "r#type");
        assert_eq!(sql_to_rs_string("1st", Snake), "_1st");
        assert_eq!(sql_to_rs_string("_private", Pascal), "_Private");
        assert_eq!(schema_mod_ident("my-schema").to_string(), "my_schema");
    }

    #[test]
    fn keywords_that_cannot_be_raw_get_a_suffix() {
        for (name, case, expected) in [
            ("self", Snake, "self_"),
            ("self", Pascal, "Self_"),
            ("crate", Snake, "crate_"),
            ("super", Snake, "super_"),
        ] {
            assert_eq!(sql_to_rs_string(name, case), expected);
            // Must tokenize without panicking.
            assert_eq!(sql_to_rs_ident(name, case).to_string(), expected);
        }
    }

    #[test]
    fn names_with_nothing_usable_get_a_stable_fallback() {
        assert_eq!(sql_to_rs_string("+", Pascal), "X2b");
        assert_eq!(sql_to_rs_string("-", Pascal), "X2d");
        assert_eq!(sql_to_rs_string("", Snake), "x");
        assert_eq!(sql_to_rs_string("_", Snake), "x5f");
        assert_eq!(sql_to_rs_string("my col", Snake), "my_col");
        assert_eq!(sql_to_rs_string("-1", Pascal), "_1");
        assert_eq!(sql_to_rs_ident("+", Pascal).to_string(), "X2b");
    }

    #[test]
    fn duplicates_get_numeric_suffixes() {
        let idents = unique_rs_idents(["A+", "A-", "a", "A2"], Pascal);
        let idents: Vec<String> = idents.iter().map(|t| t.to_string()).collect();
        // `A2` is taken by a later entry, so the duplicates skip it.
        assert_eq!(idents, ["A", "A3", "A4", "A2"]);

        let idents = unique_rs_idents(["foo-bar", "foo_bar", "type", "Type"], Snake);
        let idents: Vec<String> = idents.iter().map(|t| t.to_string()).collect();
        assert_eq!(idents, ["foo_bar", "foo_bar2", "r#type", "type2"]);
    }
}
