use crate::config::Config;
use crate::exceptions::PgException;
use crate::ident::{sql_to_rs_ident, CaseType::Pascal};
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::pg_rel::PgRelKind;
use crate::rel_index::RelIndex;
use heck::ToPascalCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

/// Trait for types that can provide access to an underlying DbError
pub trait AsDbError {
    /// Returns a reference to the underlying DbError if this error contains one
    fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError>;
}

/// Extension trait for error types that provides helper methods for PostgreSQL errors
pub trait PgRpcErrorExt: AsDbError {
    /// Returns the column name from the underlying PostgreSQL error, if available
    fn column(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.column())
    }

    /// Returns the constraint name from the underlying PostgreSQL error, if available
    fn constraint(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.constraint())
    }

    /// Returns the table name from the underlying PostgreSQL error, if available
    fn table(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.table())
    }

    /// Returns the error message from the underlying PostgreSQL error, if available
    fn message(&self) -> Option<&str> {
        self.as_db_error().map(|e| e.message())
    }

    /// Returns the SQL state code from the underlying PostgreSQL error, if available
    fn code(&self) -> Option<&tokio_postgres::error::SqlState> {
        self.as_db_error().map(|e| e.code())
    }

    /// Returns the schema name from the underlying PostgreSQL error, if available
    fn schema(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.schema())
    }

    /// Returns the data type name from the underlying PostgreSQL error, if available
    fn datatype(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.datatype())
    }

    /// Returns the detail from the underlying PostgreSQL error, if available
    fn detail(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.detail())
    }

    /// Returns the hint from the underlying PostgreSQL error, if available
    fn hint(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.hint())
    }

    /// Returns the position from the underlying PostgreSQL error, if available
    fn position(&self) -> Option<&tokio_postgres::error::ErrorPosition> {
        self.as_db_error().and_then(|e| e.position())
    }

    /// Returns the where clause from the underlying PostgreSQL error, if available
    fn where_(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.where_())
    }

    /// Returns the routine from the underlying PostgreSQL error, if available
    fn routine(&self) -> Option<&str> {
        self.as_db_error().and_then(|e| e.routine())
    }
}

/// Blanket implementation of PgRpcErrorExt for all types that implement AsDbError
impl<T: AsDbError> PgRpcErrorExt for T {}

/// Collects all constraints from all tables and groups them by table
/// Only includes constraints that will generate enum variants (excludes Default constraints)
pub fn collect_table_constraints(rel_index: &RelIndex) -> BTreeMap<PgId, Vec<Constraint>> {
    let mut table_constraints: BTreeMap<PgId, Vec<Constraint>> = BTreeMap::new();

    for rel in rel_index.deref().values() {
        // Only process actual tables, not views
        if !matches!(rel.kind, PgRelKind::Table) {
            continue;
        }
        
        // Filter out constraints that won't generate enum variants
        let non_default_constraints: Vec<Constraint> = rel
            .constraints
            .iter()
            .filter(|constraint| !matches!(constraint, Constraint::Default(_)))
            .cloned()
            .collect();

        if !non_default_constraints.is_empty() {
            table_constraints.insert(rel.id.clone(), non_default_constraints);
        }
    }

    table_constraints
}

/// Generates constraint enums for each table, each with `name()`,
/// `message()` and `field()` accessors and a `Display` that prints the
/// Postgres constraint name.
pub fn generate_constraint_enums(
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
) -> Vec<TokenStream> {
    let mut enums = Vec::new();

    for (table_id, constraints) in table_constraints {
        let table_name_str = table_id.name();
        let table_name_pascal = table_name_str.to_pascal_case();
        let enum_name = quote::format_ident!("{}Constraint", table_name_pascal);

        let mut variants: Vec<(proc_macro2::Ident, &Constraint)> = Vec::new();
        let mut seen_names = BTreeSet::new();

        for constraint in constraints {
            let variant_name = match constraint {
                Constraint::PrimaryKey(_) => quote::format_ident!("PrimaryKey"),
                Constraint::Unique(u) => {
                    let name = u.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                }
                Constraint::Check(c) => {
                    let name = c.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                }
                Constraint::ForeignKey(fk) => {
                    let name = fk.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                }
                Constraint::NotNull(nn) => {
                    let col_name = nn.column.to_pascal_case();
                    quote::format_ident!("{}NotNull", col_name)
                }
                Constraint::Domain(d) => {
                    let name = d.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                }
                Constraint::Exclusion(x) => {
                    let name = x.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                }
                Constraint::Default(_) => continue, // Skip default constraints
            };

            // Avoid duplicate variant names
            if seen_names.insert(variant_name.to_string()) {
                variants.push((variant_name, constraint));
            }
        }

        if variants.is_empty() {
            continue;
        }

        let variant_idents: Vec<&proc_macro2::Ident> = variants.iter().map(|(v, _)| v).collect();
        let name_arms = variants.iter().map(|(v, c)| {
            let name = c.name();
            let name = name.as_str();
            quote! { #enum_name::#v => #name }
        });
        let message_arms = variants.iter().map(|(v, c)| match c.message() {
            Some(m) => quote! { #enum_name::#v => Some(#m) },
            None => quote! { #enum_name::#v => None },
        });
        let field_arms = variants.iter().map(|(v, c)| match c.field() {
            Some(f) => quote! { #enum_name::#v => Some(#f) },
            None => quote! { #enum_name::#v => None },
        });

        enums.push(quote! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum #enum_name {
                #(#variant_idents),*
            }

            impl #enum_name {
                /// The Postgres constraint name, as reported by
                /// `DbError::constraint()`. Not-null variants, which have no
                /// constraint object, use `<column>_not_null`.
                pub fn name(&self) -> &'static str {
                    match self {
                        #(#name_arms),*
                    }
                }

                /// The `@pgrpc_message` from the constraint's comment, if it has one.
                pub fn message(&self) -> Option<&'static str> {
                    match self {
                        #(#message_arms),*
                    }
                }

                /// The field the message belongs to: the single constrained
                /// column, or the `@pgrpc_field` override. `None` for
                /// multi-column constraints without an override and for
                /// `@pgrpc_field()`.
                pub fn field(&self) -> Option<&'static str> {
                    match self {
                        #(#field_arms),*
                    }
                }

                /// `message()` and `field()` together, when the constraint is tagged.
                pub fn user_message(&self) -> Option<ConstraintMessage> {
                    Some(ConstraintMessage {
                        message: self.message()?,
                        field: self.field(),
                    })
                }
            }

            impl std::fmt::Display for #enum_name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(self.name())
                }
            }
        });
    }

    enums
}

/// Generates `ConstraintMessage` and the name-keyed lookups
/// `constraint_message(table, constraint)` and
/// `domain_constraint_message(domain, constraint)` over every tagged
/// constraint, for errors that did not get a typed variant.
pub fn generate_constraint_message_lookup(
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
) -> TokenStream {
    // (schema, table, constraint) -> (message, field)
    let mut table_entries: BTreeMap<(String, String, String), (String, Option<String>)> =
        BTreeMap::new();
    // (domain schema, domain, constraint) -> (message, explicit field)
    let mut domain_entries: BTreeMap<(String, String, String), (String, Option<String>)> =
        BTreeMap::new();

    for (table_id, constraints) in table_constraints {
        for constraint in constraints {
            let Some(message) = constraint.message() else { continue };
            match constraint {
                Constraint::Domain(d) => {
                    let (Some(domain), Some(schema)) = (&d.domain, &d.domain_schema) else {
                        continue;
                    };
                    // A domain has no column to default to: only an explicit
                    // `@pgrpc_field(name)` attaches a field.
                    let field = match constraint.comment().and_then(crate::annotations::parse_field) {
                        Some(Some(f)) => Some(f),
                        _ => None,
                    };
                    domain_entries
                        .entry((schema.clone(), domain.clone(), d.name.to_string()))
                        .or_insert((message, field));
                }
                _ => {
                    table_entries.insert(
                        (
                            table_id.schema().to_string(),
                            table_id.name().to_string(),
                            constraint.name().to_string(),
                        ),
                        (message, constraint.field()),
                    );
                }
            }
        }
    }

    // `(Some("schema") | None, "table", "constraint")`: an exact schema
    // matches, and a caller without a schema gets the first entry in
    // (schema, table, constraint) order.
    let arm = |(schema, name, constraint): &(String, String, String), (message, field): &(String, Option<String>)| {
        let field = match field {
            Some(f) => quote! { Some(#f) },
            None => quote! { None },
        };
        quote! {
            (Some(#schema) | None, #name, #constraint) => Some(ConstraintMessage { message: #message, field: #field })
        }
    };
    let table_arms = table_entries.iter().map(|(k, v)| arm(k, v));
    let domain_arms = domain_entries.iter().map(|(k, v)| arm(k, v));

    quote! {
        /// A user-facing message attached to a constraint (or a typed error's
        /// payload type) with `@pgrpc_message`, and the form field it belongs
        /// to (see `@pgrpc_field`).
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct ConstraintMessage {
            pub message: &'static str,
            /// `None` means the message is form-level rather than tied to a field.
            pub field: Option<&'static str>,
        }

        /// `(schema, table, constraint)` -> message, for every tagged table
        /// constraint, keyed by the bare names `DbError::schema()`,
        /// `DbError::table()` and `DbError::constraint()` report. With
        /// `schema = None` the first matching table wins.
        pub fn constraint_message(schema: Option<&str>, table: &str, constraint: &str) -> Option<ConstraintMessage> {
            match (schema, table, constraint) {
                #(#table_arms,)*
                _ => None,
            }
        }

        /// `(schema, domain, constraint)` -> message, for every tagged domain
        /// CHECK, keyed by `DbError::schema()`, `DbError::datatype()` and
        /// `DbError::constraint()`.
        pub fn domain_constraint_message(schema: Option<&str>, domain: &str, constraint: &str) -> Option<ConstraintMessage> {
            match (schema, domain, constraint) {
                #(#domain_arms,)*
                _ => None,
            }
        }
    }
}

/// Generates the unified PgRpcError enum (without wrapper)
pub fn generate_unified_error(
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
    custom_exceptions: &BTreeSet<PgException>,
    config: &Config,
) -> TokenStream {
    let mut error_variants: Vec<TokenStream> = Vec::new();

    // Generate variants for each table's constraints
    for (table_id, _) in table_constraints {
        let table_name_str = table_id.name();
        let table_name_pascal = table_name_str.to_pascal_case();
        let variant_name = quote::format_ident!("{}Constraint", table_name_pascal);
        let constraint_enum = quote::format_ident!("{}Constraint", table_name_pascal);

        error_variants.push(quote! {
            #[error("{} table constraint violation", #table_name_str)]
            #variant_name(#constraint_enum, #[source] tokio_postgres::error::DbError)
        });
    }

    // Add custom SQL state exceptions from config
    let mut config_variants = BTreeSet::new();
    for (_sql_code, description) in &config.exceptions {
        let variant_name = sql_to_rs_ident(description, Pascal);
        if config_variants.insert(variant_name.to_string()) {
            error_variants.push(quote! {
                #[error(#description)]
                #variant_name(#[source] tokio_postgres::error::DbError)
            });
        }
    }

    // Add any custom SQL state exceptions not already covered by config
    let mut additional_custom_variants = BTreeSet::new();
    for exception in custom_exceptions {
        if let PgException::Explicit(sql_state) = exception {
            let sql_code = sql_state.code();
            // Skip if already handled by config
            if !config.exceptions.contains_key(sql_code) {
                let variant_name = exception.rs_name(config);
                if additional_custom_variants.insert(variant_name.to_string()) {
                    error_variants.push(quote! {
                        #[error("SQL state {}", #sql_code)]
                        #variant_name(#[source] tokio_postgres::error::DbError)
                    });
                }
            }
        }
    }

    // Add custom error variant if errors config is present
    if config.errors.is_some() {
        error_variants.push(quote! {
            #[error("{0:?}")]
            CustomError(super::custom_errors::CustomError, #[source] tokio_postgres::error::DbError)
        });
    }

    // A row column that could not be decoded into the generated Rust type.
    error_variants.push(quote! {
        #[error(transparent)]
        Decode(DecodeError)
    });

    // Add generic database error - using transparent to show underlying SQL error
    error_variants.push(quote! {
        #[error(transparent)]
        Database(tokio_postgres::Error)
    });

    // No longer need From<tokio_postgres::Error> implementation

    // Build the error enum variants with proper comma separation
    let all_error_variants = if error_variants.is_empty() {
        quote! {}
    } else {
        let variants_with_commas = error_variants
            .into_iter()
            .map(|variant| quote! { #variant, })
            .collect::<TokenStream>();
        variants_with_commas
    };

    let error_enum_def = quote! {
        #[derive(Debug, thiserror::Error)]
        pub enum PgRpcError {
            #all_error_variants
        }
    };

    // From implementation removed - will be handled at function level

    // No wrapper needed anymore

    // Generate helper methods for the error type
    let helper_methods = generate_helper_methods(table_constraints, custom_exceptions, config);

    // Generate AsDbError implementation
    let as_db_error_impl = generate_as_db_error_impl(table_constraints, custom_exceptions, config);

    quote! {
        #error_enum_def

        #helper_methods

        #as_db_error_impl
    }
}

/// Generates the match arms for the From<tokio_postgres::Error> implementation
/// This is now used by function-specific error enums
/// Note: Constraint names are unique within a schema in PostgreSQL
pub fn generate_from_impl_arms(
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
    custom_exceptions: &BTreeSet<PgException>,
    config: &Config,
) -> TokenStream {
    let mut unique_arms: Vec<TokenStream> = Vec::new();
    let mut check_arms: Vec<TokenStream> = Vec::new();
    let mut fk_arms: Vec<TokenStream> = Vec::new();
    let mut not_null_arms: Vec<TokenStream> = Vec::new();
    let mut custom_arms: Vec<TokenStream> = Vec::new();

    // Process constraints by table
    for (table_id, constraints) in table_constraints {
        let table_name_str = table_id.name();
        let table_name_pascal = table_name_str.to_pascal_case();
        let error_variant = quote::format_ident!("{}Constraint", table_name_pascal);
        let constraint_enum = quote::format_ident!("{}Constraint", table_name_pascal);

        for constraint in constraints {
            match constraint {
                Constraint::PrimaryKey(pk) => {
                    let constraint_name = pk.name.as_str();
                    unique_arms.push(quote! {
                        Some(#constraint_name) => PgRpcError::#error_variant(
                            #constraint_enum::PrimaryKey,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::Unique(u) => {
                    let constraint_name = u.name.as_str();
                    let variant_name_str = u.name.to_pascal_case();
                    let variant_name = quote::format_ident!("{}", variant_name_str);
                    unique_arms.push(quote! {
                        Some(#constraint_name) => PgRpcError::#error_variant(
                            #constraint_enum::#variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::Check(c) => {
                    let constraint_name = c.name.as_str();
                    let variant_name_str = c.name.to_pascal_case();
                    let variant_name = quote::format_ident!("{}", variant_name_str);
                    check_arms.push(quote! {
                        Some(#constraint_name) => PgRpcError::#error_variant(
                            #constraint_enum::#variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::ForeignKey(fk) => {
                    let constraint_name = fk.name.as_str();
                    let variant_name_str = fk.name.to_pascal_case();
                    let variant_name = quote::format_ident!("{}", variant_name_str);
                    fk_arms.push(quote! {
                        Some(#constraint_name) => PgRpcError::#error_variant(
                            #constraint_enum::#variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                Constraint::NotNull(nn) => {
                    let column_name = nn.column.as_str();
                    let col_variant_str = nn.column.to_pascal_case();
                    let variant_name = quote::format_ident!("{}NotNull", col_variant_str);
                    not_null_arms.push(quote! {
                        (Some(#table_name_str), Some(#column_name)) => PgRpcError::#error_variant(
                            #constraint_enum::#variant_name,
                            db_error.to_owned()
                        )
                    });
                }
                _ => {} // Skip other constraint types
            }
        }
    }

    // Process custom exceptions from config first
    for (sql_code, description) in &config.exceptions {
        let variant_name = sql_to_rs_ident(description, Pascal);
        custom_arms.push(quote! {
            code if code == &tokio_postgres::error::SqlState::from_code(#sql_code) => {
                PgRpcError::#variant_name(db_error.to_owned())
            }
        });
    }

    // Process any additional custom exceptions not covered by config
    for exception in custom_exceptions {
        if let PgException::Explicit(sql_state) = exception {
            let code = sql_state.code();
            // Skip if already handled by config
            if !config.exceptions.contains_key(code) {
                let variant_name = exception.rs_name(config);
                custom_arms.push(quote! {
                    code if code == &tokio_postgres::error::SqlState::from_code(#code) => {
                        PgRpcError::#variant_name(db_error.to_owned())
                    }
                });
            }
        }
    }

    // Build the complete match expression
    let mut all_arms = Vec::new();

    if !unique_arms.is_empty() {
        all_arms.push(quote! {
            &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => {
                match db_error.constraint() {
                    #(#unique_arms,)*
                    _ => PgRpcError::Database(e)
                }
            },
        });
    }

    if !check_arms.is_empty() {
        all_arms.push(quote! {
            &tokio_postgres::error::SqlState::CHECK_VIOLATION => {
                match db_error.constraint() {
                    #(#check_arms,)*
                    _ => PgRpcError::Database(e)
                }
            },
        });
    }

    if !fk_arms.is_empty() {
        all_arms.push(quote! {
            &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION => {
                match db_error.constraint() {
                    #(#fk_arms,)*
                    _ => PgRpcError::Database(e)
                }
            },
        });
    }

    if !not_null_arms.is_empty() {
        all_arms.push(quote! {
            &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => {
                match (db_error.table(), db_error.column()) {
                    #(#not_null_arms,)*
                    _ => PgRpcError::Database(e)
                }
            },
        });
    }

    if !custom_arms.is_empty() {
        all_arms.extend(custom_arms);
    }

    // Combine all arms into a single TokenStream
    let combined_arms = all_arms.into_iter().collect::<TokenStream>();

    combined_arms
}

/// Generates helper methods for the PgRpcError enum
fn generate_helper_methods(
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
    custom_exceptions: &BTreeSet<PgException>,
    config: &Config,
) -> TokenStream {
    let mut methods = Vec::new();

    // Only generate helper methods for custom exceptions from config
    for (_sql_code, description) in &config.exceptions {
        let method_name = format_ident!(
            "is_{}",
            sql_to_rs_ident(description, crate::ident::CaseType::Snake).to_string()
        );
        let variant_name = sql_to_rs_ident(description, Pascal);

        methods.push(quote! {
            #[doc = concat!("Returns true if this error is a ", #description, " error")]
            pub fn #method_name(&self) -> bool {
                matches!(self, PgRpcError::#variant_name(_))
            }
        });
    }

    quote! {
        impl PgRpcError {
            #(#methods)*
        }
    }
}

/// Implementation of AsDbError for PgRpcError
/// This allows all error types to use the helper methods via PgRpcErrorExt
pub fn generate_as_db_error_impl(
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
    custom_exceptions: &BTreeSet<PgException>,
    config: &Config,
) -> TokenStream {
    // Collect all constraint variant arms
    let constraint_arms: Vec<TokenStream> = table_constraints
        .keys()
        .map(|table_id| {
            let table_name_pascal = table_id.name().to_pascal_case();
            let variant_name = format_ident!("{}Constraint", table_name_pascal);
            quote! { PgRpcError::#variant_name(_, db_error) => Some(db_error) }
        })
        .collect();

    // Collect all custom exception variant names for arms that return None
    let mut custom_variant_names = Vec::new();

    // Custom exceptions from config
    for (_sql_code, description) in &config.exceptions {
        let variant_name = sql_to_rs_ident(description, Pascal);
        custom_variant_names.push(variant_name);
    }

    // Additional custom exceptions not in config
    for exception in custom_exceptions {
        if let PgException::Explicit(sql_state) = exception {
            let code = sql_state.code();
            if !config.exceptions.contains_key(code) {
                let variant_name = exception.rs_name(config);
                custom_variant_names.push(variant_name);
            }
        }
    }

    let mut custom_arms: Vec<TokenStream> = custom_variant_names
        .iter()
        .map(|variant_name| {
            quote! { PgRpcError::#variant_name(db_error) => Some(db_error) }
        })
        .collect();
    if config.errors.is_some() {
        custom_arms.push(quote! { PgRpcError::CustomError(_, db_error) => Some(db_error) });
    }

    quote! {
        impl AsDbError for PgRpcError {
            fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError> {
                match self {
                    #(#constraint_arms,)*
                    #(#custom_arms,)*
                    PgRpcError::Decode(e) => e.source.as_db_error(),
                    PgRpcError::Database(e) => e.as_db_error(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::{CheckConstraint, Constraint, UniqueConstraint};
    use crate::pg_id::PgId;
    use crate::pg_rel::{PgRel, PgRelKind};
    use smallvec::SmallVec;
    use std::collections::{BTreeMap, BTreeSet};
    use ustr::ustr;

    #[test]
    fn test_constraint_enum_generation() {
        // Create test constraints
        let mut table_constraints = BTreeMap::new();

        let users_constraints = vec![
            Constraint::Unique(UniqueConstraint {
                comment: None,
                name: ustr("users_email_key"),
                columns: SmallVec::from_slice(&[ustr("email")]),
            }),
            Constraint::Check(CheckConstraint {
                comment: None,
                name: ustr("users_age_check"),
                columns: SmallVec::from_slice(&[ustr("age")]),
                check_expression: None,
            }),
        ];

        let users_id = PgId::new(Some(ustr("public")), ustr("users"));
        table_constraints.insert(users_id, users_constraints);

        // Generate constraint enums
        let enums = generate_constraint_enums(&table_constraints);

        // Should generate one enum
        assert_eq!(enums.len(), 1);

        // Check that the generated code contains the expected enum
        let code_str = enums[0].to_string();
        assert!(code_str.contains("enum UsersConstraint"));
        assert!(code_str.contains("UsersEmailKey"));
        assert!(code_str.contains("UsersAgeCheck"));
    }

    #[test]
    fn test_unified_error_generation() {
        // Create test data
        let mut table_constraints = BTreeMap::new();

        let users_constraints = vec![Constraint::Unique(UniqueConstraint {
            comment: None,
            name: ustr("users_email_key"),
            columns: SmallVec::from_slice(&[ustr("email")]),
        })];

        let users_id = PgId::new(Some(ustr("public")), ustr("users"));
        table_constraints.insert(users_id, users_constraints);

        let custom_exceptions = BTreeSet::new();

        let mut config = Config::default();
        config
            .exceptions
            .insert("P0001".to_string(), "Custom application error".to_string());
        config
            .exceptions
            .insert("P0002".to_string(), "Invalid user input".to_string());

        // Generate unified error
        let error_code = generate_unified_error(&table_constraints, &custom_exceptions, &config);

        // Check that the generated code contains the expected error enum
        let code_str = error_code.to_string();
        assert!(code_str.contains("enum PgRpcError"));
        assert!(code_str.contains("UsersConstraint"));
        assert!(code_str.contains("Database"));
        // From<tokio_postgres::Error> is now implemented at function level, not globally
        assert!(code_str.contains("impl AsDbError for PgRpcError"));

        // Check that custom exception variants are generated
        assert!(code_str.contains("CustomApplicationError"));
        assert!(code_str.contains("InvalidUserInput"));

        // Check that helper methods are generated
        assert!(code_str.contains("impl PgRpcError"));
        assert!(code_str.contains("is_custom_application_error"));
        assert!(code_str.contains("is_invalid_user_input"));
    }

    #[test]
    fn test_collect_table_constraints() {
        let mut rel_index = RelIndex::default();

        // Create a test relation with constraints
        let users_rel = PgRel {
            oid: 1,
            id: PgId::new(Some(ustr("public")), ustr("users")),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::Unique(UniqueConstraint {
                comment: None,
                name: ustr("users_email_key"),
                columns: SmallVec::from_slice(&[ustr("email")]),
            })],
            columns: vec![ustr("id"), ustr("email"), ustr("name")],
            column_types: vec![],
        };

        rel_index.insert(1, users_rel);

        // Collect constraints
        let table_constraints = collect_table_constraints(&rel_index);

        assert_eq!(table_constraints.len(), 1);
        let users_id = PgId::new(Some(ustr("public")), ustr("users"));
        assert!(table_constraints.contains_key(&users_id));
        assert_eq!(table_constraints[&users_id].len(), 1);
    }
}
