use crate::config::Config;
use crate::exceptions::PgException;
use crate::ident::{ident_tokens, sanitize_ident, sql_to_rs_ident, CaseType, CaseType::Pascal};
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::pg_rel::PgRelKind;
use crate::rel_index::RelIndex;
use heck::ToPascalCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

/// The name of a table's constraint enum, which is also the name of the
/// variant that carries it in `PgRpcError` and in each function's and query's
/// error enum: `{Table}Constraint`.
///
/// Tables of the same name in different schemas would get the same enum, so
/// when a name is shared every table sharing it is qualified with its
/// schema: `{Schema}{Table}Constraint`.
pub fn constraint_enum_ident(
    table_id: &PgId,
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
) -> TokenStream {
    let table = table_id.name().to_pascal_case();
    let shared = table_constraints
        .keys()
        .any(|other| other != table_id && other.name().to_pascal_case() == table);
    let name = if shared {
        format!("{}{}Constraint", table_id.schema().to_pascal_case(), table)
    } else {
        format!("{table}Constraint")
    };
    ident_tokens(&sanitize_ident(name, table_id.name(), Pascal))
}

/// The variant of a table's constraint enum for `constraint`: the constraint
/// name in Pascal case, `PrimaryKey`, or `{Column}NotNull`. `None` for
/// constraints that cannot be violated (defaults).
pub fn constraint_variant_ident(constraint: &Constraint) -> Option<TokenStream> {
    let pascal = |name: &str| {
        let ident = sanitize_ident(name.to_pascal_case(), name, Pascal);
        ident.strip_prefix("r#").unwrap_or(&ident).to_string()
    };
    let name = match constraint {
        Constraint::PrimaryKey(_) => "PrimaryKey".to_string(),
        Constraint::Unique(u) => pascal(&u.name),
        Constraint::Check(c) => pascal(&c.name),
        Constraint::ForeignKey(fk) => pascal(&fk.name),
        Constraint::NotNull(nn) => format!("{}NotNull", pascal(&nn.column)),
        Constraint::Domain(d) => pascal(&d.name),
        Constraint::Exclusion(x) => pascal(&x.name),
        Constraint::Default(_) => return None,
    };
    Some(ident_tokens(&name))
}

/// The SQLSTATE class a constraint violation is reported under, which decides
/// what identifies the constraint in the `DbError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Violation {
    /// 23505, by (schema, table, constraint).
    Unique,
    /// 23514 on a table, by (schema, table, constraint).
    Check,
    /// 23514 on a domain, by (schema, datatype, constraint): Postgres
    /// reports the domain, not the table whose column has that type.
    DomainCheck,
    /// 23503, by (schema, table, constraint).
    ForeignKey,
    /// 23502, by (schema, table, column).
    NotNull,
    /// 23P01, by (schema, table, constraint).
    Exclusion,
}

/// The arms of `match db_error.code() { .. }` that turn a constraint
/// violation into `#error_enum::{Table}Constraint(variant, db_error)`, for the
/// constraints in `constraints`. Shared by the function and query error
/// enums; anything unmatched falls through to
/// `#error_enum::Other(PgRpcError::Database(e))`.
///
/// A violation is matched on everything Postgres reports about it: the
/// schema and table (or domain) as well as the constraint name, since CHECK
/// and FOREIGN KEY names are only unique per table.
///
/// A constraint inherited by a partition or child table is reported with the
/// child's table name, which is not known here. So a constraint whose name
/// only one of the tables in `constraints` uses is also matched by name
/// alone, after the exact matches.
pub fn constraint_violation_arms(
    error_enum: &TokenStream,
    constraints: &BTreeMap<PgId, Vec<Constraint>>,
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
) -> Vec<TokenStream> {
    // Per violation class: (constraint name, exact pattern, result).
    let mut found: BTreeMap<Violation, Vec<(String, TokenStream, TokenStream)>> = BTreeMap::new();
    // Two tables with a column of one domain share its CHECK; the violation
    // does not say which table, so the first one gets the arm.
    let mut seen: BTreeSet<(Violation, String, String, String)> = BTreeSet::new();

    for (table_id, table) in constraints {
        let table_enum = constraint_enum_ident(table_id, table_constraints);
        for constraint in table {
            let Some(variant) = constraint_variant_ident(constraint) else {
                continue;
            };
            let (violation, schema, relation, name) = match constraint {
                Constraint::PrimaryKey(_) | Constraint::Unique(_) => (
                    Violation::Unique,
                    table_id.schema().to_string(),
                    table_id.name().to_string(),
                    constraint.name().to_string(),
                ),
                Constraint::Check(_) => (
                    Violation::Check,
                    table_id.schema().to_string(),
                    table_id.name().to_string(),
                    constraint.name().to_string(),
                ),
                Constraint::ForeignKey(_) => (
                    Violation::ForeignKey,
                    table_id.schema().to_string(),
                    table_id.name().to_string(),
                    constraint.name().to_string(),
                ),
                Constraint::Exclusion(_) => (
                    Violation::Exclusion,
                    table_id.schema().to_string(),
                    table_id.name().to_string(),
                    constraint.name().to_string(),
                ),
                Constraint::NotNull(nn) => (
                    Violation::NotNull,
                    table_id.schema().to_string(),
                    table_id.name().to_string(),
                    nn.column.to_string(),
                ),
                Constraint::Domain(d) => {
                    let (Some(domain), Some(schema)) = (&d.domain, &d.domain_schema) else {
                        continue;
                    };
                    (
                        Violation::DomainCheck,
                        schema.clone(),
                        domain.clone(),
                        d.name.to_string(),
                    )
                }
                Constraint::Default(_) => continue,
            };
            if !seen.insert((violation, schema.clone(), relation.clone(), name.clone())) {
                continue;
            }
            found.entry(violation).or_default().push((
                name.clone(),
                quote! { (Some(#schema), Some(#relation), Some(#name)) },
                quote! {
                    #error_enum::#table_enum(
                        super::errors::#table_enum::#variant,
                        db_error.to_owned()
                    )
                },
            ));
        }
    }

    let mut arms: BTreeMap<Violation, Vec<TokenStream>> = found
        .into_iter()
        .map(|(violation, entries)| {
            let exact = entries
                .iter()
                .map(|(_, pattern, result)| quote! { #pattern => #result });
            // Not-null violations name a column and domain CHECKs a type;
            // neither is reported differently for a child table.
            let by_name_alone = entries
                .iter()
                .filter(|(name, _, _)| {
                    !matches!(violation, Violation::NotNull | Violation::DomainCheck)
                        && entries.iter().filter(|(other, _, _)| other == name).count() == 1
                })
                .map(|(name, _, result)| quote! { (_, _, Some(#name)) => #result });
            (violation, exact.chain(by_name_alone).collect())
        })
        .collect();

    let fallback = quote! { #error_enum::Other(super::errors::PgRpcError::Database(e)) };
    let by_constraint = quote! { (db_error.schema(), db_error.table(), db_error.constraint()) };
    let mut take = |violation: Violation| arms.remove(&violation).unwrap_or_default();
    let mut result = Vec::new();

    let unique = take(Violation::Unique);
    if !unique.is_empty() {
        result.push(quote! {
            &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => {
                match #by_constraint {
                    #(#unique,)*
                    _ => #fallback
                }
            },
        });
    }

    let check = take(Violation::Check);
    let domain_check = take(Violation::DomainCheck);
    if !check.is_empty() || !domain_check.is_empty() {
        let by_domain = if domain_check.is_empty() {
            fallback.clone()
        } else {
            quote! {
                match (db_error.schema(), db_error.datatype(), db_error.constraint()) {
                    #(#domain_check,)*
                    _ => #fallback
                }
            }
        };
        let body = if check.is_empty() {
            by_domain
        } else {
            quote! {
                match #by_constraint {
                    #(#check,)*
                    _ => #by_domain
                }
            }
        };
        result.push(quote! {
            &tokio_postgres::error::SqlState::CHECK_VIOLATION => {
                #body
            },
        });
    }

    let foreign_key = take(Violation::ForeignKey);
    if !foreign_key.is_empty() {
        result.push(quote! {
            &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION => {
                match #by_constraint {
                    #(#foreign_key,)*
                    _ => #fallback
                }
            },
        });
    }

    let not_null = take(Violation::NotNull);
    if !not_null.is_empty() {
        result.push(quote! {
            &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => {
                match (db_error.schema(), db_error.table(), db_error.column()) {
                    #(#not_null,)*
                    _ => #fallback
                }
            },
        });
    }

    let exclusion = take(Violation::Exclusion);
    if !exclusion.is_empty() {
        result.push(quote! {
            &tokio_postgres::error::SqlState::EXCLUSION_VIOLATION => {
                match #by_constraint {
                    #(#exclusion,)*
                    _ => #fallback
                }
            },
        });
    }

    result
}

/// The error enum of one function or query, with its conversions: from
/// `tokio_postgres::Error` (classifying the failure), to and from
/// `PgRpcError`, from `DecodeError`, and `AsDbError`.
///
/// `constraints` are the tables whose constraints the item can violate, each
/// a `{Table}Constraint` variant. `exceptions` are everything else it can
/// raise; they become variants in the order given. Two exceptions that map to
/// one variant name (the same exception listed twice, or two SQLSTATEs
/// configured with one description) share the variant, and every such
/// SQLSTATE is classified as it.
pub fn generate_error_enum(
    error_enum_name: &TokenStream,
    constraints: &BTreeMap<PgId, Vec<Constraint>>,
    table_constraints: &BTreeMap<PgId, Vec<Constraint>>,
    exceptions: &[PgException],
    config: &Config,
) -> TokenStream {
    let mut error_variants = Vec::new();
    let mut to_pgrpc_arms = Vec::new();
    let mut as_db_error_arms = Vec::new();

    for table_id in constraints.keys() {
        let table_name_str = table_id.name();
        let variant_name = constraint_enum_ident(table_id, table_constraints);

        error_variants.push(quote! {
            #[error("{} table constraint violation", #table_name_str)]
            #variant_name(super::errors::#variant_name, #[source] tokio_postgres::error::DbError)
        });
        to_pgrpc_arms.push(quote! {
            #error_enum_name::#variant_name(constraint, db_error) => {
                super::errors::PgRpcError::#variant_name(constraint, db_error)
            }
        });
        as_db_error_arms.push(quote! {
            #error_enum_name::#variant_name(_, db_error) => Some(db_error)
        });
    }

    // Arms of `match db_error.code()`: by SQLSTATE, and, inside the JSON-hint
    // arm, by the name of a typed error (an errors-schema payload).
    let mut sql_state_arms = Vec::new();
    let mut typed_error_arms = Vec::new();

    let mut handled_variants = BTreeSet::new();
    let mut sql_state_variants = BTreeSet::new();
    let mut handled_codes = BTreeSet::new();
    for exception in exceptions {
        let variant_name = exception.rs_name(config);
        let is_new = handled_variants.insert(variant_name.to_string());
        match exception {
            PgException::Explicit(sql_state) => {
                let code = sql_state.code();
                if is_new {
                    sql_state_variants.insert(variant_name.to_string());
                    let message = match config.exceptions.get(code) {
                        Some(description) => quote! { #[error(#description)] },
                        None => quote! { #[error("SQL state {}", #code)] },
                    };
                    error_variants.push(quote! {
                        #message
                        #variant_name(#[source] tokio_postgres::error::DbError)
                    });
                    to_pgrpc_arms.push(quote! {
                        #error_enum_name::#variant_name(db_error) => {
                            super::errors::PgRpcError::#variant_name(db_error)
                        }
                    });
                    as_db_error_arms.push(quote! {
                        #error_enum_name::#variant_name(db_error) => Some(db_error)
                    });
                }
                if sql_state_variants.contains(&variant_name.to_string())
                    && handled_codes.insert(code.to_string())
                {
                    sql_state_arms.push(quote! {
                        code if code == &tokio_postgres::error::SqlState::from_code(#code) => {
                            #error_enum_name::#variant_name(db_error.to_owned())
                        }
                    });
                }
            }
            PgException::CustomError(error_name) => {
                if !is_new {
                    continue;
                }
                let error_name_str = error_name.as_str();
                error_variants.push(quote! {
                    #[error("{}", #error_name_str)]
                    #variant_name(super::custom_errors::#variant_name, #[source] tokio_postgres::error::DbError)
                });
                to_pgrpc_arms.push(quote! {
                    #error_enum_name::#variant_name(payload, db_error) => {
                        super::errors::PgRpcError::CustomError(
                            super::custom_errors::CustomError::#variant_name(payload),
                            db_error,
                        )
                    }
                });
                as_db_error_arms.push(quote! {
                    #error_enum_name::#variant_name(_, db_error) => Some(db_error)
                });
                typed_error_arms.push(quote! {
                    Some(#error_name_str) => {
                        if let Ok(payload) = serde_json::from_value(json.clone()) {
                            #error_enum_name::#variant_name(payload, db_error.to_owned())
                        } else {
                            #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                        }
                    }
                });
            }
        }
    }

    // Catch-all for everything not detected.
    error_variants.push(quote! {
        #[error(transparent)]
        Other(super::errors::PgRpcError)
    });

    // Typed errors are identified by the JSON hint, not the SQLSTATE, so that
    // arm leads: a typed error raised with a mapped SQLSTATE is still the
    // typed error. Then constraint classes, then SQLSTATE codes.
    let mut from_postgres_arms = Vec::new();
    if !typed_error_arms.is_empty() {
        from_postgres_arms.push(quote! {
            _code if db_error.hint() == Some("application/json") => {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(db_error.message()) {
                    match json.get("type").and_then(|t| t.as_str()) {
                        #(#typed_error_arms,)*
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    }
                } else {
                    #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            }
        });
    }
    from_postgres_arms.extend(constraint_violation_arms(
        error_enum_name,
        constraints,
        table_constraints,
    ));
    from_postgres_arms.extend(sql_state_arms);

    quote! {
        #[derive(Debug, thiserror::Error)]
        pub enum #error_enum_name {
            #(#error_variants),*
        }

        impl From<tokio_postgres::Error> for #error_enum_name {
            fn from(e: tokio_postgres::Error) -> Self {
                match e.as_db_error() {
                    Some(db_error) => match db_error.code() {
                        #(#from_postgres_arms)*
                        _ => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                    },
                    None => #error_enum_name::Other(super::errors::PgRpcError::Database(e))
                }
            }
        }

        impl From<#error_enum_name> for super::errors::PgRpcError {
            fn from(err: #error_enum_name) -> Self {
                match err {
                    #error_enum_name::Other(e) => e,
                    #(#to_pgrpc_arms),*
                }
            }
        }

        impl From<super::errors::PgRpcError> for #error_enum_name {
            fn from(err: super::errors::PgRpcError) -> Self {
                #error_enum_name::Other(err)
            }
        }

        impl From<super::errors::DecodeError> for #error_enum_name {
            fn from(err: super::errors::DecodeError) -> Self {
                #error_enum_name::Other(super::errors::PgRpcError::Decode(err))
            }
        }

        impl super::errors::AsDbError for #error_enum_name {
            fn as_db_error(&self) -> Option<&tokio_postgres::error::DbError> {
                match self {
                    #(#as_db_error_arms,)*
                    #error_enum_name::Other(e) => e.as_db_error(),
                }
            }
        }
    }
}

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
            table_constraints.insert(rel.id, non_default_constraints);
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
        let enum_name = constraint_enum_ident(table_id, table_constraints);

        let mut variants: Vec<(TokenStream, &Constraint)> = Vec::new();
        let mut seen_names = BTreeSet::new();

        for constraint in constraints {
            let Some(variant_name) = constraint_variant_ident(constraint) else {
                continue; // Default constraints cannot be violated
            };

            // Avoid duplicate variant names
            if seen_names.insert(variant_name.to_string()) {
                variants.push((variant_name, constraint));
            }
        }

        if variants.is_empty() {
            continue;
        }

        let variant_idents: Vec<&TokenStream> = variants.iter().map(|(v, _)| v).collect();
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
                    let field = match crate::annotations::Annotations::of(constraint.comment()).field {
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
    for table_id in table_constraints.keys() {
        let table_name_str = table_id.name();
        let variant_name = constraint_enum_ident(table_id, table_constraints);

        error_variants.push(quote! {
            #[error("{} table constraint violation", #table_name_str)]
            #variant_name(#variant_name, #[source] tokio_postgres::error::DbError)
        });
    }

    // Add custom SQL state exceptions from config
    let mut config_variants = BTreeSet::new();
    for description in config.exceptions.values() {
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

    // A `:one` query that matched nothing.
    error_variants.push(quote! {
        /// A query declared `:one` returned no rows. `item` is the query's name.
        #[error("{item}: query returned no rows")]
        NoRows { item: &'static str }
    });

    // Add generic database error - using transparent to show underlying SQL error
    error_variants.push(quote! {
        #[error(transparent)]
        Database(tokio_postgres::Error)
    });

    let error_enum_def = quote! {
        #[derive(Debug, thiserror::Error)]
        pub enum PgRpcError {
            #(#error_variants,)*
        }
    };

    let helper_methods = generate_helper_methods(config);
    let as_db_error_impl = generate_as_db_error_impl(table_constraints, custom_exceptions, config);

    quote! {
        #error_enum_def

        #helper_methods

        #as_db_error_impl
    }
}

/// Generates helper methods for the PgRpcError enum
fn generate_helper_methods(config: &Config) -> TokenStream {
    let mut methods = Vec::new();

    // Only generate helper methods for custom exceptions from config
    for description in config.exceptions.values() {
        let snake = crate::ident::sql_to_rs_string(description, CaseType::Snake);
        let method_name = format_ident!("is_{}", snake.strip_prefix("r#").unwrap_or(&snake));
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
            let variant_name = constraint_enum_ident(table_id, table_constraints);
            quote! { PgRpcError::#variant_name(_, db_error) => Some(db_error) }
        })
        .collect();

    // Collect all custom exception variant names for arms that return None
    let mut custom_variant_names = Vec::new();

    // Custom exceptions from config
    for description in config.exceptions.values() {
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
                    PgRpcError::Decode(e) => e.source.downcast_ref::<tokio_postgres::Error>().and_then(|e| e.as_db_error()),
                    PgRpcError::NoRows { .. } => None,
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
                columns: SmallVec::from([ustr("email")]),
                conditional: false,
            }),
            Constraint::Check(CheckConstraint {
                comment: None,
                name: ustr("users_age_check"),
                columns: SmallVec::from([ustr("age")]),
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
            columns: SmallVec::from([ustr("email")]),
            conditional: false,
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
                columns: SmallVec::from([ustr("email")]),
                conditional: false,
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

    fn check(name: &str) -> Constraint {
        Constraint::Check(CheckConstraint {
            comment: None,
            name: ustr(name),
            columns: SmallVec::from([ustr("qty")]),
            check_expression: None,
        })
    }

    #[test]
    fn tables_sharing_a_name_get_schema_qualified_enums() {
        let a_item = PgId::new(Some(ustr("a")), ustr("item"));
        let b_item = PgId::new(Some(ustr("b")), ustr("item"));
        let other = PgId::new(Some(ustr("a")), ustr("other"));
        let table_constraints = BTreeMap::from([
            (a_item, vec![check("positive")]),
            (b_item, vec![check("positive")]),
            (other, vec![check("1_positive")]),
        ]);

        let name = |id: &PgId| constraint_enum_ident(id, &table_constraints).to_string();
        assert_eq!(name(&a_item), "AItemConstraint");
        assert_eq!(name(&b_item), "BItemConstraint");
        // Unshared names are unchanged.
        assert_eq!(name(&other), "OtherConstraint");

        let enums: String = generate_constraint_enums(&table_constraints)
            .iter()
            .map(|e| e.to_string())
            .collect();
        assert!(enums.contains("enum AItemConstraint"));
        assert!(enums.contains("enum BItemConstraint"));
        // A constraint name starting with a digit is still an identifier.
        assert!(enums.contains("_1Positive"));
    }

    #[test]
    fn violations_match_on_schema_table_and_constraint() {
        let a_item = PgId::new(Some(ustr("a")), ustr("item"));
        let a_other = PgId::new(Some(ustr("a")), ustr("other"));
        let table_constraints = BTreeMap::from([
            (a_item, vec![check("positive"), check("only_item")]),
            (a_other, vec![check("positive")]),
        ]);

        let arms: String = constraint_violation_arms(
            &quote! { FooError },
            &table_constraints,
            &table_constraints,
        )
        .iter()
        .map(|arm| arm.to_string())
        .collect();

        assert!(arms.contains("db_error . schema () , db_error . table () , db_error . constraint ()"));
        assert!(arms.contains(r#"(Some ("a") , Some ("item") , Some ("positive")) => FooError :: ItemConstraint"#));
        assert!(arms.contains(r#"(Some ("a") , Some ("other") , Some ("positive")) => FooError :: OtherConstraint"#));
        // Only an unambiguous name is also matched on its own (for
        // partitions, which report their own table name).
        assert!(arms.contains(r#"(_ , _ , Some ("only_item")) => FooError :: ItemConstraint"#));
        assert!(!arms.contains(r#"(_ , _ , Some ("positive"))"#));
    }

    fn error_enum(exceptions: &[PgException], descriptions: &[(&str, &str)]) -> String {
        let config = Config {
            exceptions: descriptions
                .iter()
                .map(|(code, description)| (code.to_string(), description.to_string()))
                .collect(),
            ..Config::default()
        };
        generate_error_enum(
            &quote! { DoItError },
            &BTreeMap::new(),
            &BTreeMap::new(),
            exceptions,
            &config,
        )
        .to_string()
        .replace(' ', "")
    }

    fn explicit(code: &str) -> PgException {
        PgException::Explicit(crate::sql_state::SqlState::from_code(code))
    }

    #[test]
    fn error_enum_variants_follow_the_exception_list() {
        let code = error_enum(
            &[
                explicit("P0002"),
                PgException::CustomError("out_of_stock".to_string()),
                explicit("M0001"),
            ],
            &[],
        );
        let at = |needle: &str| code.find(needle).unwrap_or_else(|| panic!("{needle} in {code}"));
        assert!(at("NoDataFound(#[source]") < at("OutOfStock(super::custom_errors::OutOfStock"));
        assert!(at("OutOfStock(super::custom_errors::OutOfStock") < at("M0001(#[source]"));
        // The JSON-hint arm leads the SQLSTATE arms.
        assert!(at(r#"db_error.hint()==Some("application/json")"#) < at(r#"from_code("P0002")"#));
    }

    #[test]
    fn exceptions_sharing_a_variant_are_emitted_once() {
        let code = error_enum(
            &[
                explicit("P0001"),
                explicit("P0002"),
                explicit("P0001"),
                PgException::CustomError("denied".to_string()),
                PgException::CustomError("denied".to_string()),
            ],
            &[("P0001", "app error"), ("P0002", "app error")],
        );
        assert_eq!(code.matches("AppError(#[source]").count(), 1, "{code}");
        assert_eq!(code.matches("DoItError::AppError(db_error)=>Some(db_error)").count(), 1);
        assert_eq!(code.matches("PgRpcError::AppError(db_error)").count(), 1);
        // Both SQLSTATEs are still classified as the shared variant, once each.
        assert_eq!(code.matches(r#"from_code("P0001")"#).count(), 1);
        assert_eq!(code.matches(r#"from_code("P0002")"#).count(), 1);
        assert_eq!(code.matches("Denied(super::custom_errors::Denied").count(), 1);
        assert_eq!(code.matches(r#"Some("denied")=>"#).count(), 1);
    }
}
