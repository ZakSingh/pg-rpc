use crate::config::Config;
use crate::exceptions::PgException;
use crate::ident::{sql_to_rs_ident, CaseType::Pascal};
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::rel_index::RelIndex;
use quote::{quote, format_ident};
use proc_macro2::TokenStream;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use heck::ToPascalCase;

/// Collects all constraints from all tables and groups them by table
pub fn collect_table_constraints(rel_index: &RelIndex) -> HashMap<PgId, Vec<Constraint>> {
    let mut table_constraints: HashMap<PgId, Vec<Constraint>> = HashMap::new();
    
    for rel in rel_index.deref().values() {
        if !rel.constraints.is_empty() {
            table_constraints.insert(rel.id.clone(), rel.constraints.clone());
        }
    }
    
    table_constraints
}

/// Generates constraint enums for each table
pub fn generate_constraint_enums(table_constraints: &HashMap<PgId, Vec<Constraint>>) -> Vec<TokenStream> {
    let mut enums = Vec::new();
    
    for (table_id, constraints) in table_constraints {
        let table_name_str = table_id.name();
        let table_name_pascal = table_name_str.to_pascal_case();
        let enum_name = quote::format_ident!("{}Constraint", table_name_pascal);
        
        let mut variants = Vec::new();
        let mut seen_names = HashSet::new();
        
        for constraint in constraints {
            let variant_name = match constraint {
                Constraint::PrimaryKey(_) => quote::format_ident!("PrimaryKey"),
                Constraint::Unique(u) => {
                    let name = u.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                },
                Constraint::Check(c) => {
                    let name = c.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                },
                Constraint::ForeignKey(fk) => {
                    let name = fk.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                },
                Constraint::NotNull(nn) => {
                    let col_name = nn.column.to_pascal_case();
                    quote::format_ident!("{}NotNull", col_name)
                },
                Constraint::Domain(d) => {
                    let name = d.name.to_pascal_case();
                    quote::format_ident!("{}", name)
                },
                Constraint::Default(_) => continue, // Skip default constraints
            };
            
            // Avoid duplicate variant names
            if seen_names.insert(variant_name.to_string()) {
                variants.push(variant_name);
            }
        }
        
        if !variants.is_empty() {
            let enum_def = quote! {
                #[derive(Debug, Clone, Copy, PartialEq, Eq)]
                pub enum #enum_name {
                    #(#variants),*
                }
                
                impl std::fmt::Display for #enum_name {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        match self {
                            #(#enum_name::#variants => write!(f, "{}", stringify!(#variants))),*
                        }
                    }
                }
            };
            
            enums.push(enum_def);
        }
    }
    
    enums
}

/// Generates the unified PgRpcError enum
pub fn generate_unified_error(
    table_constraints: &HashMap<PgId, Vec<Constraint>>,
    custom_exceptions: &HashSet<PgException>,
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
    let mut config_variants = HashSet::new();
    for (_sql_code, description) in &config.exceptions {
        let variant_name = sql_to_rs_ident(description, Pascal);
        if config_variants.insert(variant_name.to_string()) {
            error_variants.push(quote! {
                #[error(#description ": {0}")]
                #variant_name(String)
            });
        }
    }
    
    // Add any custom SQL state exceptions not already covered by config
    let mut additional_custom_variants = HashSet::new();
    for exception in custom_exceptions {
        if let PgException::Explicit(sql_state) = exception {
            let sql_code = sql_state.code();
            // Skip if already handled by config
            if !config.exceptions.contains_key(sql_code) {
                let variant_name = exception.rs_name(config);
                if additional_custom_variants.insert(variant_name.to_string()) {
                    error_variants.push(quote! {
                        #[error("SQL state {}: {{0}}", #sql_code)]
                        #variant_name(String)
                    });
                }
            }
        }
    }
    
    // Add generic database error
    error_variants.push(quote! {
        #[error("database error")]
        Database(tokio_postgres::Error)
    });
    
    // Generate the From implementation match arms
    let from_match_arms = generate_from_impl_arms(table_constraints, custom_exceptions, config);
    
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
    
    let from_impl = quote! {
        impl From<tokio_postgres::Error> for PgRpcError {
            fn from(e: tokio_postgres::Error) -> Self {
                let Some(db_error) = e.as_db_error() else {
                    return PgRpcError::Database(e);
                };
                
                match db_error.code() {
                    #from_match_arms
                    _ => PgRpcError::Database(e)
                }
            }
        }
    };
    
    // Generate helper methods for the error type
    let helper_methods = generate_helper_methods(table_constraints, config);

    quote! {
        #error_enum_def
        
        #from_impl
        
        #helper_methods
    }
}

/// Generates the match arms for the From<tokio_postgres::Error> implementation
fn generate_from_impl_arms(
    table_constraints: &HashMap<PgId, Vec<Constraint>>,
    custom_exceptions: &HashSet<PgException>,
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
                },
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
                },
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
                },
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
                },
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
                },
                _ => {} // Skip other constraint types
            }
        }
    }
    
    // Process custom exceptions from config first
    for (sql_code, description) in &config.exceptions {
        let variant_name = sql_to_rs_ident(description, Pascal);
        custom_arms.push(quote! {
            code if code == &tokio_postgres::error::SqlState::from_code(#sql_code) => {
                let message = db_error.message().to_string();
                PgRpcError::#variant_name(message)
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
                        let message = db_error.message().to_string();
                        PgRpcError::#variant_name(message)
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
    table_constraints: &HashMap<PgId, Vec<Constraint>>,
    config: &Config,
) -> TokenStream {
    let mut methods = Vec::new();
    
    // Generate helper methods for custom exceptions from config
    for (_sql_code, description) in &config.exceptions {
        let method_name = format_ident!("is_{}", sql_to_rs_ident(description, crate::ident::CaseType::Snake).to_string());
        let variant_name = sql_to_rs_ident(description, Pascal);
        
        methods.push(quote! {
            #[doc = concat!("Returns true if this error is a ", #description, " error")]
            pub fn #method_name(&self) -> bool {
                matches!(self, PgRpcError::#variant_name(_))
            }
        });
        
        let get_method_name = format_ident!("get_{}_message", sql_to_rs_ident(description, crate::ident::CaseType::Snake).to_string());
        methods.push(quote! {
            #[doc = concat!("Returns the error message if this is a ", #description, " error")]
            pub fn #get_method_name(&self) -> Option<&str> {
                match self {
                    PgRpcError::#variant_name(msg) => Some(msg),
                    _ => None,
                }
            }
        });
    }
    
    // Generate general helper methods
    let constraint_arms: Vec<TokenStream> = table_constraints
        .keys()
        .map(|table_id| {
            let table_name_pascal = table_id.name().to_pascal_case();
            let variant_name = format_ident!("{}Constraint", table_name_pascal);
            quote! { PgRpcError::#variant_name(_, _) }
        })
        .collect();

    methods.push(quote! {
        /// Returns true if this error is related to a database constraint violation
        pub fn is_constraint_violation(&self) -> bool {
            match self {
                #(#constraint_arms)|* => true,
                _ => false,
            }
        }
    });
    
    methods.push(quote! {
        /// Returns true if this error is a generic database error
        pub fn is_database_error(&self) -> bool {
            matches!(self, PgRpcError::Database(_))
        }
    });
    
    quote! {
        impl PgRpcError {
            #(#methods)*
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::{Constraint, UniqueConstraint, CheckConstraint};
    use crate::pg_id::PgId;
    use crate::pg_rel::{PgRel, PgRelKind};
    use std::collections::{HashMap, HashSet};
    use ustr::ustr;
    use smallvec::SmallVec;

    #[test]
    fn test_constraint_enum_generation() {
        // Create test constraints
        let mut table_constraints = HashMap::new();
        
        let users_constraints = vec![
            Constraint::Unique(UniqueConstraint {
                name: ustr("users_email_key"),
                columns: SmallVec::from_slice(&[ustr("email")]),
            }),
            Constraint::Check(CheckConstraint {
                name: ustr("users_age_check"),
                columns: SmallVec::from_slice(&[ustr("age")]),
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
        let mut table_constraints = HashMap::new();
        
        let users_constraints = vec![
            Constraint::Unique(UniqueConstraint {
                name: ustr("users_email_key"),
                columns: SmallVec::from_slice(&[ustr("email")]),
            }),
        ];
        
        let users_id = PgId::new(Some(ustr("public")), ustr("users"));
        table_constraints.insert(users_id, users_constraints);
        
        let custom_exceptions = HashSet::new();
        
        let mut config = Config::default();
        config.exceptions.insert("P0001".to_string(), "Custom application error".to_string());
        config.exceptions.insert("P0002".to_string(), "Invalid user input".to_string());
        
        // Generate unified error
        let error_code = generate_unified_error(&table_constraints, &custom_exceptions, &config);
        
        // Check that the generated code contains the expected error enum
        let code_str = error_code.to_string();
        assert!(code_str.contains("enum PgRpcError"));
        assert!(code_str.contains("UsersConstraint"));
        assert!(code_str.contains("Database"));
        assert!(code_str.contains("impl From < tokio_postgres :: Error > for PgRpcError"));
        
        // Check that custom exception variants are generated
        assert!(code_str.contains("CustomApplicationError"));
        assert!(code_str.contains("InvalidUserInput"));
        
        // Check that helper methods are generated
        assert!(code_str.contains("impl PgRpcError"));
        assert!(code_str.contains("is_custom_application_error"));
        assert!(code_str.contains("get_custom_application_error_message"));
        assert!(code_str.contains("is_constraint_violation"));
        assert!(code_str.contains("is_database_error"));
    }

    #[test]
    fn test_collect_table_constraints() {
        let mut rel_index = RelIndex::default();
        
        // Create a test relation with constraints
        let users_rel = PgRel {
            oid: 1,
            id: PgId::new(Some(ustr("public")), ustr("users")),
            kind: PgRelKind::Table,
            constraints: vec![
                Constraint::Unique(UniqueConstraint {
                    name: ustr("users_email_key"),
                    columns: SmallVec::from_slice(&[ustr("email")]),
                }),
            ],
            columns: vec![ustr("id"), ustr("email"), ustr("name")],
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