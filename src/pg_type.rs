use crate::codegen::ToRust;
use crate::codegen::{SchemaName, OID};
use crate::config::Config;
use crate::ident::{sql_to_rs_ident, sql_to_rs_string, CaseType};
use crate::parse_domain::non_null_cols_from_checks;
use itertools::izip;
use quote::__private::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;
use std::fmt::Debug;
use postgres::fallible_iterator::FallibleIterator;
use postgres_types::FromSql;
use tokio_postgres::Row;

#[derive(Debug, FromSql)]
struct DomainConstraint {
    name: String,
    definition: String
}

#[derive(Debug)]
pub enum PgType {
    Array {
        schema: String,
        element_type_oid: OID,
    },
    Composite {
        schema: String,
        name: String,
        fields: Vec<PgField>,
        comment: Option<String>,
    },
    Enum {
        schema: String,
        name: String,
        variants: Vec<String>,
        comment: Option<String>,
    },
    Domain {
        schema: String,
        name: String,
        type_oid: OID,
        constraints: Vec<DomainConstraint>,
        comment: Option<String>,
    },
    Custom {
        schema: String,
        name: String,
    },
    Int16,
    Int32,
    Int64,
    Numeric,
    Bool,
    Text,
    Timestamptz,
    Date,
    INet,
    Void,
    Bytea,
    Json
}

#[derive(Debug)]
struct PgField {
    name: String,
    type_oid: OID,
    nullable: bool,
    comment: Option<String>,
}

impl PgType {
    pub fn schema(&self) -> SchemaName {
        match self {
            PgType::Composite { schema, .. } => schema,
            PgType::Enum { schema, .. } => schema,
            PgType::Array { schema, .. } => schema,
            PgType::Domain { schema, .. } => schema,
            PgType::Custom { schema, .. } => schema,
            _ => "pg_catalog",
        }
        .to_owned()
    }

    pub fn to_rust_ident(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        match self {
            // For user-defined types, qualify with schema name
            PgType::Domain { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Composite { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Enum { schema, name, .. } => {
                let schema_mod = sql_to_rs_ident(schema, CaseType::Snake);
                let type_name = sql_to_rs_ident(name, CaseType::Pascal);
                quote! { super::#schema_mod::#type_name }
            }
            PgType::Array {
                element_type_oid, ..
            } => {
                let inner = types.get(element_type_oid).unwrap().to_rust_ident(types);
                quote! { Vec<#inner> }
            }
            // Built-in types don't need schema qualification
            PgType::Int16 => quote! { i16 },
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Numeric => quote! { rust_decimal::Decimal },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { String },
            PgType::Timestamptz => quote! { time::OffsetDateTime },
            PgType::Date => quote! { time::Date },
            PgType::INet => quote! { std::net::IpAddr },
            PgType::Bytea => quote! { Vec<u8> },
            PgType::Json => quote! { serde_json::Value },
            PgType::Void => quote! { () },
            x => unimplemented!("unknown type {:?}", x),
        }
    }
}

impl TryFrom<Row> for PgType {
    type Error = tokio_postgres::Error;

    fn try_from(t: Row) -> Result<Self, Self::Error> {
        let name: String = t.get("typname");
        let comment: Option<String> = t.get("type_comment");
        let schema: String = t.get("schema_name");

        let pg_type = match t.get::<_, i8>("typtype") as u8 as char {
            'b' => match name.as_ref() {
                "int2" => PgType::Int16,
                "int4" => PgType::Int32,
                "int8" => PgType::Int64,
                "numeric" => PgType::Numeric,
                "text" | "citext" => PgType::Text,
                "bool" => PgType::Bool,
                "timestamptz" => PgType::Timestamptz,
                _ if t.get::<&str, u32>("array_element_type") != 0 => PgType::Array {
                    schema,
                    element_type_oid: t.get("array_element_type"),
                },
                "date" => PgType::Date,
                "inet" => PgType::INet,
                "bytea" => PgType::Bytea,
                "ltree" => PgType::Text,
                "json" | "jsonb" => PgType::Json,
                x => unimplemented!("base type not implemented {}", x),
            },
            'c' =>
                 PgType::Composite {
                    schema,
                    name,
                    comment,
                    fields: izip!(
                    t.get::<&str, Vec<&str>>("composite_field_names"),
                    t.get::<&str, Vec<u32>>("composite_field_types"),
                    t.get::<&str, Vec<bool>>("composite_field_nullables"),
                    t.get::<&str, Vec<Option<String>>>("composite_field_comments")
                )
                      .map(|(name, ty, nullable, comment)| {
                          PgField {
                              name: name.to_string(),
                              type_oid: ty,
                              nullable: nullable
                                && !comment
                                .as_ref()
                                .is_some_and(|c| c.contains("@pgrpc_not_null")),
                              comment,
                          }
                      })
                      .collect(),
            },
            'd' => {
                let constraints =
                izip!(t.try_get::<_, Vec<String>>("domain_constraint_names").unwrap_or(Vec::default()), t.try_get::<_, Vec<String>>("domain_composite_constraints")
                  .unwrap_or(Vec::default())).map(|(name, definition)| DomainConstraint { name, definition }).collect();

                PgType::Domain {
                    schema,
                    name,
                    comment,
                    type_oid: t.get("domain_base_type"),
                    constraints
                }
            },
            'e' => PgType::Enum {
                schema,
                name,
                comment,
                variants: t.get("enum_variants"),
            },
            'p' => match name.as_ref() {
                "void" => PgType::Void,
                p => unimplemented!("pseudo type not implemented: {}", p),
            },
            x => unimplemented!("ttype not implemented {}", x),
        };

        Ok(pg_type)
    }
}

impl ToRust for PgType {
    fn to_rust(&self, types: &HashMap<OID, PgType>, config: &Config) -> TokenStream {
        match self {
            PgType::Composite {
                name,
                comment,
                fields,
                ..
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);
                let field_tokens: Vec<TokenStream> = fields
                    .into_iter()
                    .map(|f| f.to_rust(types, config))
                    .collect();

                let comment_macro = if comment.is_some() {
                    quote! { #[doc=#comment] }
                } else {
                    quote! {}
                };

                let field_mappings: Vec<_> = fields
                    .into_iter()
                    .map(|f| {
                        let sql_name = &f.name;
                        let rs_name = sql_to_rs_ident(&f.name, CaseType::Snake);

                        quote! { #rs_name: row.try_get(#sql_name)? }
                    })
                    .collect();

                quote! {
                    #comment_macro
                    #[derive(Clone, Debug, postgres_types::ToSql, postgres_types::FromSql, serde::Serialize, serde::Deserialize)]
                    #[postgres(name = #name)]
                    pub struct #rs_name {
                        #(#field_tokens),*
                    }

                    impl TryFrom<tokio_postgres::Row> for #rs_name {
                        type Error = tokio_postgres::Error;

                        fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                            Ok(Self {
                                #(#field_mappings),*
                            })
                        }
                    }
                }
            }
            PgType::Domain {
                name,
                comment,
                type_oid,
                constraints,
                ..
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);

                match types.get(type_oid).unwrap() {
                    PgType::Composite { fields, name: pg_inner_name, .. } => {
                        let rs_dom_name = format_ident!("Dom{}", sql_to_rs_string(name, CaseType::Pascal));
                        let rs_dom_inner_name = format_ident!("Inner{}", sql_to_rs_string(name, CaseType::Pascal));

                        // If the domain wraps a composite type, we want to create a new composite type
                        // with the non-null constraints enforced by the domain rather than creating a new wrapper type.
                        // TODO: Refactor for DRY
                        let c: Vec<&str> = constraints.into_iter().map(|s| s.definition.as_str()).collect();
                        let non_null_cols = non_null_cols_from_checks(&c).unwrap();

                        // TODO: strip out 'postgres()
                        let field_tokens: Vec<TokenStream> = fields
                            .into_iter()
                            .map(|f| {
                                PgField {
                                    nullable: f.nullable && !non_null_cols.contains(&f.name),
                                    name: f.name.clone(),
                                    comment: f.comment.clone(),
                                    type_oid: f.type_oid,
                                }
                                .to_rust_inner(types, false)
                            })
                            .collect();

                        let field_mappings: Vec<_> = fields
                            .into_iter()
                            .map(|f| {
                                let sql_name = &f.name;
                                let rs_name = sql_to_rs_ident(&f.name, CaseType::Snake);

                                quote! { #rs_name: row.try_get(#sql_name)? }
                            })
                            .collect();

                        let comment_macro = if comment.is_some() {
                            quote! { #[doc=#comment] }
                        } else {
                            quote! {}
                        };

                        quote! {
                            #comment_macro
                            #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
                            pub struct #rs_name {
                                #(#field_tokens),*
                            }

                            /// Internal; used for serialization/deserialization only
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql, serde::Deserialize, serde::Serialize)]
                            #[postgres(name = #pg_inner_name)]
                            struct #rs_dom_inner_name {
                                #(#field_tokens),*
                            }

                            /// Internal; used for serialization/deserialization only
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql, serde::Deserialize, serde::Serialize)]
                            #[postgres(name = #name)]
                            struct #rs_dom_name(#rs_dom_inner_name);


                            /// Dispatch FromSql implementation to internal domain wrapper struct
                            impl<'a> postgres_types::FromSql<'a> for #rs_name {
                                fn from_sql(_type: &postgres_types::Type, buf: &'a [u8]) -> std::result::Result<#rs_name, Box<dyn std::error::Error + std::marker::Sync + std::marker::Send>> {
                                    <#rs_dom_name as postgres_types::FromSql>::from_sql(_type, buf).map(|dom| unsafe { std::mem::transmute::<#rs_dom_inner_name, #rs_name>(dom.0) })
                                }

                                fn accepts(type_: &postgres_types::Type) -> bool {
                                    <#rs_dom_name as postgres_types::FromSql>::accepts(type_)
                                }
                            }

                            impl TryFrom<tokio_postgres::Row> for #rs_name {
                                type Error = tokio_postgres::Error;

                                fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
                                    Ok(Self {
                                        #(#field_mappings),*
                                    })
                                }
                            }

                            impl ToSql for #rs_name {
                                fn to_sql(
                                    &self,
                                    ty: &Type,
                                    out: &mut BytesMut,
                                ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                                    let inner = unsafe { std::mem::transmute::<#rs_name, #rs_dom_inner_name>(self.clone()) };

                                    let inner_ty = match ty.kind() {
                                        postgres_types::Kind::Domain(inner) => inner,
                                        _ => return Err("Expected domain type".into()),
                                    };

                                    inner.to_sql(inner_ty, out)
                                }

                                fn accepts(ty: &Type) -> bool {
                                    // Match domain by name
                                    ty.name() == #name
                                }
                                postgres_types::to_sql_checked!();
                            }
                        }
                    }
                    pg_type => {
                        // For any type besides composite types, domains are just a wrapper.
                        let inner = pg_type.to_rust_ident(types);
                        quote! {
                            #[derive(Debug, Clone, derive_more::Deref, serde::Serialize, serde::Deserialize, postgres_types::ToSql, postgres_types::FromSql)]
                            #[postgres(name = #name)]
                            pub struct #rs_name(pub #inner);
                        }
                    }
                }
            }
            PgType::Enum {
                name,
                comment,
                variants,
                ..
            } => {
                let rs_enum_name = sql_to_rs_ident(name, CaseType::Pascal);
                let rs_variants: Vec<TokenStream> = variants
                    .into_iter()
                    .map(|sql_variant| {
                        let rs_variant = sql_to_rs_ident(&sql_variant, CaseType::Pascal);

                        quote! {
                            #[postgres(name = #sql_variant)]
                            #[serde(rename = #sql_variant)]
                            #rs_variant
                        }
                    })
                    .collect();

                let comment_macro = if comment.is_some() {
                    quote! { #[doc=#comment] }
                } else {
                    quote! {}
                };

                quote! {
                    #comment_macro
                    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, postgres_types::FromSql, postgres_types::ToSql, serde::Deserialize, serde::Serialize)]
                    #[postgres(name = #name)]
                    pub enum #rs_enum_name {
                        #(#rs_variants),*
                    }
                }
            }
            // Skip base types like i32/i64 as they are already-defined primitives
            PgType::Int32
            | PgType::Int64
            | PgType::Int16
            | PgType::Text
            | PgType::Bool
            | PgType::Timestamptz
            | PgType::INet
            | PgType::Date
            | PgType::Bytea
            | PgType::Numeric
            | PgType::Json
            | PgType::Void => {
                quote! {}
            }
            // No need to create type aliases for arrays. Instead they'll be used as Vec<Inner>
            PgType::Array { .. } => quote! {},
            x => unimplemented!("Unhandled PgType {:?}", x),
        }
    }
}

impl ToRust for PgField {
    fn to_rust(&self, types: &HashMap<OID, PgType>, config: &Config) -> TokenStream {
        self.to_rust_inner(types, true)
    }
}

impl PgField {
    pub fn to_rust_inner(&self, types: &HashMap<OID, PgType>, include_pg_derive: bool) -> TokenStream {
        let ident = types.get(&self.type_oid).unwrap().to_rust_ident(types);
        let field_name = sql_to_rs_ident(&self.name, CaseType::Snake);
        let pg_name = &self.name;

        let comment_macro = match self.comment.as_ref() {
            Some(comment) => quote! { #[doc=#comment] },
            None => quote! {},
        };

        let pg_macro = if include_pg_derive { quote! { #[postgres(name = #pg_name)] } } else { quote! {} };
        let option_macro = if self.nullable { quote! { Option<#ident> } } else { quote! {#ident} };

        quote! {
            #comment_macro
            #pg_macro
            #[serde(rename = #pg_name)]
            pub #field_name: #option_macro
        }
    }
}
