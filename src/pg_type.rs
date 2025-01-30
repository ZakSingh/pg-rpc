use crate::codegen::ToRust;
use crate::codegen::{SchemaName, OID};
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::parse_domain::non_null_cols_from_checks;
use itertools::izip;
use quote::__private::TokenStream;
use quote::quote;
use std::collections::HashMap;
use tokio_postgres::Row;

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
        constraints: Vec<String>,
        comment: Option<String>,
    },
    Custom {
        schema: String,
        name: String,
    },
    Int32,
    Int64,
    Bool,
    Text,
    Timestamptz,
    Void,
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
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { String },
            PgType::Timestamptz => quote! { time::OffsetDateTime },
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
                "int4" => PgType::Int32,
                "int8" => PgType::Int64,
                "text" | "citext" => PgType::Text,
                "bool" => PgType::Bool,
                "timestamptz" => PgType::Timestamptz,
                _ if t.get::<&str, u32>("array_element_type") != 0 => PgType::Array {
                    schema,
                    element_type_oid: t.get("array_element_type"),
                },
                x => unimplemented!("base type not implemented {}", x),
            },
            'c' => PgType::Composite {
                schema,
                name,
                comment,
                fields: izip!(
                    t.get::<&str, Vec<&str>>("composite_field_names"),
                    t.get::<&str, Vec<u32>>("composite_field_types"),
                    t.get::<&str, Vec<bool>>("composite_field_nullables"),
                    t.get::<&str, Vec<Option<String>>>("composite_field_comments")
                )
                .map(|(name, ty, nullable, comment)| PgField {
                    name: name.to_string(),
                    type_oid: ty,
                    nullable: nullable
                        && !comment
                            .as_ref()
                            .is_some_and(|c| c.contains("@pgrpc_not_null")),
                    comment,
                })
                .collect(),
            },
            'd' => PgType::Domain {
                schema,
                name,
                comment,
                type_oid: t.get("domain_base_type"),
                constraints: t
                    .try_get("domain_composite_constraints")
                    .unwrap_or(Vec::default()),
            },
            'e' => PgType::Enum {
                schema,
                name,
                comment,
                variants: t.get("enum_variants"),
            },
            'p' => match name.as_ref() {
                "void" => PgType::Void,
                _ => unimplemented!(),
            },
            x => unimplemented!("ttype not implemented {}", x),
        };

        Ok(pg_type)
    }
}

impl ToRust for PgType {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        match self {
            PgType::Composite {
                schema,
                name,
                comment,
                fields,
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);
                let field_tokens: Vec<TokenStream> =
                    fields.into_iter().map(|f| f.to_rust(types)).collect();

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
                    #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
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
                schema,
                name,
                comment,
                type_oid,
                constraints,
            } => {
                let rs_name = sql_to_rs_ident(name, CaseType::Pascal);

                match types.get(type_oid).unwrap() {
                    PgType::Composite { fields, .. } => {
                        // If the domain wraps a composite type, we want to create a new composite type
                        // with the non-null constraints enforced by the domain rather than creating a new wrapper type.
                        // TODO: Refactor for DRY
                        let c: Vec<&str> = constraints.into_iter().map(|s| s.as_str()).collect();
                        let non_null_cols = non_null_cols_from_checks(&c).unwrap();

                        let field_tokens: Vec<TokenStream> = fields
                            .into_iter()
                            .map(|f| {
                                PgField {
                                    nullable: f.nullable && !non_null_cols.contains(&f.name),
                                    name: f.name.clone(),
                                    comment: f.comment.clone(),
                                    type_oid: f.type_oid,
                                }
                                .to_rust(types)
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

                        // TODO: Do I need custom ToSql and FromSql implementations that re-add the wrapper?
                        let comment_macro = if comment.is_some() {
                            quote! { #[doc=#comment] }
                        } else {
                            quote! {}
                        };

                        quote! {
                            #comment_macro
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
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
                    pg_type => {
                        // For any type besides composite types, domains are just a wrapper.
                        let inner = pg_type.to_rust_ident(types);
                        quote! {
                            #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
                            #[postgres(name = #name)]
                            pub struct #rs_name(#inner);
                        }
                    }
                }
            }
            PgType::Enum {
                schema,
                name,
                comment,
                variants,
            } => {
                let rs_enum_name = sql_to_rs_ident(name, CaseType::Pascal);
                let rs_variants: Vec<TokenStream> = variants
                    .into_iter()
                    .map(|sql_variant| {
                        let rs_variant = sql_to_rs_ident(&sql_variant, CaseType::Pascal);

                        quote! {
                            #[postgres(name = #sql_variant)]
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
                    #[derive(Debug, Clone, Copy, postgres_types::FromSql, postgres_types::ToSql)]
                    #[postgres(name = #name)]
                    pub enum #rs_enum_name {
                        #(#rs_variants),*
                    }
                }
            }
            // Skip base types like i32/i64 as they are already-defined primitives
            PgType::Int32
            | PgType::Int64
            | PgType::Text
            | PgType::Bool
            | PgType::Timestamptz
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
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let ident = types.get(&self.type_oid).unwrap().to_rust_ident(types);
        let field_name = sql_to_rs_ident(&self.name, CaseType::Snake);
        let pg_name = &self.name;

        let comment_macro = match self.comment.as_ref() {
            Some(comment) => quote! { #[doc=#comment] },
            None => quote! {},
        };

        if self.nullable {
            quote! {
                #comment_macro
                #[postgres(name = #pg_name)]
                pub #field_name: Option<#ident>
            }
        } else {
            quote! {
                #comment_macro
                #[postgres(name = #pg_name)]
                pub #field_name: #ident
            }
        }
    }
}
