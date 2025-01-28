use crate::ident::{sql_to_rs_ident, CaseType};
use crate::infer_types::{FunctionName, SchemaName, ToRust, OID};
use crate::pg_type::PgType;
use crate::pgsql_check::{PgSqlIssue, PgSqlReport};
use quote::__private::TokenStream;
use quote::quote;
use std::collections::HashMap;
use tokio_postgres::Row;

#[derive(Debug)]
pub struct PgFn {
    oid: OID,
    pub(crate) schema: SchemaName,
    pub(crate) name: FunctionName,
    args: Vec<PgArg>,
    return_type_oid: OID,
    returns_set: bool,
    definition: String,
    comment: Option<String>,
    issues: Vec<PgSqlIssue>,
}

#[derive(Debug, Clone)]
pub struct PgArg {
    name: String,
    type_oid: OID,
    has_default: bool,
}

impl PgArg {
    pub fn rs_name(&self) -> TokenStream {
        sql_to_rs_ident(&self.name, CaseType::Snake)
    }
}

impl PgFn {
    pub fn rs_name(&self) -> TokenStream {
        sql_to_rs_ident(&self.name, CaseType::Snake)
    }
}

impl TryFrom<Row> for PgFn {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let arg_type_oids: Vec<OID> = row.try_get("arg_oids")?;
        let arg_names: Vec<String> = row.try_get("arg_names")?;
        let arg_defaults: Vec<bool> = row.try_get("has_defaults")?;

        let args = arg_type_oids
            .iter()
            .zip(arg_names)
            .zip(arg_defaults)
            .map(|((oid, name), has_default)| PgArg {
                name: name.clone(),
                type_oid: *oid,
                has_default,
            })
            .collect();

        Ok(Self {
            oid: row.try_get("oid")?,
            name: row.try_get("function_name")?,
            schema: row.try_get("schema_name")?,
            definition: row.try_get("function_definition")?,
            returns_set: row.try_get("returns_set")?,
            comment: row.try_get("comment")?,
            issues: row.try_get::<&str, PgSqlReport>("plpgsql_check")?.issues,
            args,
            return_type_oid: row.try_get("return_type")?,
        })
    }
}

impl ToRust for PgArg {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let name = sql_to_rs_ident(&self.name, CaseType::Snake);
        let ty = match types.get(&self.type_oid).unwrap() {
            PgType::Int32 => quote! { i32 },
            PgType::Int64 => quote! { i64 },
            PgType::Bool => quote! { bool },
            PgType::Text => quote! { &str },
            t @ PgType::Enum { .. } => {
                // Enums are passed by copy
                let id = t.to_rust_ident(types);
                quote! { #id }
            }
            t => {
                let id = t.to_rust_ident(types);
                quote! { &#id }
            }
        };

        if self.has_default {
            quote! {
                #name: Option<#ty>
            }
        } else {
            quote! {
                #name: #ty
            }
        }
    }
}

impl ToRust for PgFn {
    fn to_rust(&self, types: &HashMap<OID, PgType>) -> TokenStream {
        let fn_body = {
            let (opt_args, req_args): (Vec<PgArg>, Vec<PgArg>) =
                self.args.iter().cloned().partition(|n| n.has_default);

            // Generate required query parameters positionally ($1, $2, ..., $n)
            let required_query_params = (1..=self.args.len() - opt_args.len())
                .map(|i| format!("${}", i))
                .collect::<Vec<_>>()
                .join(", ");

            let req_arg_names: Vec<TokenStream> = req_args.iter().map(|a| a.rs_name()).collect();
            let opt_arg_names: Vec<TokenStream> = opt_args.iter().map(|a| a.rs_name()).collect();
            let opt_arg_sql_names: Vec<&str> = opt_args.iter().map(|a| a.name.as_str()).collect();

            let query_string = if self.returns_set {
                format!("select * from {}.{}", self.schema, self.name)
            } else {
                format!("select {}.{}", self.schema, self.name)
            };

            let query = if self.returns_set {
                quote! {
                    client
                        .query(&query, &params)
                        .await
                        .and_then(|rows| {
                            rows.into_iter().map(TryInto::try_into).collect()
                        })
                }
            } else {
                quote! {
                    client
                        .query_one(&query, &params)
                        .await
                        .and_then(|r| r.try_get(0))
                }
            };

            quote! {
                let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![#(&#req_arg_names),*];
                let mut query = concat!(#query_string, "(", #required_query_params).to_string();

                #(
                    if let Some(ref value) = #opt_arg_names {
                        params.push(value);
                        query.push_str(concat!(", ", #opt_arg_sql_names, ":= $"));
                        query.push_str(&params.len().to_string());
                    }
                )*

                query.push_str(")");

                #query
            }
        };

        // Build fn signature
        let rs_fn_name = self.rs_name();
        let args: Vec<TokenStream> = self.args.iter().map(|a| a.to_rust(types)).collect();
        let return_type = {
            let inner_ty = types
                .get(&self.return_type_oid)
                .unwrap()
                .to_rust_ident(types);

            if self.returns_set {
                quote! { Vec<#inner_ty> }
            } else {
                inner_ty
            }
        };

        let comment_macro = match self.comment.as_ref() {
            Some(comment) => quote! { #[doc=#comment] },
            None => quote! {},
        };

        // TODO: replace with custom error type later
        let error_type: TokenStream = "tokio_postgres::Error".parse().unwrap();

        quote! {
            #comment_macro
            pub async fn #rs_fn_name(client: &tokio_postgres::Client, #(#args),*) -> Result<#return_type, #error_type> {
                #fn_body
            }
        }
    }
}
