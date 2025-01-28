use crate::codegen::{FunctionName, SchemaName, ToRust, OID};
use crate::fn_index::FunctionId;
use crate::fn_src_location::SrcLoc;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::pg_type::PgType;
use crate::pgsql_check::{line_to_span, PgSqlIssue, PgSqlReport};
use ariadne::{sources, Config, Label, Report};
use quote::__private::TokenStream;
use quote::quote;
use regex::Regex;
use std::cmp::max;
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

/// Get the position of the first `$...$` tag
fn quote_ind(src: &str) -> Option<(&str, usize)> {
    let re = Regex::new(r"\$.*?\$").unwrap();

    // Find the first match
    if let Some(matched) = re.find(src) {
        let s = matched.as_str();
        Some((s, matched.end()))
    } else {
        None
    }
}

impl PgFn {
    pub fn id(&self) -> FunctionId {
        FunctionId {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }

    pub fn has_issues(&self) -> bool {
        !self.issues.is_empty()
    }

    pub fn rs_name(&self) -> TokenStream {
        sql_to_rs_ident(&self.name, CaseType::Snake)
    }

    // TODO: line numbers are indexed from the start of the body string???
    // No, postgres collapses the header into ONE LINE, no matter how many arguments there are.
    // it also uppercases keywords in the header and moves the language to the header
    // Once you hit the $$ string though, it's exactly what the user entered.
    // I think line numbers are counted where the line with the opening $$ is line 1?

    /// Retrieve the body (between the `$$`)
    fn body(&self) -> &str {
        let (tag, start_ind) = quote_ind(&self.definition).unwrap();
        let end_ind = self.definition[start_ind..].find(tag).unwrap() + start_ind;

        &self.definition[start_ind..end_ind]
    }

    fn body_start(&self) -> usize {
        let (tag, start_ind) = quote_ind(&self.definition).unwrap();
        start_ind
    }

    pub fn report(&self, src_loc: &SrcLoc) {
        let body = self.body();
        self.issues.iter().for_each(|i| {
            let line_span = i
                .statement
                .as_ref()
                .and_then(|s| line_to_span(body, s.line_number))
                .unwrap_or(line_to_span(body, 1).unwrap());

            let query = i
                .query
                .as_ref()
                .and_then(|s| Some((s.text.as_ref(), s.position)));

            let file_name = src_loc.0.to_string_lossy().to_string();

            let body = match query {
                Some((text, position)) => text,
                _ => body,
            };

            let span = match query {
                Some((_, position)) => position..position,
                _ => line_span,
            };

            let cache = sources(vec![(file_name.clone(), body)]);

            Report::build(i.level.into(), (file_name.clone(), span.clone()))
                .with_code(i.sql_state.as_str())
                .with_message(self.name.as_str())
                .with_label(
                    Label::new((file_name.clone(), span.clone())).with_message(i.message.as_str()),
                )
                .finish()
                .eprint(cache)
                .unwrap()
        });
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
