use crate::codegen::{FunctionName, SchemaName, ToRust, OID};
use crate::fn_index::FunctionId;
use crate::fn_src_location::SrcLoc;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::pg_type::PgType;
use crate::pgsql_check::{line_to_span, PgSqlIssue, PgSqlReport};
use ariadne::{sources, Config, Label, Report};
use heck::ToPascalCase;
use itertools::izip;
use postgres_types::{FromSql, Json, Type};
use quote::__private::TokenStream;
use quote::quote;
use regex::Regex;
use serde::Deserialize;
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
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
    pub issues: Vec<PgSqlIssue>,
    dependencies: Vec<Dependency>,
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

            let (body, span): (&str, Range<usize>) = match query {
                Some((text, position)) => (text, position..text.len()),
                _ => (body, line_span),
            };

            let file_name = src_loc.0.to_string_lossy().to_string();
            let cache = sources(vec![(file_name.clone(), body)]);

            Report::build(i.level.into(), (file_name.clone(), span.clone()))
                .with_config(
                    Config::default()
                        .with_multiline_arrows(false)
                        .with_tab_width(2),
                )
                .with_code(i.sql_state.as_str())
                .with_message("in ".to_string() + self.schema.as_str() + "." + self.name.as_str())
                .with_label(
                    Label::new((file_name.clone(), span.clone())).with_message(i.message.as_str()),
                )
                .finish()
                .eprint(cache)
                .unwrap()
        });
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize)]
enum SqlOp {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum DepKind {
    #[serde(rename = "RELATION")]
    Relation {
        #[serde(default)]
        operations: HashSet<SqlOp>,
    },
    #[serde(rename = "FUNCTION")]
    Function,
}

#[serde_as]
#[derive(Debug, Deserialize)]
struct Dependency {
    #[serde_as(as = "DisplayFromStr")]
    oid: OID,
    #[serde(flatten)]
    kind: DepKind,
}

impl TryFrom<Row> for PgFn {
    type Error = tokio_postgres::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let arg_type_oids: Vec<OID> = row.try_get("arg_oids")?;
        let arg_names: Vec<String> = row.try_get("arg_names")?;
        let arg_defaults: Vec<bool> = row.try_get("has_defaults")?;

        let args: Vec<PgArg> = izip!(arg_type_oids, arg_names, arg_defaults)
            .map(|(oid, name, has_default)| PgArg {
                name: name.clone(),
                type_oid: oid,
                has_default,
            })
            .collect();

        let dep = row.try_get::<_, Json<Vec<Dependency>>>("dependencies")?.0;
        dbg!(row.get::<&str, &str>("function_name"), dep);
        Ok(Self {
            oid: row.try_get("oid")?,
            name: row.try_get("function_name")?,
            schema: row.try_get("schema_name")?,
            definition: row.try_get("function_definition")?,
            returns_set: row.try_get("returns_set")?,
            comment: row.try_get("comment")?,
            issues: row
                .try_get::<&str, Option<PgSqlReport>>("plpgsql_check")?
                .map(|r| r.issues)
                .unwrap_or(Vec::new()),
            args,
            return_type_oid: row.try_get("return_type")?,
            dependencies: row.try_get::<_, Json<Vec<Dependency>>>("dependencies")?.0,
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

            if self.return_type_oid == 2278 {
                // Returns void
                unimplemented!("Void returning functions not yet supported")
            }

            let query = if self.returns_set {
                quote! {
                    client
                        .query(&query, &params)
                        .await
                        .and_then(|rows| {
                            rows.into_iter().map(TryInto::try_into).collect()
                        }).map_err(Into::into)
                }
            } else {
                quote! {
                    client
                        .query_one(&query, &params)
                        .await
                        .and_then(|r| r.try_get(0))
                        .map_err(Into::into)
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

        let err_enum_name: TokenStream = (self.name.to_pascal_case() + "Error").parse().unwrap();

        quote! {
            #comment_macro
            pub async fn #rs_fn_name(client: &tokio_postgres::Client, #(#args),*) -> Result<#return_type, #err_enum_name> {
                #fn_body
            }

            #[derive(Debug)]
            pub enum #err_enum_name {
                Other(tokio_postgres::Error)
            }

            impl From<tokio_postgres::Error> for #err_enum_name {
                fn from(e: tokio_postgres::Error) -> Self {
                    match e {
                        e => #err_enum_name::Other(e),
                    }
                }
            }
        }
    }
}
