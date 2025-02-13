use crate::codegen::{FunctionName, SchemaName, ToRust, OID};
use crate::config;
use crate::exceptions::{get_exceptions, PgException};
use crate::fn_index::FunctionId;
use crate::fn_src_location::SrcLoc;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::pg_type::PgType;
use crate::pgsql_check::{line_to_span, PgSqlIssue, PgSqlReport};
use crate::rel_index::RelIndex;
use ariadne::{sources, Label, Report};
use config::Config;
use heck::ToPascalCase;
use itertools::{izip, Itertools};
use jsonpath_rust::JsonPath;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{DeleteStmt, InsertStmt, SelectStmt, UpdateStmt};
use pg_query::{parse_plpgsql, NodeRef, ParseResult};
use quote::__private::TokenStream;
use quote::{quote, ToTokens};
use regex::Regex;
use serde_json::Value;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::ops::Range;
use tokio_postgres::Row;
use ustr::Ustr;

#[derive(Debug)]
pub struct PgFn {
    pub oid: OID,
    pub schema: SchemaName,
    pub name: FunctionName,
    pub args: Vec<PgArg>,
    pub return_type_oid: OID,
    pub returns_set: bool,
    pub definition: String,
    pub comment: Option<String>,
    pub issues: Vec<PgSqlIssue>,
    pub exceptions: Vec<PgException>,
}

#[derive(Debug, Clone)]
pub struct PgArg {
    pub name: String,
    pub type_oid: OID,
    pub has_default: bool,
}

impl PgArg {
    pub fn rs_name(&self) -> TokenStream {
        sql_to_rs_ident(&self.name, CaseType::Snake)
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

    /// Get all type OIDs in the function signature (args + return type)
    pub fn ty_oids(&self) -> Vec<OID> {
        let mut oids = self.args.iter().map(|a| a.type_oid).collect_vec();
        oids.push(self.return_type_oid);
        oids
    }

    /// Retrieve the body (between the `$$`)
    fn body(&self) -> &str {
        let (tag, start_ind) = quote_ind(&self.definition).unwrap();
        let end_ind = self.definition[start_ind..].find(tag).unwrap() + start_ind;

        &self.definition[start_ind..end_ind]
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
                    ariadne::Config::default()
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

impl PgFn {
    pub fn from_row(row: Row, rel_index: &RelIndex) -> Result<Self, anyhow::Error> {
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

        let definition = row.try_get::<_, String>("function_definition")?;
        let parsed = parse_plpgsql(&definition)?;
        let exceptions = get_exceptions(&parsed, rel_index)?;

        Ok(Self {
            oid: row.try_get("oid")?,
            name: row.try_get("function_name")?,
            schema: row.try_get("schema_name")?,
            definition,
            returns_set: row.try_get("returns_set")?,
            comment: row.try_get("comment")?,
            issues: row
                .try_get::<&str, Option<PgSqlReport>>("plpgsql_check")?
                .map(|r| r.issues)
                .unwrap_or(Vec::new()),
            args,
            return_type_oid: row.try_get("return_type")?,
            exceptions,
        })
    }
}

impl ToRust for PgArg {
    fn to_rust(&self, types: &HashMap<OID, PgType>, _config: &Config) -> TokenStream {
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
    fn to_rust(&self, types: &HashMap<OID, PgType>, config: &Config) -> TokenStream {
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
        let args: Vec<TokenStream> = self.args.iter().map(|a| a.to_rust(types, config)).collect();
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

        let err_variants: Vec<TokenStream> =
            self.exceptions.iter().map(|e| e.rs_name(config)).collect();

        // By SqlState, I can differentiate different error types
        // but then I still need to get the constraint name.

        let mut custom_handlers = Vec::new();
        let mut check_handlers = Vec::new();
        let mut unique_handlers = Vec::new();
        let mut fk_handlers = Vec::new();
        let mut not_null_handlers = Vec::new();

        self.exceptions.iter().for_each(|e| match e {
            PgException::Explicit(sql_state) => {
                let code = sql_state.code();
                let sql_state_rs = e.rs_name(config);
                custom_handlers.push(quote! {
                    code if code == &tokio_postgres::error::SqlState::from_code(#code) => #err_enum_name::#sql_state_rs(db_error.to_owned())
                })
            }
            PgException::Constraint(constraint) => match constraint {
                Constraint::Check(check) => {
                    let constraint_name = check.name.as_str();
                    let constraint_rs_name = constraint.into_token_stream();
                    check_handlers.push(quote! {
                    #constraint_name => #err_enum_name::#constraint_rs_name(db_error.to_owned())
                    });
                }
                Constraint::ForeignKey(fk) => {
                    let constraint_name = fk.name.as_str();
                    let constraint_rs_name = constraint.into_token_stream();
                    fk_handlers.push(quote! {
                    #constraint_name => #err_enum_name::#constraint_rs_name(db_error.to_owned())
                    });
                }
                Constraint::PrimaryKey(pk) => {}
                Constraint::Unique(_) => {
                    let constraint_name = constraint.name().as_str();
                    let constraint_rs_name = constraint.into_token_stream();
                    unique_handlers.push(quote! {
                    #constraint_name => #err_enum_name::#constraint_rs_name(db_error.to_owned())
                    });
                }
                Constraint::NotNull(not_null) => {
                    let col = not_null.column.as_str();
                    let constraint_rs_name = constraint.into_token_stream();
                    not_null_handlers.push(quote! {
                    #col => #err_enum_name::#constraint_rs_name(db_error.to_owned())
                    });
                }
                _ => {}
            },
            _ => {}
        });

        let checks = if check_handlers.is_empty() {
            quote! {}
        } else {
            quote! {
                &tokio_postgres::error::SqlState::CHECK_VIOLATION => match db_error.constraint().unwrap() {
                    #(#check_handlers),*,
                    _ => #err_enum_name::Other(e)
                },
            }
        };

        let fks = if fk_handlers.is_empty() {
            quote! {}
        } else {
            quote! {
                &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION => match db_error.constraint().unwrap() {
                    #(#fk_handlers),*,
                    _ => #err_enum_name::Other(e)
                },
            }
        };

        let not_nulls = if not_null_handlers.is_empty() {
            quote! {}
        } else {
            quote! {
                &tokio_postgres::error::SqlState::NOT_NULL_VIOLATION => match db_error.column().unwrap() {
                    #(#not_null_handlers),*,
                    _ => #err_enum_name::Other(e)
                },
            }
        };

        let uniques = if unique_handlers.is_empty() {
            quote! {}
        } else {
            quote! {
                &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => match db_error.constraint().unwrap() {
                    #(#unique_handlers),*,
                    _ => #err_enum_name::Other(e)
                },
            }
        };

        quote! {
            #comment_macro
            pub async fn #rs_fn_name(client: &impl tokio_postgres::GenericClient, #(#args),*) -> Result<#return_type, #err_enum_name> {
                #fn_body
            }

            #[derive(Debug)]
            pub enum #err_enum_name {
                #(#err_variants(tokio_postgres::error::DbError)),*,
                Other(tokio_postgres::Error)
            }

            impl From<tokio_postgres::Error> for #err_enum_name {
                fn from(e: tokio_postgres::Error) -> Self {
                    let Some(db_error) = e.as_db_error() else {
                        return #err_enum_name::Other(e);
                    };

                    match db_error.code() {
                        #(#custom_handlers),*,
                        #checks
                        #fks
                        #not_nulls
                        #uniques
                        _ => #err_enum_name::Other(e),
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    Select { cols: Vec<Ustr> },
    Update { cols: Vec<Ustr> },
    Insert { cols: Vec<Ustr> },
    Delete,
}

/// A function's dependency on a relation
#[derive(Debug, Clone)]
pub struct RelDep {
    pub(crate) rel_oid: OID,
    pub(crate) cmd: Cmd,
}

/// Extract and parse all SQL queries inside a PlPgSQL function
pub fn extract_queries(fn_json: &Value) -> Vec<ParseResult> {
    let query_path = JsonPath::try_from("$..PLpgSQL_expr.query").unwrap();

    query_path
        .find_slice(fn_json)
        .into_iter()
        .map(|x| pg_query::parse(x.to_data().as_str().unwrap()))
        .flatten() // remove unparsable queries
        .collect()
}

fn get_select_deps(_stmt: &SelectStmt, _rel_index: &RelIndex) -> Option<RelDep> {
    // TODO
    None
}

/// Collect all dependencies from an `insert`.
fn get_insert_deps(stmt: &InsertStmt, rel_index: &RelIndex) -> Option<RelDep> {
    let Some(rel_oid) = stmt
        .relation
        .as_ref()
        .map(|r| rel_index.id_to_oid(&PgId::from(r)))
        .flatten()
    else {
        // Could not determine what relation was being inserted into
        unreachable!("Insert statement without relation")
    };

    let cols: Vec<Ustr> = {
        let referenced_cols: Vec<Ustr> = stmt
            .cols
            .iter()
            .flat_map(|n| n.node.as_ref().map(|n| n.nodes()).unwrap_or_default())
            .filter_map(|(n, _, _, _)| match n {
                NodeRef::ResTarget(t) => Some(t.name.as_str().into()),
                _ => None,
            })
            .collect_vec();

        if referenced_cols.is_empty() {
            // If column names aren't explicitly stated, e.g. `insert into account (1, 'Zak')`,
            // all columns must be inserted.
            rel_index
                .get(&rel_oid)
                .expect("Referenced relation to be in relation index")
                .columns
                .clone()
        } else {
            referenced_cols
        }
    };

    Some(RelDep {
        rel_oid,
        cmd: Cmd::Insert { cols },
    })

    // if it's an on conflict do nothing, we don't need the
    // constraint named in the clause OR all the constraints if omitted
    // let c =
    //     stmt.on_conflict_clause
    //         .as_ref()
    //         .map(|c| match OnConflictAction::from_i32(c.action).and_then(|a| match a {
    //             OnConflictAction::Undefined => None,
    //             OnConflictAction::OnconflictNone => None,
    //             OnConflictAction::OnconflictNothing => None,
    //             OnConflictAction::OnconflictUpdate =>
    //         });
}

fn get_delete_deps(stmt: &DeleteStmt, rel_index: &RelIndex) -> Option<RelDep> {
    let Some(rel_oid) = stmt
        .relation
        .as_ref()
        .map(|r| rel_index.id_to_oid(&PgId::from(r)))
        .flatten()
    else {
        unreachable!("Delete statement without relation")
    };

    Some(RelDep {
        rel_oid,
        cmd: Cmd::Delete,
    })
}

fn get_update_deps(stmt: &UpdateStmt, rel_index: &RelIndex) -> Option<RelDep> {
    let Some(rel_oid) = stmt
        .relation
        .as_ref()
        .map(|r| rel_index.id_to_oid(&PgId::from(r)))
        .flatten()
    else {
        unreachable!("Update statement without relation")
    };

    let cols: Vec<Ustr> = stmt
        .target_list
        .iter()
        .flat_map(|n| n.node.as_ref().unwrap().nodes())
        .map(|(n, _, _, _)| n)
        .filter_map(|n| match n {
            NodeRef::ResTarget(t) => Some(t.name.as_str().into()),
            _ => None,
        })
        .collect();

    Some(RelDep {
        rel_oid,
        cmd: Cmd::Update { cols },
    })
}

fn get_query_deps(query: &ParseResult, rel_index: &RelIndex) -> Vec<RelDep> {
    query
        .protobuf
        .nodes()
        .iter()
        .filter_map(|(node, _, _, _)| match node.to_enum() {
            Node::SelectStmt(stmt) => get_select_deps(stmt.as_ref(), rel_index),
            Node::InsertStmt(stmt) => get_insert_deps(stmt.as_ref(), rel_index),
            Node::UpdateStmt(stmt) => get_update_deps(stmt.as_ref(), rel_index),
            Node::DeleteStmt(stmt) => get_delete_deps(stmt.as_ref(), rel_index),
            _ => None,
        })
        .collect()
}

/// Get the relations the given queries depend upon.
pub fn get_rel_deps(queries: &[ParseResult], rel_index: &RelIndex) -> Vec<RelDep> {
    queries
        .iter()
        .flat_map(|q| get_query_deps(q, rel_index))
        .collect()
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

#[cfg(test)]
mod test {
    use crate::pg_fn::{extract_queries, get_rel_deps};
    use crate::pg_id::PgId;
    use crate::pg_rel::PgRel;
    use crate::rel_index::RelIndex;
    use pg_query::parse_plpgsql;
    use ustr::ustr;

    #[test]
    fn test_get_rels() -> anyhow::Result<()> {
        let fn_def = r#"
        create or replace function test() returns void as
        $$
        begin
            insert into a (field_1) values ('Example') on conflict do nothing;
            select * from b;
        end;
        $$ language plpgsql;"#;

        let f = parse_plpgsql(fn_def)?;

        let mut rel_index = RelIndex::default();
        rel_index.insert(
            1,
            PgRel {
                oid: 1,
                id: PgId::new(None, ustr("a")),
                constraints: Vec::default(),
                columns: vec![ustr("field_1")],
            },
        );

        rel_index.insert(
            2,
            PgRel {
                oid: 2,
                id: PgId::new(None, ustr("b")),
                constraints: Vec::default(),
                columns: Vec::default(),
            },
        );

        let queries = extract_queries(&f);
        let rels = get_rel_deps(&queries, &rel_index);
        assert_eq!(rels.len(), 2);

        Ok(())
    }
}
