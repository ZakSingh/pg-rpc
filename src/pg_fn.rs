use crate::codegen::{FunctionName, SchemaName, ToRust, OID};
use crate::config;
use crate::exceptions::{get_exceptions_with_triggers, PgException};
use crate::fn_index::FunctionId;
use crate::ident::{sql_to_rs_ident, CaseType};
use crate::pg_id::PgId;
use crate::pg_type::PgType;
use crate::rel_index::RelIndex;
use crate::trigger_index::{TriggerIndex, TriggerEvent};
use config::Config;
use itertools::{izip, Itertools};
use jsonpath_rust::JsonPath;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{DeleteStmt, InsertStmt, OnConflictAction, SelectStmt, UpdateStmt};
use pg_query::{parse_plpgsql, NodeRef, ParseResult};
use proc_macro2::TokenStream;
use quote::quote;
use regex::Regex;
use serde_json::Value;
use std::cmp::PartialEq;
use std::collections::HashMap;
use postgres::Row;
use ustr::{Ustr};

const VOID_TYPE_OID: u32 = 2278;

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


}

impl PgFn {
    pub fn from_row(
        row: Row, 
        rel_index: &RelIndex,
        trigger_index: Option<&TriggerIndex>
    ) -> Result<Self, anyhow::Error> {
        let arg_type_oids: Vec<OID> = row.try_get("arg_oids")?;
        let arg_names: Vec<String> = row.try_get("arg_names")?;
        let arg_defaults: Vec<bool> = row.try_get("has_defaults")?;
        let comment: Option<String> = row.try_get("comment")?;
        let language: String = row.try_get("language")?;

        let args: Vec<PgArg> = izip!(arg_type_oids, arg_names, arg_defaults)
            .map(|(oid, name, has_default)| PgArg {
                name: name.clone(),
                type_oid: oid,
                has_default,
            })
            .collect();

        let definition = row.try_get::<_, String>("function_definition")?;
        
        // Only parse and analyze PL/pgSQL functions
        let exceptions = if language == "plpgsql" {
            let parsed = parse_plpgsql(&definition)?;
            get_exceptions_with_triggers(&parsed, comment.as_ref(), rel_index, trigger_index)?
        } else {
            // SQL functions don't need parsing and have no exceptions
            Vec::new()
        };

        Ok(Self {
            oid: row.try_get("oid")?,
            name: row.try_get("function_name")?,
            schema: row.try_get("schema_name")?,
            definition,
            returns_set: row.try_get("returns_set")?,
            comment,
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
            PgType::Int16 => quote! { i16 },
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
        // Use the unified error type from the errors module
        let err_type: TokenStream = quote! { crate::errors::PgRpcError };

        let return_opt = self.comment.as_ref().is_some_and(|c| c.contains("@pgrpc_return_opt"));
        if return_opt && !self.returns_set  {
            panic!("{}: Only set-returning functions can have @pgrpc_return_opt.", self.name)
        }


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

            // Check if return type needs special handling
            let return_pg_type = types.get(&self.return_type_oid);
            let needs_expansion = if let Some(PgType::Composite { fields, .. }) = return_pg_type {
                fields.iter().any(|f| f.flatten)
            } else {
                false
            };
            
            let query_string = if self.returns_set || needs_expansion {
                format!("select * from {}.{}", self.schema, self.name)
            } else {
                format!("select {}.{}", self.schema, self.name)
            };

            let query = if return_opt {
                quote! {
                    client
                        .query_opt(&query, &params)
                        .await
                        .and_then(|opt_row| match opt_row {
                            None => Ok(None),
                            Some(row) => row.try_get(0).map(Some),
                        })
                        .map_err(Into::into)
                }
            } else if self.returns_set {
                quote! {
                    client
                        .query(&query, &params)
                        .await
                        .and_then(|rows| {
                            rows.into_iter().map(TryInto::try_into).collect()
                        }).map_err(Into::into)
                }
            } else if self.return_type_oid == VOID_TYPE_OID {
                // Void returning function
                quote! {
                    client
                        .execute(&query, &params)
                        .await
                        .map_err(Into::into)?;

                    Ok(())
                }
            } else {
                // Check if return type is a composite type with custom TryFrom
                let return_pg_type = types.get(&self.return_type_oid).unwrap();
                match return_pg_type {
                    PgType::Composite { fields, .. } => {
                        // Check if any fields have flatten annotation
                        let has_flatten = fields.iter().any(|f| f.flatten);
                        if has_flatten {
                            // For composite types with flatten, we use SELECT * FROM function()
                            // which expands the composite type into columns that our TryFrom can handle
                            let return_not_null = self.comment.as_ref().is_some_and(|c| c.contains("@pgrpc_not_null"));
                            if return_not_null {
                                quote! {
                                    client
                                        .query_one(&query, &params)
                                        .await
                                        .and_then(|r| r.try_into())
                                        .map_err(Into::into)
                                }
                            } else {
                                quote! {
                                    client
                                        .query_one(&query, &params)
                                        .await
                                        .and_then(|r| Ok(Some(r.try_into()?)))
                                        .map_err(Into::into)
                                }
                            }
                        } else {
                            // Regular composite types can use try_get
                            quote! {
                                client
                                    .query_one(&query, &params)
                                    .await
                                    .and_then(|r| r.try_get(0))
                                    .map_err(Into::into)
                            }
                        }
                    }
                    _ => {
                        // Non-composite types use try_get
                        quote! {
                            client
                                .query_one(&query, &params)
                                .await
                                .and_then(|r| r.try_get(0))
                                .map_err(Into::into)
                        }
                    }
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
        let return_type = if self.return_type_oid == VOID_TYPE_OID {
            quote! { () }
        } else {
            let inner_ty = types
                .get(&self.return_type_oid)
                .unwrap()
                .to_rust_ident(types);

            if self.returns_set {
                quote! { Vec<#inner_ty> }
            } else {
                let return_not_null = self.comment.as_ref().is_some_and(|c| c.contains("@pgrpc_not_null"));
                if return_not_null {
                    inner_ty
                } else {
                    quote! { Option<#inner_ty> }
                }
            }
        };

        let comment_macro = match self.comment.as_ref() {
            Some(comment) => quote! { #[doc=#comment] },
            None => quote! {},
        };

        quote! {
            #comment_macro
            pub async fn #rs_fn_name(client: &impl deadpool_postgres::GenericClient, #(#args),*) -> Result<#return_type, #err_type> {
                #fn_body
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictTarget {
    Columns(Vec<Ustr>),             // ON CONFLICT (col1, col2)
    Constraint(Ustr),             // ON CONFLICT ON CONSTRAINT constraint_name
    Any,                            // ON CONFLICT without target
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictClause {
    pub target: ConflictTarget,
    pub action: ConflictAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Outer
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    Select { cols: Vec<Ustr>, join_type: JoinType },
    Update { cols: Vec<Ustr> },
    Insert { cols: Vec<Ustr>, on_conflict: Option<ConflictClause> },
    Delete,
}

/// A function's dependency on a relation
#[derive(Debug, Clone)]
pub struct RelDep {
    pub(crate) rel_oid: OID,
    pub(crate) cmd: Cmd,
    /// Exceptions that could be raised by triggers on this relation for this command
    pub(crate) trigger_exceptions: Vec<crate::exceptions::PgException>,
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

/// Get the relations the given queries depend upon.
pub fn get_rel_deps(queries: &[ParseResult], rel_index: &RelIndex) -> Vec<RelDep> {
    queries
      .iter()
      .flat_map(|q| get_query_deps(q, rel_index))
      .collect()
}

/// Populate trigger exceptions for relation dependencies
pub fn populate_trigger_exceptions(
    rel_deps: &mut [RelDep],
    trigger_index: &TriggerIndex,
) {
    for rel_dep in rel_deps {
        let trigger_event = match &rel_dep.cmd {
            Cmd::Insert { .. } => Some(TriggerEvent::Insert),
            Cmd::Update { .. } => Some(TriggerEvent::Update),
            Cmd::Delete => Some(TriggerEvent::Delete),
            _ => None,
        };
        
        if let Some(event) = trigger_event {
            rel_dep.trigger_exceptions = trigger_index.get_exceptions_for_event(rel_dep.rel_oid, &event);
        }
    }
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

    // Collect the inserted column names
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

    // Parse ON CONFLICT clause if present
    let on_conflict = stmt.on_conflict_clause.as_ref().map(|conflict| {
        let action = match OnConflictAction::from_i32(conflict.action).unwrap() {
            OnConflictAction::OnconflictNothing => ConflictAction::DoNothing,
            OnConflictAction::OnconflictUpdate => ConflictAction::DoUpdate,
            _ => unreachable!("Invalid ON CONFLICT action"),
        };

        // Get conflict target - either columns, constraint name, or none
        let target = if let Some(infer) = &conflict.infer {
            // ON CONFLICT (col1, col2)
            let cols = infer
              .index_elems
              .iter()
              .filter_map(|col| {
                  col.node.as_ref().map(|n| match n {
                      Node::InferenceElem(elem) => elem.expr.as_ref().and_then(|e| {
                          if let Some(Node::ColumnRef(col_ref)) = &e.node {
                              col_ref.fields.last().and_then(|f| {
                                  if let Some(Node::String(s)) = &f.node {
                                      Some(s.sval.as_str().into())
                                  } else {
                                      None
                                  }
                              })
                          } else {
                              None
                          }
                      }),
                      _ => None,
                  })
              })
              .flatten()
              .collect::<Vec<_>>();

            if !cols.is_empty() {
                ConflictTarget::Columns(cols)
            } else {
                ConflictTarget::Any
            }
        } else if let Some(infer) = &conflict.infer {
            // ON CONFLICT ON CONSTRAINT name
            ConflictTarget::Constraint(infer.conname.as_str().into())
        } else {
            // ON CONFLICT
            ConflictTarget::Any
        };

        ConflictClause { target, action }
    });

    Some(RelDep {
        rel_oid,
        cmd: Cmd::Insert { cols, on_conflict },
        trigger_exceptions: Vec::new(), // Will be populated later with trigger analysis
    })
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
        trigger_exceptions: Vec::new(), // Will be populated later with trigger analysis
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
        trigger_exceptions: Vec::new(), // Will be populated later with trigger analysis
    })
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
    use crate::pg_rel::{PgRel, PgRelKind};
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
                kind: PgRelKind::Table,
                oid: 1,
                id: PgId::new(None, ustr("a")),
                constraints: Vec::default(),
                columns: vec![ustr("field_1")],
            },
        );

        rel_index.insert(
            2,
            PgRel {
                kind: PgRelKind::Table,
                oid: 2,
                id: PgId::new(None, ustr("b")),
                constraints: Vec::default(),
                columns: Vec::default(),
            },
        );

        let queries = extract_queries(&f);
        let rels = get_rel_deps(&queries, &rel_index);
        // Only INSERT is detected, SELECT deps are not implemented (TODO in get_select_deps)
        assert_eq!(rels.len(), 1);

        Ok(())
    }
}
