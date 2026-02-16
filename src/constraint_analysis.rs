//! Shared constraint analysis code used by both functions and queries.
//!
//! This module extracts common logic for analyzing SQL statements to determine
//! which table constraints might be violated.

use crate::codegen::OID;
use crate::exceptions::PgException;
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::rel_index::RelIndex;
use crate::trigger_index::{TriggerEvent, TriggerIndex};
use jsonpath_rust::JsonPath;
use log::warn;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{DeleteStmt, InsertStmt, OnConflictAction, SelectStmt, UpdateStmt};
use pg_query::{NodeRef, ParseResult};
use serde_json::Value;
use std::collections::HashMap;
use std::ops::Deref;
use ustr::Ustr;

/// The type of conflict target in an ON CONFLICT clause
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictTarget {
    /// ON CONFLICT (col1, col2)
    Columns(Vec<Ustr>),
    /// ON CONFLICT ON CONSTRAINT constraint_name
    Constraint(Ustr),
    /// ON CONFLICT without target
    Any,
}

/// The action to take on conflict
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate,
}

/// Represents an ON CONFLICT clause
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictClause {
    pub target: ConflictTarget,
    pub action: ConflictAction,
}

/// The type of join in a SELECT statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Outer,
}

/// Represents the command type being executed against a relation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
    Select {
        cols: Vec<Ustr>,
        join_type: JoinType,
    },
    Update {
        cols: Vec<Ustr>,
    },
    Insert {
        cols: Vec<Ustr>,
        on_conflict: Option<ConflictClause>,
    },
    Delete,
}

/// A dependency on a relation, tracking the command type and potential exceptions
#[derive(Debug, Clone)]
pub struct RelDep {
    pub rel_oid: OID,
    pub cmd: Cmd,
    /// Exceptions that could be raised by triggers on this relation for this command
    pub trigger_exceptions: Vec<PgException>,
}

/// Extract and parse all SQL queries inside a PlPgSQL function
pub fn extract_queries(fn_json: &Value) -> Vec<ParseResult> {
    let query_path = JsonPath::try_from("$..PLpgSQL_expr.query").unwrap();

    query_path
        .find_slice(fn_json)
        .into_iter()
        .filter_map(|x| {
            let data = x.to_data();
            match data.as_str() {
                Some(query_str) => Some(pg_query::parse(query_str)),
                None => {
                    warn!("Failed to extract query string from JSON value: {:?}", data);
                    None
                }
            }
        })
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
pub fn populate_trigger_exceptions(rel_deps: &mut [RelDep], trigger_index: &TriggerIndex) {
    for rel_dep in rel_deps {
        let trigger_event = match &rel_dep.cmd {
            Cmd::Insert { .. } => Some(TriggerEvent::Insert),
            Cmd::Update { .. } => Some(TriggerEvent::Update),
            Cmd::Delete => Some(TriggerEvent::Delete),
            _ => None,
        };

        if let Some(event) = trigger_event {
            rel_dep.trigger_exceptions =
                trigger_index.get_exceptions_for_event(rel_dep.rel_oid, &event);
        }
    }
}

/// Get table dependencies and applicable constraints for a SQL statement
///
/// This is the main entry point for analyzing a SQL statement to determine
/// which constraints might be violated.
pub fn analyze_sql_for_constraints(
    sql: &str,
    rel_index: &RelIndex,
    trigger_index: Option<&TriggerIndex>,
) -> (Vec<PgException>, HashMap<PgId, Vec<Constraint>>) {
    let parsed = match pg_query::parse(sql) {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to parse SQL for constraint analysis: {}", e);
            return (Vec::new(), HashMap::new());
        }
    };

    let queries = vec![parsed];
    let mut rel_deps = get_rel_deps(&queries, rel_index);

    // Populate trigger exceptions if available
    if let Some(trigger_idx) = trigger_index {
        populate_trigger_exceptions(&mut rel_deps, trigger_idx);
    }

    let mut exceptions = Vec::new();
    let mut table_constraints: HashMap<PgId, Vec<Constraint>> = HashMap::new();

    for dep in &rel_deps {
        // Find the PgId for this OID
        let pg_id = rel_index
            .iter()
            .find(|(oid, _)| **oid == dep.rel_oid)
            .map(|(_, rel)| &rel.id);

        if let Some(id) = pg_id {
            if let Some(rel) = rel_index.get(&dep.rel_oid) {
                let relevant_constraints: Vec<Constraint> = rel
                    .constraints
                    .iter()
                    .filter(|c| {
                        // Filter constraints based on the operation type
                        match &dep.cmd {
                            Cmd::Update { cols } => c.contains_columns(cols),
                            Cmd::Insert { cols, on_conflict } => {
                                // Skip default constraints
                                if matches!(c, Constraint::Default(_)) {
                                    return false;
                                }

                                if !c.contains_columns(cols) {
                                    return false;
                                }

                                // Filter out unique/primary key constraints if they're handled by ON CONFLICT
                                if let Some(conflict) = on_conflict {
                                    match c {
                                        Constraint::PrimaryKey(pk) => {
                                            let constraint_cols = &pk.columns;
                                            match &conflict.target {
                                                ConflictTarget::Columns(conflict_cols) => {
                                                    !constraint_cols.iter().eq(conflict_cols.iter())
                                                }
                                                ConflictTarget::Constraint(name) => {
                                                    c.name().as_str() != name.as_str()
                                                }
                                                ConflictTarget::Any => false,
                                            }
                                        }
                                        Constraint::Unique(unique) => {
                                            let constraint_cols = &unique.columns;
                                            match &conflict.target {
                                                ConflictTarget::Columns(conflict_cols) => {
                                                    !constraint_cols.iter().eq(conflict_cols.iter())
                                                }
                                                ConflictTarget::Constraint(name) => {
                                                    c.name().as_str() != name.as_str()
                                                }
                                                ConflictTarget::Any => false,
                                            }
                                        }
                                        _ => true,
                                    }
                                } else {
                                    true
                                }
                            }
                            Cmd::Delete => matches!(c, Constraint::ForeignKey(_)),
                            _ => false,
                        }
                    })
                    .cloned()
                    .collect();

                if !relevant_constraints.is_empty() {
                    // Add constraint exceptions
                    for constraint in &relevant_constraints {
                        exceptions.push(PgException::Constraint(constraint.clone()));
                    }
                    table_constraints.insert(id.clone(), relevant_constraints);
                }
            }
        }

        // Add trigger exceptions
        exceptions.extend(dep.trigger_exceptions.iter().cloned());
    }

    (exceptions, table_constraints)
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
            Node::CallStmt(_stmt) => None, // We'll handle CallStmt separately for custom errors
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
        // This can happen with CTEs, temp tables, or relations not in our index
        warn!("Skipping INSERT statement dependency analysis: {stmt:?}");
        return None;
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
            .collect();

        if referenced_cols.is_empty() {
            // If column names aren't explicitly stated, e.g. `insert into account (1, 'Zak')`,
            // all columns must be inserted.
            rel_index
                .deref()
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
            // Check for constraint name first
            if !infer.conname.is_empty() {
                ConflictTarget::Constraint(infer.conname.as_str().into())
            } else {
                // ON CONFLICT (col1, col2)
                let cols: Vec<Ustr> = infer
                    .index_elems
                    .iter()
                    .filter_map(|col| {
                        col.node.as_ref().and_then(|n| match n {
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
                    .collect();

                if !cols.is_empty() {
                    ConflictTarget::Columns(cols)
                } else {
                    ConflictTarget::Any
                }
            }
        } else {
            // ON CONFLICT without target
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
        warn!("Skipping DELETE statement dependency analysis: {stmt:?}");
        return None;
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
        warn!("Skipping UPDATE statement dependency analysis: {stmt:?}");
        return None;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_rel::{PgRel, PgRelKind};
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
                column_types: vec![],
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
                column_types: vec![],
            },
        );

        let queries = extract_queries(&f);
        let rels = get_rel_deps(&queries, &rel_index);
        // Only INSERT is detected, SELECT deps are not implemented (TODO in get_select_deps)
        assert_eq!(rels.len(), 1);

        Ok(())
    }

    #[test]
    fn test_analyze_insert_sql() {
        let mut rel_index = RelIndex::default();

        use crate::pg_constraint::{NotNullConstraint, UniqueConstraint};
        use smallvec::SmallVec;

        rel_index.insert(
            1,
            PgRel {
                kind: PgRelKind::Table,
                oid: 1,
                id: PgId::new(None, ustr("users")),
                constraints: vec![
                    Constraint::Unique(UniqueConstraint {
                        name: ustr("users_email_key"),
                        columns: SmallVec::from_slice(&[ustr("email")]),
                    }),
                    Constraint::NotNull(NotNullConstraint {
                        name: ustr("users_email_not_null"),
                        column: ustr("email"),
                    }),
                ],
                columns: vec![ustr("id"), ustr("email"), ustr("name")],
                column_types: vec![],
            },
        );

        let sql = "INSERT INTO users (email, name) VALUES ($1, $2)";
        let (exceptions, constraints) = analyze_sql_for_constraints(sql, &rel_index, None);

        // Should find unique constraint on email
        assert!(!exceptions.is_empty());
        assert!(!constraints.is_empty());
    }
}
