//! Shared constraint analysis code used by both functions and queries.
//!
//! This module extracts common logic for analyzing SQL statements to determine
//! which table constraints might be violated.

use crate::codegen::OID;
use crate::exceptions::PgException;
use crate::pg_constraint::{Constraint, OnDelete};
use crate::pg_id::PgId;
use crate::rel_index::RelIndex;
use crate::trigger_index::{TriggerEvent, TriggerIndex};
use jsonpath_rust::JsonPath;
use log::warn;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{DeleteStmt, InsertStmt, OnConflictAction, SelectStmt, UpdateStmt};
use pg_query::{NodeRef, ParseResult};
use serde_json::Value;
use std::collections::BTreeMap;
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

/// Extract and parse all SQL queries inside a PlPgSQL function.
///
/// `scope` names the function (or query) being analyzed, for diagnostics.
pub fn extract_queries(fn_json: &Value, scope: &str) -> Vec<ParseResult> {
    let query_path = JsonPath::try_from("$..PLpgSQL_expr.query").unwrap();

    query_path
        .find_slice(fn_json)
        .into_iter()
        .filter_map(|x| {
            let data = x.to_data();
            match data.as_str() {
                Some(query_str) => Some(pg_query::parse(query_str)),
                None => {
                    warn!("{scope}: failed to extract query string from JSON value: {:?}", data);
                    None
                }
            }
        })
        .flatten() // remove unparsable queries
        .collect()
}

/// Get the relations the given queries depend upon.
pub fn get_rel_deps(queries: &[ParseResult], rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    queries
        .iter()
        .flat_map(|q| get_query_deps(q, rel_index, scope))
        .collect()
}

/// The event a DML dependency fires triggers for, if any.
pub fn trigger_event_for(cmd: &Cmd) -> Option<TriggerEvent> {
    match cmd {
        Cmd::Insert { .. } => Some(TriggerEvent::Insert),
        Cmd::Update { .. } => Some(TriggerEvent::Update),
        Cmd::Delete => Some(TriggerEvent::Delete),
        Cmd::Select { .. } => None,
    }
}

/// The DML performed by triggers that fire for these dependencies, so the
/// tables a trigger writes get their constraint arms on the caller too.
pub fn trigger_rel_deps(rel_deps: &[RelDep], trigger_index: &TriggerIndex) -> Vec<RelDep> {
    rel_deps
        .iter()
        .filter_map(|dep| trigger_event_for(&dep.cmd).map(|event| (dep.rel_oid, event)))
        .flat_map(|(oid, event)| trigger_index.get_rel_deps_for_event(oid, &event))
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
    scope: &str,
) -> (Vec<PgException>, BTreeMap<PgId, Vec<Constraint>>) {
    let parsed = match pg_query::parse(sql) {
        Ok(p) => p,
        Err(e) => {
            warn!("{scope}: failed to parse SQL for constraint analysis: {}", e);
            return (Vec::new(), BTreeMap::new());
        }
    };
    let rel_deps = sql_rel_deps(&parsed, rel_index, trigger_index, scope);
    constraints_from_deps(&rel_deps, rel_index)
}

/// Relation dependencies of one parsed statement, with trigger exceptions
/// populated when a trigger index is given.
pub fn sql_rel_deps(
    parsed: &ParseResult,
    rel_index: &RelIndex,
    trigger_index: Option<&TriggerIndex>,
    scope: &str,
) -> Vec<RelDep> {
    let mut rel_deps = get_query_deps(parsed, rel_index, scope);
    if let Some(trigger_idx) = trigger_index {
        populate_trigger_exceptions(&mut rel_deps, trigger_idx);
    }
    rel_deps
}

/// The constraints one DML dependency can violate, each paired with the table
/// Postgres reports the violation on (`DbError::table()`).
///
/// INSERT and UPDATE can trip the written table's own constraints on the
/// touched columns (minus unique/primary keys an `ON CONFLICT` absorbs).
/// DELETE cannot violate the deleted table's own constraints; what fails is a
/// `RESTRICT` / `NO ACTION` foreign key on *another* table that still
/// references the row, so those are returned under the referencing table.
/// `NOT NULL` is included; callers that cannot classify it drop it.
pub fn applicable_constraints(dep: &RelDep, rel_index: &RelIndex) -> Vec<(PgId, Constraint)> {
    let Some(rel) = rel_index.get(&dep.rel_oid) else {
        return Vec::new();
    };
    match &dep.cmd {
        Cmd::Update { cols } => rel
            .constraints
            .iter()
            .filter(|c| !matches!(c, Constraint::Default(_)) && c.contains_columns(cols))
            .map(|c| (rel.id.clone(), c.clone()))
            .collect(),
        Cmd::Insert { cols, on_conflict } => rel
            .constraints
            .iter()
            .filter(|c| {
                if matches!(c, Constraint::Default(_)) || !c.contains_columns(cols) {
                    return false;
                }
                let key_columns = match c {
                    Constraint::PrimaryKey(pk) => &pk.columns,
                    Constraint::Unique(u) => &u.columns,
                    _ => return true,
                };
                // ON CONFLICT absorbs the matching unique/primary key.
                match on_conflict.as_ref().map(|oc| &oc.target) {
                    None => true,
                    Some(ConflictTarget::Any) => false,
                    Some(ConflictTarget::Columns(conflict_cols)) => {
                        !key_columns.iter().eq(conflict_cols.iter())
                    }
                    Some(ConflictTarget::Constraint(name)) => c.name() != *name,
                }
            })
            .map(|c| (rel.id.clone(), c.clone()))
            .collect(),
        Cmd::Delete => rel_index
            .values()
            .flat_map(|other| {
                other.constraints.iter().filter_map(move |c| match c {
                    Constraint::ForeignKey(fk)
                        if fk.ref_table_oid == Some(dep.rel_oid)
                            && matches!(fk.on_delete, OnDelete::Restrict | OnDelete::NoAction) =>
                    {
                        Some((other.id.clone(), c.clone()))
                    }
                    _ => None,
                })
            })
            .collect(),
        Cmd::Select { .. } => Vec::new(),
    }
}

/// The constraints that the given relation dependencies can violate, as both
/// a flat exception list (constraints plus trigger exceptions) and a
/// per-table map used to build `XxxConstraint` error variants.
pub fn constraints_from_deps(
    rel_deps: &[RelDep],
    rel_index: &RelIndex,
) -> (Vec<PgException>, BTreeMap<PgId, Vec<Constraint>>) {
    let mut exceptions = Vec::new();
    let mut table_constraints: BTreeMap<PgId, Vec<Constraint>> = BTreeMap::new();

    for dep in rel_deps {
        for (table_id, constraint) in applicable_constraints(dep, rel_index) {
            exceptions.push(PgException::Constraint(constraint.clone()));
            // Merge: the same table may be written by several statements, or
            // by the query and a function it calls.
            let entry = table_constraints.entry(table_id).or_default();
            if !entry.iter().any(|c| c.name() == constraint.name()) {
                entry.push(constraint);
            }
        }

        // Add trigger exceptions
        exceptions.extend(dep.trigger_exceptions.iter().cloned());
    }

    (exceptions, table_constraints)
}

fn get_query_deps(query: &ParseResult, rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    query
        .protobuf
        .nodes()
        .iter()
        .filter_map(|(node, _, _, _)| match node.to_enum() {
            Node::SelectStmt(stmt) => get_select_deps(stmt.as_ref(), rel_index),
            Node::InsertStmt(stmt) => get_insert_deps(stmt.as_ref(), rel_index, scope),
            Node::UpdateStmt(stmt) => get_update_deps(stmt.as_ref(), rel_index, scope),
            Node::DeleteStmt(stmt) => get_delete_deps(stmt.as_ref(), rel_index, scope),
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
fn get_insert_deps(stmt: &InsertStmt, rel_index: &RelIndex, scope: &str) -> Option<RelDep> {
    let Some(rel_oid) = stmt
        .relation
        .as_ref()
        .map(|r| rel_index.id_to_oid(&PgId::from(r)))
        .flatten()
    else {
        // Could not determine what relation was being inserted into
        // This can happen with CTEs, temp tables, or relations not in our index
        warn!("{scope}: skipping INSERT statement dependency analysis (no plain target table): {stmt:?}");
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

fn get_delete_deps(stmt: &DeleteStmt, rel_index: &RelIndex, scope: &str) -> Option<RelDep> {
    let Some(rel_oid) = stmt
        .relation
        .as_ref()
        .map(|r| rel_index.id_to_oid(&PgId::from(r)))
        .flatten()
    else {
        warn!("{scope}: skipping DELETE statement dependency analysis (no plain target table): {stmt:?}");
        return None;
    };

    Some(RelDep {
        rel_oid,
        cmd: Cmd::Delete,
        trigger_exceptions: Vec::new(), // Will be populated later with trigger analysis
    })
}

fn get_update_deps(stmt: &UpdateStmt, rel_index: &RelIndex, scope: &str) -> Option<RelDep> {
    let Some(rel_oid) = stmt
        .relation
        .as_ref()
        .map(|r| rel_index.id_to_oid(&PgId::from(r)))
        .flatten()
    else {
        warn!("{scope}: skipping UPDATE statement dependency analysis (no plain target table): {stmt:?}");
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

        let queries = extract_queries(&f, "test");
        let rels = get_rel_deps(&queries, &rel_index, "test");
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
                        comment: None,
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
        let (exceptions, constraints) = analyze_sql_for_constraints(sql, &rel_index, None, "test");

        // Should find unique constraint on email
        assert!(!exceptions.is_empty());
        assert!(!constraints.is_empty());
    }
}
