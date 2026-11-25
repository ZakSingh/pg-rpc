//! Shared constraint analysis code used by both functions and queries.
//!
//! This module extracts common logic for analyzing SQL statements to determine
//! which table constraints might be violated.
//!
//! The unit of analysis is a [`RelDep`]: "this code runs `cmd` against that
//! relation". Statements are turned into dependencies by [`get_rel_deps`] /
//! [`sql_rel_deps`], which also add the writes a statement *implies*
//! ([`expand_implied_deps`]): the base table behind an auto-updatable view,
//! the partitions of a partitioned table, and the rows a foreign key's
//! `ON DELETE` / `ON UPDATE` action rewrites in other tables.
//! [`applicable_constraints`] then says which constraints each dependency can
//! violate. Everything errs towards reporting a constraint that cannot
//! actually fire rather than missing one that can.

use crate::call_graph::parse_plpgsql_expr;
use crate::codegen::OID;
use crate::exceptions::PgException;
use crate::pg_constraint::{Constraint, FkAction, ForeignKeyConstraint};
use crate::pg_id::PgId;
use crate::pg_rel::{PgRel, PgRelKind};
use crate::rel_index::RelIndex;
use crate::trigger_index::{TriggerEvent, TriggerIndex};
use jsonpath_rust::JsonPath;
use log::{debug, warn};
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{
    CmdType, DeleteStmt, InsertStmt, MergeStmt, OnConflictAction, OnConflictClause, RangeVar,
    SetOperation, UpdateStmt,
};
use pg_query::ParseResult;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use ustr::Ustr;

/// What an `ON CONFLICT` clause arbitrates on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConflictTarget {
    /// `ON CONFLICT (col1, col2)`: every unique key over exactly these columns.
    Columns(Vec<Ustr>),
    /// `ON CONFLICT ON CONSTRAINT constraint_name`
    Constraint(Ustr),
    /// `ON CONFLICT DO NOTHING` without a target: any unique key.
    Any,
    /// A target containing an expression (`ON CONFLICT (lower(email))`),
    /// which cannot be matched to a key by column name. Absorbs nothing.
    Unmatched,
}

/// Represents the command type being executed against a relation
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cmd {
    /// An UPDATE assigning these columns.
    Update { cols: Vec<Ustr> },
    /// An INSERT. Every column receives a value (given or defaulted), so the
    /// column list does not narrow which constraints apply.
    Insert { on_conflict: Option<ConflictTarget> },
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

impl RelDep {
    fn new(rel_oid: OID, cmd: Cmd) -> Self {
        Self {
            rel_oid,
            cmd,
            // Populated later by `populate_trigger_exceptions`.
            trigger_exceptions: Vec::new(),
        }
    }
}

/// Extract and parse every SQL statement and expression inside a PL/pgSQL
/// function (the JSON produced by `pg_query::parse_plpgsql`).
///
/// Statements parse as they are. Bare expressions (assignment right-hand
/// sides, `IF` conditions, `RETURN` values) are not valid SQL on their own
/// and are wrapped in a `SELECT` by [`parse_plpgsql_expr`], so calls made
/// from them are visible too. Anything that still fails to parse is logged
/// and skipped.
///
/// `scope` names the function (or query) being analyzed, for diagnostics.
pub fn extract_queries(fn_json: &Value, scope: &str) -> Vec<ParseResult> {
    let exprs = match fn_json.query("$..PLpgSQL_expr.query") {
        Ok(exprs) => exprs,
        Err(e) => {
            warn!("{scope}: could not walk the PL/pgSQL parse tree, no statements analyzed: {e}");
            return Vec::new();
        }
    };

    exprs
        .into_iter()
        .filter_map(|data| {
            let Some(query_str) = data.as_str() else {
                warn!("{scope}: PL/pgSQL expression is not a string: {data:?}");
                return None;
            };
            let parsed = parse_plpgsql_expr(query_str);
            if parsed.is_none() {
                debug!("{scope}: skipping unparsable PL/pgSQL expression: {query_str}");
            }
            parsed
        })
        .collect()
}

/// Get the relations the given queries write, including implied writes (see
/// [`expand_implied_deps`]).
pub fn get_rel_deps(queries: &[ParseResult], rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    let direct = queries
        .iter()
        .flat_map(|q| get_query_deps(q, rel_index, scope))
        .collect();
    expand_implied_deps(direct, rel_index)
}

/// The event a DML dependency fires triggers for.
pub fn trigger_event_for(cmd: &Cmd) -> TriggerEvent {
    match cmd {
        Cmd::Insert { .. } => TriggerEvent::Insert,
        Cmd::Update { .. } => TriggerEvent::Update,
        Cmd::Delete => TriggerEvent::Delete,
    }
}

/// The DML performed by triggers that fire for these dependencies, so the
/// tables a trigger writes get their constraint arms on the caller too.
pub fn trigger_rel_deps(rel_deps: &[RelDep], trigger_index: &TriggerIndex) -> Vec<RelDep> {
    rel_deps
        .iter()
        .flat_map(|dep| {
            trigger_index.get_rel_deps_for_event(dep.rel_oid, &trigger_event_for(&dep.cmd))
        })
        .collect()
}

/// Populate trigger exceptions for relation dependencies
pub fn populate_trigger_exceptions(rel_deps: &mut [RelDep], trigger_index: &TriggerIndex) {
    for rel_dep in rel_deps {
        rel_dep.trigger_exceptions = trigger_index
            .get_exceptions_for_event(rel_dep.rel_oid, &trigger_event_for(&rel_dep.cmd));
    }
}

/// Relation dependencies of one parsed statement (implied writes included),
/// with trigger exceptions populated when a trigger index is given.
pub fn sql_rel_deps(
    parsed: &ParseResult,
    rel_index: &RelIndex,
    trigger_index: Option<&TriggerIndex>,
    scope: &str,
) -> Vec<RelDep> {
    let mut rel_deps = expand_implied_deps(get_query_deps(parsed, rel_index, scope), rel_index);
    if let Some(trigger_idx) = trigger_index {
        populate_trigger_exceptions(&mut rel_deps, trigger_idx);
    }
    rel_deps
}

/// The constraints one DML dependency can violate, each paired with the table
/// Postgres reports the violation on (`DbError::table()`).
///
/// INSERT can trip any of the written table's own constraints: every column
/// gets a value, given or defaulted (minus unique/primary keys an
/// `ON CONFLICT` absorbs). UPDATE can trip the ones touching the assigned
/// columns.
/// DELETE cannot violate the deleted table's own constraints; what fails is a
/// `RESTRICT` / `NO ACTION` foreign key on *another* table that still
/// references the row, so those are returned under the referencing table.
/// An UPDATE that changes a referenced key fails the same way, on the keys
/// whose `ON UPDATE` action is `RESTRICT` / `NO ACTION`. Foreign keys with a
/// repairing action (`CASCADE`, `SET NULL`, `SET DEFAULT`) are followed by
/// [`expand_implied_deps`] instead.
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
            .map(|c| (rel.id, c.clone()))
            .chain(referencing_foreign_keys(dep.rel_oid, rel_index, |fk| {
                blocks(&fk.on_update) && references_any(fk, cols)
            }))
            .collect(),
        Cmd::Insert { on_conflict } => rel
            .constraints
            .iter()
            .filter(|c| {
                !matches!(c, Constraint::Default(_)) && !absorbed_by(c, on_conflict.as_ref())
            })
            .map(|c| (rel.id, c.clone()))
            .collect(),
        Cmd::Delete => {
            referencing_foreign_keys(dep.rel_oid, rel_index, |fk| blocks(&fk.on_delete)).collect()
        }
    }
}

/// Whether an INSERT's `ON CONFLICT` clause turns a violation of `constraint`
/// into the conflict action instead of an error. Only unique and primary
/// keys can be arbiters. When unsure, the answer is no (keep reporting it).
fn absorbed_by(constraint: &Constraint, on_conflict: Option<&ConflictTarget>) -> bool {
    let (key_columns, conditional) = match constraint {
        Constraint::PrimaryKey(pk) => (&pk.columns, false),
        Constraint::Unique(u) => (&u.columns, u.conditional),
        _ => return false,
    };
    match on_conflict {
        None | Some(ConflictTarget::Unmatched) => false,
        Some(ConflictTarget::Any) => true,
        // The column list infers every unique index over the same *set* of
        // columns, in any order. A partial or expression index needs its
        // predicate / expressions matched as well, which is not attempted.
        Some(ConflictTarget::Columns(conflict_cols)) => {
            !conditional
                && key_columns.len() == conflict_cols.len()
                && key_columns.iter().all(|k| conflict_cols.contains(k))
        }
        Some(ConflictTarget::Constraint(name)) => constraint.name() == *name,
    }
}

/// True for the actions that fail the statement instead of repairing the
/// referencing rows.
fn blocks(action: &FkAction) -> bool {
    matches!(action, FkAction::Restrict | FkAction::NoAction)
}

/// Whether an update of `cols` on the referenced table changes the key
/// `fk` points at. Only a change to the referenced key can strand a row.
/// Without the key's columns, assume it can.
fn references_any(fk: &ForeignKeyConstraint, cols: &[Ustr]) -> bool {
    fk.ref_columns
        .as_ref()
        .is_none_or(|key| cols.iter().any(|c| key.contains(c)))
}

/// Foreign keys on other tables that reference `rel_oid` and satisfy `keep`,
/// each with the table that owns it.
fn foreign_keys_referencing<'a>(
    rel_oid: OID,
    rel_index: &'a RelIndex,
    keep: impl Fn(&ForeignKeyConstraint) -> bool + 'a,
) -> impl Iterator<Item = (&'a PgRel, &'a ForeignKeyConstraint)> + 'a {
    rel_index
        .values()
        .flat_map(|other| other.constraints.iter().map(move |c| (other, c)))
        .filter_map(move |(other, c)| match c {
            Constraint::ForeignKey(fk) if fk.ref_table_oid == Some(rel_oid) && keep(fk) => {
                Some((other, fk))
            }
            _ => None,
        })
}

/// [`foreign_keys_referencing`], as constraints under the owning table's name.
fn referencing_foreign_keys<'a>(
    rel_oid: OID,
    rel_index: &'a RelIndex,
    keep: impl Fn(&ForeignKeyConstraint) -> bool + 'a,
) -> impl Iterator<Item = (PgId, Constraint)> + 'a {
    foreign_keys_referencing(rel_oid, rel_index, keep)
        .map(|(other, fk)| (other.id, Constraint::ForeignKey(fk.clone())))
}

/// Add the writes that the given writes imply, transitively:
///
/// * **Views.** DML on an auto-updatable view (a plain single-table `SELECT`)
///   is DML on its base table, whose constraints and triggers then apply.
///   The dependency on the view itself is kept for its `INSTEAD OF` triggers.
/// * **Partitions / inheritance children.** Rows written through a parent
///   land in its children, and a violation there is reported with the
///   child's table and constraint names.
/// * **Foreign key actions.** Deleting a row deletes the rows that reference
///   it `ON DELETE CASCADE` and updates the ones that reference it
///   `ON DELETE SET NULL` / `SET DEFAULT`; updating a referenced key does the
///   same through `ON UPDATE`. Those child-table writes fire the child's
///   triggers, check its constraints, and can themselves be blocked by a
///   `RESTRICT` key on a grandchild.
///
/// The input dependencies are returned first, unchanged and in order; implied
/// ones follow, each `(relation, command)` at most once, so cycles (a
/// self-referencing cascade, mutually cascading tables) terminate.
pub fn expand_implied_deps(deps: Vec<RelDep>, rel_index: &RelIndex) -> Vec<RelDep> {
    let mut seen: BTreeSet<(OID, Cmd)> = deps.iter().map(dep_key).collect();
    let mut pending: VecDeque<RelDep> = deps.iter().cloned().collect();
    let mut out = deps;

    while let Some(dep) = pending.pop_front() {
        for implied in implied_deps(&dep, rel_index) {
            if seen.insert(dep_key(&implied)) {
                pending.push_back(implied.clone());
                out.push(implied);
            }
        }
    }
    out
}

/// Identity of a dependency for the visited set: updates of the same columns
/// in a different order are the same write.
fn dep_key(dep: &RelDep) -> (OID, Cmd) {
    let cmd = match &dep.cmd {
        Cmd::Update { cols } => {
            let mut cols = cols.clone();
            cols.sort();
            cols.dedup();
            Cmd::Update { cols }
        }
        other => other.clone(),
    };
    (dep.rel_oid, cmd)
}

/// The writes directly implied by one write (one step of
/// [`expand_implied_deps`]).
fn implied_deps(dep: &RelDep, rel_index: &RelIndex) -> Vec<RelDep> {
    let Some(rel) = rel_index.get(&dep.rel_oid) else {
        return Vec::new();
    };
    let mut implied = Vec::new();

    // View -> base table.
    if let Some(base) = updatable_view_base(rel, rel_index) {
        let cmd = match &dep.cmd {
            Cmd::Update { cols } => Cmd::Update {
                cols: base.map_columns(cols),
            },
            other => other.clone(),
        };
        implied.push(RelDep::new(base.rel.oid, cmd));
    }

    // Parent -> partitions / inheritance children (same column names).
    for &child in rel_index.children_of(dep.rel_oid) {
        implied.push(RelDep::new(child, dep.cmd.clone()));
    }

    // Referential actions on the tables that reference this one.
    let action_dep = |other: &PgRel, fk: &ForeignKeyConstraint, action: &FkAction| match action {
        FkAction::Restrict | FkAction::NoAction => None,
        FkAction::Cascade if matches!(dep.cmd, Cmd::Delete) => {
            Some(RelDep::new(other.oid, Cmd::Delete))
        }
        // ON UPDATE CASCADE rewrites the referencing columns; SET NULL and
        // SET DEFAULT do so for either event.
        FkAction::Cascade | FkAction::SetNull | FkAction::SetDefault => Some(RelDep::new(
            other.oid,
            Cmd::Update {
                cols: fk.columns.to_vec(),
            },
        )),
    };
    match &dep.cmd {
        Cmd::Delete => implied.extend(
            foreign_keys_referencing(dep.rel_oid, rel_index, |_| true)
                .filter_map(|(other, fk)| action_dep(other, fk, &fk.on_delete)),
        ),
        Cmd::Update { cols } => implied.extend(
            foreign_keys_referencing(dep.rel_oid, rel_index, |fk| references_any(fk, cols))
                .filter_map(|(other, fk)| action_dep(other, fk, &fk.on_update)),
        ),
        // A new row has nothing referencing it yet.
        Cmd::Insert { .. } => {}
    }

    implied
}

/// The base table of an auto-updatable view and how the view's columns map
/// onto it.
struct ViewBase<'a> {
    rel: &'a PgRel,
    /// View column -> base column, for the columns that are plain references.
    columns: BTreeMap<Ustr, Ustr>,
}

impl ViewBase<'_> {
    /// Base-table columns written by an update of these view columns. A view
    /// column that is not a plain column reference cannot be mapped, in which
    /// case the update is assumed to touch every base column.
    fn map_columns(&self, view_cols: &[Ustr]) -> Vec<Ustr> {
        view_cols
            .iter()
            .map(|c| self.columns.get(c).copied())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_else(|| self.rel.columns.clone())
    }
}

/// If `rel` is a view Postgres can write through automatically, its base
/// relation: the definition must be one `SELECT` from one relation, with no
/// `WITH`, `DISTINCT`, `GROUP BY`, `HAVING`, `LIMIT`, `OFFSET`, window or set
/// operation. (Postgres additionally rejects aggregates and set-returning
/// functions in the select list; such a view cannot be the target of DML at
/// all, so treating it as updatable only over-reports.) Anything else, e.g. a
/// join view written through `INSTEAD OF` triggers or rules, is left alone.
fn updatable_view_base<'a>(rel: &PgRel, rel_index: &'a RelIndex) -> Option<ViewBase<'a>> {
    let PgRelKind::View { definition } = &rel.kind else {
        return None;
    };
    let parsed = pg_query::parse(definition).ok()?;
    let [stmt] = parsed.protobuf.stmts.as_slice() else {
        return None;
    };
    let Some(Node::SelectStmt(select)) = stmt.stmt.as_ref().and_then(|s| s.node.as_ref()) else {
        return None;
    };

    let simple = select.op == SetOperation::SetopNone as i32
        && select.with_clause.is_none()
        && select.distinct_clause.is_empty()
        && select.group_clause.is_empty()
        && select.having_clause.is_none()
        && select.window_clause.is_empty()
        && select.limit_count.is_none()
        && select.limit_offset.is_none();
    if !simple {
        return None;
    }
    let [from] = select.from_clause.as_slice() else {
        return None;
    };
    let Some(Node::RangeVar(base)) = &from.node else {
        return None;
    };
    let base_rel = rel_index.resolve_range_var(base)?;

    let columns = select
        .target_list
        .iter()
        .filter_map(|target| {
            let Some(Node::ResTarget(res)) = &target.node else {
                return None;
            };
            let Some(Node::ColumnRef(col)) = res.val.as_ref().and_then(|v| v.node.as_ref()) else {
                return None;
            };
            let Some(Node::String(source)) = col.fields.last().and_then(|f| f.node.as_ref()) else {
                return None;
            };
            let view_col = if res.name.is_empty() {
                &source.sval
            } else {
                &res.name
            };
            Some((Ustr::from(view_col), Ustr::from(&source.sval)))
        })
        .collect();

    Some(ViewBase {
        rel: base_rel,
        columns,
    })
}

/// The constraints that the given relation dependencies can violate, as both
/// a flat exception list (what the triggers fired by those writes raise) and
/// a per-table map used to build `XxxConstraint` error variants.
pub fn constraints_from_deps(
    rel_deps: &[RelDep],
    rel_index: &RelIndex,
) -> (Vec<PgException>, BTreeMap<PgId, Vec<Constraint>>) {
    let mut exceptions = Vec::new();
    let mut table_constraints: BTreeMap<PgId, Vec<Constraint>> = BTreeMap::new();

    for dep in rel_deps {
        for (table_id, constraint) in applicable_constraints(dep, rel_index) {
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

/// The writes one parsed statement performs directly, including those of
/// data-modifying CTEs nested in it.
fn get_query_deps(query: &ParseResult, rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    query
        .protobuf
        .nodes()
        .iter()
        .flat_map(|(node, _, _, _)| match node.to_enum() {
            Node::InsertStmt(stmt) => get_insert_deps(stmt.as_ref(), rel_index, scope),
            Node::UpdateStmt(stmt) => get_update_deps(stmt.as_ref(), rel_index, scope),
            Node::DeleteStmt(stmt) => get_delete_deps(stmt.as_ref(), rel_index, scope),
            Node::MergeStmt(stmt) => get_merge_deps(stmt.as_ref(), rel_index, scope),
            _ => Vec::new(),
        })
        .collect()
}

/// Resolve the target of a DML statement. `None` (with a warning) when it is
/// not an indexed relation: a temp table, or a statement with no target.
fn target_oid(
    relation: Option<&RangeVar>,
    statement: &str,
    rel_index: &RelIndex,
    scope: &str,
) -> Option<OID> {
    let Some(relation) = relation else {
        warn!("{scope}: skipping {statement} dependency analysis: statement has no target relation");
        return None;
    };
    // Unqualified names follow the connection's search path.
    let oid = rel_index.resolve_range_var(relation).map(|rel| rel.oid);
    if oid.is_none() {
        let schema = if relation.schemaname.is_empty() {
            String::new()
        } else {
            format!("{}.", relation.schemaname)
        };
        warn!(
            "{scope}: skipping {statement} dependency analysis: target {schema}{} is not an indexed \
             relation (or is not on the search path)",
            relation.relname
        );
    }
    oid
}

/// The column names assigned by a `SET` list / named by an INSERT column
/// list. Only the top-level targets: `SET a = (SELECT b AS x ...)` assigns
/// `a`, not `x`.
fn target_columns(target_list: &[pg_query::protobuf::Node]) -> Vec<Ustr> {
    target_list
        .iter()
        .filter_map(|n| match &n.node {
            Some(Node::ResTarget(t)) if !t.name.is_empty() => Some(Ustr::from(&t.name)),
            _ => None,
        })
        .collect()
}

/// What an `ON CONFLICT` clause arbitrates on.
fn conflict_target(conflict: &OnConflictClause) -> ConflictTarget {
    let Some(infer) = &conflict.infer else {
        return ConflictTarget::Any;
    };
    if !infer.conname.is_empty() {
        return ConflictTarget::Constraint(infer.conname.as_str().into());
    }
    if infer.index_elems.is_empty() {
        return ConflictTarget::Any;
    }
    // `(col, ...)`: each element is an IndexElem naming a column, or holding
    // an expression.
    infer
        .index_elems
        .iter()
        .map(|elem| match &elem.node {
            Some(Node::IndexElem(e)) if !e.name.is_empty() && e.expr.is_none() => {
                Some(Ustr::from(&e.name))
            }
            _ => None,
        })
        .collect::<Option<Vec<Ustr>>>()
        .map_or(ConflictTarget::Unmatched, ConflictTarget::Columns)
}

/// Collect all dependencies from an `insert`: the insert itself and, for
/// `ON CONFLICT ... DO UPDATE`, the update of the conflicting row.
fn get_insert_deps(stmt: &InsertStmt, rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    let Some(rel_oid) = target_oid(stmt.relation.as_ref(), "INSERT", rel_index, scope) else {
        return Vec::new();
    };

    let conflict = stmt.on_conflict_clause.as_deref();
    let mut deps = vec![RelDep::new(
        rel_oid,
        Cmd::Insert {
            on_conflict: conflict.map(conflict_target),
        },
    )];

    if let Some(conflict) = conflict {
        if conflict.action == OnConflictAction::OnconflictUpdate as i32 {
            // The SET list is an UPDATE of the existing row: it checks the
            // constraints on the assigned columns (which need not be among
            // the inserted ones) and fires UPDATE triggers.
            deps.push(RelDep::new(
                rel_oid,
                Cmd::Update {
                    cols: target_columns(&conflict.target_list),
                },
            ));
        }
    }

    deps
}

fn get_delete_deps(stmt: &DeleteStmt, rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    target_oid(stmt.relation.as_ref(), "DELETE", rel_index, scope)
        .map(|rel_oid| RelDep::new(rel_oid, Cmd::Delete))
        .into_iter()
        .collect()
}

fn get_update_deps(stmt: &UpdateStmt, rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    target_oid(stmt.relation.as_ref(), "UPDATE", rel_index, scope)
        .map(|rel_oid| {
            RelDep::new(
                rel_oid,
                Cmd::Update {
                    cols: target_columns(&stmt.target_list),
                },
            )
        })
        .into_iter()
        .collect()
}

/// `MERGE INTO target ...`: one dependency per kind of action its `WHEN`
/// clauses take on the target.
fn get_merge_deps(stmt: &MergeStmt, rel_index: &RelIndex, scope: &str) -> Vec<RelDep> {
    let Some(rel_oid) = target_oid(stmt.relation.as_ref(), "MERGE", rel_index, scope) else {
        return Vec::new();
    };

    let mut inserts = false;
    let mut deletes = false;
    let mut updated: Option<Vec<Ustr>> = None;
    for clause in &stmt.merge_when_clauses {
        let Some(Node::MergeWhenClause(when)) = &clause.node else {
            continue;
        };
        match CmdType::try_from(when.command_type) {
            Ok(CmdType::CmdInsert) => inserts = true,
            Ok(CmdType::CmdDelete) => deletes = true,
            Ok(CmdType::CmdUpdate) => updated
                .get_or_insert_with(Vec::new)
                .extend(target_columns(&when.target_list)),
            // DO NOTHING
            _ => {}
        }
    }

    let mut deps = Vec::new();
    if inserts {
        deps.push(RelDep::new(rel_oid, Cmd::Insert { on_conflict: None }));
    }
    if let Some(mut cols) = updated {
        cols.sort();
        cols.dedup();
        deps.push(RelDep::new(rel_oid, Cmd::Update { cols }));
    }
    if deletes {
        deps.push(RelDep::new(rel_oid, Cmd::Delete));
    }
    deps
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::{
        CheckConstraint, NotNullConstraint, PrimaryKeyConstraint, UniqueConstraint,
    };
    use pg_query::parse_plpgsql;
    use smallvec::SmallVec;
    use ustr::ustr;

    fn table(oid: OID, name: &str, columns: &[&str], constraints: Vec<Constraint>) -> PgRel {
        PgRel {
            kind: PgRelKind::Table,
            oid,
            id: PgId::new(None, ustr(name)),
            constraints,
            columns: columns.iter().map(|c| ustr(c)).collect(),
            column_types: vec![],
        }
    }

    fn view(oid: OID, name: &str, columns: &[&str], definition: &str) -> PgRel {
        PgRel {
            kind: PgRelKind::View {
                definition: definition.to_string(),
            },
            ..table(oid, name, columns, vec![])
        }
    }

    fn index(rels: Vec<PgRel>) -> RelIndex {
        let mut rel_index = RelIndex::default();
        for rel in rels {
            rel_index.insert(rel.oid, rel);
        }
        rel_index
    }

    fn cols(names: &[&str]) -> SmallVec<Ustr, 2> {
        names.iter().map(|c| ustr(c)).collect()
    }

    fn unique(name: &str, columns: &[&str]) -> Constraint {
        Constraint::Unique(UniqueConstraint {
            comment: None,
            name: ustr(name),
            columns: cols(columns),
            conditional: false,
        })
    }

    fn partial_unique(name: &str, columns: &[&str]) -> Constraint {
        Constraint::Unique(UniqueConstraint {
            comment: None,
            name: ustr(name),
            columns: cols(columns),
            conditional: true,
        })
    }

    fn pkey(name: &str, columns: &[&str]) -> Constraint {
        Constraint::PrimaryKey(PrimaryKeyConstraint {
            comment: None,
            name: ustr(name),
            columns: cols(columns),
        })
    }

    fn check(name: &str, columns: &[&str]) -> Constraint {
        Constraint::Check(CheckConstraint {
            comment: None,
            name: ustr(name),
            columns: cols(columns),
            check_expression: None,
        })
    }

    fn fkey(
        name: &str,
        columns: &[&str],
        ref_oid: OID,
        ref_columns: &[&str],
        on_delete: FkAction,
        on_update: FkAction,
    ) -> Constraint {
        Constraint::ForeignKey(ForeignKeyConstraint {
            comment: None,
            name: ustr(name),
            columns: cols(columns),
            on_delete,
            on_update,
            ref_table: None,
            ref_columns: Some(cols(ref_columns)),
            ref_table_oid: Some(ref_oid),
        })
    }

    fn deps_of(sql: &str, rel_index: &RelIndex) -> Vec<RelDep> {
        sql_rel_deps(&pg_query::parse(sql).unwrap(), rel_index, None, "test")
    }

    /// `(table name, command)` for each dependency, in order.
    fn summary(deps: &[RelDep], rel_index: &RelIndex) -> Vec<(String, Cmd)> {
        deps.iter()
            .map(|d| (rel_index[&d.rel_oid].id.name().to_string(), d.cmd.clone()))
            .collect()
    }

    /// Names of the constraints `sql` can violate, as `table.constraint`, sorted.
    fn violations(sql: &str, rel_index: &RelIndex) -> Vec<String> {
        let (_, by_table) = constraints_from_deps(&deps_of(sql, rel_index), rel_index);
        let mut names: Vec<String> = by_table
            .iter()
            .flat_map(|(table, cs)| {
                cs.iter()
                    .map(move |c| format!("{}.{}", table.name(), c.name()))
            })
            .collect();
        names.sort();
        names
    }

    fn update(columns: &[&str]) -> Cmd {
        Cmd::Update {
            cols: columns.iter().map(|c| ustr(c)).collect(),
        }
    }

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
        let rel_index = index(vec![
            table(1, "a", &["field_1"], vec![]),
            table(2, "b", &[], vec![]),
        ]);

        let queries = extract_queries(&f, "test");
        let rels = get_rel_deps(&queries, &rel_index, "test");
        // Only the INSERT writes anything.
        assert_eq!(rels.len(), 1);

        Ok(())
    }

    #[test]
    fn extract_queries_keeps_bare_expressions() {
        // The assignment and the IF condition are not standalone SQL; they
        // must still be parsed so calls inside them are visible.
        let f = parse_plpgsql(
            r#"create function f() returns int language plpgsql as $$
            declare v int;
            begin
                v := g(1);
                if h(v) then return 1; end if;
                return 2;
            end $$;"#,
        )
        .unwrap();
        let queries = extract_queries(&f, "test");
        let called: BTreeSet<String> = queries
            .iter()
            .flat_map(crate::call_graph::sql_callees)
            .map(|c| c.name)
            .collect();
        assert!(called.contains("g") && called.contains("h"), "{called:?}");
    }

    #[test]
    fn insert_checks_constraints_on_columns_it_does_not_list() {
        let rel_index = index(vec![table(
            1,
            "users",
            &["id", "email", "name", "status"],
            vec![
                unique("users_email_key", &["email"]),
                // `status` is defaulted by the insert below; the default can
                // still fail the check.
                check("users_status_check", &["status"]),
                Constraint::NotNull(NotNullConstraint {
                    name: ustr("email_not_null"),
                    column: ustr("email"),
                }),
            ],
        )]);

        assert_eq!(
            violations("INSERT INTO users (email, name) VALUES ($1, $2)", &rel_index),
            ["users.email_not_null", "users.users_email_key", "users.users_status_check"]
        );
    }

    #[test]
    fn on_conflict_columns_absorb_the_matching_key_in_any_order() {
        let rel_index = index(vec![table(
            1,
            "t",
            &["a", "b", "c"],
            vec![
                unique("t_a_b_key", &["a", "b"]),
                unique("t_c_key", &["c"]),
                pkey("t_pkey", &["a"]),
            ],
        )]);

        // (b, a) names the same key as (a, b); the other keys still apply.
        assert_eq!(
            violations(
                "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (b, a) DO NOTHING",
                &rel_index
            ),
            ["t.t_c_key", "t.t_pkey"]
        );
        // No target: every unique key is an arbiter.
        assert!(violations(
            "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO NOTHING",
            &rel_index
        )
        .is_empty());
        assert_eq!(
            violations(
                "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT ON CONSTRAINT t_c_key DO NOTHING",
                &rel_index
            ),
            ["t.t_a_b_key", "t.t_pkey"]
        );
    }

    #[test]
    fn on_conflict_does_not_absorb_partial_or_expression_keys() {
        let rel_index = index(vec![table(
            1,
            "t",
            &["email", "deleted_at"],
            vec![partial_unique("t_live_email", &["email"])],
        )]);
        for sql in [
            "INSERT INTO t (email) VALUES ($1) ON CONFLICT (email) DO NOTHING",
            "INSERT INTO t (email) VALUES ($1) ON CONFLICT (lower(email)) DO NOTHING",
        ] {
            assert_eq!(violations(sql, &rel_index), ["t.t_live_email"], "{sql}");
        }
    }

    #[test]
    fn on_conflict_do_update_is_also_an_update_of_the_set_columns() {
        let rel_index = index(vec![table(
            1,
            "t",
            &["id", "qty", "note"],
            vec![pkey("t_pkey", &["id"]), check("t_qty_check", &["qty"])],
        )]);
        let sql = "INSERT INTO t (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET qty = t.qty + 1";
        let deps = deps_of(sql, &rel_index);
        assert_eq!(
            summary(&deps, &rel_index),
            [
                (
                    "t".to_string(),
                    Cmd::Insert {
                        on_conflict: Some(ConflictTarget::Columns(vec![ustr("id")]))
                    }
                ),
                ("t".to_string(), update(&["qty"])),
            ]
        );
        assert_eq!(violations(sql, &rel_index), ["t.t_qty_check"]);
    }

    #[test]
    fn update_only_checks_assigned_columns_but_always_partial_indexes() {
        let rel_index = index(vec![table(
            1,
            "t",
            &["email", "name", "deleted_at"],
            vec![
                unique("t_name_key", &["name"]),
                partial_unique("t_live_email", &["email"]),
            ],
        )]);
        // Un-deleting a row can collide on the partial index although only
        // `deleted_at` is written.
        assert_eq!(
            violations("UPDATE t SET deleted_at = NULL WHERE email = $1", &rel_index),
            ["t.t_live_email"]
        );
    }

    #[test]
    fn update_set_subquery_aliases_are_not_assigned_columns() {
        let rel_index = index(vec![table(1, "t", &["a", "b"], vec![])]);
        let deps = deps_of("UPDATE t SET a = (SELECT 1 AS b)", &rel_index);
        assert_eq!(summary(&deps, &rel_index), [("t".to_string(), update(&["a"]))]);
    }

    #[test]
    fn merge_yields_a_dependency_per_action() {
        let rel_index = index(vec![
            table(
                1,
                "stock",
                &["sku", "qty"],
                vec![pkey("stock_pkey", &["sku"]), check("stock_qty_check", &["qty"])],
            ),
            table(2, "incoming", &["sku", "qty"], vec![]),
        ]);
        let sql = "MERGE INTO stock s USING incoming i ON s.sku = i.sku \
                   WHEN MATCHED AND i.qty = 0 THEN DELETE \
                   WHEN MATCHED THEN UPDATE SET qty = s.qty + i.qty \
                   WHEN NOT MATCHED THEN INSERT (sku, qty) VALUES (i.sku, i.qty)";
        let deps = deps_of(sql, &rel_index);
        assert_eq!(
            summary(&deps, &rel_index),
            [
                ("stock".to_string(), Cmd::Insert { on_conflict: None }),
                ("stock".to_string(), update(&["qty"])),
                ("stock".to_string(), Cmd::Delete),
            ]
        );
        assert_eq!(
            violations(sql, &rel_index),
            ["stock.stock_pkey", "stock.stock_qty_check"]
        );
    }

    #[test]
    fn merge_do_nothing_writes_nothing() {
        let rel_index = index(vec![
            table(1, "stock", &["sku"], vec![]),
            table(2, "incoming", &["sku"], vec![]),
        ]);
        let deps = deps_of(
            "MERGE INTO stock s USING incoming i ON s.sku = i.sku WHEN MATCHED THEN DO NOTHING",
            &rel_index,
        );
        assert!(deps.is_empty(), "{deps:?}");
    }

    /// parent <- child (cascade) <- grandchild (restrict), and
    /// parent <- note (set null).
    fn cascade_index() -> RelIndex {
        index(vec![
            table(1, "parent", &["id", "name"], vec![pkey("parent_pkey", &["id"])]),
            table(
                2,
                "child",
                &["id", "parent_id"],
                vec![
                    pkey("child_pkey", &["id"]),
                    fkey(
                        "child_parent_fkey",
                        &["parent_id"],
                        1,
                        &["id"],
                        FkAction::Cascade,
                        FkAction::Cascade,
                    ),
                ],
            ),
            table(
                3,
                "grandchild",
                &["id", "child_id"],
                vec![fkey(
                    "grandchild_child_fkey",
                    &["child_id"],
                    2,
                    &["id"],
                    FkAction::Restrict,
                    FkAction::NoAction,
                )],
            ),
            table(
                4,
                "note",
                &["id", "parent_id"],
                vec![
                    fkey(
                        "note_parent_fkey",
                        &["parent_id"],
                        1,
                        &["id"],
                        FkAction::SetNull,
                        FkAction::SetDefault,
                    ),
                    check("note_parent_check", &["parent_id"]),
                ],
            ),
        ])
    }

    #[test]
    fn delete_follows_cascades_to_restrict_keys_on_grandchildren() {
        let rel_index = cascade_index();
        let deps = deps_of("DELETE FROM parent WHERE id = $1", &rel_index);
        assert_eq!(
            summary(&deps, &rel_index),
            [
                ("parent".to_string(), Cmd::Delete),
                // ON DELETE CASCADE deletes the children...
                ("child".to_string(), Cmd::Delete),
                // ...and ON DELETE SET NULL updates the notes.
                ("note".to_string(), update(&["parent_id"])),
            ]
        );
        assert_eq!(
            violations("DELETE FROM parent WHERE id = $1", &rel_index),
            [
                // The cascaded child delete is blocked by the grandchild.
                "grandchild.grandchild_child_fkey",
                // The SET NULL write re-checks the note's own constraints.
                "note.note_parent_check",
                "note.note_parent_fkey",
            ]
        );
    }

    #[test]
    fn key_update_follows_on_update_actions() {
        let rel_index = cascade_index();
        let deps = deps_of("UPDATE parent SET id = $1 WHERE id = $2", &rel_index);
        assert_eq!(
            summary(&deps, &rel_index),
            [
                ("parent".to_string(), update(&["id"])),
                ("child".to_string(), update(&["parent_id"])),
                ("note".to_string(), update(&["parent_id"])),
            ]
        );
        // A non-key update strands nothing and cascades nowhere.
        let deps = deps_of("UPDATE parent SET name = $1", &rel_index);
        assert_eq!(summary(&deps, &rel_index), [("parent".to_string(), update(&["name"]))]);
    }

    #[test]
    fn cascade_cycles_terminate() {
        // A self-referencing tree and two tables that cascade into each other.
        let rel_index = index(vec![
            table(
                1,
                "node",
                &["id", "parent_id"],
                vec![fkey(
                    "node_parent_fkey",
                    &["parent_id"],
                    1,
                    &["id"],
                    FkAction::Cascade,
                    FkAction::Cascade,
                )],
            ),
            table(
                2,
                "a",
                &["id", "b_id"],
                vec![fkey("a_b_fkey", &["b_id"], 3, &["id"], FkAction::Cascade, FkAction::NoAction)],
            ),
            table(
                3,
                "b",
                &["id", "a_id"],
                vec![fkey("b_a_fkey", &["a_id"], 2, &["id"], FkAction::Cascade, FkAction::NoAction)],
            ),
        ]);
        let deps = deps_of("DELETE FROM node", &rel_index);
        assert_eq!(summary(&deps, &rel_index), [("node".to_string(), Cmd::Delete)]);
        let deps = deps_of("DELETE FROM a", &rel_index);
        assert_eq!(
            summary(&deps, &rel_index),
            [("a".to_string(), Cmd::Delete), ("b".to_string(), Cmd::Delete)]
        );
    }

    #[test]
    fn dml_through_a_simple_view_reaches_the_base_table() {
        let rel_index = index(vec![
            table(
                1,
                "users",
                &["id", "email", "name"],
                vec![unique("users_email_key", &["email"]), check("users_name_check", &["name"])],
            ),
            view(
                2,
                "active_users",
                &["id", "mail", "name"],
                "SELECT users.id, users.email AS mail, users.name FROM users WHERE users.active",
            ),
            // A view over the view.
            view(3, "vip_users", &["id", "mail"], "SELECT id, mail FROM active_users"),
        ]);

        // The renamed column maps back to the base column.
        let deps = deps_of("UPDATE active_users SET mail = $1", &rel_index);
        assert_eq!(
            summary(&deps, &rel_index),
            [
                ("active_users".to_string(), update(&["mail"])),
                ("users".to_string(), update(&["email"])),
            ]
        );
        assert_eq!(
            violations("UPDATE vip_users SET mail = $1", &rel_index),
            ["users.users_email_key"]
        );
        assert_eq!(
            violations("INSERT INTO active_users (id) VALUES (1)", &rel_index),
            ["users.users_email_key", "users.users_name_check"]
        );
    }

    #[test]
    fn non_updatable_views_are_left_alone() {
        let users = table(1, "users", &["id", "email"], vec![unique("users_email_key", &["email"])]);
        let orders = table(2, "orders", &["id", "user_id"], vec![]);
        for definition in [
            "SELECT u.id, u.email FROM users u JOIN orders o ON o.user_id = u.id",
            "SELECT DISTINCT email FROM users",
            "SELECT email FROM users GROUP BY email",
            "SELECT email FROM users LIMIT 1",
            "SELECT email FROM users UNION SELECT email FROM users",
            "WITH x AS (SELECT 1) SELECT email FROM users",
        ] {
            let rel_index = index(vec![
                users.clone(),
                orders.clone(),
                view(3, "v", &["email"], definition),
            ]);
            let deps = deps_of("UPDATE v SET email = $1", &rel_index);
            assert_eq!(
                summary(&deps, &rel_index),
                [("v".to_string(), update(&["email"]))],
                "{definition}"
            );
        }
    }

    #[test]
    fn writes_to_a_partitioned_table_reach_its_partitions() {
        let mut rel_index = index(vec![
            table(1, "events", &["id", "at"], vec![pkey("events_pkey", &["id", "at"])]),
            table(2, "events_2024", &["id", "at"], vec![pkey("events_2024_pkey", &["id", "at"])]),
            table(3, "events_2025", &["id", "at"], vec![pkey("events_2025_pkey", &["id", "at"])]),
        ]);
        rel_index.add_child(1, 2);
        rel_index.add_child(1, 3);

        // The violation is reported with the partition's names.
        assert_eq!(
            violations("INSERT INTO events (id, at) VALUES ($1, $2)", &rel_index),
            [
                "events.events_pkey",
                "events_2024.events_2024_pkey",
                "events_2025.events_2025_pkey",
            ]
        );
    }

    /// An unqualified DML target is the first relation of that name on the
    /// search path, not `public`'s; one that is not on the path is skipped.
    #[test]
    fn unqualified_targets_follow_the_search_path() {
        let in_schema = |oid, schema: &str, name: &str, constraint: &str| PgRel {
            id: PgId::new(Some(ustr(schema)), ustr(name)),
            ..table(oid, name, &["a"], vec![unique(constraint, &["a"])])
        };
        let rel_index = index(vec![
            in_schema(1, "public", "t", "public_t_key"),
            in_schema(2, "app", "t", "app_t_key"),
            in_schema(3, "hidden", "h", "hidden_h_key"),
            PgRel {
                id: PgId::new(Some(ustr("app")), ustr("v")),
                ..view(4, "v", &["a"], "SELECT a FROM t")
            },
        ])
        .with_search_path(&["app", "public"]);

        assert_eq!(violations("INSERT INTO t (a) VALUES (1)", &rel_index), ["t.app_t_key"]);
        assert_eq!(violations("INSERT INTO public.t (a) VALUES (1)", &rel_index), ["t.public_t_key"]);
        // The view's own unqualified base table resolves the same way.
        assert_eq!(violations("INSERT INTO v (a) VALUES (1)", &rel_index), ["t.app_t_key"]);
        assert!(deps_of("INSERT INTO h (a) VALUES (1)", &rel_index).is_empty());
        assert_eq!(violations("INSERT INTO hidden.h (a) VALUES (1)", &rel_index), ["h.hidden_h_key"]);

        let rel_index = rel_index.with_search_path(&["public", "app"]);
        assert_eq!(violations("UPDATE t SET a = 1", &rel_index), ["t.public_t_key"]);
    }

    #[test]
    fn unknown_targets_are_skipped_not_fatal() {
        let rel_index = index(vec![table(1, "t", &["a"], vec![])]);
        for sql in [
            "INSERT INTO nope VALUES (1)",
            "UPDATE nope SET a = 1",
            "DELETE FROM nope",
            "MERGE INTO nope USING t ON true WHEN MATCHED THEN DELETE",
        ] {
            assert!(deps_of(sql, &rel_index).is_empty(), "{sql}");
        }
    }
}
