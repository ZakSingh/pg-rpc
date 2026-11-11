//! Static call-graph extraction for PL/pgSQL and SQL functions.
//!
//! A function's error enum should describe everything that can go wrong while
//! it runs, including what happens inside the helpers it calls. The exception
//! and DML analyses in [`crate::exceptions`] and [`crate::constraint_analysis`]
//! only look at a single function body, so this module finds the *callees* of
//! a body and [`crate::fn_index::FunctionIndex::follow_calls`] unions the
//! callees' analysis results into each caller.
//!
//! Only static, direct calls are found: `perform s.f(...)`, `v := s.f(...)`,
//! `select s.f(...) into ...`, `select * from s.f(...)`, `call s.p(...)`, and
//! calls nested inside any of those expressions. Dynamic SQL (`execute`) is
//! not inspected.

use jsonpath_rust::JsonPath;
use once_cell::sync::Lazy;
use pg_query::protobuf::node::Node;
use pg_query::ParseResult;
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeSet;

/// A function referenced by name from inside another function's body.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CallTarget {
    /// Explicit schema qualifier, if the call was written `schema.name(...)`.
    pub schema: Option<String>,
    pub name: String,
}

/// Leading `target :=` of a PL/pgSQL assignment, e.g. `v := `, `r.f := `,
/// `arr[i] := `.
static ASSIGN_PREFIX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\s*[A-Za-z_][\w.]*(?:\[[^\]]*\])*\s*:=\s*").unwrap());

/// Parse one `PLpgSQL_expr.query` string into a SQL AST.
///
/// libpg_query stores statement-level queries (`SELECT …`, `INSERT …`, the
/// rewritten `PERFORM`) verbatim, but assignment right-hand sides, `IF`
/// conditions and `RETURN` expressions are bare expressions. Those are wrapped
/// in a `SELECT` so they parse; the `target :=` prefix of an assignment is
/// stripped first.
pub fn parse_plpgsql_expr(query: &str) -> Option<ParseResult> {
    if let Ok(parsed) = pg_query::parse(query) {
        return Some(parsed);
    }
    let expr = ASSIGN_PREFIX.replace(query, "");
    pg_query::parse(&format!("SELECT {}", expr)).ok()
}

/// All functions called from a PL/pgSQL function body (the JSON produced by
/// `pg_query::parse_plpgsql`).
pub fn plpgsql_callees(fn_json: &Value) -> BTreeSet<CallTarget> {
    let query_path = JsonPath::try_from("$..PLpgSQL_expr.query").unwrap();
    let mut out = BTreeSet::new();
    for slice in query_path.find_slice(fn_json) {
        let data = slice.to_data();
        let Some(query) = data.as_str() else { continue };
        if let Some(parsed) = parse_plpgsql_expr(query) {
            out.extend(sql_callees(&parsed));
        }
    }
    out
}

/// All functions called from a parsed SQL statement (or SQL-language function
/// body), including calls nested in `FROM`, `WHERE`, subqueries and CTEs.
pub fn sql_callees(parsed: &ParseResult) -> BTreeSet<CallTarget> {
    let mut out = BTreeSet::new();
    for (node, _, _, _) in parsed.protobuf.nodes() {
        let funcname = match node.to_enum() {
            Node::FuncCall(f) => f.funcname.clone(),
            Node::CallStmt(stmt) => match &stmt.funccall {
                Some(f) => f.funcname.clone(),
                None => continue,
            },
            _ => continue,
        };
        if let Some(target) = call_target(&funcname) {
            out.insert(target);
        }
    }
    out
}

/// Turn a `funcname` list (`[schema, name]` or `[name]`) into a [`CallTarget`].
fn call_target(funcname: &[pg_query::protobuf::Node]) -> Option<CallTarget> {
    let parts: Vec<&str> = funcname
        .iter()
        .filter_map(|n| match &n.node {
            Some(Node::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect();
    match parts.as_slice() {
        [name] => Some(CallTarget {
            schema: None,
            name: (*name).to_string(),
        }),
        [schema, name] => Some(CallTarget {
            schema: Some((*schema).to_string()),
            name: (*name).to_string(),
        }),
        // `db.schema.name` or an empty list; not something we resolve.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(schema: Option<&str>, name: &str) -> CallTarget {
        CallTarget {
            schema: schema.map(str::to_string),
            name: name.to_string(),
        }
    }

    #[test]
    fn finds_calls_in_every_plpgsql_position() {
        let def = r#"
        CREATE FUNCTION api.caller(p_id int) RETURNS int LANGUAGE plpgsql AS $$
        DECLARE
          v int;
          r record;
        BEGIN
          v := core.helper_assign(p_id);
          v := v + core.helper_nested(p_id, 'x');
          PERFORM core.helper_perform(p_id);
          SELECT core.helper_select(p_id) INTO v;
          SELECT * INTO r FROM core.helper_from(p_id);
          INSERT INTO core.t (a) SELECT x FROM core.helper_insert_src(p_id);
          IF core.helper_if(p_id) THEN RETURN 1; END IF;
          CALL core.proc_call(p_id);
          RETURN unqualified_return(v);
        END
        $$;"#;
        let json = pg_query::parse_plpgsql(def).unwrap();
        let callees = plpgsql_callees(&json);

        for name in [
            "helper_assign",
            "helper_nested",
            "helper_perform",
            "helper_select",
            "helper_from",
            "helper_insert_src",
            "helper_if",
            "proc_call",
        ] {
            assert!(
                callees.contains(&target(Some("core"), name)),
                "missing core.{name} in {callees:?}"
            );
        }
        assert!(callees.contains(&target(None, "unqualified_return")));
    }

    #[test]
    fn assignment_prefix_variants_are_stripped() {
        for q in ["v := f(1)", "r.field := f(1)", "arr[i] := f(1)", "  v:=f(1)"] {
            let parsed = parse_plpgsql_expr(q).unwrap_or_else(|| panic!("{q} did not parse"));
            assert!(sql_callees(&parsed).contains(&target(None, "f")), "{q}");
        }
    }

    #[test]
    fn sql_function_body_calls() {
        let parsed = pg_query::parse(
            "select a.x, core.g(b.y) from core.f(1) a join lateral core.h(a.x) b on true where exists (select core.k())",
        )
        .unwrap();
        let callees = sql_callees(&parsed);
        for name in ["f", "g", "h", "k"] {
            assert!(callees.contains(&target(Some("core"), name)), "missing {name}");
        }
    }
}
