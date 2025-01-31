use crate::pg_fn::extract_queries;
use crate::pg_fn::{get_rel_deps, Cmd};
use crate::rel_index::Constraint;
use crate::sql_state::{SqlState, SYM_SQL_STATE_TO_CODE};
use anyhow::anyhow;
use jsonpath_rust::{JsonPath, JsonPathValue};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgException {
    Explicit(SqlState),
    Constraint(Constraint),
    Strict,
    Cast(String, String), // parsed from 'cannot cast x to y' IntegerStringCast
}

pub fn get_exceptions(
    parsed: &Value,
    rel_index: &crate::rel_index::RelIndex,
) -> anyhow::Result<Vec<PgException>> {
    let queries = extract_queries(&parsed);

    let constraints: Vec<&Constraint> = get_rel_deps(&queries, rel_index)?
        .iter()
        .filter(|dep| dep.cmd == Cmd::DML)
        .try_fold(Vec::new(), |mut acc, dep| {
            let rel = rel_index
                .get(&dep.rel_id)
                .ok_or_else(|| anyhow!("Relation {} not found in index", dep.rel_id))?;

            acc.extend(rel.constraints.iter());
            Ok::<_, anyhow::Error>(acc)
        })?;

    let raised_exceptions = get_raised_sql_states(&parsed)?
        .into_iter()
        .map(|sql_state| PgException::Explicit(sql_state));

    let exceptions: Vec<PgException> = constraints
        .into_iter()
        .cloned()
        .map(PgException::Constraint)
        .chain(raised_exceptions)
        .chain(get_strict_exceptions(&parsed).into_iter())
        .collect();

    Ok(exceptions)
}

/// Collect all explicit raise sqlstate codes
fn get_raised_sql_states(fn_json: &Value) -> anyhow::Result<HashSet<SqlState>> {
    let err_raises = JsonPath::try_from("$..PLpgSQL_stmt_raise[?(@.elog_level == 21)].condname")?;

    let code_as_option =
    JsonPath::try_from("$..PLpgSQL_stmt_raise[?(@.elog_level == 21)]..PLpgSQL_raise_option[?(@.opt_type == 0)].expr.PLpgSQL_expr.query")?;

    let err_count = JsonPath::try_from("$..PLpgSQL_stmt_raise[?(@.elog_level == 21)]")?
        .find_slice(fn_json)
        .len();

    let mut sql_states: HashSet<SqlState> = code_as_option
        .find_slice(fn_json)
        .into_iter()
        .chain(err_raises.find_slice(fn_json).into_iter())
        .filter(|x| x.has_value())
        .map(|x| {
            x.to_data()
                .as_str()
                .map(|s| match s.trim_matches('\'') {
                    s => SYM_SQL_STATE_TO_CODE
                        .get(s)
                        .cloned()
                        .unwrap_or_else(|| SqlState::from_code(s)),
                })
                .ok_or_else(|| anyhow!("Failed to parse sql state"))
        })
        .try_collect()?;

    // add P0001 code if errors exist w/o code
    if err_count > sql_states.len() {
        sql_states.insert(SqlState::default());
    }

    Ok(sql_states)
}

fn get_strict_exceptions(parsed: &Value) -> Option<PgException> {
    let strict_path = JsonPath::try_from("$..PLpgSQL_stmt_execsql[?(@.strict)]").unwrap();

    strict_path
        .find_slice(parsed)
        .into_iter()
        .find(|x| match x {
            JsonPathValue::Slice(data, _) => data
                .get("strict")
                .is_some_and(|s| s.as_bool().is_some_and(|b| b)),
            _ => false,
        })
        .map(|_| PgException::Strict)
}
