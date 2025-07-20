use crate::config::Config;
use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use crate::pg_constraint::Constraint;
use crate::pg_fn::{extract_queries, ConflictTarget};
use crate::pg_fn::{get_rel_deps, Cmd};
use crate::sql_state::{SqlState, SYM_SQL_STATE_TO_CODE};
use crate::trigger_index::TriggerIndex;
use anyhow::anyhow;
use jsonpath_rust::{JsonPath, JsonPathValue};
use quote::ToTokens;
use proc_macro2::TokenStream;
use serde_json::Value;
use std::collections::HashSet;
use itertools::Itertools;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PgException {
    Explicit(SqlState),
    Constraint(Constraint),
    Strict,
    // Cast(String, String), // parsed from 'cannot cast x to y' IntegerStringCast
}

impl PgException {
    pub fn rs_name(&self, config: &Config) -> TokenStream {
        match self {
            PgException::Explicit(ex) => sql_to_rs_ident(
                config
                    .exceptions
                    .get(ex.code())
                    .map(|x| x.as_str())
                    .unwrap_or(ex.code()),
                Pascal,
            ),
            PgException::Strict => "Strict".parse().unwrap(),
            PgException::Constraint(c) => c.to_token_stream(),
        }
    }
}

/// Get exceptions from a function body JSON parse (without trigger analysis)
pub fn get_exceptions(
    parsed: &Value,
    comment: Option<&String>,
    rel_index: &crate::rel_index::RelIndex,
) -> anyhow::Result<Vec<PgException>> {
    get_exceptions_with_triggers(parsed, comment, rel_index, None)
}

/// Get exceptions from a function body JSON parse with optional trigger analysis
pub fn get_exceptions_with_triggers(
    parsed: &Value,
    comment: Option<&String>,
    rel_index: &crate::rel_index::RelIndex,
    trigger_index: Option<&TriggerIndex>,
) -> anyhow::Result<Vec<PgException>> {
    let queries = extract_queries(&parsed);

    let mut rel_deps = get_rel_deps(&queries, rel_index);
    
    // Populate trigger exceptions if trigger index is available
    if let Some(trigger_idx) = trigger_index {
        crate::pg_fn::populate_trigger_exceptions(&mut rel_deps, trigger_idx);
    }

    let constraints: Vec<&Constraint> = rel_deps
        .iter()
        .flat_map(|dep| {
            let rel_oid = dep.rel_oid;
            let constraints = rel_index.get(&rel_oid).unwrap().constraints.iter();
            match &dep.cmd {
                Cmd::Update { cols } => constraints.filter(|c| c.contains_columns(cols)).collect(),
                Cmd::Insert { cols, on_conflict } => {
                    constraints.filter(|c| {
                        // Filter out constraints that we know will not apply via static analysis.

                        #[cfg(not(feature = "null_tracking"))]
                        if matches!(c, Constraint::NotNull(..)) {
                            return false;
                        }

                        // Default constraints can't cause an exception
                        if matches!(c, Constraint::Default(..)) {
                            return false;
                        }

                        if !c.contains_columns(cols) {
                            // If the constraint doesn't apply to the columns being inserted,
                            // we can safely assume it will never occur.
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
                                        },
                                        ConflictTarget::Constraint(name) => {
                                            c.name().as_str() != name
                                        },
                                        ConflictTarget::Any => false,
                                    }
                                }
                                Constraint::Unique(unique) => {
                                    let constraint_cols = &unique.columns;

                                    match &conflict.target {
                                        // If specific columns listed, only filter if they match the constraint
                                        ConflictTarget::Columns(conflict_cols) => {
                                            !constraint_cols.iter().eq(conflict_cols.iter())
                                        },
                                        // If specific constraint named, only filter that one
                                        ConflictTarget::Constraint(name) => {
                                            &c.name() != name
                                        },
                                        // If no target specified, filter all unique/primary key constraints
                                        ConflictTarget::Any => false,
                                    }
                                }
                                _ => true,
                            }
                        } else {
                            true
                        }
                    }).collect()
                },
                Cmd::Delete => constraints
                    .filter(|c| matches!(c, Constraint::ForeignKey(_)))
                    .collect(),
                _ => vec![],
            }
        })
        .collect();

    let raised_exceptions = get_raised_sql_states(&parsed)?
        .into_iter()
        .map(|sql_state| PgException::Explicit(sql_state));

    let comment_exceptions = comment.map(|c| get_comment_exceptions(c)).unwrap_or_default();
    
    // Collect trigger exceptions from all relation dependencies
    let trigger_exceptions: Vec<PgException> = rel_deps
        .iter()
        .flat_map(|dep| dep.trigger_exceptions.iter().cloned())
        .collect();

    let exceptions: Vec<PgException> = constraints
        .into_iter()
        .unique_by(|c| c.name())
        .cloned()
        .map(PgException::Constraint)
        .chain(raised_exceptions)
        .chain(get_strict_exceptions(&parsed).into_iter())
        .chain(comment_exceptions.into_iter())
        .chain(trigger_exceptions.into_iter())
        .collect();

    Ok(exceptions)
}

/// Collect all explicit raise sqlstate codes
fn get_raised_sql_states(fn_json: &Value) -> anyhow::Result<HashSet<SqlState>> {
    let err_raises = JsonPath::try_from("$..PLpgSQL_stmt_raise[?(@.elog_level == 21)].condname")?;

    let code_as_option =
    JsonPath::try_from("$..PLpgSQL_stmt_raise[?(@.elog_level == 21)]..PLpgSQL_raise_option[?(@.opt_type == 0)].expr.PLpgSQL_expr.query")?;

    let binding = JsonPath::try_from("$..PLpgSQL_stmt_raise[?(@.elog_level == 21)]")?;
    let errs = binding
        .find_slice(fn_json);

    let err_count = if errs.first().is_some_and(|e| matches!(e, JsonPathValue::NoValue)) {
        0
    } else { errs.len() };

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

pub fn get_comment_exceptions(comment: &str) -> Vec<PgException> {
    let re = Regex::new(r"@pgrpc_throws\s+([a-zA-Z0-9]{5})").unwrap();
    re.captures_iter(comment)
      .filter_map(|cap| cap.get(1).map(|m| PgException::Explicit(SqlState::from_code(m.as_str()))))
      .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use pg_query::parse_plpgsql;

    #[test]
    fn test_strict_exception() {
        let fn_def = r#"
        create or replace function raises() returns void as
        $$
        declare
            x int;
        begin
            select id into strict x
            from t;
        end;
        $$ language plpgsql;
        "#;

        let s = get_strict_exceptions(&parse_plpgsql(fn_def).unwrap());
        assert_eq!(s, Some(PgException::Strict));
    }

    #[test]
    fn test_non_strict_exception() {
        let fn_def = r#"
        create or replace function raises() returns void as
        $$
        declare
            x int;
        begin
            select id into x
            from t;
        end;
        $$ language plpgsql;
        "#;

        let s = get_strict_exceptions(&parse_plpgsql(fn_def).unwrap());
        assert_eq!(s, None);
    }

    #[test]
    fn test_explicit_raise() -> anyhow::Result<()> {
        let fn_def = r#"
        create or replace function raises() returns void as
        $$
        begin
            raise notice 'not an error'; -- should be ignored
            raise division_by_zero;
            raise exception 'error two'; -- should default to P0001
            raise sqlstate '22014';
            raise unique_violation using message = 'Duplicate user ID: ' || 2;
            raise 'Duplicate user ID %', 1; -- should default to P0001
            raise 'hello world' using errcode = 'P0007', detail = 'det', hint = 'hint';
        end;
        $$ language plpgsql;
        "#;

        let exceptions = get_raised_sql_states(&parse_plpgsql(fn_def)?)?;

        assert_eq!(exceptions.len(), 5);

        Ok(())
    }
}
