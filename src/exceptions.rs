use crate::config::Config;
use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use crate::pg_constraint::Constraint;
use crate::pg_fn::extract_queries;
use crate::pg_fn::{get_rel_deps, Cmd};
use crate::sql_state::{SqlState, SYM_SQL_STATE_TO_CODE};
use anyhow::anyhow;
use jsonpath_rust::{JsonPath, JsonPathValue};
use quote::ToTokens;
use quote::__private::TokenStream;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub fn get_exceptions(
    parsed: &Value,
    rel_index: &crate::rel_index::RelIndex,
) -> anyhow::Result<Vec<PgException>> {
    let queries = extract_queries(&parsed);

    let constraints: Vec<&Constraint> = get_rel_deps(&queries, rel_index)?
        .iter()
        .flat_map(|dep| {
            let rel_oid = dep.rel_oid;
            let constraints = rel_index.get(&rel_oid).unwrap().constraints.iter();
            match &dep.cmd {
                Cmd::Update { cols } => constraints.filter(|c| c.contains_columns(cols)).collect(),
                Cmd::Insert { cols } => constraints.filter(|c| c.contains_columns(cols)).collect(),
                Cmd::Delete => constraints
                    .filter(|c| matches!(c, Constraint::ForeignKey(_)))
                    .collect(),
                _ => vec![],
            }
        })
        .collect();

    dbg!(&constraints);

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

        dbg!(&exceptions);
        assert_eq!(exceptions.len(), 5);

        Ok(())
    }
}
