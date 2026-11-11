use crate::annotations;
use crate::config::Config;
use crate::constraint_analysis::{extract_queries, get_rel_deps, Cmd, ConflictTarget, RelDep};
use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use crate::pg_constraint::Constraint;
use crate::sql_state::{SqlState, SYM_SQL_STATE_TO_CODE};
use crate::trigger_index::TriggerIndex;
use itertools::Itertools;
use jsonpath_rust::{JsonPath, JsonPathValue};
use proc_macro2::TokenStream;
use quote::ToTokens;
use serde_json::Value;
use std::collections::BTreeSet;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PgException {
    Explicit(SqlState),
    Constraint(Constraint),
    Strict,
    CustomError(String), // Name of the error type from errors schema
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
            PgException::CustomError(name) => sql_to_rs_ident(name, Pascal),
        }
    }
}

/// Get exceptions from a function body JSON parse (without trigger analysis)
pub fn get_exceptions(
    parsed: &Value,
    comment: Option<&String>,
    rel_index: &crate::rel_index::RelIndex,
    scope: &str,
) -> anyhow::Result<Vec<PgException>> {
    get_exceptions_with_triggers(parsed, comment, rel_index, None, None, scope)
}

/// Get exceptions from a function body JSON parse with optional trigger analysis
pub fn get_exceptions_with_triggers(
    parsed: &Value,
    comment: Option<&String>,
    rel_index: &crate::rel_index::RelIndex,
    trigger_index: Option<&TriggerIndex>,
    errors_config: Option<&crate::config::ErrorsConfig>,
    scope: &str,
) -> anyhow::Result<Vec<PgException>> {
    let queries = extract_queries(parsed, scope);

    let mut rel_deps = get_rel_deps(&queries, rel_index, scope);

    // Populate trigger exceptions if trigger index is available
    if let Some(trigger_idx) = trigger_index {
        crate::pg_fn::populate_trigger_exceptions(&mut rel_deps, trigger_idx);
    }

    get_exceptions_from_deps(parsed, &rel_deps, comment, rel_index, errors_config, scope)
}

/// Get exceptions from a function body JSON parse, given the relation
/// dependencies already extracted from it (with trigger exceptions populated
/// if the caller wants them).
pub fn get_exceptions_from_deps(
    parsed: &Value,
    rel_deps: &[RelDep],
    comment: Option<&String>,
    rel_index: &crate::rel_index::RelIndex,
    errors_config: Option<&crate::config::ErrorsConfig>,
    scope: &str,
) -> anyhow::Result<Vec<PgException>> {
    let constraints: Vec<&Constraint> = rel_deps
        .iter()
        .flat_map(|dep| {
            let rel_oid = dep.rel_oid;
            let constraints = rel_index.deref().get(&rel_oid).unwrap().constraints.iter();
            match &dep.cmd {
                Cmd::Update { cols } => constraints.filter(|c| c.contains_columns(cols)).collect(),
                Cmd::Insert { cols, on_conflict } => {
                    constraints
                        .filter(|c| {
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
                                            }
                                            ConflictTarget::Constraint(name) => {
                                                c.name().as_str() != name
                                            }
                                            ConflictTarget::Any => false,
                                        }
                                    }
                                    Constraint::Unique(unique) => {
                                        let constraint_cols = &unique.columns;

                                        match &conflict.target {
                                            // If specific columns listed, only filter if they match the constraint
                                            ConflictTarget::Columns(conflict_cols) => {
                                                !constraint_cols.iter().eq(conflict_cols.iter())
                                            }
                                            // If specific constraint named, only filter that one
                                            ConflictTarget::Constraint(name) => &c.name() != name,
                                            // If no target specified, filter all unique/primary key constraints
                                            ConflictTarget::Any => false,
                                        }
                                    }
                                    _ => true,
                                }
                            } else {
                                true
                            }
                        })
                        .collect()
                }
                Cmd::Delete => constraints
                    .filter(|c| matches!(c, Constraint::ForeignKey(_)))
                    .collect(),
                _ => vec![],
            }
        })
        .collect();

    let (raised_sql_states, custom_errors) =
        get_raised_sql_states_and_custom_errors(parsed, errors_config, scope)?;

    let raised_exceptions = raised_sql_states
        .into_iter()
        .map(|sql_state| PgException::Explicit(sql_state));

    let custom_error_exceptions = custom_errors
        .into_iter()
        .map(|error_name| PgException::CustomError(error_name));

    let comment_exceptions = comment
        .map(|c| get_comment_exceptions(c))
        .unwrap_or_default();

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
        .chain(custom_error_exceptions)
        .chain(get_strict_exceptions(parsed).into_iter())
        .chain(comment_exceptions.into_iter())
        .chain(trigger_exceptions.into_iter())
        .collect();

    Ok(exceptions)
}

/// Collect all explicit raise sqlstate codes and custom error types
fn get_raised_sql_states_and_custom_errors(
    fn_json: &Value,
    errors_config: Option<&crate::config::ErrorsConfig>,
    scope: &str,
) -> anyhow::Result<(BTreeSet<SqlState>, BTreeSet<String>)> {
    let sql_states = raised_sql_states(fn_json)?;

    // Detect custom errors if errors_config is provided using AST parsing
    let mut custom_errors = BTreeSet::new();
    if let Some(config) = errors_config {
        // Extract queries from the function JSON and parse them
        let queries = crate::pg_fn::extract_queries(fn_json, scope);

        // Use AST-based detection to find custom errors (and their optional SQLSTATEs)
        let found_errors = crate::pg_fn::extract_custom_errors_from_queries(&queries, config);

        #[cfg(test)]
        eprintln!("Custom errors found via AST: {:?}", found_errors);

        // Add all found custom errors
        for error_type in found_errors {
            custom_errors.insert(error_type);
            // Note: We no longer automatically add a specific SQLSTATE here
            // The SQLSTATE will be determined by the raise_error call or RAISE statement
        }

        // A hand-written `RAISE ... USING HINT = 'application/json', MESSAGE =
        // jsonb_build_object('type', 'some_error', ...)` is the same contract
        // as the raise helper; pick the type out of the literal.
        custom_errors.extend(json_hint_raise_types(fn_json)?);
    }

    Ok((sql_states, custom_errors))
}

/// SQLSTATEs statically raised by `RAISE EXCEPTION` statements in a function.
///
/// Per statement: `RAISE condition_name` and `USING ERRCODE = 'XXXXX'` (or a
/// quoted condition name) contribute that code; a `RAISE` with no `ERRCODE`
/// contributes the default `P0001`. An `ERRCODE` that is an expression rather
/// than a string literal (`USING ERRCODE = p_sqlstate`, as in a generic
/// raise helper) is dynamic and contributes nothing, since any value is
/// possible at runtime.
fn raised_sql_states(fn_json: &Value) -> anyhow::Result<BTreeSet<SqlState>> {
    let raise_path = JsonPath::try_from("$..PLpgSQL_stmt_raise")?;
    let mut sql_states = BTreeSet::new();

    for slice in raise_path.find_slice(fn_json) {
        let JsonPathValue::Slice(stmt, _) = slice else { continue };
        // 21 is ERROR; lower levels (NOTICE, WARNING, …) don't abort the call.
        if stmt.get("elog_level").and_then(Value::as_i64) != Some(21) {
            continue;
        }

        if let Some(condname) = stmt.get("condname").and_then(Value::as_str) {
            sql_states.insert(sql_state_from_literal(condname));
            continue;
        }

        // `USING ERRCODE = <expr>` is raise option type 0.
        let errcode_expr = stmt
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|o| o.get("PLpgSQL_raise_option"))
            .find(|o| o.get("opt_type").and_then(Value::as_i64) == Some(0))
            .and_then(|o| o.pointer("/expr/PLpgSQL_expr/query"))
            .and_then(Value::as_str)
            .map(str::trim);

        match errcode_expr {
            None => {
                sql_states.insert(SqlState::default());
            }
            Some(expr) => {
                let literal = expr
                    .strip_prefix('\'')
                    .and_then(|e| e.strip_suffix('\''));
                match literal {
                    Some(code) => {
                        sql_states.insert(sql_state_from_literal(code));
                    }
                    None => log::debug!(
                        "RAISE with dynamic ERRCODE `{}` contributes no static SQLSTATE",
                        expr
                    ),
                }
            }
        }
    }

    Ok(sql_states)
}

/// Error type names from `RAISE` statements that set `HINT = 'application/json'`
/// and build their message with a literal `'type', '<name>'` pair.
fn json_hint_raise_types(fn_json: &Value) -> anyhow::Result<BTreeSet<String>> {
    static TYPE_LITERAL: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
        regex::Regex::new(r#"'type'\s*,\s*'([A-Za-z_][A-Za-z0-9_]*)'"#).unwrap()
    });

    let raise_path = JsonPath::try_from("$..PLpgSQL_stmt_raise")?;
    let mut names = BTreeSet::new();
    for slice in raise_path.find_slice(fn_json) {
        let JsonPathValue::Slice(stmt, _) = slice else { continue };
        if stmt.get("elog_level").and_then(Value::as_i64) != Some(21) {
            continue;
        }
        let options: Vec<&Value> = stmt
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|o| o.get("PLpgSQL_raise_option"))
            .collect();
        let option_query = |opt_type: i64| {
            options
                .iter()
                .find(|o| o.get("opt_type").and_then(Value::as_i64) == Some(opt_type))
                .and_then(|o| o.pointer("/expr/PLpgSQL_expr/query"))
                .and_then(Value::as_str)
        };
        // opt_type 3 is HINT, 1 is MESSAGE.
        let is_json = option_query(3)
            .map(|h| h.trim().trim_matches('\'') == "application/json")
            .unwrap_or(false);
        if !is_json {
            continue;
        }
        if let Some(message) = option_query(1) {
            for cap in TYPE_LITERAL.captures_iter(message) {
                names.insert(cap[1].to_string());
            }
        }
    }
    Ok(names)
}

/// `unique_violation` -> `23505`; anything else is taken as a literal code.
fn sql_state_from_literal(s: &str) -> SqlState {
    SYM_SQL_STATE_TO_CODE
        .get(s)
        .cloned()
        .unwrap_or_else(|| SqlState::from_code(s))
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
    annotations::parse_throws(comment)
        .into_iter()
        .map(|code| PgException::Explicit(SqlState::from_code(&code)))
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use pg_query::parse_plpgsql;

    fn states_of(body: &str) -> BTreeSet<String> {
        let def = format!(
            "create function f() returns void language plpgsql as $$ begin {} end $$;",
            body
        );
        raised_sql_states(&parse_plpgsql(&def).unwrap())
            .unwrap()
            .into_iter()
            .map(|s| s.code().to_string())
            .collect()
    }

    #[test]
    fn literal_errcode_does_not_also_add_p0001() {
        assert_eq!(
            states_of("raise exception 'nope' using errcode = 'P0002';"),
            BTreeSet::from(["P0002".to_string()])
        );
    }

    #[test]
    fn bare_raise_is_p0001() {
        assert_eq!(
            states_of("raise exception 'nope';"),
            BTreeSet::from(["P0001".to_string()])
        );
    }

    #[test]
    fn condition_names_map_to_codes() {
        assert_eq!(
            states_of("raise unique_violation; raise exception 'x' using errcode = 'check_violation';"),
            BTreeSet::from(["23505".to_string(), "23514".to_string()])
        );
    }

    #[test]
    fn dynamic_errcode_contributes_nothing() {
        // A generic raise helper: `using errcode = p_sqlstate`. Any code is
        // possible at runtime, so no static variant must be invented.
        let def = "create function f(p_sqlstate text) returns void language plpgsql as $$ \
                   begin raise exception 'x' using errcode = p_sqlstate; end $$;";
        let states = raised_sql_states(&parse_plpgsql(def).unwrap()).unwrap();
        assert!(states.is_empty(), "{states:?}");
    }

    #[test]
    fn notices_are_not_exceptions() {
        assert!(states_of("raise notice 'just saying';").is_empty());
    }

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

        let (exceptions, _) =
            get_raised_sql_states_and_custom_errors(&parse_plpgsql(fn_def)?, None, "test")?;

        assert_eq!(exceptions.len(), 5);

        Ok(())
    }

    #[test]
    fn test_detect_core_raise_error_calls() -> anyhow::Result<()> {
        let fn_def = r#"
        CREATE OR REPLACE FUNCTION test_fn() RETURNS BOOLEAN
        LANGUAGE plpgsql AS $$
        BEGIN
            IF some_condition THEN
                CALL core.raise_error(ROW(1, 'test')::errors.validation_error);
            END IF;
            
            -- Alternative syntax
            PERFORM core.raise_error(ROW(2)::errors.authorization_error);
            
            RETURN TRUE;
        END;
        $$;
        "#;

        let parsed = parse_plpgsql(fn_def)?;

        let errors_config = crate::config::ErrorsConfig {
            schema: "errors".to_string(),
            raise_function: Some("core.raise_error".to_string()),
        };

        let (_sql_states, custom_errors) =
            get_raised_sql_states_and_custom_errors(&parsed, Some(&errors_config), "test")?;

        // Should detect both custom error types
        assert!(
            custom_errors.contains("validation_error"),
            "Should detect validation_error from CALL statement"
        );
        assert!(
            custom_errors.contains("authorization_error"),
            "Should detect authorization_error from PERFORM statement"
        );

        Ok(())
    }

    #[test]
    fn test_detect_hint_application_json() -> anyhow::Result<()> {
        let fn_def = r#"
        CREATE OR REPLACE FUNCTION test_fn() RETURNS BOOLEAN
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION SQLSTATE '23505' USING
                HINT = 'application/json',
                MESSAGE = '{"type": "duplicate_key", "table": "users"}';
            RETURN TRUE;
        END;
        $$;
        "#;

        let parsed = parse_plpgsql(fn_def)?;

        let errors_config = crate::config::ErrorsConfig {
            schema: "errors".to_string(),
            raise_function: Some("core.raise_error".to_string()),
        };

        let (sql_states, _custom_errors) =
            get_raised_sql_states_and_custom_errors(&parsed, Some(&errors_config), "test")?;

        // Should detect the SQLSTATE 23505 (unique violation)
        assert!(
            sql_states.iter().any(|s| s.code() == "23505"),
            "Should detect SQLSTATE 23505 from RAISE statement"
        );

        Ok(())
    }

    #[test]
    fn test_raise_error_with_custom_sqlstate() -> anyhow::Result<()> {
        let fn_def = r#"
        CREATE OR REPLACE FUNCTION test_fn() RETURNS BOOLEAN
        LANGUAGE plpgsql AS $$
        BEGIN
            -- Using custom SQLSTATE
            CALL core.raise_error(ROW(1)::errors.validation_error, 'M0422');
            RETURN TRUE;
        END;
        $$;
        "#;

        let parsed = parse_plpgsql(fn_def)?;

        let errors_config = crate::config::ErrorsConfig {
            schema: "errors".to_string(),
            raise_function: Some("core.raise_error".to_string()),
        };

        let (_sql_states, custom_errors) =
            get_raised_sql_states_and_custom_errors(&parsed, Some(&errors_config), "test")?;

        // Should detect the validation_error type
        assert!(
            custom_errors.contains("validation_error"),
            "Should detect validation_error from CALL with custom SQLSTATE"
        );

        Ok(())
    }

    #[test]
    fn test_detect_custom_raise_function() -> anyhow::Result<()> {
        let fn_def = r#"
        CREATE OR REPLACE FUNCTION test_fn() RETURNS BOOLEAN
        LANGUAGE plpgsql AS $$
        BEGIN
            CALL my_app.throw_error(ROW(1)::errors.custom_error);
            RETURN TRUE;
        END;
        $$;
        "#;

        let parsed = parse_plpgsql(fn_def)?;

        let errors_config = crate::config::ErrorsConfig {
            schema: "errors".to_string(),
            raise_function: Some("my_app.throw_error".to_string()),
        };

        let (_sql_states, custom_errors) =
            get_raised_sql_states_and_custom_errors(&parsed, Some(&errors_config), "test")?;

        // Should detect custom error with different function name
        assert!(
            custom_errors.contains("custom_error"),
            "Should detect custom_error from configured raise function"
        );

        Ok(())
    }
}
