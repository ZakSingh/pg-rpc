use crate::config::{Config, ErrorsConfig};
use crate::constraint_analysis::{extract_queries, RelDep};
use crate::ident::sql_to_rs_ident;
use crate::ident::CaseType::Pascal;
use crate::sql_state::{SqlState, SYM_SQL_STATE_TO_CODE};
use jsonpath_rust::JsonPath;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::FuncCall;
use pg_query::ParseResult;
use proc_macro2::TokenStream;
use serde_json::Value;
use std::collections::BTreeSet;

/// Something a function or query can raise other than a constraint
/// violation. Constraint violations are derived from the relations written
/// (see [`crate::constraint_analysis::applicable_constraints`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PgException {
    Explicit(SqlState),
    CustomError(String), // Name of the error type from errors schema
}

impl PgException {
    pub fn rs_name(&self, config: &Config) -> TokenStream {
        match self {
            // A configured description wins; otherwise the standard condition
            // name (`NoDataFound`), falling back to the raw code (`M0422`).
            PgException::Explicit(ex) => match config.exceptions.get(ex.code()) {
                Some(description) => sql_to_rs_ident(description, Pascal),
                None => ex.rs_name(),
            },
            PgException::CustomError(name) => sql_to_rs_ident(name, Pascal),
        }
    }
}

/// Get exceptions from a function body JSON parse, given the relation
/// dependencies already extracted from it (with trigger exceptions populated
/// if the caller wants them).
///
/// Constraint violations are not part of the result: callers derive those
/// from `rel_deps` directly. `_rel_index` is unused and kept only so existing
/// call sites keep compiling.
pub fn get_exceptions_from_deps(
    parsed: &Value,
    rel_deps: &[RelDep],
    _rel_index: &crate::rel_index::RelIndex,
    errors_config: Option<&ErrorsConfig>,
    scope: &str,
) -> anyhow::Result<Vec<PgException>> {
    let (raised_sql_states, custom_errors) =
        get_raised_sql_states_and_custom_errors(parsed, errors_config, scope)?;

    // Collect trigger exceptions from all relation dependencies
    let trigger_exceptions = rel_deps
        .iter()
        .flat_map(|dep| dep.trigger_exceptions.iter().cloned());

    let exceptions: Vec<PgException> = raised_sql_states
        .into_iter()
        .map(PgException::Explicit)
        .chain(custom_errors.into_iter().map(PgException::CustomError))
        .chain(get_strict_exceptions(parsed)?)
        .chain(get_assert_exceptions(parsed)?)
        .chain(trigger_exceptions)
        .collect();

    Ok(exceptions)
}

/// Collect all explicit raise sqlstate codes and custom error types
fn get_raised_sql_states_and_custom_errors(
    fn_json: &Value,
    errors_config: Option<&ErrorsConfig>,
    scope: &str,
) -> anyhow::Result<(BTreeSet<SqlState>, BTreeSet<String>)> {
    let sql_states = raised_sql_states(fn_json)?;

    // Detect custom errors if errors_config is provided using AST parsing
    let mut custom_errors = BTreeSet::new();
    if let Some(config) = errors_config {
        // Every statement *and* bare expression of the body: the raise helper
        // may be called from an assignment or a condition as well as from
        // `CALL` / `PERFORM`.
        let queries = extract_queries(fn_json, scope);
        custom_errors.extend(extract_custom_errors_from_queries(&queries, config));

        // A hand-written `RAISE ... USING HINT = 'application/json', MESSAGE =
        // jsonb_build_object('type', 'some_error', ...)` is the same contract
        // as the raise helper; pick the type out of the literal.
        custom_errors.extend(json_hint_raise_types(fn_json)?);
    }

    Ok((sql_states, custom_errors))
}

/// The typed errors raised through the configured raise function
/// (`errors.raise_function`) anywhere in the given statements: every call to
/// it, at any nesting depth (`CALL`, a select target, a `CASE` branch, a
/// `WHERE` clause, a CTE, ...), whose argument is cast to a type in the
/// errors schema, e.g. `core.raise_error(ROW(1, 'x')::errors.validation_error)`.
///
/// Returns the error type names.
pub fn extract_custom_errors_from_queries(
    queries: &[ParseResult],
    errors_config: &ErrorsConfig,
) -> BTreeSet<String> {
    let raise_function = RaiseFunction::new(errors_config.get_raise_function());
    let errors_schema = errors_config.schema.as_str();
    let mut custom_errors = BTreeSet::new();

    let mut visit = |call: &FuncCall| {
        if raise_function.is_called_by(call) {
            custom_errors.extend(
                call.args
                    .iter()
                    .filter_map(|arg| error_type_of_arg(arg, errors_schema)),
            );
        }
    };
    for query in queries {
        for (node, _, _, _) in query.protobuf.nodes() {
            match node.to_enum() {
                Node::FuncCall(call) => visit(&call),
                Node::CallStmt(stmt) => {
                    if let Some(call) = &stmt.funccall {
                        visit(call);
                    }
                }
                _ => {}
            }
        }
    }

    custom_errors
}

/// The configured raise function, split into schema and name.
struct RaiseFunction<'a> {
    schema: Option<&'a str>,
    name: &'a str,
}

impl<'a> RaiseFunction<'a> {
    fn new(configured: &'a str) -> Self {
        match configured.rsplit_once('.') {
            Some((schema, name)) => Self {
                // `db.schema.name` -> `schema`
                schema: Some(schema.rsplit('.').next().unwrap_or(schema)),
                name,
            },
            None => Self {
                schema: None,
                name: configured,
            },
        }
    }

    /// Whether `call` invokes the raise function. A schema-qualified call
    /// must name the configured schema. An unqualified call (resolved by
    /// `search_path` at runtime) matches on the bare name alone, as does any
    /// call when the configured name is itself unqualified: mistaking a
    /// same-named function for the raise helper only adds a variant.
    fn is_called_by(&self, call: &FuncCall) -> bool {
        let parts: Vec<&str> = call
            .funcname
            .iter()
            .filter_map(|n| match &n.node {
                Some(Node::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .collect();
        match parts.as_slice() {
            [name] => *name == self.name,
            [.., schema, name] => {
                *name == self.name && self.schema.is_none_or(|expected| expected == *schema)
            }
            [] => false,
        }
    }
}

/// The error type an argument of the raise function is cast to, if it is a
/// type in the errors schema: `ROW(...)::errors.some_error`, possibly passed
/// by name (`p_error => ROW(...)::errors.some_error`) or cast again.
fn error_type_of_arg(arg: &pg_query::protobuf::Node, errors_schema: &str) -> Option<String> {
    match arg.node.as_ref()? {
        Node::NamedArgExpr(named) => error_type_of_arg(named.arg.as_deref()?, errors_schema),
        Node::TypeCast(cast) => {
            let type_parts: Vec<&str> = cast
                .type_name
                .as_ref()?
                .names
                .iter()
                .filter_map(|n| match &n.node {
                    Some(Node::String(s)) => Some(s.sval.as_str()),
                    _ => None,
                })
                .collect();
            match type_parts.as_slice() {
                [schema, name] if *schema == errors_schema => Some((*name).to_string()),
                _ => error_type_of_arg(cast.arg.as_deref()?, errors_schema),
            }
        }
        _ => None,
    }
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
    let mut sql_states = BTreeSet::new();

    for stmt in fn_json.query("$..PLpgSQL_stmt_raise")? {
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

    let mut names = BTreeSet::new();
    for stmt in fn_json.query("$..PLpgSQL_stmt_raise")? {
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
    match SYM_SQL_STATE_TO_CODE.get(s) {
        Some(code) => SqlState::from_code(code),
        None => SqlState::from_code(s),
    }
}

/// `SELECT ... INTO STRICT` and `EXECUTE ... INTO STRICT` raise
/// `no_data_found` (P0002) for zero rows and `too_many_rows` (P0003) for more
/// than one.
fn get_strict_exceptions(parsed: &Value) -> anyhow::Result<Vec<PgException>> {
    let mut has_strict = false;
    for path in ["$..PLpgSQL_stmt_execsql", "$..PLpgSQL_stmt_dynexecute"] {
        has_strict |= parsed
            .query(path)?
            .into_iter()
            .any(|stmt| stmt.get("strict").and_then(Value::as_bool) == Some(true));
    }

    Ok(if has_strict {
        vec![
            PgException::Explicit(SqlState::from_code("P0002")),
            PgException::Explicit(SqlState::from_code("P0003")),
        ]
    } else {
        Vec::new()
    })
}

/// `ASSERT condition` raises `assert_failure` (P0004) when the condition is
/// false or null (unless `plpgsql.check_asserts` is off).
fn get_assert_exceptions(parsed: &Value) -> anyhow::Result<Vec<PgException>> {
    Ok(if parsed.query("$..PLpgSQL_stmt_assert")?.is_empty() {
        Vec::new()
    } else {
        vec![PgException::Explicit(SqlState::from_code("P0004"))]
    })
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

        let s = get_strict_exceptions(&parse_plpgsql(fn_def).unwrap()).unwrap();
        assert!(s.contains(&PgException::Explicit(SqlState::from_code("P0002"))), "{s:?}");
        assert!(s.contains(&PgException::Explicit(SqlState::from_code("P0003"))), "{s:?}");
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

        let s = get_strict_exceptions(&parse_plpgsql(fn_def).unwrap()).unwrap();
        assert!(s.is_empty(), "{s:?}");
    }

    fn plpgsql(body: &str) -> Value {
        parse_plpgsql(&format!(
            "create function f() returns void language plpgsql as $$ declare x int; r record; begin {body} end $$;"
        ))
        .unwrap()
    }

    fn codes(exceptions: Vec<PgException>) -> BTreeSet<String> {
        exceptions
            .into_iter()
            .map(|e| match e {
                PgException::Explicit(s) => s.code().to_string(),
                PgException::CustomError(name) => name,
            })
            .collect()
    }

    #[test]
    fn execute_into_strict_raises_like_select_into_strict() {
        let strict = plpgsql("execute 'select id from t where id = $1' into strict x using 1;");
        assert_eq!(
            codes(get_strict_exceptions(&strict).unwrap()),
            BTreeSet::from(["P0002".to_string(), "P0003".to_string()])
        );

        let lax = plpgsql("execute 'select id from t' into x;");
        assert!(get_strict_exceptions(&lax).unwrap().is_empty());
    }

    #[test]
    fn assert_raises_assert_failure() {
        let with_assert = plpgsql("assert x > 0, 'x must be positive';");
        assert_eq!(
            codes(get_assert_exceptions(&with_assert).unwrap()),
            BTreeSet::from(["P0004".to_string()])
        );
        assert!(get_assert_exceptions(&plpgsql("x := 1;")).unwrap().is_empty());
    }

    fn errors_config(raise_function: &str) -> ErrorsConfig {
        ErrorsConfig {
            schema: "errors".to_string(),
            raise_function: Some(raise_function.to_string()),
        }
    }

    fn custom_errors_of(body: &str, raise_function: &str) -> BTreeSet<String> {
        get_raised_sql_states_and_custom_errors(
            &plpgsql(body),
            Some(&errors_config(raise_function)),
            "test",
        )
        .unwrap()
        .1
    }

    fn names(expected: &[&str]) -> BTreeSet<String> {
        expected.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn raise_function_is_found_at_any_depth() {
        // None of these is a `CALL` or a bare select target.
        let found = custom_errors_of(
            r#"
            x := core.raise_error(ROW(1)::errors.in_assignment);
            if core.raise_error(ROW(1)::errors.in_condition) then null; end if;
            perform 1 where core.raise_error(ROW(1)::errors.in_where) is null;
            select case when x > 0 then core.raise_error(ROW(1)::errors.in_case) end into x;
            perform coalesce(x, core.raise_error(ROW(1)::errors.in_coalesce));
            with c as (select core.raise_error(ROW(1)::errors.in_cte)) select 1 from c into x;
            return core.raise_error(ROW(1)::errors.in_return);
            "#,
            "core.raise_error",
        );
        assert_eq!(
            found,
            names(&[
                "in_assignment",
                "in_case",
                "in_coalesce",
                "in_condition",
                "in_cte",
                "in_return",
                "in_where"
            ])
        );
    }

    #[test]
    fn unqualified_raise_call_matches_by_bare_name() {
        // `core` on the search_path: the call carries no schema.
        let found = custom_errors_of(
            "perform raise_error(ROW(1)::errors.not_found); call raise_error(ROW(1)::errors.forbidden);",
            "core.raise_error",
        );
        assert_eq!(found, names(&["forbidden", "not_found"]));
    }

    #[test]
    fn raise_call_in_another_schema_is_not_the_raise_function() {
        let found = custom_errors_of(
            "perform other.raise_error(ROW(1)::errors.not_found); perform core.something_else(ROW(1)::errors.forbidden);",
            "core.raise_error",
        );
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn unqualified_configured_name_matches_qualified_calls() {
        let found = custom_errors_of(
            "perform core.raise_error(ROW(1)::errors.not_found);",
            "raise_error",
        );
        assert_eq!(found, names(&["not_found"]));
    }

    #[test]
    fn named_and_recast_error_arguments_are_recognised() {
        let found = custom_errors_of(
            "perform core.raise_error(p_sqlstate => 'M0422', p_error => ROW(1)::errors.by_name);
             perform core.raise_error((ROW(1)::errors.recast)::record);
             perform core.raise_error(ROW(1)::other.not_an_error);",
            "core.raise_error",
        );
        assert_eq!(found, names(&["by_name", "recast"]));
    }

    #[test]
    fn raise_function_in_sql_statement() {
        let parsed = pg_query::parse(
            "select id from t where id = $1 or raise_error(ROW($1)::errors.missing_thing) is null",
        )
        .unwrap();
        let found = extract_custom_errors_from_queries(&[parsed], &errors_config("core.raise_error"));
        assert_eq!(found, names(&["missing_thing"]));
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

        let errors_config = ErrorsConfig {
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

        let errors_config = ErrorsConfig {
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

        let errors_config = ErrorsConfig {
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

        let errors_config = ErrorsConfig {
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
