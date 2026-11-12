use crate::codegen::OID;
use crate::call_graph::{self, CallTarget};
use crate::constraint_analysis::{extract_queries, get_rel_deps, RelDep};
use crate::exceptions::{get_exceptions_from_deps, PgException};
use crate::fn_index::FunctionIndex;
use itertools::Itertools;
use std::collections::BTreeSet;
use crate::rel_index::RelIndex;
use pg_query::parse_plpgsql;
use postgres::Client;
use std::collections::HashMap;

/// Represents timing of when a trigger fires
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
}

/// Represents the event that triggers the function
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Represents the level at which a trigger fires
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerLevel {
    Row,
    Statement,
}

/// Information about a trigger function
#[derive(Debug, Clone)]
pub struct TriggerFunction {
    pub oid: OID,
    pub name: String,
    pub schema: String,
    pub source: String,
    pub comment: Option<String>,
    /// Exceptions that this trigger function can raise, including those of
    /// the functions it calls.
    pub exceptions: Vec<PgException>,
    /// DML the trigger function (and its callees) performs.
    pub rel_deps: Vec<RelDep>,
    /// Functions called from the trigger body, by name as written.
    pub callees: BTreeSet<CallTarget>,
}

/// Information about a trigger
#[derive(Debug, Clone)]
pub struct Trigger {
    pub oid: OID,
    pub name: String,
    pub table_oid: OID,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub level: TriggerLevel,
    pub function: TriggerFunction,
}

/// Index of triggers organized by table OID
#[derive(Debug, Clone)]
pub struct TriggerIndex {
    /// Maps table OID to list of triggers on that table
    triggers_by_table: HashMap<OID, Vec<Trigger>>,
}

impl TriggerIndex {
    /// Create a new trigger index by querying the database
    ///
    /// Covers tables in every user schema: a generated function may write to a
    /// table outside the codegen schemas (directly or via a helper), and the
    /// triggers on that table can still fail the call.
    pub fn new(
        db: &mut Client,
        rel_index: &RelIndex,
        errors_config: Option<&crate::config::ErrorsConfig>,
        fn_index: &FunctionIndex,
    ) -> anyhow::Result<Self> {
        let trigger_sql = include_str!("./queries/trigger_introspection.sql");

        let mut triggers_by_table: HashMap<OID, Vec<Trigger>> = HashMap::new();

        let rows = db.query(trigger_sql, &[])?;

        for row in rows {
            // Postgres `oid` columns deserialize as `u32`, not `i32` —
            // postgres-rust rejects the i32 conversion for oid types.
            let table_oid: OID = row.get("table_oid");
            let trigger_oid: OID = row.get("trigger_oid");

            let trigger_name: String = row.get("trigger_name");
            let timing_str: String = row.get("timing");
            let events_array: Vec<String> = row.get("events");
            let level_str: String = row.get("level");

            let function_oid: OID = row.get("function_oid");
            let function_name: String = row.get("function_name");
            let function_schema: String = row.get("function_schema");
            let function_source: String = row.get("function_source");
            let function_comment: Option<String> = row.get("function_comment");

            // Parse timing
            let timing = match timing_str.as_str() {
                "BEFORE" => TriggerTiming::Before,
                "AFTER" => TriggerTiming::After,
                _ => continue, // Skip unknown timing
            };

            // Parse events
            let events: Vec<TriggerEvent> = events_array
                .into_iter()
                .filter_map(|event| match event.as_str() {
                    "INSERT" => Some(TriggerEvent::Insert),
                    "UPDATE" => Some(TriggerEvent::Update),
                    "DELETE" => Some(TriggerEvent::Delete),
                    "TRUNCATE" => Some(TriggerEvent::Truncate),
                    _ => None,
                })
                .collect();

            // Parse level
            let level = match level_str.as_str() {
                "ROW" => TriggerLevel::Row,
                "STATEMENT" => TriggerLevel::Statement,
                _ => continue, // Skip unknown level
            };

            // Analyze the trigger function for exceptions
            let (exceptions, rel_deps, callees) = Self::analyze_trigger_function(
                &function_source,
                function_comment.as_ref(),
                rel_index,
                errors_config,
                &format!("trigger function {function_schema}.{function_name}"),
            )
            .unwrap_or_default();

            let trigger_function = TriggerFunction {
                oid: function_oid,
                name: function_name,
                schema: function_schema,
                source: function_source,
                comment: function_comment,
                exceptions,
                rel_deps,
                callees,
            };

            let trigger = Trigger {
                oid: trigger_oid,
                name: trigger_name,
                table_oid,
                timing,
                events,
                level,
                function: trigger_function,
            };

            triggers_by_table
                .entry(table_oid)
                .or_default()
                .push(trigger);
        }

        let mut index = Self { triggers_by_table };
        index.follow_calls(fn_index);
        Ok(index)
    }

    /// Union each trigger function's callees' exceptions and DML into the
    /// trigger, so `perform core.validate(...)` inside a trigger body surfaces
    /// on every function that writes the triggered table. Callee data is
    /// already transitive after `FunctionIndex::follow_calls`.
    fn follow_calls(&mut self, fn_index: &FunctionIndex) {
        for trigger in self.triggers_by_table.values_mut().flatten() {
            let function = &mut trigger.function;
            let mut extra_exceptions = Vec::new();
            let mut extra_deps = Vec::new();
            for target in &function.callees {
                let Some(callee) = fn_index.resolve(target, Some(&function.schema)) else {
                    continue;
                };
                log::debug!(
                    "trigger function {}.{} calls {}.{}; folding its errors in",
                    function.schema,
                    function.name,
                    callee.schema,
                    callee.name
                );
                extra_exceptions.extend(callee.exceptions.iter().cloned());
                extra_deps.extend(callee.rel_deps.iter().cloned());
            }
            if extra_exceptions.is_empty() && extra_deps.is_empty() {
                continue;
            }
            function.exceptions.extend(extra_exceptions);
            function.exceptions = std::mem::take(&mut function.exceptions)
                .into_iter()
                .unique()
                .collect();
            function.rel_deps.extend(extra_deps);
        }
    }

    /// DML performed by the triggers that fire for an event on a table.
    pub fn get_rel_deps_for_event(&self, table_oid: OID, event: &TriggerEvent) -> Vec<RelDep> {
        self.get_triggers_for_event(table_oid, event)
            .into_iter()
            .flat_map(|trigger| trigger.function.rel_deps.iter().cloned())
            .collect()
    }

    /// Get all triggers for a specific table
    pub fn get_triggers_for_table(&self, table_oid: OID) -> &[Trigger] {
        self.triggers_by_table
            .get(&table_oid)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get triggers for a specific table and event type
    pub fn get_triggers_for_event(&self, table_oid: OID, event: &TriggerEvent) -> Vec<&Trigger> {
        self.get_triggers_for_table(table_oid)
            .iter()
            .filter(|trigger| trigger.events.contains(event))
            .collect()
    }

    /// Get all exceptions that could be raised by triggers on a table for a specific event
    pub fn get_exceptions_for_event(
        &self,
        table_oid: OID,
        event: &TriggerEvent,
    ) -> Vec<PgException> {
        self.get_triggers_for_event(table_oid, event)
            .into_iter()
            .flat_map(|trigger| &trigger.function.exceptions)
            .cloned()
            .collect()
    }

    /// Analyze a trigger function source code for potential exceptions
    fn analyze_trigger_function(
        source: &str,
        comment: Option<&String>,
        rel_index: &RelIndex,
        errors_config: Option<&crate::config::ErrorsConfig>,
        scope: &str,
    ) -> anyhow::Result<(Vec<PgException>, Vec<RelDep>, BTreeSet<CallTarget>)> {
        // Create a mock function definition for parsing
        let function_def = format!(
            "CREATE OR REPLACE FUNCTION trigger_func() RETURNS TRIGGER AS $$ {} $$ LANGUAGE plpgsql;",
            source
        );

        // Parse the function using pg_query
        match parse_plpgsql(&function_def) {
            Ok(parsed) => {
                let queries = extract_queries(&parsed, scope);
                let rel_deps = get_rel_deps(&queries, rel_index, scope);
                let exceptions = get_exceptions_from_deps(
                    &parsed,
                    &rel_deps,
                    comment,
                    rel_index,
                    errors_config,
                    scope,
                )?;
                let callees = call_graph::plpgsql_callees(&parsed);
                Ok((exceptions, rel_deps, callees))
            }
            Err(_) => {
                // If parsing fails, try to extract basic RAISE statements with regex
                let exceptions = Self::extract_basic_exceptions(source, comment)?;
                Ok((exceptions, Vec::new(), BTreeSet::new()))
            }
        }
    }

    /// Fallback method to extract basic RAISE EXCEPTION statements using regex
    fn extract_basic_exceptions(
        source: &str,
        comment: Option<&String>,
    ) -> anyhow::Result<Vec<PgException>> {
        use crate::sql_state::SqlState;
        use regex::Regex;

        let mut exceptions = Vec::new();

        // Pattern to match RAISE EXCEPTION statements
        let raise_pattern = Regex::new(r"(?i)\bRAISE\s+EXCEPTION\s+([^;]+)")?;
        let sqlstate_pattern = Regex::new(r"(?i)SQLSTATE\s+'([A-Z0-9]{5})'")?;

        for cap in raise_pattern.captures_iter(source) {
            if let Some(statement) = cap.get(1) {
                let stmt = statement.as_str();

                // Look for SQLSTATE codes
                if let Some(sqlstate_cap) = sqlstate_pattern.captures(stmt) {
                    if let Some(code) = sqlstate_cap.get(1) {
                        exceptions.push(PgException::Explicit(SqlState::from_code(code.as_str())));
                    }
                }
                // If no SQLSTATE found, assume default P0001
                else if !stmt.trim().is_empty() {
                    exceptions.push(PgException::Explicit(SqlState::default()));
                }
            }
        }

        // Add comment-based exceptions if present
        if let Some(comment) = comment {
            use crate::exceptions::get_comment_exceptions;
            exceptions.extend(get_comment_exceptions(comment));
        }

        Ok(exceptions)
    }

    /// Get all tables that have triggers
    pub fn get_tables_with_triggers(&self) -> Vec<OID> {
        self.triggers_by_table.keys().copied().collect()
    }

    /// Check if a table has any triggers for a specific event
    pub fn has_triggers_for_event(&self, table_oid: OID, event: &TriggerEvent) -> bool {
        !self.get_triggers_for_event(table_oid, event).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_basic_exceptions() {
        let source = r#"
            BEGIN
                IF NEW.age < 0 THEN
                    RAISE EXCEPTION 'Age cannot be negative';
                END IF;
                
                IF NEW.email IS NULL THEN
                    RAISE EXCEPTION SQLSTATE '23502' USING MESSAGE = 'Email is required';
                END IF;
            END;
        "#;

        let exceptions = TriggerIndex::extract_basic_exceptions(source, None).unwrap();
        assert_eq!(exceptions.len(), 2);

        // Should find one default P0001 and one explicit 23502
        let has_default = exceptions
            .iter()
            .any(|e| matches!(e, PgException::Explicit(state) if state.code() == "P0001"));
        let has_not_null = exceptions
            .iter()
            .any(|e| matches!(e, PgException::Explicit(state) if state.code() == "23502"));

        assert!(has_default);
        assert!(has_not_null);
    }
}
