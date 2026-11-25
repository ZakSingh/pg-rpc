use crate::codegen::OID;
use crate::call_graph::{self, CallTarget};
use crate::constraint_analysis::{extract_queries, get_rel_deps, trigger_event_for, RelDep};
use crate::exceptions::{get_exceptions_from_deps, PgException};
use crate::fn_index::FunctionIndex;
use itertools::Itertools;
use std::collections::BTreeSet;
use crate::rel_index::RelIndex;
use anyhow::Context;
use pg_query::parse_plpgsql;
use postgres::Client;
use std::collections::HashMap;

/// Represents the event that triggers the function
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

/// Information about a trigger function
#[derive(Debug, Clone)]
pub struct TriggerFunction {
    pub name: String,
    pub schema: String,
    /// Exceptions that this trigger function can raise, including those of
    /// the functions it calls and of the triggers its DML fires.
    pub exceptions: Vec<PgException>,
    /// DML the trigger function performs, itself or through its callees and
    /// the triggers that DML fires.
    pub rel_deps: Vec<RelDep>,
    /// Functions called from the trigger body, by name as written.
    pub callees: BTreeSet<CallTarget>,
}

impl TriggerFunction {
    /// Add exceptions and DML that reach this function indirectly.
    fn absorb(&mut self, exceptions: Vec<PgException>, rel_deps: Vec<RelDep>) {
        if !exceptions.is_empty() {
            self.exceptions.extend(exceptions);
            self.exceptions = std::mem::take(&mut self.exceptions)
                .into_iter()
                .unique()
                .collect();
        }
        self.rel_deps.extend(rel_deps);
    }
}

/// Information about a trigger
#[derive(Debug, Clone)]
pub struct Trigger {
    pub oid: OID,
    pub name: String,
    /// The events that fire it. Timing (BEFORE / AFTER / INSTEAD OF) and
    /// level (ROW / STATEMENT) are not tracked: whichever they are, an error
    /// in the trigger fails the statement.
    pub events: Vec<TriggerEvent>,
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
    /// Covers relations in every user schema: a generated function may write
    /// to a table outside the codegen schemas (directly or via a helper), and
    /// the triggers on that table can still fail the call. Partitioned tables
    /// and views (`INSTEAD OF` triggers) are included.
    pub fn new(
        db: &mut Client,
        rel_index: &RelIndex,
        errors_config: Option<&crate::config::ErrorsConfig>,
        fn_index: &FunctionIndex,
    ) -> anyhow::Result<Self> {
        let trigger_sql = include_str!("./queries/trigger_introspection.sql");

        let mut triggers_by_table: HashMap<OID, Vec<Trigger>> = HashMap::new();

        let rows = db
            .query(trigger_sql, &[])
            .context("Trigger introspection query failed")?;

        for row in rows {
            // Postgres `oid` columns deserialize as `u32`, not `i32` —
            // postgres-rust rejects the i32 conversion for oid types.
            let table_oid: OID = row.try_get("table_oid")?;
            let trigger_oid: OID = row.try_get("trigger_oid")?;

            let trigger_name: String = row.try_get("trigger_name")?;
            let table_name = format!(
                "{}.{}",
                row.try_get::<_, String>("schema_name")?,
                row.try_get::<_, String>("table_name")?
            );
            let events = parse_events(
                &row.try_get::<_, Vec<String>>("events")?,
                &trigger_name,
                &table_name,
            );

            let function_name: String = row.try_get("function_name")?;
            let function_schema: String = row.try_get("function_schema")?;
            let function_source: String = row.try_get("function_source")?;

            // Analyze the trigger function for exceptions. A function that
            // cannot be analyzed still fires; say so instead of silently
            // treating it as raising nothing.
            let (exceptions, rel_deps, callees) = Self::analyze_trigger_function(
                &function_source,
                rel_index,
                errors_config,
                &format!("trigger function {function_schema}.{function_name}"),
            )
            .unwrap_or_else(|e| {
                log::warn!(
                    "Could not analyze trigger {trigger_name} on {table_name} \
                     (function {function_schema}.{function_name}): {e:#}. \
                     Errors it raises will be missing from generated error enums."
                );
                Default::default()
            });

            let trigger_function = TriggerFunction {
                name: function_name,
                schema: function_schema,
                exceptions,
                rel_deps,
                callees,
            };

            let trigger = Trigger {
                oid: trigger_oid,
                name: trigger_name,
                events,
                function: trigger_function,
            };

            triggers_by_table
                .entry(table_oid)
                .or_default()
                .push(trigger);
        }

        let mut index = Self { triggers_by_table };
        index.follow_calls(fn_index);
        index.follow_triggers();
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
            function.absorb(extra_exceptions, extra_deps);
        }
    }

    /// Union into each trigger function what the triggers fired by its own
    /// DML raise and write, to any depth: a trigger that writes table B makes
    /// B's triggers part of every statement that fires it. Runs after
    /// [`Self::follow_calls`], so DML done through callees counts.
    fn follow_triggers(&mut self) {
        let by_oid: HashMap<OID, &Trigger> = self
            .triggers_by_table
            .values()
            .flatten()
            .map(|t| (t.oid, t))
            .collect();
        let fired_by = |trigger: &Trigger| -> Vec<OID> {
            trigger
                .function
                .rel_deps
                .iter()
                .flat_map(|dep| {
                    self.get_triggers_for_event(dep.rel_oid, &trigger_event_for(&dep.cmd))
                })
                .map(|t| t.oid)
                .collect()
        };

        let mut additions: HashMap<OID, (Vec<PgException>, Vec<RelDep>)> = HashMap::new();
        for trigger in by_oid.values() {
            // Every trigger reachable from this one. `seen` starts with the
            // trigger itself, so one that re-fires itself terminates.
            let mut seen = BTreeSet::from([trigger.oid]);
            let mut pending = fired_by(trigger);
            let mut exceptions = Vec::new();
            let mut deps = Vec::new();
            while let Some(oid) = pending.pop() {
                if !seen.insert(oid) {
                    continue;
                }
                let nested = by_oid[&oid];
                exceptions.extend(nested.function.exceptions.iter().cloned());
                deps.extend(nested.function.rel_deps.iter().cloned());
                pending.extend(fired_by(nested));
            }
            additions.insert(trigger.oid, (exceptions, deps));
        }

        for trigger in self.triggers_by_table.values_mut().flatten() {
            if let Some((exceptions, deps)) = additions.remove(&trigger.oid) {
                trigger.function.absorb(exceptions, deps);
            }
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
                    rel_index,
                    errors_config,
                    scope,
                )?;
                let callees = call_graph::plpgsql_callees(&parsed);
                Ok((exceptions, rel_deps, callees))
            }
            Err(e) => {
                // If parsing fails, try to extract basic RAISE statements with regex
                log::warn!(
                    "{scope}: could not parse the PL/pgSQL body ({e}); \
                     only its RAISE EXCEPTION statements are analyzed"
                );
                let exceptions = Self::extract_basic_exceptions(source)?;
                Ok((exceptions, Vec::new(), BTreeSet::new()))
            }
        }
    }

    /// Fallback method to extract basic RAISE EXCEPTION statements using regex
    fn extract_basic_exceptions(
        source: &str,
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

        Ok(exceptions)
    }
}

/// Decode the event names the introspection query reports for a trigger.
/// A name this code does not know is logged, not silently dropped.
fn parse_events(events: &[String], trigger_name: &str, table_name: &str) -> Vec<TriggerEvent> {
    events
        .iter()
        .filter_map(|event| match event.as_str() {
            "INSERT" => Some(TriggerEvent::Insert),
            "UPDATE" => Some(TriggerEvent::Update),
            "DELETE" => Some(TriggerEvent::Delete),
            other => {
                log::warn!(
                    "Trigger {trigger_name} on {table_name}: ignoring unknown event '{other}'"
                );
                None
            }
        })
        .collect()
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

        let exceptions = TriggerIndex::extract_basic_exceptions(source).unwrap();
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

    #[test]
    fn events_are_decoded_and_unknown_ones_skipped() {
        let events = parse_events(
            &["INSERT".to_string(), "TRUNCATE".to_string(), "DELETE".to_string()],
            "trg",
            "public.t",
        );
        assert_eq!(events, [TriggerEvent::Insert, TriggerEvent::Delete]);
    }

    #[test]
    fn unparsable_trigger_body_falls_back_to_raise_scan() {
        // Not valid PL/pgSQL, but the RAISE is still found.
        let (exceptions, rel_deps, callees) = TriggerIndex::analyze_trigger_function(
            "BEGIN this is not plpgsql; RAISE EXCEPTION SQLSTATE 'M0001' USING MESSAGE = 'x'; END",
            &RelIndex::default(),
            None,
            "trigger function public.broken",
        )
        .unwrap();
        assert!(exceptions
            .iter()
            .any(|e| matches!(e, PgException::Explicit(s) if s.code() == "M0001")));
        assert!(rel_deps.is_empty() && callees.is_empty());
    }
}
