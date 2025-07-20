use crate::codegen::OID;
use crate::exceptions::{get_exceptions, PgException};
use crate::rel_index::RelIndex;
use postgres::Client;
use std::collections::HashMap;
use pg_query::parse_plpgsql;

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
    /// Exceptions that this trigger function can raise
    pub exceptions: Vec<PgException>,
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
    pub fn new(db: &mut Client, rel_index: &RelIndex, schemas: &[String]) -> anyhow::Result<Self> {
        let trigger_sql = include_str!("./queries/trigger_introspection.sql");
        
        let mut triggers_by_table: HashMap<OID, Vec<Trigger>> = HashMap::new();
        
        let rows = db.query(trigger_sql, &[])?;
        
        for row in rows {
            let schema_name: String = row.get("schema_name");
            
            // Only include triggers on tables in the requested schemas
            if !schemas.contains(&schema_name) {
                continue;
            }
            
            let table_oid: i32 = row.get("table_oid");
            let table_oid = table_oid as OID;
            
            let trigger_oid: i32 = row.get("trigger_oid");
            let trigger_oid = trigger_oid as OID;
            
            let trigger_name: String = row.get("trigger_name");
            let timing_str: String = row.get("timing");
            let events_array: Vec<String> = row.get("events");
            let level_str: String = row.get("level");
            
            let function_oid: i32 = row.get("function_oid");
            let function_oid = function_oid as OID;
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
            let exceptions = Self::analyze_trigger_function(&function_source, function_comment.as_ref(), rel_index)
                .unwrap_or_else(|_| Vec::new());
            
            let trigger_function = TriggerFunction {
                oid: function_oid,
                name: function_name,
                schema: function_schema,
                source: function_source,
                comment: function_comment,
                exceptions,
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
            
            triggers_by_table.entry(table_oid).or_default().push(trigger);
        }
        
        Ok(Self { triggers_by_table })
    }
    
    /// Get all triggers for a specific table
    pub fn get_triggers_for_table(&self, table_oid: OID) -> &[Trigger] {
        self.triggers_by_table.get(&table_oid).map(|v| v.as_slice()).unwrap_or(&[])
    }
    
    /// Get triggers for a specific table and event type
    pub fn get_triggers_for_event(&self, table_oid: OID, event: &TriggerEvent) -> Vec<&Trigger> {
        self.get_triggers_for_table(table_oid)
            .iter()
            .filter(|trigger| trigger.events.contains(event))
            .collect()
    }
    
    /// Get all exceptions that could be raised by triggers on a table for a specific event
    pub fn get_exceptions_for_event(&self, table_oid: OID, event: &TriggerEvent) -> Vec<PgException> {
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
        rel_index: &RelIndex
    ) -> anyhow::Result<Vec<PgException>> {
        // Create a mock function definition for parsing
        let function_def = format!(
            "CREATE OR REPLACE FUNCTION trigger_func() RETURNS TRIGGER AS $$ {} $$ LANGUAGE plpgsql;",
            source
        );
        
        // Parse the function using pg_query
        match parse_plpgsql(&function_def) {
            Ok(parsed) => {
                // Use existing exception analysis logic
                get_exceptions(&parsed, comment, rel_index)
            },
            Err(_) => {
                // If parsing fails, try to extract basic RAISE statements with regex
                Self::extract_basic_exceptions(source, comment)
            }
        }
    }
    
    /// Fallback method to extract basic RAISE EXCEPTION statements using regex
    fn extract_basic_exceptions(source: &str, comment: Option<&String>) -> anyhow::Result<Vec<PgException>> {
        use regex::Regex;
        use crate::sql_state::SqlState;
        
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
        let has_default = exceptions.iter().any(|e| matches!(e, PgException::Explicit(state) if state.code() == "P0001"));
        let has_not_null = exceptions.iter().any(|e| matches!(e, PgException::Explicit(state) if state.code() == "23502"));
        
        assert!(has_default);
        assert!(has_not_null);
    }
}