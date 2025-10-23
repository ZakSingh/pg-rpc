use crate::codegen::OID;
use crate::rel_index::RelIndex;
use crate::sql_parser::{ParameterSpec, ParsedQuery, QueryType};
use crate::ty_index::TypeIndex;
use crate::view_nullability::ViewNullabilityAnalyzer;
use anyhow::{anyhow, Context, Result};
use postgres::Client;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct QueryParam {
    pub name: String,
    pub type_oid: OID,
    pub position: usize,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct QueryColumn {
    pub name: String,
    pub type_oid: OID,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct IntrospectedQuery {
    pub name: String,
    pub query_type: QueryType,
    pub sql: String,
    pub params: Vec<QueryParam>,
    pub return_columns: Option<Vec<QueryColumn>>,
    pub file_path: String,
    pub line_number: usize,
}

pub struct QueryIntrospector<'a> {
    client: &'a mut Client,
    rel_index: &'a RelIndex,
    type_index: &'a TypeIndex,
    view_nullability_cache: &'a crate::view_nullability::ViewNullabilityCache,
}

impl<'a> QueryIntrospector<'a> {
    pub fn new(
        client: &'a mut Client,
        rel_index: &'a RelIndex,
        type_index: &'a TypeIndex,
        view_nullability_cache: &'a crate::view_nullability::ViewNullabilityCache,
    ) -> Self {
        Self {
            client,
            rel_index,
            type_index,
            view_nullability_cache,
        }
    }

    /// Introspect a parsed query to determine parameter and return types
    pub fn introspect(&mut self, parsed: &ParsedQuery) -> Result<IntrospectedQuery> {
        log::info!(
            "Introspecting query '{}' from {}:{}",
            parsed.name,
            parsed.file_path.display(),
            parsed.line_number
        );

        // Use a unique prepared statement name for this query
        let stmt_name = format!("_pgrpc_{}", parsed.name.to_lowercase());

        // Prepare the statement to analyze parameters
        let prepare_sql = format!("PREPARE {} AS {}", stmt_name, parsed.postgres_sql);

        self.client
            .execute(&prepare_sql, &[])
            .with_context(|| {
                format!(
                    "Failed to prepare query '{}' from {}:{}\nSQL: {}",
                    parsed.name,
                    parsed.file_path.display(),
                    parsed.line_number,
                    parsed.postgres_sql
                )
            })?;

        // Get parameter types from pg_prepared_statements
        let param_types = self.get_parameter_types(&stmt_name)?;

        // Determine parameter names and nullable flags
        let param_info = self.determine_parameter_info(&parsed.parameters, &parsed.postgres_sql, param_types.len())?;

        // Build parameter list
        let params: Vec<QueryParam> = param_types
            .into_iter()
            .enumerate()
            .map(|(i, type_oid)| {
                let (name, nullable) = param_info.get(i).cloned().unwrap_or_else(|| (format!("param_{}", i + 1), false));
                QueryParam {
                    name,
                    type_oid,
                    position: i + 1,
                    nullable,
                }
            })
            .collect();

        // Get return columns for queries that return data
        let return_columns = match parsed.query_type {
            QueryType::One | QueryType::Many => {
                // Check if this is a SELECT or has RETURNING clause
                // Pass the prepared statement name so we can use it for introspection
                Some(self.introspect_return_columns(&stmt_name, &parsed.postgres_sql)?)
            }
            QueryType::Exec | QueryType::ExecRows => None,
        };

        // Deallocate prepared statement
        self.client
            .execute(&format!("DEALLOCATE {}", stmt_name), &[])
            .ok(); // Ignore errors on cleanup

        Ok(IntrospectedQuery {
            name: parsed.name.clone(),
            query_type: parsed.query_type.clone(),
            sql: parsed.postgres_sql.clone(),
            params,
            return_columns,
            file_path: parsed.file_path.display().to_string(),
            line_number: parsed.line_number,
        })
    }

    /// Get parameter types from a prepared statement
    fn get_parameter_types(&mut self, stmt_name: &str) -> Result<Vec<OID>> {
        // Query parameter_types, which is an array of regtype (OID)
        // We need to unnest the array to get individual OIDs
        let rows = self
            .client
            .query(
                "SELECT unnest(parameter_types)::oid as param_type FROM pg_prepared_statements WHERE name = $1",
                &[&stmt_name],
            )
            .context("Failed to query pg_prepared_statements")?;

        let param_type_oids: Vec<OID> = rows.iter().map(|row| row.get(0)).collect();
        Ok(param_type_oids)
    }

    /// Introspect return columns using the prepared statement
    fn introspect_return_columns(&mut self, stmt_name: &str, sql: &str) -> Result<Vec<QueryColumn>> {
        // Check if this is a SELECT query or an INSERT/UPDATE/DELETE with RETURNING
        let is_select = sql.trim_start().to_uppercase().starts_with("SELECT");
        let has_returning = sql.to_uppercase().contains("RETURNING");

        if is_select {
            // For SELECT queries, create a temporary view
            self.introspect_select_columns(sql)
        } else if has_returning {
            // For INSERT/UPDATE/DELETE with RETURNING, execute the prepared statement in a transaction
            self.introspect_returning_columns(stmt_name, sql)
        } else {
            Ok(Vec::new())
        }
    }

    /// Introspect columns from a SELECT query by creating a temporary view
    fn introspect_select_columns(&mut self, sql: &str) -> Result<Vec<QueryColumn>> {
        // Create a unique temporary view name using timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let view_name = format!("_pgrpc_introspect_{}", timestamp);

        log::debug!("[INTROSPECT] View name: {}", view_name);
        log::debug!("[INTROSPECT] Original SQL: {}", sql);

        // Begin a transaction for temp view
        self.client.execute("BEGIN", &[])?;

        // Replace parameter markers with NULL::unknown to create a valid view
        // This allows us to introspect column names and types
        let sql_without_params = regex::Regex::new(r"\$\d+")
            .unwrap()
            .replace_all(sql, "NULL::unknown");

        log::debug!("[INTROSPECT] SQL with params replaced: {}", sql_without_params);

        // Create temporary view
        let create_view_sql = format!("CREATE TEMP VIEW {} AS {}", view_name, sql_without_params);
        self.client
            .execute(&create_view_sql, &[])
            .with_context(|| format!("Failed to create temp view for introspection: {}", create_view_sql))?;

        log::debug!("[INTROSPECT] Temporary view created successfully");

        // Query column information from PostgreSQL catalogs
        // Using pg_attribute instead of information_schema to properly handle temporary views
        let columns_query = r#"
            SELECT
                a.attname AS column_name,
                a.atttypid AS type_oid,
                NOT a.attnotnull AS is_nullable
            FROM pg_class c
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE c.relname = $1
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        "#;

        let rows = self.client.query(columns_query, &[&view_name])?;

        log::debug!("[INTROSPECT] pg_attribute query returned {} rows", rows.len());

        let mut columns = Vec::new();
        for row in rows {
            let column_name: String = row.get(0);
            let type_oid: OID = row.get(1);
            let is_nullable: bool = row.get(2);

            log::debug!("[INTROSPECT] Found column: {} (OID: {}, nullable: {})", column_name, type_oid, is_nullable);

            columns.push(QueryColumn {
                name: column_name,
                type_oid,
                nullable: is_nullable,
            });
        }

        log::debug!("[INTROSPECT] Total columns collected: {}", columns.len());

        // Drop the temporary view and rollback
        self.client.execute(&format!("DROP VIEW {}", view_name), &[])?;
        self.client.execute("ROLLBACK", &[])?;

        // Apply nullability analysis if we have a SELECT query
        if let Some(refined_columns) = self.refine_nullability(sql, &columns)? {
            log::debug!("[INTROSPECT] After refine_nullability: {} columns", refined_columns.len());
            Ok(refined_columns)
        } else {
            log::debug!("[INTROSPECT] No refinement applied, returning {} columns", columns.len());
            Ok(columns)
        }
    }

    /// Introspect columns from INSERT/UPDATE/DELETE with RETURNING clause
    fn introspect_returning_columns(&mut self, stmt_name: &str, sql: &str) -> Result<Vec<QueryColumn>> {
        // For RETURNING clauses, we need to try executing the statement to get column metadata
        // We'll use a savepoint so we can rollback if the execution fails
        self.client.execute("BEGIN", &[])?;
        self.client.execute("SAVEPOINT introspect", &[])?;

        // Get the number of parameters
        let param_count_row = self.client.query_one(
            "SELECT array_length(parameter_types, 1) FROM pg_prepared_statements WHERE name = $1",
            &[&stmt_name],
        )?;

        let param_count: Option<i32> = param_count_row.get(0);
        let param_count = param_count.unwrap_or(0) as usize;

        // Execute the prepared statement with NULL parameters
        let execute_sql = if param_count > 0 {
            let placeholders = (1..=param_count).map(|_| "NULL").collect::<Vec<_>>().join(", ");
            format!("EXECUTE {}({})", stmt_name, placeholders)
        } else {
            format!("EXECUTE {}", stmt_name)
        };

        // Try to execute and get the result metadata
        // If it fails (e.g., NOT NULL constraint), we rollback to savepoint and extract column info from the error
        let result = self.client.query(&execute_sql, &[]);

        let mut columns = Vec::new();

        match result {
            Ok(rows) => {
                // Success - get column metadata from the result
                if let Some(first_row) = rows.first() {
                    for column in first_row.columns() {
                        columns.push(QueryColumn {
                            name: column.name().to_string(),
                            type_oid: column.type_().oid(),
                            nullable: true, // Conservative: assume nullable for RETURNING
                        });
                    }
                }
            }
            Err(e) => {
                // Execution failed (e.g., NOT NULL constraint violation)
                // Rollback to savepoint
                self.client.execute("ROLLBACK TO SAVEPOINT introspect", &[])?;

                log::warn!("Failed to execute prepared statement for introspection, will parse RETURNING clause: {}", e);

                // Since execution failed, parse the RETURNING clause as fallback
                columns = self.parse_returning_clause(sql)?;
            }
        }

        self.client.execute("ROLLBACK", &[])?;

        Ok(columns)
    }

    /// Parse the RETURNING clause to extract column names and look up their types
    fn parse_returning_clause(&mut self, sql: &str) -> Result<Vec<QueryColumn>> {
        // Find the RETURNING keyword
        let upper_sql = sql.to_uppercase();
        if let Some(returning_pos) = upper_sql.find("RETURNING") {
            let returning_part = &sql[returning_pos + 9..]; // Skip "RETURNING"
            let returning_clause = returning_part.split(';').next().unwrap_or(returning_part).trim();

            // Parse column names (very simple parser, just split by comma)
            let column_names: Vec<String> = returning_clause
                .split(',')
                .map(|s| {
                    // Extract column name (handle "table.column" and "column AS alias")
                    let trimmed = s.trim();
                    if let Some(as_pos) = trimmed.to_uppercase().find(" AS ") {
                        trimmed[as_pos + 4..].trim().to_string()
                    } else if trimmed == "*" {
                        "*".to_string()
                    } else {
                        // Remove table prefix if present
                        trimmed.split('.').last().unwrap_or(trimmed).trim().to_string()
                    }
                })
                .collect();

            // For each column name, try to resolve its type
            // This is a best-effort approach
            let mut columns = Vec::new();
            for col_name in column_names {
                // For simplicity, use a generic type (text) - this will be refined later
                let type_oid = self.resolve_type_name("text").unwrap_or(25); // 25 is OID for text

                columns.push(QueryColumn {
                    name: col_name,
                    type_oid,
                    nullable: true,
                });
            }

            Ok(columns)
        } else {
            Ok(Vec::new())
        }
    }

    /// Refine column nullability using ViewNullabilityAnalyzer
    fn refine_nullability(&self, sql: &str, columns: &[QueryColumn]) -> Result<Option<Vec<QueryColumn>>> {
        // Try to apply view nullability analysis
        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

        let mut analyzer = ViewNullabilityAnalyzer::new(self.rel_index, self.view_nullability_cache);

        match analyzer.analyze_view(sql, &column_names) {
            Ok(nullability_map) => {
                let refined_columns = columns
                    .iter()
                    .map(|col| {
                        let is_not_null = nullability_map.get(&col.name).copied().unwrap_or(false);
                        QueryColumn {
                            name: col.name.clone(),
                            type_oid: col.type_oid,
                            nullable: !is_not_null,
                        }
                    })
                    .collect();

                Ok(Some(refined_columns))
            }
            Err(e) => {
                log::warn!("Could not apply nullability analysis: {}", e);
                Ok(None)
            }
        }
    }

    /// Resolve a PostgreSQL type name to an OID
    fn resolve_type_name(&mut self, type_name: &str) -> Result<OID> {
        let row = self
            .client
            .query_one(
                "SELECT oid FROM pg_type WHERE typname = $1",
                &[&type_name],
            )
            .with_context(|| format!("Failed to resolve type name: {}", type_name))?;

        Ok(row.get(0))
    }

    /// Determine parameter names and nullable flags from specs, inference, or fallback
    fn determine_parameter_info(
        &self,
        param_specs: &[ParameterSpec],
        sql: &str,
        param_count: usize,
    ) -> Result<Vec<(String, bool)>> {
        let mut param_info = Vec::new();

        for i in 0..param_count {
            let position = i + 1;

            // Priority 1: Check for explicit @name or @name?
            if let Some(param_spec) = param_specs.iter().find(|s| match s {
                ParameterSpec::Named { position: pos, .. } => *pos == position,
                ParameterSpec::Positional { position: pos, .. } => *pos == position,
            }) {
                let (name, nullable) = match param_spec {
                    ParameterSpec::Named { name, nullable, .. } => (name.clone(), *nullable),
                    ParameterSpec::Positional { nullable, .. } => {
                        // For positional, try to infer name, otherwise use fallback
                        let inferred_name = self.infer_param_name_from_ast(sql, position)
                            .unwrap_or_else(|| format!("param_{}", position));
                        (inferred_name, *nullable)
                    }
                };
                param_info.push((name, nullable));
                continue;
            }

            // Priority 2: Try AST inference (no spec found)
            if let Some(inferred_name) = self.infer_param_name_from_ast(sql, position) {
                param_info.push((inferred_name, false));
                continue;
            }

            // Priority 3: Fallback
            param_info.push((format!("param_{}", position), false));
        }

        Ok(param_info)
    }

    /// Infer parameter name from SQL AST by finding column_name = $N patterns
    fn infer_param_name_from_ast(&self, sql: &str, position: usize) -> Option<String> {
        // Parse the SQL to find column references near this parameter
        match pg_query::parse(sql) {
            Ok(parse_result) => {
                // Look for patterns like "column_name = $N" or "column_name IN ($N)"
                // For now, use a simple heuristic: look for the param marker in the SQL
                let param_marker = format!("${}", position);

                // Find the parameter in the SQL text
                if let Some(param_pos) = sql.find(&param_marker) {
                    // Look backwards for a potential column name
                    let before = &sql[..param_pos];

                    // Try to extract column name using regex
                    // Look for patterns like "column_name = $N" or "column_name > $N"
                    if let Some(captures) =
                        regex::Regex::new(r"(\w+)\s*[=<>!]+\s*$").unwrap().captures(before)
                    {
                        if let Some(col_name) = captures.get(1) {
                            let name = col_name.as_str().to_string();
                            // Avoid SQL keywords
                            if !Self::is_sql_keyword(&name) {
                                return Some(name);
                            }
                        }
                    }
                }

                None
            }
            Err(_) => None,
        }
    }

    /// Check if a string is a SQL keyword (simple heuristic)
    fn is_sql_keyword(s: &str) -> bool {
        matches!(
            s.to_uppercase().as_str(),
            "SELECT" | "FROM" | "WHERE" | "AND" | "OR" | "IN" | "NOT" | "NULL" | "IS" | "AS"
        )
    }
}
