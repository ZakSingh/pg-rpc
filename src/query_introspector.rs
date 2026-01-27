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

#[derive(Debug, Clone, Copy, PartialEq)]
enum QueryAstType {
    Select,              // SELECT or CTE with final SELECT
    DmlWithReturning,    // INSERT/UPDATE/DELETE with RETURNING
    DmlWithoutReturning, // INSERT/UPDATE/DELETE without RETURNING
    Unknown,             // Parsing failed
}

fn determine_query_type_from_ast(sql: &str) -> QueryAstType {
    match pg_query::parse(sql) {
        Ok(result) => {
            if let Some(stmt) = result.protobuf.stmts.first() {
                if let Some(node) = &stmt.stmt {
                    match &node.node {
                        Some(pg_query::protobuf::node::Node::SelectStmt(_)) => QueryAstType::Select,
                        Some(pg_query::protobuf::node::Node::InsertStmt(s)) => {
                            if s.returning_list.is_empty() {
                                QueryAstType::DmlWithoutReturning
                            } else {
                                QueryAstType::DmlWithReturning
                            }
                        }
                        Some(pg_query::protobuf::node::Node::UpdateStmt(s)) => {
                            if s.returning_list.is_empty() {
                                QueryAstType::DmlWithoutReturning
                            } else {
                                QueryAstType::DmlWithReturning
                            }
                        }
                        Some(pg_query::protobuf::node::Node::DeleteStmt(s)) => {
                            if s.returning_list.is_empty() {
                                QueryAstType::DmlWithoutReturning
                            } else {
                                QueryAstType::DmlWithReturning
                            }
                        }
                        _ => QueryAstType::Unknown,
                    }
                } else {
                    QueryAstType::Unknown
                }
            } else {
                QueryAstType::Unknown
            }
        }
        Err(_) => QueryAstType::Unknown,
    }
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
        match determine_query_type_from_ast(sql) {
            QueryAstType::Select => self.introspect_select_columns(sql),
            QueryAstType::DmlWithReturning => self.introspect_returning_columns(stmt_name, sql),
            QueryAstType::DmlWithoutReturning => Ok(Vec::new()),
            QueryAstType::Unknown => {
                // Fallback to string-based detection
                log::warn!("Could not parse SQL for query type detection, falling back to string matching");
                let trimmed = sql.trim_start().to_uppercase();
                if trimmed.starts_with("SELECT") || trimmed.starts_with("WITH") {
                    self.introspect_select_columns(sql)
                } else if sql.to_uppercase().contains("RETURNING") {
                    self.introspect_returning_columns(stmt_name, sql)
                } else {
                    Ok(Vec::new())
                }
            }
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

            log::info!("[INTROSPECT] pg_attribute reports - column: {} (OID: {}, nullable: {})", column_name, type_oid, is_nullable);

            columns.push(QueryColumn {
                name: column_name,
                type_oid,
                nullable: is_nullable,
            });
        }

        log::info!("[INTROSPECT] Total columns from pg_attribute: {}", columns.len());

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
        // Parse column names from RETURNING clause
        let column_names = self.parse_returning_column_names(sql)?;
        if column_names.is_empty() {
            return Ok(Vec::new());
        }

        // Extract target table to look up column types from table definition
        let target_table = self.extract_dml_target_table(sql)?;
        let table_rel = self.rel_index.values().find(|rel| rel.id.name() == target_table);

        // Get column types by creating a temporary view that selects the RETURNING columns from the target table
        let result_types = self.get_returning_column_types(&target_table, &column_names)?;

        // Build columns with types and nullability
        let columns: Vec<QueryColumn> = column_names
            .into_iter()
            .zip(result_types.into_iter())
            .map(|(name, type_oid)| {
                let nullable = table_rel
                    .map(|rel| !self.is_column_not_null(rel, &name))
                    .unwrap_or(true);
                QueryColumn { name, type_oid, nullable }
            })
            .collect();

        Ok(columns)
    }

    /// Get column types for RETURNING columns by querying the target table's column definitions
    fn get_returning_column_types(&mut self, table_name: &str, column_names: &[String]) -> Result<Vec<OID>> {
        // Build a SELECT statement that retrieves just those columns from the table
        // This lets PostgreSQL resolve the types for us
        let columns_sql = column_names.join(", ");
        let select_sql = format!("SELECT {} FROM {} WHERE false", columns_sql, table_name);

        // Create a temporary view to introspect the column types
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let view_name = format!("_pgrpc_returning_{}", timestamp);

        self.client.execute("BEGIN", &[])?;

        let create_view_sql = format!("CREATE TEMP VIEW {} AS {}", view_name, select_sql);
        let view_result = self.client.execute(&create_view_sql, &[]);

        if let Err(e) = view_result {
            self.client.execute("ROLLBACK", &[])?;
            return Err(anyhow!("Failed to create temp view for RETURNING introspection: {}", e));
        }

        // Query column types from pg_attribute
        let columns_query = r#"
            SELECT a.atttypid AS type_oid
            FROM pg_class c
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE c.relname = $1
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        "#;

        let rows = self.client.query(columns_query, &[&view_name])?;
        let type_oids: Vec<OID> = rows.iter().map(|row| row.get(0)).collect();

        self.client.execute(&format!("DROP VIEW {}", view_name), &[])?;
        self.client.execute("ROLLBACK", &[])?;

        Ok(type_oids)
    }

    /// Parse RETURNING clause to extract column names
    fn parse_returning_column_names(&self, sql: &str) -> Result<Vec<String>> {
        // Use pg_query to parse the SQL and extract RETURNING column names
        let parse_result = pg_query::parse(sql)
            .with_context(|| format!("Failed to parse SQL for RETURNING clause: {}", sql))?;

        let mut column_names = Vec::new();

        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                let returning_list = match &node.node {
                    Some(pg_query::protobuf::node::Node::InsertStmt(insert)) => {
                        &insert.returning_list
                    }
                    Some(pg_query::protobuf::node::Node::UpdateStmt(update)) => {
                        &update.returning_list
                    }
                    Some(pg_query::protobuf::node::Node::DeleteStmt(delete)) => {
                        &delete.returning_list
                    }
                    _ => return Ok(Vec::new()),
                };

                for target in returning_list {
                    if let Some(pg_query::protobuf::node::Node::ResTarget(res_target)) = &target.node {
                        // If there's an alias, use it
                        if !res_target.name.is_empty() {
                            column_names.push(res_target.name.clone());
                        } else if let Some(val) = &res_target.val {
                            // Extract column name from the expression
                            if let Some(name) = self.extract_column_name_from_node(val) {
                                column_names.push(name);
                            } else {
                                // Fallback: use a placeholder name
                                column_names.push(format!("column_{}", column_names.len() + 1));
                            }
                        }
                    }
                }
            }
        }

        Ok(column_names)
    }

    /// Extract column name from a pg_query Node
    fn extract_column_name_from_node(&self, node: &pg_query::protobuf::Node) -> Option<String> {
        match &node.node {
            Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) => {
                // Get the last field as the column name
                if let Some(last_field) = col_ref.fields.last() {
                    if let Some(pg_query::protobuf::node::Node::String(s)) = &last_field.node {
                        return Some(s.sval.clone());
                    }
                }
                None
            }
            Some(pg_query::protobuf::node::Node::TypeCast(type_cast)) => {
                // For type casts, extract from the argument
                if let Some(arg) = &type_cast.arg {
                    return self.extract_column_name_from_node(arg);
                }
                None
            }
            _ => None,
        }
    }

    /// Extract target table name from INSERT/UPDATE/DELETE statement
    fn extract_dml_target_table(&self, sql: &str) -> Result<String> {
        let parse_result = pg_query::parse(sql)
            .with_context(|| format!("Failed to parse SQL for target table: {}", sql))?;

        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                let relation = match &node.node {
                    Some(pg_query::protobuf::node::Node::InsertStmt(insert)) => {
                        insert.relation.as_ref()
                    }
                    Some(pg_query::protobuf::node::Node::UpdateStmt(update)) => {
                        update.relation.as_ref()
                    }
                    Some(pg_query::protobuf::node::Node::DeleteStmt(delete)) => {
                        delete.relation.as_ref()
                    }
                    _ => None,
                };

                if let Some(range_var) = relation {
                    return Ok(range_var.relname.clone());
                }
            }
        }

        Err(anyhow!("Could not extract target table from SQL: {}", sql))
    }

    /// Check if column has NOT NULL constraint
    fn is_column_not_null(&self, rel: &crate::pg_rel::PgRel, column_name: &str) -> bool {
        use crate::pg_constraint::Constraint;
        rel.constraints.iter().any(|c| {
            matches!(c, Constraint::NotNull(n) if n.column.as_str() == column_name)
        })
    }

    /// Refine column nullability using ViewNullabilityAnalyzer
    fn refine_nullability(&self, sql: &str, columns: &[QueryColumn]) -> Result<Option<Vec<QueryColumn>>> {
        log::info!("[REFINE] Starting nullability refinement for query: {}", sql);
        log::info!("[REFINE] Input columns from pg_attribute:");
        for col in columns {
            log::info!("[REFINE]   {} -> nullable={}", col.name, col.nullable);
        }

        // Try to apply view nullability analysis
        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

        let mut analyzer = ViewNullabilityAnalyzer::new(self.rel_index, self.view_nullability_cache);

        match analyzer.analyze_view(sql, &column_names) {
            Ok(nullability_map) => {
                log::info!("[REFINE] ViewNullabilityAnalyzer results:");
                for (name, is_not_null) in &nullability_map {
                    log::info!("[REFINE]   {} -> is_not_null={} (nullable={})", name, is_not_null, !is_not_null);
                }

                let refined_columns = columns
                    .iter()
                    .map(|col| {
                        let is_not_null = nullability_map.get(&col.name).copied().unwrap_or(false);
                        let new_nullable = !is_not_null;
                        log::info!("[REFINE] Column '{}': pg_attribute says nullable={}, analyzer says is_not_null={}, final nullable={}",
                            col.name, col.nullable, is_not_null, new_nullable);
                        QueryColumn {
                            name: col.name.clone(),
                            type_oid: col.type_oid,
                            nullable: new_nullable,
                        }
                    })
                    .collect();

                log::info!("[REFINE] Refinement succeeded, returning refined columns");
                Ok(Some(refined_columns))
            }
            Err(e) => {
                log::warn!("[REFINE] Could not apply nullability analysis: {}", e);
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
