use crate::cardinality_inference::CardinalityAnalyzer;
use crate::codegen::OID;
use crate::constraint_analysis::analyze_sql_for_constraints;
use crate::exceptions::PgException;
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::rel_index::RelIndex;
use crate::sql_parser::{ParameterSpec, ParsedQuery, QueryType};
use crate::trigger_index::TriggerIndex;
use crate::ty_index::TypeIndex;
use crate::view_nullability::ViewNullabilityAnalyzer;
use anyhow::{Context, Result};
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
    /// Exceptions that could be raised by this query
    pub exceptions: Vec<PgException>,
    /// Map of table ID to constraints that apply to this query
    pub table_dependencies: HashMap<PgId, Vec<Constraint>>,
}

pub struct QueryIntrospector<'a> {
    client: &'a mut Client,
    rel_index: &'a RelIndex,
    #[allow(dead_code)]
    type_index: &'a TypeIndex,
    view_nullability_cache: &'a crate::view_nullability::ViewNullabilityCache,
    trigger_index: Option<&'a TriggerIndex>,
}

impl<'a> QueryIntrospector<'a> {
    pub fn new(
        client: &'a mut Client,
        rel_index: &'a RelIndex,
        type_index: &'a TypeIndex,
        view_nullability_cache: &'a crate::view_nullability::ViewNullabilityCache,
        trigger_index: Option<&'a TriggerIndex>,
    ) -> Self {
        Self {
            client,
            rel_index,
            type_index,
            view_nullability_cache,
            trigger_index,
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

        // Determine the final query type: use explicit if specified, otherwise infer
        let final_query_type = match &parsed.explicit_query_type {
            Some(explicit) => {
                log::info!(
                    "Query '{}' has explicit type: {:?}",
                    parsed.name,
                    explicit
                );
                explicit.clone()
            }
            None => {
                // Infer cardinality from SQL structure and database constraints
                let analyzer = CardinalityAnalyzer::new(self.rel_index);
                match analyzer.infer(&parsed.postgres_sql) {
                    Ok(inference) => {
                        log::info!(
                            "Inferred {:?} for '{}': {:?} (confidence: {:?})",
                            inference.cardinality,
                            parsed.name,
                            inference.reason,
                            inference.confidence
                        );
                        inference.cardinality.into()
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to infer cardinality for '{}': {}. Defaulting to :many",
                            parsed.name,
                            e
                        );
                        QueryType::Many
                    }
                }
            }
        };

        // Use rust-postgres prepare() to get statement metadata
        // This works for ALL query types including CTEs with data-modifying statements
        let stmt = self
            .client
            .prepare(&parsed.postgres_sql)
            .with_context(|| {
                format!(
                    "Failed to prepare query '{}' from {}:{}\nSQL: {}",
                    parsed.name,
                    parsed.file_path.display(),
                    parsed.line_number,
                    parsed.postgres_sql
                )
            })?;

        // Get parameter types directly from the statement
        // Note: PostgreSQL resolves domain types to their base types here
        let postgres_param_types: Vec<OID> = stmt.params().iter().map(|t| t.oid()).collect();

        // Determine parameter names, nullable flags, and inferred column types
        // The inferred types preserve domain types by looking up the actual column type
        let param_info = self.determine_parameter_info(
            &parsed.parameters,
            &parsed.postgres_sql,
            postgres_param_types.len(),
        )?;

        // Build parameter list, preferring inferred column types (preserves domains)
        // over PostgreSQL's resolved types
        let params: Vec<QueryParam> = postgres_param_types
            .into_iter()
            .enumerate()
            .map(|(i, postgres_type_oid)| {
                let (name, nullable, inferred_type) = param_info
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| (format!("param_{}", i + 1), false, None));

                // Prefer inferred type (preserves domain) over PostgreSQL's resolved type
                let type_oid = inferred_type.unwrap_or(postgres_type_oid);

                QueryParam {
                    name,
                    type_oid,
                    position: i + 1,
                    nullable,
                }
            })
            .collect();

        // Get return columns using statement metadata
        let return_columns = match final_query_type {
            QueryType::One | QueryType::Opt | QueryType::Many => {
                Some(self.introspect_return_columns_from_stmt(&stmt, &parsed.postgres_sql)?)
            }
            QueryType::Exec | QueryType::ExecRows => None,
        };

        // Analyze exceptions and table dependencies
        let (exceptions, table_dependencies) = analyze_sql_for_constraints(
            &parsed.postgres_sql,
            self.rel_index,
            self.trigger_index,
        );

        // No need to DEALLOCATE - Statement is dropped automatically

        Ok(IntrospectedQuery {
            name: parsed.name.clone(),
            query_type: final_query_type,
            sql: parsed.postgres_sql.clone(),
            params,
            return_columns,
            file_path: parsed.file_path.display().to_string(),
            line_number: parsed.line_number,
            exceptions,
            table_dependencies,
        })
    }

    /// Introspect return columns using the prepared statement metadata
    fn introspect_return_columns_from_stmt(
        &mut self,
        stmt: &postgres::Statement,
        sql: &str,
    ) -> Result<Vec<QueryColumn>> {
        // Get column info directly from the prepared statement
        let columns: Vec<QueryColumn> = stmt
            .columns()
            .iter()
            .map(|col| QueryColumn {
                name: col.name().to_string(),
                type_oid: col.type_().oid(),
                nullable: true, // Default to nullable, will be refined by analysis
            })
            .collect();

        if columns.is_empty() {
            return Ok(Vec::new());
        }

        log::debug!(
            "[INTROSPECT] Statement returned {} columns",
            columns.len()
        );
        for col in &columns {
            log::debug!(
                "[INTROSPECT] Column from statement: {} (OID: {})",
                col.name,
                col.type_oid
            );
        }

        // Apply nullability analysis
        if let Some(refined_columns) = self.refine_nullability(sql, &columns)? {
            log::debug!(
                "[INTROSPECT] After refine_nullability: {} columns",
                refined_columns.len()
            );
            Ok(refined_columns)
        } else {
            log::debug!(
                "[INTROSPECT] No refinement applied, returning {} columns",
                columns.len()
            );
            Ok(columns)
        }
    }

    /// Refine column nullability using ViewNullabilityAnalyzer or DML table analysis
    fn refine_nullability(&self, sql: &str, columns: &[QueryColumn]) -> Result<Option<Vec<QueryColumn>>> {
        log::info!("[REFINE] Starting nullability refinement for query: {}", sql);
        log::info!("[REFINE] Input columns:");
        for col in columns {
            log::info!("[REFINE]   {} -> nullable={}", col.name, col.nullable);
        }

        // First, try to refine using DML target table (for INSERT/UPDATE/DELETE with RETURNING)
        if let Some(refined) = self.refine_nullability_from_dml_target(sql, columns)? {
            log::info!("[REFINE] DML target analysis succeeded");
            return Ok(Some(refined));
        }

        // Fall back to view nullability analysis (for SELECT queries)
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
                        log::info!("[REFINE] Column '{}': says nullable={}, analyzer says is_not_null={}, final nullable={}",
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

    /// Refine nullability for INSERT/UPDATE/DELETE RETURNING by looking at the target table
    fn refine_nullability_from_dml_target(
        &self,
        sql: &str,
        columns: &[QueryColumn],
    ) -> Result<Option<Vec<QueryColumn>>> {
        use crate::pg_constraint::Constraint;

        // Try to extract the target table from a DML statement
        let target_table = match self.extract_dml_target_table(sql) {
            Some(table) => table,
            None => return Ok(None), // Not a DML statement
        };

        log::info!("[REFINE-DML] Found DML target table: {}", target_table);

        // Find the table in rel_index
        let table_rel = self.rel_index.values().find(|rel| rel.id.name() == target_table);

        let table_rel = match table_rel {
            Some(rel) => rel,
            None => {
                log::warn!("[REFINE-DML] Could not find table '{}' in rel_index", target_table);
                return Ok(None);
            }
        };

        // Refine nullability based on table constraints
        let refined_columns: Vec<QueryColumn> = columns
            .iter()
            .map(|col| {
                // Check if this column has a NOT NULL constraint
                let is_not_null = table_rel.constraints.iter().any(|c| {
                    matches!(c, Constraint::NotNull(n) if n.column.as_str() == col.name)
                });
                let nullable = !is_not_null;
                log::info!(
                    "[REFINE-DML] Column '{}': is_not_null={}, nullable={}",
                    col.name,
                    is_not_null,
                    nullable
                );
                QueryColumn {
                    name: col.name.clone(),
                    type_oid: col.type_oid,
                    nullable,
                }
            })
            .collect();

        Ok(Some(refined_columns))
    }

    /// Extract target table name from INSERT/UPDATE/DELETE statement
    fn extract_dml_target_table(&self, sql: &str) -> Option<String> {
        let parse_result = pg_query::parse(sql).ok()?;

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
                    return Some(range_var.relname.clone());
                }
            }
        }

        None
    }

    /// Determine parameter names, nullable flags, and inferred type OIDs from specs, inference, or fallback.
    /// Returns Vec of (name, nullable, Option<inferred_type_oid>).
    /// The inferred type OID comes from looking up the actual column type in RelIndex,
    /// which preserves domain types that PostgreSQL's type inference resolves to base types.
    fn determine_parameter_info(
        &self,
        param_specs: &[ParameterSpec],
        sql: &str,
        param_count: usize,
    ) -> Result<Vec<(String, bool, Option<OID>)>> {
        // Build alias-to-table map from FROM clause for resolving qualified column references
        let alias_map = self.build_alias_map(sql);

        let mut param_info = Vec::new();

        for i in 0..param_count {
            let position = i + 1;

            // Try to infer column info (table_name, column_name) from AST
            let column_info = self.infer_column_info_from_ast(sql, position, &alias_map);

            // Look up the actual column type from RelIndex (preserves domain types)
            let inferred_type = column_info.as_ref().and_then(|(table, column)| {
                self.rel_index.get_column_type(table, column)
            });

            // Priority 1: Check for explicit @name or @name?
            if let Some(param_spec) = param_specs.iter().find(|s| match s {
                ParameterSpec::Named { position: pos, .. } => *pos == position,
                ParameterSpec::Positional { position: pos, .. } => *pos == position,
            }) {
                let (name, nullable) = match param_spec {
                    ParameterSpec::Named { name, nullable, .. } => (name.clone(), *nullable),
                    ParameterSpec::Positional { nullable, .. } => {
                        // For positional, try to infer name from column info, otherwise use fallback
                        let inferred_name = column_info
                            .as_ref()
                            .map(|(_, col)| col.clone())
                            .unwrap_or_else(|| format!("param_{}", position));
                        (inferred_name, *nullable)
                    }
                };
                param_info.push((name, nullable, inferred_type));
                continue;
            }

            // Priority 2: Try AST inference (no spec found)
            if let Some((_, column_name)) = column_info {
                param_info.push((column_name, false, inferred_type));
                continue;
            }

            // Priority 3: Fallback
            param_info.push((format!("param_{}", position), false, None));
        }

        Ok(param_info)
    }

    /// Build a map of table aliases to actual table names from the FROM clause.
    /// This helps resolve qualified column references like `u.id` to `users.id`.
    fn build_alias_map(&self, sql: &str) -> HashMap<String, String> {
        let mut alias_map = HashMap::new();

        let parse_result = match pg_query::parse(sql) {
            Ok(result) => result,
            Err(_) => return alias_map,
        };

        // Extract table references from the parsed query
        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                self.extract_table_aliases_from_node(node, &mut alias_map);
            }
        }

        alias_map
    }

    /// Recursively extract table aliases from a query node
    fn extract_table_aliases_from_node(
        &self,
        node: &pg_query::protobuf::Node,
        alias_map: &mut HashMap<String, String>,
    ) {
        use pg_query::protobuf::node::Node;

        match &node.node {
            Some(Node::SelectStmt(select)) => {
                // Process FROM clause
                for from_item in &select.from_clause {
                    self.extract_table_aliases_from_node(from_item, alias_map);
                }
            }
            Some(Node::RangeVar(range_var)) => {
                let table_name = &range_var.relname;
                // If there's an alias, map alias -> table
                // If no alias, map table -> table (for unqualified references)
                if let Some(alias) = &range_var.alias {
                    alias_map.insert(alias.aliasname.clone(), table_name.clone());
                }
                // Always map the table name to itself for direct references
                alias_map.insert(table_name.clone(), table_name.clone());
            }
            Some(Node::JoinExpr(join)) => {
                if let Some(larg) = &join.larg {
                    self.extract_table_aliases_from_node(larg, alias_map);
                }
                if let Some(rarg) = &join.rarg {
                    self.extract_table_aliases_from_node(rarg, alias_map);
                }
            }
            Some(Node::UpdateStmt(update)) => {
                if let Some(relation) = &update.relation {
                    let table_name = &relation.relname;
                    if let Some(alias) = &relation.alias {
                        alias_map.insert(alias.aliasname.clone(), table_name.clone());
                    }
                    alias_map.insert(table_name.clone(), table_name.clone());
                }
                for from_item in &update.from_clause {
                    self.extract_table_aliases_from_node(from_item, alias_map);
                }
            }
            Some(Node::DeleteStmt(delete)) => {
                if let Some(relation) = &delete.relation {
                    let table_name = &relation.relname;
                    if let Some(alias) = &relation.alias {
                        alias_map.insert(alias.aliasname.clone(), table_name.clone());
                    }
                    alias_map.insert(table_name.clone(), table_name.clone());
                }
            }
            Some(Node::InsertStmt(insert)) => {
                if let Some(relation) = &insert.relation {
                    let table_name = &relation.relname;
                    alias_map.insert(table_name.clone(), table_name.clone());
                }
            }
            _ => {}
        }
    }

    /// Infer column info (table_name, column_name) from SQL AST by finding column = $N patterns.
    /// Returns the actual table name (resolved from alias if needed) and column name.
    fn infer_column_info_from_ast(
        &self,
        sql: &str,
        position: usize,
        alias_map: &HashMap<String, String>,
    ) -> Option<(String, String)> {
        let param_marker = format!("${}", position);

        // Find the parameter in the SQL text
        let param_pos = sql.find(&param_marker)?;
        let before = &sql[..param_pos];

        // Try to extract qualified column reference: "table.column = $N" or "alias.column = $N"
        if let Some(captures) = regex::Regex::new(r"(\w+)\.(\w+)\s*[=<>!]+\s*$")
            .ok()?
            .captures(before)
        {
            let table_or_alias = captures.get(1)?.as_str();
            let column_name = captures.get(2)?.as_str();

            // Resolve alias to actual table name
            let table_name = alias_map
                .get(table_or_alias)
                .cloned()
                .unwrap_or_else(|| table_or_alias.to_string());

            return Some((table_name, column_name.to_string()));
        }

        // Try to extract unqualified column reference: "column = $N"
        if let Some(captures) = regex::Regex::new(r"(\w+)\s*[=<>!]+\s*$")
            .ok()?
            .captures(before)
        {
            let column_name = captures.get(1)?.as_str();

            // Avoid SQL keywords
            if Self::is_sql_keyword(column_name) {
                return None;
            }

            // For unqualified columns, try to find which table this column belongs to
            // by checking all tables in the alias map
            for table_name in alias_map.values() {
                if self.rel_index.get_column_type(table_name, column_name).is_some() {
                    return Some((table_name.clone(), column_name.to_string()));
                }
            }

            // If we can't find the table, return just the column name with empty table
            // The type lookup will fail but we still have the column name for the parameter
            return Some((String::new(), column_name.to_string()));
        }

        None
    }

    /// Check if a string is a SQL keyword (simple heuristic)
    fn is_sql_keyword(s: &str) -> bool {
        matches!(
            s.to_uppercase().as_str(),
            "SELECT" | "FROM" | "WHERE" | "AND" | "OR" | "IN" | "NOT" | "NULL" | "IS" | "AS"
        )
    }
}
