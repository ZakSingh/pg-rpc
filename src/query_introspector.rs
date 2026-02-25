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
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub struct QueryParam {
    pub name: String,
    /// The type OID to use for the Rust parameter type (may be a domain type).
    pub type_oid: OID,
    /// The type OID that PostgreSQL actually expects at runtime.
    /// This may differ from `type_oid` when PostgreSQL resolves a domain to its base type.
    pub postgres_type_oid: OID,
    pub position: usize,
    pub nullable: bool,
    /// Whether the column has a DEFAULT constraint.
    /// When true and the column is NOT NULL, passing None will emit DEFAULT instead of NULL.
    pub has_default: bool,
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
    pub table_dependencies: BTreeMap<PgId, Vec<Constraint>>,
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
                let (name, nullable, inferred_type, has_default) = param_info
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| (format!("param_{}", i + 1), false, None, false));

                // Prefer inferred type (preserves domain) over PostgreSQL's resolved type
                let type_oid = inferred_type.unwrap_or(postgres_type_oid);

                QueryParam {
                    name,
                    type_oid,
                    postgres_type_oid,
                    position: i + 1,
                    nullable,
                    has_default,
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

    /// Refine column nullability and types using ViewNullabilityAnalyzer or DML table analysis.
    /// Also looks up actual column types from the table schema to preserve domain types.
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

                // Build a map of column name -> (table_name, inferred_type_oid) from the query
                let column_type_map = self.infer_column_types_from_query(sql);

                let refined_columns = columns
                    .iter()
                    .map(|col| {
                        let is_not_null = nullability_map.get(&col.name).copied().unwrap_or(false);
                        let new_nullable = !is_not_null;

                        // Try to get the actual column type from the table schema (preserves domains)
                        let refined_type_oid = column_type_map
                            .get(&col.name)
                            .and_then(|(table, _)| self.rel_index.get_column_type(table, &col.name))
                            .unwrap_or(col.type_oid);

                        log::info!("[REFINE] Column '{}': nullable={}, type_oid={} -> {}",
                            col.name, new_nullable, col.type_oid, refined_type_oid);
                        QueryColumn {
                            name: col.name.clone(),
                            type_oid: refined_type_oid,
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

    /// Infer which table each selected column comes from by parsing the query.
    /// Returns a map of column_name -> (table_name, column_name).
    fn infer_column_types_from_query(&self, sql: &str) -> HashMap<String, (String, String)> {
        let mut result = HashMap::new();

        let parse_result = match pg_query::parse(sql) {
            Ok(r) => r,
            Err(_) => return result,
        };

        // Build alias map first
        let alias_map = self.build_alias_map(sql);

        // Extract the SELECT statement
        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                if let Some(pg_query::protobuf::node::Node::SelectStmt(select)) = &node.node {
                    for target in &select.target_list {
                        if let Some(pg_query::protobuf::node::Node::ResTarget(res_target)) = &target.node {
                            // Get the output column name
                            let output_name = if !res_target.name.is_empty() {
                                res_target.name.clone()
                            } else if let Some(val) = &res_target.val {
                                self.extract_column_name_from_node(val).unwrap_or_default()
                            } else {
                                continue;
                            };

                            // Try to extract table.column from the expression
                            if let Some(val) = &res_target.val {
                                if let Some((table_or_alias, col_name)) = self.extract_table_column_from_node(val) {
                                    // Resolve alias to actual table name
                                    let table_name = alias_map
                                        .get(&table_or_alias)
                                        .cloned()
                                        .unwrap_or(table_or_alias);
                                    result.insert(output_name, (table_name, col_name));
                                }
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Extract the column name from a node (for simple column references)
    fn extract_column_name_from_node(&self, node: &pg_query::protobuf::Node) -> Option<String> {
        if let Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) = &node.node {
            // Get the last field (column name)
            if let Some(last) = col_ref.fields.last() {
                if let Some(pg_query::protobuf::node::Node::String(s)) = &last.node {
                    return Some(s.sval.clone());
                }
            }
        }
        None
    }

    /// Extract (table_or_alias, column_name) from a column reference node
    fn extract_table_column_from_node(&self, node: &pg_query::protobuf::Node) -> Option<(String, String)> {
        if let Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) = &node.node {
            let fields: Vec<_> = col_ref.fields.iter()
                .filter_map(|f| {
                    if let Some(pg_query::protobuf::node::Node::String(s)) = &f.node {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .collect();

            match fields.len() {
                1 => {
                    // Unqualified column - try to find which table it belongs to
                    let col_name = &fields[0];
                    for rel in self.rel_index.values() {
                        let table_name = rel.id.name();
                        if self.rel_index.get_column_type(table_name, col_name).is_some() {
                            return Some((table_name.to_string(), col_name.clone()));
                        }
                    }
                    None
                }
                2 => {
                    // Qualified: table.column or alias.column
                    Some((fields[0].clone(), fields[1].clone()))
                }
                _ => None,
            }
        } else {
            None
        }
    }

    /// Refine nullability and types for INSERT/UPDATE/DELETE RETURNING by looking at the target table
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

        // Refine nullability and type based on table schema
        let refined_columns: Vec<QueryColumn> = columns
            .iter()
            .map(|col| {
                // Check if this column has a NOT NULL constraint
                let is_not_null = table_rel.constraints.iter().any(|c| {
                    matches!(c, Constraint::NotNull(n) if n.column.as_str() == col.name)
                });
                let nullable = !is_not_null;

                // Look up the actual column type from the table (preserves domain types)
                let refined_type_oid = self.rel_index
                    .get_column_type(&target_table, &col.name)
                    .unwrap_or(col.type_oid);

                log::info!(
                    "[REFINE-DML] Column '{}': is_not_null={}, nullable={}, type_oid={} -> {}",
                    col.name,
                    is_not_null,
                    nullable,
                    col.type_oid,
                    refined_type_oid
                );
                QueryColumn {
                    name: col.name.clone(),
                    type_oid: refined_type_oid,
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

    /// Determine parameter names, nullable flags, inferred type OIDs, and has_default from specs, inference, or fallback.
    /// Returns Vec of (name, nullable, Option<inferred_type_oid>, has_default).
    /// The inferred type OID comes from looking up the actual column type in RelIndex,
    /// which preserves domain types that PostgreSQL's type inference resolves to base types.
    fn determine_parameter_info(
        &self,
        param_specs: &[ParameterSpec],
        sql: &str,
        param_count: usize,
    ) -> Result<Vec<(String, bool, Option<OID>, bool)>> {
        // Build alias-to-table map from FROM clause for resolving qualified column references
        let alias_map = self.build_alias_map(sql);

        // Infer nullability from expression context (COALESCE, IS NULL, etc.)
        let context_nullability = self.infer_parameter_nullability_from_context(sql, param_count);

        // Get column mapping for column-based nullability/default inference (INSERT or UPDATE)
        let param_columns = self.extract_param_columns(sql);

        let mut param_info = Vec::new();

        for i in 0..param_count {
            let position = i + 1;

            // Try to infer column info (table_name, column_name) from AST
            let column_info = self.infer_column_info_from_ast(sql, position, &alias_map);

            // Look up the actual column type from RelIndex (preserves domain types)
            let inferred_type = column_info.as_ref().and_then(|(table, column)| {
                self.rel_index.get_column_type(table, column)
            });

            // Get context-inferred nullability for this parameter
            let context_nullable = context_nullability.get(i).copied().unwrap_or(false);

            // Check if this parameter maps to a nullable column (in INSERT or UPDATE)
            let column_nullable = param_columns
                .get(&position)
                .map(|(table, col)| self.is_column_nullable(table, col))
                .unwrap_or(false);

            // Check if this parameter maps to a column with DEFAULT constraint
            let has_default = param_columns
                .get(&position)
                .map(|(table, col)| self.column_has_default(table, col))
                .unwrap_or(false);

            // Priority 1: Check for explicit @name or @name?
            if let Some(param_spec) = param_specs.iter().find(|s| match s {
                ParameterSpec::Named { position: pos, .. } => *pos == position,
                ParameterSpec::Positional { position: pos, .. } => *pos == position,
            }) {
                let (name, explicit_nullable) = match param_spec {
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
                // Combine all nullability sources
                let nullable = explicit_nullable || context_nullable || column_nullable;
                param_info.push((name, nullable, inferred_type, has_default));
                continue;
            }

            // Priority 2: Try AST inference (no spec found)
            if let Some((_, column_name)) = column_info {
                let nullable = context_nullable || column_nullable;
                param_info.push((column_name, nullable, inferred_type, has_default));
                continue;
            }

            // Priority 3: Fallback
            let nullable = context_nullable || column_nullable;
            param_info.push((format!("param_{}", position), nullable, None, false));
        }

        Ok(param_info)
    }

    /// For INSERT/UPDATE statements (including those in CTEs), returns a map of
    /// parameter_position -> (table_name, column_name).
    /// This maps each parameter in VALUES/SET clauses to its target column.
    fn extract_param_columns(&self, sql: &str) -> HashMap<usize, (String, String)> {
        let parse_result = match pg_query::parse(sql) {
            Ok(r) => r,
            Err(_) => return HashMap::new(),
        };

        let mut result = HashMap::new();

        // Process all top-level statements
        for raw_stmt in &parse_result.protobuf.stmts {
            if let Some(stmt) = &raw_stmt.stmt {
                self.extract_param_columns_from_node(stmt, &mut result);
            }
        }

        result
    }

    /// Recursively extract param columns from any node, handling CTEs
    fn extract_param_columns_from_node(
        &self,
        node: &pg_query::protobuf::Node,
        result: &mut HashMap<usize, (String, String)>,
    ) {
        use pg_query::protobuf::node::Node;

        match &node.node {
            Some(Node::InsertStmt(insert)) => {
                // Process the INSERT statement
                let insert_result = self.extract_insert_param_columns_from_stmt(insert);
                result.extend(insert_result);

                // Also check for CTEs within the INSERT
                if let Some(with_clause) = &insert.with_clause {
                    self.extract_param_columns_from_ctes(with_clause, result);
                }
            }
            Some(Node::UpdateStmt(update)) => {
                // Process the UPDATE statement
                let update_result = self.extract_update_param_columns_from_stmt(update);
                result.extend(update_result);

                // Also check for CTEs within the UPDATE
                if let Some(with_clause) = &update.with_clause {
                    self.extract_param_columns_from_ctes(with_clause, result);
                }
            }
            Some(Node::SelectStmt(select)) => {
                // Check for CTEs in the SELECT's WITH clause
                if let Some(with_clause) = &select.with_clause {
                    self.extract_param_columns_from_ctes(with_clause, result);
                }
                // Note: We don't need to recurse into subqueries in FROM/WHERE
                // as those wouldn't affect parameter optionality for INSERT/UPDATE
            }
            Some(Node::DeleteStmt(delete)) => {
                // Check for CTEs in DELETE
                if let Some(with_clause) = &delete.with_clause {
                    self.extract_param_columns_from_ctes(with_clause, result);
                }
            }
            _ => {}
        }
    }

    /// Extract param columns from CTEs in a WITH clause
    fn extract_param_columns_from_ctes(
        &self,
        with_clause: &pg_query::protobuf::WithClause,
        result: &mut HashMap<usize, (String, String)>,
    ) {
        for cte_node in &with_clause.ctes {
            if let Some(pg_query::protobuf::node::Node::CommonTableExpr(cte)) = &cte_node.node {
                if let Some(ctequery) = &cte.ctequery {
                    self.extract_param_columns_from_node(ctequery, result);
                }
            }
        }
    }

    /// Extract param columns from a parsed InsertStmt
    fn extract_insert_param_columns_from_stmt(
        &self,
        insert: &pg_query::protobuf::InsertStmt,
    ) -> HashMap<usize, (String, String)> {
        let table_name = match &insert.relation {
            Some(r) => r.relname.clone(),
            None => return HashMap::new(),
        };

        // Extract column names from INSERT column list (stmt.cols)
        let columns: Vec<String> = insert
            .cols
            .iter()
            .filter_map(|n| match &n.node {
                Some(pg_query::protobuf::node::Node::ResTarget(t)) => Some(t.name.clone()),
                _ => None,
            })
            .collect();

        // If no explicit columns, we can't map parameters to columns
        if columns.is_empty() {
            return HashMap::new();
        }

        let mut result = HashMap::new();

        // Extract ParamRef positions from VALUES clause
        if let Some(select) = &insert.select_stmt {
            if let Some(pg_query::protobuf::node::Node::SelectStmt(s)) = &select.node {
                if let Some(first_values) = s.values_lists.first() {
                    if let Some(pg_query::protobuf::node::Node::List(list)) = &first_values.node {
                        for (i, item) in list.items.iter().enumerate() {
                            if let Some(pos) = self.extract_param_position(item) {
                                if i < columns.len() {
                                    result.insert(pos, (table_name.clone(), columns[i].clone()));
                                }
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Extract param columns from a parsed UpdateStmt
    fn extract_update_param_columns_from_stmt(
        &self,
        update: &pg_query::protobuf::UpdateStmt,
    ) -> HashMap<usize, (String, String)> {
        let table_name = match &update.relation {
            Some(r) => r.relname.clone(),
            None => return HashMap::new(),
        };

        let mut result = HashMap::new();

        // Extract column = $N mappings from SET clause (target_list)
        for target in &update.target_list {
            if let Some(pg_query::protobuf::node::Node::ResTarget(res_target)) = &target.node {
                let column_name = &res_target.name;
                if column_name.is_empty() {
                    continue;
                }

                if let Some(val) = &res_target.val {
                    if let Some(pos) = self.extract_param_position(val) {
                        result.insert(pos, (table_name.clone(), column_name.clone()));
                    }
                }
            }
        }

        result
    }

    /// Extract ParamRef position from a node (handles TypeCast wrapping)
    fn extract_param_position(&self, node: &pg_query::protobuf::Node) -> Option<usize> {
        match &node.node {
            Some(pg_query::protobuf::node::Node::ParamRef(p)) => Some(p.number as usize),
            Some(pg_query::protobuf::node::Node::TypeCast(tc)) => {
                tc.arg.as_ref().and_then(|a| self.extract_param_position(a))
            }
            _ => None,
        }
    }

    /// Check if a column has a DEFAULT constraint.
    fn column_has_default(&self, table: &str, column: &str) -> bool {
        let table_rel = self.rel_index.values().find(|rel| rel.id.name() == table);

        match table_rel {
            Some(rel) => rel.constraints.iter().any(|c| {
                matches!(c, Constraint::Default(d) if d.column.as_str() == column)
            }),
            None => false,
        }
    }

    /// Check if a column allows NULL/omission in INSERT statements.
    /// A parameter is nullable/optional if:
    /// - The column does NOT have a NOT NULL constraint, OR
    /// - The column has a DEFAULT value (can be omitted, letting DB use the default)
    fn is_column_nullable(&self, table: &str, column: &str) -> bool {
        // Find the table in rel_index
        let table_rel = self.rel_index.values().find(|rel| rel.id.name() == table);

        match table_rel {
            Some(rel) => {
                // Check if column has a DEFAULT constraint (can omit parameter)
                let has_default = rel.constraints.iter().any(|c| {
                    matches!(c, Constraint::Default(d) if d.column.as_str() == column)
                });

                // Check if column allows NULL (no NOT NULL constraint)
                let allows_null = !rel.constraints.iter().any(|c| {
                    matches!(c, Constraint::NotNull(n) if n.column.as_str() == column)
                });

                // Parameter is optional if column has default OR allows null
                has_default || allows_null
            }
            None => false, // Conservative: if we can't find the table, assume not nullable
        }
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

    /// Infer parameter nullability from SQL expression context.
    /// Parameters are considered nullable when they appear in contexts that expect NULL values:
    /// - First argument of COALESCE($N, fallback) - COALESCE exists to handle NULL
    /// - Argument of IS NULL / IS NOT NULL tests
    /// - NULLIF($N, value) - first argument may be NULL
    fn infer_parameter_nullability_from_context(&self, sql: &str, param_count: usize) -> Vec<bool> {
        let mut nullable = vec![false; param_count];

        let parse_result = match pg_query::parse(sql) {
            Ok(result) => result,
            Err(_) => return nullable,
        };

        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                self.check_param_nullability_in_node(node, &mut nullable);
            }
        }

        nullable
    }

    /// Recursively check for parameters in nullable-implying expression contexts
    fn check_param_nullability_in_node(
        &self,
        node: &pg_query::protobuf::Node,
        nullable: &mut Vec<bool>,
    ) {
        use pg_query::protobuf::node::Node;

        match &node.node {
            Some(Node::CoalesceExpr(coalesce)) => {
                // All arguments to COALESCE except the last are expected to potentially be NULL
                // (if they couldn't be NULL, there'd be no need for the fallback)
                for arg in coalesce.args.iter().take(coalesce.args.len().saturating_sub(1)) {
                    self.mark_params_nullable_in_expr(arg, nullable);
                }
                // Recurse into all arguments for nested expressions
                for arg in &coalesce.args {
                    self.check_param_nullability_in_node(arg, nullable);
                }
            }
            Some(Node::NullTest(null_test)) => {
                // Parameter in IS NULL / IS NOT NULL should be nullable
                if let Some(arg) = &null_test.arg {
                    self.mark_params_nullable_in_expr(arg, nullable);
                    self.check_param_nullability_in_node(arg, nullable);
                }
            }
            Some(Node::FuncCall(func_call)) => {
                // Check for NULLIF and similar functions
                let func_name = func_call
                    .funcname
                    .iter()
                    .filter_map(|n| {
                        if let Some(Node::String(s)) = &n.node {
                            Some(s.sval.to_lowercase())
                        } else {
                            None
                        }
                    })
                    .last();

                if let Some(name) = func_name {
                    match name.as_str() {
                        "nullif" => {
                            // First argument to NULLIF may be NULL
                            if let Some(first_arg) = func_call.args.first() {
                                self.mark_params_nullable_in_expr(first_arg, nullable);
                            }
                        }
                        "coalesce" => {
                            // COALESCE can also appear as a FuncCall
                            for arg in func_call.args.iter().take(func_call.args.len().saturating_sub(1)) {
                                self.mark_params_nullable_in_expr(arg, nullable);
                            }
                        }
                        _ => {}
                    }
                }
                // Recurse into arguments
                for arg in &func_call.args {
                    self.check_param_nullability_in_node(arg, nullable);
                }
            }
            // Recurse into various statement and expression types
            Some(Node::SelectStmt(select)) => {
                for target in &select.target_list {
                    self.check_param_nullability_in_node(target, nullable);
                }
                if let Some(where_clause) = &select.where_clause {
                    self.check_param_nullability_in_node(where_clause, nullable);
                }
                for from_item in &select.from_clause {
                    self.check_param_nullability_in_node(from_item, nullable);
                }
                for group_item in &select.group_clause {
                    self.check_param_nullability_in_node(group_item, nullable);
                }
                if let Some(having) = &select.having_clause {
                    self.check_param_nullability_in_node(having, nullable);
                }
                // Handle VALUES clause (used in INSERT ... VALUES (...))
                for values_list in &select.values_lists {
                    self.check_param_nullability_in_node(values_list, nullable);
                }
            }
            Some(Node::InsertStmt(insert)) => {
                if let Some(select) = &insert.select_stmt {
                    self.check_param_nullability_in_node(select, nullable);
                }
                for col in &insert.returning_list {
                    self.check_param_nullability_in_node(col, nullable);
                }
            }
            Some(Node::UpdateStmt(update)) => {
                for target in &update.target_list {
                    self.check_param_nullability_in_node(target, nullable);
                }
                if let Some(where_clause) = &update.where_clause {
                    self.check_param_nullability_in_node(where_clause, nullable);
                }
                for col in &update.returning_list {
                    self.check_param_nullability_in_node(col, nullable);
                }
            }
            Some(Node::DeleteStmt(delete)) => {
                if let Some(where_clause) = &delete.where_clause {
                    self.check_param_nullability_in_node(where_clause, nullable);
                }
                for col in &delete.returning_list {
                    self.check_param_nullability_in_node(col, nullable);
                }
            }
            Some(Node::ResTarget(res_target)) => {
                if let Some(val) = &res_target.val {
                    self.check_param_nullability_in_node(val, nullable);
                }
            }
            Some(Node::AExpr(aexpr)) => {
                if let Some(lexpr) = &aexpr.lexpr {
                    self.check_param_nullability_in_node(lexpr, nullable);
                }
                if let Some(rexpr) = &aexpr.rexpr {
                    self.check_param_nullability_in_node(rexpr, nullable);
                }
            }
            Some(Node::BoolExpr(bool_expr)) => {
                for arg in &bool_expr.args {
                    self.check_param_nullability_in_node(arg, nullable);
                }
            }
            Some(Node::SubLink(sublink)) => {
                if let Some(subselect) = &sublink.subselect {
                    self.check_param_nullability_in_node(subselect, nullable);
                }
                if let Some(testexpr) = &sublink.testexpr {
                    self.check_param_nullability_in_node(testexpr, nullable);
                }
            }
            Some(Node::CaseExpr(case_expr)) => {
                if let Some(arg) = &case_expr.arg {
                    self.check_param_nullability_in_node(arg, nullable);
                }
                for when in &case_expr.args {
                    self.check_param_nullability_in_node(when, nullable);
                }
                if let Some(defresult) = &case_expr.defresult {
                    self.check_param_nullability_in_node(defresult, nullable);
                }
            }
            Some(Node::CaseWhen(case_when)) => {
                if let Some(expr) = &case_when.expr {
                    self.check_param_nullability_in_node(expr, nullable);
                }
                if let Some(result) = &case_when.result {
                    self.check_param_nullability_in_node(result, nullable);
                }
            }
            Some(Node::TypeCast(type_cast)) => {
                if let Some(arg) = &type_cast.arg {
                    self.check_param_nullability_in_node(arg, nullable);
                }
            }
            Some(Node::JoinExpr(join)) => {
                if let Some(larg) = &join.larg {
                    self.check_param_nullability_in_node(larg, nullable);
                }
                if let Some(rarg) = &join.rarg {
                    self.check_param_nullability_in_node(rarg, nullable);
                }
                if let Some(quals) = &join.quals {
                    self.check_param_nullability_in_node(quals, nullable);
                }
            }
            Some(Node::RangeSubselect(range_subselect)) => {
                if let Some(subquery) = &range_subselect.subquery {
                    self.check_param_nullability_in_node(subquery, nullable);
                }
            }
            Some(Node::List(list)) => {
                for item in &list.items {
                    self.check_param_nullability_in_node(item, nullable);
                }
            }
            _ => {}
        }
    }

    /// Mark any ParamRef nodes in this expression as nullable
    fn mark_params_nullable_in_expr(
        &self,
        node: &pg_query::protobuf::Node,
        nullable: &mut Vec<bool>,
    ) {
        use pg_query::protobuf::node::Node;

        match &node.node {
            Some(Node::ParamRef(param)) => {
                let idx = param.number as usize;
                if idx > 0 && idx <= nullable.len() {
                    log::debug!(
                        "[PARAM-NULLABILITY] Marking parameter ${} as nullable from expression context",
                        idx
                    );
                    nullable[idx - 1] = true;
                }
            }
            Some(Node::TypeCast(type_cast)) => {
                // Handle casted parameters like $1::text
                if let Some(arg) = &type_cast.arg {
                    self.mark_params_nullable_in_expr(arg, nullable);
                }
            }
            Some(Node::AExpr(aexpr)) => {
                // Handle expressions containing parameters
                if let Some(lexpr) = &aexpr.lexpr {
                    self.mark_params_nullable_in_expr(lexpr, nullable);
                }
                if let Some(rexpr) = &aexpr.rexpr {
                    self.mark_params_nullable_in_expr(rexpr, nullable);
                }
            }
            _ => {}
        }
    }
}
