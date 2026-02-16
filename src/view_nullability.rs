use crate::codegen::OID;
use crate::pg_constraint::Constraint;
use crate::pg_rel::PgRel;
use crate::rel_index::RelIndex;
use indexmap::IndexMap;
use pg_query::protobuf;
use std::collections::{HashMap, HashSet};

/// Cache for storing view nullability analysis results
/// Key: (schema, view_name), Value: map of column_name -> is_not_null (order-preserving)
pub type ViewNullabilityCache = HashMap<(Option<String>, String), IndexMap<String, bool>>;

/// Analyzes view definitions to infer column nullability
pub struct ViewNullabilityAnalyzer<'a> {
    /// Reference to the relation index for base table information
    rel_index: &'a RelIndex,
    /// Map from table alias to actual table info during analysis
    alias_map: HashMap<String, TableInfo>,
    /// Track which columns are nullable in the final result
    nullable_columns: HashSet<String>,
    /// Cache of previously analyzed view nullability
    view_nullability_cache: &'a ViewNullabilityCache,
}

#[derive(Debug, Clone)]
enum TableInfo {
    /// Base table with OID and nullability constraints
    Table {
        schema: Option<String>,
        name: String,
        oid: OID,
        /// True if this table reference can produce NULL values due to outer joins
        is_nullable: bool,
    },
    /// View with cached nullability information
    View {
        schema: Option<String>,
        name: String,
        /// Column name -> is_not_null mapping (order-preserving)
        nullability: IndexMap<String, bool>,
        /// True if this view reference can produce NULL values due to outer joins
        is_nullable: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl<'a> ViewNullabilityAnalyzer<'a> {
    pub fn new(rel_index: &'a RelIndex, view_nullability_cache: &'a ViewNullabilityCache) -> Self {
        Self {
            rel_index,
            alias_map: HashMap::new(),
            nullable_columns: HashSet::new(),
            view_nullability_cache,
        }
    }

    /// Extract potential view dependencies from a view definition
    /// Returns a set of (schema, view_name) tuples that might be views this view depends on
    /// Note: This returns all non-base-table references, which may include views
    pub fn extract_view_dependencies(
        &self,
        view_definition: &str,
    ) -> anyhow::Result<HashSet<(Option<String>, String)>> {
        let mut dependencies = HashSet::new();

        // Parse the view definition
        let parse_result = pg_query::parse(view_definition)?;

        // Find the main SELECT statement
        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                if let Some(protobuf::node::Node::SelectStmt(select)) = &node.node {
                    // Analyze FROM clause
                    for from_item in &select.from_clause {
                        self.extract_dependencies_from_node(from_item, &mut dependencies)?;
                    }
                }
            }
        }

        Ok(dependencies)
    }

    fn extract_dependencies_from_node(
        &self,
        node: &protobuf::Node,
        dependencies: &mut HashSet<(Option<String>, String)>,
    ) -> anyhow::Result<()> {
        if let Some(node) = &node.node {
            match node {
                protobuf::node::Node::RangeVar(range_var) => {
                    let table_name = range_var.relname.clone();
                    let schema = if range_var.schemaname.is_empty() {
                        None
                    } else {
                        Some(range_var.schemaname.clone())
                    };

                    // If this is not a base table, it might be a view
                    if self.find_relation(&schema, &table_name).is_none() {
                        log::debug!(
                            "Found potential view dependency: schema={:?}, table={}",
                            schema,
                            table_name
                        );
                        dependencies.insert((schema, table_name));
                    } else {
                        log::debug!(
                            "Found base table: schema={:?}, table={}",
                            schema,
                            table_name
                        );
                    }
                }
                protobuf::node::Node::JoinExpr(join) => {
                    // Recursively check join arguments
                    if let Some(larg) = &join.larg {
                        self.extract_dependencies_from_node(larg, dependencies)?;
                    }
                    if let Some(rarg) = &join.rarg {
                        self.extract_dependencies_from_node(rarg, dependencies)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Analyze a view definition and return a map of column names to their nullability
    /// The map value is true if the column is NOT NULL, false if nullable
    /// Returns an IndexMap to preserve column order
    pub fn analyze_view(
        &mut self,
        view_definition: &str,
        view_columns: &[String],
    ) -> anyhow::Result<IndexMap<String, bool>> {
        log::info!("Analyzing view with {} columns", view_columns.len());
        log::info!("View definition: {}", view_definition);

        // Parse the view definition
        let parse_result = pg_query::parse(view_definition)?;

        // Clear previous state
        self.alias_map.clear();
        self.nullable_columns.clear();

        // Track column results
        let mut column_nullability = Vec::new();

        // Find the main SELECT statement
        if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                if let Some(protobuf::node::Node::SelectStmt(select)) = &node.node {
                    // First analyze FROM clause
                    for from_item in &select.from_clause {
                        self.analyze_from_item(from_item)?;
                    }

                    // Then analyze each target column, expanding star expressions
                    for target in &select.target_list {
                        if let Some(protobuf::node::Node::ResTarget(res_target)) = &target.node {
                            if let Some(val) = &res_target.val {
                                // Check if this is a star expression
                                if self.is_star_expression(val) {
                                    // Expand star expression into individual columns
                                    let expanded = self.expand_star_expression(val)?;
                                    for (_col_name, is_not_null) in expanded {
                                        column_nullability.push(is_not_null);
                                    }
                                } else {
                                    // Regular column expression
                                    let is_nullable = self.is_expression_nullable(val)?;
                                    column_nullability.push(!is_nullable); // true = NOT NULL
                                }
                            }
                        }
                    }
                }
            }
        }

        // Map view columns to their nullability
        // Use IndexMap to preserve column order
        let mut result = IndexMap::new();
        for (i, column_name) in view_columns.iter().enumerate() {
            if let Some(&not_null) = column_nullability.get(i) {
                result.insert(column_name.clone(), not_null);
            } else {
                // Default to nullable if we couldn't analyze
                result.insert(column_name.clone(), false);
            }
        }

        Ok(result)
    }

    fn analyze_from_item(&mut self, from: &protobuf::Node) -> anyhow::Result<()> {
        if let Some(node) = &from.node {
            match node {
                protobuf::node::Node::RangeVar(range_var) => {
                    // Simple table reference
                    let table_name = range_var.relname.clone();
                    let schema = if range_var.schemaname.is_empty() {
                        None
                    } else {
                        Some(range_var.schemaname.clone())
                    };
                    let alias = range_var
                        .alias
                        .as_ref()
                        .map(|a| a.aliasname.clone())
                        .unwrap_or_else(|| table_name.clone());

                    // First try to find it as a base table in rel_index
                    if let Some(rel) = self.find_relation(&schema, &table_name) {
                        self.alias_map.insert(
                            alias,
                            TableInfo::Table {
                                schema,
                                name: table_name,
                                oid: rel.oid,
                                is_nullable: false, // Will be updated by join analysis
                            },
                        );
                    } else {
                        // Not a base table, check if it's a view in the cache
                        let cache_key = (schema.clone(), table_name.clone());
                        if let Some(view_nullability) = self.view_nullability_cache.get(&cache_key)
                        {
                            self.alias_map.insert(
                                alias,
                                TableInfo::View {
                                    schema,
                                    name: table_name,
                                    nullability: view_nullability.clone(),
                                    is_nullable: false, // Will be updated by join analysis
                                },
                            );
                        }
                        // If not found in either, it's ignored (conservative approach)
                    }
                }
                protobuf::node::Node::JoinExpr(join) => {
                    // Handle joins
                    if let Some(larg) = &join.larg {
                        self.analyze_from_item(larg)?;
                    }
                    if let Some(rarg) = &join.rarg {
                        self.analyze_from_item(rarg)?;
                    }

                    // Determine join type and mark appropriate columns as nullable
                    let join_type = self.get_join_type(join);
                    self.apply_join_nullability(join, join_type)?;
                }
                _ => {
                    // Handle other from item types as needed
                }
            }
        }
        Ok(())
    }

    fn is_expression_nullable(&self, expr: &protobuf::Node) -> anyhow::Result<bool> {
        if let Some(node) = &expr.node {
            log::trace!(
                "Analyzing expression type: {:?}",
                std::mem::discriminant(node)
            );
            match node {
                protobuf::node::Node::ColumnRef(col_ref) => {
                    // Check if this column reference is nullable
                    self.is_column_ref_nullable(col_ref)
                }
                protobuf::node::Node::AConst(_const_val) => {
                    // Check if this is a NULL constant
                    // For now, assume all constants are non-null
                    // TODO: Detect NULL constants properly
                    Ok(false)
                }
                protobuf::node::Node::FuncCall(func) => {
                    // Analyze function calls
                    log::trace!("Found FuncCall node");
                    self.is_function_nullable(func)
                }
                protobuf::node::Node::AExpr(aexpr) => {
                    // Analyze arithmetic/logical expressions
                    self.is_aexpr_nullable(aexpr)
                }
                protobuf::node::Node::CaseExpr(case_expr) => {
                    // CASE expression analysis
                    self.is_case_expr_nullable(case_expr)
                }
                protobuf::node::Node::CoalesceExpr(coalesce) => {
                    // COALESCE is non-null if at least one argument is guaranteed non-null
                    self.is_coalesce_nullable(&coalesce.args)
                }
                protobuf::node::Node::TypeCast(type_cast) => {
                    // Type casts preserve nullability of the argument
                    if let Some(arg) = &type_cast.arg {
                        self.is_expression_nullable(arg)
                    } else {
                        Ok(true)
                    }
                }
                protobuf::node::Node::SubLink(sub_link) => {
                    // SubLink represents a subquery
                    self.is_sublink_nullable(sub_link)
                }
                protobuf::node::Node::AArrayExpr(_) => {
                    // Array constructor is never null (empty array is not null)
                    Ok(false)
                }
                protobuf::node::Node::RowExpr(_) => {
                    // ROW constructor creates a non-null composite value
                    Ok(false)
                }
                protobuf::node::Node::FieldSelect(field_select) => {
                    // Field selection from composite type
                    self.is_field_select_nullable(field_select)
                }
                protobuf::node::Node::Aggref(agg) => {
                    // Aggregate function
                    log::debug!("Found Aggref with fnoid: {}", agg.aggfnoid);
                    // For now, assume all aggregate functions like array_agg never return NULL
                    Ok(false)
                }
                protobuf::node::Node::AIndirection(indirection) => {
                    // AIndirection is used for field selection like (t.item_subtotal).amount
                    self.is_aindirection_nullable(indirection)
                }
                protobuf::node::Node::NullTest(_null_test) => {
                    // IS NULL / IS NOT NULL always return boolean (never NULL)
                    log::debug!("Found NullTest expression");
                    Ok(false)
                }
                _ => {
                    // Default to nullable for unknown expressions
                    log::debug!(
                        "Unknown expression type, defaulting to nullable: {:?}",
                        std::mem::discriminant(node)
                    );
                    log::debug!("Unknown node type: {:?}", node);
                    Ok(true)
                }
            }
        } else {
            Ok(true)
        }
    }

    fn extract_column_name(&self, node: &protobuf::Node) -> Option<String> {
        match &node.node {
            Some(protobuf::node::Node::ColumnRef(col_ref)) => {
                // Extract the last field as the column name
                if let Some(last_field) = col_ref.fields.last() {
                    if let Some(protobuf::node::Node::String(s)) = &last_field.node {
                        return Some(s.sval.clone());
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Check if an expression is a star expression (SELECT * or SELECT table.*)
    fn is_star_expression(&self, expr: &protobuf::Node) -> bool {
        if let Some(protobuf::node::Node::ColumnRef(col_ref)) = &expr.node {
            // Check if the last field is an AStar node
            if let Some(last_field) = col_ref.fields.last() {
                if let Some(protobuf::node::Node::AStar(_)) = &last_field.node {
                    return true;
                }
            }
        }
        false
    }

    /// Expand a star expression into individual columns with their nullability
    fn expand_star_expression(&self, expr: &protobuf::Node) -> anyhow::Result<Vec<(String, bool)>> {
        let mut result = Vec::new();

        if let Some(protobuf::node::Node::ColumnRef(col_ref)) = &expr.node {
            match col_ref.fields.len() {
                1 => {
                    // SELECT * - expand all tables
                    log::info!("[STAR_EXPAND] Expanding SELECT * for {} tables", self.alias_map.len());
                    for (alias, table_info) in &self.alias_map {
                        log::info!("[STAR_EXPAND] Processing table alias '{}'", alias);
                        let columns = self.get_table_columns(table_info)?;
                        for (col_name, is_not_null) in columns {
                            log::info!("[STAR_EXPAND]   Adding column '{}.{}' with is_not_null={}", alias, col_name, is_not_null);
                            result.push((format!("{}.{}", alias, col_name), is_not_null));
                        }
                    }
                }
                2 => {
                    // SELECT table.* - expand specific table
                    if let Some(protobuf::node::Node::String(table_ref)) = &col_ref.fields[0].node {
                        if let Some(table_info) = self.alias_map.get(&table_ref.sval) {
                            log::info!("[STAR_EXPAND] Expanding SELECT {}.* ", table_ref.sval);
                            let columns = self.get_table_columns(table_info)?;
                            for (col_name, is_not_null) in columns {
                                log::info!("[STAR_EXPAND]   Adding column '{}' with is_not_null={}", col_name, is_not_null);
                                result.push((col_name, is_not_null));
                            }
                        } else {
                            return Err(anyhow::anyhow!(
                                "Unknown table reference: {}",
                                table_ref.sval
                            ));
                        }
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!("Unexpected star expression format"));
                }
            }
        }

        log::info!("[STAR_EXPAND] Expansion complete, {} columns", result.len());
        Ok(result)
    }

    /// Get columns and their nullability from a table
    fn get_table_columns(&self, table_info: &TableInfo) -> anyhow::Result<Vec<(String, bool)>> {
        let mut result = Vec::new();

        match table_info {
            TableInfo::Table {
                schema,
                name,
                is_nullable,
                ..
            } => {
                log::info!("[GET_COLUMNS] Getting columns for table {:?}.{} (is_nullable={})", schema, name, is_nullable);
                if let Some(rel) = self.find_relation(schema, name) {
                    for column in &rel.columns {
                        // Column is NOT NULL if table is not nullable (from join) AND column has NOT NULL constraint
                        let is_not_null = !is_nullable && self.is_column_not_null(rel, &column);
                        log::info!("[GET_COLUMNS]   Column '{}': is_not_null={}", column, is_not_null);
                        result.push((column.to_string(), is_not_null));
                    }
                }
            }
            TableInfo::View {
                schema,
                name,
                nullability,
                is_nullable,
                ..
            } => {
                log::info!("[GET_COLUMNS] Getting columns for view {:?}.{} (is_nullable={})", schema, name, is_nullable);
                log::info!("[GET_COLUMNS] View has {} columns in cache", nullability.len());
                for (col_name, &is_not_null) in nullability {
                    // View column nullability is affected by join context
                    let final_not_null = is_not_null && !is_nullable;
                    log::info!("[GET_COLUMNS]   Column '{}': cached is_not_null={}, final is_not_null={}", col_name, is_not_null, final_not_null);
                    result.push((col_name.clone(), final_not_null));
                }
            }
        }

        log::info!("[GET_COLUMNS] Returning {} columns", result.len());
        Ok(result)
    }

    fn is_column_ref_nullable(&self, col_ref: &protobuf::ColumnRef) -> anyhow::Result<bool> {
        // Extract table alias and column name from fields
        let fields = &col_ref.fields;
        log::debug!("Checking column ref with {} fields", fields.len());

        match fields.len() {
            1 => {
                // Single field - could be column name OR table alias (for whole-row reference)
                if let Some(protobuf::node::Node::String(name)) = &fields[0].node {
                    // First check if it's a table alias (whole-row reference like 'a AS address')
                    if let Some(table_info) = self.alias_map.get(&name.sval) {
                        log::debug!("Found table alias '{}' as whole-row reference", name.sval);
                        // For whole-row references, nullability depends on join type
                        match table_info {
                            TableInfo::Table { is_nullable, .. }
                            | TableInfo::View { is_nullable, .. } => {
                                return Ok(*is_nullable);
                            }
                        }
                    }

                    // If not a table alias, treat as column name
                    // If we have only one table in the FROM clause, use it
                    if self.alias_map.len() == 1 {
                        if let Some((_, table_info)) = self.alias_map.iter().next() {
                            match table_info {
                                TableInfo::Table {
                                    schema,
                                    name: table_name,
                                    ..
                                } => {
                                    if let Some(rel) = self.find_relation(schema, table_name) {
                                        let not_null = self.is_column_not_null(rel, &name.sval);
                                        return Ok(!not_null);
                                    }
                                }
                                TableInfo::View { nullability, .. } => {
                                    if let Some(&is_not_null) = nullability.get(&name.sval) {
                                        return Ok(!is_not_null);
                                    }
                                }
                            }
                        }
                    }

                    // Without a clear table qualifier, conservatively assume nullable
                    Ok(true)
                } else {
                    Ok(true)
                }
            }
            2 => {
                // Two fields - could be table.column OR table.* (whole-row reference)
                if let Some(protobuf::node::Node::String(table_ref)) = &fields[0].node {
                    // Check if second field is a column name or something else
                    if let Some(protobuf::node::Node::String(col_name)) = &fields[1].node {
                        // Normal table.column reference
                        // Look up table info from alias map
                        if let Some(table_info) = self.alias_map.get(&table_ref.sval) {
                            match table_info {
                                TableInfo::Table {
                                    schema,
                                    name,
                                    is_nullable,
                                    ..
                                } => {
                                    // If table is nullable due to outer join, column is nullable
                                    if *is_nullable {
                                        return Ok(true);
                                    }

                                    // Find the relation in rel_index
                                    if let Some(rel) = self.find_relation(schema, name) {
                                        // Check if column has NOT NULL constraint
                                        return Ok(!self.is_column_not_null(rel, &col_name.sval));
                                    }
                                }
                                TableInfo::View {
                                    nullability,
                                    is_nullable,
                                    ..
                                } => {
                                    // If view is nullable due to outer join, column is nullable
                                    if *is_nullable {
                                        return Ok(true);
                                    }

                                    // Look up column nullability from cached view results
                                    if let Some(&is_not_null) = nullability.get(&col_name.sval) {
                                        return Ok(!is_not_null);
                                    }
                                }
                            }
                        }
                        // Table not found or no constraint info - assume nullable
                        Ok(true)
                    } else {
                        // Second field is not a string - this is a whole-row reference (e.g., 'a AS address')
                        log::debug!(
                            "Found whole-row reference with table alias '{}'",
                            table_ref.sval
                        );
                        if let Some(table_info) = self.alias_map.get(&table_ref.sval) {
                            match table_info {
                                TableInfo::Table { is_nullable, .. }
                                | TableInfo::View { is_nullable, .. } => {
                                    return Ok(*is_nullable);
                                }
                            }
                        }
                        // Table not found - assume nullable
                        Ok(true)
                    }
                } else {
                    Ok(true)
                }
            }
            _ => {
                // More complex reference (e.g., schema.table.column) - default to nullable
                Ok(true)
            }
        }
    }

    fn is_column_not_null(&self, rel: &PgRel, column_name: &str) -> bool {
        // Check if column has a NOT NULL constraint
        for constraint in &rel.constraints {
            if let Constraint::NotNull(not_null) = constraint {
                if not_null.column.as_str() == column_name {
                    return true;
                }
            }
        }
        false
    }

    fn is_function_nullable(&self, func: &protobuf::FuncCall) -> anyhow::Result<bool> {
        // Extract function name
        if let Some(name_node) = func.funcname.last() {
            if let Some(protobuf::node::Node::String(s)) = &name_node.node {
                match s.sval.to_lowercase().as_str() {
                    // Aggregate functions that always return a value
                    "count" => Ok(false),

                    // Aggregate functions that can return NULL
                    "sum" | "avg" | "min" | "max" | "stddev" | "variance" => Ok(true),

                    // String functions that preserve nullability
                    "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "substring" | "substr" => {
                        // These return NULL if input is NULL
                        self.check_function_args_nullable(&func.args)
                    }

                    // String functions that always return a value
                    "length" | "char_length" | "bit_length" | "octet_length" => {
                        // These might return NULL if input is NULL in PostgreSQL
                        self.check_function_args_nullable(&func.args)
                    }

                    // COALESCE returns the first non-null argument
                    "coalesce" => {
                        // COALESCE is non-null if at least one argument is non-null
                        // For now, conservatively assume nullable
                        Ok(true)
                    }

                    // Mathematical functions
                    "abs" | "ceil" | "floor" | "round" | "trunc" => {
                        self.check_function_args_nullable(&func.args)
                    }

                    // Type conversion functions
                    "cast" => {
                        // CAST preserves nullability
                        self.check_function_args_nullable(&func.args)
                    }

                    // Date/time functions
                    "now" | "current_date" | "current_time" | "current_timestamp" => Ok(false),
                    "date_part" | "extract" => self.check_function_args_nullable(&func.args),

                    // Array functions
                    "array_agg" | "string_agg" => Ok(true), // Can return NULL on empty sets
                    "array_length" | "array_upper" | "array_lower" => Ok(true),

                    // JSON functions
                    "row_to_json" | "to_json" | "to_jsonb" => {
                        self.check_function_args_nullable(&func.args)
                    }

                    _ => Ok(true), // Unknown functions default to nullable
                }
            } else {
                Ok(true)
            }
        } else {
            Ok(true)
        }
    }

    fn check_function_args_nullable(&self, args: &[protobuf::Node]) -> anyhow::Result<bool> {
        // Check if any argument is nullable
        for arg in args {
            if self.is_expression_nullable(arg)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_join_type(&self, join: &protobuf::JoinExpr) -> JoinType {
        use protobuf::JoinType as ProtoJoinType;

        match join.jointype() {
            ProtoJoinType::JoinInner => JoinType::Inner,
            ProtoJoinType::JoinLeft => JoinType::Left,
            ProtoJoinType::JoinRight => JoinType::Right,
            ProtoJoinType::JoinFull => JoinType::Full,
            _ => JoinType::Inner, // Default to inner join
        }
    }

    fn apply_join_nullability(
        &mut self,
        join: &protobuf::JoinExpr,
        join_type: JoinType,
    ) -> anyhow::Result<()> {
        // Track which tables become nullable based on join type
        let mut nullable_tables = HashSet::new();

        match join_type {
            JoinType::Inner => {
                // Inner join doesn't affect nullability
            }
            JoinType::Left => {
                // LEFT JOIN makes right-side tables nullable
                if let Some(rarg) = &join.rarg {
                    self.collect_table_aliases(rarg, &mut nullable_tables)?;
                }
            }
            JoinType::Right => {
                // RIGHT JOIN makes left-side tables nullable
                if let Some(larg) = &join.larg {
                    self.collect_table_aliases(larg, &mut nullable_tables)?;
                }
            }
            JoinType::Full => {
                // FULL JOIN makes both sides nullable
                if let Some(larg) = &join.larg {
                    self.collect_table_aliases(larg, &mut nullable_tables)?;
                }
                if let Some(rarg) = &join.rarg {
                    self.collect_table_aliases(rarg, &mut nullable_tables)?;
                }
            }
        }

        // Mark these tables as producing nullable columns
        for alias in nullable_tables {
            if let Some(table_info) = self.alias_map.get_mut(&alias) {
                match table_info {
                    TableInfo::Table { is_nullable, .. } | TableInfo::View { is_nullable, .. } => {
                        *is_nullable = true;
                    }
                }
            }
        }

        Ok(())
    }

    fn collect_table_aliases(
        &self,
        node: &protobuf::Node,
        aliases: &mut HashSet<String>,
    ) -> anyhow::Result<()> {
        if let Some(node) = &node.node {
            match node {
                protobuf::node::Node::RangeVar(range_var) => {
                    let alias = range_var
                        .alias
                        .as_ref()
                        .map(|a| a.aliasname.clone())
                        .unwrap_or_else(|| range_var.relname.clone());
                    aliases.insert(alias);
                }
                protobuf::node::Node::JoinExpr(join) => {
                    if let Some(larg) = &join.larg {
                        self.collect_table_aliases(larg, aliases)?;
                    }
                    if let Some(rarg) = &join.rarg {
                        self.collect_table_aliases(rarg, aliases)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn find_relation(&self, schema: &Option<String>, table_name: &str) -> Option<&PgRel> {
        use crate::pg_rel::PgRelKind;
        self.rel_index.values().find(|rel| {
            rel.id.name() == table_name &&
            matches!(rel.kind, PgRelKind::Table) && // Only find base tables, not views
            match (rel.id.schema(), schema.as_deref()) {
                (rel_schema, Some(search_schema)) => rel_schema == search_schema,
                ("public", None) => true, // Default to public schema
                _ => false,
            }
        })
    }

    fn is_case_expr_nullable(&self, case_expr: &protobuf::CaseExpr) -> anyhow::Result<bool> {
        // Check if default result exists and is non-null
        if let Some(defresult) = &case_expr.defresult {
            if !self.is_expression_nullable(defresult)? {
                // If we have a non-null default, check all WHEN results
                let mut _all_results_covered = true;
                for when_clause in &case_expr.args {
                    if let Some(protobuf::node::Node::CaseWhen(when)) = &when_clause.node {
                        if let Some(result) = &when.result {
                            if self.is_expression_nullable(result)? {
                                // At least one result is nullable
                                return Ok(true);
                            }
                        }
                    }
                }
                // All results including default are non-null
                return Ok(false);
            }
        }

        // No default or nullable default - check if all WHEN results are non-null
        let mut _has_non_null_result = false;
        for when_clause in &case_expr.args {
            if let Some(protobuf::node::Node::CaseWhen(when)) = &when_clause.node {
                if let Some(result) = &when.result {
                    if !self.is_expression_nullable(result)? {
                        _has_non_null_result = true;
                    } else {
                        // At least one nullable result
                        return Ok(true);
                    }
                }
            }
        }

        // Conservative: assume nullable unless we can prove otherwise
        Ok(true)
    }

    fn is_coalesce_nullable(&self, args: &[protobuf::Node]) -> anyhow::Result<bool> {
        // COALESCE returns NULL only if all arguments are NULL
        // So it's non-null if at least one argument is guaranteed non-null
        for arg in args {
            if !self.is_expression_nullable(arg)? {
                return Ok(false); // Found a non-null argument
            }
        }
        Ok(true) // All arguments can be null
    }

    fn is_aexpr_nullable(&self, aexpr: &protobuf::AExpr) -> anyhow::Result<bool> {
        // AExpr represents various expressions like arithmetic, concatenation, etc.
        // For now, check if we have an operator
        if !aexpr.name.is_empty() {
            // Binary or unary operations
            // Get operator name
            if let Some(name_node) = aexpr.name.first() {
                if let Some(protobuf::node::Node::String(op_str)) = &name_node.node {
                    log::debug!("Found AExpr operator: {}", op_str.sval);
                    match op_str.sval.as_str() {
                        // Arithmetic operators - result is NULL if any operand is NULL
                        "+" | "-" | "*" | "/" | "%" | "^" => {
                            // Check if all operands are non-null
                            if let Some(lexpr) = &aexpr.lexpr {
                                if self.is_expression_nullable(lexpr)? {
                                    return Ok(true);
                                }
                            }
                            if let Some(rexpr) = &aexpr.rexpr {
                                if self.is_expression_nullable(rexpr)? {
                                    return Ok(true);
                                }
                            }
                            // Both operands are non-null
                            Ok(false)
                        }
                        // String concatenation
                        "||" => {
                            // In PostgreSQL, string concatenation with NULL returns NULL
                            // Check if all operands are non-null
                            if let Some(lexpr) = &aexpr.lexpr {
                                if self.is_expression_nullable(lexpr)? {
                                    return Ok(true);
                                }
                            }
                            if let Some(rexpr) = &aexpr.rexpr {
                                if self.is_expression_nullable(rexpr)? {
                                    return Ok(true);
                                }
                            }
                            // Both operands are non-null
                            Ok(false)
                        }
                        // Comparison operators - return NULL if either operand is NULL
                        "=" | "<>" | "!=" | "<" | ">" | "<=" | ">=" => {
                            // Check if either operand can be NULL
                            if let Some(lexpr) = &aexpr.lexpr {
                                if self.is_expression_nullable(lexpr)? {
                                    return Ok(true);
                                }
                            }
                            if let Some(rexpr) = &aexpr.rexpr {
                                if self.is_expression_nullable(rexpr)? {
                                    return Ok(true);
                                }
                            }
                            // Both operands are non-null, result is boolean
                            Ok(false)
                        }
                        // IS NULL / IS NOT NULL - always return boolean (never NULL)
                        "IS" | "IS NOT" => Ok(false),
                        _ => {
                            // Unknown operator, be conservative
                            log::debug!("Unknown operator in AExpr: {}", op_str.sval);
                            Ok(true)
                        }
                    }
                } else {
                    Ok(true)
                }
            } else {
                Ok(true)
            }
        } else {
            // No operator name, be conservative
            Ok(true)
        }
    }

    fn is_sublink_nullable(&self, sub_link: &protobuf::SubLink) -> anyhow::Result<bool> {
        // SubLink represents a subquery
        // Check the subquery type
        use protobuf::SubLinkType;

        match sub_link.sub_link_type() {
            SubLinkType::ExistsSublink | SubLinkType::AnySublink | SubLinkType::AllSublink => {
                // EXISTS, ANY, ALL always return boolean
                Ok(false)
            }
            SubLinkType::ExprSublink | SubLinkType::ArraySublink => {
                // Scalar subquery or array subquery
                // Need to analyze the subquery itself
                if let Some(subselect) = &sub_link.subselect {
                    if let Some(protobuf::node::Node::SelectStmt(select)) = &subselect.node {
                        // Check if it's a COUNT(*) or COUNT(column) query
                        if select.target_list.len() == 1 {
                            if let Some(target) = select.target_list.first() {
                                if let Some(protobuf::node::Node::ResTarget(res_target)) =
                                    &target.node
                                {
                                    if let Some(val) = &res_target.val {
                                        if let Some(protobuf::node::Node::FuncCall(func)) =
                                            &val.node
                                        {
                                            // Check if it's COUNT
                                            if let Some(name_node) = func.funcname.last() {
                                                if let Some(protobuf::node::Node::String(s)) =
                                                    &name_node.node
                                                {
                                                    if s.sval.to_lowercase() == "count" {
                                                        // COUNT always returns a value
                                                        return Ok(false);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // For other cases, be conservative
                Ok(true)
            }
            _ => Ok(true),
        }
    }

    fn is_field_select_nullable(
        &self,
        field_select: &protobuf::FieldSelect,
    ) -> anyhow::Result<bool> {
        // Field selection from composite type
        // The nullability depends on:
        // 1. Whether the composite value itself is nullable
        // 2. Whether the field within the composite is nullable

        if let Some(arg) = &field_select.arg {
            // First check if the composite value is nullable
            if self.is_expression_nullable(arg)? {
                // If the composite is nullable, the field selection is nullable
                return Ok(true);
            }

            // The composite value is not null
            // However, we don't have access to the composite type definition
            // to check if the specific field is nullable
            // For now, conservatively assume the field can be null
            Ok(true)
        } else {
            Ok(true)
        }
    }

    fn is_aindirection_nullable(
        &self,
        indirection: &protobuf::AIndirection,
    ) -> anyhow::Result<bool> {
        // AIndirection represents field access like (expr).field
        // First check if the base expression is nullable
        if let Some(arg) = &indirection.arg {
            if self.is_expression_nullable(arg)? {
                // If the base expression is nullable, the field access is nullable
                return Ok(true);
            }

            // The base expression is not null
            // For field access, we conservatively assume the field can be null
            // unless we have specific knowledge about the composite type
            Ok(true)
        } else {
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::NotNullConstraint;
    use crate::pg_id::PgId;
    use crate::pg_rel::PgRelKind;

    fn create_test_rel_index() -> RelIndex {
        let mut rel_index = RelIndex::default();

        // Add a test table with some columns
        let users_rel = PgRel {
            oid: 1001,
            id: PgId::new(Some("public".into()), "users".into()),
            kind: PgRelKind::Table,
            constraints: vec![
                Constraint::NotNull(NotNullConstraint {
                    name: "users_id_not_null".into(),
                    column: "id".into(),
                }),
                Constraint::NotNull(NotNullConstraint {
                    name: "users_email_not_null".into(),
                    column: "email".into(),
                }),
            ],
            columns: vec!["id".into(), "name".into(), "email".into()],
            column_types: vec![],
        };

        let posts_rel = PgRel {
            oid: 1002,
            id: PgId::new(Some("public".into()), "posts".into()),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::NotNull(NotNullConstraint {
                name: "posts_id_not_null".into(),
                column: "id".into(),
            })],
            columns: vec![
                "id".into(),
                "user_id".into(),
                "title".into(),
                "content".into(),
            ],
            column_types: vec![],
        };

        rel_index.insert(1001, users_rel);
        rel_index.insert(1002, posts_rel);

        rel_index
    }

    #[test]
    fn test_simple_select() {
        let rel_index = create_test_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);

        let view_def = "SELECT id, name, email FROM users";
        let columns = vec!["id".to_string(), "name".to_string(), "email".to_string()];

        let result = analyzer.analyze_view(view_def, &columns).unwrap();

        // id and email should be NOT NULL, name should be nullable
        assert_eq!(result.get("id"), Some(&true));
        assert_eq!(result.get("name"), Some(&false));
        assert_eq!(result.get("email"), Some(&true));
    }

    #[test]
    fn test_left_join() {
        let rel_index = create_test_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);

        let view_def = "SELECT u.id, u.name, p.title 
                        FROM users u 
                        LEFT JOIN posts p ON u.id = p.user_id";
        let columns = vec!["id".to_string(), "name".to_string(), "title".to_string()];

        let result = analyzer.analyze_view(view_def, &columns).unwrap();

        // u.id should be NOT NULL, u.name and p.title should be nullable (p.title due to LEFT JOIN)
        assert_eq!(result.get("id"), Some(&true));
        assert_eq!(result.get("name"), Some(&false));
        assert_eq!(result.get("title"), Some(&false)); // nullable due to LEFT JOIN
    }

    #[test]
    fn test_count_aggregate() {
        let rel_index = create_test_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);

        let view_def = "SELECT COUNT(*) as total, MAX(id) as max_id FROM users";
        let columns = vec!["total".to_string(), "max_id".to_string()];

        let result = analyzer.analyze_view(view_def, &columns).unwrap();

        // COUNT should be NOT NULL, MAX can be NULL
        assert_eq!(result.get("total"), Some(&true));
        assert_eq!(result.get("max_id"), Some(&false));
    }

    #[test]
    fn test_constants() {
        let rel_index = create_test_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);

        let view_def = "SELECT 1 as one, 'hello' as greeting, id FROM users";
        let columns = vec!["one".to_string(), "greeting".to_string(), "id".to_string()];

        let result = analyzer.analyze_view(view_def, &columns).unwrap();

        // Constants are always NOT NULL
        assert_eq!(result.get("one"), Some(&true));
        assert_eq!(result.get("greeting"), Some(&true));
        assert_eq!(result.get("id"), Some(&true));
    }
}
