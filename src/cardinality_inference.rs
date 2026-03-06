use crate::pg_constraint::Constraint;
use crate::pg_rel::PgRel;
use crate::rel_index::RelIndex;
use crate::sql_parser::QueryType;
use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};

/// Analyzes SQL queries to infer their cardinality (one, opt, many, exec, execrows).
pub struct CardinalityAnalyzer<'a> {
    rel_index: &'a RelIndex,
}

/// Result of cardinality inference.
#[derive(Debug, Clone)]
pub struct CardinalityInference {
    pub cardinality: InferredCardinality,
    pub confidence: InferenceConfidence,
    pub reason: InferenceReason,
}

/// The inferred cardinality of a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferredCardinality {
    /// Exactly one row (panics if zero rows)
    One,
    /// Zero or one row (returns Option<T>)
    Opt,
    /// Zero or more rows (returns Vec<T>)
    Many,
    /// No rows returned (returns ())
    Exec,
    /// No rows returned, returns affected row count (returns u64)
    ExecRows,
}

impl From<InferredCardinality> for QueryType {
    fn from(c: InferredCardinality) -> Self {
        match c {
            InferredCardinality::One => QueryType::One,
            InferredCardinality::Opt => QueryType::Opt,
            InferredCardinality::Many => QueryType::Many,
            InferredCardinality::Exec => QueryType::Exec,
            InferredCardinality::ExecRows => QueryType::ExecRows,
        }
    }
}

/// Confidence level of the inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceConfidence {
    /// High confidence - based on SQL structure (LIMIT, aggregates, statement type)
    High,
    /// Medium confidence - based on constraint analysis
    Medium,
    /// Low confidence - fallback inference
    Low,
}

/// Reason for the inference decision.
#[derive(Debug, Clone)]
pub enum InferenceReason {
    /// INSERT/UPDATE/DELETE without RETURNING
    DmlWithoutReturning,
    /// INSERT with RETURNING (no ON CONFLICT), single row
    InsertReturning,
    /// INSERT with multiple rows (VALUES with multiple tuples or SELECT) with RETURNING
    InsertMultipleRowsReturning,
    /// INSERT ... ON CONFLICT DO UPDATE ... RETURNING (upsert)
    UpsertReturning,
    /// INSERT ... ON CONFLICT DO NOTHING ... RETURNING
    InsertOnConflictDoNothingReturning,
    /// SELECT with LIMIT 1
    SelectLimit1,
    /// Aggregate function without GROUP BY
    AggregateWithoutGroupBy,
    /// WHERE clause matches primary key columns
    PrimaryKeyMatch { table: String, columns: Vec<String> },
    /// WHERE clause matches unique constraint columns
    UniqueConstraintMatch {
        table: String,
        constraint_name: String,
        columns: Vec<String>,
    },
    /// WHERE clause matches unique constraint with to-one joins
    UniqueMatchWithJoins {
        base_table: String,
        columns: Vec<String>,
        joins: Vec<String>,
    },
    /// Default fallback for SELECT statements
    DefaultSelect,
    /// Complex query (CTEs, UNIONs, subqueries)
    ComplexQuery,
}

/// Internal type to track which uniqueness constraint matched
#[derive(Debug, Clone)]
enum MatchedConstraint {
    PrimaryKey,
    Unique { name: String },
}

/// Information about a table in the FROM clause
#[derive(Debug, Clone)]
struct TableRef {
    /// The actual table name in the database
    name: String,
    /// The alias used in the query (if any)
    alias: Option<String>,
}

impl TableRef {
    /// Get the name used to reference this table in the query (alias if present, otherwise name)
    fn query_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Information about a JOIN in the query
#[derive(Debug, Clone)]
struct JoinInfo {
    /// The table being joined to
    table: TableRef,
    /// The join type (INNER, LEFT, RIGHT, FULL)
    join_type: JoinType,
    /// Columns from the left side of the join condition
    left_columns: Vec<(String, String)>, // (table_ref, column)
    /// Columns from the right side of the join condition
    right_columns: Vec<(String, String)>, // (table_ref, column)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Extracted FROM clause information including base table and joins
#[derive(Debug)]
struct FromClauseInfo {
    /// The base (leftmost) table
    base_table: TableRef,
    /// All joins in order
    joins: Vec<JoinInfo>,
    /// Map from query reference name (alias or table name) to actual table name
    table_map: HashMap<String, String>,
}

impl<'a> CardinalityAnalyzer<'a> {
    pub fn new(rel_index: &'a RelIndex) -> Self {
        Self { rel_index }
    }

    /// Infer the cardinality of a SQL query.
    pub fn infer(&self, sql: &str) -> Result<CardinalityInference> {
        let parse_result =
            pg_query::parse(sql).map_err(|e| anyhow!("Failed to parse SQL: {}", e))?;

        let stmt = parse_result
            .protobuf
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .ok_or_else(|| anyhow!("No statement found in SQL"))?;

        match &stmt.node {
            Some(pg_query::protobuf::node::Node::SelectStmt(select)) => {
                self.analyze_select(select)
            }
            Some(pg_query::protobuf::node::Node::InsertStmt(insert)) => {
                self.analyze_insert(insert)
            }
            Some(pg_query::protobuf::node::Node::UpdateStmt(update)) => {
                self.analyze_update(update)
            }
            Some(pg_query::protobuf::node::Node::DeleteStmt(delete)) => {
                self.analyze_delete(delete)
            }
            _ => Ok(CardinalityInference {
                cardinality: InferredCardinality::Many,
                confidence: InferenceConfidence::Low,
                reason: InferenceReason::ComplexQuery,
            }),
        }
    }

    /// Analyze a SELECT statement.
    fn analyze_select(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<CardinalityInference> {
        // Check for UNION/INTERSECT/EXCEPT - these are complex queries
        if select.op != pg_query::protobuf::SetOperation::SetopNone as i32 {
            return Ok(CardinalityInference {
                cardinality: InferredCardinality::Many,
                confidence: InferenceConfidence::Low,
                reason: InferenceReason::ComplexQuery,
            });
        }

        // Check for LIMIT 1
        if let Some(limit_count) = &select.limit_count {
            if let Some(pg_query::protobuf::node::Node::AConst(a_const)) = &limit_count.node {
                if let Some(val) = &a_const.val {
                    if let pg_query::protobuf::a_const::Val::Ival(int_val) = val {
                        if int_val.ival == 1 {
                            return Ok(CardinalityInference {
                                cardinality: InferredCardinality::Opt,
                                confidence: InferenceConfidence::High,
                                reason: InferenceReason::SelectLimit1,
                            });
                        }
                    }
                }
            }
        }

        // Check for aggregate functions without GROUP BY
        if select.group_clause.is_empty() && self.has_aggregate_without_group_by(select) {
            return Ok(CardinalityInference {
                cardinality: InferredCardinality::One,
                confidence: InferenceConfidence::High,
                reason: InferenceReason::AggregateWithoutGroupBy,
            });
        }

        // Check for unique constraint matches in WHERE clause
        if let Some(inference) = self.analyze_where_for_uniqueness(select)? {
            return Ok(inference);
        }

        // Default: return many
        Ok(CardinalityInference {
            cardinality: InferredCardinality::Many,
            confidence: InferenceConfidence::Low,
            reason: InferenceReason::DefaultSelect,
        })
    }

    /// Check if the SELECT has aggregate functions without GROUP BY.
    fn has_aggregate_without_group_by(&self, select: &pg_query::protobuf::SelectStmt) -> bool {
        // Look for aggregate function calls in target list
        for target in &select.target_list {
            if self.node_contains_aggregate(target) {
                return true;
            }
        }
        false
    }

    /// Check if a node contains an aggregate function call.
    fn node_contains_aggregate(&self, node: &pg_query::protobuf::Node) -> bool {
        if let Some(inner) = &node.node {
            match inner {
                pg_query::protobuf::node::Node::ResTarget(res_target) => {
                    if let Some(val) = &res_target.val {
                        return self.node_contains_aggregate(val);
                    }
                }
                pg_query::protobuf::node::Node::FuncCall(func_call) => {
                    // Check if this is an aggregate function
                    let func_name = func_call
                        .funcname
                        .iter()
                        .filter_map(|n| {
                            if let Some(pg_query::protobuf::node::Node::String(s)) = &n.node {
                                Some(s.sval.to_lowercase())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(".");

                    // Common aggregate functions
                    let aggregates = [
                        "count", "sum", "avg", "min", "max", "array_agg", "string_agg", "bool_and",
                        "bool_or", "every", "bit_and", "bit_or", "json_agg", "jsonb_agg",
                        "json_object_agg", "jsonb_object_agg", "xmlagg",
                    ];

                    if aggregates.contains(&func_name.as_str()) {
                        return true;
                    }

                    // Also check if agg_star or agg_distinct is set
                    if func_call.agg_star || func_call.agg_distinct {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Analyze WHERE clause for unique constraint matches, including through joins.
    fn analyze_where_for_uniqueness(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<Option<CardinalityInference>> {
        // Extract FROM clause info (base table + joins)
        let from_info = match self.extract_from_clause_info(select) {
            Some(info) => info,
            None => return Ok(None),
        };

        // Find the base table in rel_index
        let base_table_rel = match self.find_table_rel(&from_info.base_table.name) {
            Some(rel) => rel,
            None => return Ok(None),
        };

        // Extract equality conditions from WHERE clause
        let where_clause = match &select.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Extract equality conditions for each table
        let equality_map = self.extract_equality_columns_by_table(where_clause, &from_info);

        // Get equality columns for the base table
        let base_query_name = from_info.base_table.query_name().to_string();
        let base_equality_columns = equality_map
            .get(&base_query_name)
            .cloned()
            .unwrap_or_default();

        if base_equality_columns.is_empty() {
            return Ok(None);
        }

        // Check if base table has uniqueness guarantee
        let matched_constraint = match self.find_matching_uniqueness_constraint(base_table_rel, &base_equality_columns) {
            Some(m) => m,
            None => return Ok(None),
        };

        // If no joins, return simple uniqueness match
        if from_info.joins.is_empty() {
            return Ok(Some(self.create_uniqueness_inference(
                &from_info.base_table.name,
                &base_equality_columns,
                matched_constraint,
            )));
        }

        // Check if all joins are "to-one" (joining to unique/primary key columns)
        let mut join_descriptions = Vec::new();
        for join in &from_info.joins {
            if !self.is_to_one_join(&join, &from_info)? {
                // This join could produce multiple rows, so we can't guarantee uniqueness
                return Ok(None);
            }
            join_descriptions.push(format!(
                "{} -> {}",
                join.table.query_name(),
                join.table.name
            ));
        }

        // All joins are to-one, so uniqueness is preserved
        Ok(Some(CardinalityInference {
            cardinality: InferredCardinality::Opt,
            confidence: InferenceConfidence::Medium,
            reason: InferenceReason::UniqueMatchWithJoins {
                base_table: from_info.base_table.name.clone(),
                columns: base_equality_columns.into_iter().collect(),
                joins: join_descriptions,
            },
        }))
    }

    /// Check if a table has a uniqueness guarantee based on equality columns.
    /// Returns Some with the matched constraint info, or None if no uniqueness constraint matches.
    fn find_matching_uniqueness_constraint(
        &self,
        table_rel: &PgRel,
        equality_columns: &HashSet<String>,
    ) -> Option<MatchedConstraint> {
        for constraint in &table_rel.constraints {
            match constraint {
                Constraint::PrimaryKey(pk) => {
                    if self.columns_match_constraint(equality_columns, &pk.columns) {
                        return Some(MatchedConstraint::PrimaryKey);
                    }
                }
                Constraint::Unique(unique) => {
                    if self.columns_match_constraint(equality_columns, &unique.columns) {
                        return Some(MatchedConstraint::Unique {
                            name: unique.name.to_string(),
                        });
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Create a uniqueness inference result
    fn create_uniqueness_inference(
        &self,
        table_name: &str,
        columns: &HashSet<String>,
        matched: MatchedConstraint,
    ) -> CardinalityInference {
        let reason = match matched {
            MatchedConstraint::PrimaryKey => InferenceReason::PrimaryKeyMatch {
                table: table_name.to_string(),
                columns: columns.iter().cloned().collect(),
            },
            MatchedConstraint::Unique { name } => InferenceReason::UniqueConstraintMatch {
                table: table_name.to_string(),
                constraint_name: name,
                columns: columns.iter().cloned().collect(),
            },
        };
        CardinalityInference {
            cardinality: InferredCardinality::Opt,
            confidence: InferenceConfidence::Medium,
            reason,
        }
    }

    /// Check if a join is "to-one" - i.e., the join will produce at most one row
    /// from the joined table for each row in the source table.
    ///
    /// This can happen in two ways:
    /// 1. The joined table's columns in the condition form a unique/primary key
    /// 2. The join follows a foreign key relationship (FK always points to unique/PK)
    fn is_to_one_join(&self, join: &JoinInfo, from_info: &FromClauseInfo) -> Result<bool> {
        // Get the joined table
        let joined_table_rel = match self.find_table_rel(&join.table.name) {
            Some(rel) => rel,
            None => return Ok(false), // Unknown table, assume many
        };

        // Collect columns from each side of the join condition
        let joined_table_query_name = join.table.query_name();
        let mut joined_columns: HashSet<String> = HashSet::new();
        let mut source_columns: Vec<(String, String)> = Vec::new(); // (table_ref, column)

        // Pair up left and right columns from the join condition
        for ((left_table, left_col), (right_table, right_col)) in
            join.left_columns.iter().zip(join.right_columns.iter())
        {
            if right_table == joined_table_query_name {
                joined_columns.insert(right_col.clone());
                source_columns.push((left_table.clone(), left_col.clone()));
            } else if left_table == joined_table_query_name {
                joined_columns.insert(left_col.clone());
                source_columns.push((right_table.clone(), right_col.clone()));
            }
        }

        if joined_columns.is_empty() {
            return Ok(false);
        }

        // Method 1: Check if joined columns form a unique key on the joined table
        if self.find_matching_uniqueness_constraint(joined_table_rel, &joined_columns).is_some() {
            return Ok(true);
        }

        // Method 2: Check if this join follows a foreign key relationship
        // Look for FK on any source table that references the joined table
        if self.is_fk_based_to_one_join(&source_columns, &join.table.name, &joined_columns, from_info) {
            return Ok(true);
        }

        Ok(false)
    }

    /// Check if a join follows a foreign key relationship, making it "to-one".
    ///
    /// For a join like: `orders JOIN customers ON orders.customer_id = customers.customer_id`
    /// If orders.customer_id has a FK to customers.customer_id (which must be unique/PK),
    /// then this is a to-one join.
    fn is_fk_based_to_one_join(
        &self,
        source_columns: &[(String, String)], // (table_ref, column) from source side
        target_table_name: &str,
        target_columns: &HashSet<String>,
        from_info: &FromClauseInfo,
    ) -> bool {
        // Group source columns by their table
        let mut columns_by_source_table: HashMap<String, Vec<String>> = HashMap::new();
        for (table_ref, col) in source_columns {
            // Resolve table_ref to actual table name
            let actual_table = if table_ref.is_empty() {
                from_info.base_table.name.clone()
            } else if table_ref == from_info.base_table.query_name() {
                from_info.base_table.name.clone()
            } else {
                // Check joins for matching alias
                from_info.joins.iter()
                    .find(|j| j.table.query_name() == table_ref)
                    .map(|j| j.table.name.clone())
                    .unwrap_or_else(|| table_ref.clone())
            };
            columns_by_source_table
                .entry(actual_table)
                .or_default()
                .push(col.clone());
        }

        // Check each source table for a matching FK
        for (source_table_name, source_cols) in &columns_by_source_table {
            if let Some(source_rel) = self.find_table_rel(source_table_name) {
                if self.has_matching_fk(source_rel, source_cols, target_table_name, target_columns) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a table has a foreign key that matches the join condition.
    ///
    /// Returns true if there's an FK where:
    /// - FK columns match source_cols
    /// - FK references target_table
    /// - FK referenced columns match target_cols
    fn has_matching_fk(
        &self,
        source_rel: &PgRel,
        source_cols: &[String],
        target_table_name: &str,
        target_cols: &HashSet<String>,
    ) -> bool {
        let source_cols_set: HashSet<&str> = source_cols.iter().map(|s| s.as_str()).collect();

        for constraint in &source_rel.constraints {
            if let Constraint::ForeignKey(fk) = constraint {
                // Check if FK references the target table
                let fk_ref_table = match &fk.ref_table {
                    Some(t) => t,
                    None => continue,
                };

                if fk_ref_table != target_table_name {
                    continue;
                }

                // Check if FK columns match source columns
                let fk_cols: HashSet<&str> = fk.columns.iter().map(|c| c.as_str()).collect();
                if fk_cols != source_cols_set {
                    continue;
                }

                // Check if FK referenced columns match target columns
                let fk_ref_cols: HashSet<String> = match &fk.ref_columns {
                    Some(cols) => cols.iter().map(|c| c.to_string()).collect(),
                    None => continue,
                };

                if &fk_ref_cols == target_cols {
                    // Found matching FK - this is a to-one join
                    return true;
                }
            }
        }

        false
    }

    /// Find a table relation by name
    fn find_table_rel(&self, table_name: &str) -> Option<&PgRel> {
        self.rel_index.values().find(|rel| rel.id.name() == table_name)
    }

    /// Extract FROM clause information including base table and all joins
    fn extract_from_clause_info(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Option<FromClauseInfo> {
        if select.from_clause.len() != 1 {
            return None;
        }

        let from_node = select.from_clause.first()?;
        let mut table_map = HashMap::new();

        match &from_node.node {
            // Simple table reference (no joins)
            Some(pg_query::protobuf::node::Node::RangeVar(range_var)) => {
                let table_ref = TableRef {
                    name: range_var.relname.clone(),
                    alias: range_var.alias.as_ref().map(|a| a.aliasname.clone()),
                };
                table_map.insert(table_ref.query_name().to_string(), table_ref.name.clone());

                Some(FromClauseInfo {
                    base_table: table_ref,
                    joins: Vec::new(),
                    table_map,
                })
            }
            // JOIN expression
            Some(pg_query::protobuf::node::Node::JoinExpr(join_expr)) => {
                self.parse_join_expr(join_expr, &mut table_map)
            }
            _ => None,
        }
    }

    /// Recursively parse a JoinExpr to extract base table and all joins
    fn parse_join_expr(
        &self,
        join_expr: &pg_query::protobuf::JoinExpr,
        table_map: &mut HashMap<String, String>,
    ) -> Option<FromClauseInfo> {
        // Parse the left side (could be a table or another join)
        let (base_table, mut joins) = self.parse_join_arg(join_expr.larg.as_ref()?, table_map)?;

        // Parse the right side (should be a table)
        let right_table = self.extract_table_from_node(join_expr.rarg.as_ref()?, table_map)?;

        // Determine join type
        let join_type = match join_expr.jointype() {
            pg_query::protobuf::JoinType::JoinInner => JoinType::Inner,
            pg_query::protobuf::JoinType::JoinLeft => JoinType::Left,
            pg_query::protobuf::JoinType::JoinRight => JoinType::Right,
            pg_query::protobuf::JoinType::JoinFull => JoinType::Full,
            _ => JoinType::Inner, // Default to inner
        };

        // Extract join condition columns
        let (left_columns, right_columns) = join_expr
            .quals
            .as_ref()
            .map(|quals| self.extract_join_columns(quals))
            .unwrap_or_default();

        joins.push(JoinInfo {
            table: right_table,
            join_type,
            left_columns,
            right_columns,
        });

        Some(FromClauseInfo {
            base_table,
            joins,
            table_map: table_map.clone(),
        })
    }

    /// Parse a join argument (left or right side of a join)
    fn parse_join_arg(
        &self,
        node: &pg_query::protobuf::Node,
        table_map: &mut HashMap<String, String>,
    ) -> Option<(TableRef, Vec<JoinInfo>)> {
        match &node.node {
            Some(pg_query::protobuf::node::Node::RangeVar(range_var)) => {
                let table_ref = TableRef {
                    name: range_var.relname.clone(),
                    alias: range_var.alias.as_ref().map(|a| a.aliasname.clone()),
                };
                table_map.insert(table_ref.query_name().to_string(), table_ref.name.clone());
                Some((table_ref, Vec::new()))
            }
            Some(pg_query::protobuf::node::Node::JoinExpr(join_expr)) => {
                let info = self.parse_join_expr(join_expr, table_map)?;
                Some((info.base_table, info.joins))
            }
            _ => None,
        }
    }

    /// Extract a table reference from a node
    fn extract_table_from_node(
        &self,
        node: &pg_query::protobuf::Node,
        table_map: &mut HashMap<String, String>,
    ) -> Option<TableRef> {
        if let Some(pg_query::protobuf::node::Node::RangeVar(range_var)) = &node.node {
            let table_ref = TableRef {
                name: range_var.relname.clone(),
                alias: range_var.alias.as_ref().map(|a| a.aliasname.clone()),
            };
            table_map.insert(table_ref.query_name().to_string(), table_ref.name.clone());
            return Some(table_ref);
        }
        None
    }

    /// Extract column references from a join condition
    fn extract_join_columns(
        &self,
        quals: &pg_query::protobuf::Node,
    ) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let mut left_columns = Vec::new();
        let mut right_columns = Vec::new();

        self.collect_join_equality_columns(quals, &mut left_columns, &mut right_columns);

        (left_columns, right_columns)
    }

    /// Recursively collect equality column pairs from join conditions
    fn collect_join_equality_columns(
        &self,
        node: &pg_query::protobuf::Node,
        left_columns: &mut Vec<(String, String)>,
        right_columns: &mut Vec<(String, String)>,
    ) {
        if let Some(inner) = &node.node {
            match inner {
                pg_query::protobuf::node::Node::BoolExpr(bool_expr) => {
                    // Process AND expressions
                    if bool_expr.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
                        for arg in &bool_expr.args {
                            self.collect_join_equality_columns(arg, left_columns, right_columns);
                        }
                    }
                }
                pg_query::protobuf::node::Node::AExpr(a_expr) => {
                    // Check if this is an equality expression
                    let is_equality = a_expr.name.iter().any(|n| {
                        if let Some(pg_query::protobuf::node::Node::String(s)) = &n.node {
                            s.sval == "="
                        } else {
                            false
                        }
                    });

                    if is_equality {
                        if let (Some(lexpr), Some(rexpr)) = (&a_expr.lexpr, &a_expr.rexpr) {
                            if let (Some(left_col), Some(right_col)) = (
                                self.extract_qualified_column(lexpr),
                                self.extract_qualified_column(rexpr),
                            ) {
                                left_columns.push(left_col);
                                right_columns.push(right_col);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Extract a qualified column reference (table.column or alias.column)
    fn extract_qualified_column(&self, node: &pg_query::protobuf::Node) -> Option<(String, String)> {
        if let Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) = &node.node {
            let fields: Vec<String> = col_ref
                .fields
                .iter()
                .filter_map(|f| {
                    if let Some(pg_query::protobuf::node::Node::String(s)) = &f.node {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .collect();

            match fields.len() {
                2 => Some((fields[0].clone(), fields[1].clone())),
                1 => Some((String::new(), fields[0].clone())), // Unqualified column
                _ => None,
            }
        } else {
            None
        }
    }

    /// Extract equality columns organized by table reference.
    /// Handles AND expressions by collecting all columns, and OR expressions by
    /// finding the intersection of columns across all branches.
    fn extract_equality_columns_by_table(
        &self,
        where_clause: &pg_query::protobuf::Node,
        from_info: &FromClauseInfo,
    ) -> HashMap<String, HashSet<String>> {
        self.collect_equality_columns_recursive(where_clause, from_info)
    }

    /// Recursively collect equality columns, handling both AND and OR expressions.
    /// For AND: union of all columns from each branch
    /// For OR: intersection of columns from each branch (only columns in ALL branches count)
    fn collect_equality_columns_recursive(
        &self,
        node: &pg_query::protobuf::Node,
        from_info: &FromClauseInfo,
    ) -> HashMap<String, HashSet<String>> {
        if let Some(inner) = &node.node {
            match inner {
                pg_query::protobuf::node::Node::BoolExpr(bool_expr) => {
                    if bool_expr.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
                        // AND: merge all columns from each branch
                        let mut result: HashMap<String, HashSet<String>> = HashMap::new();
                        for arg in &bool_expr.args {
                            let branch_cols = self.collect_equality_columns_recursive(arg, from_info);
                            for (table, cols) in branch_cols {
                                result.entry(table).or_default().extend(cols);
                            }
                        }
                        result
                    } else if bool_expr.boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
                        // OR: intersect columns across all branches
                        // A column only counts if it has an equality condition in EVERY branch
                        self.intersect_or_branches(&bool_expr.args, from_info)
                    } else {
                        // NOT or other - ignore
                        HashMap::new()
                    }
                }
                pg_query::protobuf::node::Node::AExpr(a_expr) => {
                    let mut result: HashMap<String, HashSet<String>> = HashMap::new();
                    let is_equality = a_expr.name.iter().any(|n| {
                        if let Some(pg_query::protobuf::node::Node::String(s)) = &n.node {
                            s.sval == "="
                        } else {
                            false
                        }
                    });

                    if is_equality {
                        if let (Some(lexpr), Some(rexpr)) = (&a_expr.lexpr, &a_expr.rexpr) {
                            // Try left side as column with parameter on right
                            if let Some((table_ref, col_name)) = self.extract_qualified_column(lexpr) {
                                if self.is_parameter_or_literal(rexpr) {
                                    let table_key = if table_ref.is_empty() {
                                        from_info.base_table.query_name().to_string()
                                    } else {
                                        table_ref
                                    };
                                    result.entry(table_key).or_default().insert(col_name);
                                }
                            }
                            // Try right side as column with parameter on left
                            if let Some((table_ref, col_name)) = self.extract_qualified_column(rexpr) {
                                if self.is_parameter_or_literal(lexpr) {
                                    let table_key = if table_ref.is_empty() {
                                        from_info.base_table.query_name().to_string()
                                    } else {
                                        table_ref
                                    };
                                    result.entry(table_key).or_default().insert(col_name);
                                }
                            }
                        }
                    }
                    result
                }
                _ => HashMap::new(),
            }
        } else {
            HashMap::new()
        }
    }

    /// For OR expressions, find columns that appear in ALL branches.
    /// Returns the intersection of equality columns across branches.
    fn intersect_or_branches(
        &self,
        args: &[pg_query::protobuf::Node],
        from_info: &FromClauseInfo,
    ) -> HashMap<String, HashSet<String>> {
        if args.is_empty() {
            return HashMap::new();
        }

        // Get columns from first branch as starting point
        let mut result = self.collect_equality_columns_recursive(&args[0], from_info);

        // Intersect with each subsequent branch
        for arg in args.iter().skip(1) {
            let branch_cols = self.collect_equality_columns_recursive(arg, from_info);

            // For each table in result, keep only columns that also exist in this branch
            result = result
                .into_iter()
                .filter_map(|(table, cols)| {
                    if let Some(branch_table_cols) = branch_cols.get(&table) {
                        let intersection: HashSet<String> = cols
                            .intersection(branch_table_cols)
                            .cloned()
                            .collect();
                        if intersection.is_empty() {
                            None
                        } else {
                            Some((table, intersection))
                        }
                    } else {
                        // This table doesn't appear in this branch, remove it
                        None
                    }
                })
                .collect();
        }

        result
    }

    /// Extract the single table name and optional alias from the FROM clause.
    /// Returns None if there are multiple tables or complex FROM structures.
    fn extract_single_table_from_from_clause(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Option<(String, Option<String>)> {
        let from_info = self.extract_from_clause_info(select)?;
        if from_info.joins.is_empty() {
            Some((from_info.base_table.name, from_info.base_table.alias))
        } else {
            None
        }
    }

    /// Extract column names that have equality conditions in the WHERE clause.
    /// Only considers AND-connected conditions.
    fn extract_equality_columns(
        &self,
        where_clause: &pg_query::protobuf::Node,
        table_alias: Option<&str>,
    ) -> HashSet<String> {
        let mut columns = HashSet::new();
        self.collect_equality_columns(where_clause, table_alias, &mut columns);
        columns
    }

    /// Recursively collect equality columns from a WHERE clause.
    fn collect_equality_columns(
        &self,
        node: &pg_query::protobuf::Node,
        table_alias: Option<&str>,
        columns: &mut HashSet<String>,
    ) {
        if let Some(inner) = &node.node {
            match inner {
                pg_query::protobuf::node::Node::BoolExpr(bool_expr) => {
                    // Only process AND expressions
                    if bool_expr.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
                        for arg in &bool_expr.args {
                            self.collect_equality_columns(arg, table_alias, columns);
                        }
                    }
                    // OR expressions break uniqueness guarantees, so we don't recurse into them
                }
                pg_query::protobuf::node::Node::AExpr(a_expr) => {
                    // Check if this is an equality expression
                    let is_equality = a_expr.name.iter().any(|n| {
                        if let Some(pg_query::protobuf::node::Node::String(s)) = &n.node {
                            s.sval == "="
                        } else {
                            false
                        }
                    });

                    if is_equality {
                        // Check if one side is a column reference and other is a parameter or literal
                        if let (Some(lexpr), Some(rexpr)) = (&a_expr.lexpr, &a_expr.rexpr) {
                            // Try left side as column
                            if let Some(col_name) =
                                self.extract_column_name(lexpr, table_alias)
                            {
                                if self.is_parameter_or_literal(rexpr) {
                                    columns.insert(col_name);
                                }
                            }
                            // Try right side as column
                            if let Some(col_name) =
                                self.extract_column_name(rexpr, table_alias)
                            {
                                if self.is_parameter_or_literal(lexpr) {
                                    columns.insert(col_name);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Extract column name from a node if it's a column reference.
    fn extract_column_name(
        &self,
        node: &pg_query::protobuf::Node,
        table_alias: Option<&str>,
    ) -> Option<String> {
        if let Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) = &node.node {
            let fields: Vec<String> = col_ref
                .fields
                .iter()
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
                    // Just column name
                    return Some(fields[0].clone());
                }
                2 => {
                    // table.column or alias.column
                    let qualifier = &fields[0];
                    let column = &fields[1];

                    // Accept if qualifier matches table alias or if no alias specified
                    if table_alias.map_or(true, |alias| alias == qualifier) {
                        return Some(column.clone());
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Check if a node is a parameter ($N) or a literal value.
    fn is_parameter_or_literal(&self, node: &pg_query::protobuf::Node) -> bool {
        if let Some(inner) = &node.node {
            match inner {
                pg_query::protobuf::node::Node::ParamRef(_) => return true,
                pg_query::protobuf::node::Node::AConst(_) => return true,
                pg_query::protobuf::node::Node::TypeCast(type_cast) => {
                    // Check the inner expression of a type cast
                    if let Some(arg) = &type_cast.arg {
                        return self.is_parameter_or_literal(arg);
                    }
                }
                _ => {}
            }
        }
        false
    }

    /// Check if the equality columns cover all columns in a constraint.
    fn columns_match_constraint<T: AsRef<str>>(
        &self,
        equality_columns: &HashSet<String>,
        constraint_columns: &[T],
    ) -> bool {
        // All constraint columns must have equality conditions
        constraint_columns
            .iter()
            .all(|c| equality_columns.contains(c.as_ref()))
    }

    /// Analyze an INSERT statement.
    fn analyze_insert(
        &self,
        insert: &pg_query::protobuf::InsertStmt,
    ) -> Result<CardinalityInference> {
        let has_returning = !insert.returning_list.is_empty();

        if !has_returning {
            return Ok(CardinalityInference {
                cardinality: InferredCardinality::Exec,
                confidence: InferenceConfidence::High,
                reason: InferenceReason::DmlWithoutReturning,
            });
        }

        // Check if this is INSERT ... SELECT or multi-row VALUES (could return multiple rows)
        if insert.select_stmt.is_some() && !self.is_single_row_values(insert) {
            return Ok(CardinalityInference {
                cardinality: InferredCardinality::Many,
                confidence: InferenceConfidence::High,
                reason: InferenceReason::InsertMultipleRowsReturning,
            });
        }

        // Check for ON CONFLICT clause
        if let Some(on_conflict) = &insert.on_conflict_clause {
            // ON CONFLICT DO NOTHING - may not insert (for single-row insert)
            if on_conflict.action == pg_query::protobuf::OnConflictAction::OnconflictNothing as i32
            {
                return Ok(CardinalityInference {
                    cardinality: InferredCardinality::Opt,
                    confidence: InferenceConfidence::High,
                    reason: InferenceReason::InsertOnConflictDoNothingReturning,
                });
            }

            // ON CONFLICT DO UPDATE (upsert) - always returns one row for single-row insert
            if on_conflict.action == pg_query::protobuf::OnConflictAction::OnconflictUpdate as i32 {
                return Ok(CardinalityInference {
                    cardinality: InferredCardinality::One,
                    confidence: InferenceConfidence::High,
                    reason: InferenceReason::UpsertReturning,
                });
            }
        }

        // Single-row INSERT with RETURNING
        Ok(CardinalityInference {
            cardinality: InferredCardinality::One,
            confidence: InferenceConfidence::High,
            reason: InferenceReason::InsertReturning,
        })
    }

    /// Check if INSERT has a single-row VALUES clause.
    fn is_single_row_values(&self, insert: &pg_query::protobuf::InsertStmt) -> bool {
        if let Some(select_node) = &insert.select_stmt {
            if let Some(pg_query::protobuf::node::Node::SelectStmt(select)) = &select_node.node {
                // Check if this is a VALUES clause (not a SELECT subquery)
                if !select.values_lists.is_empty() {
                    // Single row = exactly one values list
                    return select.values_lists.len() == 1;
                }
                // If values_lists is empty, this is INSERT ... SELECT (not single row)
            }
        }
        false
    }

    /// Analyze an UPDATE statement.
    fn analyze_update(
        &self,
        update: &pg_query::protobuf::UpdateStmt,
    ) -> Result<CardinalityInference> {
        let has_returning = !update.returning_list.is_empty();

        if !has_returning {
            return Ok(CardinalityInference {
                cardinality: InferredCardinality::Exec,
                confidence: InferenceConfidence::High,
                reason: InferenceReason::DmlWithoutReturning,
            });
        }

        // Check if WHERE clause matches unique constraints
        if let Some(where_clause) = &update.where_clause {
            if let Some(relation) = &update.relation {
                let table_name = &relation.relname;

                // Find the table in rel_index
                if let Some(table_rel) = self.rel_index.values().find(|rel| rel.id.name() == table_name) {
                    let equality_columns = self.extract_equality_columns(where_clause, None);

                    // Check primary key
                    for constraint in &table_rel.constraints {
                        if let Constraint::PrimaryKey(pk) = constraint {
                            if self.columns_match_constraint(&equality_columns, &pk.columns) {
                                return Ok(CardinalityInference {
                                    cardinality: InferredCardinality::Opt,
                                    confidence: InferenceConfidence::Medium,
                                    reason: InferenceReason::PrimaryKeyMatch {
                                        table: table_name.clone(),
                                        columns: pk.columns.iter().map(|c| c.to_string()).collect(),
                                    },
                                });
                            }
                        }
                    }

                    // Check unique constraints
                    for constraint in &table_rel.constraints {
                        if let Constraint::Unique(unique) = constraint {
                            if self.columns_match_constraint(&equality_columns, &unique.columns) {
                                return Ok(CardinalityInference {
                                    cardinality: InferredCardinality::Opt,
                                    confidence: InferenceConfidence::Medium,
                                    reason: InferenceReason::UniqueConstraintMatch {
                                        table: table_name.clone(),
                                        constraint_name: unique.name.to_string(),
                                        columns: unique.columns.iter().map(|c| c.to_string()).collect(),
                                    },
                                });
                            }
                        }
                    }
                }
            }
        }

        // UPDATE with RETURNING but no unique constraint match - could return multiple rows
        Ok(CardinalityInference {
            cardinality: InferredCardinality::Many,
            confidence: InferenceConfidence::Low,
            reason: InferenceReason::DefaultSelect,
        })
    }

    /// Analyze a DELETE statement.
    fn analyze_delete(
        &self,
        delete: &pg_query::protobuf::DeleteStmt,
    ) -> Result<CardinalityInference> {
        let has_returning = !delete.returning_list.is_empty();

        if !has_returning {
            return Ok(CardinalityInference {
                cardinality: InferredCardinality::Exec,
                confidence: InferenceConfidence::High,
                reason: InferenceReason::DmlWithoutReturning,
            });
        }

        // Check if WHERE clause matches unique constraints
        if let Some(where_clause) = &delete.where_clause {
            if let Some(relation) = &delete.relation {
                let table_name = &relation.relname;

                // Find the table in rel_index
                if let Some(table_rel) = self.rel_index.values().find(|rel| rel.id.name() == table_name) {
                    let equality_columns = self.extract_equality_columns(where_clause, None);

                    // Check primary key
                    for constraint in &table_rel.constraints {
                        if let Constraint::PrimaryKey(pk) = constraint {
                            if self.columns_match_constraint(&equality_columns, &pk.columns) {
                                return Ok(CardinalityInference {
                                    cardinality: InferredCardinality::Opt,
                                    confidence: InferenceConfidence::Medium,
                                    reason: InferenceReason::PrimaryKeyMatch {
                                        table: table_name.clone(),
                                        columns: pk.columns.iter().map(|c| c.to_string()).collect(),
                                    },
                                });
                            }
                        }
                    }

                    // Check unique constraints
                    for constraint in &table_rel.constraints {
                        if let Constraint::Unique(unique) = constraint {
                            if self.columns_match_constraint(&equality_columns, &unique.columns) {
                                return Ok(CardinalityInference {
                                    cardinality: InferredCardinality::Opt,
                                    confidence: InferenceConfidence::Medium,
                                    reason: InferenceReason::UniqueConstraintMatch {
                                        table: table_name.clone(),
                                        constraint_name: unique.name.to_string(),
                                        columns: unique.columns.iter().map(|c| c.to_string()).collect(),
                                    },
                                });
                            }
                        }
                    }
                }
            }
        }

        // DELETE with RETURNING but no unique constraint match - could return multiple rows
        Ok(CardinalityInference {
            cardinality: InferredCardinality::Many,
            confidence: InferenceConfidence::Low,
            reason: InferenceReason::DefaultSelect,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::UniqueConstraint;
    use crate::pg_id::PgId;
    use crate::pg_rel::{PgRel, PgRelKind};
    use smallvec::smallvec;
    use ustr::ustr;

    fn empty_rel_index() -> RelIndex {
        RelIndex::default()
    }

    fn rel_index_with_unique_index() -> RelIndex {
        let mut index = RelIndex::default();
        // Simulate a table with a unique index (section, slug)
        // The SQL now synthesizes Constraint::Unique for unique indexes
        let rel = PgRel {
            oid: 12345,
            id: PgId::new(Some(ustr("public")), ustr("help_center_article")),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::Unique(UniqueConstraint {
                name: ustr("help_center_article_section_slug_idx"),
                columns: smallvec![ustr("section"), ustr("slug")],
            })],
            columns: vec![
                ustr("id"),
                ustr("section"),
                ustr("slug"),
                ustr("body_html"),
                ustr("is_published"),
            ],
            column_types: vec![23, 25, 25, 25, 16], // int4, text, text, text, bool
        };
        index.insert(12345, rel);
        index
    }

    #[test]
    fn test_insert_without_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "INSERT INTO users (name, email) VALUES ($1, $2)";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Exec);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_insert_with_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::One);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_insert_on_conflict_do_nothing_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO NOTHING RETURNING *";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_insert_on_conflict_do_update_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "INSERT INTO users (email, name) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET name = $2 RETURNING *";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::One);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_insert_multiple_values_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Multiple VALUES tuples
        let sql = "INSERT INTO users (name) VALUES ('a'), ('b') RETURNING *";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
        assert_eq!(inference.confidence, InferenceConfidence::High);
        assert!(matches!(
            inference.reason,
            InferenceReason::InsertMultipleRowsReturning
        ));
    }

    #[test]
    fn test_insert_select_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // INSERT ... SELECT
        let sql = "INSERT INTO users (name) SELECT name FROM temp_users RETURNING *";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
        assert_eq!(inference.confidence, InferenceConfidence::High);
        assert!(matches!(
            inference.reason,
            InferenceReason::InsertMultipleRowsReturning
        ));
    }

    #[test]
    fn test_insert_three_rows_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Three rows
        let sql = "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie') RETURNING *";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_update_without_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "UPDATE users SET name = $1 WHERE id = $2";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Exec);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_delete_without_returning() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "DELETE FROM users WHERE id = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Exec);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_select_limit_1() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users ORDER BY created_at DESC LIMIT 1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_select_aggregate_count() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT COUNT(*) FROM users";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::One);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_select_aggregate_sum() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT SUM(amount) FROM orders";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::One);
        assert_eq!(inference.confidence, InferenceConfidence::High);
    }

    #[test]
    fn test_select_aggregate_with_group_by() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }

    #[test]
    fn test_select_default() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users WHERE status = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }

    #[test]
    fn test_select_unique_index_match() {
        // Test that filtering on columns covered by a unique index returns Opt
        let rel_index = rel_index_with_unique_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Query filters on both columns of the unique index (section, slug)
        let sql = "SELECT body_html FROM help_center_article WHERE section = $1 AND slug = $2 AND is_published";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert_eq!(inference.confidence, InferenceConfidence::Medium);
        assert!(matches!(
            inference.reason,
            InferenceReason::UniqueConstraintMatch { .. }
        ));
    }

    #[test]
    fn test_select_unique_index_partial_match() {
        // Test that filtering on only one column of a multi-column unique index returns Many
        let rel_index = rel_index_with_unique_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Query only filters on 'section', missing 'slug' from the unique index
        let sql = "SELECT body_html FROM help_center_article WHERE section = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }

    fn rel_index_with_orders_and_customers() -> RelIndex {
        use crate::pg_constraint::PrimaryKeyConstraint;
        let mut index = RelIndex::default();

        // Orders table with primary key
        let orders = PgRel {
            oid: 10001,
            id: PgId::new(Some(ustr("public")), ustr("orders")),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
                name: ustr("orders_pkey"),
                columns: smallvec![ustr("order_id")],
            })],
            columns: vec![ustr("order_id"), ustr("customer_id"), ustr("total")],
            column_types: vec![23, 23, 1700], // int4, int4, numeric
        };
        index.insert(10001, orders);

        // Customers table with primary key
        let customers = PgRel {
            oid: 10002,
            id: PgId::new(Some(ustr("public")), ustr("customers")),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
                name: ustr("customers_pkey"),
                columns: smallvec![ustr("customer_id")],
            })],
            columns: vec![ustr("customer_id"), ustr("name"), ustr("email")],
            column_types: vec![23, 25, 25], // int4, text, text
        };
        index.insert(10002, customers);

        index
    }

    #[test]
    fn test_select_with_to_one_join() {
        // Test that a query with a to-one join (joining on primary key) preserves uniqueness
        let rel_index = rel_index_with_orders_and_customers();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Query joins orders to customers on customer_id (PK of customers)
        // and filters by orders.order_id (PK of orders)
        let sql = "SELECT o.*, c.name FROM orders o JOIN customers c ON c.customer_id = o.customer_id WHERE o.order_id = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::UniqueMatchWithJoins { .. }
        ));
    }

    #[test]
    fn test_select_with_to_many_join() {
        // Test that a query joining on non-unique columns does not guarantee single row
        let rel_index = rel_index_with_orders_and_customers();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Query filters by customer_id (not unique in orders), join doesn't help
        // Note: customers.customer_id IS unique, but orders.customer_id is NOT
        // So this should return Many because multiple orders can have the same customer_id
        let sql = "SELECT o.*, c.name FROM orders o JOIN customers c ON c.customer_id = o.customer_id WHERE o.customer_id = $1";
        let inference = analyzer.infer(sql).unwrap();

        // The WHERE clause doesn't match a unique constraint on orders
        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }

    #[test]
    fn test_select_left_join_to_one() {
        // Test that LEFT JOIN to a unique key also preserves uniqueness
        let rel_index = rel_index_with_orders_and_customers();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT o.*, c.name FROM orders o LEFT JOIN customers c ON c.customer_id = o.customer_id WHERE o.order_id = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::UniqueMatchWithJoins { .. }
        ));
    }

    fn rel_index_with_users_pk() -> RelIndex {
        use crate::pg_constraint::PrimaryKeyConstraint;
        let mut index = RelIndex::default();

        let users = PgRel {
            oid: 20001,
            id: PgId::new(Some(ustr("public")), ustr("users")),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
                name: ustr("users_pkey"),
                columns: smallvec![ustr("user_id")],
            })],
            columns: vec![ustr("user_id"), ustr("email"), ustr("status")],
            column_types: vec![23, 25, 25], // int4, text, text
        };
        index.insert(20001, users);

        index
    }

    #[test]
    fn test_or_with_same_unique_column_in_all_branches() {
        // WHERE (user_id = $1 AND status = 'a') OR (user_id = $1 AND status = 'b')
        // user_id appears in both branches, so uniqueness is preserved
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users WHERE (user_id = $1 AND status = 'active') OR (user_id = $1 AND status = 'pending')";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::PrimaryKeyMatch { .. }
        ));
    }

    #[test]
    fn test_or_with_different_columns_in_branches() {
        // WHERE user_id = $1 OR email = $2
        // Different columns in each branch, no common uniqueness
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users WHERE user_id = $1 OR email = $2";
        let inference = analyzer.infer(sql).unwrap();

        // Should be Many because we can't guarantee uniqueness
        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }

    #[test]
    fn test_or_with_unique_column_only_in_one_branch() {
        // WHERE (user_id = $1) OR (status = 'active')
        // user_id only in first branch, status not unique
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users WHERE user_id = $1 OR status = 'active'";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }

    #[test]
    fn test_nested_and_or_with_common_unique_column() {
        // WHERE user_id = $1 AND (status = 'a' OR status = 'b')
        // user_id is outside the OR, should still match
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users WHERE user_id = $1 AND (status = 'active' OR status = 'pending')";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::PrimaryKeyMatch { .. }
        ));
    }

    #[test]
    fn test_simple_or_same_column() {
        // WHERE user_id = $1 OR user_id = $2
        // Same column in both branches, uniqueness preserved
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT * FROM users WHERE user_id = $1 OR user_id = $2";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::PrimaryKeyMatch { .. }
        ));
    }

    /// Creates a rel_index where orders has a FK to customers, but customers.customer_id
    /// is NOT marked as a primary key. This tests FK-based inference.
    fn rel_index_with_fk_relationship() -> RelIndex {
        use crate::pg_constraint::{ForeignKeyConstraint, PrimaryKeyConstraint};
        use crate::pg_constraint::OnDelete;
        let mut index = RelIndex::default();

        // Orders table with PK and FK to customers
        let orders = PgRel {
            oid: 30001,
            id: PgId::new(Some(ustr("public")), ustr("orders")),
            kind: PgRelKind::Table,
            constraints: vec![
                Constraint::PrimaryKey(PrimaryKeyConstraint {
                    name: ustr("orders_pkey"),
                    columns: smallvec![ustr("order_id")],
                }),
                Constraint::ForeignKey(ForeignKeyConstraint {
                    name: ustr("orders_customer_id_fkey"),
                    columns: smallvec![ustr("customer_id")],
                    on_delete: OnDelete::NoAction,
                    ref_table: Some("customers".to_string()),
                    ref_columns: Some(smallvec![ustr("customer_id")]),
                }),
            ],
            columns: vec![ustr("order_id"), ustr("customer_id"), ustr("total")],
            column_types: vec![23, 23, 1700],
        };
        index.insert(30001, orders);

        // Customers table - note: we intentionally DON'T add PK constraint here
        // to test that FK-based inference works without needing to check target uniqueness
        // (In real DBs, FK target must be unique, so we can trust the FK definition)
        let customers = PgRel {
            oid: 30002,
            id: PgId::new(Some(ustr("public")), ustr("customers")),
            kind: PgRelKind::Table,
            constraints: vec![], // No constraints - testing FK-based inference
            columns: vec![ustr("customer_id"), ustr("name"), ustr("email")],
            column_types: vec![23, 25, 25],
        };
        index.insert(30002, customers);

        // Products table with PK
        let products = PgRel {
            oid: 30003,
            id: PgId::new(Some(ustr("public")), ustr("products")),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
                name: ustr("products_pkey"),
                columns: smallvec![ustr("product_id")],
            })],
            columns: vec![ustr("product_id"), ustr("name"), ustr("price")],
            column_types: vec![23, 25, 1700],
        };
        index.insert(30003, products);

        // Order_items table with FK to orders and products
        let order_items = PgRel {
            oid: 30004,
            id: PgId::new(Some(ustr("public")), ustr("order_items")),
            kind: PgRelKind::Table,
            constraints: vec![
                Constraint::PrimaryKey(PrimaryKeyConstraint {
                    name: ustr("order_items_pkey"),
                    columns: smallvec![ustr("item_id")],
                }),
                Constraint::ForeignKey(ForeignKeyConstraint {
                    name: ustr("order_items_order_id_fkey"),
                    columns: smallvec![ustr("order_id")],
                    on_delete: OnDelete::Cascade,
                    ref_table: Some("orders".to_string()),
                    ref_columns: Some(smallvec![ustr("order_id")]),
                }),
                Constraint::ForeignKey(ForeignKeyConstraint {
                    name: ustr("order_items_product_id_fkey"),
                    columns: smallvec![ustr("product_id")],
                    on_delete: OnDelete::Restrict,
                    ref_table: Some("products".to_string()),
                    ref_columns: Some(smallvec![ustr("product_id")]),
                }),
            ],
            columns: vec![ustr("item_id"), ustr("order_id"), ustr("product_id"), ustr("quantity")],
            column_types: vec![23, 23, 23, 23],
        };
        index.insert(30004, order_items);

        index
    }

    #[test]
    fn test_fk_based_to_one_join() {
        // Join from orders to customers using FK relationship
        // Even though customers has no PK constraint in our test data,
        // the FK relationship tells us this is a to-one join
        let rel_index = rel_index_with_fk_relationship();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_id = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::UniqueMatchWithJoins { .. }
        ));
    }

    #[test]
    fn test_fk_based_to_one_join_reversed_condition() {
        // Same as above but with reversed join condition order
        let rel_index = rel_index_with_fk_relationship();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "SELECT o.*, c.name FROM orders o JOIN customers c ON c.customer_id = o.customer_id WHERE o.order_id = $1";
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::UniqueMatchWithJoins { .. }
        ));
    }

    #[test]
    fn test_multiple_fk_based_joins() {
        // order_items -> orders -> customers (chain of FK relationships)
        // But we're only joining order_items to orders and products
        let rel_index = rel_index_with_fk_relationship();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // order_items has FK to orders (to-one via FK) and FK to products (to-one via PK on products)
        let sql = r#"
            SELECT oi.*, o.total, p.name
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE oi.item_id = $1
        "#;
        let inference = analyzer.infer(sql).unwrap();

        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::UniqueMatchWithJoins { .. }
        ));
    }

    #[test]
    fn test_no_fk_no_pk_join_is_many() {
        // Join without FK or PK match should return Many
        let rel_index = rel_index_with_fk_relationship();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Joining customers to orders on customer_id
        // customers does NOT have FK to orders, and orders.customer_id is not unique
        // So this could return multiple rows
        let sql = "SELECT c.*, o.total FROM customers c JOIN orders o ON c.customer_id = o.customer_id WHERE c.customer_id = $1";
        let inference = analyzer.infer(sql).unwrap();

        // This should be Many because:
        // - customers.customer_id is in WHERE (but customers has no PK constraint)
        // - The join to orders is "to-many" (one customer can have many orders)
        assert_eq!(inference.cardinality, InferredCardinality::Many);
    }
}
