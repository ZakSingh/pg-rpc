use crate::pg_constraint::Constraint;
use crate::rel_index::RelIndex;
use crate::sql_parser::QueryType;
use anyhow::{anyhow, Result};
use std::collections::HashSet;

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
    /// Default fallback for SELECT statements
    DefaultSelect,
    /// Complex query (CTEs, UNIONs, subqueries)
    ComplexQuery,
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

    /// Analyze WHERE clause for unique constraint matches.
    fn analyze_where_for_uniqueness(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<Option<CardinalityInference>> {
        // Extract the FROM clause to get the target table
        let (table_name, table_alias) = match self.extract_single_table_from_from_clause(select) {
            Some(info) => info,
            None => return Ok(None), // Multiple tables or complex FROM clause
        };

        // Find the table in rel_index
        let table_rel = match self.rel_index.values().find(|rel| rel.id.name() == table_name) {
            Some(rel) => rel,
            None => return Ok(None),
        };

        // Extract equality conditions from WHERE clause
        let where_clause = match &select.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        let equality_columns = self.extract_equality_columns(where_clause, table_alias.as_deref());

        if equality_columns.is_empty() {
            return Ok(None);
        }

        // Check if equality columns match primary key
        for constraint in &table_rel.constraints {
            if let Constraint::PrimaryKey(pk) = constraint {
                if self.columns_match_constraint(&equality_columns, &pk.columns) {
                    return Ok(Some(CardinalityInference {
                        cardinality: InferredCardinality::Opt,
                        confidence: InferenceConfidence::Medium,
                        reason: InferenceReason::PrimaryKeyMatch {
                            table: table_name.to_string(),
                            columns: pk.columns.iter().map(|c| c.to_string()).collect(),
                        },
                    }));
                }
            }
        }

        // Check if equality columns match any unique constraint
        for constraint in &table_rel.constraints {
            if let Constraint::Unique(unique) = constraint {
                if self.columns_match_constraint(&equality_columns, &unique.columns) {
                    return Ok(Some(CardinalityInference {
                        cardinality: InferredCardinality::Opt,
                        confidence: InferenceConfidence::Medium,
                        reason: InferenceReason::UniqueConstraintMatch {
                            table: table_name.to_string(),
                            constraint_name: unique.name.to_string(),
                            columns: unique.columns.iter().map(|c| c.to_string()).collect(),
                        },
                    }));
                }
            }
        }

        Ok(None)
    }

    /// Extract the single table name and optional alias from the FROM clause.
    /// Returns None if there are multiple tables or complex FROM structures.
    fn extract_single_table_from_from_clause(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Option<(String, Option<String>)> {
        if select.from_clause.len() != 1 {
            return None;
        }

        let from_node = select.from_clause.first()?;
        if let Some(pg_query::protobuf::node::Node::RangeVar(range_var)) = &from_node.node {
            let table_name = range_var.relname.clone();
            let alias = range_var.alias.as_ref().map(|a| a.aliasname.clone());
            return Some((table_name, alias));
        }

        None
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

    fn empty_rel_index() -> RelIndex {
        RelIndex::default()
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
}
