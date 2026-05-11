use crate::codegen::OID;
use crate::pg_constraint::Constraint;
use crate::pg_rel::PgRel;
use crate::rel_index::RelIndex;
use indexmap::IndexMap;
use pg_query::protobuf;
use std::collections::{HashMap, HashSet};

/// Two-level nullability for a single column.
///
/// Distinguishes between "nullable because of a LEFT/RIGHT/FULL JOIN" and
/// "nullable because the base column itself allows NULLs". This is needed
/// for double-underscore column grouping: when all grouped columns are
/// join-nullable, the group becomes `Option<Struct>` while inner fields
/// use only their base-table nullability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnNullability {
    /// Column is nullable because its source table/view is on the outer side of a join.
    pub nullable_due_to_join: bool,
    /// Column is nullable on its base table/view (independent of joins).
    pub nullable_on_base: bool,
}

impl ColumnNullability {
    /// Returns true if the column is NOT NULL (neither join-nullable nor base-nullable).
    pub fn is_not_null(&self) -> bool {
        !self.nullable_due_to_join && !self.nullable_on_base
    }

    /// Returns true if the column is nullable for any reason.
    pub fn is_nullable(&self) -> bool {
        self.nullable_due_to_join || self.nullable_on_base
    }
}

/// Cache for storing view nullability analysis results
/// Key: (schema, view_name), Value: map of column_name -> nullability (order-preserving)
pub type ViewNullabilityCache =
    HashMap<(Option<String>, String), IndexMap<String, ColumnNullability>>;

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
    /// CTE name -> column nullability map, populated while walking `WITH` clauses.
    /// Looked up when a CTE name appears in a FROM clause so we can resolve
    /// `cte_alias.col` references against the CTE's inferred output shape.
    cte_columns: HashMap<String, IndexMap<String, ColumnNullability>>,
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
        /// Column name -> nullability mapping (order-preserving)
        nullability: IndexMap<String, ColumnNullability>,
        /// True if this view reference can produce NULL values due to outer joins
        is_nullable: bool,
    },
    /// "Derived" table — a CTE reference, FROM-clause subquery, or set-returning
    /// function. Behaves like [`TableInfo::View`] for column lookup, but isn't
    /// backed by a real `pg_class` entry, so things like star-expansion via
    /// `get_table_columns` should treat it as opaque.
    Derived {
        /// Column name -> nullability, in declaration order.
        nullability: IndexMap<String, ColumnNullability>,
        /// True if this reference can produce NULL values due to outer joins.
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

/// Extract the function name from a node inside a `RangeFunction.functions`
/// list. The pg_query AST wraps the call in a `List` with a `FuncCall` as the
/// first item. Returns `(name, true)` if a FuncCall was found, otherwise
/// `("", false)`.
fn extract_range_function_name(node: &protobuf::Node) -> (String, bool) {
    let inner = match node.node.as_ref() {
        Some(protobuf::node::Node::List(list)) => list.items.first(),
        _ => Some(node),
    };
    let func = match inner.and_then(|n| n.node.as_ref()) {
        Some(protobuf::node::Node::FuncCall(f)) => f,
        _ => return (String::new(), false),
    };
    // funcname may be schema-qualified (`pg_catalog.unnest`); take the last segment.
    let last = func.funcname.last().and_then(|n| n.node.as_ref());
    match last {
        Some(protobuf::node::Node::String(s)) => (s.sval.clone(), true),
        _ => (String::new(), true),
    }
}

/// Collapse a positional column list (output of [`ViewNullabilityAnalyzer::analyze_select_stmt`])
/// into an order-preserving map. Duplicate or empty names get a synthetic
/// `column_N` placeholder so they survive the round-trip.
fn vec_to_indexmap(cols: Vec<(String, ColumnNullability)>) -> IndexMap<String, ColumnNullability> {
    let mut out = IndexMap::new();
    for (i, (name, cn)) in cols.into_iter().enumerate() {
        let key = if name.is_empty() {
            format!("column_{}", i)
        } else {
            name
        };
        out.insert(key, cn);
    }
    out
}

/// Merge two set-operation arms (UNION / INTERSECT / EXCEPT).
///
/// PostgreSQL matches columns positionally. A column is NOT NULL iff *every*
/// arm reports it NOT NULL. Mismatched lengths fall back to all-nullable for
/// safety — we never claim NOT NULL on a column we couldn't verify on both
/// arms. Output column names come from the left arm (matching pg's behaviour).
fn merge_set_op_columns(
    larg: &[(String, ColumnNullability)],
    rarg: &[(String, ColumnNullability)],
) -> Vec<(String, ColumnNullability)> {
    if larg.len() != rarg.len() {
        return larg
            .iter()
            .map(|(n, _)| {
                (
                    n.clone(),
                    ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    },
                )
            })
            .collect();
    }
    larg.iter()
        .zip(rarg.iter())
        .map(|((name, l), (_, r))| {
            (
                name.clone(),
                ColumnNullability {
                    // Either side reaching the column through an outer join taints the union.
                    nullable_due_to_join: l.nullable_due_to_join || r.nullable_due_to_join,
                    // Base-nullable on either arm makes the merged column base-nullable.
                    nullable_on_base: l.nullable_on_base || r.nullable_on_base,
                },
            )
        })
        .collect()
}

impl<'a> ViewNullabilityAnalyzer<'a> {
    pub fn new(rel_index: &'a RelIndex, view_nullability_cache: &'a ViewNullabilityCache) -> Self {
        Self {
            rel_index,
            alias_map: HashMap::new(),
            nullable_columns: HashSet::new(),
            view_nullability_cache,
            cte_columns: HashMap::new(),
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

    /// Analyze a view definition and return a map of column names to their nullability.
    /// Returns an IndexMap to preserve column order.
    pub fn analyze_view(
        &mut self,
        view_definition: &str,
        view_columns: &[String],
    ) -> anyhow::Result<IndexMap<String, ColumnNullability>> {
        log::info!("Analyzing view with {} columns", view_columns.len());
        log::info!("View definition: {}", view_definition);

        // Parse the view definition
        let parse_result = pg_query::parse(view_definition)?;

        // Clear previous state
        self.alias_map.clear();
        self.nullable_columns.clear();
        self.cte_columns.clear();

        // Walk the top-level SelectStmt with the shared helper. Anything else
        // (INSERT/UPDATE/DELETE … RETURNING) is handled by
        // `refine_nullability_from_dml_target` in the introspector, not here.
        let column_nullability = if let Some(stmt) = parse_result.protobuf.stmts.first() {
            if let Some(node) = &stmt.stmt {
                if let Some(protobuf::node::Node::SelectStmt(select)) = &node.node {
                    self.analyze_select_stmt(select)?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Map view columns to their nullability
        // Use IndexMap to preserve column order
        let mut result = IndexMap::new();
        for (i, column_name) in view_columns.iter().enumerate() {
            if let Some((_, cn)) = column_nullability.get(i) {
                result.insert(column_name.clone(), *cn);
            } else {
                // Default to nullable if we couldn't analyze
                result.insert(
                    column_name.clone(),
                    ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    },
                );
            }
        }

        Ok(result)
    }

    /// Analyze a `SelectStmt` and return its output columns in declaration order,
    /// each paired with its inferred nullability.
    ///
    /// Handles three top-level shapes:
    ///   * `WITH` prefix — each CTE is analyzed and registered in `cte_columns`
    ///     before the outer SELECT body, so later FROM references resolve.
    ///   * Set operations (`UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`) — both
    ///     arms are analyzed and merged column-wise. A column is NOT NULL only
    ///     if it is NOT NULL on *every* arm; mismatched column counts produce
    ///     all-nullable output (conservative).
    ///   * Plain `SELECT … FROM …` — FROM items are walked into `alias_map`,
    ///     then each target expression is analyzed.
    ///
    /// FROM-clause handling mutates `self.alias_map`. Callers that want to
    /// analyze a *nested* SelectStmt without poisoning the outer alias state
    /// should snapshot/restore (the CTE-walk does this).
    fn analyze_select_stmt(
        &mut self,
        select: &protobuf::SelectStmt,
    ) -> anyhow::Result<Vec<(String, ColumnNullability)>> {
        // 1. Process the WITH clause first so the outer query's FROM items can
        //    reference CTE aliases.
        if let Some(with) = &select.with_clause {
            self.analyze_with_clause(with)?;
        }

        // 2. UNION/INTERSECT/EXCEPT: analyze each arm and merge. The arms each
        //    have their own FROM clause, so we snapshot alias state to keep
        //    them independent.
        use protobuf::SetOperation;
        let op = SetOperation::try_from(select.op).unwrap_or(SetOperation::Undefined);
        if op != SetOperation::SetopNone {
            let larg_cols = if let Some(larg) = &select.larg {
                self.analyze_subselect(larg)?
            } else {
                Vec::new()
            };
            let rarg_cols = if let Some(rarg) = &select.rarg {
                self.analyze_subselect(rarg)?
            } else {
                Vec::new()
            };
            return Ok(merge_set_op_columns(&larg_cols, &rarg_cols));
        }

        // 3. Plain SELECT.
        for from_item in &select.from_clause {
            self.analyze_from_item(from_item)?;
        }

        let mut columns: Vec<(String, ColumnNullability)> = Vec::new();
        for target in &select.target_list {
            if let Some(protobuf::node::Node::ResTarget(res_target)) = &target.node {
                if let Some(val) = &res_target.val {
                    if self.is_star_expression(val) {
                        let expanded = self.expand_star_expression_detailed(val)?;
                        columns.extend(expanded);
                    } else {
                        let cn = self.expression_nullability_detailed(val)?;
                        // Output column name is the `AS alias` if present, else
                        // best-effort derived from the expression. Missing names
                        // become empty strings — callers that care about names
                        // (the CTE path) supply explicit `aliascolnames` overrides.
                        let name = if !res_target.name.is_empty() {
                            res_target.name.clone()
                        } else {
                            self.extract_column_name(val).unwrap_or_default()
                        };
                        columns.push((name, cn));
                    }
                }
            }
        }
        Ok(columns)
    }

    /// Determine the column-nullability map produced by a `RangeFunction`
    /// (set-returning function in FROM). Conservative by design: if we don't
    /// recognize the function, return an empty map so callers fall back to
    /// "all columns nullable".
    ///
    /// What we DO infer:
    ///   * `unnest(arr)` — produces one column. Element type matches the array
    ///     element, but pg's introspection over `unnest` can't see whether the
    ///     producing array contained NULLs, so the column stays nullable.
    ///   * `unnest(...) WITH ORDINALITY` — appends a NOT NULL `bigint`
    ///     ordinality column at the end. We only assert NOT NULL on that one.
    ///   * `generate_series(...)` — single NOT NULL column (`generate_series`).
    ///     With ordinality, an additional NOT NULL `bigint`.
    ///
    /// Anything with a `coldeflist` (e.g. `jsonb_to_recordset(...) AS t(a int, b text)`)
    /// has columns we can name but not prove non-null — JSON values can be
    /// missing per row — so we list them all as nullable.
    fn analyze_range_function(
        &self,
        range_fn: &protobuf::RangeFunction,
    ) -> IndexMap<String, ColumnNullability> {
        let mut columns: IndexMap<String, ColumnNullability> = IndexMap::new();

        // `coldeflist` is the explicit `AS t(col1 type1, col2 type2, …)` form.
        // Always nullable — we can't prove otherwise without per-row evidence.
        for col in &range_fn.coldeflist {
            if let Some(protobuf::node::Node::ColumnDef(col_def)) = &col.node {
                columns.insert(
                    col_def.colname.clone(),
                    ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    },
                );
            }
        }

        // Inspect each function call to learn its output shape and name.
        // Without `coldeflist`, the alias's `colnames` (or the function name)
        // determines the column name.
        if columns.is_empty() {
            for (i, fn_node) in range_fn.functions.iter().enumerate() {
                let (fn_name, has_func_call) = extract_range_function_name(fn_node);
                if !has_func_call {
                    continue;
                }
                // Try to use the alias's per-position colnames first, falling
                // back to the function name for the single-output case.
                let column_name = range_fn
                    .alias
                    .as_ref()
                    .and_then(|a| a.colnames.get(i))
                    .and_then(|n| {
                        if let Some(protobuf::node::Node::String(s)) = &n.node {
                            Some(s.sval.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| fn_name.clone());
                let cn = match fn_name.as_str() {
                    // `generate_series` is the textbook NOT NULL set-returning
                    // function — every element is by construction non-null.
                    "generate_series" => ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: false,
                    },
                    // `unnest` propagates whatever NULLs were in the array.
                    // Without per-array-element evidence we assume nullable.
                    _ => ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    },
                };
                columns.insert(column_name, cn);
            }
        }

        // `WITH ORDINALITY` appends one NOT NULL `bigint` column. The alias's
        // colnames list (if any) gives its name; otherwise pg defaults to
        // "ordinality".
        if range_fn.ordinality {
            let ord_name = range_fn
                .alias
                .as_ref()
                .and_then(|a| a.colnames.last())
                .and_then(|n| {
                    if let Some(protobuf::node::Node::String(s)) = &n.node {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| "ordinality".to_string());
            columns.insert(
                ord_name,
                ColumnNullability {
                    nullable_due_to_join: false,
                    nullable_on_base: false,
                },
            );
        }

        columns
    }

    /// Analyze a boxed `SelectStmt` (from a UNION arm or subquery) with a
    /// snapshot/restore of `alias_map` so the inner scope can't leak.
    fn analyze_subselect(
        &mut self,
        select: &protobuf::SelectStmt,
    ) -> anyhow::Result<Vec<(String, ColumnNullability)>> {
        let saved = std::mem::take(&mut self.alias_map);
        let result = self.analyze_select_stmt(select);
        self.alias_map = saved;
        result
    }

    /// Populate `self.cte_columns` from a `WITH` clause. Each CTE's body may
    /// itself be a SELECT (possibly with another WITH) or a data-modifying
    /// statement with `RETURNING`. Anything we can't analyze conservatively
    /// produces no entry, so later references fall back to all-nullable.
    fn analyze_with_clause(&mut self, with: &protobuf::WithClause) -> anyhow::Result<()> {
        for cte_node in &with.ctes {
            let cte = match &cte_node.node {
                Some(protobuf::node::Node::CommonTableExpr(c)) => c,
                _ => continue,
            };
            let columns = match self.analyze_cte_body(cte)? {
                Some(cols) => cols,
                None => continue,
            };

            // Apply `aliascolnames` (`WITH cte(a, b, c) AS …`) if present —
            // these rename the CTE's output columns positionally.
            let renamed = if cte.aliascolnames.is_empty() {
                columns
            } else {
                let mut out = IndexMap::new();
                for (i, alias_node) in cte.aliascolnames.iter().enumerate() {
                    if let Some(protobuf::node::Node::String(s)) = &alias_node.node {
                        if let Some((_, cn)) = columns.get_index(i) {
                            out.insert(s.sval.clone(), *cn);
                        }
                    }
                }
                // Carry over any unrenamed trailing columns under their original names.
                for (i, (k, v)) in columns.iter().enumerate() {
                    if i >= cte.aliascolnames.len() {
                        out.insert(k.clone(), *v);
                    }
                }
                out
            };

            self.cte_columns.insert(cte.ctename.clone(), renamed);
        }
        Ok(())
    }

    /// Analyze a single CTE body. Returns `None` if the body shape isn't one
    /// we know how to introspect (very deeply-nested or future Postgres
    /// constructs) — callers should treat that as "all columns nullable".
    fn analyze_cte_body(
        &mut self,
        cte: &protobuf::CommonTableExpr,
    ) -> anyhow::Result<Option<IndexMap<String, ColumnNullability>>> {
        let body = match cte.ctequery.as_ref().and_then(|n| n.node.as_ref()) {
            Some(node) => node,
            None => return Ok(None),
        };

        let cols = match body {
            protobuf::node::Node::SelectStmt(select) => {
                let cols = self.analyze_subselect(select)?;
                vec_to_indexmap(cols)
            }
            protobuf::node::Node::InsertStmt(insert) => self
                .returning_columns_from_target(&insert.relation, &insert.returning_list)?
                .unwrap_or_default(),
            protobuf::node::Node::UpdateStmt(update) => self
                .returning_columns_from_target(&update.relation, &update.returning_list)?
                .unwrap_or_default(),
            protobuf::node::Node::DeleteStmt(delete) => self
                .returning_columns_from_target(&delete.relation, &delete.returning_list)?
                .unwrap_or_default(),
            _ => return Ok(None),
        };

        if cols.is_empty() {
            Ok(None)
        } else {
            Ok(Some(cols))
        }
    }

    /// Build the column-nullability map produced by a DML statement's
    /// `RETURNING` list. Looks each plain column up in `rel_index` to see if
    /// the target table has a NOT NULL constraint on it. Expressions, casts,
    /// and anything else default to nullable.
    fn returning_columns_from_target(
        &self,
        relation: &Option<protobuf::RangeVar>,
        returning_list: &[protobuf::Node],
    ) -> anyhow::Result<Option<IndexMap<String, ColumnNullability>>> {
        if returning_list.is_empty() {
            return Ok(None);
        }
        let range_var = match relation {
            Some(rv) => rv,
            None => return Ok(None),
        };
        let schema = if range_var.schemaname.is_empty() {
            None
        } else {
            Some(range_var.schemaname.clone())
        };
        let rel = match self.find_relation(&schema, &range_var.relname) {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut result: IndexMap<String, ColumnNullability> = IndexMap::new();
        for target in returning_list {
            if let Some(protobuf::node::Node::ResTarget(res_target)) = &target.node {
                let val = match &res_target.val {
                    Some(v) => v,
                    None => continue,
                };

                // The output column name in RETURNING: explicit `AS alias`,
                // otherwise the referenced column's name (single-field
                // ColumnRef).
                let alias = if !res_target.name.is_empty() {
                    Some(res_target.name.clone())
                } else {
                    self.extract_column_name(val)
                };

                let cn = match &val.node {
                    Some(protobuf::node::Node::ColumnRef(col_ref)) => {
                        // `RETURNING table.col` or `RETURNING col` — both
                        // resolve to a column on `rel`.
                        let col_name = match col_ref.fields.last().and_then(|f| f.node.as_ref()) {
                            Some(protobuf::node::Node::String(s)) => Some(s.sval.clone()),
                            _ => None,
                        };
                        match col_name {
                            Some(name) => ColumnNullability {
                                nullable_due_to_join: false,
                                nullable_on_base: !self.is_column_not_null(rel, &name),
                            },
                            None => ColumnNullability {
                                nullable_due_to_join: false,
                                nullable_on_base: true,
                            },
                        }
                    }
                    _ => ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    },
                };

                if let Some(name) = alias {
                    result.insert(name, cn);
                }
            }
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
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
                    } else if let Some(cte) = self.cte_columns.get(&table_name) {
                        // The name resolves to a CTE in the enclosing scope —
                        // use its inferred column-nullability map.
                        self.alias_map.insert(
                            alias,
                            TableInfo::Derived {
                                nullability: cte.clone(),
                                is_nullable: false,
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
                protobuf::node::Node::RangeSubselect(range_sub) => {
                    // `(SELECT …) AS alias` in FROM. We recursively analyze
                    // the inner SELECT (with snapshot/restore via
                    // `analyze_subselect`) and register the result as a Derived
                    // alias. Lateral subqueries follow the same path; the
                    // surrounding LEFT JOIN sets `is_nullable` later.
                    let alias = range_sub
                        .alias
                        .as_ref()
                        .map(|a| a.aliasname.clone())
                        .unwrap_or_default();
                    if let Some(sub) = &range_sub.subquery {
                        if let Some(protobuf::node::Node::SelectStmt(inner)) = &sub.node {
                            let cols = self.analyze_subselect(inner)?;
                            self.alias_map.insert(
                                alias,
                                TableInfo::Derived {
                                    nullability: vec_to_indexmap(cols),
                                    is_nullable: false,
                                },
                            );
                        }
                    }
                }
                protobuf::node::Node::RangeFunction(range_fn) => {
                    // Set-returning functions like `unnest`, `generate_series`,
                    // `jsonb_to_recordset`. Inference is deliberately limited
                    // to a small allowlist; anything else stays all-nullable.
                    let columns = self.analyze_range_function(range_fn);
                    let alias = range_fn
                        .alias
                        .as_ref()
                        .map(|a| a.aliasname.clone())
                        .unwrap_or_default();
                    if !alias.is_empty() {
                        self.alias_map.insert(
                            alias,
                            TableInfo::Derived {
                                nullability: columns,
                                is_nullable: false,
                            },
                        );
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
                for (col_name, cn) in nullability {
                    // View column nullability is affected by join context
                    let final_not_null = cn.is_not_null() && !is_nullable;
                    log::info!("[GET_COLUMNS]   Column '{}': cached is_not_null={}, final is_not_null={}", col_name, cn.is_not_null(), final_not_null);
                    result.push((col_name.clone(), final_not_null));
                }
            }
            TableInfo::Derived {
                nullability,
                is_nullable,
            } => {
                for (col_name, cn) in nullability {
                    let final_not_null = cn.is_not_null() && !is_nullable;
                    result.push((col_name.clone(), final_not_null));
                }
            }
        }

        log::info!("[GET_COLUMNS] Returning {} columns", result.len());
        Ok(result)
    }

    /// Get two-level nullability for a table's columns.
    fn get_table_columns_detailed(
        &self,
        table_info: &TableInfo,
    ) -> anyhow::Result<Vec<(String, ColumnNullability)>> {
        let mut result = Vec::new();

        match table_info {
            TableInfo::Table {
                schema,
                name,
                is_nullable,
                ..
            } => {
                if let Some(rel) = self.find_relation(schema, name) {
                    for column in &rel.columns {
                        let base_not_null = self.is_column_not_null(rel, &column);
                        result.push((
                            column.to_string(),
                            ColumnNullability {
                                nullable_due_to_join: *is_nullable,
                                nullable_on_base: !base_not_null,
                            },
                        ));
                    }
                }
            }
            TableInfo::View {
                nullability,
                is_nullable,
                ..
            }
            | TableInfo::Derived {
                nullability,
                is_nullable,
            } => {
                for (col_name, cn) in nullability {
                    result.push((
                        col_name.clone(),
                        ColumnNullability {
                            // Compose: if the view ref itself is join-nullable, OR the
                            // cached column was already join-nullable
                            nullable_due_to_join: *is_nullable || cn.nullable_due_to_join,
                            nullable_on_base: cn.nullable_on_base,
                        },
                    ));
                }
            }
        }

        Ok(result)
    }

    /// Expand a star expression (SELECT * or SELECT t.*) returning detailed nullability.
    fn expand_star_expression_detailed(
        &self,
        expr: &protobuf::Node,
    ) -> anyhow::Result<Vec<(String, ColumnNullability)>> {
        let mut result = Vec::new();

        if let Some(protobuf::node::Node::ColumnRef(col_ref)) = &expr.node {
            match col_ref.fields.len() {
                1 => {
                    // SELECT * - expand all tables
                    for (_alias, table_info) in &self.alias_map {
                        let columns = self.get_table_columns_detailed(table_info)?;
                        result.extend(columns);
                    }
                }
                2 => {
                    // SELECT table.* - expand specific table
                    if let Some(protobuf::node::Node::String(table_ref)) =
                        &col_ref.fields[0].node
                    {
                        if let Some(table_info) = self.alias_map.get(&table_ref.sval) {
                            let columns = self.get_table_columns_detailed(table_info)?;
                            result.extend(columns);
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

        Ok(result)
    }

    /// Get two-level nullability for an expression.
    ///
    /// For column references, this distinguishes join-nullability from base-nullability.
    /// For computed expressions (CASE, COALESCE, function calls, etc.), join-nullability
    /// is always false and the overall nullability goes into `nullable_on_base`.
    fn expression_nullability_detailed(
        &self,
        expr: &protobuf::Node,
    ) -> anyhow::Result<ColumnNullability> {
        if let Some(node) = &expr.node {
            match node {
                protobuf::node::Node::ColumnRef(col_ref) => {
                    self.column_ref_nullability_detailed(col_ref)
                }
                protobuf::node::Node::TypeCast(type_cast) => {
                    // Type casts preserve the nullability of the argument
                    if let Some(arg) = &type_cast.arg {
                        self.expression_nullability_detailed(arg)
                    } else {
                        Ok(ColumnNullability {
                            nullable_due_to_join: false,
                            nullable_on_base: true,
                        })
                    }
                }
                _ => {
                    // For all other expression types, fall back to the existing
                    // is_expression_nullable and treat result as base-nullable.
                    let is_nullable = self.is_expression_nullable(expr)?;
                    Ok(ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: is_nullable,
                    })
                }
            }
        } else {
            Ok(ColumnNullability {
                nullable_due_to_join: false,
                nullable_on_base: true,
            })
        }
    }

    /// Get two-level nullability for a column reference.
    fn column_ref_nullability_detailed(
        &self,
        col_ref: &protobuf::ColumnRef,
    ) -> anyhow::Result<ColumnNullability> {
        let fields = &col_ref.fields;

        match fields.len() {
            1 => {
                if let Some(protobuf::node::Node::String(name)) = &fields[0].node {
                    // Check if it's a table alias (whole-row reference)
                    if let Some(table_info) = self.alias_map.get(&name.sval) {
                        match table_info {
                            TableInfo::Table { is_nullable, .. }
                            | TableInfo::View { is_nullable, .. }
                            | TableInfo::Derived { is_nullable, .. } => {
                                return Ok(ColumnNullability {
                                    nullable_due_to_join: *is_nullable,
                                    nullable_on_base: false,
                                });
                            }
                        }
                    }

                    // Single column name, try single-table case
                    if self.alias_map.len() == 1 {
                        if let Some((_, table_info)) = self.alias_map.iter().next() {
                            match table_info {
                                TableInfo::Table {
                                    schema,
                                    name: table_name,
                                    is_nullable,
                                    ..
                                } => {
                                    if let Some(rel) = self.find_relation(schema, table_name) {
                                        let base_not_null =
                                            self.is_column_not_null(rel, &name.sval);
                                        return Ok(ColumnNullability {
                                            nullable_due_to_join: *is_nullable,
                                            nullable_on_base: !base_not_null,
                                        });
                                    }
                                }
                                TableInfo::View {
                                    nullability,
                                    is_nullable,
                                    ..
                                }
                                | TableInfo::Derived {
                                    nullability,
                                    is_nullable,
                                } => {
                                    if let Some(cn) = nullability.get(&name.sval) {
                                        return Ok(ColumnNullability {
                                            nullable_due_to_join: *is_nullable
                                                || cn.nullable_due_to_join,
                                            nullable_on_base: cn.nullable_on_base,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // Conservative default
                    Ok(ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    })
                } else {
                    Ok(ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    })
                }
            }
            2 => {
                if let Some(protobuf::node::Node::String(table_ref)) = &fields[0].node {
                    if let Some(protobuf::node::Node::String(col_name)) = &fields[1].node {
                        // Normal table.column reference
                        if let Some(table_info) = self.alias_map.get(&table_ref.sval) {
                            match table_info {
                                TableInfo::Table {
                                    schema,
                                    name,
                                    is_nullable,
                                    ..
                                } => {
                                    if let Some(rel) = self.find_relation(schema, name) {
                                        let base_not_null =
                                            self.is_column_not_null(rel, &col_name.sval);
                                        return Ok(ColumnNullability {
                                            nullable_due_to_join: *is_nullable,
                                            nullable_on_base: !base_not_null,
                                        });
                                    }
                                }
                                TableInfo::View {
                                    nullability,
                                    is_nullable,
                                    ..
                                }
                                | TableInfo::Derived {
                                    nullability,
                                    is_nullable,
                                } => {
                                    if let Some(cn) = nullability.get(&col_name.sval) {
                                        return Ok(ColumnNullability {
                                            nullable_due_to_join: *is_nullable
                                                || cn.nullable_due_to_join,
                                            nullable_on_base: cn.nullable_on_base,
                                        });
                                    }
                                }
                            }
                        }
                        // Not found — conservative
                        Ok(ColumnNullability {
                            nullable_due_to_join: false,
                            nullable_on_base: true,
                        })
                    } else {
                        // Whole-row reference (table.*)
                        if let Some(table_info) = self.alias_map.get(&table_ref.sval) {
                            match table_info {
                                TableInfo::Table { is_nullable, .. }
                                | TableInfo::View { is_nullable, .. }
                                | TableInfo::Derived { is_nullable, .. } => {
                                    return Ok(ColumnNullability {
                                        nullable_due_to_join: *is_nullable,
                                        nullable_on_base: false,
                                    });
                                }
                            }
                        }
                        Ok(ColumnNullability {
                            nullable_due_to_join: false,
                            nullable_on_base: true,
                        })
                    }
                } else {
                    Ok(ColumnNullability {
                        nullable_due_to_join: false,
                        nullable_on_base: true,
                    })
                }
            }
            _ => Ok(ColumnNullability {
                nullable_due_to_join: false,
                nullable_on_base: true,
            }),
        }
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
                            | TableInfo::View { is_nullable, .. }
                            | TableInfo::Derived { is_nullable, .. } => {
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
                                TableInfo::View { nullability, .. }
                                | TableInfo::Derived { nullability, .. } => {
                                    if let Some(cn) = nullability.get(&name.sval) {
                                        return Ok(cn.is_nullable());
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
                                }
                                | TableInfo::Derived {
                                    nullability,
                                    is_nullable,
                                } => {
                                    // If view is nullable due to outer join, column is nullable
                                    if *is_nullable {
                                        return Ok(true);
                                    }

                                    // Look up column nullability from cached view results
                                    if let Some(cn) = nullability.get(&col_name.sval) {
                                        return Ok(cn.is_nullable());
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
                                | TableInfo::View { is_nullable, .. }
                                | TableInfo::Derived { is_nullable, .. } => {
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

                    // Window functions in the ranking/numbering family — these
                    // are documented NOT NULL by Postgres regardless of the
                    // PARTITION BY / ORDER BY contents.
                    //   * `row_number()` — sequential 1, 2, 3, …
                    //   * `rank()`, `dense_rank()` — integer ranks
                    //   * `percent_rank()`, `cume_dist()` — `double precision`
                    //   * `ntile(n)` — bucket index
                    // (We don't list `lag`/`lead`/`first_value`/`last_value`/
                    // `nth_value` here: those return whatever the underlying
                    // expression yields, which can be NULL at the partition
                    // edges or if the source value is NULL.)
                    "row_number"
                    | "rank"
                    | "dense_rank"
                    | "percent_rank"
                    | "cume_dist"
                    | "ntile" => Ok(false),

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
                    TableInfo::Table { is_nullable, .. }
                    | TableInfo::View { is_nullable, .. }
                    | TableInfo::Derived { is_nullable, .. } => {
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
                protobuf::node::Node::RangeSubselect(range_sub) => {
                    if let Some(alias) = &range_sub.alias {
                        aliases.insert(alias.aliasname.clone());
                    }
                }
                protobuf::node::Node::RangeFunction(range_fn) => {
                    if let Some(alias) = &range_fn.alias {
                        aliases.insert(alias.aliasname.clone());
                    }
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

    /// Helper: assert a column is NOT NULL (neither join-nullable nor base-nullable)
    fn assert_not_null(result: &IndexMap<String, ColumnNullability>, col: &str) {
        let cn = result.get(col).unwrap_or_else(|| panic!("column '{}' not found", col));
        assert!(
            cn.is_not_null(),
            "expected '{}' to be NOT NULL, got {:?}",
            col,
            cn
        );
    }

    /// Helper: assert a column is nullable (for any reason)
    fn assert_nullable(result: &IndexMap<String, ColumnNullability>, col: &str) {
        let cn = result.get(col).unwrap_or_else(|| panic!("column '{}' not found", col));
        assert!(
            cn.is_nullable(),
            "expected '{}' to be nullable, got {:?}",
            col,
            cn
        );
    }

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
        assert_not_null(&result, "id");
        assert_nullable(&result, "name");
        assert_not_null(&result, "email");
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

        // u.id should be NOT NULL, u.name nullable on base, p.title nullable due to LEFT JOIN
        assert_not_null(&result, "id");
        assert_nullable(&result, "name");
        let title_cn = result.get("title").unwrap();
        assert!(title_cn.nullable_due_to_join, "title should be join-nullable");
    }

    #[test]
    fn test_left_join_two_level_nullability() {
        let rel_index = create_test_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);

        // posts.id is NOT NULL, posts.title is nullable
        let view_def = "SELECT u.id, p.id as post_id, p.title
                        FROM users u
                        LEFT JOIN posts p ON u.id = p.user_id";
        let columns = vec![
            "id".to_string(),
            "post_id".to_string(),
            "title".to_string(),
        ];

        let result = analyzer.analyze_view(view_def, &columns).unwrap();

        // u.id: NOT NULL
        assert_not_null(&result, "id");

        // p.id (as post_id): nullable_due_to_join=true, nullable_on_base=false
        let post_id = result.get("post_id").unwrap();
        assert!(post_id.nullable_due_to_join);
        assert!(!post_id.nullable_on_base);

        // p.title: nullable_due_to_join=true, nullable_on_base=true
        let title = result.get("title").unwrap();
        assert!(title.nullable_due_to_join);
        assert!(title.nullable_on_base);
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
        assert_not_null(&result, "total");
        assert_nullable(&result, "max_id");
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
        assert_not_null(&result, "one");
        assert_not_null(&result, "greeting");
        assert_not_null(&result, "id");
    }
}
