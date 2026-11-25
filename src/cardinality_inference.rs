use crate::call_graph::CallTarget;
use crate::fn_index::FunctionIndex;
use crate::pg_constraint::Constraint;
use crate::pg_rel::PgRel;
use crate::rel_index::RelIndex;
use crate::sql_parser::QueryType;
use anyhow::{anyhow, Result};
use pg_query::protobuf as pb;
use pg_query::protobuf::node::Node as N;
use std::collections::{HashMap, HashSet};

/// Analyzes SQL queries to infer their cardinality (one, opt, many, exec, execrows).
///
/// The inference is deliberately one-sided: `One` and `Opt` are only reported
/// when the row count is provable from the statement and the catalog. Anything
/// the analyzer does not understand is `Many`.
pub struct CardinalityAnalyzer<'a> {
    rel_index: &'a RelIndex,
    /// When given, user-defined functions in a target list are checked for
    /// `RETURNS SETOF`, which multiplies rows.
    fn_index: Option<&'a FunctionIndex>,
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
    /// INSERT ... ON CONFLICT DO UPDATE ... WHERE ... RETURNING: the conflicting
    /// row is only updated (and returned) when the WHERE holds
    ConditionalUpsertReturning,
    /// INSERT ... ON CONFLICT DO NOTHING ... RETURNING
    InsertOnConflictDoNothingReturning,
    /// SELECT with LIMIT 1
    SelectLimit1,
    /// Aggregate function without GROUP BY
    AggregateWithoutGroupBy,
    /// SELECT without a FROM clause and without set-returning functions
    SelectWithoutFrom,
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

/// A table reference as written in the query.
#[derive(Debug, Clone)]
struct TableRef {
    /// Schema qualifier, when the query wrote one
    schema: Option<String>,
    /// The table name as written
    name: String,
    /// The alias used in the query (if any)
    alias: Option<String>,
}

impl TableRef {
    fn from_range_var(range_var: &pb::RangeVar) -> Self {
        Self {
            schema: (!range_var.schemaname.is_empty()).then(|| range_var.schemaname.clone()),
            name: range_var.relname.clone(),
            alias: range_var.alias.as_ref().map(|a| a.aliasname.clone()),
        }
    }

    /// The name columns of this table are qualified with (alias if present, otherwise name)
    fn query_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// `a.x = b.y` in a join condition: (qualifier, column) on each side.
type JoinEquality = ((String, String), (String, String));

/// Information about a JOIN in the query
#[derive(Debug, Clone)]
struct JoinInfo {
    /// The table being joined to
    table: TableRef,
    /// `a.x = b.y` pairs from the AND-ed join condition, as (qualifier, column)
    /// on each side. Unqualified columns are not recorded.
    equalities: Vec<JoinEquality>,
}

/// Extracted FROM clause information including base table and joins
#[derive(Debug)]
struct FromClauseInfo {
    /// The base (leftmost) table
    base_table: TableRef,
    /// All joins in order
    joins: Vec<JoinInfo>,
}

impl FromClauseInfo {
    /// The table a column qualifier refers to.
    fn table_for_qualifier(&self, qualifier: &str) -> Option<&TableRef> {
        std::iter::once(&self.base_table)
            .chain(self.joins.iter().map(|j| &j.table))
            .find(|t| t.query_name() == qualifier)
    }
}

/// Columns compared for equality against a value, keyed by the column's
/// qualifier as written (`""` when unqualified). Each column carries the
/// values it is compared to, in a canonical form, so that two branches of an
/// OR can be checked for comparing against the *same* value.
type EqualityMap = HashMap<String, HashMap<String, HashSet<String>>>;

/// What a scan of the target list found.
#[derive(Debug, Default)]
struct TargetScan {
    /// A plain (non-window) aggregate call
    aggregate: bool,
    /// A function known to return a set
    set_returning: bool,
    /// A function or expression that could not be proven single-valued
    unproven: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FunctionKind {
    SetReturning,
    Scalar,
    Unknown,
}

/// Built-in aggregates. A call to one of these without `OVER` collapses the
/// query to a single group.
const AGGREGATES: &[&str] = &[
    "count", "sum", "avg", "min", "max", "array_agg", "string_agg", "bool_and", "bool_or",
    "every", "bit_and", "bit_or", "bit_xor", "json_agg", "jsonb_agg", "json_agg_strict",
    "jsonb_agg_strict", "json_object_agg", "jsonb_object_agg", "json_object_agg_strict",
    "jsonb_object_agg_strict", "json_object_agg_unique", "jsonb_object_agg_unique", "xmlagg",
    "any_value", "range_agg", "range_intersect_agg", "stddev", "stddev_pop", "stddev_samp",
    "variance", "var_pop", "var_samp", "corr", "covar_pop", "covar_samp",
];

/// Built-in set-returning functions. In a target list these multiply rows.
const SET_RETURNING: &[&str] = &[
    "unnest", "generate_series", "generate_subscripts", "regexp_matches",
    "regexp_split_to_table", "string_to_table", "json_array_elements", "jsonb_array_elements",
    "json_array_elements_text", "jsonb_array_elements_text", "json_each", "jsonb_each",
    "json_each_text", "jsonb_each_text", "json_object_keys", "jsonb_object_keys",
    "json_populate_recordset", "jsonb_populate_recordset", "json_to_recordset",
    "jsonb_to_recordset", "jsonb_path_query", "ts_stat", "ts_parse", "ts_token_type",
    "ts_debug", "aclexplode", "xpath_table", "pg_listening_channels",
];

/// Built-in functions known to return exactly one value. Only consulted where
/// a function must be *proven* single-valued (a SELECT without FROM).
const SCALARS: &[&str] = &[
    "now", "clock_timestamp", "statement_timestamp", "transaction_timestamp", "timeofday",
    "gen_random_uuid", "random", "version", "current_database", "current_schema",
    "current_setting", "set_config", "nextval", "currval", "setval", "lastval", "txid_current",
    "pg_advisory_lock", "pg_advisory_xact_lock", "pg_try_advisory_lock",
    "pg_try_advisory_xact_lock", "pg_advisory_unlock", "pg_notify", "lower", "upper", "length",
    "char_length", "concat", "concat_ws", "format", "md5", "abs", "round", "ceil", "floor",
    "trim", "btrim", "ltrim", "rtrim", "replace", "to_char", "to_date", "to_timestamp",
    "to_number", "date_trunc", "date_part", "age", "to_json", "to_jsonb", "row_to_json",
    "json_build_object", "jsonb_build_object", "json_build_array", "jsonb_build_array",
    "array_length", "cardinality", "array_append", "array_cat", "array_position",
    "pg_typeof", "nullif",
];

fn low(reason: InferenceReason) -> CardinalityInference {
    CardinalityInference {
        cardinality: InferredCardinality::Many,
        confidence: InferenceConfidence::Low,
        reason,
    }
}

fn high(cardinality: InferredCardinality, reason: InferenceReason) -> CardinalityInference {
    CardinalityInference {
        cardinality,
        confidence: InferenceConfidence::High,
        reason,
    }
}

/// `col = value` and nothing else: not `= ANY (...)`, `NULLIF`, `IS DISTINCT
/// FROM` or another operator whose name merely contains `=`.
fn is_plain_equality(a_expr: &pb::AExpr) -> bool {
    if a_expr.kind() != pb::AExprKind::AexprOp {
        return false;
    }
    let names: Vec<&str> = a_expr
        .name
        .iter()
        .filter_map(|n| match &n.node {
            Some(N::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect();
    matches!(names.as_slice(), ["="] | ["pg_catalog", "="])
}

/// The string parts of a name list (`funcname`, `ColumnRef.fields`, type names).
/// `None` if any part is not a plain string (e.g. `*`).
fn name_parts(nodes: &[pb::Node]) -> Option<Vec<&str>> {
    nodes
        .iter()
        .map(|n| match &n.node {
            Some(N::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect()
}

/// A column reference as (qualifier, column); the qualifier is `""` when the
/// column is unqualified. Three-part names (`schema.table.col`) are not handled.
fn column_ref(node: &pb::Node) -> Option<(String, String)> {
    let Some(N::ColumnRef(col_ref)) = &node.node else {
        return None;
    };
    match name_parts(&col_ref.fields)?.as_slice() {
        [column] => Some((String::new(), (*column).to_string())),
        [qualifier, column] => Some(((*qualifier).to_string(), (*column).to_string())),
        _ => None,
    }
}

/// A canonical spelling for a parameter or literal (optionally cast), used to
/// tell whether two comparisons are against the same value. `None` for
/// anything else, including a NULL literal (`col = NULL` matches nothing).
fn value_key(node: &pb::Node) -> Option<String> {
    use pb::a_const::Val;
    match node.node.as_ref()? {
        N::ParamRef(p) => Some(format!("${}", p.number)),
        N::AConst(c) => match c.val.as_ref()? {
            Val::Ival(v) => Some(format!("i:{}", v.ival)),
            Val::Fval(v) => Some(format!("f:{}", v.fval)),
            Val::Boolval(v) => Some(format!("b:{}", v.boolval)),
            Val::Sval(v) => Some(format!("s:{:?}", v.sval)),
            Val::Bsval(v) => Some(format!("x:{:?}", v.bsval)),
        },
        N::TypeCast(tc) => {
            let inner = value_key(tc.arg.as_deref()?)?;
            let type_name = tc.type_name.as_ref()?;
            let name = name_parts(&type_name.names)?.join(".");
            let typmods = type_name
                .typmods
                .iter()
                .map(value_key)
                .collect::<Option<Vec<_>>>()?
                .join(",");
            Some(format!(
                "({inner})::{name}({typmods})[{}]",
                type_name.array_bounds.len()
            ))
        }
        _ => None,
    }
}

/// Direct sub-expressions of an expression node. A `SubLink`'s subquery is not
/// entered: it is a query of its own and yields a single value to this one.
/// `None` for node kinds this walker does not know.
fn expr_children(node: &pb::Node) -> Option<Vec<&pb::Node>> {
    let mut out: Vec<&pb::Node> = Vec::new();
    match node.node.as_ref()? {
        N::ColumnRef(_)
        | N::ParamRef(_)
        | N::AConst(_)
        | N::AStar(_)
        | N::String(_)
        | N::Integer(_)
        | N::Float(_)
        | N::SqlvalueFunction(_)
        | N::SetToDefault(_) => {}
        N::ResTarget(t) => {
            out.extend(t.val.as_deref());
            out.extend(&t.indirection);
        }
        N::FuncCall(f) => {
            out.extend(&f.args);
            out.extend(&f.agg_order);
            out.extend(f.agg_filter.as_deref());
            if let Some(over) = &f.over {
                out.extend(&over.partition_clause);
                out.extend(&over.order_clause);
                out.extend(over.start_offset.as_deref());
                out.extend(over.end_offset.as_deref());
            }
        }
        N::TypeCast(t) => out.extend(t.arg.as_deref()),
        N::CollateClause(c) => out.extend(c.arg.as_deref()),
        N::NamedArgExpr(a) => out.extend(a.arg.as_deref()),
        N::AExpr(a) => {
            out.extend(a.lexpr.as_deref());
            out.extend(a.rexpr.as_deref());
        }
        N::BoolExpr(b) => out.extend(&b.args),
        N::NullTest(t) => out.extend(t.arg.as_deref()),
        N::BooleanTest(t) => out.extend(t.arg.as_deref()),
        N::CoalesceExpr(c) => out.extend(&c.args),
        N::MinMaxExpr(m) => out.extend(&m.args),
        N::CaseExpr(c) => {
            out.extend(c.arg.as_deref());
            out.extend(&c.args);
            out.extend(c.defresult.as_deref());
        }
        N::CaseWhen(w) => {
            out.extend(w.expr.as_deref());
            out.extend(w.result.as_deref());
        }
        N::AIndirection(i) => {
            out.extend(i.arg.as_deref());
            out.extend(&i.indirection);
        }
        N::AIndices(i) => {
            out.extend(i.lidx.as_deref());
            out.extend(i.uidx.as_deref());
        }
        N::AArrayExpr(a) => out.extend(&a.elements),
        N::RowExpr(r) => out.extend(&r.args),
        N::List(l) => out.extend(&l.items),
        N::SortBy(s) => out.extend(s.node.as_deref()),
        N::SubLink(s) => out.extend(s.testexpr.as_deref()),
        _ => return None,
    }
    Some(out)
}

impl<'a> CardinalityAnalyzer<'a> {
    pub fn new(rel_index: &'a RelIndex) -> Self {
        Self {
            rel_index,
            fn_index: None,
        }
    }

    /// Consult the function index for `RETURNS SETOF` when a target list calls
    /// a user-defined function.
    pub(crate) fn with_fn_index(mut self, fn_index: Option<&'a FunctionIndex>) -> Self {
        self.fn_index = fn_index;
        self
    }

    /// Infer the cardinality of a SQL query.
    pub fn infer(&self, sql: &str) -> Result<CardinalityInference> {
        let parse_result =
            pg_query::parse(sql).map_err(|e| anyhow!("Failed to parse SQL: {}", e))?;
        self.infer_parsed(&parse_result.protobuf)
    }

    /// Infer the cardinality of an already-parsed query.
    pub fn infer_parsed(&self, parse_result: &pb::ParseResult) -> Result<CardinalityInference> {
        let stmt = parse_result
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .ok_or_else(|| anyhow!("No statement found in SQL"))?;

        Ok(match &stmt.node {
            Some(N::SelectStmt(select)) => self.analyze_select(select),
            Some(N::InsertStmt(insert)) => self.analyze_insert(insert),
            Some(N::UpdateStmt(update)) => self.analyze_dml_returning(
                update.relation.as_ref(),
                update.where_clause.as_deref(),
                !update.from_clause.is_empty(),
                &update.returning_list,
            ),
            Some(N::DeleteStmt(delete)) => self.analyze_dml_returning(
                delete.relation.as_ref(),
                delete.where_clause.as_deref(),
                !delete.using_clause.is_empty(),
                &delete.returning_list,
            ),
            _ => low(InferenceReason::ComplexQuery),
        })
    }

    /// Analyze a SELECT statement.
    fn analyze_select(&self, select: &pb::SelectStmt) -> CardinalityInference {
        // UNION/INTERSECT/EXCEPT and bare VALUES lists are not analyzed.
        if select.op() != pb::SetOperation::SetopNone || !select.values_lists.is_empty() {
            return low(InferenceReason::ComplexQuery);
        }

        // LIMIT 1 caps the result at one row -- unless it is `FETCH FIRST 1
        // ROW WITH TIES`, which returns every row tied with the first.
        if select.limit_option() != pb::LimitOption::WithTies && Self::is_limit_one(select) {
            return high(InferredCardinality::Opt, InferenceReason::SelectLimit1);
        }

        let targets = self.scan_targets(&select.target_list);

        // LIMIT n / OFFSET n can remove the single row a query would return.
        let exactly_one = if select.limit_count.is_none() && select.limit_offset.is_none() {
            InferredCardinality::One
        } else {
            InferredCardinality::Opt
        };

        // Without GROUP BY, an aggregate (or a HAVING clause) makes the whole
        // input one group. A window function (`count(*) OVER ()`) does not.
        // HAVING can discard that one group; a set-returning function in the
        // target list can multiply it.
        if select.group_clause.is_empty() && !targets.set_returning {
            if select.having_clause.is_some() {
                return high(InferredCardinality::Opt, InferenceReason::AggregateWithoutGroupBy);
            }
            if targets.aggregate {
                return high(exactly_one, InferenceReason::AggregateWithoutGroupBy);
            }
        }

        // No FROM: one row, as long as nothing in the target list returns a set.
        // A WHERE clause can still discard it.
        if select.from_clause.is_empty()
            && select.group_clause.is_empty()
            && select.having_clause.is_none()
            && !select.target_list.is_empty()
            && !targets.set_returning
            && !targets.unproven
        {
            let cardinality = if select.where_clause.is_some() {
                InferredCardinality::Opt
            } else {
                exactly_one
            };
            return high(cardinality, InferenceReason::SelectWithoutFrom);
        }

        // A unique-key match bounds the rows read from the table, not the rows
        // returned: a set-returning function or GROUPING SETS can emit several
        // rows for the one that matched.
        let has_grouping_sets = select
            .group_clause
            .iter()
            .any(|g| matches!(g.node, Some(N::GroupingSet(_))));
        if !targets.set_returning && !has_grouping_sets {
            if let Some(inference) = self.analyze_where_for_uniqueness(select) {
                return inference;
            }
        }

        low(InferenceReason::DefaultSelect)
    }

    /// `LIMIT 1` / `FETCH FIRST 1 ROW ONLY` with a literal count.
    fn is_limit_one(select: &pb::SelectStmt) -> bool {
        let Some(limit_count) = &select.limit_count else {
            return false;
        };
        match &limit_count.node {
            Some(N::AConst(a_const)) => {
                matches!(&a_const.val, Some(pb::a_const::Val::Ival(i)) if i.ival == 1)
            }
            _ => false,
        }
    }

    /// Look through the target list for aggregates and set-returning functions.
    fn scan_targets(&self, target_list: &[pb::Node]) -> TargetScan {
        let mut scan = TargetScan::default();
        for target in target_list {
            self.scan_expr(target, &mut scan);
        }
        scan
    }

    fn scan_expr(&self, node: &pb::Node, scan: &mut TargetScan) {
        if let Some(N::FuncCall(func_call)) = &node.node {
            self.scan_func_call(func_call, scan);
        }
        match expr_children(node) {
            Some(children) => {
                for child in children {
                    self.scan_expr(child, scan);
                }
            }
            None => {
                // A node kind the walker does not know: nothing in it can be
                // proven single-valued. Still look for known set-returning
                // functions anywhere beneath it.
                scan.unproven = true;
                if let Some(inner) = &node.node {
                    for (descendant, _, _, _) in inner.nodes() {
                        if let pg_query::NodeRef::FuncCall(func_call) = descendant {
                            if func_call.over.is_none()
                                && self.classify_function(func_call) == FunctionKind::SetReturning
                            {
                                scan.set_returning = true;
                            }
                        }
                    }
                }
            }
        }
    }

    fn scan_func_call(&self, func_call: &pb::FuncCall, scan: &mut TargetScan) {
        // Window functions neither collapse nor multiply rows.
        if func_call.over.is_some() {
            return;
        }
        if Self::is_aggregate_call(func_call) {
            scan.aggregate = true;
            return;
        }
        match self.classify_function(func_call) {
            FunctionKind::SetReturning => scan.set_returning = true,
            FunctionKind::Unknown => scan.unproven = true,
            FunctionKind::Scalar => {}
        }
    }

    /// Whether a (non-window) call is an aggregate: a built-in aggregate by
    /// name, or any call using aggregate-only syntax.
    fn is_aggregate_call(func_call: &pb::FuncCall) -> bool {
        if func_call.agg_star
            || func_call.agg_distinct
            || func_call.agg_within_group
            || func_call.agg_filter.is_some()
            || !func_call.agg_order.is_empty()
        {
            return true;
        }
        match name_parts(&func_call.funcname).as_deref() {
            Some([name]) | Some(["pg_catalog", name]) => AGGREGATES.contains(name),
            _ => false,
        }
    }

    fn classify_function(&self, func_call: &pb::FuncCall) -> FunctionKind {
        let (schema, name) = match name_parts(&func_call.funcname).as_deref() {
            Some([name]) => (None, *name),
            Some([schema, name]) => (Some(*schema), *name),
            _ => return FunctionKind::Unknown,
        };
        let builtin = matches!(schema, None | Some("pg_catalog"));

        if builtin && SET_RETURNING.contains(&name) {
            return FunctionKind::SetReturning;
        }

        if let Some(fn_index) = self.fn_index {
            let target = CallTarget {
                schema: schema.map(str::to_string),
                name: name.to_string(),
            };
            if let Some(resolved) = fn_index.resolve(&target, None) {
                // Overloads share a name; any of them returning a set counts.
                let returns_set = fn_index.values().any(|f| {
                    f.schema == resolved.schema && f.name == resolved.name && f.returns_set
                });
                return if returns_set {
                    FunctionKind::SetReturning
                } else {
                    FunctionKind::Scalar
                };
            }
        }

        if builtin && SCALARS.contains(&name) {
            return FunctionKind::Scalar;
        }
        FunctionKind::Unknown
    }

    /// Analyze WHERE clause for unique constraint matches, including through joins.
    fn analyze_where_for_uniqueness(
        &self,
        select: &pb::SelectStmt,
    ) -> Option<CardinalityInference> {
        let from_info = Self::extract_from_clause_info(select)?;

        // A CTE shadows a table of the same name.
        let cte_names: HashSet<&str> = select
            .with_clause
            .iter()
            .flat_map(|w| &w.ctes)
            .filter_map(|cte| match &cte.node {
                Some(N::CommonTableExpr(cte)) => Some(cte.ctename.as_str()),
                _ => None,
            })
            .collect();
        let find_rel = |table: &TableRef| {
            if table.schema.is_none() && cte_names.contains(table.name.as_str()) {
                return None;
            }
            self.find_table_rel(table)
        };

        let base_table_rel = find_rel(&from_info.base_table)?;
        let where_clause = select.where_clause.as_deref()?;

        let equalities = Self::collect_equalities(where_clause);
        let base_equality_columns =
            Self::columns_for(&equalities, &["", from_info.base_table.query_name()]);
        if base_equality_columns.is_empty() {
            return None;
        }

        let matched_constraint =
            Self::find_matching_uniqueness_constraint(base_table_rel, &base_equality_columns)?;

        if from_info.joins.is_empty() {
            return Some(Self::create_uniqueness_inference(
                &from_info.base_table.name,
                matched_constraint,
            ));
        }

        // Every join must be "to-one" (joining to unique/primary key columns),
        // otherwise it could produce several rows for the one base row.
        let mut join_descriptions = Vec::new();
        for join in &from_info.joins {
            let joined_rel = find_rel(&join.table)?;
            if !self.is_to_one_join(join, joined_rel, &from_info, &find_rel) {
                return None;
            }
            join_descriptions.push(format!(
                "{} -> {}",
                join.table.query_name(),
                join.table.name
            ));
        }

        let mut columns: Vec<String> = base_equality_columns.into_iter().collect();
        columns.sort();
        Some(CardinalityInference {
            cardinality: InferredCardinality::Opt,
            confidence: InferenceConfidence::Medium,
            reason: InferenceReason::UniqueMatchWithJoins {
                base_table: from_info.base_table.name.clone(),
                columns,
                joins: join_descriptions,
            },
        })
    }

    /// The primary key or unique constraint whose columns are all pinned by
    /// `equality_columns`, if any. Shared by SELECT, UPDATE and DELETE.
    fn find_matching_uniqueness_constraint(
        table_rel: &PgRel,
        equality_columns: &HashSet<String>,
    ) -> Option<(MatchedConstraint, Vec<String>)> {
        let covered = |columns: &[ustr::Ustr]| {
            !columns.is_empty() && columns.iter().all(|c| equality_columns.contains(c.as_str()))
        };
        let names = |columns: &[ustr::Ustr]| columns.iter().map(|c| c.to_string()).collect();

        for constraint in &table_rel.constraints {
            match constraint {
                Constraint::PrimaryKey(pk) if covered(&pk.columns) => {
                    return Some((MatchedConstraint::PrimaryKey, names(&pk.columns)));
                }
                // A partial / expression unique index does not make its
                // plain columns unique on their own.
                Constraint::Unique(unique) if !unique.conditional && covered(&unique.columns) => {
                    return Some((
                        MatchedConstraint::Unique {
                            name: unique.name.to_string(),
                        },
                        names(&unique.columns),
                    ));
                }
                _ => {}
            }
        }
        None
    }

    /// Create a uniqueness inference result
    fn create_uniqueness_inference(
        table_name: &str,
        (matched, columns): (MatchedConstraint, Vec<String>),
    ) -> CardinalityInference {
        let reason = match matched {
            MatchedConstraint::PrimaryKey => InferenceReason::PrimaryKeyMatch {
                table: table_name.to_string(),
                columns,
            },
            MatchedConstraint::Unique { name } => InferenceReason::UniqueConstraintMatch {
                table: table_name.to_string(),
                constraint_name: name,
                columns,
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
    fn is_to_one_join<'r>(
        &self,
        join: &JoinInfo,
        joined_rel: &PgRel,
        from_info: &FromClauseInfo,
        find_rel: &dyn Fn(&TableRef) -> Option<&'r PgRel>,
    ) -> bool {
        let joined = join.table.query_name();
        let mut joined_columns: HashSet<String> = HashSet::new();
        let mut source_columns: Vec<(&str, &str)> = Vec::new(); // (qualifier, column)

        // Pair each joined-table column with the column of *another* table it
        // is equated to. `c.id = c.other` says nothing about the join.
        for ((left_table, left_col), (right_table, right_col)) in &join.equalities {
            if right_table == joined && left_table != joined {
                joined_columns.insert(right_col.clone());
                source_columns.push((left_table, left_col));
            } else if left_table == joined && right_table != joined {
                joined_columns.insert(left_col.clone());
                source_columns.push((right_table, right_col));
            }
        }

        if joined_columns.is_empty() {
            return false;
        }

        // Method 1: the joined columns form a unique key on the joined table
        if Self::find_matching_uniqueness_constraint(joined_rel, &joined_columns).is_some() {
            return true;
        }

        // Method 2: the join follows a foreign key from a source table to the
        // joined table (whose target must be unique).
        let mut columns_by_source: HashMap<&str, Vec<&str>> = HashMap::new();
        for (qualifier, column) in source_columns {
            columns_by_source.entry(qualifier).or_default().push(column);
        }
        columns_by_source.iter().any(|(qualifier, source_cols)| {
            from_info
                .table_for_qualifier(qualifier)
                .and_then(find_rel)
                .is_some_and(|source_rel| {
                    Self::has_matching_fk(source_rel, source_cols, joined_rel, &joined_columns)
                })
        })
    }

    /// Check if a table has a foreign key that matches the join condition.
    ///
    /// Returns true if there's an FK where:
    /// - FK columns match source_cols
    /// - FK references the joined table
    /// - FK referenced columns match target_cols
    fn has_matching_fk(
        source_rel: &PgRel,
        source_cols: &[&str],
        target_rel: &PgRel,
        target_cols: &HashSet<String>,
    ) -> bool {
        let source_cols_set: HashSet<&str> = source_cols.iter().copied().collect();

        source_rel.constraints.iter().any(|constraint| {
            let Constraint::ForeignKey(fk) = constraint else {
                return false;
            };
            // Prefer the referenced table's OID; the name alone is ambiguous
            // across schemas.
            let references_target = match fk.ref_table_oid {
                Some(oid) => oid == target_rel.oid,
                None => fk.ref_table.as_deref() == Some(target_rel.id.name()),
            };
            if !references_target {
                return false;
            }
            let fk_cols: HashSet<&str> = fk.columns.iter().map(|c| c.as_str()).collect();
            if fk_cols != source_cols_set {
                return false;
            }
            match &fk.ref_columns {
                Some(cols) => {
                    let fk_ref_cols: HashSet<String> = cols.iter().map(|c| c.to_string()).collect();
                    &fk_ref_cols == target_cols
                }
                None => false,
            }
        })
    }

    /// Find the relation a table reference names: exactly when it is
    /// schema-qualified, and otherwise through the search path
    /// (see [`RelIndex::resolve`]).
    fn find_table_rel(&self, table: &TableRef) -> Option<&'a PgRel> {
        self.rel_index.resolve(table.schema.as_deref(), &table.name)
    }

    /// Extract FROM clause information including base table and all joins
    fn extract_from_clause_info(select: &pb::SelectStmt) -> Option<FromClauseInfo> {
        let [from_node] = select.from_clause.as_slice() else {
            return None;
        };
        let (base_table, joins) = Self::parse_join_arg(from_node)?;
        Some(FromClauseInfo { base_table, joins })
    }

    /// Parse the left side of a join (or the sole FROM item): a table, or
    /// another join.
    fn parse_join_arg(node: &pb::Node) -> Option<(TableRef, Vec<JoinInfo>)> {
        match &node.node {
            Some(N::RangeVar(range_var)) => Some((TableRef::from_range_var(range_var), Vec::new())),
            Some(N::JoinExpr(join_expr)) => {
                let (base_table, mut joins) = Self::parse_join_arg(join_expr.larg.as_deref()?)?;

                // The right side must be a plain table.
                let Some(N::RangeVar(right)) = &join_expr.rarg.as_deref()?.node else {
                    return None;
                };

                let mut equalities = Vec::new();
                if let Some(quals) = &join_expr.quals {
                    Self::collect_join_equalities(quals, &mut equalities);
                }
                joins.push(JoinInfo {
                    table: TableRef::from_range_var(right),
                    equalities,
                });
                Some((base_table, joins))
            }
            _ => None,
        }
    }

    /// Collect `a.x = b.y` pairs from an AND-ed join condition. Only qualified
    /// columns are recorded: an unqualified one cannot be attributed to either
    /// side without resolving it.
    fn collect_join_equalities(
        node: &pb::Node,
        equalities: &mut Vec<JoinEquality>,
    ) {
        match &node.node {
            Some(N::BoolExpr(bool_expr)) if bool_expr.boolop() == pb::BoolExprType::AndExpr => {
                for arg in &bool_expr.args {
                    Self::collect_join_equalities(arg, equalities);
                }
            }
            Some(N::AExpr(a_expr)) if is_plain_equality(a_expr) => {
                if let (Some(left), Some(right)) = (
                    a_expr.lexpr.as_deref().and_then(column_ref),
                    a_expr.rexpr.as_deref().and_then(column_ref),
                ) {
                    if !left.0.is_empty() && !right.0.is_empty() {
                        equalities.push((left, right));
                    }
                }
            }
            _ => {}
        }
    }

    /// Collect the columns a WHERE clause pins to a value.
    ///
    /// - `a AND b`: everything either side pins.
    /// - `a OR b`: only what *every* branch pins **to the same value**.
    ///   `id = $1 OR id = $2` pins nothing -- it can match two rows.
    /// - anything else (NOT, other operators, subqueries): nothing.
    fn collect_equalities(node: &pb::Node) -> EqualityMap {
        let mut result = EqualityMap::new();
        match &node.node {
            Some(N::BoolExpr(bool_expr)) => match bool_expr.boolop() {
                pb::BoolExprType::AndExpr => {
                    for arg in &bool_expr.args {
                        for (table, columns) in Self::collect_equalities(arg) {
                            let entry = result.entry(table).or_default();
                            for (column, values) in columns {
                                entry.entry(column).or_default().extend(values);
                            }
                        }
                    }
                }
                pb::BoolExprType::OrExpr => {
                    let mut branches = bool_expr.args.iter().map(Self::collect_equalities);
                    result = branches.next().unwrap_or_default();
                    for branch in branches {
                        result = result
                            .into_iter()
                            .filter_map(|(table, columns)| {
                                let other = branch.get(&table)?;
                                let common: HashMap<String, HashSet<String>> = columns
                                    .into_iter()
                                    .filter_map(|(column, values)| {
                                        let shared: HashSet<String> = values
                                            .intersection(other.get(&column)?)
                                            .cloned()
                                            .collect();
                                        (!shared.is_empty()).then_some((column, shared))
                                    })
                                    .collect();
                                (!common.is_empty()).then_some((table, common))
                            })
                            .collect();
                    }
                }
                _ => {}
            },
            Some(N::AExpr(a_expr)) if is_plain_equality(a_expr) => {
                if let (Some(lexpr), Some(rexpr)) = (a_expr.lexpr.as_deref(), a_expr.rexpr.as_deref())
                {
                    for (column_side, value_side) in [(lexpr, rexpr), (rexpr, lexpr)] {
                        if let (Some((qualifier, column)), Some(value)) =
                            (column_ref(column_side), value_key(value_side))
                        {
                            result
                                .entry(qualifier)
                                .or_default()
                                .entry(column)
                                .or_default()
                                .insert(value);
                        }
                    }
                }
            }
            _ => {}
        }
        result
    }

    /// The pinned columns written with any of the given qualifiers.
    fn columns_for(equalities: &EqualityMap, qualifiers: &[&str]) -> HashSet<String> {
        qualifiers
            .iter()
            .filter_map(|q| equalities.get(*q))
            .flat_map(|columns| columns.keys().cloned())
            .collect()
    }

    /// Analyze an INSERT statement.
    fn analyze_insert(&self, insert: &pb::InsertStmt) -> CardinalityInference {
        if insert.returning_list.is_empty() {
            return high(InferredCardinality::Exec, InferenceReason::DmlWithoutReturning);
        }

        // INSERT ... SELECT or multi-row VALUES (could return multiple rows)
        if insert.select_stmt.is_some() && !Self::is_single_row_values(insert) {
            return high(
                InferredCardinality::Many,
                InferenceReason::InsertMultipleRowsReturning,
            );
        }

        if let Some(on_conflict) = &insert.on_conflict_clause {
            match on_conflict.action() {
                // The row may not be inserted at all.
                pb::OnConflictAction::OnconflictNothing => {
                    return high(
                        InferredCardinality::Opt,
                        InferenceReason::InsertOnConflictDoNothingReturning,
                    );
                }
                // `DO UPDATE ... WHERE cond`: a conflicting row that fails the
                // condition is neither updated nor returned.
                pb::OnConflictAction::OnconflictUpdate if on_conflict.where_clause.is_some() => {
                    return high(
                        InferredCardinality::Opt,
                        InferenceReason::ConditionalUpsertReturning,
                    );
                }
                pb::OnConflictAction::OnconflictUpdate => {
                    return high(InferredCardinality::One, InferenceReason::UpsertReturning);
                }
                _ => {}
            }
        }

        // Single-row INSERT with RETURNING
        high(InferredCardinality::One, InferenceReason::InsertReturning)
    }

    /// Check if INSERT has a single-row VALUES clause.
    fn is_single_row_values(insert: &pb::InsertStmt) -> bool {
        match insert.select_stmt.as_deref().and_then(|s| s.node.as_ref()) {
            // An empty `values_lists` means INSERT ... SELECT.
            Some(N::SelectStmt(select)) => select.values_lists.len() == 1,
            _ => false,
        }
    }

    /// Analyze an UPDATE or DELETE.
    ///
    /// With RETURNING, the statement returns at most one row when its WHERE
    /// clause pins a primary key or unique constraint of the target table.
    /// `has_extra_tables` is UPDATE's FROM / DELETE's USING: the target row
    /// is then joined against other tables, and a column in the WHERE clause
    /// may belong to one of those, so nothing is inferred.
    fn analyze_dml_returning(
        &self,
        relation: Option<&pb::RangeVar>,
        where_clause: Option<&pb::Node>,
        has_extra_tables: bool,
        returning_list: &[pb::Node],
    ) -> CardinalityInference {
        if returning_list.is_empty() {
            return high(InferredCardinality::Exec, InferenceReason::DmlWithoutReturning);
        }

        let matched = || {
            if has_extra_tables {
                return None;
            }
            let target = TableRef::from_range_var(relation?);
            let table_rel = self.find_table_rel(&target)?;
            // Only columns that are unqualified or qualified with the target's
            // own name (its alias, when it has one) are the target's.
            let equalities = Self::collect_equalities(where_clause?);
            let columns = Self::columns_for(&equalities, &["", target.query_name()]);
            let constraint = Self::find_matching_uniqueness_constraint(table_rel, &columns)?;
            Some(Self::create_uniqueness_inference(&target.name, constraint))
        };

        // No unique match: could return multiple rows.
        matched().unwrap_or_else(|| low(InferenceReason::DefaultSelect))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::UniqueConstraint;
    use crate::pg_id::PgId;
    use crate::pg_rel::{PgRel, PgRelKind};
    use smallvec::SmallVec;
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
                comment: None,
                name: ustr("help_center_article_section_slug_idx"),
                columns: SmallVec::from([ustr("section"), ustr("slug")]),
                conditional: false,
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
    fn test_partial_unique_index_does_not_make_its_columns_unique() {
        // `unique (section, slug) where is_published`: unpublished rows may
        // share (section, slug), so equality on both can return many rows.
        let mut rel_index = rel_index_with_unique_index();
        for constraint in &mut rel_index.get_mut(&12345).unwrap().constraints {
            if let Constraint::Unique(unique) = constraint {
                unique.conditional = true;
            }
        }
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT body_html FROM help_center_article WHERE section = $1 AND slug = $2",
            "UPDATE help_center_article SET body_html = $3 WHERE section = $1 AND slug = $2 RETURNING id",
            "DELETE FROM help_center_article WHERE section = $1 AND slug = $2 RETURNING id",
        ] {
            let inference = analyzer.infer(sql).unwrap();
            assert_eq!(inference.cardinality, InferredCardinality::Many, "{sql}");
        }
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
                comment: None,
                name: ustr("orders_pkey"),
                columns: SmallVec::from([ustr("order_id")]),
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
                comment: None,
                name: ustr("customers_pkey"),
                columns: SmallVec::from([ustr("customer_id")]),
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
                comment: None,
                name: ustr("users_pkey"),
                columns: SmallVec::from([ustr("user_id")]),
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
    fn test_or_same_column_different_values_is_many() {
        // WHERE user_id = $1 OR user_id = $2 can match two rows: the branches
        // pin the key to different values.
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT * FROM users WHERE user_id = $1 OR user_id = $2",
            "SELECT * FROM users WHERE user_id = 1 OR user_id = 2",
            "SELECT * FROM users WHERE user_id = $1 OR user_id = $1::bigint",
            "UPDATE users SET status = 'x' WHERE user_id = $1 OR user_id = $2 RETURNING *",
        ] {
            let inference = analyzer.infer(sql).unwrap();
            assert_eq!(inference.cardinality, InferredCardinality::Many, "{sql}");
        }
    }

    #[test]
    fn test_or_same_column_same_value_is_opt() {
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT * FROM users WHERE user_id = $1 OR user_id = $1",
            "SELECT * FROM users WHERE (user_id = 7 AND status = 'a') OR (7 = user_id AND email = $1)",
        ] {
            let inference = analyzer.infer(sql).unwrap();
            assert_eq!(inference.cardinality, InferredCardinality::Opt, "{sql}");
        }
    }

    #[test]
    fn test_non_equality_operators_do_not_pin_a_key() {
        // `= ANY`, `IN`, `IS NOT DISTINCT FROM` and friends all carry `=` in
        // their operator name without being a single-value equality.
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT * FROM users WHERE user_id = ANY($1)",
            "SELECT * FROM users WHERE user_id IN ($1, $2)",
            "SELECT * FROM users WHERE user_id >= $1",
            "SELECT * FROM users WHERE NOT (user_id = $1)",
            "SELECT * FROM users WHERE user_id = NULL",
        ] {
            let inference = analyzer.infer(sql).unwrap();
            assert_eq!(inference.cardinality, InferredCardinality::Many, "{sql}");
        }
    }

    fn cardinality_of(analyzer: &CardinalityAnalyzer, sql: &str) -> InferredCardinality {
        analyzer.infer(sql).unwrap().cardinality
    }

    #[test]
    fn test_window_function_is_not_an_aggregate() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT count(*) OVER () FROM users",
            "SELECT id, sum(amount) OVER (PARTITION BY user_id) FROM orders",
            "SELECT row_number() OVER (ORDER BY id) FROM users",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many, "{sql}");
        }

        // A window over a real aggregate still sees one group.
        assert_eq!(
            cardinality_of(&analyzer, "SELECT sum(count(*)) OVER () FROM users"),
            InferredCardinality::One
        );
    }

    #[test]
    fn test_aggregate_with_having_is_opt() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        assert_eq!(
            cardinality_of(&analyzer, "SELECT count(*) FROM users HAVING count(*) > 5"),
            InferredCardinality::Opt
        );
    }

    #[test]
    fn test_aggregate_through_wrappers() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT count(*)::int FROM users",
            "SELECT coalesce(sum(amount), 0) FROM orders",
            "SELECT sum(amount) / nullif(count(*), 0) AS mean FROM orders",
            "SELECT CASE WHEN count(*) > 0 THEN max(id) ELSE 0 END FROM users",
            "SELECT count(*) FILTER (WHERE active), array_agg(id ORDER BY id) FROM users",
            "SELECT pg_catalog.count(*) FROM users",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::One, "{sql}");
        }

        // An aggregate inside a scalar subquery belongs to the subquery.
        assert_eq!(
            cardinality_of(&analyzer, "SELECT id, (SELECT count(*) FROM orders) FROM users"),
            InferredCardinality::Many
        );
        // LIMIT / OFFSET can remove the single row.
        assert_eq!(
            cardinality_of(&analyzer, "SELECT count(*) FROM users OFFSET $1"),
            InferredCardinality::Opt
        );
        // A set-returning function multiplies it.
        assert_eq!(
            cardinality_of(&analyzer, "SELECT count(*), generate_series(1, 3) FROM users"),
            InferredCardinality::Many
        );
    }

    #[test]
    fn test_select_without_from() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)",
            "SELECT 1",
            "SELECT $1::int + 1 AS next, now() AS at",
            "SELECT coalesce($1, 'x'), (SELECT max(id) FROM users)",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::One, "{sql}");
        }

        // WHERE can discard the row.
        assert_eq!(
            cardinality_of(&analyzer, "SELECT 1 WHERE $1"),
            InferredCardinality::Opt
        );

        for sql in [
            // Set-returning.
            "SELECT unnest($1::int[])",
            "SELECT generate_series(1, $1)",
            "SELECT (jsonb_each($1)).*",
            // Not known to be single-valued: could be RETURNS SETOF.
            "SELECT api.list_things($1)",
            "SELECT some_function($1)",
            // Not a SELECT list at all.
            "VALUES (1), (2)",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many, "{sql}");
        }
    }

    #[test]
    fn test_conditional_upsert_is_opt() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "INSERT INTO users (email, name) VALUES ($1, $2) \
                   ON CONFLICT (email) DO UPDATE SET name = excluded.name \
                   WHERE users.name IS DISTINCT FROM excluded.name RETURNING *";
        let inference = analyzer.infer(sql).unwrap();
        assert_eq!(inference.cardinality, InferredCardinality::Opt);
        assert!(matches!(
            inference.reason,
            InferenceReason::ConditionalUpsertReturning
        ));
    }

    #[test]
    fn test_dml_returning_key_match() {
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "UPDATE users SET status = $2 WHERE user_id = $1 RETURNING *",
            "UPDATE users u SET status = $2 WHERE u.user_id = $1 RETURNING *",
            "UPDATE users SET status = $2 WHERE users.user_id = $1 RETURNING *",
            "DELETE FROM users WHERE user_id = $1 RETURNING user_id",
            "DELETE FROM public.users AS u WHERE u.user_id = $1 RETURNING user_id",
        ] {
            let inference = analyzer.infer(sql).unwrap();
            assert_eq!(inference.cardinality, InferredCardinality::Opt, "{sql}");
            assert!(
                matches!(inference.reason, InferenceReason::PrimaryKeyMatch { .. }),
                "{sql}"
            );
        }

        assert_eq!(
            cardinality_of(&analyzer, "UPDATE users SET status = $1 WHERE status = $2 RETURNING *"),
            InferredCardinality::Many
        );
    }

    #[test]
    fn test_dml_with_extra_tables_is_many() {
        // `u.user_id = $1` pins a row of the *other* table; many target rows
        // can reference it. The qualifier must not be ignored.
        let mut rel_index = rel_index_with_users_pk();
        rel_index.insert(
            20002,
            table_with_pk(20002, "public", "sessions", "user_id", &["user_id", "token"]),
        );
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "UPDATE sessions s SET token = $2 FROM users u \
             WHERE u.user_id = $1 AND s.token = u.email RETURNING s.*",
            "UPDATE sessions SET token = $2 FROM users u WHERE user_id = $1 RETURNING *",
            "DELETE FROM sessions s USING users u \
             WHERE u.user_id = $1 AND s.token = u.email RETURNING s.*",
            // A qualifier that is not the target's.
            "UPDATE sessions s SET token = $2 WHERE other.user_id = $1 RETURNING *",
            // Once aliased, the table name itself is no longer a valid qualifier.
            "UPDATE sessions s SET token = $2 WHERE sessions.user_id = $1 RETURNING *",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many, "{sql}");
        }
    }

    fn table_with_pk(oid: u32, schema: &str, name: &str, pk: &str, columns: &[&str]) -> PgRel {
        use crate::pg_constraint::PrimaryKeyConstraint;
        PgRel {
            oid,
            id: PgId::new(Some(ustr(schema)), ustr(name)),
            kind: PgRelKind::Table,
            constraints: vec![Constraint::PrimaryKey(PrimaryKeyConstraint {
                comment: None,
                name: ustr(&format!("{name}_pkey")),
                columns: SmallVec::from([ustr(pk)]),
            })],
            columns: columns.iter().map(|c| ustr(c)).collect(),
            column_types: vec![23; columns.len()],
        }
    }

    #[test]
    fn test_same_named_tables_in_different_schemas() {
        // `audit.widget` is keyed by `event_id`, `core.widget` by `widget_id`.
        let mut rel_index = RelIndex::default();
        rel_index.insert(
            100,
            table_with_pk(100, "audit", "widget", "event_id", &["event_id", "widget_id"]),
        );
        rel_index.insert(
            200,
            table_with_pk(200, "core", "widget", "widget_id", &["widget_id", "event_id"]),
        );
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        // Qualified: the named schema's key decides.
        for (sql, expected) in [
            ("SELECT * FROM core.widget WHERE widget_id = $1", InferredCardinality::Opt),
            ("SELECT * FROM core.widget WHERE event_id = $1", InferredCardinality::Many),
            ("SELECT * FROM audit.widget WHERE event_id = $1", InferredCardinality::Opt),
            ("SELECT * FROM audit.widget WHERE widget_id = $1", InferredCardinality::Many),
            (
                "DELETE FROM audit.widget WHERE widget_id = $1 RETURNING *",
                InferredCardinality::Many,
            ),
            (
                "UPDATE core.widget SET event_id = 1 WHERE widget_id = $1 RETURNING *",
                InferredCardinality::Opt,
            ),
            ("SELECT * FROM nowhere.widget WHERE widget_id = $1", InferredCardinality::Many),
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), expected, "{sql}");
        }

        // Unqualified, and neither schema is on the search path (`public`):
        // Postgres would not find the table, so no inference either way.
        for sql in [
            "SELECT * FROM widget WHERE widget_id = $1",
            "SELECT * FROM widget WHERE event_id = $1",
            "DELETE FROM widget WHERE event_id = $1 RETURNING *",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many, "{sql}");
        }

        // Unqualified with both schemas on the path: the first one's table.
        for (path, by_widget_id, by_event_id) in [
            (["core", "audit"], InferredCardinality::Opt, InferredCardinality::Many),
            (["audit", "core"], InferredCardinality::Many, InferredCardinality::Opt),
        ] {
            let mut rel_index = RelIndex::default().with_search_path(&path);
            rel_index.insert(
                100,
                table_with_pk(100, "audit", "widget", "event_id", &["event_id", "widget_id"]),
            );
            rel_index.insert(
                200,
                table_with_pk(200, "core", "widget", "widget_id", &["widget_id", "event_id"]),
            );
            let analyzer = CardinalityAnalyzer::new(&rel_index);
            for (sql, expected) in [
                ("SELECT * FROM widget WHERE widget_id = $1", by_widget_id),
                ("SELECT * FROM widget WHERE event_id = $1", by_event_id),
                ("DELETE FROM widget WHERE event_id = $1 RETURNING *", by_event_id),
                ("UPDATE widget SET event_id = 1 WHERE widget_id = $1 RETURNING *", by_widget_id),
            ] {
                assert_eq!(cardinality_of(&analyzer, sql), expected, "{path:?}: {sql}");
            }
        }
    }

    #[test]
    fn test_cte_shadowing_a_table_is_not_resolved_to_it() {
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        let sql = "WITH users AS (SELECT 1 AS user_id FROM generate_series(1, 3)) \
                   SELECT * FROM users WHERE user_id = $1";
        assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many);
    }

    #[test]
    fn test_set_returning_target_defeats_key_match() {
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT unnest(tags) FROM users WHERE user_id = $1",
            "SELECT user_id, generate_series(1, 3) FROM users WHERE user_id = $1",
            "SELECT lower(regexp_split_to_table(email, '@')) FROM users WHERE user_id = $1",
            "SELECT (jsonb_each(meta)).* FROM users WHERE user_id = $1",
            "SELECT pg_catalog.jsonb_array_elements_text(meta) FROM users WHERE user_id = $1",
            "SELECT string_to_table(email, '.') FROM users WHERE user_id = $1",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many, "{sql}");
        }

        // Ordinary functions, and set-returning functions confined to a
        // subquery, leave the match intact. LIMIT 1 caps the result regardless.
        for sql in [
            "SELECT lower(email) FROM users WHERE user_id = $1",
            "SELECT (SELECT array_agg(t) FROM unnest(tags) t) FROM users WHERE user_id = $1",
            "SELECT unnest(tags) FROM users LIMIT 1",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Opt, "{sql}");
        }
    }

    #[test]
    fn test_user_defined_setof_function_via_fn_index() {
        use crate::pg_fn::PgFn;
        use std::collections::BTreeSet;

        let function = |oid: u32, name: &str, returns_set: bool| PgFn {
            oid,
            schema: "public".to_string(),
            name: name.to_string(),
            args: vec![],
            out_args: vec![],
            return_type_oid: 23,
            returns_set,
            is_procedure: false,
            has_table_params: false,
            definition: String::new(),
            language: "sql".to_string(),
            comment: None,
            exceptions: vec![],
            rel_deps: vec![],
            callees: BTreeSet::new(),
        };
        let fn_index = FunctionIndex::from_fns(
            [function(1, "explode", true), function(2, "tidy", false)],
            &["public"],
        );
        let rel_index = rel_index_with_users_pk();
        let analyzer = CardinalityAnalyzer::new(&rel_index).with_fn_index(Some(&fn_index));

        assert_eq!(
            cardinality_of(&analyzer, "SELECT explode(email) FROM users WHERE user_id = $1"),
            InferredCardinality::Many
        );
        assert_eq!(
            cardinality_of(&analyzer, "SELECT tidy(email) FROM users WHERE user_id = $1"),
            InferredCardinality::Opt
        );
        // Without FROM, a function proven single-valued gives exactly one row.
        assert_eq!(
            cardinality_of(&analyzer, "SELECT public.tidy($1)"),
            InferredCardinality::One
        );
        assert_eq!(
            cardinality_of(&analyzer, "SELECT explode($1)"),
            InferredCardinality::Many
        );
    }

    #[test]
    fn test_limit_1_with_ties_is_many() {
        let rel_index = empty_rel_index();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        assert_eq!(
            cardinality_of(
                &analyzer,
                "SELECT * FROM users ORDER BY created_at FETCH FIRST 1 ROW WITH TIES"
            ),
            InferredCardinality::Many
        );
        assert_eq!(
            cardinality_of(
                &analyzer,
                "SELECT * FROM users ORDER BY created_at FETCH FIRST 1 ROW ONLY"
            ),
            InferredCardinality::Opt
        );
    }

    #[test]
    fn test_join_condition_within_one_table_is_not_to_one() {
        // `c.customer_id = c.customer_id` equates the joined table with itself:
        // every customer row joins to the one order.
        let rel_index = rel_index_with_orders_and_customers();
        let analyzer = CardinalityAnalyzer::new(&rel_index);

        for sql in [
            "SELECT o.* FROM orders o JOIN customers c ON c.customer_id = c.customer_id \
             WHERE o.order_id = $1",
            "SELECT o.* FROM orders o JOIN customers c ON c.customer_id = customer_id \
             WHERE o.order_id = $1",
            "SELECT o.* FROM orders o CROSS JOIN customers c WHERE o.order_id = $1",
            "SELECT o.* FROM orders o, customers c WHERE o.order_id = $1",
        ] {
            assert_eq!(cardinality_of(&analyzer, sql), InferredCardinality::Many, "{sql}");
        }
    }

    /// Creates a rel_index where orders has a FK to customers, but customers.customer_id
    /// is NOT marked as a primary key. This tests FK-based inference.
    fn rel_index_with_fk_relationship() -> RelIndex {
        use crate::pg_constraint::{ForeignKeyConstraint, PrimaryKeyConstraint};
        use crate::pg_constraint::FkAction;
        let mut index = RelIndex::default();

        // Orders table with PK and FK to customers
        let orders = PgRel {
            oid: 30001,
            id: PgId::new(Some(ustr("public")), ustr("orders")),
            kind: PgRelKind::Table,
            constraints: vec![
                Constraint::PrimaryKey(PrimaryKeyConstraint {
                    comment: None,
                    name: ustr("orders_pkey"),
                    columns: SmallVec::from([ustr("order_id")]),
                }),
                Constraint::ForeignKey(ForeignKeyConstraint {
                    comment: None,
                    ref_table_oid: None,
                    name: ustr("orders_customer_id_fkey"),
                    columns: SmallVec::from([ustr("customer_id")]),
                    on_delete: FkAction::NoAction,
                    on_update: FkAction::NoAction,
                    ref_table: Some("customers".to_string()),
                    ref_columns: Some(SmallVec::from([ustr("customer_id")])),
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
                comment: None,
                name: ustr("products_pkey"),
                columns: SmallVec::from([ustr("product_id")]),
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
                    comment: None,
                    name: ustr("order_items_pkey"),
                    columns: SmallVec::from([ustr("item_id")]),
                }),
                Constraint::ForeignKey(ForeignKeyConstraint {
                    comment: None,
                    ref_table_oid: None,
                    name: ustr("order_items_order_id_fkey"),
                    columns: SmallVec::from([ustr("order_id")]),
                    on_delete: FkAction::Cascade,
                    on_update: FkAction::NoAction,
                    ref_table: Some("orders".to_string()),
                    ref_columns: Some(SmallVec::from([ustr("order_id")])),
                }),
                Constraint::ForeignKey(ForeignKeyConstraint {
                    comment: None,
                    ref_table_oid: None,
                    name: ustr("order_items_product_id_fkey"),
                    columns: SmallVec::from([ustr("product_id")]),
                    on_delete: FkAction::Restrict,
                    on_update: FkAction::NoAction,
                    ref_table: Some("products".to_string()),
                    ref_columns: Some(SmallVec::from([ustr("product_id")])),
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
