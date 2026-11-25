use crate::codegen::OID;
use crate::composite_index::{CompositeIndex, ResolvedAttr};
use crate::pg_constraint::Constraint;
use crate::pg_rel::PgRel;
use crate::rel_index::RelIndex;
use indexmap::map::Entry;
use indexmap::IndexMap;
use pg_query::protobuf;
use pg_query::protobuf::node::Node as N;
use std::collections::{HashMap, HashSet};
use ustr::Ustr;

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

/// The conservative answer: nothing proves the value non-null.
const NULLABLE: ColumnNullability = ColumnNullability {
    nullable_due_to_join: false,
    nullable_on_base: true,
};

const NOT_NULL: ColumnNullability = ColumnNullability {
    nullable_due_to_join: false,
    nullable_on_base: false,
};

/// Column name -> nullability, in column order: the result of analyzing a
/// view or query, and what the cache holds per view.
pub type ColumnMap = IndexMap<String, ColumnNullability>;

/// The catalog column a value is read from unchanged: "this reference
/// resolves to relation `rel_oid`, column `column`". Carried through CTEs,
/// subqueries and aliases as long as the value stays a plain column
/// reference, so its declared type (a domain, say) is the value's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnSource {
    pub rel_oid: OID,
    pub column: Ustr,
}

/// What is known about one column in scope or in a result.
#[derive(Debug, Clone, Copy)]
struct Col {
    cn: ColumnNullability,
    /// `None` for anything computed, or a reference that did not resolve.
    source: Option<ColumnSource>,
}

impl From<ColumnNullability> for Col {
    fn from(cn: ColumnNullability) -> Self {
        Col { cn, source: None }
    }
}

/// Output columns of one statement, in order. `None` when the shape could not
/// be determined (a `*` over a relation whose columns we don't know), in which
/// case nothing positional may be concluded from it.
type SelectShape = Option<Vec<(String, Col)>>;

/// Inferred nullability of every view, keyed by the view's relation OID so
/// that `public.v`, `v` and a view reached through the search path all find
/// the same entry once the name has been resolved (see [`RelIndex::resolve`]).
#[derive(Debug, Clone, Default)]
pub struct ViewNullabilityCache {
    by_oid: HashMap<OID, ColumnMap>,
    /// `(schema, name)` of each cached view, for callers that hold a view's
    /// name but not its OID.
    names: HashMap<(String, String), OID>,
}

impl ViewNullabilityCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the analysis of view `schema.name`, whose relation OID is `oid`.
    pub fn insert(&mut self, oid: OID, schema: &str, name: &str, columns: ColumnMap) {
        self.names.insert((schema.to_string(), name.to_string()), oid);
        self.by_oid.insert(oid, columns);
    }

    pub fn get_by_oid(&self, oid: OID) -> Option<&ColumnMap> {
        self.by_oid.get(&oid)
    }

    /// Look a view up by its fully qualified name.
    pub fn get_by_name(&self, schema: &str, name: &str) -> Option<&ColumnMap> {
        let oid = self.names.get(&(schema.to_string(), name.to_string()))?;
        self.by_oid.get(oid)
    }

    pub fn len(&self) -> usize {
        self.by_oid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_oid.is_empty()
    }
}

/// Columns of a relation that has no catalog entry: a CTE, a FROM-clause
/// subquery or a set-returning function.
#[derive(Debug, Clone, Default)]
struct DerivedColumns {
    /// Column name -> what is known about it, in declaration order.
    columns: IndexMap<String, Col>,
    /// True when `columns` is the relation's full column list in the right
    /// order. When false the map only holds columns we happen to know by
    /// name, so `*` cannot be expanded from it. The default — no columns,
    /// incomplete — is a relation we know nothing about.
    complete: bool,
}

impl DerivedColumns {
    /// Build from a positional column list. Unnamed columns get a
    /// `column_N` placeholder so they keep their position. A name that occurs
    /// twice can't be referenced unambiguously and would collapse to one map
    /// slot, so it becomes nullable and the list is no longer `complete`.
    fn from_positional(cols: Vec<(String, Col)>) -> Self {
        let mut columns = IndexMap::new();
        let mut complete = true;
        for (i, (name, cn)) in cols.into_iter().enumerate() {
            let key = if name.is_empty() {
                format!("column_{}", i)
            } else {
                name
            };
            match columns.entry(key) {
                Entry::Occupied(mut e) => {
                    complete = false;
                    *e.get_mut() = NULLABLE.into();
                }
                Entry::Vacant(e) => {
                    e.insert(cn);
                }
            }
        }
        Self { columns, complete }
    }

    /// Columns of a SELECT with a positional column alias list applied
    /// (`AS t(a, b)`, `WITH cte(a, b)`). The aliases go on first: they can
    /// tell apart columns that the SELECT gave the same name.
    fn from_shape(shape: SelectShape, colnames: &[protobuf::Node]) -> Self {
        shape
            .map(|cols| Self::from_positional(rename_positional(cols, colnames)))
            .unwrap_or_default()
    }
}

/// Rename the leading columns of a positional column list to `colnames`.
fn rename_positional<T>(
    mut cols: Vec<(String, T)>,
    colnames: &[protobuf::Node],
) -> Vec<(String, T)> {
    for ((name, _), alias) in cols.iter_mut().zip(colnames) {
        if let Some(alias) = string_field(alias) {
            *name = alias.to_string();
        }
    }
    cols
}

#[derive(Debug, Clone)]
enum TableInfo {
    /// Base table.
    Table {
        oid: OID,
        /// True if this table reference can produce NULL values due to outer joins
        is_nullable: bool,
    },
    /// View: its columns with the nullability the cache holds for them, or
    /// all nullable when it has none.
    View {
        oid: OID,
        /// Column name -> nullability mapping (order-preserving)
        nullability: ColumnMap,
        /// True if this view reference can produce NULL values due to outer joins
        is_nullable: bool,
    },
    /// "Derived" table — a CTE reference, FROM-clause subquery, set-returning
    /// function, or a relation we could not resolve at all.
    Derived {
        columns: DerivedColumns,
        /// True if this reference can produce NULL values due to outer joins.
        is_nullable: bool,
    },
}

impl TableInfo {
    fn unknown() -> Self {
        TableInfo::Derived {
            columns: DerivedColumns::default(),
            is_nullable: false,
        }
    }

    fn is_join_nullable(&self) -> bool {
        match self {
            TableInfo::Table { is_nullable, .. }
            | TableInfo::View { is_nullable, .. }
            | TableInfo::Derived { is_nullable, .. } => *is_nullable,
        }
    }

    fn set_join_nullable(&mut self) {
        match self {
            TableInfo::Table { is_nullable, .. }
            | TableInfo::View { is_nullable, .. }
            | TableInfo::Derived { is_nullable, .. } => *is_nullable = true,
        }
    }
}

/// The single output column a `JOIN … USING (col)` merges its two inputs
/// into.
#[derive(Debug, Clone)]
struct MergedColumn {
    name: String,
    nullability: ColumnNullability,
    /// The catalog column the merged value has the type of: set when both
    /// sides come from catalog columns of one declared type.
    source: Option<ColumnSource>,
    /// Every alias under the join that produced the column. When an enclosing
    /// outer join makes all of them nullable, the merged column is too.
    aliases: Vec<String>,
    /// An enclosing join merged the same column name again, so this entry is
    /// no longer what an unqualified reference sees.
    hidden: bool,
}

/// One column of what a bare `*` expands to, in Postgres's order.
#[derive(Debug, Clone)]
enum StarColumn {
    /// `alias.column`
    Of { alias: String, column: String },
    /// Index into [`Scope::merged`].
    Merged(usize),
}

/// What one FROM item contributed to the scope.
struct FromItem {
    /// Keys in [`Scope::aliases`] registered under this item.
    aliases: Vec<String>,
    /// The item's columns in `*` order, `None` if unknown.
    star: Option<Vec<StarColumn>>,
}

/// Name-resolution state of one query level (a SELECT, or a DML statement
/// with its target and FROM/USING items). Swapped out wholesale around nested
/// statements; a nested statement that may refer to the enclosing level (a
/// subquery in an expression, a LATERAL item) finds it on
/// [`ViewNullabilityAnalyzer::outer`].
#[derive(Default, Clone)]
struct Scope {
    /// Table alias -> relation, in FROM order.
    aliases: IndexMap<String, TableInfo>,
    /// `USING` merged columns. A merged join key is `COALESCE(left, right)`,
    /// so its nullability follows the *preserved* side of the join (the left
    /// side of a LEFT JOIN, the right side of a RIGHT JOIN, either for INNER,
    /// neither for FULL). Consulted for unqualified column references before
    /// the owner lookup, since the column lives in both joined relations.
    merged: Vec<MergedColumn>,
    /// Expansion of a bare `*`; `None` if any FROM item's columns are unknown.
    star: Option<Vec<StarColumn>>,
    /// Columns proved NOT NULL by the WHERE clause, directly or through
    /// CHECK-derived facts. Key: table alias, so a self-join's two references
    /// to one table are refined independently.
    refined_not_null: HashMap<String, HashSet<String>>,
    /// `GROUP BY` uses GROUPING SETS / ROLLUP / CUBE, which emits rows where
    /// grouped columns are NULL regardless of their constraints.
    grouping_sets: bool,
    /// The level has a FROM item whose alias and columns we know nothing
    /// about (XMLTABLE, JSON_TABLE, TABLESAMPLE, ...). Any name not found here
    /// may belong to it, so nothing unresolved may be looked up further out.
    opaque: bool,
    /// Join aliases (`(a JOIN b) AS j`): names that exist at this level but
    /// are not in `aliases`. A reference qualified with one is unknown, not a
    /// reference to an enclosing level.
    join_aliases: HashSet<String>,
}

/// Result of looking a column reference up in one [`Scope`].
enum Lookup {
    Found(Col),
    /// The name belongs (or may belong) to this level, but not to exactly one
    /// thing we know.
    Unknown,
    /// Nothing at this level has the name: an enclosing level may.
    NotHere,
}

/// What [`ViewNullabilityAnalyzer::analyze_parsed`] found out about a
/// statement.
#[derive(Debug, Default)]
pub struct StatementAnalysis {
    /// The statement's output columns in order (a SELECT's target list, a DML
    /// statement's RETURNING list). `None` when the shape is unknown.
    pub columns: Option<Vec<OutputColumn>>,
    /// Where each resolved `ColumnRef` in the statement reads from, keyed by
    /// the reference's `location`.
    column_refs: HashMap<i32, ColumnSource>,
}

impl StatementAnalysis {
    /// The catalog column the `ColumnRef` at `location` resolves to, in the
    /// scope it is written in: CTEs, subqueries, aliases and the search path
    /// are honored. `None` when it does not resolve to exactly one.
    pub fn column_ref_source(&self, location: i32) -> Option<ColumnSource> {
        self.column_refs.get(&location).copied()
    }
}

/// One output column of an analyzed statement.
#[derive(Debug, Clone)]
pub struct OutputColumn {
    /// The name the statement gives the column; empty when it has none we
    /// can tell.
    pub name: String,
    pub nullability: ColumnNullability,
    /// Set when the column is a plain reference to a catalog column.
    pub source: Option<ColumnSource>,
}

/// A data-modifying statement.
#[derive(Clone, Copy)]
enum Dml<'s> {
    Insert(&'s protobuf::InsertStmt),
    Update(&'s protobuf::UpdateStmt),
    Delete(&'s protobuf::DeleteStmt),
    Merge(&'s protobuf::MergeStmt),
}

impl<'s> Dml<'s> {
    fn of(node: &'s N) -> Option<Self> {
        match node {
            N::InsertStmt(stmt) => Some(Dml::Insert(stmt)),
            N::UpdateStmt(stmt) => Some(Dml::Update(stmt)),
            N::DeleteStmt(stmt) => Some(Dml::Delete(stmt)),
            N::MergeStmt(stmt) => Some(Dml::Merge(stmt)),
            _ => None,
        }
    }

    fn with_clause(self) -> Option<&'s protobuf::WithClause> {
        match self {
            Dml::Insert(stmt) => stmt.with_clause.as_ref(),
            Dml::Update(stmt) => stmt.with_clause.as_ref(),
            Dml::Delete(stmt) => stmt.with_clause.as_ref(),
            Dml::Merge(stmt) => stmt.with_clause.as_ref(),
        }
    }

    fn target(self) -> Option<&'s protobuf::RangeVar> {
        match self {
            Dml::Insert(stmt) => stmt.relation.as_ref(),
            Dml::Update(stmt) => stmt.relation.as_ref(),
            Dml::Delete(stmt) => stmt.relation.as_ref(),
            Dml::Merge(stmt) => stmt.relation.as_ref(),
        }
    }

    fn returning_list(self) -> &'s [protobuf::Node] {
        match self {
            Dml::Insert(stmt) => &stmt.returning_list,
            Dml::Update(stmt) => &stmt.returning_list,
            Dml::Delete(stmt) => &stmt.returning_list,
            Dml::Merge(stmt) => &stmt.returning_list,
        }
    }
}

/// Analyzes view definitions to infer column nullability
pub struct ViewNullabilityAnalyzer<'a> {
    /// Reference to the relation index for base table information
    rel_index: &'a RelIndex,
    scope: Scope,
    /// Enclosing query levels the statement being analyzed may refer to,
    /// innermost last.
    outer: Vec<Scope>,
    /// When set, every `ColumnRef` met is resolved in its scope and its
    /// source recorded here by `location`.
    refs: Option<HashMap<i32, ColumnSource>>,
    /// Cache of previously analyzed view nullability
    view_nullability_cache: &'a ViewNullabilityCache,
    /// CTEs visible at the point being analyzed. Entries are added while
    /// walking a `WITH` clause and removed again when its statement ends.
    cte_columns: HashMap<String, DerivedColumns>,
    /// Lazily-populated cache of CHECK-derived nullability facts per relation.
    /// Filled on first access during predicate refinement; reused across
    /// subsequent SELECTs in the same analyzer instance.
    relation_facts_cache: HashMap<OID, Vec<crate::parse_check_facts::NotNullFact>>,
    /// Composite-type attribute metadata, used to decide whether a projection
    /// like `(p.length).amount` can be NULL once `p.length` itself is known
    /// NOT NULL. Without it every attribute projection stays conservatively
    /// nullable. Attached with [`Self::with_composite_index`].
    composite_index: Option<&'a CompositeIndex>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// True iff the active WHERE-derived predicate `active` is strong enough to
/// satisfy the fact-antecedent `required`.
///
/// Direct structural matches (eq↔eq, is-not-null↔is-not-null) are the obvious
/// case. We also accept `col = literal` as proof that `col IS NOT NULL`,
/// since equality with a non-NULL literal rules out NULL — this lets a single
/// discriminator WHERE refine columns whose constraints are NULL-keyed rather
/// than literal-keyed.
fn predicate_implies(
    active: &crate::parse_check_facts::Predicate,
    required: &crate::parse_check_facts::Predicate,
) -> bool {
    use crate::parse_check_facts::Predicate;
    match (active, required) {
        (
            Predicate::Eq { column: c1, value: v1 },
            Predicate::Eq { column: c2, value: v2 },
        ) => c1 == c2 && v1 == v2,
        (
            Predicate::IsNotNull { column: c1 },
            Predicate::IsNotNull { column: c2 },
        ) => c1 == c2,
        // `col = literal` implies `col IS NOT NULL` (the literal is non-NULL by
        // construction in our model, which only carries text literals today).
        (
            Predicate::Eq { column: c1, .. },
            Predicate::IsNotNull { column: c2 },
        ) => c1 == c2,
        // `col IS NOT NULL` does NOT imply `col = some_literal` — we can't
        // infer the value side from a NULL test.
        (Predicate::IsNotNull { .. }, Predicate::Eq { .. }) => false,
    }
}

/// The name of a function call, if the call can be taken to mean the builtin
/// of that name: unqualified, or qualified with `pg_catalog` (which is how
/// the parser spells SQL-syntax calls such as `EXTRACT(…)`). A call qualified
/// with any other schema is a user function that merely shares the name.
fn builtin_function_name(func: &protobuf::FuncCall) -> Option<&str> {
    match func.funcname.as_slice() {
        [name] => string_field(name),
        [schema, name] if string_field(schema) == Some("pg_catalog") => string_field(name),
        _ => None,
    }
}

/// How a builtin function's result nullability follows from its arguments.
#[derive(Debug, Clone, Copy, PartialEq)]
enum FnRule {
    /// Never returns NULL, whatever the arguments.
    NeverNull,
    /// Returns NULL only if an argument is NULL.
    Strict,
    /// Returns NULL only if one of the first N arguments is NULL.
    StrictPrefix(usize),
    /// Can return NULL for non-NULL arguments.
    Nullable,
}

/// Nullability rule for the builtin scalar, aggregate and window functions we
/// know. Nothing in this table returns a set, which
/// [`ViewNullabilityAnalyzer::is_srf_free`] relies on.
fn builtin_function_rule(name: &str) -> Option<FnRule> {
    use FnRule::*;
    Some(match name {
        // Aggregates that always return a value.
        "count" => NeverNull,

        // Window functions in the ranking/numbering family are NOT NULL
        // regardless of the PARTITION BY / ORDER BY contents. (`lag`/`lead`/
        // `first_value`/`last_value`/`nth_value` are not listed: they return
        // whatever the underlying expression yields, and NULL at the
        // partition edges.)
        "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist" => NeverNull,
        // `ntile(NULL)` is NULL.
        "ntile" => Strict,

        // Aggregates that return NULL over empty (or all-NULL) input.
        "sum" | "avg" | "min" | "max" | "stddev" | "stddev_samp" | "stddev_pop" | "variance"
        | "var_samp" | "var_pop" | "bool_and" | "bool_or" | "every" | "array_agg"
        | "string_agg" | "json_agg" | "jsonb_agg" | "json_object_agg" | "jsonb_object_agg" => {
            Nullable
        }

        // Strict string functions: NULL in any arg → NULL out.
        "lower" | "upper" | "initcap" | "trim" | "ltrim" | "rtrim" | "btrim" | "substr"
        | "replace" | "translate" | "repeat" | "reverse" | "lpad" | "rpad" | "left" | "right"
        | "split_part" | "position" | "strpos" | "ascii" | "chr" | "md5" | "encode"
        | "decode" | "to_hex" | "quote_ident" | "quote_literal" | "quote_nullable"
        | "regexp_replace" | "regexp_split_to_array" | "starts_with" => Strict,

        // NULL when the pattern does not match. `substring` has a positional
        // form that is strict; see `is_function_nullable`.
        "regexp_match" | "substring" => Nullable,

        // Strict length/size functions.
        "length" | "char_length" | "character_length" | "bit_length" | "octet_length" => Strict,

        // `concat` skips NULL arguments and always returns a string (empty
        // when every arg is NULL).
        "concat" => NeverNull,
        // `concat_ws(sep, …)` is NULL for a NULL separator; `format(fmt, …)`
        // for a NULL format string. Their other arguments may be NULL.
        "concat_ws" | "format" => StrictPrefix(1),

        // Strict mathematical functions.
        "abs" | "ceil" | "ceiling" | "floor" | "round" | "trunc" | "sign" | "mod" | "power"
        | "sqrt" | "cbrt" | "exp" | "ln" | "log" | "log10" | "div" | "gcd" | "lcm" | "sin"
        | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "sinh" | "cosh" | "tanh"
        | "degrees" | "radians" => Strict,

        // Math constants / non-deterministic but never-NULL.
        "pi" | "random" => NeverNull,

        // Clock functions. (`CURRENT_TIMESTAMP` and friends are not function
        // calls in the parse tree; see the `SqlvalueFunction` arm.)
        "now" | "clock_timestamp" | "statement_timestamp" | "transaction_timestamp"
        | "timeofday" => NeverNull,

        // Strict date/time functions.
        "date_part" | "extract" | "date_trunc" | "age" | "to_char" | "to_date"
        | "to_timestamp" | "to_number" | "make_date" | "make_time" | "make_timestamp"
        | "make_timestamptz" | "make_interval" | "justify_days" | "justify_hours"
        | "justify_interval" | "isfinite" => Strict,

        // Array introspection functions return NULL for empty arrays or
        // out-of-bounds dimensions.
        "array_length" | "array_upper" | "array_lower" | "array_ndims" | "array_dims"
        | "array_position" => Nullable,

        // Array functions that return NULL only for NULL input.
        "cardinality" | "array_positions" | "array_remove" | "array_replace"
        | "array_append" | "array_prepend" | "array_cat" => Strict,

        // `array_to_string(array, delimiter [, null_string])` is NULL for a
        // NULL array or delimiter; `null_string` may be NULL.
        "array_to_string" => StrictPrefix(2),

        // JSON builders encode NULL arguments as JSON `null`.
        "json_build_object" | "jsonb_build_object" | "json_build_array"
        | "jsonb_build_array" => NeverNull,

        // Strict JSON functions.
        "json_object" | "jsonb_object" | "row_to_json" | "to_json" | "to_jsonb"
        | "array_to_json" | "jsonb_set" | "jsonb_insert" | "jsonb_strip_nulls"
        | "json_strip_nulls" | "jsonb_typeof" | "json_typeof" | "jsonb_array_length"
        | "json_array_length" | "jsonb_pretty" => Strict,

        // UUID generation: never NULL.
        "gen_random_uuid" | "uuid_generate_v1" | "uuid_generate_v1mc" | "uuid_generate_v4" => {
            NeverNull
        }

        _ => return None,
    })
}

/// True iff `node` is a plain (non-window) call to a builtin aggregate. A
/// query whose only target is one, with no GROUP BY / HAVING, returns exactly
/// one row — `(SELECT count(*) FROM t)`, `(SELECT max(x) FROM t)`.
///
/// We deliberately keep the allowlist small and well-known. Aggregates outside
/// this list fall through to the conservative "could return zero rows" path.
fn is_aggregate_call(node: &protobuf::Node) -> bool {
    let func = match node.node.as_ref() {
        Some(N::FuncCall(f)) => f,
        // TypeCast wrapping an aggregate (`count(*)::int4`) still counts —
        // the wrapper doesn't change cardinality.
        Some(N::TypeCast(tc)) => {
            return tc.arg.as_deref().map(is_aggregate_call).unwrap_or(false);
        }
        _ => return false,
    };
    // `count(*) OVER ()` is a window function: one row per input row.
    if func.over.is_some() {
        return false;
    }
    matches!(
        builtin_function_name(func),
        Some(
            "count"
                | "sum"
                | "avg"
                | "min"
                | "max"
                | "stddev"
                | "stddev_samp"
                | "stddev_pop"
                | "variance"
                | "var_samp"
                | "var_pop"
                | "array_agg"
                | "string_agg"
                | "json_agg"
                | "jsonb_agg"
                | "bool_and"
                | "bool_or"
                | "every"
        )
    )
}

/// The call inside one entry of `RangeFunction.functions`, and whether the
/// entry carries its own column definition list. pg_query wraps each entry in
/// a `List` of `[call, coldeflist]`.
fn range_function_call(node: &protobuf::Node) -> (Option<&protobuf::FuncCall>, bool) {
    let (call, coldefs) = match node.node.as_ref() {
        Some(N::List(list)) => (list.items.first(), list.items.get(1)),
        _ => (Some(node), None),
    };
    let func = match call.and_then(|n| n.node.as_ref()) {
        Some(N::FuncCall(f)) => Some(&**f),
        _ => None,
    };
    let has_coldefs = match coldefs.and_then(|n| n.node.as_ref()) {
        None => false,
        Some(N::List(list)) => !list.items.is_empty(),
        Some(_) => true,
    };
    (func, has_coldefs)
}

/// Output columns of a builtin set-returning function called in FROM, as
/// `(name, nullability)`. A `None` name is a scalar result column, which
/// Postgres names after the table alias or the function. `None` overall when
/// we don't know how many columns the function returns.
fn builtin_srf_columns(
    func: &protobuf::FuncCall,
) -> Option<Vec<(Option<&'static str>, ColumnNullability)>> {
    Some(match builtin_function_name(func)? {
        // Every generated element is non-null by construction.
        "generate_series" => vec![(None, NOT_NULL)],
        // `unnest` propagates whatever NULLs were in the array. In FROM it
        // takes several arrays and returns one column per array.
        "unnest" => vec![(None, NULLABLE); func.args.len().max(1)],
        "generate_subscripts"
        | "json_array_elements"
        | "json_array_elements_text"
        | "jsonb_array_elements"
        | "jsonb_array_elements_text"
        | "json_object_keys"
        | "jsonb_object_keys"
        | "regexp_matches"
        | "regexp_split_to_table"
        | "string_to_table" => vec![(None, NULLABLE)],
        "json_each" | "json_each_text" | "jsonb_each" | "jsonb_each_text" => {
            vec![(Some("key"), NULLABLE), (Some("value"), NULLABLE)]
        }
        _ => return None,
    })
}

impl<'a> ViewNullabilityAnalyzer<'a> {
    pub fn new(rel_index: &'a RelIndex, view_nullability_cache: &'a ViewNullabilityCache) -> Self {
        Self {
            rel_index,
            scope: Scope::default(),
            outer: Vec::new(),
            refs: None,
            view_nullability_cache,
            cte_columns: HashMap::new(),
            relation_facts_cache: HashMap::new(),
            composite_index: None,
        }
    }

    /// Attach composite-type metadata so attribute projections
    /// (`(p.length).amount`) can be proven NOT NULL from the composite's
    /// annotations or a wrapping domain's CHECKs.
    pub fn with_composite_index(mut self, composite_index: &'a CompositeIndex) -> Self {
        self.composite_index = Some(composite_index);
        self
    }

    /// The views a view definition reads — the ones it must be analyzed
    /// *after* — by relation OID.
    ///
    /// Every reference counts, wherever it sits: CTE bodies, subqueries in
    /// FROM, set-operation branches, scalar subqueries in the target list or
    /// WHERE. Missing one silently breaks the topological ordering of view
    /// analysis, and a view analyzed before its dependency sees that
    /// dependency as unknown and infers every column nullable — with the
    /// outcome depending on hash-map iteration order, i.e. varying between
    /// runs.
    ///
    /// Names are resolved as everywhere else (see [`RelIndex::resolve`]); one
    /// that resolves to nothing is not a dependency we could order by.
    pub fn extract_view_dependencies(&self, view_definition: &str) -> anyhow::Result<HashSet<OID>> {
        use crate::pg_rel::PgRelKind;
        let parse_result = pg_query::parse(view_definition)?;

        // pg_query collects every RangeVar in the tree and drops CTE names it
        // has already seen; re-filter here so traversal order can't leak a
        // CTE reference through.
        let cte_names: HashSet<&str> = parse_result
            .cte_names
            .iter()
            .map(String::as_str)
            .collect();

        let mut dependencies = HashSet::new();
        for table in parse_result.tables() {
            let (schema, name) = match table.split_once('.') {
                Some((schema, name)) => (Some(schema), name),
                None => (None, table.as_str()),
            };
            if schema.is_none() && cte_names.contains(name) {
                continue;
            }
            match self.rel_index.resolve(schema, name) {
                Some(rel) if matches!(rel.kind, PgRelKind::View { .. }) => {
                    log::debug!("Found view dependency: {}.{}", rel.id.schema(), rel.id.name());
                    dependencies.insert(rel.oid);
                }
                Some(_) => log::debug!("Found base table: schema={:?}, table={}", schema, name),
                None => log::debug!("Unresolved relation: schema={:?}, table={}", schema, name),
            }
        }

        Ok(dependencies)
    }

    /// Analyze a view definition (or any single query) and return a map of
    /// column names to their nullability, in column order.
    pub fn analyze_view(
        &mut self,
        view_definition: &str,
        view_columns: &[String],
    ) -> anyhow::Result<ColumnMap> {
        self.analyze_statement(view_definition, view_columns, false)
    }

    /// Like [`Self::analyze_view`] for the body of a `LANGUAGE sql` function,
    /// whose result is that of its *last* statement.
    pub fn analyze_function_body(
        &mut self,
        body: &str,
        output_columns: &[String],
    ) -> anyhow::Result<ColumnMap> {
        self.analyze_statement(body, output_columns, true)
    }

    /// Analyze the first statement of an already parsed query: its output
    /// columns (nullability and provenance) and the source of every column
    /// reference in it. Handles SELECT and INSERT / UPDATE / DELETE / MERGE
    /// (whose output is the RETURNING list).
    pub fn analyze_parsed(
        &mut self,
        parse_result: &protobuf::ParseResult,
    ) -> anyhow::Result<StatementAnalysis> {
        self.reset();
        self.refs = Some(HashMap::new());
        let node = parse_result
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .and_then(|n| n.node.as_ref());
        let shape = self.analyze_top_level(node, true);
        let column_refs = self.refs.take().unwrap_or_default();
        let columns = shape?.map(|cols| {
            cols.into_iter()
                .map(|(name, col)| OutputColumn {
                    name,
                    nullability: col.cn,
                    source: col.source,
                })
                .collect()
        });
        Ok(StatementAnalysis {
            columns,
            column_refs,
        })
    }

    /// Clear per-statement state. `relation_facts_cache` is intentionally
    /// kept — it's a pure derived cache keyed by relation OID and safe to
    /// reuse across analyses.
    fn reset(&mut self) {
        self.scope = Scope::default();
        self.outer.clear();
        self.cte_columns.clear();
        self.refs = None;
    }

    /// `dml`: whether a data-modifying statement is analyzed (for its
    /// RETURNING list) or left unknown.
    fn analyze_top_level(&mut self, node: Option<&N>, dml: bool) -> anyhow::Result<SelectShape> {
        match node {
            Some(N::SelectStmt(select)) => self.analyze_select_stmt(select),
            Some(other) if dml => match Dml::of(other) {
                Some(stmt) => self.analyze_dml(stmt),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    fn analyze_statement(
        &mut self,
        sql: &str,
        output_columns: &[String],
        last_statement: bool,
    ) -> anyhow::Result<ColumnMap> {
        log::debug!("Analyzing query with {} columns", output_columns.len());
        log::trace!("Query: {}", sql);

        let parse_result = pg_query::parse(sql)?;
        self.reset();

        // A function body's result is its last statement's, and only a
        // SELECT is analyzed there. A query or view is its first statement,
        // which may also be INSERT / UPDATE / DELETE / MERGE … RETURNING.
        let stmts = &parse_result.protobuf.stmts;
        let stmt = if last_statement {
            stmts.last()
        } else {
            stmts.first()
        };
        let node = stmt
            .and_then(|s| s.stmt.as_ref())
            .and_then(|n| n.node.as_ref());
        let shape = self.analyze_top_level(node, !last_statement)?;

        // Results are paired with the output columns by position, so a count
        // mismatch means we misread the query: conclude nothing.
        let inferred = shape
            .filter(|cols| cols.len() == output_columns.len())
            .unwrap_or_default();

        Ok(output_columns
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let cn = inferred.get(i).map(|(_, col)| col.cn).unwrap_or(NULLABLE);
                (name.clone(), cn)
            })
            .collect())
    }

    /// Analyze a `SelectStmt` and return its output columns in declaration order,
    /// each paired with its inferred nullability.
    ///
    /// Handles these shapes:
    ///   * `WITH` prefix — each CTE is analyzed and registered in `cte_columns`
    ///     for the duration of the statement, so FROM references resolve.
    ///   * Set operations (`UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`) — both
    ///     arms are analyzed and merged column-wise. A column is NOT NULL only
    ///     if it is NOT NULL on *every* arm; mismatched column counts produce
    ///     all-nullable output (conservative).
    ///   * `VALUES` lists.
    ///   * Plain `SELECT … FROM …` — FROM items are walked into the scope,
    ///     then each target expression is analyzed.
    ///
    /// Works in `self.scope`. Callers analyzing a *nested* SelectStmt go
    /// through [`Self::analyze_subselect`] or [`Self::analyze_correlated`],
    /// which give it a fresh scope and decide whether it sees this one.
    fn analyze_select_stmt(&mut self, select: &protobuf::SelectStmt) -> anyhow::Result<SelectShape> {
        // CTEs are visible to this statement only (including its set-operation
        // arms and subqueries).
        let Some(with) = &select.with_clause else {
            return self.analyze_select_body(select);
        };
        let saved_ctes = self.cte_columns.clone();
        let result = self
            .analyze_with_clause(with)
            .and_then(|()| self.analyze_select_body(select));
        self.cte_columns = saved_ctes;
        result
    }

    fn analyze_select_body(&mut self, select: &protobuf::SelectStmt) -> anyhow::Result<SelectShape> {
        // UNION/INTERSECT/EXCEPT: analyze each arm in its own scope and merge.
        use protobuf::SetOperation;
        let op = SetOperation::try_from(select.op).unwrap_or(SetOperation::Undefined);
        if op != SetOperation::SetopNone {
            let larg_cols = match &select.larg {
                Some(larg) => self.analyze_subselect(larg)?,
                None => None,
            };
            let rarg_cols = match &select.rarg {
                Some(rarg) => self.analyze_subselect(rarg)?,
                None => None,
            };
            return Ok(match (larg_cols, rarg_cols) {
                (Some(l), Some(r)) => Some(self.merge_set_op_columns(&l, &r)),
                // Names come from the left arm; without the right arm's
                // columns nothing can be claimed.
                (Some(l), None) => Some(self.merge_set_op_columns(&l, &[])),
                (None, _) => None,
            });
        }

        // `VALUES (…), (…)`: a column is nullable if it is in any row.
        if !select.values_lists.is_empty() {
            self.record_all(&select.values_lists)?;
            let mut columns: Vec<(String, Col)> = Vec::new();
            for (row_idx, row) in select.values_lists.iter().enumerate() {
                let Some(N::List(row)) = &row.node else {
                    return Ok(None);
                };
                if row_idx == 0 {
                    columns = (1..=row.items.len())
                        .map(|i| (format!("column{}", i), NOT_NULL.into()))
                        .collect();
                } else if row.items.len() != columns.len() {
                    return Ok(None);
                }
                for (item, column) in row.items.iter().zip(columns.iter_mut()) {
                    if self.is_expression_nullable(item)? {
                        column.1 = NULLABLE.into();
                    }
                }
            }
            return Ok(Some(columns));
        }

        // Plain SELECT.
        let mut star = Some(Vec::new());
        for from_item in &select.from_clause {
            let item = self.analyze_from_item(from_item)?;
            star = match (star, item.star) {
                (Some(mut all), Some(cols)) => {
                    all.extend(cols);
                    Some(all)
                }
                _ => None,
            };
        }
        self.scope.star = star;

        // Refine column nullability using WHERE-derived predicates against
        // CHECK-derived facts.
        if let Some(where_clause) = &select.where_clause {
            self.refine_from_where(where_clause);
        }

        // GROUPING SETS / ROLLUP / CUBE (and the empty grouping set `()`)
        // produce rows in which grouped columns are NULL, and groups that can
        // be empty.
        self.scope.grouping_sets = select
            .group_clause
            .iter()
            .any(|g| matches!(g.node, Some(N::GroupingSet(_))));
        let plain_group_by = !select.group_clause.is_empty() && !self.scope.grouping_sets;

        // Every expression of this level, for column-reference provenance.
        self.record_all(&select.distinct_clause)?;
        self.record_all(&select.target_list)?;
        self.record_opt(select.where_clause.as_deref())?;
        self.record_all(&select.group_clause)?;
        self.record_opt(select.having_clause.as_deref())?;
        self.record_all(&select.window_clause)?;
        self.record_all(&select.sort_clause)?;
        self.record_opt(select.limit_offset.as_deref())?;
        self.record_opt(select.limit_count.as_deref())?;

        self.analyze_target_list(&select.target_list, plain_group_by)
    }

    /// The output columns of a target list (a SELECT's, or a RETURNING
    /// list), evaluated in the current scope.
    fn analyze_target_list(
        &self,
        target_list: &[protobuf::Node],
        plain_group_by: bool,
    ) -> anyhow::Result<SelectShape> {
        let mut columns: Vec<(String, Col)> = Vec::new();
        for target in target_list {
            let Some(N::ResTarget(res_target)) = &target.node else {
                continue;
            };
            let Some(val) = &res_target.val else {
                continue;
            };
            if self.is_star_expression(val) {
                let Some(mut expanded) = self.expand_star_expression(val) else {
                    return Ok(None);
                };
                if self.scope.grouping_sets {
                    for (_, col) in &mut expanded {
                        col.cn.nullable_on_base = true;
                    }
                }
                columns.extend(expanded);
            } else {
                let mut cn = self.expression_nullability_detailed(val)?;
                // array_agg/json_agg/… return NULL only over an empty input
                // set. Under a plain GROUP BY every group has at least one
                // row, so the aggregate is never NULL.
                if plain_group_by && self.group_aggregate_never_null(val)? {
                    cn.nullable_on_base = false;
                }
                // Output column name is the `AS alias` if present, else
                // best-effort derived from the expression. Missing names
                // become empty strings.
                let name = if !res_target.name.is_empty() {
                    res_target.name.clone()
                } else {
                    self.extract_column_name(val).unwrap_or_default()
                };
                // Only a plain column reference hands its source on: a cast
                // or any other expression has a type of its own.
                let source = match &val.node {
                    Some(N::ColumnRef(col_ref)) => {
                        self.resolve_column_ref(col_ref).and_then(|col| col.source)
                    }
                    _ => None,
                };
                columns.push((name, Col { cn, source }));
            }
        }
        Ok(Some(columns))
    }

    /// Merge two set-operation arms (UNION / INTERSECT / EXCEPT).
    ///
    /// PostgreSQL matches columns positionally. A column is NOT NULL iff *every*
    /// arm reports it NOT NULL. Mismatched lengths fall back to all-nullable for
    /// safety — we never claim NOT NULL on a column we couldn't verify on both
    /// arms. Output column names come from the left arm (matching pg's behaviour).
    /// A column keeps a source only when both arms read catalog columns of
    /// one declared type, which is then the type of the merged column.
    fn merge_set_op_columns(&self, larg: &[(String, Col)], rarg: &[(String, Col)]) -> Vec<(String, Col)> {
        if larg.len() != rarg.len() {
            return larg.iter().map(|(n, _)| (n.clone(), NULLABLE.into())).collect();
        }
        larg.iter()
            .zip(rarg.iter())
            .map(|((name, l), (_, r))| {
                (
                    name.clone(),
                    Col {
                        cn: ColumnNullability {
                            // Either side reaching the column through an outer join taints the union.
                            nullable_due_to_join: l.cn.nullable_due_to_join
                                || r.cn.nullable_due_to_join,
                            // Base-nullable on either arm makes the merged column base-nullable.
                            nullable_on_base: l.cn.nullable_on_base || r.cn.nullable_on_base,
                        },
                        source: self.common_source(l.source, r.source),
                    },
                )
            })
            .collect()
    }

    /// Declared type of a catalog column.
    fn source_type(&self, source: ColumnSource) -> Option<OID> {
        self.rel_index.column_type(source.rel_oid, &source.column)
    }

    /// The source two merged values share: the left one's, when both are
    /// catalog columns of the same declared type.
    fn common_source(
        &self,
        left: Option<ColumnSource>,
        right: Option<ColumnSource>,
    ) -> Option<ColumnSource> {
        let (left, right) = (left?, right?);
        let ty = self.source_type(left)?;
        (self.source_type(right)? == ty).then_some(left)
    }

    /// Resolve every column reference in `node` at the current query level,
    /// when provenance is being recorded. Subqueries met on the way are
    /// analyzed as nested levels that see this one.
    fn record(&mut self, node: &N) -> anyhow::Result<()> {
        if self.refs.is_none() {
            return Ok(());
        }
        match node {
            N::ColumnRef(col_ref) => {
                let source = self.resolve_column_ref(col_ref).and_then(|col| col.source);
                if let (Some(source), Some(refs)) = (source, self.refs.as_mut()) {
                    refs.insert(col_ref.location, source);
                }
                Ok(())
            }
            N::SubLink(sub_link) => {
                self.record_opt(sub_link.testexpr.as_deref())?;
                if let Some(N::SelectStmt(select)) =
                    sub_link.subselect.as_ref().and_then(|n| n.node.as_ref())
                {
                    self.analyze_correlated(select)?;
                }
                Ok(())
            }
            N::SelectStmt(select) => self.analyze_correlated(select).map(|_| ()),
            // FROM items and nested statements are walked where their scope
            // is built, not as expressions.
            N::InsertStmt(_)
            | N::UpdateStmt(_)
            | N::DeleteStmt(_)
            | N::MergeStmt(_)
            | N::RangeVar(_)
            | N::RangeSubselect(_)
            | N::RangeFunction(_)
            | N::JoinExpr(_) => Ok(()),
            other => {
                let mut result = Ok(());
                // A node kind whose children aren't enumerated is skipped:
                // its references stay unresolved.
                crate::query_introspector::for_each_child(other, &mut |child| {
                    if result.is_ok() {
                        result = self.record(child);
                    }
                });
                result
            }
        }
    }

    fn record_opt(&mut self, node: Option<&protobuf::Node>) -> anyhow::Result<()> {
        match node.and_then(|n| n.node.as_ref()) {
            Some(node) => self.record(node),
            None => Ok(()),
        }
    }

    fn record_all(&mut self, nodes: &[protobuf::Node]) -> anyhow::Result<()> {
        nodes.iter().try_for_each(|node| self.record_opt(Some(node)))
    }

    /// True iff `node` is a bare aggregate call that cannot be NULL over a
    /// non-empty group: `array_agg`/`json_agg`/… which return NULL *only* for
    /// an empty input set, and `string_agg` of a non-null value (it skips
    /// NULL inputs, so an all-NULL group yields NULL).
    ///
    /// `sum`/`avg`/`min`/`max`/`bool_or`/… are excluded: they return NULL
    /// over a non-empty group of NULL inputs.
    fn group_aggregate_never_null(&self, node: &protobuf::Node) -> anyhow::Result<bool> {
        let func = match node.node.as_ref() {
            Some(N::FuncCall(f)) => f,
            // A surrounding cast (`array_agg(x)::text[]`) doesn't change the value.
            Some(N::TypeCast(tc)) => {
                return match tc.arg.as_deref() {
                    Some(arg) => self.group_aggregate_never_null(arg),
                    None => Ok(false),
                };
            }
            _ => return Ok(false),
        };
        // A FILTER clause can shrink the input to empty even inside a
        // non-empty group, and so can a window frame.
        if func.agg_filter.is_some() || func.over.is_some() {
            return Ok(false);
        }
        match builtin_function_name(func) {
            Some("array_agg" | "json_agg" | "jsonb_agg" | "json_object_agg" | "jsonb_object_agg") => {
                Ok(true)
            }
            Some("string_agg") => match func.args.first() {
                Some(value) => Ok(!self.is_expression_nullable(value)?),
                None => Ok(false),
            },
            _ => Ok(false),
        }
    }

    /// Determine the columns produced by a `RangeFunction` (set-returning
    /// function in FROM), and the name the item goes by when it has no alias.
    ///
    /// The column list is `complete` only when we know how many columns every
    /// function returns: an explicit `AS t(a int, b text)` definition list, or
    /// the builtins in [`builtin_srf_columns`]. That count is also what
    /// locates the `WITH ORDINALITY` column among the alias's column names —
    /// a column alias list may be shorter than the column list, so "the last
    /// alias" is not it.
    ///
    /// Only `generate_series` values and the ordinality column are NOT NULL;
    /// everything else a function returns stays nullable.
    fn analyze_range_function(&self, range_fn: &protobuf::RangeFunction) -> (DerivedColumns, String) {
        let colnames: &[protobuf::Node] = range_fn
            .alias
            .as_ref()
            .map(|a| a.colnames.as_slice())
            .unwrap_or(&[]);
        let alias_name = range_fn
            .alias
            .as_ref()
            .map(|a| a.aliasname.as_str())
            .filter(|a| !a.is_empty());

        let calls: Vec<(Option<&protobuf::FuncCall>, bool)> = range_fn
            .functions
            .iter()
            .map(range_function_call)
            .collect();
        let function_name = |func: &protobuf::FuncCall| {
            func.funcname
                .last()
                .and_then(string_field)
                .unwrap_or_default()
                .to_string()
        };
        // A lone function without an alias is referred to by its own name.
        let single_function_name = match calls.as_slice() {
            [(Some(func), _)] => Some(function_name(func)),
            _ => None,
        };
        let item_name = alias_name
            .map(str::to_string)
            .or_else(|| single_function_name.clone())
            .unwrap_or_default();

        // The functions' output columns in order, if we know them all.
        let mut outputs: Option<Vec<(String, ColumnNullability)>> = Some(Vec::new());
        if !range_fn.coldeflist.is_empty() {
            // `AS t(col1 type1, col2 type2, …)`: named, but nothing proves a
            // value non-null.
            outputs = range_fn
                .coldeflist
                .iter()
                .map(|col| match &col.node {
                    Some(N::ColumnDef(def)) => Some((def.colname.clone(), NULLABLE)),
                    _ => None,
                })
                .collect();
        } else {
            for (func, has_coldefs) in &calls {
                let known = match func {
                    Some(func) if !has_coldefs => {
                        builtin_srf_columns(func).map(|cols| (function_name(func), cols))
                    }
                    _ => None,
                };
                match (outputs.as_mut(), known) {
                    (Some(outputs), Some((fn_name, cols))) => {
                        for (name, cn) in cols {
                            // A scalar result is named after the table alias
                            // when the function stands alone, else after the
                            // function.
                            let name = match name {
                                Some(name) => name.to_string(),
                                None if calls.len() == 1 => item_name.clone(),
                                None => fn_name.clone(),
                            };
                            outputs.push((name, cn));
                        }
                    }
                    _ => outputs = None,
                }
            }
        }

        let columns = match outputs {
            Some(mut outputs) => {
                if range_fn.ordinality {
                    outputs.push(("ordinality".to_string(), NOT_NULL));
                }
                // Rename before building the map: several functions' columns
                // share a default name until the alias list tells them apart.
                let outputs = outputs
                    .into_iter()
                    .map(|(name, cn)| (name, Col::from(cn)))
                    .collect();
                DerivedColumns::from_positional(rename_positional(outputs, colnames))
            }
            None => {
                // Unknown shape: record what can be referenced by name, all
                // nullable.
                let mut columns = IndexMap::new();
                if colnames.is_empty() {
                    if !item_name.is_empty() {
                        columns.insert(item_name.clone(), Col::from(NULLABLE));
                    }
                    if range_fn.ordinality {
                        columns
                            .entry("ordinality".to_string())
                            .or_insert(Col::from(NOT_NULL));
                    }
                } else {
                    for name in colnames.iter().filter_map(string_field) {
                        columns.insert(name.to_string(), Col::from(NULLABLE));
                    }
                }
                DerivedColumns {
                    columns,
                    complete: false,
                }
            }
        };
        (columns, item_name)
    }

    /// Analyze a nested `SelectStmt` that cannot refer to the current query
    /// level (a UNION arm, CTE body or non-LATERAL FROM subquery) in a fresh
    /// scope. Levels further out stay visible to it.
    fn analyze_subselect(&mut self, select: &protobuf::SelectStmt) -> anyhow::Result<SelectShape> {
        let saved = std::mem::take(&mut self.scope);
        let result = self.analyze_select_stmt(select);
        self.scope = saved;
        result
    }

    /// Analyze a nested `SelectStmt` that may refer to the current query
    /// level (a subquery in an expression, a LATERAL FROM item): it gets a
    /// fresh scope of its own and the current one becomes its innermost
    /// enclosing level.
    fn analyze_correlated(&mut self, select: &protobuf::SelectStmt) -> anyhow::Result<SelectShape> {
        let current = std::mem::take(&mut self.scope);
        self.outer.push(current);
        let result = self.analyze_select_stmt(select);
        self.scope = self.outer.pop().unwrap_or_default();
        result
    }

    /// A copy of this analyzer positioned to analyze a subquery written in an
    /// expression of the current level. Expression analysis only reads the
    /// analyzer, so a subquery met there is analyzed on the side.
    fn fork_for_sublink(&self) -> Self {
        let mut outer = self.outer.clone();
        outer.push(self.scope.clone());
        Self {
            rel_index: self.rel_index,
            scope: Scope::default(),
            outer,
            refs: None,
            view_nullability_cache: self.view_nullability_cache,
            cte_columns: self.cte_columns.clone(),
            relation_facts_cache: HashMap::new(),
            composite_index: self.composite_index,
        }
    }

    /// Register the CTEs of a `WITH` clause in `self.cte_columns`. Each CTE's
    /// body may itself be a SELECT (possibly with another WITH) or a
    /// data-modifying statement with `RETURNING`. A body we can't analyze is
    /// still registered, with unknown columns: the name must shadow any table
    /// of the same name.
    fn analyze_with_clause(&mut self, with: &protobuf::WithClause) -> anyhow::Result<()> {
        let ctes: Vec<&protobuf::CommonTableExpr> = with
            .ctes
            .iter()
            .filter_map(|n| match &n.node {
                Some(N::CommonTableExpr(c)) => Some(&**c),
                _ => None,
            })
            .collect();

        // Under WITH RECURSIVE every CTE name is in scope in every body,
        // including its own. Until a body has been analyzed its columns are
        // unknown.
        if with.recursive {
            for cte in &ctes {
                self.cte_columns
                    .insert(cte.ctename.clone(), DerivedColumns::default());
            }
        }

        for cte in ctes {
            let columns = self.analyze_cte_body(cte)?;
            self.cte_columns.insert(cte.ctename.clone(), columns);
        }
        Ok(())
    }

    /// Analyze a single CTE body, applying the CTE's column alias list
    /// (`WITH cte(a, b) AS …`). A shape we don't know how to introspect yields
    /// unknown columns.
    fn analyze_cte_body(&mut self, cte: &protobuf::CommonTableExpr) -> anyhow::Result<DerivedColumns> {
        let colnames = &cte.aliascolnames;
        let shape = match cte.ctequery.as_ref().and_then(|n| n.node.as_ref()) {
            Some(N::SelectStmt(select)) => self.analyze_subselect(select)?,
            Some(other) => match Dml::of(other) {
                Some(stmt) => self.analyze_dml(stmt)?,
                None => None,
            },
            None => None,
        };
        Ok(DerivedColumns::from_shape(shape, colnames))
    }

    /// Analyze a data-modifying statement — at the top level or in a CTE —
    /// and return the columns of its `RETURNING` list.
    ///
    /// The statement is a query level of its own: the target relation (under
    /// its alias) plus the `UPDATE … FROM` / `DELETE … USING` / `MERGE …
    /// USING` items, in that order. RETURNING expressions are evaluated in
    /// that scope like any SELECT target, so `RETURNING u.x`, `RETURNING
    /// coalesce(t.a, 0)` and `RETURNING *` follow the same rules.
    fn analyze_dml(&mut self, stmt: Dml<'_>) -> anyhow::Result<SelectShape> {
        let saved_scope = std::mem::take(&mut self.scope);
        let saved_ctes = stmt.with_clause().map(|_| self.cte_columns.clone());
        let result = match stmt.with_clause() {
            Some(with) => self.analyze_with_clause(with),
            None => Ok(()),
        }
        .and_then(|()| self.analyze_dml_body(stmt));
        if let Some(ctes) = saved_ctes {
            self.cte_columns = ctes;
        }
        self.scope = saved_scope;
        result
    }

    fn analyze_dml_body(&mut self, stmt: Dml<'_>) -> anyhow::Result<SelectShape> {
        let target = self.register_dml_target(stmt.target());
        let mut star = target.star;
        let add_star = |star: &mut Option<Vec<StarColumn>>, item: FromItem| {
            *star = match (star.take(), item.star) {
                (Some(mut all), Some(cols)) => {
                    all.extend(cols);
                    Some(all)
                }
                _ => None,
            };
        };

        match stmt {
            Dml::Insert(insert) => {
                // The rows' source query does not see the target; it is only
                // looked at for the column references in it.
                if self.refs.is_some() {
                    if let Some(N::SelectStmt(select)) =
                        insert.select_stmt.as_ref().and_then(|n| n.node.as_ref())
                    {
                        self.analyze_subselect(select)?;
                    }
                    if let Some(on_conflict) = &insert.on_conflict_clause {
                        self.record_on_conflict(on_conflict, &target.aliases)?;
                    }
                }
            }
            Dml::Update(update) => {
                for from in &update.from_clause {
                    let item = self.analyze_from_item(from)?;
                    add_star(&mut star, item);
                }
                // No WHERE refinement: RETURNING shows the row as updated,
                // which need not satisfy the WHERE that selected it.
                self.record_all(&update.target_list)?;
                self.record_opt(update.where_clause.as_deref())?;
            }
            Dml::Delete(delete) => {
                for using in &delete.using_clause {
                    let item = self.analyze_from_item(using)?;
                    add_star(&mut star, item);
                }
                // RETURNING shows the deleted row, which did satisfy WHERE.
                if let Some(where_clause) = &delete.where_clause {
                    self.refine_from_where(where_clause);
                }
                self.record_opt(delete.where_clause.as_deref())?;
            }
            Dml::Merge(merge) => {
                if let Some(source) = &merge.source_relation {
                    let item = self.analyze_from_item(source)?;
                    // `WHEN NOT MATCHED BY SOURCE` actions return rows with
                    // no source row.
                    self.mark_join_nullable(&item.aliases);
                }
                // The order `*` lists source and target columns in is not
                // something we rely on.
                star = None;
                self.record_opt(merge.join_condition.as_deref())?;
                self.record_all(&merge.merge_when_clauses)?;
            }
        }
        self.scope.star = star;

        let returning = stmt.returning_list();
        self.record_all(returning)?;
        self.analyze_target_list(returning, false)
    }

    /// Put a DML statement's target into the (fresh) scope. Only a base table
    /// lends its NOT NULL constraints: rows written through a view come back
    /// through its rules or triggers, so a view target's columns are known
    /// (with their declared types) but all nullable. A CTE of the same name
    /// does not shadow a DML target.
    fn register_dml_target(&mut self, target: Option<&protobuf::RangeVar>) -> FromItem {
        use crate::pg_rel::PgRelKind;
        let Some(target) = target else {
            self.scope.opaque = true;
            return FromItem {
                aliases: Vec::new(),
                star: None,
            };
        };
        let alias = target
            .alias
            .as_ref()
            .map(|a| a.aliasname.clone())
            .unwrap_or_else(|| target.relname.clone());
        let info = match self.rel_index.resolve_range_var(target) {
            Some(rel) if matches!(rel.kind, PgRelKind::Table) => TableInfo::Table {
                oid: rel.oid,
                is_nullable: false,
            },
            Some(rel) => TableInfo::View {
                oid: rel.oid,
                nullability: rel.columns.iter().map(|c| (c.to_string(), NULLABLE)).collect(),
                is_nullable: false,
            },
            None => TableInfo::unknown(),
        };
        self.register_from_item(alias, info)
    }

    /// Column references in `ON CONFLICT … [WHERE …] DO UPDATE SET … [WHERE
    /// …]`, which sees the target and, as `excluded`, the row proposed for
    /// insertion (same columns, but nothing is known to be NOT NULL in it
    /// before the insert's constraints have been checked).
    fn record_on_conflict(
        &mut self,
        on_conflict: &protobuf::OnConflictClause,
        target_aliases: &[String],
    ) -> anyhow::Result<()> {
        let excluded = target_aliases
            .first()
            .and_then(|alias| self.scope.aliases.get(alias))
            .and_then(|info| match info {
                TableInfo::Table { oid, .. } | TableInfo::View { oid, .. } => {
                    let rel = self.rel_index.get(oid)?;
                    Some(TableInfo::View {
                        oid: *oid,
                        nullability: rel.columns.iter().map(|c| (c.to_string(), NULLABLE)).collect(),
                        is_nullable: false,
                    })
                }
                TableInfo::Derived { .. } => None,
            })
            .unwrap_or_else(TableInfo::unknown);
        let had_excluded = self.scope.aliases.contains_key("excluded");
        if !had_excluded {
            self.scope.aliases.insert("excluded".to_string(), excluded);
        }
        let result = (|| {
            if let Some(infer) = &on_conflict.infer {
                self.record_all(&infer.index_elems)?;
                self.record_opt(infer.where_clause.as_deref())?;
            }
            self.record_all(&on_conflict.target_list)?;
            self.record_opt(on_conflict.where_clause.as_deref())
        })();
        if !had_excluded {
            self.scope.aliases.shift_remove("excluded");
        }
        result
    }

    /// Add a FROM item to the scope under `alias`.
    fn register_from_item(&mut self, alias: String, info: TableInfo) -> FromItem {
        if self.scope.aliases.contains_key(&alias) {
            // Two items under one name (`FROM a.t, b.t`): references to it
            // can't be told apart here, so neither is trusted.
            self.scope.aliases.insert(alias.clone(), TableInfo::unknown());
            return FromItem {
                aliases: vec![alias],
                star: None,
            };
        }
        // An item without a name can't be referenced by qualifier, but its
        // columns are still in scope.
        let alias = if alias.is_empty() {
            format!("?unnamed_{}?", self.scope.aliases.len())
        } else {
            alias
        };
        let star = self.column_names(&info).map(|names: Vec<String>| {
            names
                .into_iter()
                .map(|column| StarColumn::Of {
                    alias: alias.clone(),
                    column,
                })
                .collect()
        });
        self.scope.aliases.insert(alias.clone(), info);
        FromItem {
            aliases: vec![alias],
            star,
        }
    }

    /// Resolve a relation name in FROM. An unqualified name is a CTE first;
    /// otherwise the name is resolved by [`RelIndex::resolve`] — exact with a
    /// schema, along the search path without — to a base table or a view.
    /// Anything else is a relation with unknown columns.
    fn resolve_range_var(&self, schema: Option<&str>, name: &str) -> TableInfo {
        use crate::pg_rel::PgRelKind;
        if schema.is_none() {
            if let Some(cte) = self.cte_columns.get(name) {
                return TableInfo::Derived {
                    columns: cte.clone(),
                    is_nullable: false,
                };
            }
        }
        match self.rel_index.resolve(schema, name) {
            Some(rel) if matches!(rel.kind, PgRelKind::Table) => TableInfo::Table {
                oid: rel.oid,
                is_nullable: false,
            },
            Some(rel) => TableInfo::View {
                oid: rel.oid,
                nullability: match self.view_nullability_cache.get_by_oid(rel.oid) {
                    Some(nullability) => nullability.clone(),
                    // Not analyzed (inference off, or it failed): the columns
                    // are known, their nullability is not.
                    None => rel.columns.iter().map(|c| (c.to_string(), NULLABLE)).collect(),
                },
                is_nullable: false,
            },
            None => TableInfo::unknown(),
        }
    }

    /// Walk one FROM item into the scope. `is_nullable` starts false on every
    /// relation and is set by the joins above it.
    fn analyze_from_item(&mut self, from: &protobuf::Node) -> anyhow::Result<FromItem> {
        let Some(node) = &from.node else {
            return Ok(self.register_opaque_item());
        };
        match node {
            N::RangeVar(range_var) => {
                let schema = (!range_var.schemaname.is_empty())
                    .then_some(range_var.schemaname.as_str());
                let alias = range_var
                    .alias
                    .as_ref()
                    .map(|a| a.aliasname.clone())
                    .unwrap_or_else(|| range_var.relname.clone());
                let colnames: &[protobuf::Node] = range_var
                    .alias
                    .as_ref()
                    .map(|a| a.colnames.as_slice())
                    .unwrap_or(&[]);

                let mut info = self.resolve_range_var(schema, &range_var.relname);
                if !colnames.is_empty() {
                    // `FROM t AS a(x, y)` renames the columns positionally.
                    let columns = DerivedColumns::from_shape(
                        self.table_columns(&self.scope, &alias, &info),
                        colnames,
                    );
                    info = TableInfo::Derived {
                        columns,
                        is_nullable: false,
                    };
                }
                Ok(self.register_from_item(alias, info))
            }
            N::RangeSubselect(range_sub) => {
                // `(SELECT …) AS alias` in FROM, analyzed in its own scope.
                // A LATERAL one may refer to the FROM items before it.
                let shape = match range_sub.subquery.as_ref().and_then(|s| s.node.as_ref()) {
                    Some(N::SelectStmt(inner)) if range_sub.lateral => {
                        self.analyze_correlated(inner)?
                    }
                    Some(N::SelectStmt(inner)) => self.analyze_subselect(inner)?,
                    _ => None,
                };
                let (alias, colnames): (String, &[protobuf::Node]) = match &range_sub.alias {
                    Some(a) => (a.aliasname.clone(), &a.colnames),
                    None => (String::new(), &[]),
                };
                let columns = DerivedColumns::from_shape(shape, colnames);
                Ok(self.register_from_item(
                    alias,
                    TableInfo::Derived {
                        columns,
                        is_nullable: false,
                    },
                ))
            }
            N::RangeFunction(range_fn) => {
                // Set-returning functions like `unnest`, `generate_series`,
                // `jsonb_to_recordset`.
                // The arguments may refer to the FROM items before it.
                self.record_all(&range_fn.functions)?;
                let (columns, alias) = self.analyze_range_function(range_fn);
                Ok(self.register_from_item(
                    alias,
                    TableInfo::Derived {
                        columns,
                        is_nullable: false,
                    },
                ))
            }
            N::JoinExpr(join) => {
                let left = match &join.larg {
                    Some(larg) => self.analyze_from_item(larg)?,
                    None => FromItem {
                        aliases: Vec::new(),
                        star: None,
                    },
                };
                let right = match &join.rarg {
                    Some(rarg) => self.analyze_from_item(rarg)?,
                    None => FromItem {
                        aliases: Vec::new(),
                        star: None,
                    },
                };

                // An outer join makes the relations on its non-preserved
                // side(s) nullable.
                let join_type = self.get_join_type(join);
                match join_type {
                    JoinType::Inner => {}
                    JoinType::Left => self.mark_join_nullable(&right.aliases),
                    JoinType::Right => self.mark_join_nullable(&left.aliases),
                    JoinType::Full => {
                        self.mark_join_nullable(&left.aliases);
                        self.mark_join_nullable(&right.aliases);
                    }
                }

                let mut aliases = left.aliases.clone();
                aliases.extend(right.aliases.iter().cloned());

                self.record_opt(join.quals.as_deref())?;
                if let Some(alias) = &join.alias {
                    self.scope.join_aliases.insert(alias.aliasname.clone());
                }

                // `USING (...)` columns collapse to a single output column
                // sourced from the join's preserved side.
                let using: Vec<&str> = join.using_clause.iter().filter_map(string_field).collect();
                let merged: Vec<StarColumn> = using
                    .iter()
                    .map(|column| {
                        StarColumn::Merged(self.record_using_column(
                            column,
                            join_type,
                            &left.aliases,
                            &right.aliases,
                        ))
                    })
                    .collect();

                // `*` over a join: the merged columns, then the rest of the
                // left side, then the rest of the right side. A NATURAL join
                // merges every common column, which we don't work out — its
                // common columns have two owners and so resolve as nullable —
                // and a join alias hides the relations under it; in both
                // cases the expansion is unknown.
                let star = match (left.star, right.star) {
                    (Some(l), Some(r)) if !join.is_natural && join.alias.is_none() => {
                        let rest: Vec<StarColumn> = l
                            .into_iter()
                            .chain(r)
                            .filter(|c| !using.contains(&self.star_column_name(c)))
                            .collect();
                        Some(merged.into_iter().chain(rest).collect())
                    }
                    _ => None,
                };
                Ok(FromItem { aliases, star })
            }
            // XMLTABLE, JSON_TABLE, TABLESAMPLE, …: nothing is known about
            // their columns.
            _ => Ok(self.register_opaque_item()),
        }
    }

    /// Note a FROM item we know neither the name nor the columns of.
    fn register_opaque_item(&mut self) -> FromItem {
        self.scope.opaque = true;
        FromItem {
            aliases: Vec::new(),
            star: None,
        }
    }

    fn star_column_name<'s>(&'s self, column: &'s StarColumn) -> &'s str {
        match column {
            StarColumn::Of { column, .. } => column,
            StarColumn::Merged(idx) => self
                .scope
                .merged
                .get(*idx)
                .map(|m| m.name.as_str())
                .unwrap_or_default(),
        }
    }

    fn is_expression_nullable(&self, expr: &protobuf::Node) -> anyhow::Result<bool> {
        let Some(node) = &expr.node else {
            return Ok(true);
        };
        match node {
            N::ColumnRef(col_ref) => Ok(self.column_ref_nullability_detailed(col_ref).is_nullable()),
            // A literal is NULL only if it is the NULL keyword.
            N::AConst(constant) => Ok(constant.isnull),
            N::FuncCall(func) => self.is_function_nullable(func),
            N::AExpr(aexpr) => self.is_aexpr_nullable(aexpr),
            N::CaseExpr(case_expr) => self.is_case_expr_nullable(case_expr),
            // COALESCE is non-null if at least one argument is guaranteed non-null
            N::CoalesceExpr(coalesce) => self.is_coalesce_nullable(&coalesce.args),
            // GREATEST/LEAST skip NULL inputs and return NULL only when *all*
            // arguments are NULL — the same rule as COALESCE.
            N::MinMaxExpr(min_max) => self.is_coalesce_nullable(&min_max.args),
            // Type casts and COLLATE preserve nullability of the argument
            N::TypeCast(type_cast) => match &type_cast.arg {
                Some(arg) => self.is_expression_nullable(arg),
                None => Ok(true),
            },
            N::CollateClause(collate) => match &collate.arg {
                Some(arg) => self.is_expression_nullable(arg),
                None => Ok(true),
            },
            N::SubLink(sub_link) => self.is_sublink_nullable(sub_link),
            // Array constructor is never null (empty array is not null)
            N::AArrayExpr(_) => Ok(false),
            // ROW constructor creates a non-null composite value
            N::RowExpr(_) => Ok(false),
            N::FieldSelect(field_select) => self.is_field_select_nullable(field_select),
            // AIndirection is used for field selection like (t.item_subtotal).amount
            N::AIndirection(indirection) => self.is_aindirection_nullable(indirection),
            // IS [NOT] NULL and IS [NOT] TRUE/FALSE/UNKNOWN always return a
            // boolean.
            N::NullTest(_) | N::BooleanTest(_) => Ok(false),
            // AND / OR / NOT are NULL only if an operand is.
            N::BoolExpr(bool_expr) => self.check_function_args_nullable(&bool_expr.args),
            // CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_USER, … always have a
            // value. CURRENT_SCHEMA is NULL when the search path is empty.
            N::SqlvalueFunction(svf) => Ok(matches!(
                svf.op(),
                protobuf::SqlValueFunctionOp::SvfopCurrentSchema
                    | protobuf::SqlValueFunctionOp::SqlvalueFunctionOpUndefined
            )),
            _ => {
                // Default to nullable for unknown expressions
                log::trace!("Unknown expression, defaulting to nullable: {:?}", node);
                Ok(true)
            }
        }
    }

    fn extract_column_name(&self, node: &protobuf::Node) -> Option<String> {
        match &node.node {
            // The last field is the column name
            Some(N::ColumnRef(col_ref)) => col_ref
                .fields
                .last()
                .and_then(string_field)
                .map(str::to_string),
            _ => None,
        }
    }

    /// Check if an expression is a star expression (SELECT * or SELECT table.*)
    fn is_star_expression(&self, expr: &protobuf::Node) -> bool {
        match &expr.node {
            Some(N::ColumnRef(col_ref)) => matches!(
                col_ref.fields.last().and_then(|f| f.node.as_ref()),
                Some(N::AStar(_))
            ),
            _ => false,
        }
    }

    /// Whether we have the relation's full column list.
    fn columns_known(&self, table_info: &TableInfo) -> bool {
        match table_info {
            TableInfo::Table { oid, .. } => self.rel_index.contains_key(oid),
            TableInfo::View { .. } => true,
            TableInfo::Derived { columns, .. } => columns.complete,
        }
    }

    /// Column names of a relation in declaration order, `None` if unknown.
    fn column_names(&self, table_info: &TableInfo) -> Option<Vec<String>> {
        match table_info {
            TableInfo::Table { oid, .. } => {
                let rel = self.rel_index.get(oid)?;
                Some(rel.columns.iter().map(|c| c.to_string()).collect())
            }
            TableInfo::View { nullability, .. } => Some(nullability.keys().cloned().collect()),
            TableInfo::Derived { columns, .. } => columns
                .complete
                .then(|| columns.columns.keys().cloned().collect()),
        }
    }

    /// Every column of the relation registered as `alias` in `scope`, `None`
    /// if its column list is unknown.
    fn table_columns(
        &self,
        scope: &Scope,
        alias: &str,
        table_info: &TableInfo,
    ) -> Option<Vec<(String, Col)>> {
        self.column_names(table_info)?
            .into_iter()
            .map(|column| {
                let col = self.alias_column(scope, alias, table_info, &column)?;
                Some((column, col))
            })
            .collect()
    }

    /// `alias.column` in `scope`: its two-level nullability and the catalog
    /// column it is. `None` if the relation is not known to have such a
    /// column.
    fn alias_column(
        &self,
        scope: &Scope,
        alias: &str,
        table_info: &TableInfo,
        column: &str,
    ) -> Option<Col> {
        match table_info {
            TableInfo::Table { oid, is_nullable } => {
                let rel = self.rel_index.get(oid)?;
                let name = *rel.columns.iter().find(|c| c.as_str() == column)?;
                Some(Col {
                    cn: ColumnNullability {
                        nullable_due_to_join: *is_nullable,
                        nullable_on_base: !self.is_column_not_null(scope, alias, rel, column),
                    },
                    source: Some(ColumnSource {
                        rel_oid: *oid,
                        column: name,
                    }),
                })
            }
            TableInfo::View {
                oid,
                nullability,
                is_nullable,
            } => {
                let cached = nullability.get(column)?;
                Some(Col {
                    cn: ColumnNullability {
                        // Compose: the reference itself is join-nullable, or
                        // the column already was inside the view.
                        nullable_due_to_join: *is_nullable || cached.nullable_due_to_join,
                        nullable_on_base: cached.nullable_on_base,
                    },
                    source: Some(ColumnSource {
                        rel_oid: *oid,
                        column: Ustr::from(column),
                    }),
                })
            }
            TableInfo::Derived {
                columns,
                is_nullable,
            } => {
                let inner = columns.columns.get(column)?;
                Some(Col {
                    cn: ColumnNullability {
                        nullable_due_to_join: *is_nullable || inner.cn.nullable_due_to_join,
                        nullable_on_base: inner.cn.nullable_on_base,
                    },
                    source: inner.source,
                })
            }
        }
    }

    /// The relations in `scope` that have a column called `name`: how many,
    /// and the first. Postgres resolves an unqualified column only when
    /// exactly one relation owns it.
    fn column_owners<'s>(
        &self,
        scope: &'s Scope,
        name: &str,
    ) -> (usize, Option<(&'s str, &'s TableInfo)>) {
        let mut owners = scope
            .aliases
            .iter()
            .filter(|(alias, info)| self.alias_column(scope, alias, info, name).is_some())
            .map(|(alias, info)| (alias.as_str(), info));
        let first = owners.next();
        (first.iter().count() + owners.count(), first)
    }

    /// The `USING` merged column an unqualified `name` refers to in `scope`,
    /// if any. Two unrelated joins merging the same name make the reference
    /// ambiguous, hence nullable.
    fn merged_column(&self, scope: &Scope, name: &str) -> Option<Col> {
        let mut visible = scope.merged.iter().filter(|m| !m.hidden && m.name == name);
        let first = visible.next()?;
        Some(match visible.next() {
            None => Col {
                cn: first.nullability,
                source: first.source,
            },
            Some(_) => NULLABLE.into(),
        })
    }

    /// Expand a star expression (`SELECT *` or `SELECT t.*`). `None` if the
    /// columns it stands for are not all known.
    fn expand_star_expression(&self, expr: &protobuf::Node) -> Option<Vec<(String, Col)>> {
        let Some(N::ColumnRef(col_ref)) = &expr.node else {
            return None;
        };
        let scope = &self.scope;
        match col_ref.fields.as_slice() {
            [_] => scope
                .star
                .as_ref()?
                .iter()
                .map(|column| match column {
                    StarColumn::Of { alias, column } => {
                        let info = scope.aliases.get(alias)?;
                        Some((column.clone(), self.alias_column(scope, alias, info, column)?))
                    }
                    StarColumn::Merged(idx) => {
                        let merged = scope.merged.get(*idx)?;
                        Some((
                            merged.name.clone(),
                            Col {
                                cn: merged.nullability,
                                source: merged.source,
                            },
                        ))
                    }
                })
                .collect(),
            [table, _] => {
                let alias = string_field(table)?;
                self.table_columns(scope, alias, scope.aliases.get(alias)?)
            }
            _ => None,
        }
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
        let Some(node) = &expr.node else {
            return Ok(NULLABLE);
        };
        match node {
            N::ColumnRef(col_ref) => Ok(self.column_ref_nullability_detailed(col_ref)),
            N::AIndirection(ind) => {
                // `(expr).attr` inherits its base's two-level nullability
                // (a LEFT JOIN'd composite column projects a join-nullable
                // attribute); whether the attribute itself can be NULL is
                // a base-level property.
                let Some(arg) = &ind.arg else {
                    return Ok(NULLABLE);
                };
                let base = self.expression_nullability_detailed(arg)?;
                Ok(ColumnNullability {
                    nullable_due_to_join: base.nullable_due_to_join,
                    nullable_on_base: base.nullable_on_base
                        || !self.attribute_chain_not_null(arg, &ind.indirection),
                })
            }
            N::FieldSelect(fs) => {
                let Some(arg) = &fs.arg else {
                    return Ok(NULLABLE);
                };
                let base = self.expression_nullability_detailed(arg)?;
                Ok(ColumnNullability {
                    nullable_due_to_join: base.nullable_due_to_join,
                    nullable_on_base: base.nullable_on_base
                        || !self.field_select_not_null(arg, fs.fieldnum),
                })
            }
            // Type casts preserve the nullability of the argument
            N::TypeCast(type_cast) => match &type_cast.arg {
                Some(arg) => self.expression_nullability_detailed(arg),
                None => Ok(NULLABLE),
            },
            // Every other expression is computed: nullable or not as a whole.
            _ => Ok(ColumnNullability {
                nullable_due_to_join: false,
                nullable_on_base: self.is_expression_nullable(expr)?,
            }),
        }
    }

    /// Get two-level nullability for a column reference.
    fn column_ref_nullability_detailed(&self, col_ref: &protobuf::ColumnRef) -> ColumnNullability {
        let mut cn = self
            .resolve_column_ref(col_ref)
            .map_or(NULLABLE, |col| col.cn);
        if self.scope.grouping_sets {
            cn.nullable_on_base = true;
        }
        cn
    }

    /// Resolve a column reference: the one place it is done, for nullability,
    /// provenance and type alike. The reference is looked up at the current
    /// query level and, only when nothing there can own the name, at each
    /// enclosing level in turn (a correlated or LATERAL reference).
    ///
    /// `None` when the reference doesn't resolve to exactly one thing we
    /// know.
    fn resolve_column_ref(&self, col_ref: &protobuf::ColumnRef) -> Option<Col> {
        for scope in std::iter::once(&self.scope).chain(self.outer.iter().rev()) {
            match self.lookup_in(scope, col_ref) {
                Lookup::Found(mut col) => {
                    // The level it belongs to may emit it NULL-extended.
                    if scope.grouping_sets {
                        col.cn.nullable_on_base = true;
                    }
                    return Some(col);
                }
                Lookup::Unknown => return None,
                Lookup::NotHere => {}
            }
        }
        None
    }

    fn lookup_in(&self, scope: &Scope, col_ref: &protobuf::ColumnRef) -> Lookup {
        // A whole-row value is non-null unless an outer join nulls the row.
        let whole_row = |info: &TableInfo| {
            Col::from(ColumnNullability {
                nullable_due_to_join: info.is_join_nullable(),
                nullable_on_base: false,
            })
        };
        let found = |col: Option<Col>| col.map_or(Lookup::Unknown, Lookup::Found);
        match col_ref.fields.as_slice() {
            [field] => {
                let Some(name) = string_field(field) else {
                    return Lookup::Unknown;
                };

                // A `USING` join merges same-named columns into a single
                // output column whose nullability follows the preserved side.
                // That column exists in both relations, so the owner lookup
                // below would call it ambiguous.
                if let Some(col) = self.merged_column(scope, name) {
                    return Lookup::Found(col);
                }

                // Unqualified name: Postgres looks for a column first, and
                // only when no relation in scope has one does the name mean
                // the whole row of the relation with that alias. Two owners
                // would make the reference ambiguous (a query error); we
                // stay conservative.
                match self.column_owners(scope, name) {
                    (1, Some((alias, info))) => found(self.alias_column(scope, alias, info, name)),
                    // "No relation has one" needs every relation's columns
                    // to be known.
                    (0, _)
                        if !scope.opaque
                            && scope.aliases.values().all(|i| self.columns_known(i)) =>
                    {
                        match scope.aliases.get(name) {
                            Some(info) => Lookup::Found(whole_row(info)),
                            None => Lookup::NotHere,
                        }
                    }
                    _ => Lookup::Unknown,
                }
            }
            [table, field] => {
                let Some(alias) = string_field(table) else {
                    return Lookup::Unknown;
                };
                let Some(info) = scope.aliases.get(alias) else {
                    return if scope.opaque || scope.join_aliases.contains(alias) {
                        Lookup::Unknown
                    } else {
                        Lookup::NotHere
                    };
                };
                match field.node.as_ref() {
                    Some(N::String(column)) => {
                        found(self.alias_column(scope, alias, info, &column.sval))
                    }
                    // `t.*` in expression position is the whole row.
                    Some(N::AStar(_)) => Lookup::Found(whole_row(info)),
                    _ => Lookup::Unknown,
                }
            }
            // schema.table.column, or a column's composite attribute chain.
            _ => Lookup::Unknown,
        }
    }

    /// `alias` is the name the relation is registered under in `scope`:
    /// WHERE-derived refinements are per table reference.
    fn is_column_not_null(&self, scope: &Scope, alias: &str, rel: &PgRel, column_name: &str) -> bool {
        // Declared NOT NULL takes priority — cheapest check, most common case.
        let declared = rel.constraints.iter().any(|constraint| {
            matches!(constraint, Constraint::NotNull(not_null) if not_null.column.as_str() == column_name)
        });
        // Then consult predicate-derived refinements (active WHERE clause
        // combined with this relation's CHECK constraints).
        declared
            || scope
                .refined_not_null
                .get(alias)
                .is_some_and(|refined| refined.contains(column_name))
    }

    /// Walk the WHERE clause, extract predicates, and use them against each
    /// FROM-table's CHECK-derived facts to populate
    /// `self.scope.refined_not_null`.
    ///
    /// Each predicate is attributed to exactly one table reference: the one
    /// its qualifier names, or for an unqualified column the single relation
    /// in scope that has it. A predicate we can't place is dropped.
    ///
    /// This is intentionally best-effort: any unrecognized WHERE shape just
    /// produces no extra refinements, and unrecognized CHECK shapes likewise
    /// contribute nothing. We never claim NOT NULL on a column we can't prove.
    fn refine_from_where(&mut self, where_clause: &protobuf::Node) {
        use crate::parse_check_facts::Predicate;

        let mut by_alias: IndexMap<String, (OID, Vec<Predicate>)> = IndexMap::new();
        for scoped in crate::where_predicates::extract_predicates(where_clause) {
            let (Predicate::Eq { column, .. } | Predicate::IsNotNull { column }) =
                &scoped.predicate;
            let alias = match &scoped.qualifier {
                Some(qualifier) => qualifier.as_str(),
                // An unqualified name that a USING join merged is not a
                // column of either table.
                None if self.merged_column(&self.scope, column).is_some() => continue,
                None => match self.column_owners(&self.scope, column) {
                    (1, Some((alias, _))) => alias,
                    _ => continue,
                },
            };
            // Only base tables carry CHECK facts. Outer-joined tables are
            // skipped: their columns stay join-nullable whatever WHERE says.
            let Some(TableInfo::Table {
                oid,
                is_nullable: false,
            }) = self.scope.aliases.get(alias)
            else {
                continue;
            };
            by_alias
                .entry(alias.to_string())
                .or_insert_with(|| (*oid, Vec::new()))
                .1
                .push(scoped.predicate);
        }

        for (alias, (oid, predicates)) in by_alias {
            // Two independent sources of refinement:
            //   (a) The WHERE itself: both `IS NOT NULL` and `col = literal`
            //       (a non-NULL literal rules out NULL on the column) prove
            //       the column non-NULL.
            //   (b) An active predicate matches a CHECK-derived fact's
            //       antecedent.
            self.ensure_relation_facts(oid);
            let facts = self
                .relation_facts_cache
                .get(&oid)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let entry = self.scope.refined_not_null.entry(alias).or_default();
            for p in &predicates {
                let (Predicate::Eq { column, .. } | Predicate::IsNotNull { column }) = p;
                entry.insert(column.clone());
            }
            for fact in facts {
                if predicates.iter().any(|p| predicate_implies(p, &fact.when)) {
                    entry.insert(fact.column.clone());
                }
            }
        }
    }

    /// Fill `relation_facts_cache` for `oid` if not already present. Looks at
    /// every CHECK constraint on the relation and accumulates the facts.
    fn ensure_relation_facts(&mut self, oid: OID) {
        if self.relation_facts_cache.contains_key(&oid) {
            return;
        }
        let mut facts = Vec::new();
        if let Some(rel) = self.rel_index.get(&oid) {
            for c in &rel.constraints {
                if let Constraint::Check(check) = c {
                    if let Some(expr) = &check.check_expression {
                        let parsed = crate::parse_check_facts::parse_check_as_facts(expr);
                        facts.extend(parsed);
                    }
                }
            }
        }
        self.relation_facts_cache.insert(oid, facts);
    }

    /// Nullability of a function call. Only builtins have rules; a call that
    /// may resolve to a user function is nullable.
    fn is_function_nullable(&self, func: &protobuf::FuncCall) -> anyhow::Result<bool> {
        let Some(name) = builtin_function_name(func) else {
            return Ok(true);
        };
        let rule = match name {
            // `substring(string, start [, count])` is strict. The regex forms
            // return NULL when the pattern doesn't match; they take a string
            // where the positional form takes a number.
            "substring" => match func.args.get(1).and_then(|a| a.node.as_ref()) {
                Some(N::AConst(protobuf::AConst {
                    val: Some(protobuf::a_const::Val::Ival(_)),
                    ..
                })) => FnRule::Strict,
                _ => FnRule::Nullable,
            },
            _ => match builtin_function_rule(name) {
                Some(rule) => rule,
                None => return Ok(true), // Unknown functions default to nullable
            },
        };
        let strict_args = match rule {
            FnRule::Nullable => return Ok(true),
            // `f(VARIADIC arr)` with a NULL array is NULL even for functions
            // that otherwise tolerate NULL arguments.
            FnRule::NeverNull | FnRule::StrictPrefix(_) if func.func_variadic => &func.args[..],
            FnRule::NeverNull => return Ok(false),
            FnRule::Strict => &func.args[..],
            FnRule::StrictPrefix(n) => match func.args.get(..n) {
                Some(args) => args,
                None => return Ok(true),
            },
        };
        self.check_function_args_nullable(strict_args)
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

    /// An outer join can NULL-extend every relation in `aliases`, and with
    /// them any `USING` column merged entirely from those relations.
    fn mark_join_nullable(&mut self, aliases: &[String]) {
        for alias in aliases {
            if let Some(table_info) = self.scope.aliases.get_mut(alias) {
                table_info.set_join_nullable();
            }
        }
        for merged in &mut self.scope.merged {
            if merged.aliases.iter().all(|a| aliases.contains(a)) {
                merged.nullability.nullable_due_to_join = true;
            }
        }
    }

    /// Record the output column a join's `USING (column)` produces and return
    /// its index in `scope.merged`. The column equals
    /// `COALESCE(left.col, right.col)`, so it is NOT NULL exactly when the
    /// preserved side's column is NOT NULL:
    ///   * INNER — both sides present and equal; either source works, prefer
    ///     non-nullable.
    ///   * LEFT  — left preserved; follow the left side.
    ///   * RIGHT — right preserved; follow the right side.
    ///   * FULL  — neither preserved; the merged column is nullable.
    ///
    /// The column is resolved against the preserved side's relations, whose
    /// join-nullability is already up to date for this join. If it can't be
    /// attributed, the merged column is nullable.
    fn record_using_column(
        &mut self,
        column: &str,
        join_type: JoinType,
        left: &[String],
        right: &[String],
    ) -> usize {
        let preserved: Vec<&String> = match join_type {
            JoinType::Inner => left.iter().chain(right).collect(),
            JoinType::Left => left.iter().collect(),
            JoinType::Right => right.iter().collect(),
            JoinType::Full => Vec::new(),
        };

        let side_column = |aliases: &[String]| {
            aliases.iter().find_map(|alias| {
                let info = self.scope.aliases.get(alias)?;
                self.alias_column(&self.scope, alias, info, column)
            })
        };
        let mut best: Option<ColumnNullability> = None;
        for alias in preserved {
            let Some(col) = side_column(std::slice::from_ref(alias)) else {
                continue;
            };
            best = Some(match best {
                Some(prev) if prev.is_not_null() => prev,
                _ => col.cn,
            });
        }
        // The merged value has the two sides' common type.
        let source = self.common_source(
            side_column(left).and_then(|col| col.source),
            side_column(right).and_then(|col| col.source),
        );

        // This join's column replaces the same-named merged column of any
        // join nested inside it.
        let aliases: Vec<String> = left.iter().chain(right).cloned().collect();
        for merged in &mut self.scope.merged {
            if merged.name == column && merged.aliases.iter().all(|a| aliases.contains(a)) {
                merged.hidden = true;
            }
        }
        self.scope.merged.push(MergedColumn {
            name: column.to_string(),
            nullability: best.unwrap_or(NULLABLE),
            source,
            aliases,
            hidden: false,
        });
        self.scope.merged.len() - 1
    }

    /// CASE is non-null only when every branch is: an ELSE must exist (a
    /// missing ELSE yields NULL) and it and every THEN result must be
    /// non-null.
    fn is_case_expr_nullable(&self, case_expr: &protobuf::CaseExpr) -> anyhow::Result<bool> {
        let Some(defresult) = &case_expr.defresult else {
            return Ok(true);
        };
        if self.is_expression_nullable(defresult)? {
            return Ok(true);
        }
        for when_clause in &case_expr.args {
            let result = match &when_clause.node {
                Some(N::CaseWhen(when)) => when.result.as_deref(),
                _ => None,
            };
            match result {
                Some(result) if !self.is_expression_nullable(result)? => {}
                _ => return Ok(true),
            }
        }
        Ok(false)
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

    /// Operator expressions. The parser uses `AExpr` for much more than
    /// binary operators — `NULLIF`, `IN`, `BETWEEN`, `IS DISTINCT FROM` all
    /// arrive here with an operator name like `=` — so the kind decides.
    fn is_aexpr_nullable(&self, aexpr: &protobuf::AExpr) -> anyhow::Result<bool> {
        use protobuf::AExprKind;
        match aexpr.kind() {
            // IS [NOT] DISTINCT FROM treats NULL as a value.
            AExprKind::AexprDistinct | AExprKind::AexprNotDistinct => Ok(false),
            AExprKind::AexprOp => {
                let op = match aexpr.name.as_slice() {
                    [name] => string_field(name),
                    // `OPERATOR(schema.op)`: could be anyone's operator.
                    _ => None,
                };
                match op {
                    // Arithmetic, concatenation and comparison: NULL only if
                    // an operand is NULL. The left operand is absent for a
                    // prefix operator (`-x`).
                    Some(
                        "+" | "-" | "*" | "/" | "%" | "^" | "||" | "=" | "<>" | "!=" | "<" | ">"
                        | "<=" | ">=",
                    ) => {
                        if let Some(lexpr) = &aexpr.lexpr {
                            if self.is_operand_nullable(lexpr)? {
                                return Ok(true);
                            }
                        }
                        match &aexpr.rexpr {
                            Some(rexpr) => self.is_operand_nullable(rexpr),
                            None => Ok(true),
                        }
                    }
                    _ => {
                        log::trace!("Unknown operator in AExpr: {:?}", aexpr.name);
                        Ok(true)
                    }
                }
            }
            // `x IN (a, b)`, `x [I]LIKE p`, `x BETWEEN a AND b`: NULL only if
            // an operand (or list item) is NULL.
            AExprKind::AexprIn
            | AExprKind::AexprLike
            | AExprKind::AexprIlike
            | AExprKind::AexprBetween
            | AExprKind::AexprNotBetween
            | AExprKind::AexprBetweenSym
            | AExprKind::AexprNotBetweenSym => match (&aexpr.lexpr, &aexpr.rexpr) {
                (Some(lexpr), Some(rexpr)) => {
                    Ok(self.is_operand_nullable(lexpr)? || self.is_operand_nullable(rexpr)?)
                }
                _ => Ok(true),
            },
            // NULLIF returns NULL when its arguments are equal. `op ANY/ALL
            // (array)` is NULL for a NULL array or NULL elements. SIMILAR TO
            // and anything newer: unknown.
            _ => Ok(true),
        }
    }

    /// Nullability of an operator's operand for the purpose of "NULL in, NULL
    /// out". An operand list (`IN (…)`, `BETWEEN … AND …`) is nullable if any
    /// item is. A row constructor is itself non-null, but comparing rows
    /// yields NULL when their fields are NULL, so it counts as nullable.
    fn is_operand_nullable(&self, operand: &protobuf::Node) -> anyhow::Result<bool> {
        match &operand.node {
            Some(N::List(list)) => {
                for item in &list.items {
                    if self.is_operand_nullable(item)? {
                        return Ok(true);
                    }
                }
                Ok(list.items.is_empty())
            }
            Some(N::RowExpr(_)) => Ok(true),
            _ => self.is_expression_nullable(operand),
        }
    }

    fn is_sublink_nullable(&self, sub_link: &protobuf::SubLink) -> anyhow::Result<bool> {
        use protobuf::SubLinkType;

        match sub_link.sub_link_type() {
            // EXISTS is always true or false. `ARRAY(SELECT …)` is an empty
            // array, not NULL, when the subquery returns no rows.
            SubLinkType::ExistsSublink | SubLinkType::ArraySublink => Ok(false),
            SubLinkType::ExprSublink => {
                // Scalar subquery (`(SELECT expr)`): NULL when the inner
                // SELECT yields zero rows; otherwise its nullability is the
                // projected expression's.
                let select = match sub_link.subselect.as_ref().and_then(|n| n.node.as_ref()) {
                    Some(N::SelectStmt(s)) => s,
                    _ => return Ok(true),
                };

                // Multi-column subqueries can't appear in scalar position; bail.
                // (This also rejects set operations and VALUES, which have an
                // empty target list.)
                let [target] = select.target_list.as_slice() else {
                    return Ok(true);
                };
                let val = match &target.node {
                    Some(N::ResTarget(rt)) => match &rt.val {
                        Some(v) => v,
                        None => return Ok(true),
                    },
                    _ => return Ok(true),
                };

                // Anything that can discard the row.
                if !select.group_clause.is_empty()
                    || select.having_clause.is_some()
                    || select.limit_count.is_some()
                    || select.limit_offset.is_some()
                {
                    return Ok(true);
                }

                // We can promise the subquery returns exactly one row in two
                // cases:
                //   (a) No FROM and no WHERE — `(SELECT 1)`, `(SELECT NOW())` —
                //       and no set-returning function in the target, which
                //       would produce one row per element, possibly none.
                //   (b) A single bare aggregate — aggregates over zero rows
                //       still produce one row (the aggregate's own
                //       NULL/non-NULL contract takes over).
                let single_row = select.from_clause.is_empty()
                    && select.where_clause.is_none()
                    && self.is_srf_free(val);
                if !(single_row || is_aggregate_call(val)) {
                    return Ok(true);
                }

                // Exactly one row, then: the value is the subquery's single
                // output column. Analyze the subquery as the query level it
                // is — its own FROM items first, this level's after — so a
                // name means what it means there. count(*) -> not null,
                // max/sum/avg -> nullable (NULL on empty input), literals,
                // NOW(), correlated references etc. by the usual rules.
                let mut subquery = self.fork_for_sublink();
                Ok(match subquery.analyze_select_stmt(select)?.as_deref() {
                    Some([(_, col)]) => col.cn.is_nullable(),
                    _ => true,
                })
            }
            // `x IN (SELECT …)`, `x op ANY/ALL (SELECT …)` are NULL when
            // nothing matches and a NULL was compared. Everything else:
            // unknown.
            _ => Ok(true),
        }
    }

    /// True if `expr` provably contains no set-returning function call at
    /// its own query level. Conservative: any expression shape or function
    /// we don't know counts as possibly set-returning.
    fn is_srf_free(&self, expr: &protobuf::Node) -> bool {
        let all = |nodes: &[protobuf::Node]| nodes.iter().all(|n| self.is_srf_free(n));
        let opt = |node: &Option<Box<protobuf::Node>>| {
            node.as_deref().is_none_or(|n| self.is_srf_free(n))
        };
        let Some(node) = &expr.node else {
            return false;
        };
        match node {
            N::ColumnRef(_) | N::AConst(_) | N::ParamRef(_) | N::SqlvalueFunction(_) => true,
            N::TypeCast(e) => opt(&e.arg),
            N::CollateClause(e) => opt(&e.arg),
            N::NullTest(e) => opt(&e.arg),
            N::BooleanTest(e) => opt(&e.arg),
            N::AExpr(e) => opt(&e.lexpr) && opt(&e.rexpr),
            N::BoolExpr(e) => all(&e.args),
            N::CoalesceExpr(e) => all(&e.args),
            N::MinMaxExpr(e) => all(&e.args),
            N::AArrayExpr(e) => all(&e.elements),
            N::RowExpr(e) => all(&e.args),
            N::List(e) => all(&e.items),
            N::CaseExpr(e) => opt(&e.arg) && all(&e.args) && opt(&e.defresult),
            N::CaseWhen(e) => opt(&e.expr) && opt(&e.result),
            N::FuncCall(func) => {
                builtin_function_name(func).is_some_and(|name| builtin_function_rule(name).is_some())
                    && all(&func.args)
                    && all(&func.agg_order)
                    && opt(&func.agg_filter)
            }
            // A subquery is its own query level; only its test expression
            // (`x IN (SELECT …)`) belongs to ours.
            N::SubLink(e) => opt(&e.testexpr),
            _ => false,
        }
    }

    /// `FieldSelect` is the post-analysis form of attribute projection
    /// (raw parsing yields `AIndirection`); it addresses the attribute by
    /// `attnum` rather than by name.
    ///
    /// Nullable if the composite value can be NULL, or if nothing proves the
    /// selected attribute NOT NULL.
    fn is_field_select_nullable(
        &self,
        field_select: &protobuf::FieldSelect,
    ) -> anyhow::Result<bool> {
        let Some(arg) = &field_select.arg else {
            return Ok(true);
        };
        if self.is_expression_nullable(arg)? {
            return Ok(true);
        }
        Ok(!self.field_select_not_null(arg, field_select.fieldnum))
    }

    /// `(expr).a.b` — attribute projection as the raw parser produces it. All
    /// hops of one node share the node; nested parentheses produce nested
    /// nodes, which recurse through [`Self::is_expression_nullable`].
    ///
    /// Nullable if the base expression can be NULL, or if any hop's attribute
    /// isn't provably NOT NULL. Subscripts (`(expr).arr[1]`) and `.*` stay
    /// conservatively nullable.
    fn is_aindirection_nullable(
        &self,
        indirection: &protobuf::AIndirection,
    ) -> anyhow::Result<bool> {
        let Some(arg) = &indirection.arg else {
            return Ok(true);
        };
        if self.is_expression_nullable(arg)? {
            return Ok(true);
        }
        Ok(!self.attribute_chain_not_null(arg, &indirection.indirection))
    }

    /// Whether the attribute chain `hops`, projected out of `arg`, is
    /// provably NOT NULL *given that `arg` itself is*. `false` without
    /// evidence.
    fn attribute_chain_not_null(&self, arg: &protobuf::Node, hops: &[protobuf::Node]) -> bool {
        self.resolve_expression_type(arg)
            .and_then(|base| self.walk_attribute_chain(base, hops))
            .is_some_and(|a| a.not_null)
    }

    /// [`Self::attribute_chain_not_null`] for a `FieldSelect`, which names the
    /// attribute by `attnum`.
    fn field_select_not_null(&self, arg: &protobuf::Node, fieldnum: i32) -> bool {
        self.composite_index
            .zip(self.resolve_expression_type(arg))
            .and_then(|(index, base)| index.attribute_by_num(base, fieldnum))
            .is_some_and(|a| a.not_null)
    }

    /// Follow a chain of attribute names from a composite (or domain over
    /// composite) type. The result is NOT NULL only if every hop is.
    /// `None` if any hop isn't a plain attribute name or can't be resolved.
    fn walk_attribute_chain(&self, base_type: OID, hops: &[protobuf::Node]) -> Option<ResolvedAttr> {
        let index = self.composite_index?;
        let mut current = ResolvedAttr {
            type_oid: base_type,
            not_null: true,
        };
        for hop in hops {
            let name = match hop.node.as_ref()? {
                N::String(s) => &s.sval,
                _ => return None,
            };
            let attr = index.attribute(current.type_oid, name)?;
            current = ResolvedAttr {
                type_oid: attr.type_oid,
                not_null: current.not_null && attr.not_null,
            };
        }
        Some(current)
    }

    /// Best-effort static type of an expression, for the shapes that can sit
    /// under an attribute projection: a column reference (resolved through the
    /// FROM-clause aliases to the relation's declared column type) or a nested
    /// projection. Anything else — casts, function calls, row constructors,
    /// subqueries — yields `None`, keeping the projection conservatively
    /// nullable.
    fn resolve_expression_type(&self, expr: &protobuf::Node) -> Option<OID> {
        match expr.node.as_ref()? {
            N::ColumnRef(col_ref) => self.resolve_column_ref_type(col_ref),
            N::AIndirection(ind) => {
                let base = self.resolve_expression_type(ind.arg.as_deref()?)?;
                self.walk_attribute_chain(base, &ind.indirection)
                    .map(|a| a.type_oid)
            }
            N::FieldSelect(fs) => {
                let base = self.resolve_expression_type(fs.arg.as_deref()?)?;
                self.composite_index?
                    .attribute_by_num(base, fs.fieldnum)
                    .map(|a| a.type_oid)
            }
            _ => None,
        }
    }

    /// Declared type of the catalog column a `ColumnRef` resolves to (see
    /// [`Self::resolve_column_ref`]). `None` for anything without one: an
    /// unresolved reference, a computed column of a derived table, a `USING`
    /// column whose sides differ in type.
    fn resolve_column_ref_type(&self, col_ref: &protobuf::ColumnRef) -> Option<OID> {
        let source = self.resolve_column_ref(col_ref)?.source?;
        self.source_type(source)
    }
}

/// The string payload of a `ColumnRef` field node, if it is one.
fn string_field(node: &protobuf::Node) -> Option<&str> {
    match node.node.as_ref()? {
        N::String(s) => Some(&s.sval),
        _ => None,
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

    // ------------------------------------------------------------------
    // Attribute projections: `(col).attr`
    // ------------------------------------------------------------------

    const INT8_OID: OID = 20;
    const NUMERIC_OID: OID = 1700;
    const TEXT_OID: OID = 25;
    const TEXT_ARRAY_OID: OID = 1009;
    const DISTANCE_COMPOSITE_OID: OID = 50_001;
    const DISTANCE_DOMAIN_OID: OID = 50_002;
    const SPAN_COMPOSITE_OID: OID = 50_003;

    fn not_null(name: &str, column: &str) -> Constraint {
        Constraint::NotNull(NotNullConstraint {
            name: name.into(),
            column: column.into(),
        })
    }

    /// `package(package_id bigint NOT NULL, length distance NOT NULL,
    ///          optional_length distance, span _span NOT NULL)`
    /// and a view `package_dims(package_id, length)` over it.
    fn create_composite_rel_index() -> RelIndex {
        let mut rel_index = RelIndex::default();
        rel_index.insert(
            2001,
            PgRel {
                oid: 2001,
                id: PgId::new(Some("public".into()), "package".into()),
                kind: PgRelKind::Table,
                constraints: vec![
                    not_null("package_id_not_null", "package_id"),
                    not_null("length_not_null", "length"),
                    not_null("span_not_null", "span"),
                ],
                columns: vec![
                    "package_id".into(),
                    "length".into(),
                    "optional_length".into(),
                    "span".into(),
                ],
                column_types: vec![
                    INT8_OID,
                    DISTANCE_DOMAIN_OID,
                    DISTANCE_DOMAIN_OID,
                    SPAN_COMPOSITE_OID,
                ],
            },
        );
        rel_index.insert(
            2002,
            PgRel {
                oid: 2002,
                id: PgId::new(Some("public".into()), "package_dims".into()),
                kind: PgRelKind::View {
                    definition: "SELECT package_id, length FROM package".into(),
                },
                constraints: vec![],
                columns: vec!["package_id".into(), "length".into()],
                column_types: vec![INT8_OID, DISTANCE_DOMAIN_OID],
            },
        );
        rel_index
    }

    /// `_distance(amount numeric @pgrpc_not_null, unit text)`; domain
    /// `distance` over it whose CHECK proves `unit`; and
    /// `_span(length distance @pgrpc_not_null, label text, tags text[] @pgrpc_not_null)`.
    fn create_composite_index() -> CompositeIndex {
        use crate::composite_index::RawAttr;
        let mut index = CompositeIndex::empty();
        index.insert_composite(
            DISTANCE_COMPOSITE_OID,
            None,
            vec![
                RawAttr::new("amount", 1, NUMERIC_OID, Some("@pgrpc_not_null")),
                RawAttr::new("unit", 2, TEXT_OID, None),
            ],
        );
        index.insert_domain(
            DISTANCE_DOMAIN_OID,
            DISTANCE_COMPOSITE_OID,
            &["CHECK ((VALUE).unit IS NOT NULL)"],
        );
        index.insert_composite(
            SPAN_COMPOSITE_OID,
            None,
            vec![
                RawAttr::new("length", 1, DISTANCE_DOMAIN_OID, Some("@pgrpc_not_null")),
                RawAttr::new("label", 2, TEXT_OID, None),
                RawAttr::new("tags", 3, TEXT_ARRAY_OID, Some("@pgrpc_not_null")),
            ],
        );
        index
    }

    fn analyze_with_composites(
        view_def: &str,
        columns: &[&str],
    ) -> IndexMap<String, ColumnNullability> {
        let rel_index = create_composite_rel_index();
        let composite_index = create_composite_index();
        let mut cache = ViewNullabilityCache::new();
        let not_null = ColumnNullability {
            nullable_due_to_join: false,
            nullable_on_base: false,
        };
        cache.insert(
            2002,
            "public",
            "package_dims",
            IndexMap::from([
                ("package_id".to_string(), not_null),
                ("length".to_string(), not_null),
            ]),
        );
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache)
            .with_composite_index(&composite_index);
        let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        analyzer.analyze_view(view_def, &columns).unwrap()
    }

    #[test]
    fn test_attribute_projection_from_not_null_column() {
        let result = analyze_with_composites(
            "SELECT (p.length).amount AS amount, (p.length).unit AS unit FROM package p",
            &["amount", "unit"],
        );
        // `amount` via @pgrpc_not_null on the attribute; `unit` via the domain CHECK.
        assert_not_null(&result, "amount");
        assert_not_null(&result, "unit");
    }

    #[test]
    fn test_attribute_projection_from_nullable_column_stays_nullable() {
        let result = analyze_with_composites(
            "SELECT (p.optional_length).amount AS amount FROM package p",
            &["amount"],
        );
        assert_nullable(&result, "amount");
    }

    #[test]
    fn test_attribute_projection_unproven_attribute_stays_nullable() {
        let result = analyze_with_composites(
            "SELECT (p.span).label AS label, (p.span).length AS length FROM package p",
            &["label", "length"],
        );
        assert_nullable(&result, "label");
        assert_not_null(&result, "length");
    }

    #[test]
    fn test_nested_attribute_projection() {
        // Both spellings: one node with two hops, and two nested nodes.
        let result = analyze_with_composites(
            "SELECT (p.span).length.amount AS a, \
                    ((p.span).length).unit AS u, \
                    ((p.span).label).x AS bogus \
             FROM package p",
            &["a", "u", "bogus"],
        );
        assert_not_null(&result, "a");
        assert_not_null(&result, "u");
        // `label` is text, so `.x` resolves to nothing.
        assert_nullable(&result, "bogus");
    }

    #[test]
    fn test_attribute_projection_with_subscript_stays_nullable() {
        let result = analyze_with_composites(
            "SELECT (p.span).tags[1] AS first_tag, \
                    ((p.span).tags)[1] AS first_tag_nested, \
                    (p.span).tags AS tags \
             FROM package p",
            &["first_tag", "first_tag_nested", "tags"],
        );
        // Out-of-range subscripts yield NULL.
        assert_nullable(&result, "first_tag");
        assert_nullable(&result, "first_tag_nested");
        assert_not_null(&result, "tags");
    }

    #[test]
    fn test_attribute_projection_unqualified_single_table() {
        let result =
            analyze_with_composites("SELECT (length).amount AS amount FROM package", &["amount"]);
        assert_not_null(&result, "amount");
    }

    #[test]
    fn test_attribute_projection_through_view_column() {
        let result = analyze_with_composites(
            "SELECT (v.length).amount AS amount FROM package_dims v",
            &["amount"],
        );
        assert_not_null(&result, "amount");
    }

    #[test]
    fn test_attribute_projection_through_left_join_is_join_nullable() {
        let result = analyze_with_composites(
            "SELECT (p.length).amount AS amount \
             FROM package_dims v \
             LEFT JOIN package p ON p.package_id = v.package_id",
            &["amount"],
        );
        let amount = result.get("amount").unwrap();
        assert!(amount.nullable_due_to_join, "got {:?}", amount);
        assert!(!amount.nullable_on_base, "got {:?}", amount);
    }

    /// View dependencies must be found wherever a relation is read — CTE
    /// bodies, FROM subqueries, UNION branches, scalar subqueries — with CTE
    /// names and base tables excluded. A dependency missed here lets the view
    /// be analyzed before the view it reads from, which infers every column
    /// nullable, and which views that affects then varies from run to run.
    #[test]
    fn test_extract_view_dependencies_sees_through_ctes_subqueries_and_unions() {
        let mut rel_index = create_test_rel_index();
        for (oid, schema, name) in [
            (3001, "public", "user_stats"),
            (3002, "other", "post_view"),
            (3003, "public", "flagged"),
            // Never referenced: `recent` below is a CTE, which shadows it.
            (3004, "public", "recent"),
            // Same name as a dependency, but not the schema it is read from.
            (3005, "public", "post_view"),
        ] {
            rel_index.insert(oid, view_rel(oid, schema, name, &["id"]));
        }
        let cache = ViewNullabilityCache::new();
        let analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);

        let view_def = "WITH recent AS (SELECT * FROM user_stats), \
                             twice AS (SELECT * FROM recent) \
                        SELECT r.id \
                        FROM twice r \
                        JOIN (SELECT * FROM other.post_view) p ON p.id = r.id \
                        WHERE r.id IN (SELECT id FROM flagged) \
                        UNION ALL \
                        SELECT id FROM users";

        let deps = analyzer.extract_view_dependencies(view_def).unwrap();

        assert_eq!(deps, HashSet::from([3001, 3002, 3003]));

        // The same view, however it is named, is one dependency.
        let deps = analyzer
            .extract_view_dependencies("SELECT a.id FROM flagged a JOIN public.flagged b USING (id)")
            .unwrap();
        assert_eq!(deps, HashSet::from([3003]));
    }

    fn view_rel(oid: OID, schema: &str, name: &str, columns: &[&str]) -> PgRel {
        PgRel {
            oid,
            id: PgId::new(Some(schema.into()), name.into()),
            kind: PgRelKind::View {
                definition: String::new(),
            },
            constraints: vec![],
            columns: columns.iter().map(|c| (*c).into()).collect(),
            column_types: vec![],
        }
    }

    fn table_rel(oid: OID, schema: &str, name: &str, columns: &[(&str, bool)]) -> PgRel {
        PgRel {
            oid,
            id: PgId::new(Some(schema.into()), name.into()),
            kind: PgRelKind::Table,
            constraints: columns
                .iter()
                .filter(|(_, is_not_null)| *is_not_null)
                .map(|(c, _)| not_null(&format!("{name}_{c}_not_null"), c))
                .collect(),
            columns: columns.iter().map(|(c, _)| (*c).into()).collect(),
            column_types: vec![INT8_OID; columns.len()],
        }
    }

    // ------------------------------------------------------------------
    // Relation names: search path and the view cache
    // ------------------------------------------------------------------

    /// `app.t(a NOT NULL, b)` and `public.t(a, b NOT NULL)`, plus `hidden.h(a
    /// NOT NULL)` in a schema that is on no search path used here.
    fn two_schema_rel_index(search_path: &[&str]) -> RelIndex {
        let mut rel_index = RelIndex::default().with_search_path(search_path);
        rel_index.insert(1, table_rel(1, "app", "t", &[("a", true), ("b", false)]));
        rel_index.insert(2, table_rel(2, "public", "t", &[("a", false), ("b", true)]));
        rel_index.insert(3, table_rel(3, "hidden", "h", &[("a", true)]));
        rel_index
    }

    fn not_null_flags(rel_index: &RelIndex, cache: &ViewNullabilityCache, sql: &str, n: usize) -> Vec<bool> {
        let columns: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        ViewNullabilityAnalyzer::new(rel_index, cache)
            .analyze_view(sql, &columns)
            .unwrap()
            .values()
            .map(ColumnNullability::is_not_null)
            .collect()
    }

    #[test]
    fn test_unqualified_table_follows_the_search_path() {
        let cache = ViewNullabilityCache::new();
        let sql = "SELECT a, b FROM t";

        // First schema on the path that has the table, not `public`.
        let rel_index = two_schema_rel_index(&["app", "public"]);
        assert_eq!(not_null_flags(&rel_index, &cache, sql, 2), [true, false]);
        let rel_index = two_schema_rel_index(&["public", "app"]);
        assert_eq!(not_null_flags(&rel_index, &cache, sql, 2), [false, true]);
        // A schema without the table is skipped over.
        let rel_index = two_schema_rel_index(&["hidden", "app", "public"]);
        assert_eq!(not_null_flags(&rel_index, &cache, sql, 2), [true, false]);

        // Qualified names ignore the path.
        assert_eq!(
            not_null_flags(&rel_index, &cache, "SELECT a, b FROM public.t", 2),
            [false, true]
        );
        // Both at once, told apart by alias.
        assert_eq!(
            not_null_flags(
                &rel_index,
                &cache,
                "SELECT x.a, y.a FROM t x CROSS JOIN public.t y",
                2
            ),
            [true, false]
        );
    }

    #[test]
    fn test_unqualified_table_not_on_the_search_path_is_unknown() {
        let cache = ViewNullabilityCache::new();
        let rel_index = two_schema_rel_index(&["app", "public"]);
        // `h` exists once in the whole database, but not on the path:
        // Postgres would not find it, and neither do we.
        assert_eq!(not_null_flags(&rel_index, &cache, "SELECT a FROM h", 1), [false]);
        assert_eq!(not_null_flags(&rel_index, &cache, "SELECT a FROM hidden.h", 1), [true]);
        // Nor is its column list known, so `*` concludes nothing.
        assert_eq!(not_null_flags(&rel_index, &cache, "SELECT * FROM h", 1), [false]);
    }

    /// One cache entry per view, found however the query names the view.
    #[test]
    fn test_view_cache_is_hit_by_any_spelling_of_the_view() {
        let not_null_map =
            |flags: &[(&str, bool)]| -> ColumnMap {
                flags
                    .iter()
                    .map(|(c, is_not_null)| (c.to_string(), if *is_not_null { NOT_NULL } else { NULLABLE }))
                    .collect()
            };
        let mut rel_index = RelIndex::default().with_search_path(&["app", "public"]);
        rel_index.insert(10, view_rel(10, "public", "v", &["a", "b"]));
        rel_index.insert(11, view_rel(11, "app", "w", &["a", "b"]));
        // Shadowed by `app.w` on this path.
        rel_index.insert(12, view_rel(12, "public", "w", &["a", "b"]));
        let mut cache = ViewNullabilityCache::new();
        cache.insert(10, "public", "v", not_null_map(&[("a", true), ("b", false)]));
        cache.insert(11, "app", "w", not_null_map(&[("a", false), ("b", true)]));
        cache.insert(12, "public", "w", not_null_map(&[("a", true), ("b", true)]));

        for sql in ["SELECT a, b FROM v", "SELECT a, b FROM public.v", "SELECT * FROM v", "SELECT x.* FROM public.v x"] {
            assert_eq!(not_null_flags(&rel_index, &cache, sql, 2), [true, false], "{sql}");
        }
        // A view in a non-public schema, reached through the search path or by name.
        for sql in ["SELECT a, b FROM w", "SELECT a, b FROM app.w"] {
            assert_eq!(not_null_flags(&rel_index, &cache, sql, 2), [false, true], "{sql}");
        }
        assert_eq!(not_null_flags(&rel_index, &cache, "SELECT a, b FROM public.w", 2), [true, true]);

        // Lookups by name agree with lookups by OID.
        assert_eq!(cache.get_by_name("app", "w"), cache.get_by_oid(11));
        assert_eq!(cache.get_by_name("public", "v"), cache.get_by_oid(10));
        assert_eq!(cache.get_by_name("public", "w"), cache.get_by_oid(12));
        assert!(cache.get_by_name("app", "v").is_none());

        // A view the cache has nothing for: columns known, all nullable.
        rel_index.insert(13, view_rel(13, "public", "fresh", &["a", "b"]));
        assert_eq!(not_null_flags(&rel_index, &cache, "SELECT * FROM fresh", 2), [false, false]);
    }

    #[test]
    fn test_attribute_projection_without_composite_index_stays_nullable() {
        let rel_index = create_composite_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);
        let result = analyzer
            .analyze_view(
                "SELECT (p.length).amount AS amount FROM package p",
                &["amount".to_string()],
            )
            .unwrap();
        assert_nullable(&result, "amount");
    }

    // ------------------------------------------------------------------
    // Expression, scope and FROM-clause rules
    // ------------------------------------------------------------------

    /// `users` and `posts` from [`create_test_rel_index`], plus
    /// `comments(id NOT NULL, post_id, title, body)` and `tags(tag NOT NULL)`.
    fn create_wide_rel_index() -> RelIndex {
        let mut rel_index = create_test_rel_index();
        rel_index.insert(
            1003,
            PgRel {
                oid: 1003,
                id: PgId::new(Some("public".into()), "comments".into()),
                kind: PgRelKind::Table,
                constraints: vec![not_null("comments_id_not_null", "id")],
                columns: vec!["id".into(), "post_id".into(), "title".into(), "body".into()],
                column_types: vec![],
            },
        );
        rel_index.insert(
            1004,
            PgRel {
                oid: 1004,
                id: PgId::new(Some("public".into()), "tags".into()),
                kind: PgRelKind::Table,
                constraints: vec![not_null("tags_tag_not_null", "tag")],
                columns: vec!["tag".into()],
                column_types: vec![],
            },
        );
        rel_index
    }

    fn analyze(sql: &str, columns: &[&str]) -> IndexMap<String, ColumnNullability> {
        let rel_index = create_wide_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);
        let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        analyzer.analyze_view(sql, &columns).unwrap()
    }

    /// Analyze a query whose targets are all aliased; `expected` pairs each
    /// alias with whether it must come out NOT NULL.
    fn check(sql: &str, expected: &[(&str, bool)]) {
        let columns: Vec<&str> = expected.iter().map(|(c, _)| *c).collect();
        let result = analyze(sql, &columns);
        for (col, not_null) in expected {
            if *not_null {
                assert_not_null(&result, col);
            } else {
                assert_nullable(&result, col);
            }
        }
    }

    #[test]
    fn test_null_constants_are_nullable() {
        check(
            "SELECT NULL AS bare, \
                    NULL::text AS casted, \
                    COALESCE(name, NULL) AS coalesced, \
                    COALESCE(name, 'x') AS coalesced_literal, \
                    CASE WHEN id > 0 THEN 1 ELSE NULL END AS case_null_else, \
                    CASE WHEN id > 0 THEN NULL ELSE 1 END AS case_null_then, \
                    CASE WHEN id > 0 THEN 1 ELSE 2 END AS case_total, \
                    CASE WHEN id > 0 THEN 1 END AS case_no_else \
             FROM users",
            &[
                ("bare", false),
                ("casted", false),
                ("coalesced", false),
                ("coalesced_literal", true),
                ("case_null_else", false),
                ("case_null_then", false),
                ("case_total", true),
                ("case_no_else", false),
            ],
        );
    }

    #[test]
    fn test_aexpr_kinds() {
        check(
            "SELECT NULLIF(id, 0) AS nullif, \
                    id IS DISTINCT FROM 1 AS distinct_from, \
                    name IS NOT DISTINCT FROM 'x' AS not_distinct_from, \
                    id = ANY (ARRAY[1, 2]) AS op_any, \
                    id <> ALL (ARRAY[1, 2]) AS op_all, \
                    id IN (1, 2) AS in_list, \
                    id IN (1, NULL) AS in_list_null, \
                    name IN ('a', 'b') AS nullable_in_list, \
                    email LIKE 'a%' AS like_nn, \
                    name ILIKE 'a%' AS ilike_nullable, \
                    id BETWEEN 1 AND 2 AS between_nn, \
                    id NOT BETWEEN 1 AND NULL AS between_null, \
                    -id AS negated, \
                    id + 1 AS plus, \
                    (id, 1) = (1, NULL) AS row_compare, \
                    id OPERATOR(myschema.+) 1 AS foreign_op \
             FROM users",
            &[
                ("nullif", false),
                ("distinct_from", true),
                ("not_distinct_from", true),
                ("op_any", false),
                ("op_all", false),
                ("in_list", true),
                ("in_list_null", false),
                ("nullable_in_list", false),
                ("like_nn", true),
                ("ilike_nullable", false),
                ("between_nn", true),
                ("between_null", false),
                ("negated", true),
                ("plus", true),
                ("row_compare", false),
                ("foreign_op", false),
            ],
        );
    }

    #[test]
    fn test_boolean_and_keyword_expressions() {
        check(
            "SELECT id > 1 AND email = 'x' AS and_nn, \
                    name = 'x' OR true AS or_nullable, \
                    NOT (id > 1) AS not_nn, \
                    (name = 'x') IS TRUE AS is_true, \
                    (name = 'x') IS NOT UNKNOWN AS is_not_unknown, \
                    CURRENT_TIMESTAMP AS ts, \
                    CURRENT_DATE AS today, \
                    CURRENT_USER AS who, \
                    CURRENT_SCHEMA AS search_schema, \
                    email COLLATE \"C\" AS collated, \
                    name COLLATE \"C\" AS collated_nullable \
             FROM users",
            &[
                ("and_nn", true),
                ("or_nullable", false),
                ("not_nn", true),
                ("is_true", true),
                ("is_not_unknown", true),
                ("ts", true),
                ("today", true),
                ("who", true),
                ("search_schema", false),
                ("collated", true),
                ("collated_nullable", false),
            ],
        );
    }

    #[test]
    fn test_sublinks() {
        check(
            "SELECT id IN (SELECT user_id FROM posts) AS in_subquery, \
                    id = ANY (SELECT id FROM posts) AS any_subquery, \
                    id > ALL (SELECT id FROM posts) AS all_subquery, \
                    EXISTS (SELECT 1 FROM posts) AS has_posts, \
                    ARRAY(SELECT title FROM posts) AS titles \
             FROM users",
            &[
                ("in_subquery", false),
                ("any_subquery", false),
                ("all_subquery", false),
                ("has_posts", true),
                ("titles", true),
            ],
        );
    }

    #[test]
    fn test_scalar_subquery_row_count() {
        check(
            "SELECT (SELECT u.email) AS one_row, \
                    (SELECT u.email WHERE u.id > 5) AS filtered, \
                    (SELECT count(*) FROM posts) AS agg, \
                    (SELECT count(*) FROM posts WHERE false) AS agg_filtered, \
                    (SELECT count(*) OVER () FROM posts) AS windowed, \
                    (SELECT count(*) FROM posts OFFSET 1) AS agg_offset, \
                    (SELECT count(*) FROM posts LIMIT 0) AS agg_limit, \
                    (SELECT count(*) FROM posts HAVING count(*) > 1) AS agg_having, \
                    (SELECT 1 LIMIT 0) AS const_limit, \
                    (SELECT id FROM posts) AS from_table, \
                    (SELECT unnest(ARRAY[1])) AS srf, \
                    (SELECT ARRAY[generate_series(1, 0)]) AS nested_srf, \
                    (SELECT myschema.count(1)) AS foreign_count \
             FROM users u",
            &[
                ("one_row", true),
                ("filtered", false),
                ("agg", true),
                ("agg_filtered", true),
                ("windowed", false),
                ("agg_offset", false),
                ("agg_limit", false),
                ("agg_having", false),
                ("const_limit", false),
                ("from_table", false),
                ("srf", false),
                ("nested_srf", false),
                ("foreign_count", false),
            ],
        );
    }

    #[test]
    fn test_where_refinement_is_per_table_reference() {
        // Single table: bare or qualified.
        check(
            "SELECT title AS t FROM posts WHERE title IS NOT NULL",
            &[("t", true)],
        );
        check(
            "SELECT p.title AS t FROM posts p WHERE p.title IS NOT NULL",
            &[("t", true)],
        );
        // The predicate is about the outer-joined table, not the one that
        // happens to be the only inner-joined one.
        check(
            "SELECT p.title AS t FROM posts p LEFT JOIN comments c ON c.post_id = p.id \
             WHERE c.title IS NOT NULL",
            &[("t", false)],
        );
        // A qualifier that names nothing in scope (an outer query's table).
        check(
            "SELECT p.title AS t FROM posts p WHERE o.title IS NOT NULL",
            &[("t", false)],
        );
        // Self-join: only the reference the predicate names.
        check(
            "SELECT a.title AS a_title, b.title AS b_title \
             FROM posts a JOIN posts b ON a.id = b.user_id \
             WHERE a.title IS NOT NULL",
            &[("a_title", true), ("b_title", false)],
        );
        // Unqualified column in a join goes to the one table that has it.
        check(
            "SELECT p.content AS content, c.body AS body, p.title AS title \
             FROM posts p JOIN comments c ON c.post_id = p.id \
             WHERE content IS NOT NULL AND title = 'x'",
            &[("content", true), ("body", false), ("title", false)],
        );
    }

    #[test]
    fn test_star_expansion_follows_from_order() {
        let result = analyze(
            "SELECT * FROM users u JOIN posts p ON p.user_id = u.id LEFT JOIN tags t ON true",
            &["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"],
        );
        // users(id, name, email), posts(id, user_id, title, content), tags(tag)
        let not_null: Vec<bool> = result.values().map(|cn| cn.is_not_null()).collect();
        assert_eq!(
            not_null,
            vec![true, false, true, true, false, false, false, false]
        );
        assert!(result["c8"].nullable_due_to_join);
        assert!(!result["c8"].nullable_on_base);
    }

    #[test]
    fn test_star_expansion_merges_using_columns_once() {
        // id, then users(name, email), then posts(user_id, title, content).
        let result = analyze(
            "SELECT * FROM users JOIN posts USING (id)",
            &["id", "name", "email", "user_id", "title", "content"],
        );
        let not_null: Vec<bool> = result.values().map(|cn| cn.is_not_null()).collect();
        assert_eq!(not_null, vec![true, false, true, false, false, false]);
    }

    #[test]
    fn test_star_over_unknown_relation_concludes_nothing() {
        for sql in [
            "SELECT * FROM users u JOIN mystery m ON true",
            "SELECT * FROM mystery m JOIN users u ON true",
            "SELECT u.*, m.* FROM users u JOIN mystery m ON true",
            "SELECT * FROM users NATURAL JOIN posts",
            "SELECT * FROM users, some_function() f",
            "SELECT * FROM (SELECT * FROM mystery) s, users",
            "WITH c AS (SELECT * FROM mystery) SELECT * FROM users, c",
            "SELECT * FROM users u, posts u",
        ] {
            let result = analyze(sql, &["c1", "c2", "c3"]);
            for (col, cn) in &result {
                assert!(cn.is_nullable(), "{}: {} should be nullable", sql, col);
            }
        }
        // `u.*` alone is still fine next to an unknown relation.
        let result = analyze(
            "SELECT u.* FROM users u JOIN mystery m ON true",
            &["id", "name", "email"],
        );
        assert_not_null(&result, "id");
        assert_not_null(&result, "email");
    }

    #[test]
    fn test_column_count_mismatch_concludes_nothing() {
        let result = analyze("SELECT id, email FROM users", &["id", "email", "extra"]);
        assert_nullable(&result, "id");
        assert_nullable(&result, "email");
        assert_nullable(&result, "extra");
    }

    #[test]
    fn test_function_rules() {
        check(
            "SELECT regexp_match(email, 'a') AS re_match, \
                    substring(email from 'a.c') AS substring_regex, \
                    substring(email, 'a.c') AS substring_regex_call, \
                    substring(email from 2 for 3) AS substring_pos, \
                    substring(email, 2) AS substring_pos_call, \
                    substr(email, 2) AS substr_pos, \
                    array_dims(ARRAY[id]) AS dims, \
                    json_object(ARRAY[email, email]) AS json_obj, \
                    json_object(ARRAY[email], NULL) AS json_obj_nullable, \
                    json_build_object('k', name) AS json_built, \
                    json_build_object(VARIADIC NULL::text[]) AS json_built_variadic, \
                    format('%s', name) AS formatted, \
                    myschema.format('%s', email) AS foreign_format, \
                    pg_catalog.lower(email) AS catalog_lower, \
                    extract(year from now()) AS extracted, \
                    concat(name, email) AS concatenated, \
                    concat_ws(name, email) AS ws_nullable_sep, \
                    concat_ws(',', name, email) AS ws, \
                    array_to_string(ARRAY[id], name) AS joined_nullable_sep, \
                    array_to_string(ARRAY[id], ',', name) AS joined, \
                    ntile(id) OVER () AS tile, \
                    ntile(id + NULL) OVER () AS tile_null, \
                    lower(email) AS lowered, \
                    lower(name) AS lowered_nullable \
             FROM users",
            &[
                ("re_match", false),
                ("substring_regex", false),
                ("substring_regex_call", false),
                ("substring_pos", true),
                ("substring_pos_call", true),
                ("substr_pos", true),
                ("dims", false),
                ("json_obj", true),
                ("json_obj_nullable", false),
                ("json_built", true),
                ("json_built_variadic", false),
                ("formatted", true),
                ("foreign_format", false),
                ("catalog_lower", true),
                ("extracted", true),
                ("concatenated", true),
                ("ws_nullable_sep", false),
                ("ws", true),
                ("joined_nullable_sep", false),
                ("joined", true),
                ("tile", true),
                ("tile_null", false),
                ("lowered", true),
                ("lowered_nullable", false),
            ],
        );
    }

    #[test]
    fn test_group_by_aggregates() {
        check(
            "SELECT email AS email, \
                    array_agg(id) AS ids, \
                    array_agg(id) FILTER (WHERE id > 1) AS filtered_ids, \
                    string_agg(email, ',') AS emails, \
                    string_agg(name, ',') AS names, \
                    max(id) AS max_id, \
                    count(*) AS n \
             FROM users GROUP BY email",
            &[
                ("email", true),
                ("ids", true),
                ("filtered_ids", false),
                ("emails", true),
                ("names", false),
                ("max_id", false),
                ("n", true),
            ],
        );
        // No GROUP BY: one group, possibly empty.
        check("SELECT array_agg(id) AS ids FROM users", &[("ids", false)]);
    }

    #[test]
    fn test_grouping_sets_null_out_grouped_columns() {
        for group_by in [
            "ROLLUP (email, id)",
            "CUBE (email, id)",
            "GROUPING SETS ((email, id), ())",
            "email, ROLLUP (id)",
        ] {
            let sql = format!(
                "SELECT email AS email, id AS id, lower(email) AS lowered, \
                        array_agg(id) AS ids, count(*) AS n, COALESCE(email, 'all') AS label \
                 FROM users GROUP BY {}",
                group_by
            );
            check(
                &sql,
                &[
                    ("email", false),
                    ("id", false),
                    ("lowered", false),
                    ("ids", false),
                    ("n", true),
                    ("label", true),
                ],
            );
        }
    }

    #[test]
    fn test_with_ordinality_column_position() {
        // A partial column alias list names the function's column, not the
        // ordinality column.
        check(
            "SELECT elem AS elem, ordinality AS ord \
             FROM unnest(ARRAY[1, NULL]) WITH ORDINALITY AS t(elem)",
            &[("elem", false), ("ord", true)],
        );
        check(
            "SELECT t.elem AS elem, t.ord AS ord \
             FROM unnest(ARRAY[1, NULL]) WITH ORDINALITY AS t(elem, ord)",
            &[("elem", false), ("ord", true)],
        );
        check(
            "SELECT t.k AS k, t.v AS v, t.n AS n \
             FROM jsonb_each('{}'::jsonb) WITH ORDINALITY AS t(k, v, n)",
            &[("k", false), ("v", false), ("n", true)],
        );
        check(
            "SELECT g AS g, ordinality AS ord FROM generate_series(1, 3) WITH ORDINALITY AS g",
            &[("g", true), ("ord", true)],
        );
        // Several arrays: one column each, then the ordinality.
        check(
            "SELECT name AS name, age AS age, ord::int4 AS ord \
             FROM unnest($1::text[], $2::int4[]) WITH ORDINALITY AS t(name, age, ord)",
            &[("name", false), ("age", false), ("ord", true)],
        );
        // Unknown function: we can't tell which alias is the ordinality.
        check(
            "SELECT t.a AS a, t.b AS b FROM some_function() WITH ORDINALITY AS t(a, b)",
            &[("a", false), ("b", false)],
        );
        check(
            "SELECT ordinality AS ord FROM some_function() WITH ORDINALITY",
            &[("ord", true)],
        );
    }

    #[test]
    fn test_range_function_column_naming() {
        // A scalar function's column is named after the table alias, so the
        // bare alias is the (nullable) element, not a whole-row value.
        check(
            "SELECT x AS x FROM unnest(ARRAY[1, NULL]) AS x",
            &[("x", false)],
        );
        check(
            "SELECT generate_series AS n FROM generate_series(1, 3)",
            &[("n", true)],
        );
        check(
            "SELECT * FROM generate_series(1, 3) AS g, users",
            &[("g", true), ("id", true), ("name", false), ("email", true)],
        );
        check(
            "SELECT v.a AS a, v.b AS b FROM jsonb_to_recordset('[]') AS v(a int, b text)",
            &[("a", false), ("b", false)],
        );
    }

    #[test]
    fn test_using_columns_follow_enclosing_joins() {
        check(
            "SELECT id AS id FROM users JOIN posts USING (id)",
            &[("id", true)],
        );
        // The USING join as a whole is on the nullable side of an outer join.
        let result = analyze(
            "SELECT id AS id, tag AS tag \
             FROM tags LEFT JOIN (users JOIN posts USING (id)) ON true",
            &["id", "tag"],
        );
        assert!(result["id"].nullable_due_to_join, "got {:?}", result["id"]);
        assert_not_null(&result, "tag");
        check(
            "SELECT id AS id FROM users JOIN posts USING (id) RIGHT JOIN tags ON true",
            &[("id", false)],
        );
        check(
            "SELECT id AS id FROM users JOIN posts USING (id) FULL JOIN tags ON true",
            &[("id", false)],
        );
        // Same through `*`.
        let result = analyze(
            "SELECT * FROM tags LEFT JOIN (users JOIN posts USING (id)) ON true",
            &["tag", "id", "name", "email", "user_id", "title", "content"],
        );
        assert_not_null(&result, "tag");
        assert_nullable(&result, "id");
        assert_nullable(&result, "email");
        // Preserved side decides.
        check(
            "SELECT id AS id FROM users LEFT JOIN posts USING (id)",
            &[("id", true)],
        );
        check(
            "SELECT id AS id FROM users RIGHT JOIN posts USING (id)",
            &[("id", true)],
        );
        check(
            "SELECT title AS title FROM posts RIGHT JOIN comments USING (title)",
            &[("title", false)],
        );
        check(
            "SELECT id AS id FROM users FULL JOIN posts USING (id)",
            &[("id", false)],
        );
        // A chain re-merges the column; the outermost join decides.
        check(
            "SELECT id AS id FROM users FULL JOIN posts USING (id) JOIN comments USING (id)",
            &[("id", true)],
        );
        check(
            "SELECT id AS id FROM users JOIN posts USING (id) FULL JOIN comments USING (id)",
            &[("id", false)],
        );
    }

    #[test]
    fn test_natural_join_common_columns_are_nullable() {
        // `id` is common to both tables; we don't work out which side a
        // NATURAL join takes it from.
        check(
            "SELECT id AS id, email AS email, title AS title FROM users NATURAL LEFT JOIN posts",
            &[("id", false), ("email", true), ("title", false)],
        );
    }

    #[test]
    fn test_bare_name_prefers_column_over_alias() {
        // `name` is both the alias and a (nullable) column.
        check("SELECT name AS n FROM users name", &[("n", false)]);
        check("SELECT email AS e FROM users email", &[("e", true)]);
        // No such column: the whole row.
        check("SELECT u AS u FROM users u", &[("u", true)]);
        // ...unless a relation we can't see into might have that column.
        check("SELECT u AS u FROM users u, mystery m", &[("u", false)]);
        let result = analyze(
            "SELECT p AS p FROM users u LEFT JOIN posts p ON p.user_id = u.id",
            &["p"],
        );
        assert!(result["p"].nullable_due_to_join);
        assert!(!result["p"].nullable_on_base);
    }

    #[test]
    fn test_cte_shadows_table() {
        check(
            "WITH users AS (SELECT NULL::int AS id, 'x' AS tag) SELECT id AS id, tag AS tag FROM users",
            &[("id", false), ("tag", true)],
        );
        // A schema-qualified name is never a CTE.
        check(
            "WITH users AS (SELECT NULL::int AS id) SELECT id AS id FROM public.users",
            &[("id", true)],
        );
        // A CTE we can't analyze still shadows.
        check(
            "WITH users AS (SELECT * FROM mystery) SELECT id AS id FROM users",
            &[("id", false)],
        );
        // Inside its own (non-recursive) body the name still means the table.
        check(
            "WITH users AS (SELECT id, name AS email FROM users) SELECT id AS id, email AS email FROM users",
            &[("id", true), ("email", false)],
        );
        // Recursive: the self-reference has unknown columns.
        check(
            "WITH RECURSIVE users AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM users) \
             SELECT id AS id FROM users",
            &[("id", false)],
        );
    }

    #[test]
    fn test_cte_scope_ends_with_its_statement() {
        check(
            "SELECT id AS id, x AS x \
             FROM (WITH users AS (SELECT NULL::int AS id) SELECT 1 AS x FROM users) s, users",
            &[("id", true), ("x", true)],
        );
        check(
            "SELECT (SELECT 1) AS one, id AS id FROM users \
             WHERE EXISTS (WITH users AS (SELECT NULL::int AS id) SELECT 1 FROM users)",
            &[("one", true), ("id", true)],
        );
        // An inner WITH shadows an outer one only inside its statement.
        check(
            "WITH c AS (SELECT 1 AS v) \
             SELECT s.v AS inner_v, c.v AS outer_v \
             FROM (WITH c AS (SELECT NULL::int AS v) SELECT v FROM c) s, c",
            &[("inner_v", false), ("outer_v", true)],
        );
    }

    #[test]
    fn test_cte_returning() {
        check(
            "WITH ins AS (INSERT INTO users (id, email) VALUES (1, 'a') RETURNING id, name, email AS mail) \
             SELECT id AS id, name AS name, mail AS mail FROM ins",
            &[("id", true), ("name", false), ("mail", true)],
        );
        // A column of another table in RETURNING is not the target's.
        check(
            "WITH upd AS (UPDATE users u SET name = p.title FROM posts p RETURNING u.id AS uid, p.id AS pid) \
             SELECT uid AS uid, pid AS pid FROM upd",
            &[("uid", true), ("pid", true)],
        );
        // RETURNING * expands from the target, so `*` over the CTE is known.
        check(
            "WITH del AS (DELETE FROM users RETURNING *) SELECT * FROM del",
            &[("id", true), ("name", false), ("email", true)],
        );
        // Column aliases on the CTE rename the RETURNING list positionally.
        check(
            "WITH ins(a, b) AS (INSERT INTO users (id) VALUES (1) RETURNING id, name) \
             SELECT a AS a, b AS b FROM ins",
            &[("a", true), ("b", false)],
        );
    }

    // ------------------------------------------------------------------
    // RETURNING, evaluated like any target list over the DML scope
    // ------------------------------------------------------------------

    #[test]
    fn test_returning_insert() {
        check(
            "INSERT INTO users (id, email) VALUES (1, 'a') \
             RETURNING id AS id, name AS name, users.email AS mail, coalesce(name, 'x') AS n, \
                       id + 1 AS next, upper(name) AS up, 1 AS one",
            &[
                ("id", true),
                ("name", false),
                ("mail", true),
                ("n", true),
                ("next", true),
                ("up", false),
                ("one", true),
            ],
        );
        // The target under an alias; its own name is then not in scope.
        check(
            "INSERT INTO users AS u (id) VALUES (1) RETURNING u.id AS a, users.id AS b",
            &[("a", true), ("b", false)],
        );
        check(
            "INSERT INTO users (id) VALUES (1) RETURNING *",
            &[("id", true), ("name", false), ("email", true)],
        );
        check(
            "INSERT INTO users (id) VALUES (1) RETURNING users.*, id AS again",
            &[("id", true), ("name", false), ("email", true), ("again", true)],
        );
        // The rows' source is not in scope in RETURNING.
        check(
            "INSERT INTO users (id) SELECT p.id FROM posts p RETURNING id AS id, p.id AS pid",
            &[("id", true), ("pid", false)],
        );
        // Upsert: the row as it ends up, still under the table's constraints.
        check(
            "INSERT INTO users (id, email) VALUES (1, 'a') \
             ON CONFLICT (id) DO UPDATE SET email = excluded.email RETURNING id AS id, email AS email",
            &[("id", true), ("email", true)],
        );
    }

    #[test]
    fn test_returning_update() {
        check(
            "UPDATE users SET name = 'x' WHERE id = 1 RETURNING id AS id, name AS name",
            &[("id", true), ("name", false)],
        );
        // The WHERE that picked the row says nothing about the row as updated.
        check(
            "UPDATE users SET name = NULL WHERE name IS NOT NULL RETURNING name AS name",
            &[("name", false)],
        );
        // FROM items are in scope alongside the target.
        check(
            "UPDATE users u SET name = p.title FROM posts p WHERE p.user_id = u.id \
             RETURNING u.id AS uid, p.id AS pid, p.title AS title, u.name AS name",
            &[("uid", true), ("pid", true), ("title", false), ("name", false)],
        );
        // Unqualified and owned by both: ambiguous.
        check(
            "UPDATE users u SET name = 'x' FROM posts p RETURNING id AS id, title AS title, email AS email",
            &[("id", false), ("title", false), ("email", true)],
        );
        // An outer join inside FROM still nulls its inner side.
        check(
            "UPDATE users u SET name = 'x' FROM posts p LEFT JOIN comments c ON c.post_id = p.id \
             RETURNING p.id AS pid, c.id AS cid",
            &[("pid", true), ("cid", false)],
        );
        // `*` is the target's columns, then the FROM items'.
        check(
            "UPDATE users u SET name = 'x' FROM tags RETURNING *",
            &[("id", true), ("name", false), ("email", true), ("tag", true)],
        );
        // A subquery in FROM.
        check(
            "UPDATE users u SET name = 'x' FROM (SELECT id, title FROM posts) p RETURNING p.id AS pid",
            &[("pid", true)],
        );
        // `old` / `new` (PostgreSQL 18) are not relations we know.
        check(
            "UPDATE users SET name = 'x' RETURNING old.id AS before, new.id AS after",
            &[("before", false), ("after", false)],
        );
    }

    #[test]
    fn test_returning_delete() {
        check(
            "DELETE FROM users WHERE id = 1 RETURNING id AS id, name AS name, email AS email",
            &[("id", true), ("name", false), ("email", true)],
        );
        // The deleted row did satisfy WHERE.
        check(
            "DELETE FROM users WHERE name IS NOT NULL RETURNING name AS name",
            &[("name", true)],
        );
        check(
            "DELETE FROM users u USING posts p WHERE p.user_id = u.id \
             RETURNING u.id AS uid, p.id AS pid, p.content AS content",
            &[("uid", true), ("pid", true), ("content", false)],
        );
        check(
            "DELETE FROM users USING tags RETURNING *",
            &[("id", true), ("name", false), ("email", true), ("tag", true)],
        );
    }

    #[test]
    fn test_returning_merge() {
        // Target columns are those of the row written or deleted; a source
        // row may be absent (`WHEN NOT MATCHED BY SOURCE`).
        check(
            "MERGE INTO users u USING posts p ON p.user_id = u.id \
             WHEN MATCHED THEN UPDATE SET name = p.title \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (p.id) \
             RETURNING u.id AS uid, u.name AS name, p.id AS pid, merge_action() AS action",
            &[("uid", true), ("name", false), ("pid", false), ("action", false)],
        );
        check(
            "MERGE INTO users u USING posts p ON p.user_id = u.id \
             WHEN MATCHED THEN DELETE RETURNING u.*",
            &[("id", true), ("name", false), ("email", true)],
        );
        // Bare `*` over a MERGE is not expanded.
        let result = analyze(
            "MERGE INTO users u USING tags t ON true WHEN MATCHED THEN DELETE RETURNING *",
            &["id", "name", "email", "tag"],
        );
        assert_nullable(&result, "id");
    }

    #[test]
    fn test_returning_with_unresolved_or_view_target() {
        // Unknown target: nothing is known, `*` included.
        check("INSERT INTO nope (id) VALUES (1) RETURNING id AS id", &[("id", false)]);
        let result = analyze("DELETE FROM nope RETURNING *", &["id"]);
        assert_nullable(&result, "id");

        // A CTE feeding the statement is in scope for FROM, not as a target.
        check(
            "WITH src AS (SELECT id FROM posts) \
             UPDATE users u SET name = 'x' FROM src WHERE src.id = u.id RETURNING src.id AS sid, u.id AS uid",
            &[("sid", true), ("uid", true)],
        );

        // Written through a view: nothing is NOT NULL, however the view reads.
        let mut rel_index = create_wide_rel_index();
        rel_index.insert(3001, view_rel(3001, "public", "user_view", &["id", "email"]));
        let mut cache = ViewNullabilityCache::new();
        cache.insert(
            3001,
            "public",
            "user_view",
            IndexMap::from([("id".to_string(), NOT_NULL), ("email".to_string(), NOT_NULL)]),
        );
        assert_eq!(
            not_null_flags(&rel_index, &cache, "INSERT INTO user_view (id) VALUES (1) RETURNING id, email", 2),
            [false, false]
        );
        assert_eq!(
            not_null_flags(&rel_index, &cache, "SELECT id, email FROM user_view", 2),
            [true, true]
        );
    }

    // ------------------------------------------------------------------
    // Nested query levels: correlated and LATERAL references
    // ------------------------------------------------------------------

    #[test]
    fn test_lateral_subquery_sees_preceding_from_items() {
        check(
            "SELECT s.uid AS uid, s.nm AS nm, s.cid AS cid \
             FROM users u CROSS JOIN LATERAL \
                  (SELECT u.id AS uid, u.name AS nm, c.id AS cid FROM comments c WHERE c.post_id = u.id) s",
            &[("uid", true), ("nm", false), ("cid", true)],
        );
        // Unqualified, and not a column of anything inside: the outer item's.
        check(
            "SELECT s.mail AS mail FROM users u, LATERAL (SELECT email AS mail FROM tags) s",
            &[("mail", true)],
        );
        // LEFT JOIN LATERAL nulls the whole subquery row.
        let result = analyze(
            "SELECT s.uid AS uid FROM users u LEFT JOIN LATERAL (SELECT u.id AS uid) s ON true",
            &["uid"],
        );
        assert!(result["uid"].nullable_due_to_join);
        assert!(!result["uid"].nullable_on_base);
        // A reference to an item that an outer join already nulled.
        let result = analyze(
            "SELECT s.pid AS pid FROM users u LEFT JOIN posts p ON p.user_id = u.id \
             CROSS JOIN LATERAL (SELECT p.id AS pid) s",
            &["pid"],
        );
        assert!(result["pid"].nullable_due_to_join);
        // A set-returning function's arguments don't make its output known.
        check(
            "SELECT x AS x FROM users u, LATERAL unnest(ARRAY[u.id]) AS x",
            &[("x", false)],
        );
    }

    #[test]
    fn test_inner_names_shadow_outer_ones() {
        // The inner `u` is `posts`, which has no `email`: not the outer `u`.
        check(
            "SELECT s.v AS v FROM users u CROSS JOIN LATERAL (SELECT u.email AS v FROM posts u) s",
            &[("v", false)],
        );
        // `title` is owned by the inner relation.
        check(
            "SELECT s.v AS v FROM users u CROSS JOIN LATERAL (SELECT title AS v FROM posts) s",
            &[("v", false)],
        );
        // An inner relation with unknown columns may own any unqualified
        // name, so nothing is looked up further out.
        check(
            "SELECT s.v AS v FROM users u CROSS JOIN LATERAL (SELECT email AS v FROM mystery m) s",
            &[("v", false)],
        );
        check(
            "SELECT s.v AS v FROM users u CROSS JOIN LATERAL (SELECT u.email AS v FROM mystery m) s",
            &[("v", true)],
        );
        // Ambiguous inside stays ambiguous, whatever the outer level has.
        check(
            "SELECT s.v AS v FROM users u CROSS JOIN LATERAL \
                 (SELECT id AS v FROM posts p CROSS JOIN comments c) s",
            &[("v", false)],
        );
    }

    #[test]
    fn test_only_correlated_levels_see_the_enclosing_query() {
        // A plain (non-LATERAL) FROM subquery does not see its siblings.
        check(
            "SELECT s.v AS v FROM users u, (SELECT u.id AS v) s",
            &[("v", false)],
        );
        // Nor does a CTE of the same statement, or a set-operation arm.
        check(
            "WITH c AS (SELECT u.id AS v) SELECT c.v AS v FROM users u, c",
            &[("v", false)],
        );
        check(
            "SELECT id AS v FROM users u UNION ALL SELECT u.id FROM posts p",
            &[("v", false)],
        );
    }

    #[test]
    fn test_scalar_subquery_is_analyzed_as_its_own_level() {
        // One row guaranteed, value from the enclosing level.
        check(
            "SELECT (SELECT u.id) AS a, (SELECT u.name) AS b, (SELECT (SELECT u.email)) AS c, \
                    (SELECT coalesce(u.name, 'x')) AS d \
             FROM users u",
            &[("a", true), ("b", false), ("c", true), ("d", true)],
        );
        // The enclosing level's own nullability carries over.
        check(
            "SELECT (SELECT p.id) AS a FROM users u LEFT JOIN posts p ON p.user_id = u.id",
            &[("a", false)],
        );
        check(
            "SELECT (SELECT u.id) AS a FROM users u GROUP BY ROLLUP (u.id)",
            &[("a", false)],
        );
        check(
            "SELECT (SELECT u.name) AS a FROM users u WHERE u.name IS NOT NULL",
            &[("a", true)],
        );
        // Zero rows are possible: nullable, however NOT NULL the column.
        check(
            "SELECT (SELECT c.id FROM comments c WHERE c.post_id = p.id) AS a, \
                    (SELECT u.id FROM tags) AS b, \
                    (SELECT u.id WHERE u.name = 'x') AS c \
             FROM users u JOIN posts p ON p.user_id = u.id",
            &[("a", false), ("b", false), ("c", false)],
        );
        // An aggregate always yields its one row.
        check(
            "SELECT (SELECT count(*) FROM comments c WHERE c.post_id = p.id) AS n, \
                    (SELECT max(c.id) FROM comments c WHERE c.post_id = p.id) AS m \
             FROM posts p",
            &[("n", true), ("m", false)],
        );
        // In RETURNING too.
        check(
            "UPDATE users u SET name = 'x' RETURNING (SELECT u.id) AS a, \
                 (SELECT count(*) FROM posts p WHERE p.user_id = u.id) AS n, \
                 (SELECT p.id FROM posts p WHERE p.user_id = u.id) AS p",
            &[("a", true), ("n", true), ("p", false)],
        );
    }

    // ------------------------------------------------------------------
    // Provenance: which catalog column a reference or an output column is
    // ------------------------------------------------------------------

    fn analyze_parsed(sql: &str) -> StatementAnalysis {
        let rel_index = create_wide_rel_index();
        let cache = ViewNullabilityCache::new();
        let parsed = pg_query::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        ViewNullabilityAnalyzer::new(&rel_index, &cache)
            .analyze_parsed(&parsed.protobuf)
            .unwrap()
    }

    const USERS: OID = 1001;
    const POSTS: OID = 1002;
    const COMMENTS: OID = 1003;

    fn described(source: Option<ColumnSource>) -> Option<(OID, &'static str)> {
        source.map(|s| (s.rel_oid, s.column.as_str()))
    }

    /// Source of the column reference that starts at `needle` in `sql`.
    fn ref_source(sql: &str, needle: &str) -> Option<(OID, &'static str)> {
        let location = sql.find(needle).unwrap_or_else(|| panic!("`{needle}` not in `{sql}`"));
        described(analyze_parsed(sql).column_ref_source(location as i32))
    }

    fn output_sources(sql: &str) -> Vec<Option<(OID, &'static str)>> {
        analyze_parsed(sql)
            .columns
            .unwrap_or_else(|| panic!("unknown shape: {sql}"))
            .into_iter()
            .map(|c| described(c.source))
            .collect()
    }

    #[test]
    fn test_output_column_sources() {
        assert_eq!(
            output_sources(
                "SELECT u.id, email AS mail, p.title, upper(u.name) AS up, u.id::text AS cast_id, 1 AS one \
                 FROM users u JOIN posts p ON p.user_id = u.id"
            ),
            [
                Some((USERS, "id")),
                Some((USERS, "email")),
                Some((POSTS, "title")),
                None,
                None,
                None,
            ]
        );
        // `*`, in FROM order.
        assert_eq!(
            output_sources("SELECT * FROM tags t, users"),
            [Some((1004, "tag")), Some((USERS, "id")), Some((USERS, "name")), Some((USERS, "email"))]
        );
        // Through CTEs, subqueries, renames and LATERAL, as long as the
        // value stays a plain column.
        assert_eq!(
            output_sources(
                "WITH c(a, b) AS (SELECT id, lower(email) FROM users), d AS (SELECT a AS x, b AS y FROM c) \
                 SELECT s.x, s.y, l.t FROM (SELECT * FROM d) s, LATERAL (SELECT p.title AS t FROM posts p) l"
            ),
            [Some((USERS, "id")), None, Some((POSTS, "title"))]
        );
        // A CTE named like a table is the CTE.
        assert_eq!(
            output_sources("WITH posts AS (SELECT id AS title FROM users) SELECT title FROM posts"),
            [Some((USERS, "id"))]
        );
        // RETURNING: target and FROM items alike.
        assert_eq!(
            output_sources(
                "UPDATE users u SET name = p.title FROM posts p WHERE p.user_id = u.id \
                 RETURNING u.id, p.title, name, u.name || 'x' AS n"
            ),
            [Some((USERS, "id")), Some((POSTS, "title")), Some((USERS, "name")), None]
        );
        assert_eq!(
            output_sources("INSERT INTO users (id) VALUES (1) RETURNING *"),
            [Some((USERS, "id")), Some((USERS, "name")), Some((USERS, "email"))]
        );
        // Unresolved relation or ambiguous column: no provenance.
        assert_eq!(output_sources("SELECT m.id FROM mystery m"), [None]);
        assert_eq!(output_sources("SELECT title FROM posts, comments"), [None]);
        // Nothing but a SELECT or DML statement has output columns.
        assert!(analyze_parsed("CREATE TABLE z (a int)").columns.is_none());
    }

    #[test]
    fn test_column_ref_sources_are_resolved_in_their_own_scope() {
        // Inside a subquery: the subquery's table, not the outer query's.
        let sql = "SELECT 1 FROM users WHERE id IN (SELECT user_id FROM posts WHERE title = $1 AND id > 0)";
        assert_eq!(ref_source(sql, "title ="), Some((POSTS, "title")));
        assert_eq!(ref_source(sql, "id > 0"), Some((POSTS, "id")));
        assert_eq!(ref_source(sql, "id IN"), Some((USERS, "id")));

        // Inside a CTE, a FROM subquery, a set-operation arm.
        let sql = "WITH c AS (SELECT post_id FROM comments WHERE title = $1) \
                   SELECT 1 FROM posts p JOIN c ON c.post_id = p.id WHERE title = $2";
        assert_eq!(ref_source(sql, "title = $1"), Some((COMMENTS, "title")));
        assert_eq!(ref_source(sql, "title = $2"), Some((POSTS, "title")));
        assert_eq!(ref_source(sql, "c.post_id"), Some((COMMENTS, "post_id")));
        let sql = "SELECT id FROM posts WHERE title = $1 UNION SELECT id FROM comments WHERE title = $2";
        assert_eq!(ref_source(sql, "title = $1"), Some((POSTS, "title")));
        assert_eq!(ref_source(sql, "title = $2"), Some((COMMENTS, "title")));

        // Correlated: the inner level first, then the enclosing one.
        let sql = "SELECT 1 FROM users u WHERE EXISTS \
                   (SELECT 1 FROM posts p WHERE p.user_id = u.id AND name = $1 AND title = $2)";
        assert_eq!(ref_source(sql, "name ="), Some((USERS, "name")));
        assert_eq!(ref_source(sql, "title ="), Some((POSTS, "title")));
        assert_eq!(ref_source(sql, "u.id"), Some((USERS, "id")));
        assert_eq!(ref_source(sql, "p.user_id"), Some((POSTS, "user_id")));

        // A FROM subquery or CTE inside a correlated subquery still sees
        // the levels that enclose the whole subquery.
        let sql = "SELECT (SELECT s.v FROM (SELECT u.id AS v) s), \
                          (WITH c AS (SELECT u.name AS v) SELECT v FROM c) FROM users u";
        assert_eq!(ref_source(sql, "u.id"), Some((USERS, "id")));
        assert_eq!(ref_source(sql, "u.name"), Some((USERS, "name")));

        // Join conditions, function arguments in FROM, GROUP BY / HAVING /
        // ORDER BY.
        let sql = "SELECT count(*) FROM users u JOIN posts p ON p.user_id = u.id AND p.title = $1, \
                   LATERAL generate_series(1, p.id) g \
                   GROUP BY u.name HAVING u.name = $2 ORDER BY u.name";
        assert_eq!(ref_source(sql, "p.title"), Some((POSTS, "title")));
        assert_eq!(ref_source(sql, "p.id) g"), Some((POSTS, "id")));
        assert_eq!(ref_source(sql, "u.name = $2"), Some((USERS, "name")));

        // DML: SET values, WHERE, ON CONFLICT, the INSERT's source query.
        let sql = "UPDATE users SET name = email WHERE id = $1";
        assert_eq!(ref_source(sql, "email"), Some((USERS, "email")));
        assert_eq!(ref_source(sql, "id ="), Some((USERS, "id")));
        let sql = "INSERT INTO users (id) SELECT user_id FROM posts WHERE title = $1 \
                   ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.email = $2";
        assert_eq!(ref_source(sql, "title ="), Some((POSTS, "title")));
        assert_eq!(ref_source(sql, "excluded.name"), Some((USERS, "name")));
        assert_eq!(ref_source(sql, "users.email"), Some((USERS, "email")));
    }

    /// When the name cannot be pinned down, there is no source — in
    /// particular, a reference is never handed to an enclosing level just
    /// because the inner one could not resolve it.
    #[test]
    fn test_unresolvable_column_refs_have_no_source() {
        // Owned by two relations.
        let sql = "SELECT 1 FROM posts p, comments c WHERE title = $1";
        assert_eq!(ref_source(sql, "title ="), None);
        // The inner relation is unknown: `name` may be its column.
        let sql = "SELECT 1 FROM users u WHERE EXISTS (SELECT 1 FROM mystery m WHERE name = $1)";
        assert_eq!(ref_source(sql, "name ="), None);
        // An inner FROM item we cannot look into at all.
        let sql = "SELECT 1 FROM users u WHERE EXISTS \
                   (SELECT 1 FROM XMLTABLE('/r' PASSING xmlparse(document '<r/>') COLUMNS name text PATH 'n') x WHERE name = $1)";
        assert_eq!(ref_source(sql, "name ="), None);
        // An inner join alias hides the outer relation of the same name.
        let sql = "SELECT 1 FROM users j WHERE EXISTS \
                   (SELECT 1 FROM (posts p JOIN comments c ON true) AS j WHERE j.name = $1)";
        assert_eq!(ref_source(sql, "j.name"), None);
        // A CTE named like a table is not that table; computed columns have
        // no column behind them.
        let sql = "WITH users AS (SELECT 1 AS name) SELECT 1 FROM users WHERE name = $1";
        assert_eq!(ref_source(sql, "name ="), None);
        // Not visible from a plain FROM subquery or from an INSERT's source.
        let sql = "SELECT 1 FROM users u, (SELECT 1 AS one WHERE u.name = $1) s";
        assert_eq!(ref_source(sql, "u.name"), None);
        let sql = "INSERT INTO users (id) SELECT 1 WHERE users.name = $1";
        assert_eq!(ref_source(sql, "users.name"), None);
    }

    /// A function body ending in DML is still not analyzed.
    #[test]
    fn test_function_body_dml_is_not_analyzed() {
        let rel_index = create_wide_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);
        let result = analyzer
            .analyze_function_body("INSERT INTO users (id) VALUES (1) RETURNING id", &["id".to_string()])
            .unwrap();
        assert_nullable(&result, "id");
    }

    #[test]
    fn test_function_body_uses_last_statement() {
        let rel_index = create_wide_rel_index();
        let cache = ViewNullabilityCache::new();
        let mut analyzer = ViewNullabilityAnalyzer::new(&rel_index, &cache);
        let body = "SELECT name FROM users; SELECT id FROM users";
        let columns = vec!["out".to_string()];

        let as_function = analyzer.analyze_function_body(body, &columns).unwrap();
        assert_not_null(&as_function, "out");
        let as_query = analyzer.analyze_view(body, &columns).unwrap();
        assert_nullable(&as_query, "out");

        // A body that ends in something other than a SELECT is not analyzed.
        let dml_last = analyzer
            .analyze_function_body("SELECT id FROM users; DELETE FROM users RETURNING id", &columns)
            .unwrap();
        assert_nullable(&dml_last, "out");
    }

    #[test]
    fn test_nested_expressions_resolve_unqualified_columns_in_joins() {
        check(
            "SELECT lower(email) AS e, upper(title) AS t, content || email AS c, user_id + 1 AS n, \
                    COALESCE(body, email) AS fallback \
             FROM users u JOIN posts p ON p.user_id = u.id LEFT JOIN comments c ON c.post_id = p.id",
            &[
                ("e", true),
                ("t", false),
                ("c", false),
                ("n", false),
                ("fallback", true),
            ],
        );
        // Ambiguous between users and posts: conservative.
        check(
            "SELECT id + 1 AS n FROM users u JOIN posts p ON p.user_id = u.id",
            &[("n", false)],
        );
    }

    #[test]
    fn test_values_lists() {
        check(
            "SELECT v.a AS a, v.b AS b FROM (VALUES (1, 'x'), (2, NULL)) AS v(a, b)",
            &[("a", true), ("b", false)],
        );
        // Column aliases separate two same-named subquery columns.
        check(
            "SELECT s.uid AS uid, s.title AS title \
             FROM (SELECT u.id, p.id, p.title FROM users u LEFT JOIN posts p ON true) AS s(uid, pid)",
            &[("uid", true), ("title", false)],
        );
        check(
            "WITH s(uid, pid) AS (SELECT u.id, p.id FROM users u LEFT JOIN posts p ON true) \
             SELECT uid AS uid, pid AS pid FROM s",
            &[("uid", true), ("pid", false)],
        );
        // Without them the name is ambiguous.
        check(
            "SELECT s.id AS id FROM (SELECT u.id, p.id FROM users u JOIN posts p ON true) AS s",
            &[("id", false)],
        );
        check(
            "SELECT * FROM (VALUES (1, NULL)) AS v, tags",
            &[("column1", true), ("column2", false), ("tag", true)],
        );
    }

    #[test]
    fn test_table_column_aliases_rename_positionally() {
        // `u(email, id, x)` names users.id "email" and users.name "id".
        check(
            "SELECT u.id AS was_name, u.email AS was_id FROM users AS u(email, id, x)",
            &[("was_name", false), ("was_id", true)],
        );
        // A partial list leaves the third column as "email": two columns of
        // that name, so the reference is ambiguous.
        check(
            "SELECT u.email AS ambiguous FROM users AS u(email, id)",
            &[("ambiguous", false)],
        );
    }

    #[test]
    fn test_set_operations() {
        check(
            "SELECT id AS a, email AS b FROM users UNION ALL SELECT id, title FROM posts",
            &[("a", true), ("b", false)],
        );
        // One arm of unknown shape.
        check(
            "SELECT id AS a FROM users UNION ALL SELECT * FROM mystery",
            &[("a", false)],
        );
    }
}
