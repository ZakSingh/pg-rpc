use crate::cardinality_inference::CardinalityAnalyzer;
use crate::codegen::OID;
use crate::call_graph;
use crate::constraint_analysis::{constraints_from_deps, sql_rel_deps, trigger_rel_deps};
use crate::fn_index::FunctionIndex;
use itertools::Itertools;
use crate::exceptions::PgException;
use crate::pg_constraint::Constraint;
use crate::pg_id::PgId;
use crate::rel_index::RelIndex;
use crate::pg_rel::PgRel;
use crate::sql_parser::{ParameterSpec, ParsedQuery, QueryType};
use crate::trigger_index::TriggerIndex;
use crate::view_nullability::{StatementAnalysis, ViewNullabilityAnalyzer};
use anyhow::{Context, Result};
use pg_query::protobuf as pb;
use pg_query::protobuf::node::Node as N;
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
    /// True if this column's nullability comes from a LEFT/RIGHT/FULL JOIN.
    /// Used by double-underscore column grouping.
    pub nullable_due_to_join: bool,
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
    rel_index: &'a RelIndex,
    view_nullability_cache: &'a crate::view_nullability::ViewNullabilityCache,
    trigger_index: Option<&'a TriggerIndex>,
    domain_index: &'a crate::domain_index::DomainIndex,
    composite_index: &'a crate::composite_index::CompositeIndex,
    /// When given, functions the query calls contribute their exceptions and
    /// DML targets to the query's error enum (see `FunctionIndex::follow_calls`).
    fn_index: Option<&'a FunctionIndex>,
    /// Typed-error configuration, so direct calls to the raise helper in a
    /// query are detected.
    errors_config: Option<&'a crate::config::ErrorsConfig>,
}

impl<'a> QueryIntrospector<'a> {
    pub fn new(
        rel_index: &'a RelIndex,
        view_nullability_cache: &'a crate::view_nullability::ViewNullabilityCache,
        trigger_index: Option<&'a TriggerIndex>,
        domain_index: &'a crate::domain_index::DomainIndex,
        composite_index: &'a crate::composite_index::CompositeIndex,
        fn_index: Option<&'a FunctionIndex>,
        errors_config: Option<&'a crate::config::ErrorsConfig>,
    ) -> Self {
        Self {
            rel_index,
            view_nullability_cache,
            trigger_index,
            domain_index,
            composite_index,
            fn_index,
            errors_config,
        }
    }

    /// Exceptions and per-table constraints for a query: its own DML plus
    /// everything reachable through the functions it calls.
    fn analyze_errors(
        &self,
        query_name: &str,
        parsed: Option<&pg_query::ParseResult>,
    ) -> (Vec<PgException>, BTreeMap<PgId, Vec<Constraint>>) {
        let mut exceptions: Vec<PgException> = Vec::new();

        let Some(parsed) = parsed else {
            log::warn!(
                "Query {}: SQL did not parse; skipping error analysis",
                query_name
            );
            return (exceptions, BTreeMap::new());
        };

        let mut rel_deps = sql_rel_deps(parsed, self.rel_index, self.trigger_index, query_name);

        if let Some(cfg) = self.errors_config {
            exceptions.extend(
                crate::pg_fn::extract_custom_errors_from_queries(std::slice::from_ref(parsed), cfg)
                    .into_iter()
                    .map(PgException::CustomError),
            );
        }

        if let Some(fn_index) = self.fn_index {
            for target in call_graph::sql_callees(parsed) {
                // Query files have no schema of their own; unqualified names
                // resolve along the connection's search path.
                let Some(callee) = fn_index.resolve(&target, None) else { continue };
                log::debug!(
                    "Query {} calls {}.{}; folding its errors in",
                    query_name,
                    callee.schema,
                    callee.name
                );
                // Callee data is already transitive after `follow_calls`.
                exceptions.extend(callee.exceptions.iter().cloned());
                rel_deps.extend(callee.rel_deps.iter().cloned());
            }
        }

        // Tables written by the triggers that fire for this query's DML.
        if let Some(trigger_index) = self.trigger_index {
            rel_deps.extend(trigger_rel_deps(&rel_deps, trigger_index));
        }

        let (own_exceptions, table_dependencies) = constraints_from_deps(&rel_deps, self.rel_index);
        exceptions.extend(own_exceptions);
        let exceptions = exceptions.into_iter().unique().collect();
        (exceptions, table_dependencies)
    }

    /// Introspect a parsed query to determine parameter and return types
    pub fn introspect(&self, client: &mut Client, parsed: &ParsedQuery) -> Result<IntrospectedQuery> {
        log::info!(
            "Introspecting query '{}' from {}:{}",
            parsed.name,
            parsed.file_path.display(),
            parsed.line_number
        );

        // Parse once; every analysis below shares this AST. A query the
        // parser rejects will also fail to prepare, which reports the error.
        let ast = match pg_query::parse(&parsed.postgres_sql) {
            Ok(ast) => Some(ast),
            Err(e) => {
                log::debug!("Query '{}': pg_query could not parse SQL: {}", parsed.name, e);
                None
            }
        };
        let ast = ast.as_ref();

        // Name resolution for the whole statement, shared by everything that
        // asks what a column reference or an output column is.
        let analysis = ast.and_then(|ast| self.analyze_statement(&parsed.name, ast));
        let analysis = analysis.as_ref();

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
                let analyzer =
                    CardinalityAnalyzer::new(self.rel_index).with_fn_index(self.fn_index);
                let inferred = match ast {
                    Some(ast) => analyzer.infer_parsed(&ast.protobuf),
                    None => Err(anyhow::anyhow!("SQL did not parse")),
                };
                match inferred {
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
        let stmt = client
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
            ast,
            analysis,
            postgres_param_types.len(),
        );

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
                let mut cols =
                    self.introspect_return_columns_from_stmt(&parsed.name, &stmt, ast, analysis);
                // `-- @pgrpc_not_null(col1, col2)` lets the user assert NOT NULL
                // on columns whose nullability we couldn't (or won't) prove —
                // unnest output, jsonb_to_recordset values, lateral subqueries,
                // anywhere the user's invariants are stronger than the SQL.
                if !parsed.not_null_annotations.is_empty() {
                    for name in &parsed.not_null_annotations {
                        if !cols.iter().any(|col| &col.name == name) {
                            anyhow::bail!(
                                "query {}: @pgrpc_not_null names `{}`, which is not a result column",
                                parsed.name,
                                name
                            );
                        }
                    }
                    for col in &mut cols {
                        if parsed.not_null_annotations.contains(&col.name) {
                            if !col.nullable {
                                crate::annotations::warn_redundant(
                                    &format!("query {}", parsed.name),
                                    &col.name,
                                );
                            }
                            // An outer join is the one cause of NULL the
                            // analysis can prove; asserting NOT NULL against
                            // it will fail at decode time.
                            if col.nullable_due_to_join {
                                log::warn!(
                                    "query {}: @pgrpc_not_null on `{}`, which comes from the nullable \
                                     side of an outer join; rows without a match will fail to decode",
                                    parsed.name,
                                    col.name
                                );
                            }
                            col.nullable = false;
                            col.nullable_due_to_join = false;
                        }
                    }
                }
                Some(cols)
            }
            QueryType::Exec | QueryType::ExecRows => None,
        };

        // Analyze exceptions and table dependencies, following function calls
        let (exceptions, table_dependencies) =
            self.analyze_errors(&parsed.name, ast);

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

    /// Resolve the names in a statement: one analysis, by the same engine and
    /// the same rules as view nullability, that answers both "what is this
    /// output column" and "which column does this reference mean".
    fn analyze_statement(
        &self,
        query_name: &str,
        ast: &pg_query::ParseResult,
    ) -> Option<StatementAnalysis> {
        let mut analyzer = ViewNullabilityAnalyzer::new(self.rel_index, self.view_nullability_cache)
            .with_composite_index(self.composite_index);
        match analyzer.analyze_parsed(&ast.protobuf) {
            Ok(analysis) => Some(analysis),
            Err(e) => {
                log::warn!(
                    "Query {query_name}: could not apply nullability analysis, columns stay nullable: {e}"
                );
                None
            }
        }
    }

    /// Introspect return columns using the prepared statement metadata
    fn introspect_return_columns_from_stmt(
        &self,
        query_name: &str,
        stmt: &postgres::Statement,
        ast: Option<&pg_query::ParseResult>,
        analysis: Option<&StatementAnalysis>,
    ) -> Vec<QueryColumn> {
        // Get column info directly from the prepared statement
        let columns: Vec<QueryColumn> = stmt
            .columns()
            .iter()
            .map(|col| QueryColumn {
                name: col.name().to_string(),
                type_oid: col.type_().oid(),
                nullable: true, // Default to nullable, will be refined by analysis
                nullable_due_to_join: false,
            })
            .collect();

        for col in &columns {
            log::debug!(
                "[INTROSPECT] Column from statement: {} (OID: {})",
                col.name,
                col.type_oid
            );
        }

        self.refine_columns(query_name, ast, analysis, columns)
    }

    /// Refine the nullability and types of a statement's result columns — a
    /// SELECT's target list or a DML statement's RETURNING list — from its
    /// analysis.
    ///
    /// Columns are matched by position. Each takes the nullability the
    /// expression engine inferred. Its type is, in order: the domain it is
    /// cast to; the declared type of the catalog column it is a plain
    /// reference to (prepared-statement metadata reports a domain's base
    /// type); the type the prepared statement reported.
    ///
    /// Without an analysis, or when its column count differs from the
    /// statement's (we misread the query), every column stays nullable with
    /// its reported type.
    fn refine_columns(
        &self,
        query_name: &str,
        ast: Option<&pg_query::ParseResult>,
        analysis: Option<&StatementAnalysis>,
        columns: Vec<QueryColumn>,
    ) -> Vec<QueryColumn> {
        if columns.is_empty() {
            return columns;
        }
        if let Some(target) = ast.and_then(dml_target) {
            if self.rel_index.resolve_range_var(target).is_none() {
                log::warn!(
                    "Query {query_name}: DML target table '{}' not found in the relation index \
                     or on the search path; RETURNING columns stay nullable",
                    target.relname
                );
            }
        }
        let Some(outputs) = analysis
            .and_then(|a| a.columns.as_deref())
            .filter(|outputs| outputs.len() == columns.len())
        else {
            log::debug!("[REFINE] {query_name}: output shape unknown, columns stay nullable");
            return columns;
        };

        // Output column name -> domain OID for `expr::domain` casts.
        // Prepared-statement metadata reports the domain's *base* type for a
        // cast expression, so without this a `least(...)::currency` column
        // would generate the base composite (`_currency`) instead of the
        // `currency` domain that `pg_typeof` reports. Only domain casts are
        // recorded — for non-domain casts (`::int4`, `::text`) the
        // prepared-statement type is already authoritative.
        let cast_domain_map = ast
            .map(|ast| self.infer_cast_domain_types(&ast.protobuf))
            .unwrap_or_default();

        columns
            .into_iter()
            .zip(outputs)
            .map(|(col, output)| {
                let type_oid = cast_domain_map
                    .get(&col.name)
                    .copied()
                    .or_else(|| {
                        let source = output.source?;
                        self.rel_index.column_type(source.rel_oid, &source.column)
                    })
                    .unwrap_or(col.type_oid);
                log::debug!(
                    "[REFINE] Column '{}': nullable={} (join_nullable={}), type_oid={} -> {}",
                    col.name,
                    output.nullability.is_nullable(),
                    output.nullability.nullable_due_to_join,
                    col.type_oid,
                    type_oid
                );
                QueryColumn {
                    name: col.name,
                    type_oid,
                    nullable: output.nullability.is_nullable(),
                    nullable_due_to_join: output.nullability.nullable_due_to_join,
                }
            })
            .collect()
    }

    /// Find SELECT columns that are casts to a domain type (`expr::domain`) and
    /// map their output name to the domain's OID.
    ///
    /// Prepared-statement metadata resolves a cast result to the domain's base
    /// type, so a `least(...)::currency` column would otherwise be generated as
    /// the base type (e.g. the composite `_currency`) rather than the `currency`
    /// domain that `pg_typeof` reports. Only casts whose target is a domain are
    /// recorded — for casts to ordinary types the prepared-statement type is
    /// already correct.
    ///
    /// Cost: walks the already-parsed AST (no re-parse) and resolves cast
    /// type-names against the pre-loaded in-memory `DomainIndex` — no DB query.
    fn infer_cast_domain_types(
        &self,
        parse_result: &pg_query::protobuf::ParseResult,
    ) -> HashMap<String, OID> {
        let mut result = HashMap::new();

        // The list that names the result columns: a SELECT's target list, or
        // the RETURNING list of a data-modifying statement.
        use pg_query::protobuf::node::Node;
        let targets = match parse_result
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .and_then(|n| n.node.as_ref())
        {
            Some(Node::SelectStmt(select)) => &select.target_list,
            Some(Node::InsertStmt(insert)) => &insert.returning_list,
            Some(Node::UpdateStmt(update)) => &update.returning_list,
            Some(Node::DeleteStmt(delete)) => &delete.returning_list,
            Some(Node::MergeStmt(merge)) => &merge.returning_list,
            _ => return result,
        };

        for target in targets {
            let res_target = match &target.node {
                Some(pg_query::protobuf::node::Node::ResTarget(rt)) => rt,
                _ => continue,
            };
            let val = match &res_target.val {
                Some(v) => v,
                None => continue,
            };
            let type_cast = match &val.node {
                Some(pg_query::protobuf::node::Node::TypeCast(tc)) => tc,
                _ => continue,
            };

            // Reassemble the (possibly schema-qualified) target type name and
            // resolve it against the domain index. Bail early on non-domains so
            // we don't even compute the output name for ordinary casts.
            let type_name = match &type_cast.type_name {
                Some(tn) => tn,
                None => continue,
            };
            let name_parts: Vec<String> = type_name
                .names
                .iter()
                .filter_map(|n| match &n.node {
                    Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.clone()),
                    _ => None,
                })
                .collect();
            if name_parts.is_empty() {
                continue;
            }
            // Try the fully-qualified name, then the bare type name. The
            // DomainIndex stores both forms (bare omitted only when ambiguous
            // across schemas).
            let qualified_name = name_parts.join(".");
            let domain_oid = self
                .domain_index
                .get(&qualified_name)
                .or_else(|| self.domain_index.get(name_parts.last().unwrap()));
            let domain_oid = match domain_oid {
                Some(oid) => oid,
                None => continue,
            };

            // Output name: explicit alias, else the cast argument's column name.
            let output_name = if !res_target.name.is_empty() {
                res_target.name.clone()
            } else if let Some(arg) = &type_cast.arg {
                match self.extract_column_name_from_node(arg) {
                    Some(name) => name,
                    None => continue,
                }
            } else {
                continue;
            };

            result.insert(output_name, domain_oid);
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

    /// Determine parameter names, nullable flags, inferred type OIDs, and has_default from specs, inference, or fallback.
    /// Returns Vec of (name, nullable, Option<inferred_type_oid>, has_default).
    /// The inferred type OID comes from looking up the actual column type in RelIndex,
    /// which preserves domain types that PostgreSQL's type inference resolves to base types.
    fn determine_parameter_info(
        &self,
        param_specs: &[ParameterSpec],
        parse_result: Option<&pg_query::ParseResult>,
        analysis: Option<&StatementAnalysis>,
        param_count: usize,
    ) -> Vec<(String, bool, Option<OID>, bool)> {
        // Infer nullability from expression context (COALESCE, IS NULL, etc.)
        let context_nullability = infer_parameter_nullability_from_ast(parse_result, param_count);

        // Columns each parameter is compared with (`col = $N`), for names and types
        let compared = parse_result.map(compared_columns).unwrap_or_default();

        // Columns each parameter is written to (INSERT or UPDATE), for
        // column-based nullability/default inference
        let targets = parse_result.map(extract_param_targets).unwrap_or_default();

        let mut param_info = Vec::new();
        // Whether each name was written by the author (`:name`) or inferred.
        let mut inferred_name = Vec::new();

        for i in 0..param_count {
            let position = i + 1;

            // The column this parameter is compared with
            let compared_column = compared.get(&position);

            // The declared type of that column (preserves domain types),
            // when the reference resolves — in the scope it is written in —
            // to a catalog column. Not for `col = ANY($N)`: the parameter is
            // an array there.
            let inferred_type = compared_column
                .filter(|compared| compared.same_type)
                .and_then(|compared| analysis?.column_ref_source(compared.location))
                .and_then(|source| self.rel_index.column_type(source.rel_oid, &source.column));

            // Get context-inferred nullability for this parameter
            let context_nullable = context_nullability.get(i).copied().unwrap_or(false);

            // A parameter written to columns (in INSERT or UPDATE) may be NULL
            // if every one of them accepts it; and may be replaced by DEFAULT
            // if every one of them has a default.
            let param_targets = targets.get(&position);
            let has_default = param_targets
                .is_some_and(|t| t.default_capable && t.columns.iter().all(|c| self.column_has_default(c)));
            let column_nullable = param_targets.is_some_and(|t| {
                t.columns.iter().all(|c| {
                    self.column_allows_null(c)
                        || (t.default_capable && self.column_has_default(c))
                })
            });

            let nullable = context_nullable || column_nullable;

            // Priority 1: an explicit spec (`:name`, `$N`, `$N?`)
            let spec = param_specs.iter().find(|s| match s {
                ParameterSpec::Named { position: pos, .. } => *pos == position,
                ParameterSpec::Positional { position: pos, .. } => *pos == position,
            });
            let (name, explicit_nullable, inferred) = match spec {
                Some(ParameterSpec::Named { name, nullable, .. }) => (Some(name.clone()), *nullable, false),
                Some(ParameterSpec::Positional { nullable, .. }) => (None, *nullable, true),
                None => (None, false, true),
            };

            // Priority 2: the compared column's name. Priority 3: a fallback.
            let has_column = compared_column.is_some();
            let name = name
                .or_else(|| compared_column.map(|compared| compared.column.clone()))
                .unwrap_or_else(|| format!("param_{}", position));

            // (Kept from the original behavior: with neither a spec nor a
            // compared column, a parameter gets no default handling.)
            let has_default = has_default && (spec.is_some() || has_column);

            param_info.push((name, explicit_nullable || nullable, inferred_type, has_default));
            inferred_name.push(inferred);
        }

        // Two parameters compared with the same column (`a > $1 AND a < $2`,
        // `a IN ($1, $2)`) would both be named after it. Names the author
        // wrote are kept; inferred duplicates get their position appended.
        let mut used: std::collections::HashSet<String> = param_info
            .iter()
            .zip(&inferred_name)
            .filter(|(_, inferred)| !**inferred)
            .map(|(info, _)| info.0.clone())
            .collect();
        for (i, info) in param_info.iter_mut().enumerate() {
            if !inferred_name[i] {
                continue;
            }
            if used.contains(&info.0) {
                let mut candidate = format!("{}_{}", info.0, i + 1);
                while used.contains(&candidate) {
                    candidate.push('_');
                }
                info.0 = candidate;
            }
            used.insert(info.0.clone());
        }

        param_info
    }

    /// The relation a written-to column belongs to.
    fn target_rel(&self, column: &TargetColumn) -> Option<&PgRel> {
        self.rel_index
            .resolve(column.schema.as_deref(), &column.table)
    }

    /// Check if a column has a DEFAULT constraint.
    fn column_has_default(&self, column: &TargetColumn) -> bool {
        self.target_rel(column).is_some_and(|rel| {
            rel.constraints.iter().any(
                |c| matches!(c, Constraint::Default(d) if d.column.as_str() == column.column),
            )
        })
    }

    /// Check if a column accepts NULL (has no NOT NULL constraint).
    /// Conservative: if the table can't be found, assume it does not.
    fn column_allows_null(&self, column: &TargetColumn) -> bool {
        self.target_rel(column).is_some_and(|rel| {
            !rel.constraints.iter().any(
                |c| matches!(c, Constraint::NotNull(n) if n.column.as_str() == column.column),
            )
        })
    }

}

/// The target relation of a top-level INSERT / UPDATE / DELETE / MERGE.
fn dml_target(ast: &pg_query::ParseResult) -> Option<&pb::RangeVar> {
    match ast.protobuf.stmts.first()?.stmt.as_ref()?.node.as_ref()? {
        N::InsertStmt(stmt) => stmt.relation.as_ref(),
        N::UpdateStmt(stmt) => stmt.relation.as_ref(),
        N::DeleteStmt(stmt) => stmt.relation.as_ref(),
        N::MergeStmt(stmt) => stmt.relation.as_ref(),
        _ => None,
    }
}

/// A column a parameter is compared with: `col = $N`, `$N = col`,
/// `col IN ($N, ...)`, `col = ANY($N)`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ComparedColumn {
    /// Table name or alias the column was qualified with, if any
    qualifier: Option<String>,
    column: String,
    /// `location` of the `ColumnRef`, which identifies it in the statement's
    /// analysis.
    location: i32,
    /// Whether the parameter has the column's type. False for
    /// `col = ANY($N)`, where the parameter is an array of it.
    same_type: bool,
}

/// A column a parameter is written to.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetColumn {
    /// Schema the target table was qualified with, if any
    schema: Option<String>,
    table: String,
    column: String,
}

/// Where a parameter is written: `INSERT ... VALUES ($N)`,
/// `INSERT ... SELECT $N`, `UPDATE ... SET col = $N`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ParamTargets {
    columns: Vec<TargetColumn>,
    /// Whether the parameter can be swapped for the keyword `DEFAULT`: every
    /// place it appears in the statement is a bare VALUES item or SET value.
    /// (`SELECT DEFAULT`, `DEFAULT::int` and `WHERE x = DEFAULT` are errors.)
    default_capable: bool,
}

/// Extract ParamRef position from a node (handles TypeCast wrapping)
fn extract_param_position(node: &pb::Node) -> Option<usize> {
    match &node.node {
        Some(N::ParamRef(p)) => Some(p.number as usize),
        Some(N::TypeCast(tc)) => tc.arg.as_ref().and_then(|a| extract_param_position(a)),
        _ => None,
    }
}

/// A comparison operator: `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`.
fn is_comparison_op(name: &[pb::Node]) -> bool {
    match name.last().and_then(|n| n.node.as_ref()) {
        Some(N::String(s)) => {
            !s.sval.is_empty() && s.sval.chars().all(|c| matches!(c, '=' | '<' | '>' | '!'))
        }
        _ => false,
    }
}

/// A plain column reference as (qualifier, column, location).
fn plain_column_ref(node: &pb::Node) -> Option<(Option<String>, String, i32)> {
    let Some(N::ColumnRef(col_ref)) = &node.node else {
        return None;
    };
    let fields: Vec<&str> = col_ref
        .fields
        .iter()
        .map(|f| match &f.node {
            Some(N::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect::<Option<_>>()?;
    match fields.as_slice() {
        [column] => Some((None, (*column).to_string(), col_ref.location)),
        [.., qualifier, column] => Some((
            Some((*qualifier).to_string()),
            (*column).to_string(),
            col_ref.location,
        )),
        [] => None,
    }
}

/// For each parameter, the column it is compared with, found on the AST:
/// `col = $N` and `$N = col` (any comparison operator, the parameter
/// optionally cast), `col IN ($N, ...)`, and `col = ANY($N)`.
///
/// When a parameter is compared with several columns, the first comparison in
/// the query text wins.
fn compared_columns(parse_result: &pg_query::ParseResult) -> HashMap<usize, ComparedColumn> {
    // position -> (location of the comparison, column)
    let mut found: HashMap<usize, (i32, ComparedColumn)> = HashMap::new();
    let mut record = |position: usize, location: i32, column: ComparedColumn| {
        match found.get(&position) {
            Some((existing, _)) if *existing <= location => {}
            _ => {
                found.insert(position, (location, column));
            }
        }
    };

    walk_statements(parse_result, &mut |node| {
        let N::AExpr(a_expr) = node else { return };
        let (Some(lexpr), Some(rexpr)) = (a_expr.lexpr.as_deref(), a_expr.rexpr.as_deref()) else {
            return;
        };
        let column = |side: &pb::Node, same_type: bool| {
            plain_column_ref(side).map(|(qualifier, column, location)| ComparedColumn {
                qualifier,
                column,
                location,
                same_type,
            })
        };
        match a_expr.kind() {
            pb::AExprKind::AexprOp if is_comparison_op(&a_expr.name) => {
                for (column_side, param_side) in [(lexpr, rexpr), (rexpr, lexpr)] {
                    if let (Some(column), Some(position)) =
                        (column(column_side, true), extract_param_position(param_side))
                    {
                        record(position, a_expr.location, column);
                    }
                }
            }
            pb::AExprKind::AexprIn => {
                if let (Some(column), Some(N::List(list))) = (column(lexpr, true), &rexpr.node) {
                    for position in list.items.iter().filter_map(extract_param_position) {
                        record(position, a_expr.location, column.clone());
                    }
                }
            }
            pb::AExprKind::AexprOpAny if is_comparison_op(&a_expr.name) => {
                if let (Some(column), Some(position)) =
                    (column(lexpr, false), extract_param_position(rexpr))
                {
                    record(position, a_expr.location, column);
                }
            }
            _ => {}
        }
    });

    found
        .into_iter()
        .map(|(position, (_, column))| (position, column))
        .collect()
}

/// For INSERT/UPDATE statements anywhere in the query (including in CTEs),
/// the columns each parameter is written to.
///
/// - `INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)`: every row.
/// - `INSERT INTO t (a, b) SELECT $1, $2 ...`: the select list, by position.
/// - `UPDATE t SET a = $1`, and the same in `ON CONFLICT DO UPDATE SET`.
fn extract_param_targets(parse_result: &pg_query::ParseResult) -> HashMap<usize, ParamTargets> {
    // (position, column, whether this occurrence may become DEFAULT)
    let mut writes: Vec<(usize, TargetColumn, bool)> = Vec::new();
    // How often each parameter appears in the statement at all.
    let mut occurrences: HashMap<usize, usize> = HashMap::new();

    let mut record = |value: &pb::Node, relation: &pb::RangeVar, column: &str, in_values: bool| {
        if let Some(position) = extract_param_position(value) {
            let bare = matches!(value.node, Some(N::ParamRef(_)));
            writes.push((
                position,
                TargetColumn {
                    schema: (!relation.schemaname.is_empty()).then(|| relation.schemaname.clone()),
                    table: relation.relname.clone(),
                    column: column.to_string(),
                },
                in_values && bare,
            ));
        }
    };
    // `SET col = value` items; `col[1] = ...` and `col.field = ...` write
    // part of the column, and `(a, b) = (...)` is not a plain value.
    let record_set_list =
        |target_list: &[pb::Node], relation: &pb::RangeVar, record: &mut dyn FnMut(&pb::Node, &pb::RangeVar, &str, bool)| {
            for target in target_list {
                if let Some(N::ResTarget(res_target)) = &target.node {
                    if res_target.name.is_empty() || !res_target.indirection.is_empty() {
                        continue;
                    }
                    if let Some(value) = &res_target.val {
                        record(value, relation, &res_target.name, true);
                    }
                }
            }
        };

    walk_statements(parse_result, &mut |node| match node {
        N::ParamRef(p) => *occurrences.entry(p.number as usize).or_default() += 1,
        N::InsertStmt(insert) => {
            let Some(relation) = &insert.relation else { return };

            if let Some(on_conflict) = &insert.on_conflict_clause {
                record_set_list(&on_conflict.target_list, relation, &mut record);
            }

            // Without an explicit column list, parameters can't be mapped to columns
            let columns: Vec<&str> = insert
                .cols
                .iter()
                .filter_map(|n| match &n.node {
                    Some(N::ResTarget(t)) => Some(t.name.as_str()),
                    _ => None,
                })
                .collect();
            let Some(N::SelectStmt(select)) =
                insert.select_stmt.as_deref().and_then(|s| s.node.as_ref())
            else {
                return;
            };

            if !select.values_lists.is_empty() {
                // VALUES: every row maps onto the column list
                for row in &select.values_lists {
                    if let Some(N::List(list)) = &row.node {
                        for (item, column) in list.items.iter().zip(&columns) {
                            record(item, relation, column, true);
                        }
                    }
                }
            } else if select.op() == pb::SetOperation::SetopNone {
                // INSERT ... SELECT: the select list maps onto the column
                // list -- unless a `*` makes the positions unknowable.
                let values: Option<Vec<&pb::Node>> = select
                    .target_list
                    .iter()
                    .map(|target| match &target.node {
                        Some(N::ResTarget(t)) => t.val.as_deref().filter(|v| !is_star(v)),
                        _ => None,
                    })
                    .collect();
                for (value, column) in values.iter().flatten().zip(&columns) {
                    record(value, relation, column, false);
                }
            }
        }
        N::UpdateStmt(update) => {
            if let Some(relation) = &update.relation {
                record_set_list(&update.target_list, relation, &mut record);
            }
        }
        _ => {}
    });

    let mut result: HashMap<usize, ParamTargets> = HashMap::new();
    let mut capable_occurrences: HashMap<usize, usize> = HashMap::new();
    for (position, column, capable) in writes {
        let targets = result.entry(position).or_default();
        if !targets.columns.contains(&column) {
            targets.columns.push(column);
        }
        if capable {
            *capable_occurrences.entry(position).or_default() += 1;
        }
    }
    for (position, targets) in &mut result {
        targets.default_capable = capable_occurrences.get(position).copied().unwrap_or(0)
            == occurrences.get(position).copied().unwrap_or(0);
    }
    result
}

/// `*` or `t.*`
fn is_star(node: &pb::Node) -> bool {
    match &node.node {
        Some(N::ColumnRef(col_ref)) => col_ref
            .fields
            .iter()
            .any(|f| matches!(f.node, Some(N::AStar(_)))),
        _ => false,
    }
}

/// Infer parameter nullability from SQL expression context.
/// Parameters are considered nullable when they appear in contexts that expect NULL values:
/// - Any argument but the last of COALESCE($N, fallback) - COALESCE exists to handle NULL
/// - Argument of IS NULL / IS NOT NULL tests
/// - NULLIF($N, value) - first argument may be NULL
///
/// These are found anywhere in the statement: CTEs, subqueries, ON CONFLICT,
/// RETURNING, ORDER BY / LIMIT, FILTER, and so on.
fn infer_parameter_nullability_from_ast(
    parse_result: Option<&pg_query::ParseResult>,
    param_count: usize,
) -> Vec<bool> {
    let mut nullable = vec![false; param_count];
    let Some(parse_result) = parse_result else {
        return nullable;
    };
    walk_statements(parse_result, &mut |node| match node {
        N::CoalesceExpr(coalesce) => {
            // All arguments to COALESCE except the last are expected to potentially be NULL
            // (if they couldn't be NULL, there'd be no need for the fallback)
            for arg in coalesce.args.iter().take(coalesce.args.len().saturating_sub(1)) {
                mark_params_nullable_in_expr(arg, &mut nullable);
            }
        }
        N::NullTest(null_test) => {
            // Parameter in IS NULL / IS NOT NULL should be nullable
            if let Some(arg) = &null_test.arg {
                mark_params_nullable_in_expr(arg, &mut nullable);
            }
        }
        // `NULLIF(a, b)` parses as an operator expression, not a function call.
        N::AExpr(a_expr) if a_expr.kind() == pb::AExprKind::AexprNullif => {
            if let Some(first_arg) = &a_expr.lexpr {
                mark_params_nullable_in_expr(first_arg, &mut nullable);
            }
        }
        _ => {}
    });
    nullable
}

/// Mark any ParamRef nodes in this expression as nullable
fn mark_params_nullable_in_expr(node: &pb::Node, nullable: &mut [bool]) {
    match &node.node {
        Some(N::ParamRef(param)) => {
            let idx = param.number as usize;
            if idx > 0 && idx <= nullable.len() {
                log::debug!(
                    "[PARAM-NULLABILITY] Marking parameter ${} as nullable from expression context",
                    idx
                );
                nullable[idx - 1] = true;
            }
        }
        Some(N::TypeCast(type_cast)) => {
            // Handle casted parameters like $1::text
            if let Some(arg) = &type_cast.arg {
                mark_params_nullable_in_expr(arg, nullable);
            }
        }
        Some(N::AExpr(aexpr)) => {
            // Handle expressions containing parameters
            if let Some(lexpr) = &aexpr.lexpr {
                mark_params_nullable_in_expr(lexpr, nullable);
            }
            if let Some(rexpr) = &aexpr.rexpr {
                mark_params_nullable_in_expr(rexpr, nullable);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// AST traversal
// ---------------------------------------------------------------------------

/// Call `visit` on every node of every statement, parents before children.
///
/// pg_query's own `nodes()` skips several clauses (RETURNING, ON CONFLICT,
/// LIMIT/OFFSET, FILTER, window definitions, array constructors, ...), so the
/// query-shaped part of the tree is walked here by hand. Node kinds not listed
/// fall back to `nodes()`, which is better than not looking at all.
/// Visit every node of every statement, at any depth.
fn walk_statements(parse_result: &pg_query::ParseResult, visit: &mut dyn FnMut(&N)) {
    for raw_stmt in &parse_result.protobuf.stmts {
        if let Some(inner) = raw_stmt.stmt.as_deref().and_then(|n| n.node.as_ref()) {
            visit(inner);
            walk_children(inner, visit);
        }
    }
}

/// Visit every descendant of `node`.
fn walk_children(node: &N, visit: &mut dyn FnMut(&N)) {
    let known = for_each_child(node, &mut |child| {
        visit(child);
        walk_children(child, visit);
    });
    if !known {
        // Whatever pg_query's generic traversal reaches.
        for (descendant, _, _, _) in node.nodes() {
            visit(&descendant.to_enum());
        }
    }
}

// The helpers below report *direct* children only; recursion is the
// caller's (see `walk_children`).

fn walk_opt(node: Option<&pb::Node>, visit: &mut dyn FnMut(&N)) {
    if let Some(inner) = node.and_then(|n| n.node.as_ref()) {
        visit(inner);
    }
}

fn walk_all(nodes: &[pb::Node], visit: &mut dyn FnMut(&N)) {
    for node in nodes {
        walk_opt(Some(node), visit);
    }
}

fn walk_with(with_clause: Option<&pb::WithClause>, visit: &mut dyn FnMut(&N)) {
    if let Some(with_clause) = with_clause {
        walk_all(&with_clause.ctes, visit);
    }
}

fn walk_window(window: &pb::WindowDef, visit: &mut dyn FnMut(&N)) {
    walk_all(&window.partition_clause, visit);
    walk_all(&window.order_clause, visit);
    walk_opt(window.start_offset.as_deref(), visit);
    walk_opt(window.end_offset.as_deref(), visit);
}

fn walk_select(select: &pb::SelectStmt, visit: &mut dyn FnMut(&N)) {
    walk_with(select.with_clause.as_ref(), visit);
    walk_all(&select.distinct_clause, visit);
    walk_all(&select.target_list, visit);
    walk_all(&select.from_clause, visit);
    walk_opt(select.where_clause.as_deref(), visit);
    walk_all(&select.group_clause, visit);
    walk_opt(select.having_clause.as_deref(), visit);
    walk_all(&select.window_clause, visit);
    walk_all(&select.values_lists, visit);
    walk_all(&select.sort_clause, visit);
    walk_opt(select.limit_offset.as_deref(), visit);
    walk_opt(select.limit_count.as_deref(), visit);
    // UNION / INTERSECT / EXCEPT arms
    for arm in [&select.larg, &select.rarg].into_iter().flatten() {
        walk_select(arm, visit);
    }
}

fn walk_on_conflict(on_conflict: &pb::OnConflictClause, visit: &mut dyn FnMut(&N)) {
    if let Some(infer) = &on_conflict.infer {
        walk_all(&infer.index_elems, visit);
        walk_opt(infer.where_clause.as_deref(), visit);
    }
    walk_all(&on_conflict.target_list, visit);
    walk_opt(on_conflict.where_clause.as_deref(), visit);
}

fn walk_json_value(value: Option<&pb::JsonValueExpr>, visit: &mut dyn FnMut(&N)) {
    if let Some(value) = value {
        walk_opt(value.raw_expr.as_deref(), visit);
    }
}

/// Call `visit` on each direct child of `node` (the arms of a set operation
/// count as part of their `SelectStmt`, so their children are reported too).
/// Returns `false`, having visited nothing, for a node kind whose children
/// are not enumerated here.
pub(crate) fn for_each_child(node: &N, visit: &mut dyn FnMut(&N)) -> bool {
    match node {
        // Leaves
        N::ColumnRef(_)
        | N::ParamRef(_)
        | N::AConst(_)
        | N::AStar(_)
        | N::String(_)
        | N::Integer(_)
        | N::Float(_)
        | N::Boolean(_)
        | N::BitString(_)
        | N::RangeVar(_)
        | N::TypeName(_)
        | N::ColumnDef(_)
        | N::SqlvalueFunction(_)
        | N::SetToDefault(_)
        | N::CurrentOfExpr(_)
        | N::LockingClause(_) => {}

        // Statements
        N::SelectStmt(select) => walk_select(select, visit),
        N::InsertStmt(insert) => {
            walk_with(insert.with_clause.as_ref(), visit);
            walk_opt(insert.select_stmt.as_deref(), visit);
            if let Some(on_conflict) = &insert.on_conflict_clause {
                walk_on_conflict(on_conflict, visit);
            }
            walk_all(&insert.returning_list, visit);
        }
        N::UpdateStmt(update) => {
            walk_with(update.with_clause.as_ref(), visit);
            walk_all(&update.target_list, visit);
            walk_all(&update.from_clause, visit);
            walk_opt(update.where_clause.as_deref(), visit);
            walk_all(&update.returning_list, visit);
        }
        N::DeleteStmt(delete) => {
            walk_with(delete.with_clause.as_ref(), visit);
            walk_all(&delete.using_clause, visit);
            walk_opt(delete.where_clause.as_deref(), visit);
            walk_all(&delete.returning_list, visit);
        }
        N::MergeStmt(merge) => {
            walk_with(merge.with_clause.as_ref(), visit);
            walk_opt(merge.source_relation.as_deref(), visit);
            walk_opt(merge.join_condition.as_deref(), visit);
            walk_all(&merge.merge_when_clauses, visit);
            walk_all(&merge.returning_list, visit);
        }
        N::MergeWhenClause(when) => {
            walk_opt(when.condition.as_deref(), visit);
            walk_all(&when.target_list, visit);
            walk_all(&when.values, visit);
        }
        N::WithClause(with_clause) => walk_all(&with_clause.ctes, visit),
        N::CommonTableExpr(cte) => walk_opt(cte.ctequery.as_deref(), visit),
        N::OnConflictClause(on_conflict) => walk_on_conflict(on_conflict, visit),

        // FROM items
        N::JoinExpr(join) => {
            walk_opt(join.larg.as_deref(), visit);
            walk_opt(join.rarg.as_deref(), visit);
            walk_opt(join.quals.as_deref(), visit);
        }
        N::RangeSubselect(range) => walk_opt(range.subquery.as_deref(), visit),
        N::RangeFunction(range) => walk_all(&range.functions, visit),
        N::RangeTableSample(sample) => {
            walk_opt(sample.relation.as_deref(), visit);
            walk_all(&sample.args, visit);
            walk_opt(sample.repeatable.as_deref(), visit);
        }

        // Expressions
        N::ResTarget(target) => {
            walk_all(&target.indirection, visit);
            walk_opt(target.val.as_deref(), visit);
        }
        N::AExpr(a_expr) => {
            walk_opt(a_expr.lexpr.as_deref(), visit);
            walk_opt(a_expr.rexpr.as_deref(), visit);
        }
        N::BoolExpr(bool_expr) => walk_all(&bool_expr.args, visit),
        N::FuncCall(func_call) => {
            walk_all(&func_call.args, visit);
            walk_all(&func_call.agg_order, visit);
            walk_opt(func_call.agg_filter.as_deref(), visit);
            if let Some(over) = &func_call.over {
                walk_window(over, visit);
            }
        }
        N::WindowDef(window) => walk_window(window, visit),
        N::NamedArgExpr(arg) => walk_opt(arg.arg.as_deref(), visit),
        N::NullTest(test) => walk_opt(test.arg.as_deref(), visit),
        N::BooleanTest(test) => walk_opt(test.arg.as_deref(), visit),
        N::CoalesceExpr(coalesce) => walk_all(&coalesce.args, visit),
        N::MinMaxExpr(min_max) => walk_all(&min_max.args, visit),
        N::CaseExpr(case_expr) => {
            walk_opt(case_expr.arg.as_deref(), visit);
            walk_all(&case_expr.args, visit);
            walk_opt(case_expr.defresult.as_deref(), visit);
        }
        N::CaseWhen(case_when) => {
            walk_opt(case_when.expr.as_deref(), visit);
            walk_opt(case_when.result.as_deref(), visit);
        }
        N::TypeCast(type_cast) => walk_opt(type_cast.arg.as_deref(), visit),
        N::CollateClause(collate) => walk_opt(collate.arg.as_deref(), visit),
        N::SubLink(sublink) => {
            walk_opt(sublink.testexpr.as_deref(), visit);
            walk_opt(sublink.subselect.as_deref(), visit);
        }
        N::AIndirection(indirection) => {
            walk_opt(indirection.arg.as_deref(), visit);
            walk_all(&indirection.indirection, visit);
        }
        N::AIndices(indices) => {
            walk_opt(indices.lidx.as_deref(), visit);
            walk_opt(indices.uidx.as_deref(), visit);
        }
        N::AArrayExpr(array) => walk_all(&array.elements, visit),
        N::RowExpr(row) => walk_all(&row.args, visit),
        N::List(list) => walk_all(&list.items, visit),
        N::SortBy(sort_by) => walk_opt(sort_by.node.as_deref(), visit),
        N::GroupingSet(set) => walk_all(&set.content, visit),
        N::GroupingFunc(func) => walk_all(&func.args, visit),
        N::MultiAssignRef(multi) => walk_opt(multi.source.as_deref(), visit),
        N::XmlExpr(xml) => {
            walk_all(&xml.named_args, visit);
            walk_all(&xml.args, visit);
        }
        N::XmlSerialize(xml) => walk_opt(xml.expr.as_deref(), visit),
        N::JsonObjectConstructor(json) => walk_all(&json.exprs, visit),
        N::JsonArrayConstructor(json) => walk_all(&json.exprs, visit),
        N::JsonKeyValue(pair) => {
            walk_opt(pair.key.as_deref(), visit);
            walk_json_value(pair.value.as_deref(), visit);
        }
        N::JsonValueExpr(value) => walk_json_value(Some(value), visit),

        _ => return false,
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_constraint::{DefaultConstraint, NotNullConstraint};
    use crate::pg_rel::PgRelKind;
    use ustr::ustr;

    fn parse(sql: &str) -> pg_query::ParseResult {
        pg_query::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// A table whose columns are given as (name, type oid, not null, has default).
    fn table(oid: OID, schema: &str, name: &str, cols: &[(&str, OID, bool, bool)]) -> PgRel {
        let mut constraints = Vec::new();
        for (col, _, not_null, has_default) in cols {
            if *not_null {
                constraints.push(Constraint::NotNull(NotNullConstraint {
                    name: ustr(&format!("{name}_{col}_not_null")),
                    column: ustr(col),
                }));
            }
            if *has_default {
                constraints.push(Constraint::Default(DefaultConstraint {
                    name: ustr(&format!("{name}_{col}_default")),
                    column: ustr(col),
                }));
            }
        }
        PgRel {
            oid,
            id: PgId::new(Some(ustr(schema)), ustr(name)),
            kind: PgRelKind::Table,
            constraints,
            columns: cols.iter().map(|(c, ..)| ustr(c)).collect(),
            column_types: cols.iter().map(|(_, t, ..)| *t).collect(),
        }
    }

    const INT: OID = 23;
    const TEXT: OID = 25;
    /// Stand-in for a domain over text.
    const EMAIL: OID = 90001;

    fn test_rel_index() -> RelIndex {
        let mut index = RelIndex::default();
        for rel in [
            table(
                1,
                "public",
                "users",
                &[
                    ("id", INT, true, true),
                    ("email", EMAIL, true, false),
                    ("name", TEXT, true, false),
                    ("bio", TEXT, false, false),
                    ("status", TEXT, true, true),
                ],
            ),
            table(
                2,
                "public",
                "teams",
                &[("id", INT, true, true), ("name", TEXT, false, false), ("slug", TEXT, false, false)],
            ),
            // Same name in two schemas, with opposite nullability and a
            // `code` column of different types.
            table(
                3,
                "core",
                "widget",
                &[("id", INT, true, false), ("label", TEXT, true, false), ("code", INT, true, false)],
            ),
            table(
                4,
                "audit",
                "widget",
                &[("id", INT, false, false), ("label", TEXT, false, false), ("code", TEXT, false, false)],
            ),
            // Shares `email` with `users`, as plain text.
            table(
                5,
                "public",
                "invites",
                &[("id", INT, true, true), ("team_id", INT, true, false), ("email", TEXT, false, false)],
            ),
        ] {
            index.insert(rel.oid, rel);
        }
        index
    }

    struct Fixture {
        rel_index: RelIndex,
        cache: crate::view_nullability::ViewNullabilityCache,
        domains: crate::domain_index::DomainIndex,
        composites: crate::composite_index::CompositeIndex,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_search_path(&["public"])
        }

        fn with_search_path(search_path: &[&str]) -> Self {
            Self {
                rel_index: test_rel_index().with_search_path(search_path),
                cache: Default::default(),
                domains: crate::domain_index::DomainIndex::empty(),
                composites: Default::default(),
            }
        }

        fn introspector(&self) -> QueryIntrospector<'_> {
            QueryIntrospector::new(
                &self.rel_index,
                &self.cache,
                None,
                &self.domains,
                &self.composites,
                None,
                None,
            )
        }
    }

    fn nullable_params(sql: &str, count: usize) -> Vec<bool> {
        infer_parameter_nullability_from_ast(Some(&parse(sql)), count)
    }

    #[test]
    fn nullif_and_coalesce_mark_params_nullable() {
        // Both parse as dedicated nodes, never as FuncCall.
        assert_eq!(nullable_params("SELECT nullif($1, '')", 1), vec![true]);
        assert_eq!(nullable_params("SELECT nullif($1::text, $2)", 2), vec![true, false]);
        assert_eq!(nullable_params("SELECT coalesce($1, $2, $3)", 3), vec![true, true, false]);
        assert_eq!(nullable_params("SELECT coalesce($1::int + 1, 0)", 1), vec![true]);
        assert_eq!(nullable_params("SELECT $1 IS NULL, $2::int IS NOT NULL, $3", 3), vec![true, true, false]);
        // A function call does not imply anything.
        assert_eq!(nullable_params("SELECT lower($1), greatest($2, 1)", 2), vec![false, false]);
    }

    #[test]
    fn param_nullability_is_found_in_every_clause() {
        for sql in [
            // WITH on each statement kind
            "WITH c AS (SELECT coalesce($1, 1)) SELECT * FROM c",
            "WITH c AS (SELECT coalesce($1, 1)) INSERT INTO t (a) SELECT * FROM c",
            "WITH c AS (SELECT coalesce($1, 1)) UPDATE t SET a = 1",
            "WITH c AS (SELECT coalesce($1, 1)) DELETE FROM t",
            // A data-modifying CTE
            "WITH c AS (UPDATE t SET a = coalesce($1, a) RETURNING a) SELECT * FROM c",
            // INSERT
            "INSERT INTO t (a) VALUES (coalesce($1, 2))",
            "INSERT INTO t (a) VALUES (1), (coalesce($1, 2))",
            "INSERT INTO t (a) SELECT coalesce($1, 2)",
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = coalesce($1, 2)",
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2 WHERE $1 IS NULL",
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) WHERE coalesce($1, true) DO NOTHING",
            "INSERT INTO t (a) VALUES (1) RETURNING coalesce($1, a)",
            // UPDATE
            "UPDATE t SET a = coalesce($1, a)",
            "UPDATE t SET a = 1 FROM (SELECT coalesce($1, 1) AS x) u WHERE t.a = u.x",
            "UPDATE t SET a = 1 FROM u WHERE coalesce($1, true)",
            "UPDATE t SET a = 1 RETURNING nullif($1, a)",
            // DELETE
            "DELETE FROM t USING (SELECT coalesce($1, 1) AS x) u WHERE t.a = u.x",
            "DELETE FROM t WHERE $1 IS NULL OR a = $1",
            "DELETE FROM t RETURNING coalesce($1, a)",
            // SELECT clauses
            "SELECT a FROM t ORDER BY coalesce($1, a)",
            "SELECT a FROM t LIMIT coalesce($1, 10)",
            "SELECT a FROM t OFFSET coalesce($1, 0)",
            "SELECT a FROM t GROUP BY a HAVING coalesce($1, true)",
            "SELECT DISTINCT ON (coalesce($1, a)) a FROM t",
            "SELECT count(*) FILTER (WHERE $1 IS NULL OR a = $1) FROM t",
            "SELECT array_agg(a ORDER BY coalesce($1, a)) FROM t",
            "SELECT sum(a) OVER (PARTITION BY coalesce($1, a)) FROM t",
            "SELECT sum(a) OVER w FROM t WINDOW w AS (ORDER BY coalesce($1, a))",
            "SELECT 1 UNION ALL SELECT coalesce($1, 2)",
            "SELECT * FROM t JOIN u ON coalesce($1, true)",
            "SELECT * FROM (VALUES (coalesce($1, 1))) v",
            "SELECT * FROM t WHERE a IN (SELECT coalesce($1, 1))",
            "SELECT * FROM t WHERE EXISTS (SELECT 1 WHERE $1 IS NULL)",
            // Expression forms
            "SELECT greatest(coalesce($1, 1), 2)",
            "SELECT least(1, nullif($1, 0))",
            "SELECT ROW(coalesce($1, 1), 2)",
            "SELECT ARRAY[coalesce($1, 1), 2]",
            "SELECT (ARRAY[1, 2])[coalesce($1, 1)]",
            "SELECT CASE WHEN $1 IS NULL THEN 1 ELSE 2 END",
            "SELECT f(x => coalesce($1, 1))",
            "SELECT (coalesce($1, 1))::text",
            "SELECT a FROM t WHERE b = ANY(coalesce($1, '{}'))",
            // Functions in FROM
            "SELECT * FROM generate_series(1, coalesce($1, 10))",
            "SELECT * FROM ROWS FROM (generate_series(1, coalesce($1, 10))) x",
            "SELECT * FROM t, LATERAL unnest(coalesce($1, t.tags)) u",
        ] {
            assert_eq!(nullable_params(sql, 1), vec![true], "{sql}");
        }
    }

    fn targets(sql: &str) -> HashMap<usize, ParamTargets> {
        extract_param_targets(&parse(sql))
    }

    fn target_columns(sql: &str, position: usize) -> Vec<String> {
        targets(sql)
            .get(&position)
            .map(|t| t.columns.iter().map(|c| c.column.clone()).collect())
            .unwrap_or_default()
    }

    #[test]
    fn insert_param_targets_cover_every_values_row() {
        let sql = "INSERT INTO users (name, bio) VALUES ($1, $2), ($3, $4::text), ('x', $5)";
        assert_eq!(target_columns(sql, 1), vec!["name"]);
        assert_eq!(target_columns(sql, 2), vec!["bio"]);
        assert_eq!(target_columns(sql, 3), vec!["name"]);
        assert_eq!(target_columns(sql, 4), vec!["bio"]);
        assert_eq!(target_columns(sql, 5), vec!["bio"]);

        let t = targets(sql);
        assert!(t[&1].default_capable);
        // `DEFAULT::text` is not valid SQL.
        assert!(!t[&4].default_capable);
    }

    #[test]
    fn insert_select_param_targets() {
        let sql = "INSERT INTO core.widget (id, label) SELECT $1, $2::text FROM users WHERE id = $3";
        let t = targets(sql);
        assert_eq!(
            t[&1].columns,
            vec![TargetColumn {
                schema: Some("core".into()),
                table: "widget".into(),
                column: "id".into()
            }]
        );
        assert_eq!(target_columns(sql, 2), vec!["label"]);
        assert!(!t.contains_key(&3));
        // `SELECT DEFAULT` is not valid SQL.
        assert!(!t[&1].default_capable);

        // A star makes the positions unknowable.
        assert!(targets("INSERT INTO users (name, bio) SELECT u.*, $1 FROM users u").is_empty());
        // No column list, no mapping.
        assert!(targets("INSERT INTO users VALUES ($1, $2)").is_empty());
    }

    #[test]
    fn update_and_upsert_param_targets() {
        let sql = "INSERT INTO users (email, name) VALUES ($1, $2) \
                   ON CONFLICT (email) DO UPDATE SET name = $2, bio = $3";
        assert_eq!(target_columns(sql, 2), vec!["name"]);
        assert_eq!(target_columns(sql, 3), vec!["bio"]);
        // $2 is a bare VALUES item and a bare SET value: DEFAULT works in both.
        assert!(targets(sql)[&2].default_capable);

        let sql = "WITH up AS (UPDATE users SET bio = $1, tags[1] = $2 WHERE id = $3 RETURNING id) \
                   SELECT * FROM up";
        assert_eq!(target_columns(sql, 1), vec!["bio"]);
        // A subscripted assignment writes an element, not the column.
        assert!(target_columns(sql, 2).is_empty());
        assert!(target_columns(sql, 3).is_empty());

        // Used again outside a VALUES/SET position: DEFAULT would break it.
        let sql = "UPDATE users SET bio = $1 WHERE bio IS DISTINCT FROM $1";
        assert!(!targets(sql)[&1].default_capable);
    }

    fn compared(sql: &str) -> HashMap<usize, (Option<String>, String, bool)> {
        compared_columns(&parse(sql))
            .into_iter()
            .map(|(pos, c)| (pos, (c.qualifier, c.column, c.same_type)))
            .collect()
    }

    #[test]
    fn compared_columns_come_from_the_ast() {
        let c = compared(
            "SELECT * FROM users u WHERE u.id = $1 AND $2 = name AND bio > $3::text \
             AND status IN ($4, $5) AND u.email = ANY($6) AND lower(name) = $7",
        );
        assert_eq!(c[&1], (Some("u".into()), "id".into(), true));
        assert_eq!(c[&2], (None, "name".into(), true));
        assert_eq!(c[&3], (None, "bio".into(), true));
        assert_eq!(c[&4], (None, "status".into(), true));
        assert_eq!(c[&5], (None, "status".into(), true));
        // An array of the column's type, not the column's type.
        assert_eq!(c[&6], (Some("u".into()), "email".into(), false));
        // Compared with an expression, not a column.
        assert!(!c.contains_key(&7));
    }

    #[test]
    fn compared_columns_ignore_text_that_only_looks_like_a_comparison() {
        // The old text search found `$1` inside `$10`, in string literals and
        // in comments, then read whatever word preceded it.
        let c = compared(
            "SELECT 'id = $2' AS note /* name = $3 */ FROM users \
             WHERE a = $10 AND b = $1 -- c = $4\n AND d::text = $5 AND e = f($6)",
        );
        assert_eq!(c[&10], (None, "a".into(), true));
        assert_eq!(c[&1], (None, "b".into(), true));
        for position in [2, 3, 4, 5, 6] {
            assert!(!c.contains_key(&position), "${position}");
        }
    }

    #[test]
    fn first_comparison_in_the_text_wins() {
        let c = compared("SELECT * FROM users WHERE name = $1 OR email = $1");
        assert_eq!(c[&1].1, "name");
        let c = compared("SELECT * FROM users WHERE id IN (SELECT id FROM teams WHERE slug = $1) AND name = $1");
        assert_eq!(c[&1].1, "slug");
    }

    fn param_info(sql: &str, specs: &[ParameterSpec], count: usize) -> Vec<(String, bool, Option<OID>, bool)> {
        param_info_in(&Fixture::new(), sql, specs, count)
    }

    fn param_info_in(
        fixture: &Fixture,
        sql: &str,
        specs: &[ParameterSpec],
        count: usize,
    ) -> Vec<(String, bool, Option<OID>, bool)> {
        let ast = parse(sql);
        let introspector = fixture.introspector();
        let analysis = introspector.analyze_statement("test", &ast);
        introspector.determine_parameter_info(specs, Some(&ast), analysis.as_ref(), count)
    }

    /// The type each parameter takes from the column it is compared with.
    fn param_types(sql: &str, count: usize) -> Vec<Option<OID>> {
        param_info(sql, &[], count).into_iter().map(|i| i.2).collect()
    }

    /// A parameter compared with a column inside a CTE or subquery binds to
    /// the column that name means *there* — never to a same-named column of
    /// a table in the outer query.
    #[test]
    fn compared_columns_resolve_in_the_scope_they_are_written_in() {
        // `email` is `invites.email` (text), not the outer `users.email`.
        assert_eq!(
            param_types("SELECT * FROM users WHERE id IN (SELECT id FROM invites WHERE email = $1)", 1),
            [Some(TEXT)]
        );
        assert_eq!(
            param_types(
                "WITH i AS (SELECT team_id FROM invites WHERE email = $1) \
                 SELECT * FROM users u JOIN i ON i.team_id = u.id WHERE u.email = $2",
                2
            ),
            [Some(TEXT), Some(EMAIL)]
        );
        assert_eq!(
            param_types(
                "SELECT * FROM users u, LATERAL (SELECT 1 FROM invites i WHERE i.email = $1 AND i.id = u.id) x",
                1
            ),
            [Some(TEXT)]
        );
        assert_eq!(
            param_types("SELECT * FROM users u JOIN (SELECT * FROM invites WHERE email = $1) i ON i.id = u.id", 1),
            [Some(TEXT)]
        );
        // The inner query's own table wins over the outer one...
        assert_eq!(
            param_types(
                "SELECT 1 FROM invites i WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = i.id AND email = $1)",
                1
            ),
            [Some(EMAIL)]
        );
        // ...and a name the inner query does not have is a correlated
        // reference to the outer one.
        assert_eq!(
            param_types(
                "SELECT 1 FROM users u WHERE EXISTS (SELECT 1 FROM teams t WHERE t.id = u.id AND email = $1 AND u.bio = $2)",
                2
            ),
            [Some(EMAIL), Some(TEXT)]
        );
        // A scalar subquery in the target list, and one in a join condition.
        assert_eq!(
            param_types("SELECT (SELECT count(*) FROM invites WHERE email = $1) FROM users", 1),
            [Some(TEXT)]
        );
        assert_eq!(
            param_types(
                "SELECT 1 FROM users u JOIN teams t ON t.id = (SELECT team_id FROM invites WHERE email = $1)",
                1
            ),
            [Some(TEXT)]
        );
        // Set-operation arms are scopes of their own.
        assert_eq!(
            param_types(
                "SELECT id FROM users WHERE email = $1 UNION ALL SELECT id FROM invites WHERE email = $2",
                2
            ),
            [Some(EMAIL), Some(TEXT)]
        );
    }

    #[test]
    fn compared_columns_resolve_through_ctes_and_subqueries() {
        // A CTE column that is a plain column keeps that column's type.
        assert_eq!(
            param_types("WITH c AS (SELECT email AS e FROM users) SELECT 1 FROM c WHERE e = $1", 1),
            [Some(EMAIL)]
        );
        assert_eq!(
            param_types("SELECT 1 FROM (SELECT email FROM users) s WHERE s.email = $1", 1),
            [Some(EMAIL)]
        );
        // A computed one has no column behind it.
        assert_eq!(
            param_types("WITH c AS (SELECT lower(email) AS e FROM users) SELECT 1 FROM c WHERE e = $1", 1),
            [None]
        );
        // A CTE named like a table is not that table.
        let info = param_info("WITH users AS (SELECT 'x'::text AS email) SELECT 1 FROM users WHERE email = $1", &[], 1);
        assert_eq!(info[0], ("email".to_string(), false, None, false));
        // Unresolvable relation: named after the column, no type.
        let info = param_info("SELECT 1 FROM nowhere WHERE email = $1", &[], 1);
        assert_eq!(info[0], ("email".to_string(), false, None, false));
    }

    #[test]
    fn compared_columns_resolve_in_dml() {
        assert_eq!(
            param_types("UPDATE users SET bio = $1 WHERE email = $2", 2),
            [None, Some(EMAIL)]
        );
        assert_eq!(
            param_types("UPDATE users u SET bio = i.email FROM invites i WHERE i.email = $1 AND u.email = $2", 2),
            [Some(TEXT), Some(EMAIL)]
        );
        // Both `users` and `invites` have `email`: ambiguous, no type.
        assert_eq!(
            param_types("UPDATE users u SET bio = 'x' FROM invites i WHERE email = $1", 1),
            [None]
        );
        assert_eq!(
            param_types("DELETE FROM users u USING invites i WHERE i.email = $1 AND u.email = $2", 2),
            [Some(TEXT), Some(EMAIL)]
        );
        // INSERT ... SELECT: the SELECT's tables, not the target.
        assert_eq!(
            param_types("INSERT INTO users (name) SELECT email FROM invites WHERE email = $1", 1),
            [Some(TEXT)]
        );
        assert_eq!(
            param_types(
                "INSERT INTO users (id, email) VALUES (1, 'a') \
                 ON CONFLICT (id) DO UPDATE SET bio = 'x' WHERE users.email = $1 AND excluded.email = $2",
                2
            ),
            [Some(EMAIL), Some(EMAIL)]
        );
        // A data-modifying CTE.
        assert_eq!(
            param_types(
                "WITH gone AS (DELETE FROM invites WHERE email = $1 RETURNING email, team_id) \
                 SELECT 1 FROM users u JOIN gone g ON g.team_id = u.id WHERE g.email = $2 AND u.email = $3",
                3
            ),
            [Some(TEXT), Some(TEXT), Some(EMAIL)]
        );
        assert_eq!(
            param_types(
                "MERGE INTO users u USING invites i ON i.id = u.id AND i.email = $1 \
                 WHEN MATCHED AND u.email = $2 THEN DELETE",
                2
            ),
            [Some(TEXT), Some(EMAIL)]
        );
    }

    #[test]
    fn compared_columns_follow_the_search_path() {
        let sql = "SELECT 1 FROM widget WHERE code = $1";
        let types = |path: &[&str], sql: &str| -> Option<OID> {
            param_info_in(&Fixture::with_search_path(path), sql, &[], 1)[0].2
        };
        assert_eq!(types(&["core", "audit"], sql), Some(INT));
        assert_eq!(types(&["audit", "core"], sql), Some(TEXT));
        // Not on the path: unresolved, however unique the column.
        assert_eq!(types(&["public"], sql), None);
        assert_eq!(types(&["public"], "SELECT 1 FROM audit.widget WHERE code = $1"), Some(TEXT));
    }

    #[test]
    fn parameter_names_and_types_from_compared_columns() {
        let info = param_info(
            "SELECT * FROM users u WHERE u.email = $1 AND bio = $2 AND u.email = ANY($3)",
            &[],
            3,
        );
        // The column's declared (domain) type, not the base type.
        assert_eq!(info[0], ("email".to_string(), false, Some(EMAIL), false));
        assert_eq!(info[1], ("bio".to_string(), false, Some(TEXT), false));
        // Named after the column, but an array: no type taken from it.
        assert_eq!(info[2], ("email_3".to_string(), false, None, false));
    }

    #[test]
    fn inferred_parameter_names_are_made_distinct() {
        let info = param_info("SELECT * FROM users WHERE id > $1 AND id < $2 AND id IN ($3, $4)", &[], 4);
        let names: Vec<&str> = info.iter().map(|i| i.0.as_str()).collect();
        assert_eq!(names, vec!["id", "id_2", "id_3", "id_4"]);

        // A name the author wrote is never renamed; the inferred one yields.
        let specs = [ParameterSpec::Named {
            name: "id".to_string(),
            position: 2,
            nullable: false,
        }];
        let info = param_info("SELECT * FROM users WHERE id > $1 AND name = $2", &specs, 2);
        let names: Vec<&str> = info.iter().map(|i| i.0.as_str()).collect();
        assert_eq!(names, vec!["id_1", "id"]);
    }

    #[test]
    fn ambiguous_unqualified_column_gets_a_name_but_no_type() {
        // Both `users` and `teams` have `name`.
        let info = param_info("SELECT 1 FROM users u JOIN teams t ON t.id = u.id WHERE name = $1", &[], 1);
        assert_eq!(info[0], ("name".to_string(), false, None, false));
    }

    #[test]
    fn insert_param_nullability_and_defaults() {
        // id: NOT NULL DEFAULT; name: NOT NULL; bio: nullable; status: NOT NULL DEFAULT
        let specs: Vec<ParameterSpec> = (1..=8)
            .map(|position| ParameterSpec::Positional {
                position,
                nullable: false,
            })
            .collect();
        let info = param_info(
            "INSERT INTO users (id, name, bio, status) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8::text)",
            &specs,
            8,
        );
        let flags: Vec<(bool, bool)> = info.iter().map(|i| (i.1, i.3)).collect();
        assert_eq!(
            flags,
            vec![
                (true, true),   // id: DEFAULT can stand in
                (false, false), // name: required
                (true, false),  // bio: nullable column
                (true, true),   // status
                // The second row is mapped too.
                (true, true),
                (false, false),
                (true, false),
                // Cast: DEFAULT can't stand in, and the column is NOT NULL.
                (false, false),
            ]
        );
    }

    #[test]
    fn insert_select_params_are_never_default_eligible() {
        let specs: Vec<ParameterSpec> = (1..=3)
            .map(|position| ParameterSpec::Positional {
                position,
                nullable: false,
            })
            .collect();
        let info = param_info("INSERT INTO users (id, name, bio) SELECT $1, $2, $3", &specs, 3);
        let flags: Vec<(bool, bool)> = info.iter().map(|i| (i.1, i.3)).collect();
        // id is NOT NULL: without DEFAULT, NULL would violate it.
        assert_eq!(flags, vec![(false, false), (false, false), (true, false)]);
    }

    #[test]
    fn param_written_to_several_columns_needs_all_of_them_nullable() {
        let specs = [ParameterSpec::Positional {
            position: 1,
            nullable: false,
        }];
        let info = param_info("INSERT INTO users (name, bio) VALUES ($1, $1)", &specs, 1);
        assert!(!info[0].1, "name is NOT NULL");
        let info = param_info("INSERT INTO teams (name, slug) VALUES ($1, $1)", &specs, 1);
        assert!(info[0].1);
    }

    #[test]
    fn write_targets_resolve_by_schema() {
        let specs = [ParameterSpec::Positional {
            position: 1,
            nullable: false,
        }];
        // audit.widget.label is nullable, core.widget.label is not.
        assert!(param_info("INSERT INTO audit.widget (label) VALUES ($1)", &specs, 1)[0].1);
        assert!(!param_info("INSERT INTO core.widget (label) VALUES ($1)", &specs, 1)[0].1);
        // Unqualified and on no schema of the search path: assume nothing.
        assert!(!param_info("INSERT INTO widget (label) VALUES ($1)", &specs, 1)[0].1);
        assert!(!param_info("UPDATE widget SET label = $1", &specs, 1)[0].1);

        // Unqualified: the first schema on the search path that has it.
        let audit_first = Fixture::with_search_path(&["audit", "core"]);
        let core_first = Fixture::with_search_path(&["core", "audit"]);
        for sql in ["INSERT INTO widget (label) VALUES ($1)", "UPDATE widget SET label = $1"] {
            assert!(param_info_in(&audit_first, sql, &specs, 1)[0].1, "{sql}");
            assert!(!param_info_in(&core_first, sql, &specs, 1)[0].1, "{sql}");
        }
    }

    /// Result columns of `sql` as (name, nullable, type), given the names the
    /// prepared statement would report; every column is reported as `text`.
    fn result_columns(sql: &str, names: &[&str]) -> Vec<(String, bool, OID)> {
        result_columns_in(&Fixture::new(), sql, names)
    }

    fn result_columns_in(fixture: &Fixture, sql: &str, names: &[&str]) -> Vec<(String, bool, OID)> {
        let columns: Vec<QueryColumn> = names
            .iter()
            .map(|name| QueryColumn {
                name: name.to_string(),
                type_oid: TEXT,
                nullable: true,
                nullable_due_to_join: false,
            })
            .collect();
        let ast = parse(sql);
        let introspector = fixture.introspector();
        let analysis = introspector.analyze_statement("test", &ast);
        introspector
            .refine_columns("test", Some(&ast), analysis.as_ref(), columns)
            .into_iter()
            .map(|c| (c.name, c.nullable, c.type_oid))
            .collect()
    }

    fn nullable_flags(columns: &[(String, bool, OID)]) -> Vec<bool> {
        columns.iter().map(|c| c.1).collect()
    }

    fn types(columns: &[(String, bool, OID)]) -> Vec<OID> {
        columns.iter().map(|c| c.2).collect()
    }

    #[test]
    fn returning_plain_target_columns_take_the_columns_nullability_and_type() {
        let cols = result_columns(
            "INSERT INTO users (email, name) VALUES ($1, $2) RETURNING id, email, bio, name AS display",
            &["id", "email", "bio", "display"],
        );
        assert_eq!(
            cols,
            vec![
                ("id".to_string(), false, INT),
                ("email".to_string(), false, EMAIL),
                ("bio".to_string(), true, TEXT),
                // Aliased: still the `name` column.
                ("display".to_string(), false, TEXT),
            ]
        );

        let cols = result_columns(
            "UPDATE users u SET bio = $1 RETURNING u.id, u.*",
            &["id", "id", "email", "name", "bio", "status"],
        );
        assert_eq!(nullable_flags(&cols), vec![false, false, false, false, true, false]);
        assert_eq!(types(&cols), vec![INT, INT, EMAIL, TEXT, TEXT, TEXT]);

        let cols = result_columns(
            "DELETE FROM users WHERE id = $1 RETURNING *",
            &["id", "email", "name", "bio", "status"],
        );
        assert_eq!(nullable_flags(&cols), vec![false, false, false, true, false]);
        assert_eq!(types(&cols), vec![INT, EMAIL, TEXT, TEXT, TEXT]);

        let cols = result_columns(
            "MERGE INTO users u USING invites i ON i.id = u.id WHEN MATCHED THEN DELETE \
             RETURNING u.email, i.email AS invite_email",
            &["email", "invite_email"],
        );
        assert_eq!(cols[0], ("email".to_string(), false, EMAIL));
        assert!(cols[1].1);
    }

    #[test]
    fn returning_expressions_follow_the_expression_rules_not_their_names() {
        // Each output is *named* like a NOT NULL column of `users`, but none
        // of the first three is that column.
        let cols = result_columns(
            "UPDATE users SET bio = $1 FROM teams t WHERE t.id = users.id \
             RETURNING (SELECT max(id) FROM teams WHERE false) AS id, t.name, \
                       nullif(users.email, '') AS email, users.status, \
                       lower(users.email) AS name, t.id AS team_id",
            &["id", "name", "email", "status", "name", "team_id"],
        );
        assert_eq!(
            cols,
            vec![
                ("id".to_string(), true, TEXT),      // subquery
                ("name".to_string(), true, TEXT),    // teams.name, which is nullable
                ("email".to_string(), true, TEXT),   // expression: type as prepared
                ("status".to_string(), false, TEXT), // the target's own column
                // A strict function of a NOT NULL column; its type is its own.
                ("name".to_string(), false, TEXT),
                // A NOT NULL column of the FROM item, with its declared type.
                ("team_id".to_string(), false, INT),
            ]
        );

        // Once aliased, the bare table name is not the target's qualifier.
        let cols = result_columns("UPDATE users u SET bio = $1 FROM teams users RETURNING users.name", &["name"]);
        assert!(cols[0].1);
        // `old` / `new` (PostgreSQL 18) are not the target's name either.
        let cols = result_columns("UPDATE users SET bio = $1 RETURNING old.name", &["name"]);
        assert!(cols[0].1);
        // A column count that does not match the statement's: conclude nothing.
        let cols = result_columns("DELETE FROM users RETURNING *", &["id", "bio"]);
        assert_eq!(nullable_flags(&cols), vec![true, true]);
        assert_eq!(types(&cols), vec![TEXT, TEXT]);
    }

    #[test]
    fn returning_resolves_the_target_by_schema() {
        let insert = |target: &str| format!("INSERT INTO {target} (id, label) VALUES ($1, $2) RETURNING label, code");
        let cols = result_columns(&insert("core.widget"), &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (false, INT));
        let cols = result_columns(&insert("audit.widget"), &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (true, TEXT));
        // Unqualified and on no schema of the search path: nothing is known.
        let cols = result_columns(&insert("widget"), &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (true, TEXT));
        // Unqualified: the first schema on the search path that has it.
        let cols = result_columns_in(&Fixture::with_search_path(&["core", "audit"]), &insert("widget"), &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (false, INT));
        let cols = result_columns_in(&Fixture::with_search_path(&["audit", "core"]), &insert("widget"), &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (true, TEXT));
    }

    /// A result column that is a plain column reference takes the column's
    /// declared type, through aliases, CTEs, subqueries and `*`.
    #[test]
    fn select_columns_take_declared_types_through_ctes_and_subqueries() {
        let cols = result_columns(
            "SELECT u.email, u.email AS e, t.name, email::text AS cast_email, lower(u.email) AS lowered \
             FROM users u LEFT JOIN teams t ON t.id = u.id",
            &["email", "e", "name", "cast_email", "lowered"],
        );
        assert_eq!(types(&cols), vec![EMAIL, EMAIL, TEXT, TEXT, TEXT]);
        assert_eq!(nullable_flags(&cols), vec![false, false, true, false, false]);

        let cols = result_columns("SELECT * FROM users", &["id", "email", "name", "bio", "status"]);
        assert_eq!(types(&cols), vec![INT, EMAIL, TEXT, TEXT, TEXT]);
        let cols = result_columns(
            "SELECT t.*, u.* FROM users u JOIN teams t ON t.id = u.id",
            &["id", "name", "slug", "id", "email", "name", "bio", "status"],
        );
        assert_eq!(types(&cols), vec![INT, TEXT, TEXT, INT, EMAIL, TEXT, TEXT, TEXT]);

        let cols = result_columns(
            "WITH c AS (SELECT email, id AS user_id FROM users) SELECT email, user_id FROM c",
            &["email", "user_id"],
        );
        assert_eq!(types(&cols), vec![EMAIL, INT]);
        let cols = result_columns("SELECT s.email FROM (SELECT * FROM users) s", &["email"]);
        assert_eq!(types(&cols), vec![EMAIL]);
        let cols = result_columns(
            "SELECT (SELECT email FROM users LIMIT 1) AS email, e.email AS direct \
             FROM invites i, LATERAL (SELECT u.email FROM users u WHERE u.id = i.id) e",
            &["email", "direct"],
        );
        // A scalar subquery is an expression; the LATERAL column is a column.
        assert_eq!(types(&cols), vec![TEXT, EMAIL]);

        // A CTE named like a table is not that table.
        let cols = result_columns("WITH users AS (SELECT 'x'::text AS email) SELECT email FROM users", &["email"]);
        assert_eq!(cols, vec![("email".to_string(), false, TEXT)]);

        // Both arms the same column type: kept. Different: as prepared.
        let cols = result_columns("SELECT email FROM users UNION ALL SELECT email FROM users", &["email"]);
        assert_eq!(types(&cols), vec![EMAIL]);
        let cols = result_columns("SELECT email FROM users UNION ALL SELECT email FROM invites", &["email"]);
        assert_eq!(types(&cols), vec![TEXT]);

        // A USING column: its sides agree on `int`; unqualified `email` is
        // owned by both tables, so it is not resolved.
        let cols = result_columns("SELECT id FROM users JOIN invites USING (id)", &["id"]);
        assert_eq!(types(&cols), vec![INT]);
        let cols = result_columns("SELECT i.email, u.email FROM users u JOIN invites i USING (id)", &["email", "email"]);
        assert_eq!(types(&cols), vec![TEXT, EMAIL]);
    }

    #[test]
    fn select_columns_follow_the_search_path() {
        let sql = "SELECT label, code FROM widget";
        let cols = result_columns_in(&Fixture::with_search_path(&["core", "audit"]), sql, &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (false, INT));
        let cols = result_columns_in(&Fixture::with_search_path(&["audit", "core"]), sql, &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (true, TEXT));
        let cols = result_columns(sql, &["label", "code"]);
        assert_eq!((cols[0].1, cols[1].2), (true, TEXT));
    }
}
