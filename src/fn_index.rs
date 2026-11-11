use crate::call_graph::CallTarget;
use crate::codegen::OID;
use crate::constraint_analysis::RelDep;
use crate::exceptions::PgException;
use crate::pg_fn::PgFn;
use crate::rel_index::RelIndex;
use crate::trigger_index::TriggerIndex;
use anyhow::Context;
use itertools::Itertools;
use postgres::Client;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};

const FUNCTION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/function_introspection.sql");

/// Every PL/pgSQL and SQL function in every user schema, keyed by OID.
///
/// The whole database is indexed so that error analysis can follow calls from
/// a generated function into helpers in other schemas (see [`Self::follow_calls`]).
/// Rust code is only emitted for the schemas listed in the config; anything
/// that feeds generated output must iterate [`Self::codegen_fns`] rather than
/// the raw map.
#[derive(Debug)]
pub struct FunctionIndex {
    fns: BTreeMap<OID, PgFn>,
    codegen_schemas: BTreeSet<String>,
    /// `(schema, name)` -> OID, for resolving calls by name.
    by_name: HashMap<(String, String), OID>,
}

impl Deref for FunctionIndex {
    type Target = BTreeMap<OID, PgFn>;

    fn deref(&self) -> &Self::Target {
        &self.fns
    }
}

impl DerefMut for FunctionIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fns
    }
}

impl FunctionIndex {
    pub fn new(
        db: &mut Client,
        rel_index: &RelIndex,
        trigger_index: &TriggerIndex,
        codegen_schemas: &[String],
        errors_config: Option<&crate::config::ErrorsConfig>,
    ) -> anyhow::Result<Self> {
        let fn_rows = db
            .query(FUNCTION_INTROSPECTION_QUERY, &[])
            .context("Function introspection query failed")?;

        let fns = fn_rows
            .into_par_iter()
            .map(|row| {
                let oid: OID = row.get("oid");
                let schema: String = row.get("schema_name");
                let name: String = row.get("function_name");
                PgFn::from_row(row, rel_index, Some(trigger_index), errors_config)
                    .map(|f| (oid, f))
                    .with_context(|| format!("Failed to index function {schema}.{name}"))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;

        let by_name = name_map(&fns);
        let mut index = Self {
            fns,
            codegen_schemas: codegen_schemas.iter().cloned().collect(),
            by_name,
        };
        index.follow_calls();
        Ok(index)
    }

    /// Build an index from already-analyzed functions (tests).
    #[cfg(test)]
    pub fn from_fns(fns: impl IntoIterator<Item = PgFn>, codegen_schemas: &[&str]) -> Self {
        let fns: BTreeMap<OID, PgFn> = fns.into_iter().map(|f| (f.oid, f)).collect();
        let by_name = name_map(&fns);
        Self {
            fns,
            codegen_schemas: codegen_schemas.iter().map(|s| s.to_string()).collect(),
            by_name,
        }
    }

    /// Resolve a call target to an indexed function.
    ///
    /// A schema-qualified name resolves directly. An unqualified name is looked
    /// up in `caller_schema` (if any) and then in `public`. Built-ins,
    /// extension functions and anything else not indexed resolve to `None`.
    pub fn resolve(&self, target: &CallTarget, caller_schema: Option<&str>) -> Option<&PgFn> {
        let lookup = |schema: &str| {
            self.by_name
                .get(&(schema.to_string(), target.name.clone()))
                .and_then(|oid| self.fns.get(oid))
        };
        match &target.schema {
            Some(schema) => lookup(schema),
            None => caller_schema.and_then(lookup).or_else(|| lookup("public")),
        }
    }

    /// Whether Rust code is generated for this function.
    pub fn is_codegen(&self, f: &PgFn) -> bool {
        self.codegen_schemas.contains(&f.schema)
    }

    /// Functions in the configured codegen schemas: the ones that get Rust
    /// bindings, error enums and whose types are emitted.
    pub fn codegen_fns(&self) -> impl Iterator<Item = &PgFn> {
        self.fns.values().filter(move |f| self.is_codegen(f))
    }

    fn codegen_fns_mut(&mut self) -> impl Iterator<Item = &mut PgFn> {
        let Self {
            fns,
            codegen_schemas,
            ..
        } = self;
        fns.values_mut()
            .filter(move |f| codegen_schemas.contains(&f.schema))
    }

    /// Type OIDs referenced by the signatures of codegen functions. Helpers in
    /// other schemas are deliberately excluded: their types are not emitted,
    /// and an exotic type used only by a helper must not fail the build.
    pub fn get_type_oids(&self) -> Vec<OID> {
        self.codegen_fns().flat_map(|f| f.ty_oids()).collect()
    }

    /// Union every function's callees' exceptions and DML targets into the
    /// caller, transitively.
    ///
    /// A `raise` inside `core.helper()` or an `insert` it performs surfaces to
    /// whoever calls `core.helper()`, so the caller's error enum must carry
    /// those variants too. Calls are resolved by name: a qualified
    /// `schema.name(...)` directly, an unqualified `name(...)` first in the
    /// caller's own schema and then in `public`. Unresolvable names (built-ins,
    /// extension functions, dynamic SQL) are ignored. Recursion is handled by
    /// computing the reachable set per function before mutating anything.
    pub fn follow_calls(&mut self) {
        let edges: BTreeMap<OID, Vec<OID>> = self
            .fns
            .values()
            .map(|caller| {
                let targets = caller
                    .callees
                    .iter()
                    .filter_map(|target| {
                        let resolved = self
                            .resolve(target, Some(&caller.schema))
                            .map(|f| f.oid);
                        match resolved {
                            Some(oid) => log::debug!(
                                "{}.{} calls {} (oid {})",
                                caller.schema,
                                caller.name,
                                target_display(target),
                                oid
                            ),
                            None => log::trace!(
                                "{}.{} calls {} which is not an indexed function",
                                caller.schema,
                                caller.name,
                                target_display(target)
                            ),
                        }
                        resolved
                    })
                    .filter(|&oid| oid != caller.oid)
                    .unique()
                    .collect();
                (caller.oid, targets)
            })
            .collect();

        // Reachable callees per function, then the union of their analysis
        // results, all computed from the pre-propagation state so that the
        // order of iteration and cycles don't matter.
        let mut updates: BTreeMap<OID, (Vec<PgException>, Vec<RelDep>)> = BTreeMap::new();
        for &oid in self.fns.keys() {
            let mut reachable = BTreeSet::new();
            let mut stack: Vec<OID> = edges.get(&oid).cloned().unwrap_or_default();
            while let Some(next) = stack.pop() {
                if next != oid && reachable.insert(next) {
                    stack.extend(edges.get(&next).into_iter().flatten().copied());
                }
            }
            if reachable.is_empty() {
                continue;
            }

            let mut exceptions = Vec::new();
            let mut rel_deps = Vec::new();
            for callee in &reachable {
                let callee = &self.fns[callee];
                exceptions.extend(callee.exceptions.iter().cloned());
                rel_deps.extend(callee.rel_deps.iter().cloned());
            }
            updates.insert(oid, (exceptions, rel_deps));
        }

        for (oid, (exceptions, rel_deps)) in updates {
            let f = self.fns.get_mut(&oid).unwrap();
            f.exceptions.extend(exceptions);
            f.exceptions = std::mem::take(&mut f.exceptions)
                .into_iter()
                .unique()
                .collect();
            f.rel_deps.extend(rel_deps);
        }
    }

    /// Apply nullability inference to SQL language functions with OUT parameters
    /// This should be called after the view nullability cache has been built
    pub fn apply_sql_function_nullability(
        &mut self,
        rel_index: &RelIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
        composite_index: &crate::composite_index::CompositeIndex,
    ) -> anyhow::Result<()> {
        log::info!("Applying nullability inference to SQL functions...");

        let mut analyzed_count = 0;
        let mut error_count = 0;

        for pg_fn in self.codegen_fns_mut() {
            // Only analyze SQL language functions with OUT parameters
            if pg_fn.out_args.is_empty() {
                continue;
            }

            let is_sql_function = pg_fn.definition.contains("LANGUAGE sql")
                || pg_fn.definition.contains("language sql")
                || pg_fn.definition.contains("LANGUAGE SQL");

            if !is_sql_function {
                continue;
            }

            analyzed_count += 1;

            match pg_fn.infer_out_param_nullability(
                rel_index,
                view_nullability_cache,
                composite_index,
            ) {
                Ok(()) => {
                    log::debug!("Successfully analyzed SQL function: {}", pg_fn.name);
                }
                Err(e) => {
                    log::warn!(
                        "Error analyzing SQL function {}.{} for nullability: {}",
                        pg_fn.schema,
                        pg_fn.name,
                        e
                    );
                    error_count += 1;
                }
            }
        }

        log::info!(
            "SQL function nullability analysis complete: {} analyzed, {} errors",
            analyzed_count,
            error_count
        );

        Ok(())
    }
}

fn name_map(fns: &BTreeMap<OID, PgFn>) -> HashMap<(String, String), OID> {
    fns.values()
        .map(|f| ((f.schema.clone(), f.name.clone()), f.oid))
        .collect()
}

fn target_display(target: &CallTarget) -> String {
    match &target.schema {
        Some(schema) => format!("{}.{}", schema, target.name),
        None => target.name.clone(),
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct FunctionId {
    pub schema: String,
    pub name: String,
}

impl FunctionId {
    pub fn new(name: &str) -> Self {
        if let Some((schema, name)) = name.split_once('.') {
            Self {
                schema: schema.to_string(),
                name: name.to_string(),
            }
        } else {
            Self {
                schema: "public".to_string(),
                name: name.to_string(),
            }
        }
    }
}
