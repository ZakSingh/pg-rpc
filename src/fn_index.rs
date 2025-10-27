use crate::codegen::OID;
use crate::pg_fn::PgFn;
use crate::rel_index::RelIndex;
use crate::trigger_index::TriggerIndex;
use anyhow::Context;
use postgres::Client;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

const FUNCTION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/function_introspection.sql");

#[derive(Debug)]
pub struct FunctionIndex(HashMap<OID, PgFn>);

impl Deref for FunctionIndex {
    type Target = HashMap<OID, PgFn>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FunctionIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FunctionIndex {
    pub fn new(
        db: &mut Client,
        rel_index: &RelIndex,
        trigger_index: &TriggerIndex,
        schemas: &Vec<String>,
    ) -> anyhow::Result<Self> {
        let fn_rows = db
            .query(FUNCTION_INTROSPECTION_QUERY, &[schemas])
            .context("Function introspection query failed")?;

        let fns = fn_rows
            .into_par_iter()
            .map(|row| {
                (
                    row.get::<_, u32>("oid"),
                    PgFn::from_row(row, rel_index, Some(trigger_index)).unwrap(),
                )
            })
            .collect();

        Ok(Self(fns))
    }

    pub fn get_type_oids(&self) -> Vec<OID> {
        self.deref()
            .values()
            .map(|f| f.ty_oids())
            .flatten()
            .collect()
    }

    /// Apply nullability inference to SQL language functions with OUT parameters
    /// This should be called after the view nullability cache has been built
    pub fn apply_sql_function_nullability(
        &mut self,
        rel_index: &RelIndex,
        view_nullability_cache: &crate::view_nullability::ViewNullabilityCache,
    ) -> anyhow::Result<()> {
        log::info!("Applying nullability inference to SQL functions...");

        let mut analyzed_count = 0;
        let mut error_count = 0;

        for (_oid, pg_fn) in self.deref_mut().iter_mut() {
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

            match pg_fn.infer_out_param_nullability(rel_index, view_nullability_cache) {
                Ok(()) => {
                    log::debug!("Successfully analyzed SQL function: {}", pg_fn.name);
                }
                Err(e) => {
                    log::warn!(
                        "Error analyzing SQL function {} for nullability: {}",
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
