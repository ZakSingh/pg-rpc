use crate::codegen::{SchemaName, OID};
use crate::pg_fn::PgFn;
use crate::rel_index::RelIndex;
use anyhow::Context;
use itertools::Itertools;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use tokio_postgres::Client;

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
    pub async fn new(db: &Client, rel_index: &RelIndex) -> anyhow::Result<Self> {
        let fn_rows = db
            .query(FUNCTION_INTROSPECTION_QUERY, &[])
            .await
            .context("Function introspection query failed")?;

        let fns = fn_rows
            // .into_par_iter()
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, u32>("oid"),
                    PgFn::from_row(row, rel_index).unwrap(),
                )
            })
            .collect();

        Ok(Self(fns))
    }

    pub fn get_type_oids(&self) -> Vec<OID> {
        self.values().map(|f| f.ty_oids()).flatten().collect()
    }

    /// Get all the functions in a given schema
    pub fn get_schema_fns(&self, schema_name: SchemaName) -> Vec<&PgFn> {
        self.values().filter(|f| f.schema == schema_name).collect()
    }

    pub fn get_fn(&self, fn_id: &FunctionId) -> Option<&PgFn> {
        self.values()
            .find(|f| f.schema == fn_id.schema && f.name == fn_id.name)
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
