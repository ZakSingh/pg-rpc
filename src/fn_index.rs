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
