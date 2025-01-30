use crate::codegen::{FunctionName, SchemaName, OID};
use crate::pg_fn::PgFn;
use anyhow::Context;
use std::collections::HashMap;
use tokio_postgres::Client;

const FUNCTION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/function_introspection.sql");

#[derive(Debug)]
pub struct FunctionIndex {
    pub type_oids: Vec<OID>,
    pub fn_index: HashMap<SchemaName, HashMap<FunctionName, PgFn>>,
}

impl FunctionIndex {
    pub async fn new(db: &Client) -> anyhow::Result<Self> {
        let (type_oids, fn_index) = db
            .query(FUNCTION_INTROSPECTION_QUERY, &[])
            .await
            .context("Function introspection query failed")?
            .into_iter()
            .try_fold(
                (Vec::new(), HashMap::new()),
                |(mut all_oids, mut index), row| -> anyhow::Result<_> {
                    let mut oids = row.try_get::<_, Vec<OID>>("arg_oids")?;
                    oids.push(row.try_get::<_, OID>("return_type")?);
                    all_oids.extend(oids);

                    let f = PgFn::try_from(row)?;

                    index
                        .entry(f.schema.clone())
                        .or_insert_with(HashMap::new)
                        .insert(f.name.clone(), f);

                    Ok((all_oids, index))
                },
            )?;

        Ok(Self {
            type_oids,
            fn_index,
        })
    }

    pub fn get_fn(&self, fn_id: &FunctionId) -> Option<&PgFn> {
        Some(self.fn_index.get(&fn_id.schema)?.get(&fn_id.name)?)
    }

    pub fn fn_count(&self) -> usize {
        self.fn_index.values().map(|f| f.len()).sum()
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
