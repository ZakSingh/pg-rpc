use crate::infer_types::{FunctionName, SchemaName, OID};
use crate::pg_fn::PgFn;
use std::collections::HashMap;
use tokio_postgres::Client;

const FUNCTION_INTROSPECTION_QUERY: &'static str =
    include_str!("./queries/function_introspection.sql");

pub struct FunctionIndex {
    pub type_oids: Vec<OID>,
    pub fn_index: HashMap<SchemaName, HashMap<FunctionName, PgFn>>,
}

impl FunctionIndex {
    pub async fn new(db: &Client) -> anyhow::Result<Self> {
        let (type_oids, fn_index) = db
            .query(FUNCTION_INTROSPECTION_QUERY, &[])
            .await?
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
}
