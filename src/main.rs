use crate::codegen::codegen;
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::load_ddl::load_ddl;
use crate::ty_index::TypeIndex;
use std::fs;
use std::path::Path;

mod codegen;
mod db;
mod dump;
mod fn_index;
mod fn_src_location;
mod ident;
mod load_ddl;
mod parse_domain;
mod pg_fn;
mod pg_image;
mod pg_type;
mod pgsql_check;
mod tests;
mod ty_index;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    run(Path::new("schema"), Path::new("pgrpc.rs")).await?;

    Ok(())
}

pub async fn run(dir: &Path, output_path: &Path) -> anyhow::Result<()> {
    let (src, fn_map) = load_ddl(dir)?;

    let db = Db::new(&src).await;

    let fn_index = FunctionIndex::new(&db.client).await?;
    let ty_index = TypeIndex::new(&db.client, &fn_index.type_oids).await?;

    for (schema, fns) in fn_index.fn_index.iter() {
        let fns: Vec<_> = fns
            .values()
            .filter_map(|f| {
                f.has_issues()
                    .then(|| f.report(&fn_map.get(&f.id()).unwrap()))
            })
            .collect();
    }

    let code = codegen(&fn_index, &ty_index).await?;

    fs::write(output_path, code)?;

    Ok(())
}
