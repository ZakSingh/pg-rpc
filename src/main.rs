#![feature(iterator_try_collect)]

use crate::codegen::codegen;
use crate::db::Db;
use crate::fn_index::FunctionIndex;
use crate::load_ddl::load_ddl;
use crate::rel_index::RelIndex;
use crate::ty_index::TypeIndex;
use clap::Parser;
use clio::{ClioPath, Output};
use rayon::iter::IntoParallelRefIterator;
use std::fs;
use std::path::Path;

mod codegen;
mod db;
mod exceptions;
mod fn_index;
mod fn_src_location;
mod ident;
mod load_ddl;
mod parse_domain;
mod pg_fn;
mod pg_id;
mod pg_image;
mod pg_type;
mod pgrpc;
mod pgsql_check;
mod rel_index;
mod sql_state;
mod tests;
mod ty_index;

#[derive(Parser)]
#[clap(name = "pgrpc")]
struct Opt {
    /// Postgres schema directory
    #[clap(long, short, value_parser = clap::value_parser!(ClioPath).exists().is_dir(), default_value = "schema")]
    schema_dir: ClioPath,

    /// Function schemas
    #[clap(long, short)]
    fn_schemas: Vec<String>,

    /// Output file '-' for stdout
    #[clap(long, short, value_parser, default_value = "src/pgrpc.rs")]
    output: Output,
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let mut opt = Opt::parse();

    if !opt.schema_dir.is_dir() {
        return Err(anyhow::anyhow!("{} is not a directory", &opt.schema_dir));
    }

    run(opt.schema_dir.path(), opt.output.path()).await?;

    Ok(())
}

use rayon::iter::ParallelIterator;

pub async fn run(dir: &Path, output_path: &Path) -> anyhow::Result<()> {
    println!("Generating PgRPC functions...");

    let (src, fn_src_map) = load_ddl(dir)?;

    let db = Db::new(&src).await;

    let rel_index = RelIndex::new(&db.client).await?;
    let fn_index = FunctionIndex::new(&db.client, &rel_index).await?;
    let ty_index = TypeIndex::new(&db.client, fn_index.get_type_oids().as_slice()).await?;

    let mut err_count = 0;
    for f in fn_index.values() {
        if f.has_issues() {
            err_count += f.issues.len();
            f.report(&fn_src_map.get(&f.id()).unwrap());
        }
    }

    let code = codegen(&fn_index, &ty_index).await?;

    fs::write(output_path, code)?;

    if err_count > 0 {
        eprintln!("❌  {} problems detected by plpgsql_check", err_count);
    }

    println!(
        "✅  {} PgRPC functions written to {}",
        fn_index.len(),
        output_path.display()
    );

    Ok(())
}
