#![feature(iterator_try_collect)]
use clap::Parser;
use clio::{ClioPath, Output};

mod codegen;
mod config;
mod db;
mod exceptions;
mod fn_index;
mod fn_src_location;
mod ident;
mod load_ddl;
mod parse_domain;
mod pg_constraint;
mod pg_fn;
mod pg_id;
mod pg_image;
mod pg_rel;
mod pg_type;
mod pgsql_check;
mod rel_index;
mod sql_state;
mod tests;
mod ty_index;
use pgrpc::run;

#[derive(Parser)]
#[clap(name = "pgrpc")]
struct Opt {
    /// Postgres schema directory
    #[clap(long, short, value_parser = clap::value_parser!(ClioPath).exists().is_dir(), default_value = "schema")]
    schema_dir: ClioPath,

    #[clap(long, short, value_parser = clap::value_parser!(ClioPath).exists().is_file(), default_value = "pgrpc.toml")]
    config_path: ClioPath,

    /// Schemas containing functions we want to generate Rust code for.
    schemas: Vec<String>,

    #[clap(long, short, value_parser, default_value = "src/pgrpc.rs")]
    output: Output,
}

pub fn main() -> anyhow::Result<()> {
    let mut opt = Opt::parse();

    run(
        opt.schema_dir.path(),
        opt.output.path(),
        opt.config_path.path(),
    )?;

    Ok(())
}
