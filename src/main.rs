#![feature(iterator_try_collect)]
use clap::Parser;
use clio::{ClioPath, Output};

mod codegen;
mod config;
mod db;
mod exceptions;
mod fn_index;
mod ident;
mod parse_domain;
mod pg_constraint;
mod pg_fn;
mod pg_id;
mod pg_rel;
mod pg_type;
mod rel_index;
mod sql_state;
mod tests;
mod ty_index;
use pgrpc::PgrpcBuilder;

#[derive(Parser)]
#[clap(name = "pgrpc")]
struct Opt {
    #[clap(long, short, value_parser = clap::value_parser!(ClioPath).exists().is_file(), default_value = "pgrpc.toml")]
    config_path: ClioPath,

    #[clap(long, short, value_parser, default_value = "src/pgrpc")]
    output: Output,
}

pub fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    println!("Generating PgRPC functions...");
    
    PgrpcBuilder::from_config_file(opt.config_path.path())?
        .output_path(opt.output.path().to_path_buf())
        .build()?;

    Ok(())
}
