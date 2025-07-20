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
mod unified_error;
use pgrpc::PgrpcBuilder;

#[derive(Parser)]
#[clap(name = "pgrpc")]
struct Opt {
    #[clap(long, short, value_parser = clap::value_parser!(ClioPath).exists().is_file(), default_value = "pgrpc.toml")]
    config_path: ClioPath,

    #[clap(long, short, value_parser)]
    output: Option<Output>,
}

pub fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    println!("Generating PgRPC functions...");
    
    let mut builder = PgrpcBuilder::from_config_file(opt.config_path.path())?;
    
    // If CLI output is provided, it overrides config
    if let Some(output) = opt.output {
        builder = builder.output_path(output.path().to_path_buf());
    }
    
    builder.build()?;

    Ok(())
}
