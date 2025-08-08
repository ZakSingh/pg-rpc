use clap::Parser;
use clio::{ClioPath, Output};
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
