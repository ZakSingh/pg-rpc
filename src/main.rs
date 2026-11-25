use clap::Parser;
use pgrpc::PgrpcBuilder;
use std::path::PathBuf;

#[derive(Parser)]
#[clap(name = "pgrpc")]
struct Opt {
    /// Path to the pgrpc config file
    #[clap(long, short, default_value = "pgrpc.toml")]
    config_path: PathBuf,

    /// Output directory for the generated code (overrides `output_path` in the config)
    #[clap(long, short)]
    output: Option<PathBuf>,
}

pub fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    println!("Generating PgRPC functions...");

    let mut builder = PgrpcBuilder::from_config_file(&opt.config_path)?;

    if let Some(output) = opt.output {
        builder = builder.output_path(output);
    }

    builder.build()?;

    Ok(())
}
