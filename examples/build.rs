// Example build.rs file showing how to use pgrpc in a build script

use pgrpc::PgrpcBuilder;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get the output directory for generated files
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    
    // Option 1: Use a config file (uses output_path from config if specified)
    PgrpcBuilder::from_config_file("pgrpc.toml")?
        .build()?;
    
    // Option 2: Use config file but override output path
    // PgrpcBuilder::from_config_file("pgrpc.toml")?
    //     .output_path(out_dir.join("pgrpc"))
    //     .build()?;
    
    // Option 3: Configure programmatically
    // let connection_string = env::var("DATABASE_URL")
    //     .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/mydb".to_string());
    //
    // PgrpcBuilder::new()
    //     .connection_string(connection_string)
    //     .schema("public")
    //     .schema("api") 
    //     .type_mapping("ltree", "String")
    //     .exception("P0001", "Custom application error")
    //     .output_path(out_dir.join("pgrpc"))
    //     .build()?;
    
    println!("cargo:rerun-if-changed=pgrpc.toml");
    
    Ok(())
}