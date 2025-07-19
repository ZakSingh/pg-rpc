// Example showing how to use pgrpc as a library at runtime

use pgrpc::PgrpcBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate code at runtime
    PgrpcBuilder::new()
        .connection_string("postgres://postgres:postgres@localhost:5432/mydb")
        .schemas(vec!["public", "api"])
        .type_mapping("ltree", "String")
        .exception("P0001", "Custom application error")
        .output_path("src/generated")
        .build()?;
    
    Ok(())
}