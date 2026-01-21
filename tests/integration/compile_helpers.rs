use indoc::indoc;
use std::path::Path;
use std::process::{Command, Output};
use tempfile::TempDir;

/// Create a minimal Cargo project for testing generated code
pub fn create_test_cargo_project(conn_string: &str, schemas: Vec<&str>) -> TempDir {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let project_dir = temp_dir.path();

    // Create src directory
    let src_dir = project_dir.join("src");
    std::fs::create_dir(&src_dir).expect("Should create src directory");

    // Generate pgrpc code in a subdirectory
    let pgrpc_dir = src_dir.join("generated");
    std::fs::create_dir(&pgrpc_dir).expect("Should create generated directory");

    let mut builder = pgrpc::PgrpcBuilder::new()
        .connection_string(conn_string)
        .output_path(&pgrpc_dir);

    for schema in schemas {
        builder = builder.schema(schema);
    }

    builder.build().expect("Code generation should succeed");

    // Create a simple lib.rs that uses the generated code
    let lib_rs_content = indoc! {"
        pub mod generated;
        
        pub use generated::*;
        
        // Re-export commonly used types
        pub use generated::errors::PgRpcError;
        #[allow(unused_imports)]
        pub use generated::public::*;
        #[allow(unused_imports)]
        pub use generated::api::*;
    "};
    std::fs::write(src_dir.join("lib.rs"), lib_rs_content).expect("Should write lib.rs");

    // Create Cargo.toml with all required dependencies
    let cargo_toml_content = indoc! {r#"
        [package]
        name = "test-project"
        version = "0.1.0"
        edition = "2021"
        
        [dependencies]
        tokio-postgres = { version = "0.7", features = ["with-serde_json-1", "with-time-0_3"] }
        postgres-types = { version = "0.2", features = ["derive"] }
        serde = { version = "1.0", features = ["derive"] }
        serde_json = "1.0"
        time = { version = "0.3", features = ["serde", "macros", "formatting", "parsing"] }
        thiserror = "1.0"
        deadpool-postgres = "0.12"
        tokio = { version = "1.0", features = ["full"] }
        postgis-butmaintained = "0.12"
        
    "#};
    std::fs::write(project_dir.join("Cargo.toml"), cargo_toml_content)
        .expect("Should write Cargo.toml");

    temp_dir
}

/// Add a test binary to the project
pub fn add_test_binary(project_dir: &Path, test_name: &str, test_code: &str) {
    let src_dir = project_dir.join("src");
    let test_file = src_dir.join(format!("{}.rs", test_name));
    std::fs::write(&test_file, test_code).expect(&format!("Should write {}.rs", test_name));

    // Update Cargo.toml to include the new binary
    let cargo_toml_path = project_dir.join("Cargo.toml");
    let mut cargo_content =
        std::fs::read_to_string(&cargo_toml_path).expect("Should read Cargo.toml");

    cargo_content.push_str(&format!(
        r#"
[[bin]]
name = "{}"
path = "src/{}.rs"
"#,
        test_name, test_name
    ));

    std::fs::write(cargo_toml_path, cargo_content).expect("Should update Cargo.toml");
}

/// Compile the test project
pub fn compile_project(project_dir: &Path) -> Output {
    Command::new("cargo")
        .arg("check")
        .current_dir(project_dir)
        .output()
        .expect("Should run cargo check")
}

/// Compile and run a specific binary in the test project
pub fn compile_and_run(project_dir: &Path, binary_name: &str) -> Output {
    // First compile
    let compile_output = Command::new("cargo")
        .arg("build")
        .arg("--bin")
        .arg(binary_name)
        .current_dir(project_dir)
        .output()
        .expect("Should run cargo build");

    if !compile_output.status.success() {
        return compile_output;
    }

    // Then run
    Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg(binary_name)
        .current_dir(project_dir)
        .output()
        .expect("Should run binary")
}

/// Compile and run a specific binary in the test project with arguments
pub fn compile_and_run_with_args(project_dir: &Path, binary_name: &str, args: &[&str]) -> Output {
    // First compile
    let compile_output = Command::new("cargo")
        .arg("build")
        .arg("--bin")
        .arg(binary_name)
        .current_dir(project_dir)
        .output()
        .expect("Should run cargo build");

    if !compile_output.status.success() {
        return compile_output;
    }

    // Then run with arguments
    let mut cmd = Command::new("cargo");
    cmd.arg("run").arg("--bin").arg(binary_name).arg("--");

    for arg in args {
        cmd.arg(arg);
    }

    cmd.current_dir(project_dir)
        .output()
        .expect("Should run binary")
}

/// Helper to print command output for debugging
pub fn print_output(output: &Output) {
    if !output.status.success() {
        eprintln!("Command failed with status: {:?}", output.status);
        eprintln!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
        eprintln!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));
    }
}

/// Assert that compilation succeeded
pub fn assert_compilation_success(output: Output) {
    if !output.status.success() {
        print_output(&output);
        panic!("Compilation failed");
    }
}

/// Assert that execution succeeded and optionally check output
pub fn assert_execution_success(output: Output, expected_output: Option<&str>) {
    if !output.status.success() {
        print_output(&output);
        panic!("Execution failed");
    }

    if let Some(expected) = expected_output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(expected),
            "Expected output '{}' not found in:\n{}",
            expected,
            stdout
        );
    }
}
