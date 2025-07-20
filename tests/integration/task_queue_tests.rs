use super::*;
use pgrpc::*;
use std::collections::HashMap;
use tempfile::TempDir;
use indoc::indoc;

/// Helper to create a test database with task queue schema and composite types
fn setup_task_queue_schema(client: &mut Client) -> Result<(), postgres::Error> {
    let task_schema_sql = indoc! {"
        -- Create the tasks schema for message queue task types
        CREATE SCHEMA IF NOT EXISTS tasks;
        
        -- Create composite types for different task types
        CREATE TYPE tasks.send_welcome_email AS (
            user_id INTEGER,
            email TEXT,
            template_name TEXT,
            send_at TIMESTAMPTZ
        );
        
        CREATE TYPE tasks.process_payment AS (
            payment_id UUID,
            amount DECIMAL(10,2),
            currency TEXT,
            retry_count INTEGER
        );
        
        CREATE TYPE tasks.resize_image AS (
            image_id BIGINT,
            target_width INTEGER,
            target_height INTEGER,
            quality INTEGER,
            format TEXT
        );
        
        CREATE TYPE tasks.cleanup_files AS (
            directory_path TEXT,
            older_than_days INTEGER,
            dry_run BOOLEAN
        );
        
        -- Create the actual message queue table that would use these task types
        CREATE TABLE IF NOT EXISTS mq.task (
            task_id SERIAL PRIMARY KEY,
            task_name TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            claimed_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            failed_at TIMESTAMPTZ,
            retry_count INTEGER NOT NULL DEFAULT 0
        );
        
        -- Sample function that might use the task system
        CREATE OR REPLACE FUNCTION mq.claim_task()
        RETURNS TABLE(task_id INTEGER, task_name TEXT, payload JSONB)
        LANGUAGE SQL AS $$
            UPDATE mq.task 
            SET claimed_at = NOW()
            WHERE task_id = (
                SELECT t.task_id FROM mq.task t
                WHERE t.claimed_at IS NULL AND t.failed_at IS NULL
                ORDER BY t.created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING task.task_id, task.task_name, task.payload;
        $$;
    "};
    
    // First create the mq schema for the queue table
    execute_sql(client, "CREATE SCHEMA IF NOT EXISTS mq;")?;
    execute_sql(client, task_schema_sql)?;
    
    Ok(())
}

/// Helper to create a test database with custom task queue schema
fn setup_custom_task_queue_schema(client: &mut Client) -> Result<(), postgres::Error> {
    let task_schema_sql = indoc! {"
        -- Create the task_types schema for composite types
        CREATE SCHEMA IF NOT EXISTS task_types;
        
        -- Create the queue schema for the task table
        CREATE SCHEMA IF NOT EXISTS queue;
        
        -- Create composite types for different task types in custom schema
        CREATE TYPE task_types.send_notification AS (
            user_id INTEGER,
            message TEXT,
            channel TEXT
        );
        
        CREATE TYPE task_types.backup_data AS (
            table_name TEXT,
            backup_location TEXT,
            compression BOOLEAN
        );
        
        -- Create the actual task table with custom schema and columns
        CREATE TABLE IF NOT EXISTS queue.jobs (
            id SERIAL PRIMARY KEY,
            job_type TEXT NOT NULL,
            data JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            status TEXT DEFAULT 'pending'
        );
    "};
    
    execute_sql(client, task_schema_sql)?;
    
    Ok(())
}

/// Helper to create a pgrpc.toml config with task queue configuration
fn create_task_queue_config(temp_dir: &std::path::Path, conn_string: &str) -> std::path::PathBuf {
    let config_content = format!(
        r#"
connection_string = "{}"
schemas = ["public", "api", "mq"]

[task_queue]
schema = "tasks"
task_name_column = "task_name"
payload_column = "payload"
"#,
        conn_string
    );
    
    let config_path = temp_dir.join("pgrpc.toml");
    std::fs::write(&config_path, config_content).expect("Failed to write config file");
    config_path
}

/// Helper to create a pgrpc.toml config with custom table configuration
fn create_custom_task_queue_config(temp_dir: &std::path::Path, conn_string: &str) -> std::path::PathBuf {
    let config_content = format!(
        r#"
connection_string = "{}"
schemas = ["public", "api", "queue"]

[task_queue]
schema = "task_types"
table_schema = "queue"
table_name = "jobs"
task_name_column = "job_type"
payload_column = "data"
"#,
        conn_string
    );
    
    let config_path = temp_dir.join("pgrpc.toml");
    std::fs::write(&config_path, config_content).expect("Failed to write config file");
    config_path
}

/// Helper to test task queue code generation 
fn test_task_queue_generation(conn_string: &str) -> HashMap<String, String> {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config_path = create_task_queue_config(temp_dir.path(), conn_string);
    let output_path = temp_dir.path().join("generated");
    
    let builder = PgrpcBuilder::from_config_file(config_path)
        .expect("Should load config file")
        .output_path(&output_path);
    
    // Execute the build
    builder.build().expect("Code generation should succeed");
    
    // Read generated files
    let mut generated_files = HashMap::new();
    
    for entry in std::fs::read_dir(&output_path).expect("Should read output directory") {
        let entry = entry.expect("Should read directory entry");
        let path = entry.path();
        
        if path.extension().map_or(false, |ext| ext == "rs") {
            let filename = path.file_name().unwrap().to_string_lossy().to_string();
            let content = std::fs::read_to_string(&path).expect("Should read generated file");
            generated_files.insert(filename, content);
        }
    }
    
    generated_files
}

#[test]
fn test_task_queue_schema_introspection() {
    with_isolated_database(|client| {
        // Set up task queue schema
        setup_task_queue_schema(client).expect("Should setup task queue schema");
        
        // Test that we can introspect the task types
        let query = include_str!("../../src/queries/task_introspection.sql");
        let rows = client.query(query, &[&"tasks"])
            .expect("Should execute task introspection query");
        
        assert!(!rows.is_empty(), "Should find task types in the tasks schema");
        
        // Verify we found our expected task types
        let task_names: Vec<String> = rows.iter()
            .map(|row| row.get::<_, String>("task_name"))
            .collect();
        
        assert!(task_names.contains(&"send_welcome_email".to_string()));
        assert!(task_names.contains(&"process_payment".to_string()));
        assert!(task_names.contains(&"resize_image".to_string()));
        assert!(task_names.contains(&"cleanup_files".to_string()));
        
        // Test field introspection for one task type
        let send_email_row = rows.iter()
            .find(|row| row.get::<_, String>("task_name") == "send_welcome_email")
            .expect("Should find send_welcome_email task");
        
        let fields_json: serde_json::Value = send_email_row.get("fields");
        let fields: Vec<serde_json::Value> = serde_json::from_value(fields_json)
            .expect("Should parse fields JSON");
        
        assert_eq!(fields.len(), 4, "send_welcome_email should have 4 fields");
        
        // Verify field names and types
        let field_names: Vec<String> = fields.iter()
            .map(|f| f["name"].as_str().unwrap().to_string())
            .collect();
        
        assert!(field_names.contains(&"user_id".to_string()));
        assert!(field_names.contains(&"email".to_string()));
        assert!(field_names.contains(&"template_name".to_string()));
        assert!(field_names.contains(&"send_at".to_string()));
    });
}

#[test]
fn test_task_queue_code_generation() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Set up task queue schema
        setup_task_queue_schema(client).expect("Should setup task queue schema");
        
        let generated = test_task_queue_generation(conn_string);
        
        // Should have generated the tasks module
        assert!(generated.contains_key("tasks.rs"), "Should generate tasks.rs file");
        
        let tasks_content = generated.get("tasks.rs").unwrap();
        
        // Verify the generated TaskPayload enum exists
        assert!(tasks_content.contains("pub enum TaskPayload"), 
                "Should generate TaskPayload enum");
        
        // Verify task variants are generated with proper names
        assert!(tasks_content.contains("SendWelcomeEmail"), 
                "Should generate SendWelcomeEmail variant");
        assert!(tasks_content.contains("ProcessPayment"), 
                "Should generate ProcessPayment variant");
        assert!(tasks_content.contains("ResizeImage"), 
                "Should generate ResizeImage variant");
        assert!(tasks_content.contains("CleanupFiles"), 
                "Should generate CleanupFiles variant");
        
        // Verify serde annotations
        assert!(tasks_content.contains("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]"),
                "Should have serde derives");
        assert!(tasks_content.contains("#[serde(tag = \"task_name\", content = \"payload\")]"),
                "Should have serde tag configuration");
        
        // Verify field mapping - check some specific fields
        assert!(tasks_content.contains("pub user_id: i32"), 
                "Should map INTEGER to i32");
        assert!(tasks_content.contains("pub email: String"), 
                "Should map TEXT to String");
        assert!(tasks_content.contains("pub amount: rust_decimal::Decimal"), 
                "Should map DECIMAL to rust_decimal::Decimal");
        assert!(tasks_content.contains("pub payment_id: uuid::Uuid"), 
                "Should map UUID to uuid::Uuid");
        
        // Verify generated methods
        assert!(tasks_content.contains("pub fn task_name(&self)"), 
                "Should generate task_name method");
        assert!(tasks_content.contains("pub fn from_database_row"), 
                "Should generate from_database_row method");
    });
}

#[test]
fn test_generated_task_enum_compiles() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Set up task queue schema
        setup_task_queue_schema(client).expect("Should setup task queue schema");
        
        let generated = test_task_queue_generation(conn_string);
        let tasks_content = generated.get("tasks.rs").unwrap();
        
        // Create a temporary Rust project to test compilation
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let project_dir = temp_dir.path();
        
        // Create Cargo.toml
        let cargo_toml = indoc! {"
            [package]
            name = \"test_tasks\"
            version = \"0.1.0\"
            edition = \"2021\"
            
            [dependencies]
            serde = { version = \"1.0\", features = [\"derive\"] }
            serde_json = \"1.0\"
            uuid = { version = \"1.0\", features = [\"v4\"] }
            rust_decimal = \"1.0\"
            chrono = { version = \"0.4\", features = [\"serde\"] }
        "};
        
        std::fs::write(project_dir.join("Cargo.toml"), cargo_toml)
            .expect("Should write Cargo.toml");
        
        // Create src directory
        let src_dir = project_dir.join("src");
        std::fs::create_dir(&src_dir).expect("Should create src directory");
        
        // Write the generated tasks code
        std::fs::write(src_dir.join("tasks.rs"), tasks_content)
            .expect("Should write tasks.rs");
        
        // Create main.rs that uses the tasks
        let main_rs = indoc! {"
            mod tasks;
            use tasks::TaskPayload;
            
            fn main() {
                // Test that we can create and serialize task payloads
                let email_task = TaskPayload::SendWelcomeEmail {
                    user_id: 123,
                    email: \"test@example.com\".to_string(),
                    template_name: \"welcome\".to_string(),
                    send_at: chrono::Utc::now(),
                };
                
                // Test serialization
                let json = serde_json::to_string(&email_task).expect(\"Should serialize\");
                println!(\"Serialized: {}\", json);
                
                // Test task_name method
                assert_eq!(email_task.task_name(), \"send_welcome_email\");
                
                // Test deserialization
                let task_name = \"send_welcome_email\";
                let payload = serde_json::json!({
                    \"user_id\": 456,
                    \"email\": \"test2@example.com\",
                    \"template_name\": \"welcome\",
                    \"send_at\": \"2023-01-01T00:00:00Z\"
                });
                
                let reconstructed = TaskPayload::from_database_row(task_name, payload)
                    .expect(\"Should deserialize from database format\");
                
                match reconstructed {
                    TaskPayload::SendWelcomeEmail { user_id, .. } => {
                        assert_eq!(user_id, 456);
                    }
                    _ => panic!(\"Should be SendWelcomeEmail variant\"),
                }
                
                println!(\"All tests passed!\");
            }
        "};
        
        std::fs::write(src_dir.join("main.rs"), main_rs)
            .expect("Should write main.rs");
        
        // Try to compile the project
        let output = std::process::Command::new("cargo")
            .arg("check")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo check");
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("Generated code failed to compile:\n{}", stderr);
        }
        
        // Also try to build and run it
        let output = std::process::Command::new("cargo")
            .arg("run")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo run");
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("Generated code failed to run:\nstdout: {}\nstderr: {}", stdout, stderr);
        }
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("All tests passed!"), 
                "Runtime tests should pass: {}", stdout);
    });
}

#[test]
fn test_empty_task_schema() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create tasks schema but no composite types
        execute_sql(client, "CREATE SCHEMA IF NOT EXISTS tasks;")
            .expect("Should create empty tasks schema");
        
        let generated = test_task_queue_generation(conn_string);
        
        // Should still generate files but tasks.rs should be minimal
        if let Some(tasks_content) = generated.get("tasks.rs") {
            // Should not contain TaskPayload enum since no tasks found
            assert!(!tasks_content.contains("pub enum TaskPayload"), 
                    "Should not generate TaskPayload enum when no tasks found");
        }
    });
}

#[test]
fn test_task_queue_without_config() {
    with_isolated_database_and_container(|_client, _container, conn_string| {
        // Test generation without task_queue config
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();
        
        let builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path);
        
        // Should succeed without task queue config
        builder.build().expect("Code generation should succeed without task queue config");
        
        // Read generated files
        let mut has_tasks_file = false;
        for entry in std::fs::read_dir(output_path).expect("Should read output directory") {
            let entry = entry.expect("Should read directory entry");
            let filename = entry.file_name().to_string_lossy().to_string();
            if filename == "tasks.rs" {
                has_tasks_file = true;
                break;
            }
        }
        
        // Should not generate tasks.rs when no task queue config
        assert!(!has_tasks_file, "Should not generate tasks.rs without task queue config");
    });
}

#[test]
fn test_task_queue_type_mapping() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a task type with various PostgreSQL types to test mapping
        let complex_task_sql = indoc! {"
            CREATE SCHEMA IF NOT EXISTS tasks;
            
            CREATE TYPE tasks.complex_task AS (
                id BIGINT,
                name TEXT,
                active BOOLEAN,
                score REAL,
                precise_score DOUBLE PRECISION,
                amount NUMERIC(10,2),
                data JSONB,
                created_at TIMESTAMP,
                created_at_tz TIMESTAMPTZ,
                birth_date DATE,
                work_time TIME,
                file_data BYTEA,
                ip_address INET
            );
        "};
        
        execute_sql(client, complex_task_sql).expect("Should create complex task type");
        
        let generated = test_task_queue_generation(conn_string);
        let tasks_content = generated.get("tasks.rs").unwrap();
        
        // Verify type mappings
        assert!(tasks_content.contains("pub id: i64"), "BIGINT -> i64");
        assert!(tasks_content.contains("pub name: String"), "TEXT -> String");
        assert!(tasks_content.contains("pub active: bool"), "BOOLEAN -> bool");
        assert!(tasks_content.contains("pub score: f32"), "REAL -> f32");
        assert!(tasks_content.contains("pub precise_score: f64"), "DOUBLE PRECISION -> f64");
        assert!(tasks_content.contains("pub amount: rust_decimal::Decimal"), "NUMERIC -> rust_decimal::Decimal");
        assert!(tasks_content.contains("pub data: serde_json::Value"), "JSONB -> serde_json::Value");
        assert!(tasks_content.contains("pub created_at: chrono::NaiveDateTime"), "TIMESTAMP -> chrono::NaiveDateTime");
        assert!(tasks_content.contains("pub created_at_tz: chrono::DateTime<chrono::Utc>"), "TIMESTAMPTZ -> chrono::DateTime<Utc>");
        assert!(tasks_content.contains("pub birth_date: chrono::NaiveDate"), "DATE -> chrono::NaiveDate");
        assert!(tasks_content.contains("pub work_time: chrono::NaiveTime"), "TIME -> chrono::NaiveTime");
        assert!(tasks_content.contains("pub file_data: Vec<u8>"), "BYTEA -> Vec<u8>");
        assert!(tasks_content.contains("pub ip_address: std::net::IpAddr"), "INET -> std::net::IpAddr");
    });
}

#[test]
fn test_custom_table_configuration() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Set up custom task queue schema
        setup_custom_task_queue_schema(client).expect("Should setup custom task queue schema");
        
        // Generate with custom configuration
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let config_path = create_custom_task_queue_config(temp_dir.path(), conn_string);
        let output_path = temp_dir.path().join("generated");
        
        let builder = PgrpcBuilder::from_config_file(config_path)
            .expect("Should load config file")
            .output_path(&output_path);
        
        // Execute the build
        builder.build().expect("Code generation should succeed");
        
        // Read generated files
        let mut generated_files = HashMap::new();
        for entry in std::fs::read_dir(&output_path).expect("Should read output directory") {
            let entry = entry.expect("Should read directory entry");
            let path = entry.path();
            
            if path.extension().map_or(false, |ext| ext == "rs") {
                let filename = path.file_name().unwrap().to_string_lossy().to_string();
                let content = std::fs::read_to_string(&path).expect("Should read generated file");
                generated_files.insert(filename, content);
            }
        }
        
        // Should have generated the tasks module
        assert!(generated_files.contains_key("tasks.rs"), "Should generate tasks.rs file");
        
        let tasks_content = generated_files.get("tasks.rs").unwrap();
        
        // Verify the generated TaskPayload enum exists
        assert!(tasks_content.contains("pub enum TaskPayload"), 
                "Should generate TaskPayload enum");
        
        // Verify task variants are generated with proper names from custom schema
        assert!(tasks_content.contains("SendNotification"), 
                "Should generate SendNotification variant");
        assert!(tasks_content.contains("BackupData"), 
                "Should generate BackupData variant");
        
        // Verify serde configuration uses custom column names
        assert!(tasks_content.contains("#[serde(tag = \"job_type\", content = \"data\")]"),
                "Should have custom serde tag configuration");
        
        // Verify helper methods return custom table configuration
        assert!(tasks_content.contains("\"queue.jobs\""),
                "Should include custom table name in generated code");
        assert!(tasks_content.contains("\"job_type\""),
                "Should include custom task name column");
        assert!(tasks_content.contains("\"data\""),
                "Should include custom payload column");
        
        // Verify field mappings work correctly
        assert!(tasks_content.contains("pub user_id: i32"), "Should map INTEGER to i32");
        assert!(tasks_content.contains("pub message: String"), "Should map TEXT to String");
        assert!(tasks_content.contains("pub compression: bool"), "Should map BOOLEAN to bool");
    });
}

#[test]
fn test_custom_table_compilation() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Set up custom task queue schema
        setup_custom_task_queue_schema(client).expect("Should setup custom task queue schema");
        
        // Generate with custom configuration
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let config_path = create_custom_task_queue_config(temp_dir.path(), conn_string);
        let output_path = temp_dir.path().join("generated");
        
        let builder = PgrpcBuilder::from_config_file(config_path)
            .expect("Should load config file")
            .output_path(&output_path);
        
        builder.build().expect("Code generation should succeed");
        
        // Read generated tasks file
        let tasks_file_path = output_path.join("tasks.rs");
        let tasks_content = std::fs::read_to_string(&tasks_file_path)
            .expect("Should read generated tasks.rs");
        
        // Create a temporary Rust project to test compilation
        let test_dir = TempDir::new().expect("Failed to create temp directory");
        let project_dir = test_dir.path();
        
        // Create Cargo.toml
        let cargo_toml = indoc! {"
            [package]
            name = \"test_custom_tasks\"
            version = \"0.1.0\"
            edition = \"2021\"
            
            [dependencies]
            serde = { version = \"1.0\", features = [\"derive\"] }
            serde_json = \"1.0\"
            chrono = { version = \"0.4\", features = [\"serde\"] }
        "};
        
        std::fs::write(project_dir.join("Cargo.toml"), cargo_toml)
            .expect("Should write Cargo.toml");
        
        // Create src directory
        let src_dir = project_dir.join("src");
        std::fs::create_dir(&src_dir).expect("Should create src directory");
        
        // Write the generated tasks code
        std::fs::write(src_dir.join("tasks.rs"), tasks_content)
            .expect("Should write tasks.rs");
        
        // Create main.rs that uses the custom configuration
        let main_rs = indoc! {"
            mod tasks;
            use tasks::TaskPayload;
            
            fn main() {
                // Test that we can create and serialize task payloads with custom config
                let notification_task = TaskPayload::SendNotification {
                    user_id: 456,
                    message: \"Hello, World!\".to_string(),
                    channel: \"email\".to_string(),
                };
                
                // Test serialization with custom column names
                let json = serde_json::to_string(&notification_task).expect(\"Should serialize\");
                println!(\"Serialized: {}\", json);
                
                // Test custom helper methods
                assert_eq!(TaskPayload::table_name(), \"queue.jobs\");
                assert_eq!(TaskPayload::task_name_column(), \"job_type\");
                assert_eq!(TaskPayload::payload_column(), \"data\");
                
                // Test task_name method
                assert_eq!(notification_task.task_name(), \"send_notification\");
                
                // Test deserialization
                let task_name = \"backup_data\";
                let payload = serde_json::json!({
                    \"table_name\": \"users\",
                    \"backup_location\": \"/backups/users.sql\",
                    \"compression\": true
                });
                
                let reconstructed = TaskPayload::from_database_row(task_name, payload)
                    .expect(\"Should deserialize from database format\");
                
                match reconstructed {
                    TaskPayload::BackupData { table_name, compression, .. } => {
                        assert_eq!(table_name, \"users\");
                        assert_eq!(compression, true);
                    }
                    _ => panic!(\"Should be BackupData variant\"),
                }
                
                println!(\"All custom configuration tests passed!\");
            }
        "};
        
        std::fs::write(src_dir.join("main.rs"), main_rs)
            .expect("Should write main.rs");
        
        // Try to compile the project
        let output = std::process::Command::new("cargo")
            .arg("check")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo check");
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("Generated code failed to compile:\n{}", stderr);
        }
        
        // Also try to build and run it
        let output = std::process::Command::new("cargo")
            .arg("run")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo run");
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("Generated code failed to run:\nstdout: {}\nstderr: {}", stdout, stderr);
        }
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("All custom configuration tests passed!"), 
                "Runtime tests should pass: {}", stdout);
    });
}