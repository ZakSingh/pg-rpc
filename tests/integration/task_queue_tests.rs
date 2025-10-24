use super::*;
use indoc::indoc;
use pgrpc::*;
use std::collections::HashMap;
use tempfile::TempDir;

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
fn create_custom_task_queue_config(
    temp_dir: &std::path::Path,
    conn_string: &str,
) -> std::path::PathBuf {
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
        let rows = client
            .query(query, &[&"tasks"])
            .expect("Should execute task introspection query");

        assert!(
            !rows.is_empty(),
            "Should find task types in the tasks schema"
        );

        // Verify we found our expected task types
        let task_names: Vec<String> = rows
            .iter()
            .map(|row| row.get::<_, String>("task_name"))
            .collect();

        assert!(task_names.contains(&"send_welcome_email".to_string()));
        assert!(task_names.contains(&"process_payment".to_string()));
        assert!(task_names.contains(&"resize_image".to_string()));
        assert!(task_names.contains(&"cleanup_files".to_string()));

        // Test field introspection for one task type
        let send_email_row = rows
            .iter()
            .find(|row| row.get::<_, String>("task_name") == "send_welcome_email")
            .expect("Should find send_welcome_email task");

        let fields_json: serde_json::Value = send_email_row.get("fields");
        let fields: Vec<serde_json::Value> =
            serde_json::from_value(fields_json).expect("Should parse fields JSON");

        assert_eq!(fields.len(), 4, "send_welcome_email should have 4 fields");

        // Verify field names and types
        let field_names: Vec<String> = fields
            .iter()
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
        assert!(
            generated.contains_key("tasks.rs"),
            "Should generate tasks.rs file"
        );

        let tasks_content = generated.get("tasks.rs").unwrap();

        // Verify the generated TaskPayload enum exists
        assert!(
            tasks_content.contains("pub enum TaskPayload"),
            "Should generate TaskPayload enum"
        );

        // Verify task variants are generated with proper names
        assert!(
            tasks_content.contains("SendWelcomeEmail"),
            "Should generate SendWelcomeEmail variant"
        );
        assert!(
            tasks_content.contains("ProcessPayment"),
            "Should generate ProcessPayment variant"
        );
        assert!(
            tasks_content.contains("ResizeImage"),
            "Should generate ResizeImage variant"
        );
        assert!(
            tasks_content.contains("CleanupFiles"),
            "Should generate CleanupFiles variant"
        );

        // Verify serde annotations
        assert!(
            tasks_content.contains("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]"),
            "Should have serde derives"
        );
        assert!(
            tasks_content.contains("#[serde(tag = \"task_name\", content = \"payload\")]"),
            "Should have serde tag configuration"
        );

        // Verify field mapping - check some specific fields (all nullable by default)
        assert!(
            tasks_content.contains("user_id") && tasks_content.contains("Option<i32>"),
            "Should map INTEGER to Option<i32>"
        );
        assert!(
            tasks_content.contains("email") && tasks_content.contains("Option<String>"),
            "Should map TEXT to Option<String>"
        );
        assert!(
            tasks_content.contains("amount") && tasks_content.contains("Option<rust_decimal::Decimal>"),
            "Should map DECIMAL to Option<rust_decimal::Decimal>"
        );
        assert!(
            tasks_content.contains("payment_id") && tasks_content.contains("Option<uuid::Uuid>"),
            "Should map UUID to Option<uuid::Uuid>"
        );

        // Verify generated methods
        assert!(
            tasks_content.contains("pub fn task_name(&self)"),
            "Should generate task_name method"
        );
        assert!(
            tasks_content.contains("pub fn from_database_row"),
            "Should generate from_database_row method"
        );
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
            time = { version = \"0.3\", features = [\"serde-well-known\"] }
        "};

        std::fs::write(project_dir.join("Cargo.toml"), cargo_toml)
            .expect("Should write Cargo.toml");

        // Create src directory
        let src_dir = project_dir.join("src");
        std::fs::create_dir(&src_dir).expect("Should create src directory");

        // Write the generated tasks code
        std::fs::write(src_dir.join("tasks.rs"), tasks_content).expect("Should write tasks.rs");

        // Create main.rs that uses the tasks
        let main_rs = indoc! {"
            mod tasks;
            use tasks::TaskPayload;

            fn main() {
                // Test serialization
                let email_payload = tasks::SendWelcomeEmailPayload {
                    user_id: Some(123),
                    email: Some(\"test@example.com\".to_string()),
                    template_name: Some(\"welcome\".to_string()),
                    send_at: Some(time::OffsetDateTime::now_utc()),
                };
                let email_task = TaskPayload::SendWelcomeEmail(email_payload);

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
                    TaskPayload::SendWelcomeEmail(payload) => {
                        assert_eq!(payload.user_id, Some(456));
                    }
                    _ => panic!(\"Should be SendWelcomeEmail variant\"),
                }

                println!(\"All tests passed!\");
            }
        "};

        std::fs::write(src_dir.join("main.rs"), main_rs).expect("Should write main.rs");

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
            panic!(
                "Generated code failed to run:\nstdout: {}\nstderr: {}",
                stdout, stderr
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("All tests passed!"),
            "Runtime tests should pass: {}",
            stdout
        );
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
            assert!(
                !tasks_content.contains("pub enum TaskPayload"),
                "Should not generate TaskPayload enum when no tasks found"
            );
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
        builder
            .build()
            .expect("Code generation should succeed without task queue config");

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
        assert!(
            !has_tasks_file,
            "Should not generate tasks.rs without task queue config"
        );
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

        // Verify type mappings (all fields are nullable by default in task payloads)
        // The fields have serde annotations, so we check that both the field name and type exist
        assert!(tasks_content.contains("id") && tasks_content.contains("Option<i64>"), "BIGINT -> Option<i64>");
        assert!(tasks_content.contains("name") && tasks_content.contains("Option<String>"), "TEXT -> Option<String>");
        assert!(
            tasks_content.contains("active") && tasks_content.contains("Option<bool>"),
            "BOOLEAN -> Option<bool>"
        );
        assert!(tasks_content.contains("score") && tasks_content.contains("Option<f32>"), "REAL -> Option<f32>");
        assert!(
            tasks_content.contains("precise_score") && tasks_content.contains("Option<f64>"),
            "DOUBLE PRECISION -> Option<f64>"
        );
        assert!(
            tasks_content.contains("amount") && tasks_content.contains("Option<rust_decimal::Decimal>"),
            "NUMERIC -> Option<rust_decimal::Decimal>"
        );
        assert!(
            tasks_content.contains("data") && tasks_content.contains("Option<serde_json::Value>"),
            "JSONB -> Option<serde_json::Value>"
        );
        // timestamp without timezone and with timezone both map to time::OffsetDateTime
        assert!(
            tasks_content.contains("created_at") && tasks_content.contains("Option<time::OffsetDateTime>"),
            "TIMESTAMP -> Option<time::OffsetDateTime>"
        );
        assert!(
            tasks_content.contains("created_at_tz") && tasks_content.contains("Option<time::OffsetDateTime>"),
            "TIMESTAMPTZ -> Option<time::OffsetDateTime>"
        );
        assert!(
            tasks_content.contains("birth_date") && tasks_content.contains("Option<time::Date>"),
            "DATE -> Option<time::Date>"
        );
        assert!(
            tasks_content.contains("work_time") && tasks_content.contains("Option<time::Time>"),
            "TIME -> Option<time::Time>"
        );
        assert!(
            tasks_content.contains("file_data") && tasks_content.contains("Option<Vec<u8>>"),
            "BYTEA -> Option<Vec<u8>>"
        );
        assert!(
            tasks_content.contains("ip_address") && tasks_content.contains("Option<std::net::IpAddr>"),
            "INET -> Option<std::net::IpAddr>"
        );
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
        assert!(
            generated_files.contains_key("tasks.rs"),
            "Should generate tasks.rs file"
        );

        let tasks_content = generated_files.get("tasks.rs").unwrap();

        // Verify the generated TaskPayload enum exists
        assert!(
            tasks_content.contains("pub enum TaskPayload"),
            "Should generate TaskPayload enum"
        );

        // Verify task variants are generated with proper names from custom schema
        assert!(
            tasks_content.contains("SendNotification"),
            "Should generate SendNotification variant"
        );
        assert!(
            tasks_content.contains("BackupData"),
            "Should generate BackupData variant"
        );

        // Verify serde configuration uses custom column names
        assert!(
            tasks_content.contains("#[serde(tag = \"job_type\", content = \"data\")]"),
            "Should have custom serde tag configuration"
        );

        // Verify helper methods return custom table configuration
        assert!(
            tasks_content.contains("\"queue.jobs\""),
            "Should include custom table name in generated code"
        );
        assert!(
            tasks_content.contains("\"job_type\""),
            "Should include custom task name column"
        );
        assert!(
            tasks_content.contains("\"data\""),
            "Should include custom payload column"
        );

        // Verify field mappings work correctly (all nullable by default)
        assert!(
            tasks_content.contains("user_id: Option<i32>"),
            "Should map INTEGER to Option<i32>"
        );
        assert!(
            tasks_content.contains("message: Option<String>"),
            "Should map TEXT to Option<String>"
        );
        assert!(
            tasks_content.contains("compression: Option<bool>"),
            "Should map BOOLEAN to Option<bool>"
        );
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
        let tasks_content =
            std::fs::read_to_string(&tasks_file_path).expect("Should read generated tasks.rs");

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
            time = { version = \"0.3\", features = [\"serde-well-known\"] }
            uuid = \"1.0\"
            rust_decimal = \"1.0\"
        "};

        std::fs::write(project_dir.join("Cargo.toml"), cargo_toml)
            .expect("Should write Cargo.toml");

        // Create src directory
        let src_dir = project_dir.join("src");
        std::fs::create_dir(&src_dir).expect("Should create src directory");

        // Write the generated tasks code
        std::fs::write(src_dir.join("tasks.rs"), tasks_content).expect("Should write tasks.rs");

        // Create main.rs that uses the custom configuration
        let main_rs = indoc! {"
            mod tasks;
            use tasks::TaskPayload;

            fn main() {
                // Test that we can create and serialize task payloads with custom config
                let notification_payload = tasks::SendNotificationPayload {
                    user_id: Some(456),
                    message: Some(\"Hello, World!\".to_string()),
                    channel: Some(\"email\".to_string()),
                };
                let notification_task = TaskPayload::SendNotification(notification_payload);

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
                    TaskPayload::BackupData(payload) => {
                        assert_eq!(payload.table_name, Some(\"users\".to_string()));
                        assert_eq!(payload.compression, Some(true));
                    }
                    _ => panic!(\"Should be BackupData variant\"),
                }

                println!(\"All custom configuration tests passed!\");
            }
        "};

        std::fs::write(src_dir.join("main.rs"), main_rs).expect("Should write main.rs");

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
            panic!(
                "Generated code failed to run:\nstdout: {}\nstderr: {}",
                stdout, stderr
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("All custom configuration tests passed!"),
            "Runtime tests should pass: {}",
            stdout
        );
    });
}

#[test]
fn test_timestamptz_serialization_deserialization() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a task type with timestamptz fields (like the bug report scenario)
        let task_schema_sql = indoc! {"
            CREATE SCHEMA IF NOT EXISTS tasks;
            CREATE SCHEMA IF NOT EXISTS mq;

            -- Create a task with direct timestamptz fields
            CREATE TYPE tasks.schedule_task AS (
                task_id BIGINT,
                scheduled_at TIMESTAMPTZ,
                description TEXT
            );

            -- Create another task with nullable timestamptz
            CREATE TYPE tasks.reminder_task AS (
                reminder_id BIGINT,
                remind_at TIMESTAMPTZ,
                message TEXT
            );
        "};

        execute_sql(client, task_schema_sql).expect("Should create task types with timestamptz");

        // Generate code
        let generated = test_task_queue_generation(conn_string);
        let tasks_content = generated.get("tasks.rs").unwrap();

        // Verify that the generated code includes the RFC3339 serde annotations
        assert!(
            tasks_content.contains("time::serde::rfc3339"),
            "Should generate time::serde::rfc3339 annotation for timestamptz fields"
        );

        // Verify the task payloads are generated
        assert!(
            tasks_content.contains("ScheduleTaskPayload") ||
            tasks_content.contains("schedule_task"),
            "Should generate ScheduleTask task"
        );

        assert!(
            tasks_content.contains("ReminderTaskPayload") ||
            tasks_content.contains("reminder_task"),
            "Should generate ReminderTask task"
        );

        // Verify type mapping for direct timestamptz fields (nullable by default)
        assert!(
            tasks_content.contains("pub scheduled_at: Option<time::OffsetDateTime>") ||
            tasks_content.contains("pub scheduled_at: time::OffsetDateTime"),
            "Should map timestamptz to Option<time::OffsetDateTime> (nullable) or time::OffsetDateTime"
        );

        // Create a test project to verify serialization/deserialization works
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let project_dir = temp_dir.path();

        // Create Cargo.toml with all necessary dependencies
        let cargo_toml = indoc! {"
            [package]
            name = \"test_timestamptz_serde\"
            version = \"0.1.0\"
            edition = \"2021\"

            [dependencies]
            serde = { version = \"1.0\", features = [\"derive\"] }
            serde_json = \"1.0\"
            time = { version = \"0.3\", features = [\"serde-well-known\", \"macros\"] }
            uuid = \"1.0\"
            rust_decimal = \"1.0\"
        "};

        std::fs::write(project_dir.join("Cargo.toml"), cargo_toml)
            .expect("Should write Cargo.toml");

        // Create src directory
        let src_dir = project_dir.join("src");
        std::fs::create_dir(&src_dir).expect("Should create src directory");

        // Write the generated tasks code
        std::fs::write(src_dir.join("tasks.rs"), tasks_content)
            .expect("Should write tasks.rs");

        // Create main.rs that tests serialization/deserialization
        let main_rs = indoc! {r#"
            mod tasks;

            use serde_json;

            fn main() {
                // Test: Direct timestamptz field serialization/deserialization
                // Simulate JSONB payload from database (like the bug report)
                let json_payload = serde_json::json!({
                    "task_id": 123,
                    "scheduled_at": "2025-10-24T00:49:00.064902+00:00",
                    "description": "Test task"
                });

                // This should deserialize successfully with the RFC3339 annotation
                // This is THE KEY TEST - without the annotation, this would fail with:
                // "invalid type: string "2025-10-24T00:49:00.064902+00:00", expected an `OffsetDateTime`"
                let deserialized: tasks::ScheduleTaskPayload =
                    serde_json::from_value(json_payload.clone())
                        .expect("Should deserialize timestamptz from RFC3339 string");

                assert_eq!(deserialized.task_id, Some(123));
                assert_eq!(deserialized.description, Some("Test task".to_string()));
                assert!(deserialized.scheduled_at.is_some(), "scheduled_at should be Some");

                // Verify serialization round-trip
                let serialized = serde_json::to_value(&deserialized)
                    .expect("Should serialize");

                // The timestamp should be in RFC3339 format (or null if None)
                let scheduled_at_value = &serialized["scheduled_at"];
                if !scheduled_at_value.is_null() {
                    assert!(scheduled_at_value.is_string(), "scheduled_at should be string if not null");
                    // Verify it's actually RFC3339 format
                    let timestamp_str = scheduled_at_value.as_str().unwrap();
                    assert!(timestamp_str.contains('T'), "RFC3339 timestamp should contain 'T'");
                    assert!(timestamp_str.contains('+') || timestamp_str.contains('Z'),
                           "RFC3339 timestamp should have timezone");
                }

                println!("✓ Test passed: timestamptz serialization/deserialization works!");

                // Test nullable field with null value
                let null_payload = serde_json::json!({
                    "reminder_id": 456,
                    "remind_at": null,
                    "message": "Reminder"
                });

                let null_deserialized: tasks::ReminderTaskPayload =
                    serde_json::from_value(null_payload)
                        .expect("Should deserialize null timestamptz");

                assert_eq!(null_deserialized.reminder_id, Some(456));
                assert!(null_deserialized.remind_at.is_none(), "remind_at should be None");

                println!("✓ Test passed: null timestamptz works!");

                println!("\n✅ All timestamptz serialization/deserialization tests passed!");
            }
        "#};

        std::fs::write(src_dir.join("main.rs"), main_rs)
            .expect("Should write main.rs");

        // Try to compile and run the test
        let output = std::process::Command::new("cargo")
            .arg("run")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo run");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!(
                "Timestamptz serialization test failed to run:\nstdout: {}\nstderr: {}",
                stdout, stderr
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("All timestamptz serialization/deserialization tests passed!"),
            "Timestamptz tests should pass: {}",
            stdout
        );
    });
}

#[test]
fn test_date_serialization_deserialization() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a task type with date fields
        let task_schema_sql = indoc! {"
            CREATE SCHEMA IF NOT EXISTS tasks;
            CREATE SCHEMA IF NOT EXISTS mq;

            -- Create a task with date fields
            CREATE TYPE tasks.birthday_reminder AS (
                person_id BIGINT,
                birth_date DATE,
                reminder_date DATE
            );
        "};

        execute_sql(client, task_schema_sql).expect("Should create task types with date");

        // Generate code
        let generated = test_task_queue_generation(conn_string);
        let tasks_content = generated.get("tasks.rs").unwrap();

        // Verify that Date fields are generated
        assert!(
            tasks_content.contains("time::Date"),
            "Should map DATE to time::Date"
        );

        // Create a test project to verify serialization/deserialization works
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let project_dir = temp_dir.path();

        // Create Cargo.toml with all necessary dependencies
        let cargo_toml = indoc! {"
            [package]
            name = \"test_date_serde\"
            version = \"0.1.0\"
            edition = \"2021\"

            [dependencies]
            serde = { version = \"1.0\", features = [\"derive\"] }
            serde_json = \"1.0\"
            time = { version = \"0.3\", features = [\"serde-well-known\", \"macros\"] }
            uuid = \"1.0\"
            rust_decimal = \"1.0\"
        "};

        std::fs::write(project_dir.join("Cargo.toml"), cargo_toml)
            .expect("Should write Cargo.toml");

        // Create src directory
        let src_dir = project_dir.join("src");
        std::fs::create_dir(&src_dir).expect("Should create src directory");

        // Write the generated tasks code
        std::fs::write(src_dir.join("tasks.rs"), tasks_content)
            .expect("Should write tasks.rs");

        // Create main.rs that tests serialization/deserialization
        let main_rs = indoc! {r#"
            mod tasks;

            use serde_json;

            fn main() {
                // Test: Date field serialization/deserialization
                // Simulate JSONB payload from database with DATE values
                let json_payload = serde_json::json!({
                    "person_id": 123,
                    "birth_date": "2022-11-05",
                    "reminder_date": "2025-11-05"
                });

                // This should deserialize successfully with serde-human-readable
                // This is THE KEY TEST - without proper serde support, this would fail with:
                // "invalid type: string \"2022-11-05\", expected a `Date`"
                let deserialized: tasks::BirthdayReminderPayload =
                    serde_json::from_value(json_payload.clone())
                        .expect("Should deserialize Date from YYYY-MM-DD string");

                assert_eq!(deserialized.person_id, Some(123));
                assert!(deserialized.birth_date.is_some(), "birth_date should be Some");
                assert!(deserialized.reminder_date.is_some(), "reminder_date should be Some");

                // Verify serialization round-trip
                let serialized = serde_json::to_value(&deserialized)
                    .expect("Should serialize");

                // The date should be in YYYY-MM-DD format
                let birth_date_value = &serialized["birth_date"];
                if !birth_date_value.is_null() {
                    assert!(birth_date_value.is_string(), "birth_date should be string if not null");
                    let date_str = birth_date_value.as_str().unwrap();
                    assert_eq!(date_str.len(), 10, "Date should be YYYY-MM-DD format (10 chars)");
                    assert_eq!(date_str, "2022-11-05", "Date should match input");
                }

                println!("✓ Test passed: Date serialization/deserialization works!");

                // Test nullable field with null value
                let null_payload = serde_json::json!({
                    "person_id": 456,
                    "birth_date": null,
                    "reminder_date": "2025-12-25"
                });

                let null_deserialized: tasks::BirthdayReminderPayload =
                    serde_json::from_value(null_payload)
                        .expect("Should deserialize null Date");

                assert_eq!(null_deserialized.person_id, Some(456));
                assert!(null_deserialized.birth_date.is_none(), "birth_date should be None");
                assert!(null_deserialized.reminder_date.is_some(), "reminder_date should be Some");

                println!("✓ Test passed: null Date works!");

                println!("\n✅ All Date serialization/deserialization tests passed!");
            }
        "#};

        std::fs::write(src_dir.join("main.rs"), main_rs)
            .expect("Should write main.rs");

        // Try to compile and run the test
        let output = std::process::Command::new("cargo")
            .arg("run")
            .current_dir(project_dir)
            .output()
            .expect("Should run cargo run");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!(
                "Date serialization test failed to run:\nstdout: {}\nstderr: {}",
                stdout, stderr
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("All Date serialization/deserialization tests passed!"),
            "Date tests should pass: {}",
            stdout
        );
    });
}
