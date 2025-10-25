use indoc::indoc;
use postgres::{Client, NoTls};
use std::sync::{Mutex, Once};
use testcontainers_modules::{postgres as postgres_module, testcontainers::runners::SyncRunner};
use uuid::Uuid;

pub mod codegen_tests;
pub mod compile_helpers;
pub mod composite_fromsql_tests;
pub mod custom_error_tests;
pub mod error_tests;
pub mod flatten_tests;
pub mod function_flatten_tests;
pub mod function_out_params_tests;
pub mod miniswap_view_nullability_tests;
pub mod out_param_nullability_tests;
pub mod postgis_tests;
pub mod procedure_tests;
pub mod query_tests;
pub mod schema_tests;
pub mod sql_function_tests;
pub mod task_queue_tests;
pub mod trigger_tests;
pub mod bulk_nullability_minimal_tests;
pub mod view_constraint_test;
pub mod view_nullability_tests;
pub mod view_query_nullability_test;
pub mod workflow_tests;

/// Global container instance that's shared across all tests
static INIT: Once = Once::new();
static CONTAINER: Mutex<Option<PostgresTestContainer>> = Mutex::new(None);

/// Wrapper around the testcontainer for easier management
pub struct PostgresTestContainer {
    _container: testcontainers_modules::testcontainers::Container<postgres_module::Postgres>,
    connection_string: String,
    host: String,
    port: u16,
}

impl PostgresTestContainer {
    fn new() -> Self {
        let container = postgres_module::Postgres::default()
            .start()
            .expect("Failed to start PostgreSQL container");

        let host = "127.0.0.1".to_string();
        let port = container
            .get_host_port_ipv4(5432)
            .expect("Failed to get container port");

        let connection_string = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

        Self {
            _container: container,
            connection_string,
            host,
            port,
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Create a new database connection to the default postgres database
    pub fn connect(&self) -> Result<Client, postgres::Error> {
        Client::connect(&self.connection_string, NoTls)
    }

    /// Get connection string for the admin postgres database
    pub fn admin_connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Create a new isolated database and return its name
    pub fn create_test_database(&self) -> Result<String, postgres::Error> {
        let db_name = format!("test_{}", Uuid::new_v4().to_string().replace('-', "_"));
        let mut admin_client = self.connect()?;
        admin_client.execute(&format!("CREATE DATABASE \"{}\"", db_name), &[])?;
        Ok(db_name)
    }

    /// Get connection string for a specific database
    pub fn database_connection_string(&self, db_name: &str) -> String {
        format!(
            "postgres://postgres:postgres@{}:{}/{}",
            self.host, self.port, db_name
        )
    }

    /// Connect to a specific database
    pub fn connect_to_database(&self, db_name: &str) -> Result<Client, postgres::Error> {
        let conn_string = self.database_connection_string(db_name);
        Client::connect(&conn_string, NoTls)
    }
}

/// Get the shared test container instance
pub fn get_test_container() -> &'static PostgresTestContainer {
    INIT.call_once(|| {
        let container = PostgresTestContainer::new();
        let mut guard = CONTAINER.lock().unwrap();
        *guard = Some(container);
    });

    // Safety: After INIT.call_once(), we know the container is initialized
    unsafe {
        let guard = CONTAINER.lock().unwrap();
        std::mem::transmute(guard.as_ref().unwrap())
    }
}

/// Execute SQL statements from a string
pub fn execute_sql(client: &mut Client, sql: &str) -> Result<(), postgres::Error> {
    // For complex SQL with dollar-quoted strings, execute as one block
    let trimmed = sql.trim();
    if !trimmed.is_empty() {
        client.batch_execute(trimmed)?;
    }
    Ok(())
}

/// Load and execute all schema files in order
pub fn setup_test_schema(client: &mut Client) -> Result<(), postgres::Error> {
    // Execute init script - create extension if not exists
    let init_sql =
        std::fs::read_to_string("schema/00_init.sql").expect("Failed to read init SQL file");
    execute_sql(client, &init_sql)?;

    // Create the role enum type manually to avoid conflicts
    let _ = execute_sql(client, "CREATE TYPE role AS ENUM ('admin', 'user')");

    // Execute table scripts - account.sql has table definitions
    let account_sql = std::fs::read_to_string("schema/01_tables/01_account.sql")
        .expect("Failed to read account SQL file");
    // Skip the first line (CREATE TYPE) as we already handled it
    let lines: Vec<&str> = account_sql.lines().collect();
    let table_sql = lines[1..].join("\n");
    execute_sql(client, &table_sql)?;

    // Create a proper post table for testing
    let post_table_sql = indoc! {"
        CREATE TABLE post (
            post_id SERIAL PRIMARY KEY,
            account_id INT NOT NULL REFERENCES account(account_id),
            title TEXT NOT NULL,
            content TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT post_title_length CHECK (length(title) > 0)
        );

        CREATE VIEW post_with_author AS (
            SELECT post.*, row(account.*)::account as author 
            FROM post
            LEFT JOIN account USING (account_id)
        );

        COMMENT ON COLUMN post_with_author.author IS '@pgrpc_not_null';
    "};
    execute_sql(client, post_table_sql)?;

    // Execute API scripts
    let api_sql =
        std::fs::read_to_string("schema/02_api/01_api.sql").expect("Failed to read API SQL file");
    execute_sql(client, &api_sql)?;

    Ok(())
}

/// Clean up the database between tests
pub fn cleanup_database(client: &mut Client) -> Result<(), postgres::Error> {
    // Drop all objects more thoroughly to avoid conflicts
    // Execute each drop separately to continue on errors
    let drop_commands = vec![
        // Drop views first
        "DROP VIEW IF EXISTS account_view CASCADE",
        "DROP VIEW IF EXISTS post_with_author CASCADE",
        // Drop trigger test tables
        "DROP TABLE IF EXISTS test_trigger_table CASCADE",
        "DROP TABLE IF EXISTS multi_trigger_table CASCADE",
        "DROP TABLE IF EXISTS event_specific_table CASCADE",
        "DROP TABLE IF EXISTS error_code_table CASCADE",
        // Drop regular test tables
        "DROP TABLE IF EXISTS new_table CASCADE",
        "DROP TABLE IF EXISTS test_table CASCADE",
        "DROP TABLE IF EXISTS post CASCADE",
        "DROP TABLE IF EXISTS login_details CASCADE",
        "DROP TABLE IF EXISTS account CASCADE",
        // Drop schemas (this will cascade to functions)
        "DROP SCHEMA IF EXISTS trigger_api CASCADE",
        "DROP SCHEMA IF EXISTS api CASCADE",
        "DROP SCHEMA IF EXISTS test_schema CASCADE",
        // Drop custom types
        "DROP TYPE IF EXISTS role CASCADE",
    ];

    for cmd in drop_commands {
        // Ignore errors on individual drops - some objects might not exist
        let _ = execute_sql(client, cmd);
    }

    // Recreate the test schema
    setup_test_schema(client)?;

    Ok(())
}

/// Create a test database connection with isolated database
pub fn with_isolated_database<F, R>(test_fn: F) -> R
where
    F: FnOnce(&mut Client) -> R,
{
    let container = get_test_container();

    // Create a new isolated database for this test
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");

    // Connect to the isolated database
    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    // Set up the schema in the fresh database
    setup_test_schema(&mut client).expect("Failed to setup test schema");

    // Run the test
    let result = test_fn(&mut client);

    // Note: Database will be cleaned up when container shuts down
    // No explicit cleanup needed since each test gets its own database

    result
}

/// Create a test database connection with isolated database and provide container access
pub fn with_isolated_database_and_container<F, R>(test_fn: F) -> R
where
    F: FnOnce(&mut Client, &PostgresTestContainer, &str) -> R,
{
    let container = get_test_container();

    // Create a new isolated database for this test
    let db_name = container
        .create_test_database()
        .expect("Failed to create test database");

    // Connect to the isolated database
    let mut client = container
        .connect_to_database(&db_name)
        .expect("Failed to connect to test database");

    // Set up the schema in the fresh database
    setup_test_schema(&mut client).expect("Failed to setup test schema");

    // Run the test with access to client, container, and connection string
    let result = test_fn(
        &mut client,
        container,
        &container.database_connection_string(&db_name),
    );

    result
}

/// Legacy function for backwards compatibility during transition
pub fn with_clean_database<F, R>(test_fn: F) -> R
where
    F: FnOnce(&mut Client) -> R,
{
    with_isolated_database(test_fn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_functionality() {
        // Basic test that doesn't require Docker to show the test infrastructure works
        assert_eq!(2 + 2, 4);

        // Test that we can create SQL statements with indoc
        let sql = indoc! {"
            SELECT 1 as test_value
        "};
        assert!(sql.contains("SELECT 1"));
    }

    #[test]
    fn test_container_setup() {
        let container = get_test_container();
        let mut client = container.connect().expect("Should connect to database");

        // Test basic connectivity
        let rows = client
            .query("SELECT 1 as test_value", &[])
            .expect("Should execute basic query");

        assert_eq!(rows.len(), 1);
        let test_value: i32 = rows[0].get("test_value");
        assert_eq!(test_value, 1);
    }

    #[test]
    fn test_schema_setup() {
        with_isolated_database(|client| {
            // Test that our schema setup worked
            let rows = client.query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", 
                &[]
            ).expect("Should query information_schema");

            let table_names: Vec<String> = rows
                .iter()
                .map(|row| row.get::<_, String>("table_name"))
                .collect();

            // Should have our test tables
            assert!(table_names.contains(&"account".to_string()));
            assert!(table_names.contains(&"login_details".to_string()));
            assert!(table_names.contains(&"post".to_string()));
        });
    }

    #[test]
    fn test_api_schema_setup() {
        with_isolated_database(|client| {
            // Test that API functions exist
            let rows = client.query(
                "SELECT routine_name FROM information_schema.routines WHERE routine_schema = 'api'",
                &[]
            ).expect("Should query routines");

            let function_names: Vec<String> = rows
                .iter()
                .map(|row| row.get::<_, String>("routine_name"))
                .collect();

            assert!(function_names.contains(&"get_account_by_email".to_string()));
        });
    }
}
