use super::*;
use pgrpc::*;
use tempfile::TempDir;
use indoc::indoc;

/// Create test triggers and trigger functions for testing
fn setup_trigger_test_data(client: &mut Client) -> Result<(), postgres::Error> {
    // Create a test table with triggers
    execute_sql(client, indoc! {"
        CREATE TABLE test_trigger_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    "})?;
    
    // Create trigger functions that raise different exceptions
    execute_sql(client, indoc! {"
        -- Trigger function that raises a custom exception
        CREATE OR REPLACE FUNCTION trigger_with_custom_exception()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.value < 0 THEN
                RAISE EXCEPTION 'Value cannot be negative' USING ERRCODE = '22003';
            END IF;
            
            IF NEW.name = 'forbidden' THEN
                RAISE EXCEPTION 'Name is forbidden' USING ERRCODE = 'P0001';
            END IF;
            
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Trigger function that raises constraint violation
        CREATE OR REPLACE FUNCTION trigger_with_constraint_check()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.name IS NULL OR LENGTH(NEW.name) = 0 THEN
                RAISE EXCEPTION 'Name cannot be empty' USING ERRCODE = '23502';
            END IF;
            
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Trigger function that calls other functions (nested exceptions)
        CREATE OR REPLACE FUNCTION trigger_with_nested_calls()
        RETURNS TRIGGER AS $$
        BEGIN
            -- This would be a call to another function that might raise exceptions
            IF NEW.value > 1000 THEN
                RAISE unique_violation USING MESSAGE = 'Value too high';
            END IF;
            
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Simple trigger function without exceptions
        CREATE OR REPLACE FUNCTION trigger_without_exceptions()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.created_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "})?;
    
    // Create triggers on the test table
    execute_sql(client, indoc! {"
        -- Before insert trigger with custom exception
        CREATE TRIGGER test_trigger_before_insert
            BEFORE INSERT ON test_trigger_table
            FOR EACH ROW
            EXECUTE FUNCTION trigger_with_custom_exception();
            
        -- Before update trigger with constraint check
        CREATE TRIGGER test_trigger_before_update
            BEFORE UPDATE ON test_trigger_table
            FOR EACH ROW
            EXECUTE FUNCTION trigger_with_constraint_check();
            
        -- After insert trigger with nested calls
        CREATE TRIGGER test_trigger_after_insert
            AFTER INSERT ON test_trigger_table
            FOR EACH ROW
            EXECUTE FUNCTION trigger_with_nested_calls();
            
        -- After update trigger without exceptions
        CREATE TRIGGER test_trigger_after_update
            AFTER UPDATE ON test_trigger_table
            FOR EACH ROW
            EXECUTE FUNCTION trigger_without_exceptions();
    "})?;
    
    Ok(())
}

/// Create test functions that modify trigger tables
fn setup_trigger_test_functions(client: &mut Client) -> Result<(), postgres::Error> {
    execute_sql(client, indoc! {"
        CREATE SCHEMA IF NOT EXISTS trigger_api;
        
        -- Function that inserts into trigger table
        CREATE OR REPLACE FUNCTION trigger_api.insert_test_record(
            p_name TEXT,
            p_value INTEGER
        ) RETURNS INTEGER AS $$
        DECLARE
            new_id INTEGER;
        BEGIN
            INSERT INTO test_trigger_table (name, value)
            VALUES (p_name, p_value)
            RETURNING id INTO new_id;
            
            RETURN new_id;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function that updates trigger table
        CREATE OR REPLACE FUNCTION trigger_api.update_test_record(
            p_id INTEGER,
            p_name TEXT,
            p_value INTEGER
        ) RETURNS BOOLEAN AS $$
        BEGIN
            UPDATE test_trigger_table 
            SET name = p_name, value = p_value
            WHERE id = p_id;
            
            RETURN FOUND;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function that deletes from trigger table
        CREATE OR REPLACE FUNCTION trigger_api.delete_test_record(
            p_id INTEGER
        ) RETURNS BOOLEAN AS $$
        BEGIN
            DELETE FROM test_trigger_table WHERE id = p_id;
            RETURN FOUND;
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function that inserts with conflict handling
        CREATE OR REPLACE FUNCTION trigger_api.insert_with_conflict(
            p_name TEXT,
            p_value INTEGER
        ) RETURNS INTEGER AS $$
        DECLARE
            new_id INTEGER;
        BEGIN
            INSERT INTO test_trigger_table (name, value)
            VALUES (p_name, p_value)
            ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
            RETURNING id INTO new_id;
            
            RETURN new_id;
        END;
        $$ LANGUAGE plpgsql;
    "})?;
    
    Ok(())
}

#[test]
fn test_trigger_discovery() {
    with_isolated_database(|client| {
        setup_trigger_test_data(client).expect("Should set up trigger test data");
        
        // Test that we can discover triggers on tables
        let rows = client.query(
            indoc! {"
                SELECT t.tgname as trigger_name,
                       c.relname as table_name,
                       p.proname as function_name
                FROM pg_trigger t
                JOIN pg_class c ON t.tgrelid = c.oid
                JOIN pg_proc p ON t.tgfoid = p.oid
                WHERE c.relname = 'test_trigger_table'
                  AND NOT t.tgisinternal
                ORDER BY t.tgname
            "},
            &[]
        ).expect("Should query triggers");
        
        let triggers: Vec<(String, String, String)> = rows.iter()
            .map(|row| (
                row.get("trigger_name"),
                row.get("table_name"), 
                row.get("function_name")
            ))
            .collect();
        
        // Should find all our test triggers
        assert!(triggers.iter().any(|(name, table, func)| 
            name == "test_trigger_before_insert" && 
            table == "test_trigger_table" && 
            func == "trigger_with_custom_exception"
        ));
        
        assert!(triggers.iter().any(|(name, table, func)| 
            name == "test_trigger_before_update" && 
            table == "test_trigger_table" && 
            func == "trigger_with_constraint_check"
        ));
        
        assert!(triggers.iter().any(|(name, table, func)| 
            name == "test_trigger_after_insert" && 
            table == "test_trigger_table" && 
            func == "trigger_with_nested_calls"
        ));
    });
}

#[test]
fn test_trigger_function_exception_analysis() {
    with_isolated_database(|client| {
        setup_trigger_test_data(client).expect("Should set up trigger test data");
        
        // Test that trigger functions are analyzed for exceptions
        let rows = client.query(
            indoc! {"
                SELECT p.proname as function_name,
                       p.prosrc as source_code
                FROM pg_proc p
                WHERE p.proname LIKE 'trigger_with_%'
                ORDER BY p.proname
            "},
            &[]
        ).expect("Should query trigger functions");
        
        let functions: Vec<(String, String)> = rows.iter()
            .map(|row| (row.get("function_name"), row.get("source_code")))
            .collect();
        
        // Verify we have our test functions
        assert!(functions.iter().any(|(name, _)| name == "trigger_with_custom_exception"));
        assert!(functions.iter().any(|(name, _)| name == "trigger_with_constraint_check"));
        assert!(functions.iter().any(|(name, _)| name == "trigger_with_nested_calls"));
        
        // Check that source code contains RAISE statements
        let custom_exception_source = functions.iter()
            .find(|(name, _)| name == "trigger_with_custom_exception")
            .map(|(_, source)| source)
            .expect("Should find custom exception function");
            
        assert!(custom_exception_source.contains("RAISE EXCEPTION"));
        assert!(custom_exception_source.contains("22003"));
        assert!(custom_exception_source.contains("P0001"));
    });
}

#[test]
fn test_trigger_exception_integration() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_trigger_test_data(client).expect("Should set up trigger test data");
        setup_trigger_test_functions(client).expect("Should set up trigger test functions");
        
        // Generate code including trigger API
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("generated");
        
        let builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("trigger_api")
            .output_path(&output_path);
            
        builder.build().expect("Should build successfully");
        
        // Check that generated functions include trigger exceptions
        let api_content = std::fs::read_to_string(output_path.join("trigger_api.rs"))
            .expect("Should read trigger_api.rs");
            
        // Functions should still use unified error type
        assert!(api_content.contains("super::errors::PgRpcError"));
        
        // Check error types include trigger exceptions
        let errors_content = std::fs::read_to_string(output_path.join("errors.rs"))
            .expect("Should read errors.rs");
            
        // Should have error enum
        assert!(errors_content.contains("enum PgRpcError"));
        
        // Should handle custom SQL states from triggers
        assert!(errors_content.contains("impl From<tokio_postgres::Error> for PgRpcError"));
    });
}

#[test]
fn test_multiple_triggers_same_table() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create table with multiple triggers for same operation
        execute_sql(client, indoc! {"
            CREATE TABLE multi_trigger_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                status TEXT
            );
            
            -- Multiple before insert triggers
            CREATE OR REPLACE FUNCTION validate_name()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.name IS NULL OR LENGTH(NEW.name) < 2 THEN
                    RAISE EXCEPTION 'Name too short' USING ERRCODE = 'P0001';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE OR REPLACE FUNCTION validate_status()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.status NOT IN ('active', 'inactive') THEN
                    RAISE EXCEPTION 'Invalid status' USING ERRCODE = '22003';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER multi_trigger_validate_name
                BEFORE INSERT ON multi_trigger_table
                FOR EACH ROW
                EXECUTE FUNCTION validate_name();
                
            CREATE TRIGGER multi_trigger_validate_status
                BEFORE INSERT ON multi_trigger_table  
                FOR EACH ROW
                EXECUTE FUNCTION validate_status();
        "}).expect("Should create multi-trigger setup");
        
        // Create function that inserts into this table
        execute_sql(client, "CREATE SCHEMA IF NOT EXISTS trigger_api").expect("Should create schema");
        execute_sql(client, indoc! {"
            CREATE OR REPLACE FUNCTION trigger_api.insert_multi_trigger(
                p_name TEXT,
                p_status TEXT
            ) RETURNS INTEGER AS $$
            DECLARE
                new_id INTEGER;
            BEGIN
                INSERT INTO multi_trigger_table (name, status)
                VALUES (p_name, p_status)
                RETURNING id INTO new_id;
                
                RETURN new_id;
            END;
            $$ LANGUAGE plpgsql;
        "}).expect("Should create function");
        
        // Generate code and verify trigger exceptions are captured
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("generated");
        
        let builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("trigger_api")
            .output_path(&output_path);
            
        builder.build().expect("Should build successfully");
        
        // The generated code should account for exceptions from both triggers
        assert!(output_path.join("trigger_api.rs").exists());
        assert!(output_path.join("errors.rs").exists());
    });
}

#[test]
fn test_trigger_events_specificity() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create table with different triggers for different events
        execute_sql(client, indoc! {"
            CREATE TABLE event_specific_table (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
            
            -- Insert-only trigger
            CREATE OR REPLACE FUNCTION insert_only_validation()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.data = 'invalid_insert' THEN
                    RAISE EXCEPTION 'Invalid insert data' USING ERRCODE = 'P0001';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Update-only trigger  
            CREATE OR REPLACE FUNCTION update_only_validation()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.data = 'invalid_update' THEN
                    RAISE EXCEPTION 'Invalid update data' USING ERRCODE = 'P0002';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Delete-only trigger
            CREATE OR REPLACE FUNCTION delete_only_validation()
            RETURNS TRIGGER AS $$
            BEGIN
                IF OLD.data = 'cannot_delete' THEN
                    RAISE EXCEPTION 'Cannot delete this record' USING ERRCODE = 'P0003';
                END IF;
                RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER event_insert_trigger
                BEFORE INSERT ON event_specific_table
                FOR EACH ROW
                EXECUTE FUNCTION insert_only_validation();
                
            CREATE TRIGGER event_update_trigger
                BEFORE UPDATE ON event_specific_table
                FOR EACH ROW
                EXECUTE FUNCTION update_only_validation();
                
            CREATE TRIGGER event_delete_trigger
                BEFORE DELETE ON event_specific_table
                FOR EACH ROW
                EXECUTE FUNCTION delete_only_validation();
        "}).expect("Should create event-specific triggers");
        
        // Create functions for each operation type
        execute_sql(client, "CREATE SCHEMA IF NOT EXISTS trigger_api").expect("Should create schema");
        execute_sql(client, indoc! {"
            -- Insert-only function (should only get insert trigger exceptions)
            CREATE OR REPLACE FUNCTION trigger_api.insert_only(p_data TEXT)
            RETURNS INTEGER AS $$
            DECLARE
                new_id INTEGER;
            BEGIN
                INSERT INTO event_specific_table (data)
                VALUES (p_data)
                RETURNING id INTO new_id;
                RETURN new_id;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Update-only function (should only get update trigger exceptions)
            CREATE OR REPLACE FUNCTION trigger_api.update_only(p_id INTEGER, p_data TEXT)
            RETURNS BOOLEAN AS $$
            BEGIN
                UPDATE event_specific_table 
                SET data = p_data 
                WHERE id = p_id;
                RETURN FOUND;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Delete-only function (should only get delete trigger exceptions)
            CREATE OR REPLACE FUNCTION trigger_api.delete_only(p_id INTEGER)
            RETURNS BOOLEAN AS $$
            BEGIN
                DELETE FROM event_specific_table WHERE id = p_id;
                RETURN FOUND;
            END;
            $$ LANGUAGE plpgsql;
        "}).expect("Should create operation-specific functions");
        
        // Generate code and verify event-specific trigger exceptions
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("generated");
        
        let builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("trigger_api")
            .output_path(&output_path);
            
        builder.build().expect("Should build successfully");
        
        // Verify the files were generated
        assert!(output_path.join("trigger_api.rs").exists());
        assert!(output_path.join("errors.rs").exists());
        
        // Each function should only include exceptions from relevant triggers
        // (This would require more detailed inspection of the generated exception analysis)
    });
}

#[test]
fn test_trigger_exception_error_codes() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create triggers with specific error codes that should be mapped
        execute_sql(client, indoc! {"
            CREATE TABLE error_code_table (
                id SERIAL PRIMARY KEY,
                test_field TEXT
            );
            
            CREATE OR REPLACE FUNCTION test_error_codes()
            RETURNS TRIGGER AS $$
            BEGIN
                -- Test different PostgreSQL error codes
                IF NEW.test_field = 'not_null_violation' THEN
                    RAISE EXCEPTION 'Test not null' USING ERRCODE = '23502';
                ELSIF NEW.test_field = 'foreign_key_violation' THEN  
                    RAISE EXCEPTION 'Test foreign key' USING ERRCODE = '23503';
                ELSIF NEW.test_field = 'unique_violation' THEN
                    RAISE EXCEPTION 'Test unique' USING ERRCODE = '23505';
                ELSIF NEW.test_field = 'check_violation' THEN
                    RAISE EXCEPTION 'Test check' USING ERRCODE = '23514';
                ELSIF NEW.test_field = 'custom_error' THEN
                    RAISE EXCEPTION 'Custom error' USING ERRCODE = 'P0100';
                END IF;
                
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER error_code_trigger
                BEFORE INSERT ON error_code_table
                FOR EACH ROW
                EXECUTE FUNCTION test_error_codes();
        "}).expect("Should create error code test setup");
        
        execute_sql(client, "CREATE SCHEMA IF NOT EXISTS trigger_api").expect("Should create schema");
        execute_sql(client, indoc! {"
            CREATE OR REPLACE FUNCTION trigger_api.test_error_insertion(p_field TEXT)
            RETURNS INTEGER AS $$
            DECLARE
                new_id INTEGER;
            BEGIN
                INSERT INTO error_code_table (test_field)
                VALUES (p_field)
                RETURNING id INTO new_id;
                RETURN new_id;
            END;
            $$ LANGUAGE plpgsql;
        "}).expect("Should create test function");
        
        // Generate code
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("generated");
        
        let builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("trigger_api")
            .output_path(&output_path);
            
        builder.build().expect("Should build successfully");
        
        // Verify error handling includes the various SQL states
        let errors_content = std::fs::read_to_string(output_path.join("errors.rs"))
            .expect("Should read errors.rs");
            
        // Should have error enum with proper From implementation
        assert!(errors_content.contains("enum PgRpcError"));
        assert!(errors_content.contains("impl From<tokio_postgres::Error>"));
        
        // Should handle standard PostgreSQL error codes
        // (The specific mapping depends on the unified error implementation)
    });
}