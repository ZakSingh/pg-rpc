use super::*;
use tempfile::TempDir;

#[test]
fn test_sql_language_function_parsing() {
    with_isolated_database_and_container(|client, _container, _conn_string| {
        // Create a test schema with both SQL and PL/pgSQL functions
        execute_sql(client, "CREATE SCHEMA test_sql").expect("Should create schema");

        // Create a test table
        execute_sql(
            client,
            r#"
            CREATE TABLE test_sql.users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL CHECK (name != '')
            )
        "#,
        )
        .expect("Should create table");

        // Create a SQL language function
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_sql.get_user_by_email(p_email TEXT)
            RETURNS TABLE(id INTEGER, name TEXT)
            LANGUAGE SQL AS $$
                SELECT id, name 
                FROM test_sql.users 
                WHERE email = p_email;
            $$;
        "#,
        )
        .expect("Should create SQL function");

        // Create another SQL function with UPDATE
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_sql.update_user_name(p_id INTEGER, p_name TEXT)
            RETURNS BOOLEAN
            LANGUAGE SQL AS $$
                UPDATE test_sql.users 
                SET name = p_name 
                WHERE id = p_id
                RETURNING true;
            $$;
        "#,
        )
        .expect("Should create SQL UPDATE function");

        // Create a PL/pgSQL function for comparison
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_sql.insert_user(p_email TEXT, p_name TEXT)
            RETURNS INTEGER
            LANGUAGE plpgsql AS $$
            DECLARE
                new_id INTEGER;
            BEGIN
                INSERT INTO test_sql.users (email, name)
                VALUES (p_email, p_name)
                RETURNING id INTO new_id;
                
                RETURN new_id;
            END;
            $$;
        "#,
        )
        .expect("Should create PL/pgSQL function");

        // Now generate code using pgrpc to verify SQL functions are processed
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        // Build and generate code
        pgrpc::PgrpcBuilder::new()
            .connection_string(_conn_string)
            .schema("test_sql")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let generated = std::fs::read_to_string(output_path.join("test_sql.rs"))
            .expect("Should read generated file");

        println!("Generated code:\n{}", generated);

        // Check that all 3 functions were generated
        assert!(
            generated.contains("get_user_by_email"),
            "Should contain SQL SELECT function"
        );
        assert!(
            generated.contains("update_user_name"),
            "Should contain SQL UPDATE function"
        );
        assert!(
            generated.contains("insert_user"),
            "Should contain PL/pgSQL function"
        );

        // Count how many functions have error enums (indicates exception handling)
        let error_enum_count = generated.matches("pub enum").count();
        println!("\nFound {} error enums in generated code", error_enum_count);

        // SQL functions should now have error handling if they touch tables with constraints
        assert!(
            error_enum_count >= 1,
            "Should have at least one error enum for functions touching constrained tables"
        );

        println!("\nSQL function parsing test completed successfully!");
    });
}

#[test]
fn test_sql_function_with_triggers() {
    with_isolated_database_and_container(|client, _container, _conn_string| {
        // Create schema and table
        execute_sql(client, "CREATE SCHEMA test_triggers").expect("Should create schema");
        execute_sql(
            client,
            r#"
            CREATE TABLE test_triggers.audit_log (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                action TEXT NOT NULL,
                user_id INTEGER,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        "#,
        )
        .expect("Should create audit table");

        execute_sql(
            client,
            r#"
            CREATE TABLE test_triggers.users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL
            )
        "#,
        )
        .expect("Should create users table");

        // Create a trigger function
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_triggers.audit_changes()
            RETURNS TRIGGER
            LANGUAGE plpgsql AS $$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    RAISE EXCEPTION 'Deletion not allowed';
                END IF;
                
                INSERT INTO test_triggers.audit_log (table_name, action, user_id)
                VALUES (TG_TABLE_NAME, TG_OP, NEW.id);
                
                RETURN NEW;
            END;
            $$;
        "#,
        )
        .expect("Should create trigger function");

        // Create trigger
        execute_sql(
            client,
            r#"
            CREATE TRIGGER users_audit
            BEFORE INSERT OR UPDATE OR DELETE ON test_triggers.users
            FOR EACH ROW
            EXECUTE FUNCTION test_triggers.audit_changes();
        "#,
        )
        .expect("Should create trigger");

        // Create a SQL function that modifies the table with trigger
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_triggers.delete_user(p_id INTEGER)
            RETURNS BOOLEAN
            LANGUAGE SQL AS $$
                DELETE FROM test_triggers.users WHERE id = p_id RETURNING true;
            $$;
        "#,
        )
        .expect("Should create SQL delete function");

        // Generate code to verify SQL functions get trigger exceptions
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(_conn_string)
            .schema("test_triggers")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let generated = std::fs::read_to_string(output_path.join("test_triggers.rs"))
            .expect("Should read generated file");

        println!(
            "Generated code for SQL function with triggers:\n{}",
            generated
        );

        // Check that the SQL delete function was generated
        assert!(
            generated.contains("delete_user"),
            "Should contain SQL delete function"
        );

        // The delete function should have error handling due to the trigger that raises exceptions
        assert!(
            generated.contains("DeleteUserError") || generated.contains("delete_user_error"),
            "SQL function with triggers should have error handling"
        );

        println!("SQL function with triggers test completed successfully!");
    });
}

#[test]
fn test_sql_function_constraint_exceptions() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a test schema
        execute_sql(client, "CREATE SCHEMA test_constraints").expect("Should create schema");

        // Create tables with various constraints
        execute_sql(
            client,
            r#"
            CREATE TABLE test_constraints.users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL CHECK (length(name) > 0),
                age INTEGER CHECK (age >= 18)
            )
        "#,
        )
        .expect("Should create users table");

        execute_sql(
            client,
            r#"
            CREATE TABLE test_constraints.posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES test_constraints.users(id),
                title TEXT NOT NULL,
                content TEXT
            )
        "#,
        )
        .expect("Should create posts table");

        // Create SQL functions that could violate constraints

        // Function that could violate unique constraint
        execute_sql(client, r#"
            CREATE OR REPLACE FUNCTION test_constraints.create_user(p_email TEXT, p_name TEXT, p_age INTEGER)
            RETURNS INTEGER
            LANGUAGE SQL AS $$
                INSERT INTO test_constraints.users (email, name, age)
                VALUES (p_email, p_name, p_age)
                RETURNING id;
            $$;
        "#).expect("Should create insert function");

        // Function that could violate check constraint
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_constraints.update_user_age(p_id INTEGER, p_age INTEGER)
            RETURNS BOOLEAN
            LANGUAGE SQL AS $$
                UPDATE test_constraints.users 
                SET age = p_age
                WHERE id = p_id
                RETURNING true;
            $$;
        "#,
        )
        .expect("Should create update function");

        // Function that could violate foreign key constraint
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION test_constraints.delete_user(p_id INTEGER)
            RETURNS BOOLEAN
            LANGUAGE SQL AS $$
                DELETE FROM test_constraints.users 
                WHERE id = p_id
                RETURNING true;
            $$;
        "#,
        )
        .expect("Should create delete function");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("test_constraints")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let generated = std::fs::read_to_string(output_path.join("test_constraints.rs"))
            .expect("Should read generated file");

        println!(
            "Generated code for SQL functions with constraints:\n{}",
            generated
        );

        // Verify that error enums are generated for SQL functions
        assert!(
            generated.contains("CreateUserError"),
            "Should have error enum for create_user"
        );
        assert!(
            generated.contains("UpdateUserAgeError"),
            "Should have error enum for update_user_age"
        );
        assert!(
            generated.contains("DeleteUserError"),
            "Should have error enum for delete_user"
        );

        // Verify that constraint variants are included
        assert!(
            generated.contains("UsersConstraint"),
            "Should have users constraint enum"
        );

        // Read the errors module to check constraint handling
        let errors_content =
            std::fs::read_to_string(output_path.join("errors.rs")).expect("Should read errors.rs");

        // Verify constraint enums are generated
        assert!(
            errors_content.contains("UsersEmailKey"),
            "Should have unique constraint variant"
        );
        assert!(
            errors_content.contains("UsersNameCheck"),
            "Should have name check constraint"
        );
        assert!(
            errors_content.contains("UsersAgeCheck"),
            "Should have age check constraint"
        );

        println!("SQL function constraint exception test completed successfully!");
    });
}

#[test]
fn test_sql_function_nullability_inference() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test schema with tables
        execute_sql(client, "CREATE SCHEMA nullability_test").expect("Should create schema");

        // Create tables with NOT NULL constraints
        execute_sql(
            client,
            r#"
            CREATE TABLE nullability_test.users (
                id SERIAL PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT,
                age INTEGER NOT NULL
            )
        "#,
        )
        .expect("Should create users table");

        execute_sql(
            client,
            r#"
            CREATE TABLE nullability_test.posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT
            )
        "#,
        )
        .expect("Should create posts table");

        // SQL function returning columns with NOT NULL constraints
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION nullability_test.get_user_info(p_user_id INTEGER)
            RETURNS TABLE(user_id INTEGER, email TEXT, age INTEGER)
            LANGUAGE SQL AS $$
                SELECT id, email, age FROM nullability_test.users WHERE id = p_user_id;
            $$;
        "#,
        )
        .expect("Should create get_user_info function");

        // SQL function with LEFT JOIN (should make joined columns nullable)
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION nullability_test.get_user_with_posts(p_user_id INTEGER)
            RETURNS TABLE(user_email TEXT, post_title TEXT)
            LANGUAGE SQL AS $$
                SELECT u.email, p.title
                FROM nullability_test.users u
                LEFT JOIN nullability_test.posts p ON u.id = p.user_id
                WHERE u.id = p_user_id;
            $$;
        "#,
        )
        .expect("Should create get_user_with_posts function");

        // SQL function with INNER JOIN (columns retain NOT NULL status)
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION nullability_test.get_user_posts_inner(p_user_id INTEGER)
            RETURNS TABLE(user_email TEXT, post_title TEXT)
            LANGUAGE SQL AS $$
                SELECT u.email, p.title
                FROM nullability_test.users u
                INNER JOIN nullability_test.posts p ON u.id = p.user_id
                WHERE u.id = p_user_id;
            $$;
        "#,
        )
        .expect("Should create get_user_posts_inner function");

        // SQL function with nullable column
        execute_sql(
            client,
            r#"
            CREATE OR REPLACE FUNCTION nullability_test.get_user_name(p_user_id INTEGER)
            RETURNS TABLE(user_name TEXT)
            LANGUAGE SQL AS $$
                SELECT name FROM nullability_test.users WHERE id = p_user_id;
            $$;
        "#,
        )
        .expect("Should create get_user_name function");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("nullability_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("nullability_test.rs"))
            .expect("Should read generated file");

        println!("Generated code for nullability inference:\n{}", content);

        // Verify get_user_info struct has correct nullability
        // user_id and email should be NOT NULL (i32/String), age should be NOT NULL (i32)
        assert!(
            content.contains("pub struct GetUserInfoRow"),
            "Should generate GetUserInfoRow struct"
        );

        // Check that the struct fields have correct types
        // user_id should be i32 (not Option<i32>)
        // email should be String (not Option<String>)
        // age should be i32 (not Option<i32>)

        // Find the struct definition
        if let Some(struct_start) = content.find("pub struct GetUserInfoRow") {
            let struct_end = content[struct_start..].find('}').unwrap() + struct_start;
            let struct_def = &content[struct_start..struct_end];

            println!("GetUserInfoRow struct:\n{}", struct_def);

            // user_id comes from id column which is PRIMARY KEY (NOT NULL)
            assert!(
                struct_def.contains("pub user_id: i32") || struct_def.contains("pub user_id : i32"),
                "user_id should be i32 (NOT NULL)"
            );

            // email has NOT NULL constraint
            assert!(
                struct_def.contains("pub email: String") || struct_def.contains("pub email : String"),
                "email should be String (NOT NULL)"
            );

            // age has NOT NULL constraint
            assert!(
                struct_def.contains("pub age: i32") || struct_def.contains("pub age : i32"),
                "age should be i32 (NOT NULL)"
            );
        }

        // Verify get_user_with_posts struct (LEFT JOIN should make post_title nullable)
        if let Some(struct_start) = content.find("pub struct GetUserWithPostsRow") {
            let struct_end = content[struct_start..].find('}').unwrap() + struct_start;
            let struct_def = &content[struct_start..struct_end];

            println!("GetUserWithPostsRow struct:\n{}", struct_def);

            // user_email should be NOT NULL (from users table with NOT NULL constraint)
            assert!(
                struct_def.contains("pub user_email: String") || struct_def.contains("pub user_email : String"),
                "user_email should be String (NOT NULL)"
            );

            // post_title should be nullable due to LEFT JOIN
            assert!(
                struct_def.contains("pub post_title: Option<String>") || struct_def.contains("pub post_title : Option < String >"),
                "post_title should be Option<String> (nullable due to LEFT JOIN)"
            );
        }

        // Verify get_user_posts_inner struct (INNER JOIN preserves NOT NULL)
        if let Some(struct_start) = content.find("pub struct GetUserPostsInnerRow") {
            let struct_end = content[struct_start..].find('}').unwrap() + struct_start;
            let struct_def = &content[struct_start..struct_end];

            println!("GetUserPostsInnerRow struct:\n{}", struct_def);

            // Both columns should be NOT NULL (INNER JOIN + NOT NULL constraints)
            assert!(
                struct_def.contains("pub user_email: String") || struct_def.contains("pub user_email : String"),
                "user_email should be String (NOT NULL)"
            );

            assert!(
                struct_def.contains("pub post_title: String") || struct_def.contains("pub post_title : String"),
                "post_title should be String (NOT NULL from INNER JOIN)"
            );
        }

        // Verify get_user_name struct (nullable column)
        if let Some(struct_start) = content.find("pub struct GetUserNameRow") {
            let struct_end = content[struct_start..].find('}').unwrap() + struct_start;
            let struct_def = &content[struct_start..struct_end];

            println!("GetUserNameRow struct:\n{}", struct_def);

            // name column is nullable in the table
            assert!(
                struct_def.contains("pub user_name: Option<String>") || struct_def.contains("pub user_name : Option < String >"),
                "user_name should be Option<String> (nullable column)"
            );
        }

        println!("SQL function nullability inference test completed successfully!");
    });
}
