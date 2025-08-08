use super::*;
use tempfile::TempDir;

/// Tests @pgrpc_not_null annotations for INOUT parameters in procedures
#[test]
fn test_procedure_out_param_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test schema with procedures having INOUT parameters
        execute_sql(
            client,
            r#"
            CREATE SCHEMA out_param_test;
            SET search_path TO out_param_test;
            
            -- Create a simple users table for the test
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            );
            
            -- Procedure with single INOUT parameter (should be nullable by default)
            CREATE OR REPLACE PROCEDURE get_user_email(
                IN p_user_id INTEGER,
                INOUT email TEXT DEFAULT NULL
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                SELECT u.email INTO email FROM users u WHERE u.id = p_user_id;
            END;
            $$;
            
            -- Procedure with single INOUT parameter marked as not null
            CREATE OR REPLACE PROCEDURE get_user_name(
                IN p_user_id INTEGER,
                INOUT name TEXT DEFAULT NULL
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                SELECT u.name INTO name FROM users u WHERE u.id = p_user_id;
            END;
            $$;
            
            COMMENT ON PROCEDURE get_user_name IS 'Gets user name @pgrpc_not_null(name)';
            
            -- Procedure with multiple INOUT parameters, some nullable
            CREATE OR REPLACE PROCEDURE get_user_details(
                IN p_user_id INTEGER,
                INOUT user_name TEXT DEFAULT NULL,
                INOUT email TEXT DEFAULT NULL,
                INOUT phone TEXT DEFAULT NULL,
                INOUT last_login TIMESTAMP DEFAULT NULL
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                -- Implementation
            END;
            $$;
            
            COMMENT ON PROCEDURE get_user_details IS 
            'Gets user details @pgrpc_not_null(user_name) @pgrpc_not_null(email)';
        "#,
        )
        .expect("Should create test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("out_param_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("out_param_test.rs"))
            .expect("Should read generated file");
            
        // Debug: Print the generated function for get_user_name
        if content.contains("get_user_name") {
            let start = content.find("get_user_name").unwrap();
            let snippet = &content[start.saturating_sub(100)..std::cmp::min(start + 300, content.len())];
            println!("Generated code around get_user_name:\n{}", snippet);
        }
        
        // Also look for get_user_email
        if content.contains("get_user_email") {
            let start = content.find("get_user_email").unwrap();
            let snippet = &content[start.saturating_sub(100)..std::cmp::min(start + 300, content.len())];
            println!("Generated code around get_user_email:\n{}", snippet);
        }

        // Test single INOUT parameter - default nullable
        assert!(
            content.contains("pub async fn get_user_email") && 
            content.contains("-> Result<Option<String>,"),
            "Single INOUT parameter should be wrapped in Option by default"
        );

        // Test single INOUT parameter - marked as not null
        // Find the get_user_name function and check its return type specifically
        let get_user_name_start = content.find("pub async fn get_user_name").expect("Should find get_user_name");
        let get_user_name_end = get_user_name_start + content[get_user_name_start..].find("\n}").expect("Should find function end") + 2;
        let get_user_name_fn = &content[get_user_name_start..get_user_name_end];
        
        assert!(
            get_user_name_fn.contains("-> Result<String, GetUserNameError>") &&
            !get_user_name_fn.contains("-> Result<Option<String>,"),
            "Single INOUT parameter marked with @pgrpc_not_null should not be wrapped in Option"
        );

        // Test multiple INOUT parameters struct
        assert!(
            content.contains("pub struct GetUserDetailsOut"),
            "Should generate struct for multiple INOUT parameters"
        );

        // Check field types in the struct
        assert!(
            content.contains("pub user_name: String") &&
            !content.contains("pub user_name: Option<String>"),
            "user_name should not be wrapped in Option due to @pgrpc_not_null"
        );
        
        assert!(
            content.contains("pub email: String") &&
            !content.contains("pub email: Option<String>"),
            "email should not be wrapped in Option due to @pgrpc_not_null"
        );
        
        assert!(
            content.contains("pub phone: Option<String>"),
            "phone should be wrapped in Option (nullable by default)"
        );
        
        assert!(
            content.contains("pub last_login: Option<time::OffsetDateTime>"),
            "last_login should be wrapped in Option (nullable by default)"
        );

        println!("✅ Procedure INOUT parameter nullability correctly implemented!");
    });
}

/// Tests @pgrpc_not_null annotations for RETURNS TABLE functions
#[test]
fn test_returns_table_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test schema with RETURNS TABLE functions
        execute_sql(
            client,
            r#"
            CREATE SCHEMA returns_table_test;
            SET search_path TO returns_table_test;
            
            -- Create a sample table
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                price NUMERIC,
                in_stock BOOLEAN NOT NULL,
                category TEXT
            );
            
            -- RETURNS TABLE function with mixed nullability
            CREATE OR REPLACE FUNCTION search_products(p_query TEXT)
            RETURNS TABLE (
                product_id INTEGER,
                product_name TEXT,
                description TEXT,
                price NUMERIC,
                in_stock BOOLEAN,
                category TEXT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                RETURN QUERY
                SELECT id, name, description, price, in_stock, category
                FROM products
                WHERE name ILIKE '%' || p_query || '%';
            END;
            $$;
            
            COMMENT ON FUNCTION search_products IS 
            'Search products by name @pgrpc_not_null(product_id) @pgrpc_not_null(product_name) @pgrpc_not_null(in_stock)';
            
            -- RETURNS TABLE function without annotations (all nullable by default)
            CREATE OR REPLACE FUNCTION get_product_stats()
            RETURNS TABLE (
                category TEXT,
                total_products INTEGER,
                avg_price NUMERIC
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                RETURN QUERY
                SELECT category, COUNT(*)::INTEGER, AVG(price)
                FROM products
                GROUP BY category;
            END;
            $$;
        "#,
        )
        .expect("Should create test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("returns_table_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("returns_table_test.rs"))
            .expect("Should read generated file");

        // Test RETURNS TABLE with annotations
        assert!(
            content.contains("pub struct SearchProductsRow"),
            "Should generate struct for RETURNS TABLE function"
        );

        // Check field types with annotations
        assert!(
            content.contains("pub product_id: i32") &&
            !content.contains("pub product_id: Option<i32>"),
            "product_id should not be wrapped in Option due to @pgrpc_not_null"
        );
        
        assert!(
            content.contains("pub product_name: String") &&
            !content.contains("pub product_name: Option<String>"),
            "product_name should not be wrapped in Option due to @pgrpc_not_null"
        );
        
        assert!(
            content.contains("pub description: Option<String>"),
            "description should be wrapped in Option (nullable by default)"
        );
        
        assert!(
            content.contains("pub price: Option<rust_decimal::Decimal>"),
            "price should be wrapped in Option (nullable by default)"
        );
        
        assert!(
            content.contains("pub in_stock: bool") &&
            !content.contains("pub in_stock: Option<bool>"),
            "in_stock should not be wrapped in Option due to @pgrpc_not_null"
        );

        // Test RETURNS TABLE without annotations (all nullable)
        assert!(
            content.contains("pub struct GetProductStatsRow"),
            "Should generate struct for RETURNS TABLE function without annotations"
        );

        assert!(
            content.contains("pub category: Option<String>"),
            "category should be wrapped in Option (nullable by default)"
        );
        
        assert!(
            content.contains("pub total_products: Option<i32>"),
            "total_products should be wrapped in Option (nullable by default)"
        );
        
        assert!(
            content.contains("pub avg_price: Option<rust_decimal::Decimal>"),
            "avg_price should be wrapped in Option (nullable by default)"
        );

        println!("✅ RETURNS TABLE nullability correctly implemented!");
    });
}

/// Test error cases and edge cases
#[test]
fn test_out_param_annotation_edge_cases() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            CREATE SCHEMA edge_case_test;
            SET search_path TO edge_case_test;
            
            -- Test with multiple annotations on same line
            CREATE OR REPLACE PROCEDURE test_multiple_annotations(
                INOUT param1 TEXT DEFAULT NULL,
                INOUT param2 INTEGER DEFAULT NULL,
                INOUT param3 BOOLEAN DEFAULT NULL
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                -- Implementation
            END;
            $$;
            
            COMMENT ON PROCEDURE test_multiple_annotations IS 
            'Test procedure @pgrpc_not_null(param1) @pgrpc_not_null(param2) @pgrpc_not_null(param3)';
            
            -- Test with annotation for non-existent parameter (should be ignored)
            CREATE OR REPLACE PROCEDURE test_invalid_annotation(
                INOUT real_param TEXT DEFAULT NULL
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                -- Implementation
            END;
            $$;
            
            COMMENT ON PROCEDURE test_invalid_annotation IS 
            'Test procedure @pgrpc_not_null(real_param) @pgrpc_not_null(fake_param)';
        "#,
        )
        .expect("Should create test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("edge_case_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("edge_case_test.rs"))
            .expect("Should read generated file");

        // Test multiple annotations
        assert!(
            content.contains("pub param1: String") &&
            content.contains("pub param2: i32") &&
            content.contains("pub param3: bool"),
            "All three parameters should be non-nullable"
        );

        // Test invalid annotation is ignored
        assert!(
            content.contains("-> Result<String,"),
            "real_param should be non-nullable despite invalid annotation for fake_param"
        );

        println!("✅ Edge cases handled correctly!");
    });
}