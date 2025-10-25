use super::*;
use pgrpc::*;
use tempfile::TempDir;

/// Test that demonstrates nullability discrepancy between view struct and query result struct
///
/// Setup:
/// - base_table (with NOT NULL constraints)
/// - view_two (selects from base_table)
/// - view_one (selects from view_two)
/// - query: SELECT * FROM view_one
///
/// Expected: query result struct should match view_one struct exactly
/// Actual: They have different nullability
#[test]
fn test_view_query_nullability_discrepancy() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create base table with NOT NULL constraints
        client.execute(
            "CREATE TABLE base_table (
                id SERIAL PRIMARY KEY,
                required_field TEXT NOT NULL,
                optional_field TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        ).unwrap();

        // Create view_two that selects from base_table
        client.execute(
            "CREATE VIEW view_two AS
             SELECT id, required_field, optional_field, created_at
             FROM base_table",
            &[],
        ).unwrap();

        // Create view_one that selects from view_two
        client.execute(
            "CREATE VIEW view_one AS
             SELECT id, required_field, optional_field, created_at
             FROM view_two",
            &[],
        ).unwrap();

        // Create temporary SQL file with query
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: GetFromViewOne :many
SELECT * FROM view_one;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .infer_view_nullability(true);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated public.rs (has view structs)
        let public_file = output_dir.join("public.rs");
        let public_code = std::fs::read_to_string(&public_file).expect("Should read public.rs");

        // Read generated queries.rs (has query result structs)
        let queries_file = output_dir.join("queries.rs");
        let queries_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== VIEW STRUCT (public.rs) ===");
        // Find ViewOne struct
        if let Some(start) = public_code.find("pub struct ViewOne") {
            let end = public_code[start..].find("impl").unwrap_or(500);
            println!("{}", &public_code[start..start+end]);
        }

        println!("\n=== QUERY RESULT STRUCT (queries.rs) ===");
        // Find GetFromViewOneRow struct
        if let Some(start) = queries_code.find("pub struct GetFromViewOneRow") {
            let end = queries_code[start..].find("impl").unwrap_or(500);
            println!("{}", &queries_code[start..start+end]);
        }

        // Test nullability expectations for view_one struct
        println!("\n=== TESTING VIEW STRUCT ===");
        assert!(public_code.contains("pub struct ViewOne"), "Should have ViewOne struct");
        assert!(public_code.contains("pub id: i32"), "id should be NOT NULL (i32)");
        assert!(public_code.contains("pub required_field: String"), "required_field should be NOT NULL (String)");
        assert!(public_code.contains("pub optional_field: Option<String>"), "optional_field should be NULLABLE (Option<String>)");
        assert!(public_code.contains("pub created_at: time::OffsetDateTime"), "created_at should be NOT NULL");

        // Test nullability expectations for query result struct
        println!("\n=== TESTING QUERY STRUCT ===");
        assert!(queries_code.contains("pub struct GetFromViewOneRow"), "Should have GetFromViewOneRow struct");

        // THESE SHOULD PASS BUT MIGHT FAIL - demonstrating the bug
        assert!(queries_code.contains("pub id: i32"),
            "BUG: id should be NOT NULL (i32) in query result, same as view");
        assert!(queries_code.contains("pub required_field: String"),
            "BUG: required_field should be NOT NULL (String) in query result, same as view");
        assert!(queries_code.contains("pub optional_field: Option<String>"),
            "optional_field should be NULLABLE (Option<String>) in query result, same as view");
        assert!(queries_code.contains("pub created_at: time::OffsetDateTime"),
            "BUG: created_at should be NOT NULL in query result, same as view");
    });
}

/// Test with JOINs - this is where nullability often differs
#[test]
fn test_view_query_nullability_with_joins() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create base tables
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                title TEXT NOT NULL,
                content TEXT
            )",
            &[],
        ).unwrap();

        // Create view with LEFT JOIN
        client.execute(
            "CREATE VIEW user_posts AS
             SELECT
                u.id as user_id,
                u.username,
                p.id as post_id,
                p.title as post_title
             FROM users u
             LEFT JOIN posts p ON u.id = p.user_id",
            &[],
        ).unwrap();

        // Create temporary SQL file with query
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: GetUserPosts :many
SELECT * FROM user_posts;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .infer_view_nullability(true);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated files
        let public_code = std::fs::read_to_string(output_dir.join("public.rs"))
            .expect("Should read public.rs");
        let queries_code = std::fs::read_to_string(output_dir.join("queries.rs"))
            .expect("Should read queries.rs");

        println!("\n=== VIEW STRUCT (public.rs) ===");
        if let Some(start) = public_code.find("pub struct UserPosts") {
            let end = public_code[start..].find("\nimpl").unwrap_or(600);
            println!("{}", &public_code[start..start+end]);
        }

        println!("\n=== QUERY RESULT STRUCT (queries.rs) ===");
        if let Some(start) = queries_code.find("pub struct GetUserPostsRow") {
            let end = queries_code[start..].find("\nimpl").unwrap_or(600);
            println!("{}", &queries_code[start..start+end]);
        }

        // The view should correctly identify that:
        // - user_id, username are NOT NULL (from users table in LEFT JOIN left side)
        // - post_id, post_title are NULLABLE (from posts table in LEFT JOIN right side)

        // Test view struct
        assert!(public_code.contains("pub user_id: i32"), "user_id should be NOT NULL in view");
        assert!(public_code.contains("pub username: String"), "username should be NOT NULL in view");
        assert!(public_code.contains("pub post_id: Option<i32>"), "post_id should be NULLABLE in view (LEFT JOIN)");
        assert!(public_code.contains("pub post_title: Option<String>"), "post_title should be NULLABLE in view (LEFT JOIN)");

        // Test query struct - SHOULD MATCH VIEW
        assert!(queries_code.contains("pub user_id: i32"),
            "BUG: user_id should be NOT NULL in query result, same as view");
        assert!(queries_code.contains("pub username: String"),
            "BUG: username should be NOT NULL in query result, same as view");
        assert!(queries_code.contains("pub post_id: Option<i32>"),
            "BUG: post_id should be NULLABLE in query result, same as view");
        assert!(queries_code.contains("pub post_title: Option<String>"),
            "BUG: post_title should be NULLABLE in query result, same as view");
    });
}

/// Test with aggregate functions
#[test]
fn test_view_query_nullability_with_aggregates() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create base table
        client.execute(
            "CREATE TABLE sales (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                amount DECIMAL NOT NULL,
                quantity INTEGER NOT NULL
            )",
            &[],
        ).unwrap();

        // Create view with aggregates
        client.execute(
            "CREATE VIEW product_stats AS
             SELECT
                product_id,
                COUNT(*) as sale_count,
                SUM(amount) as total_amount,
                MAX(quantity) as max_quantity
             FROM sales
             GROUP BY product_id",
            &[],
        ).unwrap();

        // Create temporary SQL file with query
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: GetProductStats :many
SELECT * FROM product_stats;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .infer_view_nullability(true);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated files
        let public_code = std::fs::read_to_string(output_dir.join("public.rs"))
            .expect("Should read public.rs");
        let queries_code = std::fs::read_to_string(output_dir.join("queries.rs"))
            .expect("Should read queries.rs");

        println!("\n=== VIEW STRUCT (public.rs) ===");
        if let Some(start) = public_code.find("pub struct ProductStats") {
            let end = public_code[start..].find("\nimpl").unwrap_or(600);
            println!("{}", &public_code[start..start+end]);
        }

        println!("\n=== QUERY RESULT STRUCT (queries.rs) ===");
        if let Some(start) = queries_code.find("pub struct GetProductStatsRow") {
            let end = queries_code[start..].find("\nimpl").unwrap_or(600);
            println!("{}", &queries_code[start..start+end]);
        }

        // Expected nullability:
        // - product_id: NOT NULL (GROUP BY column)
        // - sale_count: NOT NULL (COUNT(*) always returns a value)
        // - total_amount: NULLABLE (SUM can return NULL)
        // - max_quantity: NULLABLE (MAX can return NULL on empty groups)

        // These assertions check if the view and query structs match
        println!("\n=== CHECKING CONSISTENCY ===");

        // Extract nullability from view struct
        let view_has_product_id_option = public_code.contains("pub product_id: Option<");
        let view_has_sale_count_option = public_code.contains("pub sale_count: Option<");
        let view_has_total_amount_option = public_code.contains("pub total_amount: Option<");
        let view_has_max_quantity_option = public_code.contains("pub max_quantity: Option<");

        // Extract nullability from query struct
        let query_has_product_id_option = queries_code.contains("pub product_id: Option<");
        let query_has_sale_count_option = queries_code.contains("pub sale_count: Option<");
        let query_has_total_amount_option = queries_code.contains("pub total_amount: Option<");
        let query_has_max_quantity_option = queries_code.contains("pub max_quantity: Option<");

        println!("product_id: view={:?}, query={:?}", view_has_product_id_option, query_has_product_id_option);
        println!("sale_count: view={:?}, query={:?}", view_has_sale_count_option, query_has_sale_count_option);
        println!("total_amount: view={:?}, query={:?}", view_has_total_amount_option, query_has_total_amount_option);
        println!("max_quantity: view={:?}, query={:?}", view_has_max_quantity_option, query_has_max_quantity_option);

        // The key assertion: view and query should match
        assert_eq!(view_has_product_id_option, query_has_product_id_option,
            "product_id nullability should match between view and query");
        assert_eq!(view_has_sale_count_option, query_has_sale_count_option,
            "sale_count nullability should match between view and query");
        assert_eq!(view_has_total_amount_option, query_has_total_amount_option,
            "total_amount nullability should match between view and query");
        assert_eq!(view_has_max_quantity_option, query_has_max_quantity_option,
            "max_quantity nullability should match between view and query");
    });
}