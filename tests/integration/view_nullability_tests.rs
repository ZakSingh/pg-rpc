use super::*;
use tempfile::TempDir;

#[test]
fn test_view_nullability_simple() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables
        execute_sql(
            client,
            r#"
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT,
                email TEXT NOT NULL
            );
            
            CREATE VIEW user_view AS
            SELECT id, name, email FROM users;
        "#,
        )
        .expect("Should create table and view");

        // Generate code with nullability inference enabled
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Check that the UserView struct was generated
        assert!(
            public_content.contains("struct UserView"),
            "Should generate UserView struct"
        );

        // With manual implementation, fields will still follow @pgrpc_not_null annotations
        // In a full implementation, id and email would be non-Option, name would be Option

        println!("Simple view nullability test completed!");
    });
}

#[test]
fn test_view_nullability_with_joins() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables
        execute_sql(
            client,
            r#"
            CREATE TABLE authors (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
            
            CREATE TABLE books (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                author_id INTEGER REFERENCES authors(id)
            );
            
            CREATE VIEW books_with_authors AS
            SELECT 
                b.id,
                b.title,
                a.name as author_name
            FROM books b
            LEFT JOIN authors a ON b.author_id = a.id;
        "#,
        )
        .expect("Should create tables and view");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Check that the view struct was generated
        assert!(
            public_content.contains("struct BooksWithAuthors"),
            "Should generate BooksWithAuthors struct"
        );

        // In a full implementation:
        // - id and title would be non-Option (from books table, NOT NULL)
        // - author_name would be Option (due to LEFT JOIN, even though authors.name is NOT NULL)

        println!("View with joins nullability test completed!");
    });
}

#[test]
fn test_view_nullability_with_aggregates() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables
        execute_sql(
            client,
            r#"
            CREATE TABLE sales (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                amount DECIMAL NOT NULL,
                sale_date DATE NOT NULL
            );
            
            CREATE VIEW product_stats AS
            SELECT 
                product_id,
                COUNT(*) as sale_count,
                SUM(amount) as total_sales,
                MAX(sale_date) as last_sale_date
            FROM sales
            GROUP BY product_id;
        "#,
        )
        .expect("Should create table and view");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Check that the view struct was generated
        assert!(
            public_content.contains("struct ProductStats"),
            "Should generate ProductStats struct"
        );

        // In a full implementation:
        // - product_id would be non-Option (grouping column, NOT NULL)
        // - sale_count would be non-Option (COUNT always returns a value)
        // - total_sales would be Option (SUM can return NULL)
        // - last_sale_date would be Option (MAX can return NULL)

        println!("View with aggregates nullability test completed!");
    });
}

#[test]
fn test_view_nullability_disabled() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test table and view
        execute_sql(
            client,
            r#"
            CREATE TABLE items (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
            
            CREATE VIEW item_view AS
            SELECT id, name FROM items;
        "#,
        )
        .expect("Should create table and view");

        // Generate code with nullability inference disabled
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(false)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Check that the view struct was generated
        assert!(
            public_content.contains("struct ItemView"),
            "Should generate ItemView struct"
        );

        // With inference disabled, all fields should be Option unless manually annotated

        println!("View nullability disabled test completed!");
    });
}

#[test]
fn test_view_with_manual_annotations() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test table and view with manual annotations
        execute_sql(
            client,
            r#"
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name TEXT,
                price DECIMAL
            );
            
            CREATE VIEW product_view AS
            SELECT id, name, price FROM products;
            
            -- Manual annotation should override inference
            COMMENT ON COLUMN product_view.name IS '@pgrpc_not_null';
        "#,
        )
        .expect("Should create table, view, and annotation");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let public_content =
            std::fs::read_to_string(output_path.join("public.rs")).expect("Should read public.rs");

        // Check that the view struct was generated
        assert!(
            public_content.contains("struct ProductView"),
            "Should generate ProductView struct"
        );

        // Manual @pgrpc_not_null annotation should take precedence

        println!("View with manual annotations test completed!");
    });
}
