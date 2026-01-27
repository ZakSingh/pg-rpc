use super::*;
use pgrpc::*;
use std::path::PathBuf;
use tempfile::TempDir;

/// Test parsing SQL files with :param syntax
#[test]
fn test_sql_parser_named_params() {
    let sql = r#"
-- name: GetUser :one
SELECT * FROM users WHERE id = :user_id LIMIT 1;

-- name: UpdateUser :exec
UPDATE users SET name = :name, email = :email WHERE id = :user_id;
"#;

    let parser = pgrpc::sql_parser::SqlParser::new();
    let queries = parser.parse_content(sql, PathBuf::from("test.sql")).unwrap();

    assert_eq!(queries.len(), 2);

    // First query - GetUser
    let get_user = &queries[0];
    assert_eq!(get_user.name, "GetUser");
    assert_eq!(get_user.query_type, pgrpc::sql_parser::QueryType::One);
    assert_eq!(get_user.parameters.len(), 1);

    if let pgrpc::sql_parser::ParameterSpec::Named { name, position, nullable } = &get_user.parameters[0] {
        assert_eq!(name, "user_id");
        assert_eq!(*position, 1);
        assert_eq!(*nullable, false);
    } else {
        panic!("Expected named parameter");
    }

    // SQL should be transformed to use $1 instead of :user_id
    assert!(get_user.postgres_sql.contains("$1"));
    assert!(!get_user.postgres_sql.contains(":user_id"));

    // Second query - UpdateUser
    let update_user = &queries[1];
    assert_eq!(update_user.name, "UpdateUser");
    assert_eq!(update_user.query_type, pgrpc::sql_parser::QueryType::Exec);
    assert_eq!(update_user.parameters.len(), 3);

    // Parameters should be in order: name, email, user_id
    let param_names: Vec<String> = update_user.parameters.iter().map(|p| {
        match p {
            pgrpc::sql_parser::ParameterSpec::Named { name, nullable, .. } => {
                assert_eq!(*nullable, false);
                name.clone()
            },
            _ => panic!("Expected named parameter"),
        }
    }).collect();

    assert_eq!(param_names, vec!["name", "email", "user_id"]);
}

/// Test parsing with positional parameters
#[test]
fn test_sql_parser_positional_params() {
    let sql = r#"
-- name: GetUser :one
SELECT * FROM users WHERE id = $1 LIMIT 1;

-- name: UpdateUser :exec
UPDATE users SET name = $1, email = $2 WHERE id = $3;
"#;

    let parser = pgrpc::sql_parser::SqlParser::new();
    let queries = parser.parse_content(sql, PathBuf::from("test.sql")).unwrap();

    assert_eq!(queries.len(), 2);

    let get_user = &queries[0];
    assert_eq!(get_user.parameters.len(), 1);

    let update_user = &queries[1];
    assert_eq!(update_user.parameters.len(), 3);
}

/// Test all query types
#[test]
fn test_sql_parser_query_types() {
    let sql = r#"
-- name: GetOne :one
SELECT * FROM users WHERE id = :id;

-- name: GetMany :many
SELECT * FROM users;

-- name: DoExec :exec
UPDATE users SET active = true;

-- name: DoExecRows :execrows
DELETE FROM users WHERE inactive = true;
"#;

    let parser = pgrpc::sql_parser::SqlParser::new();
    let queries = parser.parse_content(sql, PathBuf::from("test.sql")).unwrap();

    assert_eq!(queries.len(), 4);
    assert_eq!(queries[0].query_type, pgrpc::sql_parser::QueryType::One);
    assert_eq!(queries[1].query_type, pgrpc::sql_parser::QueryType::Many);
    assert_eq!(queries[2].query_type, pgrpc::sql_parser::QueryType::Exec);
    assert_eq!(queries[3].query_type, pgrpc::sql_parser::QueryType::ExecRows);
}

/// Test query introspection with real database
#[test]
fn test_query_introspection() {
    with_isolated_database_and_container(|client, _container, _conn_string| {
        // Create a test table
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                bio TEXT,
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        ).unwrap();

        // Create a simple query
        let sql = "-- name: GetUser :one\nSELECT id, username, email FROM users WHERE id = :user_id;";

        let parser = pgrpc::sql_parser::SqlParser::new();
        let queries = parser.parse_content(sql, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 1);

        let parsed = &queries[0];

        // Now introspect it
        let rel_index = pgrpc::rel_index::RelIndex::new(client).unwrap();
        let temp_type_index = pgrpc::ty_index::TypeIndex::new(
            client,
            &[],
        ).unwrap();

        let view_cache = pgrpc::view_nullability::ViewNullabilityCache::new();

        let mut introspector = pgrpc::query_introspector::QueryIntrospector::new(
            client,
            &rel_index,
            &temp_type_index,
            &view_cache,
        );

        let introspected = introspector.introspect(parsed).unwrap();

        // Verify parameter types
        assert_eq!(introspected.params.len(), 1);
        assert_eq!(introspected.params[0].name, "user_id");

        // Verify return columns
        let return_cols = introspected.return_columns.as_ref().expect("Should have return columns");
        assert_eq!(return_cols.len(), 3);

        let col_names: Vec<String> = return_cols
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(col_names, vec!["id", "username", "email"]);

        // Verify nullability - id, username, email should all be NOT NULL
        assert!(!return_cols[0].nullable, "id should be NOT NULL");
        assert!(!return_cols[1].nullable, "username should be NOT NULL");
        assert!(!return_cols[2].nullable, "email should be NOT NULL");
    });
}

/// Test nullability analysis with LEFT JOIN
#[test]
fn test_query_nullability_with_join() {
    with_isolated_database_and_container(|client, _container, _conn_string| {
        // Create test tables
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE posts (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL REFERENCES users(id),
                title TEXT NOT NULL,
                content TEXT
            )",
            &[],
        ).unwrap();

        // Query with LEFT JOIN - post columns should become nullable
        let sql = r#"
-- name: GetUserWithPosts :many
SELECT
    u.id as user_id,
    u.username,
    p.id as post_id,
    p.title,
    p.content
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
WHERE u.id = :user_id;
"#;

        let parser = pgrpc::sql_parser::SqlParser::new();
        let queries = parser.parse_content(sql, PathBuf::from("test.sql")).unwrap();
        assert_eq!(queries.len(), 1);

        let parsed = &queries[0];

        let rel_index = pgrpc::rel_index::RelIndex::new(client).unwrap();
        let temp_type_index = pgrpc::ty_index::TypeIndex::new(
            client,
            &[],
        ).unwrap();

        // Build view nullability cache (empty in this case)
        let view_cache = pgrpc::view_nullability::ViewNullabilityCache::new();

        let mut introspector = pgrpc::query_introspector::QueryIntrospector::new(
            client,
            &rel_index,
            &temp_type_index,
            &view_cache,
        );

        let introspected = introspector.introspect(parsed).unwrap();

        let return_cols = introspected.return_columns.as_ref().expect("Should have return columns");
        assert_eq!(return_cols.len(), 5);

        // user_id and username from users table should be NOT NULL
        assert!(!return_cols[0].nullable, "user_id should be NOT NULL");
        assert!(!return_cols[1].nullable, "username should be NOT NULL");

        // post_id, title, content from posts table should be NULLABLE due to LEFT JOIN
        assert!(return_cols[2].nullable, "post_id should be NULLABLE (LEFT JOIN)");
        assert!(return_cols[3].nullable, "title should be NULLABLE (LEFT JOIN)");
        assert!(return_cols[4].nullable, "content should be NULLABLE (LEFT JOIN)");
    });
}

/// Test end-to-end code generation for queries
#[test]
fn test_query_code_generation() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        ).unwrap();

        // Create temporary SQL file
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: GetUser :one
SELECT id, username, email FROM users WHERE id = :user_id;

-- name: ListUsers :many
SELECT id, username, email FROM users ORDER BY username;

-- name: CreateUser :one
INSERT INTO users (username, email) VALUES (:username, :email) RETURNING id, username, email, created_at;

-- name: UpdateUser :exec
UPDATE users SET username = :username WHERE id = :user_id;

-- name: DeleteUser :execrows
DELETE FROM users WHERE id = :user_id;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        // Add queries config
        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated queries.rs file
        let queries_file = output_dir.join("queries.rs");
        assert!(queries_file.exists(), "queries.rs should be generated");

        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        // Verify GetUser function
        assert!(generated_code.contains("pub async fn get_user"), "Should generate get_user function");
        assert!(generated_code.contains("user_id:"), "Should have user_id parameter");
        assert!(generated_code.contains("Result<Option<GetUserRow>"), "Should return Option<GetUserRow>");
        assert!(generated_code.contains("struct GetUserRow"), "Should generate GetUserRow struct");

        // Verify ListUsers function
        assert!(generated_code.contains("pub async fn list_users"), "Should generate list_users function");
        assert!(generated_code.contains("Result<Vec<ListUsersRow>"), "Should return Vec<ListUsersRow>");

        // Verify CreateUser function
        assert!(generated_code.contains("pub async fn create_user"), "Should generate create_user function");
        assert!(generated_code.contains("username:"), "Should have username parameter");
        assert!(generated_code.contains("email:"), "Should have email parameter");

        // Verify UpdateUser function
        assert!(generated_code.contains("pub async fn update_user"), "Should generate update_user function");
        assert!(generated_code.contains("Result<()"), "Should return Result<()> for exec");

        // Verify DeleteUser function
        assert!(generated_code.contains("pub async fn delete_user"), "Should generate delete_user function");
        assert!(generated_code.contains("Result<u64"), "Should return Result<u64> for execrows");

        // Verify row structs have proper types
        assert!(generated_code.contains("pub id: i32"), "Should have i32 id field");
        assert!(generated_code.contains("pub username: String"), "Should have String username field");
        assert!(generated_code.contains("pub email: String"), "Should have String email field");
    });
}

/// Test that queries selecting from views inherit view nullability
#[test]
fn test_query_view_nullability_inheritance() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE posts (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL REFERENCES users(id),
                title TEXT NOT NULL
            )",
            &[],
        ).unwrap();

        // Create a view with LEFT JOIN
        client.execute(
            "CREATE VIEW user_posts AS
             SELECT u.id as user_id, u.username, p.id as post_id, p.title
             FROM users u
             LEFT JOIN posts p ON u.id = p.user_id",
            &[],
        ).unwrap();

        // Create temporary SQL file that selects from the view
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: GetUserPosts :many
SELECT user_id, username, post_id, title FROM user_posts WHERE user_id = :user_id;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code with nullability inference enabled
        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir)
            .infer_view_nullability(true); // Enable nullability inference

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated code
        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        // Verify that columns from the LEFT JOIN are nullable
        // user_id and username should be NOT NULL
        assert!(generated_code.contains("pub user_id: i32"), "user_id should be i32 (not Option)");
        assert!(generated_code.contains("pub username: String"), "username should be String (not Option)");

        // post_id and title should be NULLABLE due to LEFT JOIN in the view
        assert!(generated_code.contains("pub post_id: Option<i32>"), "post_id should be Option<i32>");
        assert!(generated_code.contains("pub title: Option<String>"), "title should be Option<String>");
    });
}

/// Test queries with COALESCE (should make column NOT NULL)
#[test]
fn test_query_coalesce_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                bio TEXT
            )",
            &[],
        ).unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        // COALESCE should make bio NOT NULL
        let sql = r#"
-- name: GetUserWithBio :one
SELECT id, username, COALESCE(bio, 'No bio') as bio FROM users WHERE id = :user_id;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

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

        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        // bio should be String (not Option<String>) because of COALESCE
        assert!(generated_code.contains("pub bio: String"), "bio should be String due to COALESCE");
        assert!(!generated_code.contains("pub bio: Option<String>"), "bio should not be Option");
    });
}

/// Test multiple SQL files
#[test]
fn test_multiple_query_files() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client.execute(
            "CREATE TABLE users (id SERIAL PRIMARY KEY, username TEXT NOT NULL)",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT NOT NULL)",
            &[],
        ).unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");

        // Create two SQL files
        let users_sql = temp_dir.path().join("users.sql");
        std::fs::write(&users_sql, "-- name: GetUser :one\nSELECT * FROM users WHERE id = :id;").unwrap();

        let posts_sql = temp_dir.path().join("posts.sql");
        std::fs::write(&posts_sql, "-- name: GetPost :one\nSELECT * FROM posts WHERE id = :id;").unwrap();

        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![
                users_sql.to_string_lossy().to_string(),
                posts_sql.to_string_lossy().to_string(),
            ],
        });

        builder.build().expect("Code generation should succeed");

        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        // Both queries should be generated
        assert!(generated_code.contains("pub async fn get_user"));
        assert!(generated_code.contains("pub async fn get_post"));
    });
}

#[test]
fn test_query_with_joins_and_type_casts() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables similar to the shipping query structure
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
                user_id INT NOT NULL REFERENCES users(id),
                title TEXT NOT NULL,
                content TEXT
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE comments (
                id SERIAL PRIMARY KEY,
                post_id INT NOT NULL REFERENCES posts(id),
                user_id INT NOT NULL REFERENCES users(id),
                comment_text TEXT NOT NULL
            )",
            &[],
        ).unwrap();

        // Create temporary SQL file with a query similar to the shipping query
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        // Query with JOINs, type casts, and aliases - mimics the shipping query structure
        let sql = r#"
-- name: GetPostDetails :one
SELECT
    p.title::text AS post_title,
    u.username::text AS author_name,
    u.email::text AS author_email,
    c.comment_text::text AS latest_comment
FROM posts p
INNER JOIN users u ON u.id = p.user_id
LEFT JOIN comments c ON c.post_id = p.id
WHERE p.id = $1;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated queries.rs file
        let queries_file = output_dir.join("queries.rs");
        assert!(queries_file.exists(), "queries.rs should be generated");

        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE ===\n{}\n======================", generated_code);

        // Verify the struct is NOT empty
        assert!(generated_code.contains("struct GetPostDetailsRow"), "Should generate GetPostDetailsRow struct");

        // Check for struct fields - this is where the bug would manifest
        assert!(generated_code.contains("pub post_title:"), "Struct should have post_title field");
        assert!(generated_code.contains("pub author_name:"), "Struct should have author_name field");
        assert!(generated_code.contains("pub author_email:"), "Struct should have author_email field");
        assert!(generated_code.contains("pub latest_comment:"), "Struct should have latest_comment field");
    });
}

/// Test INSERT with SELECT and RETURNING clause generates correct struct
#[test]
fn test_insert_select_with_returning() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables similar to the chat/message scenario in the bug report
        client.execute(
            "CREATE TABLE chat (
                chat_id SERIAL PRIMARY KEY,
                chat_nanoid TEXT NOT NULL UNIQUE
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE chat_account (
                chat_id INT NOT NULL REFERENCES chat(chat_id),
                account_id INT NOT NULL,
                PRIMARY KEY (chat_id, account_id)
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE message (
                message_id SERIAL PRIMARY KEY,
                chat_id INT NOT NULL REFERENCES chat(chat_id),
                account_id INT NOT NULL,
                item_id INT,
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        ).unwrap();

        // Create temporary SQL file with INSERT...SELECT...RETURNING
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        // This is the query pattern that was generating empty structs
        let sql = r#"
-- name: InsertMessage :one
INSERT INTO message (chat_id, account_id, content)
SELECT c.chat_id, :account_id, :content
FROM chat c
     JOIN chat_account ca USING (chat_id)
WHERE c.chat_nanoid = :chat_nanoid
  AND ca.account_id = :account_id
RETURNING message_id, account_id, item_id, content, created_at, updated_at;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated queries.rs file
        let queries_file = output_dir.join("queries.rs");
        assert!(queries_file.exists(), "queries.rs should be generated");

        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE ===\n{}\n======================", generated_code);

        // Verify the struct is NOT empty - this was the main bug
        assert!(generated_code.contains("struct InsertMessageRow"), "Should generate InsertMessageRow struct");

        // Verify all RETURNING columns are present with correct types
        assert!(generated_code.contains("pub message_id: i32"), "Should have message_id field");
        assert!(generated_code.contains("pub account_id: i32"), "Should have account_id field");
        assert!(generated_code.contains("pub item_id: Option<i32>"), "item_id should be Option<i32> (nullable)");
        assert!(generated_code.contains("pub content: String"), "Should have content field");

        // created_at and updated_at should be OffsetDateTime (not Option) because they're NOT NULL
        assert!(
            generated_code.contains("pub created_at: time::OffsetDateTime") ||
            generated_code.contains("pub created_at: chrono::DateTime"),
            "Should have created_at field with proper type"
        );
        assert!(
            generated_code.contains("pub updated_at: time::OffsetDateTime") ||
            generated_code.contains("pub updated_at: chrono::DateTime"),
            "Should have updated_at field with proper type"
        );
    });
}

/// Test UPDATE and DELETE with RETURNING clause generates correct struct
#[test]
fn test_update_delete_with_returning() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test table
        client.execute(
            "CREATE TABLE items (
                item_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                price NUMERIC(10,2) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        ).unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: UpdateItem :one
UPDATE items
SET name = :name, price = :price, updated_at = NOW()
WHERE item_id = :item_id
RETURNING item_id, name, description, price, updated_at;

-- name: DeleteItem :one
DELETE FROM items
WHERE item_id = :item_id
RETURNING item_id, name, description, price;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE ===\n{}\n======================", generated_code);

        // Verify UPDATE RETURNING struct
        assert!(generated_code.contains("struct UpdateItemRow"), "Should generate UpdateItemRow struct");
        assert!(generated_code.contains("pub item_id: i32"), "UpdateItemRow should have item_id");
        assert!(generated_code.contains("pub name: String"), "UpdateItemRow should have name");
        assert!(generated_code.contains("pub description: Option<String>"), "description should be Option<String>");
        assert!(generated_code.contains("pub price: rust_decimal::Decimal"), "UpdateItemRow should have price");

        // Verify DELETE RETURNING struct
        assert!(generated_code.contains("struct DeleteItemRow"), "Should generate DeleteItemRow struct");
    });
}

/// Test nullable parameter syntax and code generation
#[test]
fn test_nullable_parameters() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test table
        client.execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT,
                age INT
            )",
            &[],
        ).unwrap();

        // Insert test data
        client.execute(
            "INSERT INTO users (username, email, age) VALUES
             ('alice', 'alice@example.com', 30),
             ('bob', NULL, 25),
             ('charlie', 'charlie@example.com', NULL)",
            &[],
        ).unwrap();

        // Create temporary SQL file with nullable parameters
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        let sql = r#"
-- name: FindUsers :many
SELECT id, username, email, age
FROM users
WHERE (email = pgrpc.narg('email') OR pgrpc.narg('email') IS NULL)
  AND (age = pgrpc.narg('age') OR pgrpc.narg('age') IS NULL);
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        // Generate code
        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        // Read generated queries.rs file
        let queries_file = output_dir.join("queries.rs");
        assert!(queries_file.exists(), "queries.rs should be generated");

        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE ===\n{}\n======================", generated_code);

        // Verify nullable parameters are Option types
        assert!(generated_code.contains("email: Option<&str>"), "email parameter should be Option<&str>");
        assert!(generated_code.contains("age: Option<i32>"), "age parameter should be Option<i32>");

        // Verify function signature
        assert!(generated_code.contains("pub async fn find_users"), "Should generate find_users function");
        assert!(generated_code.contains("Result<Vec<FindUsersRow>"), "Should return Vec<FindUsersRow>");

        // Verify row struct
        assert!(generated_code.contains("struct FindUsersRow"), "Should generate FindUsersRow struct");
        assert!(generated_code.contains("pub id: i32"), "Should have id field");
        assert!(generated_code.contains("pub username: String"), "Should have username field");
    });
}

/// Test CTE (WITH clause) queries are correctly detected and introspected
#[test]
fn test_cte_select_query() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables
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
                user_id INT NOT NULL REFERENCES users(id),
                title TEXT NOT NULL,
                published BOOLEAN NOT NULL DEFAULT false
            )",
            &[],
        ).unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        // CTE with final SELECT - this was previously generating empty structs
        let sql = r#"
-- name: GetActiveUserPosts :many
WITH active_users AS (
    SELECT id, username FROM users WHERE id = :user_id
)
SELECT au.id as user_id, au.username, p.id as post_id, p.title
FROM active_users au
JOIN posts p ON p.user_id = au.id
WHERE p.published = true;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE (CTE SELECT) ===\n{}\n======================", generated_code);

        // Verify the struct is NOT empty - this was the main bug
        assert!(generated_code.contains("struct GetActiveUserPostsRow"), "Should generate GetActiveUserPostsRow struct");

        // Verify all columns are present
        assert!(generated_code.contains("pub user_id:"), "Should have user_id field");
        assert!(generated_code.contains("pub username:"), "Should have username field");
        assert!(generated_code.contains("pub post_id:"), "Should have post_id field");
        assert!(generated_code.contains("pub title:"), "Should have title field");
    });
}

/// Test CTE with INSERT and RETURNING clause
#[test]
fn test_cte_insert_with_returning() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test tables similar to the original bug report
        client.execute(
            "CREATE TABLE accounts (
                account_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            )",
            &[],
        ).unwrap();

        client.execute(
            "CREATE TABLE account_connections (
                connection_id SERIAL PRIMARY KEY,
                account_id INT NOT NULL REFERENCES accounts(account_id),
                provider TEXT NOT NULL,
                external_id TEXT NOT NULL,
                connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        ).unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        // CTE with INSERT...RETURNING - the connect_shopify pattern from bug report
        let sql = r#"
-- name: ConnectProvider :one
WITH target_account AS (
    SELECT account_id FROM accounts WHERE account_id = :account_id
)
INSERT INTO account_connections (account_id, provider, external_id)
SELECT account_id, :provider, :external_id
FROM target_account
RETURNING account_id;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE (CTE INSERT) ===\n{}\n======================", generated_code);

        // Verify the struct has the account_id field from RETURNING
        assert!(generated_code.contains("struct ConnectProviderRow"), "Should generate ConnectProviderRow struct");
        assert!(generated_code.contains("pub account_id: i32"), "Should have account_id field");
    });
}

/// Test recursive CTE
#[test]
fn test_recursive_cte() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a hierarchical table for recursive CTE
        client.execute(
            "CREATE TABLE categories (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id INT REFERENCES categories(id)
            )",
            &[],
        ).unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let sql_file_path = temp_dir.path().join("test.sql");

        // Recursive CTE to get category hierarchy
        let sql = r#"
-- name: GetCategoryHierarchy :many
WITH RECURSIVE category_tree AS (
    SELECT id, name, parent_id, 1 as depth
    FROM categories
    WHERE id = :root_id

    UNION ALL

    SELECT c.id, c.name, c.parent_id, ct.depth + 1
    FROM categories c
    JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT id, name, parent_id, depth FROM category_tree;
"#;

        std::fs::write(&sql_file_path, sql).expect("Failed to write SQL file");

        let output_dir = temp_dir.path().join("generated");

        let mut builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(&output_dir);

        builder = builder.queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file_path.to_string_lossy().to_string()],
        });

        builder.build().expect("Code generation should succeed");

        let queries_file = output_dir.join("queries.rs");
        let generated_code = std::fs::read_to_string(&queries_file).expect("Should read queries.rs");

        println!("=== GENERATED CODE (RECURSIVE CTE) ===\n{}\n======================", generated_code);

        // Verify the struct is NOT empty
        assert!(generated_code.contains("struct GetCategoryHierarchyRow"), "Should generate GetCategoryHierarchyRow struct");

        // Verify all columns are present
        assert!(generated_code.contains("pub id:"), "Should have id field");
        assert!(generated_code.contains("pub name:"), "Should have name field");
        assert!(generated_code.contains("pub parent_id:"), "Should have parent_id field");
        assert!(generated_code.contains("pub depth:"), "Should have depth field");
    });
}
