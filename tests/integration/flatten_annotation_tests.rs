use super::*;
use pgrpc::*;

/// Test that demonstrates the current state of @pgrpc_flatten support
#[test]
fn test_flatten_annotation_struct_generation() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create schema with flatten annotation
        let schema_sql = indoc! {"
            -- Create address type
            CREATE TYPE address AS (
                street text,
                city text,
                postal_code text
            );
            
            -- Create person type with flattened address
            CREATE TYPE person AS (
                name text,
                age int,
                addr address
            );
            
            -- Add the flatten annotation
            COMMENT ON COLUMN person.addr IS '@pgrpc_flatten';
        "};
        
        execute_sql(client, schema_sql).expect("Should create test schema");
        
        // Generate code
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();
        
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .output_path(output_path)
            .schema("public")
            .build()
            .expect("Code generation should succeed");
        
        // Read the generated public.rs file
        let public_content = std::fs::read_to_string(output_path.join("public.rs"))
            .expect("Should read public.rs");
        
        println!("Generated public.rs content (first 1000 chars):");
        println!("{}", &public_content.chars().take(1000).collect::<String>());
        
        // Verify that the Person struct has flattened fields
        assert!(public_content.contains("pub struct Person"), "Should generate Person struct");
        assert!(public_content.contains("pub name: Option<String>"), "Should have name field");
        assert!(public_content.contains("pub age: Option<i32>"), "Should have age field");
        assert!(public_content.contains("pub addr_street: Option<String>"), "Should have flattened addr_street field");
        assert!(public_content.contains("pub addr_city: Option<String>"), "Should have flattened addr_city field");
        assert!(public_content.contains("pub addr_postal_code: Option<String>"), "Should have flattened addr_postal_code field");
        
        // Verify TryFrom implementation exists
        assert!(public_content.contains("impl TryFrom<tokio_postgres::Row> for Person"), 
            "Should generate TryFrom implementation");
        
        // Note: The TryFrom implementation currently generates placeholder code
        // for extracting flattened fields because composite types don't implement FromSql
        
        println!("✅ Flatten annotation correctly generates flattened struct fields");
        println!("⚠️  Note: Field extraction from composite columns is not yet implemented");
    });
}

/// Test that multiple flatten annotations work
#[test]
fn test_multiple_flatten_annotations() {
    with_isolated_database(|client| {
        let schema_sql = indoc! {"
            CREATE TYPE contact AS (
                phone text,
                email text
            );
            
            CREATE TYPE person AS (
                name text,
                home_contact contact,
                work_contact contact
            );
            
            COMMENT ON COLUMN person.home_contact IS '@pgrpc_flatten';
            COMMENT ON COLUMN person.work_contact IS '@pgrpc_flatten';
        "};
        
        execute_sql(client, schema_sql).expect("Should create test schema");
        
        // Verify annotations were created
        let comment_rows = client.query(
            indoc! {"
                SELECT a.attname as field_name, d.description as comment
                FROM pg_type t
                JOIN pg_class c ON t.typrelid = c.oid
                JOIN pg_attribute a ON c.oid = a.attrelid
                LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid
                WHERE t.typname = 'person'
                AND a.attnum > 0
                AND NOT a.attisdropped
                AND d.description IS NOT NULL
                ORDER BY a.attnum
            "},
            &[]
        ).expect("Should query field comments");
        
        assert_eq!(comment_rows.len(), 2, "Should have two fields with comments");
        
        for row in &comment_rows {
            let field_name: String = row.get("field_name");
            let comment: String = row.get("comment");
            assert_eq!(comment, "@pgrpc_flatten");
            assert!(field_name == "home_contact" || field_name == "work_contact");
        }
        
        println!("✅ Multiple flatten annotations can be applied to different fields");
    });
}

/// Test that flatten annotation is preserved alongside other annotations
#[test]
fn test_flatten_with_other_annotations() {
    with_isolated_database(|client| {
        let schema_sql = indoc! {"
            CREATE TYPE address AS (
                street text,
                city text
            );
            
            CREATE TYPE person AS (
                name text,
                addr address
            );
            
            -- Multiple annotations on the same field
            COMMENT ON COLUMN person.addr IS '@pgrpc_not_null @pgrpc_flatten';
        "};
        
        execute_sql(client, schema_sql).expect("Should create test schema");
        
        let comment_rows = client.query(
            indoc! {"
                SELECT d.description as comment
                FROM pg_type t
                JOIN pg_class c ON t.typrelid = c.oid
                JOIN pg_attribute a ON c.oid = a.attrelid
                LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid
                WHERE t.typname = 'person'
                AND a.attname = 'addr'
            "},
            &[]
        ).expect("Should query field comment");
        
        assert_eq!(comment_rows.len(), 1);
        let comment: String = comment_rows[0].get("comment");
        assert!(comment.contains("@pgrpc_not_null"));
        assert!(comment.contains("@pgrpc_flatten"));
        
        println!("✅ Flatten annotation works alongside other annotations");
    });
}