use super::*;
use pgrpc::*;

#[test]
fn test_schema_introspection() {
    with_isolated_database_and_container(|client, container, conn_string| {
        // Test that we can connect and introspect the schema
        let _builder = PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .schema("api");
            
        // This should work without errors - we're testing that the database
        // connection and schema introspection works
        // Note: We can't easily test the full build() here since it writes files,
        // but we can test that the connection and basic setup works.
        
        // Let's verify the schema state directly
        let rows = client.query(
            "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('public', 'api')",
            &[]
        ).expect("Should query pg_tables");
        
        let tables: Vec<(String, String)> = rows.iter()
            .map(|row| (row.get("schemaname"), row.get("tablename")))
            .collect();
        
        // Verify we have the expected tables
        assert!(tables.contains(&("public".to_string(), "account".to_string())));
        assert!(tables.contains(&("public".to_string(), "post".to_string())));
        assert!(tables.contains(&("public".to_string(), "login_details".to_string())));
    });
}

#[test]
fn test_constraint_discovery() {
    with_isolated_database(|client| {
        // Test that we can discover constraints properly
        let rows = client.query(
            indoc! {"
                SELECT tc.constraint_name, tc.constraint_type, tc.table_name
                FROM information_schema.table_constraints tc
                WHERE tc.table_schema = 'public'
                ORDER BY tc.table_name, tc.constraint_name
            "},
            &[]
        ).expect("Should query constraints");
        
        let constraints: Vec<(String, String, String)> = rows.iter()
            .map(|row| (
                row.get("constraint_name"),
                row.get("constraint_type"), 
                row.get("table_name")
            ))
            .collect();
        
        // Check for specific constraints we expect
        let account_constraints: Vec<_> = constraints.iter()
            .filter(|(_, _, table)| table == "account")
            .collect();
        
        // Should have primary key, unique, and check constraints
        assert!(account_constraints.iter().any(|(_, ctype, _)| ctype == "PRIMARY KEY"));
        assert!(account_constraints.iter().any(|(_, ctype, _)| ctype == "UNIQUE"));
        assert!(account_constraints.iter().any(|(_, ctype, _)| ctype == "CHECK"));
        
        let post_constraints: Vec<_> = constraints.iter()
            .filter(|(_, _, table)| table == "post")
            .collect();
        
        // Post should have primary key, foreign key, and check constraints
        assert!(post_constraints.iter().any(|(_, ctype, _)| ctype == "PRIMARY KEY"));
        assert!(post_constraints.iter().any(|(_, ctype, _)| ctype == "FOREIGN KEY"));
        assert!(post_constraints.iter().any(|(_, ctype, _)| ctype == "CHECK"));
    });
}

#[test]
fn test_function_discovery() {
    with_isolated_database(|client| {
        // Test that API functions are discoverable
        let rows = client.query(
            indoc! {"
                SELECT r.routine_name, r.routine_schema, r.data_type
                FROM information_schema.routines r
                WHERE r.routine_schema = 'api'
                AND r.routine_type = 'FUNCTION'
            "},
            &[]
        ).expect("Should query functions");
        
        let functions: Vec<(String, String, String)> = rows.iter()
            .map(|row| (
                row.get("routine_name"),
                row.get("routine_schema"),
                row.get("data_type")
            ))
            .collect();
        
        // Should find our API function
        assert!(functions.iter().any(|(name, schema, _)| 
            name == "get_account_by_email" && schema == "api"
        ));
    });
}

#[test]
fn test_enum_type_discovery() {
    with_isolated_database(|client| {
        // Test that custom enum types are discoverable
        let rows = client.query(
            indoc! {"
                SELECT t.typname, t.typtype, n.nspname as schema_name
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE t.typtype = 'e'
                AND n.nspname = 'public'
            "},
            &[]
        ).expect("Should query enum types");
        
        let enums: Vec<(String, i8, String)> = rows.iter()
            .map(|row| (
                row.get("typname"),
                row.get("typtype"),
                row.get("schema_name")
            ))
            .collect();
        
        // Should find our role enum
        assert!(enums.iter().any(|(name, _, schema)| 
            name == "role" && schema == "public"
        ));
    });
}

#[test]
fn test_composite_type_discovery() {
    with_isolated_database(|client| {
        // Test that composite types (like table types) are discoverable
        let rows = client.query(
            indoc! {"
                SELECT t.typname, t.typtype, n.nspname as schema_name
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE t.typtype = 'c'
                AND n.nspname = 'public'
                AND t.typname IN ('account', 'post', 'login_details')
            "},
            &[]
        ).expect("Should query composite types");
        
        let composites: Vec<(String, i8, String)> = rows.iter()
            .map(|row| (
                row.get("typname"),
                row.get("typtype"),
                row.get("schema_name")
            ))
            .collect();
        
        // Should find our table composite types
        assert!(composites.iter().any(|(name, _, schema)| 
            name == "account" && schema == "public"
        ));
        assert!(composites.iter().any(|(name, _, schema)| 
            name == "post" && schema == "public"
        ));
    });
}