use super::*;
use pgrpc::*;

#[test]
fn test_function_with_flattened_composite_parameter() {
    with_isolated_database(|client| {
        // Create test composite types with flatten annotations
        let schema_sql = indoc! {"
            -- Create a composite type with flatten annotations
            CREATE TYPE contact_info AS (
                phone text,
                email text
            );

            CREATE TYPE person AS (
                name text,
                age int,
                contact contact_info -- @pgrpc_flatten
            );

            -- Add the flatten annotation
            COMMENT ON COLUMN person.contact IS '@pgrpc_flatten';

            -- Create a function that takes a flattened composite type as parameter
            CREATE OR REPLACE FUNCTION api.create_person(p person) RETURNS person AS $$
            BEGIN
                RETURN p;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Create a function that returns a flattened composite type  
            CREATE OR REPLACE FUNCTION api.get_test_person() RETURNS person AS $$
            BEGIN
                RETURN ROW('John Doe', 30, ROW('555-1234', 'john@example.com'));
            END;
            $$ LANGUAGE plpgsql;
        "};
        
        execute_sql(client, schema_sql).expect("Should create test schema");
        
        // Verify the functions were created
        let function_rows = client.query(
            indoc! {"
                SELECT p.proname, pg_get_function_result(p.oid) as result_type
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'api'
                AND p.proname IN ('create_person', 'get_test_person')
                ORDER BY p.proname
            "},
            &[]
        ).expect("Should query functions");
        
        assert_eq!(function_rows.len(), 2);
        
        let create_person_row = &function_rows[0];
        let get_test_person_row = &function_rows[1];
        
        assert_eq!(create_person_row.get::<_, String>("proname"), "create_person");
        assert_eq!(create_person_row.get::<_, String>("result_type"), "person");
        
        assert_eq!(get_test_person_row.get::<_, String>("proname"), "get_test_person");
        assert_eq!(get_test_person_row.get::<_, String>("result_type"), "person");
        
        println!("✅ Successfully created functions with flattened composite types");
    });
}

#[test] 
fn test_function_with_nested_flattened_composite() {
    with_isolated_database(|client| {
        // Create nested composite types with multiple flatten annotations
        let schema_sql = indoc! {"
            -- Create nested composite types
            CREATE TYPE contact_info AS (
                phone text,
                email text
            );

            CREATE TYPE address AS (
                street text,
                city text,
                contact contact_info -- @pgrpc_flatten
            );

            CREATE TYPE person AS (
                name text,
                addr address -- @pgrpc_flatten
            );

            -- Add the flatten annotations
            COMMENT ON COLUMN address.contact IS '@pgrpc_flatten';
            COMMENT ON COLUMN person.addr IS '@pgrpc_flatten';

            -- Create a function that works with nested flattened types
            CREATE OR REPLACE FUNCTION api.process_person(p person) RETURNS text AS $$
            BEGIN
                RETURN p.name || ' lives at ' || (p.addr).street || ' in ' || (p.addr).city;
            END;
            $$ LANGUAGE plpgsql;
        "};
        
        execute_sql(client, schema_sql).expect("Should create nested test schema");
        
        // Verify the function was created
        let function_rows = client.query(
            indoc! {"
                SELECT p.proname, pg_get_function_arguments(p.oid) as args
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'api'
                AND p.proname = 'process_person'
            "},
            &[]
        ).expect("Should query process_person function");
        
        assert_eq!(function_rows.len(), 1);
        
        let function_row = &function_rows[0];
        assert_eq!(function_row.get::<_, String>("proname"), "process_person");
        assert_eq!(function_row.get::<_, String>("args"), "p person");
        
        println!("✅ Successfully created function with nested flattened composite parameter");
    });
}