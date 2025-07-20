use super::*;
use indoc::indoc;
use pgrpc::PgrpcBuilder;

#[test]
fn test_composite_type_fromsql_generation() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create a simple composite type
        let schema_sql = indoc! {"
            -- Create a simple composite type
            CREATE TYPE contact AS (
                phone text,
                email text
            );
            
            -- Create another composite type that contains the first one
            CREATE TYPE person AS (
                name text,
                age int,
                contact contact
            );
            
            -- Create a function that returns a composite type
            CREATE OR REPLACE FUNCTION api.get_test_contact() RETURNS contact AS $$
            BEGIN
                RETURN ROW('555-1234', 'test@example.com')::contact;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Create a function that returns a nested composite type
            CREATE OR REPLACE FUNCTION api.get_test_person() RETURNS person AS $$
            BEGIN
                RETURN ROW('John Doe', 30, ROW('555-1234', 'john@example.com')::contact)::person;
            END;
            $$ LANGUAGE plpgsql;
        "};
        
        execute_sql(client, schema_sql).expect("Should create test schema");
        
        // Generate code
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();
        
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .output_path(output_path)
            .schema("public")
            .schema("api")
            .build()
            .expect("Code generation should succeed");
        
        // Read generated code to verify it has FromSql/ToSql derives
        let public_content = std::fs::read_to_string(output_path.join("public.rs"))
            .expect("Should read public.rs");
        
        println!("Generated public.rs content (first 1000 chars):");
        println!("{}", &public_content.chars().take(1000).collect::<String>());
        
        // Verify Contact type has FromSql and ToSql derives
        assert!(public_content.contains("pub struct Contact"), "Should generate Contact struct");
        assert!(public_content.contains("postgres_types::FromSql"), "Should have FromSql derive");
        assert!(public_content.contains("postgres_types::ToSql"), "Should have ToSql derive");
        assert!(public_content.contains("#[postgres(name = \"contact\")]"), "Should have postgres name attribute");
        
        // Verify Person type also has the derives
        assert!(public_content.contains("pub struct Person"), "Should generate Person struct");
        
        // Create a test project to verify compilation
        use crate::integration::compile_helpers::*;
        
        let project_dir = create_test_cargo_project(conn_string, vec!["public", "api"]);
        
        // Create a simple test that uses the generated types
        let test_code = indoc! {r#"
            use test_project::*;
            use test_project::api::*;
            use deadpool_postgres::{Config, Runtime};
            
            #[tokio::main]
            async fn main() {
                let conn_string = std::env::args().nth(1).expect("Connection string required");
                
                // Parse connection string and create deadpool config
                let mut config = Config::new();
                config.url = Some(conn_string);
                
                let pool = config.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
                    .expect("Failed to create pool");
                
                let client = pool.get().await.expect("Failed to get client");
                
                // Test calling function that returns composite type
                let contact = get_test_contact(&client)
                    .await
                    .expect("Should get contact")
                    .expect("Contact should not be null");
                
                println!("Contact phone: {:?}", contact.phone);
                println!("Contact email: {:?}", contact.email);
                
                // Test calling function that returns nested composite type
                let person = get_test_person(&client)
                    .await
                    .expect("Should get person")
                    .expect("Person should not be null");
                
                println!("Person name: {:?}", person.name);
                println!("Person age: {:?}", person.age);
                if let Some(contact) = &person.contact {
                    println!("Person contact phone: {:?}", contact.phone);
                    println!("Person contact email: {:?}", contact.email);
                }
                
                println!("✅ FromSql/ToSql works for composite types!");
            }
        "#};
        
        add_test_binary(&project_dir.path(), "test_composite", &test_code);
        
        // Compile and run the test
        let output = compile_and_run_with_args(&project_dir.path(), "test_composite", &[conn_string]);
        
        // First check if compilation succeeded
        if !output.status.success() {
            print_output(&output);
            panic!("Test compilation failed");
        }
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("✅ FromSql/ToSql works for composite types!"), 
            "Test should complete successfully. Output: {}", stdout);
        
        println!("✅ Composite types now support FromSql/ToSql!");
    });
}

#[test]
fn test_nested_composite_extraction() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create nested composite types
        let schema_sql = indoc! {"
            -- Create nested composite types
            CREATE TYPE address AS (
                street text,
                city text,
                zip text
            );
            
            CREATE TYPE contact_info AS (
                phone text,
                email text,
                address address
            );
            
            CREATE TYPE employee AS (
                id int,
                name text,
                contact contact_info
            );
            
            -- Create a function that returns deeply nested composite
            CREATE OR REPLACE FUNCTION api.get_test_employee() RETURNS employee AS $$
            BEGIN
                RETURN ROW(
                    1,
                    'Jane Smith',
                    ROW(
                        '555-5678',
                        'jane@example.com',
                        ROW('123 Main St', 'Anytown', '12345')::address
                    )::contact_info
                )::employee;
            END;
            $$ LANGUAGE plpgsql;
        "};
        
        execute_sql(client, schema_sql).expect("Should create test schema");
        
        // Generate code
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();
        
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .output_path(output_path)
            .schema("public")
            .schema("api")
            .build()
            .expect("Code generation should succeed");
        
        // Create test project
        use crate::integration::compile_helpers::*;
        
        let project_dir = create_test_cargo_project(conn_string, vec!["public", "api"]);
        
        // Create test that extracts nested composite values
        let test_code = indoc! {r#"
            use test_project::*;
            use test_project::api::*;
            use deadpool_postgres::{Config, Runtime};
            
            #[tokio::main]
            async fn main() {
                let conn_string = std::env::args().nth(1).expect("Connection string required");
                
                // Parse connection string and create deadpool config
                let mut config = Config::new();
                config.url = Some(conn_string);
                
                let pool = config.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
                    .expect("Failed to create pool");
                
                let client = pool.get().await.expect("Failed to get client");
                
                // Test calling function that returns deeply nested composite type
                let employee = get_test_employee(&client)
                    .await
                    .expect("Should get employee")
                    .expect("Employee should not be null");
                
                println!("Employee ID: {:?}", employee.id);
                println!("Employee Name: {:?}", employee.name);
                
                if let Some(contact) = &employee.contact {
                    println!("Employee Phone: {:?}", contact.phone);
                    println!("Employee Email: {:?}", contact.email);
                    
                    if let Some(address) = &contact.address {
                        println!("Employee Street: {:?}", address.street);
                        println!("Employee City: {:?}", address.city);
                        println!("Employee Zip: {:?}", address.zip);
                    }
                }
                
                println!("✅ Nested composite extraction works!");
            }
        "#};
        
        add_test_binary(&project_dir.path(), "test_nested", &test_code);
        
        // Compile and run the test
        let output = compile_and_run_with_args(&project_dir.path(), "test_nested", &[conn_string]);
        
        if !output.status.success() {
            print_output(&output);
            panic!("Test compilation failed");
        }
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("✅ Nested composite extraction works!"), 
            "Test should complete successfully. Output: {}", stdout);
        
        println!("✅ Nested composite types work properly with FromSql/ToSql!");
    });
}