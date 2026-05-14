use super::*;
use crate::integration::compile_helpers::*;
use indoc::indoc;
use pgrpc::PgrpcBuilder;

/// Verify that arrays of composite types codegen and round-trip correctly.
///
/// Three flavors are exercised:
///   * function returning `composite[]`
///   * function taking `composite[]` as a parameter
///   * function returning SETOF a row containing a `composite[]` column
#[test]
fn test_array_of_composites_codegen_and_runtime() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        let schema_sql = indoc! {"
            CREATE TYPE contact AS (
                phone text,
                email text
            );

            CREATE OR REPLACE FUNCTION api.get_contacts() RETURNS contact[] AS $$
            BEGIN
                RETURN ARRAY[
                    ROW('555-1234', 'a@example.com')::contact,
                    ROW('555-5678', 'b@example.com')::contact
                ];
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION api.count_contacts(cs contact[]) RETURNS int AS $$
            BEGIN
                RETURN coalesce(array_length(cs, 1), 0);
            END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION api.contacts_with_label()
                RETURNS TABLE(label text, contacts contact[]) AS $$
            BEGIN
                RETURN QUERY SELECT
                    'primary'::text,
                    ARRAY[ROW('555-9999', 'c@example.com')::contact];
            END;
            $$ LANGUAGE plpgsql;
        "};
        execute_sql(client, schema_sql).expect("Should create test schema");

        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();

        PgrpcBuilder::new()
            .connection_string(conn_string)
            .output_path(output_path)
            .schema("public")
            .schema("api")
            .build()
            .expect("Code generation should succeed");

        let api_content = read_pretty(output_path.join("api.rs"));
        println!("Generated api.rs (first 4000 chars):\n{}", &api_content.chars().take(4000).collect::<String>());

        // Return type: Vec<public::Contact> (path is `super::public::Contact` from the api module,
        // possibly wrapped in Option for nullability).
        assert!(
            api_content.contains("Vec<super::public::Contact>"),
            "get_contacts return type should be Vec<super::public::Contact>. Got:\n{}",
            api_content
        );

        // Parameter type: should also be Vec<public::Contact> (possibly behind a borrow).
        // The bon builder pattern wraps args; we just confirm the type name appears
        // in the count_contacts signature region.
        let count_contacts_idx = api_content
            .find("fn count_contacts")
            .or_else(|| api_content.find("count_contacts"))
            .expect("count_contacts should be generated");
        let snippet = &api_content[count_contacts_idx..count_contacts_idx.saturating_add(800).min(api_content.len())];
        assert!(
            snippet.contains("public::Contact"),
            "count_contacts should reference public::Contact in its signature. Got:\n{}",
            snippet
        );

        // Compile & run end-to-end.
        let project_dir = create_test_cargo_project(conn_string, vec!["public", "api"]);

        let test_code = indoc! {r#"
            use test_project::api::*;
            use test_project::public::Contact;
            use deadpool_postgres::{Config, Runtime};

            #[tokio::main]
            async fn main() {
                let conn_string = std::env::args().nth(1).expect("Connection string required");
                let mut config = Config::new();
                config.url = Some(conn_string);
                let pool = config.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
                    .expect("Failed to create pool");
                let client = pool.get().await.expect("Failed to get client");

                // 1. Function returning composite[]
                let contacts = get_contacts()
                    .exec(&client)
                    .await
                    .expect("get_contacts call should succeed")
                    .expect("get_contacts should return non-null array");

                println!("got {} contacts", contacts.len());
                assert_eq!(contacts.len(), 2, "expected 2 contacts, got {}", contacts.len());
                assert_eq!(contacts[0].phone.as_deref(), Some("555-1234"));
                assert_eq!(contacts[1].email.as_deref(), Some("b@example.com"));

                // 2. Function taking composite[] as a parameter
                let payload = vec![
                    Contact { phone: Some("111".into()), email: Some("x@y.z".into()) },
                    Contact { phone: Some("222".into()), email: Some("p@q.r".into()) },
                    Contact { phone: Some("333".into()), email: Some("m@n.o".into()) },
                ];
                let n = count_contacts()
                    .cs(&payload)
                    .exec(&client)
                    .await
                    .expect("count_contacts call should succeed")
                    .expect("count_contacts should return a value");
                assert_eq!(n, 3, "expected count 3, got {}", n);

                // 3. SETOF row containing composite[]
                let rows = contacts_with_label()
                    .exec(&client)
                    .await
                    .expect("contacts_with_label call should succeed");
                assert_eq!(rows.len(), 1);
                let row = &rows[0];
                println!("label = {:?}", row.label);
                let inner: &Vec<Contact> = row.contacts.as_ref().expect("contacts column populated");
                assert_eq!(inner.len(), 1);
                assert_eq!(inner[0].email.as_deref(), Some("c@example.com"));

                println!("OK: arrays of composites round-trip");
            }
        "#};

        add_test_binary(project_dir.path(), "test_array_of_composites", test_code);

        let output = compile_and_run_with_args(
            project_dir.path(),
            "test_array_of_composites",
            &[conn_string],
        );

        if !output.status.success() {
            print_output(&output);
            panic!("array-of-composites test failed (see compile/run output above)");
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("OK: arrays of composites round-trip"),
            "test binary did not reach success marker. stdout:\n{}",
            stdout
        );
    });
}
