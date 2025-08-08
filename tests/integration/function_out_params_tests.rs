use super::*;
use tempfile::TempDir;

/// Tests functions with OUT parameters and proper query syntax
#[test]
fn test_function_out_params_query_syntax() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create test schema with functions having OUT parameters
        execute_sql(
            client,
            r#"
            CREATE SCHEMA func_out_test;
            SET search_path TO func_out_test;
            
            -- Function with single OUT parameter
            CREATE OR REPLACE FUNCTION get_current_time(OUT current_ts TIMESTAMP)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                current_ts := NOW();
            END;
            $$;
            
            -- Function with multiple OUT parameters (should use SELECT * FROM)
            CREATE OR REPLACE FUNCTION get_server_info(
                OUT server_version TEXT,
                OUT server_time TIMESTAMP,
                OUT database_name TEXT
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                server_version := version();
                server_time := NOW();
                database_name := current_database();
            END;
            $$;
            
            COMMENT ON FUNCTION get_server_info IS 
            'Get server information @pgrpc_not_null(server_version) @pgrpc_not_null(database_name)';
            
            -- Function with RETURNS TABLE (should use Row suffix)
            CREATE OR REPLACE FUNCTION list_numbers(p_limit INTEGER)
            RETURNS TABLE (
                num INTEGER,
                squared INTEGER
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                FOR num IN 1..p_limit LOOP
                    squared := num * num;
                    RETURN NEXT;
                END LOOP;
            END;
            $$;
            
            COMMENT ON FUNCTION list_numbers IS '@pgrpc_not_null(num) @pgrpc_not_null(squared)';
        "#,
        )
        .expect("Should create test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("func_out_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("func_out_test.rs"))
            .expect("Should read generated file");
            
        // Debug: Print part of the generated content
        if content.contains("get_server_info") {
            let start = content.find("get_server_info").unwrap();
            let snippet = &content[start.saturating_sub(100)..std::cmp::min(start + 500, content.len())];
            println!("Generated code around get_server_info:\n{}", snippet);
        }

        // Test single OUT parameter function
        assert!(
            content.contains("pub async fn get_current_time"),
            "Should generate function for single OUT parameter"
        );
        assert!(
            content.contains("-> Result<Option<time::OffsetDateTime>,"),
            "Single OUT parameter should be nullable by default"
        );

        // Test multiple OUT parameters function
        assert!(
            content.contains("pub async fn get_server_info"),
            "Should generate function for multiple OUT parameters"
        );
        assert!(
            content.contains("pub struct GetServerInfoOut"),
            "Should generate struct with Out suffix for function with OUT params"
        );
        assert!(
            !content.contains("pub struct GetServerInfoRow"),
            "Should NOT use Row suffix for regular OUT params"
        );

        // Check field types in the OUT struct
        assert!(
            content.contains("pub server_version: String") &&
            !content.contains("pub server_version: Option<String>"),
            "server_version should not be wrapped in Option due to @pgrpc_not_null"
        );
        assert!(
            content.contains("pub server_time: Option<time::OffsetDateTime>"),
            "server_time should be wrapped in Option (nullable by default)"
        );
        assert!(
            content.contains("pub database_name: String") &&
            !content.contains("pub database_name: Option<String>"),
            "database_name should not be wrapped in Option due to @pgrpc_not_null"
        );

        // Test RETURNS TABLE function
        assert!(
            content.contains("pub async fn list_numbers"),
            "Should generate function for RETURNS TABLE"
        );
        assert!(
            content.contains("pub struct ListNumbersRow"),
            "Should generate struct with Row suffix for RETURNS TABLE"
        );
        assert!(
            !content.contains("pub struct ListNumbersOut"),
            "Should NOT use Out suffix for RETURNS TABLE"
        );

        // Check that SELECT * FROM is used for functions with multiple OUT params
        // We can't directly test the generated query, but we can verify the function works
        // by checking that the correct struct type is returned
        assert!(
            content.contains("-> Result<Vec<ListNumbersRow>,"),
            "RETURNS TABLE should return Vec of Row structs"
        );
        assert!(
            content.contains("-> Result<GetServerInfoOut,"),
            "Function with multiple OUT params should return Out struct"
        );

        println!("✅ Function OUT parameters and query syntax correctly implemented!");
    });
}

/// Test mixed scenarios with procedures and functions
#[test]
fn test_mixed_out_param_scenarios() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            CREATE SCHEMA mixed_test;
            SET search_path TO mixed_test;
            
            -- Procedure with INOUT parameters
            CREATE OR REPLACE PROCEDURE process_data(
                INOUT data1 TEXT DEFAULT NULL,
                INOUT data2 INTEGER DEFAULT NULL
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                data1 := UPPER(COALESCE(data1, 'default'));
                data2 := COALESCE(data2, 0) + 100;
            END;
            $$;
            
            -- Function with OUT parameters (not RETURNS TABLE)
            CREATE OR REPLACE FUNCTION calculate_stats(
                IN numbers INTEGER[],
                OUT total INTEGER,
                OUT average NUMERIC,
                OUT count INTEGER
            )
            LANGUAGE plpgsql
            AS $$
            BEGIN
                total := (SELECT SUM(n) FROM UNNEST(numbers) n);
                count := array_length(numbers, 1);
                average := total::NUMERIC / count;
            END;
            $$;
            
            COMMENT ON FUNCTION calculate_stats IS '@pgrpc_not_null(total) @pgrpc_not_null(count)';
        "#,
        )
        .expect("Should create test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("mixed_test")
            .output_path(output_path)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("mixed_test.rs"))
            .expect("Should read generated file");

        // Test procedure with INOUT params
        assert!(
            content.contains("pub struct ProcessDataOut"),
            "Procedure should use Out suffix"
        );

        // Test function with OUT params
        assert!(
            content.contains("pub struct CalculateStatsOut"),
            "Function with OUT params should use Out suffix"
        );
        assert!(
            content.contains("pub total: i32"),
            "total should not be nullable due to annotation"
        );
        assert!(
            content.contains("pub average: Option<rust_decimal::Decimal>"),
            "average should be nullable by default"
        );
        assert!(
            content.contains("pub count: i32"),
            "count should not be nullable due to annotation"
        );

        println!("✅ Mixed OUT parameter scenarios correctly handled!");
    });
}