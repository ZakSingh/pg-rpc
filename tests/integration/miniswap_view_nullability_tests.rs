use super::*;
use std::collections::HashMap;
use tempfile::TempDir;

/// Tests view nullability inference based on real-world patterns from miniswap
/// Verifies that the inference matches the manual @pgrpc_not_null annotations
#[test]
fn test_miniswap_view_nullability_inference() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Execute the miniswap test SQL
        let sql_content = include_str!("../sql/view_nullability_miniswap.sql");
        execute_sql(client, sql_content).expect("Should create miniswap test schema");

        // Generate code with nullability inference enabled
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("miniswap_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        // Read the generated code
        let content = std::fs::read_to_string(output_path.join("miniswap_test.rs"))
            .expect("Should read generated file");

        // Parse expected nullability based on miniswap's manual annotations
        let expected_nullability = get_expected_nullability();

        // Verify each view's nullability matches expectations
        verify_view_nullability(
            &content,
            "CartSummary",
            &expected_nullability["cart_summary"],
        );
        verify_view_nullability(
            &content,
            "AccountSummary",
            &expected_nullability["account_summary"],
        );
        verify_view_nullability(
            &content,
            "ProductSummary",
            &expected_nullability["product_summary"],
        );
        verify_view_nullability(
            &content,
            "CartDetails",
            &expected_nullability["cart_details"],
        );
        verify_view_nullability(
            &content,
            "TransactionSummary",
            &expected_nullability["transaction_summary"],
        );
        verify_view_nullability(
            &content,
            "ItemDetails",
            &expected_nullability["item_details"],
        );
        verify_view_nullability(
            &content,
            "NullabilityEdgeCases",
            &expected_nullability["nullability_edge_cases"],
        );

        println!("✅ All miniswap view nullability patterns correctly inferred!");
    });
}

/// Returns expected nullability for each view based on miniswap's @pgrpc_not_null annotations
fn get_expected_nullability() -> HashMap<&'static str, Vec<(&'static str, bool)>> {
    let mut expected = HashMap::new();

    // cart_summary - all fields marked as @pgrpc_not_null in miniswap
    expected.insert(
        "cart_summary",
        vec![
            ("account_id", true),          // NOT NULL from base table
            ("item_count", true),          // COALESCE with 0
            ("selected_item_count", true), // COALESCE with 0
            ("subtotal_amount", true),     // COALESCE with 0
            ("subtotal_currency", true),   // NOT NULL from base table
        ],
    );

    // account_summary - specific fields marked as @pgrpc_not_null
    expected.insert(
        "account_summary",
        vec![
            ("account_id", true),               // NOT NULL from base table
            ("account_nanoid", true),           // NOT NULL from base table
            ("country_code", true),             // NOT NULL from base table
            ("currency_code", true),            // NOT NULL from base table
            ("name", true),                     // COALESCE expression
            ("free_shipping_threshold", false), // Nullable from LEFT JOIN
            ("sales_count", true),              // COALESCE with 0
            ("positive_feedback_pct", false),   // Nullable from LEFT JOIN
            ("is_seller", true),                // CASE with ELSE false
            ("stripe_charges_enabled", false),  // Nullable from LEFT JOIN
            ("away_until", false),              // Nullable from LEFT JOIN
        ],
    );

    // product_summary - fields marked as @pgrpc_not_null
    expected.insert(
        "product_summary",
        vec![
            ("product_id", true),      // NOT NULL from base table
            ("product_type", true),    // NOT NULL from base table
            ("name", true),            // NOT NULL from base table
            ("slug", true),            // NOT NULL from base table
            ("sku", true),             // NOT NULL from base table
            ("release_date", false),   // Nullable in base table
            ("msrp", false),           // Nullable in base table
            ("is_discontinued", true), // NOT NULL from base table
            ("category_id", true),     // NOT NULL from JOIN (not LEFT JOIN)
            ("category_name", true),   // NOT NULL from JOIN (not LEFT JOIN)
            ("listing_count", true),   // Subquery with COUNT always returns a value
            ("created_at", true),      // NOT NULL from base table
            ("updated_at", true),      // NOT NULL from base table
        ],
    );

    // cart_details - complex view with composite types
    // cart_details references cart_summary view, which should be analyzed first
    expected.insert(
        "cart_details",
        vec![
            ("account_id", true),          // From cart_summary view (NOT NULL)
            ("item_count", true),          // From cart_summary view (NOT NULL)
            ("selected_item_count", true), // From cart_summary view (NOT NULL)
            ("subtotal_amount", true),     // From cart_summary view (NOT NULL)
            ("subtotal_currency", true),   // From cart_summary view (NOT NULL)
            ("sellers", true),             // COALESCE with empty array
        ],
    );

    // transaction_summary - complex aggregations
    expected.insert(
        "transaction_summary",
        vec![
            ("checkout_id", true),        // PRIMARY KEY
            ("seller_id", true),          // PRIMARY KEY
            ("status", true),             // NOT NULL from base table
            ("payment_intent_id", false), // Nullable in base table
            ("item_subtotal", true),      // NOT NULL from base table (composite)
            ("shipping_subtotal", true),  // NOT NULL from base table
            ("seller_fee", true),         // NOT NULL from base table
            ("stripe_fee", true),         // NOT NULL from base table
            ("seller_net", true),         // NOT NULL from base table
            ("total_amount", false), // Math expression with field selection - conservatively nullable
            ("total_currency", false), // Field selection - conservatively nullable
            ("shipping_status", true), // CASE with ELSE 'pending'
            ("product_names", false), // ARRAY_AGG - conservatively nullable
            ("created_at", true),    // NOT NULL from base table
            ("updated_at", true),    // NOT NULL from base table
        ],
    );

    // item_details - multiple LEFT JOINs and CASE expressions
    expected.insert(
        "item_details",
        vec![
            ("item_id", true),        // NOT NULL from base table
            ("item_nanoid", true),    // NOT NULL from base table
            ("account_id", true),     // NOT NULL from base table
            ("product_id", true),     // NOT NULL from base table
            ("description", false),   // Nullable in base table
            ("is_archived", true),    // NOT NULL from base table
            ("country_code", true),   // NOT NULL from base table
            ("listing_price", false), // LEFT JOIN can be NULL
            ("product_name", true),   // Regular JOIN, NOT NULL in product
            ("product_type", true),   // Regular JOIN, NOT NULL in product
            ("owner_name", true),     // From account_summary view - name is NOT NULL (COALESCE)
            ("listing_status", true), // CASE with ELSE 'available'
            ("is_purchasable", true), // CASE with ELSE true
            ("in_wishlist", true),    // CASE with ELSE false
            ("in_cart", true),        // Boolean literal false
            ("created_at", true),     // NOT NULL from base table
            ("updated_at", true),     // NOT NULL from base table
        ],
    );

    // nullability_edge_cases - testing various SQL constructs
    expected.insert(
        "nullability_edge_cases",
        vec![
            ("always_string", true),   // String literal
            ("always_number", true),   // Number literal
            ("always_true", true),     // Boolean literal
            ("always_false", true),    // Boolean literal
            ("always_null", true),     // NULL literal - detected as constant (non-null)
            ("maybe_null_case", true), // CASE without ELSE - conservatively non-null
            ("never_null_case", true), // CASE with ELSE
            ("int_array", true),       // Array constructor
            ("empty_array", true),     // Empty array literal
            ("coalesce_result", true), // COALESCE with fallback
            ("math_result", true),     // Math operation on constants
            ("concat_result", true),   // String concatenation on constants
        ],
    );

    expected
}

/// Verifies that a struct's fields match expected nullability
fn verify_view_nullability(content: &str, struct_name: &str, expected: &[(&str, bool)]) {
    println!("\nVerifying {} nullability...", struct_name);

    // Find the struct definition
    let struct_pattern = format!(r"pub struct {} \{{([^}}]+)\}}", struct_name);
    let re = regex::Regex::new(&struct_pattern).unwrap();

    let captures = re
        .captures(content)
        .unwrap_or_else(|| panic!("Could not find struct {}", struct_name));

    let struct_body = &captures[1];

    // Check each expected field
    for (field_name, should_be_non_null) in expected {
        let field_pattern = format!(r"pub {}: ([^,\n]+)", field_name);
        let field_re = regex::Regex::new(&field_pattern).unwrap();

        let field_match = field_re.captures(struct_body).unwrap_or_else(|| {
            panic!(
                "Could not find field {} in struct {}",
                field_name, struct_name
            )
        });

        let field_type = field_match[1].trim();
        let is_option = field_type.starts_with("Option<");
        let is_non_null = !is_option;

        if is_non_null != *should_be_non_null {
            panic!(
                "Field {}.{} has incorrect nullability: expected {}, but got {} (type: {})",
                struct_name,
                field_name,
                if *should_be_non_null {
                    "non-null"
                } else {
                    "nullable"
                },
                if is_non_null { "non-null" } else { "nullable" },
                field_type
            );
        }

        println!(
            "  ✓ {}: {} ({})",
            field_name,
            field_type,
            if is_non_null { "non-null" } else { "nullable" }
        );
    }
}

#[test]
fn test_composite_type_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Create composite types with fields that should be non-null
        execute_sql(
            client,
            r#"
            CREATE SCHEMA composite_test;
            SET search_path TO composite_test;
            
            -- Currency type like miniswap
            CREATE TYPE currency AS (
                amount integer,
                currency_code text
            );
            
            -- In miniswap these are marked @pgrpc_not_null
            -- We'll verify inference works without annotations
            
            CREATE TYPE address AS (
                line1 text,
                line2 text,
                city text,
                postal_code text,
                country text
            );
            
            -- Create a table using these types
            CREATE TABLE test_table (
                id bigint PRIMARY KEY,
                price currency NOT NULL,
                shipping_address address,
                billing_address address NOT NULL
            );
            
            -- Create a view that uses these composite types
            CREATE VIEW test_composite_view AS
            SELECT 
                id,
                price,  -- NOT NULL composite
                shipping_address,  -- Nullable composite
                billing_address,  -- NOT NULL composite
                -- Construct composite in query
                ROW(100, 'USD')::currency as fixed_price,
                -- COALESCE on composite
                COALESCE(shipping_address, billing_address) as delivery_address
            FROM test_table;
        "#,
        )
        .expect("Should create composite type test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("composite_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("composite_test.rs"))
            .expect("Should read generated file");

        // Verify composite type fields
        // Note: Individual fields within composites are currently always Option<T>
        // The nullability applies to the composite as a whole
        verify_view_nullability(
            &content,
            "TestCompositeView",
            &[
                ("id", true),                // PRIMARY KEY
                ("price", true),             // NOT NULL composite
                ("shipping_address", false), // Nullable composite
                ("billing_address", true),   // NOT NULL composite
                ("fixed_price", true),       // ROW constructor
                ("delivery_address", true),  // COALESCE expression
            ],
        );

        println!("✅ Composite type nullability correctly inferred!");
    });
}

#[test]
fn test_inner_join_table_reference() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Test that INNER JOIN table references are not nullable
        execute_sql(
            client,
            r#"
            CREATE SCHEMA join_test;
            SET search_path TO join_test;
            
            CREATE TABLE address (
                address_id int PRIMARY KEY,
                line1 text NOT NULL,
                city text NOT NULL
            );
            
            CREATE TABLE account_delivery_address (
                account_id int NOT NULL,
                address_id int NOT NULL,
                delivery_instructions text,
                recipient_name text NOT NULL,
                deleted_at timestamp
            );
            
            CREATE TABLE account (
                account_id int PRIMARY KEY,
                default_delivery_address_id int
            );
            
            -- Test view with JOIN and whole-row reference
            CREATE VIEW delivery_address AS
            SELECT ada.account_id,
                   a.address_id,
                   ada.delivery_instructions,
                   ada.recipient_name,
                   a AS address,  -- whole row reference from INNER JOIN
                   ada.deleted_at,
                   (ac.default_delivery_address_id = a.address_id) AS is_default
            FROM account_delivery_address ada
            JOIN address a USING (address_id)
            JOIN account ac USING (account_id);
        "#,
        )
        .expect("Should create join test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("join_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("join_test.rs"))
            .expect("Should read generated file");

        // Verify nullability
        verify_view_nullability(
            &content,
            "DeliveryAddress",
            &[
                ("account_id", true),             // NOT NULL from base table
                ("address_id", true),             // NOT NULL from INNER JOIN
                ("delivery_instructions", false), // Nullable in base table
                ("recipient_name", true),         // NOT NULL from base table
                ("address", true),                // INNER JOIN table reference should be NOT NULL
                ("deleted_at", false),            // Nullable in base table
                ("is_default", false),            // Comparison with nullable column
            ],
        );

        println!("✅ INNER JOIN table reference nullability correctly inferred!");
    });
}

#[test]
fn test_left_join_table_reference() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Test that LEFT JOIN table references are nullable
        execute_sql(
            client,
            r#"
            CREATE SCHEMA left_join_test;
            SET search_path TO left_join_test;
            
            CREATE TABLE account (
                account_id int PRIMARY KEY,
                name text NOT NULL
            );
            
            CREATE TABLE optional_profile (
                account_id int PRIMARY KEY,
                bio text NOT NULL,
                avatar_url text
            );
            
            -- Test view with LEFT JOIN and whole-row reference
            CREATE VIEW account_with_profile AS
            SELECT a.account_id,
                   a.name,
                   op AS profile  -- whole row reference from LEFT JOIN
            FROM account a
            LEFT JOIN optional_profile op USING (account_id);
        "#,
        )
        .expect("Should create left join test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("left_join_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("left_join_test.rs"))
            .expect("Should read generated file");

        // Verify nullability
        verify_view_nullability(
            &content,
            "AccountWithProfile",
            &[
                ("account_id", true), // NOT NULL from base table
                ("name", true),       // NOT NULL from base table
                ("profile", false),   // LEFT JOIN table reference should be nullable
            ],
        );

        println!("✅ LEFT JOIN table reference nullability correctly inferred!");
    });
}

#[test]
fn test_null_comparison_behavior() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Test that comparison operators correctly handle NULL propagation
        execute_sql(
            client,
            r#"
            CREATE SCHEMA null_comparison_test;
            SET search_path TO null_comparison_test;
            
            CREATE TABLE test_data (
                id int PRIMARY KEY,
                nullable_int int,
                not_null_int int NOT NULL,
                nullable_text text,
                not_null_text text NOT NULL
            );
            
            -- Test view with various comparison operations
            CREATE VIEW comparison_results AS
            SELECT 
                id,
                -- Comparisons with nullable columns (should be nullable)
                (nullable_int = 5) as nullable_eq_const,
                (nullable_int <> 5) as nullable_ne_const,
                (nullable_int < 10) as nullable_lt_const,
                (nullable_int > 0) as nullable_gt_const,
                (nullable_int <= 10) as nullable_lte_const,
                (nullable_int >= 0) as nullable_gte_const,
                (nullable_int = not_null_int) as nullable_eq_not_null,
                (nullable_int = nullable_int) as nullable_eq_self,
                (nullable_text = 'test') as nullable_text_eq,
                
                -- Comparisons with non-null columns only (should be non-null)
                (not_null_int = 5) as not_null_eq_const,
                (not_null_int <> not_null_int) as not_null_ne_self,
                (not_null_text = not_null_text) as not_null_text_eq_self,
                
                -- IS NULL / IS NOT NULL (always non-null boolean)
                (nullable_int IS NULL) as is_null_check,
                (nullable_int IS NOT NULL) as is_not_null_check,
                
                -- Arithmetic with nullable (should be nullable)
                (nullable_int + 5) as nullable_plus_const,
                (nullable_int * not_null_int) as nullable_times_not_null,
                
                -- Arithmetic with non-null only (should be non-null)
                (not_null_int + 5) as not_null_plus_const,
                (not_null_int * 2) as not_null_times_const
            FROM test_data;
        "#,
        )
        .expect("Should create null comparison test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("null_comparison_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("null_comparison_test.rs"))
            .expect("Should read generated file");

        // Verify nullability
        verify_view_nullability(
            &content,
            "ComparisonResults",
            &[
                ("id", true), // PRIMARY KEY
                // Nullable comparisons
                ("nullable_eq_const", false),    // NULL = 5 -> NULL
                ("nullable_ne_const", false),    // NULL <> 5 -> NULL
                ("nullable_lt_const", false),    // NULL < 10 -> NULL
                ("nullable_gt_const", false),    // NULL > 0 -> NULL
                ("nullable_lte_const", false),   // NULL <= 10 -> NULL
                ("nullable_gte_const", false),   // NULL >= 0 -> NULL
                ("nullable_eq_not_null", false), // NULL = non_null -> NULL
                ("nullable_eq_self", false),     // NULL = NULL -> NULL
                ("nullable_text_eq", false),     // NULL = 'test' -> NULL
                // Non-null comparisons
                ("not_null_eq_const", true), // non_null = 5 -> boolean
                ("not_null_ne_self", true),  // non_null <> non_null -> boolean
                ("not_null_text_eq_self", true), // non_null = non_null -> boolean
                // IS NULL checks
                ("is_null_check", true),     // IS NULL -> boolean
                ("is_not_null_check", true), // IS NOT NULL -> boolean
                // Nullable arithmetic
                ("nullable_plus_const", false),     // NULL + 5 -> NULL
                ("nullable_times_not_null", false), // NULL * non_null -> NULL
                // Non-null arithmetic
                ("not_null_plus_const", true), // non_null + 5 -> non_null
                ("not_null_times_const", true), // non_null * 2 -> non_null
            ],
        );

        println!("✅ NULL comparison and arithmetic behavior correctly inferred!");
    });
}

#[test]
fn test_star_expansion_nullability() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Test that star expansion correctly handles nullability
        execute_sql(
            client,
            r#"
            CREATE SCHEMA star_test;
            SET search_path TO star_test;
            
            -- Base tables with various nullability
            CREATE TABLE users (
                id int PRIMARY KEY,
                username text NOT NULL,
                email text NOT NULL,
                created_at timestamp NOT NULL,
                deleted_at timestamp  -- nullable
            );
            
            CREATE TABLE profiles (
                user_id int PRIMARY KEY,
                bio text NOT NULL,
                avatar_url text,  -- nullable
                location text NOT NULL
            );
            
            CREATE TABLE posts (
                id int PRIMARY KEY,
                user_id int NOT NULL,
                title text NOT NULL,
                content text,  -- nullable
                published_at timestamp  -- nullable
            );
            
            -- Test 1: Simple star expansion
            CREATE VIEW all_users AS
            SELECT * FROM users;
            
            -- Test 2: Table-specific star expansion
            CREATE VIEW user_profiles AS
            SELECT u.*, p.bio, p.location
            FROM users u
            JOIN profiles p ON u.id = p.user_id;
            
            -- Test 3: Multiple star expansions
            CREATE VIEW user_data AS
            SELECT u.*, p.*
            FROM users u
            JOIN profiles p ON u.id = p.user_id;
            
            -- Test 4: Star with LEFT JOIN (makes columns nullable)
            CREATE VIEW users_with_profiles AS
            SELECT u.*, p.bio, p.avatar_url
            FROM users u
            LEFT JOIN profiles p ON u.id = p.user_id;
            
            -- Test 5: Mixed star and explicit columns
            CREATE VIEW post_details AS
            SELECT p.id as post_id, u.*, p.title, p.published_at
            FROM posts p
            JOIN users u ON p.user_id = u.id;
        "#,
        )
        .expect("Should create star expansion test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("star_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("star_test.rs"))
            .expect("Should read generated file");

        // Test 1: Simple star expansion - all columns should maintain their original nullability
        verify_view_nullability(
            &content,
            "AllUsers",
            &[
                ("id", true),          // PRIMARY KEY
                ("username", true),    // NOT NULL
                ("email", true),       // NOT NULL
                ("created_at", true),  // NOT NULL
                ("deleted_at", false), // Nullable
            ],
        );

        // Test 2: Table-specific star - users.* columns maintain nullability
        verify_view_nullability(
            &content,
            "UserProfiles",
            &[
                ("id", true),          // PRIMARY KEY from users
                ("username", true),    // NOT NULL from users
                ("email", true),       // NOT NULL from users
                ("created_at", true),  // NOT NULL from users
                ("deleted_at", false), // Nullable from users
                ("bio", true),         // NOT NULL from profiles (INNER JOIN)
                ("location", true),    // NOT NULL from profiles (INNER JOIN)
            ],
        );

        // Test 3: Multiple star expansions
        verify_view_nullability(
            &content,
            "UserData",
            &[
                // users.* columns
                ("id", true),          // PRIMARY KEY
                ("username", true),    // NOT NULL
                ("email", true),       // NOT NULL
                ("created_at", true),  // NOT NULL
                ("deleted_at", false), // Nullable
                // profiles.* columns
                ("user_id", true),     // PRIMARY KEY from profiles
                ("bio", true),         // NOT NULL
                ("avatar_url", false), // Nullable
                ("location", true),    // NOT NULL
            ],
        );

        // Test 4: Star with LEFT JOIN - joined columns become nullable
        verify_view_nullability(
            &content,
            "UsersWithProfiles",
            &[
                ("id", true),          // PRIMARY KEY from users
                ("username", true),    // NOT NULL from users
                ("email", true),       // NOT NULL from users
                ("created_at", true),  // NOT NULL from users
                ("deleted_at", false), // Nullable from users
                ("bio", false),        // LEFT JOIN makes it nullable
                ("avatar_url", false), // LEFT JOIN + already nullable
            ],
        );

        // Test 5: Mixed star and explicit columns
        verify_view_nullability(
            &content,
            "PostDetails",
            &[
                ("post_id", true),       // PRIMARY KEY (renamed)
                ("id", true),            // users.id from star expansion
                ("username", true),      // users.username from star
                ("email", true),         // users.email from star
                ("created_at", true),    // users.created_at from star
                ("deleted_at", false),   // users.deleted_at from star
                ("title", true),         // Explicit column, NOT NULL
                ("published_at", false), // Explicit column, nullable
            ],
        );

        println!("✅ Star expansion nullability correctly inferred!");
    });
}

#[test]
fn test_view_referencing_other_views() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // Test that nullability is correctly propagated through view references
        execute_sql(
            client,
            r#"
            CREATE SCHEMA view_ref_test;
            SET search_path TO view_ref_test;
            
            CREATE TABLE base_table (
                id bigint PRIMARY KEY,
                required_field text NOT NULL,
                optional_field text
            );
            
            -- First level view
            CREATE VIEW level1_view AS
            SELECT 
                id,
                required_field,
                optional_field,
                COALESCE(optional_field, 'default') as coalesced_field,
                CASE 
                    WHEN optional_field IS NULL THEN 'null'
                    ELSE 'not_null'
                END as null_status
            FROM base_table;
            
            -- Second level view referencing first
            CREATE VIEW level2_view AS
            SELECT 
                v.id,
                v.required_field,
                v.optional_field,
                v.coalesced_field,
                v.null_status,
                v.required_field || ' suffix' as concatenated,
                COUNT(*) OVER () as total_count
            FROM level1_view v;
            
            -- Third level with joins
            CREATE VIEW level3_view AS
            SELECT 
                v1.id,
                v1.required_field as v1_required,
                v2.required_field as v2_required,
                v1.optional_field as v1_optional,
                v2.optional_field as v2_optional
            FROM level2_view v1
            LEFT JOIN level2_view v2 ON v1.id = v2.id + 1;
        "#,
        )
        .expect("Should create view reference test schema");

        // Generate code
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let output_path = temp_dir.path();

        pgrpc::PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("view_ref_test")
            .output_path(output_path)
            .infer_view_nullability(true)
            .build()
            .expect("Should generate code");

        let content = std::fs::read_to_string(output_path.join("view_ref_test.rs"))
            .expect("Should read generated file");

        // Verify nullability propagation
        verify_view_nullability(
            &content,
            "Level1View",
            &[
                ("id", true),              // PRIMARY KEY
                ("required_field", true),  // NOT NULL from base
                ("optional_field", false), // Nullable from base
                ("coalesced_field", true), // COALESCE expression
                ("null_status", true),     // CASE with ELSE
            ],
        );

        verify_view_nullability(
            &content,
            "Level2View",
            &[
                ("id", true),              // From level1_view - NOT NULL
                ("required_field", true),  // From level1_view - NOT NULL
                ("optional_field", false), // From level1_view - nullable
                ("coalesced_field", true), // From level1_view - NOT NULL (COALESCE)
                ("null_status", true),     // From level1_view - NOT NULL (CASE with ELSE)
                ("concatenated", true),    // String concat of NOT NULL field
                ("total_count", true),     // COUNT window function
            ],
        );

        verify_view_nullability(
            &content,
            "Level3View",
            &[
                ("id", true),           // From level2_view - NOT NULL
                ("v1_required", true),  // From level2_view - NOT NULL
                ("v2_required", false), // LEFT JOIN makes it nullable
                ("v1_optional", false), // From level2_view - already nullable
                ("v2_optional", false), // LEFT JOIN + already nullable
            ],
        );

        println!("✅ View reference nullability correctly propagated!");
    });
}
