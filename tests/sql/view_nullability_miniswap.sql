-- Test SQL based on miniswap patterns for view nullability inference
-- This file contains views WITHOUT @pgrpc_not_null annotations
-- The test will verify that nullability is correctly inferred

-- Create test schema
CREATE SCHEMA IF NOT EXISTS miniswap_test;
SET search_path TO miniswap_test, public;

-- Currency type (composite type)
CREATE TYPE currency AS (
    amount integer,
    currency_code text
);

-- Base tables
CREATE TABLE account (
    account_id bigint PRIMARY KEY,
    account_nanoid text NOT NULL,
    display_name text NOT NULL,
    bio text,
    country_code text NOT NULL,
    currency_code text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE seller (
    account_id bigint PRIMARY KEY REFERENCES account(account_id),
    dba_name text NOT NULL,
    stripe_charges_enabled boolean NOT NULL DEFAULT false,
    away_until date,
    free_shipping_threshold currency
);

CREATE TABLE seller_stats (
    account_id bigint PRIMARY KEY REFERENCES account(account_id),
    sales_count integer NOT NULL DEFAULT 0,
    positive_feedback_pct numeric
);

CREATE TABLE product (
    product_id bigint PRIMARY KEY,
    product_type text NOT NULL,
    name text NOT NULL,
    slug text NOT NULL UNIQUE,
    sku text NOT NULL,
    description text,
    official_url text,
    msrp currency,
    release_date date,
    is_discontinued boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE category (
    category_id integer PRIMARY KEY,
    name text NOT NULL,
    path text NOT NULL
);

CREATE TABLE product_category (
    product_id bigint REFERENCES product(product_id),
    category_id integer REFERENCES category(category_id),
    is_primary boolean NOT NULL DEFAULT false,
    PRIMARY KEY (product_id, category_id)
);

CREATE TABLE item (
    item_id bigint PRIMARY KEY,
    item_nanoid text NOT NULL,
    account_id bigint NOT NULL REFERENCES account(account_id),
    product_id bigint NOT NULL REFERENCES product(product_id),
    description text,
    is_archived boolean NOT NULL DEFAULT false,
    country_code text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE listing (
    item_id bigint PRIMARY KEY REFERENCES item(item_id),
    price currency NOT NULL,
    quantity_available integer NOT NULL DEFAULT 0,
    is_enabled boolean NOT NULL DEFAULT true
);

CREATE TABLE cart_listing (
    account_id bigint REFERENCES account(account_id),
    item_id bigint REFERENCES item(item_id),
    quantity integer NOT NULL DEFAULT 1,
    selected_for_checkout boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, item_id)
);

CREATE TABLE wishlist (
    account_id bigint REFERENCES account(account_id),
    item_id bigint REFERENCES item(item_id),
    PRIMARY KEY (account_id, item_id)
);

CREATE TABLE checkout (
    checkout_id integer PRIMARY KEY,
    account_id bigint NOT NULL REFERENCES account(account_id),
    delivery_contact_id integer
);

CREATE TABLE transaction (
    checkout_id integer REFERENCES checkout(checkout_id),
    seller_id bigint REFERENCES account(account_id),
    status text NOT NULL,
    payment_intent_id text,
    platform_payment_method_id text,
    item_subtotal currency NOT NULL,
    shipping_subtotal currency NOT NULL,
    actual_shipping_cost currency,
    seller_fee currency NOT NULL,
    stripe_fee currency NOT NULL,
    seller_net currency NOT NULL,
    available_at date,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (checkout_id, seller_id)
);

-- Simple views with various nullability patterns

-- View 1: cart_summary - Tests COALESCE, COUNT, casting
CREATE VIEW cart_summary AS
SELECT 
    a.account_id,
    COALESCE(SUM(cl.quantity), 0)::int as item_count,
    COALESCE(SUM(cl.quantity) FILTER (WHERE cl.selected_for_checkout), 0)::int as selected_item_count,
    COALESCE(
        SUM((l.price).amount * cl.quantity) FILTER (WHERE cl.selected_for_checkout),
        0
    ) as subtotal_amount,
    a.currency_code as subtotal_currency
FROM account a
LEFT JOIN cart_listing cl ON cl.account_id = a.account_id
LEFT JOIN listing l ON l.item_id = cl.item_id
GROUP BY a.account_id, a.currency_code;

-- View 2: account_summary - Tests LEFT JOIN nullability, CASE expressions
CREATE VIEW account_summary AS
SELECT 
    a.account_id,
    a.account_nanoid,
    a.country_code,
    a.currency_code,
    COALESCE(s.dba_name, a.display_name) as name,
    s.free_shipping_threshold,
    COALESCE(ss.sales_count, 0) as sales_count,
    ss.positive_feedback_pct,
    CASE 
        WHEN s.account_id IS NOT NULL THEN true
        ELSE false
    END as is_seller,
    s.stripe_charges_enabled,
    s.away_until
FROM account a
LEFT JOIN seller s USING (account_id)
LEFT JOIN seller_stats ss USING (account_id);

-- View 3: product_summary - Tests array access, subqueries, NOT NULL columns
CREATE VIEW product_summary AS
SELECT 
    p.product_id,
    p.product_type,
    p.name,
    p.slug,
    p.sku,
    p.release_date,
    p.msrp,
    p.is_discontinued,
    c.category_id,
    c.name as category_name,
    -- Subquery with COUNT (always returns a value)
    (
        SELECT COUNT(*)
        FROM item i
        JOIN listing l USING (item_id)
        WHERE i.product_id = p.product_id
          AND NOT i.is_archived
          AND l.is_enabled = true
          AND l.quantity_available > 0
    )::int as listing_count,
    p.created_at,
    p.updated_at
FROM product p
JOIN product_category pc ON pc.product_id = p.product_id AND pc.is_primary = true
JOIN category c ON c.category_id = pc.category_id;

-- Composite types for complex views
CREATE TYPE account_summary_type AS (
    account_id bigint,
    account_nanoid text,
    name text
);

CREATE TYPE cart_item AS (
    cart_account_id bigint,
    item_id bigint,
    selected_for_checkout boolean,
    quantity_requested integer,
    added_at timestamptz
);

CREATE TYPE cart_seller AS (
    seller account_summary_type,
    items cart_item[]
);

-- View 4: cart_item_view - Tests view used in composite type
CREATE VIEW cart_item_view AS
SELECT 
    cl.account_id as cart_account_id,
    cl.item_id,
    cl.selected_for_checkout,
    cl.quantity as quantity_requested,
    cl.created_at as added_at
FROM cart_listing cl;

-- View 5: cart_details - Tests array aggregation, lateral joins, complex composite types
CREATE VIEW cart_details AS
SELECT 
    cs.account_id,
    cs.item_count,
    cs.selected_item_count,
    cs.subtotal_amount,
    cs.subtotal_currency,
    COALESCE(
        sellers.items,
        ARRAY[]::cart_seller[]
    ) as sellers
FROM cart_summary cs
LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(
        ROW(
            seller_info,
            seller_items
        )::cart_seller
    ) as items
    FROM (
        SELECT 
            ROW(s.account_id, s.account_nanoid, s.name)::account_summary_type as seller_info,
            ARRAY_AGG(
                ROW(
                    ci.cart_account_id,
                    ci.item_id,
                    ci.selected_for_checkout,
                    ci.quantity_requested,
                    ci.added_at
                )::cart_item
            ) as seller_items
        FROM cart_item_view ci
        JOIN item i ON i.item_id = ci.item_id
        JOIN account_summary s ON s.account_id = i.account_id
        WHERE ci.cart_account_id = cs.account_id
        GROUP BY s.account_id, s.account_nanoid, s.name
    ) grouped
) sellers ON true;

-- View 6: transaction_summary - Tests complex CASE, EXISTS, aggregates, MAX
CREATE VIEW transaction_summary AS
SELECT 
    t.checkout_id,
    t.seller_id,
    t.status,
    t.payment_intent_id,
    t.item_subtotal,
    t.shipping_subtotal,
    t.seller_fee,
    t.stripe_fee,
    t.seller_net,
    -- Calculated field
    (t.item_subtotal).amount + (t.shipping_subtotal).amount as total_amount,
    (t.item_subtotal).currency_code as total_currency,
    -- Complex CASE expression
    CASE
        WHEN EXISTS (
            SELECT 1 FROM transaction t2 
            WHERE t2.checkout_id = t.checkout_id 
              AND t2.status = 'delivered'
        ) THEN 'delivered'
        WHEN EXISTS (
            SELECT 1 FROM transaction t2 
            WHERE t2.checkout_id = t.checkout_id 
              AND t2.status = 'shipped'
        ) THEN 'in_transit'
        ELSE 'pending'
    END as shipping_status,
    -- Array aggregation
    ARRAY_AGG(DISTINCT p.name ORDER BY p.name) as product_names,
    t.created_at,
    t.updated_at
FROM transaction t
JOIN checkout c ON c.checkout_id = t.checkout_id
JOIN account a ON a.account_id = t.seller_id
JOIN account ba ON ba.account_id = c.account_id
LEFT JOIN item i ON i.account_id = t.seller_id
LEFT JOIN product p ON p.product_id = i.product_id
GROUP BY 
    t.checkout_id, t.seller_id, t.status, t.payment_intent_id,
    t.item_subtotal, t.shipping_subtotal, t.seller_fee,
    t.stripe_fee, t.seller_net, t.created_at, t.updated_at;

-- View 7: item_details - Tests multiple LEFT JOINs, boolean literals, auth functions
CREATE VIEW item_details AS
SELECT 
    i.item_id,
    i.item_nanoid,
    i.account_id,
    i.product_id,
    i.description,
    i.is_archived,
    i.country_code,
    l.price as listing_price,
    p.name as product_name,
    p.product_type,
    o.name as owner_name,
    -- CASE with multiple conditions
    CASE
        WHEN i.is_archived THEN 'item_archived'
        WHEN l.item_id IS NULL OR NOT l.is_enabled THEN 'not_listed'
        WHEN l.quantity_available = 0 THEN 'out_of_stock'
        ELSE 'available'
    END as listing_status,
    -- Boolean expressions
    CASE
        WHEN i.is_archived THEN false
        WHEN l.item_id IS NULL OR NOT l.is_enabled THEN false
        WHEN l.quantity_available = 0 THEN false
        ELSE true
    END as is_purchasable,
    -- Simulate auth context (would be NULL in real scenario)
    CASE 
        WHEN 1 = 2 THEN true  -- Never true, simulates auth check
        ELSE false
    END as in_wishlist,
    false as in_cart,  -- Direct boolean literal
    i.created_at,
    i.updated_at
FROM item i
JOIN product p USING (product_id)
JOIN account_summary o ON o.account_id = i.account_id
LEFT JOIN listing l USING (item_id)
LEFT JOIN wishlist w ON w.item_id = i.item_id AND w.account_id = NULL
LEFT JOIN cart_listing cl ON cl.item_id = i.item_id AND cl.account_id = NULL;

-- View 8: Test edge cases
CREATE VIEW nullability_edge_cases AS
SELECT 
    -- String literals
    'constant_string' as always_string,
    -- Number literals
    42 as always_number,
    -- Boolean literals
    true as always_true,
    false as always_false,
    -- NULL literal
    NULL::text as always_null,
    -- CASE without ELSE
    CASE 
        WHEN false THEN 'never'
    END as maybe_null_case,
    -- CASE with ELSE
    CASE 
        WHEN false THEN 'never'
        ELSE 'default'
    END as never_null_case,
    -- Array constructor
    ARRAY[1, 2, 3] as int_array,
    ARRAY[]::text[] as empty_array,
    -- COALESCE with NULL
    COALESCE(NULL::text, NULL::text, 'fallback') as coalesce_result,
    -- Math operations
    1 + 1 as math_result,
    -- String concatenation
    'hello' || ' ' || 'world' as concat_result
FROM (SELECT 1) dummy;