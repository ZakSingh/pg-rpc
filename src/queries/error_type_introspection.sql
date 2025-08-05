-- Query to introspect error types from the errors schema
-- Returns composite types and their fields that represent custom error types
WITH error_types AS (
    SELECT 
        t.oid AS type_oid,
        t.typname AS error_name,
        json_agg(
            json_build_object(
                'name', a.attname,
                'type_oid', a.atttypid,
                'postgres_type', format_type(a.atttypid, a.atttypmod),
                'position', a.attnum,
                'not_null', a.attnotnull,
                'comment', col_description(t.typrelid, a.attnum)
            ) ORDER BY a.attnum
        ) AS fields
    FROM pg_type t
    JOIN pg_namespace n ON t.typnamespace = n.oid
    JOIN pg_class c ON t.typrelid = c.oid
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = $1
        AND t.typtype = 'c'  -- composite type
        AND a.attnum > 0      -- exclude system columns
        AND NOT a.attisdropped -- exclude dropped columns
    GROUP BY t.oid, t.typname
)
SELECT 
    error_name,
    type_oid::oid::int4 AS type_oid,
    COALESCE(fields, '[]'::json) AS fields
FROM error_types
ORDER BY error_name;