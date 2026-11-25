-- Standalone composite types (`create type ... as (...)`) in one schema, with
-- their fields. Shared by the task queue (task payload types) and typed
-- errors (error payload types).
--
-- $1: schema name
--
-- The join on pg_class.relkind = 'c' keeps only standalone composites:
-- typtype = 'c' alone also matches the row type of every table and view in
-- the schema. The LEFT JOIN keeps a type with no fields (`as ()`).
SELECT
    t.typname::text AS name,
    t.oid AS type_oid,
    obj_description(t.oid, 'pg_type') AS comment,
    COALESCE(
        json_agg(
            json_build_object(
                'name', a.attname,
                'type_oid', a.atttypid::int8,
                'postgres_type', format_type(a.atttypid, a.atttypmod),
                'comment', col_description(t.typrelid, a.attnum)
            )
            ORDER BY a.attnum
        ) FILTER (WHERE a.attname IS NOT NULL),
        '[]'::json
    ) AS fields
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_class c ON c.oid = t.typrelid AND c.relkind = 'c'
LEFT JOIN pg_attribute a ON a.attrelid = t.typrelid
    AND a.attnum > 0          -- exclude system columns
    AND NOT a.attisdropped    -- exclude dropped columns
WHERE n.nspname = $1
  AND t.typtype = 'c'
GROUP BY t.oid, t.typname, t.typrelid
ORDER BY t.typname;
