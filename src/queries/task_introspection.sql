-- Introspect composite types in task schema for task queue enum generation
-- First, get all composite types, then LEFT JOIN to get their fields
WITH task_types AS (
    SELECT 
        t.typname as task_name,
        t.oid as type_oid,
        t.typrelid
    FROM pg_type t
    WHERE t.typnamespace = (
        SELECT oid FROM pg_namespace WHERE nspname = $1  -- task schema name
    )
      AND t.typtype = 'c'  -- composite types only
)
SELECT 
    tt.task_name,
    tt.type_oid,
    COALESCE(
        json_agg(
            CASE 
                WHEN a.attname IS NOT NULL THEN
                    json_build_object(
                        'name', a.attname,
                        'type_oid', a.atttypid,
                        'postgres_type', format_type(a.atttypid, a.atttypmod),
                        'position', a.attnum,
                        'not_null', a.attnotnull,
                        'comment', d.description
                    )
                ELSE NULL
            END
            ORDER BY a.attnum
        ) FILTER (WHERE a.attname IS NOT NULL),
        '[]'::json
    ) as fields
FROM task_types tt
LEFT JOIN pg_attribute a ON a.attrelid = tt.typrelid 
    AND a.attnum > 0
    AND NOT a.attisdropped
LEFT JOIN pg_description d ON d.objoid = tt.typrelid AND d.objsubid = a.attnum
GROUP BY tt.task_name, tt.type_oid
ORDER BY tt.task_name;