-- Introspect composite types in task schema for task queue enum generation
SELECT 
    t.typname as task_name,
    t.oid as type_oid,
    json_agg(
        json_build_object(
            'name', a.attname,
            'type_oid', a.atttypid,
            'postgres_type', format_type(a.atttypid, a.atttypmod),
            'position', a.attnum,
            'not_null', a.attnotnull,
            'comment', d.description
        ) ORDER BY a.attnum
    ) as fields
FROM pg_type t
JOIN pg_attribute a ON a.attrelid = t.typrelid  
LEFT JOIN pg_description d ON d.objoid = t.typrelid AND d.objsubid = a.attnum
WHERE t.typnamespace = (
    SELECT oid FROM pg_namespace WHERE nspname = $1  -- task schema name
)
  AND t.typtype = 'c'  -- composite types only
  AND a.attnum > 0
  AND NOT a.attisdropped
GROUP BY t.typname, t.oid
ORDER BY t.typname;