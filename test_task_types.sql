-- Check if tasks schema exists
SELECT nspname 
FROM pg_namespace 
WHERE nspname = 'tasks';

-- Check composite types in the tasks schema
SELECT t.typname, t.oid, t.typrelid
FROM pg_type t
WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tasks')
  AND t.typtype = 'c'
ORDER BY t.typname;

-- Check attributes of composite types with full details
SELECT 
    t.typname as composite_type,
    a.attname as field_name,
    a.atttypid as type_oid,
    format_type(a.atttypid, a.atttypmod) as postgres_type,
    a.attnum as position,
    a.attnotnull as not_null
FROM pg_type t
JOIN pg_attribute a ON a.attrelid = t.typrelid
WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tasks')
  AND t.typtype = 'c'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY t.typname, a.attnum;

-- Run the actual introspection query used by pgrpc
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
    SELECT oid FROM pg_namespace WHERE nspname = 'tasks'
)
  AND t.typtype = 'c'  -- composite types only
  AND a.attnum > 0
  AND NOT a.attisdropped
GROUP BY t.typname, t.oid
ORDER BY t.typname;