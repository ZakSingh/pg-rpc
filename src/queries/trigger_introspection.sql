-- Introspect triggers on user relations and their associated functions
-- This query finds all triggers on relations in user schemas and gets details
-- about the trigger functions that could raise exceptions

with user_tables as (
    -- Every user relation that can carry a trigger: ordinary, partitioned and
    -- foreign tables, and views (INSTEAD OF triggers).
    select c.oid as table_oid,
           n.nspname as schema_name,
           c.relname as table_name
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
      and c.relkind in ('r', 'p', 'f', 'v')
),
trigger_details as (
    -- Get trigger information with the events decoded from tgtype
    -- (bit 4 INSERT, 8 DELETE, 16 UPDATE). Timing and level (bit 1 ROW, 2
    -- BEFORE, 64 INSTEAD OF) are not decoded: a trigger of any timing or level
    -- can fail the statement that fires it. TRUNCATE triggers (bit 32) are not
    -- reported: TRUNCATE is not analyzed.
    select t.oid as trigger_oid,
           t.tgname as trigger_name,
           t.tgrelid as table_oid,
           t.tgfoid as function_oid,
           array_remove(array[
               case when (t.tgtype & 4) != 0 then 'INSERT' end,
               case when (t.tgtype & 8) != 0 then 'DELETE' end,
               case when (t.tgtype & 16) != 0 then 'UPDATE' end
           ], null) as events,
           t.tgenabled as is_enabled
    from pg_trigger t
    where not t.tgisinternal  -- exclude internal constraint triggers
),
trigger_functions as (
    -- Get function details for trigger functions
    select p.oid as function_oid,
           p.proname as function_name,
           p.prosrc as function_source,
           n.nspname as function_schema,
           obj_description(p.oid, 'pg_proc') as function_comment
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where p.prorettype = (select oid from pg_type where typname = 'trigger')
)
select ut.schema_name,
       ut.table_name,
       ut.table_oid,
       td.trigger_name,
       td.trigger_oid,
       td.events,
       td.is_enabled,
       tf.function_oid,
       tf.function_name,
       tf.function_schema,
       tf.function_source,
       tf.function_comment
from user_tables ut
join trigger_details td on td.table_oid = ut.table_oid
join trigger_functions tf on tf.function_oid = td.function_oid
where td.is_enabled != 'D'  -- exclude disabled triggers
order by ut.schema_name, ut.table_name, td.trigger_name;