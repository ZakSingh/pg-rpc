with user_relations as ( select c.oid,
                                n.nspname                    as schema_name,
                                c.relname                    as relation_name,
                                ( select array_agg(a.attname order by a.attnum)
                                  from pg_attribute a
                                  where a.attrelid = c.oid
                                    and a.attnum > 0
                                    and not a.attisdropped ) as column_names
                         from pg_class c
                                  join pg_namespace n on n.oid = c.relnamespace
                         where n.nspname not in ('pg_catalog', 'information_schema')
                           and c.relkind in ('r', 'v', 'm', 'f', 'p') ),
     all_constraints as (
         -- Regular constraints (non-foreign keys)
         select ur.oid,
                json_build_object('name', con.conname::text, 'type', con.contype::text, 'columns',
                                  ( select array_agg(a.attname order by a.attnum)
                                    from unnest(con.conkey) k
                                             join pg_attribute a on a.attrelid = ur.oid and a.attnum = k ))::json as constraint_info
         from user_relations ur
                  join pg_constraint con on con.conrelid = ur.oid
         where con.contype not in ('f') -- exclude foreign keys

         union all

         -- Foreign key constraints (handled separately to include ON DELETE)
         select ur.oid,
                json_build_object('name', con.conname::text, 'type', con.contype::text, 'columns',
                                  ( select array_agg(a.attname order by a.attnum)
                                    from unnest(con.conkey) k
                                             join pg_attribute a on a.attrelid = ur.oid and a.attnum = k ), 'on_delete',
                                  con.confdeltype)::json as constraint_info
         from user_relations ur
                  join pg_constraint con on con.conrelid = ur.oid
         where con.contype = 'f'

         union all

         -- NOT NULL constraints
         select ur.oid,
                json_build_object('name', a.attname || '_not_null', 'type', 'n', 'column',
                                  a.attname)::json as constraint_info
         from user_relations ur
                  join pg_attribute a on a.attrelid = ur.oid
         where a.attnotnull
           and a.attnum > 0
           and not a.attisdropped

         union all

         -- DEFAULT constraints
         select ur.oid,
                json_build_object('name', a.attname || '_default', 'type', 'd', 'column',
                                  a.attname)::json as constraint_info
         from user_relations ur
                  join pg_attribute a on a.attrelid = ur.oid
                  join pg_attrdef def on def.adrelid = ur.oid and def.adnum = a.attnum
         where a.attnum > 0
           and not a.attisdropped ),
     grouped_constraints as ( select oid, array_agg(constraint_info) as constraints from all_constraints group by oid )
select ur.relation_name                           as name,
       ur.schema_name                             as schema,
       ur.oid,
       ur.column_names,
       coalesce(gc.constraints, array []::json[]) as constraints
from user_relations ur
         left join grouped_constraints gc on gc.oid = ur.oid
order by ur.schema_name, ur.relation_name;