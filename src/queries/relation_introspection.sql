with recursive user_relations as (
    select c.oid,
           c.relkind::text as kind,
           case when c.relkind in ('v', 'm') then pg_get_viewdef(c.oid) end as view_definition,
           n.nspname as schema_name,
           c.relname as relation_name,
           (select array_agg(a.attname order by a.attnum)
            from pg_attribute a
            where a.attrelid = c.oid
              and a.attnum > 0
              and not a.attisdropped) as column_names,
           (select array_agg(a.atttypid order by a.attnum)
            from pg_attribute a
            where a.attrelid = c.oid
              and a.attnum > 0
              and not a.attisdropped) as column_types
    from pg_class c
             join pg_namespace n on n.oid = c.relnamespace
    where n.nspname not in ('pg_catalog', 'information_schema')
      and c.relkind in ('r', 'v', 'm', 'f', 'p')
),
               composite_type_hierarchy as (
                   -- Base case: direct column types
                   select a.attrelid as rel_oid,
                          a.attname as column_name,
                          t.oid as type_oid,
                          t.typname as type_name,
                          t.typtype,
                          coalesce(et.oid, t.oid) as element_type_oid,
                          coalesce(et.typname, t.typname) as element_type_name,
                          coalesce(et.typtype, t.typtype) as element_typtype,
                          1 as level,
                          a.attname as root_column_name,
                          t.typelem != 0 as is_array
                   from pg_attribute a
                            join pg_type t on t.oid = a.atttypid
                            left join pg_type et on et.oid = t.typelem
                   where a.attnum > 0
                     and not a.attisdropped

                   union all

                   -- Recursive case: composite type fields
                   select cth.rel_oid,
                          cth.column_name,
                          t.oid as type_oid,
                          t.typname as type_name,
                          t.typtype,
                          coalesce(et.oid, t.oid) as element_type_oid,
                          coalesce(et.typname, t.typname) as element_type_name,
                          coalesce(et.typtype, t.typtype) as element_typtype,
                          cth.level + 1,
                          cth.root_column_name,
                          t.typelem != 0 as is_array
                   from composite_type_hierarchy cth
                            join pg_type ct on ct.oid = cth.element_type_oid
                            join pg_attribute a on a.attrelid = ct.typrelid
                            join pg_type t on t.oid = a.atttypid
                            left join pg_type et on et.oid = t.typelem
                   where ct.typtype = 'c'
                     and a.attnum > 0
                     and not a.attisdropped
               ),
               domain_constraints as (
                   select cth.rel_oid as oid,
                          json_build_object(
                                  'name', dc.conname::text,
                                  'type', 'domain',
                                  'column', cth.root_column_name,
                                  'is_array', cth.is_array,
                                  'check_clause', pg_get_constraintdef(dc.oid)
                          )::json as constraint_info
                   from composite_type_hierarchy cth
                            join pg_type t on t.oid = cth.element_type_oid
                            join pg_constraint dc on dc.contypid = t.oid
                   where cth.element_typtype = 'd'
               ),
               all_constraints as (
                   -- Regular constraints (non-foreign keys)
                   select ur.oid,
                          json_build_object('name', con.conname::text, 'type', con.contype::text, 'columns',
                                            (select array_agg(a.attname order by a.attnum)
                                             from unnest(con.conkey) k
                                                      join pg_attribute a on a.attrelid = ur.oid and a.attnum = k))::json as constraint_info
                   from user_relations ur
                            join pg_constraint con on con.conrelid = ur.oid
                   where con.contype not in ('f', 't')

                   union all

                   -- Foreign key constraints
                   select ur.oid,
                          json_build_object('name', con.conname::text, 'type', con.contype::text, 'columns',
                                            (select array_agg(a.attname order by a.attnum)
                                             from unnest(con.conkey) k
                                                      join pg_attribute a on a.attrelid = ur.oid and a.attnum = k), 'on_delete',
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
                     and not a.attisdropped

                   union all

                   -- Add domain constraints
                   select * from domain_constraints
               ),
               grouped_constraints as (
                   select oid, array_agg(constraint_info) as constraints
                   from all_constraints
                   group by oid
               )
select ur.relation_name as name,
       ur.schema_name as schema,
       ur.oid,
       ur.kind,
       ur.view_definition,
       ur.column_names,
       ur.column_types,
       coalesce(gc.constraints, array[]::json[]) as constraints
from user_relations ur
         left join grouped_constraints gc on gc.oid = ur.oid
order by ur.schema_name, ur.relation_name;