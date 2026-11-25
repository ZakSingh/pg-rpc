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
              and not a.attisdropped) as column_types,
           -- Partitions (and classic inheritance children): a write to the
           -- parent is routed to / also applies to these relations.
           (select array_agg(inh.inhrelid order by inh.inhrelid)
            from pg_inherits inh
            where inh.inhparent = c.oid) as child_oids
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
                          a.attname as root_column_name
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
                          cth.root_column_name
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
                                  'domain', t.typname::text,
                                  'domain_schema', (select nspname from pg_namespace where oid = t.typnamespace),
                                  'comment', obj_description(dc.oid, 'pg_constraint')
                          )::json as constraint_info
                   from composite_type_hierarchy cth
                            join pg_type t on t.oid = cth.element_type_oid
                            join pg_constraint dc on dc.contypid = t.oid
                   where cth.element_typtype = 'd'
                     -- Only CHECK constraints. A domain's NOT NULL is a
                     -- pg_constraint row too from PG 17 on, but its violation
                     -- is a not_null_violation with no constraint name.
                     and dc.contype = 'c'
               ),
               unique_indexes as (
                   select i.indrelid as oid,
                          json_build_object(
                                  'name', c.relname::text,
                                  'type', 'u',
                                  'comment', obj_description(c.oid, 'pg_class'),
                                  -- Plain key columns only: an expression column is a 0 in
                                  -- indkey and has no attribute to name.
                                  'columns', coalesce((
                                      select array_agg(a.attname order by array_position(i.indkey::int[], a.attnum::int))
                                      from pg_attribute a
                                      where a.attrelid = i.indrelid
                                        and a.attnum > 0
                                        and a.attnum = any(i.indkey)
                                  ), array[]::name[]),
                                  -- Partial and expression indexes: uniqueness depends on
                                  -- more than `columns` (the predicate's and expressions'
                                  -- columns), so any write to the table may violate them
                                  -- and equality on `columns` does not pin down one row.
                                  'conditional', (i.indexprs is not null or i.indpred is not null)
                          )::json as constraint_info
                   from pg_index i
                            join pg_class c on c.oid = i.indexrelid
                   where i.indisunique
                     and not i.indisprimary
                     -- Exclude indexes that back a constraint (already captured via pg_constraint)
                     and not exists (
                       select 1 from pg_constraint con
                       where con.conindid = i.indexrelid
                     )
                     and i.indrelid in (select ur.oid from user_relations ur)
               ),
               all_constraints as (
                   -- CHECK, PRIMARY KEY, UNIQUE and EXCLUSION constraints. Listed
                   -- explicitly: foreign keys and NOT NULL have their own branches
                   -- below (PG 18 stores table NOT NULL as contype 'n' rows, which
                   -- carry no `column` key here), triggers ('t') are not constraints
                   -- a statement violates by name.
                   select ur.oid,
                          json_build_object('name', con.conname::text, 'type', con.contype::text, 'columns',
                                            -- Empty (not null) when the constraint names no plain
                                            -- column: `check (false)`, an exclusion constraint over
                                            -- expressions only.
                                            coalesce((select array_agg(a.attname order by a.attnum)
                                             from unnest(con.conkey) k
                                                      join pg_attribute a on a.attrelid = ur.oid and a.attnum = k),
                                                     array[]::name[]),
                                            'check_expression', case when con.contype = 'c' then pg_get_constraintdef(con.oid) end,
                                            'comment', obj_description(con.oid, 'pg_constraint'))::json as constraint_info
                   from user_relations ur
                            join pg_constraint con on con.conrelid = ur.oid
                   where con.contype in ('c', 'p', 'u', 'x')

                   union all

                   -- Foreign key constraints
                   select ur.oid,
                          json_build_object('name', con.conname::text, 'type', con.contype::text, 'columns',
                                            (select array_agg(a.attname order by array_position(con.conkey::int[], a.attnum::int))
                                             from unnest(con.conkey) k
                                                      join pg_attribute a on a.attrelid = ur.oid and a.attnum = k),
                                            'on_delete', con.confdeltype,
                                            'on_update', con.confupdtype,
                                            'comment', obj_description(con.oid, 'pg_constraint'),
                                            'ref_table', ref_class.relname::text,
                                            'ref_table_oid', con.confrelid::bigint,
                                            'ref_columns', (select array_agg(a.attname order by array_position(con.confkey::int[], a.attnum::int))
                                                            from unnest(con.confkey) k
                                                                     join pg_attribute a on a.attrelid = con.confrelid and a.attnum = k))::json as constraint_info
                   from user_relations ur
                            join pg_constraint con on con.conrelid = ur.oid
                            join pg_class ref_class on ref_class.oid = con.confrelid
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

                   union all

                   -- Unique indexes (not backing constraints)
                   select * from unique_indexes
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
       ur.child_oids,
       coalesce(gc.constraints, array[]::json[]) as constraints
from user_relations ur
         left join grouped_constraints gc on gc.oid = ur.oid
order by ur.schema_name, ur.relation_name;