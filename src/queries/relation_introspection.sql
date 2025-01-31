with user_relations as ( select c.oid, n.nspname as schema_name, c.relname as relation_name
                         from pg_class c
                                  join pg_namespace n on n.oid = c.relnamespace
                         where n.nspname not in ('pg_catalog', 'information_schema')
                           and c.relkind in ('r', 'v', 'm', 'f', 'p') ),
     notnull_constraints as ( select ur.oid,
                                     array_agg(a.attname || '_not_null'::text) as nn_names,
                                     array_agg('n'::text)                      as nn_types
                              from user_relations ur
                                       join pg_attribute a on a.attrelid = ur.oid
                              where a.attnotnull
                                and a.attnum > 0
                                and not a.attisdropped
                              group by ur.oid ),
     regular_constraints as ( select ur.oid,
                                     array_remove(array_agg(con.conname::text), null) as r_names,
                                     array_remove(array_agg(con.contype::text), null) as r_types
                              from user_relations ur
                                       left join pg_constraint con on con.conrelid = ur.oid
                              group by ur.oid )
select ur.relation_name                                                            as name,
       ur.schema_name                                                              as schema,
       ur.oid,
       coalesce(r_names, array []::text[]) || coalesce(nn_names, array []::text[]) as constraint_names,
       coalesce(r_types, array []::text[]) || coalesce(nn_types, array []::text[]) as constraint_types
from user_relations ur
         left join regular_constraints rc on rc.oid = ur.oid
         left join notnull_constraints nc on nc.oid = ur.oid
order by ur.schema_name, ur.relation_name;