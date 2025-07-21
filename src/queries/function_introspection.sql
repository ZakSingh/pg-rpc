select distinct on (n.nspname, p.proname) n.nspname                                                           as schema_name,
                                          p.proname                                                           as function_name,
                                          p.oid                                                               as oid,
                                          pg_get_functiondef(p.oid)                                           as function_definition,
                                          p.proargtypes                                                       as arg_oids,
                                          p.prorettype                                                        as return_type,
                                          p.proretset                                                         as returns_set,
                                          case when p.proargnames is not null
                                                   then ( select p.proargnames[:array_length(p.proargtypes, 1)] )
                                               else array []::text[] end                                      as arg_names,
                                          array(select n > (array_length(p.proargtypes, 1) - p.pronargdefaults)
                                                from generate_series(1, array_length(p.proargtypes, 1)) as n) as has_defaults,
                                          d.description                                                       as comment,
                                          l.lanname                                                           as language,
                                          '[]'::json                                                       as dependencies,
                                          p.proargmodes                                                       as arg_modes,
                                          p.proallargtypes                                                    as all_arg_types,
                                          p.proargnames                                                       as all_arg_names
from pg_proc p
         join pg_namespace n on p.pronamespace = n.oid
         join pg_catalog.pg_language l on p.prolang = l.oid
         left join pg_type t on t.oid = any (p.proargtypes)
         left join pg_description d on d.objoid = p.oid
where p.prokind = 'f'
  and l.lanname in ('plpgsql', 'sql')
  and n.nspname  = any($1)  --not in ('pg_catalog', 'information_schema')
  and not exists ( select 1 from pg_depend d where d.objid = p.oid and d.deptype = 'e' )
  and not exists (select 1 from pg_trigger where tgfoid = p.oid)  -- No triggers
order by n.nspname, p.proname;