-- One row per (schema, name). Overloads are not supported: the one with the
-- fewest arguments is kept, ties broken by the text of the argument list, so
-- the choice does not depend on OIDs or on catalog order. `overload_count`
-- says how many there were.
select distinct on (n.nspname, p.proname)
       n.nspname                                                     as schema_name,
       p.proname                                                     as function_name,
       p.oid                                                         as oid,
       pg_get_functiondef(p.oid)                                     as function_definition,
       -- Input arguments only (IN, INOUT, VARIADIC).
       p.proargtypes                                                 as arg_oids,
       -- Their type names, to cast the arguments of a call to an overloaded
       -- function so that it resolves to this overload.
       array(select format_type(a.type_oid, null)
             from unnest(p.proargtypes::oid[]) with ordinality as a(type_oid, position)
             order by a.position)                                    as arg_type_names,
       p.prorettype                                                  as return_type,
       p.proretset                                                   as returns_set,
       p.proisstrict                                                 as is_strict,
       -- Per input argument: the trailing `pronargdefaults` have a default.
       array(select i > (p.pronargs - p.pronargdefaults)
             from generate_series(1, p.pronargs) as i
             order by i)                                             as has_defaults,
       d.description                                                 as comment,
       l.lanname                                                     as language,
       -- All arguments, when any is not IN; NULL otherwise.
       p.proargmodes                                                 as arg_modes,
       p.proallargtypes                                              as all_arg_types,
       -- Parallel to `all_arg_types` (or to `arg_oids` when that is NULL),
       -- '' for an unnamed argument; NULL when no argument is named.
       p.proargnames                                                 as all_arg_names,
       p.prokind                                                     as prokind,
       pg_get_function_identity_arguments(p.oid)                     as identity_args,
       count(*) over (partition by n.nspname, p.proname)             as overload_count
from pg_proc p
         join pg_namespace n on p.pronamespace = n.oid
         join pg_catalog.pg_language l on p.prolang = l.oid
         left join pg_description d
                   on d.objoid = p.oid
                       and d.classoid = 'pg_proc'::regclass
                       and d.objsubid = 0
where p.prokind in ('f', 'p')  -- Include both functions and procedures
  and l.lanname in ('plpgsql', 'sql')
  -- Every user schema is indexed so that calls from a generated function into a
  -- helper elsewhere (e.g. `core.*`) can be followed. Which schemas get Rust
  -- output is decided later by `FunctionIndex::codegen_fns`.
  and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
  and not exists ( select 1 from pg_depend dep where dep.objid = p.oid and dep.classid = 'pg_proc'::regclass and dep.deptype = 'e' )
  and not exists (select 1 from pg_trigger where tgfoid = p.oid)  -- No triggers
order by n.nspname, p.proname, p.pronargs, pg_get_function_identity_arguments(p.oid), p.oid;
