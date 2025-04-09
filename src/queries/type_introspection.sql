with recursive type_tree as (
    -- base case: start with input oids
    select t.oid,
           t.typname,
           t.typtype,
           n.nspname                                                                                              as schema_name,
           t.typelem                                                                                              as array_element_type,
           t.typbasetype                                                                                          as domain_base_type,
           case when t.typtype = 'd' then ( select array_agg(pg_get_constraintdef(c.oid))
                                            from pg_constraint c
                                            where c.contypid = t.oid ) end                                        as domain_composite_constraints,
           case when t.typtype = 'd' then ( select array_agg(c.conname)
                                            from pg_constraint c
                                            where c.contypid = t.oid ) end                                        as domain_constraint_names,
           case when t.typtype = 'e' then ( select array_agg(enumlabel order by enumsortorder)
                                            from pg_enum
                                            where enumtypid = t.oid ) end                                         as enum_variants,
           case when t.typtype = 'c' then ( select array_agg(attname order by attnum)
                                            from pg_attribute
                                            where attrelid = t.typrelid
                                              and attnum > 0
                                              and not attisdropped ) end                                          as composite_field_names,
           case when t.typtype = 'c' then ( select array_agg(atttypid order by attnum)
                                            from pg_attribute
                                            where attrelid = t.typrelid
                                              and attnum > 0
                                              and not attisdropped ) end                                          as composite_field_types,
           case when t.typtype = 'c'
                    then ( select array_agg(not attnotnull order by attnum)
                                      from pg_attribute
                                      where attrelid = t.typrelid
                                        and attnum > 0
                                        and not attisdropped )  end                                           as composite_field_nullables,
           -- Get type comments with different behavior for each type
           case when t.typtype = 'd' then (
               -- Domains: Only get direct comments, never inherit
               select description from pg_description where objoid = t.oid and objsubid = 0 )
                when t.typtype = 'c' then (
                    -- Composite types: Get direct comment or inherit from table/view
                    select description
                    from pg_description
                    where objoid in (t.oid, t.typrelid) and objsubid = 0
                    limit 1 )
                else (
                    -- Other types: Get direct comments
                    select description
                    from pg_description
                    where objoid = t.oid
                      and objsubid = 0 ) end                                                                      as type_comment,
           -- Get comments on the composite type columns
           case when t.typtype = 'c' then ( select array_agg(d.description order by a.attnum)
                                            from pg_attribute a
                                                     left join pg_description d on d.objoid = t.typrelid and d.objsubid = a.attnum
                                            where a.attrelid = t.typrelid
                                              and a.attnum > 0
                                              and not attisdropped ) end                                          as composite_field_comments
    from pg_type t
             join pg_namespace n on t.typnamespace = n.oid
    where t.oid = any ($1)

    union all

    -- recursive case
    select t.oid,
           t.typname,
           t.typtype,
           n.nspname                                                                                              as schema_name,
           t.typelem                                                                                              as array_element_type,
           t.typbasetype                                                                                          as domain_base_type,
           case when t.typtype = 'd' then ( select array_agg(pg_get_constraintdef(c.oid))
                                            from pg_constraint c
                                            where c.contypid = t.oid ) end                                        as domain_composite_constraints,
           case when t.typtype = 'd' then ( select array_agg(c.conname)
                                            from pg_constraint c
                                            where c.contypid = t.oid ) end                                        as domain_constraint_names,
           case when t.typtype = 'e' then ( select array_agg(enumlabel order by enumsortorder)
                                            from pg_enum
                                            where enumtypid = t.oid ) end                                         as enum_variants,
           case when t.typtype = 'c' then ( select array_agg(attname order by attnum)
                                            from pg_attribute
                                            where attrelid = t.typrelid
                                              and attnum > 0
                                              and not attisdropped ) end                                          as composite_field_names,
           case when t.typtype = 'c' then ( select array_agg(atttypid order by attnum)
                                            from pg_attribute
                                            where attrelid = t.typrelid
                                              and attnum > 0
                                              and not attisdropped ) end                                          as composite_field_types,
           case when t.typtype = 'c'
                    then (select array_agg(not attnotnull order by attnum)
                                     from pg_attribute
                                     where attrelid = t.typrelid
                                       and attnum > 0
                                       and not attisdropped) end                                                  as composite_field_nullables,
           -- Get type comments with different behavior for each type
           case when t.typtype = 'd' then (
               -- Domains: Only get direct comments, never inherit
               select description from pg_description where objoid = t.oid and objsubid = 0 )
                when t.typtype = 'c' then (
                    -- Composite types: Get direct comment or inherit from table/view
                    select description
                    from pg_description
                    where objoid in (t.oid, t.typrelid) and objsubid = 0
                    limit 1 )
                else (
                    -- Other types: Get direct comments
                    select description
                    from pg_description
                    where objoid = t.oid
                      and objsubid = 0 ) end                                                                      as type_comment,
           -- Get comments on the composite type columns
           case when t.typtype = 'c' then ( select array_agg(d.description order by a.attnum)
                                            from pg_attribute a
                                                     left join pg_description d on d.objoid = t.typrelid and d.objsubid = a.attnum
                                            where a.attrelid = t.typrelid
                                              and a.attnum > 0
                                              and not attisdropped ) end                                          as composite_field_comments
    from pg_type t
             join pg_namespace n on t.typnamespace = n.oid
             join type_tree tt on (
        -- array elements
        t.oid = ( select typelem from pg_type where oid = tt.oid ) or
            -- domain base types
        t.oid = ( select typbasetype from pg_type where oid = tt.oid ) or
            -- composite type fields
        (tt.typtype = 'c' and t.oid in ( select atttypid
                                         from pg_attribute
                                         where attrelid = ( select typrelid from pg_type where oid = tt.oid )
                                           and attnum > 0 ))) )
select distinct on (oid) oid,
                         typname,
                         typtype,
                         schema_name,
                         array_element_type,
                         domain_base_type,
                         domain_composite_constraints,
                         domain_constraint_names,
                         enum_variants,
                         composite_field_names,
                         composite_field_types,
                         composite_field_nullables,
                         type_comment,
                         composite_field_comments
from type_tree;