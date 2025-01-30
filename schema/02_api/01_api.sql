create schema api;

create function api.create_account(
    email citext,
    name text,
    role role
) returns account as
$$
declare
    account_res account;
begin
    with test_cte as ( select 1 )
    insert
    into account (email, name, role)
    values (email, name, role)
    returning * into account_res;

    return account_res;
end;
$$ language plpgsql;

create function api.create_post(
    account_id int,
    title text,
    content text default null
) returns post_with_author as
$$
declare
    ret post_with_author;
begin
    with new_post as ( insert into post (account_id, title, content) values (account_id, title, content) returning * )
    select new_post.*, row (a.*)::account as author
    into strict ret
    from new_post
             join account a on a.account_id = new_post.account_id;

    return ret;
end;
$$ language plpgsql;

create function api.deppy(un int) returns int as
$$
begin
    return un;
end;
$$ language plpgsql;

create function api.middle(un int) returns int as
$$
begin
    return api.deppy(un);
end;
$$ language plpgsql;

create function api.optional_argument(un int) returns int as
$$
begin
    return api.middle(un);
end;
$$ language plpgsql;

comment on function api.optional_argument(int) is 'fn comment';