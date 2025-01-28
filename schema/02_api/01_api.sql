create schema api;

create function api.create_account(
    email citext,
    name text,
    role role,
    unused int
) returns account
    stable as
$$
declare
    account_res account;
begin
    -- comment s
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
    post_res post;
begin
    insert into post (account_id, title, content)
    values (account_id, title, content)
    returning (post.*, ( select * from account where account.account_id = post.account_id )) into post_res;

    return post_res;
end;
$$ language plpgsql;

create function api.optional_argument(required int, opt_1 int, opt_2 bool) returns post_with_author as
$$
begin
    return 3;
end;
$$ language plpgsql;

comment on function api.optional_argument(int, int, bool) is 'fn comment';