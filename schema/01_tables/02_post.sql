-- create table post
-- (
--     post_id    serial primary key,
--     account_id int         not null references account,
--     title      text        not null,
--     content    text,
--     created_at timestamptz not null default now()
-- );
--
-- create view post_with_author as
-- (
-- select post.*, row (account.*)::account as author
-- from post
--          left join account using (account_id) );
--
-- comment on column post_with_author.author is '@pgrpc_not_null';

insert into account (account_id, email) values (1, 'zs391@cam.ac.uk')