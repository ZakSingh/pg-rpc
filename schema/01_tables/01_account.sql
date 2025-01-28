create type role as enum ('admin', 'user');
create table account
(
    account_id serial primary key,
    email      text        not null unique,
    name       text        not null,
    role       role        not null,
    created_at timestamptz not null default now()
);