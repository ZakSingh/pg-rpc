create type role as enum ('admin', 'user');
create table account
(
    account_id serial primary key,
    email      citext      not null unique,
    name       text        not null,
    role       role        not null,
    created_at timestamptz not null default now(),

    check ( length(name) > 1 )
);

create table login_details (
    email citext references account (email) primary key,
    password_hash text not null
);


create view account_view as
    select a.account_id, a.email, ld.password_hash from account a left join login_details ld using (email);