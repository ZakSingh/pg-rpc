use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

fn test_schema() -> (TempDir, PathBuf) {
    let tmp_dir = tempfile::tempdir().unwrap();
    let base_path = tmp_dir.path().to_path_buf();

    // Create schema directories
    let tables_dir = base_path.join("tables");
    fs::create_dir(&tables_dir).unwrap();

    // Write Account table DDL
    fs::write(
        tables_dir.join("01_account.sql"),
        r#"
          create type role as enum ('admin', 'user');
          create table account (
              account_id serial primary key,
              email text not null unique,
              name text not null,
              role role not null,
              created_at timestamptz not null default now()
          );
        "#,
    )
    .unwrap();

    // Write Post table DDL
    fs::write(
        tables_dir.join("02_post.sql"),
        r#"
          create table post (
              post_id serial primary key,
              account_id int not null references account,
              title text not null,
              content text,
              created_at timestamptz not null default now()
          );

          create view post_with_author as (
              select post.*, row(account.*)::account as author from post
              left join account using (account_id)
          );

          comment on column post_with_author.author is '@pgrpc_not_null';
        "#,
    )
    .unwrap();

    fs::write(
        tables_dir.join("03_api.sql"),
        r#"
          create schema api;

          create function api.optional_argument(required int, opt_1 int, opt_2 bool)
          returns int as $$
          begin
             return 3;
          end;
          $$ language plpgsql;

          comment on function api.optional_argument(int, int, bool) is 'fn comment';
        "#,
    )
    .unwrap();

    (tmp_dir, base_path)
}
