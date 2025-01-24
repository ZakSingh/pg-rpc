mod api {
    pub async fn optional_argument(
        client: &tokio_postgres::Client,
        required: i32,
        opt_1: Option<i32>,
        opt_2: Option<bool>,
    ) -> Result<i32, tokio_postgres::Error> {
        let mut params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&required];
        let mut query = concat!("select api.optional_argument", "(", "$1").to_string();
        if let Some(ref value) = opt_1 {
            params.push(value);
            query.push_str(concat!(", ", "opt_1", ":= $"));
            query.push_str(&params.len().to_string());
        }
        if let Some(ref value) = opt_2 {
            params.push(value);
            query.push_str(concat!(", ", "opt_2", ":= $"));
            query.push_str(&params.len().to_string());
        }
        query.push_str(")");
        client
            .query_one(&query, &params)
            .await
            .and_then(|r| r.try_get(0))
    }
}

mod test {
    use crate::dummy::api::optional_argument;
    use crate::tests::setup_test_db;

    #[tokio::test]
    async fn t() -> anyhow::Result<()> {
        let db = setup_test_db().await;
        db.client
          .simple_query(
              r#"
                create type role as enum ('admin', 'user');

                create table account (
                    account_id int primary key generated always as identity,
                    name text not null,
                    age int,
                    role role not null
                );

                create table post (
                    post_id int primary key generated always as identity,
                    author_id int not null references account(account_id),
                    content text
                );

                create schema api;

                create function api.optional_argument(required int, opt_1 int default 1, opt_2 bool default null)
                returns int as $$
                begin
                   return 1;
                end;
                $$ language plpgsql;
                "#,
          )
          .await?;

        let x = optional_argument(&db.client, 1, None, Some(true)).await?;
        Ok(())
    }
}
