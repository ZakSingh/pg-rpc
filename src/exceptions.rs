use crate::parse_explicit_exceptions::StmtRaise;
use crate::PgFnBody;
use serde_json::Value;
use serde_json_path::JsonPath;

/// Implicit exceptions can come from the following:
/// 1. For any inserts/updates/deletes:
///   a. Check constraints
///   b. Foreign key constraints
///   c. Unique constraints
/// 2. Triggers on any table queried. Requires their own full analysis. They can be considered
/// the same way as a function called from within the function. So maybe they should be deferred to that
/// analysis.
// pub fn get_implicit_exceptions(body: &PgFnBody) -> anyhow::Result<Vec<PgException>> {}

// Implicit exceptions don't have all the fields of a StmtRaise, at least not at analysis time...
// Their messages are dynamically generated. E.g. check constraints.

enum ImplicitException {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_explicit_exceptions::get_explicit_exceptions;
    use indoc::indoc;
    use serde_json::to_string_pretty;

    #[test]
    fn test_get_exceptions() -> anyhow::Result<()> {
        let f_str = indoc! {"
      create or replace function api.test_fn(
        p_one int, p_two bool
      )
        returns int
        language plpgsql
      as
      $$
      begin

      raise;
      raise notice 'not an error';
      raise division_by_zero;
      raise exception 'error two';
      raise sqlstate '22012';
      raise unique_violation using message = 'Duplicate user ID: ' || user_id;
      raise 'Duplicate user ID %', p_one;
      raise 'hello world' using errcode = '22012', detail = 'det', hint = 'hint';

      end;
      $$
    "};

        println!(
            "{}",
            to_string_pretty(&pg_query::parse_plpgsql(f_str)?).unwrap()
        );

        println!(
            "{:?}",
            get_explicit_exceptions(&PgFnBody::PgSQL(pg_query::parse_plpgsql(f_str)?))?
        );

        Ok(())
    }
}
