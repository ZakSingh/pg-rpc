//! `tsvector` and `tsquery` travel in the binary wire format in both
//! directions. Values are checked against what the server itself prints.

use super::*;
use crate::integration::run_helpers::{generate_project, run_binary};

#[test]
fn tsvector_and_tsquery_round_trip() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        execute_sql(
            client,
            r#"
            create schema ts;

            create function ts.parse_query(p_text text) returns tsquery
                language sql as $$ select p_text::tsquery $$;
            create function ts.query_text(p_text text) returns text
                language sql as $$ select p_text::tsquery::text $$;
            create function ts.echo_query(p_query tsquery) returns tsquery
                language sql as $$ select p_query $$;
            create function ts.shown_query(p_query tsquery) returns text
                language sql as $$ select p_query::text $$;

            create function ts.parse_vector(p_text text) returns tsvector
                language sql as $$ select p_text::tsvector $$;
            create function ts.vector_text(p_text text) returns text
                language sql as $$ select p_text::tsvector::text $$;
            create function ts.echo_vector(p_vector tsvector) returns tsvector
                language sql as $$ select p_vector $$;
            create function ts.shown_vector(p_vector tsvector) returns text
                language sql as $$ select p_vector::text $$;

            create function ts.matches(p_vector tsvector, p_query tsquery) returns bool
                language sql as $$ select p_vector @@ p_query $$;
            "#,
        )
        .expect("schema");

        let project = generate_project(conn_string, |b| b.schema("ts"));
        run_binary(
            &project,
            conn_string,
            "ts_roundtrip",
            "use test_project::ts::tsvector::{TsQuery, TsVector};",
            r#"
    let queries = [
        "cat",
        "cat & dog",
        "cat | dog",
        "!cat",
        "cat & !dog",
        "!( cat | dog )",
        "cat & ( dog | bird )",
        "( cat | dog ) & bird",
        "cat | dog & bird",
        "cat <-> dog",
        "cat <3> dog",
        "cat <-> dog <-> bird",
        "cat <-> ( dog <-> bird )",
        "( cat | dog ) <-> bird",
        "!cat <-> dog",
        "!( cat <-> dog ) & bird | !fish",
        "sup:*",
        "cat:A",
        "cat:*AB & dog:cd",
        "'two words' & 'it''s'",
        "'back\\\\slash'",
    ];
    for text in queries {
        let expected = ts::query_text().text(text).exec(&client).await.expect("server text").expect("text");

        // Decoding: what the server sends in binary reads as its own text form.
        let parsed = ts::parse_query().text(text).exec(&client).await
            .unwrap_or_else(|e| panic!("decode tsquery {text:?}: {e:?}"))
            .expect("tsquery");
        assert_eq!(parsed.0, expected, "decoded tsquery for {text:?}");

        // Encoding: sending that text form back yields the same query.
        let sent = TsQuery::from(text);
        let shown = ts::shown_query().query(&sent).exec(&client).await
            .unwrap_or_else(|e| panic!("encode tsquery {text:?}: {e:?}"))
            .expect("text");
        assert_eq!(shown, expected, "encoded tsquery for {text:?}");

        let echoed = ts::echo_query().query(&parsed).exec(&client).await
            .unwrap_or_else(|e| panic!("echo tsquery {text:?}: {e:?}"))
            .expect("tsquery");
        assert_eq!(echoed, parsed, "echoed tsquery for {text:?}");
    }

    let vectors = [
        "cat",
        "the quick brown fox",
        "fox:4 quick:2A,7 brown:3B,9C",
        "'two words':1 'it''s':2",
        "b a b a:3",
        "",
    ];
    for text in vectors {
        let expected = ts::vector_text().text(text).exec(&client).await.expect("server text").expect("text");

        let parsed = ts::parse_vector().text(text).exec(&client).await
            .unwrap_or_else(|e| panic!("decode tsvector {text:?}: {e:?}"))
            .expect("tsvector");
        assert_eq!(parsed.0, expected, "decoded tsvector for {text:?}");

        let sent = TsVector::from(text);
        let shown = ts::shown_vector().vector(&sent).exec(&client).await
            .unwrap_or_else(|e| panic!("encode tsvector {text:?}: {e:?}"))
            .expect("text");
        assert_eq!(shown, expected, "encoded tsvector for {text:?}");

        let echoed = ts::echo_vector().vector(&parsed).exec(&client).await
            .unwrap_or_else(|e| panic!("echo tsvector {text:?}: {e:?}"))
            .expect("tsvector");
        assert_eq!(echoed, parsed, "echoed tsvector for {text:?}");
    }

    let vector = TsVector::from("fat:1 cat:2 sat:3");
    let hit = ts::matches().vector(&vector).query(&TsQuery::from("fat <-> cat & !dog")).exec(&client).await.expect("match");
    assert_eq!(hit, Some(true));
    let miss = ts::matches().vector(&vector).query(&TsQuery::from("cat <-> fat")).exec(&client).await.expect("match");
    assert_eq!(miss, Some(false));

    // Malformed text is rejected on the client, before anything is sent.
    let bad = ts::shown_query().query(&TsQuery::from("cat &")).exec(&client).await;
    assert!(bad.is_err(), "a dangling operator should not encode: {bad:?}");
"#,
        );
    });
}
