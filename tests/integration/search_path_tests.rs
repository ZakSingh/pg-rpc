use super::*;
use crate::integration::compile_helpers::read_pretty;
use pgrpc::*;
use tempfile::TempDir;

/// Two schemas hold a table named `widget`, with opposite constraints, and
/// the database's `search_path` puts the non-`public` one first. pgrpc opens
/// its own connection, so the path is set on the database rather than on the
/// test's session.
///
/// Every analysis must resolve an unqualified `widget` the way Postgres does
/// on that connection — to `app.widget` — and a qualified `public.widget` to
/// the other one.
fn setup_two_widgets(client: &mut Client) {
    client
        .batch_execute(
            r#"
            CREATE SCHEMA app;
            CREATE DOMAIN app.label AS text;

            -- Keyed by id; label is a NOT NULL domain; note is nullable.
            CREATE TABLE app.widget (
                id    int PRIMARY KEY,
                label app.label NOT NULL,
                note  text
            );
            -- Keyed by note; id and label are nullable plain columns.
            CREATE TABLE public.widget (
                id    int,
                label text,
                note  text PRIMARY KEY
            );
            -- Only in a schema that is not on the path.
            CREATE SCHEMA hidden;
            CREATE TABLE hidden.gadget (id int PRIMARY KEY, label text NOT NULL);

            CREATE VIEW app.widget_view AS SELECT id, label, note FROM app.widget;
            CREATE VIEW public.widget_view AS SELECT id, label, note FROM public.widget;
            "#,
        )
        .unwrap();
    let db: String = client.query_one("SELECT current_database()", &[]).unwrap().get(0);
    client
        .batch_execute(&format!(r#"ALTER DATABASE "{db}" SET search_path TO app, public"#))
        .unwrap();
}

fn generate(conn_string: &str, sql: &str) -> String {
    let temp_dir = TempDir::new().expect("temp dir");
    let sql_file = temp_dir.path().join("test.sql");
    std::fs::write(&sql_file, sql).expect("write sql");
    let output_dir = temp_dir.path().join("generated");
    PgrpcBuilder::new()
        .connection_string(conn_string)
        .schema("app")
        .schema("public")
        .schema("hidden")
        .output_path(&output_dir)
        .infer_view_nullability(true)
        .queries_config(pgrpc::QueriesConfig {
            paths: vec![sql_file.to_string_lossy().to_string()],
        })
        .build()
        .expect("codegen should succeed");
    read_pretty(output_dir.join("queries.rs"))
}

/// The generated struct `name`, declaration to closing brace.
fn struct_of<'a>(code: &'a str, name: &str) -> &'a str {
    let marker = format!("pub struct {name} {{");
    let start = code
        .find(&marker)
        .unwrap_or_else(|| panic!("`{marker}` not found in generated code:\n{code}"));
    let rest = &code[start..];
    &rest[..rest.find("\n}").expect("struct end")]
}

/// The signature of the generated query function `name`, up to its body.
fn signature_of<'a>(code: &'a str, name: &str) -> &'a str {
    let marker = format!("pub async fn {name}(");
    let start = code
        .find(&marker)
        .unwrap_or_else(|| panic!("`{marker}` not found in generated code:\n{code}"));
    let rest = &code[start..];
    &rest[..rest.find(" {\n").expect("fn body")]
}

fn assert_contains(haystack: &str, needle: &str) {
    assert!(haystack.contains(needle), "expected `{needle}` in:\n{haystack}");
}

#[test]
fn test_unqualified_relations_follow_the_connections_search_path() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup_two_widgets(client);

        let sql = indoc! {r#"
            -- name: PathWidget
            SELECT id, label, note FROM widget WHERE id = $1;

            -- name: PublicWidget
            SELECT id, label, note FROM public.widget WHERE id = $1;

            -- name: PublicWidgetByNote
            SELECT id, label, note FROM public.widget WHERE note = $1;

            -- name: PathWidgetByNote
            SELECT id, label, note FROM widget WHERE note = $1;

            -- name: PathWidgetView
            SELECT id, label, note FROM widget_view;

            -- name: PublicWidgetView
            SELECT id, label, note FROM public.widget_view;

            -- name: UpdatePathWidget
            UPDATE widget SET note = $2 WHERE id = $1 RETURNING id, label, note;

            -- name: UpdatePublicWidget
            UPDATE public.widget SET note = $2 WHERE id = $1 RETURNING id, label, note;

            -- name: PathWidgetInCte
            WITH w AS (SELECT id, label FROM widget WHERE label = $1)
            SELECT w.id, w.label FROM w;

            -- name: HiddenGadget
            SELECT id, label FROM hidden.gadget WHERE id = $1;
        "#};
        let code = generate(conn_string, sql);

        // Unqualified `widget` is `app.widget`: id and label NOT NULL, label
        // of the domain type, keyed by id.
        for row in ["PathWidgetRow", "UpdatePathWidgetRow"] {
            let row = struct_of(&code, row);
            assert_contains(row, "pub id: i32,");
            assert_contains(row, "pub label: super::app::Label,");
            assert_contains(row, "pub note: Option<String>,");
        }
        assert_contains(signature_of(&code, "path_widget"), "Result<Option<PathWidgetRow>,");
        assert_contains(signature_of(&code, "update_path_widget"), "Result<Option<UpdatePathWidgetRow>,");
        // `note` is not a key of `app.widget`.
        assert_contains(signature_of(&code, "path_widget_by_note"), "Result<Vec<PathWidgetByNoteRow>,");

        // Qualified `public.widget`: id and label nullable plain columns,
        // keyed by note.
        for row in ["PublicWidgetRow", "UpdatePublicWidgetRow"] {
            let row = struct_of(&code, row);
            assert_contains(row, "pub id: Option<i32>,");
            assert_contains(row, "pub label: Option<String>,");
            assert_contains(row, "pub note: String,");
        }
        assert_contains(signature_of(&code, "public_widget"), "Result<Vec<PublicWidgetRow>,");
        assert_contains(signature_of(&code, "update_public_widget"), "Result<Vec<UpdatePublicWidgetRow>,");
        assert_contains(
            signature_of(&code, "public_widget_by_note"),
            "Result<Option<PublicWidgetByNoteRow>,",
        );

        // Errors: writing `note` can break `public.widget`'s key, not
        // `app.widget`'s.
        assert!(
            code.contains("widget_pkey") || code.contains("WidgetPkey"),
            "UpdatePublicWidget should know public.widget's primary key:\n{code}"
        );
        let path_error = {
            let start = code.find("pub enum UpdatePathWidgetError {").expect("error enum");
            let rest = &code[start..];
            &rest[..rest.find("\n}").expect("enum end")]
        };
        assert!(
            !path_error.to_lowercase().contains("pkey"),
            "UpdatePathWidget does not write app.widget's key:\n{path_error}"
        );

        // Views: each `widget_view` is found under its own cache entry.
        let row = struct_of(&code, "PathWidgetViewRow");
        assert_contains(row, "pub id: i32,");
        assert_contains(row, "pub label: super::app::Label,");
        let row = struct_of(&code, "PublicWidgetViewRow");
        assert_contains(row, "pub id: Option<i32>,");
        assert_contains(row, "pub label: Option<String>,");
        assert_contains(row, "pub note: String,");

        // Through a CTE: still `app.widget`'s columns, nullability and type.
        let row = struct_of(&code, "PathWidgetInCteRow");
        assert_contains(row, "pub id: i32,");
        assert_contains(row, "pub label: super::app::Label,");

        // A schema that is not on the path is reachable by name only.
        let row = struct_of(&code, "HiddenGadgetRow");
        assert_contains(row, "pub id: i32,");
        assert_contains(row, "pub label: String,");
        assert_contains(signature_of(&code, "hidden_gadget"), "Result<Option<HiddenGadgetRow>,");
    });
}

/// A parameter compared with an unqualified column inside a subquery or CTE
/// takes its type from the column that name means there.
#[test]
fn test_parameter_compared_inside_a_subquery_binds_to_the_inner_table() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        client
            .batch_execute(
                r#"
                CREATE DOMAIN sp_email AS text;
                CREATE TABLE sp_member (id int PRIMARY KEY, email sp_email NOT NULL);
                CREATE TABLE sp_invite (id int PRIMARY KEY, account_id int NOT NULL, email text);
                "#,
            )
            .unwrap();

        let sql = indoc! {r#"
            -- name: AccountsInvitedAs
            SELECT a.id FROM sp_member a
            WHERE a.id IN (SELECT account_id FROM sp_invite WHERE email = $1);

            -- name: AccountsInvitedAsViaCte
            WITH hits AS (SELECT account_id FROM sp_invite WHERE email = $1)
            SELECT a.id FROM sp_member a JOIN hits h ON h.account_id = a.id;

            -- name: AccountByEmail
            SELECT a.id FROM sp_member a WHERE email = $1;

            -- name: AccountEmailThroughCte
            WITH a AS (SELECT id, email FROM sp_member)
            SELECT email FROM a WHERE email = $1;
        "#};
        let code = generate(conn_string, sql);

        // `sp_invite.email` is plain text: the parameter must not take
        // `sp_member.email`'s domain type from the outer query.
        for name in ["accounts_invited_as", "accounts_invited_as_via_cte"] {
            let signature = signature_of(&code, name);
            assert!(
                !signature.contains("SpEmail"),
                "{name}: parameter bound to the outer table's column:\n{signature}"
            );
            assert_contains(signature, "email: &str");
        }
        // Compared with `sp_member.email` itself — directly or through a CTE
        // that passes the column on — it has the domain type.
        for name in ["account_by_email", "account_email_through_cte"] {
            assert_contains(signature_of(&code, name), "SpEmail");
        }
    });
}
