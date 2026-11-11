//! `@pgrpc_message` / `@pgrpc_field` on constraint comments surface as
//! `message()` / `field()` on the per-table constraint enums and through the
//! name-keyed `constraint_message` lookup.

use super::*;
use crate::integration::compile_helpers::{
    assert_compilation_success, compile_project, create_test_cargo_project, read_pretty,
};
use indoc::indoc;

/// Strip whitespace, trailing commas and braces so substring checks survive
/// prettyplease wrapping (`=> { Some(..) }` vs `=> Some(..)`).
fn compact(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_whitespace() && *c != '{' && *c != '}')
        .collect::<String>()
        .replace(",)", ")")
}

fn setup(client: &mut Client) -> Result<(), postgres::Error> {
    execute_sql(
        client,
        indoc! {"
        CREATE DOMAIN postcode AS TEXT
            CONSTRAINT postcode_format CHECK (VALUE ~ '^[A-Z0-9 ]+$');
        COMMENT ON CONSTRAINT postcode_format ON DOMAIN postcode IS
            '@pgrpc_message Not a valid postcode';

        CREATE TABLE submission (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            token TEXT,
            graded BOOLEAN NOT NULL DEFAULT FALSE,
            postcode postcode,
            account_id INT REFERENCES account(account_id),
            CONSTRAINT title_length CHECK (length(title) <= 60),
            CONSTRAINT token_follows_grading CHECK (token IS NULL OR graded),
            CONSTRAINT submission_token_key UNIQUE (token)
        );

        COMMENT ON CONSTRAINT title_length ON submission IS $$
            Printed on the certificate, where there is room for this much.

            @pgrpc_message Title must be 60 characters or fewer
        $$;
        COMMENT ON CONSTRAINT token_follows_grading ON submission IS $$
            @pgrpc_message A token can only be assigned once the piece is graded
            @pgrpc_field()
        $$;
        COMMENT ON CONSTRAINT submission_token_key ON submission IS $$
            @pgrpc_message That token isn't valid, it's taken
            @pgrpc_field(token_code)
        $$;
        -- The FK stays untagged: a maintenance note must not leak into forms.
        COMMENT ON CONSTRAINT submission_account_id_fkey ON submission IS
            'Owner. Cascade deliberately omitted; see runbook.';

        CREATE SCHEMA msg_api;
        CREATE FUNCTION msg_api.create_submission(p_title TEXT, p_token TEXT, p_postcode postcode)
        RETURNS INT LANGUAGE plpgsql AS $$
        DECLARE v_id INT;
        BEGIN
            INSERT INTO submission (title, token, postcode) VALUES (p_title, p_token, p_postcode)
            RETURNING id INTO v_id;
            RETURN v_id;
        END $$;
    "},
    )
}

#[test]
fn constraint_messages_are_generated_and_compile() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        setup(client).expect("setup");

        let project = create_test_cargo_project(conn_string, vec!["msg_api"]);
        let generated = project.path().join("src").join("generated");
        let errors = read_pretty(generated.join("errors.rs"));
        let errors_c = compact(&errors);

        // Accessors on the per-table enum.
        for expected in [
            "SubmissionConstraint::TitleLength => \"title_length\"",
            "SubmissionConstraint::TitleLength => Some(\"Title must be 60 characters or fewer\")",
            "SubmissionConstraint::TokenFollowsGrading => Some(\"A token can only be assigned once the piece is graded\")",
            "SubmissionConstraint::SubmissionTokenKey => Some(\"That token isn't valid, it's taken\")",
            // field(): single column, explicit override, explicit none
            "SubmissionConstraint::TitleLength => Some(\"title\")",
            "SubmissionConstraint::SubmissionTokenKey => Some(\"token_code\")",
            "SubmissionConstraint::TokenFollowsGrading => None",
            // untagged FK has no message, but still has a field (its one column)
            "SubmissionConstraint::SubmissionAccountIdFkey => None",
            "SubmissionConstraint::SubmissionAccountIdFkey => Some(\"account_id\")",
            "SubmissionConstraint::PrimaryKey => \"submission_pkey\"",
        ] {
            assert!(errors_c.contains(&compact(expected)), "missing {expected}\n{errors}");
        }

        // Display prints the Postgres name now.
        assert!(errors_c.contains(&compact("f.write_str(self.name())")), "{errors}");

        // Flat lookups by raw names.
        for expected in [
            "(\"submission\", \"title_length\") => Some(ConstraintMessage { message: \"Title must be 60 characters or fewer\", field: Some(\"title\") })",
            "(\"submission\", \"token_follows_grading\") => Some(ConstraintMessage { message: \"A token can only be assigned once the piece is graded\", field: None })",
            "(\"submission\", \"submission_token_key\") => Some(ConstraintMessage { message: \"That token isn't valid, it's taken\", field: Some(\"token_code\") })",
            "(\"postcode\", \"postcode_format\") => Some(ConstraintMessage { message: \"Not a valid postcode\", field: None })",
        ] {
            assert!(errors_c.contains(&compact(expected)), "missing {expected}\n{errors}");
        }
        assert!(
            !errors_c.contains("submission_account_id_fkey\")=>Some"),
            "untagged constraint must not get a message:\n{errors}"
        );

        // One generic conversion on every error enum.
        assert!(errors_c.contains(&compact("fn user_message(&self) -> Option<ConstraintMessage>")), "{errors}");

        // The function's own enum still routes the typed variants.
        let api = read_pretty(generated.join("msg_api.rs"));
        assert!(api.contains("SubmissionConstraint::TitleLength"), "{api}");

        assert_compilation_success(compile_project(project.path()));
    });
}
