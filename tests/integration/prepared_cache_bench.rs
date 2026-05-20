//! Microbenchmark proving prepared-statement caching pays off.
//!
//! Generates a single `GetUserById :one` query, then runs it in a tight loop
//! against a deadpool pool pinned to one connection (so the StatementCache is
//! shared across iterations). Compares:
//!
//!   * cached  — calls the pgrpc-generated function (uses `prepare_typed_cached`)
//!   * uncached — same SQL + params, but `client.query_opt(sql_str, &params)`,
//!                which is what pgrpc *used to* generate. Forces a fresh parse /
//!                bind / describe round-trip on every iteration.
//!
//! Asserts that cached is at least 1.10× faster. The real-world speedup on an
//! idle local docker postgres is 2-3×; we set the threshold low to stay robust
//! when the full integration suite runs in parallel and the postgres container's
//! CPU is contended (which compresses both timings toward the network-only
//! floor and makes the ratio noisier). Prints both timings to stdout so
//! regressions and real-world improvements are both visible in CI logs.
//!
//! Set `PGRPC_BENCH_ITERS` (default 2000) to override the iteration count.

use super::*;
use crate::integration::compile_helpers::*;
use indoc::indoc;
use pgrpc::PgrpcBuilder;

#[test]
fn prepared_statement_cache_speeds_up_repeated_queries() {
    with_isolated_database_and_container(|client, _container, conn_string| {
        // A wide-enough row to make the response cost non-trivial relative to the
        // prepare overhead, but small enough to keep the bench fast.
        client
            .execute(
                "CREATE TABLE bench_users (
                    id SERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    email TEXT NOT NULL,
                    bio TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )",
                &[],
            )
            .unwrap();
        // Seed enough rows that the query has actual work to do (and the planner
        // doesn't optimise the whole thing into a no-op).
        client
            .execute(
                "INSERT INTO bench_users (username, email, bio) \
                 SELECT 'user_' || g, 'user_' || g || '@example.com', \
                        repeat('x', 80) \
                 FROM generate_series(1, 500) g",
                &[],
            )
            .unwrap();

        let project_dir = create_test_cargo_project(conn_string, vec!["public"]);
        let sql_file = project_dir.path().join("bench.sql");
        std::fs::write(
            &sql_file,
            indoc! {r#"
                -- name: GetUserById :one
                SELECT id, username, email, bio, created_at
                FROM bench_users
                WHERE id = :user_id;
            "#},
        )
        .unwrap();

        // Regenerate with the query file included.
        PgrpcBuilder::new()
            .connection_string(conn_string)
            .schema("public")
            .output_path(project_dir.path().join("src/generated"))
            .queries_config(pgrpc::QueriesConfig {
                paths: vec![sql_file.to_string_lossy().to_string()],
            })
            .build()
            .expect("regen should succeed");

        // Re-export queries so the test binary can call them.
        let lib_rs = project_dir.path().join("src/lib.rs");
        let mut lib_src = std::fs::read_to_string(&lib_rs).unwrap();
        if !lib_src.contains("generated::queries") {
            lib_src.push_str("\n#[allow(unused_imports)] pub use generated::queries::*;\n");
            std::fs::write(&lib_rs, lib_src).unwrap();
        }

        // The benchmark binary. Key invariants:
        //   * `max_size=1` — guarantees the pool hands out the same connection
        //     each `pool.get()`, so the StatementCache built up during the cached
        //     loop persists across iterations. (Different connection => empty
        //     cache => no speedup, which would falsely fail this test.)
        //   * We hold the client across the whole loop instead of calling
        //     `pool.get()` per iteration. That's the realistic hot-path pattern
        //     (one acquire per HTTP request, many queries inside it) and avoids
        //     adding pool-acquire jitter to the measurement.
        //   * Warm-up first, then measure. Without warm-up, the first iteration
        //     pays connection setup, and the cached run also pays its one-time
        //     prepare. We want steady-state numbers.
        let bench_code = indoc! {r#"
            use deadpool_postgres::{Config, Runtime};
            use std::time::Instant;
            use test_project::*;

            #[tokio::main]
            async fn main() {
                let conn_string = std::env::args().nth(1).expect("conn string required");
                let iters: usize = std::env::var("PGRPC_BENCH_ITERS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(2000);

                let mut config = Config::new();
                config.url = Some(conn_string);
                // pin to one connection so the per-connection StatementCache is
                // the same one across all iterations.
                let mut pool_config = deadpool_postgres::PoolConfig::default();
                pool_config.max_size = 1;
                config.pool = Some(pool_config);

                let pool = config
                    .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
                    .expect("create pool");

                let client = pool.get().await.expect("get client");

                let raw_sql = "SELECT id, username, email, bio, created_at \
                               FROM bench_users WHERE id = $1;";

                // Warm-up. Touches the cache for both paths and amortises any
                // first-call connection-level setup costs.
                for i in 0..50i32 {
                    let id = (i % 500) + 1;
                    let _ = get_user_by_id(&client, id).await.expect("warmup cached");
                    let _ = client
                        .query_opt(raw_sql, &[&id])
                        .await
                        .expect("warmup uncached");
                }

                // ---- cached path: pgrpc-generated, uses prepare_typed_cached ----
                let t0 = Instant::now();
                for i in 0..iters as i32 {
                    let id = (i % 500) + 1;
                    let _row = get_user_by_id(&client, id).await.expect("cached call");
                }
                let cached = t0.elapsed();

                // ---- uncached path: raw &str — re-parses every call ----
                let t0 = Instant::now();
                for i in 0..iters as i32 {
                    let id = (i % 500) + 1;
                    let row = client
                        .query_opt(raw_sql, &[&id])
                        .await
                        .expect("uncached call");
                    // Defeat dead-code-elimination of the response.
                    let _: i32 = row.unwrap().try_get(0).unwrap();
                }
                let uncached = t0.elapsed();

                let cached_us = cached.as_secs_f64() * 1_000_000.0 / iters as f64;
                let uncached_us = uncached.as_secs_f64() * 1_000_000.0 / iters as f64;
                let speedup = uncached_us / cached_us;

                // Machine-readable line the test harness will parse.
                println!(
                    "BENCH iters={} cached_us={:.2} uncached_us={:.2} speedup={:.2}x",
                    iters, cached_us, uncached_us, speedup
                );
            }
        "#};

        add_test_binary(project_dir.path(), "bench_prepared_cache", bench_code);

        // Build release to make the measurement meaningful (debug builds are
        // dominated by allocator overhead). The actual *prepare* round-trip
        // savings show up regardless of opt level, but release keeps the
        // comparison clean.
        let build = std::process::Command::new("cargo")
            .args(["build", "--release", "--bin", "bench_prepared_cache"])
            .current_dir(project_dir.path())
            .output()
            .expect("cargo build");
        if !build.status.success() {
            print_output(&build);
            panic!("bench binary failed to compile");
        }
        let run = std::process::Command::new("cargo")
            .args(["run", "--release", "--bin", "bench_prepared_cache", "--", conn_string])
            .current_dir(project_dir.path())
            .output()
            .expect("cargo run");

        let stdout = String::from_utf8_lossy(&run.stdout);
        let stderr = String::from_utf8_lossy(&run.stderr);
        if !run.status.success() {
            eprintln!("STDOUT:\n{}", stdout);
            eprintln!("STDERR:\n{}", stderr);
            panic!("bench binary failed at runtime");
        }

        let line = stdout
            .lines()
            .find(|l| l.starts_with("BENCH "))
            .unwrap_or_else(|| {
                panic!("bench did not emit BENCH line. STDOUT:\n{}\nSTDERR:\n{}", stdout, stderr)
            });
        eprintln!("{}", line);

        let speedup: f64 = line
            .split_whitespace()
            .find_map(|tok| tok.strip_prefix("speedup="))
            .and_then(|s| s.strip_suffix('x'))
            .and_then(|s| s.parse().ok())
            .expect("parse speedup from BENCH line");

        // Conservative threshold. When this test runs alone the speedup is
        // typically 2-3×; under the full parallel test suite the docker
        // postgres CPU becomes the bottleneck and the ratio compresses toward
        // 1× (both paths queue behind the same network/CPU). 1.10× is a noise-
        // floor sanity check that survives parallel runs while still catching
        // a regression that removes prepared-statement caching entirely.
        assert!(
            speedup >= 1.10,
            "Expected cached path to be ≥1.10× faster than uncached, got {}× — \
             prepared-statement caching may have regressed",
            speedup
        );
    });
}
