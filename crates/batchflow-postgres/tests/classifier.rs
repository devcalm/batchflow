//! The classifier is only meaningful against SQLSTATEs a real Postgres emits.
//! Hand-built `sqlx::Error`s would prove that the `match` arms exist, not that
//! the codes are the ones the database actually sends.

use batchflow_core::{BatchError, Cause, Classifier, ErrorAction};
use batchflow_postgres::PostgresClassifier;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// The container handle must outlive the test; dropping it stops the database.
async fn start() -> (ContainerAsync<PostgresImage>, PgPool) {
    let container = PostgresImage::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();

    sqlx::query("CREATE TABLE rows_under_test (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    (container, pool)
}

/// Runs a statement expected to fail, and hands the error back the way a user's
/// writer would: wrapped, not stringified.
async fn write_error(pool: &PgPool, sql: &'static str) -> BatchError {
    let error = sqlx::query(sql)
        .execute(pool)
        .await
        .expect_err("statement was supposed to fail");

    BatchError::write(error)
}

async fn insert_valid_row(pool: &PgPool) {
    sqlx::query("INSERT INTO rows_under_test (id, label) VALUES (1, 'ok')")
        .execute(pool)
        .await
        .unwrap();
}

/// 23505 unique_violation — one duplicated row, the rest of the chunk is fine.
#[tokio::test]
async fn a_duplicate_key_is_skippable() {
    let (_container, pool) = start().await;
    insert_valid_row(&pool).await;

    let error = write_error(
        &pool,
        "INSERT INTO rows_under_test (id, label) VALUES (1, 'again')",
    )
    .await;

    assert_eq!(PostgresClassifier.classify(&error), ErrorAction::Skip);
}

/// 23502 not_null_violation — US-3's malformed row, arriving as a constraint.
#[tokio::test]
async fn a_null_in_a_not_null_column_is_skippable() {
    let (_container, pool) = start().await;

    let error = write_error(
        &pool,
        "INSERT INTO rows_under_test (id, label) VALUES (2, NULL)",
    )
    .await;

    assert_eq!(PostgresClassifier.classify(&error), ErrorAction::Skip);
}

/// 22P02 invalid_text_representation — class 22, matched by class rather than
/// by an enumerated code.
#[tokio::test]
async fn a_malformed_value_is_skippable() {
    let (_container, pool) = start().await;

    let error = write_error(
        &pool,
        "INSERT INTO rows_under_test (id, label) VALUES ('not-a-number', 'x')",
    )
    .await;

    assert_eq!(PostgresClassifier.classify(&error), ErrorAction::Skip);
}

/// 55P03 lock_not_available — the deterministic cousin of a deadlock. A real
/// 40P01 needs two transactions racing in opposite orders; this exercises the
/// same arm without a race in the test itself.
#[tokio::test]
async fn a_lock_conflict_is_retryable() {
    let (_container, pool) = start().await;
    insert_valid_row(&pool).await;

    // Holder keeps the row locked for the duration.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM rows_under_test WHERE id = 1 FOR UPDATE")
        .fetch_one(&mut *holder)
        .await
        .unwrap();

    let mut contender = pool.begin().await.unwrap();
    let error = sqlx::query("SELECT id FROM rows_under_test WHERE id = 1 FOR UPDATE NOWAIT")
        .fetch_one(&mut *contender)
        .await
        .expect_err("the row is locked");

    assert_eq!(
        PostgresClassifier.classify(&BatchError::write(error)),
        ErrorAction::Retry
    );
}

/// 42P01 undefined_table — a programming error. Skipping would discard every
/// row of a job that can never work, and retrying would just wait to fail.
#[tokio::test]
async fn a_broken_statement_fails() {
    let (_container, pool) = start().await;

    let error = write_error(&pool, "INSERT INTO no_such_table (id) VALUES (1)").await;

    assert_eq!(PostgresClassifier.classify(&error), ErrorAction::Fail);
}

/// The verdict must survive nesting: the engine wraps an item error in
/// `SkipLimitExceeded`, and a user's writer may wrap `sqlx::Error` in its own
/// type. Matching on `BatchError`'s variants would see only the outermost.
#[tokio::test]
async fn a_nested_database_error_is_still_classified() {
    let (_container, pool) = start().await;
    insert_valid_row(&pool).await;

    let inner = write_error(
        &pool,
        "INSERT INTO rows_under_test (id, label) VALUES (1, 'again')",
    )
    .await;

    // Two hops from the top: SkipLimitExceeded -> BatchError::Write -> sqlx.
    let nested = BatchError::SkipLimitExceeded {
        limit: 3,
        cause: Cause::from(inner),
    };

    assert_eq!(PostgresClassifier.classify(&nested), ErrorAction::Skip);
}
