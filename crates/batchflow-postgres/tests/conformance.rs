//! PostgreSQL's half of the shared `JobRepository` contract.
//!
//! The case list lives in `batchflow_core::conformance` and is the same one the
//! in-memory store runs, so a property can no longer hold for one backend and
//! quietly not the other. Backend-specific behaviour — migrations, rollback,
//! CHECK constraints — stays in `repository.rs`, because it is not part of the
//! trait's promise.

use batchflow_postgres::PostgresJobRepository;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A throwaway database per case. The container handle is returned as the guard
/// because dropping it stops the database.
async fn setup() -> (ContainerAsync<PostgresImage>, PostgresJobRepository) {
    let container = PostgresImage::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();

    let repository = PostgresJobRepository::new(pool);
    repository.migrate().await.unwrap();
    (container, repository)
}

batchflow_core::job_repository_conformance!(setup());
