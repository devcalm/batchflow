//! Redis's half of the shared `JobRepository` contract.
//!
//! The same 32 cases the in-memory store and PostgreSQL run. This backend was
//! written against them: the suite existed before the implementation did, so
//! the contract was executable from the first line.

use batchflow_redis::RedisJobRepository;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A throwaway Redis per case. The container handle is the guard, because
/// dropping it stops the server.
async fn setup() -> (ContainerAsync<Redis>, RedisJobRepository) {
    let container = Redis::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let repository = RedisJobRepository::connect(&format!("redis://127.0.0.1:{port}/"))
        .await
        .unwrap();
    (container, repository)
}

batchflow_core::job_repository_conformance!(setup());
