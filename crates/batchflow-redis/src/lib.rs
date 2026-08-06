//! # BatchFlow Redis
//!
//! Redis metadata store for BatchFlow.
//!
//! # Durability — read this before choosing it
//!
//! The metadata store *is* the exactly-once guarantee: restart is emergent
//! from what was recorded, so a lost `StepExecution` commit means a restarted
//! job re-does or skips work that had already committed. Redis's default
//! persistence (RDB snapshots) can lose the last seconds of writes, which for
//! most Redis workloads is a fine trade and for this one is data loss.
//!
//! **Run Redis with `appendonly yes` and `appendfsync always`.** Anything
//! weaker makes this backend's correctness probabilistic, and no amount of care
//! in this crate can compensate for it. If you cannot, use
//! [`batchflow-postgres`](https://docs.rs/batchflow-postgres) instead — that is
//! the recommendation, not a footnote.
//!
//! # Transactions
//!
//! `Tx` is a `redis::Pipeline` in `MULTI`/`EXEC` mode. Commands are buffered
//! client-side and sent only at commit, so a rolled-back chunk was never sent
//! at all — a stronger property than Postgres offers, and a much narrower one:
//! Redis has no read-your-writes inside a transaction and no conflict
//! detection, so a `TransactionalWriter` here queues writes blindly.
//!
//! A transaction spans only what Redis executes. If your writer targets a
//! *different* store, that store's writes are not in this transaction and the
//! step is at-least-once.
//!
//! # Atomicity
//!
//! Every check-then-act operation — resolving an instance, opening an
//! execution, abandoning one — runs as a Lua script, so the check and the write
//! share one round trip and cannot interleave. Redis's single-threaded command
//! execution is what makes this sufficient.
//!
//! # Eviction
//!
//! **Run Redis with `maxmemory-policy noeviction`.** Under `allkeys-lru` or
//! `allkeys-random` Redis will evict any key under memory pressure, including a
//! step execution — and `HGETALL` on an evicted key returns an empty hash
//! rather than an error. An evicted record is detected and reported here rather
//! than read back as a step that has never run, but the run still fails:
//! eviction of batch metadata has no safe interpretation.
//!
//! # Redis Cluster is not supported
//!
//! Not merely untested — structurally impossible as written. The scripts
//! declare keys that hash to different slots (a lookup key and the shared
//! sequence counter), and they construct further keys inside the script from
//! `ARGV`, which Cluster rejects as non-local. Use a single instance, or
//! [`batchflow-postgres`](https://docs.rs/batchflow-postgres).
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use batchflow_core::{
    BatchError, BatchStatus, ExecutionContext, JobExecution, JobExecutionId, JobInstance,
    JobInstanceId, JobParameters, JobRepository, StepContribution, StepExecution, StepExecutionId,
};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};
use std::sync::LazyLock;

const STARTING: &str = "STARTING";
const STARTED: &str = "STARTED";
const COMPLETED: &str = "COMPLETED";
const FAILED: &str = "FAILED";
const STOPPED: &str = "STOPPED";
const ABANDONED: &str = "ABANDONED";

/// Namespace for every key this crate writes, so a shared Redis stays legible.
const NS: &str = "batchflow";

fn seq_key() -> String {
    format!("{NS}:seq")
}
fn instance_key(id: i64) -> String {
    format!("{NS}:instance:{id}")
}
fn instance_lookup_key(job_name: &str, parameters: &str) -> String {
    format!("{NS}:lookup:{job_name}:{parameters}")
}
fn executions_key(instance_id: i64) -> String {
    format!("{NS}:instance:{instance_id}:executions")
}
fn execution_key(id: i64) -> String {
    format!("{NS}:execution:{id}")
}
fn execution_steps_key(execution_id: i64) -> String {
    format!("{NS}:execution:{execution_id}:steps")
}
fn step_key(id: i64) -> String {
    format!("{NS}:step:{id}")
}
fn instance_step_key(instance_id: i64, step_name: &str) -> String {
    format!("{NS}:instance:{instance_id}:step:{step_name}")
}

/// A [`JobRepository`] backed by Redis.
///
/// See the [module docs](self) for the durability requirement, which is not
/// optional.
#[derive(Clone)]
pub struct RedisJobRepository {
    connection: ConnectionManager,
}

impl std::fmt::Debug for RedisJobRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisJobRepository").finish_non_exhaustive()
    }
}

impl RedisJobRepository {
    /// Wraps an existing connection manager.
    #[must_use]
    pub fn new(connection: ConnectionManager) -> Self {
        Self { connection }
    }

    /// Connects to `url` (for example `redis://127.0.0.1/`).
    ///
    /// # Errors
    ///
    /// [`BatchError::Repository`] if the URL is invalid or the connection
    /// cannot be established.
    pub async fn connect(url: &str) -> Result<Self, BatchError> {
        let client = redis::Client::open(url).map_err(re)?;
        let connection = ConnectionManager::new(client).await.map_err(re)?;
        Ok(Self::new(connection))
    }

    fn conn(&self) -> ConnectionManager {
        self.connection.clone()
    }
}

fn re(error: redis::RedisError) -> BatchError {
    BatchError::repository(error)
}

fn status_name(status: BatchStatus) -> Result<&'static str, BatchError> {
    Ok(match status {
        BatchStatus::Starting => STARTING,
        BatchStatus::Started => STARTED,
        BatchStatus::Completed => COMPLETED,
        BatchStatus::Failed => FAILED,
        BatchStatus::Stopped => STOPPED,
        BatchStatus::Abandoned => ABANDONED,
        // `BatchStatus` is `#[non_exhaustive]`, so a new variant compiles here
        // and has to be caught at runtime.
        other => {
            return Err(BatchError::repository(format!(
                "status {other:?} has no stored representation"
            )));
        }
    })
}

fn status_from(name: &str) -> Result<BatchStatus, BatchError> {
    Ok(match name {
        STARTING => BatchStatus::Starting,
        STARTED => BatchStatus::Started,
        COMPLETED => BatchStatus::Completed,
        FAILED => BatchStatus::Failed,
        STOPPED => BatchStatus::Stopped,
        ABANDONED => BatchStatus::Abandoned,
        other => return Err(BatchError::repository(format!("unknown status {other:?}"))),
    })
}

fn encode_context(context: &ExecutionContext) -> Result<String, BatchError> {
    serde_json::to_string(context).map_err(BatchError::repository)
}

fn decode_context(raw: &str) -> Result<ExecutionContext, BatchError> {
    serde_json::from_str(raw).map_err(BatchError::repository)
}

fn encode_parameters(parameters: &JobParameters) -> Result<String, BatchError> {
    // `JobParameters` is a newtype over a `BTreeMap`, so this is key-ordered
    // and therefore a stable identity key. A `HashMap` here would make the same
    // parameters hash differently between processes.
    serde_json::to_string(parameters).map_err(BatchError::repository)
}

fn count(value: i64) -> Result<usize, BatchError> {
    usize::try_from(value).map_err(|_| BatchError::repository(format!("negative counter {value}")))
}

/// Hashed once at first use rather than on every call: `Script::new` computes
/// the script's SHA-1 eagerly, and `invoke_async` sends `EVALSHA` against it.
///
/// Resolve-or-create in one round trip. Check-then-act across two commands is a
/// TOCTOU race two schedulers would both win.
static FIND_OR_CREATE_INSTANCE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
    local id = redis.call('GET', KEYS[1])
    if id then return tonumber(id) end
    id = redis.call('INCR', KEYS[2])
    redis.call('SET', KEYS[1], id)
    redis.call('HSET', ARGV[3] .. ':instance:' .. id,
               'job_name', ARGV[1], 'parameters', ARGV[2])
    return id
",
    )
});

/// Returns -1 when the instance does not exist, so an unknown parent is an
/// error rather than an orphan row.
static CREATE_EXECUTION: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
    if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
    local id = redis.call('INCR', KEYS[1])
    redis.call('HSET', ARGV[4] .. ':execution:' .. id,
               'instance_id', ARGV[1], 'status', ARGV[2], 'context', ARGV[3])
    redis.call('RPUSH', KEYS[3], id)
    return id
",
    )
});

/// Returns 0 when the execution is unknown, so an update cannot silently
/// insert.
static UPDATE_EXECUTION: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    redis.call('HSET', KEYS[1], 'status', ARGV[1], 'context', ARGV[2])
    return 1
",
    )
});

/// Returns the current status so the caller can distinguish "no such
/// execution" from "that one is already finished" — different operator
/// responses.
static ABANDON_EXECUTION: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
    if redis.call('EXISTS', KEYS[1]) == 0 then return 'missing' end
    local status = redis.call('HGET', KEYS[1], 'status')
    if status == ARGV[1] then return status end
    redis.call('HSET', KEYS[1], 'status', ARGV[2])
    return 'ok'
",
    )
});

static CREATE_STEP_EXECUTION: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
    if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
    local instance_id = redis.call('HGET', KEYS[2], 'instance_id')
    local id = redis.call('INCR', KEYS[1])
    redis.call('HSET', ARGV[4] .. ':step:' .. id,
               'job_execution_id', ARGV[1], 'step_name', ARGV[2], 'status', ARGV[3],
               'context', '{}', 'read', 0, 'write', 0, 'filter', 0, 'skip', 0)
    redis.call('RPUSH', KEYS[3], id)
    redis.call('RPUSH', ARGV[4] .. ':instance:' .. instance_id .. ':step:' .. ARGV[2], id)
    return id
",
    )
});

static UPDATE_STEP_EXECUTION: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
    if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
    redis.call('HSET', KEYS[1], 'status', ARGV[1], 'context', ARGV[2],
               'read', ARGV[3], 'write', ARGV[4], 'filter', ARGV[5], 'skip', ARGV[6])
    return 1
",
    )
});

/// Refuses a record the engine believes exists but Redis does not have.
///
/// `HGETALL` on an absent key returns an *empty hash*, not an error. Every
/// field lookup below has a default, so without this check an evicted or
/// flushed record reads back as a pristine `STARTING` with an empty bookmark —
/// indistinguishable from a step that has never run. The restart would then
/// re-read the input from the beginning and re-write every already-committed
/// item, which is exactly the duplicate delivery this framework exists to
/// prevent, and it would do it silently.
///
/// Failing loudly is the right trade: the run stops, and the operator learns
/// that `maxmemory-policy` is not `noeviction` before the data is duplicated
/// rather than after.
fn missing(
    fields: &std::collections::HashMap<String, String>,
    kind: &str,
    id: i64,
) -> Result<(), BatchError> {
    if fields.is_empty() {
        return Err(BatchError::repository(format!(
            "{kind} {id} is missing from redis: it was evicted or the store was flushed, \
             so restart safety cannot be guaranteed. Check `maxmemory-policy noeviction` \
             and `appendfsync always`"
        )));
    }
    Ok(())
}

/// Rebuilds a [`JobExecution`] from its hash fields.
fn execution_from(
    id: i64,
    fields: &std::collections::HashMap<String, String>,
) -> Result<JobExecution, BatchError> {
    missing(fields, "execution", id)?;

    let instance_id: i64 = fields
        .get("instance_id")
        .ok_or_else(|| BatchError::repository("execution has no instance_id"))?
        .parse()
        .map_err(BatchError::repository)?;

    let mut execution = JobExecution::new(JobExecutionId::new(id), JobInstanceId::new(instance_id));
    execution.set_status(status_from(
        fields.get("status").map_or(STARTING, String::as_str),
    )?);
    execution.set_execution_context(decode_context(
        fields.get("context").map_or("{}", String::as_str),
    )?);
    Ok(execution)
}

/// Rebuilds a [`StepExecution`], including its counters.
fn step_from(
    id: i64,
    fields: &std::collections::HashMap<String, String>,
) -> Result<StepExecution, BatchError> {
    missing(fields, "step execution", id)?;

    let job_execution_id: i64 = fields
        .get("job_execution_id")
        .ok_or_else(|| BatchError::repository("step execution has no job_execution_id"))?
        .parse()
        .map_err(BatchError::repository)?;
    let step_name = fields
        .get("step_name")
        .ok_or_else(|| BatchError::repository("step execution has no step_name"))?;

    let mut step = StepExecution::new(
        StepExecutionId::new(id),
        JobExecutionId::new(job_execution_id),
        step_name,
    );
    step.set_status(status_from(
        fields.get("status").map_or(STARTING, String::as_str),
    )?);
    step.set_execution_context(decode_context(
        fields.get("context").map_or("{}", String::as_str),
    )?);

    // Counters are private and fold-only, so they are restored through a
    // contribution rather than assigned.
    let read = counter(fields, "read")?;
    let write = counter(fields, "write")?;
    let filter = counter(fields, "filter")?;
    let skip = counter(fields, "skip")?;
    let mut contribution = StepContribution::new();
    contribution.increment_read(count(read)?);
    contribution.increment_write(count(write)?);
    contribution.increment_filter(count(filter)?);
    contribution.increment_skip(count(skip)?);
    step.apply(&contribution);

    Ok(step)
}

fn counter(
    fields: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<i64, BatchError> {
    fields
        .get(name)
        .map_or(Ok(0), |raw| raw.parse().map_err(BatchError::repository))
}

impl JobRepository for RedisJobRepository {
    /// A `MULTI`/`EXEC` pipeline. Nothing is sent until commit, so a rollback
    /// is genuinely a no-op rather than a compensating write.
    type Tx = redis::Pipeline;

    async fn begin(&self) -> Result<Self::Tx, BatchError> {
        let mut pipeline = redis::Pipeline::new();
        pipeline.atomic();
        Ok(pipeline)
    }

    async fn commit(&self, tx: Self::Tx) -> Result<(), BatchError> {
        tx.query_async::<()>(&mut self.conn()).await.map_err(re)
    }

    async fn rollback(&self, _tx: Self::Tx) -> Result<(), BatchError> {
        // The pipeline was never sent. Dropping it *is* the rollback.
        Ok(())
    }

    async fn update_step_execution_in(
        &self,
        tx: &mut Self::Tx,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        // Queued, not executed: a pipeline cannot branch, so unlike
        // `update_step_execution` this cannot reject an unknown id. The engine
        // only ever passes a record it just minted.
        tx.cmd("HSET")
            .arg(step_key(step_execution.id().get()))
            .arg("status")
            .arg(status_name(step_execution.status())?)
            .arg("context")
            .arg(encode_context(step_execution.execution_context())?)
            .arg("read")
            .arg(step_execution.read_count())
            .arg("write")
            .arg(step_execution.write_count())
            .arg("filter")
            .arg(step_execution.filter_count())
            .arg("skip")
            .arg(step_execution.skip_count())
            .ignore();
        Ok(())
    }

    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        let encoded = encode_parameters(parameters)?;
        let id: i64 = FIND_OR_CREATE_INSTANCE
            .key(instance_lookup_key(job_name, &encoded))
            .key(seq_key())
            .arg(job_name)
            .arg(&encoded)
            .arg(NS)
            .invoke_async(&mut self.conn())
            .await
            .map_err(re)?;

        Ok(JobInstance::new(
            JobInstanceId::new(id),
            job_name,
            parameters.clone(),
        ))
    }

    async fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<Option<JobInstance>, BatchError> {
        let encoded = encode_parameters(parameters)?;
        let id: Option<i64> = self
            .conn()
            .get(instance_lookup_key(job_name, &encoded))
            .await
            .map_err(re)?;

        Ok(id.map(|id| JobInstance::new(JobInstanceId::new(id), job_name, parameters.clone())))
    }

    async fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        let id: i64 = CREATE_EXECUTION
            .key(seq_key())
            .key(instance_key(instance_id.get()))
            .key(executions_key(instance_id.get()))
            .arg(instance_id.get())
            .arg(STARTING)
            .arg("{}")
            .arg(NS)
            .invoke_async(&mut self.conn())
            .await
            .map_err(re)?;

        if id < 0 {
            return Err(BatchError::repository(format!(
                "unknown instance {instance_id:?}"
            )));
        }
        Ok(JobExecution::new(JobExecutionId::new(id), instance_id))
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        let updated: i64 = UPDATE_EXECUTION
            .key(execution_key(execution.id().get()))
            .arg(status_name(execution.status())?)
            .arg(encode_context(execution.execution_context())?)
            .invoke_async(&mut self.conn())
            .await
            .map_err(re)?;

        if updated == 0 {
            return Err(BatchError::repository(format!(
                "unknown execution {:?}",
                execution.id()
            )));
        }
        Ok(())
    }

    async fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<JobExecution>, BatchError> {
        let id: Option<i64> = self
            .conn()
            .lindex(executions_key(instance_id.get()), -1)
            .await
            .map_err(re)?;

        match id {
            Some(id) => Ok(Some(self.load_execution(id).await?)),
            None => Ok(None),
        }
    }

    async fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Vec<JobExecution>, BatchError> {
        let ids: Vec<i64> = self
            .conn()
            .lrange(executions_key(instance_id.get()), 0, -1)
            .await
            .map_err(re)?;

        let mut executions = Vec::with_capacity(ids.len());
        for id in ids {
            executions.push(self.load_execution(id).await?);
        }
        Ok(executions)
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        let outcome: String = ABANDON_EXECUTION
            .key(execution_key(execution_id.get()))
            .arg(COMPLETED)
            .arg(ABANDONED)
            .invoke_async(&mut self.conn())
            .await
            .map_err(re)?;

        match outcome.as_str() {
            "ok" => Ok(()),
            "missing" => Err(BatchError::repository(format!(
                "unknown execution {execution_id:?}"
            ))),
            status => Err(BatchError::CannotAbandon {
                execution_id,
                status: status_from(status)?,
            }),
        }
    }

    async fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> Result<StepExecution, BatchError> {
        let id: i64 = CREATE_STEP_EXECUTION
            .key(seq_key())
            .key(execution_key(job_execution_id.get()))
            .key(execution_steps_key(job_execution_id.get()))
            .arg(job_execution_id.get())
            .arg(step_name)
            .arg(STARTING)
            .arg(NS)
            .invoke_async(&mut self.conn())
            .await
            .map_err(re)?;

        if id < 0 {
            return Err(BatchError::repository(format!(
                "unknown job execution {job_execution_id:?}"
            )));
        }
        Ok(StepExecution::new(
            StepExecutionId::new(id),
            job_execution_id,
            step_name,
        ))
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        let updated: i64 = UPDATE_STEP_EXECUTION
            .key(step_key(step_execution.id().get()))
            .arg(status_name(step_execution.status())?)
            .arg(encode_context(step_execution.execution_context())?)
            .arg(step_execution.read_count())
            .arg(step_execution.write_count())
            .arg(step_execution.filter_count())
            .arg(step_execution.skip_count())
            .invoke_async(&mut self.conn())
            .await
            .map_err(re)?;

        if updated == 0 {
            return Err(BatchError::repository(format!(
                "unknown step execution {:?}",
                step_execution.id()
            )));
        }
        Ok(())
    }

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        // Indexed per (instance, step name) at creation time, so this is one
        // lookup rather than a scan back through every attempt.
        let id: Option<i64> = self
            .conn()
            .lindex(instance_step_key(instance_id.get(), step_name), -1)
            .await
            .map_err(re)?;

        match id {
            Some(id) => Ok(Some(self.load_step(id).await?)),
            None => Ok(None),
        }
    }

    async fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Vec<StepExecution>, BatchError> {
        let ids: Vec<i64> = self
            .conn()
            .lrange(execution_steps_key(job_execution_id.get()), 0, -1)
            .await
            .map_err(re)?;

        let mut steps = Vec::with_capacity(ids.len());
        for id in ids {
            steps.push(self.load_step(id).await?);
        }
        Ok(steps)
    }
}

impl RedisJobRepository {
    async fn load_execution(&self, id: i64) -> Result<JobExecution, BatchError> {
        let fields: std::collections::HashMap<String, String> =
            self.conn().hgetall(execution_key(id)).await.map_err(re)?;
        execution_from(id, &fields)
    }

    async fn load_step(&self, id: i64) -> Result<StepExecution, BatchError> {
        let fields: std::collections::HashMap<String, String> =
            self.conn().hgetall(step_key(id)).await.map_err(re)?;
        step_from(id, &fields)
    }
}
