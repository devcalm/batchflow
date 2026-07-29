use crate::{ExecutionContext, StepContribution};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BatchStatus {
    Starting,
    Started,
    Completed,
    Failed,
    Stopped,
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum JobParameter {
    String(String),
    Long(i64),
    Bool(bool),
}

/// Identity key of a `JobInstance` (FR-4.2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct JobParameters(BTreeMap<String, JobParameter>);

impl JobParameters {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn with(mut self, key: impl Into<String>, value: JobParameter) -> Self {
        self.0.insert(key.into(), value);
        self
    }

    pub fn get(&self, key: &str) -> Option<&JobParameter> {
        self.0.get(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobInstanceId(i64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobExecutionId(i64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepExecutionId(i64);

impl JobInstanceId {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn get(&self) -> i64 {
        self.0
    }
}

impl JobExecutionId {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn get(&self) -> i64 {
        self.0
    }
}

impl StepExecutionId {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn get(&self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInstance {
    id: JobInstanceId,
    job_name: String,
    parameters: JobParameters,
}

impl JobInstance {
    /// The `id` is assigned by the `JobRepository`, never by the caller.
    pub fn new(id: JobInstanceId, job_name: impl Into<String>, parameters: JobParameters) -> Self {
        Self {
            id,
            job_name: job_name.into(),
            parameters,
        }
    }

    pub fn id(&self) -> JobInstanceId {
        self.id
    }

    pub fn job_name(&self) -> &str {
        &self.job_name
    }

    pub fn parameters(&self) -> &JobParameters {
        &self.parameters
    }
}

/// A single *attempt* at a [`JobInstance`]. A failed instance can have several.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobExecution {
    id: JobExecutionId,
    instance_id: JobInstanceId,
    status: BatchStatus,
    execution_context: ExecutionContext,
}

impl JobExecution {
    pub fn new(id: JobExecutionId, instance_id: JobInstanceId) -> Self {
        Self {
            id,
            instance_id,
            status: BatchStatus::Starting,
            execution_context: ExecutionContext::new(),
        }
    }

    pub fn id(&self) -> JobExecutionId {
        self.id
    }

    pub fn instance_id(&self) -> JobInstanceId {
        self.instance_id
    }

    pub fn status(&self) -> BatchStatus {
        self.status
    }

    pub fn set_status(&mut self, status: BatchStatus) {
        self.status = status;
    }

    /// Job-scoped bookmark bag — for data one step needs to hand to a later
    /// one. Nothing in the engine writes it yet; the per-step context on
    /// [`StepExecution`] is what carries reader positions.
    pub fn execution_context(&self) -> &ExecutionContext {
        &self.execution_context
    }

    pub fn set_execution_context(&mut self, context: ExecutionContext) {
        self.execution_context = context;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepExecution {
    id: StepExecutionId,
    job_execution_id: JobExecutionId,
    step_name: String,
    status: BatchStatus,
    read_count: usize,
    write_count: usize,
    filter_count: usize,
    execution_context: ExecutionContext,
}

impl StepExecution {
    /// The `id` is assigned by the `JobRepository`, never by the caller — which
    /// is precisely why a `Step` cannot produce one of these itself.
    pub fn new(
        id: StepExecutionId,
        job_execution_id: JobExecutionId,
        step_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            job_execution_id,
            step_name: step_name.into(),
            status: BatchStatus::Starting,
            read_count: 0,
            write_count: 0,
            filter_count: 0,
            execution_context: ExecutionContext::new(),
        }
    }

    pub fn id(&self) -> StepExecutionId {
        self.id
    }

    pub fn job_execution_id(&self) -> JobExecutionId {
        self.job_execution_id
    }

    pub fn step_name(&self) -> &str {
        &self.step_name
    }

    pub fn status(&self) -> BatchStatus {
        self.status
    }

    pub fn set_status(&mut self, status: BatchStatus) {
        self.status = status;
    }

    /// Items pulled from the reader.
    pub fn read_count(&self) -> usize {
        self.read_count
    }

    /// Items written (i.e. survived the processor's filter).
    pub fn write_count(&self) -> usize {
        self.write_count
    }

    /// Items dropped by the processor (it returned `None`).
    pub fn filter_count(&self) -> usize {
        self.filter_count
    }

    /// The step's bookmark: where its reader had got to when the last chunk
    /// committed. This is what Phase 9 feeds back in to resume rather than
    /// re-run.
    pub fn execution_context(&self) -> &ExecutionContext {
        &self.execution_context
    }

    pub fn set_execution_context(&mut self, context: ExecutionContext) {
        self.execution_context = context;
    }

    /// Fold a step's reported deltas into this record.
    ///
    /// The engine calls this once the step's work is safe; Phase 11 moves the
    /// call inside the chunk loop, where a rollback drops the contribution
    /// unapplied and these counters stay consistent with what committed.
    pub fn apply(&mut self, contribution: &StepContribution) {
        self.read_count += contribution.read_count();
        self.write_count += contribution.write_count();
        self.filter_count += contribution.filter_count();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn parameters_identity_ignores_insertion_order() {
        let a = JobParameters::new()
            .with("date", JobParameter::String("2026-07-27".into()))
            .with("region", JobParameter::String("eu".into()));
        let b = JobParameters::new()
            .with("region", JobParameter::String("eu".into()))
            .with("date", JobParameter::String("2026-07-27".into()));

        assert_eq!(a, b);
        // Equality alone is not enough: `HashMap` lookup needs matching hashes too.
        // This is the assertion that would catch a switch to a non-deterministic map.
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn different_values_are_different_instances() {
        let eu = JobParameters::new().with("region", JobParameter::String("eu".into()));
        let us = JobParameters::new().with("region", JobParameter::String("us".into()));

        assert_ne!(eu, us);
    }

    #[test]
    fn parameters_are_typed_not_stringly() {
        let long = JobParameters::new().with("v", JobParameter::Long(1));
        let text = JobParameters::new().with("v", JobParameter::String("1".into()));

        // The whole reason `JobParameter` is an enum rather than a String.
        assert_ne!(long, text);
    }

    #[test]
    fn a_missing_parameter_changes_identity() {
        let one = JobParameters::new().with("a", JobParameter::Bool(true));
        let two = JobParameters::new()
            .with("a", JobParameter::Bool(true))
            .with("b", JobParameter::Bool(false));

        assert_ne!(one, two);
    }

    #[test]
    fn ids_of_different_kinds_are_distinct_types() {
        // Not a runtime assertion — the value is that `JobExecutionId::new(1)` would
        // not compile where a `JobInstanceId` is expected.
        let instance = JobInstanceId::new(1);
        let execution = JobExecutionId::new(1);

        assert_eq!(instance.get(), execution.get());
    }
}
