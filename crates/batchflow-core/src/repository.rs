use crate::{BatchError, JobExecution, JobInstance, JobInstanceId, JobParameters};
use std::future::Future;

pub trait JobRepository: Send + Sync {
    fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<JobInstance, BatchError>> + Send;

    fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> impl Future<Output = Result<Option<JobInstance>, BatchError>> + Send;

    fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<JobExecution, BatchError>> + Send;

    fn update_execution(
        &self,
        execution: &JobExecution,
    ) -> impl Future<Output = Result<(), BatchError>> + Send;

    fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> impl Future<Output = Result<Option<JobExecution>, BatchError>> + Send;
}
