mod batch;
mod logs;

use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::{
    ApiError, BatchStatus, ChildJobSummary, HomeTab, JobDetail, JobPage, JobQueue, JobSummary,
    LogDirection, LogLocation, LogPage,
};

pub use batch::RealAws;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeJobsPage {
    pub jobs: Vec<JobSummary>,
    pub next_token: Option<String>,
    pub failed_queues: Vec<String>,
}

#[async_trait]
pub trait BatchApi: Send + Sync {
    async fn discover_queues(&self) -> Result<Vec<JobQueue>, ApiError>;

    async fn list_home_jobs(
        &self,
        queues: &[JobQueue],
        tab: HomeTab,
        next_token: Option<String>,
    ) -> Result<HomeJobsPage, ApiError>;

    async fn describe_jobs(&self, job_ids: &[String]) -> Result<Vec<JobDetail>, ApiError>;

    async fn list_children(
        &self,
        parent_job_id: &str,
        status: Option<BatchStatus>,
        next_token: Option<String>,
    ) -> Result<JobPage<ChildJobSummary>, ApiError>;
}

#[async_trait]
pub trait LogsApi: Send + Sync {
    async fn get_log_events(
        &self,
        location: &LogLocation,
        direction: LogDirection,
        next_token: Option<String>,
    ) -> Result<LogPage, ApiError>;
}

pub trait AwsApi: BatchApi + LogsApi {}

impl<T: BatchApi + LogsApi> AwsApi for T {}

pub type SharedAws = Arc<dyn AwsApi>;
