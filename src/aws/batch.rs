use std::collections::HashMap;

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_batch::{
    Client,
    types::{JobStatus, JobSummary as AwsJobSummary},
};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

use super::{BatchApi, HomeJobsPage};
use crate::domain::{
    ApiError, ArrayProgress, AttemptDetail, BatchStatus, ChildJobSummary, ContainerInfo, HomeTab,
    JobDetail, JobKind, JobPage, JobQueue, JobQueueKind, JobSummary,
};

#[derive(Clone)]
pub struct RealAws {
    pub(super) batch: Client,
    pub(super) shared_config: SdkConfig,
    pub(super) request_limit: std::sync::Arc<Semaphore>,
    concurrency: usize,
}

impl RealAws {
    pub fn new(shared_config: SdkConfig) -> Self {
        Self {
            batch: Client::new(&shared_config),
            shared_config,
            request_limit: std::sync::Arc::new(Semaphore::new(8)),
            concurrency: 8,
        }
    }

    pub(super) async fn request_permit(&self) -> OwnedSemaphorePermit {
        self.request_limit
            .clone()
            .acquire_owned()
            .await
            .expect("AWS request semaphore is never closed")
    }

    async fn list_one_page(
        &self,
        queue: &JobQueue,
        status: BatchStatus,
        max_results: i32,
        next_token: Option<String>,
    ) -> Result<(Vec<JobSummary>, Option<String>), ApiError> {
        let _permit = self.request_permit().await;
        let output = self
            .batch
            .list_jobs()
            .job_queue(queue.arn.clone())
            .job_status(to_aws_status(status))
            .max_results(max_results)
            .set_next_token(next_token)
            .send()
            .await
            .map_err(|error| ApiError::new("AWS Batch ListJobs", error.to_string()))?;

        let jobs = output
            .job_summary_list()
            .iter()
            .filter_map(|summary| map_job_summary(summary, &queue.name))
            .collect();
        Ok((jobs, output.next_token().map(ToOwned::to_owned)))
    }

    async fn list_all_for_source(
        &self,
        queue: JobQueue,
        status: BatchStatus,
    ) -> Result<(String, Vec<JobSummary>), (String, ApiError)> {
        let mut jobs = Vec::new();
        let mut token = None;
        loop {
            let (mut page, next) = self
                .list_one_page(&queue, status, 1_000, token)
                .await
                .map_err(|error| (queue.name.clone(), error))?;
            jobs.append(&mut page);
            if next.is_none() {
                break;
            }
            token = next;
        }
        Ok((queue.name, jobs))
    }

    async fn list_complete_tab(
        &self,
        queues: &[JobQueue],
        tab: HomeTab,
    ) -> Result<HomeJobsPage, ApiError> {
        let statuses: &[BatchStatus] = match tab {
            HomeTab::Active => &BatchStatus::ACTIVE,
            HomeTab::Failed => &[BatchStatus::Failed],
            HomeTab::Recent => &BatchStatus::TERMINAL,
            HomeTab::All => unreachable!("All uses lazy source pagination"),
        };
        let sources: Vec<_> = queues
            .iter()
            .flat_map(|queue| {
                statuses
                    .iter()
                    .copied()
                    .map(move |status| (queue.clone(), status))
            })
            .collect();

        let mut jobs = Vec::new();
        let mut failed_queues = Vec::new();
        let mut responses = stream::iter(sources)
            .map(|(queue, status)| async move {
                if tab == HomeTab::Recent {
                    self.list_one_page(&queue, status, 500, None)
                        .await
                        .map(|(jobs, _)| (queue.name.clone(), jobs))
                        .map_err(|error| (queue.name.clone(), error))
                } else {
                    self.list_all_for_source(queue, status).await
                }
            })
            .buffer_unordered(self.concurrency);

        while let Some(result) = responses.next().await {
            match result {
                Ok((_, mut source_jobs)) => jobs.append(&mut source_jobs),
                Err((queue, error)) => {
                    warn!(queue, %error, "partial home refresh failure");
                    if !failed_queues.contains(&queue) {
                        failed_queues.push(queue);
                    }
                }
            }
        }

        deduplicate(&mut jobs);
        match tab {
            HomeTab::Active => jobs.sort_by(active_order),
            HomeTab::Failed => jobs.sort_by(|a, b| b.stopped_at.cmp(&a.stopped_at)),
            HomeTab::Recent => {
                jobs.sort_by(|a, b| b.stopped_at.cmp(&a.stopped_at));
                jobs.truncate(500);
            }
            HomeTab::All => unreachable!(),
        }

        Ok(HomeJobsPage {
            jobs,
            next_token: None,
            failed_queues,
        })
    }

    async fn list_all_lazy(
        &self,
        queues: &[JobQueue],
        next_token: Option<String>,
    ) -> Result<HomeJobsPage, ApiError> {
        if queues.is_empty() {
            return Ok(HomeJobsPage {
                jobs: Vec::new(),
                next_token: None,
                failed_queues: Vec::new(),
            });
        }
        let mut cursor = next_token
            .as_deref()
            .map(serde_json::from_str::<AllCursor>)
            .transpose()
            .map_err(|error| ApiError::new("All pagination cursor", error.to_string()))?
            .unwrap_or_default();

        while cursor.queue_index < queues.len() {
            let queue = &queues[cursor.queue_index];
            let statuses = BatchStatus::ALL;
            if cursor.status_index >= statuses.len() {
                cursor.queue_index += 1;
                cursor.status_index = 0;
                cursor.aws_token = None;
                continue;
            }
            let status = statuses[cursor.status_index];
            match self
                .list_one_page(queue, status, 100, cursor.aws_token.take())
                .await
            {
                Ok((mut jobs, token)) => {
                    if let Some(token) = token {
                        cursor.aws_token = Some(token);
                    } else {
                        cursor.status_index += 1;
                    }
                    jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    let has_more = cursor.queue_index < queues.len()
                        && (cursor.status_index < statuses.len()
                            || cursor.queue_index + 1 < queues.len());
                    let next_token = has_more.then(|| encode_cursor(&cursor)).transpose()?;
                    return Ok(HomeJobsPage {
                        jobs,
                        next_token,
                        failed_queues: Vec::new(),
                    });
                }
                Err(error) => {
                    let failed = queue.name.clone();
                    warn!(queue = failed, %error, "All page source failed");
                    cursor.status_index += 1;
                    return Ok(HomeJobsPage {
                        jobs: Vec::new(),
                        next_token: Some(encode_cursor(&cursor)?),
                        failed_queues: vec![failed],
                    });
                }
            }
        }
        Ok(HomeJobsPage {
            jobs: Vec::new(),
            next_token: None,
            failed_queues: Vec::new(),
        })
    }

    async fn describe_in_chunks(&self, job_ids: &[String]) -> Result<Vec<JobDetail>, ApiError> {
        let mut details = Vec::new();
        for chunk in job_ids.chunks(100) {
            let _permit = self.request_permit().await;
            let output = self
                .batch
                .describe_jobs()
                .set_jobs(Some(chunk.to_vec()))
                .send()
                .await
                .map_err(|error| ApiError::new("AWS Batch DescribeJobs", error.to_string()))?;
            details.extend(output.jobs().iter().filter_map(map_job_detail));
        }
        Ok(details)
    }

    async fn list_child_page(
        &self,
        parent_job_id: &str,
        status: BatchStatus,
        next_token: Option<String>,
    ) -> Result<(Vec<ChildJobSummary>, Option<String>, Vec<String>), ApiError> {
        let permit = self.request_permit().await;
        let output = self
            .batch
            .list_jobs()
            .array_job_id(parent_job_id)
            .job_status(to_aws_status(status))
            .max_results(100)
            .set_next_token(next_token)
            .send()
            .await
            .map_err(|error| ApiError::new("AWS Batch ListJobs children", error.to_string()))?;
        drop(permit);

        let mut children: Vec<_> = output
            .job_summary_list()
            .iter()
            .filter_map(map_child_summary)
            .collect();
        let ids: Vec<_> = children.iter().map(|child| child.job_id.clone()).collect();
        let mut warnings = Vec::new();
        if !ids.is_empty() {
            match self.describe_in_chunks(&ids).await {
                Ok(details) => {
                    let by_id: HashMap<_, _> = details
                        .into_iter()
                        .map(|detail| (detail.summary.job_id.clone(), detail))
                        .collect();
                    for child in &mut children {
                        if let Some(detail) = by_id.get(&child.job_id) {
                            child.attempts = Some(detail.attempts.len() as u32);
                            child.max_attempts = detail.max_attempts;
                            child.exit_code = detail
                                .latest_attempt()
                                .and_then(|attempt| attempt.container.exit_code)
                                .or(detail.container.exit_code);
                            child.status_reason = detail
                                .latest_attempt()
                                .and_then(|attempt| attempt.status_reason.clone())
                                .or_else(|| child.status_reason.clone());
                        }
                    }
                }
                Err(error) => {
                    warn!(%error, "child detail enrichment failed");
                    warnings.push(error.to_string());
                }
            }
        }

        Ok((
            children,
            output.next_token().map(ToOwned::to_owned),
            warnings,
        ))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AllCursor {
    queue_index: usize,
    status_index: usize,
    aws_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ChildCursor {
    status_index: usize,
    aws_token: Option<String>,
}

#[async_trait]
impl BatchApi for RealAws {
    async fn discover_queues(&self) -> Result<Vec<JobQueue>, ApiError> {
        let mut queues = Vec::new();
        let mut token = None;
        loop {
            let _permit = self.request_permit().await;
            let output = self
                .batch
                .describe_job_queues()
                .max_results(100)
                .set_next_token(token)
                .send()
                .await
                .map_err(|error| ApiError::new("AWS Batch DescribeJobQueues", error.to_string()))?;
            for queue in output.job_queues() {
                let Some(name) = queue.job_queue_name() else {
                    continue;
                };
                let Some(arn) = queue.job_queue_arn() else {
                    continue;
                };
                let kind = queue_kind(queue);
                if !kind.is_container() {
                    debug!(
                        queue = name,
                        kind = ?kind,
                        "excluding non-container job queue"
                    );
                    continue;
                }
                queues.push(JobQueue {
                    name: name.to_owned(),
                    arn: arn.to_owned(),
                    kind,
                    enabled: queue
                        .state()
                        .is_some_and(|state| state.as_str() == "ENABLED"),
                });
            }
            token = output.next_token().map(ToOwned::to_owned);
            if token.is_none() {
                break;
            }
        }
        queues.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(queues)
    }

    async fn list_home_jobs(
        &self,
        queues: &[JobQueue],
        tab: HomeTab,
        next_token: Option<String>,
    ) -> Result<HomeJobsPage, ApiError> {
        if tab == HomeTab::All {
            self.list_all_lazy(queues, next_token).await
        } else {
            self.list_complete_tab(queues, tab).await
        }
    }

    async fn describe_jobs(&self, job_ids: &[String]) -> Result<Vec<JobDetail>, ApiError> {
        self.describe_in_chunks(job_ids).await
    }

    async fn list_children(
        &self,
        parent_job_id: &str,
        status: Option<BatchStatus>,
        next_token: Option<String>,
    ) -> Result<JobPage<ChildJobSummary>, ApiError> {
        if let Some(status) = status {
            let (items, next_token, warnings) = self
                .list_child_page(parent_job_id, status, next_token)
                .await?;
            return Ok(JobPage {
                items,
                next_token,
                warnings,
            });
        }

        let mut cursor = next_token
            .as_deref()
            .map(serde_json::from_str::<ChildCursor>)
            .transpose()
            .map_err(|error| ApiError::new("Children pagination cursor", error.to_string()))?
            .unwrap_or_default();
        while cursor.status_index < BatchStatus::ALL.len() {
            let child_status = BatchStatus::ALL[cursor.status_index];
            let (items, aws_token, warnings) = self
                .list_child_page(parent_job_id, child_status, cursor.aws_token.take())
                .await?;
            if let Some(token) = aws_token {
                cursor.aws_token = Some(token);
            } else {
                cursor.status_index += 1;
            }
            let has_more = cursor.status_index < BatchStatus::ALL.len();
            if !items.is_empty() || !warnings.is_empty() || !has_more {
                return Ok(JobPage {
                    items,
                    next_token: has_more.then(|| encode_child_cursor(&cursor)).transpose()?,
                    warnings,
                });
            }
        }
        Ok(JobPage {
            items: Vec::new(),
            next_token: None,
            warnings: Vec::new(),
        })
    }
}

fn to_aws_status(status: BatchStatus) -> JobStatus {
    match status {
        BatchStatus::Submitted => JobStatus::Submitted,
        BatchStatus::Pending => JobStatus::Pending,
        BatchStatus::Runnable => JobStatus::Runnable,
        BatchStatus::Starting => JobStatus::Starting,
        BatchStatus::Running => JobStatus::Running,
        BatchStatus::Succeeded => JobStatus::Succeeded,
        BatchStatus::Failed => JobStatus::Failed,
        BatchStatus::Unknown => unreachable!("unknown status cannot be queried"),
    }
}

fn map_job_summary(summary: &AwsJobSummary, queue: &str) -> Option<JobSummary> {
    let job_id = summary.job_id()?.to_owned();
    let array = summary.array_properties();
    if array.and_then(|properties| properties.index()).is_some() {
        return None;
    }
    let kind = if let Some(size) = array.and_then(|properties| properties.size()) {
        let properties = array.expect("size came from array properties");
        let counts = properties.status_summary();
        JobKind::ArrayParent(ArrayProgress {
            size: to_u32(size),
            submitted: summary_count(counts, "SUBMITTED"),
            pending: summary_count(counts, "PENDING"),
            runnable: summary_count(counts, "RUNNABLE"),
            starting: summary_count(counts, "STARTING"),
            running: summary_count(counts, "RUNNING"),
            succeeded: summary_count(counts, "SUCCEEDED"),
            failed: summary_count(counts, "FAILED"),
            summary_updated_at: timestamp(properties.status_summary_last_updated_at()),
        })
    } else {
        JobKind::Single
    };
    Some(JobSummary {
        job_id,
        job_name: summary.job_name()?.to_owned(),
        queue: queue.to_owned(),
        definition: summary.job_definition().map(ToOwned::to_owned),
        status: BatchStatus::from_aws(summary.status().map_or("UNKNOWN", |s| s.as_str())),
        status_reason: summary.status_reason().map(ToOwned::to_owned),
        created_at: timestamp(summary.created_at()),
        started_at: timestamp(summary.started_at()),
        stopped_at: timestamp(summary.stopped_at()),
        kind,
        is_mnp: summary.node_properties().is_some(),
    })
}

fn map_child_summary(summary: &AwsJobSummary) -> Option<ChildJobSummary> {
    let index = to_u32(summary.array_properties()?.index()?);
    Some(ChildJobSummary {
        job_id: summary.job_id()?.to_owned(),
        index,
        status: BatchStatus::from_aws(summary.status().map_or("UNKNOWN", |s| s.as_str())),
        started_at: timestamp(summary.started_at()),
        stopped_at: timestamp(summary.stopped_at()),
        exit_code: summary
            .container()
            .and_then(|container| container.exit_code()),
        attempts: None,
        max_attempts: None,
        status_reason: summary
            .container()
            .and_then(|container| container.reason())
            .or_else(|| summary.status_reason())
            .map(ToOwned::to_owned),
    })
}

fn map_job_detail(detail: &aws_sdk_batch::types::JobDetail) -> Option<JobDetail> {
    let container = detail
        .ecs_properties()
        .and_then(|properties| properties.task_properties().first())
        .and_then(|task| task.containers().first())
        .map(map_task_container)
        .or_else(|| {
            detail
                .eks_properties()
                .and_then(|properties| properties.pod_properties())
                .and_then(|pod| pod.containers().first())
                .map(map_eks_container)
        })
        .or_else(|| detail.container().map(map_container))
        .unwrap_or_default();
    let array = detail.array_properties();
    let kind = if let Some(size) = array.and_then(|properties| properties.size()) {
        let properties = array.expect("size came from array properties");
        let counts = properties.status_summary();
        JobKind::ArrayParent(ArrayProgress {
            size: to_u32(size),
            submitted: summary_count(counts, "SUBMITTED"),
            pending: summary_count(counts, "PENDING"),
            runnable: summary_count(counts, "RUNNABLE"),
            starting: summary_count(counts, "STARTING"),
            running: summary_count(counts, "RUNNING"),
            succeeded: summary_count(counts, "SUCCEEDED"),
            failed: summary_count(counts, "FAILED"),
            summary_updated_at: timestamp(properties.status_summary_last_updated_at()),
        })
    } else {
        JobKind::Single
    };
    let summary = JobSummary {
        job_id: detail.job_id()?.to_owned(),
        job_name: detail.job_name()?.to_owned(),
        queue: detail.job_queue().unwrap_or_default().to_owned(),
        definition: detail.job_definition().map(ToOwned::to_owned),
        status: BatchStatus::from_aws(detail.status().map_or("UNKNOWN", |s| s.as_str())),
        status_reason: detail.status_reason().map(ToOwned::to_owned),
        created_at: timestamp(detail.created_at()),
        started_at: timestamp(detail.started_at()),
        stopped_at: timestamp(detail.stopped_at()),
        kind,
        is_mnp: detail.node_properties().is_some(),
    };
    let mut attempts: Vec<_> = detail
        .attempts()
        .iter()
        .enumerate()
        .map(|(index, attempt)| {
            let mut attempt_container = container.clone();
            if let Some(value) = attempt
                .task_properties()
                .first()
                .and_then(|task| task.containers().first())
            {
                attempt_container.name = value.name().map(ToOwned::to_owned);
                attempt_container.exit_code = value.exit_code();
                attempt_container.reason = value.reason().map(ToOwned::to_owned);
                attempt_container.log_stream_name = value.log_stream_name().map(ToOwned::to_owned);
            } else if let Some(value) = attempt.container() {
                attempt_container.exit_code = value.exit_code();
                attempt_container.reason = value.reason().map(ToOwned::to_owned);
                attempt_container.log_stream_name = value.log_stream_name().map(ToOwned::to_owned);
            }
            AttemptDetail {
                number: index as u32 + 1,
                started_at: timestamp(attempt.started_at()),
                stopped_at: timestamp(attempt.stopped_at()),
                status_reason: attempt.status_reason().map(ToOwned::to_owned),
                container: attempt_container,
            }
        })
        .collect();
    if attempts.is_empty() {
        attempts = detail
            .eks_attempts()
            .iter()
            .enumerate()
            .map(|(index, attempt)| {
                let mut attempt_container = container.clone();
                if let Some(value) = attempt.containers().first() {
                    attempt_container.name = value.name().map(ToOwned::to_owned);
                    attempt_container.exit_code = value.exit_code();
                    attempt_container.reason = value.reason().map(ToOwned::to_owned);
                }
                AttemptDetail {
                    number: index as u32 + 1,
                    started_at: timestamp(attempt.started_at()),
                    stopped_at: timestamp(attempt.stopped_at()),
                    status_reason: attempt.status_reason().map(ToOwned::to_owned),
                    container: attempt_container,
                }
            })
            .collect();
    }
    let parameters = detail
        .parameters()
        .into_iter()
        .flat_map(|values| values.iter())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let raw_json = serde_json::to_string_pretty(&serde_json::json!({
        "jobId": &summary.job_id,
        "jobName": &summary.job_name,
        "jobQueue": &summary.queue,
        "jobDefinition": &summary.definition,
        "status": summary.status,
        "statusReason": summary.status_reason,
        "createdAt": summary.created_at,
        "startedAt": summary.started_at,
        "stoppedAt": summary.stopped_at,
        "arrayProperties": array.map(|properties| format!("{properties:#?}")),
        "parameters": detail.parameters(),
        "container": detail.container().map(|value| format!("{value:#?}")),
        "attempts": detail.attempts().iter().map(|value| format!("{value:#?}")).collect::<Vec<_>>(),
        "ecsProperties": detail.ecs_properties().map(|value| format!("{value:#?}")),
        "eksProperties": detail.eks_properties().map(|value| format!("{value:#?}")),
        "eksAttempts": detail.eks_attempts().iter().map(|value| format!("{value:#?}")).collect::<Vec<_>>(),
        "nodeProperties": detail.node_properties().map(|value| format!("{value:#?}")),
        "sdkDebug": format!("{detail:#?}"),
    }))
    .unwrap_or_else(|error| format!("{{\"renderError\":\"{error}\"}}"));

    Some(JobDetail {
        summary,
        parent_id: None,
        array_index: array.and_then(|properties| properties.index()).map(to_u32),
        parameters,
        attempts,
        max_attempts: detail
            .retry_strategy()
            .and_then(|strategy| strategy.attempts())
            .map(to_u32),
        container,
        raw_json,
    })
}

fn map_container(container: &aws_sdk_batch::types::ContainerDetail) -> ContainerInfo {
    let mut vcpus = container.vcpus().map(|value| value.to_string());
    let mut memory = container.memory().map(|value| value.to_string());
    for requirement in container.resource_requirements() {
        match requirement.r#type().map(|value| value.as_str()) {
            Some("VCPU") => vcpus = requirement.value().map(ToOwned::to_owned),
            Some("MEMORY") => memory = requirement.value().map(ToOwned::to_owned),
            _ => {}
        }
    }
    let options = container
        .log_configuration()
        .and_then(|configuration| configuration.options());
    ContainerInfo {
        name: None,
        log_driver: container
            .log_configuration()
            .and_then(|configuration| configuration.log_driver())
            .map(|driver| driver.as_str().to_owned()),
        exit_code: container.exit_code(),
        reason: container.reason().map(ToOwned::to_owned),
        vcpus,
        memory: memory.map(|value| format!("{value} MiB")),
        log_stream_name: container.log_stream_name().map(ToOwned::to_owned),
        log_group: options.and_then(|values| values.get("awslogs-group").cloned()),
        logs_region: options.and_then(|values| values.get("awslogs-region").cloned()),
    }
}

fn map_task_container(container: &aws_sdk_batch::types::TaskContainerDetails) -> ContainerInfo {
    let mut vcpus = None;
    let mut memory = None;
    for requirement in container.resource_requirements() {
        match requirement.r#type().map(|value| value.as_str()) {
            Some("VCPU") => vcpus = requirement.value().map(ToOwned::to_owned),
            Some("MEMORY") => {
                memory = requirement.value().map(|value| format!("{value} MiB"));
            }
            _ => {}
        }
    }
    let options = container
        .log_configuration()
        .and_then(|configuration| configuration.options());
    ContainerInfo {
        name: container.name().map(ToOwned::to_owned),
        log_driver: container
            .log_configuration()
            .and_then(|configuration| configuration.log_driver())
            .map(|driver| driver.as_str().to_owned()),
        exit_code: container.exit_code(),
        reason: container.reason().map(ToOwned::to_owned),
        vcpus,
        memory,
        log_stream_name: container.log_stream_name().map(ToOwned::to_owned),
        log_group: options.and_then(|values| values.get("awslogs-group").cloned()),
        logs_region: options.and_then(|values| values.get("awslogs-region").cloned()),
    }
}

fn map_eks_container(container: &aws_sdk_batch::types::EksContainerDetail) -> ContainerInfo {
    let resources = container.resources();
    let resource = |name: &str| {
        resources.and_then(|values| {
            values
                .limits()
                .and_then(|limits| limits.get(name))
                .or_else(|| values.requests().and_then(|requests| requests.get(name)))
                .cloned()
        })
    };
    ContainerInfo {
        name: container.name().map(ToOwned::to_owned),
        exit_code: container.exit_code(),
        reason: container.reason().map(ToOwned::to_owned),
        vcpus: resource("cpu"),
        memory: resource("memory"),
        ..Default::default()
    }
}

fn queue_kind(queue: &aws_sdk_batch::types::JobQueueDetail) -> JobQueueKind {
    queue
        .job_queue_type()
        .map(|value| match value.as_str() {
            "ECS" => JobQueueKind::Ecs,
            "ECS_FARGATE" => JobQueueKind::EcsFargate,
            "EKS" => JobQueueKind::Eks,
            "SAGEMAKER_TRAINING" => JobQueueKind::SageMakerTraining,
            _ => JobQueueKind::Unknown,
        })
        .unwrap_or_else(|| {
            if !queue.compute_environment_order().is_empty() {
                JobQueueKind::Ecs
            } else if !queue.service_environment_order().is_empty() {
                JobQueueKind::SageMakerTraining
            } else {
                JobQueueKind::Unknown
            }
        })
}

fn active_order(a: &JobSummary, b: &JobSummary) -> std::cmp::Ordering {
    let a_failed = a.array_progress().map_or(0, |progress| progress.failed);
    let b_failed = b.array_progress().map_or(0, |progress| progress.failed);
    b_failed
        .cmp(&a_failed)
        .then_with(|| b.created_at.cmp(&a.created_at))
        .then_with(|| a.job_id.cmp(&b.job_id))
}

fn deduplicate(jobs: &mut Vec<JobSummary>) {
    let mut seen = HashMap::new();
    jobs.retain(|job| seen.insert(job.job_id.clone(), ()).is_none());
}

fn summary_count(counts: Option<&HashMap<String, i32>>, status: &str) -> u32 {
    counts
        .and_then(|values| values.get(status))
        .copied()
        .map(to_u32)
        .unwrap_or(0)
}

fn to_u32(value: i32) -> u32 {
    u32::try_from(value).unwrap_or_default()
}

fn timestamp(value: Option<i64>) -> Option<chrono::DateTime<chrono::Utc>> {
    value.and_then(chrono::DateTime::from_timestamp_millis)
}

fn encode_cursor(cursor: &AllCursor) -> Result<String, ApiError> {
    serde_json::to_string(cursor)
        .map_err(|error| ApiError::new("All pagination cursor", error.to_string()))
}

fn encode_child_cursor(cursor: &ChildCursor) -> Result<String, ApiError> {
    serde_json::to_string(cursor)
        .map_err(|error| ApiError::new("Children pagination cursor", error.to_string()))
}

#[cfg(test)]
mod tests {
    use aws_sdk_batch::types::{
        ArrayPropertiesSummary, AttemptEcsTaskDetails, AttemptTaskContainerDetails,
        ComputeEnvironmentOrder, EcsPropertiesDetail, EcsTaskDetails, EksAttemptContainerDetail,
        EksAttemptDetail, EksContainerDetail, EksContainerResourceRequirements,
        EksPodPropertiesDetail, EksPropertiesDetail, JobQueueDetail, JobQueueType,
        LogConfiguration, LogDriver, NodePropertiesSummary, TaskContainerDetails,
    };

    use super::*;

    fn aws_job(id: &str) -> aws_sdk_batch::types::builders::JobSummaryBuilder {
        AwsJobSummary::builder()
            .job_id(id)
            .job_name(format!("job-{id}"))
            .status(JobStatus::Running)
    }

    #[test]
    fn maps_array_parent_counts_and_excludes_child_from_home() {
        let parent = aws_job("parent")
            .status(JobStatus::Pending)
            .array_properties(
                ArrayPropertiesSummary::builder()
                    .size(12)
                    .status_summary("RUNNING", 3)
                    .status_summary("SUCCEEDED", 7)
                    .status_summary("FAILED", 1)
                    .build(),
            )
            .build();
        let mapped = map_job_summary(&parent, "queue").expect("parent should be visible");
        let progress = mapped.array_progress().expect("array progress");
        assert_eq!(progress.size, 12);
        assert_eq!(progress.running, 3);
        assert_eq!(progress.succeeded, 7);
        assert_eq!(progress.failed, 1);
        assert_eq!(mapped.status, BatchStatus::Pending);

        let child = aws_job("child")
            .array_properties(ArrayPropertiesSummary::builder().index(4).build())
            .build();
        assert!(map_job_summary(&child, "queue").is_none());
    }

    #[test]
    fn maps_mnp_as_a_generic_single_job() {
        let summary = aws_job("mnp")
            .node_properties(
                NodePropertiesSummary::builder()
                    .is_main_node(true)
                    .num_nodes(4)
                    .build(),
            )
            .build();
        let mapped = map_job_summary(&summary, "queue").expect("MNP should be visible");
        assert_eq!(mapped.kind, JobKind::Single);
        assert!(mapped.is_mnp);
    }

    #[test]
    fn classifies_supported_and_excluded_queue_types() {
        for (aws_kind, expected) in [
            (JobQueueType::Ecs, JobQueueKind::Ecs),
            (JobQueueType::EcsFargate, JobQueueKind::EcsFargate),
            (JobQueueType::Eks, JobQueueKind::Eks),
            (
                JobQueueType::SagemakerTraining,
                JobQueueKind::SageMakerTraining,
            ),
        ] {
            let queue = JobQueueDetail::builder().job_queue_type(aws_kind).build();
            assert_eq!(queue_kind(&queue), expected);
        }
        assert!(JobQueueKind::Ecs.is_container());
        assert!(JobQueueKind::EcsFargate.is_container());
        assert!(JobQueueKind::Eks.is_container());
        assert!(!JobQueueKind::SageMakerTraining.is_container());
    }

    #[test]
    fn infers_legacy_container_queue_when_type_is_absent() {
        let queue = JobQueueDetail::builder()
            .compute_environment_order(
                ComputeEnvironmentOrder::builder()
                    .order(1)
                    .compute_environment("arn:aws:batch:region:account:compute-environment/test")
                    .build(),
            )
            .build();
        assert_eq!(queue_kind(&queue), JobQueueKind::Ecs);
    }

    #[test]
    fn children_cursor_covers_every_explicit_batch_status() {
        let mut cursor = ChildCursor::default();
        let visited: Vec<_> = std::iter::from_fn(|| {
            let status = BatchStatus::ALL.get(cursor.status_index).copied()?;
            cursor.status_index += 1;
            Some(status)
        })
        .collect();
        assert_eq!(visited, BatchStatus::ALL);

        cursor.status_index = 3;
        cursor.aws_token = Some("opaque-aws-token".into());
        let encoded = encode_child_cursor(&cursor).unwrap();
        let decoded: ChildCursor = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.status_index, 3);
        assert_eq!(decoded.aws_token.as_deref(), Some("opaque-aws-token"));
    }

    #[test]
    fn maps_ecs_primary_container_and_attempt_log_location() {
        let log_configuration = LogConfiguration::builder()
            .log_driver(LogDriver::Awslogs)
            .options("awslogs-group", "/custom/group")
            .options("awslogs-region", "us-west-2")
            .build();
        let primary = TaskContainerDetails::builder()
            .name("main")
            .log_configuration(log_configuration)
            .build();
        let sidecar = TaskContainerDetails::builder()
            .name("sidecar")
            .log_stream_name("sidecar/current")
            .build();
        let attempt_task = AttemptEcsTaskDetails::builder()
            .containers(
                AttemptTaskContainerDetails::builder()
                    .name("main")
                    .exit_code(0)
                    .log_stream_name("main/attempt-1")
                    .build(),
            )
            .containers(
                AttemptTaskContainerDetails::builder()
                    .name("sidecar")
                    .log_stream_name("sidecar/attempt-1")
                    .build(),
            )
            .build();
        let detail = aws_sdk_batch::types::JobDetail::builder()
            .job_id("ecs-job")
            .job_name("ecs-job")
            .ecs_properties(
                EcsPropertiesDetail::builder()
                    .task_properties(
                        EcsTaskDetails::builder()
                            .containers(primary)
                            .containers(sidecar)
                            .build(),
                    )
                    .build(),
            )
            .attempts(
                aws_sdk_batch::types::AttemptDetail::builder()
                    .started_at(1_000)
                    .task_properties(attempt_task)
                    .build(),
            )
            .build();

        let mapped = map_job_detail(&detail).expect("ECS detail");
        assert_eq!(mapped.container.name.as_deref(), Some("main"));
        assert_eq!(
            mapped.latest_attempt().unwrap().container.name.as_deref(),
            Some("main")
        );
        let location = mapped.log_location("ap-northeast-1").unwrap();
        assert_eq!(location.stream, "main/attempt-1");
        assert_eq!(location.group, "/custom/group");
        assert_eq!(location.region, "us-west-2");
    }

    #[test]
    fn maps_eks_primary_container_resources_and_attempts() {
        let resources = EksContainerResourceRequirements::builder()
            .limits("cpu", "4")
            .limits("memory", "8Gi")
            .build();
        let detail = aws_sdk_batch::types::JobDetail::builder()
            .job_id("eks-job")
            .job_name("eks-job")
            .eks_properties(
                EksPropertiesDetail::builder()
                    .pod_properties(
                        EksPodPropertiesDetail::builder()
                            .containers(
                                EksContainerDetail::builder()
                                    .name("main")
                                    .resources(resources)
                                    .build(),
                            )
                            .containers(EksContainerDetail::builder().name("sidecar").build())
                            .build(),
                    )
                    .build(),
            )
            .eks_attempts(
                EksAttemptDetail::builder()
                    .started_at(2_000)
                    .status_reason("completed")
                    .containers(
                        EksAttemptContainerDetail::builder()
                            .name("main")
                            .exit_code(0)
                            .build(),
                    )
                    .containers(EksAttemptContainerDetail::builder().name("sidecar").build())
                    .build(),
            )
            .build();

        let mapped = map_job_detail(&detail).expect("EKS detail");
        assert_eq!(mapped.container.name.as_deref(), Some("main"));
        assert_eq!(mapped.container.vcpus.as_deref(), Some("4"));
        assert_eq!(mapped.container.memory.as_deref(), Some("8Gi"));
        assert_eq!(mapped.attempts.len(), 1);
        assert_eq!(mapped.attempts[0].container.exit_code, Some(0));
        assert_eq!(mapped.log_location("ap-northeast-1"), None);
    }
}
