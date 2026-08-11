use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    time::{Duration, Instant},
};

use chrono::{DateTime, Local, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BatchStatus {
    Submitted,
    Pending,
    Runnable,
    Starting,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl BatchStatus {
    pub const ACTIVE: [Self; 5] = [
        Self::Submitted,
        Self::Pending,
        Self::Runnable,
        Self::Starting,
        Self::Running,
    ];

    pub const TERMINAL: [Self; 2] = [Self::Succeeded, Self::Failed];

    pub const ALL: [Self; 7] = [
        Self::Submitted,
        Self::Pending,
        Self::Runnable,
        Self::Starting,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
    ];

    pub fn from_aws(value: &str) -> Self {
        match value {
            "SUBMITTED" => Self::Submitted,
            "PENDING" => Self::Pending,
            "RUNNABLE" => Self::Runnable,
            "STARTING" => Self::Starting,
            "RUNNING" => Self::Running,
            "SUCCEEDED" => Self::Succeeded,
            "FAILED" => Self::Failed,
            _ => Self::Unknown,
        }
    }

    pub fn as_aws(self) -> &'static str {
        match self {
            Self::Submitted => "SUBMITTED",
            Self::Pending => "PENDING",
            Self::Runnable => "RUNNABLE",
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }

    pub fn is_waiting(self) -> bool {
        matches!(
            self,
            Self::Submitted | Self::Pending | Self::Runnable | Self::Starting
        )
    }
}

impl fmt::Display for BatchStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_aws())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArrayProgress {
    pub size: u32,
    pub submitted: u32,
    pub pending: u32,
    pub runnable: u32,
    pub starting: u32,
    pub running: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub summary_updated_at: Option<DateTime<Utc>>,
}

impl ArrayProgress {
    pub fn processed(&self) -> u32 {
        self.succeeded.saturating_add(self.failed)
    }

    pub fn waiting(&self) -> u32 {
        self.submitted
            .saturating_add(self.pending)
            .saturating_add(self.runnable)
            .saturating_add(self.starting)
    }

    pub fn progress(&self) -> f64 {
        if self.size == 0 {
            return 0.0;
        }
        (self.processed() as f64 / self.size as f64).clamp(0.0, 1.0)
    }

    pub fn success_rate(&self) -> Option<f64> {
        let terminal = self.processed();
        (terminal > 0).then(|| self.succeeded as f64 / terminal as f64)
    }

    pub fn count(&self, status: BatchStatus) -> u32 {
        match status {
            BatchStatus::Submitted => self.submitted,
            BatchStatus::Pending => self.pending,
            BatchStatus::Runnable => self.runnable,
            BatchStatus::Starting => self.starting,
            BatchStatus::Running => self.running,
            BatchStatus::Succeeded => self.succeeded,
            BatchStatus::Failed => self.failed,
            BatchStatus::Unknown => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    Single,
    ArrayParent(ArrayProgress),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSummary {
    pub job_id: String,
    pub job_name: String,
    pub queue: String,
    pub definition: Option<String>,
    pub status: BatchStatus,
    pub status_reason: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub kind: JobKind,
    pub is_mnp: bool,
}

impl JobSummary {
    pub fn array_progress(&self) -> Option<&ArrayProgress> {
        match &self.kind {
            JobKind::ArrayParent(progress) => Some(progress),
            JobKind::Single => None,
        }
    }

    pub fn elapsed(&self, now: DateTime<Utc>) -> Option<Duration> {
        let start = self.started_at.or(self.created_at)?;
        let end = self.stopped_at.unwrap_or(now);
        (end >= start).then(|| (end - start).to_std().unwrap_or_default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobQueueKind {
    Ecs,
    EcsFargate,
    Eks,
    SageMakerTraining,
    Unknown,
}

impl JobQueueKind {
    pub fn is_container(self) -> bool {
        matches!(self, Self::Ecs | Self::EcsFargate | Self::Eks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobQueue {
    pub name: String,
    pub arn: String,
    pub kind: JobQueueKind,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HomeTab {
    Active,
    Failed,
    Recent,
    All,
}

impl HomeTab {
    pub const ALL: [Self; 4] = [Self::Active, Self::Failed, Self::Recent, Self::All];

    pub fn title(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Failed => "Failed",
            Self::Recent => "Recent",
            Self::All => "All",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::Active => Self::Failed,
            Self::Failed => Self::Recent,
            Self::Recent => Self::All,
            Self::All => Self::Active,
        }
    }

    pub fn previous(self) -> Self {
        match self {
            Self::Active => Self::All,
            Self::Failed => Self::Active,
            Self::Recent => Self::Failed,
            Self::All => Self::Recent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildJobSummary {
    pub job_id: String,
    pub index: u32,
    pub status: BatchStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub attempts: Option<u32>,
    pub max_attempts: Option<u32>,
    pub status_reason: Option<String>,
}

impl ChildJobSummary {
    pub fn runtime(&self, now: DateTime<Utc>) -> Option<Duration> {
        let start = self.started_at?;
        let end = self.stopped_at.unwrap_or(now);
        (end >= start).then(|| (end - start).to_std().unwrap_or_default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContainerInfo {
    pub name: Option<String>,
    pub log_driver: Option<String>,
    pub exit_code: Option<i32>,
    pub reason: Option<String>,
    pub vcpus: Option<String>,
    pub memory: Option<String>,
    pub log_stream_name: Option<String>,
    pub log_group: Option<String>,
    pub logs_region: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptDetail {
    pub number: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub container: ContainerInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobDetail {
    pub summary: JobSummary,
    pub parent_id: Option<String>,
    pub array_index: Option<u32>,
    pub parameters: BTreeMap<String, String>,
    pub attempts: Vec<AttemptDetail>,
    pub max_attempts: Option<u32>,
    pub container: ContainerInfo,
    pub raw_json: String,
}

impl JobDetail {
    pub fn latest_attempt(&self) -> Option<&AttemptDetail> {
        self.attempts
            .iter()
            .max_by_key(|attempt| (attempt.started_at, attempt.stopped_at, attempt.number))
    }

    pub fn log_location(&self, batch_region: &str) -> Option<LogLocation> {
        let primary = self
            .latest_attempt()
            .map(|attempt| &attempt.container)
            .unwrap_or(&self.container);
        let log_driver = primary
            .log_driver
            .as_deref()
            .or(self.container.log_driver.as_deref());
        if log_driver.is_some_and(|driver| driver != "awslogs") {
            return None;
        }
        let stream = primary.log_stream_name.clone()?;
        let group = primary
            .log_group
            .clone()
            .or_else(|| self.container.log_group.clone())
            .unwrap_or_else(|| "/aws/batch/job".to_owned());
        let region = primary
            .logs_region
            .clone()
            .or_else(|| self.container.logs_region.clone())
            .unwrap_or_else(|| batch_region.to_owned());
        Some(LogLocation {
            group,
            region,
            stream,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPage<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogLocation {
    pub group: String,
    pub region: String,
    pub stream: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogEvent {
    pub timestamp: Option<DateTime<Utc>>,
    pub ingestion_time: Option<DateTime<Utc>>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogDirection {
    Initial,
    Backward,
    Forward,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub events: Vec<LogEvent>,
    pub next_backward_token: Option<String>,
    pub next_forward_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub service: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(service: &'static str, message: impl Into<String>) -> Self {
        Self {
            service,
            message: message.into(),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.service, self.message)
    }
}

impl std::error::Error for ApiError {}

#[derive(Debug, Clone)]
struct ProgressSample {
    at: Instant,
    processed: u32,
}

#[derive(Debug, Clone, Default)]
pub struct RateHistory {
    samples: VecDeque<ProgressSample>,
}

impl RateHistory {
    const WINDOW: Duration = Duration::from_secs(30);

    pub fn record(&mut self, at: Instant, processed: u32) {
        if self
            .samples
            .back()
            .is_some_and(|last| processed < last.processed)
        {
            self.samples.clear();
        }
        self.samples.push_back(ProgressSample { at, processed });
        while self
            .samples
            .front()
            .is_some_and(|sample| at.saturating_duration_since(sample.at) > Self::WINDOW)
        {
            self.samples.pop_front();
        }
    }

    pub fn rate_per_minute(&self) -> Option<f64> {
        let first = self.samples.front()?;
        let last = self.samples.back()?;
        let elapsed = last.at.saturating_duration_since(first.at).as_secs_f64();
        if elapsed <= 0.0 || last.processed < first.processed {
            return None;
        }
        Some((last.processed - first.processed) as f64 / elapsed * 60.0)
    }

    pub fn eta(&self, size: u32, processed: u32) -> Option<Duration> {
        let rate = self.rate_per_minute()?;
        if rate <= 0.0 || processed >= size {
            return None;
        }
        Some(Duration::from_secs_f64(
            (size - processed) as f64 / rate * 60.0,
        ))
    }
}

pub fn format_duration(duration: Duration) -> String {
    let total = duration.as_secs();
    if total < 60 {
        format!("{total}s")
    } else if total < 3600 {
        format!("{}m{:02}s", total / 60, total % 60)
    } else {
        format!("{}h{:02}m", total / 3600, (total % 3600) / 60)
    }
}

pub fn format_time(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|time| {
            time.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "—".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress() -> ArrayProgress {
        ArrayProgress {
            size: 10_000,
            submitted: 1,
            pending: 31,
            runnable: 1_990,
            starting: 35,
            running: 128,
            succeeded: 7_812,
            failed: 3,
            summary_updated_at: None,
        }
    }

    #[test]
    fn array_metrics_match_specification() {
        let progress = progress();
        assert_eq!(progress.processed(), 7_815);
        assert_eq!(progress.waiting(), 2_057);
        assert!((progress.progress() - 0.7815).abs() < f64::EPSILON);
        assert!((progress.success_rate().unwrap() - 7_812.0 / 7_815.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zero_terminal_children_have_no_success_rate() {
        let progress = ArrayProgress {
            size: 2,
            ..Default::default()
        };
        assert_eq!(progress.success_rate(), None);
    }

    #[test]
    fn progress_is_safe_for_zero_and_inconsistent_counts() {
        assert_eq!(ArrayProgress::default().progress(), 0.0);
        let progress = ArrayProgress {
            size: 2,
            succeeded: 3,
            ..Default::default()
        };
        assert_eq!(progress.progress(), 1.0);
    }

    #[test]
    fn status_grouping_is_correct() {
        for status in [
            BatchStatus::Submitted,
            BatchStatus::Pending,
            BatchStatus::Runnable,
            BatchStatus::Starting,
        ] {
            assert!(status.is_waiting());
            assert!(!status.is_terminal());
        }
        assert!(BatchStatus::Succeeded.is_terminal());
        assert!(BatchStatus::Failed.is_terminal());
        assert!(!BatchStatus::Running.is_waiting());
    }

    #[test]
    fn rate_and_eta_use_recent_monotonic_samples() {
        let start = Instant::now();
        let mut history = RateHistory::default();
        history.record(start, 100);
        history.record(start + Duration::from_secs(10), 200);
        assert_eq!(history.rate_per_minute(), Some(600.0));
        assert_eq!(history.eta(800, 200), Some(Duration::from_secs(60)));
    }

    #[test]
    fn decreasing_count_resets_rate_until_two_new_samples_exist() {
        let start = Instant::now();
        let mut history = RateHistory::default();
        history.record(start, 100);
        history.record(start + Duration::from_secs(10), 200);
        history.record(start + Duration::from_secs(11), 150);
        assert_eq!(history.rate_per_minute(), None);
        history.record(start + Duration::from_secs(21), 250);
        assert_eq!(history.rate_per_minute(), Some(600.0));
    }

    #[test]
    fn latest_attempt_drives_log_location_with_fallbacks() {
        let summary = JobSummary {
            job_id: "job".into(),
            job_name: "job".into(),
            queue: "queue".into(),
            definition: None,
            status: BatchStatus::Failed,
            status_reason: None,
            created_at: None,
            started_at: None,
            stopped_at: None,
            kind: JobKind::Single,
            is_mnp: false,
        };
        let detail = JobDetail {
            summary,
            parent_id: None,
            array_index: None,
            parameters: BTreeMap::new(),
            attempts: vec![AttemptDetail {
                number: 1,
                started_at: None,
                stopped_at: None,
                status_reason: None,
                container: ContainerInfo {
                    log_driver: Some("awslogs".into()),
                    log_stream_name: Some("stream".into()),
                    log_group: Some("group".into()),
                    logs_region: Some("us-west-2".into()),
                    ..Default::default()
                },
            }],
            max_attempts: Some(1),
            container: ContainerInfo::default(),
            raw_json: String::new(),
        };
        assert_eq!(
            detail.log_location("ap-northeast-1"),
            Some(LogLocation {
                group: "group".into(),
                region: "us-west-2".into(),
                stream: "stream".into(),
            })
        );
    }

    #[test]
    fn latest_attempt_is_selected_by_time_and_does_not_borrow_an_old_stream() {
        let summary = JobSummary {
            job_id: "job".into(),
            job_name: "job".into(),
            queue: "queue".into(),
            definition: None,
            status: BatchStatus::Running,
            status_reason: None,
            created_at: None,
            started_at: None,
            stopped_at: None,
            kind: JobKind::Single,
            is_mnp: false,
        };
        let older = AttemptDetail {
            number: 1,
            started_at: DateTime::from_timestamp_millis(1_000),
            stopped_at: DateTime::from_timestamp_millis(2_000),
            status_reason: None,
            container: ContainerInfo {
                log_stream_name: Some("older-stream".into()),
                ..Default::default()
            },
        };
        let latest_without_stream = AttemptDetail {
            number: 2,
            started_at: DateTime::from_timestamp_millis(3_000),
            stopped_at: None,
            status_reason: None,
            container: ContainerInfo::default(),
        };
        let detail = JobDetail {
            summary,
            parent_id: None,
            array_index: None,
            parameters: BTreeMap::new(),
            attempts: vec![latest_without_stream, older],
            max_attempts: Some(3),
            container: ContainerInfo {
                log_stream_name: Some("current-container-stream".into()),
                ..Default::default()
            },
            raw_json: String::new(),
        };
        assert_eq!(detail.latest_attempt().unwrap().number, 2);
        assert_eq!(detail.log_location("ap-northeast-1"), None);
    }

    #[test]
    fn unsupported_log_driver_has_no_cloudwatch_location() {
        let mut detail = JobDetail {
            summary: JobSummary {
                job_id: "job".into(),
                job_name: "job".into(),
                queue: "queue".into(),
                definition: None,
                status: BatchStatus::Running,
                status_reason: None,
                created_at: None,
                started_at: None,
                stopped_at: None,
                kind: JobKind::Single,
                is_mnp: false,
            },
            parent_id: None,
            array_index: None,
            parameters: BTreeMap::new(),
            attempts: Vec::new(),
            max_attempts: None,
            container: ContainerInfo {
                log_driver: Some("fluentd".into()),
                log_stream_name: Some("stream".into()),
                ..Default::default()
            },
            raw_json: String::new(),
        };
        assert_eq!(detail.log_location("ap-northeast-1"), None);
        detail.container.log_driver = Some("awslogs".into());
        assert_eq!(
            detail.log_location("ap-northeast-1").unwrap().group,
            "/aws/batch/job"
        );
    }
}
