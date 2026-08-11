use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::{
    aws::{HomeJobsPage, SharedAws},
    domain::{
        ApiError, BatchStatus, ChildJobSummary, HomeTab, JobDetail, JobKind, JobPage, JobQueue,
        JobSummary, LogDirection, LogEvent, LogLocation, LogPage, RateHistory,
    },
};

const ACTIVE_REFRESH: Duration = Duration::from_secs(2);
const QUEUE_REFRESH: Duration = Duration::from_secs(60);
const LOG_FOLLOW_REFRESH: Duration = Duration::from_millis(1_500);
const MAX_LOG_EVENTS: usize = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Home,
    Array(String),
    Job(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayTab {
    Overview,
    Children,
    Failures,
    Parameters,
    Raw,
}

impl ArrayTab {
    pub const ALL: [Self; 5] = [
        Self::Overview,
        Self::Children,
        Self::Failures,
        Self::Parameters,
        Self::Raw,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Children => "Children",
            Self::Failures => "Failures",
            Self::Parameters => "Parameters",
            Self::Raw => "Raw",
        }
    }

    fn next(self) -> Self {
        Self::ALL[(Self::ALL.iter().position(|tab| *tab == self).unwrap() + 1) % Self::ALL.len()]
    }

    fn previous(self) -> Self {
        let index = Self::ALL.iter().position(|tab| *tab == self).unwrap();
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobTab {
    Overview,
    Logs,
    Attempts,
    Container,
    Raw,
}

impl JobTab {
    pub const ALL: [Self; 5] = [
        Self::Overview,
        Self::Logs,
        Self::Attempts,
        Self::Container,
        Self::Raw,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Logs => "Logs",
            Self::Attempts => "Attempts",
            Self::Container => "Container",
            Self::Raw => "Raw",
        }
    }

    fn next(self) -> Self {
        Self::ALL[(Self::ALL.iter().position(|tab| *tab == self).unwrap() + 1) % Self::ALL.len()]
    }

    fn previous(self) -> Self {
        let index = Self::ALL.iter().position(|tab| *tab == self).unwrap();
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildFilter {
    All,
    Running,
    Waiting,
    Failed,
    Succeeded,
}

impl ChildFilter {
    pub const ALL: [Self; 5] = [
        Self::All,
        Self::Running,
        Self::Waiting,
        Self::Failed,
        Self::Succeeded,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Running => "Running",
            Self::Waiting => "Waiting",
            Self::Failed => "Failed",
            Self::Succeeded => "Succeeded",
        }
    }

    fn matches(self, status: BatchStatus) -> bool {
        match self {
            Self::All => true,
            Self::Running => status == BatchStatus::Running,
            Self::Waiting => status.is_waiting(),
            Self::Failed => status == BatchStatus::Failed,
            Self::Succeeded => status == BatchStatus::Succeeded,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchState {
    pub editing: bool,
    pub input: String,
    pub query: String,
}

#[derive(Debug, Clone, Default)]
pub struct JobListState {
    pub items: Vec<JobSummary>,
    pub selected: usize,
    pub selected_id: Option<String>,
    pub next_token: Option<String>,
    pub loaded: bool,
    pub loading: bool,
    pub partial: bool,
    pub search: SearchState,
    pub refreshed_at: Option<Instant>,
}

impl JobListState {
    pub fn filtered_indices(&self) -> Vec<usize> {
        let query = self.search.query.to_lowercase();
        self.items
            .iter()
            .enumerate()
            .filter(|(_, job)| {
                query.is_empty()
                    || job.job_name.to_lowercase().contains(&query)
                    || job.job_id.to_lowercase().contains(&query)
                    || job.queue.to_lowercase().contains(&query)
                    || job.status.as_aws().to_lowercase().contains(&query)
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub fn selected_job(&self) -> Option<&JobSummary> {
        let indices = self.filtered_indices();
        indices
            .get(self.selected.min(indices.len().saturating_sub(1)))
            .and_then(|index| self.items.get(*index))
    }

    fn preserve_selection(&mut self, id: Option<String>) {
        let indices = self.filtered_indices();
        if indices.is_empty() {
            self.selected = 0;
            self.selected_id = None;
            return;
        }
        let position = id
            .as_deref()
            .and_then(|id| {
                indices
                    .iter()
                    .position(|index| self.items[*index].job_id == id)
            })
            .unwrap_or_else(|| self.selected.min(indices.len() - 1));
        self.selected = position;
        self.selected_id = self.selected_job().map(|job| job.job_id.clone());
    }
}

#[derive(Debug, Clone)]
pub struct HomeState {
    pub tab: HomeTab,
    pub active: JobListState,
    pub failed: JobListState,
    pub recent: JobListState,
    pub all: JobListState,
}

impl Default for HomeState {
    fn default() -> Self {
        Self {
            tab: HomeTab::Active,
            active: JobListState::default(),
            failed: JobListState::default(),
            recent: JobListState::default(),
            all: JobListState::default(),
        }
    }
}

impl HomeState {
    pub fn current(&self) -> &JobListState {
        self.list(self.tab)
    }

    pub fn current_mut(&mut self) -> &mut JobListState {
        self.list_mut(self.tab)
    }

    pub fn list(&self, tab: HomeTab) -> &JobListState {
        match tab {
            HomeTab::Active => &self.active,
            HomeTab::Failed => &self.failed,
            HomeTab::Recent => &self.recent,
            HomeTab::All => &self.all,
        }
    }

    pub fn list_mut(&mut self, tab: HomeTab) -> &mut JobListState {
        match tab {
            HomeTab::Active => &mut self.active,
            HomeTab::Failed => &mut self.failed,
            HomeTab::Recent => &mut self.recent,
            HomeTab::All => &mut self.all,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChildListState {
    pub items: Vec<ChildJobSummary>,
    pub selected: usize,
    pub selected_id: Option<String>,
    pub next_token: Option<String>,
    pub loaded: bool,
    pub loading: bool,
    pub search: SearchState,
}

impl ChildListState {
    pub fn filtered_indices(&self, filter: ChildFilter) -> Vec<usize> {
        let query = self.search.query.to_lowercase();
        self.items
            .iter()
            .enumerate()
            .filter(|(_, child)| {
                filter.matches(child.status)
                    && (query.is_empty()
                        || child.index.to_string() == query
                        || child.job_id.to_lowercase().contains(&query)
                        || child.status.as_aws().to_lowercase().contains(&query)
                        || child
                            .status_reason
                            .as_deref()
                            .is_some_and(|reason| reason.to_lowercase().contains(&query)))
            })
            .map(|(index, _)| index)
            .collect()
    }

    pub fn selected_child(&self, filter: ChildFilter) -> Option<&ChildJobSummary> {
        let indices = self.filtered_indices(filter);
        indices
            .get(self.selected.min(indices.len().saturating_sub(1)))
            .and_then(|index| self.items.get(*index))
    }

    fn preserve_selection(&mut self, filter: ChildFilter, id: Option<String>) {
        let indices = self.filtered_indices(filter);
        if indices.is_empty() {
            self.selected = 0;
            self.selected_id = None;
            return;
        }
        self.selected = id
            .as_deref()
            .and_then(|id| {
                indices
                    .iter()
                    .position(|index| self.items[*index].job_id == id)
            })
            .unwrap_or_else(|| self.selected.min(indices.len() - 1));
        self.selected_id = self
            .selected_child(filter)
            .map(|child| child.job_id.clone());
    }
}

#[derive(Debug, Clone)]
pub struct ArrayState {
    pub parent: JobSummary,
    pub tab: ArrayTab,
    pub filter: ChildFilter,
    pub children: ChildListState,
    pub failures: ChildListState,
    pub raw_scroll: u16,
}

#[derive(Debug, Clone)]
pub struct LogsState {
    pub location: Option<LogLocation>,
    pub events: VecDeque<LogEvent>,
    pub selected: usize,
    pub follow: bool,
    pub next_backward_token: Option<String>,
    pub next_forward_token: Option<String>,
    pub backward_boundary: bool,
    pub truncated_before: bool,
    pub truncated_after: bool,
    pub loading: bool,
    pub search: SearchState,
    pub matches: Vec<usize>,
    pub current_match: usize,
}

impl Default for LogsState {
    fn default() -> Self {
        Self {
            location: None,
            events: VecDeque::new(),
            selected: 0,
            follow: true,
            next_backward_token: None,
            next_forward_token: None,
            backward_boundary: false,
            truncated_before: false,
            truncated_after: false,
            loading: false,
            search: SearchState::default(),
            matches: Vec::new(),
            current_match: 0,
        }
    }
}

impl LogsState {
    fn set_location(&mut self, location: Option<LogLocation>) -> bool {
        if self.location == location {
            return false;
        }
        let follow = self.follow;
        *self = Self {
            location,
            follow,
            ..Self::default()
        };
        true
    }

    fn rebuild_matches(&mut self) {
        let query = self.search.query.to_lowercase();
        self.matches = if query.is_empty() {
            Vec::new()
        } else {
            self.events
                .iter()
                .enumerate()
                .filter(|(_, event)| event.message.to_lowercase().contains(&query))
                .map(|(index, _)| index)
                .collect()
        };
        self.current_match = self.current_match.min(self.matches.len().saturating_sub(1));
        if let Some(index) = self.matches.get(self.current_match) {
            self.selected = *index;
        }
    }

    fn push_page(&mut self, page: LogPage, direction: LogDirection) {
        let old_len = self.events.len();
        let mut seen: HashSet<LogEvent> = self.events.iter().cloned().collect();
        match direction {
            LogDirection::Backward => {
                for event in page.events.into_iter().rev() {
                    if seen.insert(event.clone()) {
                        self.events.push_front(event);
                    }
                }
                self.selected = self.selected.saturating_add(self.events.len() - old_len);
            }
            LogDirection::Initial | LogDirection::Forward => {
                for event in page.events {
                    if seen.insert(event.clone()) {
                        self.events.push_back(event);
                    }
                }
                if self.follow {
                    self.selected = self.events.len().saturating_sub(1);
                }
            }
        }
        self.next_backward_token = page.next_backward_token;
        self.next_forward_token = page.next_forward_token;
        while self.events.len() > MAX_LOG_EVENTS {
            if direction == LogDirection::Backward {
                self.events.pop_back();
                self.selected = self.selected.min(self.events.len().saturating_sub(1));
                self.truncated_after = true;
            } else {
                self.events.pop_front();
                self.selected = self.selected.saturating_sub(1);
                self.truncated_before = true;
            }
        }
        self.rebuild_matches();
    }
}

#[derive(Debug, Clone)]
pub struct JobState {
    pub summary: JobSummary,
    pub parent_id: Option<String>,
    pub array_index: Option<u32>,
    pub tab: JobTab,
    pub logs: LogsState,
    pub raw_scroll: u16,
    pub attempts_scroll: u16,
    pub open_logs_when_loaded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RequestKey {
    Queues,
    Home(HomeTab),
    Detail(String),
    Children(String, bool),
    Logs(String),
}

#[derive(Debug, Clone)]
pub enum Effect {
    DiscoverQueues {
        generation: u64,
    },
    LoadHome {
        generation: u64,
        tab: HomeTab,
        queues: Vec<JobQueue>,
        next_token: Option<String>,
        append: bool,
    },
    LoadDetail {
        generation: u64,
        job_id: String,
    },
    LoadChildren {
        generation: u64,
        parent_id: String,
        failed_only: bool,
        next_token: Option<String>,
        append: bool,
    },
    LoadLogs {
        generation: u64,
        job_id: String,
        location: LogLocation,
        direction: LogDirection,
        next_token: Option<String>,
    },
}

#[derive(Debug)]
pub enum Message {
    QueuesLoaded {
        generation: u64,
        result: Result<Vec<JobQueue>, ApiError>,
    },
    HomeLoaded {
        generation: u64,
        tab: HomeTab,
        append: bool,
        result: Result<HomeJobsPage, ApiError>,
    },
    DetailLoaded {
        generation: u64,
        job_id: String,
        result: Result<Vec<JobDetail>, ApiError>,
    },
    ChildrenLoaded {
        generation: u64,
        parent_id: String,
        failed_only: bool,
        append: bool,
        result: Result<JobPage<ChildJobSummary>, ApiError>,
    },
    LogsLoaded {
        generation: u64,
        job_id: String,
        direction: LogDirection,
        result: Result<LogPage, ApiError>,
    },
}

pub async fn execute_effect(
    aws: SharedAws,
    sender: mpsc::UnboundedSender<Message>,
    effect: Effect,
) {
    let started = Instant::now();
    let operation = match &effect {
        Effect::DiscoverQueues { .. } => "DiscoverQueues",
        Effect::LoadHome { .. } => "ListJobs",
        Effect::LoadDetail { .. } => "DescribeJobs",
        Effect::LoadChildren { .. } => "ListChildren",
        Effect::LoadLogs { .. } => "GetLogEvents",
    };
    let message = match effect {
        Effect::DiscoverQueues { generation } => Message::QueuesLoaded {
            generation,
            result: aws.discover_queues().await,
        },
        Effect::LoadHome {
            generation,
            tab,
            queues,
            next_token,
            append,
        } => Message::HomeLoaded {
            generation,
            tab,
            append,
            result: aws.list_home_jobs(&queues, tab, next_token).await,
        },
        Effect::LoadDetail { generation, job_id } => Message::DetailLoaded {
            generation,
            result: aws.describe_jobs(std::slice::from_ref(&job_id)).await,
            job_id,
        },
        Effect::LoadChildren {
            generation,
            parent_id,
            failed_only,
            next_token,
            append,
        } => Message::ChildrenLoaded {
            generation,
            result: aws
                .list_children(
                    &parent_id,
                    failed_only.then_some(BatchStatus::Failed),
                    next_token,
                )
                .await,
            parent_id,
            failed_only,
            append,
        },
        Effect::LoadLogs {
            generation,
            job_id,
            location,
            direction,
            next_token,
        } => Message::LogsLoaded {
            generation,
            result: aws.get_log_events(&location, direction, next_token).await,
            job_id,
            direction,
        },
    };
    let elapsed_ms = started.elapsed().as_millis();
    if let Some(error) = message_error(&message) {
        warn!(operation, elapsed_ms, %error, "AWS request failed");
    } else {
        debug!(operation, elapsed_ms, "AWS request completed");
    }
    let _ = sender.send(message);
}

fn message_error(message: &Message) -> Option<&ApiError> {
    match message {
        Message::QueuesLoaded { result, .. } => result.as_ref().err(),
        Message::HomeLoaded { result, .. } => result.as_ref().err(),
        Message::DetailLoaded { result, .. } => result.as_ref().err(),
        Message::ChildrenLoaded { result, .. } => result.as_ref().err(),
        Message::LogsLoaded { result, .. } => result.as_ref().err(),
    }
}

pub struct App {
    pub profile_label: String,
    pub region: String,
    pub queues: Vec<JobQueue>,
    pub home: HomeState,
    pub screen_stack: Vec<Screen>,
    pub arrays: HashMap<String, ArrayState>,
    pub jobs: HashMap<String, JobState>,
    pub details: HashMap<String, JobDetail>,
    pub detail_refreshed_at: HashMap<String, Instant>,
    pub rates: HashMap<String, RateHistory>,
    pub error_banner: Option<String>,
    pub show_help: bool,
    pub should_quit: bool,
    pub started_at: Instant,
    generation: u64,
    expected: HashMap<RequestKey, u64>,
    in_flight: HashSet<RequestKey>,
    request_errors: HashMap<RequestKey, (u64, String)>,
    error_order: u64,
    last_queue_refresh: Instant,
    last_active_refresh: Instant,
    last_log_poll: Instant,
}

impl App {
    pub fn new(profile_label: String, region: String, queues: Vec<JobQueue>) -> Self {
        let now = Instant::now();
        Self {
            profile_label,
            region,
            queues,
            home: HomeState {
                tab: HomeTab::Active,
                ..Default::default()
            },
            screen_stack: vec![Screen::Home],
            arrays: HashMap::new(),
            jobs: HashMap::new(),
            details: HashMap::new(),
            detail_refreshed_at: HashMap::new(),
            rates: HashMap::new(),
            error_banner: None,
            show_help: false,
            should_quit: false,
            started_at: now,
            generation: 0,
            expected: HashMap::new(),
            in_flight: HashSet::new(),
            request_errors: HashMap::new(),
            error_order: 0,
            last_queue_refresh: now,
            last_active_refresh: now.checked_sub(ACTIVE_REFRESH).unwrap_or(now),
            last_log_poll: now,
        }
    }

    pub fn current_screen(&self) -> &Screen {
        self.screen_stack
            .last()
            .expect("screen stack is never empty")
    }

    pub fn initial_effects(&mut self) -> Vec<Effect> {
        self.request_home(HomeTab::Active, false)
            .into_iter()
            .collect()
    }

    fn begin(&mut self, key: RequestKey) -> Option<u64> {
        if self.in_flight.contains(&key) {
            return None;
        }
        self.generation = self.generation.wrapping_add(1);
        self.expected.insert(key.clone(), self.generation);
        self.in_flight.insert(key);
        Some(self.generation)
    }

    fn finish(&mut self, key: &RequestKey, generation: u64) -> bool {
        if self.expected.get(key).copied() != Some(generation) {
            debug!(?key, generation, "discarding stale response");
            return false;
        }
        self.expected.remove(key);
        self.in_flight.remove(key);
        true
    }

    fn invalidate(&mut self, key: &RequestKey) {
        self.expected.remove(key);
        self.in_flight.remove(key);
        self.clear_request_error(key);
    }

    fn set_request_error(&mut self, key: RequestKey, message: impl Into<String>) {
        self.error_order = self.error_order.wrapping_add(1);
        self.request_errors
            .insert(key, (self.error_order, message.into()));
        self.sync_error_banner();
    }

    fn clear_request_error(&mut self, key: &RequestKey) {
        self.request_errors.remove(key);
        self.sync_error_banner();
    }

    fn sync_error_banner(&mut self) {
        self.error_banner = self
            .request_errors
            .values()
            .max_by_key(|(order, _)| *order)
            .map(|(_, message)| message.clone());
    }

    fn request_queues(&mut self) -> Option<Effect> {
        let generation = self.begin(RequestKey::Queues)?;
        Some(Effect::DiscoverQueues { generation })
    }

    fn request_home(&mut self, tab: HomeTab, append: bool) -> Option<Effect> {
        let next_token = append
            .then(|| self.home.list(tab).next_token.clone())
            .flatten();
        if append && next_token.is_none() {
            return None;
        }
        let generation = self.begin(RequestKey::Home(tab))?;
        self.home.list_mut(tab).loading = true;
        Some(Effect::LoadHome {
            generation,
            tab,
            queues: self.queues.clone(),
            next_token,
            append,
        })
    }

    fn request_detail(&mut self, job_id: impl Into<String>) -> Option<Effect> {
        let job_id = job_id.into();
        let generation = self.begin(RequestKey::Detail(job_id.clone()))?;
        Some(Effect::LoadDetail { generation, job_id })
    }

    fn request_children(
        &mut self,
        parent_id: impl Into<String>,
        failed_only: bool,
        append: bool,
    ) -> Option<Effect> {
        let parent_id = parent_id.into();
        let next_token = self.arrays.get(&parent_id).and_then(|state| {
            let list = if failed_only {
                &state.failures
            } else {
                &state.children
            };
            append.then(|| list.next_token.clone()).flatten()
        });
        if append && next_token.is_none() {
            return None;
        }
        let key = RequestKey::Children(parent_id.clone(), failed_only);
        let generation = self.begin(key)?;
        if let Some(state) = self.arrays.get_mut(&parent_id) {
            if failed_only {
                state.failures.loading = true;
            } else {
                state.children.loading = true;
            }
        }
        Some(Effect::LoadChildren {
            generation,
            parent_id,
            failed_only,
            next_token,
            append,
        })
    }

    fn request_logs(
        &mut self,
        job_id: impl Into<String>,
        direction: LogDirection,
    ) -> Option<Effect> {
        let job_id = job_id.into();
        let state = self.jobs.get(&job_id)?;
        let location = state.logs.location.clone()?;
        let next_token = match direction {
            LogDirection::Initial => None,
            LogDirection::Backward if state.logs.backward_boundary => return None,
            LogDirection::Backward => state.logs.next_backward_token.clone(),
            LogDirection::Forward => state.logs.next_forward_token.clone(),
        };
        if direction != LogDirection::Initial && next_token.is_none() {
            return None;
        }
        let generation = self.begin(RequestKey::Logs(job_id.clone()))?;
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.logs.loading = true;
        }
        Some(Effect::LoadLogs {
            generation,
            job_id,
            location,
            direction,
            next_token,
        })
    }

    pub fn on_tick(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        if now.saturating_duration_since(self.last_queue_refresh) >= QUEUE_REFRESH
            && let Some(effect) = self.request_queues()
        {
            self.last_queue_refresh = now;
            effects.push(effect);
        }

        let screen = self.current_screen().clone();
        if now.saturating_duration_since(self.last_active_refresh) >= ACTIVE_REFRESH {
            let mut active_effects = Vec::new();
            match screen.clone() {
                Screen::Home if self.home.tab == HomeTab::Active => {
                    active_effects.extend(self.request_home(HomeTab::Active, false));
                }
                Screen::Array(id) => {
                    let tab = self.arrays.get(&id).and_then(|state| {
                        (!state.parent.status.is_terminal()).then_some(state.tab)
                    });
                    if let Some(tab) = tab {
                        active_effects.extend(self.request_detail(id.clone()));
                        match tab {
                            ArrayTab::Children => {
                                active_effects.extend(self.request_children(id, false, false));
                            }
                            ArrayTab::Failures => {
                                active_effects.extend(self.request_children(id, true, false));
                            }
                            _ => {}
                        }
                    }
                }
                Screen::Job(id) => {
                    let active = self
                        .jobs
                        .get(&id)
                        .is_some_and(|state| !state.summary.status.is_terminal());
                    if active {
                        active_effects.extend(self.request_detail(id));
                    }
                }
                Screen::Home => {}
            }
            if !active_effects.is_empty() {
                self.last_active_refresh = now;
                effects.extend(active_effects);
            }
        }

        if let Screen::Job(id) = screen {
            let should_follow = self.jobs.get(&id).is_some_and(|state| {
                state.tab == JobTab::Logs && state.logs.follow && state.logs.location.is_some()
            });
            if should_follow
                && now.saturating_duration_since(self.last_log_poll) >= LOG_FOLLOW_REFRESH
                && let Some(effect) = self.request_logs(id, LogDirection::Forward)
            {
                self.last_log_poll = now;
                effects.push(effect);
            }
        }
        effects
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        if self.search_active() {
            return self.handle_search_key(key);
        }
        if self.show_help {
            match key.code {
                KeyCode::Char('q') => self.should_quit = true,
                KeyCode::Esc | KeyCode::Char('?') => self.show_help = false,
                _ => {}
            }
            return Vec::new();
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return match key.code {
                KeyCode::Char('r') => self.manual_refresh(),
                KeyCode::Char('c') => {
                    self.should_quit = true;
                    Vec::new()
                }
                _ => Vec::new(),
            };
        }
        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
                return Vec::new();
            }
            KeyCode::Char('?') => {
                self.show_help = true;
                return Vec::new();
            }
            KeyCode::Esc => {
                if self.screen_stack.len() > 1 {
                    self.screen_stack.pop();
                }
                return Vec::new();
            }
            KeyCode::Char('/') => {
                self.begin_search();
                return Vec::new();
            }
            _ => {}
        }

        match self.current_screen().clone() {
            Screen::Home => self.handle_home_key(key),
            Screen::Array(id) => self.handle_array_key(&id, key),
            Screen::Job(id) => self.handle_job_key(&id, key),
        }
    }

    fn search_active(&self) -> bool {
        match self.current_screen() {
            Screen::Home => self.home.current().search.editing,
            Screen::Array(id) => self.arrays.get(id).is_some_and(|state| match state.tab {
                ArrayTab::Children => state.children.search.editing,
                ArrayTab::Failures => state.failures.search.editing,
                _ => false,
            }),
            Screen::Job(id) => self
                .jobs
                .get(id)
                .is_some_and(|state| state.tab == JobTab::Logs && state.logs.search.editing),
        }
    }

    fn begin_search(&mut self) {
        match self.current_screen().clone() {
            Screen::Home => {
                let search = &mut self.home.current_mut().search;
                search.editing = true;
                search.input = search.query.clone();
            }
            Screen::Array(id) => {
                if let Some(state) = self.arrays.get_mut(&id) {
                    let search = match state.tab {
                        ArrayTab::Children => Some(&mut state.children.search),
                        ArrayTab::Failures => Some(&mut state.failures.search),
                        _ => None,
                    };
                    if let Some(search) = search {
                        search.editing = true;
                        search.input = search.query.clone();
                    }
                }
            }
            Screen::Job(id) => {
                if let Some(state) = self.jobs.get_mut(&id)
                    && state.tab == JobTab::Logs
                {
                    state.logs.search.editing = true;
                    state.logs.search.input = state.logs.search.query.clone();
                }
            }
        }
    }

    fn handle_search_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        let screen = self.current_screen().clone();
        let mut applied = false;
        let search = match &screen {
            Screen::Home => Some(&mut self.home.current_mut().search),
            Screen::Array(id) => self.arrays.get_mut(id).and_then(|state| match state.tab {
                ArrayTab::Children => Some(&mut state.children.search),
                ArrayTab::Failures => Some(&mut state.failures.search),
                _ => None,
            }),
            Screen::Job(id) => self
                .jobs
                .get_mut(id)
                .and_then(|state| (state.tab == JobTab::Logs).then_some(&mut state.logs.search)),
        };
        let Some(search) = search else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Esc => search.editing = false,
            KeyCode::Enter => {
                search.query = search.input.clone();
                search.editing = false;
                applied = true;
            }
            KeyCode::Backspace => {
                search.input.pop();
            }
            KeyCode::Char(character) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                search.input.push(character)
            }
            _ => {}
        }
        if !applied {
            return Vec::new();
        }
        match screen {
            Screen::Home => {
                self.home.current_mut().preserve_selection(None);
                Vec::new()
            }
            Screen::Array(id) => {
                let mut load_more = false;
                if let Some(state) = self.arrays.get_mut(&id) {
                    match state.tab {
                        ArrayTab::Children => {
                            state.children.preserve_selection(state.filter, None);
                            let numeric_missing =
                                state.children.search.query.parse::<u32>().ok().is_some_and(
                                    |wanted| {
                                        !state
                                            .children
                                            .items
                                            .iter()
                                            .any(|child| child.index == wanted)
                                    },
                                );
                            load_more = numeric_missing && state.children.next_token.is_some();
                        }
                        ArrayTab::Failures => {
                            state.failures.preserve_selection(ChildFilter::All, None)
                        }
                        _ => {}
                    }
                }
                if load_more {
                    self.request_children(id, false, true).into_iter().collect()
                } else {
                    Vec::new()
                }
            }
            Screen::Job(id) => {
                if let Some(state) = self.jobs.get_mut(&id) {
                    state.logs.rebuild_matches();
                }
                Vec::new()
            }
        }
    }

    fn handle_home_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        match key.code {
            KeyCode::Tab | KeyCode::Right => {
                self.home.tab = self.home.tab.next();
                self.ensure_home_loaded()
            }
            KeyCode::BackTab | KeyCode::Left => {
                self.home.tab = self.home.tab.previous();
                self.ensure_home_loaded()
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let tab = self.home.tab;
                let list = self.home.current_mut();
                let len = list.filtered_indices().len();
                if len > 0 {
                    list.selected = (list.selected + 1).min(len - 1);
                    list.selected_id = list.selected_job().map(|job| job.job_id.clone());
                }
                if tab == HomeTab::All && list.selected + 5 >= len {
                    self.request_home(HomeTab::All, true).into_iter().collect()
                } else {
                    Vec::new()
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                let list = self.home.current_mut();
                list.selected = list.selected.saturating_sub(1);
                list.selected_id = list.selected_job().map(|job| job.job_id.clone());
                Vec::new()
            }
            KeyCode::Enter => self.open_home_selected(),
            _ => Vec::new(),
        }
    }

    fn ensure_home_loaded(&mut self) -> Vec<Effect> {
        let tab = self.home.tab;
        if self.home.list(tab).loaded {
            Vec::new()
        } else {
            self.request_home(tab, false).into_iter().collect()
        }
    }

    fn open_home_selected(&mut self) -> Vec<Effect> {
        let Some(job) = self.home.current().selected_job().cloned() else {
            return Vec::new();
        };
        match &job.kind {
            JobKind::ArrayParent(_) => {
                let id = job.job_id.clone();
                if let Some(state) = self.arrays.get_mut(&id) {
                    state.parent = job;
                    state.tab = ArrayTab::Overview;
                } else {
                    self.arrays.insert(
                        id.clone(),
                        ArrayState {
                            parent: job,
                            tab: ArrayTab::Overview,
                            filter: ChildFilter::All,
                            children: ChildListState::default(),
                            failures: ChildListState::default(),
                            raw_scroll: 0,
                        },
                    );
                }
                self.screen_stack.push(Screen::Array(id.clone()));
                self.request_detail(id).into_iter().collect()
            }
            JobKind::Single => {
                let id = job.job_id.clone();
                if let Some(state) = self.jobs.get_mut(&id) {
                    state.summary = job;
                    state.parent_id = None;
                    state.array_index = None;
                    state.tab = JobTab::Overview;
                    state.open_logs_when_loaded = false;
                } else {
                    self.jobs.insert(
                        id.clone(),
                        JobState {
                            summary: job,
                            parent_id: None,
                            array_index: None,
                            tab: JobTab::Overview,
                            logs: LogsState::default(),
                            raw_scroll: 0,
                            attempts_scroll: 0,
                            open_logs_when_loaded: false,
                        },
                    );
                }
                self.screen_stack.push(Screen::Job(id.clone()));
                self.request_detail(id).into_iter().collect()
            }
        }
    }

    fn handle_array_key(&mut self, id: &str, key: KeyEvent) -> Vec<Effect> {
        match key.code {
            KeyCode::Tab | KeyCode::Right => {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.tab = state.tab.next();
                }
                self.ensure_array_tab(id)
            }
            KeyCode::BackTab | KeyCode::Left => {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.tab = state.tab.previous();
                }
                self.ensure_array_tab(id)
            }
            KeyCode::Char('c') => {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.tab = ArrayTab::Children;
                }
                self.ensure_array_tab(id)
            }
            KeyCode::Char('p') => {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.tab = ArrayTab::Parameters;
                }
                self.ensure_array_tab(id)
            }
            KeyCode::Char('f')
                if self
                    .arrays
                    .get(id)
                    .is_some_and(|state| state.tab != ArrayTab::Children) =>
            {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.tab = ArrayTab::Failures;
                }
                self.ensure_array_tab(id)
            }
            KeyCode::Char('a') => self.set_child_filter(id, ChildFilter::All),
            KeyCode::Char('r') => self.set_child_filter(id, ChildFilter::Running),
            KeyCode::Char('w') => self.set_child_filter(id, ChildFilter::Waiting),
            KeyCode::Char('f') => self.set_child_filter(id, ChildFilter::Failed),
            KeyCode::Char('s') => self.set_child_filter(id, ChildFilter::Succeeded),
            KeyCode::Down | KeyCode::Char('j') => self.move_child(id, 1),
            KeyCode::Up | KeyCode::Char('k') => self.move_child(id, -1),
            KeyCode::Enter => self.open_selected_child(id, false),
            KeyCode::Char('l') => self.open_selected_child(id, true),
            KeyCode::PageDown => {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.raw_scroll = state.raw_scroll.saturating_add(10);
                }
                Vec::new()
            }
            KeyCode::PageUp => {
                if let Some(state) = self.arrays.get_mut(id) {
                    state.raw_scroll = state.raw_scroll.saturating_sub(10);
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn ensure_array_tab(&mut self, id: &str) -> Vec<Effect> {
        let Some(state) = self.arrays.get(id) else {
            return Vec::new();
        };
        match state.tab {
            ArrayTab::Children if !state.children.loaded => self
                .request_children(id.to_owned(), false, false)
                .into_iter()
                .collect(),
            ArrayTab::Failures if !state.failures.loaded => self
                .request_children(id.to_owned(), true, false)
                .into_iter()
                .collect(),
            ArrayTab::Overview | ArrayTab::Parameters | ArrayTab::Raw
                if !self.details.contains_key(id) =>
            {
                self.request_detail(id.to_owned()).into_iter().collect()
            }
            _ => Vec::new(),
        }
    }

    fn set_child_filter(&mut self, id: &str, filter: ChildFilter) -> Vec<Effect> {
        if let Some(state) = self.arrays.get_mut(id)
            && state.tab == ArrayTab::Children
        {
            state.filter = filter;
            state.children.preserve_selection(filter, None);
        }
        Vec::new()
    }

    fn move_child(&mut self, id: &str, delta: i8) -> Vec<Effect> {
        let mut load_more = None;
        if let Some(state) = self.arrays.get_mut(id) {
            let (list, filter, failed_only) = match state.tab {
                ArrayTab::Children => (&mut state.children, state.filter, false),
                ArrayTab::Failures => (&mut state.failures, ChildFilter::All, true),
                _ => return Vec::new(),
            };
            let len = list.filtered_indices(filter).len();
            if delta > 0 && len > 0 {
                list.selected = (list.selected + 1).min(len - 1);
            } else if delta < 0 {
                list.selected = list.selected.saturating_sub(1);
            }
            list.selected_id = list
                .selected_child(filter)
                .map(|child| child.job_id.clone());
            if delta > 0 && list.selected + 10 >= len && list.next_token.is_some() {
                load_more = Some(failed_only);
            }
        }
        load_more
            .and_then(|failed| self.request_children(id.to_owned(), failed, true))
            .into_iter()
            .collect()
    }

    fn open_selected_child(&mut self, parent_id: &str, logs: bool) -> Vec<Effect> {
        let Some(array) = self.arrays.get(parent_id) else {
            return Vec::new();
        };
        let selected = match array.tab {
            ArrayTab::Children => array.children.selected_child(array.filter),
            ArrayTab::Failures => array.failures.selected_child(ChildFilter::All),
            _ => None,
        };
        let Some(child) = selected.cloned() else {
            return Vec::new();
        };
        let id = child.job_id.clone();
        let summary = JobSummary {
            job_id: id.clone(),
            job_name: format!("{}[{}]", array.parent.job_name, child.index),
            queue: array.parent.queue.clone(),
            definition: array.parent.definition.clone(),
            status: child.status,
            status_reason: child.status_reason.clone(),
            created_at: None,
            started_at: child.started_at,
            stopped_at: child.stopped_at,
            kind: JobKind::Single,
            is_mnp: false,
        };
        if let Some(state) = self.jobs.get_mut(&id) {
            state.summary = summary;
            state.parent_id = Some(parent_id.to_owned());
            state.array_index = Some(child.index);
            state.tab = if logs { JobTab::Logs } else { JobTab::Overview };
            state.open_logs_when_loaded = logs;
        } else {
            self.jobs.insert(
                id.clone(),
                JobState {
                    summary,
                    parent_id: Some(parent_id.to_owned()),
                    array_index: Some(child.index),
                    tab: if logs { JobTab::Logs } else { JobTab::Overview },
                    logs: LogsState::default(),
                    raw_scroll: 0,
                    attempts_scroll: 0,
                    open_logs_when_loaded: logs,
                },
            );
        }
        self.screen_stack.push(Screen::Job(id.clone()));
        self.request_detail(id).into_iter().collect()
    }

    fn handle_job_key(&mut self, id: &str, key: KeyEvent) -> Vec<Effect> {
        match key.code {
            KeyCode::Tab | KeyCode::Right => {
                if let Some(state) = self.jobs.get_mut(id) {
                    state.tab = state.tab.next();
                }
                self.ensure_job_tab(id)
            }
            KeyCode::BackTab | KeyCode::Left => {
                if let Some(state) = self.jobs.get_mut(id) {
                    state.tab = state.tab.previous();
                }
                self.ensure_job_tab(id)
            }
            KeyCode::Char('l') => {
                if let Some(state) = self.jobs.get_mut(id) {
                    state.tab = JobTab::Logs;
                }
                self.ensure_job_tab(id)
            }
            KeyCode::Char('p') => {
                if self.screen_stack.len() > 1 {
                    self.screen_stack.pop();
                }
                Vec::new()
            }
            KeyCode::Char('f') => {
                if let Some(state) = self.jobs.get_mut(id)
                    && state.tab == JobTab::Logs
                {
                    state.logs.follow = !state.logs.follow;
                    if state.logs.follow {
                        state.logs.selected = state.logs.events.len().saturating_sub(1);
                    }
                }
                Vec::new()
            }
            KeyCode::Char('g') => {
                if let Some(state) = self.jobs.get_mut(id)
                    && state.tab == JobTab::Logs
                {
                    state.logs.follow = false;
                    state.logs.selected = 0;
                }
                self.request_logs(id.to_owned(), LogDirection::Backward)
                    .into_iter()
                    .collect()
            }
            KeyCode::Char('G') => {
                if let Some(state) = self.jobs.get_mut(id)
                    && state.tab == JobTab::Logs
                {
                    state.logs.selected = state.logs.events.len().saturating_sub(1);
                    state.logs.follow = true;
                }
                Vec::new()
            }
            KeyCode::Char('n') => {
                self.move_log_match(id, 1);
                Vec::new()
            }
            KeyCode::Char('N') => {
                self.move_log_match(id, -1);
                Vec::new()
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_job_view(id, 1);
                Vec::new()
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_job_view(id, -1);
                Vec::new()
            }
            KeyCode::PageDown => {
                self.move_job_view(id, 10);
                Vec::new()
            }
            KeyCode::PageUp => {
                self.move_job_view(id, -10);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn ensure_job_tab(&mut self, id: &str) -> Vec<Effect> {
        let Some(state) = self.jobs.get(id) else {
            return Vec::new();
        };
        if !self.details.contains_key(id) {
            return self.request_detail(id.to_owned()).into_iter().collect();
        }
        if state.tab == JobTab::Logs && state.logs.events.is_empty() && !state.logs.loading {
            self.request_logs(id.to_owned(), LogDirection::Initial)
                .into_iter()
                .collect()
        } else {
            Vec::new()
        }
    }

    fn move_log_match(&mut self, id: &str, delta: i8) {
        let Some(state) = self.jobs.get_mut(id) else {
            return;
        };
        let len = state.logs.matches.len();
        if len == 0 {
            return;
        }
        if delta > 0 {
            state.logs.current_match = (state.logs.current_match + 1) % len;
        } else {
            state.logs.current_match = (state.logs.current_match + len - 1) % len;
        }
        state.logs.selected = state.logs.matches[state.logs.current_match];
        state.logs.follow = false;
    }

    fn move_job_view(&mut self, id: &str, delta: i16) {
        let Some(state) = self.jobs.get_mut(id) else {
            return;
        };
        match state.tab {
            JobTab::Logs => {
                let selected = if delta.is_negative() {
                    state
                        .logs
                        .selected
                        .saturating_sub(delta.unsigned_abs() as usize)
                } else {
                    state.logs.selected.saturating_add(delta as usize)
                };
                state.logs.selected = selected.min(state.logs.events.len().saturating_sub(1));
                state.logs.follow = state.logs.selected + 1 == state.logs.events.len();
            }
            JobTab::Attempts => {
                state.attempts_scroll = if delta.is_negative() {
                    state.attempts_scroll.saturating_sub(delta.unsigned_abs())
                } else {
                    state.attempts_scroll.saturating_add(delta as u16)
                };
            }
            JobTab::Raw => {
                state.raw_scroll = if delta.is_negative() {
                    state.raw_scroll.saturating_sub(delta.unsigned_abs())
                } else {
                    state.raw_scroll.saturating_add(delta as u16)
                };
            }
            _ => {}
        }
    }

    fn manual_refresh(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(effect) = self.request_queues() {
            effects.push(effect);
        }
        match self.current_screen().clone() {
            Screen::Home => {
                if let Some(effect) = self.request_home(self.home.tab, false) {
                    effects.push(effect);
                }
            }
            Screen::Array(id) => {
                if let Some(effect) = self.request_detail(id.clone()) {
                    effects.push(effect);
                }
                if let Some(state) = self.arrays.get(&id) {
                    let failed = state.tab == ArrayTab::Failures;
                    if matches!(state.tab, ArrayTab::Children | ArrayTab::Failures)
                        && let Some(effect) = self.request_children(id, failed, false)
                    {
                        effects.push(effect);
                    }
                }
            }
            Screen::Job(id) => {
                if let Some(effect) = self.request_detail(id.clone()) {
                    effects.push(effect);
                }
                if self
                    .jobs
                    .get(&id)
                    .is_some_and(|state| state.tab == JobTab::Logs)
                    && let Some(effect) = self.request_logs(id, LogDirection::Initial)
                {
                    effects.push(effect);
                }
            }
        }
        effects
    }

    pub fn apply_message(&mut self, message: Message) -> Vec<Effect> {
        match message {
            Message::QueuesLoaded { generation, result } => {
                if !self.finish(&RequestKey::Queues, generation) {
                    return Vec::new();
                }
                match result {
                    Ok(queues) => {
                        self.queues = queues;
                        self.clear_request_error(&RequestKey::Queues);
                    }
                    Err(error) => {
                        self.set_request_error(RequestKey::Queues, error.to_string());
                    }
                }
                Vec::new()
            }
            Message::HomeLoaded {
                generation,
                tab,
                append,
                result,
            } => {
                let key = RequestKey::Home(tab);
                if !self.finish(&key, generation) {
                    return Vec::new();
                }
                let list = self.home.list_mut(tab);
                list.loading = false;
                match result {
                    Ok(page) => {
                        let selected = list.selected_id.clone();
                        list.partial = !page.failed_queues.is_empty();
                        if append {
                            for job in page.jobs {
                                if !list.items.iter().any(|item| item.job_id == job.job_id) {
                                    list.items.push(job);
                                }
                            }
                        } else if page.failed_queues.is_empty() {
                            list.items = page.jobs;
                        } else {
                            let mut jobs = page.jobs;
                            let preserved: Vec<_> = list
                                .items
                                .iter()
                                .filter(|old| {
                                    page.failed_queues.contains(&old.queue)
                                        && !jobs.iter().any(|new| new.job_id == old.job_id)
                                })
                                .cloned()
                                .collect();
                            jobs.extend(preserved);
                            list.items = jobs;
                        }
                        sort_home_jobs(&mut list.items, tab);
                        if tab == HomeTab::Recent {
                            list.items.truncate(500);
                        }
                        list.next_token = page.next_token;
                        list.loaded = true;
                        list.refreshed_at = Some(Instant::now());
                        list.preserve_selection(selected);
                        if tab == HomeTab::Active {
                            let now = Instant::now();
                            let active_ids: HashSet<_> = list
                                .items
                                .iter()
                                .filter_map(|job| {
                                    job.array_progress().map(|progress| {
                                        self.rates
                                            .entry(job.job_id.clone())
                                            .or_default()
                                            .record(now, progress.processed());
                                        job.job_id.clone()
                                    })
                                })
                                .collect();
                            self.rates.retain(|id, _| active_ids.contains(id));
                        }
                        if page.failed_queues.is_empty() {
                            self.clear_request_error(&key);
                        } else {
                            self.set_request_error(
                                key.clone(),
                                format!("partial refresh: {}", page.failed_queues.join(", ")),
                            );
                        }
                    }
                    Err(error) => self.set_request_error(key, error.to_string()),
                }
                Vec::new()
            }
            Message::DetailLoaded {
                generation,
                job_id,
                result,
            } => {
                let key = RequestKey::Detail(job_id.clone());
                if !self.finish(&key, generation) {
                    return Vec::new();
                }
                match result {
                    Ok(mut details) => {
                        let Some(mut detail) = details.drain(..).next() else {
                            self.set_request_error(key, format!("job {job_id} was not found"));
                            return Vec::new();
                        };
                        if let Some(array) = self.arrays.get_mut(&job_id) {
                            array.parent = detail.summary.clone();
                        }
                        let mut open_logs = false;
                        let mut log_location_changed = false;
                        if let Some(job) = self.jobs.get_mut(&job_id) {
                            detail.parent_id = job.parent_id.clone();
                            detail.array_index = job.array_index.or(detail.array_index);
                            job.summary = detail.summary.clone();
                            log_location_changed =
                                job.logs.set_location(detail.log_location(&self.region));
                            open_logs = job.tab == JobTab::Logs
                                && job.logs.events.is_empty()
                                && job.logs.location.is_some();
                            job.open_logs_when_loaded = false;
                        }
                        if log_location_changed {
                            self.invalidate(&RequestKey::Logs(job_id.clone()));
                        }
                        if let Some(progress) = detail.summary.array_progress() {
                            self.rates
                                .entry(job_id.clone())
                                .or_default()
                                .record(Instant::now(), progress.processed());
                        }
                        self.details.insert(job_id.clone(), detail);
                        self.detail_refreshed_at
                            .insert(job_id.clone(), Instant::now());
                        self.clear_request_error(&key);
                        if open_logs {
                            self.request_logs(job_id, LogDirection::Initial)
                                .into_iter()
                                .collect()
                        } else {
                            Vec::new()
                        }
                    }
                    Err(error) => {
                        self.set_request_error(key, error.to_string());
                        Vec::new()
                    }
                }
            }
            Message::ChildrenLoaded {
                generation,
                parent_id,
                failed_only,
                append,
                result,
            } => {
                let key = RequestKey::Children(parent_id.clone(), failed_only);
                if !self.finish(&key, generation) {
                    return Vec::new();
                }
                let visible_numeric_search = !failed_only
                    && matches!(self.current_screen(), Screen::Array(id) if id == &parent_id)
                    && self.arrays.get(&parent_id).is_some_and(|state| {
                        state.tab == ArrayTab::Children
                            && state.children.search.query.parse::<u32>().is_ok()
                    });
                let Some(array) = self.arrays.get_mut(&parent_id) else {
                    return Vec::new();
                };
                let (list, filter) = if failed_only {
                    (&mut array.failures, ChildFilter::All)
                } else {
                    (&mut array.children, array.filter)
                };
                list.loading = false;
                match result {
                    Ok(page) => {
                        let warnings = page.warnings;
                        let selected = list.selected_id.clone();
                        let was_loaded = list.loaded;
                        let old_next_token = list.next_token.clone();
                        if append || was_loaded {
                            merge_children(&mut list.items, page.items);
                        } else {
                            list.items = page.items;
                        }
                        list.items.sort_by_key(|child| child.index);
                        list.next_token = if !append && was_loaded && old_next_token.is_some() {
                            old_next_token
                        } else {
                            page.next_token
                        };
                        list.loaded = true;
                        list.preserve_selection(filter, selected);
                        let continue_numeric_search = if !failed_only {
                            let search_index = list.search.query.parse::<u32>().ok();
                            let missing = search_index.is_some_and(|wanted| {
                                !list.items.iter().any(|child| child.index == wanted)
                            });
                            missing && list.next_token.is_some() && visible_numeric_search
                        } else {
                            false
                        };
                        if warnings.is_empty() {
                            self.clear_request_error(&key);
                        } else {
                            self.set_request_error(
                                key.clone(),
                                format!("partial child details: {}", warnings.join("; ")),
                            );
                        }
                        if continue_numeric_search {
                            return self
                                .request_children(parent_id, false, true)
                                .into_iter()
                                .collect();
                        }
                    }
                    Err(error) => self.set_request_error(key, error.to_string()),
                }
                Vec::new()
            }
            Message::LogsLoaded {
                generation,
                job_id,
                direction,
                result,
            } => {
                let key = RequestKey::Logs(job_id.clone());
                if !self.finish(&key, generation) {
                    return Vec::new();
                }
                let Some(job) = self.jobs.get_mut(&job_id) else {
                    return Vec::new();
                };
                job.logs.loading = false;
                match result {
                    Ok(page) => {
                        if direction == LogDirection::Backward {
                            job.logs.backward_boundary =
                                page.next_backward_token == job.logs.next_backward_token;
                            if job.logs.backward_boundary {
                                job.logs.truncated_before = false;
                            }
                        } else if direction == LogDirection::Forward
                            && page.next_forward_token == job.logs.next_forward_token
                        {
                            job.logs.truncated_after = false;
                        }
                        job.logs.push_page(page, direction);
                        self.clear_request_error(&key);
                    }
                    Err(error) => self.set_request_error(key, error.to_string()),
                }
                Vec::new()
            }
        }
    }
}

fn sort_home_jobs(jobs: &mut [JobSummary], tab: HomeTab) {
    match tab {
        HomeTab::Active => jobs.sort_by(|a, b| {
            let a_failed = a.array_progress().map_or(0, |progress| progress.failed);
            let b_failed = b.array_progress().map_or(0, |progress| progress.failed);
            b_failed
                .cmp(&a_failed)
                .then_with(|| b.created_at.cmp(&a.created_at))
                .then_with(|| a.job_id.cmp(&b.job_id))
        }),
        HomeTab::Failed => jobs.sort_by(|a, b| b.stopped_at.cmp(&a.stopped_at)),
        HomeTab::Recent => {
            jobs.sort_by(|a, b| b.stopped_at.cmp(&a.stopped_at));
        }
        HomeTab::All => jobs.sort_by(|a, b| b.created_at.cmp(&a.created_at)),
    }
}

fn merge_children(current: &mut Vec<ChildJobSummary>, incoming: Vec<ChildJobSummary>) {
    for child in incoming {
        if let Some(existing) = current
            .iter_mut()
            .find(|existing| existing.job_id == child.job_id)
        {
            *existing = child;
        } else {
            current.push(child);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::{
        aws::{BatchApi, LogsApi},
        domain::{ArrayProgress, ContainerInfo},
    };

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn array_job(id: &str) -> JobSummary {
        JobSummary {
            job_id: id.into(),
            job_name: "array".into(),
            queue: "queue".into(),
            definition: Some("definition:1".into()),
            status: BatchStatus::Pending,
            status_reason: None,
            created_at: None,
            started_at: None,
            stopped_at: None,
            kind: JobKind::ArrayParent(ArrayProgress {
                size: 10,
                ..Default::default()
            }),
            is_mnp: false,
        }
    }

    fn child(id: &str, index: u32, status: BatchStatus) -> ChildJobSummary {
        ChildJobSummary {
            job_id: id.into(),
            index,
            status,
            started_at: None,
            stopped_at: None,
            exit_code: None,
            attempts: None,
            max_attempts: None,
            status_reason: None,
        }
    }

    fn detail_for(summary: JobSummary, stream: Option<&str>) -> JobDetail {
        JobDetail {
            summary,
            parent_id: None,
            array_index: None,
            parameters: Default::default(),
            attempts: Vec::new(),
            max_attempts: None,
            container: ContainerInfo {
                log_driver: Some("awslogs".into()),
                log_stream_name: stream.map(str::to_owned),
                ..Default::default()
            },
            raw_json: "{}".into(),
        }
    }

    fn log_event(message: impl Into<String>) -> LogEvent {
        LogEvent {
            timestamp: None,
            ingestion_time: None,
            message: message.into(),
        }
    }

    fn loaded_app() -> App {
        let mut app = App::new("test".into(), "ap-northeast-1".into(), Vec::new());
        app.home.active.items = vec![array_job("parent")];
        app.home.active.loaded = true;
        app.home.active.selected_id = Some("parent".into());
        app
    }

    fn loaded_single_app() -> App {
        let mut app = loaded_app();
        app.home.active.items[0].kind = JobKind::Single;
        app.home.active.items[0].job_name = "single".into();
        app
    }

    #[test]
    fn home_array_failure_child_logs_flow() {
        let mut app = loaded_app();
        let effects = app.handle_key(key(KeyCode::Enter));
        assert!(matches!(app.current_screen(), Screen::Array(id) if id == "parent"));
        assert!(matches!(effects.as_slice(), [Effect::LoadDetail { .. }]));

        app.handle_key(key(KeyCode::Char('f')));
        assert_eq!(app.arrays["parent"].tab, ArrayTab::Failures);
        let state = app.arrays.get_mut("parent").unwrap();
        state.failures.loaded = true;
        state.failures.loading = false;
        state.failures.items = vec![child("failed-child", 7, BatchStatus::Failed)];

        let effects = app.handle_key(key(KeyCode::Char('l')));
        assert!(matches!(app.current_screen(), Screen::Job(id) if id == "failed-child"));
        let generation = match effects.as_slice() {
            [Effect::LoadDetail { generation, job_id }] if job_id == "failed-child" => *generation,
            other => panic!("unexpected effects: {other:?}"),
        };
        let summary = app.jobs["failed-child"].summary.clone();
        let effects = app.apply_message(Message::DetailLoaded {
            generation,
            job_id: "failed-child".into(),
            result: Ok(vec![detail_for(summary, Some("stream/7"))]),
        });
        assert_eq!(app.jobs["failed-child"].tab, JobTab::Logs);
        assert!(
            matches!(effects.as_slice(), [Effect::LoadLogs { job_id, .. }] if job_id == "failed-child")
        );
    }

    #[test]
    fn home_single_job_opens_shared_overview() {
        let mut app = loaded_single_app();
        let effects = app.handle_key(key(KeyCode::Enter));
        assert!(matches!(app.current_screen(), Screen::Job(id) if id == "parent"));
        assert_eq!(app.jobs["parent"].tab, JobTab::Overview);
        assert!(
            matches!(effects.as_slice(), [Effect::LoadDetail { job_id, .. }] if job_id == "parent")
        );
    }

    #[test]
    fn reopening_resources_preserves_child_and_log_caches() {
        let mut array_app = loaded_app();
        array_app.handle_key(key(KeyCode::Enter));
        array_app.arrays.get_mut("parent").unwrap().children.items =
            vec![child("cached-child", 8, BatchStatus::Succeeded)];
        array_app.handle_key(key(KeyCode::Esc));
        array_app.handle_key(key(KeyCode::Enter));
        assert_eq!(
            array_app.arrays["parent"].children.items[0].job_id,
            "cached-child"
        );

        let mut single_app = loaded_single_app();
        single_app.handle_key(key(KeyCode::Enter));
        single_app
            .jobs
            .get_mut("parent")
            .unwrap()
            .logs
            .events
            .push_back(log_event("cached log"));
        single_app.handle_key(key(KeyCode::Esc));
        single_app.handle_key(key(KeyCode::Enter));
        assert_eq!(
            single_app.jobs["parent"].logs.events[0].message,
            "cached log"
        );
    }

    #[test]
    fn failure_opens_child_overview_then_logs() {
        let mut app = loaded_app();
        app.handle_key(key(KeyCode::Enter));
        app.handle_key(key(KeyCode::Char('f')));
        let state = app.arrays.get_mut("parent").unwrap();
        state.failures.loaded = true;
        state.failures.loading = false;
        state.failures.items = vec![child("failed-child", 7, BatchStatus::Failed)];

        let effects = app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.jobs["failed-child"].tab, JobTab::Overview);
        let generation = match effects.as_slice() {
            [Effect::LoadDetail { generation, .. }] => *generation,
            other => panic!("unexpected effects: {other:?}"),
        };
        let summary = app.jobs["failed-child"].summary.clone();
        assert!(
            app.apply_message(Message::DetailLoaded {
                generation,
                job_id: "failed-child".into(),
                result: Ok(vec![detail_for(summary, Some("stream/7"))]),
            })
            .is_empty()
        );
        let effects = app.handle_key(key(KeyCode::Char('l')));
        assert_eq!(app.jobs["failed-child"].tab, JobTab::Logs);
        assert!(matches!(effects.as_slice(), [Effect::LoadLogs { .. }]));
    }

    #[test]
    fn escape_and_tab_navigation_are_deterministic() {
        let mut app = loaded_app();
        app.handle_key(key(KeyCode::Enter));
        app.handle_key(key(KeyCode::Tab));
        assert_eq!(app.arrays["parent"].tab, ArrayTab::Children);
        app.handle_key(key(KeyCode::Esc));
        assert_eq!(app.current_screen(), &Screen::Home);
    }

    #[test]
    fn search_mode_captures_shortcut_keys() {
        let mut app = loaded_app();
        app.handle_key(key(KeyCode::Char('/')));
        app.handle_key(key(KeyCode::Char('q')));
        assert!(!app.should_quit);
        assert_eq!(app.home.active.search.input, "q");
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.home.active.search.query, "q");
    }

    #[test]
    fn global_navigation_refresh_and_quit_keys_are_wired() {
        let mut app = loaded_app();
        app.home.active.items.push(array_job("second"));
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.home.active.selected_job().unwrap().job_id, "second");
        app.handle_key(key(KeyCode::Char('k')));
        assert_eq!(app.home.active.selected_job().unwrap().job_id, "parent");

        let effects = app.handle_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::DiscoverQueues { .. }))
        );
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::LoadHome {
                tab: HomeTab::Active,
                ..
            }
        )));

        app.handle_key(key(KeyCode::Char('q')));
        assert!(app.should_quit);
    }

    #[test]
    fn all_child_filters_match_their_documented_status_groups() {
        assert!(ChildFilter::All.matches(BatchStatus::Failed));
        assert!(ChildFilter::Running.matches(BatchStatus::Running));
        for waiting in BatchStatus::ACTIVE
            .into_iter()
            .filter(|status| *status != BatchStatus::Running)
        {
            assert!(ChildFilter::Waiting.matches(waiting));
        }
        assert!(ChildFilter::Failed.matches(BatchStatus::Failed));
        assert!(ChildFilter::Succeeded.matches(BatchStatus::Succeeded));
        assert!(!ChildFilter::Succeeded.matches(BatchStatus::Failed));
    }

    #[test]
    fn selection_is_preserved_by_job_id_after_refresh() {
        let mut app = loaded_app();
        app.home.active.items.push(array_job("second"));
        app.home.active.selected = 1;
        app.home.active.selected_id = Some("second".into());
        let generation = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        app.apply_message(Message::HomeLoaded {
            generation,
            tab: HomeTab::Active,
            append: false,
            result: Ok(HomeJobsPage {
                jobs: vec![array_job("second"), array_job("parent")],
                next_token: None,
                failed_queues: Vec::new(),
            }),
        });
        assert_eq!(app.home.active.selected_job().unwrap().job_id, "second");
    }

    #[test]
    fn partial_refresh_preserves_failed_queue_snapshot_and_sorts() {
        let mut app = loaded_app();
        app.home.active.items[0].queue = "offline".into();
        let mut fresh = array_job("fresh");
        fresh.queue = "healthy".into();
        if let JobKind::ArrayParent(progress) = &mut fresh.kind {
            progress.failed = 3;
        }
        let generation = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        app.apply_message(Message::HomeLoaded {
            generation,
            tab: HomeTab::Active,
            append: false,
            result: Ok(HomeJobsPage {
                jobs: vec![fresh],
                next_token: None,
                failed_queues: vec!["offline".into()],
            }),
        });
        assert_eq!(app.home.active.items.len(), 2);
        assert_eq!(app.home.active.items[0].job_id, "fresh");
        assert!(
            app.home
                .active
                .items
                .iter()
                .any(|job| job.job_id == "parent")
        );
        assert!(app.home.active.partial);
        assert!(app.error_banner.as_deref().unwrap().contains("offline"));
    }

    #[test]
    fn refresh_error_keeps_last_good_snapshot() {
        let mut app = loaded_app();
        let generation = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        app.apply_message(Message::HomeLoaded {
            generation,
            tab: HomeTab::Active,
            append: false,
            result: Err(ApiError::new(
                "AWS Batch ListJobs",
                "ExpiredTokenException: credentials expired",
            )),
        });
        assert_eq!(app.home.active.items[0].job_id, "parent");
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("ExpiredTokenException")
        );
    }

    #[test]
    fn unrelated_success_does_not_clear_an_active_request_error() {
        let mut app = loaded_app();
        let home_generation = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        let queue_generation = app.begin(RequestKey::Queues).unwrap();
        app.apply_message(Message::HomeLoaded {
            generation: home_generation,
            tab: HomeTab::Active,
            append: false,
            result: Err(ApiError::new("AWS Batch ListJobs", "AccessDeniedException")),
        });
        app.apply_message(Message::QueuesLoaded {
            generation: queue_generation,
            result: Ok(Vec::new()),
        });
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("AccessDeniedException")
        );

        let recovered = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        app.apply_message(Message::HomeLoaded {
            generation: recovered,
            tab: HomeTab::Active,
            append: false,
            result: Ok(HomeJobsPage {
                jobs: vec![array_job("parent")],
                next_token: None,
                failed_queues: Vec::new(),
            }),
        });
        assert!(app.error_banner.is_none());
    }

    #[test]
    fn numeric_child_search_continues_through_pages() {
        let mut app = loaded_app();
        app.handle_key(key(KeyCode::Enter));
        let state = app.arrays.get_mut("parent").unwrap();
        state.tab = ArrayTab::Children;
        state.children.loaded = true;
        state.children.items = vec![child("child-1", 1, BatchStatus::Running)];
        state.children.next_token = Some("page-2".into());
        state.children.search.query = "42".into();

        let generation = app
            .begin(RequestKey::Children("parent".into(), false))
            .unwrap();
        let effects = app.apply_message(Message::ChildrenLoaded {
            generation,
            parent_id: "parent".into(),
            failed_only: false,
            append: true,
            result: Ok(JobPage {
                items: vec![child("child-2", 2, BatchStatus::Succeeded)],
                next_token: Some("page-3".into()),
                warnings: Vec::new(),
            }),
        });
        assert_eq!(app.arrays["parent"].children.items.len(), 2);
        assert!(
            matches!(effects.as_slice(), [Effect::LoadChildren { next_token: Some(token), append: true, .. }] if token == "page-3")
        );
    }

    #[test]
    fn numeric_child_search_stops_paginating_after_navigation() {
        let mut app = loaded_app();
        app.handle_key(key(KeyCode::Enter));
        let state = app.arrays.get_mut("parent").unwrap();
        state.tab = ArrayTab::Children;
        state.children.loaded = true;
        state.children.next_token = Some("page-2".into());
        state.children.search.query = "42".into();
        let generation = app
            .begin(RequestKey::Children("parent".into(), false))
            .unwrap();
        app.screen_stack.pop();

        let effects = app.apply_message(Message::ChildrenLoaded {
            generation,
            parent_id: "parent".into(),
            failed_only: false,
            append: true,
            result: Ok(JobPage {
                items: vec![child("child-2", 2, BatchStatus::Succeeded)],
                next_token: Some("page-3".into()),
                warnings: Vec::new(),
            }),
        });
        assert!(effects.is_empty());
        assert_eq!(app.arrays["parent"].children.items.len(), 1);
    }

    #[test]
    fn child_refresh_updates_first_page_without_discarding_loaded_pages() {
        let mut app = loaded_app();
        app.handle_key(key(KeyCode::Enter));
        let state = app.arrays.get_mut("parent").unwrap();
        state.tab = ArrayTab::Children;
        state.children.loaded = true;
        state.children.items = vec![
            child("child-1", 1, BatchStatus::Running),
            child("child-101", 101, BatchStatus::Running),
        ];
        state.children.selected = 1;
        state.children.selected_id = Some("child-101".into());
        state.children.next_token = Some("page-3".into());
        let generation = app
            .begin(RequestKey::Children("parent".into(), false))
            .unwrap();

        app.apply_message(Message::ChildrenLoaded {
            generation,
            parent_id: "parent".into(),
            failed_only: false,
            append: false,
            result: Ok(JobPage {
                items: vec![child("child-1", 1, BatchStatus::Succeeded)],
                next_token: Some("page-2".into()),
                warnings: vec!["ThrottlingException during DescribeJobs".into()],
            }),
        });
        let children = &app.arrays["parent"].children;
        assert_eq!(children.items.len(), 2);
        assert_eq!(children.items[0].status, BatchStatus::Succeeded);
        assert_eq!(children.items[1].job_id, "child-101");
        assert_eq!(
            children.selected_child(ChildFilter::All).unwrap().job_id,
            "child-101"
        );
        assert_eq!(children.next_token.as_deref(), Some("page-3"));
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("ThrottlingException")
        );
    }

    #[test]
    fn logs_are_deduplicated_searchable_and_bounded() {
        let mut logs = LogsState::default();
        logs.push_page(
            LogPage {
                events: (0..5_100)
                    .map(|index| log_event(format!("event-{index}")))
                    .collect(),
                next_backward_token: Some("back".into()),
                next_forward_token: Some("forward".into()),
            },
            LogDirection::Initial,
        );
        assert_eq!(logs.events.len(), MAX_LOG_EVENTS);
        assert_eq!(logs.events.front().unwrap().message, "event-100");
        assert!(logs.truncated_before);

        logs.push_page(
            LogPage {
                events: vec![log_event("event-5099"), log_event("new")],
                next_backward_token: Some("back".into()),
                next_forward_token: Some("forward-2".into()),
            },
            LogDirection::Forward,
        );
        assert_eq!(logs.events.len(), MAX_LOG_EVENTS);
        assert_eq!(logs.events.back().unwrap().message, "new");
        assert_eq!(
            logs.events
                .iter()
                .filter(|event| event.message == "event-5099")
                .count(),
            1
        );

        logs.search.query = "event-5000".into();
        logs.rebuild_matches();
        assert_eq!(logs.matches.len(), 1);
        assert_eq!(logs.selected, logs.matches[0]);
    }

    #[test]
    fn backward_log_pagination_retains_the_older_window() {
        let mut logs = LogsState::default();
        logs.push_page(
            LogPage {
                events: (1_000..6_000)
                    .map(|index| log_event(format!("event-{index}")))
                    .collect(),
                next_backward_token: Some("back-1".into()),
                next_forward_token: Some("forward-1".into()),
            },
            LogDirection::Initial,
        );
        logs.selected = 0;
        logs.follow = false;
        logs.push_page(
            LogPage {
                events: (0..1_000)
                    .map(|index| log_event(format!("event-{index}")))
                    .collect(),
                next_backward_token: Some("back-2".into()),
                next_forward_token: Some("forward-2".into()),
            },
            LogDirection::Backward,
        );
        assert_eq!(logs.events.len(), MAX_LOG_EVENTS);
        assert_eq!(logs.events.front().unwrap().message, "event-0");
        assert_eq!(logs.events.back().unwrap().message, "event-4999");
        assert!(logs.truncated_after);
    }

    #[test]
    fn log_keyboard_search_scroll_follow_and_visibility_rules_work() {
        let mut app = loaded_single_app();
        let effects = app.handle_key(key(KeyCode::Enter));
        let detail_generation = match effects.as_slice() {
            [Effect::LoadDetail { generation, .. }] => *generation,
            other => panic!("unexpected effects: {other:?}"),
        };
        let summary = app.jobs["parent"].summary.clone();
        app.apply_message(Message::DetailLoaded {
            generation: detail_generation,
            job_id: "parent".into(),
            result: Ok(vec![detail_for(summary, Some("stream"))]),
        });
        let effects = app.handle_key(key(KeyCode::Char('l')));
        let log_generation = match effects.as_slice() {
            [Effect::LoadLogs { generation, .. }] => *generation,
            other => panic!("unexpected effects: {other:?}"),
        };
        app.apply_message(Message::LogsLoaded {
            generation: log_generation,
            job_id: "parent".into(),
            direction: LogDirection::Initial,
            result: Ok(LogPage {
                events: vec![
                    log_event("match one"),
                    log_event("other"),
                    log_event("match two"),
                ],
                next_backward_token: Some("back".into()),
                next_forward_token: Some("forward".into()),
            }),
        });
        assert_eq!(app.jobs["parent"].logs.selected, 2);

        app.handle_key(key(KeyCode::Up));
        assert_eq!(app.jobs["parent"].logs.selected, 1);
        assert!(!app.jobs["parent"].logs.follow);
        app.handle_key(key(KeyCode::Char('G')));
        assert_eq!(app.jobs["parent"].logs.selected, 2);
        assert!(app.jobs["parent"].logs.follow);
        app.handle_key(key(KeyCode::Char('f')));
        assert!(!app.jobs["parent"].logs.follow);

        app.handle_key(key(KeyCode::Char('/')));
        for character in "match".chars() {
            app.handle_key(key(KeyCode::Char(character)));
        }
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.jobs["parent"].logs.matches, vec![0, 2]);
        assert_eq!(app.jobs["parent"].logs.selected, 0);
        app.handle_key(key(KeyCode::Char('n')));
        assert_eq!(app.jobs["parent"].logs.selected, 2);
        app.handle_key(key(KeyCode::Char('N')));
        assert_eq!(app.jobs["parent"].logs.selected, 0);

        app.handle_key(key(KeyCode::Esc));
        let effects = app.on_tick(Instant::now() + Duration::from_secs(5));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::LoadLogs { .. }))
        );
    }

    #[test]
    fn repeated_backward_token_marks_log_boundary() {
        let mut app = loaded_app();
        let mut summary = array_job("job");
        summary.kind = JobKind::Single;
        app.jobs.insert(
            "job".into(),
            JobState {
                summary,
                parent_id: None,
                array_index: None,
                tab: JobTab::Logs,
                logs: LogsState {
                    location: Some(LogLocation {
                        group: "group".into(),
                        region: "region".into(),
                        stream: "stream".into(),
                    }),
                    next_backward_token: Some("same".into()),
                    truncated_before: true,
                    ..Default::default()
                },
                raw_scroll: 0,
                attempts_scroll: 0,
                open_logs_when_loaded: false,
            },
        );
        let generation = app.begin(RequestKey::Logs("job".into())).unwrap();
        app.apply_message(Message::LogsLoaded {
            generation,
            job_id: "job".into(),
            direction: LogDirection::Backward,
            result: Ok(LogPage {
                events: Vec::new(),
                next_backward_token: Some("same".into()),
                next_forward_token: Some("forward".into()),
            }),
        });
        assert!(app.jobs["job"].logs.backward_boundary);
        assert!(!app.jobs["job"].logs.truncated_before);
        assert!(app.request_logs("job", LogDirection::Backward).is_none());

        app.jobs.get_mut("job").unwrap().logs.truncated_after = true;
        let forward_generation = app.begin(RequestKey::Logs("job".into())).unwrap();
        app.apply_message(Message::LogsLoaded {
            generation: forward_generation,
            job_id: "job".into(),
            direction: LogDirection::Forward,
            result: Ok(LogPage {
                events: Vec::new(),
                next_backward_token: Some("same".into()),
                next_forward_token: Some("forward".into()),
            }),
        });
        assert!(!app.jobs["job"].logs.truncated_after);
    }

    #[test]
    fn stale_response_does_not_replace_current_state() {
        let mut app = loaded_app();
        let stale = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        app.in_flight.remove(&RequestKey::Home(HomeTab::Active));
        let current = app.begin(RequestKey::Home(HomeTab::Active)).unwrap();
        assert_ne!(stale, current);
        app.apply_message(Message::HomeLoaded {
            generation: stale,
            tab: HomeTab::Active,
            append: false,
            result: Ok(HomeJobsPage {
                jobs: vec![array_job("stale")],
                next_token: None,
                failed_queues: Vec::new(),
            }),
        });
        assert_eq!(app.home.active.items[0].job_id, "parent");
    }

    #[test]
    fn terminal_tabs_are_not_polled_by_tick() {
        let mut app = loaded_app();
        app.home.tab = HomeTab::Failed;
        let effects = app.on_tick(Instant::now() + Duration::from_secs(5));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::LoadHome { .. }))
        );
    }

    #[test]
    fn changed_latest_attempt_stream_resets_logs_and_discards_old_response() {
        let mut app = loaded_single_app();
        let effects = app.handle_key(key(KeyCode::Enter));
        let detail_generation = match effects.as_slice() {
            [Effect::LoadDetail { generation, .. }] => *generation,
            other => panic!("unexpected effects: {other:?}"),
        };
        let summary = app.jobs["parent"].summary.clone();
        app.apply_message(Message::DetailLoaded {
            generation: detail_generation,
            job_id: "parent".into(),
            result: Ok(vec![detail_for(summary.clone(), Some("old-stream"))]),
        });
        app.jobs.get_mut("parent").unwrap().tab = JobTab::Logs;
        app.jobs
            .get_mut("parent")
            .unwrap()
            .logs
            .events
            .push_back(log_event("old buffered line"));
        let old_log_generation = match app
            .request_logs("parent", LogDirection::Initial)
            .expect("old log request")
        {
            Effect::LoadLogs { generation, .. } => generation,
            other => panic!("unexpected effect: {other:?}"),
        };
        let new_detail_generation = app.begin(RequestKey::Detail("parent".into())).unwrap();
        let effects = app.apply_message(Message::DetailLoaded {
            generation: new_detail_generation,
            job_id: "parent".into(),
            result: Ok(vec![detail_for(summary, Some("new-stream"))]),
        });
        assert!(matches!(
            effects.as_slice(),
            [Effect::LoadLogs { location, .. }] if location.stream == "new-stream"
        ));
        assert!(app.jobs["parent"].logs.events.is_empty());

        app.apply_message(Message::LogsLoaded {
            generation: old_log_generation,
            job_id: "parent".into(),
            direction: LogDirection::Initial,
            result: Ok(LogPage {
                events: vec![log_event("late old-stream line")],
                next_backward_token: Some("old-back".into()),
                next_forward_token: Some("old-forward".into()),
            }),
        });
        assert_eq!(
            app.jobs["parent"].logs.location.as_ref().unwrap().stream,
            "new-stream"
        );
        assert!(app.jobs["parent"].logs.events.is_empty());
    }

    #[test]
    fn visible_active_children_refresh_without_overlap() {
        let mut app = loaded_app();
        let effects = app.handle_key(key(KeyCode::Enter));
        let generation = match effects.as_slice() {
            [Effect::LoadDetail { generation, .. }] => *generation,
            other => panic!("unexpected effects: {other:?}"),
        };
        let summary = app.arrays["parent"].parent.clone();
        app.apply_message(Message::DetailLoaded {
            generation,
            job_id: "parent".into(),
            result: Ok(vec![detail_for(summary, None)]),
        });
        let state = app.arrays.get_mut("parent").unwrap();
        state.tab = ArrayTab::Children;
        state.children.loaded = true;

        let effects = app.on_tick(Instant::now() + Duration::from_secs(5));
        assert!(effects.iter().any(
            |effect| matches!(effect, Effect::LoadDetail { job_id, .. } if job_id == "parent")
        ));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::LoadChildren {
                parent_id,
                failed_only: false,
                append: false,
                ..
            } if parent_id == "parent"
        )));

        let next = app.on_tick(Instant::now() + Duration::from_secs(8));
        assert!(!next.iter().any(|effect| matches!(
            effect,
            Effect::LoadDetail { job_id, .. } if job_id == "parent"
        )));
        assert!(!next.iter().any(|effect| matches!(
            effect,
            Effect::LoadChildren { parent_id, .. } if parent_id == "parent"
        )));
    }

    #[derive(Default)]
    struct MockAws;

    #[async_trait]
    impl BatchApi for MockAws {
        async fn discover_queues(&self) -> Result<Vec<JobQueue>, ApiError> {
            Ok(vec![JobQueue {
                name: "mock-queue".into(),
                arn: "mock-arn".into(),
                kind: crate::domain::JobQueueKind::Ecs,
                enabled: true,
            }])
        }

        async fn list_home_jobs(
            &self,
            _queues: &[JobQueue],
            tab: HomeTab,
            _next_token: Option<String>,
        ) -> Result<HomeJobsPage, ApiError> {
            Ok(HomeJobsPage {
                jobs: (tab == HomeTab::Active)
                    .then(|| array_job("mock-parent"))
                    .into_iter()
                    .collect(),
                next_token: None,
                failed_queues: Vec::new(),
            })
        }

        async fn describe_jobs(&self, job_ids: &[String]) -> Result<Vec<JobDetail>, ApiError> {
            Ok(job_ids
                .iter()
                .map(|id| detail_for(array_job(id), Some("mock-stream")))
                .collect())
        }

        async fn list_children(
            &self,
            _parent_job_id: &str,
            status: Option<BatchStatus>,
            next_token: Option<String>,
        ) -> Result<JobPage<ChildJobSummary>, ApiError> {
            Ok(JobPage {
                items: vec![child(
                    "mock-child",
                    4,
                    status.unwrap_or(BatchStatus::Running),
                )],
                next_token: next_token.is_none().then(|| "mock-next".into()),
                warnings: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl LogsApi for MockAws {
        async fn get_log_events(
            &self,
            _location: &LogLocation,
            _direction: LogDirection,
            _next_token: Option<String>,
        ) -> Result<LogPage, ApiError> {
            Ok(LogPage {
                events: vec![log_event("mock log line")],
                next_backward_token: Some("mock-back".into()),
                next_forward_token: Some("mock-forward".into()),
            })
        }
    }

    #[tokio::test]
    async fn every_aws_effect_executes_against_in_memory_api_without_network() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let aws: SharedAws = Arc::new(MockAws);
        execute_effect(
            aws.clone(),
            sender.clone(),
            Effect::DiscoverQueues { generation: 9 },
        )
        .await;
        match receiver.recv().await.unwrap() {
            Message::QueuesLoaded {
                generation,
                result: Ok(queues),
            } => {
                assert_eq!(generation, 9);
                assert_eq!(queues[0].name, "mock-queue");
            }
            other => panic!("unexpected message: {other:?}"),
        }

        execute_effect(
            aws.clone(),
            sender.clone(),
            Effect::LoadHome {
                generation: 10,
                tab: HomeTab::Active,
                queues: Vec::new(),
                next_token: None,
                append: false,
            },
        )
        .await;
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Message::HomeLoaded {
                generation: 10,
                result: Ok(HomeJobsPage { jobs, .. }),
                ..
            } if jobs[0].job_id == "mock-parent"
        ));

        execute_effect(
            aws.clone(),
            sender.clone(),
            Effect::LoadDetail {
                generation: 11,
                job_id: "mock-child".into(),
            },
        )
        .await;
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Message::DetailLoaded {
                generation: 11,
                result: Ok(details),
                ..
            } if details[0].summary.job_id == "mock-child"
        ));

        execute_effect(
            aws.clone(),
            sender.clone(),
            Effect::LoadChildren {
                generation: 12,
                parent_id: "mock-parent".into(),
                failed_only: true,
                next_token: None,
                append: false,
            },
        )
        .await;
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Message::ChildrenLoaded {
                generation: 12,
                failed_only: true,
                result: Ok(JobPage { items, next_token: Some(token), .. }),
                ..
            } if items[0].status == BatchStatus::Failed && token == "mock-next"
        ));

        execute_effect(
            aws,
            sender,
            Effect::LoadLogs {
                generation: 13,
                job_id: "mock-child".into(),
                location: LogLocation {
                    group: "mock-group".into(),
                    region: "mock-region".into(),
                    stream: "mock-stream".into(),
                },
                direction: LogDirection::Initial,
                next_token: None,
            },
        )
        .await;
        assert!(matches!(
            receiver.recv().await.unwrap(),
            Message::LogsLoaded {
                generation: 13,
                result: Ok(LogPage { events, next_forward_token: Some(token), .. }),
                ..
            } if events[0].message == "mock log line" && token == "mock-forward"
        ));
    }
}
