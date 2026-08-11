use chrono::{Local, Utc};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Cell, Clear, Gauge, Paragraph, Row, Table, TableState, Tabs, Wrap},
};

use crate::{
    app::{App, ArrayTab, ChildFilter, JobTab, Screen},
    domain::{BatchStatus, JobDetail, format_duration, format_time},
};

const MIN_WIDTH: u16 = 70;
const MIN_HEIGHT: u16 = 15;

pub fn render(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        let message = format!(
            "Terminal too small\n{}x{} available; at least {MIN_WIDTH}x{MIN_HEIGHT} required",
            area.width, area.height
        );
        frame.render_widget(
            Paragraph::new(message)
                .alignment(Alignment::Center)
                .block(Block::bordered().title(" batchtop ")),
            area,
        );
        return;
    }

    let has_error = app.error_banner.is_some();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(if has_error { 1 } else { 0 }),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);
    render_top_bar(frame, chunks[0], app);
    if has_error {
        render_error(frame, chunks[1], app);
    }
    match app.current_screen() {
        Screen::Home => render_home(frame, chunks[2], app),
        Screen::Array(id) => render_array(frame, chunks[2], app, id),
        Screen::Job(id) => render_job(frame, chunks[2], app, id),
    }
    render_footer(frame, chunks[3], app);
    if app.show_help {
        render_help(frame, area);
    }
}

fn render_top_bar(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let freshness = match app.current_screen() {
        Screen::Home => app
            .home
            .current()
            .refreshed_at
            .map(|instant| format!("↻ {:.1}s", instant.elapsed().as_secs_f32()))
            .unwrap_or_else(|| "↻ loading".to_owned()),
        Screen::Array(id) | Screen::Job(id) => app
            .detail_refreshed_at
            .get(id)
            .map(|instant| format!("↻ {:.1}s", instant.elapsed().as_secs_f32()))
            .unwrap_or_else(|| "↻ loading".to_owned()),
    };
    let is_partial = matches!(app.current_screen(), Screen::Home) && app.home.current().partial;
    let partial = if is_partial { "  PARTIAL" } else { "" };
    let line = Line::from(vec![
        Span::styled(" batchtop ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            " profile: {}   region: {}",
            app.profile_label, app.region
        )),
        Span::styled(
            format!("   {freshness}{partial}"),
            if is_partial {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default()
            },
        ),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

fn render_error(frame: &mut Frame<'_>, area: Rect, app: &App) {
    frame.render_widget(
        Paragraph::new(format!(
            " ⚠ {}",
            app.error_banner.as_deref().unwrap_or_default()
        ))
        .style(Style::default().fg(Color::Yellow)),
        area,
    );
}

fn render_home(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);
    let titles = crate::domain::HomeTab::ALL.iter().map(|tab| {
        let list = app.home.list(*tab);
        let count = list.loaded.then_some(list.items.len());
        Line::from(format!(
            " {}{} ",
            tab.title(),
            count.map(|value| format!(" {value}")).unwrap_or_default()
        ))
    });
    frame.render_widget(
        Tabs::new(titles)
            .select(
                crate::domain::HomeTab::ALL
                    .iter()
                    .position(|tab| *tab == app.home.tab)
                    .unwrap(),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        chunks[0],
    );

    let list = app.home.current();
    if !list.loaded && list.items.is_empty() {
        frame.render_widget(
            Paragraph::new("Loading AWS Batch jobs…")
                .alignment(Alignment::Center)
                .block(Block::bordered()),
            chunks[1],
        );
        return;
    }

    let indices = list.filtered_indices();
    let now = Utc::now();
    let wide = chunks[1].width >= 145;
    let medium = chunks[1].width >= 105;
    let header = if wide {
        Row::new([
            "QUEUE", "JOB", "PROGRESS", "RATE", "ETA", "OK", "RUN", "FAIL", "WAIT", "TIME",
        ])
    } else if medium {
        Row::new(["QUEUE", "JOB", "PROGRESS", "RUN", "FAIL", "WAIT", "TIME"])
    } else {
        Row::new(["JOB", "PROGRESS", "FAIL", "TIME"])
    }
    .style(Style::default().add_modifier(Modifier::BOLD))
    .bottom_margin(1);
    let rows = indices
        .iter()
        .filter_map(|index| list.items.get(*index))
        .map(|job| {
            let elapsed = job
                .elapsed(now)
                .map(format_duration)
                .unwrap_or_else(|| "—".into());
            let (progress, ok, run, failed, waiting, rate, eta) = match job.array_progress() {
                Some(progress) => {
                    let rate = app
                        .rates
                        .get(&job.job_id)
                        .and_then(|history| history.rate_per_minute());
                    let eta = app
                        .rates
                        .get(&job.job_id)
                        .and_then(|history| history.eta(progress.size, progress.processed()));
                    (
                        format!(
                            "{:5.1}% {}/{}",
                            progress.progress() * 100.0,
                            progress.processed(),
                            progress.size
                        ),
                        progress.succeeded.to_string(),
                        progress.running.to_string(),
                        progress.failed.to_string(),
                        progress.waiting().to_string(),
                        rate.map(|value| format!("{value:.0}/m"))
                            .unwrap_or_else(|| "—".into()),
                        eta.map(format_duration).unwrap_or_else(|| "—".into()),
                    )
                }
                None => (
                    job.status.to_string(),
                    "—".into(),
                    "—".into(),
                    "—".into(),
                    "—".into(),
                    "—".into(),
                    "—".into(),
                ),
            };
            let failed_cell = Cell::from(failed).style(
                if job.array_progress().is_some_and(|value| value.failed > 0) {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                },
            );
            if wide {
                Row::new(vec![
                    Cell::from(job.queue.clone()),
                    Cell::from(job.job_name.clone()),
                    Cell::from(progress),
                    Cell::from(rate),
                    Cell::from(eta),
                    Cell::from(ok),
                    Cell::from(run),
                    failed_cell,
                    Cell::from(waiting),
                    Cell::from(elapsed),
                ])
            } else if medium {
                Row::new(vec![
                    Cell::from(job.queue.clone()),
                    Cell::from(job.job_name.clone()),
                    Cell::from(progress),
                    Cell::from(run),
                    failed_cell,
                    Cell::from(waiting),
                    Cell::from(elapsed),
                ])
            } else {
                Row::new(vec![
                    Cell::from(job.job_name.clone()),
                    Cell::from(progress),
                    failed_cell,
                    Cell::from(elapsed),
                ])
            }
        });
    let widths = if wide {
        vec![
            Constraint::Length(16),
            Constraint::Min(24),
            Constraint::Length(20),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(7),
            Constraint::Length(8),
        ]
    } else if medium {
        vec![
            Constraint::Length(16),
            Constraint::Min(24),
            Constraint::Length(20),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Length(8),
        ]
    } else {
        vec![
            Constraint::Min(24),
            Constraint::Length(24),
            Constraint::Length(8),
            Constraint::Length(8),
        ]
    };
    let title = search_title(&list.search, " Jobs ");
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::bordered().title(title))
        .row_highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    let mut state = TableState::default()
        .with_selected(Some(list.selected.min(indices.len().saturating_sub(1))));
    frame.render_stateful_widget(table, chunks[1], &mut state);
}

fn render_array(frame: &mut Frame<'_>, area: Rect, app: &App, id: &str) {
    let Some(state) = app.arrays.get(id) else {
        frame.render_widget(Paragraph::new("Array job is unavailable."), area);
        return;
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);
    render_tabs(
        frame,
        chunks[0],
        &ArrayTab::ALL.map(ArrayTab::title),
        state.tab as usize,
    );
    match state.tab {
        ArrayTab::Overview => render_array_overview(frame, chunks[1], app, state),
        ArrayTab::Children => render_children(frame, chunks[1], state, false),
        ArrayTab::Failures => render_children(frame, chunks[1], state, true),
        ArrayTab::Parameters => render_parameters(frame, chunks[1], app.details.get(id)),
        ArrayTab::Raw => render_raw(
            frame,
            chunks[1],
            app.details.get(id).map(|detail| detail.raw_json.as_str()),
            state.raw_scroll,
        ),
    }
}

fn render_tabs(frame: &mut Frame<'_>, area: Rect, titles: &[&str], selected: usize) {
    frame.render_widget(
        Tabs::new(titles.iter().map(|title| Line::from(format!(" {title} "))))
            .select(selected)
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        area,
    );
}

fn render_array_overview(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    state: &crate::app::ArrayState,
) {
    let Some(progress) = state.parent.array_progress() else {
        frame.render_widget(Paragraph::new("Not an Array Job."), area);
        return;
    };
    let rate = app
        .rates
        .get(&state.parent.job_id)
        .and_then(|history| history.rate_per_minute());
    let eta = app
        .rates
        .get(&state.parent.job_id)
        .and_then(|history| history.eta(progress.size, progress.processed()));
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(5)])
        .split(area);
    frame.render_widget(
        Gauge::default()
            .block(Block::bordered().title(format!(
                " {}  {} ",
                state.parent.job_name, state.parent.status
            )))
            .ratio(progress.progress())
            .label(format!(
                "{:.2}%  {}/{}",
                progress.progress() * 100.0,
                progress.processed(),
                progress.size
            ))
            .gauge_style(Style::default().fg(if progress.failed > 0 {
                Color::Yellow
            } else {
                Color::Cyan
            })),
        vertical[0],
    );
    let detail = app.details.get(&state.parent.job_id);
    let success = progress
        .success_rate()
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "—".into());
    let summary_age = progress
        .summary_updated_at
        .map(|time| {
            format!(
                "{} ago",
                format_duration((Utc::now() - time).to_std().unwrap_or_default())
            )
        })
        .unwrap_or_else(|| "—".into());
    let elapsed = state
        .parent
        .elapsed(Utc::now())
        .map(format_duration)
        .unwrap_or_else(|| "—".into());
    let text = format!(
        "Succeeded  {:>8}    Running  {:>8}    Failed  {:>8}    Waiting  {:>8}\n\n\
         Size             {:>10}\nProcessed        {:>10} / {}\nSuccess rate     {:>10}\n\
         Rate             {:>10}\nETA              {:>10}\nSummary updated  {:>10}\n\n\
         Queue            {}\nDefinition       {}\nCreated          {}\nStarted          {}\n\n\
Stopped          {}\nElapsed          {}\n\n\
         SUBMITTED {:>8}   PENDING {:>8}   RUNNABLE {:>8}\nSTARTING  {:>8}   RUNNING {:>8}   SUCCEEDED {:>8}   FAILED {:>8}",
        progress.succeeded,
        progress.running,
        progress.failed,
        progress.waiting(),
        progress.size,
        progress.processed(),
        progress.size,
        success,
        rate.map(|value| format!("{value:.0} jobs/min"))
            .unwrap_or_else(|| "—".into()),
        eta.map(format_duration).unwrap_or_else(|| "—".into()),
        summary_age,
        state.parent.queue,
        state.parent.definition.as_deref().unwrap_or("—"),
        format_time(state.parent.created_at),
        format_time(state.parent.started_at),
        format_time(state.parent.stopped_at),
        elapsed,
        progress.submitted,
        progress.pending,
        progress.runnable,
        progress.starting,
        progress.running,
        progress.succeeded,
        progress.failed,
    );
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::bordered().title(if detail.is_some() {
                " Overview "
            } else {
                " Overview · loading detail… "
            }))
            .wrap(Wrap { trim: false }),
        vertical[1],
    );
}

fn render_children(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &crate::app::ArrayState,
    failed_only: bool,
) {
    let (list, filter) = if failed_only {
        (&state.failures, ChildFilter::All)
    } else {
        (&state.children, state.filter)
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(if failed_only { 0 } else { 2 }),
            Constraint::Min(1),
        ])
        .split(area);
    if !failed_only {
        let titles = ChildFilter::ALL.map(ChildFilter::title);
        render_tabs(
            frame,
            chunks[0],
            &titles,
            ChildFilter::ALL
                .iter()
                .position(|value| *value == state.filter)
                .unwrap(),
        );
    }
    let indices = list.filtered_indices(filter);
    if list.loading && !list.loaded && list.items.is_empty() {
        frame.render_widget(
            Paragraph::new(if failed_only {
                "Loading failed children…"
            } else {
                "Loading children…"
            })
            .alignment(Alignment::Center)
            .block(Block::bordered()),
            chunks[1],
        );
        return;
    }
    let now = Utc::now();
    let rows = indices
        .iter()
        .filter_map(|index| list.items.get(*index))
        .map(|child| {
            let attempts = match (child.attempts, child.max_attempts) {
                (Some(attempt), Some(max)) => format!("{attempt}/{max}"),
                (Some(attempt), None) => attempt.to_string(),
                _ => "—".into(),
            };
            Row::new(vec![
                Cell::from(child.index.to_string()),
                Cell::from(child.status.to_string()).style(status_style(child.status)),
                Cell::from(
                    child
                        .runtime(now)
                        .map(format_duration)
                        .unwrap_or_else(|| "—".into()),
                ),
                Cell::from(
                    child
                        .started_at
                        .map(|time| time.with_timezone(&Local).format("%H:%M:%S").to_string())
                        .unwrap_or_else(|| "—".into()),
                ),
                Cell::from(
                    child
                        .exit_code
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "—".into()),
                ),
                Cell::from(attempts),
                Cell::from(child.status_reason.clone().unwrap_or_default()),
            ])
        });
    let title = search_title(
        &list.search,
        &format!(
            " {} · loaded {}{} ",
            if failed_only { "Failures" } else { "Children" },
            list.items.len(),
            list.next_token.as_ref().map(|_| "+").unwrap_or_default()
        ),
    );
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(11),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new([
            "INDEX", "STATUS", "RUNTIME", "STARTED", "EXIT", "ATTEMPT", "REASON",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::bordered().title(title))
    .row_highlight_style(Style::default().bg(Color::DarkGray))
    .highlight_symbol("> ");
    let mut table_state = TableState::default()
        .with_selected(Some(list.selected.min(indices.len().saturating_sub(1))));
    frame.render_stateful_widget(table, chunks[1], &mut table_state);
}

fn render_parameters(frame: &mut Frame<'_>, area: Rect, detail: Option<&JobDetail>) {
    let text = match detail {
        None => Text::from("Loading job parameters…"),
        Some(detail) if detail.parameters.is_empty() => Text::from("No parameters."),
        Some(detail) => Text::from(
            detail
                .parameters
                .iter()
                .map(|(key, value)| Line::from(format!("{key:<24} {value}")))
                .collect::<Vec<_>>(),
        ),
    };
    frame.render_widget(
        Paragraph::new(text).block(Block::bordered().title(" Parameters ")),
        area,
    );
}

fn render_job(frame: &mut Frame<'_>, area: Rect, app: &App, id: &str) {
    let Some(state) = app.jobs.get(id) else {
        frame.render_widget(Paragraph::new("Job is unavailable."), area);
        return;
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);
    render_tabs(
        frame,
        chunks[0],
        &JobTab::ALL.map(JobTab::title),
        state.tab as usize,
    );
    let detail = app.details.get(id);
    match state.tab {
        JobTab::Overview => render_job_overview(frame, chunks[1], state, detail),
        JobTab::Logs => render_logs(frame, chunks[1], state),
        JobTab::Attempts => render_attempts(frame, chunks[1], detail, state.attempts_scroll),
        JobTab::Container => render_container(frame, chunks[1], detail),
        JobTab::Raw => render_raw(
            frame,
            chunks[1],
            detail.map(|detail| detail.raw_json.as_str()),
            state.raw_scroll,
        ),
    }
}

fn render_job_overview(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &crate::app::JobState,
    detail: Option<&JobDetail>,
) {
    let attempt = detail.and_then(JobDetail::latest_attempt);
    let container = attempt
        .map(|value| &value.container)
        .or_else(|| detail.map(|d| &d.container));
    let runtime = state
        .summary
        .elapsed(Utc::now())
        .map(format_duration)
        .unwrap_or_else(|| "—".into());
    let mut text = format!(
        "Name          {}\nStatus        {}\n",
        state.summary.job_name, state.summary.status
    );
    if let Some(parent_id) = &state.parent_id {
        text.push_str(&format!("Parent        {parent_id}\n"));
    }
    if let Some(index) = state.array_index {
        text.push_str(&format!("Array index   {index}\n"));
    }
    text.push_str(&format!(
        "Job ID        {}\nQueue         {}\nDefinition    {}\n\nExit code     {}\nAttempts      {} / {}\n\
         Reason        {}\nStarted       {}\nStopped       {}\nRuntime       {}\n\nvCPU          {}\nMemory        {}",
        state.summary.job_id,
        state.summary.queue,
        state.summary.definition.as_deref().unwrap_or("—"),
        container
            .and_then(|value| value.exit_code)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "—".into()),
        detail
            .map(|value| value.attempts.len().to_string())
            .unwrap_or_else(|| "—".into()),
        detail
            .and_then(|value| value.max_attempts)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "—".into()),
        attempt
            .and_then(|value| value.status_reason.as_deref())
            .or(state.summary.status_reason.as_deref())
            .or_else(|| container.and_then(|value| value.reason.as_deref()))
            .unwrap_or("—"),
        format_time(state.summary.started_at),
        format_time(state.summary.stopped_at),
        runtime,
        container
            .and_then(|value| value.vcpus.as_deref())
            .unwrap_or("—"),
        container
            .and_then(|value| value.memory.as_deref())
            .unwrap_or("—"),
    ));
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::bordered().title(" Overview "))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_logs(frame: &mut Frame<'_>, area: Rect, state: &crate::app::JobState) {
    if state.logs.location.is_none() {
        frame.render_widget(
            Paragraph::new("Log stream is not available yet.\nPress Ctrl+r to retry.")
                .alignment(Alignment::Center)
                .block(Block::bordered().title(" Logs ")),
            area,
        );
        return;
    }
    if state.logs.loading && state.logs.events.is_empty() {
        frame.render_widget(
            Paragraph::new("Loading logs…")
                .alignment(Alignment::Center)
                .block(Block::bordered().title(" Logs ")),
            area,
        );
        return;
    }
    let inner_height = area.height.saturating_sub(2) as usize;
    let start = state
        .logs
        .selected
        .saturating_sub(inner_height.saturating_sub(1));
    let lines: Vec<_> = state
        .logs
        .events
        .iter()
        .enumerate()
        .skip(start)
        .take(inner_height)
        .map(|(index, event)| {
            let timestamp = event
                .timestamp
                .map(|time| time.with_timezone(&Local).format("%H:%M:%S").to_string())
                .unwrap_or_else(|| "        ".into());
            let marker = if index == state.logs.selected {
                ">"
            } else {
                " "
            };
            Line::from(vec![
                Span::styled(marker, Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(
                    format!("{timestamp} "),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw(&event.message),
            ])
        })
        .collect();
    let title = search_title(
        &state.logs.search,
        &format!(
            " Logs · FOLLOW {} · {} events{} ",
            if state.logs.follow { "ON" } else { "OFF" },
            state.logs.events.len(),
            if state.logs.truncated_before {
                " · older discarded"
            } else if state.logs.truncated_after {
                " · newer discarded"
            } else if state.logs.backward_boundary {
                " · oldest retained"
            } else {
                ""
            }
        ),
    );
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::bordered().title(title))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_attempts(frame: &mut Frame<'_>, area: Rect, detail: Option<&JobDetail>, scroll: u16) {
    let lines = detail
        .map(|detail| {
            detail
                .attempts
                .iter()
                .map(|attempt| {
                    Line::from(format!(
                        "Attempt {:>2}  {} → {}  exit={}  {}",
                        attempt.number,
                        format_time(attempt.started_at),
                        format_time(attempt.stopped_at),
                        attempt
                            .container
                            .exit_code
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "—".into()),
                        attempt.status_reason.as_deref().unwrap_or(""),
                    ))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![Line::from("Loading attempts…")]);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::bordered().title(" Attempts "))
            .scroll((scroll, 0)),
        area,
    );
}

fn render_container(frame: &mut Frame<'_>, area: Rect, detail: Option<&JobDetail>) {
    let text = if let Some(detail) = detail {
        let container = detail
            .latest_attempt()
            .map(|attempt| &attempt.container)
            .unwrap_or(&detail.container);
        format!(
            "Name          {}\nLog driver    {}\nExit code     {}\nReason        {}\nvCPU          {}\nMemory        {}\nLog group     {}\nLogs Region   {}\nLog stream    {}",
            container.name.as_deref().unwrap_or("—"),
            container
                .log_driver
                .as_deref()
                .unwrap_or("awslogs (default)"),
            container
                .exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "—".into()),
            container.reason.as_deref().unwrap_or("—"),
            container.vcpus.as_deref().unwrap_or("—"),
            container.memory.as_deref().unwrap_or("—"),
            container.log_group.as_deref().unwrap_or("/aws/batch/job"),
            container.logs_region.as_deref().unwrap_or("Batch Region"),
            container.log_stream_name.as_deref().unwrap_or("—"),
        )
    } else {
        "Loading container detail…".into()
    };
    frame.render_widget(
        Paragraph::new(text).block(Block::bordered().title(" Container ")),
        area,
    );
}

fn render_raw(frame: &mut Frame<'_>, area: Rect, raw: Option<&str>, scroll: u16) {
    frame.render_widget(
        Paragraph::new(raw.unwrap_or("Loading raw job detail…"))
            .block(Block::bordered().title(" Raw · read-only "))
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let help = match app.current_screen() {
        Screen::Home => {
            " ↑↓/jk select  Enter open  Tab view  / search  Ctrl+r refresh  ? help  q quit"
        }
        Screen::Array(_) => {
            " ↑↓/jk select  Enter detail  Tab next  c children  f failures  p parameters  Esc back"
        }
        Screen::Job(id)
            if app
                .jobs
                .get(id)
                .is_some_and(|state| state.tab == JobTab::Logs) =>
        {
            " ↑↓/jk scroll  g/G top/bottom  f follow  / search  n/N match  Esc back"
        }
        Screen::Job(_) => " ↑↓/jk scroll  l logs  Tab next  p parent  Esc back",
    };
    frame.render_widget(
        Paragraph::new(help).style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_rect(68, 24, area);
    frame.render_widget(Clear, popup);
    let text = "Global\n  q quit   ? help   Ctrl+r refresh   Esc back/cancel\n  Tab / Shift+Tab next/previous tab   / search\n\nNavigation\n  j/k or ↑/↓ move   Enter open\n\nArray Job\n  c children   f failures   p parameters\n  Children: a all, r running, w waiting, f failed, s succeeded\n\nJob Logs\n  f follow   g/G top/bottom   n/N next/previous match\n\nSearch consumes printable shortcut keys until Enter or Esc.";
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::bordered().title(" Help "))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width.saturating_sub(2));
    let height = height.min(area.height.saturating_sub(2));
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}

fn search_title(search: &crate::app::SearchState, base: &str) -> String {
    if search.editing {
        format!(" Search: {}_ ", search.input)
    } else if !search.query.is_empty() {
        format!("{base} · /{} ", search.query)
    } else {
        base.to_owned()
    }
}

fn status_style(status: BatchStatus) -> Style {
    match status {
        BatchStatus::Succeeded => Style::default().fg(Color::Green),
        BatchStatus::Failed => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        BatchStatus::Running => Style::default().fg(Color::Cyan),
        _ => Style::default(),
    }
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    use super::*;
    use crate::app::{JobState, LogsState};
    use crate::domain::{ArrayProgress, HomeTab, JobKind, JobSummary};

    fn sample_app() -> App {
        let mut app = App::new("test".into(), "ap-northeast-1".into(), Vec::new());
        app.home.active.loaded = true;
        app.home.active.items = vec![JobSummary {
            job_id: "id".into(),
            job_name: "factor_daily".into(),
            queue: "production".into(),
            definition: Some("factor:42".into()),
            status: BatchStatus::Pending,
            status_reason: None,
            created_at: None,
            started_at: None,
            stopped_at: None,
            kind: JobKind::ArrayParent(ArrayProgress {
                size: 10_000,
                running: 128,
                succeeded: 7_812,
                failed: 3,
                runnable: 2_057,
                ..Default::default()
            }),
            is_mnp: false,
        }];
        app.home.tab = HomeTab::Active;
        app
    }

    fn render_app(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, app)).unwrap();
        terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>()
            .join("")
    }

    fn render_text(width: u16, height: u16) -> String {
        render_app(&sample_app(), width, height)
    }

    #[test]
    fn renders_compact_layout_at_80_columns() {
        let text = render_text(80, 24);
        assert!(text.contains("factor_daily"));
        assert!(text.contains("PROGRESS"));
        assert!(text.contains("FAIL"));
        assert!(!text.contains("RATE"));
    }

    #[test]
    fn renders_medium_layout_at_120_columns() {
        let text = render_text(120, 30);
        assert!(text.contains("QUEUE"));
        assert!(text.contains("WAIT"));
        assert!(!text.contains("RATE"));
    }

    #[test]
    fn renders_wide_layout_at_160_columns() {
        let text = render_text(160, 30);
        assert!(text.contains("RATE"));
        assert!(text.contains("ETA"));
        assert!(text.contains("OK"));
        assert!(text.contains("profile: test"));
        assert!(text.contains("region: ap-northeast-1"));
        assert!(text.contains("↻"));
    }

    #[test]
    fn renders_minimum_size_message() {
        let text = render_text(60, 10);
        assert!(text.contains("Terminal too small"));
        assert!(text.contains("70x15"));
    }

    #[test]
    fn single_job_omits_parent_fields_and_missing_logs_are_non_fatal() {
        let mut app = sample_app();
        let mut summary = app.home.active.items[0].clone();
        summary.kind = JobKind::Single;
        app.jobs.insert(
            "id".into(),
            JobState {
                summary,
                parent_id: None,
                array_index: None,
                tab: JobTab::Overview,
                logs: LogsState::default(),
                raw_scroll: 0,
                attempts_scroll: 0,
                open_logs_when_loaded: false,
            },
        );
        app.screen_stack.push(Screen::Job("id".into()));
        let overview = render_app(&app, 120, 30);
        assert!(!overview.contains("Parent"));
        assert!(!overview.contains("Array index"));

        app.jobs.get_mut("id").unwrap().tab = JobTab::Logs;
        let logs = render_app(&app, 120, 30);
        assert!(logs.contains("Log stream is not available yet"));
    }
}
