# AWS Batch TUI — MVP Implementation Specification

**Product name:** `batchtop`  
**Document status:** Implementation-ready MVP specification  
**Target:** Coding agent / implementation engineer  
**Primary use case:** Monitor AWS Batch workloads, especially large Array Jobs, and drill down quickly from parent progress to failed child jobs and CloudWatch Logs.

---

## 1. Product objective

Build a keyboard-driven terminal UI for AWS Batch that makes the operational state of Array Jobs visible immediately.

The tool is not intended to reproduce the AWS Console. Its primary workflow is:

1. See currently active Batch jobs across the selected AWS Region.
2. For Array Jobs, see progress and child-state counts directly on the home screen.
3. Open an Array Job and inspect its aggregate state.
4. Inspect child jobs, especially failed children.
5. Open a child job and inspect its details and CloudWatch Logs.
6. Perform all of the above without blocking the UI while AWS API calls are in flight.

The MVP is **read-only**.

---

## 2. Explicit MVP decisions

The following decisions are fixed requirements.

| Item | Decision |
|---|---|
| AWS scope | One AWS profile × one Batch Region per process |
| Job Queue scope | Container Job Queues only: `ECS`, `ECS_FARGATE`, and `EKS` |
| Service jobs | `SAGEMAKER_TRAINING` Job Queues and service jobs are excluded |
| MNP jobs | Display the top-level job as a single job; no node drill-down |
| Single jobs | Display together with Array Job parents in the main job list |
| Single-job detail | Reuse the job-detail tabs: `Overview`, `Logs`, `Attempts`, `Container`, `Raw` |
| Array progress | `(Succeeded + Failed) / ArraySize` |
| Refresh cadence | 2 seconds for the currently visible active list or active job detail |
| Expected scale | Up to approximately 10 visible container Job Queues |
| AWS mutations | None in MVP; strictly read-only |
| Parent Array Job logs | No parent `Logs` tab; logs are viewed only from a selected child job |
| Child/single-job logs | Latest attempt and primary container only; honor configured log group/Region |
| Live AWS validation | Excluded; MVP verification uses mocks and local rendering/state tests |

---

## 3. Non-goals for MVP

Do **not** implement the following unless required to satisfy another requirement in this document:

- Submit jobs
- Cancel jobs
- Terminate jobs
- Retry/resubmit jobs
- Edit Job Definitions
- Edit Job Queues
- Edit Compute Environments
- Multi-account aggregation
- Multi-Region aggregation
- Persistent local database
- Web server or browser UI
- Mouse-driven primary navigation
- CloudWatch Logs aggregation across all child jobs
- Metrics dashboards for CPU/memory utilization
- ECS/EKS resource drill-down beyond information already present in `DescribeJobs`
- AWS Batch service jobs, including `SAGEMAKER_TRAINING` jobs
- MNP node listing or node drill-down
- Switching logs between attempts, sidecars, init containers, or other containers
- Job Definition or Queue management screens as top-level pages
- Notifications or alerts
- Historical analytics beyond the in-process progress history required for rate/ETA

---

## 4. Recommended technology stack

Use Rust.

### Required core libraries

- `ratatui` — terminal UI rendering
- `crossterm` — terminal input, raw mode, alternate screen
- `tokio` — async runtime
- `aws-config` — AWS SDK configuration
- `aws-sdk-batch` — AWS Batch API
- `aws-sdk-cloudwatchlogs` — CloudWatch Logs API
- `clap` — CLI argument parsing
- `serde`, `serde_json` — structured/raw data rendering
- `tracing`, `tracing-subscriber` — diagnostic logging
- `color-eyre` or equivalent — application error reporting

Additional crates may be added where they simplify implementation, but avoid introducing a framework that duplicates Ratatui/Tokio responsibilities.

Use the latest mutually compatible stable crate versions at implementation time and commit `Cargo.lock`.

---

## 5. AWS authentication and Region resolution

### 5.1 CLI

The executable shall support:

```bash
batchtop
batchtop --profile research-prod
batchtop --region ap-northeast-1
batchtop --profile research-prod --region ap-northeast-1
```

Optional short flags are acceptable:

```bash
batchtop -p research-prod -r ap-northeast-1
```

### 5.2 Resolution behavior

Use the AWS SDK for Rust credential/config provider mechanisms rather than implementing credential loading manually.

For the Region, the effective precedence shall be:

1. Explicit `--region`
2. Standard AWS SDK Region provider chain

For the profile:

1. Explicit `--profile`
2. Existing AWS configuration behavior, including `AWS_PROFILE`
3. Default AWS profile/configuration

The application must display the effective profile label and Region in the top status bar. If the profile cannot be determined because credentials come from another provider, display a neutral value such as `profile: <provider>` rather than inventing a profile name.

The selected Region is the Batch Region. A selected job's CloudWatch Logs may be read from a different Region only when its `awslogs-region` configuration explicitly names that Region. This is resource-specific log retrieval, not multi-Region Batch aggregation.

### 5.3 Authentication failures

Credential/configuration errors must not leave the terminal in raw mode.

On startup failure:

- restore the terminal,
- print a concise actionable error,
- exit non-zero.

When temporary credentials expire during an active session, surface the API error in the UI. Allow the SDK credential provider chain to refresh credentials when supported.

---

## 6. Read-only security constraint

The AWS adapter layer for the MVP must expose no mutation methods.

Do not instantiate application actions for:

- `SubmitJob`
- `CancelJob`
- `TerminateJob`
- `RegisterJobDefinition`
- `UpdateJobQueue`
- `UpdateComputeEnvironment`
- any other AWS write API

This is intentional. The architecture may allow future extension, but no UI element, command, key binding, or service method should invoke AWS mutations in MVP.

---

## 7. AWS data model assumptions

AWS Batch statuses used by the application:

- `SUBMITTED`
- `PENDING`
- `RUNNABLE`
- `STARTING`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`

Array Job parent summaries expose:

- array size
- status summary by child status
- status-summary last-updated timestamp

Array child jobs expose an array index.

A parent Array Job may remain in `PENDING` while its children are running. Therefore the application must **not** equate `Active` with AWS status `RUNNING`.

---

## 8. Domain model

Create application-owned domain types rather than passing AWS SDK response structs directly into UI components.

Suggested shape:

```rust
struct JobSummary {
    job_id: String,
    job_name: String,
    queue: String,
    definition: Option<String>,
    status: BatchStatus,
    created_at: Option<DateTime>,
    started_at: Option<DateTime>,
    stopped_at: Option<DateTime>,
    kind: JobKind,
}

enum JobKind {
    Single,
    ArrayParent(ArrayProgress),
}

struct ArrayProgress {
    size: u32,
    submitted: u32,
    pending: u32,
    runnable: u32,
    starting: u32,
    running: u32,
    succeeded: u32,
    failed: u32,
    summary_updated_at: Option<DateTime>,
}

struct ChildJobSummary {
    job_id: String,
    index: u32,
    status: BatchStatus,
    started_at: Option<DateTime>,
    stopped_at: Option<DateTime>,
    exit_code: Option<i32>,
    attempts: Option<u32>,
    status_reason: Option<String>,
}
```

Exact type names may differ, but preserve the separation between:

- AWS adapter DTOs
- application/domain state
- rendering state

---

## 9. Derived Array Job metrics

### 9.1 Processed count

```text
Processed = Succeeded + Failed
```

### 9.2 Progress

\[
Progress = \frac{Succeeded + Failed}{ArraySize}
\]

This is the percentage of children that have reached a terminal state.

A failed child therefore counts as processed.

### 9.3 Waiting count

For compact home-screen presentation:

\[
Waiting = Submitted + Pending + Runnable + Starting
\]

Detailed views must still show the individual underlying states.

### 9.4 Success rate

When at least one child is terminal:

\[
SuccessRate = \frac{Succeeded}{Succeeded + Failed}
\]

If no child is terminal, display `—`.

### 9.5 Rate

Rate is a client-side derived metric.

Maintain lightweight in-memory samples of:

```text
(timestamp, processed_count)
```

for active Array Jobs.

Use approximately the last 30 seconds of samples where available.

Conceptually:

\[
Rate = \frac{Processed_{now} - Processed_{past}}{ElapsedMinutes}
\]

Display as jobs/min.

Requirements:

- never persist this history to disk in MVP,
- discard history for jobs that disappear from the active working set,
- do not produce a negative rate,
- if AWS returns an older/stale summary or a count temporarily decreases, treat the rate as unavailable until the series becomes monotonic again.

### 9.6 ETA

When `Rate > 0`:

\[
ETA = \frac{ArraySize - Processed}{Rate}
\]

Otherwise display `—`.

ETA is explicitly an estimate produced by the client, not an AWS-provided value.

### 9.7 Stalled state

Do **not** implement a formal `STALLED` classification in MVP.

It is acceptable to display the time since `statusSummaryLastUpdatedAt` or `Last progress` in the Array Overview, but do not invent a new job status.

---

## 10. Top-level navigation model

The application has four top-level tabs:

```text
[Active]  Failed  Recent  All
```

Default on startup: `Active`.

### 10.1 Active

Contains jobs that have not reached a terminal state.

For Array Jobs, parent status behavior must be handled correctly; a parent in `PENDING` with running children is active.

Include:

- single jobs in non-terminal statuses
- Array Job parents in non-terminal statuses

Do not show Array Job children as independent rows on the home screen.

Default sort:

1. failed-child count descending for active Array Jobs where `failed > 0`
2. otherwise most recently created first

A simpler deterministic sort such as newest-first is acceptable for the first implementation if the sort logic is isolated and easy to change, but failed active arrays should be visually obvious.

### 10.2 Failed

Shows terminal top-level jobs whose result is failed.

For an Array Job, show the parent once, not its children as top-level rows.

Sort newest terminal failure first.

### 10.3 Recent

Shows recently completed top-level jobs, both successful and failed.

MVP definition:

- terminal jobs only
- newest `stopped_at` first
- retain/display up to 500 top-level rows in memory

The adapter may stop pagination once enough rows are available for this view.

### 10.4 All

Shows top-level jobs regardless of status.

This is an investigation/search view, not an exhaustive account-history database.

Use pagination/lazy loading. Do not attempt to download an unbounded account history at startup.

---

## 11. Queue discovery and top-level job retrieval

`ListJobs` is scoped to a Job Queue, Array Job ID, or MNP Job ID. Therefore the home view must first know the queues in the selected Region.

The MVP supports only container Job Queues whose `jobQueueType` is one of:

```text
ECS
ECS_FARGATE
EKS
```

Ignore `SAGEMAKER_TRAINING` Job Queues. Do not call `ListServiceJobs` or `DescribeServiceJob` in the MVP.

### 11.1 Queue discovery

Use `DescribeJobQueues` with pagination.

At startup:

1. load all visible container Job Queues in the selected Region, including disabled queues that may still contain existing jobs,
2. cache the queue identifiers/names in memory.

Refresh the queue list:

- on manual refresh, and
- periodically on a slower cadence than job status; 60 seconds is sufficient for MVP.

A failure to refresh queues after successful startup must not delete the last known queue list.

### 11.2 Job list polling

For active data, issue `ListJobs` calls per queue for the relevant non-terminal statuses.

Required non-terminal statuses:

```text
SUBMITTED
PENDING
RUNNABLE
STARTING
RUNNING
```

Do not assume omission of `jobStatus` means "all statuses"; AWS Batch defaults that case to `RUNNING`.

Use pagination correctly.

Use bounded concurrency. Do not spawn an unbounded number of requests for `number_of_queues × statuses`.

Suggested concurrency limit: 8 in-flight AWS Batch list requests.

The exact limit may be configurable internally, but must be bounded.

### 11.3 Top-level filtering

Rows intended for the home list are:

- single jobs
- Array Job parents
- top-level MNP jobs represented using the single-job UI

If a queue-scoped result represents an Array child (`arrayProperties.index` is present), exclude it from the home list.

If an Array parent is identified (`arrayProperties.size` and parent summary are present), map it to `JobKind::ArrayParent`.

If a queue-scoped result is a top-level MNP job, show it as a single-job row. Do not call `ListJobs(multiNodeJobId = ...)` and do not expose node drill-down in MVP.

Deduplicate rows by `job_id` before updating the UI state.

---

## 12. Refresh semantics

### 12.1 Active job data

Target refresh cadence: every **2 seconds**.

Apply this cadence only to the currently visible active resource:

- the `Active` home tab while it is visible, or
- an active Array, child, or single-job detail screen while it is visible.

Do not continue polling a hidden home tab or detail screen merely to keep its cache warm. `Failed`, `Recent`, and `All` load when entered and refresh only on explicit manual refresh. A terminal job detail loads when entered and refreshes only on explicit manual refresh.

Interpret this as a scheduling target, not as overlapping requests every two seconds.

If the prior refresh round has not completed:

- do not start duplicate refresh rounds for the same resource set,
- either skip/coalesce the tick or cancel/replace safely.

### 12.2 Manual refresh

Global manual refresh key:

```text
Ctrl+r
```

Manual refresh should:

- trigger an immediate refresh of data relevant to the current screen,
- not freeze the UI,
- preserve current selection where possible.

### 12.3 Stale responses

Navigation may change while an async request is in flight.

Do not allow an old response to overwrite newer state for another selected job.

Use one of:

- request generation IDs,
- cancellation tokens,
- resource keys checked when applying responses.

Example:

```text
request: LoadChildren(parent=A, generation=12)
user navigates to parent=B
response for A arrives
=> cache if useful, but do not replace B's visible state
```

---

## 13. Application architecture

Use unidirectional state flow.

Conceptual architecture:

```text
Keyboard / Timer / AWS response
            |
            v
         Action
            |
            v
        App State
       /         \
      v           v
 Render UI      Effects
                  |
                  v
             Async AWS tasks
                  |
                  v
                Action
```

### 13.1 Required separation

UI components must not directly call AWS SDK clients.

Recommended modules:

```text
src/
├── main.rs
├── cli.rs
├── app.rs
├── action.rs
├── event.rs
├── state/
│   ├── mod.rs
│   ├── home.rs
│   ├── array_job.rs
│   ├── child_job.rs
│   └── logs.rs
├── domain/
│   ├── mod.rs
│   ├── job.rs
│   └── metrics.rs
├── aws/
│   ├── mod.rs
│   ├── batch.rs
│   └── logs.rs
├── ui/
│   ├── mod.rs
│   ├── home.rs
│   ├── array_overview.rs
│   ├── children.rs
│   ├── failures.rs
│   ├── child_overview.rs
│   ├── logs.rs
│   ├── raw.rs
│   └── widgets.rs
└── terminal.rs
```

This is guidance rather than a required exact file tree.

### 13.2 Suggested Action enum

```rust
enum Action {
    Tick,
    ManualRefresh,

    NextTopTab,
    PrevTopTab,
    MoveUp,
    MoveDown,
    OpenSelected,
    Back,

    StartSearch,
    SearchInput(char),
    SearchBackspace,
    ApplySearch,
    CancelSearch,

    HomeDataLoaded(...),
    JobDetailLoaded(...),
    ChildrenLoaded(...),
    ChildDetailLoaded(...),
    LogsLoaded(...),

    AwsError(...),
}
```

Screen-specific actions may be separate.

---

## 14. Screen hierarchy

Limit navigation depth to three conceptual levels.

```text
Home
  |
  +-- Array Job
  |     |
  |     +-- Child Job
  |            |
  |            +-- Logs
  |
  +-- Single Job
        |
        +-- Overview / Logs where applicable
```

Primary Array Job path:

```text
Active / Failed / Recent / All
            |
          Enter
            v
        Array Job
   Overview / Children /
   Failures / Parameters / Raw
            |
          Enter
            v
         Child Job
   Overview / Logs / Attempts /
   Container / Raw
```

---

## 15. Home screen

### 15.1 Default large layout

Example:

```text
 AWS Batch    profile: research-prod    region: ap-northeast-1        ↻ 1.2s

 [Active 12]   Failed   Recent   All

 QUEUE       JOB                       PROGRESS          OK    RUN  FAIL  WAIT    TIME
 production  factor_daily_20260811    ███████████░ 78%  7812  128     3  2057     24m
 production  risk_model_20260811      ██████░░░░░░ 46%   458   64     0   478     13m
 research    alpha_grid_20260811      █████████████ 96%  9581   18     7   394   1h02m
 nightly     import_prices            RUNNING               —    —     —     —      8m
 nightly     universe_build           RUNNABLE              —    —     —     —      2m

──────────────────────────────────────────────────────────────────────────────
 ↑↓/jk select  Enter open  Tab view  / search  Ctrl+r refresh  ? help  q quit
```

### 15.2 Array row requirements

For an Array parent, display as much as width permits:

- Job name
- Progress
- Failed count
- Running count
- Waiting count
- Succeeded count
- Elapsed time
- Queue
- Rate
- ETA

The highest-priority information is:

1. Job name
2. Progress
3. Failed count

### 15.3 Single-job row

Single jobs share the same table.

Do not fabricate Array metrics. Example:

```text
nightly  import_prices  RUNNING  ...
```

Use `—` for Array-specific numeric fields when needed.

### 15.4 Status summary freshness

Show a small refresh/freshness indicator in the top bar.

If a parent Array Job's `statusSummaryLastUpdatedAt` is substantially older than the latest application refresh, the detail view should expose that timestamp rather than pretending the summary is current.

---

## 16. Responsive terminal behavior

Implement graceful column reduction.

### 16.1 Around 80 columns

Prioritize:

```text
JOB                 PROGRESS        FAIL   TIME
factor_daily         7815/10000        3    24m
risk_model            458/1000         0    13m
```

### 16.2 Around 120 columns

Add:

```text
QUEUE   JOB                PROGRESS       RUN   FAIL   WAIT   TIME
prod    factor_daily       78% 7815/10k   128      3   2054    24m
```

### 16.3 Around 160 columns

Add rate/ETA/succeeded:

```text
QUEUE   JOB                PROGRESS    RATE    ETA    OK    RUN  FAIL  WAIT   TIME
prod    factor_daily       78.2%       421/m   5m     7812  128     3  2057    24m
```

### 16.4 Column priority

```text
P0  Job
P0  Progress
P0  Fail

P1  Time
P1  Running
P1  Waiting

P2  Queue
P2  Rate
P2  ETA
P2  Succeeded
```

If the terminal is too small to render a usable interface, show a centered message with the required minimum dimensions instead of panicking.

Suggested minimum: approximately 70 columns × 15 rows.

---

## 17. Array Job detail screen

Tabs:

```text
[Overview]  Children  Failures  Parameters  Raw
```

There is intentionally no parent `Logs` tab.

Switch detail tabs with:

- `Tab` / `Shift+Tab`, and
- optionally left/right arrows.

### 17.1 Array Overview

Example:

```text
 factor_daily_20260811                                  PENDING   24m 18s

 [Overview]   Children   Failures   Parameters   Raw

 Progress
 ███████████████████████████████████████░░░░░░░░░  78.15%

  7,812 succeeded    128 running      3 failed       2,057 waiting

  Size                  10,000
  Processed              7,815 / 10,000
  Success rate           99.96%
  Rate                   421 jobs/min
  ETA                    ~5m 11s
  Summary updated        0.8s ago

 Job
  Queue                   production
  Definition              factor-daily:42
  Created                 10:34:11
  Started                 10:36:02
  Elapsed                 24m 18s

 Children
  SUBMITTED                  0
  PENDING                   31
  RUNNABLE                1991
  STARTING                  35
  RUNNING                  128
  SUCCEEDED               7812
  FAILED                     3

──────────────────────────────────────────────────────────────────────────────
 Esc back   Tab next   c children   f failures   Ctrl+r refresh
```

The AWS parent status such as `PENDING` must be displayed accurately even when children are running. Do not relabel it as `RUNNING`.

---

## 18. Children tab

Children are loaded lazily only when required.

Use:

```text
ListJobs(arrayJobId = parent_job_id)
```

Do not load all children on application startup.

### 18.1 Layout

```text
 factor_daily_20260811 / Children

 Overview   [Children]   Failures   Parameters   Raw

 Filter: [All] Running Waiting Failed Succeeded       / index or text search

 INDEX    STATUS       ELAPSED     STARTED       EXIT   ATTEMPT
 7819     SUCCEEDED       42s      10:58:12         0    1/3
 7820     SUCCEEDED       39s      10:58:15         0    1/3
 7821     RUNNING       2m13s      10:58:21              1/3
 7822     RUNNING       2m11s      10:58:23              1/3
>7823     FAILED          14s      10:58:24       137    3/3
 7824     RUNNABLE
 7825     RUNNABLE

 Loaded 1,000 / 10,000 children
──────────────────────────────────────────────────────────────────────────────
 ↑↓/jk select  Enter detail  a all  r running  w waiting  f failed  / search
```

### 18.2 Child filters

Required logical filters:

- `All`
- `Running`
- `Waiting`
- `Failed`
- `Succeeded`

`Waiting` groups:

- `SUBMITTED`
- `PENDING`
- `RUNNABLE`
- `STARTING`

### 18.3 Pagination

Array size may reach 10,000.

Requirements:

- support AWS pagination,
- do not block UI while loading pages,
- do not require all 10,000 children to load before first render,
- show loaded count,
- load additional pages as needed.

Acceptable strategies:

- prefetch the next page as the cursor approaches the end, or
- explicit lazy loading triggered by navigation.

Avoid firing a new pagination request on every keypress.

### 18.4 Search

`/` enters search mode.

At minimum support:

- exact/numeric Array index search against loaded data,
- substring search against locally loaded status/reason/job ID text.

If a requested numeric Array index is not loaded, the implementation may continue pagination until:

- the index is found,
- pagination ends, or
- the request is cancelled by navigation.

Do not claim server-side index filtering if AWS does not provide it.

---

## 19. Failures tab

This is a first-class view, not merely a hidden Children filter.

Load failed children using Array Job child listing with `jobStatus=FAILED` where supported by the API request semantics.

Example:

```text
 factor_daily_20260811 / Failures                         3 failures

 Overview   Children   [Failures]   Parameters   Raw

 INDEX    EXIT   ATTEMPTS   REASON                         RUNTIME
>7823      137     3/3      OutOfMemoryError                  14s
 8119        1     1/3      Essential container exited      2m31s
 9341      137     3/3      OutOfMemoryError                  19s

──────────────────────────────────────────────────────────────────────────────
 Enter detail   l logs   / search   Esc back
```

Requirements:

- failed children should be obtainable without loading all succeeded children first,
- selecting a failure and pressing `Enter` opens Child Job detail,
- pressing `l` may directly open that child's Logs screen if the log stream is available.

Failure-reason grouping is a desirable follow-up feature but is not required for MVP.

---

## 20. Child Job detail

Tabs:

```text
[Overview]  Logs  Attempts  Container  Raw
```

Example:

```text
 child 7823                                             FAILED

 [Overview]   Logs   Attempts   Container   Raw

 Parent          factor_daily_20260811
 Array index     7823
 Job ID          ...
 Exit code       137
 Attempts        3 / 3
 Reason          OutOfMemoryError

 Started         10:58:24
 Stopped         10:58:38
 Runtime         14s

 Resources
 vCPU            4
 Memory          8 GiB

──────────────────────────────────────────────────────────────────────────────
 l logs   Tab next   p parent   Esc back
```

Use `DescribeJobs` for detailed child information.

`DescribeJobs` accepts up to 100 job IDs per request. Batch descriptions where beneficial, but do not overcomplicate the MVP.

### 20.1 Single Job detail

Opening a top-level single job or top-level MNP job uses the same detail structure as Child Job detail:

```text
[Overview]  Logs  Attempts  Container  Raw
```

Requirements:

- omit parent and Array-index fields,
- show the job ID, Queue, Job Definition, status, reason, timestamps, runtime, exit code, attempt count, and resources where available,
- use `DescribeJobs` for the detail source,
- use the same Logs, Attempts, Container, and Raw behaviors as Child Job detail,
- do not expose MNP node listing or node drill-down.

---

## 21. Child and single-job Logs screen

CloudWatch Logs are loaded only for a selected child or single job. Array parent logs remain unavailable in MVP.

Derive the log stream from the latest attempt's primary container in the Batch job detail. Determine the latest attempt by its timestamps where available, with response order as a fallback. Do not provide an attempt or container selector in MVP.

Resolve the CloudWatch Logs location in this order:

1. log group from `logConfiguration.options["awslogs-group"]`
2. otherwise `/aws/batch/job`

and:

1. Logs Region from `logConfiguration.options["awslogs-region"]`
2. otherwise the selected Batch Region

Use the latest attempt's primary-container `logStreamName`. A top-level current-container `logStreamName` may be used as a fallback when the attempt representation is absent. Do not aggregate streams across attempts or containers.

Use CloudWatch Logs `GetLogEvents`.

### 21.1 Layout

```text
 factor_daily_20260811[7823] / Logs                  FOLLOW ON

 10:58:24 INFO  loading partition 7823
 10:58:27 INFO  loading parquet files
 10:58:31 INFO  constructing feature matrix
 10:58:36 WARN  memory usage 7.6 GiB
 10:58:37 WARN  memory usage 7.9 GiB
>10:58:38 ERROR process killed with exit code 137

──────────────────────────────────────────────────────────────────────────────
 ↑↓/jk scroll   g top   G bottom   f follow   / search   n next   Esc back
```

### 21.2 Log behavior

Required:

- initial log load
- scroll
- local text search
- next match
- follow mode
- pagination backward/forward as needed
- bounded in-memory log retention

Suggested maximum retained lines/events: approximately 5,000.

Use a deque/ring-buffer approach.

If older events are discarded, make that visible when the user reaches the retained boundary.

### 21.3 CloudWatch pagination correctness

Do not assume an empty or partially filled `GetLogEvents` page means pagination has completed.

Follow CloudWatch token semantics.

For follow mode:

- poll only while the Logs screen is visible and follow is enabled,
- stop/cancel polling after leaving the screen,
- avoid a tight loop when no new logs arrive.

A 1–2 second follow polling cadence is acceptable for MVP.

### 21.4 Missing or unsupported log stream

A child may not yet have a log stream.

Display a non-fatal state such as:

```text
Log stream is not available yet.
```

Allow `Ctrl+r` or normal polling to retry if the child is still active.

Apply the same non-fatal behavior when:

- the latest attempt has no primary-container log stream,
- the configured log driver is not supported by `GetLogEvents`, or
- the configured log group or Logs Region cannot be resolved.

---

## 22. Parameters tab

For an Array parent, display the job parameters returned by `DescribeJobs`.

Render as a key/value table.

Example:

```text
KEY                 VALUE
date                2026-08-11
universe            TOPIX
bucket              20260811
```

If none exist:

```text
No parameters.
```

Do not create an editor.

---

## 23. Raw tab

Provide a formatted JSON-like representation of the relevant AWS job description.

Purpose:

- expose AWS fields not yet modeled in the custom UI,
- make the tool useful for troubleshooting without immediately adding a bespoke screen for every Batch field.

Requirements:

- read-only
- scrollable
- syntax coloring optional
- local search desirable
- source data should correspond to the latest `DescribeJobs` result for the selected resource

Do not include secrets not returned by AWS Batch. Do not resolve secret values from Secrets Manager or SSM.

---

## 24. Keyboard model

### 24.1 Global

```text
q              quit
?              help
/              search in current screen where supported
Ctrl+r         manual refresh
Tab            next tab
Shift+Tab      previous tab
Esc            back / cancel mode
```

### 24.2 Navigation

```text
j / Down       next row
k / Up         previous row
Enter          open selected row
```

### 24.3 Array Job shortcuts

```text
c              Children
f              Failures
p              Parameters
```

### 24.4 Children filters

```text
a              All
r              Running
w              Waiting
f              Failed
s              Succeeded
```

Context-specific `r` is allowed because global refresh is `Ctrl+r`.

### 24.5 Child shortcuts

```text
l              Logs
p              Parent
```

### 24.6 Logs

```text
f              toggle follow
g              top of retained buffer
G              bottom of retained buffer
n              next search match
N              previous search match
```

### 24.7 Search mode

When search mode is active:

```text
Enter          apply/confirm
Esc            cancel
Backspace      delete character
printable char append character
```

Search input must consume keys that would otherwise trigger application shortcuts.

---

## 25. Focus and navigation state

Avoid an implicit UI focus model with many independently focusable widgets.

For MVP, each screen should have one primary navigation target:

- Home: job table
- Children: child table
- Failures: failed-child table
- Logs: log viewport
- Raw: raw-text viewport

Tabs are navigation state, not a separately focusable widget.

This keeps keyboard behavior deterministic.

---

## 26. Loading states

Never clear useful existing data merely because a refresh started.

Preferred pattern:

```text
existing content remains visible
top bar shows ↻
new result atomically replaces relevant cached data
```

For first load, show:

```text
Loading AWS Batch jobs…
```

For lazily loaded children/logs:

```text
Loading children…
Loading logs…
```

The UI event loop must remain responsive.

---

## 27. Error handling

### 27.1 Non-fatal API errors

Examples:

- throttling
- transient network failure
- CloudWatch Logs permission failure
- one Job Queue failing to list
- temporary credential issue

Requirements:

- preserve last known good data,
- show an unobtrusive but visible error banner/status,
- record details through `tracing`,
- allow the next polling cycle/manual refresh to recover.

Example status:

```text
⚠ refresh failed: AccessDeniedException (CloudWatch Logs)
```

Do not replace the entire screen with an error page unless no usable data exists.

### 27.2 Partial refresh

If 8 queues refresh successfully and 1 fails:

- keep previous data for the failed queue where possible,
- update successful queues,
- surface that the snapshot is partial.

### 27.3 AWS timeouts/retries

Use AWS SDK retry behavior.

Configure finite operation/attempt timeouts so a request cannot leave a screen effectively hung indefinitely.

Exact values are implementation choices, but a reasonable MVP target is:

- operation attempt timeout: approximately 5 seconds
- total operation timeout: approximately 15 seconds

The UI itself must not wait synchronously for those timeouts.

---

## 28. Terminal safety

The application must reliably restore terminal state on:

- normal quit
- `Ctrl+C`
- recoverable application error leading to exit
- panic where practical

Terminal setup:

- raw mode
- alternate screen
- cursor handling as appropriate

Encapsulate terminal setup/restoration so cleanup occurs once and is hard to bypass.

Install a panic hook that attempts terminal restoration before emitting panic information.

---

## 29. Diagnostic logging

Do not print diagnostic logs directly into the active TUI.

Support a debug option such as:

```bash
batchtop --debug-log ./batchtop.log
```

or an equivalent environment-controlled tracing destination.

Log:

- startup configuration excluding secrets
- effective Region
- queue discovery
- AWS request failures
- refresh durations
- pagination failures
- stale-response suppression
- terminal/render errors

Never log AWS secret keys, session tokens, or resolved secret values.

---

## 30. UI color semantics

Color should supplement text/symbols, not be the only carrier of meaning.

Suggested semantics:

- succeeded: success styling
- running: active styling
- failed: high-attention/error styling
- waiting states: neutral/dim styling
- selected row: obvious highlight

Do not hard-code assumptions that only work on a dark terminal.

Prefer Ratatui terminal colors/default foreground/background where possible.

The interface must remain understandable with color disabled or in limited-color terminals.

---

## 31. Sorting and selection stability

On refresh:

- preserve selected `job_id` if it is still present,
- if it disappears, select the nearest logical row,
- do not preserve selection merely by numeric row index.

Apply the same rule to children using child `job_id` or array index.

This prevents the cursor from jumping to another job when rows reorder.

---

## 32. Time display

Use the machine's local timezone for user-facing timestamps by default.

Use compact relative durations for operational views:

```text
24m
1h02m
14s
```

Raw AWS timestamps may appear in the Raw view.

Do not silently mix UTC and local wall-clock timestamps in the same human-readable table.

---

## 33. Caching policy

MVP caches are memory-only.

Cache:

- discovered Job Queues
- current top-level job summaries
- selected job `DescribeJobs` results
- loaded child pages
- loaded child details
- bounded log buffer
- short progress-rate sample history

Do not implement SQLite or persistent cache.

Cache invalidation may be simple:

- active summaries replaced on refresh,
- detail cache refreshed when screen is active or manually refreshed,
- child/log cache tied to resource IDs.

---

## 34. Performance requirements

The application must remain usable with:

- Array Job size up to 10,000 children
- multiple active Array Jobs
- up to approximately 10 visible container Job Queues

Performance rules:

1. Never load all child details at startup.
2. Never call `DescribeJobs` individually for every child solely to render the parent list.
3. Use Array parent `statusSummary` for parent progress.
4. Use lazy pagination for child lists.
5. Batch `DescribeJobs` where multiple detailed records are genuinely needed.
6. Bound AWS request concurrency.
7. Bound log memory.
8. Rendering must operate on in-memory state only; no AWS call from render functions.

---

## 35. Testing requirements

### 35.1 Domain unit tests

Test at minimum:

- processed count
- progress
- waiting count
- success rate
- zero-terminal-child success rate
- rate calculation
- ETA calculation
- rate behavior when counts decrease/stale samples occur
- status grouping
- Array parent vs Array child classification

Example:

```text
size       = 10,000
succeeded  = 7,812
failed     = 3

processed  = 7,815
progress   = 78.15%
```

### 35.2 Navigation/state tests

Test:

- Home → Array Overview
- Home → Single Job Overview
- Array Overview → Children
- Array Overview → Failures
- Failure → Child Overview
- Child Overview → Logs
- Esc/back behavior
- Tab behavior
- search-mode key capture
- selection preserved across refresh

### 35.3 AWS adapter tests

Define adapter traits/interfaces so application tests do not require live AWS credentials.

Mock at least:

- queue discovery and container/service Job Queue classification
- active job list
- top-level MNP classification as a single job
- paginated child list
- failed child list
- DescribeJobs
- latest-attempt and primary-container log selection
- custom/default log group and Logs Region resolution
- GetLogEvents
- AWS error
- partial queue failure

### 35.4 TUI rendering tests

Use Ratatui's test backend or equivalent for selected snapshots/layout assertions.

Test representative terminal widths:

- ~80 columns
- ~120 columns
- ~160 columns
- below minimum size

Avoid brittle assertions on every space if a higher-level widget/layout assertion is sufficient.

### 35.5 No live AWS tests in MVP

Do not require, implement, or run live AWS integration or smoke tests for MVP verification.

Requirements:

- the default and complete MVP test suite must run without AWS credentials,
- AWS responses, pagination tokens, throttling, partial failures, and credential/configuration errors must be represented through mocks or fixtures,
- no test may make an AWS network call,
- passing a live AWS smoke test is not an MVP acceptance criterion.

---

## 36. MVP acceptance criteria

The MVP is complete when all of the following are true. Verification is performed with unit tests, state tests, adapter mocks/fixtures, and Ratatui rendering tests; live AWS validation is explicitly excluded.

### Startup and configuration

- [ ] `batchtop` constructs its AWS SDK configuration using standard credentials/configuration providers.
- [ ] `--profile` works.
- [ ] `--region` works.
- [ ] Effective Region is visible.
- [ ] Startup failures restore terminal state.

### Home

- [ ] Default tab is `Active`.
- [ ] Jobs are collected across visible `ECS`, `ECS_FARGATE`, and `EKS` Job Queues in the selected Region.
- [ ] `SAGEMAKER_TRAINING` Job Queues are excluded without calling service-job APIs.
- [ ] Single jobs and Array parents appear in the same list.
- [ ] Top-level MNP jobs use the single-job presentation and provide no node drill-down.
- [ ] Array children do not appear as top-level rows.
- [ ] Active Array parents are visible even when AWS reports the parent as `PENDING`.
- [ ] Array progress is visible without loading all children.
- [ ] `Succeeded`, `Running`, `Failed`, and `Waiting` counts are available subject to terminal width.
- [ ] Failed child count is always high-priority information.
- [ ] The currently visible active list or active job detail has a 2-second refresh target without overlapping runaway request rounds.
- [ ] Hidden and terminal views are not polled continuously.
- [ ] UI remains responsive during refresh.

### Array Job detail

- [ ] `Enter` on an Array parent opens Array Overview.
- [ ] Overview shows size, processed count, progress, success rate, child state counts, Queue, Job Definition, timestamps.
- [ ] Rate/ETA display when enough local samples exist.
- [ ] Tabs are `Overview`, `Children`, `Failures`, `Parameters`, `Raw`.
- [ ] No parent `Logs` tab exists.

### Children

- [ ] Children load lazily.
- [ ] Array index is visible.
- [ ] Child status is visible.
- [ ] Pagination works.
- [ ] `All`, `Running`, `Waiting`, `Failed`, `Succeeded` filters work.
- [ ] User can drill into a selected child.

### Failures

- [ ] Failed children can be viewed without first loading every successful child.
- [ ] Index, exit code where available, attempt information, reason, and runtime are shown where available.
- [ ] User can open failed child detail.

### Child/single-job detail and Logs

- [ ] Child detail is loaded through `DescribeJobs`.
- [ ] Child Array index is shown.
- [ ] Single and top-level MNP jobs open `Overview`, `Logs`, `Attempts`, `Container`, and `Raw` detail tabs.
- [ ] Logs are opened from a child or single job, never from an Array parent.
- [ ] Logs use the latest attempt's primary container without attempt/container switching.
- [ ] Configured `awslogs-group` and `awslogs-region` values are honored, with documented defaults.
- [ ] `GetLogEvents` pagination is handled correctly.
- [ ] Log scrolling works.
- [ ] Log search works.
- [ ] Follow mode works.
- [ ] Log polling stops when leaving the Logs screen.
- [ ] Missing log stream is handled non-fatally.
- [ ] Log memory is bounded.

### Interaction and safety

- [ ] All required workflows are keyboard operable.
- [ ] `j/k` and arrows navigate rows.
- [ ] `Enter` drills down.
- [ ] `Esc` goes back.
- [ ] `Tab` switches tabs.
- [ ] `/` enters search where supported.
- [ ] `Ctrl+r` refreshes.
- [ ] `q` quits.
- [ ] Terminal is restored after normal quit and `Ctrl+C`.
- [ ] No AWS mutation API is exposed or called by the application.

### Quality

- [ ] Domain metrics have unit tests.
- [ ] AWS adapters are mockable.
- [ ] Key navigation flows have tests.
- [ ] Responsive layouts have tests.
- [ ] The complete test suite runs without AWS credentials or AWS network calls.
- [ ] `cargo fmt` passes.
- [ ] `cargo clippy` passes with no unjustified warnings.
- [ ] `cargo test` passes.

---

## 37. Suggested implementation order

The coding agent should implement in the following order unless repository constraints require otherwise.

### Phase 1 — Skeleton

1. Cargo project and CLI parsing
2. terminal setup/restoration
3. event loop
4. empty Ratatui shell
5. AWS config loading

### Phase 2 — Home data

1. Batch adapter trait
2. Job Queue discovery
3. container/service Job Queue classification and service-queue exclusion
4. per-queue job listing
5. domain conversion
6. top-level Array-parent/single/MNP classification
7. `Active` screen
8. visible-resource 2-second async refresh
9. selection preservation

### Phase 3 — Array progress

1. Array status summary mapping
2. progress/waiting/success-rate metrics
3. progress rendering
4. in-memory sampling
5. rate/ETA

### Phase 4 — Drill-down

1. Array Overview
2. `DescribeJobs`
3. Single Job Overview
4. Parameters
5. Raw
6. screen navigation/back stack

### Phase 5 — Children and failures

1. paginated child adapter
2. Children view
3. child filters
4. Failures view
5. Child Overview

### Phase 6 — Logs

1. CloudWatch Logs adapter
2. latest-attempt/primary-container stream resolution
3. configured/default log group and Logs Region resolution
4. initial log retrieval
5. token-aware pagination
6. bounded buffer
7. follow mode
8. search

### Phase 7 — Hardening

1. error banners
2. partial refresh behavior
3. request generation/cancellation
4. timeout configuration
5. debug logging
6. help screen
7. responsive layouts
8. tests and cleanup

---

## 38. Important implementation constraints for the coding agent

Treat these as normative:

- **MUST** keep the UI event loop non-blocking.
- **MUST** use parent Array status summaries for parent progress.
- **MUST NOT** enumerate all child details to calculate parent progress.
- **MUST** account for Array parents remaining `PENDING` while children run.
- **MUST** lazily retrieve children.
- **MUST** retrieve logs only from a child or single job in MVP; never from an Array parent.
- **MUST** exclude `SAGEMAKER_TRAINING` Job Queues and service-job APIs.
- **MUST NOT** provide MNP node drill-down.
- **MUST** use only the latest attempt's primary-container log stream.
- **MUST** honor configured `awslogs-group` and `awslogs-region` values with the documented fallbacks.
- **MUST** keep AWS operations read-only.
- **MUST** bound concurrency and log memory.
- **MUST** restore terminal state on exit.
- **MUST** isolate AWS SDK usage from rendering components.
- **MUST** prevent stale async responses from replacing state for a newly selected resource.
- **SHOULD** preserve last known good data on transient refresh failures.
- **SHOULD** preserve selection by resource identity rather than row index.
- **SHOULD** expose unmodeled job information through the Raw tab.
- **MAY** add minor UI refinements that do not alter the workflows or semantics specified here.

---

## 39. Official AWS references

Implementation should be checked against current AWS documentation at coding time.

- AWS Batch `ListJobs`  
  https://docs.aws.amazon.com/batch/latest/APIReference/API_ListJobs.html
- AWS Batch `DescribeJobs`  
  https://docs.aws.amazon.com/batch/latest/APIReference/API_DescribeJobs.html
- AWS Batch `DescribeJobQueues`  
  https://docs.aws.amazon.com/batch/latest/APIReference/API_DescribeJobQueues.html
- AWS Batch `JobQueueDetail`  
  https://docs.aws.amazon.com/batch/latest/APIReference/API_JobQueueDetail.html
- AWS Batch `ArrayPropertiesSummary`  
  https://docs.aws.amazon.com/batch/latest/APIReference/API_ArrayPropertiesSummary.html
- AWS Batch `ContainerDetail`  
  https://docs.aws.amazon.com/batch/latest/APIReference/API_ContainerDetail.html
- CloudWatch Logs `GetLogEvents`  
  https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_GetLogEvents.html
- AWS SDK for Rust credentials  
  https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html
- AWS SDK for Rust Region configuration  
  https://docs.aws.amazon.com/sdk-for-rust/latest/dg/region.html
- AWS SDK for Rust timeouts  
  https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html
- AWS SDK for Rust retries  
  https://docs.aws.amazon.com/sdk-for-rust/latest/dg/retries.html

---

## 40. Definition of the product in one sentence

> A read-only, keyboard-first AWS Batch TUI optimized for seeing Array Job progress immediately and reaching a failed child job's details and CloudWatch Logs with minimal navigation.
