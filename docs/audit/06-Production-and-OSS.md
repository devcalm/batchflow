# Pass 12 — Production Readiness

Reviewed: logging, tracing, metrics, configuration, graceful shutdown,
timeouts, retry, observability, health checks, diagnostics, profiling hooks,
versioning, feature flags, backward compatibility, MSRV, CI/CD, releases.

## Baseline

The **observability vocabulary is exceptional** and should be left alone:

- Metrics carry only bounded, author-written label values; the module doc
  explains that one execution id per label is one time series per run kept
  forever, and routes that question to tracing instead. Very few projects get
  this right, and fewer document why.
- Spans carry the ids that metrics must not. `job` and `step`, nested, with a
  deliberate absence of a chunk span (10M items at chunk size 100 would be
  100k spans per run) and the reasoning stated.
- Counters are published only after the commit that persisted the same
  numbers, with retries as the one documented exception —
  `sum(items_written_total)` reconciles with `sum(write_count)` and there is a
  test that says so.
- `describe()` is called after installation, not before, because descriptions
  live in the recorder. There is a test asserting `# HELP` lines reach the
  scrape.
- Histograms get explicit buckets, because a bucket-less
  `metrics-exporter-prometheus` renders a **summary**, whose quantiles cannot be
  aggregated across processes. There is a test asserting the scrape contains
  `le=` and not `quantile=`.

That is the ceiling. Everything below is the floor.

---

<a id="prod-1"></a>
### PROD-1 — There is no way to stop a running job

**Severity:** Critical · **Effort:** M (2–3 days)

Covered in full as [ASYNC-1](03-Async-and-Concurrency.md#async-1); repeated
here because it is a production-readiness fact rather than an async design
detail.

**The operational shape of it:** deploy a batch service to Kubernetes. A rolling
update sends SIGTERM. There is no handler the framework offers, so either the
process ignores it and is SIGKILLed at the grace period, or the application
drops the job future — and in both cases `job_execution.status` stays
`STARTED`. The next scheduled tick is refused with
`JobExecutionAlreadyRunning`, naming a process that no longer exists.
Recovery requires a human running `abandon_execution`.

`BatchStatus::Stopped` exists in the enum, is stored by both backends, has a
metric label value with a test pinning the string, and is accepted by the
launcher's restart gate — **and nothing ever sets it.**

**Recommendation:** see ASYNC-1 for the design. The point to add here is that
the restart machinery already exists, so the whole feature is roughly 80 lines
plus tests. It is the single largest gap between this framework and a
deployable one.

---

<a id="prod-2"></a>
### PROD-2 — The metadata schema records no time and no reason

**Severity:** High · **Effort:** M (2–3 days) · **Files:** `migrations/0001_init.sql`, `execution.rs`

The full schema:

```sql
CREATE TABLE job_execution (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instance_id       BIGINT NOT NULL REFERENCES job_instance (id),
    status            TEXT   NOT NULL,
    execution_context JSONB  NOT NULL
);
```

No `created_at`. No `start_time`. No `end_time`. No `last_updated`. No
`exit_message`. `step_execution` is the same plus counters.

**Questions an operator cannot answer from the store:**

| Question | Why it cannot be answered |
|---|---|
| When did last night's run start? | No timestamp. |
| How long did it take? | No timestamps. `batchflow_step_duration_seconds` exists but is a histogram in the *process's* metrics — gone when the process exits, and not attributable to an execution. |
| Is this `STARTED` execution alive or is it a zombie? | No `last_updated`. This is exactly why `abandon_execution` must be a human decision, per its own doc comment. |
| Why did execution 4712 fail? | Nowhere to record it. See [ERR-1](04-Errors-and-Security.md#err-1). |
| Which executions are older than 90 days? | No timestamp — so [PROD-3](#prod-3) cannot even be implemented today. |
| How long did each step take, historically? | Only in whatever log retention the process had. |

The absence compounds: no `last_updated` means no heartbeat, no heartbeat means
no automatic reaper, no reaper means [SEC-1](04-Errors-and-Security.md#sec-1)'s
panic and [PROD-1](#prod-1)'s SIGKILL both require manual intervention. One
missing column is the root of three separate operational gaps.

**Recommendation.** Migration `0003`:

```sql
ALTER TABLE job_execution
    ADD COLUMN created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN started_at   TIMESTAMPTZ,
    ADD COLUMN ended_at     TIMESTAMPTZ,
    -- The heartbeat. Written by every chunk commit, so an execution whose
    -- last_updated is older than the chunk duration plus a margin is a
    -- candidate for automatic abandonment.
    ADD COLUMN last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN exit_message TEXT;

ALTER TABLE step_execution
    ADD COLUMN created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN started_at   TIMESTAMPTZ,
    ADD COLUMN ended_at     TIMESTAMPTZ,
    ADD COLUMN last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN exit_message TEXT;

-- Retention and reaper queries.
CREATE INDEX job_execution_by_created ON job_execution (created_at);
CREATE INDEX job_execution_live       ON job_execution (last_updated)
    WHERE status IN ('STARTING', 'STARTED');
```

**Two design decisions this forces, and both should be made deliberately:**

1. **Clock ownership.** `DEFAULT now()` uses the *database's* clock, which is
   the right answer for a distributed system — it is the one clock all
   participants agree on, and it means `batchflow-core` needs no time
   dependency at all. The in-memory store then needs `std::time::SystemTime`,
   which is a divergence to document rather than hide.
2. **`last_updated` costs a write per chunk** — but the chunk commit is
   *already* writing that row (see
   [PERF-3](02-Rust-Performance-Memory.md#perf-3)), so it is one more column in
   an UPDATE that is happening regardless. Free.

The core types (`JobExecution`, `StepExecution`) then gain accessors. Keep them
`Option<SystemTime>` for `started_at`/`ended_at` so "not started" and "started
at the epoch" are distinguishable.

**Benefit:** unlocks retention, the reaper, "how long did it take", and "why
did it fail" — four operational capabilities from one migration.

---

<a id="prod-3"></a>
### PROD-3 — The metadata tables grow forever and nothing prunes them

**Severity:** High · **Effort:** S (1 day, after PROD-2) · **Files:** `repository.rs`, migrations

There is no `delete`, no `purge`, no retention policy, no TTL, and no
documentation acknowledging the growth. Both backends only ever insert and
update.

**The growth rate is not small.** Per job *run*:

- 1 `job_execution` row.
- 1 `step_execution` row per step.
- And, on Postgres, **one dead tuple per chunk** on the `step_execution` row,
  because every chunk commit `UPDATE`s it (see
  [PERF-3](02-Rust-Performance-Memory.md#perf-3)).

An hourly job with 3 steps over 1M rows at chunk size 100 produces 24 executions
and 72 step rows per day — trivial — but **240,000 row versions per day** of
churn on `step_execution`, forever, with no vacuum tuning guidance anywhere.

On Redis, growth is in keys: `batchflow:execution:*`,
`batchflow:step:*`, and two ever-growing lists per instance
(`:executions`, `:step:{name}`) that are `RPUSH`ed and never trimmed. On a
store that must run `noeviction` ([SEC-2](04-Errors-and-Security.md#sec-2)),
unbounded growth eventually means write failures.

**Recommendation.**

1. A repository method, gated on PROD-2's timestamps:

   ```rust
   /// Delete completed and abandoned executions older than `cutoff`, with
   /// their step executions.
   ///
   /// Deliberately does not touch `Failed` executions or the `JobInstance`
   /// rows: a failed instance may still be restarted, and deleting the
   /// instance would let a completed job run a second time.
   ///
   /// Returns the number of executions removed. Bounded by `limit` so a first
   /// run against years of history does not take a long lock.
   fn purge_executions_before(
       &self,
       cutoff: SystemTime,
       limit: usize,
   ) -> impl Future<Output = Result<usize, BatchError>> + Send;
   ```

   The "do not delete `Failed`" rule is the subtle part and must be in the
   rustdoc: FR-4.4 is enforced by reading `last_execution`, so deleting a
   `Completed` execution while keeping its instance would **make the instance
   launchable again** — which is data corruption dressed as housekeeping. The
   safe rule is: delete an instance's executions only if the instance itself is
   also being deleted, or keep the terminal `Completed` marker.

   That subtlety alone justifies the framework providing this rather than
   leaving it to users' `DELETE` statements.

2. `FILLFACTOR` and autovacuum settings on `step_execution` (see PERF-3).

3. A `docs/Operations.md` section stating the growth model.

**Benefit:** prevents the failure mode where a framework works beautifully for
six months and then the metadata table is the largest thing in the database.

---

<a id="prod-4"></a>
### PROD-4 — No health, no readiness, no diagnostic surface

**Severity:** Medium · **Effort:** M

There is no way to ask the framework anything at runtime:

- **Is the metadata store reachable?** A `JobRepository::ping()` would be the
  obvious readiness probe; there is none, so a service must invent one or issue
  a raw query against the pool.
- **What is running right now?** No `list_running_executions()`. An admin CLI
  or a status endpoint has to write its own SQL against tables whose schema is
  not documented as an interface.
- **Stop a specific job.** Requires PROD-1 first, then a way to name the target.

Spring Batch calls this surface `JobOperator` and it is the thing every
production deployment ends up needing.

**Recommendation.** Not a whole `JobOperator` for 0.2.0, but the two cheapest
pieces:

```rust
/// A round trip to the store, for a readiness probe.
fn ping(&self) -> impl Future<Output = Result<(), BatchError>> + Send;

/// Every execution in a non-terminal status, across all instances.
/// The query an operator asks after a crash, and the input to any reaper.
fn running_executions(&self)
    -> impl Future<Output = Result<Vec<JobExecution>, BatchError>> + Send;
```

Both are single queries and both have obvious conformance cases. `ping` is
`SELECT 1` / `PING` / `Ok(())`.

**Benefit:** the difference between a library and an operable service
component.

---

<a id="prod-5"></a>
### PROD-5 — Feature flags, MSRV and versioning are handled well; one gap

**Severity:** Low

**What is right:**

- Per-crate MSRV (1.85 / 1.88 / 1.94) with a CI matrix lane per tier and the
  reasoning in each manifest. This is more correct than most workspaces.
- `msrv` lane uses `cargo check` without `--all-targets`, because
  `rust-version` promises the *library* builds and dev-dependencies carry their
  own MSRVs. Precisely right, and explained.
- Features are minimal and justified: `conformance` (dev-only), `cron`
  (37 → 66 dependencies, so flagged). Both have
  `[package.metadata.docs.rs] all-features = true` so docs.rs shows them.
- Workspace dependencies carry `default-features = false` at the root *with an
  explanation* that a workspace dependency's defaults cannot be turned off
  downstream — a genuine Cargo subtlety most people learn by debugging.
- `#[non_exhaustive]` on `BatchError`, `BatchStatus`, `ContextValue`,
  `JobParameter`, `Outcome`.

**The gap: no stated API stability policy.** The README says *"Pre-1.0, so the
API may still change; breaking changes will be a minor version bump"* — which
is the right policy, but it is one sentence in a README rather than a document.
For a crate asking to be a long-term dependency, an adopter wants to know:

- What is covered by semver and what is not (the conformance case *list*
  is not — see [ARCH-3](01-Architecture-and-API.md#arch-3); metric names and
  span names arguably *are*, since the code treats them as a published
  contract).
- The MSRV bump policy: is raising MSRV a minor or a patch? (Convention is
  split; state one.)
- What happens at 1.0.

**Recommendation.** A short `docs/Stability.md` (or a section in the README)
covering exactly those three. Half a page.

---

<a id="prod-6"></a>
### PROD-6 — No release automation

**Severity:** Medium · **Effort:** S · **Files:** `.github/workflows/`

One workflow, `ci.yml`. No publish job, no tag automation, no
`cargo-release`/`release-plz` configuration — for a **six-crate workspace that
releases in lockstep** with an inter-crate dependency graph
(`batchflow` → `batchflow-core`, and three more depending on core).

Publishing that by hand means: bump six versions, publish in dependency order,
wait for crates.io index propagation between each, and hope nothing was
forgotten. It is the kind of task that gets done correctly the first time and
wrong the third.

**Recommendation.** `release-plz` fits this workspace well (it understands
workspace version bumps and changelog generation, and the project already keeps
a Keep-a-Changelog file):

```yaml
  release:
    if: startsWith(github.ref, 'refs/tags/v')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo publish -p batchflow-core --locked
      - run: cargo publish -p batchflow --locked
      # ... in dependency order, with the index-propagation waits release-plz handles
    env:
      CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
```

Add a `cargo publish --dry-run --workspace` step to the **PR** lane too — it
catches missing `include`/`exclude` entries and unpublishable path
dependencies before a release, not during one.

**Benefit:** removes the highest-risk manual operation in the project.

---

# Pass 13 — Open Source Readiness

## Inventory

| Expected | Present | Notes |
|---|:---:|---|
| LICENSE (dual MIT/Apache-2.0) | ✅ | Both at root and duplicated into `crates/batchflow/`. Correct. |
| CHANGELOG.md | ✅ | Keep a Changelog format, links to compare ranges, and — unusually — a **Known limitations** section that discloses the launcher race. Genuinely good. |
| README | ⚠️ | Root README is excellent. Facade README is wrong; see [OSS-1](05-Docs-and-Testing.md#oss-1). |
| examples/ | ✅ | Five, all runnable, including a Postgres one that starts its own container. |
| benches/ | ⚠️ | Present, but ships `TODO(you)` prompts; see [PERF-2](02-Rust-Performance-Memory.md#perf-2). |
| CI | ✅ | Strong. See PROD-5. |
| MSRV documented | ✅ | Per crate, in a README table and in each manifest. |
| Feature flags documented | ✅ | With dependency-count justifications. |
| Semantic versioning | ⚠️ | Practised; not written down. See [PROD-5](#prod-5). |
| **CONTRIBUTING.md** | ❌ | |
| **CODE_OF_CONDUCT.md** | ❌ | |
| **SECURITY.md** | ❌ | |
| **Issue templates** | ❌ | |
| **PR template** | ❌ | |
| **Release workflow** | ❌ | See [PROD-6](#prod-6). |
| **Dependabot / Renovate** | ❌ | |
| **cargo-deny / cargo-audit** | ❌ | See [SEC-5](04-Errors-and-Security.md#sec-5). |
| **Fuzzing** | ❌ | See [TEST-4](05-Docs-and-Testing.md#test-4). |
| **Coverage reporting** | ❌ | See [TEST-2](05-Docs-and-Testing.md#test-2). |
| **API stability policy** | ❌ | |
| **Badges** (CI, crates.io, docs.rs) | ❌ | |

---

<a id="oss-2"></a>
### OSS-2 — None of the community-health files exist

**Severity:** High (for adoption; zero for correctness) · **Effort:** S (1 day for all of it)

A repository that a stranger might contribute to needs four files, and this one
has none. Each is short and each answers a question that otherwise costs a
maintainer a round trip on every issue.

**`CONTRIBUTING.md`** — the highest value of the four, because this project has
**non-obvious build prerequisites** that a contributor will otherwise hit
immediately:

- Backend tests need Docker (`testcontainers`), and without it
  `cargo test --workspace` fails in a way that looks like a broken checkout.
  See [TEST-3](05-Docs-and-Testing.md#test-3).
- `DATABASE_URL` must stay **unset**, because `sqlx::query!` would then
  validate against a live database rather than the committed `.sqlx/` cache —
  and a stale cache passes CI while breaking every contributor without Docker.
  The CI workflow explains this in a comment; a contributor never reads the CI
  workflow.
- Regenerating the `.sqlx/` cache after changing a query (`cargo sqlx prepare`)
  is a required step that is currently written down nowhere.
- The commit/PR conventions the existing history follows
  (`feat(scheduler): …` — conventional commits).
- `cargo fmt --all --check` and the `-D warnings` clippy gate.

**`SECURITY.md`** — where to report a vulnerability, and the supported-version
policy. Particularly relevant here because the project makes an explicit
security claim (the closed `ContextValue` enum, motivated by Spring Batch's
deserialization CVEs); a project making a security argument needs a disclosure
channel.

**`CODE_OF_CONDUCT.md`** — the Contributor Covenant, verbatim. Five minutes.

**Issue and PR templates** — a bug template asking for: crate + version,
backend, MSRV, whether it reproduces with `InMemoryJobRepository`. That last
question alone will resolve a third of the issues this project receives.

**Recommendation.** Write all four in one sitting. Add badges to the root
README (CI status, crates.io version, docs.rs, MSRV) — they are how a reader
decides in three seconds whether a crate is maintained.

**Benefit:** this is the gap between "an impressive personal project" and
"a project other people can join". Nothing here is hard; it is all currently
absent.

---

<a id="oss-3"></a>
### OSS-3 — No dependency update automation

**Severity:** Medium · **Effort:** XS

The scheduled `latest-deps` CI lane is a clever partial substitute — it runs
`cargo update` weekly and tests against it, `continue-on-error`, so an upstream
break surfaces on a Monday rather than inside an unrelated PR. That is good
thinking and it addresses the *detection* half.

It does not address the *action* half: nothing opens a PR, so `Cargo.lock`
drifts until someone updates it by hand.

**Recommendation.** `.github/dependabot.yml`:

```yaml
version: 2
updates:
  - package-ecosystem: cargo
    directory: "/"
    schedule: { interval: weekly }
    open-pull-requests-limit: 5
    groups:
      # One PR for the patch noise, individual PRs for anything that could
      # move an MSRV.
      patch-updates:
        update-types: ["patch"]
  - package-ecosystem: github-actions
    directory: "/"
    schedule: { interval: monthly }
```

Note the MSRV interaction: this workspace has three MSRV tiers, so a
dependency bump can silently raise one. The existing MSRV matrix lane catches
that on the PR, which is exactly why it is safe to automate the updates.

**Benefit:** keeps `Cargo.lock` current with the MSRV lanes acting as the
guard rail they were built to be.
