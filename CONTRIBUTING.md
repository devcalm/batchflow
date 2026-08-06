# Contributing to BatchFlow

Thanks for taking the time. This document covers the two things that are not
obvious from the repository — the test prerequisites and the `sqlx` offline
cache — plus the conventions the existing history follows.

## Getting a green build

```sh
cargo test -p batchflow-core -p batchflow -p batchflow-metrics -p batchflow-scheduler --all-features
```

That runs everything that needs nothing but a Rust toolchain, and it is what
most changes need.

```sh
cargo test --workspace --all-features
```

also runs `batchflow-postgres` and `batchflow-redis`, which start their own
throwaway containers through [`testcontainers`](https://docs.rs/testcontainers).
**They need a running Docker daemon.** Without one they fail with container
errors that look like a broken checkout but are not.

Before opening a pull request:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --no-deps --all-features        # RUSTDOCFLAGS="-D warnings"
```

## `DATABASE_URL` must stay unset

`batchflow-postgres` uses `sqlx::query!`, which validates SQL at compile time.
With `DATABASE_URL` set it validates against a live database; without it, it
validates against the committed cache in `crates/batchflow-postgres/.sqlx/`.

**The cache is the source of truth**, and CI deliberately leaves `DATABASE_URL`
unset so that a stale cache fails there rather than passing CI and breaking
every contributor who builds without Docker.

Two consequences:

- **Do not export `DATABASE_URL` in your shell** while building this workspace.
  It will appear to work and will hide a stale cache.
- **If you change, add or remove a query — including its whitespace — you must
  regenerate the cache.** `sqlx` hashes the query *string*, so reindenting a
  statement invalidates its entry:

  ```sh
  # A throwaway database to prepare against.
  docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name bf-prepare postgres:17
  export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
  cargo sqlx migrate run --source crates/batchflow-postgres/migrations
  cargo sqlx prepare --workspace -- --all-features
  unset DATABASE_URL
  docker rm -f bf-prepare
  ```

  Commit the resulting `.sqlx/` changes with the query change, in the same
  commit.

## Migrations are append-only

`sqlx` records a checksum for every applied migration. Editing a migration that
has shipped makes every existing database refuse to migrate, so a schema change
is always a **new file** in `crates/batchflow-postgres/migrations/`, never an
edit to an old one. See `0002_skip_count.sql` for the pattern.

## Backends must pass the conformance suite

`JobRepository` is the main extension point, and its contract is executable:

```rust
batchflow_core::job_repository_conformance!(setup());
```

A change to the contract means a change to
`crates/batchflow-core/src/conformance.rs`, and every backend has to keep
passing. If you are adding a backend, invoke the macro rather than writing your
own list — that is what stopped the two shipped backends from drifting apart.

## Tests

The house style, visible throughout:

- **A test name is a sentence about behaviour**, not about the function under
  test: `a_failed_chunk_leaves_the_bookmark_at_the_last_committed_chunk`.
- **Negative controls are expected.** A test proving that retry works is paired
  with one proving the same step fails without a retry policy. Without the
  pair, a fake that never fails makes the first test pass for the wrong reason.
- **A doc comment on a test says why it exists**, not what it does — usually
  the failure it would catch, and ideally the incident that motivated it.
- **Doctests that a user must be able to write go in `crates/batchflow/`**, not
  in `batchflow-core`. Rustdoc passes `--extern` for all of a crate's own
  dependencies, so a doctest in core can compile code that a user cannot.

## Commits and pull requests

Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/),
scoped by crate:

```
feat(core): add a stop signal checked at chunk commit boundaries
fix(redis): reject an evicted step execution instead of restarting from zero
docs(guide): describe the memory cost of a large commit interval
```

A pull request should say **what would go wrong without it**. The CHANGELOG is
maintained by hand in [Keep a Changelog](https://keepachangelog.com/) format —
add an entry under `[Unreleased]` for anything a user would notice.

## Design decisions

Non-obvious decisions are recorded as ADRs in
[`docs/Architecture.md`](docs/Architecture.md). If you are changing something an
ADR covers, update the ADR in the same pull request; if you are making a new
decision of that size, add one.

`docs/audit/` holds an engineering audit of the codebase and the running record
of what has been acted on — worth reading before proposing anything structural.

## Code of conduct

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Licence

Contributions are dual-licensed under MIT and Apache-2.0, matching the project.
