<!--
What would go wrong without this change? That is the most useful thing you can
write here, and it is usually the first line of the commit message too.
-->

## What this changes

## Why

<!-- The failure it prevents, the guarantee it adds, or the issue it closes. -->

## Checklist

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features` (needs Docker for the backend
      suites; `-p batchflow-core -p batchflow` if you have none)
- [ ] A test that fails without this change
- [ ] CHANGELOG entry under `[Unreleased]`, if a user would notice

<!-- Delete whichever of these do not apply. -->

- [ ] **SQL changed:** `.sqlx/` regenerated with `cargo sqlx prepare` and
      committed in this PR. Reindenting a query counts — `sqlx` hashes the
      literal.
- [ ] **Schema changed:** a new migration file, not an edit to an applied one.
- [ ] **`JobRepository` contract changed:** the conformance suite covers it and
      every backend still passes.
- [ ] **Public API changed:** rustdoc updated, and the CHANGELOG says whether
      it is breaking.
- [ ] **A decision was made:** ADR added or updated in `docs/Architecture.md`.
