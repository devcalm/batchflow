# Security policy

## Reporting a vulnerability

**Please do not open a public issue.**

Report privately through GitHub's
[private vulnerability reporting](https://github.com/devcalm/batchflow/security/advisories/new),
or by email to <o.tsvietaiev92@gmail.com>.

Please include what you have: the affected crate and version, what an attacker
can do, and a reproduction if you have one. You will get an acknowledgement
within 72 hours and an assessment within a week. If the report is accepted, a
fix and an advisory are published together and you are credited unless you
would rather not be.

## Supported versions

Pre-1.0, only the latest minor version is supported. A fix ships in a new patch
release of that version; there are no backports to earlier minors.

| Version | Supported |
|---|---|
| 0.1.x | ✅ |
| 0.0.x | ❌ (a name reservation; it shipped nothing) |

## The security properties this project claims

Worth stating, because they are what a report can be measured against.

**No `unsafe`.** Every library crate carries `#![forbid(unsafe_code)]`. The only
`unsafe` in the repository is a counting allocator in
`crates/batchflow-core/tests/allocations.rs`, which is a test target and is
never compiled into anything a user links.

**The metadata store cannot be a deserialization gadget.** `ExecutionContext`
values are a closed enum of `String`, `i64` and `bool`. This is deliberate and
structural: Spring Batch stores serialized objects in its metadata tables, which
is where its deserialization CVEs came from. A hostile or corrupted store can
therefore make this deserializer construct nothing but those three types.
**Adding a variant that can hold arbitrary data — `serde_json::Value`, a type
tag, raw bytes decoded elsewhere — reopens that hole**, and a pull request doing
so will be refused on those grounds.

**No SQL injection surface.** Every statement is `sqlx::query!` with bind
parameters, validated at compile time. Table and column names are literals.

**No Lua injection surface.** Every Redis script is a `const`; user data reaches
it only through `KEYS`/`ARGV`.

**A panic in user code is contained.** The engine catches panics at its step and
job boundaries so that a bug in a reader, processor or writer fails the
execution rather than leaving the metadata store showing a job that is still
running. This is not a sandbox, and it is inert under `panic = "abort"`.

## What is out of scope

- **Denial of service through configuration.** `chunk_size`, `skip_limit` and a
  tasklet's pass count are author-supplied and unbounded by design; a job
  configured with an implausible chunk size will exhaust memory. See
  `docs/Operations.md` for sizing.
- **Trusting the metadata store's operator.** Anyone who can write to the store
  can alter a job's recorded status and its bookmark, which changes what a
  restart does. The store is trusted infrastructure; protect it accordingly.
- **Redis durability and eviction.** `batchflow-redis` requires
  `appendonly yes`, `appendfsync always` and `maxmemory-policy noeviction`.
  Running it without them can lose committed metadata, which is a
  misconfiguration rather than a vulnerability — it is documented in that
  crate's docs and in `docs/Operations.md`.

## Dependencies

CI runs [`cargo-deny`](https://embarkstudios.github.io/cargo-deny/) on every
pull request and weekly, checking the RUSTSEC advisory database, licences and
duplicate major versions. A weekly `latest dependencies` job additionally builds
against unpinned dependencies so that an upstream release which breaks us
surfaces on a schedule rather than inside an unrelated pull request.
