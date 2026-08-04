//! The capstone: a CSV loaded into Postgres through a real transaction, dying
//! partway, and resuming without loss or duplication.
//!
//! Run with: `cargo run -p batchflow-postgres --example csv_to_postgres`
//! (needs Docker - the example starts its own throwaway database.)
//!
//! This is the only example where the writer joins the step's transaction, so
//! it is the only one that demonstrates FR-2.4: **the rows, the counters and
//! the reader's bookmark commit together or roll back together.** The others
//! wrap their writer in `Unmanaged`, which is an explicit acceptance of
//! at-least-once; a database writer does not have to accept that.
//!
//! What it shows, in order:
//! 1. a malformed CSV row skipped, because parsing happens per item;
//! 2. a chunk that inserts rows and *then* fails - so the rollback has real
//!    work to do - leaving the table, the counters and the bookmark agreeing;
//! 3. a second launch that resumes at the bookmark and finishes the file.

use batchflow::batchflow_core::{
    BatchError, ChunkStep, Classifier, ContextValue, ErrorAction, ExecutionContext, FaultTolerance,
    ItemProcessor, ItemReader, Job, JobLauncher, JobParameter, JobParameters, JobRepository,
    RetryPolicy, TransactionalWriter,
};
use batchflow_postgres::{PostgresClassifier, PostgresJobRepository};
use sqlx::{PgPool, Postgres, Transaction};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

type PgTx = Transaction<'static, Postgres>;

/// Embedded at compile time so the example does not care what directory it is
/// run from. A real loader opens a path; nothing else about the wiring changes.
const CSV: &str = include_str!("people.csv");

const POSITION: &str = "people.line";
const JOB: &str = "load-people";

// ---------------------------------------------------------------- domain

/// A line that has not been parsed yet.
#[derive(Debug)]
struct RawRow {
    line: usize,
    text: String,
}

/// A parsed, validated person - what actually reaches the database.
#[derive(Debug)]
struct Person {
    name: String,
    age: i32,
}

/// The reader yields [`RawRow`] and the writer accepts [`Person`], which is the
/// associated-type split earning its keep: parsing is the *processor's* job, so
/// a bad line fails per item and can be skipped. Parse inside the reader and
/// the same bad line fails a whole chunk instead.
#[derive(Debug)]
struct MalformedRow {
    line: usize,
    reason: String,
}

impl fmt::Display for MalformedRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.reason)
    }
}

impl Error for MalformedRow {}

// ---------------------------------------------------------------- reader

/// Walks the CSV, skipping the header, remembering the line it is on.
struct CsvPeople {
    lines: Vec<String>,
    next: usize,
}

impl CsvPeople {
    fn new(csv: &str) -> Self {
        Self {
            // `skip(1)` drops the header. A production loader would use the
            // `csv` crate, which handles quoting, embedded newlines and BOMs -
            // none of which this file has, and none of which is what the
            // example is about.
            lines: csv
                .lines()
                .skip(1)
                .filter(|line| !line.trim().is_empty())
                .map(str::to_owned)
                .collect(),
            next: 0,
        }
    }
}

impl ItemReader for CsvPeople {
    type Item = RawRow;

    async fn read(&mut self) -> Result<Option<RawRow>, BatchError> {
        let Some(text) = self.lines.get(self.next) else {
            return Ok(None);
        };

        // +2 so the number matches what a text editor shows: one for the
        // header, one because editors count from 1.
        let row = RawRow {
            line: self.next + 2,
            text: text.clone(),
        };
        self.next += 1;

        Ok(Some(row))
    }

    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        let Some(recorded) = context.get_long(POSITION)? else {
            return Ok(());
        };

        self.next = usize::try_from(recorded).map_err(BatchError::read)?;
        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        let position = i64::try_from(self.next).expect("a line count exceeding i64 is impossible");
        context.put(POSITION, ContextValue::Long(position));
    }
}

// ---------------------------------------------------------------- processor

/// Parses and validates one row. This is where a bad line fails.
struct ParsePerson;

impl ItemProcessor for ParsePerson {
    type In = RawRow;
    type Out = Person;

    async fn process(&mut self, row: RawRow) -> Result<Option<Person>, BatchError> {
        let malformed = |reason: &str| {
            BatchError::process(MalformedRow {
                line: row.line,
                reason: reason.to_owned(),
            })
        };

        let Some((name, age)) = row.text.split_once(',') else {
            return Err(malformed("expected two comma-separated fields"));
        };

        let age: i32 = age
            .trim()
            .parse()
            .map_err(|_| malformed("age is not a number"))?;

        Ok(Some(Person {
            name: name.trim().to_owned(),
            age,
        }))
    }
}

// ---------------------------------------------------------------- writer

/// Inserts a chunk *inside the step's transaction*, and simulates the process
/// losing its connection once.
///
/// The rows go in before the failure on purpose. A writer that fails before
/// inserting leaves an empty transaction, and an empty transaction rolls back
/// identically whether or not rollback works - there would be nothing to
/// observe.
struct PeopleTable {
    armed: Arc<AtomicBool>,
}

impl TransactionalWriter<PgTx> for PeopleTable {
    type Item = Person;

    async fn write(&mut self, tx: &mut PgTx, people: &[Person]) -> Result<(), BatchError> {
        for person in people {
            sqlx::query("INSERT INTO people (name, age) VALUES ($1, $2)")
                .bind(&person.name)
                .bind(person.age)
                .execute(&mut **tx)
                .await
                .map_err(BatchError::write)?;
        }

        if people.iter().any(|person| person.name == "Frank")
            && self.armed.swap(false, Ordering::SeqCst)
        {
            println!(
                "    inserted {} rows, then the connection dropped",
                people.len()
            );
            return Err(BatchError::write("connection reset by peer"));
        }

        println!(
            "    committed {:?}",
            people.iter().map(|p| p.name.as_str()).collect::<Vec<_>>()
        );
        Ok(())
    }
}

// ---------------------------------------------------------------- classifier

/// Skips rows this loader could not parse, and defers everything else to
/// [`PostgresClassifier`].
///
/// Classifiers compose by delegation. The Postgres one knows SQLSTATEs and
/// nothing about CSV; this one knows CSV and nothing about SQLSTATEs. Neither
/// had to be modified to work with the other.
struct LoaderClassifier;

impl Classifier for LoaderClassifier {
    fn classify(&self, error: &BatchError) -> ErrorAction {
        let mut current: Option<&(dyn Error + 'static)> = Some(error);
        while let Some(cause) = current {
            if cause.downcast_ref::<MalformedRow>().is_some() {
                return ErrorAction::Skip;
            }
            current = cause.source();
        }

        PostgresClassifier.classify(error)
    }
}

// ---------------------------------------------------------------- harness

fn build_job(armed: &Arc<AtomicBool>) -> Job<PgTx> {
    let step = ChunkStep::new(
        "load",
        CsvPeople::new(CSV),
        ParsePerson,
        PeopleTable {
            armed: Arc::clone(armed),
        },
        NonZeroUsize::new(4).expect("4 is not zero"),
    )
    .with_fault_tolerance(
        FaultTolerance::new()
            .classifier(LoaderClassifier)
            .retry(RetryPolicy::attempts(
                NonZeroU32::new(2).expect("2 is not zero"),
            ))
            .skip_limit(5),
    );

    Job::builder(JOB).step(step).build()
}

/// Prints what the database believes, from both halves: the loaded data and
/// the metadata the engine wrote about loading it.
async fn report(launcher: &JobLauncher<PostgresJobRepository>, parameters: &JobParameters) {
    let repository = launcher.repository();
    let pool = repository.pool();

    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM people ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("query failed");

    println!("  people table: {names:?}");

    let instance = repository
        .find_instance(JOB, parameters)
        .await
        .expect("lookup failed")
        .expect("the launch created an instance");
    let execution = repository
        .last_execution(instance.id())
        .await
        .expect("lookup failed")
        .expect("the launch created an execution");

    for step in repository
        .step_executions(execution.id())
        .await
        .expect("lookup failed")
    {
        println!(
            "  metadata:     status={:?} read={} written={} skipped={} bookmark={:?}",
            step.status(),
            step.read_count(),
            step.write_count(),
            step.skip_count(),
            step.execution_context().get(POSITION),
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // A throwaway database so the example needs no setup beyond Docker. An
    // application replaces these five lines with one `PgPool::connect` against
    // its own server; nothing below this block changes.
    let container = PostgresImage::default().start().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await?;

    sqlx::query(
        "CREATE TABLE people (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, age INT NOT NULL)",
    )
    .execute(&pool)
    .await?;

    let repository = PostgresJobRepository::new(pool);
    // Creates the three metadata tables. An application runs this at startup or
    // ships it in its own migration pipeline.
    repository.migrate().await?;

    let launcher = JobLauncher::new(repository);
    let armed = Arc::new(AtomicBool::new(true));
    let parameters = JobParameters::new().with("date", JobParameter::String("2026-08-04".into()));

    println!("--- attempt 1: the connection drops on the second chunk ---");
    let mut job = build_job(&armed);
    match launcher.run(&mut job, &parameters).await {
        Ok(execution) => println!("  unexpectedly finished as {:?}", execution.status()),
        Err(error) => println!("  job failed: {error}"),
    }
    report(&launcher, &parameters).await;
    println!(
        "  ^ the four rows the failed chunk inserted are gone, and the bookmark\n    \
         still points at the last chunk that committed. One transaction held all three.\n"
    );

    println!("--- attempt 2: same parameters, so the same JobInstance resumes ---");
    let mut job = build_job(&armed);
    let execution = launcher.run(&mut job, &parameters).await?;
    println!("  job finished as {:?}", execution.status());
    report(&launcher, &parameters).await;

    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM people")
        .fetch_one(launcher.repository().pool())
        .await?;
    let distinct: i64 = sqlx::query_scalar("SELECT count(DISTINCT name) FROM people")
        .fetch_one(launcher.repository().pool())
        .await?;

    println!("\n  {total} rows, {distinct} distinct names");
    assert_eq!(total, distinct, "a person was loaded twice");
    println!("  every valid row loaded exactly once; Dave was skipped as malformed");

    Ok(())
}
