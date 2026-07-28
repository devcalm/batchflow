use crate::BatchError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A value that may be stored in an [`ExecutionContext`].
///
/// Deliberately a *closed* set (requirement S-4). Spring Batch stores
/// serialized Java objects in its metadata tables, which is where its
/// deserialization-gadget CVEs came from: the metadata store becomes a
/// code-execution vector. Restricting the wire format to three primitives
/// means a hostile store can never make the deserializer construct an
/// arbitrary type — the vulnerability class is removed structurally rather
/// than defended against.
///
/// Do not add a variant that can hold arbitrary data (`serde_json::Value`,
/// a type tag, raw bytes to be decoded elsewhere). That reopens exactly the
/// hole this enum exists to close.
///
/// Note what is absent: no `Double(f64)`. [`JobParameter`](crate::JobParameter)
/// excludes it because `f64` implements neither `Eq` nor `Hash` and parameters
/// are an identity key. A context is never hashed, so `Hash` does not bind
/// here — but `Eq` does: [`StepExecution`](crate::StepExecution) derives it,
/// and it can only do so while every value in here has reflexive equality.
///
/// Adding `Double` would therefore be a **breaking** change, not the free one
/// `#[non_exhaustive]` normally buys: `NaN != NaN`, so the derive would have to
/// go, and every downstream `Eq` bound with it. That cost being visible is a
/// feature — floats in a bookmark bag deserve a deliberate decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ContextValue {
    String(String),
    Long(i64),
    Bool(bool),
}

impl ContextValue {
    /// Variant name, for error messages.
    fn type_name(&self) -> &'static str {
        match self {
            ContextValue::String(_) => "string",
            ContextValue::Long(_) => "long",
            ContextValue::Bool(_) => "bool",
        }
    }
}

/// A serializable bookmark bag attached to an execution.
///
/// A reader writes its position here at each chunk boundary and reads it back
/// on [`ItemReader::open`](crate::ItemReader::open), which is what lets a
/// restart resume rather than duplicate (FR-5.2).
///
/// `BTreeMap` for sorted keys, so the serialized form is byte-stable: a
/// persisted blob diffs cleanly and tests cannot flake on iteration order.
/// Not the same reason as [`JobParameters`](crate::JobParameters), where
/// `HashMap` would not compile at all — here nothing forces the choice.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionContext(BTreeMap<String, ContextValue>);

impl ExecutionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<String>, value: ContextValue) {
        self.0.insert(key.into(), value);
    }

    pub fn get(&self, key: &str) -> Option<&ContextValue> {
        self.0.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Read `key` as an `i64`.
    ///
    /// Three outcomes that must stay distinct:
    /// - `Ok(None)` — absent: a fresh run, start from the beginning
    /// - `Ok(Some(n))` — resume from `n`
    /// - `Err(..)` — present but the wrong type: fail loudly
    ///
    /// Returning `None` for a mismatch would make a garbled bookmark silently
    /// restart a half-finished job from zero and re-write every already
    /// committed item. Duplicate data is the exact thing restart exists to
    /// prevent, so a hard failure is the better outcome.
    ///
    /// [`get_string`](Self::get_string) and [`get_bool`](Self::get_bool)
    /// repeat this shape rather than sharing a generic or a macro: three
    /// copies of six lines stay readable and each keeps its own rustdoc.
    pub fn get_long(&self, key: &str) -> Result<Option<i64>, BatchError> {
        match self.0.get(key) {
            None => Ok(None),
            Some(ContextValue::Long(n)) => Ok(Some(*n)),
            Some(other) => Err(BatchError::ExecutionContextType {
                key: key.to_string(),
                expected: "long",
                actual: other.type_name(),
            }),
        }
    }

    pub fn get_string(&self, key: &str) -> Result<Option<&str>, BatchError> {
        match self.0.get(key) {
            None => Ok(None),
            Some(ContextValue::String(n)) => Ok(Some(n)),
            Some(other) => Err(BatchError::ExecutionContextType {
                key: key.to_string(),
                expected: "string",
                actual: other.type_name(),
            }),
        }
    }

    pub fn get_bool(&self, key: &str) -> Result<Option<bool>, BatchError> {
        match self.0.get(key) {
            None => Ok(None),
            Some(ContextValue::Bool(n)) => Ok(Some(*n)),
            Some(other) => Err(BatchError::ExecutionContextType {
                key: key.to_string(),
                expected: "bool",
                actual: other.type_name(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ExecutionContext {
        let mut context = ExecutionContext::new();
        context.put("position", ContextValue::Long(4000));
        context.put("file", ContextValue::String("input.csv".into()));
        context.put("header_seen", ContextValue::Bool(true));
        context
    }

    #[test]
    fn context_round_trips_through_json() {
        let json = serde_json::to_string(&sample()).unwrap();
        let back: ExecutionContext = serde_json::from_str(&json).unwrap();

        assert_eq!(back, sample());
    }

    /// Pins the `BTreeMap`. Keys are inserted in reverse alphabetical order, so
    /// a `HashMap` would produce a different string on most runs.
    #[test]
    fn serialized_key_order_is_stable() {
        let mut context = ExecutionContext::new();
        context.put("zebra", ContextValue::Long(1));
        context.put("middle", ContextValue::Long(2));
        context.put("alpha", ContextValue::Long(3));

        let json = serde_json::to_string(&context).unwrap();

        assert_eq!(
            json,
            r#"{"alpha":{"Long":3},"middle":{"Long":2},"zebra":{"Long":1}}"#
        );
    }

    #[test]
    fn a_missing_key_is_not_an_error() {
        let context = ExecutionContext::new();

        assert_eq!(context.get_long("position").unwrap(), None);
        assert_eq!(context.get_string("file").unwrap(), None);
        assert_eq!(context.get_bool("header_seen").unwrap(), None);
    }

    #[test]
    fn a_present_key_reads_back_at_its_own_type() {
        let context = sample();

        assert_eq!(context.get_long("position").unwrap(), Some(4000));
        assert_eq!(context.get_string("file").unwrap(), Some("input.csv"));
        assert_eq!(context.get_bool("header_seen").unwrap(), Some(true));
    }

    /// The load-bearing test of this module. A mismatch must NOT read as
    /// absent: that would restart a half-finished job from zero and duplicate
    /// every committed item.
    #[test]
    fn a_mismatched_type_is_an_error_not_a_missing_value() {
        let mut context = ExecutionContext::new();
        context.put("position", ContextValue::String("4000".into()));

        let result = context.get_long("position");

        assert!(matches!(
            result,
            Err(BatchError::ExecutionContextType { .. })
        ));

        // Every accessor, not just the one we happened to write first.
        assert!(context.get_bool("position").is_err());
        assert!(sample().get_string("position").is_err());
    }

    /// The message is what an operator sees at 3am when a restart aborts, so
    /// it has to name the key and both types.
    #[test]
    fn the_type_error_names_the_key_and_both_types() {
        let mut context = ExecutionContext::new();
        context.put("position", ContextValue::Bool(true));

        let message = context.get_long("position").unwrap_err().to_string();

        assert!(message.contains("position"), "{message}");
        assert!(message.contains("bool"), "{message}");
        assert!(message.contains("long"), "{message}");
    }
}
