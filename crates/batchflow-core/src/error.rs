use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError {
    #[error("Read failed: {0}")]
    Read(String),

    #[error("Write failed: {0}")]
    Write(String),

    #[error("Process failed: {0}")]
    Process(String),

    #[error("Repository failed: {0}")]
    Repository(String),
}
