//! # BatchFlow Core
//!
//! Core traits and execution engine for BatchFlow.
#![forbid(unsafe_code)]

use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BatchError {
    #[error("Read failed: {0}")]
    Read(String),

    #[error("Write failed: {0}")]
    Write(String),
}

#[allow(async_fn_in_trait)]
pub trait ItemReader {
    type Item;
    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError>;
}

#[allow(async_fn_in_trait)]
pub trait ItemWriter {
    type Item;
    async fn write(&mut self, item: Self::Item) -> Result<(), BatchError>;
}

#[allow(async_fn_in_trait)]
pub trait ItemProcessor {
    type In;
    type Out;
    async fn process(&mut self, item: Self::In) -> Result<Self::Out, BatchError>;
}

pub async fn read_chunk<R>(reader: &mut R, chunk_size: usize) -> Result<Vec<R::Item>, BatchError>
where
    R: ItemReader,
{
    let mut chunk: Vec<R::Item> = Vec::with_capacity(chunk_size);

    for _ in 0..chunk_size {
        if let Some(item) = reader.read().await? {
            chunk.push(item);
        } else {
            break;
        }
    }

    Ok(chunk)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct VecReader {
        items: Vec<u32>,
        pos: usize,
    }

    impl ItemReader for VecReader {
        type Item = u32;

        async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
            let item = self.items.get(self.pos).copied();
            if item.is_some() {
                self.pos += 1;
            }
            Ok(item)
        }
    }

    /// A test reader that yields `remaining_ok` items, then errors.
    struct FailingReader {
        remaining_ok: usize,
    }

    impl ItemReader for FailingReader {
        type Item = u32;
        async fn read(&mut self) -> Result<Option<u32>, BatchError> {
            if self.remaining_ok == 0 {
                return Err(BatchError::Read("boom".into()));
            }
            self.remaining_ok -= 1;
            Ok(Some(7))
        }
    }

    #[tokio::test]
    async fn reads_a_full_chunk() {
        let mut reader = VecReader {
            items: vec![1, 2, 3, 4, 5],
            pos: 0,
        };
        let chunk: Vec<u32> = read_chunk(&mut reader, 3).await.unwrap();

        assert_eq!(chunk, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn reads_partial_chunk_at_eof() {
        let mut reader = VecReader {
            items: vec![1, 2],
            pos: 0,
        };
        let chunk: Vec<u32> = read_chunk(&mut reader, 5).await.unwrap();

        assert_eq!(chunk, vec![1, 2]);
    }

    #[tokio::test]
    async fn error_short_circuits() {
        let mut reader = FailingReader { remaining_ok: 2 };
        let result = read_chunk(&mut reader, 5).await;

        assert!(result.is_err());
    }
}
