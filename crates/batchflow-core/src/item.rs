use crate::BatchError;
use std::future::Future;

pub trait ItemReader {
    type Item;

    fn read(&mut self) -> impl Future<Output = Result<Option<Self::Item>, BatchError>> + Send;
}

pub trait ItemWriter {
    type Item;

    fn write(
        &mut self,
        items: &[Self::Item],
    ) -> impl Future<Output = Result<(), BatchError>> + Send;
}

pub trait ItemProcessor {
    type In;
    type Out;

    fn process(
        &mut self,
        item: Self::In,
    ) -> impl Future<Output = Result<Option<Self::Out>, BatchError>> + Send;
}
