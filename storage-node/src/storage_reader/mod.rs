use bytes::{Bytes, BytesMut};
use futures::{stream::BoxStream, Stream};

use crate::error::ParpulseResult;

pub mod local_fs;
pub mod mock_s3;
pub mod s3;

pub trait StorageReader {
    type ReaderIterator: StorageReaderIterator;
    fn read(&self) -> ParpulseResult<Self::ReaderIterator>;
}

pub trait StorageReaderIterator {
    fn next(&mut self) -> Option<ParpulseResult<usize>>;
    fn buffer(&self) -> &BytesMut;
}

pub type StorageDataStream = BoxStream<'static, ParpulseResult<Bytes>>;

// TODO: Merge `StorageReader` and `AsyncStorageReader`.
pub trait AsyncStorageReader {
    async fn read(&self) -> ParpulseResult<Bytes>;
    async fn streaming_read(&self) -> ParpulseResult<StorageDataStream>;
}
