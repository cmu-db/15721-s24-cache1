use async_trait::async_trait;
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

// TODO: Merge `StorageReader` with `AsyncStorageReader`.
#[async_trait]
pub trait AsyncStorageReader {
    /// Read all data at once from the underlying storage.
    ///
    /// NEVER call this method if you do not know the size of the data -- collecting
    /// all data into one buffer might lead to OOM.
    async fn read_all(&self) -> ParpulseResult<Bytes>;

    /// Read data from the underlying storage as a stream.
    async fn into_stream(self) -> ParpulseResult<StorageDataStream>;
}
