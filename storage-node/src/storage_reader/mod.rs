use std::pin::Pin;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream::BoxStream, Stream};

use crate::{
    cache::ParpulseCache,
    error::ParpulseResult,
    storage_manager::{ParpulseReaderIterator, ParpulseReaderStream},
};

pub mod local_fs;
pub mod s3;
// TODO: We can use `use mockall::automock;` to mock s3.
// (https://docs.aws.amazon.com/sdk-for-rust/latest/dg/testing.html)
// pub mod s3_automock;
pub mod s3_diskmock;

pub trait SyncStorageReader {
    type ReaderIterator: ParpulseReaderIterator;
    fn read_all(&self) -> ParpulseResult<Bytes>;
    fn into_iterator(self) -> ParpulseResult<Self::ReaderIterator>;
}

// TODO: Merge `StorageReader` with `AsyncStorageReader`.
#[async_trait]
pub trait AsyncStorageReader {
    type ReaderStream: ParpulseReaderStream;
    /// Read all data at once from the underlying storage.
    ///
    /// NEVER call this method if you do not know the size of the data -- collecting
    /// all data into one buffer might lead to OOM.
    async fn read_all(&self) -> ParpulseResult<Bytes>;

    /// Read data from the underlying storage as a stream.
    async fn into_stream(self) -> ParpulseResult<Pin<Box<Self::ReaderStream>>>;
}
