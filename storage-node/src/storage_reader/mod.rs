use bytes::BytesMut;

use crate::StorageResult;

pub mod local_fs;
pub mod mock_s3;
pub mod s3;

pub trait StorageReader {
    type ReaderIterator: StorageReaderIterator;
    fn read_sync(&self) -> StorageResult<Self::ReaderIterator>;
}

pub trait StorageReaderIterator {
    fn next(&mut self) -> Option<StorageResult<usize>>;
    fn buffer(&self) -> &BytesMut;
}
