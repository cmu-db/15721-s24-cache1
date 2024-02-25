use bytes::BytesMut;

use crate::StorageResult;

pub mod local_fs;
pub mod mock_s3;
pub mod s3;

pub trait StorageReader {
    fn read_sync(&self) -> StorageResult<impl StorageReaderIterator>;
}

pub trait StorageReaderIterator {
    fn next(&mut self) -> Option<StorageResult<usize>>;
    fn buffer(&self) -> &BytesMut;
}
