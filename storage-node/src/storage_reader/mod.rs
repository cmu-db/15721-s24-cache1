use bytes::BytesMut;

use crate::StorageResult;

mod local_fs;
mod mock_s3;
mod s3;

pub trait StorageReader {
    fn read_sync(&self) -> StorageResult<impl StorageReaderIterator>;
}

pub trait StorageReaderIterator {
    fn next(&mut self) -> Option<StorageResult<usize>>;
    fn buffer(&self) -> &BytesMut;
}
