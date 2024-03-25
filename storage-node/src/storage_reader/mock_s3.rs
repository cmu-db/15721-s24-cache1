use std::{env, io, thread, time::Duration};

use bytes::Bytes;

use crate::{
    disk::disk_manager::{DiskManager, DiskReadSyncIterator},
    error::ParpulseResult,
};

use super::{StorageDataStream, SyncStorageReader};

pub struct MockS3Reader {
    file_path: String,
    delay: Option<Duration>,
    buffer_size: usize,
}

impl MockS3Reader {
    pub fn new(file_path: String, delay: Option<Duration>, buffer_size: usize) -> Self {
        let current_path = env::current_dir().unwrap().display().to_string();
        MockS3Reader {
            file_path: current_path + "/" + &file_path,
            delay,
            buffer_size,
        }
    }
}

impl SyncStorageReader for MockS3Reader {
    type ReaderIterator = DiskReadSyncIterator;
    fn read_all(&self) -> ParpulseResult<Bytes> {
        // TODO: Definitely need to refactor (A real DiskManager held R&W lock, actual disk functions should be able directly call like disk_manager::xxx)
        // better to seperate manager & helper functions, mock s3 only need to call helper functions
        let mut disk_manager = DiskManager {};
        let (bytes_read, bytes) = disk_manager.read_disk_all(&self.file_path)?;
        Ok(bytes)
    }

    // FIXME: Where to put size? Do we need to also return `read_size` in this method?
    fn into_iterator(&self) -> ParpulseResult<Self::ReaderIterator> {
        if let Some(duration) = self.delay {
            thread::sleep(duration);
        }
        // Mock S3 should not count into disk statistics, so we directly new iterator
        DiskReadSyncIterator::new(&self.file_path, self.buffer_size)
    }
}
