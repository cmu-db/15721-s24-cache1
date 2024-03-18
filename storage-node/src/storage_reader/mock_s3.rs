use std::{env, io, thread, time::Duration};

use crate::{
    disk::disk_manager::{DiskManager, DiskReadSyncIterator},
    error::ParpulseResult,
};

use super::{StorageDataStream, StorageReader, StorageReaderIterator};

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

impl StorageReader for MockS3Reader {
    type ReaderIterator = DiskReadSyncIterator;
    // FIXME: Where to put size? Do we need to also return `read_size` in this method?
    fn read(&self) -> ParpulseResult<Self::ReaderIterator> {
        if let Some(duration) = self.delay {
            thread::sleep(duration);
        }
        // Mock S3 should not count into disk statistics, so we directly new iterator
        DiskReadSyncIterator::new(&self.file_path, self.buffer_size)
    }
}
