use std::{env, io, thread, time::Duration};

use crate::{
    disk::disk_manager::{DiskManager, DiskReadSyncIterator},
    StorageResult,
};

use super::{StorageReader, StorageReaderIterator};

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

    pub async fn read(&self) -> StorageResult<()> {
        todo!()
    }
}

impl StorageReader for MockS3Reader {
    // FIXME: Where to put size? Do we need to also return `read_size` in this method?
    fn read_sync(&self) -> StorageResult<impl StorageReaderIterator> {
        if let Some(duration) = self.delay {
            thread::sleep(duration);
        }
        // Mock S3 should not count into disk statistics, so we directly new iterator
        DiskReadSyncIterator::new(&self.file_path, self.buffer_size)
    }
}
