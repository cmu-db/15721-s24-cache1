use std::{io, thread, time::Duration};

use crate::{
    disk::disk_manager::{DiskManager, DiskReadSyncIterator},
    StorageResult
};

use super::{StorageReader, StorageReaderIterator};


pub struct MockS3Reader {
    file_path: String,
    delay: Option<Duration>,
    buffer_size: usize,
}

impl MockS3Reader {
    pub async fn read(&self) -> StorageResult<()> {
        todo!()
    }
}

impl StorageReader for MockS3Reader {
    fn read_sync(&self) -> StorageResult<impl StorageReaderIterator> {
        self.delay.map(|duration| {
            thread::sleep(duration);
        });
        // Mock S3 should not count into disk statistics, so we directly new iterator
        DiskReadSyncIterator::new(&self.file_path, self.buffer_size)
    }
}
