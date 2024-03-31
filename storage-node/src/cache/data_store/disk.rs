use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc::Receiver;

use crate::{
    disk::disk_manager::DiskManager, error::ParpulseResult, storage_reader::StorageReaderStream,
};

const DEFAULT_DISK_READER_BUFFER_SIZE: usize = 8192;
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1024;

/// [`DiskStore`] stores the contents of remote objects on the local disk.
pub struct DiskStore {
    disk_manager: DiskManager,
    /// The path to the directory where the data is stored on the disk.
    base_path: String,
}

impl DiskStore {
    pub fn new(disk_manager: DiskManager, base_path: String) -> Self {
        Self {
            disk_manager,
            base_path,
        }
    }
}

impl DiskStore {
    pub async fn read_data(
        &self,
        key: &str,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // FIXME: Shall we consider the situation where the data is not found?
        let mut disk_stream = self
            .disk_manager
            .disk_read_stream(key, DEFAULT_DISK_READER_BUFFER_SIZE)
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_CHANNEL_BUFFER_SIZE);
        tokio::spawn(async move {
            loop {
                match disk_stream.next().await {
                    Some(Ok(bytes_read)) => {
                        tx.send(Ok(Bytes::from(disk_stream.buffer()[..bytes_read].to_vec())))
                            .await
                            .unwrap();
                    }
                    Some(Err(e)) => tx.send(Err(e)).await.unwrap(),
                    None => break,
                }
            }
        });
        Ok(Some(rx))
    }

    pub async fn write_data(
        &self,
        key: String,
        data: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        // NOTE(Yuanxin): Shall we spawn a task to write the data to disk?
        let bytes_written = self
            .disk_manager
            .write_bytes_and_stream_to_disk(data, &key)
            .await?;
        Ok(bytes_written)
    }

    pub async fn clean_data(&self, key: &str) -> ParpulseResult<()> {
        self.disk_manager.remove_file(key).await
    }

    pub fn disk_store_key(&self, remote_location: &str) -> String {
        format!("{}{}", self.base_path, remote_location)
    }
}
