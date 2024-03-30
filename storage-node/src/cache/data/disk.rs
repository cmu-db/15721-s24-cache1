use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc::Receiver;

use crate::{
    disk::disk_manager::DiskManager, error::ParpulseResult, storage_reader::StorageReaderStream,
};

use super::DataStore;

const DEFAULT_DISK_READER_BUFFER_SIZE: usize = 8192;
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1024;

pub struct DiskStore {
    disk_manager: DiskManager,
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

#[async_trait]
impl DataStore for DiskStore {
    async fn read_data(
        &self,
        key: &str,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // FIXME: Shall we consider the situation where the data is not found?
        let mut disk_stream = self
            .disk_manager
            .disk_read_stream(&key, DEFAULT_DISK_READER_BUFFER_SIZE)
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

    async fn write_data(&self, key: String, data: StorageReaderStream) -> ParpulseResult<usize> {
        // TODO: Shall we spawn a task to write the data to disk?
        // let data_stream = ReceiverStream::new(data);
        // let writer_stream = DiskWriterReceiverStream::new(data_stream).boxed();
        let bytes_written = self
            .disk_manager
            .write_stream_reader_to_disk(data, &key)
            .await?;
        Ok(bytes_written)
    }

    async fn clean_data(&self, key: &str) -> ParpulseResult<()> {
        self.disk_manager.remove_file(&key).await
    }

    fn data_store_key(&self, remote_location: &str) -> String {
        format!("{}{}", self.base_path, remote_location)
    }
}
