use std::{fs, sync::Arc};

use bytes::Bytes;
use futures::StreamExt;
use log::info;
use tokio::sync::{mpsc::Receiver, Mutex};

use crate::{
    cache::{
        data_store_cache::memdisk::{MemDiskStoreReplacerKey, MemDiskStoreReplacerValue},
        replacer::DataStoreReplacer,
    },
    disk::disk_manager::DiskManager,
    error::ParpulseResult,
    storage_reader::StorageReaderStream,
};

/// TODO(lanlou): make them configurable.
/// MAX_DISK_READER_BUFFER_SIZE is 10MB.
const MAX_DISK_READER_BUFFER_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_DISK_CHANNEL_BUFFER_SIZE: usize = 512;

/// [`DiskStore`] stores the contents of remote objects on the local disk.
pub struct DiskStore {
    disk_manager: DiskManager,
    /// The path to the directory where the data is stored on the disk.
    base_path: String,
}

impl Drop for DiskStore {
    fn drop(&mut self) {
        if fs::metadata(&self.base_path).is_ok() {
            info!("Removing cache files: {}", self.base_path);
            fs::remove_dir_all(self.base_path.clone()).expect("remove cache files failed");
        }
    }
}

impl DiskStore {
    pub fn new(disk_manager: DiskManager, base_path: String) -> Self {
        let mut final_base_path = base_path;
        if !final_base_path.ends_with('/') {
            final_base_path += "/";
        }
        Self {
            disk_manager,
            base_path: final_base_path,
        }
    }
}

impl DiskStore {
    /// Reads data from the disk store. The method returns a stream of data read from the disk
    /// store.
    pub async fn read_data<R>(
        &self,
        key: &str,
        disk_replacer: Arc<Mutex<R>>,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>>
    where
        R: DataStoreReplacer<MemDiskStoreReplacerKey, MemDiskStoreReplacerValue> + 'static,
    {
        // TODO(lanlou): we later may consider the remaining space to decide the buffer size
        let mut buffer_size = self.disk_manager.file_size(key).await? as usize;
        if buffer_size > MAX_DISK_READER_BUFFER_SIZE {
            buffer_size = MAX_DISK_READER_BUFFER_SIZE;
        }
        // FIXME: Shall we consider the situation where the data is not found?
        let mut disk_stream = self.disk_manager.disk_read_stream(key, buffer_size).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_DISK_CHANNEL_BUFFER_SIZE);
        let key_str = key.to_string().clone();
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
            // TODO(lanlou): when second read, so there is no need to unpin, how to improve?
            disk_replacer.lock().await.unpin(&key_str);
        });
        Ok(Some(rx))
    }

    /// Writes data to the disk store. The method accepts a stream of data to write to the disk
    /// store.
    /// TODO: We may need to push the response writer down to the disk store as well.
    pub async fn write_data(
        &self,
        key: String,
        bytes_vec: Option<Vec<Bytes>>,
        stream: Option<StorageReaderStream>,
    ) -> ParpulseResult<usize> {
        // NOTE(Yuanxin): Shall we spawn a task to write the data to disk?
        let bytes_written = self
            .disk_manager
            .write_bytes_and_stream_to_disk(bytes_vec, stream, &key)
            .await?;
        Ok(bytes_written)
    }

    /// Cleans the data from the disk store.
    pub async fn clean_data(&self, key: &str) -> ParpulseResult<()> {
        self.disk_manager.remove_file(key).await
    }

    /// Returns the key for the disk store. The key should be cached in the disk store cache.
    pub fn data_store_key(&self, remote_location: &str) -> String {
        format!("{}{}", self.base_path, remote_location)
    }
}
