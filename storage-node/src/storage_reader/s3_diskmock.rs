use std::{
    pin::Pin,
    task::{Context, Poll},
    thread,
    time::Duration,
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, ready, FutureExt, Stream, StreamExt};

use crate::{
    disk::{
        disk_manager::{DiskManager, DiskReadStream},
        disk_manager_sync::{DiskManagerSync, DiskReadIterator},
    },
    error::{ParpulseError, ParpulseResult},
    storage_manager::ParpulseReaderStream,
};

use super::{s3::S3DataStream, AsyncStorageReader, SyncStorageReader};

const DELAY: Option<Duration> = Some(Duration::from_millis(100));
const DEFAULT_MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// Please DON'T use `MockS3Reader` to test performance, only use it to
/// test the correctness!!!
/// There is no chunksize in `MockS3Reader`.
/// If we want to make big change to s3.rs, please also change s3_diskmock.rs
/// TODO: We can also use automock to mock s3. (so there is no need to manually sync changes)
pub struct MockS3Reader {
    file_paths: Vec<String>,
    max_buffer_size: usize,
    disk_manager: DiskManager,
}

impl MockS3Reader {
    // Async here is to be consistent with S3Reader.
    pub async fn new(bucket: String, keys: Vec<String>) -> Self {
        let file_paths: Vec<String> = keys
            .iter()
            .map(|key| format!("{}{}", bucket.replace('-', "/") + "/", key))
            .collect();
        MockS3Reader {
            file_paths,
            max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
            disk_manager: DiskManager::default(),
        }
    }
}

pub struct MockS3DataStream {
    current_disk_stream: Option<Pin<Box<DiskReadStream>>>,
    file_paths: Vec<String>,
    current_key: usize,
    buffer: BytesMut,
    max_buffer_size: usize,
    disk_manager: DiskManager,
}

impl MockS3DataStream {
    pub fn new(file_paths: Vec<String>, max_buffer_size: usize, disk_manager: DiskManager) -> Self {
        MockS3DataStream {
            current_disk_stream: None,
            file_paths,
            current_key: 0,
            buffer: BytesMut::new(),
            max_buffer_size,
            disk_manager,
        }
    }
}

impl Stream for MockS3DataStream {
    type Item = ParpulseResult<usize>;

    // Remove `chunksize` for simplicity.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(current_disk_stream) = self.current_disk_stream.as_mut() {
            match ready!(current_disk_stream.poll_next_unpin(cx)) {
                Some(Ok(bytes_read)) => {
                    if let Some(delay) = DELAY {
                        thread::sleep(delay);
                    }
                    self.buffer = current_disk_stream.buffer().into();
                    Poll::Ready(Some(Ok(bytes_read)))
                }
                Some(Err(e)) => Poll::Ready(Some(Err(e))),
                None => {
                    self.current_key += 1;
                    self.current_disk_stream.take();
                    self.poll_next(cx)
                }
            }
        } else {
            // We need to create a new disk_stream since there is no last disk_stream, or it has
            // been consumed.
            if self.current_key >= self.file_paths.len() {
                return Poll::Ready(None);
            }
            let file_path = self.file_paths[self.current_key].clone();
            // TODO: do we need `disk_manager_helper_functions`? Sometimes we need sync for some simple methods, but async for some complex methods.
            // But we can only get one DiskManager.
            let file_size = self.disk_manager.file_size_sync(&file_path)? as usize;
            let mut buffer_size = self.max_buffer_size;
            if file_size < self.max_buffer_size {
                buffer_size = file_size;
            }
            // TODO: change `new_sync` to `new` (async version).
            match DiskReadStream::new_sync(&file_path, buffer_size) {
                Ok(disk_stream) => {
                    self.current_disk_stream = Some(Box::pin(disk_stream));
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
            self.poll_next(cx)
        }
    }
}

impl ParpulseReaderStream for MockS3DataStream {
    fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

#[async_trait]
impl AsyncStorageReader for MockS3Reader {
    type ReaderStream = MockS3DataStream;

    async fn read_all(&self) -> ParpulseResult<Bytes> {
        let mut bytes = BytesMut::new();
        for file_path in &self.file_paths {
            let (_, data) = self.disk_manager.read_disk_all(file_path).await?;
            bytes.extend(data);
        }
        Ok(bytes.freeze())
    }

    async fn into_stream(self) -> ParpulseResult<Pin<Box<Self::ReaderStream>>> {
        Ok(Box::pin(MockS3DataStream::new(
            self.file_paths,
            self.max_buffer_size,
            self.disk_manager,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_simple_write_read() {
        let bucket = "tests-parquet".to_string();
        let keys = vec![
            "userdata1.parquet".to_string(),
            "userdata2.parquet".to_string(),
        ];
        let mut reader = MockS3Reader::new(bucket, keys).await;
        let bytes = reader.read_all().await.unwrap();
        assert_eq!(bytes.len(), 113629 + 112193);
    }

    #[tokio::test]
    async fn test_mock_s3_read_streaming() {
        let bucket = "tests-parquet".to_string();
        let keys = vec![
            "userdata1.parquet".to_string(),
            "userdata2.parquet".to_string(),
        ];

        let reader = MockS3Reader::new(bucket, keys).await;
        let mut s3_stream = reader.into_stream().await.unwrap();

        let mut streaming_read_count = 0;
        let mut streaming_total_bytes = 0;
        while let Some(data_len) = s3_stream.next().await {
            let data_len = data_len.unwrap();
            streaming_read_count += 1;
            streaming_total_bytes += data_len;
            assert_eq!(s3_stream.buffer().len(), data_len);
        }
        assert_eq!(streaming_total_bytes, 113629 + 112193);
        assert_eq!(streaming_read_count, 2);
    }
}
