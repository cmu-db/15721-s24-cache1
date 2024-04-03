use std::{
    env,
    pin::Pin,
    task::{Context, Poll},
    thread,
    time::Duration,
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{ready, Stream, StreamExt};

use crate::{
    disk::{disk_manager::DiskManager, stream::RandomDiskReadStream},
    error::ParpulseResult,
};

use super::{AsyncStorageReader, StorageReaderStream};

const DELAY: Option<Duration> = Some(Duration::from_millis(100));
const MIN_DISK_READ_SIZE: usize = 1024 * 512;
const MAX_DISK_READ_SIZE: usize = 1024 * 1024;

/// Please DON'T use `MockS3Reader` to test performance, only use it to
/// test the correctness!!!
/// There is no chunksize in `MockS3Reader`.
/// If we want to make big change to s3.rs, please also change s3_diskmock.rs
/// TODO: We can also use automock to mock s3. (so there is no need to manually sync changes)
pub struct MockS3Reader {
    file_paths: Vec<String>,
    disk_manager: DiskManager,
}

impl MockS3Reader {
    // Async here is to be consistent with S3Reader.
    pub async fn new(bucket: String, keys: Vec<String>) -> Self {
        // Get the absolute path instead of relative path.
        let base_path = env::current_dir()
            .ok()
            .and_then(|current_path| {
                current_path
                    .parent()
                    .map(|root_path| root_path.join("storage-node"))
            })
            .and_then(|joined_path| joined_path.to_str().map(|s| s.to_string()))
            .unwrap_or_default();

        let file_paths: Vec<String> = keys
            .iter()
            .map(|key| format!("{}/{}/{}", base_path, bucket.replace('-', "/"), key))
            .collect();
        MockS3Reader {
            file_paths,
            disk_manager: DiskManager::default(),
        }
    }
}

pub struct MockS3ReaderStream {
    current_disk_stream: Option<Pin<Box<RandomDiskReadStream>>>,
    file_paths: Vec<String>,
    current_key: usize,
}

impl MockS3ReaderStream {
    pub fn new(file_paths: Vec<String>) -> Self {
        MockS3ReaderStream {
            current_disk_stream: None,
            file_paths,
            current_key: 0,
        }
    }
}

impl Stream for MockS3ReaderStream {
    type Item = ParpulseResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(current_disk_stream) = self.current_disk_stream.as_mut() {
            match ready!(current_disk_stream.poll_next_unpin(cx)) {
                Some(Ok(bytes)) => {
                    if let Some(delay) = DELAY {
                        thread::sleep(delay);
                    }
                    Poll::Ready(Some(Ok(bytes)))
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
            match RandomDiskReadStream::new(&file_path, MIN_DISK_READ_SIZE, MAX_DISK_READ_SIZE) {
                Ok(disk_stream) => {
                    self.current_disk_stream = Some(Box::pin(disk_stream));
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
            self.poll_next(cx)
        }
    }
}

#[async_trait]
impl AsyncStorageReader for MockS3Reader {
    async fn read_all(&self) -> ParpulseResult<Bytes> {
        let mut bytes = BytesMut::new();
        for file_path in &self.file_paths {
            let (_, data) = self.disk_manager.read_disk_all(file_path).await?;
            bytes.extend(data);
        }
        Ok(bytes.freeze())
    }

    async fn into_stream(self) -> ParpulseResult<StorageReaderStream> {
        Ok(Box::pin(MockS3ReaderStream::new(self.file_paths)))
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
        let reader = MockS3Reader::new(bucket, keys).await;
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

        let mut streaming_total_bytes = 0;
        while let Some(data) = s3_stream.next().await {
            let data = data.unwrap();
            streaming_total_bytes += data.len();
        }
        assert_eq!(streaming_total_bytes, 113629 + 112193);
    }
}
