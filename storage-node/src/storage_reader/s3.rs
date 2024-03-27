use std::{
    error::Error,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{
    operation::get_object::{GetObjectError, GetObjectOutput},
    primitives::{AggregatedBytes, ByteStreamError},
    Client,
};
use aws_smithy_runtime_api::{client::result::SdkError, http::Response};
use bytes::{Buf, Bytes, BytesMut};
use futures::{future::BoxFuture, ready, Future, FutureExt, Stream};

use crate::{
    error::{ParpulseError, ParpulseResult},
    storage_manager::ParpulseReaderStream,
};

use super::AsyncStorageReader;

const DEFAULT_CHUNK_SIZE: usize = 1024;

/// [`S3Reader`] is a reader for retrieving data from S3. It can either read the
/// data once at all or read the data in an asynchronous stream.
pub struct S3Reader {
    client: Client,
    bucket: String,
    keys: Vec<String>,
}

impl S3Reader {
    pub async fn new(bucket: String, keys: Vec<String>) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        Self {
            client,
            bucket,
            keys,
        }
    }
}

/// [`S3DataStream`] is a stream for reading data from S3. It reads the data in
/// chunks and returns the data in a stream. Currently it uses non-fixed buffer,
/// which means it will be consumed and extended.
///
/// If we want to use fixed buffer for benchmark, we can add self.last_read_size and
/// self.current_buffer_pos.
pub struct S3DataStream {
    client: Client,
    bucket: String,
    keys: Vec<String>,
    current_key: usize,
    buffer_inner: BytesMut,
    chunk_size: usize,
    buffer: BytesMut,

    object_fut:
        Option<BoxFuture<'static, Result<GetObjectOutput, SdkError<GetObjectError, Response>>>>,
    data_fut: Option<BoxFuture<'static, Result<AggregatedBytes, ByteStreamError>>>,
}

impl S3DataStream {
    pub fn new(client: Client, bucket: String, keys: Vec<String>, chunk_size: usize) -> Self {
        assert!(!keys.is_empty(), "keys should not be empty");
        let fut = client
            .get_object()
            .bucket(&bucket)
            .key(&keys[0])
            .send()
            .boxed();
        Self {
            client,
            bucket,
            keys,
            current_key: 0,
            buffer_inner: BytesMut::new(),
            chunk_size,
            object_fut: Some(fut),
            data_fut: None,
            buffer: BytesMut::new(),
        }
    }
}

impl Stream for S3DataStream {
    type Item = ParpulseResult<usize>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(object_fut) = self.object_fut.as_mut() {
            match ready!(object_fut.poll_unpin(cx)) {
                Ok(object) => {
                    self.object_fut.take();
                    let fut = object.body.collect().boxed();
                    self.data_fut = Some(fut);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(ParpulseError::from(e)))),
            }
        } else if let Some(data_fut) = self.data_fut.as_mut() {
            match ready!(data_fut.poll_unpin(cx)) {
                Ok(bytes) => {
                    self.data_fut.take();
                    // TODO: extend is real `copy`, expensive! Can we directly turn AggregateBytes to BytesMut?
                    let data = bytes.into_bytes();
                    // TODO: If we don't need chunksize (since it will cause more frequent disk I/O), we can
                    // add `self.chunksize = bytes.into_bytes().len()` here, and don't forget to modify tests!
                    // We keep chunksize for now, so we can benchmark w/o it in the future.
                    // self.chunk_size = data.len();
                    self.buffer_inner.extend(data);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(ParpulseError::from(e)))),
            }
        } else if self.buffer_inner.remaining() >= self.chunk_size {
            // There are enough data to consume in the buffer. Return the data directly.
            let chunk_size = self.chunk_size;
            self.buffer = self.buffer_inner.split_to(chunk_size);
            Poll::Ready(Some(Ok(chunk_size)))
        } else {
            // The size of the remaining data is less than the chunk size.
            if self.current_key + 1 >= self.keys.len() {
                if self.buffer_inner.is_empty() {
                    // No more data. Return None.
                    Poll::Ready(None)
                } else {
                    // No more data in S3. Just return the remaining data.
                    let remaining_len = self.buffer_inner.len();
                    self.buffer = self.buffer_inner.split_to(remaining_len);
                    Poll::Ready(Some(Ok(remaining_len)))
                }
            } else {
                // There are more data in S3. Fetch the next object.
                self.current_key += 1;
                let fut = self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(&self.keys[self.current_key])
                    .send()
                    .boxed();
                self.object_fut = Some(fut);
                self.poll_next(cx)
            }
        }
    }
}

impl ParpulseReaderStream for S3DataStream {
    fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

#[async_trait]
impl AsyncStorageReader for S3Reader {
    type ReaderStream = S3DataStream;

    /// NEVER call this method if you do not know the size of the data -- collecting
    /// all data into one buffer might lead to OOM.
    async fn read_all(&self) -> ParpulseResult<Bytes> {
        let mut bytes = BytesMut::new();
        for key in &self.keys {
            let object = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(ParpulseError::from)?;
            bytes.extend(
                object
                    .body
                    .collect()
                    .await
                    .map_err(ParpulseError::from)?
                    .into_bytes(),
            );
        }
        Ok(bytes.freeze())
    }

    async fn into_stream(self) -> ParpulseResult<Pin<Box<Self::ReaderStream>>> {
        let s3_stream = S3DataStream::new(
            self.client,
            self.bucket,
            self.keys,
            DEFAULT_CHUNK_SIZE, // TODO: Set buffer size from config
        );
        Ok(Box::pin(s3_stream))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::{future::poll_fn, StreamExt};

    use super::*;

    #[ignore = "environment variables required"]
    #[tokio::test]
    async fn test_s3_read_all() {
        let bucket = "parpulse-test".to_string();
        let keys = vec!["userdata/userdata1.parquet".to_string()];
        let mut reader = S3Reader::new(bucket, keys).await;
        let bytes = reader.read_all().await.unwrap();
        assert_eq!(bytes.len(), 113629);
    }

    #[ignore = "environment variables required"]
    #[tokio::test]
    async fn test_s3_read_streaming() {
        let bucket = "parpulse-test".to_string();
        let keys = vec![
            "userdata/userdata1.parquet".to_string(),
            "userdata/userdata2.parquet".to_string(),
            "userdata/userdata3.parquet".to_string(),
            "userdata/userdata4.parquet".to_string(),
            "userdata/userdata5.parquet".to_string(),
        ];

        let reader = S3Reader::new(bucket, keys).await;
        let mut s3_stream = reader.into_stream().await.unwrap();

        let mut streaming_read_count = 0;
        let mut streaming_total_bytes = 0;
        while let Some(data_len) = s3_stream.next().await {
            let data_len = data_len.unwrap();
            streaming_read_count += 1;
            streaming_total_bytes += data_len;
            assert_eq!(s3_stream.buffer().len(), data_len);
        }
        assert_eq!(streaming_total_bytes, 565545);
        assert_eq!(streaming_read_count, 553);
    }
}
