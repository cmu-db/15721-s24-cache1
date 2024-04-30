use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{
    operation::get_object::{GetObjectError, GetObjectOutput},
    primitives::ByteStream,
    Client,
};
use aws_smithy_runtime_api::{client::result::SdkError, http::Response};
use bytes::Bytes;
use futures::{future::BoxFuture, ready, FutureExt, Stream};

use crate::error::{ParpulseError, ParpulseResult};

use super::{AsyncStorageReader, StorageReaderStream};

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

    pub async fn get_object_size(&self) -> ParpulseResult<usize> {
        let mut size = 0;
        for key in &self.keys {
            let obj = self
                .client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await?;
            size += obj
                .content_length
                .map(|l| l as usize)
                .ok_or_else(|| ParpulseError::S3("fail to get object size".into()))?;
        }
        Ok(size)
    }
}

/// [`S3DataStream`] is a stream for reading data from S3. It reads the data in
/// chunks and returns the data in a stream. Currently it uses non-fixed buffer,
/// which means it will be consumed and extended.
///
/// If we want to use fixed buffer for benchmark, we can add self.last_read_size and
/// self.current_buffer_pos.
pub struct S3ReaderStream {
    client: Client,
    bucket: String,
    keys: Vec<String>,
    current_key: usize,

    object_fut:
        Option<BoxFuture<'static, Result<GetObjectOutput, SdkError<GetObjectError, Response>>>>,
    object_body: Option<ByteStream>,
}

impl S3ReaderStream {
    pub fn new(client: Client, bucket: String, keys: Vec<String>) -> Self {
        assert!(!keys.is_empty(), "keys should not be empty");
        Self {
            client,
            bucket,
            keys,
            current_key: 0,
            object_fut: None,
            object_body: None,
        }
    }
}

impl Stream for S3ReaderStream {
    type Item = ParpulseResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(object_fut) = self.object_fut.as_mut() {
            match ready!(object_fut.poll_unpin(cx)) {
                Ok(object) => {
                    self.object_fut.take();
                    self.object_body = Some(object.body);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(ParpulseError::from(e)))),
            }
        } else if let Some(object_body) = self.object_body.as_mut() {
            let poll_result = object_body.try_next().boxed().poll_unpin(cx);
            match poll_result {
                Poll::Ready(ready_result) => match ready_result {
                    Ok(Some(bytes)) => Poll::Ready(Some(Ok(bytes))),
                    Ok(None) => {
                        self.object_body = None;
                        self.poll_next(cx)
                    }
                    Err(e) => Poll::Ready(Some(Err(ParpulseError::from(e)))),
                },
                Poll::Pending => Poll::Pending,
            }
        } else if self.current_key >= self.keys.len() {
            // No more data to read in S3.
            Poll::Ready(None)
        } else {
            // There are more files to read in S3. Fetch the next object.
            let fut = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.keys[self.current_key])
                .send()
                .boxed();
            self.object_fut = Some(fut);
            self.current_key += 1;
            self.poll_next(cx)
        }
    }
}

#[async_trait]
impl AsyncStorageReader for S3Reader {
    /// NEVER call this method if you do not know the size of the data -- collecting
    /// all data into one buffer might lead to OOM.
    async fn read_all(&self) -> ParpulseResult<Vec<Bytes>> {
        let mut bytes_vec = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let object = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(ParpulseError::from)?;
            bytes_vec.push(
                object
                    .body
                    .collect()
                    .await
                    .map_err(ParpulseError::from)?
                    .into_bytes(),
            );
        }
        Ok(bytes_vec)
    }

    async fn into_stream(self) -> ParpulseResult<StorageReaderStream> {
        let s3_stream = S3ReaderStream::new(self.client, self.bucket, self.keys);
        Ok(Box::pin(s3_stream))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_s3_read_all() {
        let bucket = "parpulse-test".to_string();
        let keys = vec!["userdata/userdata1.parquet".to_string()];
        let reader = S3Reader::new(bucket, keys).await;
        let bytes = reader.read_all().await.unwrap();
        assert_eq!(bytes[0].len(), 113629);
    }

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

        let mut streaming_total_bytes = 0;
        while let Some(data) = s3_stream.next().await {
            let data = data.unwrap();
            streaming_total_bytes += data.len();
        }
        assert_eq!(streaming_total_bytes, 565545);
    }

    #[tokio::test]
    async fn test_s3_get_object_size() {
        let bucket = "parpulse-test".to_string();
        let keys = vec![
            "userdata/userdata1.parquet".to_string(),
            "userdata/userdata2.parquet".to_string(),
            "userdata/userdata3.parquet".to_string(),
            "userdata/userdata4.parquet".to_string(),
            "userdata/userdata5.parquet".to_string(),
        ];

        let reader = S3Reader::new(bucket, keys).await;
        let size = reader.get_object_size().await.unwrap();
        assert_eq!(size, 565545);
    }
}
