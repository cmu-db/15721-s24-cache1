use std::{
    collections::VecDeque,
    error::Error,
    pin::Pin,
    task::{Context, Poll},
};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{
    operation::get_object::{GetObjectError, GetObjectOutput},
    primitives::{AggregatedBytes, ByteStreamError},
    Client,
};
use aws_smithy_runtime_api::{client::result::SdkError, http::Response};
use bytes::{Buf, Bytes, BytesMut};
use datafusion::error::DataFusionError;
use futures::{
    future::BoxFuture,
    ready,
    stream::{poll_fn, BoxStream},
    Future, FutureExt, Stream,
};

use crate::error::{ParpulseError, ParpulseResult};

use super::{AsyncStorageReader, StorageDataStream, StorageReaderIterator};

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

pub struct S3ReaderIterator {
    client: Client,
    bucket: String,
    keys: Vec<String>,
    current_key: usize,
    buffer: Bytes,
    chunk_size: usize,

    object_fut:
        Option<BoxFuture<'static, Result<GetObjectOutput, SdkError<GetObjectError, Response>>>>,
    // data_fut: Option<BoxFuture<'static, Result<AggregatedBytes, ByteStreamError>>>,
}

impl S3ReaderIterator {
    pub fn new(client: Client, bucket: String, keys: Vec<String>, chunk_size: usize) -> Self {
        assert!(!keys.is_empty(), "keys should not be empty");
        let fut = Box::pin(client.get_object().bucket(&bucket).key(&keys[0]).send());
        Self {
            client,
            bucket,
            keys: keys.into(),
            current_key: 0,
            buffer: Bytes::new(),
            chunk_size,
            object_fut: Some(fut),
            // data_fut: None,
        }
    }
}

impl Stream for S3ReaderIterator {
    type Item = ParpulseResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.object_fut.is_some() {
            println!("object_fut is some");
            let mut object_fut = self.object_fut.take().unwrap();
            println!("object_fut is taken");
            loop {
                match object_fut.poll_unpin(cx) {
                    Poll::Ready(_) => {
                        println!("object_fut is ready");
                        break;
                    }
                    Poll::Pending => println!("object_fut is pending"),
                }
            }
            println!("object_fut is polled");
            match ready!(object_fut.poll_unpin(cx)) {
                Ok(object) => {
                    println!("poll object is ok");
                    let mut data_fut = object.body.collect().boxed();
                    match ready!(data_fut.poll_unpin(cx)) {
                        Ok(bytes) => {
                            println!("poll data is ok");
                            self.buffer = bytes.into_bytes();
                            self.poll_next(cx)
                        }
                        Err(e) => Poll::Ready(Some(Err(ParpulseError::from(e)))),
                    }
                }
                Err(e) => Poll::Ready(Some(Err(ParpulseError::from(e)))),
            }
        } else {
            if self.buffer.remaining() == 0 {
                self.current_key += 1;
                if self.current_key >= self.keys.len() {
                    Poll::Ready(None)
                } else {
                    let fut = self
                        .client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(&self.keys[self.current_key])
                        .send();
                    self.object_fut = Some(fut.boxed());
                    self.poll_next(cx)
                }
            } else {
                // Read the bytes in the buffer by `chunk_size`.
                let read_byte_count = std::cmp::min(self.chunk_size, self.buffer.remaining());
                let bytes = self.buffer.copy_to_bytes(read_byte_count);
                Poll::Ready(Some(Ok(bytes)))
            }
        }
    }
}

impl AsyncStorageReader for S3Reader {
    /// NEVER call this method if you do not know the size of the data -- collecting
    /// all data into one buffer might lead to OOM.
    async fn read(&self) -> ParpulseResult<Bytes> {
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

    async fn streaming_read(&self) -> ParpulseResult<StorageDataStream> {
        let iterator = S3ReaderIterator::new(
            self.client.clone(),
            self.bucket.clone(),
            self.keys.clone(),
            1024, // TODO: Set buffer size from config
        );
        Ok(Box::pin(iterator))
    }
}

// fn poll_obj_fut(client: Client, cx: &mut Context<'_>) -> Poll<String> {
//     let bucket = "parpulse-test".to_string();
//     let keys = vec![
//         "userdata/userdata1.parquet".to_string(),
//         // "userdata/userdata2.parquet".to_string(),
//         // "userdata/userdata3.parquet".to_string(),
//         // "userdata/userdata4.parquet".to_string(),
//         // "userdata/userdata5.parquet".to_string(),
//     ];

//     client.get_object().bucket(&bucket).key(&keys[0]).send()
// }

mod tests {
    use futures::StreamExt;

    use super::*;

    #[ignore = "environment variables required"]
    #[tokio::test]
    async fn test_s3_read_all() {
        let bucket = "parpulse-test".to_string();
        let keys = vec!["userdata/userdata1.parquet".to_string()];
        let mut reader = S3Reader::new(bucket, keys).await;
        let bytes = reader.read().await.unwrap();
        assert_eq!(bytes.len(), 113629);
    }

    #[tokio::test]
    async fn test_s3_read_streaming() {
        let bucket = "parpulse-test".to_string();
        let keys = vec![
            "userdata/userdata1.parquet".to_string(),
            // "userdata/userdata2.parquet".to_string(),
            // "userdata/userdata3.parquet".to_string(),
            // "userdata/userdata4.parquet".to_string(),
            // "userdata/userdata5.parquet".to_string(),
        ];

        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        client
            .get_object()
            .bucket(&bucket)
            .key(&keys[0])
            .send()
            .await
            .unwrap();
        println!("got object from s3 client!");

        let mut reader = S3Reader::new(bucket, keys).await;
        let mut data_stream = reader.streaming_read().await.unwrap();
        let mut read_count = 0;
        println!("{}", 111);
        while let Some(data) = data_stream.next().await {
            assert_eq!(data.unwrap().len(), 1024);
            println!("get {} bytes data", read_count);
            read_count += 1;
        }
        println!("read_count: {}", read_count);
    }
}
