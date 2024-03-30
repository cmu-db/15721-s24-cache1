use bytes::{Bytes, BytesMut};
use futures::{FutureExt, Stream};

use rand::Rng;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::error::{ParpulseError, ParpulseResult};

/// [`DiskReadStream`] reads data from disk as a stream.
pub struct DiskReadStream {
    /// The file to read from.
    f: File,
    /// Contains the data read from the file.
    /// Note that the buffer may not be fully filled with data read from the file.
    buffer: BytesMut,
}

impl DiskReadStream {
    pub fn new_sync(file_path: &str, buffer_size: usize) -> ParpulseResult<Self> {
        let f: std::fs::File = std::fs::File::open(file_path)?;

        Ok(DiskReadStream {
            f: File::from_std(f),
            buffer: BytesMut::zeroed(buffer_size),
        })
    }

    pub async fn new(file_path: &str, buffer_size: usize) -> ParpulseResult<Self> {
        let f = File::open(file_path).await?;

        Ok(DiskReadStream {
            f,
            buffer: BytesMut::zeroed(buffer_size),
        })
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

impl Stream for DiskReadStream {
    type Item = ParpulseResult<usize>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let deref_self = self.deref_mut();
        match deref_self
            .f
            .read(deref_self.buffer.as_mut())
            .boxed()
            .poll_unpin(cx)
        {
            Poll::Ready(Ok(bytes_read)) => {
                if bytes_read > 0 {
                    Poll::Ready(Some(Ok(bytes_read)))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// [`RandomDiskReadStream`] is used by `MockS3Reader` to simulate the read from S3.
/// Since every time we pull data from S3, the number of bytes read is random, we
/// need to simulate this behavior.
///
/// NOTE: The byte range here is only a hint. Due to the implementation of tokio's
/// `AsyncReadExt` trait, the actual number of bytes read may be less than `min_read_bytes`.
/// It is acceptable here because we just use this `RandomDiskReadStream` to simulate
/// the read from S3.
///
/// `RandomDiskReadStream` should only be used for testing purposes.
pub struct RandomDiskReadStream {
    f: File,
    min_read_bytes: usize,
    max_read_bytes: usize,
    buffer: BytesMut,
}

impl RandomDiskReadStream {
    pub fn new(
        file_path: &str,
        min_read_bytes: usize,
        max_read_bytes: usize,
    ) -> ParpulseResult<Self> {
        let f: std::fs::File = std::fs::File::open(file_path)?;
        if min_read_bytes >= max_read_bytes {
            return Err(ParpulseError::Internal(
                "`min_read_bytes` must be less than `max_read_bytes` in `RandomDiskReadStream`"
                    .to_string(),
            ));
        }

        Ok(RandomDiskReadStream {
            f: File::from_std(f),
            min_read_bytes,
            max_read_bytes,
            buffer: BytesMut::new(),
        })
    }
}

impl Stream for RandomDiskReadStream {
    type Item = ParpulseResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let bytes_to_read = rand::thread_rng().gen_range(self.min_read_bytes..self.max_read_bytes);
        self.buffer.resize(bytes_to_read, 0);
        let deref_self = self.deref_mut();

        let read_result = deref_self
            .f
            .read(deref_self.buffer.as_mut())
            .boxed()
            .poll_unpin(cx);
        match read_result {
            Poll::Ready(Ok(bytes_read)) => {
                if bytes_read > 0 {
                    // Though we have resized the buffer to `bytes_to_read` before, tokio's
                    // implementation doesn't ensure that `bytes_to_read` bytes have been read
                    // into the buffer. It's likely that fewer bytes have been read. So we
                    // truncate the buffer to the actual number of bytes read here.
                    deref_self.buffer.truncate(bytes_read);
                    Poll::Ready(Some(Ok(deref_self.buffer.clone().freeze())))
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;

    #[tokio::test]
    async fn test_disk_read_stream() {
        let poem = "What can I hold you with?
I offer you lean streets, desperate sunsets, the
moon of the jagged suburbs.
I offer you the bitterness of a man who has looked
long and long at the lonely moon.
I offer you my ancestors, my dead men, the ghosts
that living men have honoured in bronze.
I offer you whatever insight my books may hold,
whatever manliness or humour my life.
I offer you the loyalty of a man who has never
been loyal.
I offer you that kernel of myself that I have saved,
somehow-the central heart that deals not
in words, traffics not with dreams, and is
untouched by time, by joy, by adversities.
I offer you the memory of a yellow rose seen at
sunset, years before you were born.
I offer you explanations of yourself, theories about
yourself, authentic and surprising news of
yourself.
I can give you my loneliness, my darkness, the
hunger of my heart; I am trying to bribe you
with uncertainty, with danger, with defeat.
";

        let buffer_size = 102;
        let mut disk_read_stream =
            DiskReadStream::new("tests/text/what-can-i-hold-you-with", buffer_size)
                .await
                .unwrap();

        let mut total_bytes_read = 0;
        let mut read_count = 0;
        let mut result = String::new();
        while let Some(bytes_read) = disk_read_stream.next().await {
            let bytes_read = bytes_read.unwrap();
            result += &String::from_utf8(disk_read_stream.buffer()[..bytes_read].to_vec()).unwrap();
            total_bytes_read += bytes_read;
            read_count += 1;
        }

        assert_eq!(result, poem);
        assert_eq!(total_bytes_read, 930);
        assert_eq!(read_count, 10);
    }

    #[tokio::test]
    async fn test_random_disk_read_stream() {
        let poem = "What can I hold you with?
I offer you lean streets, desperate sunsets, the
moon of the jagged suburbs.
I offer you the bitterness of a man who has looked
long and long at the lonely moon.
I offer you my ancestors, my dead men, the ghosts
that living men have honoured in bronze.
I offer you whatever insight my books may hold,
whatever manliness or humour my life.
I offer you the loyalty of a man who has never
been loyal.
I offer you that kernel of myself that I have saved,
somehow-the central heart that deals not
in words, traffics not with dreams, and is
untouched by time, by joy, by adversities.
I offer you the memory of a yellow rose seen at
sunset, years before you were born.
I offer you explanations of yourself, theories about
yourself, authentic and surprising news of
yourself.
I can give you my loneliness, my darkness, the
hunger of my heart; I am trying to bribe you
with uncertainty, with danger, with defeat.
";

        let mut random_disk_read_stream =
            RandomDiskReadStream::new("tests/text/what-can-i-hold-you-with", 150, 250).unwrap();

        let mut total_bytes_read = 0;
        let mut result = String::new();
        while let Some(bytes) = random_disk_read_stream.next().await {
            let bytes = bytes.unwrap();
            total_bytes_read += bytes.len();
            result += &String::from_utf8(bytes.to_vec()).unwrap();
        }

        assert_eq!(result, poem);
        assert_eq!(total_bytes_read, 930);
    }
}
