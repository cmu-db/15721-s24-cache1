use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::{FutureExt, Stream};
use std::borrow::BorrowMut;
use std::future::IntoFuture;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::error::ParpulseResult;
use crate::storage_manager::ParpulseReaderStream;

// TODO: Do we need to put disk_root_path into DiskManagerAsync?
pub struct DiskManagerAsync {}

impl DiskManagerAsync {
    pub async fn write_fd(&self, path: &str, append: bool) -> ParpulseResult<File> {
        let path_buf: PathBuf = PathBuf::from(path);
        if let Some(parent) = path_buf.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await?;
            }
        }
        let mut options = OpenOptions::new();
        options.write(true);
        if !path_buf.exists() {
            options.create(true);
        }
        options.append(append);
        Ok(options.open(&path_buf).await?)
    }

    pub async fn read_disk(&self, path: &str) -> ParpulseResult<(usize, Bytes)> {
        let mut file = File::open(path).await?;
        let mut buffer = Vec::with_capacity(file.metadata().await?.len() as usize);

        let bytes_read = file.read_to_end(&mut buffer).await?;
        Ok((bytes_read, Bytes::from(buffer)))
    }

    pub async fn write_disk(&mut self, path: &str, content: &[u8]) -> ParpulseResult<()> {
        let mut file = self.write_fd(path, false).await?;
        Ok(file.write_all(content).await?)
    }

    pub async fn disk_read_stream(
        &self,
        path: &str,
        buffer_size: usize,
    ) -> ParpulseResult<Pin<Box<DiskReadStream>>> {
        let disk_read_stream = DiskReadStream::new(path, buffer_size).await?;
        Ok(Box::pin(disk_read_stream))
    }

    pub async fn write_reader_to_disk<T>(
        &mut self,
        mut stream: Pin<Box<T>>,
        disk_path: &str,
    ) -> ParpulseResult<usize>
    where
        T: ParpulseReaderStream,
    {
        if Path::new(disk_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "disk file to write already exists",
            )
            .into());
        }
        let mut file = self.write_fd(disk_path, true).await?;
        let mut bytes_written = 0;
        loop {
            match stream.next().await {
                Some(Ok(bytes_read)) => {
                    let buffer = stream.buffer();
                    file.write_all(&buffer[..bytes_read]).await?;
                    bytes_written += bytes_read;
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(bytes_written)
    }

    pub async fn file_size(&self, path: &str) -> ParpulseResult<u64> {
        let metadata = fs::metadata(path).await?;
        Ok(metadata.len())
    }

    pub async fn remove_file(&mut self, path: &str) -> ParpulseResult<()> {
        Ok(fs::remove_file(path).await?)
    }
}

pub struct DiskReadStream {
    f: File,
    pub buffer: BytesMut,
}

impl DiskReadStream {
    pub async fn new(file_path: &str, buffer_size: usize) -> ParpulseResult<Self> {
        let f = File::open(file_path).await?;

        Ok(DiskReadStream {
            f,
            buffer: BytesMut::zeroed(buffer_size),
        })
    }
}

impl ParpulseReaderStream for DiskReadStream {
    fn buffer(&self) -> &BytesMut {
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

#[cfg(test)]
mod tests {
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::Instant;

    use bytes::{Buf, BufMut};

    use super::*;
    #[test]
    fn random_test() {
        let mut bytes_mut = BytesMut::new();

        bytes_mut.resize(1024 * 1024 * 1024, 145);
        let mut bytes_mut2 = BytesMut::with_capacity(1024 * 1024 * 1024);
        println!("{:p} {:p} {}", &bytes_mut, &bytes_mut2, bytes_mut.len());
        let now = Instant::now();
        let bytes = bytes_mut.copy_to_bytes(bytes_mut.len());
        // let bytes = bytes_mut.freeze();
        println!("{:p}  {}", &bytes, bytes.len());
        // bytes_mut[0] = b'e';
        let elapsed_time = now.elapsed();
        println!("Running slow_function() took {:?} seconds.", elapsed_time);
        println!("{}", bytes[0]);
    }
}
