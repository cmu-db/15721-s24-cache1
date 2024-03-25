use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::{FutureExt, Stream};
use std::borrow::BorrowMut;
use std::future::IntoFuture;
use std::io::SeekFrom;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncSeekExt;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::error::ParpulseResult;
use crate::storage_manager::ParpulseReaderStream;

// TODO: Do we need to put disk_root_path into DiskManager?
pub struct DiskManager {}

impl DiskManager {
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

    pub async fn write_disk_all(&mut self, path: &str, content: &[u8]) -> ParpulseResult<()> {
        let mut file = self.write_fd(path, false).await?;
        Ok(file.write_all(content).await?)
    }

    pub async fn read_disk_all(&self, path: &str) -> ParpulseResult<(usize, Bytes)> {
        let mut file = File::open(path).await?;
        let mut buffer = Vec::with_capacity(file.metadata().await?.len() as usize);

        let bytes_read = file.read_to_end(&mut buffer).await?;
        Ok((bytes_read, Bytes::from(buffer)))
    }

    pub async fn read_disk(
        &self,
        path: &str,
        start_pos: u64,
        bytes_to_read: usize,
    ) -> ParpulseResult<(usize, Bytes)> {
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(start_pos)).await?;

        let mut buffer = vec![0; bytes_to_read];
        let bytes_read = file.read(&mut buffer).await?;
        buffer.truncate(bytes_read);
        Ok((bytes_read, Bytes::from(buffer)))
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
    use super::*;
    #[tokio::test]
    async fn test_simple_write_read() {
        let mut disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager1.txt").display().to_string();
        let content = "Hello, world!";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        let mut file = disk_manager
            .write_fd(path, true)
            .await
            .expect("write_fd failed");
        file.write_all(content.as_bytes()).await.unwrap();

        let file_size = disk_manager
            .file_size(path)
            .await
            .expect("file_size failed");
        assert_eq!(file_size, 2 * content.len() as u64);

        let (bytes_read, bytes) = disk_manager
            .read_disk_all(path)
            .await
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, 2 * content.len());
        assert_eq!(bytes, Bytes::from(content.to_owned() + content));

        let (bytes_read, bytes) = disk_manager
            .read_disk(path, content.len() as u64, content.len())
            .await
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));
    }

    #[tokio::test]
    async fn test_iterator_read() {
        let mut disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager2.txt").display().to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        let mut stream = disk_manager
            .disk_read_stream(path, 2)
            .await
            .expect("disk_read_iterator failed");
        let mut start_pos = 0;
        loop {
            if start_pos >= content.len() {
                break;
            }
            let bytes_read = stream
                .next()
                .await
                .expect("iterator early ended")
                .expect("iterator read failed");
            let buffer = stream.buffer();
            assert_eq!(
                &content.as_bytes()[start_pos..start_pos + bytes_read],
                &buffer[..bytes_read]
            );
            start_pos += bytes_read;
        }
        assert_eq!(start_pos, content.len());
    }

    #[tokio::test]
    async fn test_write_reader_to_disk() {
        let mut disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager3.txt").display().to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        let mut stream = disk_manager
            .disk_read_stream(path, 1)
            .await
            .expect("disk_read_iterator failed");
        let output_path = &dir
            .join("test_disk_manager3_output.txt")
            .display()
            .to_string();
        let bytes_written = disk_manager
            .write_reader_to_disk::<DiskReadStream>(stream, output_path)
            .await
            .expect("write_reader_to_disk failed");
        assert_eq!(bytes_written, content.len());

        let (bytes_read, bytes) = disk_manager
            .read_disk_all(output_path)
            .await
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));
        let file_size = disk_manager
            .file_size(output_path)
            .await
            .expect("file_size failed");
        assert_eq!(file_size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_remove_file() {
        let mut disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager4.txt").display().to_string();
        let content = "Hello, world!";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        disk_manager
            .remove_file(path)
            .await
            .expect("remove_file failed");
        assert!(!Path::new(path).exists());
    }
}
