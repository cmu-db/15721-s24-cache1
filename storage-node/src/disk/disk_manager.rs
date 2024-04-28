use bytes::Bytes;
use futures::stream::StreamExt;
use futures::{future::TryFutureExt, join};

use std::future::IntoFuture;
use std::io::SeekFrom;

use std::path::{Path, PathBuf};
use std::pin::Pin;

use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncSeekExt;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

use crate::error::{ParpulseError, ParpulseResult};
use crate::storage_reader::StorageReaderStream;

use super::stream::DiskReadStream;

/// [`DiskManager`] is responsible for reading and writing data to disk. The default
/// version is async. We keep this struct to add lock.
///
/// TODO: Do we need to put disk_root_path into DiskManager?
#[derive(Default)]
pub struct DiskManager {}

impl DiskManager {
    pub async fn open_or_create(&self, path: &str, append: bool) -> ParpulseResult<File> {
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

    pub async fn write_disk_all(&self, path: &str, content: &[u8]) -> ParpulseResult<()> {
        let mut file = self.open_or_create(path, false).await?;
        file.write_all(content).await?;
        Ok(file.flush().await?)
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

    // If needs to record statistics, use disk_read_stream, if not, please directly new DiskReadStream.
    pub async fn disk_read_stream(
        &self,
        path: &str,
        buffer_size: usize,
    ) -> ParpulseResult<Pin<Box<DiskReadStream>>> {
        let disk_read_stream = DiskReadStream::new(path, buffer_size).await?;
        Ok(Box::pin(disk_read_stream))
    }

    /// This function will try to **first** write `bytes_vec` to disk if applicable, and write all the (remaining)
    /// data polled from the `stream` to disk. The function will return the total bytes written to disk.
    ///
    /// Note these in current implementation:
    /// 1. When writing evicted data from memory cache, bytes_vec should be Some and stream should be None.
    /// 2. When memory cache is disabled, bytes_vec should be None and stream should be Some.
    /// 3. When writing data which cannot be written to memory cache, both bytes_vec and stream should be Some.
    ///
    /// FIXME: disk_path should not exist, otherwise throw an error
    /// TODO(lanlou): we must handle write-write conflict correctly in the future.
    /// One way is using `write commit` to handle read-write conflict, then there is no w-w conflict.
    /// TODO(lanlou): We need to write data to disk & send data to network at the same time.
    /// TODO(lanlou): S3 stream now returns 10^5 bytes one time, and do we need to group all the bytes for
    /// one file and write all of them to disk at once?
    pub async fn write_bytes_and_stream_to_disk(
        &self,
        bytes_vec: Option<Vec<Bytes>>,
        stream: Option<StorageReaderStream>,
        disk_path: &str,
    ) -> ParpulseResult<usize> {
        if Path::new(disk_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "disk file to write already exists",
            )
            .into());
        }
        let mut file = self.open_or_create(disk_path, true).await?;
        let mut bytes_written = 0;

        if let Some(bytes_vec) = bytes_vec {
            for bytes in bytes_vec {
                file.write_all(&bytes).await?;
                bytes_written += bytes.len();
            }
        }

        if let Some(mut stream) = stream {
            let bytes_cur = stream.next().await;
            if bytes_cur.is_none() {
                file.flush().await?;
                return Ok(bytes_written);
            }
            let mut bytes_cur = bytes_cur.unwrap()?;
            loop {
                let disk_write_fut = TryFutureExt::into_future(file.write_all(&bytes_cur));
                let bytes_next_fut = stream.next().into_future();
                match join!(disk_write_fut, bytes_next_fut) {
                    (Ok(_), Some(Ok(bytes_next))) => {
                        bytes_written += bytes_cur.len();
                        bytes_cur = bytes_next;
                    }
                    (Ok(_), None) => {
                        bytes_written += bytes_cur.len();
                        break;
                    }
                    (Err(e), _) => return Err(ParpulseError::Disk(e)),
                    (Ok(_), Some(Err(e))) => return Err(e),
                }
            }
        }
        // FIXME: do we need a flush here?
        file.flush().await?;
        Ok(bytes_written)
    }

    pub async fn file_size(&self, path: &str) -> ParpulseResult<u64> {
        let metadata = fs::metadata(path).await?;
        Ok(metadata.len())
    }

    pub fn file_size_sync(&self, path: &str) -> ParpulseResult<u64> {
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    pub async fn remove_file(&self, path: &str) -> ParpulseResult<()> {
        Ok(fs::remove_file(path).await?)
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::stream::RandomDiskReadStream;

    use super::*;
    #[tokio::test]
    async fn test_simple_write_read() {
        let disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager1.txt").display().to_string();
        let content = "Hello, world!";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        let mut file = disk_manager
            .open_or_create(path, true)
            .await
            .expect("open_or_create failed");
        file.write_all(content.as_bytes()).await.unwrap();
        // Without this code, this test will fail sometimes.
        // But even if we add this code, this test is not likely to fail in the sync version.
        file.flush().await.unwrap();

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
        let disk_manager = DiskManager {};
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
        let disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager3.txt").display().to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        let stream = RandomDiskReadStream::new(path, 2, 4).unwrap().boxed();
        let output_path = &dir
            .join("test_disk_manager3_output.txt")
            .display()
            .to_string();
        let bytes_written = disk_manager
            .write_bytes_and_stream_to_disk(None, Some(stream), output_path)
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
    async fn test_write_bytes_to_disk() {
        let disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager4.txt").display().to_string();
        let content1 = "Hello, world!";
        let content2 = "Bye, CMU!";
        let bytes_written = disk_manager
            .write_bytes_and_stream_to_disk(
                Some(vec![Bytes::from(content1), Bytes::from(content2)]),
                None,
                path,
            )
            .await
            .expect("write_bytes_to_disk failed");
        assert_eq!(bytes_written, content1.len() + content2.len());
        let (bytes_read, bytes) = disk_manager
            .read_disk_all(path)
            .await
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, content1.len() + content2.len());
        assert_eq!(bytes, Bytes::from(content1.to_owned() + content2));
    }

    #[tokio::test]
    async fn test_write_bytes_and_stream_to_disk() {
        let disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager5.txt").display().to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .await
            .expect("write_disk_all failed");
        let mut stream = RandomDiskReadStream::new(path, 2, 4).unwrap().boxed();

        let mut bytes_vec: Vec<Bytes> = Vec::new();
        for _ in 0..3 {
            let stream_data = stream.next().await.unwrap().unwrap();
            bytes_vec.push(stream_data);
        }

        let output_path = &dir
            .join("test_disk_manager5_output.txt")
            .display()
            .to_string();
        let bytes_written = disk_manager
            .write_bytes_and_stream_to_disk(Some(bytes_vec), Some(stream), output_path)
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
        let disk_manager = DiskManager {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir.join("test_disk_manager6.txt").display().to_string();
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
