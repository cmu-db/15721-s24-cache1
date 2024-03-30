use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::ParpulseResult;
use crate::storage_manager::ParpulseReaderIterator;
use crate::storage_reader::StorageReaderStream;

/// [`DiskManagerSync`] contains the common logic to read from or write to a disk.
///
/// TODO: Record statistics (maybe in statistics manager).
#[derive(Default)]
pub struct DiskManagerSync {}

// TODO: Make each method accepting `&self` instead of `&mut self`.
impl DiskManagerSync {
    pub fn open_or_create(&self, path: &str, append: bool) -> ParpulseResult<File> {
        let path_buf: PathBuf = PathBuf::from(path);
        if let Some(parent) = path_buf.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
        let mut options = OpenOptions::new();
        options.write(true);
        if !path_buf.exists() {
            options.create(true);
        }
        options.append(append);
        Ok(options.open(&path_buf)?)
    }

    // FIXME: `mut` allows future statistics computation
    pub fn write_disk_all(&mut self, path: &str, content: &[u8]) -> ParpulseResult<()> {
        // TODO: when path exists, we directly overwrite it, should we notify cache?
        let mut file = self.open_or_create(path, false)?;
        file.write_all(content)?;
        Ok(file.flush()?)
    }

    // FIXME: do we need to record statistics for read?
    pub fn read_disk_all(&self, path: &str) -> ParpulseResult<(usize, Bytes)> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::with_capacity(file.metadata()?.len() as usize);
        let bytes_read = file.read_to_end(&mut buffer)?;
        Ok((bytes_read, Bytes::from(buffer)))
    }

    pub fn read_disk(
        &self,
        path: &str,
        start_pos: u64,
        bytes_to_read: usize,
    ) -> ParpulseResult<(usize, Bytes)> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start_pos))?;

        let mut buffer = vec![0; bytes_to_read];
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        Ok((bytes_read, Bytes::from(buffer)))
    }

    // If needs to record statistics, use disk_read_iterator, if not, please directly new DiskReadIterator
    pub fn disk_read_iterator(
        &self,
        path: &str,
        buffer_size: usize,
    ) -> ParpulseResult<DiskReadIterator> {
        DiskReadIterator::new(path, buffer_size)
    }

    // FIXME: disk_path should not exist, otherwise throw an error
    pub fn write_iterator_reader_to_disk<T>(
        &mut self,
        mut iterator: T,
        disk_path: &str,
    ) -> ParpulseResult<usize>
    where
        T: ParpulseReaderIterator,
    {
        if Path::new(disk_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "disk file to write already exists",
            )
            .into());
        }
        let mut file = self.open_or_create(disk_path, true)?;
        let mut bytes_written = 0;
        loop {
            match iterator.next() {
                Some(Ok(bytes_read)) => {
                    let buffer = iterator.buffer();
                    file.write_all(&buffer[..bytes_read])?;
                    bytes_written += bytes_read;
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        // FIXME: do we need to flush?
        file.flush()?;
        Ok(bytes_written)
    }

    // I have to add a `async` here... Better way?
    pub async fn write_stream_reader_to_disk(
        &mut self,
        mut stream: StorageReaderStream,
        disk_path: &str,
    ) -> ParpulseResult<usize> {
        if Path::new(disk_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "disk file to write already exists",
            )
            .into());
        }
        let mut file = self.open_or_create(disk_path, true)?;
        let mut bytes_written = 0;

        loop {
            match stream.next().await {
                Some(Ok(bytes)) => {
                    file.write_all(&bytes)?;
                    bytes_written += bytes.len();
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(bytes_written)
    }

    pub fn file_size(&self, path: &str) -> ParpulseResult<u64> {
        let metadata = fs::metadata(path)?;
        Ok(metadata.len())
    }

    pub fn remove_file(&mut self, path: &str) -> ParpulseResult<()> {
        Ok(fs::remove_file(path)?)
    }
}

/// FIXME: iterator for sync, stream for async
pub struct DiskReadIterator {
    f: File,
    pub buffer: BytesMut,
}

impl DiskReadIterator {
    pub fn new(file_path: &str, buffer_size: usize) -> ParpulseResult<Self> {
        let f = File::open(file_path)?;

        Ok(DiskReadIterator {
            f,
            buffer: BytesMut::zeroed(buffer_size),
        })
    }
}

impl Iterator for DiskReadIterator {
    type Item = ParpulseResult<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.f.read(self.buffer.as_mut()) {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    Some(Ok(bytes_read))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl ParpulseReaderIterator for DiskReadIterator {
    fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::stream::RandomDiskReadStream;

    use super::*;
    #[test]
    fn test_simple_write_read() {
        let mut disk_manager = DiskManagerSync {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir
            .join("test_disk_manager_sync1.txt")
            .display()
            .to_string();
        let content = "Hello, world!";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .expect("write_disk_all failed");
        let mut file = disk_manager
            .open_or_create(path, true)
            .expect("open_or_create failed");
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();

        let file_size = disk_manager.file_size(path).expect("file_size failed");
        assert_eq!(file_size, 2 * content.len() as u64);

        let (bytes_read, bytes) = disk_manager
            .read_disk_all(path)
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, 2 * content.len());
        assert_eq!(bytes, Bytes::from(content.to_owned() + content));

        let (bytes_read, bytes) = disk_manager
            .read_disk(path, content.len() as u64, content.len())
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));
    }

    #[test]
    fn test_iterator_read() {
        let mut disk_manager = DiskManagerSync {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir
            .join("test_disk_manager_sync2.txt")
            .display()
            .to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .expect("write_disk_all failed");
        let mut iterator = disk_manager
            .disk_read_iterator(path, 2)
            .expect("disk_read_iterator failed");
        let mut start_pos = 0;
        loop {
            if start_pos >= content.len() {
                break;
            }
            let bytes_read = iterator
                .next()
                .expect("iterator early ended")
                .expect("iterator read failed");
            let buffer = iterator.buffer();
            assert_eq!(
                &content.as_bytes()[start_pos..start_pos + bytes_read],
                &buffer[..bytes_read]
            );
            start_pos += bytes_read;
        }
        assert_eq!(start_pos, content.len());
    }

    #[test]
    fn test_write_iterator_reader_to_disk() {
        let mut disk_manager = DiskManagerSync {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir
            .join("test_disk_manager_sync3.txt")
            .display()
            .to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .expect("write_disk_all failed");
        let iterator = disk_manager
            .disk_read_iterator(path, 1)
            .expect("disk_read_iterator failed");
        let output_path = &dir
            .join("test_disk_manager3_output.txt")
            .display()
            .to_string();
        let bytes_written = disk_manager
            .write_iterator_reader_to_disk::<DiskReadIterator>(iterator, output_path)
            .expect("write_reader_to_disk failed");
        assert_eq!(bytes_written, content.len());

        let (bytes_read, bytes) = disk_manager
            .read_disk_all(output_path)
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));
        let file_size = disk_manager
            .file_size(output_path)
            .expect("file_size failed");
        assert_eq!(file_size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_write_stream_reader_to_disk() {
        let mut disk_manager = DiskManagerSync {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir
            .join("test_disk_manager_sync4.txt")
            .display()
            .to_string();
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .expect("write_disk_all failed");
        let stream = RandomDiskReadStream::new(&path, 1, 2).unwrap().boxed();
        let output_path = &dir
            .join("test_disk_manager3_output.txt")
            .display()
            .to_string();
        let bytes_written = disk_manager
            .write_stream_reader_to_disk(stream, output_path)
            .await
            .expect("write_reader_to_disk failed");
        assert_eq!(bytes_written, content.len());

        let (bytes_read, bytes) = disk_manager
            .read_disk_all(output_path)
            .expect("read_disk_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));
        let file_size = disk_manager
            .file_size(output_path)
            .expect("file_size failed");
        assert_eq!(file_size, content.len() as u64);
    }

    #[test]
    fn test_remove_file() {
        let mut disk_manager = DiskManagerSync {};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_owned();
        let path = &dir
            .join("test_disk_manager_sync5.txt")
            .display()
            .to_string();
        let content = "Hello, world!";
        disk_manager
            .write_disk_all(path, content.as_bytes())
            .expect("write_disk_all failed");
        disk_manager.remove_file(path).expect("remove_file failed");
        assert!(!Path::new(path).exists());
    }
}
