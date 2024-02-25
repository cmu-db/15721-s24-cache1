use bytes::{Bytes, BytesMut};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::{
    storage_reader::{StorageReader, StorageReaderIterator},
    StorageResult,
};

/// [`DiskManager`] contains the common logic to read from or write to a disk.
///
/// TODO: add some statistics member in DiskManager
pub struct DiskManager {}

impl DiskManager {
    pub fn write_fd(&self, path: &str, append: bool) -> StorageResult<File> {
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
    // TODO: only practical when you want to write data all at once
    pub fn write_disk_sync_all(&mut self, path: &str, content: &[u8]) -> StorageResult<()> {
        let mut file = self.write_fd(path, false)?;
        Ok(file.write_all(content)?)
    }

    // FIXME: do we need to record statistics for read?
    pub fn read_disk_sync_all(&self, path: &str) -> StorageResult<(usize, Bytes)> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        let bytes_read = file.read_to_end(&mut buffer)?;
        Ok((bytes_read, Bytes::from(buffer)))
    }

    pub fn read_disk_sync(
        &self,
        path: &str,
        start_pos: u64,
        bytes_to_read: usize,
    ) -> StorageResult<(usize, Bytes)> {
        let mut file = File::open(path)?;
        file.seek(io::SeekFrom::Start(start_pos))?;

        let mut buffer = vec![0; bytes_to_read];
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        Ok((bytes_read, Bytes::from(buffer)))
    }

    // If needs to record statistics, use disk_read_sync_iterator, if not, please directly new DiskReadSyncIterator
    pub fn disk_read_sync_iterator(
        &self,
        path: &str,
        buffer_size: usize,
    ) -> StorageResult<DiskReadSyncIterator> {
        DiskReadSyncIterator::new(path, buffer_size)
    }

    // FIXME: `mut` allows future statistics computation
    // FIXME: disk_path should not exist, otherwise throw an error
    pub fn write_reader_to_disk_sync<T>(
        &mut self,
        mut iterator: T,
        disk_path: &str,
    ) -> StorageResult<usize>
    where
        T: StorageReaderIterator,
    {
        if Path::new(disk_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "disk file to write already exists",
            )
            .into());
        }
        let mut file = self.write_fd(disk_path, true)?;
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
        Ok(bytes_written)
    }

    pub fn file_size(&self, path: &str) -> StorageResult<u64> {
        let metadata = fs::metadata(path)?;
        Ok(metadata.len())
    }

    pub fn remove_file(&mut self, path: &str) -> StorageResult<()> {
        Ok(fs::remove_file(path)?)
    }
}

/// FIXME: iterator for sync, stream for async
pub struct DiskReadSyncIterator {
    f: File,
    pub buffer: BytesMut,
}

impl DiskReadSyncIterator {
    pub fn new(file_path: &str, buffer_size: usize) -> StorageResult<Self> {
        let file = File::open(file_path)?;

        Ok(DiskReadSyncIterator {
            f: file,
            buffer: BytesMut::zeroed(buffer_size),
        })
    }
}

impl StorageReaderIterator for DiskReadSyncIterator {
    fn next(&mut self) -> Option<StorageResult<usize>> {
        match self.f.read(self.buffer.as_mut()) {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    Some(Ok(bytes_read))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(datafusion::error::DataFusionError::IoError(e))),
        }
    }

    fn buffer(&self) -> &BytesMut {
        &self.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_simple_write_read_sync() {
        let mut disk_manager = DiskManager {};
        let path = "test_disk_manager1.txt";
        let content = "Hello, world!";
        disk_manager
            .write_disk_sync_all(path, content.as_bytes())
            .expect("write_disk_sync_all failed");
        let mut file = disk_manager.write_fd(path, true).expect("write_fd failed");
        file.write_all(content.as_bytes()).unwrap();

        let file_size = disk_manager.file_size(path).expect("file_size failed");
        assert_eq!(file_size, 2 * content.len() as u64);

        let (bytes_read, bytes) = disk_manager
            .read_disk_sync_all(path)
            .expect("read_disk_sync_all failed");
        assert_eq!(bytes_read, 2 * content.len());
        assert_eq!(bytes, Bytes::from(content.to_owned() + content));

        let (bytes_read, bytes) = disk_manager
            .read_disk_sync(path, content.len() as u64, content.len())
            .expect("read_disk_sync_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));

        disk_manager.remove_file(path).expect("remove_file failed");
        assert!(!Path::new(path).exists());
    }

    #[test]
    fn test_iterator_read() {
        let mut disk_manager = DiskManager {};
        let path = "test_disk_manager2.txt";
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_sync_all(path, content.as_bytes())
            .expect("write_disk_sync_all failed");
        let mut iterator = disk_manager
            .disk_read_sync_iterator(path, 2)
            .expect("disk_read_sync_iterator failed");
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

        disk_manager.remove_file(path).expect("remove_file failed");
        assert!(!Path::new(path).exists());
    }

    #[test]
    fn test_write_reader_to_disk_sync() {
        let mut disk_manager = DiskManager {};
        let path = "test_disk_manager3.txt";
        let content = "bhjoilkmnkbhaoijsdklmnjkbhiauosdjikbhjoilkmnkbhaoijsdklmnjkbhiauosdjik";
        disk_manager
            .write_disk_sync_all(path, content.as_bytes())
            .expect("write_disk_sync_all failed");
        let mut iterator = disk_manager
            .disk_read_sync_iterator(path, 1)
            .expect("disk_read_sync_iterator failed");
        let output_path = "test_disk_manager3_output.txt";
        let bytes_written = disk_manager
            .write_reader_to_disk_sync::<DiskReadSyncIterator>(iterator, output_path)
            .expect("write_reader_to_disk_sync failed");
        assert_eq!(bytes_written, content.len());

        let (bytes_read, bytes) = disk_manager
            .read_disk_sync_all(output_path)
            .expect("read_disk_sync_all failed");
        assert_eq!(bytes_read, content.len());
        assert_eq!(bytes, Bytes::from(content));
        let file_size = disk_manager
            .file_size(output_path)
            .expect("file_size failed");
        assert_eq!(file_size, content.len() as u64);

        disk_manager.remove_file(path).expect("remove_file failed");
        assert!(!Path::new(path).exists());
        disk_manager
            .remove_file(output_path)
            .expect("remove_file failed");
        assert!(!Path::new(output_path).exists());
    }
}
