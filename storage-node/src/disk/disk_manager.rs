use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::disk_stream::DiskReadSyncIterator;

/// [`DiskManager`] contains the common logic to read from or write to a disk.
///
/// FIXME: We should store base path in cache
/// TODO: add some statistics member in DiskManager
pub struct DiskManager {}

impl DiskManager {
    pub fn get_write_file(&self, path: &str, append: bool) -> io::Result<File> {
        let path_buf: PathBuf = PathBuf::from(path);
        if let Some(parent) = path_buf.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        OpenOptions::new()
            .write(true)
            .append(append)
            .open(&path_buf)
    }

    // FIXME: `mut` allows future statistics computation
    // Do not call this method in a loop, otherwise everytime you have to repoen the file
    // When you want to write to disk in a loop due to huge size of file, please first
    // call get_write_file, and use `file.write_all` directly in a loop.
    // But we need to record statistics... so `disk_manager` should provide this method...
    // Maybe something like `DiskWriteSyncIterator` is needed... It's weird though

    // TODO: only practical when you want to write data all at once, will be modified
    // TODO: the ideal situation is directly read from S3, directly write to cache,
    //       without memory involved, if no memory cache
    //       so after determining the S3 read func, `disk_manager` can provide such method
    pub fn write_disk_sync_all(&mut self, path: &str, content: &[u8]) -> io::Result<()> {
        let mut file = self.get_write_file(path, false)?;
        file.write_all(content)
    }

    // FIXME: another option is using &[u8], less convenient but faster maybe?
    // TODO: the ideal situation is directly read from disk, directly write to network,
    //       without memory involved, if no memory cache
    pub fn read_disk_sync_all(&mut self, path: &str, buffer: &mut Vec<u8>) -> io::Result<usize> {
        let mut file = File::open(path)?;
        file.read_to_end(buffer)
    }

    // FIXME: buffer is stored in `DiskReadSyncIterator`, every time call `next`, the buffer content will update
    // TODO: maybe we can add a statistics member(from `DiskManager`) with lifetime in `DiskReadSyncIterator` to record?
    pub fn read_disk_sync_batch(
        &mut self,
        path: &str,
        buffer_size: usize,
    ) -> io::Result<DiskReadSyncIterator> {
        DiskReadSyncIterator::new(path, buffer_size)
    }

    pub fn get_file_size(&self, path: &str) -> io::Result<u64> {
        let metadata = fs::metadata(path)?;
        Ok(metadata.len())
    }

    pub fn remove_file(&mut self, path: &str) -> io::Result<()> {
        fs::remove_file(path)
    }
}
