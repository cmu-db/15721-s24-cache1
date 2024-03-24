use bytes::{Bytes, BytesMut};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

use crate::error::ParpulseResult;

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
}

pub struct DiskReadStream {
    f: File,
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
