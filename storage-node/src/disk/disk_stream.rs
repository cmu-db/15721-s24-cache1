use std::fs::File;
use std::io::{self, Read};
use std::iter::Iterator;

/// FIXME: iterator for sync, stream for async
/// Do we really need iterator? Stream can solve anything I think...
/// Also, do we really need sync? async can also replace sync maybe??
pub struct DiskReadSyncIterator {
    f: File,
    pub buffer: Vec<u8>,
}

impl DiskReadSyncIterator {
    pub fn new(file_path: &str, buffer_size: usize) -> io::Result<Self> {
        let file = File::open(file_path)?;

        Ok(DiskReadSyncIterator {
            f: file,
            buffer: vec![0; buffer_size],
        })
    }
}

impl Iterator for DiskReadSyncIterator {
    type Item = io::Result<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.f.read(&mut self.buffer) {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    Some(Ok(bytes_read))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}
