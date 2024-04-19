use std::{io::Read, sync::Mutex};

use bytes::BytesMut;
use rusqlite::{blob::Blob, Connection, DatabaseName};

use crate::error::ParpulseResult;

use super::{SQLITE_CACHE_COLUMN_NAME, SQLITE_CACHE_TABLE_NAME};

pub type SqliteBlobKey = i64;

pub struct SqliteBlob<'a> {
    blob: Blob<'a>,
}

impl<'a> SqliteBlob<'a> {
    pub fn new(blob: Blob<'a>) -> Self {
        Self { blob }
    }

    pub fn read(&mut self, buffer: &mut [u8]) -> ParpulseResult<usize> {
        self.blob.read(buffer).map_err(Into::into)
    }

    pub fn write_at(&mut self, data: &[u8], offset: usize) -> ParpulseResult<()> {
        self.blob.write_at(data, offset).map_err(Into::into)
    }
}

unsafe impl<'a> Send for SqliteBlob<'a> {}

pub struct SqliteBlobReader<'a> {
    blob: SqliteBlob<'a>,
    buffer: BytesMut,
}

impl<'a> SqliteBlobReader<'a> {
    pub fn new(
        db: &'a Connection,
        blob_key: SqliteBlobKey,
        buffer_size: usize,
    ) -> ParpulseResult<Self> {
        let blob = db.blob_open(
            DatabaseName::Main,
            SQLITE_CACHE_TABLE_NAME,
            SQLITE_CACHE_COLUMN_NAME,
            blob_key,
            true,
        )?;
        Ok(Self {
            blob: SqliteBlob::new(blob),
            buffer: BytesMut::zeroed(buffer_size),
        })
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

impl Iterator for SqliteBlobReader<'_> {
    type Item = ParpulseResult<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.blob.read(self.buffer.as_mut()) {
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
