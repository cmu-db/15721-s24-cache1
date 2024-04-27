use std::io::Read;

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

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use tempfile::tempdir;

    #[test]
    fn test_sqlite_blob_reader() {
        let poem = Bytes::from_static(
            b"What can I hold you with?
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
",
        );
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("tmp.db");

        let db = Connection::open(&db_path).unwrap();
        db.execute(
            &format!(
                "CREATE TABLE {} ({})",
                SQLITE_CACHE_TABLE_NAME, SQLITE_CACHE_COLUMN_NAME
            ),
            [],
        )
        .unwrap();
        db.execute(
            &format!(
                "INSERT INTO {} ({}) VALUES (ZEROBLOB({}))",
                SQLITE_CACHE_TABLE_NAME,
                SQLITE_CACHE_COLUMN_NAME,
                poem.len()
            ),
            [],
        )
        .unwrap();
        let blob_key = db.last_insert_rowid();

        {
            let mut writer = db
                .blob_open(
                    DatabaseName::Main,
                    SQLITE_CACHE_TABLE_NAME,
                    SQLITE_CACHE_COLUMN_NAME,
                    blob_key,
                    false,
                )
                .unwrap();
            writer.write_at(&poem, 0).unwrap();
        }
        // FLush the result so that the blob is visible to another connnection.
        db.cache_flush().unwrap();

        let db2 = Connection::open(&db_path).unwrap();
        let buffer_size = 100;
        let mut reader = SqliteBlobReader::new(&db2, blob_key, buffer_size).unwrap();

        let mut total_bytes_read = 0;
        let mut read_count = 0;
        let mut result = String::new();
        while let Some(bytes_read) = reader.next() {
            let bytes_read = bytes_read.unwrap();
            println!("bytes_read: {}", bytes_read);
            println!("buffer: {:?}", reader.buffer()[..bytes_read].to_vec());
            result += &String::from_utf8(reader.buffer()[..bytes_read].to_vec()).unwrap();
            total_bytes_read += bytes_read;
            read_count += 1;
        }

        assert_eq!(result, poem);
        assert_eq!(total_bytes_read, 930);
        assert_eq!(read_count, 10);
    }
}
