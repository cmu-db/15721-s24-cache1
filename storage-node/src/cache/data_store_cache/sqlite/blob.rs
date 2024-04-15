use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::Stream;
use rusqlite::Connection;

pub struct SqliteBlobReader {
    db: Connection,
    blob_key: String,
}

impl SqliteBlobReader {
    pub fn new(db: Connection, blob_key: String) -> Self {
        Self { db, blob_key }
    }
}

impl Stream for SqliteBlobReader {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
