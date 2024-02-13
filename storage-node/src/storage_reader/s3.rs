use bytes::Bytes;

use crate::StorageResult;

pub struct S3Reader {
    bucket_path: String,
}

impl S3Reader {
    pub async fn read(&self) -> StorageResult<()> {
        todo!()
    }
}
