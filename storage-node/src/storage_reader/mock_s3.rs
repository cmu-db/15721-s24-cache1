use std::time::Duration;

use crate::StorageResult;

pub struct MockS3Reader {
    file_path: String,
    delay: Option<Duration>,
}

impl MockS3Reader {
    pub async fn read(&self) -> StorageResult<()> {
        todo!()
    }
}
