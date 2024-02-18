use datafusion::execution::DiskManager;

use crate::StorageResult;

pub struct LocalFsReader {
    file_path: String,
}

impl LocalFsReader {
    pub async fn read(&self) -> StorageResult<()> {
        todo!()
    }
}
