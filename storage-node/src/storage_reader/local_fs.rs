use datafusion::execution::DiskManager;

use crate::error::ParpulseResult;

pub struct LocalFsReader {
    file_path: String,
}

impl LocalFsReader {
    pub async fn read(&self) -> ParpulseResult<()> {
        todo!()
    }
}
