use crate::StorageResult;

pub struct DiskReader {
    file_path: String,
}

impl DiskReader {
    pub async fn read(&self) -> StorageResult<()> {
        todo!()
    }
}
