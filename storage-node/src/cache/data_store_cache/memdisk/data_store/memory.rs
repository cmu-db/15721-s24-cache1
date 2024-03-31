use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

use crate::error::ParpulseResult;

const DEFAULT_MEM_CHANNEL_BUFFER_SIZE: usize = 1024;

pub struct MemStore {
    /// data: remote_location -> (data, size)
    data: HashMap<String, (Vec<Bytes>, usize)>,
    max_file_size: usize,
}

impl MemStore {
    pub fn new(max_file_size: usize) -> Self {
        Self {
            data: HashMap::new(),
            max_file_size,
        }
    }

    pub fn read_data(&self, key: &str) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        let key_value = self.data.get(key);
        if key_value.is_none() {
            return Ok(None);
        }
        let data_vec = key_value.unwrap().0.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_MEM_CHANNEL_BUFFER_SIZE);
        tokio::spawn(async move {
            for data in data_vec.iter() {
                tx.send(Ok(data.clone())).await.unwrap();
            }
        });
        Ok(Some(rx))
    }

    /// Writes data to the memory store, also tracks the size. If the size for one key is too large,
    /// we will delete the data from the memory store and return all the data to the caller.
    /// If return value is None, it means successful write. Otherwise, it means unsuccessful write.
    /// TODO(lanlou): the key type should be &str maybe?
    pub fn write_data(&mut self, key: String, bytes: Bytes) -> Option<(Vec<Bytes>, usize)> {
        let (bytes_vec, size) = self.data.entry(key.clone()).or_insert((Vec::new(), 0));
        *size += bytes.len();
        bytes_vec.push(bytes);
        if *size > self.max_file_size {
            let size_copy = *size;
            let bytes_vec_copy = bytes_vec.clone();
            self.data.remove(&key);
            Some((bytes_vec_copy, size_copy))
        } else {
            None
        }
    }

    pub fn clean_data(&mut self, key: &str) -> Option<(Vec<Bytes>, usize)> {
        self.data.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_large_write() {
        // max_file_size is 10 bytes per file.
        let max_file_size = 10;
        let mut mem_store = MemStore::new(max_file_size);
        let key = "large_write_key".to_string();

        let bytes1 = Bytes::from(vec![1, 2, 3, 4]);
        let bytes2 = Bytes::from(vec![5, 6, 7, 8]);
        let bytes3 = Bytes::from(vec![9, 10, 11, 12]);

        let bytes1_cp = bytes1.clone();
        let bytes2_cp = bytes2.clone();
        let bytes3_cp = bytes3.clone();

        let res1 = mem_store.write_data(key.clone(), bytes1);
        assert!(res1.is_none());
        let res2 = mem_store.write_data(key.clone(), bytes2);
        assert!(res2.is_none());
        let res3 = mem_store.write_data(key.clone(), bytes3);
        assert!(res3.is_some());
        assert_eq!(res3.as_ref().unwrap().0.len(), 3);
        assert_eq!(res3.as_ref().unwrap().1, 12);
        assert_eq!(res3.as_ref().unwrap().0[0], bytes1_cp);
        assert_eq!(res3.as_ref().unwrap().0[1], bytes2_cp);
        assert_eq!(res3.as_ref().unwrap().0[2], bytes3_cp);

        let read_res = mem_store.read_data(key.as_str());
        assert!(read_res.is_ok());
        assert!(read_res.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_write_read() {
        let max_file_size = 10;
        let mut mem_store = MemStore::new(max_file_size);
        let key = "write_read_key".to_string();
        let bytes = Bytes::from(vec![1, 2, 3, 4]);
        let bytes_cp = bytes.clone();
        let res = mem_store.write_data(key.clone(), bytes);
        assert!(res.is_none());
        let read_res = mem_store.read_data(key.as_str());
        assert!(read_res.is_ok());
        let mut rx = read_res.unwrap().unwrap();
        let mut bytes_vec = Vec::new();
        let mut data_size: usize = 0;
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            data_size += data.len();
            bytes_vec.push(data);
        }
        assert_eq!(bytes_vec.len(), 1);
        assert_eq!(bytes_vec[0], bytes_cp);
        assert_eq!(data_size, bytes_cp.len());
    }
}
