use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

use crate::error::ParpulseResult;

const DEFAULT_MEM_CHANNEL_BUFFER_SIZE: usize = 1024;

pub struct MemStore {
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
        let (data_vec, _) = self.data.get(key).unwrap().clone();
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_MEM_CHANNEL_BUFFER_SIZE);
        tokio::spawn(async move {
            for data in data_vec.iter() {
                tx.send(Ok(data.clone())).await.unwrap();
            }
        });
        Ok(Some(rx))
    }

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

    pub fn data_store_key(&self, remote_location: &str) -> String {
        remote_location.to_string()
    }
}
