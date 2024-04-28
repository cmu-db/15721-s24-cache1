pub mod data_store;

use std::{collections::HashMap, sync::Arc};

use futures::stream::StreamExt;
use log::warn;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    cache::{
        data_store_cache::DataStoreCache,
        replacer::{DataStoreReplacer, ReplacerValue},
    },
    disk::disk_manager::DiskManager,
    error::{ParpulseError, ParpulseResult},
    storage_reader::StorageReaderStream,
};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;

use self::data_store::{disk::DiskStore, memory::MemStore};

/// The default maximum single file size for the memory cache.
/// If the file size exceeds this value, the file will be stored in the disk cache.
/// TODO(lanlou): make this value configurable.
pub const DEFAULT_MEM_CACHE_MAX_FILE_SIZE: usize = 1024 * 512;

/// [`MemDiskStoreReplacerKey`] is a path to the remote object store.
pub type MemDiskStoreReplacerKey = String;

type StatusKeyHashMap = HashMap<String, ((Status, usize), Arc<Notify>)>;

pub struct MemDiskStoreReplacerValue {
    /// The path to the data store. For mem store, it should be data's s3 path. For disk
    /// store, it should be cached data's disk path.
    pub(crate) path: String,
    /// The size of the cached item.
    pub(crate) size: usize,
}

impl MemDiskStoreReplacerValue {
    pub fn new(path: String, size: usize) -> Self {
        MemDiskStoreReplacerValue { path, size }
    }
}

impl ReplacerValue for MemDiskStoreReplacerValue {
    type Value = String;

    fn into_value(self) -> Self::Value {
        self.path
    }

    fn as_value(&self) -> &Self::Value {
        &self.path
    }

    fn size(&self) -> usize {
        self.size
    }
}

#[derive(Clone)]
enum Status {
    Incompleted,
    Completed,
}

pub struct MemDiskStoreCache<
    R: DataStoreReplacer<MemDiskStoreReplacerKey, MemDiskStoreReplacerValue>,
> {
    disk_store: DiskStore,
    mem_store: Option<RwLock<MemStore>>,
    /// Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    disk_replacer: Mutex<R>,
    mem_replacer: Option<Mutex<R>>,
    // MemDiskStoreReplacerKey -> remote_location
    // status: 0 -> incompleted; 1 -> completed; 2 -> failed
    // TODO(lanlou): we should clean this hashmap.
    status_of_keys: RwLock<StatusKeyHashMap>,
    mem_store_max_size: Option<usize>,
}

/// This method creates a `MemDiskStoreCache` instance, and memory can be disabled by setting
/// `mem_replacer` to `None`.
/// If one file is larger than `mem_max_file_size`, we will not write it into memory. And all the files
/// in the memory cannot exceed the `maximum_capacity` of the memory replacer.
/// Ideally, mem_max_file_size should always be less than or equal to the maximum capacity of the memory.
impl<R: DataStoreReplacer<MemDiskStoreReplacerKey, MemDiskStoreReplacerValue>>
    MemDiskStoreCache<R>
{
    pub fn new(
        disk_replacer: R,
        disk_base_path: String,
        mem_replacer: Option<R>,
        mem_max_file_size: Option<usize>,
    ) -> Self {
        let disk_manager = DiskManager::default();
        let disk_store = DiskStore::new(disk_manager, disk_base_path);

        if mem_replacer.is_some() {
            let mut mem_max_file_size =
                mem_max_file_size.unwrap_or(DEFAULT_MEM_CACHE_MAX_FILE_SIZE);
            let replacer_max_capacity = mem_replacer.as_ref().unwrap().max_capacity();
            if mem_max_file_size > replacer_max_capacity {
                warn!("The maximum file size > replacer's max capacity, so we set maximum file size = 1/5 of the maximum capacity.");
                // By default in this case, replacer can store at least 5 files.
                mem_max_file_size = replacer_max_capacity / 5;
            }

            let mem_store = MemStore::new(mem_max_file_size);
            let mem_store_max_size = mem_store.max_file_size;
            MemDiskStoreCache {
                disk_store,
                mem_store: Some(RwLock::new(mem_store)),
                disk_replacer: Mutex::new(disk_replacer),
                mem_replacer: Some(Mutex::new(mem_replacer.unwrap())),
                status_of_keys: RwLock::new(HashMap::new()),
                mem_store_max_size: Some(mem_store_max_size),
            }
        } else {
            MemDiskStoreCache {
                disk_store,
                mem_store: None,
                disk_replacer: Mutex::new(disk_replacer),
                mem_replacer: None,
                status_of_keys: RwLock::new(HashMap::new()),
                mem_store_max_size: None,
            }
        }
    }
}

#[async_trait]
impl<R: DataStoreReplacer<MemDiskStoreReplacerKey, MemDiskStoreReplacerValue>> DataStoreCache
    for MemDiskStoreCache<R>
{
    async fn get_data_from_cache(
        &self,
        remote_location: String,
    ) -> ParpulseResult<Option<Receiver<ParpulseResult<Bytes>>>> {
        // TODO: Refine the lock.
        if let Some(mem_store) = &self.mem_store {
            let mut mem_replacer = self.mem_replacer.as_ref().unwrap().lock().await;
            if let Some(replacer_value) = mem_replacer.get(&remote_location) {
                let data_store_key = replacer_value.as_value();
                match mem_store.read().await.read_data(data_store_key) {
                    Ok(Some(rx)) => return Ok(Some(rx)),
                    Ok(None) => {
                        return Err(ParpulseError::Internal(
                            "Memory replacer and memory store keys are inconsistent.".to_string(),
                        ))
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        let mut disk_replacer = self.disk_replacer.lock().await;
        match disk_replacer.get(&remote_location) {
            Some(replacer_value) => {
                let data_store_key = replacer_value.as_value();
                if let Some(data) = self.disk_store.read_data(data_store_key).await? {
                    Ok(Some(data))
                } else {
                    unreachable!()
                }
            }
            None => Ok(None),
        }
    }

    async fn put_data_to_cache(
        &self,
        remote_location: String,
        data_size: Option<usize>,
        mut data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        {
            // If in_progress of remote_location thread fails, it will clean the data from this hash map.
            let mut existed = false;
            loop {
                let (status, size, notify);
                if let Some(((status_ref, size_ref), notify_ref)) =
                    self.status_of_keys.read().await.get(&remote_location)
                {
                    existed = true;
                    status = status_ref.clone();
                    size = *size_ref;
                    notify = notify_ref.clone();
                } else {
                    break;
                }
                match status {
                    Status::Incompleted => {
                        notify.notified().await;
                    }
                    Status::Completed => {
                        return Ok(size);
                    }
                }
            }
            if existed {
                // Another in progress of remote_location thread fails
                return Err(ParpulseError::Internal(
                    "Put_data_to_cache fails".to_string(),
                ));
            }
            self.status_of_keys.write().await.insert(
                remote_location.clone(),
                ((Status::Incompleted, 0), Arc::new(Notify::new())),
            );
        }

        if data_size.is_none() {
            // TODO: Refine the lock.
            // TODO(lanlou): Also write the data to network.
            let mut bytes_to_disk = None;
            let mut bytes_mem_written = 0;
            let mut evicted_bytes_to_disk: Option<
                Vec<(MemDiskStoreReplacerKey, (Vec<Bytes>, usize))>,
            > = None;
            // 1. If the mem_store is enabled, first try to write the data to memory.
            // Note: Only file which size < mem_max_file_size can be written to memory.
            if let Some(mem_store) = &self.mem_store {
                loop {
                    match data_stream.next().await {
                        Some(Ok(bytes)) => {
                            bytes_mem_written += bytes.len();
                            // TODO: Need to write the data to network.
                            if let Some((bytes_vec, _)) = mem_store
                                .write()
                                .await
                                .write_data(remote_location.clone(), bytes)
                            {
                                // If write_data returns something, it means the file size is too large
                                // to fit in the memory. We should put it to disk cache.
                                bytes_to_disk = Some(bytes_vec);
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            // TODO(lanlou): Every time it returns an error, I need to manually add this clean code...
                            // How to improve?
                            self.status_of_keys.write().await.remove(&remote_location);
                            return Err(e);
                        }
                        None => break,
                    }
                }

                if bytes_to_disk.is_none() {
                    // If bytes_to_disk has nothing, it means the data has been successfully written to memory.
                    // We have to put it into mem_replacer too.
                    let mut mem_replacer = self.mem_replacer.as_ref().unwrap().lock().await;
                    let replacer_put_status = mem_replacer.put(
                        remote_location.clone(),
                        MemDiskStoreReplacerValue::new(remote_location.clone(), bytes_mem_written),
                    );
                    // Insertion fails.
                    if replacer_put_status.is_none() {
                        // Put the data to disk cache.
                        bytes_to_disk = Some(
                            mem_store
                                .write()
                                .await
                                .clean_data(&remote_location)
                                .unwrap()
                                .0,
                        );
                    } else {
                        // If successfully putting it into mem_replacer, we should record the evicted data,
                        // delete them from mem_store, and put all of them to disk cache.
                        if let Some(evicted_keys) = replacer_put_status {
                            let mut evicted_bytes_to_disk_inner = Vec::new();
                            for evicted_key in evicted_keys {
                                evicted_bytes_to_disk_inner.push((
                                    evicted_key.clone(),
                                    mem_store.write().await.clean_data(&evicted_key).unwrap(),
                                ));
                            }
                            evicted_bytes_to_disk = Some(evicted_bytes_to_disk_inner);
                        }
                    }
                }
            }

            // 2. If applicable, put all the evicted data into disk cache.
            // Don't need to write the data to network for evicted keys.
            if let Some(evicted_bytes_to_disk) = evicted_bytes_to_disk {
                for (remote_location_evicted, (bytes_vec, data_size)) in evicted_bytes_to_disk {
                    assert_ne!(remote_location_evicted, remote_location);
                    let disk_store_key = self.disk_store.data_store_key(&remote_location_evicted);
                    self.disk_store
                        .write_data(disk_store_key.clone(), Some(bytes_vec), None)
                        .await?;
                    let mut disk_replacer = self.disk_replacer.lock().await;
                    if disk_replacer
                        .put(
                            remote_location_evicted,
                            MemDiskStoreReplacerValue::new(disk_store_key.clone(), data_size),
                        )
                        .is_none()
                    {
                        if let Err(e) = self.disk_store.clean_data(&disk_store_key).await {
                            warn!(
                                "Failed to clean data ({}) from disk store: {}",
                                disk_store_key, e
                            );
                        }
                        warn!(
                            "Failed to put evicted data ({}) to disk replacer.",
                            disk_store_key
                        );
                    }
                }
            }

            // 3. If the data is successfully written to memory, directly return.
            if self.mem_store.is_some() && bytes_to_disk.is_none() {
                let mut status_of_keys = self.status_of_keys.write().await;
                let ((status, size), notify) = status_of_keys.get_mut(&remote_location).unwrap();
                *status = Status::Completed;
                *size = bytes_mem_written;
                notify.notify_waiters();
                return Ok(bytes_mem_written);
            }

            // 4. If the data is not written to memory_cache successfully, then cache it to disk.
            // Need to write the data to network for the current key.
            let disk_store_key = self.disk_store.data_store_key(&remote_location);
            // TODO: Write the data store cache first w/ `incompleted` state and update the state
            // after finishing writing into the data store.
            let data_size_wrap = self
                .disk_store
                .write_data(disk_store_key.clone(), bytes_to_disk, Some(data_stream))
                .await;
            if let Err(e) = data_size_wrap {
                self.status_of_keys.write().await.remove(&remote_location);
                return Err(e);
            }
            let data_size = data_size_wrap.unwrap();
            let mut disk_replacer = self.disk_replacer.lock().await;
            if disk_replacer
                .put(
                    remote_location.clone(),
                    MemDiskStoreReplacerValue::new(disk_store_key.clone(), data_size),
                )
                .is_none()
            {
                if let Err(e) = self.disk_store.clean_data(&disk_store_key).await {
                    // TODO: do we need to notify the caller this failure?
                    warn!(
                        "Failed to clean data ({}) from disk store: {}",
                        disk_store_key, e
                    );
                }
                self.status_of_keys.write().await.remove(&remote_location);
                return Err(ParpulseError::Internal(
                    "Failed to put data to disk replacer.".to_string(),
                ));
            }

            let mut status_of_keys = self.status_of_keys.write().await;
            let ((status, size), notify) = status_of_keys.get_mut(&remote_location).unwrap();
            *status = Status::Completed;
            *size = bytes_mem_written;
            notify.notify_waiters();
            Ok(data_size)
        } else {
            let mut bytes_to_disk = None;
            let mut evicted_bytes_to_disk: Option<
                Vec<(MemDiskStoreReplacerKey, (Vec<Bytes>, usize))>,
            > = None;
            let data_size = data_size.unwrap();
            if let Some(mem_store) = &self.mem_store {
                if data_size < self.mem_store_max_size.unwrap() {
                    let mut bytes_mem_written = 0;
                    loop {
                        match data_stream.next().await {
                            Some(Ok(bytes)) => {
                                bytes_mem_written += bytes.len();
                                // TODO: Need to write the data to network.
                                if let Some(_) = mem_store
                                    .write()
                                    .await
                                    .write_data(remote_location.clone(), bytes)
                                {
                                    unreachable!();
                                }
                            }
                            Some(Err(e)) => {
                                self.status_of_keys.write().await.remove(&remote_location);
                                return Err(e);
                            }
                            None => break,
                        }
                    }
                    // If bytes_to_disk has nothing, it means the data has been successfully written to memory.
                    // We have to put it into mem_replacer too.
                    let mut mem_replacer = self.mem_replacer.as_ref().unwrap().lock().await;
                    let replacer_put_status = mem_replacer.put(
                        remote_location.clone(),
                        MemDiskStoreReplacerValue::new(remote_location.clone(), bytes_mem_written),
                    );
                    // Insertion fails.
                    if replacer_put_status.is_none() {
                        // Put the data to disk cache.
                        bytes_to_disk = Some(
                            mem_store
                                .write()
                                .await
                                .clean_data(&remote_location)
                                .unwrap()
                                .0,
                        );
                    } else {
                        // If successfully putting it into mem_replacer, we should record the evicted data,
                        // delete them from mem_store, and put all of them to disk cache.
                        if let Some(evicted_keys) = replacer_put_status {
                            let mut evicted_bytes_to_disk_inner = Vec::new();
                            for evicted_key in evicted_keys {
                                evicted_bytes_to_disk_inner.push((
                                    evicted_key.clone(),
                                    mem_store.write().await.clean_data(&evicted_key).unwrap(),
                                ));
                            }
                            evicted_bytes_to_disk = Some(evicted_bytes_to_disk_inner);
                        }
                    }
                    if let Some(evicted_bytes_to_disk) = evicted_bytes_to_disk {
                        for (remote_location_evicted, (bytes_vec, data_size)) in
                            evicted_bytes_to_disk
                        {
                            assert_ne!(remote_location_evicted, remote_location);
                            let disk_store_key =
                                self.disk_store.data_store_key(&remote_location_evicted);
                            self.disk_store
                                .write_data(disk_store_key.clone(), Some(bytes_vec), None)
                                .await?;
                            let mut disk_replacer = self.disk_replacer.lock().await;
                            if disk_replacer
                                .put(
                                    remote_location_evicted,
                                    MemDiskStoreReplacerValue::new(
                                        disk_store_key.clone(),
                                        data_size,
                                    ),
                                )
                                .is_none()
                            {
                                if let Err(e) = self.disk_store.clean_data(&disk_store_key).await {
                                    warn!(
                                        "Failed to clean data ({}) from disk store: {}",
                                        disk_store_key, e
                                    );
                                }
                                warn!(
                                    "Failed to put evicted data ({}) to disk replacer.",
                                    disk_store_key
                                );
                            }
                        }
                    }
                    if bytes_to_disk.is_none() {
                        let mut status_of_keys = self.status_of_keys.write().await;
                        let ((status, size), notify) =
                            status_of_keys.get_mut(&remote_location).unwrap();
                        *status = Status::Completed;
                        *size = bytes_mem_written;
                        notify.notify_waiters();
                        return Ok(bytes_mem_written);
                    }
                }
            }

            let disk_store_key = self.disk_store.data_store_key(&remote_location);
            // TODO: Write the data store cache first w/ `incompleted` state and update the state
            // after finishing writing into the data store.
            let data_size_wrap = self
                .disk_store
                .write_data(disk_store_key.clone(), bytes_to_disk, Some(data_stream))
                .await;
            if let Err(e) = data_size_wrap {
                self.status_of_keys.write().await.remove(&remote_location);
                return Err(e);
            }
            let data_size = data_size_wrap.unwrap();
            let mut disk_replacer = self.disk_replacer.lock().await;
            if disk_replacer
                .put(
                    remote_location.clone(),
                    MemDiskStoreReplacerValue::new(disk_store_key.clone(), data_size),
                )
                .is_none()
            {
                if let Err(e) = self.disk_store.clean_data(&disk_store_key).await {
                    // TODO: do we need to notify the caller this failure?
                    warn!(
                        "Failed to clean data ({}) from disk store: {}",
                        disk_store_key, e
                    );
                }
                self.status_of_keys.write().await.remove(&remote_location);
                return Err(ParpulseError::Internal(
                    "Failed to put data to disk replacer.".to_string(),
                ));
            }

            let mut status_of_keys = self.status_of_keys.write().await;
            let ((status, size), notify) = status_of_keys.get_mut(&remote_location).unwrap();
            *status = Status::Completed;
            *size = data_size;
            notify.notify_waiters();
            Ok(data_size)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cache::replacer::lru::LruReplacer,
        storage_reader::{s3_diskmock::MockS3Reader, AsyncStorageReader},
    };
    use futures::join;

    use super::*;
    #[tokio::test]
    async fn test_put_data_evicted_from_mem() {
        // 1. put small data1
        // 2. put small data2, making data1 evicted
        //    (TODO(lanlou): when adding pin & unpin, this test should be modified)
        // 3. get data1, should be from disk
        // 4. get data2, should be from memory
        // TODO(lanlou): What if we continue accessing data1 and never accessing data2 in
        // the future? There is no chance for data1 to be put back to memory again currently.
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(950)),
            None,
        );
        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let data1_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let data_stream = reader.into_stream().await.unwrap();
        let written_data_size = cache
            .put_data_to_cache(data1_key.clone(), None, data_stream)
            .await
            .unwrap();
        assert_eq!(written_data_size, 930);

        let data2_key = bucket.clone() + &keys[0] + "1";
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let mut written_data_size = cache
            .put_data_to_cache(data2_key.clone(), None, reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(written_data_size, 930);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data1_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 930);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data2_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 930);
    }

    #[tokio::test]
    async fn test_put_get_disk_only() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            None,
            None,
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let written_data_size = cache
            .put_data_to_cache(
                remote_location.clone(),
                None,
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 113629);

        let keys = vec!["userdata2.parquet".to_string()];
        let remote_location2 = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket, keys).await;
        let mut written_data_size = cache
            .put_data_to_cache(
                remote_location2.clone(),
                None,
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 112193);

        let mut rx = cache
            .get_data_from_cache(remote_location)
            .await
            .unwrap()
            .unwrap();
        written_data_size = 0;
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 113629);
    }

    #[tokio::test]
    async fn test_put_get_mem_disk() {
        // 1. put a small file (-> memory)
        // 2. put a large file (-> disk)
        // 3. get the small file
        // 4. get the large file
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(950)),
            Some(950),
        );
        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let data1_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let written_data_size = cache
            .put_data_to_cache(data1_key.clone(), None, reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(written_data_size, 930);

        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let data2_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let mut written_data_size = cache
            .put_data_to_cache(data2_key.clone(), None, reader.into_stream().await.unwrap())
            .await
            .unwrap();
        assert_eq!(written_data_size, 113629);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data1_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 930);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data2_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 113629);
    }

    #[tokio::test]
    async fn test_same_requests_simultaneously_mem() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(120000)),
            Some(120000),
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader2 = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader3 = MockS3Reader::new(bucket.clone(), keys).await;

        let put_data_fut_1 = cache.put_data_to_cache(
            remote_location.clone(),
            None,
            reader.into_stream().await.unwrap(),
        );
        let put_data_fut_2 = cache.put_data_to_cache(
            remote_location.clone(),
            None,
            reader2.into_stream().await.unwrap(),
        );
        let put_data_fut_3 = cache.put_data_to_cache(
            remote_location.clone(),
            None,
            reader3.into_stream().await.unwrap(),
        );

        let res = join!(put_data_fut_1, put_data_fut_2, put_data_fut_3);
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());
        assert_eq!(res.0.unwrap(), 113629);
        assert_eq!(res.1.unwrap(), 113629);
        assert_eq!(res.2.unwrap(), 113629);
    }

    #[tokio::test]
    async fn test_same_requests_simultaneously_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(20)),
            Some(20),
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata2.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader2 = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader3 = MockS3Reader::new(bucket.clone(), keys).await;

        let put_data_fut_1 = cache.put_data_to_cache(
            remote_location.clone(),
            None,
            reader.into_stream().await.unwrap(),
        );
        let put_data_fut_2 = cache.put_data_to_cache(
            remote_location.clone(),
            None,
            reader2.into_stream().await.unwrap(),
        );
        let put_data_fut_3 = cache.put_data_to_cache(
            remote_location.clone(),
            None,
            reader3.into_stream().await.unwrap(),
        );

        let res = join!(put_data_fut_1, put_data_fut_2, put_data_fut_3);
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());
        assert_eq!(res.0.unwrap(), 112193);
        assert_eq!(res.1.unwrap(), 112193);
        assert_eq!(res.2.unwrap(), 112193);
    }

    #[tokio::test]
    async fn test_put_data_evicted_from_mem_size() {
        // 1. put small data1
        // 2. put small data2, making data1 evicted
        //    (TODO(lanlou): when adding pin & unpin, this test should be modified)
        // 3. get data1, should be from disk
        // 4. get data2, should be from memory
        // TODO(lanlou): What if we continue accessing data1 and never accessing data2 in
        // the future? There is no chance for data1 to be put back to memory again currently.
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(950)),
            None,
        );
        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let data1_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let data_stream = reader.into_stream().await.unwrap();
        let written_data_size = cache
            .put_data_to_cache(data1_key.clone(), Some(930), data_stream)
            .await
            .unwrap();
        assert_eq!(written_data_size, 930);

        let data2_key = bucket.clone() + &keys[0] + "1";
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let mut written_data_size = cache
            .put_data_to_cache(
                data2_key.clone(),
                Some(930),
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 930);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data1_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 930);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data2_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 930);
    }

    #[tokio::test]
    async fn test_put_get_disk_only_size() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            None,
            None,
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let written_data_size = cache
            .put_data_to_cache(
                remote_location.clone(),
                Some(113629),
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 113629);

        let keys = vec!["userdata2.parquet".to_string()];
        let remote_location2 = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket, keys).await;
        let mut written_data_size = cache
            .put_data_to_cache(
                remote_location2.clone(),
                Some(112193),
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 112193);

        let mut rx = cache
            .get_data_from_cache(remote_location)
            .await
            .unwrap()
            .unwrap();
        written_data_size = 0;
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 113629);
    }

    #[tokio::test]
    async fn test_put_get_mem_disk_size() {
        // 1. put a small file (-> memory)
        // 2. put a large file (-> disk)
        // 3. get the small file
        // 4. get the large file
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(950)),
            Some(950),
        );
        let bucket = "tests-text".to_string();
        let keys = vec!["what-can-i-hold-you-with".to_string()];
        let data1_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let written_data_size = cache
            .put_data_to_cache(
                data1_key.clone(),
                Some(930),
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 930);

        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let data2_key = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys).await;
        let mut written_data_size = cache
            .put_data_to_cache(
                data2_key.clone(),
                Some(113629),
                reader.into_stream().await.unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written_data_size, 113629);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data1_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 930);

        written_data_size = 0;
        let mut rx = cache.get_data_from_cache(data2_key).await.unwrap().unwrap();
        while let Some(data) = rx.recv().await {
            let data = data.unwrap();
            written_data_size += data.len();
        }
        assert_eq!(written_data_size, 113629);
    }

    #[tokio::test]
    async fn test_same_requests_simultaneously_mem_size() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(120000)),
            Some(120000),
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader2 = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader3 = MockS3Reader::new(bucket.clone(), keys).await;

        let put_data_fut_1 = cache.put_data_to_cache(
            remote_location.clone(),
            Some(113629),
            reader.into_stream().await.unwrap(),
        );
        let put_data_fut_2 = cache.put_data_to_cache(
            remote_location.clone(),
            Some(113629),
            reader2.into_stream().await.unwrap(),
        );
        let put_data_fut_3 = cache.put_data_to_cache(
            remote_location.clone(),
            Some(113629),
            reader3.into_stream().await.unwrap(),
        );

        let res = join!(put_data_fut_1, put_data_fut_2, put_data_fut_3);
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());
        assert_eq!(res.0.unwrap(), 113629);
        assert_eq!(res.1.unwrap(), 113629);
        assert_eq!(res.2.unwrap(), 113629);
    }

    #[tokio::test]
    async fn test_same_requests_simultaneously_disk_size() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(20)),
            Some(20),
        );
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata2.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader2 = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader3 = MockS3Reader::new(bucket.clone(), keys).await;

        let put_data_fut_1 = cache.put_data_to_cache(
            remote_location.clone(),
            Some(112193),
            reader.into_stream().await.unwrap(),
        );
        let put_data_fut_2 = cache.put_data_to_cache(
            remote_location.clone(),
            Some(112193),
            reader2.into_stream().await.unwrap(),
        );
        let put_data_fut_3 = cache.put_data_to_cache(
            remote_location.clone(),
            Some(112193),
            reader3.into_stream().await.unwrap(),
        );

        let res = join!(put_data_fut_1, put_data_fut_2, put_data_fut_3);
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());
        assert_eq!(res.0.unwrap(), 112193);
        assert_eq!(res.1.unwrap(), 112193);
        assert_eq!(res.2.unwrap(), 112193);
    }
}
