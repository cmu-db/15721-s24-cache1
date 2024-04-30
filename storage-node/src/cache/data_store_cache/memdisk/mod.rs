pub mod data_store;

use std::{collections::HashMap, sync::Arc};

use futures::stream::StreamExt;
use log::{debug, warn};
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

/// Status -> completed/incompleted; usize -> file_size
type StatusKeyHashMap = HashMap<String, ((Status, usize), Arc<MemDiskStoreNotify>)>;

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

struct MemDiskStoreNotify {
    inner: Notify,
    waiter_count: Mutex<usize>,
}

impl MemDiskStoreNotify {
    fn new() -> Self {
        MemDiskStoreNotify {
            inner: Notify::new(),
            waiter_count: Mutex::new(0),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
enum Status {
    Incompleted,
    MemCompleted,
    DiskCompleted,
}

pub struct MemDiskStoreCache<
    R: 'static + DataStoreReplacer<MemDiskStoreReplacerKey, MemDiskStoreReplacerValue>,
> {
    disk_store: DiskStore,
    mem_store: Option<RwLock<MemStore>>,
    /// Cache_key = S3_PATH; Cache_value = (CACHE_BASE_PATH + S3_PATH, size)
    disk_replacer: Arc<Mutex<R>>,
    mem_replacer: Option<Arc<Mutex<R>>>,
    status_of_keys: RwLock<StatusKeyHashMap>,
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
            MemDiskStoreCache {
                disk_store,
                mem_store: Some(RwLock::new(mem_store)),
                disk_replacer: Arc::new(Mutex::new(disk_replacer)),
                mem_replacer: Some(Arc::new(Mutex::new(mem_replacer.unwrap()))),
                status_of_keys: RwLock::new(HashMap::new()),
            }
        } else {
            MemDiskStoreCache {
                disk_store,
                mem_store: None,
                disk_replacer: Arc::new(Mutex::new(disk_replacer)),
                mem_replacer: None,
                status_of_keys: RwLock::new(HashMap::new()),
            }
        }
    }

    async fn notify_waiters_error(&self, key: &str) {
        let notify;
        {
            let mut status_of_keys = self.status_of_keys.write().await;
            notify = status_of_keys.remove(key).unwrap().1;
        }
        notify.inner.notify_waiters();
        debug!("Notify waiters error for key {}", key);
    }

    async fn notify_waiters_mem(&self, key: &String, bytes_mem_written: usize) {
        let notify;
        {
            let mut status_of_keys = self.status_of_keys.write().await;
            let ((status, size), notify_ref) = status_of_keys.get_mut(key).unwrap();
            *status = Status::MemCompleted;
            debug!(
                "Notify waiters disk for key {}: set status to MemCompleted",
                key,
            );
            *size = bytes_mem_written;
            let notify_waiter = notify_ref.waiter_count.lock().await;
            if *notify_waiter > 0 {
                self.mem_replacer
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .pin(key, *notify_waiter);
                debug!(
                    "Notify waiters mem for key {}: pin with waiter count {}",
                    key, *notify_waiter
                );
            } else {
                debug!("Notify waiters mem for key {}: no waiter", key);
            }
            notify = notify_ref.clone();
        }
        // FIXME: status_of_keys write lock is released here, so the waken thread can immediately grab the lock
        notify.inner.notify_waiters();
    }

    async fn notify_waiters_disk(&self, key: &String, bytes_disk_written: usize) {
        let notify;
        {
            let mut status_of_keys = self.status_of_keys.write().await;
            let ((status, size), notify_ref) = status_of_keys.get_mut(key).unwrap();
            *status = Status::DiskCompleted;
            debug!(
                "Notify waiters disk for key {}: set status to DiskCompleted",
                key,
            );
            *size = bytes_disk_written;
            let notify_waiter = notify_ref.waiter_count.lock().await;
            if *notify_waiter > 0 {
                self.disk_replacer.lock().await.pin(key, *notify_waiter);
                debug!(
                    "Notify waiters disk for key {}: pin with waiter count {}",
                    key, *notify_waiter
                );
            } else {
                debug!("Notify waiters disk for key {}: no waiter", key);
            }
            notify = notify_ref.clone();
        }
        notify.inner.notify_waiters();
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
            let mut data_store_key = None;
            {
                let mut mem_replacer = self.mem_replacer.as_ref().unwrap().lock().await;
                if let Some(replacer_value) = mem_replacer.get(&remote_location) {
                    data_store_key = Some(replacer_value.as_value().clone());
                }
            }
            if let Some(data_store_key) = data_store_key {
                match mem_store
                    .read()
                    .await
                    .read_data(&data_store_key, self.mem_replacer.as_ref().unwrap().clone())
                {
                    Ok(Some(rx)) => {
                        debug!(
                            "MemDiskStore get_data_from_cache: directly read data for {} in memory",
                            remote_location
                        );
                        return Ok(Some(rx));
                    }
                    Ok(None) => {
                        return Err(ParpulseError::Internal(
                            "Memory replacer and memory store is inconsistent.".to_string(),
                        ))
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        let data_store_key;
        {
            let mut disk_replacer = self.disk_replacer.lock().await;
            match disk_replacer.get(&remote_location) {
                Some(replacer_value) => {
                    data_store_key = replacer_value.as_value().clone();
                }
                None => return Ok(None),
            }
        }
        if let Some(data) = self
            .disk_store
            .read_data(
                &data_store_key,
                self.disk_replacer.clone(),
                remote_location.clone(),
            )
            .await?
        {
            debug!(
                "MemDiskStore get_data_from_cache: directly read data for {} from disk",
                remote_location
            );
            Ok(Some(data))
        } else {
            return Err(ParpulseError::Internal(
                "Disk replacer and disk store is inconsistent.".to_string(),
            ));
        }
    }

    async fn put_data_to_cache(
        &self,
        remote_location: String,
        _data_size: Option<usize>,
        mut data_stream: StorageReaderStream,
    ) -> ParpulseResult<usize> {
        // If in_progress of remote_location thread fails, it will clean the data from this hash map.
        {
            let status_of_keys = self.status_of_keys.read().await;
            if let Some(((status, size), notify_ref)) = status_of_keys.get(&remote_location) {
                let notify;

                match status {
                    Status::Incompleted => {
                        debug!(
                            "MemDiskStore put_data_to_cache: find incompleted status for {}",
                            remote_location
                        );
                        notify = notify_ref.clone();
                        *notify_ref.waiter_count.lock().await += 1;
                        // The Notified future is guaranteed to receive wakeups from notify_waiters()
                        // as soon as it has been created, even if it has not yet been polled.
                        let notified = notify.inner.notified();
                        drop(status_of_keys);
                        notified.await;
                        if let Some(((status, size), _)) =
                            self.status_of_keys.read().await.get(&remote_location)
                        {
                            assert!(
                                *status == Status::MemCompleted || *status == Status::DiskCompleted
                            );
                            return Ok(*size);
                        } else {
                            return Err(ParpulseError::Internal(
                                "Put_data_to_cache fails".to_string(),
                            ));
                        }
                    }
                    Status::MemCompleted => {
                        // This code only applies to: a thread tries to `put_data_to_cache` but the data is already in cache
                        // and not be evicted. It is not wait and notified situation.
                        let mut mem_replacer = self.mem_replacer.as_ref().unwrap().lock().await;
                        if mem_replacer.peek(&remote_location).is_some() {
                            debug!("MemDiskStore put_data_to_cache: find mem-completed status for {}, remote location already in mem replacer, pin + 1", remote_location);
                            mem_replacer.pin(&remote_location, 1);
                            return Ok(*size);
                        } else {
                            debug!("MemDiskStore put_data_to_cache:find mem-completed status for {}, no remote location in mem replacer", remote_location);
                        }
                        // If mem_replacer has no data, then update status_of_keys
                    }
                    Status::DiskCompleted => {
                        // This code only applies to: a thread tries to `put_data_to_cache` but the data is already in cache
                        // and not be evicted. It is not wait and notified situation.
                        let mut disk_replacer = self.disk_replacer.lock().await;
                        if disk_replacer.peek(&remote_location).is_some() {
                            debug!("MemDiskStore put_data_to_cache: find disk-completed status for {}, remote location already in disk replacer, pin + 1", remote_location);
                            disk_replacer.pin(&remote_location, 1);
                            return Ok(*size);
                        } else {
                            debug!("MemDiskStore put_data_to_cache: find disk-completed status for {}, no remote location in disk replacer", remote_location);
                        }
                        // If disk_replacer has no data, then update status_of_keys
                    }
                }
            }
        }

        self.status_of_keys.write().await.insert(
            remote_location.clone(),
            (
                (Status::Incompleted, 0),
                Arc::new(MemDiskStoreNotify::new()),
            ),
        );
        debug!(
            "MemDiskStore put_data_to_cache: put incompleted status for {} into status_of_keys",
            remote_location
        );

        // TODO: Refine the lock.
        // TODO(lanlou): Also write the data to network.
        let mut bytes_to_disk = None;
        let mut bytes_mem_written = 0;
        // If the mem_store is enabled, first try to write the data to memory.
        // Note: Only file which size < mem_max_file_size can be written to memory.
        if let Some(mem_store) = &self.mem_store {
            loop {
                match data_stream.next().await {
                    Some(Ok(bytes)) => {
                        bytes_mem_written += bytes.len();
                        // TODO: Need to write the data to network.
                        // TODO(lanlou): we need benchmark future, to put this lock outside the loop.
                        if let Some((bytes_vec, _)) = mem_store
                            .write()
                            .await
                            .write_data(remote_location.clone(), bytes)
                        {
                            // If write_data returns something, it means the file size is too large
                            // to fit in the memory. We should put it to disk cache.
                            debug!(
                                "MemDiskStore put_data_to_cache: data size for {} not fit in memory, transfer data to disk",
                                remote_location
                            );
                            bytes_to_disk = Some(bytes_vec);
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        // TODO(lanlou): Every time it returns an error, I need to manually add this clean code...
                        // How to improve?
                        self.notify_waiters_error(&remote_location).await;
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
                if replacer_put_status.is_none() {
                    // If inserting to memory fails, put the data to disk cache.
                    bytes_to_disk = Some(
                        mem_store
                            .write()
                            .await
                            .clean_data(&remote_location)
                            .unwrap()
                            .0,
                    );
                } else {
                    // TODO(lanlou): currently the pin/unpin relies on after putting, it will **always** get!
                    mem_replacer.pin(&remote_location, 1);
                    debug!(
                        "MemDiskStore put_data_to_cache: mem pin for {} + 1",
                        remote_location
                    );
                    // If successfully putting it into mem_replacer, we should record the evicted data,
                    // delete them from mem_store, and put all of them to disk cache.
                    if let Some(evicted_keys) = replacer_put_status {
                        let mut mem_store = mem_store.write().await;
                        // TODO(lanlou): I have to grab this huge lock when evicting from mem to disk, since otherwise another
                        // thread may have the same evict_key as the new coming request, and it will put `Incompleted` into status_of_keys,
                        // and there is conflict. (maybe add incompleted status for evicted keys, but tricky)
                        let mut status_of_keys = self.status_of_keys.write().await;
                        drop(mem_replacer);
                        // Put the evicted keys to disk cache.
                        for evicted_key in evicted_keys {
                            if let Some(((status, _), _)) =
                                status_of_keys.get_mut(&evicted_key.clone())
                            {
                                // If the key is still in the status_of_keys, it means the data is still in progress.
                                // We should not put the data to disk cache.
                                if *status == Status::Incompleted {
                                    debug!(
                                        "MemDiskStore put_data_to_cache: key {} still in progress, skip and not put to disk",
                                        evicted_key
                                    );
                                    continue;
                                }
                                assert!(*status == Status::MemCompleted);
                                *status = Status::DiskCompleted;
                                debug!(
                                    "MemDiskStore put_data_to_cache: set status for key {} from MemCompleted to DiskCompleted",
                                    evicted_key);
                            }

                            let (bytes_vec, data_size) =
                                mem_store.clean_data(&evicted_key).unwrap();
                            assert_ne!(evicted_key, remote_location);
                            let disk_store_key = self.disk_store.data_store_key(&evicted_key);
                            if self
                                .disk_store
                                .write_data(disk_store_key.clone(), Some(bytes_vec), None)
                                .await
                                .is_err()
                            {
                                warn!("Failed to write evicted data to disk for {}", evicted_key);
                                continue;
                            }
                            let mut disk_replacer = self.disk_replacer.lock().await;
                            match disk_replacer.put(
                                evicted_key.clone(),
                                MemDiskStoreReplacerValue::new(disk_store_key.clone(), data_size),
                            ) {
                                Some(evicted_keys) => {
                                    // evict data from disk
                                    for evicted_key in evicted_keys {
                                        // FIXME: I think all status of the evicted_key removed here should be
                                        // `Completed`?
                                        let removed_status = status_of_keys.remove(&evicted_key);
                                        if let Some(((status, _), _)) = removed_status {
                                            debug!(
                                                "MemDiskStore put_data_to_cache: 1 remove status {:?} for key {}",
                                                status,evicted_key
                                            );
                                        }
                                        self.disk_store
                                            .clean_data(
                                                &self.disk_store.data_store_key(&evicted_key),
                                            )
                                            .await?;
                                    }
                                }
                                None => {
                                    if let Err(e) =
                                        self.disk_store.clean_data(&disk_store_key).await
                                    {
                                        warn!(
                                            "Failed to clean data ({}) from disk store: {}",
                                            disk_store_key, e
                                        );
                                    }
                                    // It is not main in-progress thread for evicted_key, and its status should be
                                    // `Completed`, so there is totally no need to notify the waiters.
                                    // FIXME: can we safely remove evicted_key here?
                                    let removed_status = status_of_keys.remove(&evicted_key);
                                    if let Some(((status, _), _)) = removed_status {
                                        debug!(
                                                "MemDiskStore put_data_to_cache: 2 remove status {:?} for key {}",
                                                status,evicted_key
                                            );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // If the data is successfully written to memory, directly return.
            if bytes_to_disk.is_none() {
                self.notify_waiters_mem(&remote_location, bytes_mem_written)
                    .await;
                return Ok(bytes_mem_written);
            }
        }

        // If the data is not written to memory_cache successfully, then cache it to disk.
        // Need to write the data to network for the current key.
        let disk_store_key = self.disk_store.data_store_key(&remote_location);
        let data_size_wrap = self
            .disk_store
            .write_data(disk_store_key.clone(), bytes_to_disk, Some(data_stream))
            .await;
        if let Err(e) = data_size_wrap {
            self.notify_waiters_error(&remote_location).await;
            return Err(e);
        }
        let data_size = data_size_wrap.unwrap();
        {
            let mut disk_replacer = self.disk_replacer.lock().await;
            match disk_replacer.put(
                remote_location.clone(),
                MemDiskStoreReplacerValue::new(disk_store_key.clone(), data_size),
            ) {
                Some(evicted_keys) => {
                    let mut status_of_keys = self.status_of_keys.write().await;
                    for evicted_key in evicted_keys {
                        let removed_status = status_of_keys.remove(&evicted_key);
                        if let Some(((status, _), _)) = removed_status {
                            debug!(
                                "MemDiskStore put_data_to_cache: 3 remove status {:?} for key {}",
                                status, evicted_key
                            );
                        }
                        self.disk_store
                            .clean_data(&self.disk_store.data_store_key(&evicted_key))
                            .await?;
                    }
                }
                None => {
                    if let Err(e) = self.disk_store.clean_data(&disk_store_key).await {
                        warn!(
                            "Failed to clean data ({}) from disk store: {}",
                            disk_store_key, e
                        );
                    }
                    // We have to notify waiters, because it is the main thread for the current key.
                    self.notify_waiters_error(&remote_location).await;
                    return Err(ParpulseError::Internal(
                        "Failed to put data to disk replacer.".to_string(),
                    ));
                }
            }
            disk_replacer.pin(&remote_location, 1);
            debug!(
                "MemDiskStore put_data_to_cache: disk pin for {} + 1",
                remote_location
            );
        }
        self.notify_waiters_disk(&remote_location, data_size).await;
        Ok(data_size)
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
    async fn test_same_requests_simultaneously_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = Arc::new(MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            None,
            None,
        ));
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata2.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader2 = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader3 = MockS3Reader::new(bucket.clone(), keys).await;

        let cache_1 = cache.clone();
        let remote_location_1 = remote_location.clone();
        let put_data_fut_1 = tokio::spawn(async move {
            cache_1
                .put_data_to_cache(remote_location_1, None, reader.into_stream().await.unwrap())
                .await
        });

        let cache_2 = cache.clone();
        let remote_location_2 = remote_location.clone();
        let put_data_fut_2 = tokio::spawn(async move {
            cache_2
                .put_data_to_cache(
                    remote_location_2,
                    None,
                    reader2.into_stream().await.unwrap(),
                )
                .await
        });

        let cache_3 = cache.clone();
        let remote_location_3 = remote_location.clone();
        let put_data_fut_3 = tokio::spawn(async move {
            cache_3
                .put_data_to_cache(
                    remote_location_3,
                    None,
                    reader3.into_stream().await.unwrap(),
                )
                .await
        });

        let res = join!(put_data_fut_1, put_data_fut_2, put_data_fut_3);
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());
        assert_eq!(res.0.unwrap().unwrap(), 112193);
        assert_eq!(res.1.unwrap().unwrap(), 112193);
        assert_eq!(res.2.unwrap().unwrap(), 112193);
    }

    #[tokio::test]
    async fn test_same_requests_simultaneously_mix() {
        let tmp = tempfile::tempdir().unwrap();
        let disk_base_path = tmp.path().to_owned();
        let cache = Arc::new(MemDiskStoreCache::new(
            LruReplacer::new(1024 * 512),
            disk_base_path.to_str().unwrap().to_string(),
            Some(LruReplacer::new(120000)),
            Some(120000),
        ));
        let bucket = "tests-parquet".to_string();
        let keys = vec!["userdata1.parquet".to_string()];
        let keys2 = vec!["userdata2.parquet".to_string()];
        let remote_location = bucket.clone() + &keys[0];
        let remote_location2 = bucket.clone() + &keys2[0];
        let reader = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader2 = MockS3Reader::new(bucket.clone(), keys.clone()).await;
        let reader3 = MockS3Reader::new(bucket.clone(), keys2).await;
        let reader4 = MockS3Reader::new(bucket.clone(), keys).await;

        let cache_1 = cache.clone();
        let remote_location_1 = remote_location.clone();
        let put_data_fut_1 = tokio::spawn(async move {
            cache_1
                .put_data_to_cache(remote_location_1, None, reader.into_stream().await.unwrap())
                .await
        });

        let cache_2 = cache.clone();
        let remote_location_2 = remote_location.clone();
        let put_data_fut_2 = tokio::spawn(async move {
            cache_2
                .put_data_to_cache(
                    remote_location_2,
                    None,
                    reader2.into_stream().await.unwrap(),
                )
                .await
        });

        let cache_3 = cache.clone();
        let remote_location_3 = remote_location2.clone();
        let put_data_fut_3 = tokio::spawn(async move {
            cache_3
                .put_data_to_cache(
                    remote_location_3,
                    None,
                    reader3.into_stream().await.unwrap(),
                )
                .await
        });

        let cache_4 = cache.clone();
        let remote_location_4 = remote_location.clone();
        let put_data_fut_4 = tokio::spawn(async move {
            cache_4
                .put_data_to_cache(
                    remote_location_4,
                    None,
                    reader4.into_stream().await.unwrap(),
                )
                .await
        });

        let res = join!(
            put_data_fut_1,
            put_data_fut_2,
            put_data_fut_3,
            put_data_fut_4
        );
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());
        assert!(res.3.is_ok());
        assert_eq!(res.0.unwrap().unwrap(), 113629);
        assert_eq!(res.1.unwrap().unwrap(), 113629);
        assert_eq!(res.2.unwrap().unwrap(), 112193);
        assert_eq!(res.3.unwrap().unwrap(), 113629);
    }
}
