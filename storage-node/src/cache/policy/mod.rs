pub mod lru;
pub mod lru_k;
use std::fmt::Debug;
use std::hash::Hash;

/// [`DataStoreCacheKey`] is the key type for data store caches using different
/// cache policies in the system.
pub trait DataStoreCacheKey: Hash + Eq + Clone + Debug {}
impl<T: Hash + Eq + Clone + Debug> DataStoreCacheKey for T {}
/// [`DataStoreCacheValue`] is the value type for data store caches using different
/// cache policies in the system.
/// It might represent a logical object and we can get the actual size for this
/// logical object by calling `size()`.
pub trait DataStoreCacheValue {
    type Value: Debug;

    fn into_value(self) -> Self::Value;
    fn as_value(&self) -> &Self::Value;
    fn size(&self) -> usize;
}

/// [`ParpulseDataStoreCacheKey`] is a path to the remote object store.
pub type ParpulseDataStoreCacheKey = String;
/// [`ParpulseDataStoreCacheValue`] contains a path to the local disk indicating
/// where the cached item is stored, and also the size of the cached item.
/// This is just a prototype and we might refine it later.
pub type ParpulseDataStoreCacheValue = (String, usize);

impl DataStoreCacheValue for ParpulseDataStoreCacheValue {
    type Value = String;

    fn into_value(self) -> Self::Value {
        self.0
    }

    fn as_value(&self) -> &Self::Value {
        &self.0
    }

    fn size(&self) -> usize {
        self.1
    }
}

/// [`DataStoreCache`] records objects' locations in the data store. For example, we cache
/// the contents of s3's remote object `userdata.parquet` in the local disk. Then we may
/// store the local file system path of `userdata.parquet` in `DataStoreCache`. By querying
/// `DataStoreCache`, we can get the local file system path of `userdata.parquet` and read the
/// contents from the local disk.
///
/// There are different cache policies for the data store cache, such as LRU, LRU-K, etc. See
/// other files in this module for more details.
pub trait DataStoreReplacer {
    /// Gets a value from the cache. Might has side effect on the cache (e.g.
    /// modifying some bookkeeping fields in the cache).
    fn get(&mut self, key: &ParpulseDataStoreCacheKey) -> Option<&ParpulseDataStoreCacheValue>;

    /// Puts a value into the cache.
    fn put(&mut self, key: ParpulseDataStoreCacheKey, value: ParpulseDataStoreCacheValue) -> bool;

    /// Returns a reference to the value in the cache with no side effect on the
    /// cache.
    fn peek(&self, key: &ParpulseDataStoreCacheKey) -> Option<&ParpulseDataStoreCacheValue>;

    /// Returns the number of the objects in the cache.
    fn len(&self) -> usize;

    /// Returns the total size of the objects in the cache.
    fn size(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn max_capacity(&self) -> usize;

    fn set_max_capacity(&mut self, capacity: usize);

    fn clear(&mut self);
}
