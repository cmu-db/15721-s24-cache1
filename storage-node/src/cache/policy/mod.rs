pub mod lru;
pub mod lru_k;
use std::fmt::Debug;
use std::hash::Hash;

/// [`ReplacerKey`] is the key type for data store caches using different
/// cache policies in the system.
pub trait ReplacerKey: Hash + Eq + Clone + Debug {}
impl<T: Hash + Eq + Clone + Debug> ReplacerKey for T {}
/// [`ReplacerValue`] is the value type for data store caches using different
/// cache policies in the system.
/// It might represent a logical object and we can get the actual size for this
/// logical object by calling `size()`.
pub trait ReplacerValue {
    type Value: Debug;

    fn into_value(self) -> Self::Value;
    fn as_value(&self) -> &Self::Value;
    fn size(&self) -> usize;
}

/// [`ParpulseReplacerKey`] is a path to the remote object store.
pub type ParpulseReplacerKey = String;
/// [`ParpulseReplacerValue`] contains a path to the local disk indicating
/// where the cached item is stored, and also the size of the cached item.
/// This is just a prototype and we might refine it later.
pub type ParpulseReplacerValue = (String, usize);

impl ReplacerValue for ParpulseReplacerValue {
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
pub trait DataStoreReplacer: Send + Sync {
    /// Gets a value from the cache. Might has side effect on the cache (e.g.
    /// modifying some bookkeeping fields in the cache).
    fn get(&mut self, key: &ParpulseReplacerKey) -> Option<&ParpulseReplacerValue>;

    /// Puts a value into the cache.
    /// Returns `None`: insertion failed.
    /// Returns `Some`: insertion successful with a list of keys that are evicted from the cache.
    fn put(
        &mut self,
        key: ParpulseReplacerKey,
        value: ParpulseReplacerValue,
    ) -> Option<Vec<ParpulseReplacerKey>>;

    /// Returns a reference to the value in the cache with no side effect on the
    /// cache.
    fn peek(&self, key: &ParpulseReplacerKey) -> Option<&ParpulseReplacerValue>;

    /// Returns the number of the objects in the cache.
    fn len(&self) -> usize;

    /// Returns the total size of the objects in the cache.
    fn size(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn max_capacity(&self) -> usize;

    fn set_max_capacity(&mut self, capacity: usize);

    fn clear(&mut self);
}
