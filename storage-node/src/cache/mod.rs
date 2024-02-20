pub mod lru;
use std::fmt::Debug;
use std::hash::Hash;

/// [`CacheKey`] is the key type for different caches in the system.
pub trait CacheKey: Hash + Eq + Clone + Debug {}
impl<T: Hash + Eq + Clone + Debug> CacheKey for T {}
/// [`CacheValue`] is the value type for different caches in the system. It
/// might represent a logical object and we can get the actual size for this
/// logical object by calling `size()`.
pub trait CacheValue {
    type Value: Debug;

    fn into_value(self) -> Self::Value;
    fn as_value(&self) -> &Self::Value;
    fn size(&self) -> usize;
}

/// [`ParpulseCacheKey`] is a path to the remote object store.
pub type ParpulseCacheKey = String;
/// [`ParpulseCacheValue`] contains a path to the local disk indicating where
/// the cached item is stored, and also the size of the cached item.
/// This is just a prototype and we might refine it later.
pub type ParpulseCacheValue = (String, usize);

impl CacheValue for ParpulseCacheValue {
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

pub trait ParpulseCache {
    /// Gets a value from the cache. Might has side effect on the cache (e.g.
    /// modifying some bookkeeping fields in the cache).
    fn get(&mut self, key: &ParpulseCacheKey) -> Option<&ParpulseCacheValue>;

    /// Puts a value into the cache.
    fn put(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) -> bool;

    /// Returns a reference to the value in the cache with no side effect on the
    /// cache.
    fn peek(&self, key: &ParpulseCacheKey) -> Option<&ParpulseCacheValue>;

    /// Returns the number of the objects in the cache.
    fn len(&self) -> usize;

    /// Returns the total size of the objects in the cache.
    fn size(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn max_capacity(&self) -> usize;

    fn set_max_capacity(&mut self, capacity: usize);

    fn clear(&mut self);
}
