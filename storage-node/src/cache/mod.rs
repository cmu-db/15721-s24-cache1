use self::lru::LruCache;

pub mod lru;

/// [`ParpulseCacheKey`] is a path to the remote object store.
pub type ParpulseCacheKey = String;
/// [`ParpulseCacheValue`] contains a path to the local disk indicating where
/// the cached item is stored, and also the size of the cached item.
/// This is just a prototype and we might refine it later.
pub type ParpulseCacheValue = (String, usize);

/// [`ParpulseCache`] offers a unified interface for different cache algorithms.
///
/// Feel free to modify the methods.
pub trait ParpulseCache {
    // TODO: Consider making the cache lock-free so that we can have `&self`
    // instead of `&mut self` here, which enables multiple calls to the cache at
    // the same time.
    fn get_value(&mut self, key: &ParpulseCacheKey) -> Option<ParpulseCacheValue>;

    fn set_value(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue);
}
