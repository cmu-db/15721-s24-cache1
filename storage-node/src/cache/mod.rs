use self::lru::LruCache;

pub mod lru;

pub type ParpulseCacheKey = String;
pub type ParpulseCacheValue = String;

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
