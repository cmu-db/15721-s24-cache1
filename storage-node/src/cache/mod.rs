pub mod lru;

use std::borrow::Borrow;
use std::hash::Hash;

/// [`ParpulseCacheKey`] is a path to the remote object store.
pub type ParpulseCacheKey = String;
/// [`ParpulseCacheValue`] contains a path to the local disk indicating where
/// the cached item is stored, and also the size of the cached item.
/// This is just a prototype and we might refine it later.
pub type ParpulseCacheValue = (String, usize);

pub trait ParpulseCache
where
    ParpulseCacheKey: Eq + Hash,
{
    fn get<Q>(&mut self, key: &Q) -> Option<&ParpulseCacheValue>
    where
        ParpulseCacheKey: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn put(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue);
    /// Returns a reference to the value in the cache without updating the
    /// access order
    fn peek<Q>(&mut self, key: &Q) -> Option<&ParpulseCacheValue>
    where
        ParpulseCacheKey: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    /// Returns the number of items in the cache
    fn len(&self) -> usize;
    /// Returns the current size (i.e. capacity) of the cache
    fn size(&self) -> usize;
    fn get_max_capacity(&self) -> usize;
    fn set_max_capacity(&mut self, capacity: usize);
    fn clear(&mut self);
}
