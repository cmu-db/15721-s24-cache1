pub mod lru;
pub mod lru_k;
use std::fmt::Debug;
use std::hash::Hash;

/// [`ReplacerKey`] is the key type for data store replacers using different
/// policies in the system.
pub trait ReplacerKey: Hash + Eq + Clone + Debug + Send + Sync {}
impl<T: Hash + Eq + Clone + Debug + Send + Sync> ReplacerKey for T {}
/// [`ReplacerValue`] is the value type for data store caches using different
/// policies in the system.
/// It might represent a logical object and we can get the actual size for this
/// logical object by calling `size()`.
pub trait ReplacerValue: Send + Sync {
    type Value: Debug;

    fn into_value(self) -> Self::Value;
    fn as_value(&self) -> &Self::Value;
    fn size(&self) -> usize;
}

/// [`DataStoreReplacer`] records objects' locations in the data store. For example, we cache
/// the contents of s3's remote object `userdata.parquet` in the local disk. Then we may
/// store the local file system path of `userdata.parquet` in `DataStoreCache`. By querying
/// `DataStoreCache`, we can get the local file system path of `userdata.parquet` and read the
/// contents from the local disk.
///
/// There are different policies for the data store replacer, such as LRU, LRU-K, etc. See
/// other files in this module for more details.
pub trait DataStoreReplacer<K: ReplacerKey, V: ReplacerValue>: Send + Sync {
    /// Gets a value from the replacer. Might has side effect on the replacer (e.g.
    /// modifying some bookkeeping fields in the replacer).
    fn get(&mut self, key: &K) -> Option<&V>;

    /// Puts a value into the replacer.
    /// Returns `None`: insertion failed.
    /// Returns `Some`: insertion successful with a list of keys that are evicted from the cache.
    fn put(&mut self, key: K, value: V) -> Option<Vec<K>>;

    fn pin(&mut self, key: &K, count: usize) -> bool;

    fn unpin(&mut self, key: &K) -> bool;

    /// Returns a reference to the value in the replacer with no side effect on the
    /// replacer.
    fn peek(&self, key: &K) -> Option<&V>;

    /// Returns the number of the objects in the replacer.
    fn len(&self) -> usize;

    /// Returns the total size of the objects in the replacer.
    fn size(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn max_capacity(&self) -> usize;

    fn set_max_capacity(&mut self, capacity: usize);

    fn clear(&mut self);
}

#[cfg(test)]
mod tests {
    use super::ReplacerValue;

    pub type ParpulseTestReplacerKey = String;
    pub type ParpulseTestReplacerValue = (String, usize);

    impl ReplacerValue for ParpulseTestReplacerValue {
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
}
