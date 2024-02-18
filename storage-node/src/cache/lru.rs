use std::collections::HashMap;

use datafusion::execution::DiskManager;

use super::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

// Just an example. Feel free to modify.
pub struct LruCache<K, V> {
    kv: HashMap<K, V>,
}

impl<K, V> LruCache<K, V> {
    pub fn new() -> Self {
        Self { kv: HashMap::new() }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        todo!()
    }

    pub fn put(&mut self, key: K, value: V) -> bool {
        todo!()
    }

    pub fn pop(&mut self) -> Option<V> {
        todo!()
    }
}

impl ParpulseCache for LruCache<ParpulseCacheKey, ParpulseCacheValue> {
    fn get_value(&mut self, key: &ParpulseCacheKey) -> Option<ParpulseCacheValue> {
        self.get(key)
    }

    fn set_value(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) {
        self.put(key, value);
    }
}
