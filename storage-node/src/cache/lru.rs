use datafusion::execution::DiskManager;

use super::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

// Just an example. Feel free to modify.
pub struct LruCache {}

impl LruCache {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get(&mut self, key: &ParpulseCacheKey) -> Option<ParpulseCacheValue> {
        todo!()
    }

    pub fn put(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) -> bool {
        todo!()
    }

    pub fn pop(&mut self) -> Option<ParpulseCacheValue> {
        todo!()
    }
}

impl ParpulseCache for LruCache {
    fn get_value(&mut self, key: &ParpulseCacheKey) -> Option<ParpulseCacheValue> {
        self.get(key)
    }

    fn set_value(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) {
        self.put(key, value);
    }
}
