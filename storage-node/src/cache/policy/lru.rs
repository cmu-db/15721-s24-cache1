use hashlink::linked_hash_map;
use hashlink::LinkedHashMap;
use log::{debug, warn};

use super::DataStoreCacheKey;
use super::DataStoreCacheValue;
use super::{DataStoreReplacer, ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue};

/// [`LruReplacer`] adopts the least-recently-used algorithm to cache sized
/// objects. The cache will start evicting if a new object comes that makes
/// the cache's size exceeds its max capacity, from the oldest to the newest.
pub struct LruReplacer<K: DataStoreCacheKey, V: DataStoreCacheValue> {
    cache_map: LinkedHashMap<K, V>,
    max_capacity: usize,
    size: usize,
}

impl<K: DataStoreCacheKey, V: DataStoreCacheValue> LruReplacer<K, V> {
    pub fn new(max_capacity: usize) -> LruReplacer<K, V> {
        LruReplacer {
            cache_map: LinkedHashMap::new(),
            max_capacity,
            size: 0,
        }
    }

    fn get_value(&mut self, key: &K) -> Option<&V> {
        match self.cache_map.raw_entry_mut().from_key(key) {
            linked_hash_map::RawEntryMut::Occupied(mut entry) => {
                entry.to_back();
                Some(entry.into_mut())
            }
            linked_hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    fn put_value(&mut self, key: K, value: V) -> Option<Vec<K>> {
        if value.size() > self.max_capacity {
            // If the object size is greater than the max capacity, we do not insert the
            // object into the cache.
            // TODO(Yuanxin): Better logging approach.
            warn!("Warning: The size of the value is greater than the max capacity",);
            warn!(
                "Key: {:?}, Value: {:?}, Value size: {:?}, Max capacity: {:?}",
                key,
                value.as_value(),
                value.size(),
                self.max_capacity
            );
            return None;
        }
        if let Some(cache_value) = self.cache_map.get(&key) {
            // If the key already exists, update the cache size.
            self.size -= cache_value.size();
        }
        self.size += value.size();
        self.cache_map.insert(key.clone(), value);
        let mut evicted_keys = Vec::new();
        while self.size > self.max_capacity {
            if let Some((key, cache_value)) = self.cache_map.pop_front() {
                debug!("-------- Evicting Key: {:?} --------", key);
                evicted_keys.push(key);
                self.size -= cache_value.size();
            }
        }
        Some(evicted_keys)
    }

    fn peek_value(&self, key: &K) -> Option<&V> {
        self.cache_map.get(key)
    }
}

impl DataStoreReplacer for LruReplacer<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue> {
    fn get(&mut self, key: &ParpulseDataStoreCacheKey) -> Option<&ParpulseDataStoreCacheValue> {
        self.get_value(key)
    }

    fn put(
        &mut self,
        key: ParpulseDataStoreCacheKey,
        value: ParpulseDataStoreCacheValue,
    ) -> Option<Vec<ParpulseDataStoreCacheKey>> {
        self.put_value(key, value)
    }

    fn peek(&self, key: &ParpulseDataStoreCacheKey) -> Option<&ParpulseDataStoreCacheValue> {
        self.peek_value(key)
    }

    fn len(&self) -> usize {
        self.cache_map.len()
    }

    fn is_empty(&self) -> bool {
        self.cache_map.is_empty()
    }

    fn size(&self) -> usize {
        self.size
    }

    fn max_capacity(&self) -> usize {
        self.max_capacity
    }

    fn set_max_capacity(&mut self, capacity: usize) {
        self.max_capacity = capacity;
    }

    fn clear(&mut self) {
        self.cache_map.clear();
        self.size = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DataStoreReplacer, LruReplacer, ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue,
    };

    #[test]
    fn test_new() {
        let cache = LruReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10);
        assert_eq!(cache.max_capacity(), 10);
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_peek_and_set() {
        let mut cache =
            LruReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10);
        cache.put("key1".to_string(), ("value1".to_string(), 1));
        cache.put("key2".to_string(), ("value2".to_string(), 2));
        cache.put("key3".to_string(), ("value3".to_string(), 3));
        cache.put("key4".to_string(), ("value4".to_string(), 4));
        cache.set_max_capacity(14);
        cache.put("key5".to_string(), ("value5".to_string(), 5));
        assert_eq!(cache.peek(&"key1".to_string()), None);
        assert_eq!(
            cache.peek(&"key2".to_string()),
            Some(&("value2".to_string(), 2))
        );
        assert_eq!(
            cache.peek(&"key3".to_string()),
            Some(&("value3".to_string(), 3))
        );
        assert_eq!(
            cache.peek(&"key4".to_string()),
            Some(&("value4".to_string(), 4))
        );
        assert_eq!(
            cache.peek(&"key5".to_string()),
            Some(&("value5".to_string(), 5))
        );
    }

    #[test]
    fn test_put_different_keys() {
        let mut cache =
            LruReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10);
        cache.put("key1".to_string(), ("value1".to_string(), 1));
        assert_eq!(cache.size(), 1);
        cache.put("key2".to_string(), ("value2".to_string(), 2));
        assert_eq!(cache.size(), 3);
        cache.put("key3".to_string(), ("value3".to_string(), 3));
        assert_eq!(cache.size(), 6);
        cache.put("key4".to_string(), ("value4".to_string(), 4));
        assert_eq!(cache.size(), 10);
        cache.put("key5".to_string(), ("value5".to_string(), 5));
        assert_eq!(cache.size(), 9); // Only key4 and key5 are in the cache
        assert_eq!(cache.len(), 2);
        assert!(!cache.is_empty());
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_put_same_key() {
        let mut cache =
            LruReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10);
        cache.put("key1".to_string(), ("value1".to_string(), 1));
        cache.put("key1".to_string(), ("value2".to_string(), 2));
        cache.put("key1".to_string(), ("value3".to_string(), 3));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.size(), 3);
        cache.put("key1".to_string(), ("value4".to_string(), 100)); // Should not be inserted
        assert_eq!(
            cache.get(&"key1".to_string()),
            Some(&("value3".to_string(), 3))
        );
        assert_eq!(cache.get(&("key2".to_string())), None);
    }
}
