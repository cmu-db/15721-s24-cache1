use hashlink::linked_hash_map;
use hashlink::LinkedHashMap;

use super::CacheKey;
use super::CacheValue;
use super::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

/// [`LruCache`] adopts the least-recently-used algorithm to cache sized
/// objects. The cache will start evicting if a new object comes that makes
/// the cache's size exceeds its max capacity, from the oldest to the newest.
pub struct LruCache<K: CacheKey, V: CacheValue> {
    cache_map: LinkedHashMap<K, V>,
    max_capacity: usize,
    size: usize,
}

impl<K: CacheKey, V: CacheValue> LruCache<K, V> {
    pub fn new(max_capacity: usize) -> LruCache<K, V> {
        LruCache {
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

    fn put_value(&mut self, key: K, value: V) -> bool {
        if value.size() > self.max_capacity {
            // If the object size is greater than the max capacity, we do not insert the
            // object into the cache.
            // TODO(Yuanxin): Better logging approach.
            println!("Warning: The size of the value is greater than the max capacity",);
            println!(
                "Key: {:?}, Value: {:?}, Value size: {:?}, Max capacity: {:?}",
                key,
                value.as_value(),
                value.size(),
                self.max_capacity
            );
            return false;
        }
        if let Some(cache_value) = self.cache_map.get(&key) {
            // If the key already exists, update the cache size.
            self.size -= cache_value.size();
        }
        self.size += value.size();
        self.cache_map.insert(key.clone(), value);
        while self.size > self.max_capacity {
            if let Some((key, cache_value)) = self.cache_map.pop_front() {
                println!("-------- Evicting Key: {:?} --------", key);
                self.size -= cache_value.size();
            }
        }
        true
    }

    fn peek_value(&self, key: &K) -> Option<&V> {
        self.cache_map.get(key)
    }
}

impl ParpulseCache for LruCache<ParpulseCacheKey, ParpulseCacheValue> {
    fn get(&mut self, key: &ParpulseCacheKey) -> Option<&ParpulseCacheValue> {
        self.get_value(key)
    }

    fn put(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) -> bool {
        self.put_value(key, value)
    }

    fn peek(&self, key: &ParpulseCacheKey) -> Option<&ParpulseCacheValue> {
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
    use super::{LruCache, ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

    #[test]
    fn test_new() {
        let cache = LruCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10);
        assert_eq!(cache.max_capacity(), 10);
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_peek_and_set() {
        let mut cache = LruCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10);
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
        let mut cache = LruCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10);
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
        let mut cache = LruCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10);
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
