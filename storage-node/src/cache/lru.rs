use hashlink::linked_hash_map;
use hashlink::LinkedHashMap;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::Hash;

use super::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

pub struct LruCache<K, V> {
    cache_map: LinkedHashMap<K, V>,
    max_capacity: usize,
    curr_capacity: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Hash + Eq + Clone + Debug,
{
    pub fn new(max_capacity: usize) -> LruCache<K, V> {
        LruCache {
            cache_map: LinkedHashMap::new(),
            max_capacity,
            curr_capacity: 0,
        }
    }

    fn lru_get(&mut self, key: &K) -> Option<&V> {
        match self.cache_map.raw_entry_mut().from_key(key) {
            linked_hash_map::RawEntryMut::Occupied(mut entry) => {
                entry.to_back();
                Some(entry.into_mut())
            }
            linked_hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    fn lru_peek(&self, key: &K) -> Option<&V> {
        self.cache_map.get(key)
    }
}

impl ParpulseCache for LruCache<ParpulseCacheKey, ParpulseCacheValue> {
    fn get(&mut self, key: &ParpulseCacheKey) -> Option<&ParpulseCacheValue> {
        self.lru_get(key)
    }

    fn put(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) {
        // If the file size is greater than the max capacity, return
        if value.1 > self.max_capacity {
            println!(
                "Warning: The file size of key {:?} is greater than the max capacity",
                key
            );
            println!(
                "File size: {:?}, Max capacity: {:?}",
                value.1, self.max_capacity
            );
            return;
        }
        // If the key already exists, update the file size
        if let Some((_, file_size)) = self.cache_map.get(&key) {
            self.curr_capacity -= file_size;
        }
        self.curr_capacity += value.1;
        self.cache_map.insert(key.clone(), value);
        while self.curr_capacity > self.max_capacity {
            if let Some((key, (_, file_size))) = self.cache_map.pop_front() {
                println!("-------- Evicting Key: {:?} --------", key);
                self.curr_capacity -= file_size;
            }
        }
    }

    fn peek(&self, key: &ParpulseCacheKey) -> Option<&ParpulseCacheValue> {
        self.lru_peek(key)
    }

    fn len(&self) -> usize {
        self.cache_map.len()
    }

    fn is_empty(&self) -> bool {
        self.cache_map.is_empty()
    }

    fn size(&self) -> usize {
        self.curr_capacity
    }

    fn get_max_capacity(&self) -> usize {
        self.max_capacity
    }

    fn set_max_capacity(&mut self, capacity: usize) {
        self.max_capacity = capacity;
    }

    fn clear(&mut self) {
        self.cache_map.clear();
        self.curr_capacity = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::{LruCache, ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

    #[test]
    fn test_new() {
        let cache = LruCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10);
        assert_eq!(cache.get_max_capacity(), 10);
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
