use core::panic;
use datafusion::execution::cache;
use std::collections::HashMap;
use std::collections::VecDeque;

use super::CacheKey;
use super::CacheValue;
use super::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

type Timestamp = i32;

/// Represents a node in the LRU-K cache.
///
/// Each node contains a value of type `V` and a history of timestamps.
/// The history is stored as a `VecDeque<Timestamp>`, where the most recent
/// timestamps are at the front of the deque.
struct LruKNode<V: CacheValue> {
    value: V,
    history: VecDeque<Timestamp>,
}

/// Represents an LRU-K cache.
///
/// The LRU-K algorithm evicts a node whose backward k-distance is maximum of all
/// nodes. Backward k-distance is computed as the difference in time between current
/// timestamp and the timestamp of kth previous access. A node with fewer than k
/// historical accesses is given +inf as its backward k-distance. When multiple nodes
/// have +inf backward k-distance, the cache evicts the node with the earliest
/// overall timestamp (i.e., the frame whose least-recent recorded access is the
/// overall least recent access, overall, out of all nodes).
pub struct LruKCache<K: CacheKey, V: CacheValue> {
    cache_map: HashMap<K, LruKNode<V>>,
    max_capacity: usize,
    size: usize,
    curr_timestamp: Timestamp,
    k: usize, // The k value for LRU-K
}

impl<K: CacheKey, V: CacheValue> LruKCache<K, V> {
    pub fn new(max_capacity: usize, k: usize) -> LruKCache<K, V> {
        LruKCache {
            cache_map: HashMap::new(),
            max_capacity,
            size: 0,
            curr_timestamp: 0,
            k,
        }
    }

    fn evict(&mut self, new_key: &K) -> bool {
        let mut found = false;
        let mut max_dist = 0;
        let mut dist = 0;
        let mut earliest_timestamp = 0;
        let mut key_to_evict: Option<K> = None;
        for (key, node) in self.cache_map.iter() {
            if key == new_key {
                continue;
            }
            let history = &node.history;
            if let Some(kth_timestamp) = history.front() {
                if history.len() < self.k {
                    dist = std::i32::MAX;
                } else {
                    dist = self.curr_timestamp - kth_timestamp;
                }
                if (dist > max_dist) || (dist == max_dist && *kth_timestamp < earliest_timestamp) {
                    found = true;
                    max_dist = dist;
                    earliest_timestamp = *kth_timestamp;
                    key_to_evict = Some(key.clone());
                }
            }
        }
        if found {
            if let Some(key) = key_to_evict {
                println!("-------- Evicting Key: {:?} --------", key);
                if let Some(node) = self.cache_map.remove(&key) {
                    self.size -= node.value.size();
                }
            }
            return true;
        }
        false
    }

    fn get_value(&mut self, key: &K) -> Option<&V> {
        if let Some(node) = self.cache_map.get_mut(key) {
            // Record access
            node.history.push_back(self.curr_timestamp);
            if (node.history.len() as usize) > self.k {
                node.history.pop_front();
            }
            self.curr_timestamp += 1;
            let cache_value = &node.value;
            Some(cache_value)
        } else {
            None
        }
    }

    fn put_value(&mut self, key: K, value: V) -> bool {
        if value.size() > self.max_capacity {
            // If the object size is greater than the max capacity, we do not insert the
            // object into the cache.
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
        if let Some(node) = self.cache_map.get_mut(&key) {
            // If the key already exists, update the cache size.
            self.size -= node.value.size();
            // Record access
            node.history.push_back(self.curr_timestamp);
            if (node.history.len() as usize) > self.k {
                node.history.pop_front();
            }
            self.curr_timestamp += 1;
        }
        self.size += value.size();
        self.cache_map.insert(
            key.clone(),
            LruKNode {
                value,
                history: vec![self.curr_timestamp].into(),
            },
        );
        self.curr_timestamp += 1;
        while self.size > self.max_capacity {
            if !self.evict(&key) {
                panic!("Failed to evict a key {:?}", key);
            }
        }
        true
    }

    fn peek_value(&self, key: &K) -> Option<&V> {
        if let Some(node) = self.cache_map.get(key) {
            let cache_value = &node.value;
            Some(cache_value)
        } else {
            None
        }
    }
}

impl ParpulseCache for LruKCache<ParpulseCacheKey, ParpulseCacheValue> {
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
    use super::{LruKCache, ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

    #[test]
    fn test_new() {
        let cache = LruKCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10, 2);
        assert_eq!(cache.max_capacity(), 10);
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_peek_and_set() {
        let mut cache = LruKCache::<ParpulseCacheKey, ParpulseCacheValue>::new(10, 2);
        let key = "key1".to_string();
        let value = "value1".to_string();
        assert_eq!(cache.peek(&key), None);
        assert_eq!(cache.put(key.clone(), (value.clone(), 1)), true);
        assert_eq!(cache.peek(&key), Some(&(value.clone(), 1)));
    }

    #[test]
    fn test_evict() {
        let mut cache = LruKCache::<ParpulseCacheKey, ParpulseCacheValue>::new(13, 2);
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let key3 = "key3".to_string();
        let key4 = "key4".to_string();
        let key5 = "key5".to_string();
        let value1 = "value1".to_string();
        let value2 = "value2".to_string();
        let value3 = "value3".to_string();
        let value4 = "value4".to_string();
        let value5 = "value5".to_string();
        cache.put(key1.clone(), (value1.clone(), 1));
        cache.put(key2.clone(), (value2.clone(), 2));
        cache.put(key3.clone(), (value3.clone(), 3));
        cache.put(key4.clone(), (value4.clone(), 4));
        assert_eq!(cache.get(&key3), Some(&(value3.clone(), 3)));
        assert_eq!(cache.get(&key4), Some(&(value4.clone(), 4)));
        assert_eq!(cache.get(&key1), Some(&(value1.clone(), 1)));
        assert_eq!(cache.get(&key2), Some(&(value2.clone(), 2)));
        // Now the kth (i.e. 2nd) order from old to new is [1, 2, 3, 4]
        cache.put(key5.clone(), (value5.clone(), 4));
        assert_eq!(cache.get(&key1), None); // key1 should be evicted

        assert_eq!(cache.get(&key2), Some(&(value2.clone(), 2)));
        assert_eq!(cache.get(&key4), Some(&(value4.clone(), 4)));
        assert_eq!(cache.get(&key3), Some(&(value3.clone(), 3)));
        assert_eq!(cache.get(&key5), Some(&(value5.clone(), 4)));
        // Now the kth (i.e. 2nd) order from old to new is [3, 4, 2, 5]
        cache.put(key1.clone(), (value1.clone(), 1));
        assert_eq!(cache.get(&key3), None); // key3 should be evicted
    }
}
