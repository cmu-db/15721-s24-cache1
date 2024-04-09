/// LRU-K cache implementation.
/// Credit: https://doi.org/10.1145/170036.170081
use log::{debug, warn};
use std::collections::HashMap;
use std::collections::VecDeque;

use super::DataStoreCacheKey;
use super::DataStoreCacheValue;
use super::{DataStoreReplacer, ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue};

type Timestamp = i32;

/// Represents a node in the LRU-K cache.
///
/// Each node contains a value of type `V` and a history of timestamps.
/// The history is stored as a `VecDeque<Timestamp>`, where the most recent
/// timestamps are at the front of the deque.
struct LruKNode<V: DataStoreCacheValue> {
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
pub struct LruKReplacer<K: DataStoreCacheKey, V: DataStoreCacheValue> {
    cache_map: HashMap<K, LruKNode<V>>,
    max_capacity: usize,
    size: usize,
    curr_timestamp: Timestamp,
    k: usize, // The k value for LRU-K
}

impl<K: DataStoreCacheKey, V: DataStoreCacheValue> LruKReplacer<K, V> {
    pub fn new(max_capacity: usize, k: usize) -> LruKReplacer<K, V> {
        LruKReplacer {
            cache_map: HashMap::new(),
            max_capacity,
            size: 0,
            curr_timestamp: 0,
            k,
        }
    }

    fn evict(&mut self, new_key: &K) -> Option<K> {
        let mut found = false;
        let mut max_k_dist = 0;
        let mut k_dist;
        let mut earliest_timestamp = 0;
        let mut key_to_evict: Option<K> = None;
        for (key, node) in self.cache_map.iter() {
            if key == new_key {
                continue;
            }
            let history = &node.history;
            if let Some(kth_timestamp) = history.front() {
                k_dist = if history.len() < self.k {
                    std::i32::MAX
                } else {
                    self.curr_timestamp - kth_timestamp
                };
                if (k_dist > max_k_dist)
                    || (k_dist == max_k_dist && kth_timestamp < &earliest_timestamp)
                {
                    found = true;
                    max_k_dist = k_dist;
                    earliest_timestamp = *kth_timestamp;
                    key_to_evict = Some(key.clone());
                }
            }
        }
        if found {
            if let Some(key) = key_to_evict {
                // TODO: Should have better logging
                debug!("-------- Evicting Key: {:?} --------", key);
                if let Some(node) = self.cache_map.remove(&key) {
                    self.size -= node.value.size();
                }
                return Some(key);
            }
        }
        None
    }

    fn record_access(&mut self, node: &mut LruKNode<V>) {
        node.history.push_back(self.curr_timestamp);
        if node.history.len() > self.k {
            node.history.pop_front();
        }
        self.curr_timestamp += 1;
    }

    fn get_value(&mut self, key: &K) -> Option<&V> {
        if let Some(mut node) = self.cache_map.remove(key) {
            self.record_access(&mut node);
            self.cache_map.insert(key.clone(), node);
            return self.cache_map.get(key).map(|node| &node.value);
        }
        None
    }

    fn put_value(&mut self, key: K, value: V) -> Option<Vec<K>> {
        if value.size() > self.max_capacity {
            // If the object size is greater than the max capacity, we do not insert the
            // object into the cache.
            warn!("The size of the value is greater than the max capacity",);
            warn!(
                "Key: {:?}, Value: {:?}, Value size: {:?}, Max capacity: {:?}",
                key,
                value.as_value(),
                value.size(),
                self.max_capacity
            );
            return None;
        }
        let updated_size = value.size();
        let mut new_history: VecDeque<Timestamp> = VecDeque::new();
        if let Some(mut node) = self.cache_map.remove(&key) {
            self.record_access(&mut node);
            self.size -= node.value.size();
            new_history = node.history;
        } else {
            new_history.push_back(self.curr_timestamp);
            self.curr_timestamp += 1;
        }
        self.cache_map.insert(
            key.clone(),
            LruKNode {
                value,
                history: new_history,
            },
        );
        self.size += updated_size;
        let mut evicted_keys = Vec::new();
        while self.size > self.max_capacity {
            let key_to_evict = self.evict(&key);
            debug_assert!(
                key_to_evict.is_some(),
                "key {:?} should have been evicted when cache size is greater than max capacity",
                key
            );
            if let Some(evicted_key) = key_to_evict {
                evicted_keys.push(evicted_key);
            }
        }
        Some(evicted_keys)
    }

    fn peek_value(&self, key: &K) -> Option<&V> {
        if let Some(node) = self.cache_map.get(key) {
            let cache_value = &node.value;
            Some(cache_value)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn current_timestamp(&self) -> Timestamp {
        self.curr_timestamp
    }
}

impl DataStoreReplacer for LruKReplacer<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue> {
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
        DataStoreReplacer, LruKReplacer, ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue,
    };
    use storage_common::init_logger;

    #[test]
    fn setup() {
        init_logger();
    }

    #[test]
    fn test_new() {
        let mut cache =
            LruKReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10, 2);
        assert_eq!(cache.max_capacity(), 10);
        assert_eq!(cache.size(), 0);
        cache.set_max_capacity(20);
        assert_eq!(cache.max_capacity(), 20);
    }

    #[test]
    fn test_peek_and_set() {
        let mut cache =
            LruKReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10, 2);
        let key = "key1".to_string();
        let value = "value1".to_string();
        assert_eq!(cache.peek(&key), None);
        assert!(cache.put(key.clone(), (value.clone(), 1)).is_some());
        assert_eq!(cache.peek(&key), Some(&(value.clone(), 1)));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.size(), 1);
        assert!(!cache.is_empty());
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_evict() {
        let mut cache =
            LruKReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(13, 2);
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
        assert_eq!(cache.current_timestamp(), 4);
        assert_eq!(cache.get(&key3), Some(&(value3.clone(), 3)));
        assert_eq!(cache.get(&key4), Some(&(value4.clone(), 4)));
        assert_eq!(cache.get(&key1), Some(&(value1.clone(), 1)));
        assert_eq!(cache.get(&key2), Some(&(value2.clone(), 2)));
        assert_eq!(cache.current_timestamp(), 8);
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
        assert_eq!(cache.current_timestamp(), 14); // When get fails, the timestamp should not be updated
    }

    #[test]
    fn test_infinite() {
        let mut cache =
            LruKReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(6, 2);
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let key3 = "key3".to_string();
        let key4 = "key4".to_string();
        let value1 = "value1".to_string();
        let value2 = "value2".to_string();
        let value3 = "value3".to_string();
        let value4 = "value4".to_string();
        cache.put(key1.clone(), (value1.clone(), 1));
        cache.put(key2.clone(), (value2.clone(), 2));
        cache.put(key3.clone(), (value3.clone(), 3));
        cache.put(key4.clone(), (value4.clone(), 4));
        assert_eq!(cache.current_timestamp(), 4);
        assert_eq!(cache.get(&key1), None); // Key1 should be evicted as it has infinite k distance and the earliest overall timestamp, same for key2 and key3
        assert_eq!(cache.get(&key2), None);
        assert_eq!(cache.get(&key3), None);
        assert_eq!(cache.size(), 4); // Only key4 should be in the cache
    }

    #[test]
    fn test_put_same_key() {
        let mut cache =
            LruKReplacer::<ParpulseDataStoreCacheKey, ParpulseDataStoreCacheValue>::new(10, 2);
        cache.put("key1".to_string(), ("value1".to_string(), 1));
        cache.put("key1".to_string(), ("value2".to_string(), 2));
        cache.put("key1".to_string(), ("value3".to_string(), 3));
        cache.put("key1".to_string(), ("value3".to_string(), 4));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.size(), 4);
        cache.put("key1".to_string(), ("value4".to_string(), 100)); // Should not be inserted
        assert_eq!(
            cache.get(&"key1".to_string()),
            Some(&("value3".to_string(), 4))
        );
        assert_eq!(cache.get(&("key2".to_string())), None);
    }
}
