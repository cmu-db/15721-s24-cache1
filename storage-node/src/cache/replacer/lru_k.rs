/// LRU-K replacer implementation.
/// Credit: https://doi.org/10.1145/170036.170081
use log::{debug, warn};
use std::collections::HashMap;
use std::collections::VecDeque;

use super::DataStoreReplacer;
use super::ReplacerKey;
use super::ReplacerValue;

type Timestamp = i32;

/// Represents a node in the LRU-K replacer.
///
/// Each node contains a value of type `V` and a history of timestamps.
/// The history is stored as a `VecDeque<Timestamp>`, where the most recent
/// timestamps are at the front of the deque.
struct LruKNode<V: ReplacerValue> {
    value: V,
    history: VecDeque<Timestamp>,
    pin_count: usize,
}

/// Represents an LRU-K replacer.
///
/// The LRU-K algorithm evicts a node whose backward k-distance is maximum of all
/// nodes. Backward k-distance is computed as the difference in time between current
/// timestamp and the timestamp of kth previous access. A node with fewer than k
/// historical accesses is given +inf as its backward k-distance. When multiple nodes
/// have +inf backward k-distance, the replacer evicts the node with the earliest
/// overall timestamp (i.e., the frame whose least-recent recorded access is the
/// overall least recent access, overall, out of all nodes).
pub struct LruKReplacer<K: ReplacerKey, V: ReplacerValue> {
    cache_map: HashMap<K, LruKNode<V>>,
    max_capacity: usize,
    size: usize,
    curr_timestamp: Timestamp,
    k: usize, // The k value for LRU-K
}

impl<K: ReplacerKey, V: ReplacerValue> LruKReplacer<K, V> {
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
                if ((k_dist > max_k_dist)
                    || (k_dist == max_k_dist && kth_timestamp < &earliest_timestamp))
                    && node.pin_count == 0
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
            // object into the replacer.
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
        let mut evicted_keys = Vec::new();
        while (self.size + updated_size) > self.max_capacity {
            let key_to_evict = self.evict(&key);
            // If key_to_evict is none, return none
            key_to_evict.as_ref()?;
            if let Some(evicted_key) = key_to_evict {
                evicted_keys.push(evicted_key);
            }
        }
        self.cache_map.insert(
            key.clone(),
            LruKNode {
                value,
                history: new_history,
                pin_count: 0,
            },
        );
        self.size += updated_size;
        Some(evicted_keys)
    }

    fn pin_value(&mut self, key: &K, count: usize) -> bool {
        match self.cache_map.get_mut(key) {
            Some(node) => {
                node.pin_count += count;
                true
            }
            None => false,
        }
    }

    fn unpin_value(&mut self, key: &K) -> bool {
        match self.cache_map.get_mut(key) {
            Some(node) => {
                if node.pin_count == 0 {
                    return false;
                }
                node.pin_count -= 1;
                true
            }
            None => false,
        }
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

impl<K: ReplacerKey, V: ReplacerValue> DataStoreReplacer<K, V> for LruKReplacer<K, V> {
    fn get(&mut self, key: &K) -> Option<&V> {
        self.get_value(key)
    }

    fn put(&mut self, key: K, value: V) -> Option<Vec<K>> {
        self.put_value(key, value)
    }

    fn pin(&mut self, key: &K, count: usize) -> bool {
        self.pin_value(key, count)
    }

    fn unpin(&mut self, key: &K) -> bool {
        self.unpin_value(key)
    }

    fn peek(&self, key: &K) -> Option<&V> {
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
    use crate::cache::replacer::{
        tests::{ParpulseTestReplacerKey, ParpulseTestReplacerValue},
        DataStoreReplacer,
    };

    use super::LruKReplacer;

    #[test]
    fn test_new() {
        let mut replacer =
            LruKReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10, 2);
        assert_eq!(replacer.max_capacity(), 10);
        assert_eq!(replacer.size(), 0);
        replacer.set_max_capacity(20);
        assert_eq!(replacer.max_capacity(), 20);
    }

    #[test]
    fn test_peek_and_set() {
        let mut replacer =
            LruKReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10, 2);
        let key = "key1".to_string();
        let value = "value1".to_string();
        assert_eq!(replacer.peek(&key), None);
        assert!(replacer.put(key.clone(), (value.clone(), 1)).is_some());
        assert_eq!(replacer.peek(&key), Some(&(value.clone(), 1)));
        assert_eq!(replacer.len(), 1);
        assert_eq!(replacer.size(), 1);
        assert!(!replacer.is_empty());
        replacer.clear();
        assert!(replacer.is_empty());
    }

    #[test]
    fn test_evict() {
        let mut replacer =
            LruKReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(13, 2);
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
        replacer.put(key1.clone(), (value1.clone(), 1));
        replacer.put(key2.clone(), (value2.clone(), 2));
        replacer.put(key3.clone(), (value3.clone(), 3));
        replacer.put(key4.clone(), (value4.clone(), 4));
        assert_eq!(replacer.current_timestamp(), 4);
        assert_eq!(replacer.get(&key3), Some(&(value3.clone(), 3)));
        assert_eq!(replacer.get(&key4), Some(&(value4.clone(), 4)));
        assert_eq!(replacer.get(&key1), Some(&(value1.clone(), 1)));
        assert_eq!(replacer.get(&key2), Some(&(value2.clone(), 2)));
        assert_eq!(replacer.current_timestamp(), 8);
        // Now the kth (i.e. 2nd) order from old to new is [1, 2, 3, 4]
        replacer.put(key5.clone(), (value5.clone(), 4));
        assert_eq!(replacer.get(&key1), None); // key1 should be evicted

        assert_eq!(replacer.get(&key2), Some(&(value2.clone(), 2)));
        assert_eq!(replacer.get(&key4), Some(&(value4.clone(), 4)));
        assert_eq!(replacer.get(&key3), Some(&(value3.clone(), 3)));
        assert_eq!(replacer.get(&key5), Some(&(value5.clone(), 4)));
        // Now the kth (i.e. 2nd) order from old to new is [3, 4, 2, 5]
        replacer.put(key1.clone(), (value1.clone(), 1));
        assert_eq!(replacer.get(&key3), None); // key3 should be evicted
        assert_eq!(replacer.current_timestamp(), 14); // When get fails, the timestamp should not be updated
    }

    #[test]
    fn test_infinite() {
        let mut replacer =
            LruKReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(6, 2);
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let key3 = "key3".to_string();
        let key4 = "key4".to_string();
        let value1 = "value1".to_string();
        let value2 = "value2".to_string();
        let value3 = "value3".to_string();
        let value4 = "value4".to_string();
        replacer.put(key1.clone(), (value1.clone(), 1));
        replacer.put(key2.clone(), (value2.clone(), 2));
        replacer.put(key3.clone(), (value3.clone(), 3));
        replacer.put(key4.clone(), (value4.clone(), 4));
        assert_eq!(replacer.current_timestamp(), 4);
        assert_eq!(replacer.get(&key1), None); // Key1 should be evicted as it has infinite k distance and the earliest overall timestamp, same for key2 and key3
        assert_eq!(replacer.get(&key2), None);
        assert_eq!(replacer.get(&key3), None);
        assert_eq!(replacer.size(), 4); // Only key4 should be in the replacer
    }

    #[test]
    fn test_put_same_key() {
        let mut replacer =
            LruKReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10, 2);
        replacer.put("key1".to_string(), ("value1".to_string(), 1));
        replacer.put("key1".to_string(), ("value2".to_string(), 2));
        replacer.put("key1".to_string(), ("value3".to_string(), 3));
        replacer.put("key1".to_string(), ("value3".to_string(), 4));
        assert_eq!(replacer.len(), 1);
        assert_eq!(replacer.size(), 4);
        replacer.put("key1".to_string(), ("value4".to_string(), 100)); // Should not be inserted
        assert_eq!(
            replacer.get(&"key1".to_string()),
            Some(&("value3".to_string(), 4))
        );
        assert_eq!(replacer.get(&("key2".to_string())), None);
    }

    #[test]
    fn test_evict_pinned_key() {
        let mut replacer =
            LruKReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10, 2);
        replacer.put("key1".to_string(), ("value1".to_string(), 9));
        assert!(replacer.pin(&"key1".to_string(), 1));
        assert!(replacer
            .put("key2".to_string(), ("value2".to_string(), 2))
            .is_none());
        assert_eq!(replacer.size(), 9);
        assert!(replacer.pin(&"key1".to_string(), 1));
        assert!(replacer.unpin(&"key1".to_string()));
        assert!(replacer
            .put("key2".to_string(), ("value2".to_string(), 2))
            .is_none());
        assert!(replacer.unpin(&"key1".to_string()));
        assert!(replacer
            .put("key2".to_string(), ("value2".to_string(), 2))
            .is_some());
        assert_eq!(replacer.size(), 2);
    }
}
