use std::ops::Deref;

use hashlink::linked_hash_map;
use hashlink::LinkedHashMap;
use log::{debug, warn};

use super::DataStoreReplacer;
use super::ReplacerKey;
use super::ReplacerValue;

/// [`LruReplacer`] adopts the least-recently-used algorithm to cache sized
/// objects. The replacer will start evicting if a new object comes that makes
/// the replacer's size exceeds its max capacity, from the oldest to the newest.
pub struct LruReplacer<K: ReplacerKey, V: ReplacerValue> {
    // usize is pin count
    cache_map: LinkedHashMap<K, (V, usize)>,
    max_capacity: usize,
    size: usize,
}

impl<K: ReplacerKey, V: ReplacerValue> LruReplacer<K, V> {
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
                Some(&entry.into_mut().0)
            }
            linked_hash_map::RawEntryMut::Vacant(_) => None,
        }
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
        if let Some(cache_value) = self.cache_map.get(&key) {
            // If the key already exists, update the replacer size.
            self.size -= cache_value.0.size();
        }
        let mut evicted_keys = Vec::new();
        while (self.size + value.size()) > self.max_capacity {
            // Previous code guarantees that there is at least 1 element in the cache_map, so we
            // can safely unwrap here.
            if self.cache_map.front().unwrap().1 .1 > 0 {
                // TODO(lanlou): Actually we should look next to evict, but for simplicity, just
                // return None here, it is temporarily okay since we will not pin an element for
                // long time now.
                return None;
            }
            if let Some((key, cache_value)) = self.cache_map.pop_front() {
                debug!("-------- Evicting Key: {:?} --------", key);
                evicted_keys.push(key);
                self.size -= cache_value.0.size();
            }
        }
        self.size += value.size();
        self.cache_map.insert(key.clone(), (value, 0));
        Some(evicted_keys)
    }

    fn pin_key(&mut self, key: &K) -> bool {
        match self.cache_map.get_mut(key) {
            Some((_, pin_count)) => {
                *pin_count += 1;
                true
            }
            None => false,
        }
    }

    fn unpin_key(&mut self, key: &K) -> bool {
        match self.cache_map.get_mut(key) {
            Some((_, pin_count)) => {
                if *pin_count.deref() == 0 {
                    return false;
                }
                *pin_count -= 1;
                true
            }
            None => false,
        }
    }

    fn peek_value(&self, key: &K) -> Option<&V> {
        match self.cache_map.get(key) {
            Some((value, _)) => Some(value),
            None => None,
        }
    }
}

impl<K: ReplacerKey, V: ReplacerValue> DataStoreReplacer<K, V> for LruReplacer<K, V> {
    fn get(&mut self, key: &K) -> Option<&V> {
        self.get_value(key)
    }

    fn put(&mut self, key: K, value: V) -> Option<Vec<K>> {
        self.put_value(key, value)
    }

    fn pin(&mut self, key: &K) -> bool {
        self.pin_key(key)
    }

    fn unpin(&mut self, key: &K) -> bool {
        self.unpin_key(key)
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

    use super::LruReplacer;

    #[test]
    fn test_new() {
        let replacer = LruReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10);
        assert_eq!(replacer.max_capacity(), 10);
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_peek_and_set() {
        let mut replacer =
            LruReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10);
        replacer.put("key1".to_string(), ("value1".to_string(), 1));
        replacer.put("key2".to_string(), ("value2".to_string(), 2));
        replacer.put("key3".to_string(), ("value3".to_string(), 3));
        replacer.put("key4".to_string(), ("value4".to_string(), 4));
        replacer.set_max_capacity(14);
        replacer.put("key5".to_string(), ("value5".to_string(), 5));
        assert_eq!(replacer.peek(&"key1".to_string()), None);
        assert_eq!(
            replacer.peek(&"key2".to_string()),
            Some(&("value2".to_string(), 2))
        );
        assert_eq!(
            replacer.peek(&"key3".to_string()),
            Some(&("value3".to_string(), 3))
        );
        assert_eq!(
            replacer.peek(&"key4".to_string()),
            Some(&("value4".to_string(), 4))
        );
        assert_eq!(
            replacer.peek(&"key5".to_string()),
            Some(&("value5".to_string(), 5))
        );
    }

    #[test]
    fn test_put_different_keys() {
        let mut replacer =
            LruReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10);
        replacer.put("key1".to_string(), ("value1".to_string(), 1));
        assert_eq!(replacer.size(), 1);
        replacer.put("key2".to_string(), ("value2".to_string(), 2));
        assert_eq!(replacer.size(), 3);
        replacer.put("key3".to_string(), ("value3".to_string(), 3));
        assert_eq!(replacer.size(), 6);
        replacer.put("key4".to_string(), ("value4".to_string(), 4));
        assert_eq!(replacer.size(), 10);
        replacer.put("key5".to_string(), ("value5".to_string(), 5));
        assert_eq!(replacer.size(), 9); // Only key4 and key5 are in the replacer
        assert_eq!(replacer.len(), 2);
        assert!(!replacer.is_empty());
        replacer.clear();
        assert!(replacer.is_empty());
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.len(), 0);
    }

    #[test]
    fn test_put_same_key() {
        let mut replacer =
            LruReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10);
        replacer.put("key1".to_string(), ("value1".to_string(), 1));
        replacer.put("key1".to_string(), ("value2".to_string(), 2));
        replacer.put("key1".to_string(), ("value3".to_string(), 3));
        assert_eq!(replacer.len(), 1);
        assert_eq!(replacer.size(), 3);
        replacer.put("key1".to_string(), ("value4".to_string(), 100)); // Should not be inserted
        assert_eq!(
            replacer.get(&"key1".to_string()),
            Some(&("value3".to_string(), 3))
        );
        assert_eq!(replacer.get(&("key2".to_string())), None);
    }

    #[test]
    fn test_evict_pinned_key() {
        let mut replacer =
            LruReplacer::<ParpulseTestReplacerKey, ParpulseTestReplacerValue>::new(10);
        replacer.put("key1".to_string(), ("value1".to_string(), 9));
        assert!(replacer.pin(&"key1".to_string()));
        assert!(replacer
            .put("key2".to_string(), ("value2".to_string(), 2))
            .is_none());
        assert_eq!(replacer.size(), 9);
        assert!(replacer.pin(&"key1".to_string()));
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
