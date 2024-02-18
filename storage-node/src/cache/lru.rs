use hashlink::linked_hash_map;
use hashlink::LinkedHashMap;
use std::hash::Hash;
use std::borrow::Borrow;

use super::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

pub struct LruCache<K, V> {
    cache_map: LinkedHashMap<K, V>,
    max_capacity: usize,
    curr_capacity: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Hash + Eq + Clone,
{
    pub fn new(max_capacity: usize) -> LruCache<K, V> {
        LruCache {
            cache_map: LinkedHashMap::new(),
            max_capacity,
            curr_capacity: 0,
        }
    }
}

impl ParpulseCache<ParpulseCacheKey, ParpulseCacheValue> for LruCache<ParpulseCacheKey, ParpulseCacheValue>
{
    fn get<Q>(&mut self, key: &Q) -> Option<&ParpulseCacheValue>
    where
        ParpulseCacheKey: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.cache_map.raw_entry_mut().from_key(key) {
            linked_hash_map::RawEntryMut::Occupied(mut entry) => {
                entry.to_back();
                Some(entry.into_mut())
            }
            linked_hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    fn put(&mut self, key: ParpulseCacheKey, value: ParpulseCacheValue) {
        // If the file size is greater than the max capacity, return
        if value.1 > self.max_capacity {
            println!("Warning: The file size of key {:?} is greater than the max capacity", key);
            println!("File size: {:?}, Max capacity: {:?}", value.1, self.max_capacity);
            return;
        }
        // If the key already exists, update the value and move it to the back
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

    fn peek<Q>(&mut self, key: &Q) -> Option<&ParpulseCacheValue>
    where
        ParpulseCacheKey: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.cache_map.get(key)
    }

    fn len(&self) -> usize {
        self.cache_map.len()
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
