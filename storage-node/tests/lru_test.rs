// tests/lru_test.rs

use storage_node::cache::lru::LruCache;
use storage_node::cache::{ParpulseCache, ParpulseCacheKey, ParpulseCacheValue};

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
    assert_eq!(cache.peek(&"key2".to_string()), Some(&("value2".to_string(), 2)));
    assert_eq!(cache.peek(&"key3".to_string()), Some(&("value3".to_string(), 3)));
    assert_eq!(cache.peek(&"key4".to_string()), Some(&("value4".to_string(), 4)));
    assert_eq!(cache.peek(&"key5".to_string()), Some(&("value5".to_string(), 5)));
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
    cache.clear();
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
    assert_eq!(cache.get("key1"), Some(&("value3".to_string(), 3)));
}
