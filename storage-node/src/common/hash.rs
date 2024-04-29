use std::hash::Hasher;

pub fn calculate_hash_default(data: &[u8]) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(data);
    hasher.finish() as usize
}

pub fn calculate_hash_crc32fast(data: &[u8]) -> usize {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as usize
}
