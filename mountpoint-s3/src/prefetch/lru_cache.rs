use linked_hash_map::LinkedHashMap;
use std::hash::Hash;
use tracing::trace;

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct CacheKey {
    pub file_id: String,
    pub row_group: usize,
    pub column: usize,
}

#[derive(Debug)]
pub struct CacheValue {
    size: usize,
    last_access: std::time::Instant,
}

#[derive(Debug)]
pub struct LruCache {
    entries: LinkedHashMap<CacheKey, CacheValue>,
    memory_usage: usize,
    memory_limit: usize,
}

impl LruCache {
    pub fn new(memory_limit: usize) -> Self {
        LruCache {
            entries: LinkedHashMap::new(),
            memory_usage: 0,
            memory_limit,
        }
    }

    pub fn add_entry(&mut self, key: CacheKey, size: usize) -> Vec<CacheKey> {
        let mut evicted = Vec::new();
        trace!("Adding entry :) {:?}", &key);

        // If the entry already exists, update it
        if let Some(entry) = self.entries.get_mut(&key) {
            self.memory_usage = self.memory_usage.saturating_sub(entry.size);
            entry.size = size;
            entry.last_access = std::time::Instant::now();
            self.memory_usage += size;
            // Move the entry to the end (most recently used)
            self.move_to_back(&key);
            return evicted;
        }

        // Add new entry
        self.memory_usage += size;
        self.entries.insert(
            key.clone(),
            CacheValue {
                size,
                last_access: std::time::Instant::now(),
            },
        );

        // Evict entries if memory limit is exceeded
        while self.memory_usage > self.memory_limit {
            if let Some((evicted_key, evicted_value)) = self.entries.pop_front() {
                trace!("Evicting {:?}", &evicted_key);
                evicted.push(evicted_key);
                self.memory_usage = self.memory_usage.saturating_sub(evicted_value.size);
            } else {
                break;
            }
        }

        evicted
    }

    pub fn touch_entry(&mut self, key: &CacheKey) -> bool {
        if self.entries.contains_key(key) {
            self.move_to_back(key);
            if let Some(entry) = self.entries.get_mut(key) {
                entry.last_access = std::time::Instant::now();
            }
            true
        } else {
            false
        }
    }

    pub fn get_size(&self, key: &CacheKey) -> Option<usize> {
        self.entries.get(key).map(|v| v.size)
    }

    fn move_to_back(&mut self, key: &CacheKey) {
        if let Some(value) = self.entries.remove(key) {
            self.entries.insert(key.clone(), value);
        }
    }
}
