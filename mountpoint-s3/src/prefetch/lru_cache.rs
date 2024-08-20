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
pub struct LruCache {
    entries: LinkedHashMap<CacheKey, usize>,
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
        trace!("Adding entry: {:?}", key);

        let mut evicted = Vec::new();
        // If the entry already exists, update its size and move it to the back (most recently used)
        if let Some(entry_size) = self.entries.get_mut(&key) {
            self.memory_usage = self.memory_usage.saturating_sub(*entry_size);
            *entry_size = size;
            self.memory_usage += size;
            self.entries.get_refresh(&key);
        } else {
            // Add the new entry
            self.entries.insert(key.clone(), size);
            self.memory_usage += size;
        }

        // Evict entries if memory limit is exceeded
        while self.memory_usage > self.memory_limit {
            if let Some((evicted_key, evicted_size)) = self.entries.pop_front() {
                evicted.push(evicted_key);
                self.memory_usage = self.memory_usage.saturating_sub(evicted_size);
            } else {
                break;
            }
        }

        trace!("Evicting entries: {:?}", evicted);

        evicted
    }

    pub fn touch_entry(&mut self, key: &CacheKey) -> bool {
        self.entries.get_refresh(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_entry_within_limit() {
        let mut cache = LruCache::new(100);
        let key1 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 0,
        };
        let key2 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 1,
        };
        let key3 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 1,
            column: 0,
        };

        let evicted1 = cache.add_entry(key1.clone(), 50);
        assert_eq!(evicted1.len(), 0);
        assert_eq!(cache.memory_usage, 50);

        let evicted2 = cache.add_entry(key2.clone(), 30);
        assert_eq!(evicted2.len(), 0);
        assert_eq!(cache.memory_usage, 80);

        let evicted3 = cache.add_entry(key3.clone(), 20);
        assert_eq!(evicted3.len(), 0);
        assert_eq!(cache.memory_usage, 100);
    }

    #[test]
    fn test_add_entry_exceeding_limit() {
        let mut cache = LruCache::new(100);
        let key1 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 0,
        };
        let key2 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 1,
        };
        let key3 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 1,
            column: 0,
        };

        cache.add_entry(key1.clone(), 50);
        cache.add_entry(key2.clone(), 30);
        cache.add_entry(key3.clone(), 20);

        let key4 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 1,
            column: 1,
        };
        let evicted = cache.add_entry(key4.clone(), 60);
        assert_eq!(evicted, vec![key1, key2]);
        assert_eq!(cache.memory_usage, 80);
    }

    #[test]
    fn test_update_existing_entry() {
        let mut cache = LruCache::new(100);
        let key1 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 0,
        };
        let key2 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 1,
        };

        cache.add_entry(key1.clone(), 50);
        cache.add_entry(key2.clone(), 30);

        let evicted = cache.add_entry(key1.clone(), 40);
        assert_eq!(evicted.len(), 0);
        assert_eq!(cache.memory_usage, 70);
    }

    #[test]
    fn test_touch_entry() {
        let mut cache = LruCache::new(100);
        let key1 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 0,
        };
        let key2 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 1,
        };

        cache.add_entry(key1.clone(), 50);
        cache.add_entry(key2.clone(), 30);

        assert_eq!(cache.touch_entry(&key1), true);
        assert_eq!(cache.touch_entry(&key2), true);
        assert_eq!(
            cache.touch_entry(&CacheKey {
                file_id: "file1".to_string(),
                row_group: 1,
                column: 0
            }),
            false
        );
    }

    #[test]
    fn test_evict_multiple_entries() {
        let mut cache = LruCache::new(100);
        let key1 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 0,
        };
        let key2 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 1,
        };
        let key3 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 1,
            column: 0,
        };

        cache.add_entry(key1.clone(), 50);
        cache.add_entry(key2.clone(), 30);
        cache.add_entry(key3.clone(), 20);

        let key4 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 1,
            column: 1,
        };
        let evicted = cache.add_entry(key4.clone(), 80);
        assert_eq!(evicted, vec![key1, key2]);
        assert_eq!(cache.memory_usage, 100);
    }

    #[test]
    fn test_touch_entry_moves_to_back() {
        let mut cache = LruCache::new(100);
        let key1 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 0,
        };
        let key2 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 0,
            column: 1,
        };
        let key3 = CacheKey {
            file_id: "file1".to_string(),
            row_group: 1,
            column: 0,
        };

        cache.add_entry(key1.clone(), 30);
        cache.add_entry(key2.clone(), 40);
        cache.add_entry(key3.clone(), 20);

        // Check the initial order
        let keys: Vec<_> = cache.entries.keys().cloned().collect();
        assert_eq!(keys, vec![key1.clone(), key2.clone(), key3.clone()]);

        // Touch key1, it should move to the back
        cache.touch_entry(&key1);
        let keys: Vec<_> = cache.entries.keys().cloned().collect();
        assert_eq!(keys, vec![key2.clone(), key3.clone(), key1.clone()]);

        // Touch key3, it should move to the back
        cache.touch_entry(&key3);
        let keys: Vec<_> = cache.entries.keys().cloned().collect();
        assert_eq!(keys, vec![key2, key1, key3]);
    }
}
