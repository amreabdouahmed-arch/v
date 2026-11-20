//! Response-level cache shared by `/search` and `/results`.
//!
//! Stores the aggregated page (result list plus total count) rather than the
//! serialised body, so both representations stay in sync. Eviction is least
//! recently used, backed by a `HashMap` for lookup and a `VecDeque` for order.

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use anyhow::anyhow;

use crate::{
    error::ApiError,
    state::{AppState, SearchResult},
};

/// Default lifetime of a cached page.
const DEFAULT_TTL: Duration = Duration::from_secs(60);
/// Default upper bound on cached pages.
const DEFAULT_MAX_ENTRIES: usize = 512;

type CacheKey = (String, usize, usize);

#[derive(Clone)]
pub(crate) struct CachedPage {
    pub(crate) results: Vec<SearchResult>,
    pub(crate) total_count: u64,
}

#[derive(Clone)]
pub(crate) struct ResponseCache {
    entries: HashMap<CacheKey, (Instant, CachedPage)>,
    lru: VecDeque<CacheKey>,
    ttl: Duration,
    max_entries: usize,
}

impl ResponseCache {
    pub(crate) fn new() -> Self {
        Self::with_limits(DEFAULT_TTL, DEFAULT_MAX_ENTRIES)
    }

    pub(crate) fn with_limits(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            ttl,
            max_entries,
        }
    }

    pub(crate) fn get(&mut self, key: &CacheKey) -> Option<CachedPage> {
        let (expires_at, page) = self.entries.get(key)?;
        if *expires_at <= Instant::now() {
            self.entries.remove(key);
            self.lru.retain(|entry| entry != key);
            return None;
        }
        let page = page.clone();
        self.lru.retain(|entry| entry != key);
        self.lru.push_back(key.clone());
        Some(page)
    }

    /// Wired into the read path in C2.
    #[allow(dead_code)]
    pub(crate) fn put(&mut self, key: CacheKey, page: CachedPage) {
        self.lru.retain(|entry| entry != &key);
        self.entries
            .insert(key.clone(), (Instant::now() + self.ttl, page));
        self.lru.push_back(key);
        while self.entries.len() > self.max_entries {
            if let Some(oldest) = self.lru.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }

    pub(crate) fn invalidate_all(&mut self) {
        self.entries.clear();
        self.lru.clear();
    }
}

pub(crate) fn invalidate(state: &AppState) -> Result<(), ApiError> {
    let mut cache = state
        .response_cache
        .lock()
        .map_err(|_| ApiError::internal(anyhow!("Response cache lock is poisoned")))?;
    cache.invalidate_all();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(label: &str) -> CachedPage {
        CachedPage {
            results: vec![SearchResult {
                url: label.to_string(),
                title: label.to_string(),
                description: String::new(),
                content: String::new(),
                score: 1.0,
                favicon: None,
                source: "Velo Web",
            }],
            total_count: 1,
        }
    }

    #[test]
    fn miss_then_hit_returns_stored_page() {
        let mut cache = ResponseCache::new();
        let key = ("harvard".to_string(), 10, 0);
        assert!(cache.get(&key).is_none());
        cache.put(key.clone(), page("a"));
        let hit = cache.get(&key).expect("cached page");
        assert_eq!(hit.results[0].url, "a");
        assert_eq!(hit.total_count, 1);
    }

    #[test]
    fn entry_expires_after_configured_ttl() {
        let mut cache = ResponseCache::with_limits(Duration::from_millis(1), 8);
        let key = ("harvard".to_string(), 10, 0);
        cache.put(key.clone(), page("a"));
        std::thread::sleep(Duration::from_millis(20));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn oldest_entry_is_evicted_at_the_limit() {
        let mut cache = ResponseCache::with_limits(Duration::from_secs(60), 2);
        cache.put(("a".to_string(), 10, 0), page("a"));
        cache.put(("b".to_string(), 10, 0), page("b"));
        cache.put(("c".to_string(), 10, 0), page("c"));
        assert!(cache.get(&("a".to_string(), 10, 0)).is_none());
        assert!(cache.get(&("b".to_string(), 10, 0)).is_some());
        assert!(cache.get(&("c".to_string(), 10, 0)).is_some());
    }

    #[test]
    fn invalidate_all_clears_every_entry() {
        let mut cache = ResponseCache::new();
        cache.put(("a".to_string(), 10, 0), page("a"));
        cache.put(("b".to_string(), 10, 0), page("b"));
        cache.invalidate_all();
        assert!(cache.get(&("a".to_string(), 10, 0)).is_none());
        assert!(cache.get(&("b".to_string(), 10, 0)).is_none());
    }
}