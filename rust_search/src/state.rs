use std::{
    collections::{HashMap, VecDeque},
    env,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use mongodb::{bson::Document, Collection};
use mysql_async::Pool;
use serde::{Deserialize, Serialize};

use crate::ported;

pub(crate) const DOCUMENTS_TABLE: &str = "search_documents";
pub(crate) const SEARCH_FIELDS: [&str; 3] = ["title", "description", "content"];

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) tidb_pool: Option<Pool>,
    pub(crate) mongo_collection: Option<Collection<Document>>,
    pub(crate) mongo_queue_collection: Option<Collection<Document>>,
    pub(crate) mongo_text_index: Arc<AtomicBool>,
    pub(crate) tidb_fulltext_enabled: bool,
    pub(crate) mongo_partition_percent: u8,
    pub(crate) search_backend: String,
    pub(crate) search_cache: Arc<Mutex<SearchCache>>,
    pub(crate) response_cache: Arc<Mutex<crate::response_cache::ResponseCache>>,
    pub(crate) tantivy_engine: Option<Arc<ported::tantivy_engine::TantivyEngine>>,
    pub(crate) recent_queries: Arc<Mutex<Vec<String>>>,
    pub(crate) pagerank_scores: Arc<HashMap<String, f64>>,
    pub(crate) pagerank_weight: f64,
    pub(crate) semantic_engine: Option<Arc<ported::vector_search::VectorSearchEngine>>,
    pub(crate) semantic_min_score: f64,
    pub(crate) spell_checker: Option<Arc<ported::spellcheck::SpellChecker>>,
    pub(crate) synonyms: Arc<ported::synonyms::Synonyms>,
    pub(crate) suggester: Arc<Mutex<ported::search_suggester::SearchSuggester>>,
}

#[derive(Default)]
pub(crate) struct SearchCache {
    pub(crate) entries: HashMap<(String, usize, usize, bool), (Instant, SearchResponse)>,
    pub(crate) lru: VecDeque<(String, usize, usize, bool)>,
}

#[derive(Deserialize)]
pub(crate) struct SearchRequest {
    pub(crate) normalized: String,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) fulltext_enabled: bool,
}

#[derive(Serialize, Clone)]
pub(crate) struct SearchResult {
    pub(crate) url: String,
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) content: String,
    pub(crate) score: f64,
    pub(crate) source: &'static str,
    /// Site favicon URL discovered while crawling (stored in the document
    /// metadata). `None` when the crawler did not record one for this page.
    pub(crate) favicon: Option<String>,
}

#[derive(Serialize, Clone)]
pub(crate) struct SearchResponse {
    pub(crate) tidb_results: Vec<SearchResult>,
    pub(crate) tidb_total: u64,
    pub(crate) mongo_results: Vec<SearchResult>,
    pub(crate) mongo_total: u64,
}

pub(crate) fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("Set {name} for the Rust search service"))
}

pub(crate) fn normalize_search_backend(setting: &str) -> Result<&'static str> {
    match setting.trim().to_ascii_lowercase().as_str() {
        "" | "tidb" | "sqlite" => Ok("tidb"),
        "mongodb" => Ok("mongodb"),
        _ => Err(anyhow!("SEARCH_BACKEND must be 'tidb' or 'mongodb'")),
    }
}

pub(crate) fn project_root() -> PathBuf {
    env::var_os("VELO_PROJECT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_sqlite_backend_selects_tidb_and_mongodb_is_supported() {
        assert_eq!(normalize_search_backend("sqlite").unwrap(), "tidb");
        assert_eq!(normalize_search_backend(" TiDB ").unwrap(), "tidb");
        assert_eq!(normalize_search_backend("mongodb").unwrap(), "mongodb");
        assert!(normalize_search_backend("local").is_err());
    }
}
