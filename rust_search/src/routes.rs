use std::{
    collections::HashMap,
    env,
    time::{Duration, Instant},
};

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Document};
use mysql_async::{prelude::Queryable, Row};
use regex::escape;
use serde::{Deserialize, Serialize};

use crate::admin::require_admin;
use crate::error::ApiError;
use crate::ingest::{stage_document, IndexDocumentRequest};
use crate::pages::{ai_overview, featured_snippet, knowledge_panel};
use crate::search::federated_search;
use crate::state::{AppState, DOCUMENTS_TABLE, SearchRequest, SearchResult};
use crate::textops::{make_snippet, normalize_text};
use crate::tidb::mysql_prefix_pattern;

#[derive(Deserialize)]
pub(crate) struct SearchParams {
    q: String,
    limit: Option<usize>,
    offset: Option<usize>,
    skip: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct PublicSearchResult {
    url: String,
    title: String,
    content: String,
    score: f64,
    source: &'static str,
    /// Site favicon URL discovered while crawling (stored in document
    /// metadata). `None` when unknown; the frontend falls back to a letter.
    pub(crate) favicon: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct PublicSearchResponse {
    query: String,
    corrected_query: Option<String>,
    results: Vec<PublicSearchResult>,
    total_count: u64,
    search_time_ms: f64,
    offset: usize,
}

/// Adds `weight * (pagerank / 100)` to each result whose URL has a stored
/// PageRank score. A zero weight or an empty score table is a no-op.
fn apply_authority_boost(
    results: &mut [SearchResult],
    scores: &HashMap<String, f64>,
    weight: f64,
) {
    if weight <= 0.0 || scores.is_empty() {
        return;
    }
    for result in results {
        if let Some(authority) = pagerank_for(scores, &result.url) {
            result.score += weight * (authority / 100.0);
        }
    }
}

/// Heuristic: detect obvious clickbait titles to down-rank or filter them.
/// Catches patterns like "You won't believe...", "10 things you didn't know...",
/// "This one trick...", excessive caps, excessive punctuation, etc.
fn is_clickbait_title(title: &str) -> bool {
    if title.is_empty() { return false; }
    let t = title.to_lowercase();
    // Excessive punctuation (!!! or ???)
    if title.matches('!').count() >= 3 || title.matches('?').count() >= 3 { return true; }
    // Excessive CAPS (more than 60% uppercase letters)
    let letters: Vec<char> = title.chars().filter(|c| c.is_alphabetic()).collect();
    if letters.len() > 5 {
        let upper = letters.iter().filter(|c| c.is_uppercase()).count();
        if upper * 100 / letters.len() > 60 { return true; }
    }
    // Common clickbait phrases (English + Arabic)
    const CLICKBAIT_PHRASES: &[&str] = &[
        "you won't believe", "you wont believe", "won't believe what happened",
        "this one trick", "one weird trick", "doctors hate", "shocking truth",
        "secret revealed", "they don't want you to know", "must see", "must watch",
        "gone wrong", "gone sexual", "epic fail", "will blow your mind",
        "can't believe", "cant believe", "number 1 will", "wait for it",
        "10 things", "top 10", "top 5", "top 3", "you need to see",
        // Arabic clickbait
        "لن تصدق", "لن تتخيل", "ماذا حدث", "سر خطير", "حقيقة صادمة",
        "يجب أن ترى", "أغرب مما تتخيل", "كارثة", "فضيحة", "صدمة",
    ];
    CLICKBAIT_PHRASES.iter().any(|phrase| t.contains(phrase))
}

/// Looks up a PageRank score tolerating trailing-slash and case differences.
fn pagerank_for(scores: &HashMap<String, f64>, url: &str) -> Option<f64> {
    let trimmed = url.trim_end_matches('/');
    scores
        .get(url)
        .or_else(|| scores.get(trimmed))
        .or_else(|| scores.get(&trimmed.to_lowercase()))
        .copied()
}

/// Small, transparent ranking prior: when text relevance is otherwise close,
/// established academic and government domains should beat general-interest
/// pages. The boost is intentionally modest so it cannot override a clearly
/// better textual match.
const ACADEMIC_DOMAIN_BOOST: f64 = 4.0;
const GOVERNMENT_DOMAIN_BOOST: f64 = 3.0;

fn domain_trust_boost(url: &str) -> f64 {
    let Ok(parsed) = url::Url::parse(url) else {
        return 0.0;
    };
    let Some(host) = parsed.host_str() else {
        return 0.0;
    };
    let host = host.to_ascii_lowercase();
    let labels: Vec<&str> = host.split('.').collect();
    let last = labels.last().copied();
    let penultimate = labels
        .len()
        .checked_sub(2)
        .and_then(|index| labels.get(index))
        .copied();
    // Covers `harvard.edu`, `cu.edu.eg`, and `ox.ac.uk` without boosting
    // arbitrary hosts that merely contain those letters.
    let academic =
        last == Some("edu") || penultimate == Some("edu") || penultimate == Some("ac");
    let government = last == Some("gov") || penultimate == Some("gov") || last == Some("mil");
    if academic {
        ACADEMIC_DOMAIN_BOOST
    } else if government {
        GOVERNMENT_DOMAIN_BOOST
    } else {
        0.0
    }
}

fn apply_domain_trust_boost(results: &mut [SearchResult]) {
    for result in results {
        result.score += domain_trust_boost(&result.url);
    }
}

/// Reports the larger of the cloud total and the merged local count, without
/// flooring an empty result set to one.
fn reported_total(cloud_total: u64, merged_len: usize) -> u64 {
    cloud_total.max(merged_len as u64) // no max(1) floor: an empty result reports 0
}

/// Removes angle brackets so suggestions cannot smuggle markup into the UI.
fn sanitize_suggestion(text: &str) -> String {
    text.chars().filter(|c| *c != '<' && *c != '>').collect()
}

/// A search is sparse when the whole pipeline produced fewer than this many
/// merged results; that is when spell correction and synonyms get a retry.
const SPARSE_RESULT_COUNT: u64 = 3;

/// Gathers and merges results from the local engines and the cloud stores.
/// Does not touch recent-query bookkeeping; `public_search` owns that.
async fn gather_results(
    state: &AppState,
    cloud_query: &str,
    local_query: &str,
    limit: usize,
    offset: usize,
) -> Result<(Vec<SearchResult>, u64), ApiError> {
    let mut local_results = Vec::new();
    if let Some(engine) = state.tantivy_engine.clone() {
        let query = local_query.to_string();
        let wanted = limit.saturating_add(offset);
        let outcome =
            tokio::task::spawn_blocking(move || engine.search(&query, wanted, 0)).await;
        if let Ok(Ok(search_res)) = outcome {
            for hit in search_res.hits {
                local_results.push(SearchResult {
                    url: hit.url,
                    title: hit.title,
                    description: hit.snippet.clone(),
                    content: hit.snippet,
                    score: hit.score as f64 + 10.0,
                    favicon: None,
                    source: "Velo Web",
                });
            }
        }
    }
    if let Some(engine) = state.semantic_engine.clone() {
        let query = local_query.to_string();
        let wanted = limit.saturating_add(offset).min(50);
        let min_score = state.semantic_min_score;
        let outcome = tokio::task::spawn_blocking(move || {
            engine
                .semantic_search(&query, wanted)
                .into_iter()
                .take_while(|(_, similarity)| f64::from(*similarity) >= min_score)
                .collect::<Vec<_>>()
        })
        .await;
        if let Ok(found) = outcome {
            for (document, similarity) in found {
                local_results.push(SearchResult {
                    url: document.url,
                    title: document.title,
                    description: document.content.clone(),
                    content: document.content,
                    score: f64::from(similarity) * 10.0,
                    favicon: None,
                    source: "Velo Semantic",
                });
            }
        }
    }

    // The local index already holds enough results for this page. Waiting on
    // the cloud stores only adds their network latency, so call them only when
    // the local engines are sparse and genuinely need the extra coverage.
    let has_fast_results = local_results.len() >= limit.saturating_add(offset);
    let (tidb_results, mongo_results, cloud_total) = if has_fast_results {
        (Vec::new(), Vec::new(), 0)
    } else {
        let cloud_response = tokio::time::timeout(
            Duration::from_millis(3500),
            federated_search(
                state,
                SearchRequest {
                    normalized: cloud_query.to_owned(),
                    limit: limit.saturating_add(offset),
                    offset: 0,
                    fulltext_enabled: state.tidb_fulltext_enabled,
                },
            ),
        )
        .await;
        match cloud_response {
            Ok(Ok(resp)) => (
                resp.tidb_results,
                resp.mongo_results,
                resp.tidb_total.saturating_add(resp.mongo_total),
            ),
            _ => (Vec::new(), Vec::new(), 0),
        }
    };

    let mut by_url: HashMap<String, SearchResult> = HashMap::new();
    for result in local_results
        .into_iter()
        .chain(tidb_results)
        .chain(mongo_results)
    {
        let key = result.url.to_lowercase().trim_end_matches('/').to_owned();
        if key.is_empty() {
            continue;
        }
        match by_url.get(&key) {
            Some(existing) if existing.score >= result.score => {}
            _ => {
                by_url.insert(key, result);
            }
        }
    }
    let mut merged: Vec<SearchResult> = by_url.into_values().collect();
    apply_authority_boost(&mut merged, &state.pagerank_scores, state.pagerank_weight);
    apply_domain_trust_boost(&mut merged);
    merged.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.url.cmp(&right.url))
    });
    let total = reported_total(cloud_total, merged.len());
    Ok((merged, total))
}

pub(crate) async fn public_search(
    state: &AppState,
    query: &str,
    limit: usize,
    offset: usize,
) -> Result<(Vec<PublicSearchResult>, u64, Option<String>), ApiError> {
    {
        let mut queries = state
            .recent_queries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let q_clean = query.trim().to_string();
        if !q_clean.is_empty() && !queries.contains(&q_clean) {
            queries.push(q_clean);
            if queries.len() > 100 {
                queries.remove(0);
            }
        }
    }
    {
        let mut suggester = state.suggester.lock().unwrap_or_else(|e| e.into_inner());
        suggester.add_search_query(&normalize_text(query), None);
    }

    let (merged, total) = gather_results(state, query, query, limit, offset).await?;
    let (merged, total, corrected_query) = if total >= SPARSE_RESULT_COUNT {
        (merged, total, None)
    } else {
        retry_sparse_search(state, query, limit, offset, merged, total).await
    };

    let snippet_query = corrected_query.as_deref().unwrap_or(query);
    // Filter out obvious clickbait titles (e.g. "You won't believe...", "10 things...").
    let results: Vec<_> = merged
        .into_iter()
        .filter(|r| !is_clickbait_title(&r.title))
        .skip(offset)
        .take(limit)
        .map(|result| {
            let excerpt = if result.description.is_empty() {
                &result.content
            } else {
                &result.description
            };
            let title = if result.title.is_empty() {
                result.url.clone()
            } else {
                result.title
            };
            let favicon = result.favicon.or_else(|| {
                let raw = result.url.trim();
                let scheme_and_rest = raw.strip_prefix("https://").or_else(|| raw.strip_prefix("http://"));
                let host = scheme_and_rest
                    .and_then(|rest| rest.split('/').next())
                    .filter(|h| !h.is_empty() && h.contains('.'));
                let scheme = if raw.starts_with("https://") { "https" } else { "http" };
                host.map(|h| format!("{}://{}/favicon.ico", scheme, h))
            });
            PublicSearchResult {
                url: result.url,
                title,
                content: make_snippet(excerpt, snippet_query, 220),
                score: result.score,
                source: result.source,
                favicon,
            }
        })
        .collect();
    Ok((results, total, corrected_query))
}

/// One retry for sparse searches: a spelling fix first (reported back as
/// `corrected_query`), then a silent synonym expansion. Both must beat the
/// original result count, otherwise the original results stand.
async fn retry_sparse_search(
    state: &AppState,
    query: &str,
    limit: usize,
    offset: usize,
    original: Vec<SearchResult>,
    original_total: u64,
) -> (Vec<SearchResult>, u64, Option<String>) {
    let original_len = original.len();
    if let Some(checker) = state.spell_checker.as_ref() {
        if let Some(corrected) = checker.did_you_mean(query) {
            if let Ok((merged, total)) =
                gather_results(state, &corrected, &corrected, limit, offset).await
            {
                if merged.len() > original_len {
                    return (merged, total, Some(corrected));
                }
            }
        }
    }
    if let Some(expanded) = state.synonyms.expand_query(query) {
        if let Ok((merged, total)) =
            gather_results(state, query, &expanded, limit, offset).await
        {
            if merged.len() > original_len {
                return (merged, total, None);
            }
        }
    }
    (original, original_total, None)
}

#[derive(Deserialize)]
pub(crate) struct SemanticSearchParams {
    q: String,
    limit: Option<usize>,
    offset: Option<usize>,
}

/// Local semantic search: embeds the query with the same zero-dependency
/// embedder used to build the index and ranks by cosine similarity.
pub(crate) async fn semantic_search_route(
    State(state): State<AppState>,
    Query(params): Query<SemanticSearchParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(engine) = state.semantic_engine.as_ref() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Semantic search is not configured",
        ));
    };
    let query = params.q.trim();
    let limit = params.limit.unwrap_or(10).clamp(1, 50);
    let offset = params.offset.unwrap_or(0);
    if query.chars().count() > 512 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Query too long"));
    }
    if offset > 1000 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Offset too large"));
    }
    if query.is_empty() {
        return Ok(Json(serde_json::json!({
            "results": [],
            "total_count": 0,
            "search_time_ms": 0.0,
            "has_more": false
        })));
    }
    let started = Instant::now();
    {
        let mut suggester = state.suggester.lock().unwrap_or_else(|e| e.into_inner());
        suggester.add_search_query(&normalize_text(query), None);
    }
    let wanted = limit.saturating_add(offset).min(200);
    let (hits, _) = engine.semantic_search_with_total(query, wanted);
    let hits: Vec<_> = hits
        .into_iter()
        .take_while(|(_, similarity)| f64::from(*similarity) >= state.semantic_min_score)
        .collect();
    let total = hits.len();
    let results = hits
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(document, similarity)| {
            serde_json::json!({
                "url": document.url,
                "title": document.title,
                "content": make_snippet(&document.content, query, 220),
                "score": similarity
            })
        })
        .collect::<Vec<_>>();
    let returned = results.len();
    Ok(Json(serde_json::json!({
        "query": query,
        "results": results,
        "total_count": total,
        "search_time_ms": started.elapsed().as_secs_f64() * 1000.0,
        "has_more": total > offset.saturating_add(returned)
    })))
}

pub(crate) async fn public_search_route(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<PublicSearchResponse>, ApiError> {
    let query = params.q.trim();
    let limit = params.limit.unwrap_or(20);
    let offset = params.offset.or(params.skip).unwrap_or(0);
    if query.is_empty() || query.chars().count() > 512 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Query cannot be empty or exceed 512 characters",
        ));
    }
    if !(1..=100).contains(&limit) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Invalid pagination values",
        ));
    }
    if offset > 1000 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Offset too large",
        ));
    }
    let start = Instant::now();
    let (results, total_count, corrected_query) =
        public_search(&state, query, limit, offset).await?;
    Ok(Json(PublicSearchResponse {
        query: query.to_owned(),
        corrected_query,
        results,
        total_count,
        search_time_ms: (start.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0,
        offset,
    }))
}

#[derive(Deserialize)]
pub(crate) struct SuggestionParams {
    q: String,
    limit: Option<usize>,
}

pub(crate) async fn suggestions_route(
    State(state): State<AppState>,
    Query(params): Query<SuggestionParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let query = normalize_text(&params.q);
    let limit = params.limit.unwrap_or(8);
    if params.q.chars().count() > 128 || !(1..=20).contains(&limit) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Invalid suggestion request",
        ));
    }
    let Some(term) = query.split_whitespace().last() else {
        return Ok(Json(
            serde_json::json!({"query": params.q, "suggestions": []}),
        ));
    };
    let stem = query
        .strip_suffix(term)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let raw_term = params.q.split_whitespace().last().unwrap_or_default();
    let fixed_term = state.spell_checker.as_ref().and_then(|checker| {
        checker
            .fix_raw_token(raw_term)
            .or_else(|| checker.fix_token(term))
    });

    // Tiers: 0 = popular queries for the corrected spelling, 1 = popular
    // queries that start with the full typed input, 2 = content-word
    // completions, 3 = recent queries, 4 = page titles. Raw titles carry a
    // nominal score of 1 and corrected-spelling titles score 0, so corrected
    // titles rank after the raw ones. Items carry (tier, score, text) and
    // rank by tier, then popularity, then text.
    let mut ranked: Vec<(u8, u64, String)> = Vec::new();
    {
        let suggester = state.suggester.lock().unwrap_or_else(|e| e.into_inner());
        let popular = suggester.get_popular_queries(200);
        if let Some(fixed) = &fixed_term {
            let corrected_full = if stem.is_empty() {
                fixed.clone()
            } else {
                format!("{stem} {fixed}")
            };
            for item in &popular {
                if item.query.starts_with(&corrected_full) {
                    ranked.push((0, item.count, item.query.clone()));
                }
            }
        }
        for item in &popular {
            if item.query.starts_with(query.as_str()) {
                ranked.push((1, item.count, item.query.clone()));
            }
        }
        let mut push_completions = |prefix: &str, tier: u8| {
            for suggestion in suggester.get_content_suggestions(prefix, None) {
                let completed = if stem.is_empty() {
                    suggestion.word.clone()
                } else {
                    format!("{stem} {}", suggestion.word)
                };
                ranked.push((tier, suggestion.score, completed));
            }
        };
        push_completions(term, 2);
        if let Some(fixed) = &fixed_term {
            push_completions(fixed, 2);
        }
    }
    {
        let recent = state.recent_queries.lock().unwrap_or_else(|e| e.into_inner());
        for recent_query in recent.iter().rev() {
            let normalized_recent = normalize_text(recent_query);
            if normalized_recent.starts_with(&query)
                || normalized_recent
                    .split_whitespace()
                    .next_back()
                    .is_some_and(|token| token.starts_with(term))
            {
                ranked.push((3, 0, recent_query.clone()));
            }
        }
    }
    // Page titles are the lowest tier, so they can never enter the final page
    // when the higher tiers already fill the limit. Skipping the cloud lookups
    // in that case keeps keystroke suggestions fast.
    let unique_ranked = ranked
        .iter()
        .map(|(_, _, text)| text.to_lowercase())
        .collect::<std::collections::HashSet<_>>()
        .len();
    if unique_ranked < limit {
        let raw_titles = async {
            match tokio::time::timeout(
                Duration::from_millis(1_200),
                fetch_title_suggestions(&state, term, limit),
            )
            .await
            {
                Ok(Ok(titles)) => titles
                    .into_iter()
                    .map(|title| (4, 1, title))
                    .collect::<Vec<(u8, u64, String)>>(),
                Ok(Err(error)) => {
                    eprintln!("Title suggestions unavailable: {}", error.message);
                    Vec::new()
                }
                Err(_) => {
                    eprintln!("Title suggestions timed out");
                    Vec::new()
                }
            }
        };
        if let Some(fixed) = &fixed_term {
            let corrected_titles = async {
                match tokio::time::timeout(
                    Duration::from_millis(1_200),
                    fetch_title_suggestions(&state, fixed, limit),
                )
                .await
                {
                    Ok(Ok(titles)) => titles
                        .into_iter()
                        .map(|title| (4, 0, title))
                        .collect::<Vec<(u8, u64, String)>>(),
                    Ok(Err(error)) => {
                        eprintln!("Corrected title suggestions unavailable: {}", error.message);
                        Vec::new()
                    }
                    Err(_) => {
                        eprintln!("Corrected title suggestions timed out");
                        Vec::new()
                    }
                }
            };
            let (raw, corrected) = tokio::join!(raw_titles, corrected_titles);
            ranked.extend(raw);
            ranked.extend(corrected);
        } else {
            ranked.extend(raw_titles.await);
        }
    }
    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut seen = std::collections::HashSet::new();
    let suggestions: Vec<String> = ranked
        .into_iter()
        .filter(|(_, _, text)| seen.insert(text.to_lowercase()))
        .map(|(_, _, text)| sanitize_suggestion(&text))
        .take(limit)
        .collect();
    Ok(Json(
        serde_json::json!({"query": params.q, "suggestions": suggestions}),
    ))
}

/// Title-prefix suggestions from MongoDB and TiDB for one term, deduplicated
/// within the pair of stores. Both stores are queried concurrently so a slow
/// store does not add its latency to the other.
async fn fetch_title_suggestions(
    state: &AppState,
    term: &str,
    limit: usize,
) -> Result<Vec<String>, ApiError> {
    let mongo_titles = async {
        let mut titles = Vec::new();
        if let Some(collection) = &state.mongo_collection {
            match collection
                .find(doc! { "title": { "$regex": format!("^{}", escape(term)), "$options": "i" } })
                .projection(doc! { "title": 1, "_id": 0 })
                .sort(doc! { "crawled_at": -1 })
                .limit(limit as i64)
                .await
            {
                Ok(cursor) => match cursor.try_collect::<Vec<Document>>().await {
                    Ok(rows) => {
                        let mut seen = std::collections::HashSet::new();
                        for row in rows {
                            if let Ok(title) = row.get_str("title") {
                                if seen.insert(title.to_lowercase()) {
                                    titles.push(title.to_owned());
                                }
                            }
                        }
                    }
                    Err(error) => eprintln!("MongoDB title suggestions unavailable: {error}"),
                },
                Err(error) => eprintln!("MongoDB title suggestions unavailable: {error}"),
            }
        }
        titles
    };
    let tidb_titles = async {
        let mut titles = Vec::new();
        if let Some(pool) = &state.tidb_pool {
            match pool.get_conn().await {
                Ok(mut connection) => match connection
                    .exec::<String, _, _>(
                        format!("SELECT DISTINCT title FROM `{DOCUMENTS_TABLE}` WHERE title LIKE ? AND title<>'' ORDER BY title LIMIT ?"),
                        (mysql_prefix_pattern(term), limit as u64),
                    )
                    .await
                {
                    Ok(rows) => titles = rows,
                    Err(error) => eprintln!("TiDB title suggestions unavailable: {error}"),
                },
                Err(error) => eprintln!("TiDB title suggestions unavailable: {error}"),
            }
        }
        titles
    };
    let (mongo_titles, tidb_titles) = tokio::join!(mongo_titles, tidb_titles);
    let mut titles = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for title in mongo_titles.into_iter().chain(tidb_titles) {
        if seen.insert(title.to_lowercase()) {
            titles.push(title);
        }
    }
    titles.truncate(limit);
    Ok(titles)
}

#[derive(Deserialize)]
pub(crate) struct AdvancedSearchParams {
    q: String,
    limit: Option<usize>,
    country: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct AdvancedSearchResult {
    id: String,
    title: String,
    url: String,
    description: String,
    score: f64,
    #[serde(rename = "type")]
    result_type: &'static str,
}

#[derive(Serialize)]
pub(crate) struct AdvancedSearchResponse {
    query: String,
    total_results: usize,
    results: Vec<AdvancedSearchResult>,
    featured_snippet: Option<serde_json::Value>,
    knowledge_panel: Option<serde_json::Value>,
    ai_overview: Option<serde_json::Value>,
    search_time_ms: f64,
}

pub(crate) async fn api_root() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "message": "VELO Search API",
        "version": "3.0.0",
        "status": "running",
        "endpoints": {
            "search": "/api/search?q=query",
            "suggestions": "/api/suggestions?q=query",
            "stats": "/api/stats"
        }
    }))
}

pub(crate) async fn advanced_search_route(
    State(state): State<AppState>,
    Query(params): Query<AdvancedSearchParams>,
) -> Result<Json<AdvancedSearchResponse>, ApiError> {
    let query = params.q.trim();
    let limit = params.limit.unwrap_or(20);
    if query.is_empty() || query.chars().count() > 512 || !(1..=100).contains(&limit) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Invalid search request",
        ));
    }
    let start = Instant::now();
    let (results, _, _) = public_search(&state, query, limit, 0).await?;
    let formatted = results
        .into_iter()
        .map(|result| AdvancedSearchResult {
            id: result.url.clone(),
            title: if result.title.is_empty() {
                "Untitled".to_owned()
            } else {
                result.title
            },
            url: result.url,
            description: result.content,
            score: result.score,
            result_type: "page",
        })
        .collect::<Vec<_>>();
    let _country_filter = params.country;
    Ok(Json(AdvancedSearchResponse {
        query: query.to_owned(),
        total_results: formatted.len(),
        results: formatted,
        featured_snippet: featured_snippet(query),
        knowledge_panel: knowledge_panel(query),
        ai_overview: ai_overview(query),
        search_time_ms: (start.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0,
    }))
}

pub(crate) async fn api_suggestions_route(
    State(state): State<AppState>,
    Query(params): Query<SuggestionParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    suggestions_route(State(state), Query(params)).await
}

pub(crate) async fn api_stats_route(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let stats = stats_route(State(state)).await?.0;
    Ok(Json(serde_json::json!({
        "total_universities": stats["total_universities"],
        "total_pages": stats["total_pages"],
        "indexed_documents": stats["documents"],
        "api_version": "3.0.0"
    })))
}

pub(crate) async fn stats_route(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut tidb_documents = 0u64;
    let mut tidb_universities = 0u64;
    let mut queue = serde_json::Map::new();
    let mut queued_urls = 0u64;
    if let Some(pool) = &state.tidb_pool {
        let mut connection = pool
            .get_conn()
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
        tidb_documents = connection
            .query_first::<u64, _>(format!("SELECT COUNT(*) FROM `{DOCUMENTS_TABLE}`"))
            .await
            .map_err(|error| ApiError::internal(error.into()))?
            .unwrap_or_default();
        tidb_universities = connection
            .exec_first::<u64, _, _>(
                format!(
                    "SELECT COUNT(*) FROM `{DOCUMENTS_TABLE}` WHERE source_metadata_json LIKE ?"
                ),
                ("%\"type\":\"university\"%".to_owned(),),
            )
            .await
            .map_err(|error| ApiError::internal(error.into()))?
            .unwrap_or_default();
        let queue_rows = connection
            .query::<Row, _>("SELECT status,COUNT(*) AS n FROM `crawl_queue` GROUP BY status")
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
        for row in queue_rows {
            let status = row
                .get::<Option<String>, _>("status")
                .flatten()
                .unwrap_or_default();
            let count = row.get::<Option<u64>, _>("n").flatten().unwrap_or_default();
            if matches!(status.as_str(), "queued" | "processing") {
                queued_urls = queued_urls.saturating_add(count);
            }
            queue.insert(status, serde_json::json!(count));
        }
    } else if let Some(queue_collection) = &state.mongo_queue_collection {
        let mut cursor = queue_collection
            .aggregate(vec![doc! {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }
            }])
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
        while let Some(row) = cursor
            .try_next()
            .await
            .map_err(|error| ApiError::internal(error.into()))?
        {
            let status = row.get_str("_id").unwrap_or_default().to_owned();
            let count = row.get_i64("count").unwrap_or_default().max(0) as u64;
            if matches!(status.as_str(), "queued" | "processing") {
                queued_urls = queued_urls.saturating_add(count);
            }
            queue.insert(status, serde_json::json!(count));
        }
    }
    let mut mongo_documents = 0;
    if let Some(collection) = &state.mongo_collection {
        mongo_documents = collection
            .count_documents(doc! {})
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
    }
    let mongo_universities = if let Some(collection) = &state.mongo_collection {
        collection
            .count_documents(doc! { "source_metadata.type": "university" })
            .await
            .map_err(|error| ApiError::internal(error.into()))?
    } else {
        0
    };
    let total = tidb_documents.saturating_add(mongo_documents);
    let total_universities = tidb_universities.saturating_add(mongo_universities);
    let database_name = if state.search_backend == "mongodb" {
        env::var("MONGO_DATABASE")
            .or_else(|_| env::var("MONGO_DB"))
            .unwrap_or_else(|_| "mongodb".to_owned())
    } else {
        env::var("TIDB_DATABASE")
            .or_else(|_| env::var("VELO_TIDB_DATABASE"))
            .unwrap_or_else(|_| "velo_search".to_owned())
    };
    let collection_name = if state.search_backend == "mongodb" {
        env::var("MONGO_SEARCH_COLLECTION")
            .or_else(|_| env::var("MONGO_COLLECTION"))
            .unwrap_or_else(|_| "Crawled_Pages".to_owned())
    } else {
        DOCUMENTS_TABLE.to_owned()
    };
    Ok(Json(serde_json::json!({
        "documents": total,
        "tidb_documents": tidb_documents,
        "mongo_documents": mongo_documents,
        "total_universities": total_universities,
        "queued_urls": queued_urls,
        "crawl_queue": queue,
        "mongo_partition_percent": state.mongo_partition_percent,
        "backend": state.search_backend,
        "database": database_name,
        "total_pages": total,
        "db_name": database_name,
        "collection_name": collection_name,
        "mongo_status": if state.mongo_collection.is_some() { "connected" } else { "unconfigured" }
    })))
}

pub(crate) async fn admin_dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    let active_backend = state.search_backend.clone();
    let tidb_status = if state.tidb_pool.is_some() {
        "connected"
    } else {
        "unconfigured"
    };
    let vector_status = match state.semantic_engine.as_ref() {
        Some(engine) if engine.count_documents() > 0 => {
            serde_json::json!({"status": "ready", "indexed_documents": engine.count_documents()})
        }
        Some(_) => serde_json::json!({"status": "empty"}),
        None => serde_json::json!({"status": "disabled"}),
    };
    let pagerank_status = if state.pagerank_scores.is_empty() {
        serde_json::json!({"status": "unscored"})
    } else {
        serde_json::json!({
            "status": "ready",
            "scored_pages": state.pagerank_scores.len(),
            "weight": state.pagerank_weight
        })
    };
    let spellcheck_status = match state.spell_checker.as_ref() {
        Some(checker) => serde_json::json!({
            "status": "ready",
            "vocabulary_size": checker.vocabulary_size()
        }),
        None => serde_json::json!({"status": "unavailable"}),
    };
    let search_index = match stats_route(State(state)).await {
        Ok(response) => response.0,
        Err(error) => {
            eprintln!("Stats unavailable: {}", error.message);
            serde_json::json!({})
        }
    };
    let mongo_status = search_index
        .get("mongo_status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unconfigured");
    let tidb_pages = search_index
        .get("tidb_documents")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let mongo_pages = search_index
        .get("mongo_documents")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    Ok(Json(serde_json::json!({
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        "active_backend": active_backend,
        "search_index": search_index,
        "databases": {
            "tidb": {"status": tidb_status, "pages": tidb_pages, "writable": tidb_status == "connected"},
            "mongodb": {"status": mongo_status, "pages": mongo_pages},
            "vector": vector_status,
            "pagerank": pagerank_status,
            "spellcheck": spellcheck_status
        }
    })))
}

pub(crate) async fn upload_documents(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(documents): Json<Vec<IndexDocumentRequest>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    if documents.len() > 1_000 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Upload batch cannot exceed 1000 documents",
        ));
    }
    let mut uploaded = 0usize;
    let mut errors = 0usize;
    for document in documents {
        match stage_document(&state, document).await {
            Ok(_) => uploaded += 1,
            Err(error) => {
                eprintln!("Bulk document upload failed: {}", error.message);
                errors += 1;
            }
        }
    }
    crate::ingest::commit_tantivy(&state);
    let stats = match stats_route(State(state)).await {
        Ok(response) => response.0,
        Err(error) => {
            eprintln!("Stats unavailable: {}", error.message);
            serde_json::json!({})
        }
    };
    Ok(Json(serde_json::json!({
        "uploaded": uploaded,
        "errors": errors,
        "total_in_cloud": stats
            .get("documents")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    })))
}

pub(crate) async fn add_cors_headers(request: axum::extract::Request, next: Next) -> Response {
    let mut response = if request.method() == Method::OPTIONS {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("*"),
    );
    response
}

pub(crate) async fn health() -> &'static str {
    "ok"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(url: &str, score: f64) -> SearchResult {
        SearchResult {
            url: url.to_owned(),
            title: String::new(),
            description: String::new(),
            content: String::new(),
            score,
            favicon: None,
            source: "test",
        }
    }

    #[test]
    fn pagerank_lookup_falls_back_to_trimmed_and_lowercase_urls() {
        let mut scores = HashMap::new();
        scores.insert("https://example.com/page".to_owned(), 80.0);
        assert_eq!(
            pagerank_for(&scores, "https://example.com/page"),
            Some(80.0)
        );
        assert_eq!(
            pagerank_for(&scores, "https://example.com/page/"),
            Some(80.0)
        );
        assert_eq!(
            pagerank_for(&scores, "https://EXAMPLE.com/page"),
            Some(80.0)
        );
        assert_eq!(pagerank_for(&scores, "https://example.com/other"), None);
    }

    #[test]
    fn authority_boost_only_applies_when_configured() {
        let mut scores = HashMap::new();
        scores.insert("https://a.test/".to_owned(), 100.0);
        let mut results = vec![result("https://a.test/", 1.0), result("https://b.test/", 2.0)];

        apply_authority_boost(&mut results, &scores, 0.0);
        assert_eq!(results[0].score, 1.0);

        apply_authority_boost(&mut results, &scores, 5.0);
        assert!((results[0].score - 6.0).abs() < 1e-9);
        assert!((results[1].score - 2.0).abs() < 1e-9);
    }

    #[test]
    fn empty_score_table_is_a_no_op() {
        let scores: HashMap<String, f64> = HashMap::new();
        let mut results = vec![result("https://a.test/", 1.0)];
        apply_authority_boost(&mut results, &scores, 5.0);
        assert_eq!(results[0].score, 1.0);
    }

    #[test]
    fn trusted_domains_get_a_small_ranking_boost() {
        assert_eq!(
            domain_trust_boost("https://www.harvard.edu/"),
            ACADEMIC_DOMAIN_BOOST
        );
        assert_eq!(
            domain_trust_boost("https://www.ox.ac.uk/about"),
            ACADEMIC_DOMAIN_BOOST
        );
        assert_eq!(
            domain_trust_boost("https://www.cu.edu.eg/"),
            ACADEMIC_DOMAIN_BOOST
        );
        assert_eq!(
            domain_trust_boost("https://www.nasa.gov/"),
            GOVERNMENT_DOMAIN_BOOST
        );
        assert_eq!(domain_trust_boost("https://www.metacritic.com/"), 0.0);
        assert_eq!(domain_trust_boost("not a url"), 0.0);
    }

    #[test]
    fn academic_domain_overtakes_a_slightly_higher_entertainment_score() {
        let mut results = vec![
            result("https://www.metacritic.com/browse/movie/paramount-plus/", 16.05),
            result("https://www.ox.ac.uk/about/university-officers/", 15.85),
        ];
        apply_domain_trust_boost(&mut results);
        results.sort_by(|left, right| right.score.total_cmp(&left.score));
        assert_eq!(results[0].url, "https://www.ox.ac.uk/about/university-officers/");
    }

    #[test]
    fn reported_total_is_not_floored_to_one() {
        assert_eq!(reported_total(0, 0), 0);
        assert_eq!(reported_total(0, 2), 2);
        assert_eq!(reported_total(50, 2), 50);
    }

    #[test]
    fn sanitize_suggestion_strips_angle_brackets() {
        assert_eq!(sanitize_suggestion("<script>alert(1)</script>"), "scriptalert(1)/script");
        assert_eq!(sanitize_suggestion("جامعة القاهرة"), "جامعة القاهرة");
    }

    #[test]
    fn popular_tiers_match_full_input_not_last_token() {
        // The tier matcher must compare the full typed input, so a multi-word
        // input only completes queries that actually start with it.
        let full_input = "rust sea";
        assert!("rust sea fishing".starts_with(full_input));
        assert!(!"sea fishing".starts_with(full_input));
    }
}
