use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use axum::{extract::State, http::StatusCode, Json};

use crate::error::ApiError;
use crate::mongo::search_mongo;
use crate::state::{AppState, SearchRequest, SearchResponse};
use crate::textops::normalize_text;
use crate::tidb::search_tidb;

pub(crate) fn cached_search(
    state: &AppState,
    key: &(String, usize, usize, bool),
) -> Result<Option<SearchResponse>, ApiError> {
    let mut cache = state
        .search_cache
        .lock()
        .map_err(|_| ApiError::internal(anyhow!("Search cache lock is poisoned")))?;
    let Some((expires_at, response)) = cache.entries.get(key) else {
        return Ok(None);
    };
    if *expires_at <= Instant::now() {
        cache.entries.remove(key);
        cache.lru.retain(|entry| entry != key);
        return Ok(None);
    }
    let response = response.clone();
    cache.lru.retain(|entry| entry != key);
    cache.lru.push_back(key.clone());
    Ok(Some(response))
}

pub(crate) fn cache_search(
    state: &AppState,
    key: (String, usize, usize, bool),
    response: SearchResponse,
) -> Result<(), ApiError> {
    let mut cache = state
        .search_cache
        .lock()
        .map_err(|_| ApiError::internal(anyhow!("Search cache lock is poisoned")))?;
    cache.lru.retain(|entry| entry != &key);
    cache.entries.insert(
        key.clone(),
        (Instant::now() + Duration::from_secs(30), response),
    );
    cache.lru.push_back(key);
    while cache.entries.len() > 128 {
        if let Some(oldest) = cache.lru.pop_front() {
            cache.entries.remove(&oldest);
        }
    }
    Ok(())
}

pub(crate) fn invalidate_search_cache(state: &AppState) -> Result<(), ApiError> {
    let mut cache = state
        .search_cache
        .lock()
        .map_err(|_| ApiError::internal(anyhow!("Search cache lock is poisoned")))?;
    cache.entries.clear();
    cache.lru.clear();
    drop(cache);
    // The response-level cache is derived from the same stores, so every
    // existing invalidation point (admin.rs, ingest.rs) clears it too.
    let mut responses = state
        .response_cache
        .lock()
        .map_err(|_| ApiError::internal(anyhow!("Response cache lock is poisoned")))?;
    responses.invalidate_all();
    Ok(())
}

pub(crate) async fn federated_search(
    state: &AppState,
    request: SearchRequest,
) -> Result<SearchResponse, ApiError> {
    let normalized = normalize_text(&request.normalized);
    if normalized.is_empty()
        || normalized.chars().count() > 512
        || !(1..=100_100).contains(&request.limit)
        || request.offset > 100_000
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Invalid search request",
        ));
    }
    let key = (
        normalized.clone(),
        request.limit,
        request.offset,
        request.fulltext_enabled,
    );
    if let Some(response) = cached_search(state, &key)? {
        return Ok(response);
    }
    let terms: Vec<String> = normalized.split_whitespace().map(str::to_owned).collect();
    let tidb_future = async {
        match &state.tidb_pool {
            Some(pool) => {
                search_tidb(
                    pool.clone(),
                    &normalized,
                    request.limit,
                    request.offset,
                    request.fulltext_enabled,
                )
                .await
            }
            None => Ok((Vec::new(), 0)),
        }
    };
    let mongo_future = search_mongo(
        state.mongo_collection.clone(),
        state.mongo_text_index.load(Ordering::Relaxed),
        &terms,
        request.limit,
        request.offset,
    );
    let (tidb, (mongo_results, mongo_total)) = tokio::join!(tidb_future, mongo_future);
    let (tidb_results, tidb_total) = tidb.map_err(ApiError::internal)?;
    let response = SearchResponse {
        tidb_results,
        tidb_total,
        mongo_results,
        mongo_total,
    };
    cache_search(state, key, response.clone())?;
    Ok(response)
}

pub(crate) async fn internal_search(
    State(state): State<AppState>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    match tokio::time::timeout(
        Duration::from_secs(5),
        federated_search(&state, request),
    )
    .await
    {
        Ok(result) => result.map(Json),
        Err(_) => Err(ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "Search backends timed out",
        )),
    }
}
