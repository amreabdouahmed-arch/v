use std::{
    sync::atomic::Ordering,
    time::Duration,
};

use anyhow::anyhow;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use mongodb::{bson::doc, options::IndexOptions, IndexModel};
use serde::Deserialize;

use crate::error::ApiError;
use crate::ingest::{
    commit_tantivy, enqueue_urls, stage_document, write_document, ApiCrawlQueue, ApiCrawlerWriter,
    IndexDocumentRequest,
};
use crate::ported;
use crate::search::invalidate_search_cache;
use crate::state::{project_root, AppState};
use crate::textops::canonicalize_url;

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

pub(crate) fn require_admin(headers: &HeaderMap) -> Result<(), ApiError> {
    let expected = std::env::var("ADMIN_TOKEN").unwrap_or_default();
    let provided = headers
        .get("x-admin-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if expected.is_empty() || !constant_time_equal(expected.as_bytes(), provided.as_bytes()) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "Admin token is missing or invalid",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
pub(crate) struct CrawlSeedsRequest {
    urls: Vec<String>,
}

#[derive(Deserialize)]
pub(crate) struct CrawlRunRequest {
    #[serde(default = "default_crawl_run_limit")]
    max_pages: usize,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    urls: Vec<String>,
}

fn default_crawl_run_limit() -> usize {
    20
}

#[derive(Deserialize)]
pub(crate) struct LegacyImportRequest {
    paths: Vec<String>,
}

pub(crate) async fn index_document_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<IndexDocumentRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    let url = write_document(&state, input).await?;
    Ok(Json(serde_json::json!({"status": "indexed", "url": url})))
}

pub(crate) async fn rebuild_index_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    let Some(collection) = &state.mongo_collection else {
        if state.tidb_fulltext_enabled {
            return Ok(Json(serde_json::json!({
                "message": "TiDB full-text index is ready",
                "status": "success"
            })));
        }
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "No cloud full-text index is available",
        ));
    };
    let options = IndexOptions::builder()
        .name("velo_search_text".to_owned())
        .weights(doc! { "title": 5, "description": 2, "content": 1 })
        .default_language("none".to_owned())
        .language_override("velo_search_language".to_owned())
        .build();
    let model = IndexModel::builder()
        .keys(doc! { "title": "text", "description": "text", "content": "text" })
        .options(options)
        .build();
    collection
        .create_index(model)
        .await
        .map_err(|error| ApiError::internal(error.into()))?;
    state.mongo_text_index.store(true, Ordering::Relaxed);
    invalidate_search_cache(&state)?;
    Ok(Json(serde_json::json!({
        "message": "Cloud text index is ready",
        "status": "success"
    })))
}

pub(crate) async fn admin_popular_searches(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    let popular = state
        .suggester
        .lock()
        .map(|suggester| suggester.get_popular_queries(20))
        .unwrap_or_default();
    Ok(Json(serde_json::json!({
        "popular_searches": popular
            .iter()
            .map(|item| serde_json::json!({"query": item.query, "count": item.count}))
            .collect::<Vec<_>>()
    })))
}

pub(crate) async fn add_crawl_seeds(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CrawlSeedsRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    if request.urls.is_empty() || request.urls.len() > 1_000 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Provide between 1 and 1000 seed URLs",
        ));
    }
    let queued = enqueue_urls(&state, &request.urls, 0, true).await?;
    Ok(Json(serde_json::json!({"queued": queued})))
}

async fn run_crawl_profile(
    writer: &ApiCrawlerWriter,
    kind: ported::crawler_ingestion::CrawlerKind,
    max_pages: usize,
    urls: &[String],
) -> Result<ported::crawler_ingestion::CrawlReport, String> {
    use ported::crawler_ingestion::{CrawlerConfig, Seed};
    let mut config = match kind {
        ported::crawler_ingestion::CrawlerKind::Advanced => CrawlerConfig::advanced(),
        ported::crawler_ingestion::CrawlerKind::Arabic => CrawlerConfig::arabic(),
        ported::crawler_ingestion::CrawlerKind::Universities => CrawlerConfig::universities(),
        ported::crawler_ingestion::CrawlerKind::Global => CrawlerConfig::global(),
        ported::crawler_ingestion::CrawlerKind::SearchEngine => CrawlerConfig::search_engine(),
    };
    config.max_pages = max_pages;
    let seeds = if urls.is_empty() {
        ported::crawler_ingestion::seed_catalog(kind)
    } else {
        urls.iter().map(|url| Seed::new(url, "custom")).collect()
    };
    ported::crawler_ingestion::crawl(config, seeds, writer).await
}

async fn run_crawl_profile_after(
    writer: &ApiCrawlerWriter,
    kind: ported::crawler_ingestion::CrawlerKind,
    max_pages: usize,
    urls: &[String],
    startup_delay: Duration,
) -> Result<ported::crawler_ingestion::CrawlReport, String> {
    tokio::time::sleep(startup_delay).await;
    run_crawl_profile(writer, kind, max_pages, urls).await
}

pub(crate) async fn run_crawler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CrawlRunRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    if !(1..=10_000).contains(&request.max_pages) || request.urls.len() > 1_000 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "max_pages must be 1..10000 and at most 1000 URLs may be supplied",
        ));
    }

    let writer = ApiCrawlerWriter {
        state: state.clone(),
    };
    let kind = if request.kind.trim().is_empty() {
        "search_engine".to_owned()
    } else {
        request.kind.trim().to_ascii_lowercase()
    };
    match kind.as_str() {
        "search_engine" | "search" => {
            if !request.urls.is_empty() {
                enqueue_urls(&state, &request.urls, 0, false).await?;
            }
            let mut queue = ApiCrawlQueue { state };
            let stats = ported::crawler_ingestion::run_search_queue(&mut queue, request.max_pages)
                .await
                .map_err(|error| ApiError::internal(anyhow!(error)))?;
            Ok(Json(serde_json::json!({
                "kind": "search_engine",
                "stats": stats
            })))
        }
        "advanced" | "arabic" | "global" | "universities" => {
            let crawler_kind = match kind.as_str() {
                "advanced" => ported::crawler_ingestion::CrawlerKind::Advanced,
                "arabic" => ported::crawler_ingestion::CrawlerKind::Arabic,
                "global" => ported::crawler_ingestion::CrawlerKind::Global,
                _ => ported::crawler_ingestion::CrawlerKind::Universities,
            };
            let report = run_crawl_profile(&writer, crawler_kind, request.max_pages, &request.urls)
                .await
                .map_err(|error| ApiError::internal(anyhow!(error)))?;
            Ok(Json(serde_json::json!({
                "kind": kind,
                "stats": report.stats,
                "domains": report.domains
            })))
        }
        "all" => {
            let (arabic, global, universities, advanced) = tokio::try_join!(
                run_crawl_profile_after(
                    &writer,
                    ported::crawler_ingestion::CrawlerKind::Arabic,
                    request.max_pages,
                    &request.urls,
                    Duration::ZERO
                ),
                run_crawl_profile_after(
                    &writer,
                    ported::crawler_ingestion::CrawlerKind::Global,
                    request.max_pages,
                    &request.urls,
                    Duration::from_secs(2)
                ),
                run_crawl_profile_after(
                    &writer,
                    ported::crawler_ingestion::CrawlerKind::Universities,
                    request.max_pages,
                    &request.urls,
                    Duration::from_secs(4)
                ),
                run_crawl_profile_after(
                    &writer,
                    ported::crawler_ingestion::CrawlerKind::Advanced,
                    request.max_pages,
                    &request.urls,
                    Duration::from_secs(6)
                )
            )
            .map_err(|error| ApiError::internal(anyhow!(error)))?;
            Ok(Json(serde_json::json!({
                "kind": "all",
                "reports": [
                    {"kind": "advanced", "stats": advanced.stats, "domains": advanced.domains},
                    {"kind": "arabic", "stats": arabic.stats, "domains": arabic.domains},
                    {"kind": "global", "stats": global.stats, "domains": global.domains},
                    {"kind": "universities", "stats": universities.stats, "domains": universities.domains}
                ]
            })))
        }
        _ => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "kind must be search_engine, advanced, arabic, global, universities, or all",
        )),
    }
}

pub(crate) async fn import_legacy(
    headers: HeaderMap,
    Json(request): Json<LegacyImportRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    if request.paths.is_empty() || request.paths.len() > 100 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Provide between 1 and 100 read-only SQLite migration paths",
        ));
    }
    let imported = ported::import_legacy_files(&request.paths)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(serde_json::json!({
        "imported": imported,
        "target": "TiDB Cloud search_documents"
    })))
}

pub(crate) async fn import_universities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&headers)?;
    let contents =
        std::fs::read_to_string(project_root().join("universities.json")).map_err(|error| {
            ApiError::internal(anyhow!(error).context("Could not read universities.json"))
        })?;
    let entries: Vec<serde_json::Value> =
        serde_json::from_str(&contents).map_err(|error| ApiError::internal(error.into()))?;
    let mut imported = 0usize;
    for row in entries {
        let Some(row) = row.as_object() else {
            continue;
        };
        let url = row
            .get("main_url")
            .or_else(|| row.get("url"))
            .and_then(serde_json::Value::as_str);
        let Some(url) = url.filter(|url| !url.is_empty()) else {
            continue;
        };
        let title = row
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let country = row
            .get("country")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let description = row
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if canonicalize_url(url).is_err() {
            continue;
        }
        stage_document(
            &state,
            IndexDocumentRequest {
                url: url.to_owned(),
                title: title.to_owned(),
                content: format!("{country} {description}"),
                description: description.to_owned(),
                language: String::new(),
                source_metadata: Some(
                    serde_json::json!({"type": "university", "country": country}),
                ),
                source: None,
                status_code: 200,
                crawled_at: None,
            },
        )
        .await?;
        imported += 1;
    }
    commit_tantivy(&state);
    Ok(Json(serde_json::json!({"imported": imported})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_token_comparison_checks_full_values() {
        assert!(constant_time_equal(b"admin-secret", b"admin-secret"));
        assert!(!constant_time_equal(b"admin-secret", b"admin-secretx"));
        assert!(!constant_time_equal(b"admin-secret", b"other-secret"));
    }
}
