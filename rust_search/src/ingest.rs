use axum::http::StatusCode;
use mongodb::bson::doc;
use mysql_async::{prelude::Queryable, Row, Value};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::error::ApiError;
use crate::ported;
use crate::search::invalidate_search_cache;
use crate::state::{AppState, DOCUMENTS_TABLE};
use crate::textops::{canonicalize_url, normalize_text, truncate_chars};

#[derive(Deserialize)]
pub(crate) struct IndexDocumentRequest {
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) content: String,
    #[serde(default)]
    pub(crate) description: String,
    #[serde(default)]
    pub(crate) language: String,
    #[serde(default)]
    pub(crate) source_metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) source: Option<String>,
    #[serde(default = "default_status_code")]
    pub(crate) status_code: u16,
    #[serde(default)]
    pub(crate) crawled_at: Option<f64>,
}

fn default_status_code() -> u16 {
    200
}

pub(crate) async fn write_document(
    state: &AppState,
    input: IndexDocumentRequest,
) -> Result<String, ApiError> {
    write_document_inner(state, input, true).await
}

/// Same as `write_document`, but leaves the Tantivy commit to the caller so
/// batch writers can stage many documents and commit exactly once.
pub(crate) async fn stage_document(
    state: &AppState,
    input: IndexDocumentRequest,
) -> Result<String, ApiError> {
    write_document_inner(state, input, false).await
}

/// Commits every previously staged Tantivy document in one operation.
pub(crate) fn commit_tantivy(state: &AppState) {
    if let Some(engine) = &state.tantivy_engine {
        if let Err(error) = engine.commit() {
            eprintln!("Could not commit the fast index: {error}");
        }
    }
}

async fn write_document_inner(
    state: &AppState,
    input: IndexDocumentRequest,
    commit: bool,
) -> Result<String, ApiError> {
    let canonical = canonicalize_url(&input.url)
        .map_err(|error| ApiError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    if input.content.chars().count() > 1_000_000 {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Content exceeds 1 MB",
        ));
    }

    let title = truncate_chars(&input.title, 1_000);
    let description = truncate_chars(&input.description, 5_000);
    let content = truncate_chars(&input.content, 1_000_000);
    let language = truncate_chars(&input.language, 20);
    let status_code = if input.status_code == 0 {
        200
    } else {
        input.status_code
    };
    let host = Url::parse(&canonical)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_default();
    let searchable = normalize_text(&format!("{title} {title} {title} {description} {content}"));
    if let Some(engine) = &state.tantivy_engine {
        if let Err(error) = engine.add_document(&canonical, &title, &canonical, &content) {
            eprintln!("Could not add a document to the fast index: {error}");
        }
        if commit {
            if let Err(error) = engine.commit() {
                eprintln!("Could not commit the fast index: {error}");
            }
        }
    }
    let content_hash = format!(
        "{:x}",
        Sha256::digest(normalize_text(&format!("{title} {description} {content}")).as_bytes())
    );
    let url_hash = Sha256::digest(canonical.as_bytes()).to_vec();
    let now = input.crawled_at.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    });
    let mut bucket_bytes = [0u8; 8];
    bucket_bytes.copy_from_slice(&url_hash[..8]);
    let mongo_bucket = u64::from_be_bytes(bucket_bytes) % 100;
    let source_metadata = input.source_metadata.clone().or_else(|| {
        input
            .source
            .as_ref()
            .map(|source| serde_json::json!({"source": source}))
    });

    if mongo_bucket < u64::from(state.mongo_partition_percent) {
        if let Some(collection) = &state.mongo_collection {
            let mut document = doc! {
                "url": &canonical,
                "canonical_url": &canonical,
                "host": &host,
                "title": &title,
                "description": &description,
                "content": &content,
                "language": &language,
                "status_code": i32::from(status_code),
                "crawled_at": now,
                "content_hash": &content_hash,
            };
            if let Some(metadata) = source_metadata.clone() {
                document.insert(
                    "source_metadata",
                    mongodb::bson::to_bson(&metadata)
                        .map_err(|error| ApiError::internal(error.into()))?,
                );
            }
            match collection
                .update_one(doc! { "url": &canonical }, doc! { "$set": document })
                .upsert(true)
                .await
            {
                Ok(_) => {
                    if let Some(pool) = &state.tidb_pool {
                        match pool.get_conn().await {
                            Ok(mut connection) => {
                                if let Err(error) = connection
                                    .exec_drop(
                                        format!("DELETE FROM `{DOCUMENTS_TABLE}` WHERE url_hash=?"),
                                        (url_hash.clone(),),
                                    )
                                    .await
                                {
                                    eprintln!("Could not remove duplicate TiDB row after MongoDB upsert: {error}");
                                }
                            }
                            Err(error) => {
                                eprintln!(
                                    "Could not connect to TiDB to remove a duplicate row: {error}"
                                );
                            }
                        }
                    }
                    invalidate_search_cache(state)?;
                    return Ok(canonical);
                }
                Err(error) => {
                    if state.tidb_pool.is_none() {
                        return Err(ApiError::internal(error.into()));
                    }
                    eprintln!("MongoDB write failed; falling back to TiDB: {error}");
                }
            }
        }
    }

    let metadata_json = source_metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| ApiError::internal(error.into()))?;
    let statement = format!(
        "INSERT INTO `{DOCUMENTS_TABLE}` \
         (url_hash,url,canonical_url,host,title,description,content,search_text,language,status_code,crawled_at,content_hash,source_metadata_json) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) \
         ON DUPLICATE KEY UPDATE url=VALUES(url),canonical_url=VALUES(canonical_url),host=VALUES(host), \
         title=VALUES(title),description=VALUES(description),content=VALUES(content),search_text=VALUES(search_text), \
         language=VALUES(language),status_code=VALUES(status_code),crawled_at=VALUES(crawled_at), \
         content_hash=VALUES(content_hash),source_metadata_json=COALESCE(VALUES(source_metadata_json),source_metadata_json)"
    );
    let pool = state.tidb_pool.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "TiDB Cloud is not configured",
        )
    })?;
    let mut connection = pool
        .get_conn()
        .await
        .map_err(|error| ApiError::internal(error.into()))?;
    let params = vec![
        Value::Bytes(url_hash),
        Value::Bytes(canonical.as_bytes().to_vec()),
        Value::Bytes(canonical.as_bytes().to_vec()),
        Value::Bytes(host.into_bytes()),
        Value::Bytes(title.into_bytes()),
        Value::Bytes(description.into_bytes()),
        Value::Bytes(content.into_bytes()),
        Value::Bytes(searchable.into_bytes()),
        Value::Bytes(language.into_bytes()),
        Value::UInt(u64::from(status_code)),
        Value::Double(now),
        Value::Bytes(content_hash.into_bytes()),
        metadata_json
            .map(|value| Value::Bytes(value.into_bytes()))
            .unwrap_or(Value::NULL),
    ];
    connection
        .exec_drop(statement, mysql_async::Params::Positional(params))
        .await
        .map_err(|error| ApiError::internal(error.into()))?;
    invalidate_search_cache(state)?;
    Ok(canonical)
}

pub(crate) async fn cloud_document_exists(
    state: &AppState,
    raw_url: &str,
) -> Result<bool, ApiError> {
    let canonical = canonicalize_url(raw_url)
        .map_err(|error| ApiError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
    if let Some(collection) = &state.mongo_collection {
        match collection.find_one(doc! { "url": &canonical }).await {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(error) if state.tidb_pool.is_some() => {
                eprintln!("MongoDB existence check failed; checking TiDB instead: {error}");
            }
            Err(error) => return Err(ApiError::internal(error.into())),
        }
    }
    if let Some(pool) = &state.tidb_pool {
        let mut connection = pool
            .get_conn()
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
        let hash = Sha256::digest(canonical.as_bytes()).to_vec();
        let found: Option<i32> = connection
            .exec_first(
                format!("SELECT 1 FROM `{DOCUMENTS_TABLE}` WHERE url_hash=? LIMIT 1"),
                (hash,),
            )
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
        if found.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone)]
pub(crate) struct ApiCrawlerWriter {
    pub(crate) state: AppState,
}

impl ported::crawler_ingestion::CloudPageWriter for ApiCrawlerWriter {
    fn write_batch<'a>(
        &'a self,
        _kind: ported::crawler_ingestion::CrawlerKind,
        policy: ported::crawler_ingestion::ConflictPolicy,
        pages: &'a [ported::crawler_ingestion::CrawlPage],
    ) -> ported::crawler_ingestion::CloudWriteFuture<'a> {
        Box::pin(async move {
            let mut written = 0;
            for page in pages {
                let url = if page.canonical.is_empty() {
                    page.url.as_str()
                } else {
                    page.canonical.as_str()
                };
                if policy != ported::crawler_ingestion::ConflictPolicy::SearchStore
                    && cloud_document_exists(&self.state, url)
                        .await
                        .map_err(|error| error.message)?
                {
                    continue;
                }
                let metadata = serde_json::to_value(page).map_err(|error| error.to_string())?;
                stage_document(
                    &self.state,
                    IndexDocumentRequest {
                        url: url.to_owned(),
                        title: page.title.clone(),
                        content: page.content.clone(),
                        description: page.description.clone(),
                        language: if page.language.is_empty() {
                            page.html_lang.clone()
                        } else {
                            page.language.clone()
                        },
                        source_metadata: Some(metadata),
                        source: None,
                        status_code: page.status_code,
                        crawled_at: Some(page.crawled_at),
                    },
                )
                .await
                .map_err(|error| error.message)?;
                written += 1;
            }
            commit_tantivy(&self.state);
            Ok(written)
        })
    }
}

pub(crate) struct ApiCrawlQueue {
    pub(crate) state: AppState,
}

impl ported::crawler_ingestion::DurableCrawlQueue for ApiCrawlQueue {
    fn take_next(
        &mut self,
    ) -> ported::crawler_ingestion::QueueFuture<'_, Option<ported::crawler_ingestion::QueuedUrl>>
    {
        Box::pin(async move {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| error.to_string())?
                .as_secs_f64();
            let expired = now - 300.0;
            if let Some(pool) = &self.state.tidb_pool {
                let mut connection = pool.get_conn().await.map_err(|error| error.to_string())?;
                for _ in 0..16 {
                    let candidate: Option<(String, i32)> = connection
                        .exec_first(
                            "SELECT url,depth FROM `crawl_queue` \
                             WHERE (status='queued' AND next_attempt<=?) \
                                OR (status='processing' AND locked_at<?) \
                             ORDER BY depth,next_attempt LIMIT 1",
                            (now, expired),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    let Some((url, depth)) = candidate else {
                        return Ok(None);
                    };
                    let canonical = canonicalize_url(&url).map_err(|error| error.to_string())?;
                    let hash = Sha256::digest(canonical.as_bytes()).to_vec();
                    connection
                        .exec_drop(
                            "UPDATE `crawl_queue` SET status='processing',attempts=attempts+1,\
                             locked_at=?,last_error='' WHERE url_hash=? AND \
                             ((status='queued' AND next_attempt<=?) OR \
                              (status='processing' AND locked_at<?))",
                            (now, hash, now, expired),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    if connection.affected_rows() > 0 {
                        return Ok(Some(ported::crawler_ingestion::QueuedUrl {
                            url: canonical,
                            depth: usize::try_from(depth.max(0)).unwrap_or_default(),
                        }));
                    }
                }
                return Ok(None);
            }
            let queue = self
                .state
                .mongo_queue_collection
                .as_ref()
                .ok_or_else(|| "No cloud crawl queue is configured".to_owned())?;
            let filter = doc! {
                "$or": [
                    { "status": "queued", "next_attempt": { "$lte": now } },
                    { "status": "processing", "locked_at": { "$lt": expired } }
                ]
            };
            let row = queue
                .find_one_and_update(
                    filter,
                    doc! {
                        "$set": {
                            "status": "processing",
                            "locked_at": now,
                            "last_error": ""
                        },
                        "$inc": { "attempts": 1 }
                    },
                )
                .sort(doc! { "depth": 1, "next_attempt": 1 })
                .await
                .map_err(|error| error.to_string())?;
            Ok(row.and_then(|document| {
                let url = document.get_str("url").ok()?.to_owned();
                let depth = document.get_i32("depth").unwrap_or(0).max(0) as usize;
                Some(ported::crawler_ingestion::QueuedUrl { url, depth })
            }))
        })
    }

    fn canonicalize(&self, url: &str) -> Result<String, String> {
        canonicalize_url(url).map_err(|error| error.to_string())
    }

    fn enqueue<'a>(
        &'a mut self,
        urls: &'a [String],
        depth: usize,
    ) -> ported::crawler_ingestion::QueueFuture<'a, ()> {
        Box::pin(async move {
            let depth = i32::try_from(depth).map_err(|_| "crawl depth is out of range")?;
            enqueue_urls(&self.state, urls, depth, false)
                .await
                .map(|_| ())
                .map_err(|error| error.message)
        })
    }

    fn index_document<'a>(
        &'a mut self,
        page: &'a ported::crawler_ingestion::CrawlPage,
    ) -> ported::crawler_ingestion::QueueFuture<'a, ()> {
        Box::pin(async move {
            let url = if page.canonical.is_empty() {
                page.url.as_str()
            } else {
                page.canonical.as_str()
            };
            let metadata = serde_json::to_value(page).map_err(|error| error.to_string())?;
            write_document(
                &self.state,
                IndexDocumentRequest {
                    url: url.to_owned(),
                    title: page.title.clone(),
                    content: page.content.clone(),
                    description: page.description.clone(),
                    language: if page.language.is_empty() {
                        page.html_lang.clone()
                    } else {
                        page.language.clone()
                    },
                    source_metadata: Some(metadata),
                    source: None,
                    status_code: page.status_code,
                    crawled_at: Some(page.crawled_at),
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| error.message)
        })
    }

    fn finish_url<'a>(
        &'a mut self,
        url: &'a str,
        error: Option<&'a str>,
    ) -> ported::crawler_ingestion::QueueFuture<'a, ()> {
        Box::pin(async move {
            let canonical = canonicalize_url(url).map_err(|error| error.to_string())?;
            let status = if error.is_some() { "failed" } else { "done" };
            let message = error
                .unwrap_or_default()
                .chars()
                .take(1000)
                .collect::<String>();
            if let Some(pool) = &self.state.tidb_pool {
                let mut connection = pool.get_conn().await.map_err(|error| error.to_string())?;
                let hash = Sha256::digest(canonical.as_bytes()).to_vec();
                connection
                    .exec_drop(
                        "UPDATE `crawl_queue` SET status=?,locked_at=0,next_attempt=0,last_error=? \
                         WHERE url_hash=?",
                        (status, message, hash),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                return Ok(());
            }
            let queue = self
                .state
                .mongo_queue_collection
                .as_ref()
                .ok_or_else(|| "No cloud crawl queue is configured".to_owned())?;
            queue
                .update_one(
                    doc! { "url": canonical },
                    doc! { "$set": {
                        "status": status,
                        "locked_at": 0.0,
                        "next_attempt": 0.0,
                        "last_error": message
                    }},
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        })
    }

    fn stats(&self) -> ported::crawler_ingestion::QueueFuture<'_, serde_json::Value> {
        Box::pin(async move {
            if let Some(pool) = &self.state.tidb_pool {
                let mut connection = pool.get_conn().await.map_err(|error| error.to_string())?;
                let rows = connection
                    .query::<Row, _>(
                        "SELECT status,COUNT(*) AS n FROM `crawl_queue` GROUP BY status",
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let mut stats = serde_json::Map::new();
                for row in rows {
                    let status = row
                        .get::<Option<String>, _>("status")
                        .flatten()
                        .unwrap_or_default();
                    let count = row.get::<Option<i64>, _>("n").flatten().unwrap_or_default();
                    stats.insert(status, serde_json::Value::from(count.max(0)));
                }
                return Ok(serde_json::Value::Object(stats));
            }
            let queue = self
                .state
                .mongo_queue_collection
                .as_ref()
                .ok_or_else(|| "No cloud crawl queue is configured".to_owned())?;
            let mut stats = serde_json::Map::new();
            for status in ["queued", "processing", "done", "failed"] {
                let count = queue
                    .count_documents(doc! { "status": status })
                    .await
                    .map_err(|error| error.to_string())?;
                stats.insert(status.to_owned(), serde_json::Value::from(count));
            }
            Ok(serde_json::Value::Object(stats))
        })
    }
}

pub(crate) async fn enqueue_urls(
    state: &AppState,
    urls: &[String],
    depth: i32,
    force: bool,
) -> Result<u64, ApiError> {
    let mut added = 0u64;
    let Some(pool) = &state.tidb_pool else {
        let queue = state.mongo_queue_collection.as_ref().ok_or_else(|| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "No cloud crawl queue is configured",
            )
        })?;
        for raw_url in urls {
            let Ok(url) = canonicalize_url(raw_url) else {
                continue;
            };
            if force {
                let reset = queue
                    .update_one(
                        doc! { "url": &url, "status": { "$in": ["done", "failed"] } },
                        doc! { "$set": {
                            "depth": depth,
                            "status": "queued",
                            "attempts": 0,
                            "next_attempt": 0.0,
                            "locked_at": 0.0,
                            "last_error": ""
                        }},
                    )
                    .await
                    .map_err(|error| ApiError::internal(error.into()))?;
                if reset.modified_count > 0 {
                    added += 1;
                    continue;
                }
            }
            let inserted = queue
                .update_one(
                    doc! { "url": &url },
                    doc! { "$setOnInsert": {
                        "url": &url,
                        "depth": depth,
                        "status": "queued",
                        "attempts": 0,
                        "next_attempt": 0.0,
                        "locked_at": 0.0,
                        "last_error": ""
                    }},
                )
                .upsert(true)
                .await
                .map_err(|error| ApiError::internal(error.into()))?;
            added += u64::from(inserted.upserted_id.is_some());
        }
        return Ok(added);
    };
    let mut connection = pool
        .get_conn()
        .await
        .map_err(|error| ApiError::internal(error.into()))?;
    for raw_url in urls {
        let Ok(url) = canonicalize_url(raw_url) else {
            continue;
        };
        let url_hash = Sha256::digest(url.as_bytes()).to_vec();
        let statement = if force {
            "INSERT INTO `crawl_queue` (url_hash,url,depth) VALUES (?,?,?) \
             ON DUPLICATE KEY UPDATE \
             depth=IF(status IN ('done','failed'),VALUES(depth),depth), \
             attempts=IF(status IN ('done','failed'),0,attempts), \
             next_attempt=IF(status IN ('done','failed'),0,next_attempt), \
             locked_at=IF(status IN ('done','failed'),0,locked_at), \
             last_error=IF(status IN ('done','failed'),'',last_error), \
             status=IF(status IN ('done','failed'),'queued',status)"
        } else {
            "INSERT IGNORE INTO `crawl_queue` (url_hash,url,depth) VALUES (?,?,?)"
        };
        connection
            .exec_drop(statement, (url_hash, url, depth))
            .await
            .map_err(|error| ApiError::internal(error.into()))?;
        added = added.saturating_add(connection.affected_rows());
    }
    Ok(added)
}
