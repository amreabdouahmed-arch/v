use anyhow::{Context, Result};
use futures_util::TryStreamExt;
use mysql_async::prelude::Queryable;

use crate::ported::common::{
    canonicalize_url, page_hash, TiDbSettings, DOCUMENTS_TABLE, QUEUE_TABLE,
};

pub async fn manage_stats() -> Result<serde_json::Value> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    let mut connection = pool.get_conn().await?;
    let documents: u64 = connection
        .query_first(format!("SELECT COUNT(*) FROM `{DOCUMENTS_TABLE}`"))
        .await?
        .context("TiDB document count query returned no result")?;
    let rows: Vec<(String, u64)> = connection
        .query(format!(
            "SELECT status,COUNT(*) FROM `{QUEUE_TABLE}` GROUP BY status"
        ))
        .await?;
    let queue: serde_json::Map<String, serde_json::Value> = rows
        .into_iter()
        .map(|(status, count)| (status, serde_json::json!(count)))
        .collect();
    let queued = ["queued", "retry", "fetching"]
        .iter()
        .filter_map(|status| queue.get(*status).and_then(|value| value.as_u64()))
        .sum::<u64>();
    let (mongo_documents, mongo_status, mongo_warning) =
        match crate::ported::cloud::mongo_collection(
            &["MONGO_DATABASE", "MONGO_DB"],
            &["MONGO_SEARCH_COLLECTION", "MONGO_COLLECTION"],
            "Crawled_Pages",
        )
        .await
        {
            Ok((client, _, collection)) => {
                let count = collection
                    .count_documents(mongodb::bson::Document::new())
                    .await
                    .context("MongoDB document count query failed")?;
                drop(client);
                (count, "connected", serde_json::Value::Null)
            }
            Err(_error)
                if std::env::var("MONGO_URI").is_err() && std::env::var("MONGODB_URI").is_err() =>
            {
                (0, "unconfigured", serde_json::Value::Null)
            }
            Err(error) => (0, "disconnected", serde_json::json!(error.to_string())),
        };
    Ok(serde_json::json!({
        "documents": documents,
        "tidb_documents": documents,
        "mongo_documents": mongo_documents,
        "mongo_partition_percent": std::env::var("MONGO_PARTITION_PERCENT").ok()
            .and_then(|value| value.parse::<i64>().ok()).unwrap_or(20).clamp(0, 100),
        "queued_urls": queued,
        "crawl_queue": queue,
        "database": settings.database,
        "backend": "tidb",
        "writable": true,
        "mongodb_partition": {
            "status": mongo_status,
            "writable": null,
            "warning": mongo_warning
        }
    }))
}

pub async fn seed_urls(urls: &[String], force: bool, depth: u32) -> Result<u64> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    let mut connection = pool.get_conn().await?;
    let mut accepted = 0;
    for raw in urls {
        let Ok(url) = canonicalize_url(raw) else {
            continue;
        };
        let hash = page_hash(&url).to_vec();
        if force {
            connection
                .exec_drop(
                    format!(
                        "INSERT INTO `{QUEUE_TABLE}` (url_hash,url,depth) VALUES (?,?,?) \
                ON DUPLICATE KEY UPDATE depth=IF(status IN ('done','failed'),VALUES(depth),depth),\
                attempts=IF(status IN ('done','failed'),0,attempts),\
                next_attempt=IF(status IN ('done','failed'),0,next_attempt),\
                locked_at=IF(status IN ('done','failed'),0,locked_at),\
                last_error=IF(status IN ('done','failed'),'',last_error),\
                status=IF(status IN ('done','failed'),'queued',status)"
                    ),
                    (hash, url, depth),
                )
                .await?;
        } else {
            connection
                .exec_drop(
                    format!(
                        "INSERT IGNORE INTO `{QUEUE_TABLE}` (url_hash,url,depth) VALUES (?,?,?)"
                    ),
                    (hash, url, depth),
                )
                .await?;
        }
        let changed: Option<i64> = connection.query_first("SELECT ROW_COUNT()").await?;
        accepted += changed.unwrap_or_default().max(0) as u64;
    }
    connection.query_drop("COMMIT").await?;
    Ok(accepted)
}

pub async fn import_universities_json(path: impl AsRef<std::path::Path>) -> Result<usize> {
    let bytes = std::fs::read(path.as_ref())
        .with_context(|| format!("reading university JSON {}", path.as_ref().display()))?;
    let entries: serde_json::Value = serde_json::from_slice(&bytes)?;
    let Some(rows) = entries.as_array() else {
        return Ok(0);
    };
    let pages = rows
        .iter()
        .filter_map(|row| {
            let url = row
                .get("main_url")
                .or_else(|| row.get("url"))?
                .as_str()?
                .to_owned();
            let title = row
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let country = row
                .get("country")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let description = row
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            Some(crate::ported::Page {
                url,
                title,
                content: format!("{country} {description}"),
                description,
                ..Default::default()
            })
        })
        .collect::<Vec<_>>();
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    crate::ported::common::initialize_search_schema(&pool).await?;
    crate::ported::common::upsert_tidb_pages(&pool, &pages).await
}

pub async fn ensure_mongodb_text_index() -> Result<String> {
    use mongodb::options::IndexOptions;
    use mongodb::{bson::doc, IndexModel};
    let (_client, _database, collection) = crate::ported::cloud::mongo_collection(
        &["MONGO_DATABASE", "MONGO_DB", "MONGODB_DB"],
        &[
            "MONGO_SEARCH_COLLECTION",
            "MONGO_COLLECTION",
            "MONGODB_COLLECTION",
        ],
        "Crawled_Pages",
    )
    .await?;
    let mut indexes = collection.list_indexes().await?;
    while let Some(index) = indexes.try_next().await? {
        if index
            .keys
            .iter()
            .any(|(_, value)| value.as_str() == Some("text"))
        {
            let weights = index
                .options
                .as_ref()
                .and_then(|options| options.weights.as_ref());
            if weights.is_some_and(|weights| {
                ["title", "description", "content"]
                    .iter()
                    .all(|field| weights.contains_key(*field))
            }) {
                return Ok(index
                    .options
                    .and_then(|options| options.name)
                    .unwrap_or_default());
            }
            anyhow::bail!(
                "MongoDB already has an incompatible text index; use a dedicated search collection"
            );
        }
    }
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
    let created = collection
        .create_index(model)
        .await
        .context("creating MongoDB text index")?;
    Ok(created.index_name)
}

#[cfg(test)]
mod tests {
    use crate::ported::common::canonicalize_url;

    #[test]
    fn invalid_seed_urls_are_rejected_by_canonicalizer() {
        assert!(canonicalize_url("javascript:alert(1)").is_err());
        assert!(canonicalize_url("https://example.org/a").is_ok());
    }
}
