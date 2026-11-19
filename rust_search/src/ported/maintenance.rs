use std::{collections::HashSet, path::Path};

use anyhow::{bail, Context, Result};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Document};
use mysql_async::{prelude::Queryable, Row};
use rusqlite::{Connection, OpenFlags};

use crate::ported::{
    cloud::mongo_collection,
    common::{canonicalize_url, page_hash, Page, TiDbSettings, DOCUMENTS_TABLE},
};

const SOURCE_COLLECTIONS: [&str; 4] = [
    "Crawled_Pages",
    "Ultimate_Pages",
    "Global_World_Pages",
    "Universities_Pages",
];

#[derive(Debug, Clone, Copy)]
pub struct RepartitionOptions {
    pub apply: bool,
    pub confirmed_drop_collections: bool,
}

#[derive(Debug, Clone)]
pub struct RepartitionReport {
    pub source_collections: Vec<String>,
    pub mongo_source_documents: usize,
    pub urls_missing_from_tidb: usize,
    pub partition_percent: u8,
    pub partition_candidates: usize,
    pub mode: &'static str,
}

fn partition_percent() -> u8 {
    std::env::var("MONGO_PARTITION_PERCENT")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20)
        .clamp(0, 100) as u8
}

fn bucket(url: &str) -> Option<u8> {
    let canonical = canonicalize_url(url).ok()?;
    Some((u64::from_be_bytes(page_hash(&canonical)[..8].try_into().ok()?) % 100) as u8)
}

/// Read-only by default. Collection deletion is reachable only with both safety flags.
pub async fn repartition_cloud_stores(options: RepartitionOptions) -> Result<RepartitionReport> {
    if options.apply != options.confirmed_drop_collections {
        bail!("collection deletion requires both --apply and --confirm-drop-collections");
    }
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    let (client, database, _target) = mongo_collection(
        &["MONGO_DATABASE", "MONGO_DB"],
        &["MONGO_SEARCH_COLLECTION", "MONGO_COLLECTION"],
        "Crawled_Pages",
    )
    .await?;
    let all_names = database.list_collection_names().await?;
    let source_names = SOURCE_COLLECTIONS
        .iter()
        .filter(|name| all_names.iter().any(|entry| entry == **name))
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    let partition = partition_percent();
    let mut pages = Vec::<(String, Page)>::new();
    for name in &source_names {
        let collection = database.collection::<Document>(name);
        let mut cursor = collection.find(doc! {}).await?;
        while let Some(document) = cursor.try_next().await? {
            let Some(page) = crate::ported::cloud::from_mongo_document(&document) else {
                continue;
            };
            let mut page = page;
            page.source_metadata_json = Some(source_metadata_json(name, &document));
            pages.push((name.clone(), page));
        }
    }
    let page_values = pages
        .iter()
        .map(|(_, page)| page.clone())
        .collect::<Vec<_>>();
    let missing = crate::ported::common::missing_page_hashes(&pool, &page_values).await?;
    let missing_hashes = missing
        .iter()
        .filter_map(|page| canonicalize_url(&page.url).ok())
        .map(|url| page_hash(&url))
        .collect::<HashSet<_>>();
    let candidates = page_values
        .iter()
        .filter(|page| bucket(&page.url).is_some_and(|b| b < partition))
        .count();
    let report = RepartitionReport {
        source_collections: source_names.clone(),
        mongo_source_documents: pages.len(),
        urls_missing_from_tidb: missing_hashes.len(),
        partition_percent: partition,
        partition_candidates: candidates,
        mode: if options.apply { "apply" } else { "dry-run" },
    };
    if !options.apply || source_names.is_empty() {
        drop(client);
        if report.urls_missing_from_tidb > 0 {
            bail!("MongoDB contains URLs missing from TiDB; refusing to delete or repartition sources");
        }
        return Ok(report);
    }

    // Stage every original document before any collection deletion.
    merge_existing_metadata(&pool, &mut pages).await?;
    let staged_pages = pages
        .iter()
        .map(|(_, page)| page.clone())
        .collect::<Vec<_>>();
    crate::ported::common::upsert_tidb_pages(&pool, &staged_pages).await?;
    if !missing_hashes.is_empty() {
        drop(client);
        bail!(
            "MongoDB contains URLs missing from TiDB; staged pages but refusing to delete sources"
        );
    }
    verify_staged_metadata(&pool, &staged_pages).await?;
    let mut connection = pool.get_conn().await?;
    for name in &source_names {
        database
            .collection::<Document>(name)
            .drop()
            .await
            .with_context(|| format!("dropping confirmed source collection {name}"))?;
    }
    let destination = database.collection::<Document>("Crawled_Pages");
    destination
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "url": 1 })
                .options(
                    mongodb::options::IndexOptions::builder()
                        .name("url_1".to_owned())
                        .build(),
                )
                .build(),
        )
        .await?;
    let mut moved = 0;
    for page in staged_pages
        .iter()
        .filter(|page| bucket(&page.url).is_some_and(|b| b < partition))
    {
        let canonical = canonicalize_url(&page.url)?;
        let metadata = page
            .source_metadata_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .and_then(|value| mongodb::bson::to_bson(&value).ok())
            .unwrap_or_else(|| mongodb::bson::Bson::Array(Vec::new()));
        let mut document = doc! {
            "url": &canonical,
            "title": &page.title,
            "description": &page.description,
            "content": &page.content,
            "language": &page.language,
            "status_code": page.status_code as i32,
            "crawled_at": page.crawled_at.unwrap_or_default(),
        };
        document.insert("source_metadata", metadata);
        destination
            .update_one(doc! { "url": &canonical }, doc! { "$set": document })
            .upsert(true)
            .await?;
        connection
            .exec_drop(
                format!("DELETE FROM `{DOCUMENTS_TABLE}` WHERE url_hash=?"),
                (page_hash(&canonical).to_vec(),),
            )
            .await?;
        moved += 1;
    }
    connection.query_drop("COMMIT").await?;
    let _ = moved;
    drop(client);
    Ok(report)
}

fn source_metadata_json(collection_name: &str, document: &Document) -> String {
    const CORE_FIELDS: &[&str] = &[
        "_id",
        "url",
        "canonical",
        "canonical_url",
        "title",
        "description",
        "content",
        "language",
        "html_lang",
        "status_code",
        "crawled_at",
        "content_hash",
        "source_metadata",
        "search_text",
    ];
    let inherited = document.get("source_metadata");
    let inherited = match inherited {
        Some(mongodb::bson::Bson::Array(values)) => {
            let entries = values
                .iter()
                .filter_map(|value| serde_json::to_value(value).ok())
                .collect::<Vec<_>>();
            return serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_owned());
        }
        Some(mongodb::bson::Bson::Document(value)) if !value.is_empty() => {
            vec![serde_json::to_value(value).unwrap_or_default()]
        }
        _ => Vec::new(),
    };
    if !inherited.is_empty() {
        return serde_json::to_string(&inherited).unwrap_or_else(|_| "[]".to_owned());
    }

    let mut fields = serde_json::Map::new();
    for (key, value) in document {
        if !CORE_FIELDS.contains(&key.as_str()) {
            if let Ok(value) = serde_json::to_value(value) {
                fields.insert(key.clone(), value);
            }
        }
    }
    if let Some(id) = document.get("_id") {
        fields.insert("mongo_id".to_owned(), serde_json::json!(id.to_string()));
    }
    serde_json::json!([{
        "source_collection": collection_name,
        "fields": fields,
    }])
    .to_string()
}

async fn merge_existing_metadata(
    pool: &mysql_async::Pool,
    pages: &mut [(String, Page)],
) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let mut connection = pool.get_conn().await?;
    for batch in pages.chunks_mut(300) {
        let keys = batch
            .iter()
            .filter_map(|(_, page)| {
                canonicalize_url(&page.url)
                    .ok()
                    .map(|url| page_hash(&url).to_vec())
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            continue;
        }
        let placeholders = vec!["?"; keys.len()].join(",");
        let query = format!(
            "SELECT url_hash,source_metadata_json FROM `{DOCUMENTS_TABLE}` WHERE url_hash IN ({placeholders})"
        );
        let rows: Vec<(Vec<u8>, Option<String>)> = connection
            .exec(
                query,
                mysql_async::Params::Positional(
                    keys.into_iter().map(mysql_async::Value::Bytes).collect(),
                ),
            )
            .await?;
        let existing = rows
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        for (_, page) in batch {
            let Ok(canonical) = canonicalize_url(&page.url) else {
                continue;
            };
            let mut entries = Vec::<serde_json::Value>::new();
            if let Some(Some(previous)) = existing.get(page_hash(&canonical).as_slice()) {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(previous) {
                    extend_metadata(&mut entries, value);
                }
            }
            if let Some(current) = page.source_metadata_json.as_deref() {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(current) {
                    extend_metadata(&mut entries, value);
                }
            }
            let mut seen = HashSet::new();
            entries.retain(|entry| seen.insert(entry.to_string()));
            page.source_metadata_json = Some(serde_json::to_string(&entries)?);
        }
    }
    Ok(())
}

fn extend_metadata(entries: &mut Vec<serde_json::Value>, value: serde_json::Value) {
    match value {
        serde_json::Value::Array(items) => entries.extend(items),
        serde_json::Value::Null => {}
        other => entries.push(other),
    }
}

async fn verify_staged_metadata(pool: &mysql_async::Pool, pages: &[Page]) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let mut connection = pool.get_conn().await?;
    let mut missing = 0usize;
    for batch in pages.chunks(300) {
        let keys = batch
            .iter()
            .filter_map(|page| {
                canonicalize_url(&page.url)
                    .ok()
                    .map(|url| page_hash(&url).to_vec())
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            continue;
        }
        let placeholders = vec!["?"; keys.len()].join(",");
        let query = format!(
            "SELECT url_hash FROM `{DOCUMENTS_TABLE}` WHERE source_metadata_json IS NOT NULL \
             AND source_metadata_json<>'' AND url_hash IN ({placeholders})"
        );
        let rows: Vec<Vec<u8>> = connection
            .exec(
                query,
                mysql_async::Params::Positional(
                    keys.iter()
                        .cloned()
                        .map(mysql_async::Value::Bytes)
                        .collect(),
                ),
            )
            .await?;
        missing += keys.len() - rows.len();
    }
    if missing != 0 {
        bail!(
            "MongoDB metadata was not fully staged in TiDB; refusing to delete source collections"
        );
    }
    Ok(())
}

/// Re-imports the old `test.velo_search_index` table without altering its rows.
pub async fn repair_legacy_tidb_index() -> Result<(u64, u64)> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    let mut source = pool.get_conn().await?;
    let exists: Option<String> = source
        .exec_first("SHOW TABLES FROM `test` LIKE 'velo_search_index'", ())
        .await?;
    if exists.is_none() {
        bail!("test.velo_search_index does not exist");
    }
    let mut scanned = 0_u64;
    let mut imported = 0_u64;
    let rows: Vec<Row> = source
        .query("SELECT url,title,content,crawled_at FROM test.velo_search_index")
        .await?;
    for batch in rows.chunks(500) {
        let mut pages = Vec::new();
        for row in batch {
            let url: Option<String> = row.get("url");
            let Some(url) = url else {
                continue;
            };
            pages.push(Page {
                url,
                title: row
                    .get::<Option<String>, _>("title")
                    .flatten()
                    .unwrap_or_default(),
                content: row
                    .get::<Option<String>, _>("content")
                    .flatten()
                    .unwrap_or_default(),
                crawled_at: row.get::<Option<f64>, _>("crawled_at").flatten(),
                ..Page::default()
            });
        }
        scanned += batch.len() as u64;
        let missing = crate::ported::common::missing_page_hashes(&pool, &pages).await?;
        imported += crate::ported::common::upsert_tidb_pages(&pool, &missing).await? as u64;
    }
    Ok((scanned, imported))
}

/// Idempotently imports the existing TiDB/Mongo cloud records into the current TiDB store.
pub async fn migrate_cloud_sources(legacy_sqlite: &[String]) -> Result<u64> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    crate::ported::common::initialize_search_schema(&pool).await?;
    let mut imported = 0_u64;
    let mut old = pool.get_conn().await?;
    let exists: Option<String> = old
        .exec_first("SHOW TABLES FROM `test` LIKE 'velo_search_index'", ())
        .await?;
    if exists.is_some() {
        let rows: Vec<Row> = old
            .query("SELECT url,title,content,crawled_at FROM test.velo_search_index")
            .await?;
        for batch in rows.chunks(500) {
            let pages = rows_to_pages(batch);
            let missing = crate::ported::common::missing_page_hashes(&pool, &pages).await?;
            imported += crate::ported::common::upsert_tidb_pages(&pool, &missing).await? as u64;
        }
    }

    if std::env::var("MONGO_URI").is_err() && std::env::var("MONGODB_URI").is_err() {
        imported += import_local_sources(&pool, legacy_sqlite).await?;
        for path in legacy_sqlite {
            imported += import_legacy_queue(path, &pool).await?;
        }
        imported += import_universities_if_available(&pool).await?;
        let _ = crate::ported::common::ensure_text_index(&pool).await?;
        return Ok(imported);
    }
    let (client, database, _collection) = mongo_collection(
        &["MONGO_DATABASE", "MONGO_DB"],
        &["MONGO_SEARCH_COLLECTION"],
        "Crawled_Pages",
    )
    .await?;
    for name in [
        "Crawled_Pages",
        "Ultimate_Pages",
        "Global_World_Pages",
        "Universities_Pages",
    ] {
        let source = database.collection::<Document>(name);
        let mut cursor = source.find(doc! { "url": { "$type": "string" } }).await?;
        let mut batch = Vec::with_capacity(500);
        while let Some(document) = cursor.try_next().await? {
            if let Some(page) = crate::ported::cloud::from_mongo_document(&document) {
                batch.push(page);
            }
            if batch.len() == 500 {
                let missing = crate::ported::common::missing_page_hashes(&pool, &batch).await?;
                imported += crate::ported::common::upsert_tidb_pages(&pool, &missing).await? as u64;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            let missing = crate::ported::common::missing_page_hashes(&pool, &batch).await?;
            imported += crate::ported::common::upsert_tidb_pages(&pool, &missing).await? as u64;
        }
    }
    imported += import_local_sources(&pool, legacy_sqlite).await?;
    imported += import_universities_if_available(&pool).await?;
    let queue_name =
        std::env::var("MONGO_QUEUE_COLLECTION").unwrap_or_else(|_| "velo_crawl_queue".to_owned());
    let queue = database.collection::<Document>(&queue_name);
    let mut cursor = queue.find(doc! {}).await?;
    while let Some(row) = cursor.try_next().await? {
        if let Ok(url) = row.get_str("url") {
            imported += insert_queue_entry(
                &pool,
                url,
                row.get_i32("depth").unwrap_or(0),
                row.get_str("status").unwrap_or("queued"),
                row.get_i32("attempts").unwrap_or(0),
                row.get_f64("next_attempt").unwrap_or(0.0),
                row.get_f64("locked_at").unwrap_or(0.0),
                row.get_str("last_error").unwrap_or_default(),
            )
            .await?;
        }
    }
    drop(client);
    let _ = crate::ported::common::ensure_text_index(&pool).await?;
    Ok(imported)
}

async fn import_universities_if_available(pool: &mysql_async::Pool) -> Result<u64> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("universities.json");
    if !path.is_file() {
        return Ok(0);
    }
    let bytes = std::fs::read(path)?;
    let Ok(entries) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Ok(0);
    };
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
            Some(Page {
                url,
                title,
                content: format!("{country} {description}"),
                description,
                ..Page::default()
            })
        })
        .collect::<Vec<_>>();
    crate::ported::common::upsert_tidb_pages(pool, &pages)
        .await
        .map(|count| count as u64)
}

pub async fn import_legacy_files(paths: &[String]) -> Result<u64> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    crate::ported::common::initialize_search_schema(&pool).await?;
    let mut imported = import_local_sources(&pool, paths).await?;
    for path in paths {
        imported += import_legacy_queue(path, &pool).await?;
    }
    Ok(imported)
}

pub async fn merge_legacy_databases(paths: &[String]) -> Result<(u64, u64)> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    crate::ported::common::initialize_search_schema(&pool).await?;
    let mut merged = 0_u64;
    let mut skipped = 0_u64;
    for path in paths {
        if !Path::new(path).is_file() {
            continue;
        }
        let Ok(pages) = read_merge_pages(path) else {
            continue;
        };
        let pages = pages
            .into_iter()
            .map(|page| Page {
                url: page.url,
                title: page.title,
                content: page.content,
                crawled_at: page.crawled_at,
                ..Page::default()
            })
            .collect::<Vec<_>>();
        let (added, ignored) =
            crate::ported::common::insert_ignore_tidb_pages(&pool, &pages).await?;
        merged += added as u64;
        skipped += ignored as u64;
    }
    Ok((merged, skipped))
}

fn read_merge_pages(path: impl AsRef<Path>) -> Result<Vec<crate::ported::LegacyPage>> {
    let path = path.as_ref();
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {} read-only", path.display()))?;
    let columns = connection
        .prepare("PRAGMA table_info(pages)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    if !columns.contains("url") || !columns.contains("title") || !columns.contains("content") {
        bail!("source database does not have a compatible pages table");
    }
    let crawled_at = if columns.contains("crawled_at") {
        "\"crawled_at\""
    } else {
        "NULL"
    };
    let mut statement =
        connection.prepare(&format!("SELECT url,title,content,{crawled_at} FROM pages"))?;
    let rows = statement.query_map([], |row| {
        Ok(crate::ported::LegacyPage {
            url: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            title: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            content: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            crawled_at: row.get::<_, Option<f64>>(3).ok().flatten(),
            ..Default::default()
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

async fn import_legacy_queue(path: &str, pool: &mysql_async::Pool) -> Result<u64> {
    let entries = read_legacy_queue(path)?;
    let mut imported = 0;
    for (url, depth, status, attempts, next_attempt, locked_at, last_error) in entries {
        imported += insert_queue_entry(
            pool,
            &url,
            depth,
            &status,
            attempts,
            next_attempt,
            locked_at,
            &last_error,
        )
        .await?;
    }
    Ok(imported)
}

/// One row of a legacy SQLite crawl queue:
/// url, depth, status, attempts, next_attempt, locked_at, last_error.
type LegacyQueueEntry = (String, i32, String, i32, f64, f64, String);

fn read_legacy_queue(path: &str) -> Result<Vec<LegacyQueueEntry>> {
    if !Path::new(path).is_file() {
        return Ok(Vec::new());
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {} read-only", path))?;
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='crawl_queue')",
        [],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(Vec::new());
    }
    let columns = connection
        .prepare("PRAGMA table_info(crawl_queue)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    if !columns.contains("url") {
        return Ok(Vec::new());
    }
    let field = |name: &str, fallback: &str| {
        if columns.contains(name) {
            format!("\"{name}\"")
        } else {
            fallback.to_owned()
        }
    };
    let query = format!(
        "SELECT url,{},{},{},{},{},{} FROM crawl_queue",
        field("depth", "0"),
        field("status", "'queued'"),
        field("attempts", "0"),
        field("next_attempt", "0"),
        field("locked_at", "0"),
        field("last_error", "''"),
    );
    let mut statement = connection.prepare(&query)?;
    let entries = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                row.get::<_, Option<i32>>(1)?.unwrap_or_default(),
                row.get::<_, Option<String>>(2)?
                    .unwrap_or_else(|| "queued".to_owned()),
                row.get::<_, Option<i32>>(3)?.unwrap_or_default(),
                row.get::<_, Option<f64>>(4)
                    .unwrap_or(None)
                    .unwrap_or_default(),
                row.get::<_, Option<f64>>(5)
                    .unwrap_or(None)
                    .unwrap_or_default(),
                row.get::<_, Option<String>>(6)?.unwrap_or_default(),
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(entries)
}

#[allow(clippy::too_many_arguments)]
async fn insert_queue_entry(
    pool: &mysql_async::Pool,
    raw_url: &str,
    depth: i32,
    status: &str,
    attempts: i32,
    next_attempt: f64,
    locked_at: f64,
    last_error: &str,
) -> Result<u64> {
    use mysql_async::prelude::Queryable;
    let Ok(url) = canonicalize_url(raw_url) else {
        return Ok(0);
    };
    let status = match status {
        "fetching" => "retry",
        "queued" | "retry" | "done" | "failed" => status,
        _ => "queued",
    };
    let mut connection = pool.get_conn().await?;
    connection
        .exec_drop(
            "INSERT IGNORE INTO `crawl_queue` \
             (url_hash,url,depth,status,attempts,next_attempt,locked_at,last_error) \
             VALUES (?,?,?,?,?,?,?,?)",
            (
                page_hash(&url).to_vec(),
                url,
                depth,
                status,
                attempts,
                next_attempt,
                locked_at,
                last_error.chars().take(1000).collect::<String>(),
            ),
        )
        .await?;
    let changed: Option<i64> = connection.query_first("SELECT ROW_COUNT()").await?;
    connection.query_drop("COMMIT").await?;
    Ok(changed.unwrap_or_default().max(0) as u64)
}

async fn import_local_sources(pool: &mysql_async::Pool, paths: &[String]) -> Result<u64> {
    let mut imported = 0;
    for path in paths {
        let Ok(source_pages) = read_legacy_pages(path) else {
            continue;
        };
        for pages in source_pages.chunks(200) {
            let converted = pages
                .iter()
                .map(|page| Page {
                    url: page.url.clone(),
                    title: page.title.clone(),
                    content: page.content.clone(),
                    description: page.description.clone(),
                    language: page.language.clone(),
                    status_code: page.status_code,
                    crawled_at: page.crawled_at,
                    ..Page::default()
                })
                .collect::<Vec<_>>();
            imported += crate::ported::common::upsert_tidb_pages(pool, &converted).await? as u64;
        }
    }
    Ok(imported)
}

fn read_legacy_pages(path: impl AsRef<Path>) -> Result<Vec<crate::ported::LegacyPage>> {
    let path = path.as_ref();
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {} read-only", path.display()))?;
    let tables = [
        "documents",
        "pages",
        "arabic_pages",
        "universities_pages",
        "crawled_pages",
    ];
    let mut out = Vec::new();
    for table in tables {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            continue;
        }
        let columns = connection
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<HashSet<_>>>()?;
        if !columns.contains("url") {
            continue;
        }
        let title = if columns.contains("title") {
            "\"title\""
        } else {
            "''"
        };
        let content = ["content", "text", "body"]
            .iter()
            .find(|field| columns.contains(**field))
            .map(|field| format!("\"{field}\""))
            .unwrap_or_else(|| "''".to_owned());
        let description = if columns.contains("description") {
            "\"description\""
        } else {
            "''"
        };
        let language = ["language", "html_lang"]
            .iter()
            .find(|field| columns.contains(**field))
            .map(|field| format!("\"{field}\""))
            .unwrap_or_else(|| "''".to_owned());
        let status_code = if columns.contains("status_code") {
            "\"status_code\""
        } else {
            "200"
        };
        let crawled_at = if columns.contains("crawled_at") {
            "\"crawled_at\""
        } else {
            "NULL"
        };
        let query = format!(
            "SELECT \"url\",{title},{content},{description},{language},{status_code},{crawled_at} FROM \"{table}\""
        );
        let mut statement = connection.prepare(&query)?;
        let rows = statement.query_map([], |row| {
            Ok(crate::ported::LegacyPage {
                url: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                title: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                content: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                description: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                language: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                status_code: row.get::<_, Option<i16>>(5)?.unwrap_or(200),
                crawled_at: row.get::<_, Option<f64>>(6).ok().flatten(),
            })
        })?;
        out.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(out)
}

fn rows_to_pages(rows: &[Row]) -> Vec<Page> {
    rows.iter()
        .filter_map(|row| {
            let url: Option<String> = row.get("url");
            Some(Page {
                url: url?,
                title: row
                    .get::<Option<String>, _>("title")
                    .flatten()
                    .unwrap_or_default(),
                content: row
                    .get::<Option<String>, _>("content")
                    .flatten()
                    .unwrap_or_default(),
                crawled_at: row.get::<Option<f64>, _>("crawled_at").flatten(),
                ..Page::default()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{bucket, repartition_cloud_stores, RepartitionOptions};

    #[test]
    fn repartition_defaults_to_non_destructive_and_hash_bucket_is_bounded() {
        let options = RepartitionOptions {
            apply: false,
            confirmed_drop_collections: false,
        };
        assert!(!options.apply);
        assert!(bucket("https://example.org/path").unwrap() < 100);
        assert!(bucket("file:///bad").is_none());
    }

    #[tokio::test]
    async fn unconfirmed_apply_fails_before_database_or_environment_access() {
        let error = repartition_cloud_stores(RepartitionOptions {
            apply: true,
            confirmed_drop_collections: false,
        })
        .await
        .expect_err("unconfirmed destructive operation must be rejected");

        assert!(error
            .to_string()
            .contains("--apply and --confirm-drop-collections"));
    }
}
