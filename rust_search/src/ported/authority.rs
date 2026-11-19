//! Link-graph authority (PageRank) maintenance for the cloud search stores.
//!
//! The crawler records outbound links inside each page's source metadata.
//! This module turns those links into a canonicalized graph, computes
//! PageRank with [`pagerank::PageRankCalculator`], and writes the normalized
//! 0..=100 scores back to both cloud stores so the public search can boost
//! authoritative pages.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use mongodb::{
    bson::{doc, Document},
    Collection,
};
use mysql_async::{prelude::Queryable, Pool, Value};

use crate::ported::cloud;
use crate::ported::common::{canonicalize_url, hex_string, page_hash, DOCUMENTS_TABLE, TiDbSettings};
use crate::ported::pagerank::PageRankCalculator;

/// Rows per TiDB CASE update statement.
const UPDATE_CHUNK: usize = 500;
/// Rows fetched per paging query while reading the link graph.
const READ_BATCH: usize = 1_000;

/// Builds a canonicalized link graph from `(url, outbound links)` rows.
/// Duplicate targets and self-links are dropped; every canonical page stays a
/// graph node even when it has no outbound links.
pub fn build_link_graph(entries: &[(String, Vec<String>)]) -> HashMap<String, Vec<String>> {
    let mut graph: HashMap<String, Vec<String>> = HashMap::new();
    for (url, links) in entries {
        let Ok(page) = canonicalize_url(url) else {
            continue;
        };
        let mut targets = Vec::new();
        for link in links {
            let Ok(target) = canonicalize_url(link) else {
                continue;
            };
            if target == page || targets.contains(&target) {
                continue;
            }
            targets.push(target);
        }
        graph.entry(page).or_default().append(&mut targets);
    }
    for targets in graph.values_mut() {
        targets.sort();
        targets.dedup();
    }
    graph
}

/// Extracts the crawler-recorded outbound links from a source metadata blob.
/// Current metadata is a JSON object; legacy maintenance metadata is an array
/// of objects, so both shapes are supported.
pub fn links_from_metadata(metadata: Option<&str>) -> Vec<String> {
    let Some(metadata) = metadata.map(str::trim).filter(|value| !value.is_empty()) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata) else {
        return Vec::new();
    };
    match value {
        serde_json::Value::Object(object) => links_value(object.get("links")),
        serde_json::Value::Array(entries) => entries
            .iter()
            .find_map(|entry| {
                let links = links_value(entry.get("links"));
                (!links.is_empty()).then_some(links)
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn links_value(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .map(|links| {
            links
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Recomputes PageRank over the stored link graph and writes the normalized
/// scores to TiDB (authoritative) and MongoDB (when configured).
pub async fn recompute_pagerank() -> Result<serde_json::Value> {
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    let entries = load_link_rows(&pool).await?;
    let graph = build_link_graph(&entries);
    if graph.is_empty() {
        bail!("no canonical crawl pages with a link graph were found");
    }

    let mut pagerank = PageRankCalculator::default();
    for (url, targets) in &graph {
        for target in targets {
            pagerank.add_link(url, target);
        }
    }
    let normalized = pagerank.calculate_pagerank();
    let by_hash: Vec<(String, f64)> = normalized
        .iter()
        .map(|(url, score)| (hex_string(&page_hash(url)), *score))
        .collect();
    let mut pages_updated = 0usize;
    for chunk in by_hash.chunks(UPDATE_CHUNK) {
        pages_updated += write_tidb_pagerank(&pool, chunk).await?;
    }

    let mut mongo_updated = 0usize;
    let mut mongo_errors = 0usize;
    if let Ok((client, _database, collection)) = cloud::mongo_collection(
        &["MONGO_DATABASE", "MONGO_DB", "MONGODB_DB"],
        &[
            "MONGO_SEARCH_COLLECTION",
            "MONGO_COLLECTION",
            "MONGODB_COLLECTION",
        ],
        "Crawled_Pages",
    )
    .await
    {
        let by_url: Vec<(String, f64)> = normalized.into_iter().collect();
        (mongo_updated, mongo_errors) = write_mongo_pagerank(&collection, &by_url).await;
        drop(client);
    }

    Ok(serde_json::json!({
        "pages_scored": graph.len(),
        "pages_updated": pages_updated,
        "mongo_updated": mongo_updated,
        "mongo_errors": mongo_errors,
        "damping_factor": pagerank.damping_factor,
        "max_iterations": pagerank.max_iterations,
    }))
}

async fn load_link_rows(pool: &Pool) -> Result<Vec<(String, Vec<String>)>> {
    let mut connection = pool.get_conn().await.context("connecting to TiDB Cloud")?;
    let mut entries = Vec::new();
    let mut offset = 0usize;
    loop {
        let rows: Vec<mysql_async::Row> = connection
            .exec(
                format!(
                    "SELECT url,source_metadata_json FROM `{DOCUMENTS_TABLE}` \
                     ORDER BY url_hash LIMIT ?,?"
                ),
                (offset as u64, READ_BATCH as u64),
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let Some(url) = row.get::<Option<String>, _>(0).flatten() else {
                continue;
            };
            let metadata = row.get::<Option<String>, _>(1).flatten();
            entries.push((url, links_from_metadata(metadata.as_deref())));
        }
        if rows.len() < READ_BATCH {
            break;
        }
        offset += rows.len();
    }
    Ok(entries)
}

async fn write_tidb_pagerank(pool: &Pool, chunk: &[(String, f64)]) -> Result<usize> {
    if chunk.is_empty() {
        return Ok(0);
    }
    let (statement, params) = score_update_statement(chunk);
    let mut connection = pool.get_conn().await.context("connecting to TiDB Cloud")?;
    connection
        .exec_drop(statement, mysql_async::Params::Positional(params))
        .await
        .context("writing PageRank scores to TiDB")?;
    Ok(chunk.len())
}

/// Builds one chunked `UPDATE ... SET pagerank = CASE url_hash ... END`
/// statement. Hashes are inlined as hex literals (they are generated SHA-256
/// digests); each score is a bound double parameter.
fn score_update_statement(chunk: &[(String, f64)]) -> (String, Vec<Value>) {
    let mut cases = String::with_capacity(chunk.len() * 96);
    let mut keys = String::with_capacity(chunk.len() * 80);
    let mut params = Vec::with_capacity(chunk.len());
    for (index, (hash, score)) in chunk.iter().enumerate() {
        if index > 0 {
            cases.push(' ');
            keys.push(',');
        }
        cases.push_str(&format!("WHEN UNHEX('{hash}') THEN ?"));
        keys.push_str(&format!("UNHEX('{hash}')"));
        params.push(Value::Double(*score));
    }
    (
        format!(
            "UPDATE `{DOCUMENTS_TABLE}` SET pagerank = CASE url_hash {cases} ELSE pagerank END \
             WHERE url_hash IN ({keys})"
        ),
        params,
    )
}

async fn write_mongo_pagerank(
    collection: &Collection<Document>,
    scores: &[(String, f64)],
) -> (usize, usize) {
    let mut updated = 0usize;
    let mut errors = 0usize;
    for (url, score) in scores {
        match collection
            .update_one(doc! { "url": url }, doc! { "$set": { "pagerank": score } })
            .await
        {
            Ok(_) => updated += 1,
            Err(error) => {
                errors += 1;
                if errors <= 3 {
                    eprintln!("Could not write a MongoDB PageRank score: {error}");
                }
            }
        }
    }
    (updated, errors)
}

/// Loads stored PageRank scores (0..=100) keyed by document URL. TiDB is the
/// authoritative source; MongoDB only fills in URLs TiDB does not report.
pub async fn load_pagerank_scores(
    pool: Option<&Pool>,
    mongo: Option<&Collection<Document>>,
) -> HashMap<String, f64> {
    let mut scores = HashMap::new();
    if let Some(pool) = pool {
        match load_tidb_pagerank(pool).await {
            Ok(loaded) => scores = loaded,
            Err(error) => eprintln!("Could not load TiDB PageRank scores: {error}"),
        }
    }
    if let Some(collection) = mongo {
        match load_mongo_pagerank(collection).await {
            Ok(loaded) => {
                for (url, score) in loaded {
                    scores.entry(url).or_insert(score);
                }
            }
            Err(error) => eprintln!("Could not load MongoDB PageRank scores: {error}"),
        }
    }
    scores
}

async fn load_tidb_pagerank(pool: &Pool) -> Result<HashMap<String, f64>> {
    let mut connection = pool.get_conn().await.context("connecting to TiDB Cloud")?;
    let rows: Vec<mysql_async::Row> = connection
        .exec(
            format!("SELECT url,pagerank FROM `{DOCUMENTS_TABLE}` WHERE pagerank > 0"),
            mysql_async::Params::Empty,
        )
        .await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let url = row.get::<Option<String>, _>(0).flatten()?;
            let score = row.get::<Option<f64>, _>(1).flatten()?;
            (score > 0.0).then_some((url, score))
        })
        .collect())
}

async fn load_mongo_pagerank(collection: &Collection<Document>) -> Result<HashMap<String, f64>> {
    use futures_util::TryStreamExt;

    let mut cursor = collection
        .find(doc! { "pagerank": { "$gt": 0.0 } })
        .projection(doc! { "url": 1, "pagerank": 1 })
        .await?;
    let mut scores = HashMap::new();
    while let Some(document) = cursor.try_next().await? {
        let Ok(url) = document.get_str("url") else {
            continue;
        };
        if let Some(score) = document
            .get_f64("pagerank")
            .ok()
            .filter(|value| *value > 0.0)
        {
            scores.insert(url.to_owned(), score);
        }
    }
    Ok(scores)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_graph_canonicalizes_dedupes_and_skips_self_loops() {
        let entries = vec![
            (
                "https://Example.com/A".to_owned(),
                vec![
                    "https://example.com/b".to_owned(),
                    "https://example.com/b".to_owned(),
                    "https://Example.com/A".to_owned(),
                    "not a url".to_owned(),
                ],
            ),
            (
                "https://example.com/b".to_owned(),
                vec!["https://example.com/c".to_owned()],
            ),
        ];
        let graph = build_link_graph(&entries);
        let a = canonicalize_url("https://Example.com/A").unwrap();
        let b = canonicalize_url("https://example.com/b").unwrap();
        let c = canonicalize_url("https://example.com/c").unwrap();
        assert_eq!(graph[&a], vec![b.clone()]);
        assert_eq!(graph[&b], vec![c.clone()]);
        // `c` is only a link target; pages only enter the graph when crawled.
        assert!(!graph.contains_key(&c));
    }

    #[test]
    fn empty_or_invalid_graph_inputs_are_safe() {
        assert!(build_link_graph(&[]).is_empty());
        let entries = vec![("not a url".to_owned(), vec!["https://a.test".to_owned()])];
        assert!(build_link_graph(&entries).is_empty());
        let entries = vec![(
            "https://a.test".to_owned(),
            vec!["not a url".to_owned(), "https://a.test".to_owned()],
        )];
        let graph = build_link_graph(&entries);
        assert_eq!(graph.len(), 1);
        assert!(graph.values().all(Vec::is_empty));
    }

    #[test]
    fn metadata_links_accept_object_and_array_shapes() {
        assert_eq!(
            links_from_metadata(Some(r#"{"links":["https://a.test/x"]}"#)),
            vec!["https://a.test/x".to_owned()]
        );
        assert_eq!(
            links_from_metadata(Some(r#"[{"links":["https://a.test/x"]}]"#)),
            vec!["https://a.test/x".to_owned()]
        );
        assert!(links_from_metadata(Some("[]")).is_empty());
        assert!(links_from_metadata(Some("not json")).is_empty());
        assert!(links_from_metadata(Some(r#"{"other":1}"#)).is_empty());
        assert!(links_from_metadata(Some("   ")).is_empty());
        assert!(links_from_metadata(None).is_empty());
    }

    #[test]
    fn score_update_statement_binds_one_parameter_per_row() {
        let chunk = vec![("a".repeat(64), 12.5), ("b".repeat(64), 0.0)];
        let (statement, params) = score_update_statement(&chunk);
        assert_eq!(params.len(), 2);
        assert_eq!(statement.matches("WHEN UNHEX('").count(), 2);
        assert_eq!(statement.matches("IN (UNHEX('").count(), 1);
        assert!(statement.starts_with("UPDATE `search_documents` SET pagerank = CASE url_hash"));
        assert!(statement.contains("ELSE pagerank END WHERE url_hash IN (UNHEX('aaa"));
        assert!(statement.ends_with("'))"));
    }
}
