//! Offline evaluation harness for Velo Search.
//!
//! Usage:
//!   eval                     run the query set, print metrics, write eval/runs/<ts>.json
//!   eval --baseline          same, but write eval/baseline.json (committed)
//!   eval --pool              fetch top-30 per query into eval/pool/pool.csv for labelling
//!   eval --coverage-only     only report label coverage against the stores
//!   eval --compare FILE      compare against a stored run and apply acceptance rules
//!
//! The harness never writes to TiDB or MongoDB: store access is SELECT/GET only.

use anyhow::{bail, Context, Result};
use futures_util::TryStreamExt;
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient};
use mysql_async::{prelude::Queryable, OptsBuilder, Pool, SslOpts};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const RELEVANT_GRADE: u8 = 2;
const METRIC_K: usize = 10;
const DEFAULT_POOL_DEPTH: usize = 10;
const CHUNK_SIZE: usize = 100;
const SHUFFLE_SEED: u64 = 0x5EED_1234_ABCD_0001;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Query {
    id: String,
    query: String,
    lang: String,
    intent: String,
    freshness_sensitive: bool,
    label_source: String,
    #[serde(default)]
    labels: HashMap<String, u8>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    labelers: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct Hit {
    url: String,
    title: String,
    snippet: String,
    rank: usize,
    #[serde(skip)]
    grade: u8,
    #[serde(skip)]
    in_corpus: bool,
    #[serde(skip)]
    age_days: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PoolRow {
    pid: String,
    query_id: String,
    query: String,
    url: String,
    title: String,
    snippet: String,
    #[serde(default)]
    rank: Option<usize>,
    mode: String,
    #[serde(default)]
    grade: Option<u8>,
    #[serde(default)]
    labeler: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PoolStats {
    rows: usize,
    labeled_rows: usize,
    coverage: f64,
}

#[derive(Debug, Clone)]
struct LabelInput {
    pid: String,
    grade: u8,
    labeler: String,
}

#[derive(Debug, Default)]
struct LabelValidation {
    valid: HashMap<String, LabelInput>,
    unknown: Vec<String>,
    conflicts: Vec<String>,
    invalid_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
struct QueryResult {
    id: String,
    query: String,
    lang: String,
    intent: String,
    freshness_sensitive: bool,
    label_source: String,
    returned: usize,
    latency_ms: f64,
    server_ms: Option<f64>,
    hits: Vec<Hit>,
    ndcg: f64,
    mrr: f64,
    recall: f64,
    precision: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Metrics {
    ndcg: f64,
    mrr: f64,
    recall: f64,
    precision: f64,
    zero_result_rate: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    coverage: f64,
    labeled_urls: usize,
    present_urls: usize,
    median_top10_age_days: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct DocCounts {
    tidb: Option<u64>,
    mongo: Option<u64>,
}

#[derive(Debug, Serialize)]
struct RunRecord {
    timestamp: String,
    git_commit: Option<String>,
    doc_counts: DocCounts,
    env_flags: HashMap<String, String>,
    config: HashMap<String, String>,
    overall: Metrics,
    slices: HashMap<String, Metrics>,
    #[serde(default)]
    pool: PoolStats,
    results: Vec<QueryResult>,
}

/// Output written to baseline.json: metadata + aggregate metrics only,
/// no raw result hits.
#[derive(Debug, Serialize)]
struct BaselineOutput {
    timestamp: String,
    git_commit: Option<String>,
    doc_counts: DocCounts,
    env_flags: HashMap<String, String>,
    config: HashMap<String, String>,
    overall: Metrics,
    slices: HashMap<String, Metrics>,
    pool: PoolStats,
}

// Metric primitives (pure, unit-tested)
// ---------------------------------------------------------------------------

fn gain(grade: u8) -> f64 {
    (1u32 << grade.min(30)) as f64 - 1.0
}

fn dcg_at(grades: &[u8], k: usize) -> f64 {
    grades
        .iter()
        .take(k)
        .enumerate()
        .map(|(index, grade)| gain(*grade) / ((index + 2) as f64).log2())
        .sum()
}

pub fn ndcg_at(grades: &[u8], k: usize) -> f64 {
    let mut ideal = grades.to_vec();
    ideal.sort_unstable_by(|left, right| right.cmp(left));
    let ideal = dcg_at(&ideal, k);
    if ideal <= 0.0 {
        return 0.0;
    }
    dcg_at(grades, k) / ideal
}

pub fn mrr_at(grades: &[u8], k: usize) -> f64 {
    match grades
        .iter()
        .take(k)
        .position(|grade| *grade >= RELEVANT_GRADE)
    {
        Some(index) => 1.0 / (index + 1) as f64,
        None => 0.0,
    }
}

pub fn recall_at(grades: &[u8], k: usize, total_relevant: usize) -> f64 {
    if total_relevant == 0 {
        return 0.0;
    }
    let found = grades
        .iter()
        .take(k)
        .filter(|grade| **grade >= RELEVANT_GRADE)
        .count();
    found as f64 / total_relevant as f64
}

pub fn precision_at(grades: &[u8], k: usize) -> f64 {
    if k == 0 {
        return 0.0;
    }
    let found = grades
        .iter()
        .take(k)
        .filter(|grade| **grade >= RELEVANT_GRADE)
        .count();
    found as f64 / k as f64
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let position = (sorted.len() - 1) as f64 * (p / 100.0);
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        return sorted[lower];
    }
    let weight = position - lower as f64;
    sorted[lower] + (sorted[upper] - sorted[lower]) * weight
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    } else {
        Some(sorted[mid])
    }
}

// ---------------------------------------------------------------------------
// Store access (read-only)
// ---------------------------------------------------------------------------

struct Corpus {
    urls: HashSet<String>,
    crawled_at: HashMap<String, f64>,
    doc_counts: DocCounts,
}

fn env_var(names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .filter(|value| !value.trim().is_empty())
}

async fn load_corpus() -> Result<Corpus> {
    let mut urls = HashSet::new();
    let mut crawled_at = HashMap::new();
    let mut doc_counts = DocCounts::default();

    if let (Some(host), Some(user), Some(password)) = (
        env_var(&["TIDB_HOST"]),
        env_var(&["TIDB_USER"]),
        env_var(&["TIDB_PASSWORD"]),
    ) {
        let port = env_var(&["TIDB_PORT"])
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(4000);
        let database = env_var(&["TIDB_DATABASE", "VELO_TIDB_DATABASE"]);
        let mut ssl = SslOpts::default();
        if let Some(ca) = env_var(&["TIDB_SSL_CA"]) {
            ssl = ssl.with_root_certs(vec![PathBuf::from(ca).into()]);
        }
        let options = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(user))
            .pass(Some(password))
            .db_name(database)
            .ssl_opts(Some(ssl));
        let pool = Pool::new(options);
        match pool.get_conn().await {
            Ok(mut connection) => {
                let rows: Vec<(String, Option<f64>)> = connection
                    .query("SELECT canonical_url, crawled_at FROM search_documents")
                    .await
                    .unwrap_or_default();
                doc_counts.tidb = Some(rows.len() as u64);
                for (url, crawled) in rows {
                    let key = canonical_url(&url);
                    if key.is_empty() {
                        continue;
                    }
                    crawled_at
                        .entry(key.clone())
                        .or_insert_with(|| crawled.unwrap_or(0.0));
                    urls.insert(key);
                }
            }
            Err(error) => eprintln!("eval: TiDB unavailable ({error})"),
        }
        drop(pool);
    }

    if let Some(uri) = env_var(&["MONGO_URI", "MONGODB_URI"]) {
        match ClientOptions::parse(&uri).await {
            Ok(options) => match MongoClient::with_options(options) {
                Ok(client) => {
                    let database_name = env_var(&["MONGO_DATABASE", "MONGO_DB"]);
                    let collection_name = env_var(&["MONGO_SEARCH_COLLECTION", "MONGO_COLLECTION"])
                        .unwrap_or_else(|| "Crawled_Pages".to_string());
                    let database = match database_name {
                        Some(name) => Some(client.database(&name)),
                        None => client.default_database(),
                    };
                    if let Some(database) = database {
                        let collection =
                            database.collection::<mongodb::bson::Document>(&collection_name);
                        let pipeline = vec![
                            doc! {"$project": {"canonical_url": 1, "url": 1, "crawled_at": 1}},
                        ];
                        match collection.aggregate(pipeline).await {
                            Ok(mut cursor) => {
                                let mut count = 0u64;
                                while let Ok(Some(document)) = cursor.try_next().await {
                                    count += 1;
                                    let url = document
                                        .get_str("canonical_url")
                                        .ok()
                                        .map(str::to_string)
                                        .or_else(|| {
                                            document.get_str("url").ok().map(str::to_string)
                                        });
                                    if let Some(url) = url {
                                        let key = canonical_url(&url);
                                        if key.is_empty() {
                                            continue;
                                        }
                                        let crawled = document.get_f64("crawled_at").unwrap_or(0.0);
                                        crawled_at.entry(key.clone()).or_insert(crawled);
                                        urls.insert(key);
                                    }
                                }
                                doc_counts.mongo = Some(count);
                            }
                            Err(error) => eprintln!("eval: MongoDB unavailable ({error})"),
                        }
                    }
                }
                Err(error) => eprintln!("eval: MongoDB client error ({error})"),
            },
            Err(error) => eprintln!("eval: MongoDB URI error ({error})"),
        }
    }

    Ok(Corpus {
        urls,
        crawled_at,
        doc_counts,
    })
}

// ---------------------------------------------------------------------------
// HTTP search
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SearchResponse {
    results: Vec<SearchResponseHit>,
    #[serde(default)]
    search_time_ms: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SearchResponseHit {
    url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    content: String,
}

async fn search(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    limit: usize,
) -> Result<(Vec<Hit>, Option<f64>)> {
    let url = format!("{base}/search?q={}&limit={}", urlencode(query), limit);
    let response = client
        .get(&url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .context("search request failed")?;
    let status = response.status();
    if !status.is_success() {
        bail!("search returned HTTP {status}");
    }
    let body: SearchResponse = response
        .json()
        .await
        .context("search response was not JSON")?;
    let hits = body
        .results
        .into_iter()
        .enumerate()
        .map(|(index, hit)| Hit {
            url: hit.url,
            title: hit.title,
            snippet: hit.content.chars().take(200).collect(),
            rank: index + 1,
            grade: 0,
            in_corpus: false,
            age_days: None,
        })
        .collect();
    Ok((hits, body.search_time_ms))
}

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Query loading and coverage
// ---------------------------------------------------------------------------

fn load_queries(path: &Path) -> Result<Vec<Query>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("could not read {}", path.display()))?;
    let mut queries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let query: Query = serde_json::from_str(line)
            .with_context(|| format!("invalid JSON on line {}", index + 1))?;
        if query.id.trim().is_empty() || query.query.trim().is_empty() {
            bail!("line {} is missing id or query", index + 1);
        }
        queries.push(query);
    }
    Ok(queries)
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Parse RFC-4180-ish CSV with quoted fields and escaped quotes.
fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            }
            '"' if field.is_empty() => in_quotes = true,
            ',' if !in_quotes => row.push(std::mem::take(&mut field)),
            '\r' if !in_quotes => {}
            '\n' if !in_quotes => {
                row.push(std::mem::take(&mut field));
                if !(row.len() == 1 && row[0].is_empty()) {
                    rows.push(std::mem::take(&mut row));
                } else {
                    row.clear();
                }
            }
            _ => field.push(ch),
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows
}

fn stable_pid(query_id: &str, canonical: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn combine_modes(existing: &str, new: &str) -> String {
    let mut modes = BTreeSet::new();
    for mode in existing.split('+').chain(new.split('+')) {
        if !mode.is_empty() {
            modes.insert(mode.to_string());
        }
    }
    modes.into_iter().collect::<Vec<_>>().join("+")
}

fn merge_pool_hit(
    rows: &mut HashMap<String, PoolRow>,
    query: &Query,
    mode: &str,
    canonical: &str,
    hit: &Hit,
) {
    if canonical.is_empty() {
        return;
    }
    if let Some(row) = rows.get_mut(canonical) {
        let better = row.rank.is_none_or(|rank| hit.rank < rank);
        row.rank = Some(row.rank.map_or(hit.rank, |rank| rank.min(hit.rank)));
        row.mode = combine_modes(&row.mode, mode);
        if better {
            row.title = hit.title.clone();
            row.snippet = hit.snippet.clone();
        }
    } else {
        rows.insert(
            canonical.to_string(),
            PoolRow {
                pid: stable_pid(&query.id, canonical),
                query_id: query.id.clone(),
                query: query.query.clone(),
                url: canonical.to_string(),
                title: hit.title.clone(),
                snippet: hit.snippet.clone(),
                rank: Some(hit.rank),
                mode: mode.to_string(),
                grade: None,
                labeler: String::new(),
            },
        );
    }
}

fn parse_extra_urls(path: &Path) -> Result<HashMap<String, Vec<(String, u8)>>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = std::fs::read_to_string(path)?;
    let rows = parse_csv(&text);
    let mut map: HashMap<String, Vec<(String, u8)>> = HashMap::new();
    for row in rows.iter().skip(1) {
        if row.len() < 3 {
            continue;
        }
        let grade = row[2].trim().parse::<u8>().unwrap_or(0).min(3);
        map.entry(row[0].trim().to_string())
            .or_default()
            .push((row[1].trim().to_string(), grade));
    }
    Ok(map)
}

fn write_pool_csv(path: &Path, rows: &[PoolRow]) -> Result<()> {
    let mut out = String::from("pid,query_id,query,url,title,snippet,rank,mode,grade,labeler\n");
    for row in rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            csv_field(&row.pid),
            csv_field(&row.query_id),
            csv_field(&row.query),
            csv_field(&row.url),
            csv_field(&row.title),
            csv_field(&row.snippet),
            row.rank.map(|rank| rank.to_string()).unwrap_or_default(),
            csv_field(&row.mode),
            row.grade.map(|grade| grade.to_string()).unwrap_or_default(),
            csv_field(&row.labeler),
        ));
    }
    std::fs::write(path, out)?;
    Ok(())
}

fn read_pool_rows(path: &Path) -> Result<Vec<PoolRow>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("could not read {}", path.display()))?;
    let rows = parse_csv(&text);
    let header = rows.first().context("pool csv is empty")?;
    let column = |name: &str| -> Result<usize> {
        header
            .iter()
            .position(|value| value == name)
            .with_context(|| format!("pool csv is missing column {name}"))
    };
    let pid_col = header.iter().position(|value| value == "pid");
    let query_id_col = column("query_id")?;
    let query_col = column("query")?;
    let url_col = column("url")?;
    let title_col = column("title")?;
    let snippet_col = column("snippet")?;
    let rank_col = header.iter().position(|value| value == "rank");
    let mode_col = header.iter().position(|value| value == "mode");
    let grade_col = header.iter().position(|value| value == "grade");
    let labeler_col = header.iter().position(|value| value == "labeler");
    let mut parsed = Vec::new();
    for row in rows.iter().skip(1) {
        if row.len() <= query_id_col.max(url_col) {
            continue;
        }
        let query_id = row[query_id_col].clone();
        let url = row[url_col].clone();
        let canonical = canonical_url(&url);
        let pid = pid_col
            .and_then(|index| row.get(index))
            .filter(|value| !value.is_empty())
            .cloned()
            .unwrap_or_else(|| stable_pid(&query_id, &canonical));
        let rank = rank_col
            .and_then(|index| row.get(index))
            .and_then(|value| value.parse::<usize>().ok());
        let grade = grade_col
            .and_then(|index| row.get(index))
            .and_then(|value| value.parse::<u8>().ok())
            .filter(|grade| *grade <= 3);
        parsed.push(PoolRow {
            pid,
            query_id,
            query: row.get(query_col).cloned().unwrap_or_default(),
            url: canonical,
            title: row.get(title_col).cloned().unwrap_or_default(),
            snippet: row.get(snippet_col).cloned().unwrap_or_default(),
            rank,
            mode: mode_col
                .and_then(|index| row.get(index))
                .cloned()
                .unwrap_or_else(|| "hybrid".to_string()),
            grade,
            labeler: labeler_col
                .and_then(|index| row.get(index))
                .cloned()
                .unwrap_or_default(),
        });
    }
    Ok(parsed)
}

fn pool_stats_from_rows(rows: &[PoolRow], queries: &[Query]) -> PoolStats {
    let query_map: HashMap<&str, &Query> = queries
        .iter()
        .map(|query| (query.id.as_str(), query))
        .collect();
    let labeled_rows = rows
        .iter()
        .filter(|row| {
            query_map
                .get(row.query_id.as_str())
                .is_some_and(|query| query.labels.contains_key(&canonical_url(&row.url)))
        })
        .count();
    PoolStats {
        rows: rows.len(),
        labeled_rows,
        coverage: if rows.is_empty() {
            0.0
        } else {
            labeled_rows as f64 / rows.len() as f64
        },
    }
}

fn hash_id(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

fn shuffle<T>(items: &mut [T], seed: u64) {
    let mut state = seed;
    for index in (1..items.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let target = (state as usize) % (index + 1);
        items.swap(index, target);
    }
}

fn write_labeling_chunk(path: &Path, rows: &[&PoolRow]) -> Result<()> {
    let mut out = String::from("pid,query_id,query,url,title,snippet,grade\n");
    for row in rows {
        let snippet: String = row
            .snippet
            .chars()
            .take(150)
            .collect::<String>()
            .replace(['\r', '\n'], " ");
        let title = row.title.replace(['\r', '\n'], " ");
        out.push_str(&format!(
            "{},{},{},{},{},{},\n",
            csv_field(&row.pid),
            csv_field(&row.query_id),
            csv_field(&row.query),
            csv_field(&row.url),
            csv_field(&title),
            csv_field(&snippet),
        ));
    }
    std::fs::write(path, out)?;
    Ok(())
}

fn write_labeling_sheets(queries: &[Query], out_dir: &Path) -> Result<()> {
    let pool_path = out_dir.join("pool").join("pool.csv");
    let rows = read_pool_rows(&pool_path)?;
    let mut by_query: HashMap<&str, Vec<&PoolRow>> = HashMap::new();
    for row in &rows {
        by_query.entry(row.query_id.as_str()).or_default().push(row);
    }
    let mut groups: HashMap<&str, Vec<&str>> = HashMap::new();
    for query in queries {
        let group = if query.lang == "ar-eg" || query.lang == "arabizi" {
            "owner"
        } else {
            "silver"
        };
        groups.entry(group).or_default().push(query.id.as_str());
    }
    let chunks_dir = out_dir.join("pool").join("chunks");
    std::fs::create_dir_all(&chunks_dir)?;
    for entry in std::fs::read_dir(&chunks_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("csv") {
            let _ = std::fs::remove_file(path);
        }
    }
    for group in ["owner", "silver"] {
        let mut ids = groups.get(group).cloned().unwrap_or_default();
        ids.sort_unstable();
        let mut chunks: Vec<Vec<&PoolRow>> = Vec::new();
        let mut current: Vec<&PoolRow> = Vec::new();
        for id in ids {
            let mut query_rows = by_query.get(id).cloned().unwrap_or_default();
            let seed = SHUFFLE_SEED ^ hash_id(id);
            shuffle(&mut query_rows, seed);
            if query_rows.len() > CHUNK_SIZE {
                if !current.is_empty() {
                    chunks.push(std::mem::take(&mut current));
                }
                for chunk in query_rows.chunks(CHUNK_SIZE) {
                    chunks.push(chunk.to_vec());
                }
            } else {
                if !current.is_empty() && current.len() + query_rows.len() > CHUNK_SIZE {
                    chunks.push(std::mem::take(&mut current));
                }
                current.extend(query_rows);
            }
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        let mut total_rows = 0;
        for (index, chunk) in chunks.iter().enumerate() {
            let target = chunks_dir.join(format!("{group}_{:02}.csv", index + 1));
            write_labeling_chunk(&target, chunk)?;
            total_rows += chunk.len();
        }
        println!(
            "eval: {group} labeling rows={} chunks={}",
            total_rows,
            chunks.len()
        );
    }
    Ok(())
}

fn summarize_label_source(labelers: &HashMap<String, String>) -> String {
    if labelers.is_empty() {
        return "unlabeled".to_string();
    }
    let has_owner = labelers.values().any(|value| value == "owner");
    let has_silver = labelers.values().any(|value| value == "silver");
    match (has_owner, has_silver) {
        (true, true) => "mixed",
        (true, false) => "owner",
        (false, true) => "silver",
        (false, false) => "unlabeled",
    }
    .to_string()
}

fn validate_label_rows(
    rows: &[Vec<String>],
    header: &[String],
    known_pids: &HashSet<String>,
) -> Result<LabelValidation> {
    let column = |name: &str| -> Result<usize> {
        header
            .iter()
            .position(|value| value == name)
            .with_context(|| format!("labels csv is missing column {name}"))
    };
    let pid_col = column("pid")?;
    let grade_col = column("grade")?;
    let labeler_col = column("labeler")?;
    let mut valid: HashMap<String, LabelInput> = HashMap::new();
    let mut conflicts = Vec::new();
    let mut unknown = Vec::new();
    let mut invalid_rows = 0usize;
    for row in rows.iter().skip(1) {
        if row.len() <= pid_col.max(grade_col).max(labeler_col) {
            invalid_rows += 1;
            continue;
        }
        let pid = row[pid_col].trim();
        if pid.is_empty() {
            continue;
        }
        let Some(grade) = row[grade_col]
            .trim()
            .parse::<u8>()
            .ok()
            .filter(|grade| *grade <= 3)
        else {
            invalid_rows += 1;
            continue;
        };
        let labeler = row[labeler_col].trim().to_ascii_lowercase();
        if labeler != "owner" && labeler != "silver" {
            invalid_rows += 1;
            continue;
        }
        if !known_pids.contains(pid) {
            unknown.push(pid.to_string());
            continue;
        }
        if let Some(existing) = valid.get(pid) {
            if existing.grade != grade || existing.labeler != labeler {
                conflicts.push(pid.to_string());
            }
        } else {
            valid.insert(
                pid.to_string(),
                LabelInput {
                    pid: pid.to_string(),
                    grade,
                    labeler,
                },
            );
        }
    }
    for pid in &conflicts {
        valid.remove(pid);
    }
    Ok(LabelValidation {
        valid,
        unknown,
        conflicts,
        invalid_rows,
    })
}

fn apply_labels(
    queries: &mut [Query],
    labels_path: &Path,
    queries_path: &Path,
    out_dir: &Path,
) -> Result<()> {
    let pool_path = out_dir.join("pool").join("pool.csv");
    let pool_rows = read_pool_rows(&pool_path)?;
    let pool_by_pid: HashMap<&str, &PoolRow> = pool_rows
        .iter()
        .map(|row| (row.pid.as_str(), row))
        .collect();
    let text = std::fs::read_to_string(labels_path)
        .with_context(|| format!("could not read {}", labels_path.display()))?;
    let rows = parse_csv(&text);
    let header = rows.first().context("labels csv is empty")?;
    let known_pids: HashSet<String> = pool_rows.iter().map(|row| row.pid.clone()).collect();
    let validation = validate_label_rows(&rows, header, &known_pids)?;
    let (valid, unknown, conflicts, invalid_rows) = (
        validation.valid,
        validation.unknown,
        validation.conflicts,
        validation.invalid_rows,
    );
    let query_index: HashMap<String, usize> = queries
        .iter()
        .enumerate()
        .map(|(index, query)| (query.id.clone(), index))
        .collect();
    let mut applied = 0usize;
    for input in valid.values() {
        let Some(row) = pool_by_pid.get(input.pid.as_str()) else {
            continue;
        };
        let Some(&index) = query_index.get(&row.query_id) else {
            continue;
        };
        let canonical = canonical_url(&row.url);
        if canonical.is_empty() {
            continue;
        }
        queries[index].labels.insert(canonical.clone(), input.grade);
        queries[index]
            .labelers
            .insert(canonical, input.labeler.clone());
        applied += 1;
    }
    for query in queries.iter_mut() {
        query.label_source = summarize_label_source(&query.labelers);
    }
    let mut out = String::new();
    for query in queries.iter() {
        out.push_str(&serde_json::to_string(query)?);
        out.push('\n');
    }
    std::fs::write(queries_path, out)?;
    println!(
        "eval: applied={applied} unknown_pids={} conflicts={} invalid_rows={invalid_rows}",
        unknown.len(),
        conflicts.len()
    );
    if !unknown.is_empty() {
        println!(
            "eval: first unknown pids: {}",
            unknown
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    if !conflicts.is_empty() {
        println!("eval: conflicting pids: {}", conflicts.join(","));
    }
    Ok(())
}

fn label_coverage(queries: &[Query], corpus: &Corpus) -> (f64, usize, usize) {
    let mut labeled_urls = 0usize;
    let mut present = 0usize;
    for query in queries {
        for (url, grade) in &query.labels {
            if *grade < RELEVANT_GRADE {
                continue;
            }
            labeled_urls += 1;
            if corpus.urls.contains(&canonical_url(url)) {
                present += 1;
            }
        }
    }
    let rate = if labeled_urls == 0 {
        0.0
    } else {
        present as f64 / labeled_urls as f64
    };
    (rate, labeled_urls, present)
}

fn report_coverage(queries: &[Query], corpus: &Corpus) {
    let (rate, labeled_urls, present) = label_coverage(queries, corpus);
    println!(
        "eval: coverage {}/{} = {:.2}%",
        present,
        labeled_urls,
        rate * 100.0
    );
    let mut gaps = String::new();
    for query in queries {
        for (url, grade) in &query.labels {
            if *grade < RELEVANT_GRADE {
                continue;
            }
            if !corpus.urls.contains(&canonical_url(url)) {
                gaps.push_str(&format!(
                    "{},{},{}\n",
                    csv_field(&query.id),
                    csv_field(url),
                    grade
                ));
            }
        }
    }
    if !gaps.is_empty() {
        let _ = std::fs::write("eval/coverage_gaps.jsonl", gaps);
        println!("eval: wrote eval/coverage_gaps.jsonl");
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let mut args = std::env::args().skip(1);
    let mut server = "http://127.0.0.1:8000".to_string();
    let mut base_urls: Vec<(String, String)> = Vec::new();
    let mut queries_path = PathBuf::from("eval/queries.jsonl");
    let mut out_dir = PathBuf::from("eval");
    let mut pool = false;
    let mut top = DEFAULT_POOL_DEPTH;
    let mut labeling_sheet = false;
    let mut apply_labels_path: Option<PathBuf> = None;
    let mut baseline = false;
    let mut coverage_only = false;
    let mut compare: Option<PathBuf> = None;

    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--server" => server = args.next().context("--server needs a value")?,
            "--base-url" => {
                let value = args.next().context("--base-url needs a value")?;
                let (name, url) = value
                    .split_once('=')
                    .with_context(|| format!("--base-url must be NAME=URL, got {value}"))?;
                if name.trim().is_empty() || url.trim().is_empty() {
                    bail!("--base-url must be NAME=URL, got {value}");
                }
                base_urls.push((name.trim().to_string(), url.trim().to_string()));
            }
            "--queries" => {
                queries_path = PathBuf::from(args.next().context("--queries needs a value")?)
            }
            "--out-dir" => out_dir = PathBuf::from(args.next().context("--out-dir needs a value")?),
            "--pool" => pool = true,
            "--top" => {
                top = args
                    .next()
                    .context("--top needs a value")?
                    .parse::<usize>()
                    .context("--top must be a number")?;
                if top == 0 {
                    bail!("--top must be at least 1");
                }
            }
            "--labeling-sheet" => labeling_sheet = true,
            "--apply-labels" => {
                apply_labels_path = Some(PathBuf::from(
                    args.next().context("--apply-labels needs a value")?,
                ))
            }
            "--baseline" => baseline = true,
            "--coverage-only" => coverage_only = true,
            "--compare" => {
                compare = Some(PathBuf::from(
                    args.next().context("--compare needs a value")?,
                ))
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    if base_urls.is_empty() {
        base_urls.push(("hybrid".to_string(), server.clone()));
    }

    let mut queries = load_queries(&queries_path)?;
    if queries.is_empty() {
        bail!("no queries in {}", queries_path.display());
    }
    println!("eval: loaded {} queries", queries.len());

    if let Some(labels_path) = apply_labels_path {
        apply_labels(&mut queries, &labels_path, &queries_path, &out_dir)?;
        return Ok(());
    }
    if labeling_sheet {
        write_labeling_sheets(&queries, &out_dir)?;
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .user_agent("velo-eval/1.0")
        .build()?;
    if pool {
        let extras = parse_extra_urls(&out_dir.join("extra_urls.csv"))?;
        let mut all_rows = Vec::new();
        for query in &queries {
            let mut rows: HashMap<String, PoolRow> = HashMap::new();
            for (mode, base) in &base_urls {
                match search(&client, base, &query.query, top).await {
                    Ok((hits, _)) => {
                        for hit in &hits {
                            let canonical = canonical_url(&hit.url);
                            merge_pool_hit(&mut rows, query, mode, &canonical, hit);
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "eval: pool fetch failed for {} on {} ({error})",
                            query.id, mode
                        )
                    }
                }
            }
            if let Some(extra) = extras.get(&query.id) {
                for (url, grade) in extra {
                    let canonical = canonical_url(url);
                    if canonical.is_empty() {
                        continue;
                    }
                    if let Some(row) = rows.get_mut(&canonical) {
                        if row.grade.is_none() {
                            row.grade = Some(*grade);
                            row.labeler = "owner".to_string();
                        }
                    } else {
                        rows.insert(
                            canonical.clone(),
                            PoolRow {
                                pid: stable_pid(&query.id, &canonical),
                                query_id: query.id.clone(),
                                query: query.query.clone(),
                                url: canonical,
                                title: String::new(),
                                snippet: String::new(),
                                rank: None,
                                mode: "extra".to_string(),
                                grade: Some(*grade),
                                labeler: "owner".to_string(),
                            },
                        );
                    }
                }
            }
            let mut query_rows: Vec<PoolRow> = rows.into_values().collect();
            query_rows.sort_by(|left, right| {
                left.rank
                    .unwrap_or(usize::MAX)
                    .cmp(&right.rank.unwrap_or(usize::MAX))
                    .then_with(|| left.url.cmp(&right.url))
            });
            all_rows.extend(query_rows);
        }
        all_rows.sort_by(|left, right| {
            left.query_id
                .cmp(&right.query_id)
                .then_with(|| {
                    left.rank
                        .unwrap_or(usize::MAX)
                        .cmp(&right.rank.unwrap_or(usize::MAX))
                })
                .then_with(|| left.url.cmp(&right.url))
        });
        let dir = out_dir.join("pool");
        std::fs::create_dir_all(&dir)?;
        let target = dir.join("pool.csv");
        write_pool_csv(&target, &all_rows)?;
        let stats = pool_stats_from_rows(&all_rows, &queries);
        println!(
            "eval: wrote {} ({} rows, {} labeled)",
            target.display(),
            stats.rows,
            stats.labeled_rows
        );
        return Ok(());
    }

    let corpus = load_corpus().await?;
    println!(
        "eval: corpus urls={} tidb={:?} mongo={:?}",
        corpus.urls.len(),
        corpus.doc_counts.tidb,
        corpus.doc_counts.mongo
    );

    if coverage_only {
        report_coverage(&queries, &corpus);
        return Ok(());
    }

    let mut results = Vec::new();
    for query in &queries {
        let started = Instant::now();
        let outcome = search(&client, &server, &query.query, METRIC_K).await;
        let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
        let (mut hits, server_ms) = match outcome {
            Ok(value) => value,
            Err(error) => {
                eprintln!("eval: {} failed ({error})", query.id);
                (Vec::new(), None)
            }
        };

        let total_relevant = query
            .labels
            .values()
            .filter(|grade| **grade >= RELEVANT_GRADE)
            .count();
        for hit in hits.iter_mut().take(METRIC_K) {
            let key = canonical_url(&hit.url);
            hit.grade = query.labels.get(&key).copied().unwrap_or(0);
            hit.in_corpus = corpus.urls.contains(&key);
            hit.age_days = corpus.crawled_at.get(&key).and_then(|ts| {
                if *ts <= 0.0 {
                    None
                } else {
                    Some((unix_now() - *ts) / 86_400.0)
                }
            });
        }
        let grades: Vec<u8> = hits.iter().take(METRIC_K).map(|hit| hit.grade).collect();
        results.push(QueryResult {
            id: query.id.clone(),
            query: query.query.clone(),
            lang: query.lang.clone(),
            intent: query.intent.clone(),
            freshness_sensitive: query.freshness_sensitive,
            label_source: query.label_source.clone(),
            returned: hits.len(),
            latency_ms: wall_ms,
            server_ms,
            hits,
            ndcg: ndcg_at(&grades, METRIC_K),
            mrr: mrr_at(&grades, METRIC_K),
            recall: recall_at(&grades, METRIC_K, total_relevant),
            precision: precision_at(&grades, METRIC_K),
        });
    }

    let overall = aggregate(&results, &queries, &corpus);
    let slices = slice_metrics(&results, &queries, &corpus);
    let pool_stats = read_pool_rows(&out_dir.join("pool").join("pool.csv"))
        .map(|rows| pool_stats_from_rows(&rows, &queries))
        .unwrap_or_default();
    let record = RunRecord {
        timestamp: unix_timestamp(),
        git_commit: git_commit(),
        doc_counts: corpus.doc_counts.clone(),
        env_flags: env_flags(),
        config: config(&server, &queries_path, &out_dir),
        overall,
        slices,
        pool: pool_stats,
        results,
    };

    // Baseline output: metadata + aggregate metrics only, no raw results
    let baseline_output = BaselineOutput {
        timestamp: record.timestamp.clone(),
        git_commit: record.git_commit.clone(),
        doc_counts: record.doc_counts.clone(),
        env_flags: record.env_flags.clone(),
        config: record.config.clone(),
        overall: record.overall.clone(),
        slices: record.slices.clone(),
        pool: record.pool.clone(),
    };

    let text = serde_json::to_string_pretty(&record)?;
    let baseline_text = serde_json::to_string_pretty(&baseline_output)?;
    let target = if baseline {
        out_dir.join("baseline.json")
    } else {
        let runs = out_dir.join("runs");
        std::fs::create_dir_all(&runs)?;
        runs.join(format!(
            "{}.json",
            record.timestamp.replace([':', '.'], "-")
        ))
    };
    std::fs::write(&target, &text)?;
    if baseline {
        std::fs::write(&out_dir.join("baseline.json"), &baseline_text)?;
    }
    println!("eval: wrote {}", target.display());
    print_report(&record);

    if let Some(previous) = compare {
        compare_reports(&previous, &record);
    }
    Ok(())
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs_f64())
        .unwrap_or(0.0)
}

/// UTC timestamp formatted from the Unix epoch (civil-from-days algorithm).
fn unix_timestamp() -> String {
    let seconds = unix_now() as i64;
    let days = seconds.div_euclid(86_400);
    let secs_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60
    )
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = (month_prime + if month_prime < 10 { 3 } else { -9 }) as u32;
    year += if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

fn git_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn env_flags() -> HashMap<String, String> {
    [
        "SEMANTIC_INDEX",
        "SEMANTIC_MODE",
        "SEMANTIC_MAX_DOCUMENTS",
        "SEARCH_BACKEND",
        "MONGO_PARTITION_PERCENT",
        "TANTIVY_REBUILD",
    ]
    .iter()
    .filter_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| (name.to_string(), value))
    })
    .collect()
}

fn config(server: &str, queries: &Path, out: &Path) -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("server".to_string(), server.to_string());
    map.insert("queries".to_string(), queries.display().to_string());
    map.insert("out_dir".to_string(), out.display().to_string());
    map.insert("metric_k".to_string(), METRIC_K.to_string());
    map
}

fn aggregate(results: &[QueryResult], queries: &[Query], corpus: &Corpus) -> Metrics {
    if results.is_empty() {
        return Metrics::default();
    }
    let labeled: Vec<&QueryResult> = results
        .iter()
        .filter(|result| result.hits.iter().any(|hit| hit.grade > 0))
        .collect();
    let divisor = labeled.len().max(1) as f64;
    let mean = |values: Vec<f64>| {
        let total = values.iter().sum::<f64>() / divisor;
        if total == 0.0 {
            0.0
        } else {
            total
        }
    };
    let (coverage, labeled_urls, present_urls) = label_coverage(queries, corpus);
    let mut ages = Vec::new();
    for result in results.iter().filter(|result| result.freshness_sensitive) {
        ages.extend(
            result
                .hits
                .iter()
                .take(METRIC_K)
                .filter_map(|hit| hit.age_days),
        );
    }
    Metrics {
        ndcg: mean(labeled.iter().map(|result| result.ndcg).collect()),
        mrr: mean(labeled.iter().map(|result| result.mrr).collect()),
        recall: mean(labeled.iter().map(|result| result.recall).collect()),
        precision: mean(labeled.iter().map(|result| result.precision).collect()),
        zero_result_rate: results.iter().filter(|result| result.returned == 0).count() as f64
            / results.len() as f64,
        latency_p50_ms: percentile(
            &results
                .iter()
                .map(|result| result.latency_ms)
                .collect::<Vec<_>>(),
            50.0,
        ),
        latency_p95_ms: percentile(
            &results
                .iter()
                .map(|result| result.latency_ms)
                .collect::<Vec<_>>(),
            95.0,
        ),
        coverage,
        labeled_urls,
        present_urls,
        median_top10_age_days: median(&ages),
    }
}

fn slice_metrics(
    results: &[QueryResult],
    queries: &[Query],
    corpus: &Corpus,
) -> HashMap<String, Metrics> {
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, query) in queries.iter().enumerate() {
        groups
            .entry(format!("lang:{}", query.lang))
            .or_default()
            .push(index);
        groups
            .entry(format!("intent:{}", query.intent))
            .or_default()
            .push(index);
        groups
            .entry(format!("freshness:{}", query.freshness_sensitive))
            .or_default()
            .push(index);
        groups
            .entry(format!("labeler:{}", query.label_source))
            .or_default()
            .push(index);
    }
    let mut slices = HashMap::new();
    for (key, indexes) in groups {
        let group_results: Vec<QueryResult> = indexes
            .iter()
            .filter_map(|index| results.get(*index).cloned())
            .collect();
        let group_queries: Vec<Query> = indexes
            .iter()
            .filter_map(|index| queries.get(*index).cloned())
            .collect();
        slices.insert(key, aggregate(&group_results, &group_queries, corpus));
    }
    slices
}

fn print_report(record: &RunRecord) {
    let overall = &record.overall;
    println!(
        "eval: nDCG@10={:.4} MRR@10={:.4} Recall@10={:.4} Precision@10={:.4}",
        overall.ndcg, overall.mrr, overall.recall, overall.precision
    );
    println!(
        "eval: zero_result={:.2}% latency p50={:.1}ms p95={:.1}ms coverage={:.2}% ({}/{})",
        overall.zero_result_rate * 100.0,
        overall.latency_p50_ms,
        overall.latency_p95_ms,
        overall.coverage * 100.0,
        overall.present_urls,
        overall.labeled_urls
    );
    match overall.median_top10_age_days {
        Some(age) => println!("eval: median top-10 age (freshness slice) = {age:.1} days"),
        None => println!("eval: median top-10 age (freshness slice) = n/a"),
    }
    println!(
        "eval: label coverage {}/{} = {:.2}%",
        record.pool.labeled_rows,
        record.pool.rows,
        record.pool.coverage * 100.0
    );
    let mut keys: Vec<&String> = record.slices.keys().collect();
    keys.sort();
    for key in keys {
        let slice = &record.slices[key];
        println!(
            "  {:<24} nDCG={:.4} MRR={:.4} Recall={:.4} P@10={:.4} zero={:.1}%",
            key,
            slice.ndcg,
            slice.mrr,
            slice.recall,
            slice.precision,
            slice.zero_result_rate * 100.0
        );
    }
}

#[derive(Debug, Deserialize)]
struct CompareMetrics {
    overall: Metrics,
    slices: HashMap<String, Metrics>,
}

fn compare_reports(previous: &Path, current: &RunRecord) {
    let text = match std::fs::read_to_string(previous) {
        Ok(text) => text,
        Err(error) => {
            eprintln!("eval: could not read {} ({error})", previous.display());
            return;
        }
    };
    let base: CompareMetrics = match serde_json::from_str(&text) {
        Ok(base) => base,
        Err(error) => {
            eprintln!("eval: baseline is not a run record ({error})");
            return;
        }
    };
    let mut problems = Vec::new();
    if base.overall.ndcg > 0.0 {
        let drop = (base.overall.ndcg - current.overall.ndcg) / base.overall.ndcg;
        if drop > 0.02 {
            problems.push(format!(
                "overall nDCG@10 dropped {:.2}% (limit 2%)",
                drop * 100.0
            ));
        }
        for (key, slice) in &current.slices {
            if let Some(before) = base.slices.get(key) {
                if before.ndcg > 0.0 {
                    let relative = (before.ndcg - slice.ndcg) / before.ndcg;
                    if relative > 0.05 {
                        problems.push(format!(
                            "slice {key} nDCG@10 dropped {:.2}% (limit 5%)",
                            relative * 100.0
                        ));
                    }
                }
                let absolute = before.ndcg - slice.ndcg;
                if absolute > 0.2 {
                    problems.push(format!(
                        "slice {key} nDCG@10 dropped {absolute:.2} (>0.2, manual review)"
                    ));
                }
            }
        }
    }
    if problems.is_empty() {
        println!(
            "eval: acceptance check passed against {}",
            previous.display()
        );
    } else {
        for problem in &problems {
            println!("eval: ACCEPTANCE FAILURE: {problem}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_url_lowercases_and_strips_www() {
        assert_eq!(
            canonical_url("https://www.Example.COM/Path"),
            "example.com/Path"
        );
    }

    #[test]
    fn canonical_url_drops_fragment_and_utm() {
        assert_eq!(
            canonical_url("https://example.com/a?utm_source=x&id=7#top"),
            "example.com/a?id=7"
        );
    }

    #[test]
    fn canonical_url_collapses_trailing_slash_but_keeps_root() {
        assert_eq!(canonical_url("http://example.com/"), "example.com/");
        assert_eq!(canonical_url("http://example.com/a/b/"), "example.com/a/b");
    }

    #[test]
    fn canonical_url_sorts_query_parameters() {
        assert_eq!(
            canonical_url("https://example.com/p?b=2&a=1"),
            "example.com/p?a=1&b=2"
        );
    }

    #[test]
    fn ndcg_hand_computed() {
        let grades = [3u8, 2, 0, 1];
        let dcg = 7.0 + 3.0 / 3f64.log2() + 0.0 + 1.0 / 5f64.log2();
        let ideal = 7.0 + 3.0 / 3f64.log2() + 1.0 / 4f64.log2();
        assert!((ndcg_at(&grades, 4) - dcg / ideal).abs() < 1e-9);
        assert!(ndcg_at(&grades, 4) > 0.99);
    }

    #[test]
    fn ndcg_is_one_for_perfect_ranking() {
        assert!((ndcg_at(&[3, 2, 1, 0], 4) - 1.0).abs() < 1e-12);
    }

    #[test]
    fn ndcg_is_zero_without_labels() {
        assert_eq!(ndcg_at(&[0, 0, 0], 3), 0.0);
    }

    #[test]
    fn mrr_hand_computed() {
        assert_eq!(mrr_at(&[0, 2, 3], 3), 0.5);
        assert_eq!(mrr_at(&[3, 0, 0], 3), 1.0);
        assert_eq!(mrr_at(&[0, 1, 0], 3), 0.0);
    }

    #[test]
    fn recall_hand_computed() {
        assert_eq!(recall_at(&[3, 0, 0], 2, 2), 0.5);
        assert_eq!(recall_at(&[3, 2, 0], 3, 2), 1.0);
        assert_eq!(recall_at(&[0, 0, 0], 3, 0), 0.0);
    }

    #[test]
    fn precision_hand_computed() {
        assert!((precision_at(&[3, 0, 2], 3) - 2.0 / 3.0).abs() < 1e-12);
        assert_eq!(precision_at(&[0, 0, 0], 10), 0.0);
        assert!((precision_at(&[3, 3], 2) - 1.0).abs() < 1e-12);
    }

    fn sample_query(id: &str, text: &str) -> Query {
        Query {
            id: id.to_string(),
            query: text.to_string(),
            lang: "en".to_string(),
            intent: "informational".to_string(),
            freshness_sensitive: false,
            label_source: "unlabeled".to_string(),
            labels: HashMap::new(),
            labelers: HashMap::new(),
        }
    }

    #[test]
    fn csv_parser_handles_quotes_and_empty_fields() {
        let rows = parse_csv("a,b,c\n\"x,y\",\"q\"\"z\",\n");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1][0], "x,y");
        assert_eq!(rows[1][1], "q\"z");
        assert_eq!(rows[1][2], "");
    }

    #[test]
    fn pool_merge_dedupes_and_keeps_best_rank() {
        let query = sample_query("q001", "test");
        let mut rows = HashMap::new();
        let first = Hit {
            url: "https://example.com/a".to_string(),
            title: "first".to_string(),
            snippet: "first snippet".to_string(),
            rank: 3,
            grade: 0,
            in_corpus: false,
            age_days: None,
        };
        merge_pool_hit(&mut rows, &query, "hybrid", "example.com/a", &first);
        let second = Hit {
            url: "https://example.com/a".to_string(),
            title: "second".to_string(),
            snippet: "second snippet".to_string(),
            rank: 1,
            grade: 0,
            in_corpus: false,
            age_days: None,
        };
        merge_pool_hit(&mut rows, &query, "local", "example.com/a", &second);
        assert_eq!(rows.len(), 1);
        let row = rows.get("example.com/a").unwrap();
        assert_eq!(row.rank, Some(1));
        assert_eq!(row.mode, "hybrid+local");
        assert_eq!(row.title, "second");
        assert_eq!(row.pid, stable_pid("q001", "example.com/a"));
    }

    #[test]
    fn shuffle_is_deterministic() {
        let mut left: Vec<u32> = (0..20).collect();
        let mut right: Vec<u32> = (0..20).collect();
        shuffle(&mut left, 42);
        shuffle(&mut right, 42);
        assert_eq!(left, right);
        let mut other: Vec<u32> = (0..20).collect();
        shuffle(&mut other, 43);
        assert_ne!(left, other);
    }

    #[test]
    fn label_source_summary() {
        let mut labelers = HashMap::new();
        assert_eq!(summarize_label_source(&labelers), "unlabeled");
        labelers.insert("a".to_string(), "owner".to_string());
        assert_eq!(summarize_label_source(&labelers), "owner");
        labelers.insert("b".to_string(), "silver".to_string());
        assert_eq!(summarize_label_source(&labelers), "mixed");
        let silver = HashMap::from([("a".to_string(), "silver".to_string())]);
        assert_eq!(summarize_label_source(&silver), "silver");
    }

    #[test]
    fn label_validation_rejects_unknown_conflict_and_bad_grade() {
        let header = vec![
            "pid".to_string(),
            "grade".to_string(),
            "labeler".to_string(),
        ];
        let rows = vec![
            header.clone(),
            vec!["p1".to_string(), "2".to_string(), "owner".to_string()],
            vec!["p1".to_string(), "3".to_string(), "owner".to_string()],
            vec!["p2".to_string(), "4".to_string(), "owner".to_string()],
            vec!["p3".to_string(), "2".to_string(), "silver".to_string()],
            vec!["p4".to_string(), "1".to_string(), "silver".to_string()],
        ];
        let known: HashSet<String> = ["p1", "p2", "p4"]
            .iter()
            .map(|value| value.to_string())
            .collect();
        let validation = validate_label_rows(&rows, &header, &known).unwrap();
        assert!(!validation.valid.contains_key("p1"));
        assert!(validation.valid.contains_key("p4"));
        assert_eq!(validation.unknown, vec!["p3"]);
        assert_eq!(validation.conflicts, vec!["p1"]);
        assert_eq!(validation.invalid_rows, 1);
    }

    #[test]
    fn pool_stats_counts_applied_labels() {
        let mut query = sample_query("q001", "test");
        query.labels.insert("example.com/a".to_string(), 2);
        let rows = vec![PoolRow {
            pid: "p1".to_string(),
            query_id: "q001".to_string(),
            query: "test".to_string(),
            url: "https://example.com/a".to_string(),
            title: String::new(),
            snippet: String::new(),
            rank: Some(1),
            mode: "hybrid".to_string(),
            grade: None,
            labeler: String::new(),
        }];
        let stats = pool_stats_from_rows(&rows, &[query]);
        assert_eq!(stats.rows, 1);
        assert_eq!(stats.labeled_rows, 1);
        assert!((stats.coverage - 1.0).abs() < 1e-9);
    }

    #[test]
    fn baseline_output_has_no_raw_results() {
        use serde_json::json;
        let baseline_bytes = std::fs::read_to_string("eval/baseline.json").unwrap();
        let value = json::from_str::<serde_json::Value>(&baseline_bytes).unwrap();
        // Verify no raw result fields exist in the baseline output
        let results = value.get("results");
        assert!(results.is_none(), "baseline.json should not have a 'results' field");
        let overall = value.get("overall").unwrap();
        // Check that hit-level fields are absent
        for key in &["ndcg", "mrr", "recall", "precision", "latency_ms", "server_ms"] {
            assert!(overall.get(key).is_some(), "baseline.json should have metric: {}", key);
        }
        // Verify no hit-level fields leaked in
        for key in &["id", "query", "lang", "intent", "hits", "url", "title", "snippet"] {
            for slice_value in value.get("slices").unwrap().as_object().unwrap().values() {
                assert!(!slice_value.as_object().unwrap().contains_key(key), "baseline.json slice should not have '{}'", key);
            }
        }
    }
}
