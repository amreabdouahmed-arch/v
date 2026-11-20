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
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const RELEVANT_GRADE: u8 = 2;
const METRIC_K: usize = 10;
const POOL_DEPTH: usize = 30;

#[derive(Debug, Clone, Deserialize)]
struct Query {
    id: String,
    query: String,
    lang: String,
    intent: String,
    freshness_sensitive: bool,
    label_source: String,
    #[serde(default)]
    labels: HashMap<String, u8>,
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
    results: Vec<QueryResult>,
}

/// Canonical URL used for both labels and results.
///
/// Rules: lowercase host, drop `www.`, drop scheme, drop fragment, drop
/// `utm_*`/`fbclid`/`gclid` parameters, sort the remaining query parameters,
/// and collapse a trailing slash (except for the bare host).
pub fn canonical_url(raw: &str) -> String {
    let trimmed = raw.trim();
    let without_scheme = match trimmed.split_once("://") {
        Some((_, rest)) => rest,
        None => trimmed,
    };
    let (authority, mut path) = match without_scheme.split_once('/') {
        Some((authority, rest)) => (authority, format!("/{rest}")),
        None => (without_scheme, String::new()),
    };
    let host_port = match authority.rsplit_once('@') {
        Some((_, host)) => host,
        None => authority,
    };
    let host = host_port
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host).to_string();

    if let Some((before, _)) = path.split_once('#') {
        path = before.to_string();
    }
    let (base, query) = match path.split_once('?') {
        Some((base, query)) => (base.to_string(), Some(query.to_string())),
        None => (path, None),
    };
    let base = if base.is_empty() { "/".to_string() } else { base };
    let base = if base.len() > 1 && base.ends_with('/') {
        base[..base.len() - 1].to_string()
    } else {
        base
    };

    let mut kept: Vec<String> = match query {
        Some(query) => query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .filter(|pair| {
                let key = match pair.split_once('=') {
                    Some((key, _)) => key,
                    None => pair,
                }
                .to_ascii_lowercase();
                !key.starts_with("utm_") && key != "fbclid" && key != "gclid"
            })
            .map(str::to_string)
            .collect(),
        None => Vec::new(),
    };
    kept.sort();
    if kept.is_empty() {
        format!("{host}{base}")
    } else {
        format!("{host}{base}?{}", kept.join("&"))
    }
}

// ---------------------------------------------------------------------------
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
                        let pipeline = vec![doc! {"$project": {"canonical_url": 1, "url": 1, "crawled_at": 1}}];
                        match collection.aggregate(pipeline).await {
                            Ok(mut cursor) => {
                                let mut count = 0u64;
                                while let Ok(Some(document)) = cursor.try_next().await {
                                    count += 1;
                                    let url = document
                                        .get_str("canonical_url")
                                        .ok()
                                        .map(str::to_string)
                                        .or_else(|| document.get_str("url").ok().map(str::to_string));
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
    let url = format!(
        "{base}/search?q={}&limit={}",
        urlencode(query),
        limit
    );
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
    let mut queries_path = PathBuf::from("eval/queries.draft.jsonl");
    let mut out_dir = PathBuf::from("eval");
    let mut pool = false;
    let mut baseline = false;
    let mut coverage_only = false;
    let mut compare: Option<PathBuf> = None;

    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--server" => server = args.next().context("--server needs a value")?,
            "--queries" => {
                queries_path = PathBuf::from(args.next().context("--queries needs a value")?)
            }
            "--out-dir" => out_dir = PathBuf::from(args.next().context("--out-dir needs a value")?),
            "--pool" => pool = true,
            "--baseline" => baseline = true,
            "--coverage-only" => coverage_only = true,
            "--compare" => {
                compare = Some(PathBuf::from(args.next().context("--compare needs a value")?))
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    let queries = load_queries(&queries_path)?;
    if queries.is_empty() {
        bail!("no queries in {}", queries_path.display());
    }
    println!("eval: loaded {} queries", queries.len());

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

    let client = reqwest::Client::builder()
        .user_agent("velo-eval/1.0")
        .build()?;
    if pool {
        let dir = out_dir.join("pool");
        std::fs::create_dir_all(&dir)?;
        let mut rows = String::from("query_id,query,url,title,snippet,rank,grade,labeler\n");
        for query in &queries {
            match search(&client, &server, &query.query, POOL_DEPTH).await {
                Ok((hits, _)) => {
                    for hit in hits {
                        rows.push_str(&format!(
                            "{},{},{},{},{},{},{},{}\n",
                            csv_field(&query.id),
                            csv_field(&query.query),
                            csv_field(&hit.url),
                            csv_field(&hit.title),
                            csv_field(&hit.snippet),
                            hit.rank,
                            "",
                            ""
                        ));
                    }
                }
                Err(error) => eprintln!("eval: pool fetch failed for {} ({error})", query.id),
            }
        }
        let target = dir.join("pool.csv");
        std::fs::write(&target, rows)?;
        println!("eval: wrote {}", target.display());
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
            hit.age_days = corpus
                .crawled_at
                .get(&key)
                .and_then(|ts| if *ts <= 0.0 { None } else { Some((unix_now() - *ts) / 86_400.0) });
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
    let record = RunRecord {
        timestamp: unix_timestamp(),
        git_commit: git_commit(),
        doc_counts: corpus.doc_counts.clone(),
        env_flags: env_flags(),
        config: config(&server, &queries_path, &out_dir),
        overall,
        slices,
        results,
    };

    let text = serde_json::to_string_pretty(&record)?;
    let target = if baseline {
        out_dir.join("baseline.json")
    } else {
        let runs = out_dir.join("runs");
        std::fs::create_dir_all(&runs)?;
        runs.join(format!("{}.json", record.timestamp.replace([':', '.'], "-")))
    };
    std::fs::write(&target, &text)?;
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
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096)
        / 365;
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
    .filter_map(|name| std::env::var(name).ok().map(|value| (name.to_string(), value)))
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
        println!("eval: acceptance check passed against {}", previous.display());
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
}