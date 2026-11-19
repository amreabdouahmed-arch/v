//! Rust implementations of cloud maintenance, utility, and search modules.
//! The CLI dispatcher runs these operations explicitly; imports are read-only sources.

pub mod authority;
mod cloud;
mod common;
pub mod crawler_ingestion;
mod export;
pub mod index_builder;
pub mod keyboard;
pub mod lemmatizer;
mod maintenance;
mod manage;
pub mod ml_search;
pub mod mongodb_helper;
pub mod nlp;
pub mod pagerank;
pub mod query_parser;
pub mod ranking_engine;
pub mod rebuild;
pub mod search_engine;
pub mod search_suggester;
pub mod semantic;
pub mod spellcheck;
pub mod synonyms;
pub mod text_utils;
pub mod tantivy_engine;
pub mod vector_search;
pub mod price_tracker;
pub mod egypt_services;

pub use cloud::{migrate_legacy_pages_to_velo, sync_legacy_pages_to_mongodb};
pub use common::{normalize_text, LegacyPage, Page};
pub use export::{export_alpaca, export_llama3};
pub use maintenance::{
    import_legacy_files, migrate_cloud_sources, repair_legacy_tidb_index, repartition_cloud_stores,
    RepartitionOptions,
};
pub use manage::{ensure_mongodb_text_index, manage_stats, seed_urls};

use anyhow::{bail, Context, Result};

/// Parse and validate command intent without connecting to or changing a database.
/// Cloud operations are called separately so embedding applications must opt in.
pub fn parse_command(args: &[String]) -> Result<Command> {
    let Some(name) = args.first().map(String::as_str) else {
        bail!("expected a maintenance command");
    };
    match name {
        "export-jsonl" | "export-alpaca" => {
            if args.len() > 4 {
                bail!("usage: {name} [legacy-sqlite-input] [output] [limit]");
            }
            let limit = args
                .get(3)
                .map(|value| value.parse::<usize>())
                .transpose()?;
            if limit == Some(0) {
                bail!("limit must be greater than zero");
            }
            Ok(Command::Export {
                format: if name == "export-jsonl" {
                    ExportFormat::Llama3
                } else {
                    ExportFormat::Alpaca
                },
                input: args
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| "backrub.db".to_owned()),
                output: args.get(2).cloned().unwrap_or_else(|| {
                    if name == "export-jsonl" {
                        "training_data.jsonl".to_owned()
                    } else {
                        "alpaca_data.json".to_owned()
                    }
                }),
                limit,
            })
        }
        "sync-to-cloud" | "merge-databases" | "migrate-to-tidb" => {
            let paths = if args.len() > 1 {
                args[1..].to_vec()
            } else {
                match name {
                    "merge-databases" => (1..=5).map(|i| format!("backrub_{i}.db")).collect(),
                    _ => vec!["backrub.db".to_owned()],
                }
            };
            Ok(match name {
                "sync-to-cloud" => Command::SyncLegacy(paths),
                "merge-databases" => Command::MergeLegacy(paths),
                _ => Command::MigrateLegacy(paths),
            })
        }
        "migrate-cloud-data" => Ok(Command::MigrateCloud(args[1..].to_vec())),
        "repair-tidb-legacy-index" => {
            if args.len() != 1 {
                bail!("{name} takes no positional arguments");
            }
            Ok(Command::RepairLegacy)
        }
        "manage-search" => parse_manage_command(&args[1..]),
        "index-jsonl" => {
            if args.len() < 2 {
                bail!("usage: index-jsonl <jsonl-file> [max-docs] [index-dir]");
            }
            let input = args[1].clone();
            let limit = args.get(2).map(|v| v.parse::<usize>()).transpose()?;
            let dir = args.get(3).cloned().unwrap_or_else(|| "velo_index".to_owned());
            Ok(Command::IndexJsonl { input, limit, dir })
        }
        "tantivy-search" => {
            if args.len() < 2 {
                bail!("usage: tantivy-search <query> [limit] [index-dir]");
            }
            let query = args[1].clone();
            let limit = args.get(2).map(|v| v.parse::<usize>()).transpose()?.unwrap_or(10);
            let dir = args.get(3).cloned().unwrap_or_else(|| "velo_index".to_owned());
            Ok(Command::TantivySearch { query, limit, dir })
        }
        "index-universities" => {
            let input = args.get(1).cloned().unwrap_or_else(|| "universities.json".to_owned());
            let limit = args.get(2).map(|v| v.parse::<usize>()).transpose()?;
            let dir = args.get(3).cloned().unwrap_or_else(|| "velo_index".to_owned());
            Ok(Command::IndexUniversities { input, limit, dir })
        }
        "repartition-cloud-stores" => {
            let apply = args.iter().any(|arg| arg == "--apply");
            let confirmed = args.iter().any(|arg| arg == "--confirm-drop-collections");
            if args
                .iter()
                .skip(1)
                .any(|arg| arg != "--apply" && arg != "--confirm-drop-collections")
            {
                bail!("unsupported repartition argument");
            }
            validate_repartition_confirmation(apply, confirmed)?;
            Ok(Command::Repartition { apply, confirmed })
        }
        _ => bail!("unknown maintenance command: {name}"),
    }
}

fn parse_manage_command(args: &[String]) -> Result<Command> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        bail!("usage: manage-search <seed|crawl|stats|pagerank|ensure-mongodb-text-index|import-universities|import-legacy>");
    };
    match subcommand {
        "stats" if args.len() == 1 => Ok(Command::ManageStats),
        "pagerank" if args.len() == 1 => Ok(Command::Pagerank),
        "seed" => {
            let mut urls = Vec::new();
            let mut file = None;
            let mut i = 1;
            while i < args.len() {
                if args[i] == "--file" {
                    i += 1;
                    file = Some(args.get(i).context("--file requires a path")?.clone());
                } else {
                    urls.push(args[i].clone());
                }
                i += 1;
            }
            Ok(Command::Seed { urls, file })
        }
        "crawl" => {
            let max_pages = if args.len() == 1 {
                20
            } else if args.len() == 3 && args[1] == "--max-pages" {
                args[2]
                    .parse::<usize>()
                    .context("--max-pages must be an integer")?
            } else {
                bail!("usage: manage-search crawl [--max-pages N]");
            };
            if !(1..=10_000).contains(&max_pages) {
                bail!("--max-pages must be between 1 and 10000");
            }
            Ok(Command::Crawl { max_pages })
        }
        "ensure-mongodb-text-index" if args.len() == 1 => Ok(Command::EnsureMongoIndex),
        "import-universities" => {
            if args.len() > 2 {
                bail!("usage: manage-search import-universities [json-file]");
            }
            Ok(Command::ImportUniversities(args.get(1).cloned()))
        }
        "import-legacy" => Ok(Command::ImportLegacy(args[1..].to_vec())),
        _ => bail!("unknown manage-search subcommand: {subcommand}"),
    }
}

/// Execute a parsed command. All database work is explicit at this call site.
pub async fn dispatch_command(command: Command) -> Result<serde_json::Value> {
    match command {
        Command::Export {
            format,
            input,
            output,
            limit,
        } => {
            ensure_export_output_is_distinct(&input, &output)?;
            let pages = export::read_legacy_pages(input, limit)?;
            let records = match format {
                ExportFormat::Llama3 => export_llama3(output, &pages)?,
                ExportFormat::Alpaca => export_alpaca(output, &pages)?,
            };
            Ok(serde_json::json!({"records": records}))
        }
        Command::SyncLegacy(paths) => {
            let mut synced = 0;
            let mut errors = 0;
            for path in paths {
                let (ok, failed) = sync_legacy_pages_to_mongodb(path).await?;
                synced += ok;
                errors += failed;
            }
            Ok(serde_json::json!({"synced": synced, "errors": errors}))
        }
        Command::MergeLegacy(paths) => {
            let (merged, skipped) = maintenance::merge_legacy_databases(&paths).await?;
            Ok(serde_json::json!({
                "merged": merged, "skipped": skipped, "target": "TiDB Cloud"
            }))
        }
        Command::MigrateLegacy(paths) => {
            let mut imported = 0;
            for path in paths {
                imported += migrate_legacy_pages_to_velo(path).await?;
            }
            Ok(serde_json::json!({"imported": imported, "target": "TiDB Cloud search_documents"}))
        }
        Command::MigrateCloud(paths) => {
            let paths = if paths.is_empty() {
                default_historical_sources()
            } else {
                paths
            };
            let imported = migrate_cloud_sources(&paths).await?;
            Ok(serde_json::json!({"processed_total": imported}))
        }
        Command::RepairLegacy => {
            let (scanned, imported) = repair_legacy_tidb_index().await?;
            Ok(serde_json::json!({"scanned": scanned, "imported": imported}))
        }
        Command::Repartition { apply, confirmed } => {
            let report = repartition_cloud_stores(RepartitionOptions {
                apply,
                confirmed_drop_collections: confirmed,
            })
            .await?;
            Ok(serde_json::json!({
                "mongo_source_collections": report.source_collections,
                "mongo_source_documents": report.mongo_source_documents,
                "mongo_urls_missing_from_tidb_before_stage": report.urls_missing_from_tidb,
                "mongo_partition_percent": report.partition_percent,
                "mongo_partition_candidates_by_url_hash": report.partition_candidates,
                "mode": report.mode,
            }))
        }
        Command::ManageStats => manage_stats().await,
        Command::Pagerank => authority::recompute_pagerank().await,
        Command::Seed { mut urls, file } => {
            if let Some(path) = file {
                let contents = std::fs::read_to_string(path)?;
                urls.extend(
                    contents
                        .lines()
                        .map(str::trim)
                        .filter(|line| !line.is_empty() && !line.starts_with('#'))
                        .map(str::to_owned),
                );
            }
            let queued = seed_urls(&urls, true, 0).await?;
            Ok(serde_json::json!({"queued": queued}))
        }
        Command::EnsureMongoIndex => Ok(serde_json::json!({
            "index": ensure_mongodb_text_index().await?
        })),
        Command::ImportUniversities(path) => {
            let path = path.unwrap_or_else(|| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("universities.json")
                    .to_string_lossy()
                    .into_owned()
            });
            let imported = manage::import_universities_json(path).await?;
            Ok(serde_json::json!({"imported": imported}))
        }
        Command::ImportLegacy(paths) => {
            let paths = if paths.is_empty() {
                default_legacy_paths()
            } else {
                paths
            };
            let imported = maintenance::import_legacy_files(&paths).await?;
            Ok(serde_json::json!({"imported": imported, "target": "TiDB Cloud"}))
        }
        Command::IndexJsonl { input, limit, dir } => {
            let engine = tantivy_engine::TantivyEngine::open_or_create(std::path::Path::new(&dir))?;
            let start = std::time::Instant::now();
            let count = engine.index_jsonl(std::path::Path::new(&input), limit)?;
            let elapsed = start.elapsed().as_secs_f64();
            Ok(serde_json::json!({
                "indexed_documents": count,
                "input": input,
                "index_dir": dir,
                "elapsed_seconds": elapsed,
                "docs_per_second": if elapsed > 0.0 { count as f64 / elapsed } else { 0.0 }
            }))
        }
        Command::TantivySearch { query, limit, dir } => {
            let engine = tantivy_engine::TantivyEngine::open_or_create(std::path::Path::new(&dir))?;
            let start = std::time::Instant::now();
            let res = engine.search(&query, limit, 0)?;
            let elapsed = start.elapsed().as_millis();
            Ok(serde_json::json!({
                "query": res.query,
                "total_hits": res.total_hits,
                "elapsed_ms": elapsed,
                "hits": res.hits
            }))
        }
        Command::IndexUniversities { input, limit, dir } => {
            let engine = tantivy_engine::TantivyEngine::open_or_create(std::path::Path::new(&dir))?;
            let start = std::time::Instant::now();
            let count = engine.index_universities_json(std::path::Path::new(&input), limit)?;
            let elapsed = start.elapsed().as_secs_f64();
            Ok(serde_json::json!({
                "indexed_universities": count,
                "input": input,
                "index_dir": dir,
                "elapsed_seconds": elapsed,
            }))
        }
        Command::Crawl { max_pages } => run_local_api_crawl(max_pages).await,
    }
}

async fn run_local_api_crawl(max_pages: usize) -> Result<serde_json::Value> {
    let token = common::required_env("ADMIN_TOKEN")?;
    let base = std::env::var("VELO_API_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_owned());
    let mut base_url = url::Url::parse(&base).context("VELO_API_URL must be an absolute URL")?;
    if base_url.scheme() != "http"
        || !matches!(base_url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
    {
        bail!("manage-search crawl only sends ADMIN_TOKEN to a local HTTP server");
    }
    base_url.set_path("/admin/crawl/run");
    base_url.set_query(None);
    base_url.set_fragment(None);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let response = client
        .post(base_url)
        .header("X-Admin-Token", token)
        .json(&serde_json::json!({
            "kind": "search_engine",
            "max_pages": max_pages
        }))
        .send()
        .await
        .context("calling the local VELO crawl endpoint; start the API first")?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        bail!("local crawl endpoint returned {status}: {body}");
    }
    serde_json::from_str(&body).context("local crawl endpoint returned invalid JSON")
}

fn ensure_export_output_is_distinct(input: &str, output: &str) -> Result<()> {
    let source = std::fs::canonicalize(input)?;
    let output_path = std::path::Path::new(output);
    let destination = if output_path.exists() {
        std::fs::canonicalize(output_path)?
    } else {
        let parent = output_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        std::fs::canonicalize(parent)?.join(
            output_path
                .file_name()
                .context("output path must name a file")?,
        )
    };
    let source = source.to_string_lossy();
    let destination = destination.to_string_lossy();
    if source.eq_ignore_ascii_case(destination.as_ref()) {
        bail!("output must not overwrite the read-only SQLite migration source");
    }
    Ok(())
}

fn default_legacy_paths() -> Vec<String> {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    [
        "backrub.db",
        "backrub_arabic.db",
        "global_crawler.db",
        "backrub_1.db",
        "backrub_2.db",
        "backrub_3.db",
        "backrub_4.db",
        "backrub_5.db",
    ]
    .iter()
    .map(|name| root.join(name))
    .filter(|path| path.is_file())
    .map(|path| path.to_string_lossy().into_owned())
    .collect()
}

fn default_historical_sources() -> Vec<String> {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    [
        "velo.db",
        "backrub.db",
        "backrub_arabic.db",
        "global_crawler.db",
        "backrub_1.db",
        "backrub_2.db",
        "backrub_3.db",
        "backrub_4.db",
        "backrub_5.db",
    ]
    .iter()
    .map(|name| root.join(name))
    .filter(|path| path.is_file())
    .map(|path| path.to_string_lossy().into_owned())
    .collect()
}

pub fn validate_repartition_confirmation(apply: bool, confirmed: bool) -> Result<()> {
    if apply != confirmed {
        bail!("collection deletion requires both --apply and --confirm-drop-collections");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Llama3,
    Alpaca,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Export {
        format: ExportFormat,
        input: String,
        output: String,
        limit: Option<usize>,
    },
    SyncLegacy(Vec<String>),
    MergeLegacy(Vec<String>),
    MigrateLegacy(Vec<String>),
    MigrateCloud(Vec<String>),
    RepairLegacy,
    Repartition {
        apply: bool,
        confirmed: bool,
    },
    ManageStats,
    Pagerank,
    Seed {
        urls: Vec<String>,
        file: Option<String>,
    },
    Crawl {
        max_pages: usize,
    },
    EnsureMongoIndex,
    ImportUniversities(Option<String>),
    ImportLegacy(Vec<String>),
    IndexJsonl {
        input: String,
        limit: Option<usize>,
        dir: String,
    },
    TantivySearch {
        query: String,
        limit: usize,
        dir: String,
    },
    IndexUniversities {
        input: String,
        limit: Option<usize>,
        dir: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repartition_requires_both_safety_flags() {
        assert!(validate_repartition_confirmation(false, false).is_ok());
        assert!(validate_repartition_confirmation(true, true).is_ok());
        assert!(validate_repartition_confirmation(true, false).is_err());
        assert!(validate_repartition_confirmation(false, true).is_err());
    }

    #[test]
    fn dispatcher_rejects_bad_limits_and_unconfirmed_deletion() {
        assert!(parse_command(&[
            "export-jsonl".into(),
            "x.db".into(),
            "x.jsonl".into(),
            "0".into()
        ])
        .is_err());
        assert!(parse_command(&["repartition-cloud-stores".into(), "--apply".into()]).is_err());
        assert!(parse_command(&[
            "repartition-cloud-stores".into(),
            "--confirm-drop-collections".into()
        ])
        .is_err());
        assert!(matches!(
            parse_command(&["repartition-cloud-stores".into()]).unwrap(),
            Command::Repartition {
                apply: false,
                confirmed: false
            }
        ));
    }

    #[test]
    fn manage_crawl_limit_is_bounded() {
        assert!(parse_command(&[
            "manage-search".into(),
            "crawl".into(),
            "--max-pages".into(),
            "10001".into()
        ])
        .is_err());
        assert!(matches!(
            parse_command(&["manage-search".into(), "crawl".into()]).unwrap(),
            Command::Crawl { max_pages: 20 }
        ));
    }

    #[test]
    fn export_cannot_overwrite_its_legacy_sqlite_input() {
        let source = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let source = source.to_string_lossy().into_owned();
        assert!(ensure_export_output_is_distinct(&source, &source).is_err());
    }
}
