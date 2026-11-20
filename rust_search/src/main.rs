mod admin;
mod error;
mod ingest;
mod mongo;
mod pages;
pub mod ported;
mod response_cache;
mod routes;
mod search;
mod state;
mod textops;
mod tidb;

use std::{
    env,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use anyhow::{Context, Result};
use axum::{
    middleware,
    routing::{get, post},
    Router,
};

use crate::admin::{
    add_crawl_seeds, admin_popular_searches, import_legacy, import_universities,
    index_document_route, rebuild_index_route, run_crawler,
};
use crate::mongo::create_mongo_collection;
use crate::pages::{dashboard_page, home, results_page};
use crate::routes::{
    add_cors_headers, admin_dashboard, advanced_search_route, api_root, api_stats_route,
    api_suggestions_route, health, public_search_route, semantic_search_route,
    stats_route, suggestions_route, upload_documents,
};
use crate::search::internal_search;
use crate::state::{normalize_search_backend, AppState, SearchCache};
use crate::tidb::create_tidb_pool;

#[tokio::main]
async fn main() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let arguments: Vec<String> = env::args().skip(1).collect();
    if !arguments.is_empty() {
        let command = ported::parse_command(&arguments)?;
        let result = ported::dispatch_command(command).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    let configured_backend = env::var("SEARCH_BACKEND").unwrap_or_else(|_| "tidb".to_owned());
    let search_backend = normalize_search_backend(&configured_backend)?.to_owned();
    if configured_backend.trim().eq_ignore_ascii_case("sqlite") {
        eprintln!("SEARCH_BACKEND=sqlite is retired; selecting the TiDB cloud backend");
    }
    let (tidb_pool, tidb_fulltext_enabled) = if search_backend == "tidb" {
        let (pool, fulltext_enabled) = create_tidb_pool().await?;
        (Some(pool), fulltext_enabled)
    } else {
        (None, false)
    };
    let (mongo_collection, mongo_queue_collection, mongo_text_index) =
        match create_mongo_collection().await {
            Some((collection, queue, has_text_index)) => {
                (Some(collection), Some(queue), has_text_index)
            }
            None if search_backend == "mongodb" => {
                return Err(anyhow::anyhow!(
                    "SEARCH_BACKEND=mongodb requires a valid MONGO_URI and database"
                ));
            }
            None => (None, None, false),
        };
    let mongo_partition_percent = if search_backend == "mongodb" {
        100
    } else {
        env::var("MONGO_PARTITION_PERCENT")
            .ok()
            .and_then(|value| value.parse::<u8>().ok())
            .filter(|value| *value <= 100)
            .unwrap_or(20)
    };
    let tantivy_engine = match ported::tantivy_engine::TantivyEngine::open_or_create(
        std::path::Path::new("velo_index"),
    ) {
        Ok(engine) => Some(Arc::new(engine)),
        Err(error) => {
            eprintln!(
                "Could not open the Tantivy index: {error}; resetting velo_index and retrying once"
            );
            // Cloud stores are the source of truth, so a wiped index is
            // re-healed by the empty-index rebuild below.
            let _ = std::fs::remove_dir_all("velo_index");
            match ported::tantivy_engine::TantivyEngine::open_or_create(std::path::Path::new(
                "velo_index",
            )) {
                Ok(engine) => Some(Arc::new(engine)),
                Err(retry_error) => {
                    eprintln!("Tantivy index stays disabled: {retry_error}");
                    None
                }
            }
        }
    };
    let rebuild_mode = env::var("TANTIVY_REBUILD")
        .unwrap_or_else(|_| "auto".to_owned())
        .trim()
        .to_owned();
    if rebuild_mode != "auto" {
        eprintln!("TANTIVY_REBUILD is not \"auto\"; skipping the empty-index rebuild");
    }
    if rebuild_mode.eq_ignore_ascii_case("auto") {
        if let Some(engine) = tantivy_engine.as_ref() {
            let max_documents = env::var("TANTIVY_REBUILD_MAX_DOCUMENTS")
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(20_000);
            match ported::rebuild::rebuild_tantivy_index_if_empty(
                engine,
                tidb_pool.as_ref(),
                mongo_collection.as_ref(),
                max_documents,
            )
            .await
            {
                Ok(0) => {}
                Ok(count) => {
                    eprintln!("Tantivy index rebuilt from the cloud stores: {count} pages")
                }
                Err(error) => eprintln!("Could not rebuild the Tantivy index: {error}"),
            }
        }
    }
    let vocabulary_pairs: Vec<(String, u64)> = match tantivy_engine.as_ref() {
        Some(engine) => engine.vocabulary().unwrap_or_else(|error| {
            eprintln!("Could not read the index vocabulary: {error}");
            Vec::new()
        }),
        None => Vec::new(),
    };
    let spell_checker = {
        let checker = ported::spellcheck::SpellChecker::new(vocabulary_pairs.clone());
        if checker.vocabulary_size() == 0 {
            eprintln!("Spell-check vocabulary is empty; corrections stay disabled");
            None
        } else {
            eprintln!(
                "Spell-check vocabulary ready: {} words",
                checker.vocabulary_size()
            );
            Some(Arc::new(checker))
        }
    };
    let suggester = Arc::new(Mutex::new(ported::search_suggester::SearchSuggester::new(8)));
    let autocomplete_pairs: Vec<(String, u64)> = vocabulary_pairs
        .iter()
        .filter(|(word, docs)| {
            let length = word.chars().count();
            *docs >= 2 && (3..=20).contains(&length) && !word.chars().all(|c| c.is_numeric())
        })
        .cloned()
        .collect();
    if autocomplete_pairs.is_empty() {
        eprintln!("Autocomplete content vocabulary is empty; completions stay disabled");
    } else {
        if let Ok(mut suggester) = suggester.lock() {
            suggester.index_word_frequency(&autocomplete_pairs, "content");
        }
        eprintln!(
            "Autocomplete content vocabulary ready: {} words",
            autocomplete_pairs.len()
        );
    }
    let synonyms = Arc::new(ported::synonyms::Synonyms::default());
    let pagerank_weight = env::var("PAGERANK_WEIGHT")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|weight| weight.is_finite() && *weight >= 0.0)
        .unwrap_or(5.0);
    let pagerank_scores =
        ported::authority::load_pagerank_scores(tidb_pool.as_ref(), mongo_collection.as_ref())
            .await;
    if !pagerank_scores.is_empty() {
        eprintln!("Loaded PageRank scores for {} pages", pagerank_scores.len());
    }
    let semantic_mode = env::var("SEMANTIC_MODE")
        .unwrap_or_else(|_| "auto".to_owned())
        .trim()
        .to_owned();
    if semantic_mode != "auto" && semantic_mode != "off" {
        eprintln!(
            "Unknown SEMANTIC_MODE \"{semantic_mode}\"; only \"off\" disables semantic search"
        );
    }
    // Dev/ops escape hatch: "off" skips the derived semantic index build at
    // startup so the API binds in seconds instead of minutes. The default
    // ("auto") keeps the previous behaviour unchanged.
    let semantic_index_mode = env::var("SEMANTIC_INDEX")
        .unwrap_or_else(|_| "auto".to_owned())
        .trim()
        .to_owned();
    if semantic_index_mode != "auto" && !semantic_index_mode.eq_ignore_ascii_case("off") {
        eprintln!(
            "Unknown SEMANTIC_INDEX \"{semantic_index_mode}\"; only \"off\" skips the build"
        );
    }
    let skip_semantic_build = semantic_mode.eq_ignore_ascii_case("off")
        || semantic_index_mode.eq_ignore_ascii_case("off");
    let semantic_engine = if skip_semantic_build {
        if semantic_mode.eq_ignore_ascii_case("off") {
            eprintln!("Semantic search disabled (SEMANTIC_MODE=off)");
        } else {
            eprintln!("Semantic index build skipped (SEMANTIC_INDEX=off)");
        }
        None
    } else {
        let max_documents = env::var("SEMANTIC_MAX_DOCUMENTS")
            .or_else(|_| env::var("SEMANTIC_MAX_PAGES"))
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(20_000);
        let started = std::time::Instant::now();
        let engine = ported::semantic::build_semantic_engine(
            tidb_pool.as_ref(),
            mongo_collection.as_ref(),
            max_documents,
        )
        .await;
        match engine.as_ref() {
            Some(engine) if engine.count_documents() > 0 => eprintln!(
                "Semantic index ready: {} pages in {:.1}s",
                engine.count_documents(),
                started.elapsed().as_secs_f64()
            ),
            _ => eprintln!(
                "Semantic index has no pages yet; runtime documents are included after the next restart"
            ),
        }
        engine.map(Arc::new)
    };
    let semantic_min_score = env::var("SEMANTIC_MIN_SCORE")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 1.0)
        .unwrap_or(0.05);
    let state = AppState {
        tidb_pool,
        mongo_collection,
        mongo_queue_collection,
        mongo_text_index: Arc::new(AtomicBool::new(mongo_text_index)),
        tidb_fulltext_enabled,
        mongo_partition_percent,
        search_backend,
        search_cache: Arc::new(Mutex::new(SearchCache::default())),
        response_cache: Arc::new(Mutex::new(response_cache::ResponseCache::new())),
        tantivy_engine,
        spell_checker,
        synonyms,
        suggester,
        recent_queries: Arc::new(Mutex::new(Vec::new())),
        pagerank_scores: Arc::new(pagerank_scores),
        pagerank_weight,
        semantic_engine,
        semantic_min_score,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/docs", get(api_root))
        .route("/api", get(api_root))
        .route("/api/search", get(advanced_search_route))
        .route("/api/suggestions", get(api_suggestions_route))
        .route("/api/stats", get(api_stats_route))
        .route("/api/index/rebuild", post(rebuild_index_route))
        .route("/", get(home))
        .route("/results", get(results_page))
        .route("/dashboard", get(dashboard_page))
        .route("/search", get(public_search_route).post(internal_search))
        .route("/search/semantic", get(semantic_search_route))
        .route("/suggestions", get(suggestions_route))
        .route("/stats", get(stats_route))
        .route("/upload", post(upload_documents))
        .route("/admin/dashboard", get(admin_dashboard))
        .route("/admin/popular-searches", get(admin_popular_searches))
        .route("/admin/crawl/seeds", post(add_crawl_seeds))
        .route("/admin/crawl/run", post(run_crawler))
        .route("/admin/index/document", post(index_document_route))
        .route("/admin/index/import-universities", post(import_universities))
        .route("/admin/index/import-legacy", post(import_legacy))
        .route("/admin/index/rebuild", post(rebuild_index_route))
        .route_layer(middleware::from_fn(add_cors_headers))
        .with_state(state);
    let address = env::var("VELO_API_BIND")
        .or_else(|_| env::var("VELO_RUST_SEARCH_BIND"))
        .unwrap_or_else(|_| {
            let host = env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_owned());
            let port = env::var("PORT")
                .or_else(|_| env::var("API_PORT"))
                .unwrap_or_else(|_| "8000".to_owned());
            format!("{host}:{port}")
        });
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .with_context(|| format!("Could not bind VELO Rust API to {address}"))?;
    eprintln!("VELO Rust API listening on {address}");
    axum::serve(listener, app).await?;
    Ok(())
}
