---
title: VELO Search
emoji: 🔎
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
---

# VELO Search

VELO is a Rust search API and web interface backed by TiDB Cloud and MongoDB
Atlas. Search, crawling, and indexing use cloud storage; SQLite is supported
only as a read-only source for legacy data imports and exports.
Keyword search and crawling run on the Rust service. Semantic search runs
fully locally by default through a built-in hashing TF-IDF embedder, with no
external model, service, or API key; the semantic endpoint returns `503` only
when `SEMANTIC_MODE=off` or no cloud backend is configured.

## Architecture

VELO Search is built as a single Rust binary (`velo-search-rs`) with the following components:

### Core Modules (`rust_search/src/`)
- **`main.rs`** — Entry point, configuration loading, server setup, CLI dispatcher
- **`routes.rs`** — HTTP handlers for `/search`, `/api/*`, `/admin/*`, suggestions, stats
- **`search.rs`** — Federated search logic (TiDB + MongoDB + Tantivy + semantic), caching
- **`state.rs`** — Shared `AppState` with DB pools, indexes, caches, spell-checker, suggester
- **`ingest.rs`** — Document indexing pipeline (write to TiDB/MongoDB + Tantivy + semantic)
- **`tidb.rs`** — TiDB Cloud connection pool, schema management, full-text search
- **`mongo.rs`** — MongoDB Atlas connection, collections, text index management
- **`pages.rs`** — HTML page handlers (`/`, `/results`, `/dashboard`) with AI overviews
- **`textops.rs`** — Text normalization, URL canonicalization, snippet generation
- **`error.rs`** — Unified API error responses
- **`admin.rs`** — Admin-only routes (crawl, index, import, rebuild, dashboard)

### Ported Modules (`rust_search/src/ported/`)
Reusable Rust implementations for search, NLP, and maintenance:
- **`search_engine.rs`** — Cloud search orchestrator with query parsing & ranking
- **`query_parser.rs`** — Multi-language query parsing, stemming, stop-word removal
- **`ranking_engine.rs`** — PageRank, user signals, relevance scoring
- **`pagerank.rs`** — Link-graph PageRank calculator (used by `authority.rs`)
- **`authority.rs`** — Builds link graph from crawled metadata, computes PageRank, writes to cloud
- **`tantivy_engine.rs`** — Tantivy full-text index with Arabic-normalizing tokenizer
- **`semantic.rs`** — Local TF-IDF embeddings (word tokens + character trigrams)
- **`vector_search.rs`** — In-memory vector similarity search engine
- **`spellcheck.rs`** — Corpus-based spell correction (Damerau-Levenshtein ≤ 2)
- **`keyboard.rs`** — Arabic/Latin keyboard-layout recovery for wrong-layout queries
- **`synonyms.rs`** — Curated Arabic/English synonym expansion
- **`search_suggester.rs`** — Popularity-ranked autocomplete (query + content prefixes)
- **`lemmatizer.rs`** — Arabic/English lemmatization dictionaries
- **`index_builder.rs`** — Inverted index for fast keyword lookup
- **`rebuild.rs`** — Self-healing Tantivy index rebuild from cloud stores
- **`cloud.rs`** — MongoDB sync, legacy SQLite migration utilities
- **`maintenance.rs`** — Legacy data import/export, cloud migration, repartitioning
- **`manage.rs`** — CLI commands: stats, seed, crawl, import-universities, pagerank
- **`export.rs`** — Training data export (JSONL, Alpaca format)
- **`ml_search.rs`** — ML-enhanced search (placeholder for future models)
- **`nlp.rs`** — Text utilities, language detection
- **`mongodb_helper.rs`** — MongoDB aggregation helpers
- **`common.rs`** — Shared types: `Page`, `LegacyPage`, `normalize_text`, `canonicalize_url`
- **`text_utils.rs`** — Additional text processing helpers

### Data Flow

**At startup** (`rust_search/src/main.rs`):
1. Load `.env` from the working directory (`main.rs:42`).
2. Connect to TiDB (`main.rs:61-66`) and MongoDB (`main.rs:67-72`) and open the
   local Tantivy index.
3. Apply the rebuild policy `TANTIVY_REBUILD` (`main.rs:110-135`).
4. Load the PageRank score table into memory (`main.rs:185-190`).
5. Build the derived semantic index in memory unless `SEMANTIC_MODE=off` or
   `SEMANTIC_INDEX=off` (`main.rs:191-226`).
6. Bind the HTTP listener (`main.rs:277`).

**On ingest** (`rust_search/src/ingest.rs`):
1. Canonicalize the URL, hash it, and normalize the text (`ingest.rs:69-106`).
2. Deterministically route a `MONGO_PARTITION_PERCENT` slice of documents to
   MongoDB and delete the duplicate TiDB row (`ingest.rs:123-181`).
3. Upsert the remaining documents into TiDB `search_documents` (`ingest.rs:188-201`).
4. Add the document to the Tantivy index in memory (`ingest.rs:92-101`).

**On search** (`rust_search/src/routes.rs`):
1. The local Tantivy and semantic engines answer first (`routes.rs:250-280`).
2. Cloud stores are queried only when the local result set is sparse
   (`routes.rs:285-310`).
3. Results are merged, boosted by domain trust and PageRank, then returned
   (`routes.rs:312-340`).

### What lives where

| Store | Contents | Authority |
|---|---|---|
| TiDB `search_documents` | Canonical rows: url, title, description, content, `search_text`, language, status, `crawled_at`, `content_hash`, `source_metadata_json` (`tidb.rs:85-99`) | **Authoritative for its own documents** |
| MongoDB `Crawled_Pages` | Documents in the `MONGO_PARTITION_PERCENT` slice (default 20%), written *instead of* TiDB; also the search backend when `SEARCH_BACKEND=mongodb` (`ingest.rs:123-181`, `mongo.rs:45-60`) | **Authoritative for its own documents** |
| MongoDB `velo_crawl_queue` | Durable crawl queue (`mongo.rs:60`, `ingest.rs:328-360`) | Authoritative for queue state |
| `velo_index/` (Tantivy) | Derived full-text index: `id`, `title`, `url`, `body` (`tantivy_engine.rs:295-307`) | Derived, rebuildable |
| Semantic index | Derived TF-IDF vectors held in memory (`ported/semantic.rs`) | Derived, rebuildable |
| Process memory | Spell-check vocabulary, suggester maps, search cache, PageRank scores, recent queries (`state.rs:20-40`) | Derived, rebuilt at startup |

**Corpus model:** the corpus is the **disjoint union** TiDB ∪ MongoDB. The
partition is by design, not a replication: each document lives in exactly one
store (`ingest.rs:123-181`). A document is therefore authoritative in whichever
store holds it.

**Rebuild rule:** the Tantivy and semantic indexes are derived and MUST be
rebuildable from the union of both stores. The rebuild path reads both
(`main.rs:123-129`).

**Unknown (not confirmed in M0):**
- Whether any deployment writes the same URL to both stores (the duplicate
  TiDB row is deleted on a Mongo upsert, `ingest.rs:150-169`, so this should not
  happen — but it has not been measured across the live corpus).
- The tier limits and remaining headroom of the two cloud plans.
- Whether the MongoDB text index exists in every deployment; the local
  deployment logs `MongoDB text index is missing` at startup and falls back to
  the keyword path.

## Run locally

1. Install Rust and configure cloud credentials in a private `.env` file using
   `.env.example` as a reference. The binary loads `.env` from the working
   directory when present.
2. Build and start the API:

   ```powershell
   cargo build --release --manifest-path rust_search/Cargo.toml
   cargo run --release --manifest-path rust_search/Cargo.toml
   ```

   The API listens on `http://0.0.0.0:8000` by default, on all interfaces. Set
   `PORT`, `API_HOST`, or `VELO_API_BIND` to change the bind address. Keep it on
   a trusted network: `/admin/*` is served on this port too.
3. Open `/` for the home page, `/results` for search results, `/dashboard` for
   status, or `/docs` for the API overview.

### Prerequisites

- Rust 1.85+ (edition 2021). The toolchain is not pinned in the repo.
- A reachable TiDB (MySQL protocol) server, or MongoDB Atlas, depending on
  `SEARCH_BACKEND`.
- No Node.js and no frontend build step: the UI is three static HTML files at
  the repo root.

### Environment

Copy `.env.example` to `.env` and fill it in. The binary loads `.env` from the
working directory (`main.rs:42`). With `SEARCH_BACKEND=tidb` the required
variables are `TIDB_HOST`, `TIDB_USER`, `TIDB_PASSWORD` and `TIDB_DATABASE`.
Set `ADMIN_TOKEN` before using any `/admin/*` route. `.env` is git-ignored and
must never be committed.

### Build, test, run

```powershell
cargo test    --locked --manifest-path rust_search/Cargo.toml
cargo clippy  --locked --manifest-path rust_search/Cargo.toml --all-targets -- -D warnings
cargo run     --locked --manifest-path rust_search/Cargo.toml
```

### Example requests

```powershell
curl.exe "http://127.0.0.1:8000/api/stats"
curl.exe "http://127.0.0.1:8000/search?q=harvard&limit=5"
curl.exe "http://127.0.0.1:8000/search/semantic?q=harvard"
curl.exe "http://127.0.0.1:8000/api/suggestions?q=lwv&limit=8"
curl.exe -H "X-Admin-Token: $env:ADMIN_TOKEN" "http://127.0.0.1:8000/admin/dashboard"
```

### Fast startup for local development

The derived semantic index is built during startup and takes minutes on a full
corpus. Skip it while iterating:

```powershell
$env:SEMANTIC_INDEX='off'
cargo run --locked --manifest-path rust_search/Cargo.toml
```

Keyword search, autocomplete and ranking keep working; only semantic search is
unavailable. Use `SEMANTIC_MAX_DOCUMENTS` (alias `SEMANTIC_MAX_PAGES`) to cap
the build instead of skipping it.

Set `SEARCH_BACKEND=tidb` to use TiDB Cloud and optionally partition documents
to MongoDB Atlas with `MONGO_PARTITION_PERCENT`. Set `SEARCH_BACKEND=mongodb`
to use MongoDB alone. The server requires the matching cloud credentials. The
local Tantivy index (`velo_index/`) and the in-memory semantic index are derived
caches rebuilt from **both** cloud stores, which together form the corpus (see
"Corpus model" above).

Docker builds and runs the Rust binary directly:

```powershell
docker build -t velo-search .
docker run --rm -p 7860:7860 --env-file .env velo-search
```

## API

- `GET /search?q=...&limit=20&offset=0`
- `GET /api/search?q=...`
- `GET /search/semantic?q=...&limit=10&offset=0`
- `GET /suggestions?q=...`
- `GET /api/suggestions?q=...`
- `GET /stats` and `GET /api/stats`
- `POST /upload` (admin token required)
- `GET /admin/dashboard` (admin token required)
- `GET /admin/popular-searches` (admin token required)
- `POST /admin/crawl/seeds` (admin token required)
- `POST /admin/crawl/run` (`search_engine`, `advanced`, `arabic`, `global`,
  `universities`, or `all`; limit each run with `max_pages`; admin token
  required)
- `POST /admin/index/document` (admin token required)
- `POST /admin/index/import-legacy` (admin token required)
- `POST /admin/index/import-universities` (admin token required)
- `POST /admin/index/rebuild` (admin token required)

Deep pagination is capped: an `offset` above `1000` is rejected with `400`.

All `/admin/*` routes, reads such as `/admin/dashboard` and
`/admin/popular-searches` as well as writes, require a long random
`ADMIN_TOKEN` sent in the `X-Admin-Token` header. When `ADMIN_TOKEN` is unset
every admin request is denied; there is no open fallback. Keep these routes on
a trusted network.

## Rust maintenance commands

Run a maintenance command by adding it after `--`:

```powershell
cargo run --manifest-path rust_search/Cargo.toml -- manage-search stats
cargo run --manifest-path rust_search/Cargo.toml -- manage-search seed https://example.org
cargo run --manifest-path rust_search/Cargo.toml -- manage-search crawl --max-pages 20
cargo run --manifest-path rust_search/Cargo.toml -- manage-search import-universities
cargo run --manifest-path rust_search/Cargo.toml -- manage-search import-legacy backrub.db
cargo run --manifest-path rust_search/Cargo.toml -- manage-search pagerank
cargo run --manifest-path rust_search/Cargo.toml -- repair-tidb-legacy-index
```

`manage-search crawl` calls the local API, so start the server first and set
`ADMIN_TOKEN`; `VELO_API_URL` may select another local HTTP bind address.
Other supported commands include `export-jsonl`, `export-alpaca`,
`sync-to-cloud`, `merge-databases`, `migrate-to-tidb`, `migrate-cloud-data`,
`repartition-cloud-stores`, and `manage-search ensure-mongodb-text-index`.
SQLite inputs are opened read-only. Repartitioning is destructive and requires
both `--apply` and `--confirm-drop-collections`; take and verify independent
cloud backups before using those flags.

### Local semantic search

The semantic index is built from the cloud stores at server startup (up to
`SEMANTIC_MAX_DOCUMENTS` pages) and is available to `/search` too: strong
semantic matches are merged into normal results. Set `SEMANTIC_MODE=off` to
disable. Tune `SEMANTIC_MIN_SCORE` (default 0.05) to control how eager normal
search is about semantic-only matches. The Tantivy index is governed separately
by `TANTIVY_REBUILD_MAX_DOCUMENTS` and only rebuilds when empty, as described
in the next section. Local hashing tolerates typos and different word forms but
does not understand distant synonyms; swapping in a trained multilingual
embedding model later only requires a different `TextEmbedder`
implementation.

### Self-healing Tantivy index

The fast Tantivy index (`velo_index/`) is derived data: the cloud stores in
TiDB and MongoDB stay the source of truth. When the server starts and finds
the index empty, it rebuilds itself from the most recent cloud pages — up to
`TANTIVY_REBUILD_MAX_DOCUMENTS` (default 20000) — so a fresh deploy or a
wiped index directory recovers on its own. No external volume or service is
needed. Set `TANTIVY_REBUILD=off` to skip the check. A non-empty index is
never re-indexed, so documents cannot be duplicated. Freshly created or
rebuilt indexes index text with an Arabic-normalizing tokenizer: diacritics
and hamza variants are folded for matching, while stored text and snippets
keep their original spelling. An existing non-empty `velo_index/` keeps its
previous behavior until you delete the folder once and let the rebuild
repopulate it.

### Arabic spell-correction and synonyms

When a search comes back nearly empty (the reported result count is below 3)
the server retries once, for free and fully locally. First a spell checker
built from the corpus term dictionary tries to fix typos: Arabic diacritics and
hamza forms are normalized first, the definite article "ال" is ignored while
comparing, the interchangeable "ة"/"ه" spelling costs no edit, and
higher-frequency words win ties. Wrong keyboard layouts are recovered too: a
query typed on the Arabic layout while the Latin one is active ("lwv" for
"مصر") or the reverse is mapped back through the standard Arabic (101) layout,
and the conversion is kept only when every resulting word is in the corpus.
When the corrected query finds more results, `GET /search` carries
`corrected_query` in its response and the UI shows a Google-style "Did you
mean …?" hint. If no spelling fix helps, a small curated Arabic synonym map
silently expands the query (امتحان also matches اختبار, جامعة also matches
كلية/معهد). The dashboard reports the vocabulary size under
`databases.spellcheck`. The correction and autocomplete vocabulary is built at
server startup from the index terms, so documents ingested afterwards become
available there after the next restart.

### Google-style autocomplete

The suggestion box under the search field ranks like Google's autocomplete,
using only local in-memory signals. Every search increments a popularity
counter and suggestions are ordered by how often they were searched, not
alphabetically. Content-word completions come from the search index's own
term dictionary (typing "جام" completes to "جامعة"), ranked by corpus
frequency, and when the word being typed is misspelled the completions follow
the spell-corrected form instead. Recent searches are mixed in, and real page
titles round out the list. There is no per-user history (there are no
accounts) and no external trending feed; `/admin/popular-searches` reports the
current top queries.

### PageRank ranking

`manage-search pagerank` rebuilds the link graph from the outbound links the
crawler records in each page's source metadata, computes PageRank, and writes
normalized 0..=100 scores to TiDB (and MongoDB when configured). The public
search boosts results by `PAGERANK_WEIGHT * (pagerank / 100)` before sorting;
`PAGERANK_WEIGHT` defaults to `5.0` and `0` disables the boost. Scores load at
server startup and are refreshed only when the command runs again (restart the
server to pick up new scores). Pages crawled before this feature carry no
links, so re-crawl important seeds before expecting meaningful authority
signals.

## Tests

```powershell
cargo test --manifest-path rust_search/Cargo.toml
```

The tests use in-memory fakes and do not connect to or modify cloud databases.
