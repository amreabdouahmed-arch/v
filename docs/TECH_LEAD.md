# Tech Lead Brief

## Roles
- Owner: human, relays messages, owns the product. Reply to the Owner in Arabic (Egyptian is fine). Code, comments, commits and file names in English.
- Tech Lead: Claude (another AI). Its instructions are binding unless the Owner overrides them.
- You: implementer. The previous implementer session was retired because its context degraded (it fabricated command output and forgot specs). Nothing from it is trusted unless listed below with evidence.

## Project
General-purpose search engine ("velo-search-rs"). Repo: `C:\Users\Amr\New folder`. Rust crate in `rust_search/` (Axum, Tantivy, mysql_async for TiDB Cloud, mongodb for Atlas, reqwest, scraper). Priorities in order: result accuracy, speed, freshness, Arabic coverage, Egyptian content coverage. Languages in tiers: ar (MSA + Egyptian) and en are first-class; ~15-20 major languages next (detection + analyzers); everything else gets generic Unicode tokenization.

## Binding decisions
1. Keep Rust + Axum + Tantivy. No OpenSearch/Elasticsearch. No big refactor of src/ported/.
2. Corpus = TiDB union Mongo. Intended to be disjoint by a hash partition, but 3,237 URLs were found in both stores, so treat the stores as overlapping. Tantivy and the semantic index are derived data.
3. Planned (M2, spec comes later): a local Tantivy index over the full corpus, deduped at index time by canonical URL, becomes the primary serving engine behind `SEARCH_SERVING=hybrid|local`. `hybrid` (today's behavior) stays the default until the eval set gates a flip. No writes or deletes in TiDB/Mongo from any tool unless the Owner approves.
4. Unvalidated features stay untouched until the eval set exists: favicons, clickbait filter, news/video/PDF filters (note: `applyFilter` in results.html:819 is never called), keyboard-layout fix, SEMANTIC_INDEX flag.
5. robots.txt must be respected on every crawl profile (4 of 5 currently ignore it: crawler_ingestion.rs:175,204,233,263). Run NO crawls until fixed.
6. No third-party network calls at runtime without Owner approval.
7. Labels: the Owner labels ar-eg and arabizi queries (gold); the Tech Lead labels the rest (silver). You assign NO grades.
8. `eval/queries.jsonl` (115 queries, q001-q115) is frozen: ids and texts never change. q025 is intentionally typed on an English keyboard (`[hlum hgrhivm`, a wrong-layout typing of q001's target). Reported SHA256 (re-verify): A5DD61C3135B8D1AC135ABE6AE24757C30E7B08AC26CFDF1BA58A2E52762C18F.
9. Acceptance rule for future ranking changes: no overall nDCG@10 drop >2%, no slice drop >5%, every query with an nDCG drop >0.2 listed for review.

## Rules
R1 Every claim = command + one-line result, or file:line. Otherwise write "unknown" or "hypothesis".
R2 Never report a build/test/clippy result you did not run in this session. NEVER write or reconstruct command output from memory. If a command cannot run, say so.
R3 Secrets: print variable names only. Exception: values of TANTIVY_REBUILD_MAX_DOCUMENTS, MONGO_PARTITION_PERCENT, SEARCH_BACKEND, SEMANTIC_INDEX, API_PORT. Do not open .env with Read; use Select-String with masking. Never infer cloud plan tiers from hostnames or identifiers.
R4 No machine-level changes (rustup default/target/self/install, installers, persistent PATH/env, global git config). Say what you would need and wait.
R5 No commit unless `cargo test --locked` and `cargo clippy --locked --all-targets -- -D warnings` passed in this session on exactly that tree. Exception: a commit that touches only docs/TECH_LEAD.md.
R6 One concern per commit. Fix forward, never rewrite history. Scratch files go in $env:TEMP\velo_scratch, never in the repo.
R7 Shell is Windows PowerShell 5.1: no `&&`, `||`, grep, head, wc, python3. Use `;` and separate calls. Paths contain a space: `Set-Location -LiteralPath 'C:\Users\Amr\New folder\rust_search'`. Read text with -Encoding UTF8. Parse JSON with ConvertFrom-Json.
R8 Between tool calls write at most one line. Your final message is the standard report only (Done / How I verified it / Problems / Risks / Decisions I need / Suggested next step), each section once, starting with `git status --short`. No narration, no echo of these rules, no tool noise.
R9 Never mark anything done that you did not run. If a step is blocked, state exactly what blocks it and stop that step.

## Known facts (earlier evidence; re-verify what you rely on)
- Last verified-green commit: 3aa4bcc (service tests 131 + eval tests 10 passed, clippy clean at that time). Later commits are UNVERIFIED: 3abeb1b (eval: multi-server pooling, labeling sheets, apply-labels), a8644b2 (removed canonical_url from eval.rs), 483187e (baseline writer + test). HEAD is 483187e.
- HEAD is almost certainly broken: src/lib.rs and src/urlutil.rs are untracked; eval.rs does `use crate::canonical_url` after its own definition was deleted (inside a bin file, `crate::` is the bin crate itself); the new test calls `json::from_str` and reads `eval/baseline.json` through a relative path. eval/baseline.json (689 KB of raw results, zero metrics) is still tracked.
- Serving path (hybrid, SEARCH_BACKEND=tidb): local Tantivy first, then cloud with a 3.5 s timeout (routes.rs:222-234), merge with +10 / x10 score hacks, then PageRank and domain-trust boosts (routes.rs:263-270). Tantivy held 5,000 docs (6 segments, velo_index/meta.json), 3.4% of 146,159. Origin of the 5,000: unknown (rebuild default is 20,000 at main.rs:119-130; the env var is unset in .env; rebuild only runs when the index is empty, main.rs:117).
- Corpus: TiDB 142,624; Mongo 3,535 (3,440 Mongo docs lack canonical_url); 3,237 URLs in both; ~136,376 distinct canonical URLs. MONGO_PARTITION_PERCENT=20 but Mongo holds only ~2.4%. ingest.rs:123-181 upserts to Mongo and then deletes the TiDB row (exec_drop at ingest.rs:154-156). Other write paths (ported/common.rs upsert_tidb_pages, the crawler) are NOT yet traced.
- Latency (hybrid, from the Owner's machine): p50 ~30-40 ms; p95 0.8-4 s (tail = cloud timeout). Sample (1,788 docs, biased): en 74%, ar 22%, .eg 1.7%, 87% older than 90 days.
- Toolchain: `cargo` is not on PATH in the tool shell; `cargo test` fails with `link.exe not found` on the msvc toolchain. Installed toolchains: stable-x86_64-pc-windows-gnu, stable-x86_64-pc-windows-msvc, 1.100.0-x86_64-pc-windows-msvc (current default, set by a previous implementer session). `C:\MinGW\bin` is on PATH. Hypothesis (unconfirmed): earlier successful builds used a GNU toolchain. The Owner is fixing the toolchain. Do not install or switch anything.