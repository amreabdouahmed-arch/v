# Velo Search evaluation harness

Offline measurement of result quality. Nothing here changes serving code, and the
harness only ever **reads** TiDB and MongoDB (`SELECT` / aggregation with a
projection).

## Quick start

```powershell
# 1. start the API with the fast dev flag
$env:SEMANTIC_INDEX='off'
cargo run --locked --manifest-path rust_search/Cargo.toml

# 2. label coverage of the current query set against the corpus
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --coverage-only

# 3. build the pool from one or more named servers (top-10 per query by default)
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --pool --top 10 --base-url hybrid=http://127.0.0.1:8000

# 4. split the pool into labeler chunks (owner + silver)
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --labeling-sheet

# 5. after labels are filled in, apply them back to the query set
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --apply-labels eval/pool/labels.csv

# 6. record a run
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval --

# 7. compare a change against the committed baseline
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --compare eval/baseline.json
```

`eval/runs/` and `eval/pool/` are git-ignored. Only `eval/baseline.json`,
`eval/queries.jsonl`, `eval/extra_urls.csv` and this file are committed.

## Files

| Path | Purpose |
|---|---|
| `queries.jsonl` | frozen v1 query set: 115 queries, one JSON object per line, **unlabelled** |
| `extra_urls.csv` | `query_id,url,grade` — known-good URLs supplied by the Owner |
| `pool/pool.csv` | generated pool: `pid,query_id,query,url,title,snippet,rank,mode,grade,labeler` |
| `pool/chunks/owner_NN.csv` | generated Owner sheets: `pid,query_id,query,url,title,snippet,grade` |
| `pool/chunks/silver_NN.csv` | generated silver sheets: same columns, Tech Lead labels them |
| `pool/labels.csv` | returned labels: `pid,grade,labeler` (labeler is `owner` or `silver`) |
| `runs/<timestamp>.json` | generated run record: git commit, doc counts, env flags, raw hits, metrics |
| `baseline.json` | committed reference run used by `--compare` |
| `coverage_gaps.jsonl` | generated: labelled URLs (grade ≥ 2) that are missing from the corpus |

## Query set composition

| Slice | Count | Notes |
|---|---|---|
| `lang:ar` (MSA) | 30 | navigational, informational, local, transactional, news |
| `lang:ar-eg` (Egyptian colloquial) | 35 | services, prices, universities, transport, government |
| `lang:arabizi` (Franco-Arabic) | 5 | Egyptian chat spelling, e.g. `3andy mo2ad` |
| `lang:en` | 30 | mixed intents, includes misspellings |
| `lang:other:*` | 15 | fr, es, de, ru, tr, fa, ur, zh, ja, hi (max 2 each) |
| **Total** | **115** | at least 20 marked `freshness_sensitive: true` |

`intent` is one of `navigational`, `informational`, `local`, `transactional`,
`typo`, `news`.

`q025` (`[hlum hgrhivm`) is `جامعة القاهرة` typed on an English keyboard
layout. It targets the same pages as `q001` (`جامعة القاهرة`), so both queries
share relevance judgments for that URL set.

Freshness-sensitive queries are labelled for **topical relevance** only. A page
does not become irrelevant because it is old; page date is reported separately
by the median top-10 age metric, not folded into the grade.

## Schema (one JSON object per line)

```json
{"id":"q001","query":"جامعة القاهرة","lang":"ar","intent":"navigational",
 "freshness_sensitive":false,"label_source":"mixed",
 "labels":{"example.com/page":2},"labelers":{"example.com/page":"owner"}}
```

`labels` maps a **canonical URL** to a grade:

| Grade | Meaning |
|---|---|
| 0 | irrelevant |
| 1 | marginal (same topic, does not answer) |
| 2 | relevant (answers the query) |
| 3 | highly relevant (authoritative, complete) |

`labelers` maps each labelled canonical URL to the person who graded it:
`owner` or `silver`. `label_source` is the per-query summary: `owner` when every
label is the Owner's, `silver` when every label is the Tech Lead's, `mixed` when
both appear, and `unlabeled` when the query has no grades. The harness never
assigns grades.

## Labelling workflow

The pool comes from the systems we run. `--pool` accepts any number of named
servers, for example:

```powershell
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --pool --top 10 `
  --base-url hybrid=http://127.0.0.1:8000 `
  --base-url local=http://127.0.0.1:8001
```

Results are merged and deduplicated by canonical URL. A URL found by more than
one system keeps its best rank and lists every mode in the `mode` column
(`hybrid+local`). Rows from `extra_urls.csv` are added with mode `extra`.

`--labeling-sheet` writes the pool into `eval/pool/chunks/`:

- `owner_NN.csv` holds `ar-eg` and `arabizi` queries (the Owner's gold set).
- `silver_NN.csv` holds every other query (the Tech Lead's silver set).
- Each chunk has at most 100 data rows and columns
  `pid,query_id,query,url,title,snippet,grade`.
- Rows are shuffled within each query with a fixed seed. Rank and mode are not
  included, so labelers do not see system order.

Fill only the `grade` column (0-3) and return the file as
`pid,grade,labeler`; `--apply-labels` validates the grades, rejects unknown
`pid`s and conflicting duplicates, canonicalizes URLs, and writes `labels` and
`labelers` into `queries.jsonl`.

## Canonical URL rule

Both labels and results are normalised with the same function
(`rust_search/src/urlutil.rs`, `canonical_url`, re-exported by the library crate
as `velo_search_rs::canonical_url`):

- lowercase host, strip a leading `www.`
- drop scheme, userinfo, port and fragment
- drop `utm_*`, `fbclid`, `gclid`
- sort the remaining query parameters
- collapse a trailing slash, except for the bare host (`example.com/`)

Unit tests cover each rule, plus idempotence, output shape and key equivalence.

### Two layers, deliberately different

The codebase holds **two** URL keys. They are not interchangeable and neither is
a defect:

| Layer | Function | Scheme / `www.` / trailing slash | Used for |
|---|---|---|---|
| Storage identity | `ported/common.rs` `canonicalize_url` | **kept** — only the fragment, tracking parameters, default ports and parameter order are normalised | `url_hash` primary key of `DOCUMENTS_TABLE`, and the Mongo `url` key |
| Judgement key | `urlutil.rs` `canonical_url` | **stripped** | `labels` / `labelers` keys in `queries.jsonl`, and result dedup in the harness |

`url_hash` is referenced by the Mongo-path delete in `ingest.rs`, so changing the
storage identity would require a data migration. Do not unify the two layers
without an explicit Owner decision and a migration plan.

## Metrics

| Metric | Definition |
|---|---|
| nDCG@10 | `gain = 2^grade - 1`, `dcg = Σ gain / log2(rank + 1)`, normalised by the ideal ordering |
| MRR@10 | `1 / rank` of the first hit with grade ≥ 2 |
| Recall@10 | relevant hits (grade ≥ 2) in the top 10 ÷ all labelled relevant URLs |
| Precision@10 | relevant hits in the top 10 ÷ 10 |
| Zero-result rate | share of queries returning 0 hits |
| Latency p50 / p95 | wall-clock per query, measured from this machine |
| Coverage | labelled URLs with grade ≥ 2 that exist in TiDB ∪ MongoDB |
| Label coverage | pool rows whose URL has a grade ÷ all pool rows |
| Median top-10 age | median `crawled_at` age of top-10 hits, reported for `freshness_sensitive` queries |

Metrics are reported overall and per slice (`lang:*`, `intent:*`,
`freshness:true|false`, `labeler:owner|silver|mixed|unlabeled`).

Queries with no label with grade ≥ 1 are excluded from the ranking averages
(they measure coverage, not ranking).

## Pooling bias

`--pool` records the **top N (default 10) of each configured system** for every
query. Pooling this way over-represents documents the systems already rank
highly and can never discover a relevant document they fail to return at all.
It is good enough for comparing changes to the same systems, but absolute recall
numbers are optimistic. `extra_urls.csv` is the escape hatch: add URLs the
engine misses.

To limit bias: keep the query set fixed, record the git commit in every run
record, and after any major retrieval change re-pool and label **only the new
URLs**. Existing grades are kept, so a re-pool does not discard earlier work.

## Acceptance rule for future changes

`--compare eval/baseline.json` fails a change when:

- overall nDCG@10 drops by more than **2%**, or
- any slice nDCG@10 drops by more than **5%**, or
- any slice nDCG@10 drops by more than **0.2** absolute (listed for manual review).

## Variance

Run the harness twice on the same commit before trusting a delta. Differences
below the observed run-to-run variance are noise, not signal.
