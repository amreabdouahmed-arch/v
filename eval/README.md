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

# 3. build the labelling pool (top-30 per query) for the Owner
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --pool

# 4. after labels are filled in, record a run
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval --

# 5. compare a change against the committed baseline
cargo run --locked --manifest-path rust_search/Cargo.toml --bin eval -- --compare eval/baseline.json
```

`eval/runs/` and `eval/pool/` are git-ignored. Only `eval/baseline.json`,
`eval/queries.draft.jsonl`, `eval/extra_urls.csv` and this file are committed.

## Files

| Path | Purpose |
|---|---|
| `queries.draft.jsonl` | 115 queries, one JSON object per line, **unlabelled** |
| `extra_urls.csv` | `query_id,url,grade` — known-good URLs supplied by the Owner |
| `pool/pool.csv` | generated labelling pool: `query_id,query,url,title,snippet,rank,grade,labeler` |
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

## Schema (one JSON object per line)

```json
{"id":"q001","query":"جامعة القاهرة","lang":"ar","intent":"navigational",
 "freshness_sensitive":false,"label_source":"unlabeled","labels":{}}
```

`labels` maps a **canonical URL** to a grade:

| Grade | Meaning |
|---|---|
| 0 | irrelevant |
| 1 | marginal (same topic, does not answer) |
| 2 | relevant (answers the query) |
| 3 | highly relevant (authoritative, complete) |

`label_source` is `owner`, `silver` or `unlabeled`. The harness never assigns
grades; labelling is done by the Owner and the Tech Lead.

## Canonical URL rule

Both labels and results are normalised with the same function
(`src/bin/eval.rs`, `canonical_url`):

- lowercase host, strip a leading `www.`
- drop scheme, userinfo, port and fragment
- drop `utm_*`, `fbclid`, `gclid`
- sort the remaining query parameters
- collapse a trailing slash, except for the bare host (`example.com/`)

Unit tests cover each rule.

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
| Median top-10 age | median `crawled_at` age of top-10 hits, reported for `freshness_sensitive` queries |

Metrics are reported overall and per slice (`lang:*`, `intent:*`,
`freshness:true|false`).

Queries with no label with grade ≥ 1 are excluded from the ranking averages
(they measure coverage, not ranking).

## Pooling bias

`--pool` records the **top 30 of the current engine** for each query. Pooling
this way over-represents documents the engine already ranks highly and can
never discover a relevant document the engine fails to return at all. It is
good enough for comparing changes to the same engine, but absolute recall
numbers are optimistic. `extra_urls.csv` is the escape hatch: add URLs the
engine misses.

To limit bias: keep the query set fixed, never re-pool after a ranking change
without re-labelling, and record the git commit in every run record.

## Acceptance rule for future changes

`--compare eval/baseline.json` fails a change when:

- overall nDCG@10 drops by more than **2%**, or
- any slice nDCG@10 drops by more than **5%**, or
- any slice nDCG@10 drops by more than **0.2** absolute (listed for manual review).

## Variance

Run the harness twice on the same commit before trusting a delta. Differences
below the observed run-to-run variance are noise, not signal.