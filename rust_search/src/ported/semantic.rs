//! Fully local semantic embeddings — no external model, service, or API key.
//!
//! Text is hashed into a fixed-size TF-IDF-style vector built from word
//! tokens plus character trigrams over the Arabic-normalized text. That makes
//! the similarity search tolerant of typos and word-form differences while
//! staying free to compute and run. Swapping in a trained multilingual
//! embedding model later only requires a different
//! [`vector_search::TextEmbedder`]; the rest of the pipeline stays unchanged.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use mongodb::{
    bson::{doc, Document},
    Collection,
};
use mysql_async::{prelude::Queryable, Pool};
use sha2::{Digest, Sha256};

use crate::ported::common::{normalize_text, DOCUMENTS_TABLE};
use crate::ported::vector_search::{TextEmbedder, VectorSearchEngine, VECTOR_SIZE};

/// Dimension of locally computed embeddings; matches [`VECTOR_SIZE`] so
/// documents load straight into [`VectorSearchEngine`].
pub const LOCAL_EMBEDDING_DIMENSIONS: usize = VECTOR_SIZE;
/// Character n-gram length used for sub-word robustness.
const GRAM_SIZE: usize = 3;
const GRAM_WEIGHT: f32 = 0.35;
const TOKEN_WEIGHT: f32 = 1.0;
/// How much description/content text feeds one document embedding.
const DESCRIPTION_EMBED_CHARS: usize = 400;
const CONTENT_EMBED_CHARS: usize = 1_200;

/// The zero-dependency embedder used by the server and the tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalTextEmbedder;

impl TextEmbedder for LocalTextEmbedder {
    fn embed(&self, text: &str) -> Option<Vec<f32>> {
        Some(embed_hashed_tfidf(text))
    }
}

/// Builds the deterministic local embedding for a text snippet. Empty input
/// yields an all-zero vector (which cosine similarity rejects).
pub fn embed_hashed_tfidf(text: &str) -> Vec<f32> {
    // A BTreeMap fixes the feature-accumulation order: f32 addition is not
    // associative, so a HashMap's random order made embeddings differ by a
    // last-ulp rounding error across calls for collision-heavy texts.
    let mut counts: BTreeMap<u32, f32> = BTreeMap::new();
    for (feature, weight) in features(&normalize_text(text)) {
        *counts.entry(hash_feature(&feature)).or_default() += weight;
    }
    let mut vector = vec![0.0_f32; LOCAL_EMBEDDING_DIMENSIONS];
    for (bucket, weight) in counts {
        let index = (bucket as usize) % LOCAL_EMBEDDING_DIMENSIONS;
        vector[index] += weight;
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

/// Words are embedded whole; character trigrams add sub-word overlap so
/// misspellings and derived word forms stay close to their base word.
fn features(normalized: &str) -> Vec<(String, f32)> {
    let mut features = Vec::new();
    for word in normalized.split_whitespace() {
        features.push((word.to_owned(), TOKEN_WEIGHT));
        let chars: Vec<char> = word.chars().collect();
        if chars.len() > GRAM_SIZE {
            for window in chars.windows(GRAM_SIZE) {
                features.push((window.iter().collect::<String>(), GRAM_WEIGHT));
            }
        }
    }
    features
}

fn hash_feature(feature: &str) -> u32 {
    let digest = Sha256::digest(feature.as_bytes());
    u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]])
}

/// Composes the text embedded for one document. Titles repeat three times to
/// mirror the keyword index's title weighting.
pub fn build_document_text(title: &str, description: &str, content: &str) -> String {
    let description = description.chars().take(DESCRIPTION_EMBED_CHARS).collect::<String>();
    let content = content.chars().take(CONTENT_EMBED_CHARS).collect::<String>();
    format!("{title} {title} {title} {description} {content}")
}

#[derive(Debug, Clone, Default)]
pub struct SemanticSourcePage {
    pub url: String,
    pub title: String,
    pub description: String,
    pub content: String,
}

/// Builds the in-memory semantic index from the configured cloud stores,
/// embedding up to `max_documents` most-recent pages. Returns `None` when no
/// backend is configured or no pages were found.
pub async fn build_semantic_engine(
    pool: Option<&Pool>,
    mongo: Option<&Collection<Document>>,
    max_documents: usize,
) -> Option<VectorSearchEngine> {
    if pool.is_none() && mongo.is_none() {
        return None;
    }
    let mut engine = VectorSearchEngine::new(Some(Box::new(LocalTextEmbedder)));
    let mut loaded = 0usize;
    if let Some(pool) = pool {
        match load_tidb_pages(pool, max_documents).await {
            Ok(pages) => loaded += add_pages(&mut engine, &pages),
            Err(error) => eprintln!("Could not load pages for the semantic index: {error}"),
        }
    }
    let remaining = max_documents.saturating_sub(loaded);
    if remaining > 0 {
        if let Some(collection) = mongo {
            match load_mongo_pages(collection, remaining).await {
                Ok(pages) => loaded += add_pages(&mut engine, &pages),
                Err(error) => {
                    eprintln!("Could not load MongoDB pages for the semantic index: {error}")
                }
            }
        }
    }
    (loaded > 0).then_some(engine)
}

fn add_pages(engine: &mut VectorSearchEngine, pages: &[SemanticSourcePage]) -> usize {
    let mut added = 0;
    for page in pages {
        if page.url.is_empty() {
            continue;
        }
        let text = build_document_text(&page.title, &page.description, &page.content);
        let vector = embed_hashed_tfidf(&text);
        if engine.add_precomputed_document(
            &page.url,
            vector,
            &page.title,
            &page.content,
            &page.url,
        ) {
            added += 1;
        }
    }
    added
}

pub(crate) async fn load_tidb_pages(pool: &Pool, limit: usize) -> Result<Vec<SemanticSourcePage>> {
    let mut connection = pool.get_conn().await.context("connecting to TiDB Cloud")?;
    let rows: Vec<mysql_async::Row> = connection
        .exec(
            format!(
                "SELECT url,title,LEFT(description,400),LEFT(content,1500) \
                 FROM `{DOCUMENTS_TABLE}` ORDER BY crawled_at DESC LIMIT ?"
            ),
            (limit as u64,),
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| SemanticSourcePage {
            url: row.get::<Option<String>, _>(0).flatten().unwrap_or_default(),
            title: row.get::<Option<String>, _>(1).flatten().unwrap_or_default(),
            description: row
                .get::<Option<String>, _>(2)
                .flatten()
                .unwrap_or_default(),
            content: row.get::<Option<String>, _>(3).flatten().unwrap_or_default(),
        })
        .collect())
}

pub(crate) async fn load_mongo_pages(
    collection: &Collection<Document>,
    limit: usize,
) -> Result<Vec<SemanticSourcePage>> {
    use futures_util::TryStreamExt;

    let mut cursor = collection
        .find(doc! {})
        .sort(doc! { "crawled_at": -1 })
        .limit(limit as i64)
        .projection(doc! { "url": 1, "title": 1, "description": 1, "content": 1 })
        .await?;
    let mut pages = Vec::new();
    while let Some(document) = cursor.try_next().await? {
        // Mirror the TiDB loader's truncation so both backends feed
        // identical-sized content into the index and embeddings.
        pages.push(SemanticSourcePage {
            url: document.get_str("url").unwrap_or_default().to_owned(),
            title: document.get_str("title").unwrap_or_default().to_owned(),
            description: document
                .get_str("description")
                .unwrap_or_default()
                .chars()
                .take(400)
                .collect(),
            content: document
                .get_str("content")
                .unwrap_or_default()
                .chars()
                .take(1500)
                .collect(),
        });
    }
    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ported::vector_search::cosine_similarity;

    fn cosine(left: &str, right: &str) -> f32 {
        cosine_similarity(
            &embed_hashed_tfidf(left),
            &embed_hashed_tfidf(right),
        )
        .unwrap()
    }

    #[test]
    fn embeddings_are_deterministic_normalized_and_sized() {
        let first = embed_hashed_tfidf("دليل الجامعات المصرية");
        let second = embed_hashed_tfidf("دليل الجامعات المصرية");
        assert_eq!(first, second);
        assert_eq!(first.len(), LOCAL_EMBEDDING_DIMENSIONS);
        let norm: f32 = first.iter().map(|value| value * value).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4);
    }

    #[test]
    fn embeddings_are_stable_across_calls_with_many_collisions() {
        // Hundreds of distinct words produce thousands of hashed features,
        // far more than the 384 buckets, so many features collide into the
        // same bucket. The accumulation order must not change the result.
        let text = (0..500)
            .map(|index| format!("موضوع{index}"))
            .collect::<Vec<_>>()
            .join(" فاصل ");
        let first = embed_hashed_tfidf(&text);
        let second = embed_hashed_tfidf(&text);
        assert_eq!(first, second);
        let norm: f32 = first.iter().map(|value| value * value).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4);
    }

    #[test]
    fn typos_and_related_forms_stay_close_while_unrelated_text_stays_far() {
        let typo_arabic = cosine("الجامعة", "الجامتة");
        let typo_latin = cosine("rust programming guide", "rust programing guide");
        let far_arabic = cosine(
            "دليل قبول الجامعات المصرية وامتحانات النهاية",
            "وصفة طبخ للعشاء بالخضار واللحوم",
        );
        let far_latin = cosine(
            "university admission guide for students",
            "cooking recipe with vegetables",
        );
        // Short Arabic words share few trigrams, so a typo pair lands near
        // 0.23 — still several times the unrelated baseline.
        assert!(typo_arabic > 0.15, "typo_arabic={typo_arabic}");
        assert!(typo_latin > 0.5, "typo_latin={typo_latin}");
        assert!(far_arabic < 0.3, "far_arabic={far_arabic}");
        assert!(far_latin < 0.3, "far_latin={far_latin}");
        assert!(
            typo_arabic > far_arabic * 2.0,
            "typo_arabic={typo_arabic} far_arabic={far_arabic}"
        );
    }

    #[test]
    fn empty_text_embeds_to_a_zero_vector() {
        assert!(embed_hashed_tfidf("").iter().all(|value| *value == 0.0));
        assert!(cosine_similarity(
            &embed_hashed_tfidf(""),
            &embed_hashed_tfidf("نص")
        )
        .is_none());
    }

    #[test]
    fn document_text_repeats_titles_and_truncates_content() {
        let text = build_document_text("دليل", "وصف قصير", "محتوى");
        assert!(text.starts_with("دليل دليل دليل"));
        assert!(text.contains("وصف قصير"));
    }

    #[test]
    fn engine_ranks_precomputed_documents_semantically() {
        let mut engine = VectorSearchEngine::new(Some(Box::new(LocalTextEmbedder)));
        for (id, title) in [
            ("https://a.test", "دليل الجامعات في مصر"),
            ("https://b.test", "وصفة كيك بالشوكولاتة"),
        ] {
            let vector = embed_hashed_tfidf(&build_document_text(title, "", "محتوى عام"));
            assert!(engine.add_precomputed_document(id, vector, title, "محتوى عام", id));
        }
        let hits = engine.semantic_search("الجامعات", 2);
        assert_eq!(hits[0].0.id, "https://a.test");
    }
}
