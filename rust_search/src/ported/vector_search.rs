use std::collections::HashSet;

pub const COLLECTION_NAME: &str = "search_pages";
pub const VECTOR_SIZE: usize = 384;

pub trait TextEmbedder: Send + Sync {
    fn embed(&self, text: &str) -> Option<Vec<f32>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorDocument {
    pub id: String,
    pub vector: Vec<f32>,
    pub title: String,
    pub content: String,
    pub url: String,
}

pub struct VectorSearchEngine {
    embedder: Option<Box<dyn TextEmbedder>>,
    documents: Vec<VectorDocument>,
}

impl VectorSearchEngine {
    /// The caller supplies the sentence-embedding model; the original MiniLM model is not
    /// bundled or loaded implicitly by this module.
    pub fn new(embedder: Option<Box<dyn TextEmbedder>>) -> Self {
        Self {
            embedder,
            documents: Vec::new(),
        }
    }

    pub fn embed_text(&self, text: &str) -> Vec<f32> {
        self.embedder
            .as_ref()
            .and_then(|model| model.embed(text))
            .unwrap_or_default()
    }

    pub fn add_document(&mut self, doc_id: &str, title: &str, content: &str, url: &str) -> bool {
        let text = format!("{title}\n{content}");
        let embedding = self.embed_text(&text);
        if embedding.len() != VECTOR_SIZE {
            return false;
        }
        let document = VectorDocument {
            id: doc_id.to_owned(),
            vector: embedding,
            title: title.to_owned(),
            content: content.chars().take(500).collect(),
            url: url.to_owned(),
        };
        if let Some(existing) = self.documents.iter_mut().find(|doc| doc.id == doc_id) {
            *existing = document;
        } else {
            self.documents.push(document);
        }
        true
    }

    /// Adds a document whose embedding was already computed, e.g. loaded from
    /// a persisted index or produced by a local embedder, without calling the
    /// configured embedder again.
    pub fn add_precomputed_document(
        &mut self,
        doc_id: &str,
        vector: Vec<f32>,
        title: &str,
        content: &str,
        url: &str,
    ) -> bool {
        if vector.len() != VECTOR_SIZE {
            return false;
        }
        let document = VectorDocument {
            id: doc_id.to_owned(),
            vector,
            title: title.to_owned(),
            content: content.chars().take(500).collect(),
            url: url.to_owned(),
        };
        if let Some(existing) = self.documents.iter_mut().find(|doc| doc.id == doc_id) {
            *existing = document;
        } else {
            self.documents.push(document);
        }
        true
    }

    pub fn semantic_search(&self, query: &str, limit: usize) -> Vec<(VectorDocument, f32)> {
        let (hits, _) = self.semantic_search_with_total(query, limit);
        hits
    }

    /// Searches semantically and also reports how many documents matched
    /// before truncation, so callers can paginate correctly without the
    /// engine cloning the whole corpus for every request.
    pub fn semantic_search_with_total(
        &self,
        query: &str,
        limit: usize,
    ) -> (Vec<(VectorDocument, f32)>, usize) {
        let query = self.embed_text(query);
        if query.len() != VECTOR_SIZE {
            return (Vec::new(), 0);
        }
        // Score first, clone only the survivors: cloning every matching
        // document up front copies the full corpus on each request.
        let mut scored: Vec<(usize, f32)> = self
            .documents
            .iter()
            .enumerate()
            .filter_map(|(index, doc)| {
                cosine_similarity(&query, &doc.vector).map(|score| (index, score))
            })
            .collect();
        scored.sort_by(|a, b| {
            b.1.total_cmp(&a.1)
                .then_with(|| self.documents[a.0].id.cmp(&self.documents[b.0].id))
        });
        let total = scored.len();
        scored.truncate(limit);
        let hits = scored
            .into_iter()
            .map(|(index, score)| (self.documents[index].clone(), score))
            .collect();
        (hits, total)
    }

    pub fn hybrid_search(
        &self,
        query: &str,
        keyword_results: &[VectorDocument],
        limit: usize,
    ) -> Vec<VectorDocument> {
        let semantic = self.semantic_search(query, limit);
        let mut seen = HashSet::new();
        let mut combined = Vec::new();
        for doc in keyword_results.iter().take(limit / 2) {
            if !doc.url.is_empty() && seen.insert(doc.url.clone()) {
                combined.push(doc.clone());
            }
        }
        for (doc, _) in semantic.iter().take(limit / 2) {
            if !doc.url.is_empty() && seen.insert(doc.url.clone()) {
                combined.push(doc.clone());
            }
        }
        combined.truncate(limit);
        combined
    }

    pub fn count_documents(&self) -> usize {
        self.documents.len()
    }
}

pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f32> {
    if left.len() != right.len() {
        return None;
    }
    let dot: f32 = left.iter().zip(right).map(|(a, b)| a * b).sum();
    let norm_left = left.iter().map(|v| v * v).sum::<f32>().sqrt();
    let norm_right = right.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm_left == 0.0 || norm_right == 0.0 {
        None
    } else {
        Some(dot / (norm_left * norm_right))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TinyEmbedder;
    impl TextEmbedder for TinyEmbedder {
        fn embed(&self, text: &str) -> Option<Vec<f32>> {
            let mut vector = vec![0.0; VECTOR_SIZE];
            vector[if text.to_lowercase().contains("rust") {
                0
            } else {
                1
            }] = 1.0;
            Some(vector)
        }
    }

    #[test]
    fn adds_and_searches_vectors_by_cosine() {
        let mut engine = VectorSearchEngine::new(Some(Box::new(TinyEmbedder)));
        assert!(engine.add_document("1", "Rust", "language", "https://rust.test"));
        assert!(engine.add_document("2", "Other", "page", "https://other.test"));
        assert_eq!(engine.semantic_search("rust", 1)[0].0.id, "1");
        assert_eq!(engine.count_documents(), 2);
    }

    #[test]
    fn cosine_rejects_mismatched_and_zero_vectors() {
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), None);
        assert_eq!(cosine_similarity(&[0.0], &[1.0]), None);
    }

    #[test]
    fn precomputed_documents_skip_the_embedder() {
        let mut engine = VectorSearchEngine::new(None);
        let mut vector = vec![0.0; VECTOR_SIZE];
        vector[0] = 1.0;
        assert!(engine.add_precomputed_document(
            "1",
            vector.clone(),
            "Title",
            "Content",
            "https://a.test"
        ));
        assert!(!engine.add_precomputed_document("2", vec![0.5], "T", "C", "https://b.test"));
        assert_eq!(engine.count_documents(), 1);
        assert_eq!(engine.documents[0].title, "Title");
    }

    #[test]
    fn semantic_search_with_total_reports_true_match_count() {
        let mut engine = VectorSearchEngine::new(Some(Box::new(TinyEmbedder)));
        for (id, title) in [
            ("a", "Rust guide"),
            ("b", "Rust handbook"),
            ("c", "Rust tutorial"),
            ("d", "Rust reference"),
            ("e", "Unrelated topic"),
        ] {
            assert!(engine.add_document(id, title, "content", &format!("https://{id}.test")));
        }
        // Five matches exist but the window is two: total must still be 5.
        let (hits, total) = engine.semantic_search_with_total("rust", 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(total, 5);
        // Ordering is score descending with the id tie-break ascending.
        let ids: Vec<&str> = hits.iter().map(|(doc, _)| doc.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
    }
}
