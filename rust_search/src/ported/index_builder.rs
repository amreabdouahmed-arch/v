use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use super::text_utils::normalize_text;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexedDocument {
    pub title: String,
    pub content: String,
    pub indexed_length: usize,
    pub url: String,
    pub word_count: usize,
    pub unique_words: usize,
}

#[derive(Clone)]
pub struct InvertedIndex {
    pub index: HashMap<String, HashSet<String>>,
    pub documents: HashMap<String, IndexedDocument>,
    pub word_freq: HashMap<String, usize>,
    pub total_docs: usize,
    pub index_file: Option<PathBuf>,
}

pub struct IndexBuilder {
    pub inverted_index: InvertedIndex,
}

impl InvertedIndex {
    pub fn new(index_file: Option<PathBuf>) -> Self {
        Self {
            index: HashMap::new(),
            documents: HashMap::new(),
            word_freq: HashMap::new(),
            total_docs: 0,
            index_file: Some(index_file.unwrap_or_else(default_index_path)),
        }
    }

    pub fn tokenize(text: &str) -> Vec<String> {
        normalize_text(text)
            .split_whitespace()
            .filter(|word| word.chars().count() > 1)
            .map(str::to_owned)
            .collect()
    }

    pub fn add_document(&mut self, doc_id: &str, title: &str, content: &str, url: &str) {
        if let Some(old) = self.documents.get(doc_id) {
            for token in Self::tokenize(&format!("{} {}", old.title, old.content))
                .into_iter()
                .collect::<HashSet<_>>()
            {
                if let Some(postings) = self.index.get_mut(&token) {
                    postings.remove(doc_id);
                    if postings.is_empty() {
                        self.index.remove(&token);
                    }
                }
                let count = self.word_freq.entry(token).or_default();
                *count = count.saturating_sub(1);
            }
            self.total_docs = self.total_docs.saturating_sub(1);
        }
        let tokens = Self::tokenize(&format!("{title} {content}"));
        let unique: HashSet<String> = tokens.iter().cloned().collect();
        self.documents.insert(
            doc_id.to_owned(),
            IndexedDocument {
                title: title.to_owned(),
                content: content.chars().take(500).collect(),
                indexed_length: tokens.len().max(1),
                url: url.to_owned(),
                word_count: tokens.len(),
                unique_words: unique.len(),
            },
        );
        for token in unique {
            self.index
                .entry(token.clone())
                .or_default()
                .insert(doc_id.to_owned());
            *self.word_freq.entry(token).or_default() += 1;
        }
        self.total_docs += 1;
    }

    pub fn search(&self, query: &str) -> Vec<(String, f64)> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }
        let mut scores: HashMap<String, f64> = HashMap::new();
        for token in tokens {
            if let Some(postings) = self.index.get(&token) {
                let idf = self.total_docs as f64 / (postings.len() as f64 + 1.0);
                for doc_id in postings {
                    *scores.entry(doc_id.clone()).or_default() += idf;
                }
            }
        }
        let mut results: Vec<_> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        results.truncate(50);
        results
    }

    pub fn save(&self) -> Result<(), String> {
        let path = self
            .index_file
            .as_ref()
            .ok_or("index path was not configured")?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
        }
        let index: HashMap<String, Vec<String>> = self
            .index
            .iter()
            .map(|(term, ids)| {
                let mut ids: Vec<_> = ids.iter().cloned().collect();
                ids.sort();
                (term.clone(), ids)
            })
            .collect();
        let documents: HashMap<String, Value> = self.documents.iter().map(|(id, doc)| {
            (id.clone(), json!({
                "title":doc.title, "content":doc.content, "indexed_length":doc.indexed_length,
                "url":doc.url, "word_count":doc.word_count, "unique_words":doc.unique_words
            }))
        }).collect();
        let payload = json!({
            "version":1, "index":index, "documents":documents,
            "word_freq":self.word_freq, "total_docs":self.total_docs
        });
        fs::write(
            path,
            serde_json::to_vec(&payload).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())
    }

    pub fn load(&mut self) -> Result<bool, String> {
        let path = self
            .index_file
            .as_ref()
            .ok_or("index path was not configured")?;
        if !path.exists() {
            return Ok(false);
        }
        let payload: Value = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let invalid = || "invalid index format".to_owned();
        if payload.get("version").and_then(Value::as_u64) != Some(1) {
            return Err(invalid());
        }
        let index_value = payload
            .get("index")
            .and_then(Value::as_object)
            .ok_or_else(invalid)?;
        let mut index = HashMap::new();
        for (term, ids) in index_value {
            let ids = ids.as_array().ok_or_else(invalid)?;
            let mut postings = HashSet::new();
            for id in ids {
                postings.insert(id.as_str().ok_or_else(invalid)?.to_owned());
            }
            index.insert(term.clone(), postings);
        }
        let docs_value = payload
            .get("documents")
            .and_then(Value::as_object)
            .ok_or_else(invalid)?;
        let mut documents = HashMap::new();
        for (id, value) in docs_value {
            if !value.is_object()
                || ["title", "content", "url"].iter().any(|field| {
                    value
                        .get(field)
                        .is_some_and(|field_value| !field_value.is_string())
                })
            {
                return Err(invalid());
            }
            let title = value
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let content = value
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let url = value.get("url").and_then(Value::as_str).unwrap_or_default();
            documents.insert(
                id.clone(),
                IndexedDocument {
                    title: title.to_owned(),
                    content: content.to_owned(),
                    url: url.to_owned(),
                    indexed_length: value
                        .get("indexed_length")
                        .and_then(Value::as_u64)
                        .unwrap_or(1) as usize,
                    word_count: value.get("word_count").and_then(Value::as_u64).unwrap_or(0)
                        as usize,
                    unique_words: value
                        .get("unique_words")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as usize,
                },
            );
        }
        let total_docs = payload
            .get("total_docs")
            .and_then(Value::as_u64)
            .ok_or_else(invalid)? as usize;
        let word_freq = payload
            .get("word_freq")
            .and_then(Value::as_object)
            .ok_or_else(invalid)?
            .iter()
            .map(|(word, count)| Ok((word.clone(), count.as_u64().ok_or_else(invalid)? as usize)))
            .collect::<Result<HashMap<_, _>, String>>()?;
        self.index = index;
        self.documents = documents;
        self.word_freq = word_freq;
        self.total_docs = total_docs;
        Ok(true)
    }
}

fn default_index_path() -> PathBuf {
    let crate_dir = PathBuf::from(option_env!("CARGO_MANIFEST_DIR").unwrap_or("rust_search"));
    crate_dir
        .join("..")
        .join("Indexing")
        .join("inverted_index.json")
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new(None)
    }
}

impl IndexBuilder {
    /// Create an empty, cloud-independent index; records are supplied by the caller.
    pub fn new(index_file: Option<PathBuf>) -> Self {
        Self {
            inverted_index: InvertedIndex::new(index_file),
        }
    }
    pub fn search(&self, query: &str) -> Vec<Value> {
        self.inverted_index
            .search(query)
            .into_iter()
            .filter_map(|(id, score)| {
                self.inverted_index.documents.get(&id).map(|doc| json!({
                "title":doc.title, "content":doc.content, "indexed_length":doc.indexed_length,
                "url":doc.url, "word_count":doc.word_count, "unique_words":doc.unique_words,
                "score":score, "id":id
            }))
            })
            .collect()
    }
    pub fn add_document(&mut self, doc_id: &str, title: &str, content: &str, url: &str) {
        self.inverted_index
            .add_document(doc_id, title, content, url);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn replacement_removes_old_postings_and_updates_search() {
        let mut index = InvertedIndex::default();
        index.add_document("1", "Old title", "Arabic إِبْرَاهِيم", "/old");
        index.add_document("1", "New title", "fresh content", "/new");
        assert!(index.search("old").is_empty());
        assert_eq!(index.search("fresh")[0].0, "1");
        assert_eq!(index.total_docs, 1);
    }

    #[test]
    fn search_scores_binary_postings_and_caps_results() {
        let mut builder = IndexBuilder::new(None);
        builder.add_document("x", "HTML", "html html guide", "/x");
        builder.add_document("y", "Guide", "html", "/y");
        let results = builder.search("html");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["id"], "x");
    }

    #[test]
    fn index_json_round_trip() {
        let path = Path::new("ported-index-test.json").to_path_buf();
        let mut index = InvertedIndex::new(Some(path.clone()));
        index.add_document("id", "عنوان", "محتوى", "/");
        index.save().unwrap();
        let mut loaded = InvertedIndex::new(Some(path.clone()));
        assert!(loaded.load().unwrap());
        assert_eq!(loaded.search("عنوان")[0].0, "id");
        fs::remove_file(path).unwrap();
    }
}
