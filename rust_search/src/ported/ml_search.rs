use serde_json::{json, Map, Value};
use std::collections::HashMap;

pub struct MLQueryUnderstanding;
pub struct MLRankingEngine;
pub struct MLPersonalization {
    pub user_id: Option<String>,
    pub user_history: Vec<(String, String)>,
}
pub struct MLAutoComplete {
    query_frequencies: HashMap<String, usize>,
    insertion_order: Vec<String>,
}
pub struct MLSearchPipeline {
    pub query_understanding: MLQueryUnderstanding,
    pub ranking_engine: MLRankingEngine,
    pub personalization: MLPersonalization,
    pub autocomplete: MLAutoComplete,
}

impl MLQueryUnderstanding {
    pub fn detect_intent(query: &str) -> String {
        let intents: [(&str, &[&str]); 7] = [
            (
                "university",
                &["university", "college", "school", "academy", "institute"],
            ),
            ("news", &["news", "latest", "today", "breaking", "update"]),
            ("video", &["video", "youtube", "watch", "film", "movie"]),
            ("image", &["image", "photo", "picture", "gallery"]),
            (
                "definition",
                &["what is", "define", "meaning", "definition"],
            ),
            ("how_to", &["how to", "how do", "tutorial", "guide"]),
            (
                "comparison",
                &["vs", "versus", "compare", "difference", "better"],
            ),
        ];
        let lower = query.to_lowercase();
        let mut winner = ("general".to_owned(), 0usize);
        for (intent, patterns) in intents {
            let score = patterns
                .iter()
                .filter(|pattern| lower.contains(**pattern))
                .count();
            if score > winner.1 {
                winner = (intent.to_owned(), score);
            }
        }
        winner.0
    }

    pub fn extract_entities(query: &str) -> HashMap<String, Vec<String>> {
        let patterns: [(&str, &[&str]); 3] = [
            (
                "location",
                &["in", "at", "near", "location", "city", "country"],
            ),
            ("time", &["when", "date", "year", "time", "schedule"]),
            ("person", &["who", "person", "author", "founder"]),
        ];
        let lower = query.to_lowercase();
        let mut entities = HashMap::new();
        for (entity, terms) in patterns {
            let found: Vec<String> = terms
                .iter()
                .filter(|term| lower.contains(**term))
                .map(|term| (*term).to_owned())
                .collect();
            if !found.is_empty() {
                entities.insert(entity.to_owned(), found);
            }
        }
        entities
    }

    pub fn expand_query(query: &str) -> Vec<String> {
        let synonyms: [(&str, &[&str]); 4] = [
            ("university", &["college", "school", "academy"]),
            ("best", &["top", "leading", "excellent"]),
            ("cheap", &["affordable", "low cost", "budget"]),
            ("fast", &["quick", "speedy", "rapid"]),
        ];
        let mut expanded = vec![query.to_owned()];
        for word in query.to_lowercase().split_whitespace() {
            if let Some((_, replacements)) = synonyms.iter().find(|(key, _)| *key == word) {
                for replacement in *replacements {
                    expanded.push(query.replace(word, &format!("{word} {replacement}")));
                }
            }
        }
        expanded
    }
}

impl MLRankingEngine {
    pub fn calculate_tf_idf(term: &str, doc: &str, all_docs: &[String]) -> f64 {
        let lower_doc = doc.to_lowercase();
        let words: Vec<&str> = lower_doc.split_whitespace().collect();
        let tf = if words.is_empty() {
            0.0
        } else {
            words
                .iter()
                .filter(|word| **word == term.to_lowercase())
                .count() as f64
                / words.len() as f64
        };
        let term = term.to_lowercase();
        let doc_count = all_docs
            .iter()
            .filter(|d| d.to_lowercase().contains(&term))
            .count();
        let idf = ((all_docs.len() as f64) / (doc_count as f64 + 1.0)).ln() + 1.0;
        tf * idf
    }

    pub fn cosine_similarity(vec1: &HashMap<String, f64>, vec2: &HashMap<String, f64>) -> f64 {
        let dot: f64 = vec1
            .iter()
            .map(|(key, value)| value * vec2.get(key).unwrap_or(&0.0))
            .sum();
        let norm1 = vec1.values().map(|v| v * v).sum::<f64>().sqrt();
        let norm2 = vec2.values().map(|v| v * v).sum::<f64>().sqrt();
        if norm1 == 0.0 || norm2 == 0.0 {
            0.0
        } else {
            dot / (norm1 * norm2)
        }
    }

    pub fn rank_results(query: &str, results: &[Value]) -> Vec<Value> {
        let mut scored: Vec<(usize, Value)> = results
            .iter()
            .enumerate()
            .map(|(index, result)| {
                let title = value_string(result, "title");
                let content = value_string(result, "content");
                let url = value_string(result, "url");
                let lower = query.to_lowercase();
                let mut score = 0.0;
                if title.to_lowercase().contains(&lower) {
                    score += 0.25;
                }
                if content.to_lowercase().contains(&lower) {
                    score += 0.20;
                }
                score += 0.15 * 0.5;
                if [".edu", ".gov", "wikipedia"]
                    .iter()
                    .any(|d| url.contains(d))
                {
                    score += 0.15;
                }
                let mut result = result.clone();
                ensure_object(&mut result).insert("ml_score".to_owned(), json!(score));
                (index, result)
            })
            .collect();
        scored.sort_by(|(ia, a), (ib, b)| {
            score_value(b, "ml_score")
                .total_cmp(&score_value(a, "ml_score"))
                .then_with(|| ia.cmp(ib))
        });
        scored.into_iter().map(|(_, value)| value).collect()
    }
}

impl MLPersonalization {
    pub fn new(user_id: Option<String>) -> Self {
        Self {
            user_id,
            user_history: Vec::new(),
        }
    }
    pub fn add_click(&mut self, doc_id: &str, query: &str) {
        self.user_history
            .push((doc_id.to_owned(), query.to_owned()));
    }
    pub fn get_user_vector(&self) -> HashMap<String, f64> {
        let mut interests: HashMap<String, usize> = HashMap::new();
        for (_, query) in &self.user_history {
            for word in query
                .to_lowercase()
                .split_whitespace()
                .filter(|word| word.chars().count() > 3)
            {
                *interests.entry(word.to_owned()).or_default() += 1;
            }
        }
        let total: usize = interests.values().sum();
        if total == 0 {
            return HashMap::new();
        }
        interests
            .into_iter()
            .map(|(word, count)| (word, count as f64 / total as f64))
            .collect()
    }

    pub fn personalize_results(&self, results: &mut [Value]) {
        let user = self.get_user_vector();
        if user.is_empty() {
            return;
        }
        for result in results.iter_mut() {
            let content = format!(
                "{} {}",
                value_string(result, "title"),
                value_string(result, "content")
            )
            .to_lowercase();
            let counts =
                content
                    .split_whitespace()
                    .fold(HashMap::<&str, usize>::new(), |mut acc, w| {
                        *acc.entry(w).or_default() += 1;
                        acc
                    });
            let similarity: f64 = user
                .iter()
                .map(|(w, weight)| weight * *counts.get(w.as_str()).unwrap_or(&0) as f64)
                .sum();
            ensure_object(result).insert("personalization_score".to_owned(), json!(similarity));
        }
        results.sort_by(|a, b| {
            let a_score =
                score_value(a, "ml_score") + score_value(a, "personalization_score") * 0.2;
            let b_score =
                score_value(b, "ml_score") + score_value(b, "personalization_score") * 0.2;
            b_score.total_cmp(&a_score)
        });
    }
}

impl MLAutoComplete {
    pub fn new() -> Self {
        Self {
            query_frequencies: HashMap::new(),
            insertion_order: Vec::new(),
        }
    }
    pub fn add_query(&mut self, query: &str) {
        let key = query.to_lowercase();
        if self.query_frequencies.contains_key(&key) {
            if let Some(stored) = self
                .insertion_order
                .iter_mut()
                .find(|stored| stored.to_lowercase() == key)
            {
                *stored = query.to_owned();
            }
        } else {
            self.insertion_order.push(query.to_owned());
        }
        *self.query_frequencies.entry(key).or_default() += 1;
    }
    pub fn get_suggestions(&self, prefix: &str, limit: usize) -> Vec<(String, usize)> {
        let prefix = prefix.to_lowercase();
        let mut results: Vec<_> = self
            .insertion_order
            .iter()
            .filter(|query| query.to_lowercase().starts_with(&prefix))
            .map(|query| {
                (
                    query.clone(),
                    *self
                        .query_frequencies
                        .get(&query.to_lowercase())
                        .unwrap_or(&0),
                )
            })
            .collect();
        results.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        results.truncate(limit);
        results
    }
}

impl Default for MLAutoComplete {
    fn default() -> Self {
        Self::new()
    }
}

impl MLSearchPipeline {
    pub fn new() -> Self {
        Self {
            query_understanding: MLQueryUnderstanding,
            ranking_engine: MLRankingEngine,
            personalization: MLPersonalization::new(None),
            autocomplete: MLAutoComplete::new(),
        }
    }
    pub fn process_search(
        &mut self,
        query: &str,
        results: &[Value],
        user_id: Option<&str>,
    ) -> Value {
        let intent = MLQueryUnderstanding::detect_intent(query);
        let entities = MLQueryUnderstanding::extract_entities(query);
        let expanded = MLQueryUnderstanding::expand_query(query);
        let mut ranked = MLRankingEngine::rank_results(query, results);
        if let Some(user_id) = user_id {
            self.personalization.user_id = Some(user_id.to_owned());
            self.personalization.personalize_results(&mut ranked);
        }
        json!({
            "intent": intent,
            "entities": entities,
            "expanded_queries": expanded,
            "results": ranked.into_iter().take(20).collect::<Vec<_>>(),
            "ml_enhanced": true
        })
    }
}

impl Default for MLSearchPipeline {
    fn default() -> Self {
        Self::new()
    }
}

pub fn ml_enhance_search(query: &str, raw_results: &[Value]) -> Vec<Value> {
    MLRankingEngine::rank_results(query, raw_results)
        .into_iter()
        .take(20)
        .collect()
}

fn value_string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}
fn score_value(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}
fn ensure_object(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    value
        .as_object_mut()
        .expect("value was converted to object")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_understanding_and_expansion() {
        assert_eq!(
            MLQueryUnderstanding::detect_intent("best university today"),
            "university"
        );
        assert_eq!(
            MLQueryUnderstanding::extract_entities("universities in Boston")["location"],
            ["in"]
        );
        assert!(MLQueryUnderstanding::expand_query("best university")
            .contains(&"best top university".to_owned()));
    }

    #[test]
    fn ranking_adds_score_and_orders_results() {
        let input = vec![
            json!({"title":"Other","content":"unrelated","url":"https://example.com"}),
            json!({"title":"HTML Tutorial","content":"learn HTML","url":"https://example.edu"}),
        ];
        let ranked = MLRankingEngine::rank_results("HTML", &input);
        assert_eq!(ranked[0]["title"], "HTML Tutorial");
        assert!(ranked[0]["ml_score"].as_f64().unwrap() > ranked[1]["ml_score"].as_f64().unwrap());
    }

    #[test]
    fn autocomplete_respects_prefix_frequency() {
        let mut ac = MLAutoComplete::new();
        ac.add_query("Rust search");
        ac.add_query("Rust search");
        ac.add_query("Rust language");
        assert_eq!(
            ac.get_suggestions("RUST", 1)[0],
            ("Rust search".to_owned(), 2)
        );
    }
}
