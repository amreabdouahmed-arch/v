use crate::ported::pagerank::PageRankCalculator;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct ClickData {
    pub clicks: u64,
    pub views: u64,
    pub dwell_time: f64,
}

#[derive(Default)]
pub struct UserSignals {
    pub click_data: HashMap<String, ClickData>,
    pub bounce_data: HashMap<String, f64>,
}

#[derive(Default)]
pub struct RelevanceScorer {
    pub idf_cache: HashMap<String, f64>,
}

pub struct RankingEngine {
    pub pagerank: PageRankCalculator,
    pub user_signals: UserSignals,
    pub relevance: RelevanceScorer,
    pub weights: HashMap<String, f64>,
}

/// Scales raw PageRank scores to a 0..=100 range relative to the top score.
pub fn normalize_pagerank(scores: &HashMap<String, f64>) -> HashMap<String, f64> {
    let top = scores.values().copied().fold(0.0_f64, f64::max);
    if top <= 0.0 {
        return scores.keys().map(|page| (page.clone(), 0.0)).collect();
    }
    scores
        .iter()
        .map(|(page, score)| (page.clone(), score / top * 100.0))
        .collect()
}

impl UserSignals {
    pub fn record_click(&mut self, doc_id: &str, dwell_time: f64) {
        let data = self.click_data.entry(doc_id.to_owned()).or_default();
        data.clicks += 1;
        data.dwell_time += dwell_time;
    }
    pub fn record_view(&mut self, doc_id: &str) {
        self.click_data.entry(doc_id.to_owned()).or_default().views += 1;
    }
    pub fn get_ctr(&mut self, doc_id: &str) -> f64 {
        let data = self.click_data.entry(doc_id.to_owned()).or_default();
        if data.views == 0 {
            0.0
        } else {
            data.clicks as f64 / data.views as f64
        }
    }
    pub fn get_avg_dwell_time(&mut self, doc_id: &str) -> f64 {
        let data = self.click_data.entry(doc_id.to_owned()).or_default();
        if data.clicks == 0 {
            0.0
        } else {
            data.dwell_time / data.clicks as f64
        }
    }
    pub fn get_engagement_score(&mut self, doc_id: &str) -> f64 {
        let ctr = self.get_ctr(doc_id);
        let dwell = self.get_avg_dwell_time(doc_id);
        ctr * 0.6 + (dwell / 30.0).min(1.0) * 0.4
    }

    pub fn save(&self, filename: impl AsRef<Path>) -> Result<(), String> {
        let data: HashMap<String, Value> = self
            .click_data
            .iter()
            .map(|(id, d)| {
                (
                    id.clone(),
                    json!({"clicks": d.clicks, "views": d.views, "dwell_time": d.dwell_time}),
                )
            })
            .collect();
        let serialized = serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?;
        if let Some(parent) = filename.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
        }
        fs::write(filename, serialized).map_err(|e| e.to_string())
    }

    pub fn save_default(&self) -> Result<(), String> {
        self.save("ranking/user_signals.json")
    }

    pub fn load(&mut self, filename: impl AsRef<Path>) -> Result<bool, String> {
        if !filename.as_ref().exists() {
            return Ok(false);
        }
        let data: HashMap<String, Value> =
            serde_json::from_slice(&fs::read(filename).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        self.click_data = data
            .into_iter()
            .map(|(id, value)| {
                (
                    id,
                    ClickData {
                        clicks: value.get("clicks").and_then(Value::as_u64).unwrap_or(0),
                        views: value.get("views").and_then(Value::as_u64).unwrap_or(0),
                        dwell_time: value
                            .get("dwell_time")
                            .and_then(Value::as_f64)
                            .unwrap_or(0.0),
                    },
                )
            })
            .collect();
        Ok(true)
    }

    pub fn load_default(&mut self) -> Result<bool, String> {
        self.load("ranking/user_signals.json")
    }
}

impl RelevanceScorer {
    pub fn calculate_tf(&self, term: &str, document: &str) -> f64 {
        let words: Vec<String> = document
            .to_lowercase()
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if words.is_empty() {
            return 0.0;
        }
        words.iter().filter(|word| word.contains(term)).count() as f64 / words.len() as f64
    }

    pub fn calculate_idf(&mut self, term: &str, documents: &[String]) -> f64 {
        if let Some(score) = self.idf_cache.get(term) {
            return *score;
        }
        let count = documents
            .iter()
            .filter(|doc| doc.to_lowercase().contains(term))
            .count();
        if count == 0 {
            return 0.0;
        }
        let score = (documents.len() as f64 / count as f64).ln();
        self.idf_cache.insert(term.to_owned(), score);
        score
    }

    pub fn calculate_tf_idf(
        &mut self,
        query: &str,
        document: &str,
        all_documents: &[String],
    ) -> f64 {
        query
            .to_lowercase()
            .split_whitespace()
            .map(|term| self.calculate_tf(term, document) * self.calculate_idf(term, all_documents))
            .sum()
    }

    pub fn score_document(&mut self, query: &str, doc: &Value, all_docs: &[String]) -> f64 {
        let title = string_field(doc, "title");
        let content = string_field(doc, "content");
        let url = string_field(doc, "url");
        let text = format!("{title} {content}");
        let mut score = self.calculate_tf_idf(query, &text, all_docs);
        let query_lower = query.to_lowercase();
        let title_lower = title.to_lowercase();
        if title_lower.contains(&query_lower) {
            score += 2.0;
        } else if query_lower
            .split_whitespace()
            .any(|term| title_lower.contains(term))
        {
            score += 1.0;
        }
        if url.to_lowercase().contains(&query_lower.replace(' ', "-")) {
            score += 0.5;
        }
        score
    }
}

impl RankingEngine {
    pub fn new() -> Self {
        Self {
            pagerank: PageRankCalculator::default(),
            user_signals: UserSignals::default(),
            relevance: RelevanceScorer::default(),
            weights: [
                ("relevance".to_owned(), 0.5),
                ("pagerank".to_owned(), 0.25),
                ("user_signals".to_owned(), 0.25),
            ]
            .into(),
        }
    }

    pub fn rank_documents(
        &mut self,
        query: &str,
        documents: &[Value],
        links: Option<&HashMap<String, Vec<String>>>,
    ) -> Vec<(Value, f64)> {
        if documents.is_empty() {
            return Vec::new();
        }
        let pagerank_scores = links
            .map(|graph| {
                let mut calculator = PageRankCalculator::default();
                for (url, targets) in graph {
                    for target in targets {
                        calculator.add_link(url, target);
                    }
                }
                calculator.calculate_pagerank()
            })
            .unwrap_or_default();
        let all_texts: Vec<String> = documents
            .iter()
            .map(|d| {
                format!(
                    "{} {}",
                    string_field(d, "title"),
                    string_field(d, "content")
                )
            })
            .collect();
        let mut scored: Vec<(usize, Value, f64)> = documents
            .iter()
            .enumerate()
            .map(|(index, doc)| {
                let id = string_field(doc, "id");
                let relevance = self.relevance.score_document(query, doc, &all_texts);
                let pagerank = *pagerank_scores.get(&id).unwrap_or(&0.1);
                let user = self.user_signals.get_engagement_score(&id);
                let final_score = self.weights.get("relevance").copied().unwrap_or(0.0) * relevance
                    + self.weights.get("pagerank").copied().unwrap_or(0.0) * pagerank
                    + self.weights.get("user_signals").copied().unwrap_or(0.0) * user;
                (index, doc.clone(), final_score)
            })
            .collect();
        scored.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        scored
            .into_iter()
            .map(|(_, doc, score)| (doc, score))
            .collect()
    }

    pub fn update_weights(&mut self, relevance: f64, pagerank: f64, user_signals: f64) {
        let total = relevance + pagerank + user_signals;
        self.weights = [
            ("relevance".to_owned(), relevance / total),
            ("pagerank".to_owned(), pagerank / total),
            ("user_signals".to_owned(), user_signals / total),
        ]
        .into();
    }

    pub fn save(&self, directory: impl AsRef<Path>) -> Result<(), String> {
        fs::create_dir_all(directory.as_ref()).map_err(|e| e.to_string())?;
        self.user_signals
            .save(directory.as_ref().join("user_signals.json"))?;
        fs::write(
            directory.as_ref().join("weights.json"),
            serde_json::to_string_pretty(&self.weights).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())
    }

    pub fn save_default(&self) -> Result<(), String> {
        self.save("ranking")
    }

    pub fn load(&mut self, directory: impl AsRef<Path>) -> Result<(), String> {
        let directory = directory.as_ref();
        self.user_signals
            .load(directory.join("user_signals.json"))?;
        let weights_path = directory.join("weights.json");
        if weights_path.exists() {
            self.weights =
                serde_json::from_slice(&fs::read(weights_path).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn load_default(&mut self) -> Result<(), String> {
        self.load("ranking")
    }
}

impl Default for RankingEngine {
    fn default() -> Self {
        Self::new()
    }
}

pub fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relevance_and_user_signals_affect_ranking() {
        let mut engine = RankingEngine::new();
        engine.user_signals.record_view("1");
        engine.user_signals.record_click("1", 30.0);
        let docs = vec![
            json!({"id":"1","title":"HTML guide","content":"learn html","url":"/html"}),
            json!({"id":"2","title":"Other","content":"unrelated","url":"/other"}),
        ];
        let ranked = engine.rank_documents("html", &docs, None);
        assert_eq!(ranked[0].0["id"], "1");
        assert!(ranked[0].1 > ranked[1].1);
    }

    #[test]
    fn empty_query_and_empty_documents_are_safe() {
        let mut scorer = RelevanceScorer::default();
        assert_eq!(scorer.calculate_tf_idf(" ", "some text", &[]), 0.0);
        assert!(RankingEngine::new()
            .rank_documents("x", &[], None)
            .is_empty());
    }

    #[test]
    fn normalized_pagerank_peaks_at_100() {
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();
        graph.insert(
            "https://hub.test/".to_owned(),
            vec!["https://hub.test/page".to_owned()],
        );
        graph.insert(
            "https://hub.test/page".to_owned(),
            vec!["https://hub.test/".to_owned()],
        );
        graph.insert("https://hub.test/orphan".to_owned(), Vec::new());
        let mut calculator = PageRankCalculator::default();
        for (url, targets) in &graph {
            if targets.is_empty() {
                calculator.add_page(url);
            } else {
                for target in targets {
                    calculator.add_link(url, target);
                }
            }
        }
        let normalized = calculator.calculate_pagerank();
        let top = normalized.values().copied().fold(0.0, f64::max);
        assert!((top - 100.0).abs() < 1e-9);
        assert!(normalized
            .values()
            .all(|score| (0.0..=100.0).contains(score)));
        assert!(normalized["https://hub.test/orphan"] < top);
    }

    #[test]
    fn normalization_without_positive_scores_stays_zero() {
        let mut raw = HashMap::new();
        raw.insert("a".to_owned(), 0.0_f64);
        assert_eq!(normalize_pagerank(&raw)["a"], 0.0);
        assert!(normalize_pagerank(&HashMap::new()).is_empty());
    }
}
