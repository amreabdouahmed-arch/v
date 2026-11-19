use serde_json::{json, Value};
use std::collections::HashMap;

pub struct PageRankCalculator {
    pub damping_factor: f64,
    pub max_iterations: usize,
    pub convergence_threshold: f64,
    pub page_ranks: HashMap<String, f64>,
    pub link_graph: HashMap<String, Vec<String>>,
    pub incoming_links: HashMap<String, Vec<String>>,
}

impl PageRankCalculator {
    pub fn new(damping_factor: f64, max_iterations: usize, convergence_threshold: f64) -> Self {
        Self {
            damping_factor,
            max_iterations,
            convergence_threshold,
            page_ranks: HashMap::new(),
            link_graph: HashMap::new(),
            incoming_links: HashMap::new(),
        }
    }

    pub fn add_link(&mut self, from_url: &str, to_url: &str) {
        if from_url != to_url {
            let targets = self.link_graph.entry(from_url.to_owned()).or_default();
            if !targets.contains(&to_url.to_owned()) {
                targets.push(to_url.to_owned());
            }
            let sources = self.incoming_links.entry(to_url.to_owned()).or_default();
            if !sources.contains(&from_url.to_owned()) {
                sources.push(from_url.to_owned());
            }
        }
    }

    /// Registers a page in the graph even when it has no outbound links,
    /// so it is still scored as a dangling node.
    pub fn add_page(&mut self, url: &str) {
        self.link_graph.entry(url.to_owned()).or_default();
        self.incoming_links.entry(url.to_owned()).or_default();
    }

    pub fn build_from_crawled_data(&mut self, pages_data: &[Value]) {
        for page in pages_data {
            let from_url = page.get("url").and_then(Value::as_str).unwrap_or_default();
            let content = page
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if from_url.is_empty() {
                continue;
            }
            for other in pages_data {
                let to_url = other.get("url").and_then(Value::as_str).unwrap_or_default();
                if !to_url.is_empty() && to_url != from_url && content.contains(to_url) {
                    self.add_link(from_url, to_url);
                }
            }
        }
    }

    /// Computes PageRank and returns raw (un-normalized) scores.
    pub fn calculate_raw(&mut self) -> HashMap<String, f64> {
        let pages: Vec<String> = self
            .link_graph
            .keys()
            .chain(self.incoming_links.keys())
            .cloned()
            .collect();
        let n = pages.len();
        if n == 0 {
            self.page_ranks.clear();
            return HashMap::new();
        }
        let mut ranks: HashMap<String, f64> = pages
            .iter()
            .map(|url| (url.clone(), 1.0 / n as f64))
            .collect();
        for _ in 0..self.max_iterations {
            let mut next = HashMap::new();
            let mut total_change = 0.0;
            for page in &pages {
                let mut rank_sum = 0.0;
                for source in self.incoming_links.get(page).into_iter().flatten() {
                    let out_count = self.link_graph.get(source).map(Vec::len).unwrap_or(0);
                    if out_count > 0 {
                        rank_sum += ranks.get(source).copied().unwrap_or(0.0) / out_count as f64;
                    }
                }
                let rank = (1.0 - self.damping_factor) / n as f64 + self.damping_factor * rank_sum;
                total_change += (rank - ranks[page]).abs();
                next.insert(page.clone(), rank);
            }
            ranks = next;
            if total_change < self.convergence_threshold {
                break;
            }
        }
        self.page_ranks = ranks.clone();
        ranks
    }

    pub fn calculate_pagerank(&mut self) -> HashMap<String, f64> {
        let ranks = self.calculate_raw();
        let max_rank = ranks.values().copied().fold(0.0_f64, f64::max);
        self.page_ranks = ranks
            .into_iter()
            .map(|(url, rank)| {
                (
                    url,
                    if max_rank == 0.0 {
                        0.0
                    } else {
                        rank / max_rank * 100.0
                    },
                )
            })
            .collect();
        self.page_ranks.clone()
    }

    pub fn get_page_score(&self, url: &str) -> f64 {
        self.page_ranks.get(url).copied().unwrap_or(0.0)
    }

    pub fn get_top_pages(&self, count: usize) -> Vec<(String, f64)> {
        let mut pages: Vec<_> = self
            .page_ranks
            .iter()
            .map(|(url, score)| (url.clone(), *score))
            .collect();
        pages.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        pages.truncate(count);
        pages
    }
}

impl Default for PageRankCalculator {
    fn default() -> Self {
        Self::new(0.85, 100, 1e-6)
    }
}

pub fn calculate_combined_score(
    content_score: f64,
    pagerank_score: f64,
    pagerank_weight: f64,
) -> f64 {
    content_score * (1.0 - pagerank_weight) + pagerank_score * pagerank_weight
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round_ties_even() / 100.0
}

/// Combines in-memory/cloud-supplied scores; intentionally does not read or write SQLite.
pub fn apply_pagerank_to_results(
    results: &[Value],
    pagerank_scores: &HashMap<String, f64>,
    pagerank_weight: f64,
) -> Vec<Value> {
    let mut scored: Vec<(usize, Value)> = results
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let url = source
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let content_score = source.get("score").and_then(Value::as_f64).unwrap_or(50.0);
            let pagerank = pagerank_scores.get(url).copied().unwrap_or(0.0);
            let mut result = source.clone();
            if !result.is_object() {
                result = json!({});
            }
            let object = result.as_object_mut().expect("converted to object");
            object.insert("pagerank_score".to_owned(), json!(round_two(pagerank)));
            object.insert("content_score".to_owned(), json!(round_two(content_score)));
            object.insert(
                "final_score".to_owned(),
                json!(round_two(calculate_combined_score(
                    content_score,
                    pagerank,
                    pagerank_weight
                ))),
            );
            (index, result)
        })
        .collect();
    scored.sort_by(|(ai, a), (bi, b)| {
        let sa = a.get("final_score").and_then(Value::as_f64).unwrap_or(0.0);
        let sb = b.get("final_score").and_then(Value::as_f64).unwrap_or(0.0);
        sb.total_cmp(&sa).then_with(|| ai.cmp(bi))
    });
    scored.into_iter().map(|(_, result)| result).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranks_known_graph_and_includes_dangling_targets() {
        let mut calculator = PageRankCalculator::default();
        calculator.add_link("a", "b");
        calculator.add_link("b", "c");
        calculator.add_link("c", "a");
        let ranks = calculator.calculate_pagerank();
        assert_eq!(ranks.len(), 3);
        assert!((ranks["a"] - 100.0).abs() < 1e-3);
    }

    #[test]
    fn applies_scores_without_database_access() {
        let input = vec![
            json!({"url":"a","score":80.0}),
            json!({"url":"b","score":50.0}),
        ];
        let scores = [("a".to_owned(), 100.0)].into();
        let ranked = apply_pagerank_to_results(&input, &scores, 0.3);
        assert_eq!(ranked[0]["final_score"], 86.0);
    }
}
