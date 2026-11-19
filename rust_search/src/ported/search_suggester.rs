use std::collections::{HashMap, HashSet};

/// Hard cap on tracked unique queries: beyond this, the least popular
/// queries are pruned so the suggester's memory stays bounded.
const QUERY_COUNT_CAP: usize = 10_000;
/// How many of the most popular queries survive a prune.
const QUERY_PRUNE_KEEP: usize = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopularQuery {
    pub query: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentSuggestion {
    pub word: String,
    pub score: u64,
}

pub struct SearchSuggester {
    pub max_suggestions: usize,
    query_counts: HashMap<String, u64>,
    user_history: HashMap<String, Vec<String>>,
    content_counts: HashMap<String, HashMap<String, u64>>,
    query_prefixes: HashMap<String, HashSet<String>>,
    word_prefixes: HashMap<String, HashSet<String>>,
    trending: HashMap<usize, Vec<String>>,
}

impl SearchSuggester {
    pub fn new(max_suggestions: usize) -> Self {
        Self {
            max_suggestions,
            ..Self::default()
        }
    }

    pub fn add_search_query(&mut self, query: &str, user_id: Option<&str>) {
        if query.is_empty() {
            return;
        }
        let query = query.trim().to_lowercase();
        *self.query_counts.entry(query.clone()).or_default() += 1;
        for end in 1..=query.chars().count() {
            let prefix: String = query.chars().take(end).collect();
            self.query_prefixes
                .entry(prefix)
                .or_default()
                .insert(query.clone());
        }
        if let Some(user_id) = user_id.filter(|id| !id.is_empty()) {
            let history = self.user_history.entry(user_id.to_owned()).or_default();
            history.insert(0, query);
            history.truncate(100);
        }
        if self.query_counts.len() > QUERY_COUNT_CAP {
            self.prune_query_counts();
        }
    }

    /// Keeps only the most popular queries and rebuilds the derived prefix
    /// index from the survivors, bounding the suggester's memory growth.
    fn prune_query_counts(&mut self) {
        let mut entries: Vec<(String, u64)> = self.query_counts.drain().collect();
        // Deterministic: count descending, then key ascending.
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.truncate(QUERY_PRUNE_KEEP);
        self.query_counts = entries.into_iter().collect();
        self.query_prefixes.clear();
        for query in self.query_counts.keys() {
            for end in 1..=query.chars().count() {
                let prefix: String = query.chars().take(end).collect();
                self.query_prefixes
                    .entry(prefix)
                    .or_default()
                    .insert(query.clone());
            }
        }
    }

    pub fn get_suggestions(&self, prefix: &str, limit: Option<usize>) -> Vec<String> {
        let limit = limit
            .filter(|value| *value != 0)
            .unwrap_or(self.max_suggestions);
        let prefix = prefix.trim().to_lowercase();
        let mut suggestions: Vec<String> = self
            .query_prefixes
            .get(&prefix)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        self.sort_by_popularity(&mut suggestions);
        if suggestions.is_empty() {
            suggestions = self
                .get_popular_queries(101)
                .into_iter()
                .map(|item| item.query)
                .filter(|query| query.starts_with(&prefix))
                .take(limit)
                .collect();
        } else {
            suggestions.truncate(limit);
        }
        suggestions
    }

    /// Orders suggestions by how often they were searched, then by text so
    /// the same prefix always yields the same order.
    fn sort_by_popularity(&self, suggestions: &mut [String]) {
        suggestions.sort_by(|left, right| {
            let left_count = self.query_counts.get(left).copied().unwrap_or(0);
            let right_count = self.query_counts.get(right).copied().unwrap_or(0);
            right_count
                .cmp(&left_count)
                .then_with(|| left.cmp(right))
        });
    }

    pub fn get_popular_queries(&self, limit: usize) -> Vec<PopularQuery> {
        let ordering = |a: &PopularQuery, b: &PopularQuery| {
            b.count.cmp(&a.count).then_with(|| b.query.cmp(&a.query))
        };
        let mut popular: Vec<_> = self
            .query_counts
            .iter()
            .map(|(query, count)| PopularQuery {
                query: query.clone(),
                count: *count,
            })
            .collect();
        // Top-K selection instead of a full sort: only the requested window
        // needs ordering, and it runs under the suggester lock per keystroke.
        if limit != 0 && popular.len() > limit {
            popular.select_nth_unstable_by(limit - 1, ordering);
            popular.truncate(limit);
        }
        popular.sort_by(ordering);
        popular
    }

    pub fn get_trending_queries(&self, hours: usize, limit: usize) -> Vec<String> {
        let mut queries = self.trending.get(&hours).cloned().unwrap_or_default();
        if limit != 0 {
            queries.truncate(limit);
        }
        queries
    }

    pub fn set_trending_queries(&mut self, hours: usize, queries: Vec<String>) {
        self.trending.insert(hours, queries);
    }

    pub fn get_personalized_suggestions(
        &self,
        user_id: &str,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Vec<String> {
        if user_id.is_empty() {
            return Vec::new();
        }
        let limit = limit
            .filter(|value| *value != 0)
            .unwrap_or(self.max_suggestions);
        let Some(history) = self.user_history.get(user_id) else {
            return prefix
                .map(|p| self.get_suggestions(p, Some(limit)))
                .unwrap_or_else(|| {
                    self.get_popular_queries(limit)
                        .into_iter()
                        .map(|item| item.query)
                        .collect()
                });
        };
        let filtered: Vec<String> = history
            .iter()
            .take(51)
            .filter(|query| {
                prefix
                    .map(|p| query.starts_with(&p.to_lowercase()))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        filtered.into_iter().take(limit).collect()
    }

    pub fn index_content_words(&mut self, words: &[String], source: &str) {
        let source_counts = self.content_counts.entry(source.to_owned()).or_default();
        let mut counts: HashMap<String, u64> = HashMap::new();
        for word in words {
            *counts.entry(word.clone()).or_default() += 1;
        }
        for (original, count) in counts {
            let word = original.to_lowercase().trim().to_owned();
            if word.chars().count() <= 2 {
                continue;
            }
            *source_counts.entry(word.clone()).or_default() += count;
            for end in 1..=word.chars().count().min(9) {
                let prefix: String = word.chars().take(end).collect();
                self.word_prefixes
                    .entry(prefix)
                    .or_default()
                    .insert(word.clone());
            }
        }
    }

    /// Indexes pre-counted content words (word → occurrences) from an
    /// offline pass, e.g. the search engine's own term dictionary, without
    /// re-counting individual tokens.
    pub fn index_word_frequency(&mut self, counts: &[(String, u64)], source: &str) {
        let source_counts = self.content_counts.entry(source.to_owned()).or_default();
        for (word, count) in counts {
            let word = word.to_lowercase().trim().to_owned();
            if word.chars().count() <= 2 {
                continue;
            }
            *source_counts.entry(word.clone()).or_default() += *count;
            for end in 1..=word.chars().count().min(9) {
                let prefix: String = word.chars().take(end).collect();
                self.word_prefixes
                    .entry(prefix)
                    .or_default()
                    .insert(word.clone());
            }
        }
    }

    pub fn get_content_suggestions(
        &self,
        prefix: &str,
        limit: Option<usize>,
    ) -> Vec<ContentSuggestion> {
        let limit = limit
            .filter(|value| *value != 0)
            .unwrap_or(self.max_suggestions);
        let prefix = prefix.to_lowercase();
        let Some(words) = self.word_prefixes.get(&prefix) else {
            return Vec::new();
        };
        let mut words: Vec<String> = words.iter().cloned().collect();
        let score = |word: &str| {
            self.content_counts
                .get("content")
                .and_then(|counts| counts.get(word))
                .copied()
                .unwrap_or(0)
        };
        words.sort_by(|left, right| {
            score(right)
                .cmp(&score(left))
                .then_with(|| left.cmp(right))
        });
        words
            .into_iter()
            .take(limit)
            .map(|word| ContentSuggestion {
                score: score(&word),
                word,
            })
            .collect()
    }

    /// Prunes low-popularity queries on demand too.
    pub fn clear_old_data(&mut self, _days: usize) {
        self.prune_query_counts();
    }
}

impl Default for SearchSuggester {
    fn default() -> Self {
        Self {
            max_suggestions: 10,
            query_counts: HashMap::new(),
            user_history: HashMap::new(),
            content_counts: HashMap::new(),
            query_prefixes: HashMap::new(),
            word_prefixes: HashMap::new(),
            trending: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn popular_and_personalized_suggestions() {
        let mut suggester = SearchSuggester::new(10);
        suggester.add_search_query(" Rust Search ", Some("u"));
        suggester.add_search_query("rust search", Some("u"));
        suggester.add_search_query("rust syntax", Some("u"));
        assert_eq!(suggester.get_suggestions("RUST", Some(1)), ["rust search"]);
        assert_eq!(
            suggester.get_personalized_suggestions("u", Some("rust s"), Some(3)),
            ["rust syntax", "rust search", "rust search"]
        );
        assert_eq!(suggester.get_popular_queries(1)[0].count, 2);
    }

    #[test]
    fn content_suggestions_use_sorted_prefix_and_content_scores() {
        let mut suggester = SearchSuggester::default();
        suggester.index_content_words(
            &["search".into(), "search".into(), "semantic".into()],
            "content",
        );
        let values = suggester.get_content_suggestions("s", Some(5));
        assert_eq!(values[0].word, "search");
        assert_eq!(values[0].score, 2);
    }

    #[test]
    fn precounted_words_feed_the_same_prefix_index() {
        let mut suggester = SearchSuggester::default();
        suggester.index_word_frequency(
            &[("جامعة".to_owned(), 5), ("جامعات".to_owned(), 2)],
            "content",
        );
        let values = suggester.get_content_suggestions("جامع", Some(5));
        assert_eq!(values.len(), 2);
        let university = values.iter().find(|value| value.word == "جامعة");
        assert_eq!(university.map(|value| value.score), Some(5));
    }

    #[test]
    fn query_counts_are_capped_and_prefixes_rebuilt_from_survivors() {
        let mut suggester = SearchSuggester::new(10);
        for index in 0..10_050 {
            suggester.add_search_query(&format!("q{index:05}"), None);
        }
        for _ in 0..3 {
            suggester.add_search_query("special query", None);
        }
        assert!(suggester.get_popular_queries(0).len() <= 10_000);
        // A surviving query still autocompletes from the rebuilt prefix index.
        assert_eq!(
            suggester.get_suggestions("special", None),
            vec!["special query".to_owned()]
        );
        assert_eq!(
            suggester.get_suggestions("q00001", None),
            vec!["q00001".to_owned()]
        );
        // A pruned low-count query no longer autocompletes.
        assert!(suggester.get_suggestions("q09999", None).is_empty());
    }

    #[test]
    fn popular_queries_top_k_matches_full_sort() {
        let mut suggester = SearchSuggester::new(10);
        for index in 0..3_000usize {
            suggester.add_search_query(&format!("topic{index:04}"), None);
        }
        // Give varied counts so the ordering comparator is exercised.
        for index in (0..3_000usize).step_by(3) {
            suggester.add_search_query(&format!("topic{index:04}"), None);
        }
        let all = suggester.get_popular_queries(0);
        assert_eq!(all.len(), 3_000);
        assert_eq!(suggester.get_popular_queries(50), all[..50]);
    }

    #[test]
    fn suggestions_are_ordered_by_search_frequency() {
        let mut suggester = SearchSuggester::new(10);
        suggester.add_search_query("rust search", None);
        suggester.add_search_query("rust search", None);
        suggester.add_search_query("rust search engine", None);
        suggester.add_search_query("rust syntax", None);
        assert_eq!(
            suggester.get_suggestions("rust", Some(3)),
            ["rust search", "rust search engine", "rust syntax"]
        );
    }

    #[test]
    fn content_completions_rank_by_frequency_not_alphabetically() {
        let mut suggester = SearchSuggester::default();
        suggester.index_word_frequency(
            &[
                ("جامعة".to_owned(), 2),
                ("جامعات".to_owned(), 40),
                ("جامع".to_owned(), 9),
            ],
            "content",
        );
        let values = suggester.get_content_suggestions("جامع", Some(2));
        assert_eq!(
            values.iter().map(|value| value.word.as_str()).collect::<Vec<_>>(),
            ["جامعات", "جامع"]
        );
    }
}
