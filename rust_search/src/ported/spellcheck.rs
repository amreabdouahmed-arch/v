//! Local Arabic and English spell correction, powered by the corpus itself.
//!
//! The vocabulary is the term dictionary of the Tantivy index, normalized and
//! weighted by document frequency. A query token that is not in the
//! vocabulary is replaced by the closest known word within an edit distance
//! of two, preferring — on distance ties — words that appear in more
//! documents. Everything runs in-process: no external service, model, or
//! API key.

use std::collections::HashMap;

use crate::ported::common::normalize_text;
use crate::ported::keyboard::{arabic_to_latin, latin_to_arabic};

/// Maximum Damerau-Levenshtein distance for a correction candidate.
const MAX_EDIT_DISTANCE: usize = 2;
/// Tokens shorter than this are left untouched; correcting "في" is noise.
const MIN_TOKEN_CHARS: usize = 3;
/// Candidate words must appear in at least this many documents, so one-off
/// typos inside crawled content never become suggestions.
const MIN_TERM_DOCS: u64 = 2;
/// Terms longer than this are junk; skip them when building the vocabulary.
const MAX_TERM_CHARS: usize = 24;
/// Clitic prefixes stripped when building stem candidates, so prefixed
/// typos like "بالجامتة" can still reach the stem "جامتة".
const ARABIC_PREFIXES: [&str; 6] = ["ال", "وال", "فال", "بال", "كال", "لل"];
/// How many prefix-stripping rounds to run; "وال" + "ال" needs two.
const MAX_PREFIX_ROUNDS: usize = 3;

pub struct SpellChecker {
    vocabulary: HashMap<String, u64>,
    /// Precomputed comparison data, ordered by word for deterministic
    /// iteration: avoids re-deriving char vectors on every correction.
    candidates: Vec<SpellCandidate>,
}

struct SpellCandidate {
    word: String,
    stems: Vec<Vec<char>>,
    docs: u64,
}

impl SpellChecker {
    /// Builds the checker from `(word, document_frequency)` pairs. Words are
    /// normalized, filtered down to plausible terms, and frequency-merged.
    pub fn new(pairs: impl IntoIterator<Item = (String, u64)>) -> Self {
        let mut vocabulary: HashMap<String, u64> = HashMap::new();
        for (word, docs) in pairs {
            let normalized = normalize_text(&word);
            let length = normalized.chars().count();
            if !(MIN_TOKEN_CHARS..=MAX_TERM_CHARS).contains(&length)
                || docs < MIN_TERM_DOCS
                || normalized.chars().all(|c| c.is_numeric())
            {
                continue;
            }
            *vocabulary.entry(normalized).or_default() += docs;
        }
        let mut candidates: Vec<SpellCandidate> = vocabulary
            .iter()
            .map(|(word, docs)| {
                let chars: Vec<char> = word.chars().collect();
                SpellCandidate {
                    word: word.clone(),
                    stems: candidate_stems(&chars),
                    docs: *docs,
                }
            })
            .collect();
        candidates.sort_by(|a, b| a.word.cmp(&b.word));
        Self {
            vocabulary,
            candidates,
        }
    }

    pub fn vocabulary_size(&self) -> usize {
        self.vocabulary.len()
    }

    /// Returns the query rewritten with known words when at least one token
    /// was corrected, otherwise `None`. Correction runs token by token; word
    /// order and tokens without a close candidate are preserved.
    pub fn did_you_mean(&self, query: &str) -> Option<String> {
        if let Some(corrected) = self.layout_corrected_query(query) {
            return Some(corrected);
        }
        let normalized = normalize_text(query);
        if normalized.is_empty() {
            return None;
        }
        let mut corrected = Vec::new();
        let mut changed = false;
        for token in normalized.split_whitespace() {
            match self.fix_token(token) {
                Some(word) => {
                    corrected.push(word);
                    changed = true;
                }
                None => corrected.push(token.to_owned()),
            }
        }
        if changed {
            Some(corrected.join(" "))
        } else {
            None
        }
    }

    /// Suggests a correction for one token, or `None` when the token is
    /// known, too short, numeric, or has no close vocabulary candidate.
    pub fn fix_token(&self, token: &str) -> Option<String> {
        let normalized = normalize_text(token);
        let correctable = normalized.chars().count() >= MIN_TOKEN_CHARS
            && !normalized.chars().all(|c| c.is_numeric());
        if !correctable || self.vocabulary.contains_key(&normalized) {
            return None;
        }
        let alternatives = layout_alternatives(&normalized);
        for alternative in &alternatives {
            if self.vocabulary.contains_key(alternative) {
                return Some(alternative.clone());
            }
        }
        if let Some(word) = self.closest_word(&normalized) {
            return Some(word);
        }
        for alternative in &alternatives {
            if let Some(word) = self.closest_word(alternative) {
                return Some(word);
            }
        }
        None
    }

    /// Like `fix_token`, but works on the raw text before normalization has
    /// stripped layout-only punctuation such as `]`, `[`, or `;`. Only an
    /// exact known word is accepted, so an ordinary misspelling still goes
    /// through the distance-based path.
    pub fn fix_raw_token(&self, raw: &str) -> Option<String> {
        for converted in [latin_to_arabic(raw), arabic_to_latin(raw)]
            .into_iter()
            .flatten()
        {
            let normalized = normalize_text(&converted);
            if self.vocabulary.contains_key(&normalized) {
                return Some(normalized);
            }
        }
        self.fix_token(raw)
    }

    /// Converts a whole query from the wrong keyboard layout when every
    /// resulting token is a known word, e.g. "l]vsm" becomes "مدرسة".
    fn layout_corrected_query(&self, query: &str) -> Option<String> {
        for converted in [latin_to_arabic(query), arabic_to_latin(query)]
            .into_iter()
            .flatten()
        {
            let normalized = normalize_text(&converted);
            let correctable = normalized.chars().count() >= MIN_TOKEN_CHARS;
            if !correctable {
                continue;
            }
            let tokens: Vec<&str> = normalized.split_whitespace().collect();
            if !tokens.is_empty()
                && tokens
                    .iter()
                    .all(|token| self.vocabulary.contains_key(*token))
            {
                return Some(tokens.join(" "));
            }
        }
        None
    }

    /// Finds the vocabulary word within two edits of the token, preferring
    /// the smallest distance, then the highest document frequency, then the
    /// lexicographically smaller word — a total order, so the same typo
    /// always corrects to the same word regardless of iteration order.
    /// Both sides are compared across their stem candidates, so Arabic
    /// clitic-prefixed forms ("بالجامتة") still reach bare stems ("جامتة").
    fn closest_word(&self, token: &str) -> Option<String> {
        let token_chars: Vec<char> = token.chars().collect();
        let token_stems = candidate_stems(&token_chars);
        let mut best: Option<(usize, u64, String)> = None;
        for candidate in &self.candidates {
            let mut distance = MAX_EDIT_DISTANCE + 1;
            'pairs: for token_stem in &token_stems {
                for word_stem in &candidate.stems {
                    if !length_in_range(token_stem, word_stem) {
                        continue;
                    }
                    let current =
                        bounded_edit_distance(token_stem, word_stem, MAX_EDIT_DISTANCE);
                    if current < distance {
                        distance = current;
                        if distance == 0 {
                            break 'pairs;
                        }
                    }
                }
            }
            if distance > MAX_EDIT_DISTANCE {
                continue;
            }
            let better = match &best {
                None => true,
                Some((best_distance, best_docs, best_word)) => {
                    distance < *best_distance
                        || (distance == *best_distance
                            && (*best_docs < candidate.docs
                                || (*best_docs == candidate.docs
                                    && best_word.as_str() > candidate.word.as_str())))
                }
            };
            if better {
                best = Some((distance, candidate.docs, candidate.word.clone()));
            }
        }
        best.map(|(_, _, word)| word)
    }
}

/// Keyboard-layout interpretations of a token, normalized and deduplicated.
/// Only tokens that change under a layout swap are returned, so a normal
/// query never pays the cost or risks a bogus conversion.
fn layout_alternatives(token: &str) -> Vec<String> {
    let mut alternatives: Vec<String> = Vec::new();
    for converted in [latin_to_arabic(token), arabic_to_latin(token)]
        .into_iter()
        .flatten()
    {
        let converted = normalize_text(&converted);
        if converted != token && !alternatives.contains(&converted) {
            alternatives.push(converted);
        }
    }
    alternatives
}

/// Cheap pre-filter: true when the two words could possibly be within the
/// edit-distance budget given only their lengths.
fn length_in_range(left: &[char], right: &[char]) -> bool {
    left.len().abs_diff(right.len()) <= MAX_EDIT_DISTANCE
}

/// Collects stem candidates for a word: the word itself plus every form left
/// after stripping one of the common Arabic clitic prefixes, applied
/// iteratively ("والجامتة" → "الجامتة" → "جامتة"). A stem keeps at least
/// three characters so stripping never produces short junk.
fn candidate_stems(chars: &[char]) -> Vec<Vec<char>> {
    let mut stems = vec![chars.to_vec()];
    let mut frontier = vec![chars.to_vec()];
    for _ in 0..MAX_PREFIX_ROUNDS {
        let mut next = Vec::new();
        for current in &frontier {
            for prefix in ARABIC_PREFIXES {
                let prefix_chars: Vec<char> = prefix.chars().collect();
                if current.len() > prefix_chars.len() + 2 && current.starts_with(&prefix_chars) {
                    let stem = current[prefix_chars.len()..].to_vec();
                    if !stems.contains(&stem) {
                        stems.push(stem.clone());
                        next.push(stem);
                    }
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }
    stems
}

/// Letters Arabic writers interchange by spelling habit, not by mistake: the
/// final "ة" and its everyday "ه" replacement cost nothing to match.
fn same_letter(left: char, right: char) -> bool {
    left == right || matches!((left, right), ('ة', 'ه') | ('ه', 'ة'))
}

/// Optimal-string-alignment Damerau-Levenshtein distance with a cutoff.
/// Returns `max + 1` as soon as the true distance provably exceeds `max`.
fn bounded_edit_distance(left: &[char], right: &[char], max: usize) -> usize {
    if left.len().abs_diff(right.len()) > max {
        return max + 1;
    }
    let mut previous_previous: Vec<usize> = vec![0; right.len() + 1];
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    let mut current: Vec<usize> = vec![0; right.len() + 1];
    for (i, left_char) in left.iter().enumerate() {
        current[0] = i + 1;
        let mut row_minimum = current[0];
        for (j, right_char) in right.iter().enumerate() {
            let mut value = (previous[j + 1] + 1)
                .min(current[j] + 1)
                .min(previous[j] + usize::from(!same_letter(*left_char, *right_char)));
            if i > 0
                && j > 0
                && same_letter(left[i - 1], *right_char)
                && same_letter(*left_char, right[j - 1])
            {
                value = value.min(previous_previous[j - 1] + 1);
            }
            current[j + 1] = value;
            row_minimum = row_minimum.min(value);
        }
        if row_minimum > max {
            return max + 1;
        }
        std::mem::swap(&mut previous_previous, &mut previous);
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checker() -> SpellChecker {
        SpellChecker::new([
            ("الجامعة".to_owned(), 12),
            ("الجامعات".to_owned(), 20),
            ("الكلية".to_owned(), 8),
            ("برمجة".to_owned(), 15),
            ("مدرسة".to_owned(), 9),
            ("شبكة".to_owned(), 7),
            ("ا".to_owned(), 5),
            ("2024".to_owned(), 30),
            ("نادرة".to_owned(), 1),
        ])
    }

    #[test]
    fn corrects_arabic_typos_toward_the_most_frequent_word() {
        assert_eq!(checker().did_you_mean("الجامتة"), Some("الجامعة".to_owned()));
    }

    #[test]
    fn corrects_english_typos() {
        let checker = SpellChecker::new([("programming".to_owned(), 10), ("rust".to_owned(), 30)]);
        assert_eq!(checker.did_you_mean("programing"), Some("programming".to_owned()));
    }

    #[test]
    fn known_words_and_short_tokens_stay_untouched() {
        assert_eq!(checker().did_you_mean("الجامعة برمجة"), None);
        assert_eq!(checker().did_you_mean("سفر"), None);
        assert_eq!(checker().did_you_mean("1234"), None);
        assert_eq!(checker().did_you_mean(""), None);
    }

    #[test]
    fn frequency_breaks_distance_ties_deterministically() {
        let checker = SpellChecker::new([("test".to_owned(), 5), ("tent".to_owned(), 9)]);
        assert_eq!(checker.did_you_mean("tezt"), Some("tent".to_owned()));
    }

    #[test]
    fn mixed_queries_keep_their_unknown_tokens() {
        assert_eq!(
            checker().did_you_mean("الجامتة في القاهرة"),
            Some("الجامعة في القاهرة".to_owned())
        );
    }

    #[test]
    fn the_definite_article_does_not_block_matching() {
        let checker = SpellChecker::new([("جامعة".to_owned(), 10)]);
        assert_eq!(checker.did_you_mean("الجامتة"), Some("جامعة".to_owned()));
    }

    #[test]
    fn fix_token_answers_only_unknown_misspelled_tokens() {
        assert_eq!(checker().fix_token("الجامعة"), None);
        assert_eq!(checker().fix_token("الجامتة"), Some("الجامعة".to_owned()));
        assert_eq!(checker().fix_token("في"), None);
    }

    #[test]
    fn vocabulary_filters_junk_terms() {
        assert_eq!(checker().vocabulary_size(), 6);
        assert_eq!(SpellChecker::new(Vec::new()).vocabulary_size(), 0);
    }

    #[test]
    fn closer_candidate_beats_more_frequent_one() {
        // "شبكتة" is distance 1 from "شبكة" and distance 2 from "شبكات";
        // distance must win even though "شبكات" is far more frequent.
        let checker = SpellChecker::new([("شبكة".to_owned(), 3), ("شبكات".to_owned(), 100)]);
        assert_eq!(checker.did_you_mean("شبكتة"), Some("شبكة".to_owned()));
    }

    #[test]
    fn correction_is_independent_of_vocabulary_order() {
        let forward = SpellChecker::new([("test".to_owned(), 7u64), ("tent".to_owned(), 7)]);
        let reversed = SpellChecker::new([("tent".to_owned(), 7), ("test".to_owned(), 7)]);
        // Equal distance, equal frequency: the lexicographic tie-break
        // decides, identically for both insertion orders.
        assert_eq!(forward.did_you_mean("tezt"), Some("tent".to_owned()));
        assert_eq!(forward.did_you_mean("tezt"), reversed.did_you_mean("tezt"));
    }

    #[test]
    fn prefixed_definite_article_typos_still_find_the_stem() {
        let checker = SpellChecker::new([("جامعة".to_owned(), 10)]);
        assert_eq!(checker.did_you_mean("والجامتة"), Some("جامعة".to_owned()));
        assert_eq!(checker.did_you_mean("بالجامتة"), Some("جامعة".to_owned()));
        assert_eq!(checker.did_you_mean("للجامتة"), Some("جامعة".to_owned()));
    }

    #[test]
    fn latin_keys_typing_arabic_still_finds_the_word() {
        let checker = SpellChecker::new([("مصر".to_owned(), 30), ("مدرسة".to_owned(), 12)]);
        assert_eq!(checker.did_you_mean("lwv"), Some("مصر".to_owned()));
        assert_eq!(checker.did_you_mean("l]vsm"), Some("مدرسة".to_owned()));
    }

    #[test]
    fn arabic_keys_typing_english_still_finds_the_word() {
        let checker = SpellChecker::new([("hello".to_owned(), 30)]);
        assert_eq!(checker.did_you_mean("اثممخ"), Some("hello".to_owned()));
    }

    #[test]
    fn taa_marbuta_and_haa_are_one_spelling() {
        let checker = SpellChecker::new([("مدرسة".to_owned(), 10)]);
        assert_eq!(checker.did_you_mean("مدرسه"), Some("مدرسة".to_owned()));
        assert_eq!(checker.did_you_mean("مدرسة"), None);
    }
}
