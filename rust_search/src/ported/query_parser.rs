use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuery {
    pub original: String,
    pub language: String,
    pub tokens: Vec<String>,
    pub keywords: Vec<String>,
    pub stemmed: Vec<String>,
    pub intent: String,
    pub word_count: usize,
    pub unique_words: usize,
}

pub struct ArabicStemmer;
pub struct EnglishStemmer;
pub struct StopWords;
pub struct QueryParser;

const ARABIC_STOPWORDS: &[&str] = &[
    "في",
    "من",
    "إلى",
    "على",
    "هذا",
    "هذه",
    "التي",
    "الذي",
    "و",
    "أو",
    "هو",
    "هي",
    "هم",
    "هن",
    "أنا",
    "نحن",
    "أنت",
    "أنتم",
    "كان",
    "يكون",
    "أن",
    "عن",
    "مع",
    "بعد",
    "قبل",
    "إذا",
    "ما",
    "لا",
    "كل",
    "أي",
    "لم",
    "قد",
    "بين",
    "أيضا",
    "أيضاً",
    "ثم",
    "حتى",
    "عند",
    "عليه",
    "عليها",
    "بعض",
    "كثير",
    "كثيرة",
    "جميع",
    "غير",
    "خلال",
    "حول",
    "بها",
    "به",
    "اللذين",
];
const ENGLISH_STOPWORDS: &[&str] = &[
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might", "must", "shall",
    "can", "need", "dare", "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by",
    "from", "as", "into", "through", "during", "before", "after", "above", "below", "up", "down",
    "out", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when",
    "where", "why", "how", "all", "each", "few", "more", "most", "other", "some", "such", "no",
    "nor", "not", "only", "own", "same", "so", "than", "too", "very", "just", "and", "but", "if",
    "or", "because", "until", "while", "this", "that", "these", "those",
];

fn is_arabic(c: char) -> bool {
    ('\u{0600}'..='\u{06ff}').contains(&c)
}

impl ArabicStemmer {
    pub fn stem(word: &str) -> String {
        let mut word = word.trim().to_owned();
        for prefix in ["ال", "وال", "بال", "فال", "لل", "ف", "و", "ب", "ل", "ك"] {
            if word.starts_with(prefix) && word.chars().count() > prefix.chars().count() + 2 {
                word = word[prefix.len()..].to_owned();
                break;
            }
        }
        let mut suffixes = [
            "ات", "ون", "ين", "ان", "تم", "تن", "ته", "نا", "كم", "هم", "ية", "ي", "ة", "ا", "ن",
            "ت", "وا", "ها", "ك", "ه",
        ];
        suffixes.sort_by_key(|s| std::cmp::Reverse(s.len()));
        for suffix in suffixes {
            if word.ends_with(suffix) && word.chars().count() > suffix.chars().count() + 2 {
                word.truncate(word.len() - suffix.len());
                break;
            }
        }
        word
    }
}

impl EnglishStemmer {
    pub fn stem(word: &str) -> String {
        let mut word = word.trim().to_lowercase();
        for suffix in ["ing", "ies", "ied", "ly", "ed", "s"] {
            if word.ends_with(suffix) && word.chars().count() > suffix.chars().count() + 2 {
                word.truncate(word.len() - suffix.len());
                if suffix == "ies" || suffix == "ied" {
                    word.push('y');
                }
                break;
            }
        }
        word
    }
}

impl StopWords {
    pub fn is_stopword(word: &str, language: &str) -> bool {
        let word = word.trim().to_lowercase();
        if language == "ar" || word.chars().any(is_arabic) {
            ARABIC_STOPWORDS.contains(&word.as_str())
        } else {
            ENGLISH_STOPWORDS.contains(&word.as_str())
        }
    }

    pub fn remove_stopwords(words: &[String]) -> Vec<String> {
        words
            .iter()
            .filter(|w| !Self::is_stopword(w, "auto"))
            .cloned()
            .collect()
    }
}

impl QueryParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, query: &str) -> ParsedQuery {
        Self::parse_query(query)
    }

    pub fn expand(&self, parsed: &ParsedQuery) -> Vec<String> {
        Self::expand_query(parsed)
    }

    pub fn detect_language(text: &str) -> &'static str {
        let total = text.chars().filter(|c| c.is_alphabetic()).count();
        if total == 0 {
            return "en";
        }
        let arabic = text.chars().filter(|c| is_arabic(*c)).count();
        if arabic as f64 / total as f64 > 0.3 {
            "ar"
        } else {
            "en"
        }
    }

    pub fn tokenize(text: &str) -> Vec<String> {
        let mut words = Vec::new();
        let mut word = String::new();
        for c in text.chars() {
            if c.is_alphanumeric() || c == '_' || is_arabic(c) {
                word.extend(c.to_lowercase());
            } else if !word.is_empty() {
                if word.chars().count() > 1 {
                    words.push(std::mem::take(&mut word));
                } else {
                    word.clear();
                }
            }
        }
        if word.chars().count() > 1 {
            words.push(word);
        }
        words
    }

    pub fn stem_words(words: &[String], language: &str) -> Vec<String> {
        words
            .iter()
            .map(|w| {
                if language == "ar" || w.chars().any(is_arabic) {
                    ArabicStemmer::stem(w)
                } else {
                    EnglishStemmer::stem(w)
                }
            })
            .collect()
    }

    pub fn parse_query(query: &str) -> ParsedQuery {
        let language = Self::detect_language(query).to_owned();
        let tokens = Self::tokenize(query);
        let keywords = StopWords::remove_stopwords(&tokens);
        let stemmed = Self::stem_words(&keywords, &language);
        let intent = Self::detect_intent(query, &language).to_owned();
        let unique_words = tokens.iter().collect::<HashSet<_>>().len();
        ParsedQuery {
            original: query.to_owned(),
            language,
            word_count: tokens.len(),
            unique_words,
            tokens,
            keywords,
            stemmed,
            intent,
        }
    }

    pub fn detect_intent(query: &str, language: &str) -> &'static str {
        let lower = query.to_lowercase();
        let terms: &[&str] = if language == "ar" {
            &[
                "ما هي",
                "ما هو",
                "كيف",
                "لماذا",
                "متى",
                "أين",
                "من",
                "شرح",
                "تعريف",
            ]
        } else {
            &[
                "what is",
                "how to",
                "why",
                "when",
                "where",
                "who",
                "explain",
                "definition",
            ]
        };
        if terms.iter().any(|term| lower.contains(term)) {
            return "informational";
        }
        if query.contains("http") || query.contains(".com") || query.contains(".org") {
            return "navigational";
        }
        "general"
    }

    pub fn expand_query(parsed: &ParsedQuery) -> Vec<String> {
        let mut expanded = HashSet::new();
        expanded.extend(parsed.stemmed.iter().cloned());
        expanded.extend(parsed.keywords.iter().cloned());
        expanded.into_iter().collect()
    }
}

impl Default for QueryParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_english_intent_and_filters_stopwords() {
        let parsed = QueryParser::parse_query("what is HTML and how to use it");
        assert_eq!(parsed.language, "en");
        assert_eq!(parsed.intent, "informational");
        assert_eq!(
            parsed.tokens,
            ["what", "is", "html", "and", "how", "to", "use", "it"]
        );
        assert_eq!(parsed.keywords, ["what", "html", "use", "it"]);
    }

    #[test]
    fn detects_arabic_and_navigation() {
        let parsed = QueryParser::parse_query("كيف أتعلم البرمجة");
        assert_eq!(parsed.language, "ar");
        assert_eq!(parsed.intent, "informational");
        assert_eq!(
            QueryParser::detect_intent("example.com", "en"),
            "navigational"
        );
    }
}
