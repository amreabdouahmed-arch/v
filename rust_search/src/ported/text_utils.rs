//! Shared normalization used by query parsing and cloud-backed search adapters.
use unicode_normalization::UnicodeNormalization;

/// Normalize Arabic orthography and Latin case consistently for indexing and queries.
///
/// NFKC is applied before case folding, matching the legacy utility's Unicode normalization.
pub fn normalize_text(text: &str) -> String {
    let mut normalized = String::new();
    let nfkc: String = text.nfkc().collect();
    for mut c in nfkc.chars().flat_map(char::to_lowercase) {
        if matches!(c, '\u{064b}'..='\u{065f}' | '\u{0670}' | '\u{0640}') {
            continue;
        }
        c = match c {
            'إ' | 'أ' | 'آ' | 'ٱ' => 'ا',
            'ى' => 'ي',
            'ؤ' => 'و',
            'ئ' => 'ي',
            'ß' | 'ẞ' => {
                normalized.push_str("ss");
                continue;
            }
            'ς' => 'σ',
            _ => c,
        };
        if c.is_alphanumeric() || c == '_' || c.is_whitespace() {
            normalized.push(c);
        } else {
            normalized.push(' ');
        }
    }
    normalized.trim().to_owned()
}

/// Split normalized query text on Unicode whitespace.
pub fn tokenize(text: &str) -> Vec<String> {
    normalize_text(text)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Create the query-centered snippet used by the legacy local search helpers.
pub fn snippet(text: &str, query: &str, max_len: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let chars: Vec<char> = compact.chars().collect();
    let normalized: Vec<char> = normalize_text(&compact).chars().collect();
    let terms = tokenize(query);
    let mut positions = Vec::new();
    for term in terms {
        let term: Vec<char> = term.chars().collect();
        if term.is_empty() || term.len() > normalized.len() {
            continue;
        }
        if let Some(position) = normalized
            .windows(term.len())
            .position(|window| window == term)
        {
            positions.push(position);
        }
    }
    if positions.is_empty() {
        let end = max_len.min(chars.len());
        return format!(
            "{}{}",
            chars[..end].iter().collect::<String>(),
            if chars.len() > max_len { "…" } else { "" }
        );
    }
    let start = positions.into_iter().min().unwrap_or(0).saturating_sub(75);
    let end = chars.len().min(start.saturating_add(max_len));
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        chars[start..end].iter().collect::<String>(),
        if end < chars.len() { "…" } else { "" }
    )
}

/// Convenience wrapper using the legacy default snippet length (220 codepoints).
pub fn default_snippet(text: &str, query: &str) -> String {
    snippet(text, query, 220)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_arabic_marks_and_orthography() {
        assert_eq!(normalize_text("إِبْرَاهِيم ى ؤ ئ"), "ابراهيم ي و ي");
        assert_eq!(tokenize("A,B  الْعَرَبِيَّة"), ["a", "b", "العربية"]);
    }

    #[test]
    fn snippets_collapse_space_and_mark_truncation() {
        assert_eq!(snippet("one   two three", "two", 7), "one two…");
        assert_eq!(snippet("unrelated", "missing", 3), "unr…");
    }
}
