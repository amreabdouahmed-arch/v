use anyhow::{anyhow, Context, Result};
use url::Url;

/// Single source of truth: this delegates to the ported engine's
/// implementation so the query path and the index path can never drift.
pub(crate) fn normalize_text(text: &str) -> String {
    crate::ported::normalize_text(text)
}

pub(crate) fn canonicalize_url(raw_url: &str) -> Result<String> {
    let mut url =
        Url::parse(raw_url.trim()).context("Only absolute http/https URLs can be indexed")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(anyhow!("Only absolute http/https URLs can be indexed"));
    }

    let mut query: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| {
            let key = key.to_ascii_lowercase();
            !key.starts_with("utm_") && key != "fbclid" && key != "gclid"
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    query.sort();
    url.set_fragment(None);
    url.set_query(None);
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query);
    }
    Ok(url.to_string())
}

pub(crate) fn make_snippet(text: &str, query: &str, max_len: usize) -> String {
    let source = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let chars: Vec<char> = source.chars().collect();
    if chars.len() <= max_len {
        return source;
    }
    let normalized = normalize_text(&source);
    let terms = normalize_text(query);
    let normalized_chars: Vec<char> = normalized.chars().collect();
    let positions: Vec<usize> = terms
        .split_whitespace()
        .filter_map(|term| {
            let term: Vec<char> = term.chars().collect();
            normalized_chars
                .windows(term.len())
                .position(|window| window == term.as_slice())
        })
        .collect();
    let start = positions
        .iter()
        .min()
        .copied()
        .unwrap_or(0)
        .saturating_sub(75)
        .min(chars.len().saturating_sub(max_len));
    let end = (start + max_len).min(chars.len());
    let snippet = chars[start..end].iter().collect::<String>();
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        snippet,
        if end < chars.len() { "…" } else { "" }
    )
}

pub(crate) fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_arabic_and_latin_queries() {
        assert_eq!(normalize_text("مُحَرِّك بحث VELO"), "محرك بحث velo");
    }

    #[test]
    fn canonicalizes_urls_without_tracking_parameters() {
        assert_eq!(
            canonicalize_url("HTTPS://Example.com:443/a?utm_source=x&b=2&a=1#section").unwrap(),
            "https://example.com/a?a=1&b=2"
        );
        assert!(canonicalize_url("javascript:alert(1)").is_err());
    }

    #[test]
    fn snippets_are_bounded_and_highlight_near_matches() {
        let text = format!("{}matching phrase{}", "x".repeat(100), "y".repeat(200));
        let snippet = make_snippet(&text, "matching phrase", 220);
        assert!(snippet.len() < 230);
        assert!(snippet.contains("matching phrase"));
        assert!(snippet.starts_with('…'));
        assert!(snippet.ends_with('…'));
    }
}
