//! Canonical URL utilities.
//!
//! Provides the single shared `canonical_url` function used by the service,
//! the eval harness and any reindex tooling, so that URL normalization has
//! exactly one definition.

/// Canonical URL used for both labels and results.
///
/// Rules: lowercase host, drop `www.`, drop scheme, drop fragment, drop
/// `utm_*`/`fbclid`/`gclid` parameters, sort the remaining query parameters,
/// and collapse a trailing slash (except for the bare host).
pub fn canonical_url(raw: &str) -> String {
    let trimmed = raw.trim();
    let without_scheme = match trimmed.split_once("://") {
        Some((_, rest)) => rest,
        None => trimmed,
    };
    let (authority, mut path) = match without_scheme.split_once('/') {
        Some((authority, rest)) => (authority, format!("/{rest}")),
        None => (without_scheme, String::new()),
    };
    let host_port = match authority.rsplit_once('@') {
        Some((_, host)) => host,
        None => authority,
    };
    let host = host_port
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host).to_string();

    if let Some((before, _)) = path.split_once('#') {
        path = before.to_string();
    }
    let (base, query) = match path.split_once('?') {
        Some((base, query)) => (base.to_string(), Some(query.to_string())),
        None => (path, None),
    };
    let base = if base.is_empty() {
        "/".to_string()
    } else {
        base
    };

    let mut kept: Vec<String> = match query {
        Some(query) => query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .filter(|pair| {
                let key = match pair.split_once('=') {
                    Some((key, _)) => key,
                    None => pair,
                }
                .to_ascii_lowercase();
                !key.starts_with("utm_") && key != "fbclid" && key != "gclid"
            })
            .map(str::to_string)
            .collect(),
        None => Vec::new(),
    };
    kept.sort();
    if kept.is_empty() {
        format!("{host}{base}")
    } else {
        format!("{host}{base}?{}", kept.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::canonical_url;

    #[test]
    fn strips_scheme_and_www_and_lowercases_host() {
        assert_eq!(
            canonical_url("https://www.Example.COM/Path"),
            "example.com/Path"
        );
    }

    #[test]
    fn drops_fragment_and_utm_parameters() {
        assert_eq!(
            canonical_url("https://example.com/a?utm_source=x&id=7#top"),
            "example.com/a?id=7"
        );
    }

    #[test]
    fn collapses_trailing_slash_but_keeps_root() {
        assert_eq!(canonical_url("http://example.com/"), "example.com/");
        assert_eq!(canonical_url("http://example.com/a/b/"), "example.com/a/b");
    }

    #[test]
    fn sorts_query_parameters() {
        assert_eq!(
            canonical_url("https://example.com/p?b=2&a=1"),
            "example.com/p?a=1&b=2"
        );
    }
}