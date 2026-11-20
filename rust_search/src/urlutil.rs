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
    // The authority ends at the first '/', '?' or '#'. Splitting on '/' alone
    // would fold a query or a fragment into the host.
    let split_at = without_scheme
        .find(&['/', '?', '#'][..])
        .unwrap_or(without_scheme.len());
    let authority = &without_scheme[..split_at];
    let rest = &without_scheme[split_at..];

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

    // Drop the fragment first, then split the query off the path.
    let path_and_query = match rest.split_once('#') {
        Some((before, _)) => before,
        None => rest,
    };
    let (path, query) = match path_and_query.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path_and_query, None),
    };

    // Collapse every trailing '/', but keep the bare-host root.
    let mut base = path;
    while base.len() > 1 && base.ends_with('/') {
        base = &base[..base.len() - 1];
    }
    let base = if base.is_empty() { "/" } else { base };

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

    #[test]
    fn d1_trailing_slash_is_collapsed_except_at_root() {
        assert_eq!(canonical_url("http://example.com/a/b/"), "example.com/a/b");
        assert_eq!(canonical_url("http://example.com/"), "example.com/");
        assert_eq!(canonical_url("https://example.com"), "example.com/");
    }

    #[test]
    fn d6_authority_ends_at_query_or_fragment_delimiter() {
        assert_eq!(canonical_url("https://example.com?a=1"), "example.com/?a=1");
        assert_eq!(canonical_url("https://example.com#frag"), "example.com/");
    }

    #[test]
    fn canonical_url_is_idempotent() {
        let inputs = [
            "https://example.com",
            "http://example.com/",
            "https://www.Example.com/a/b/",
            "https://example.com/a?b=2&a=1",
            "https://example.com/a?utm_source=x&id=7#top",
            "http://example.com/a/b/",
        ];
        for input in inputs {
            let once = canonical_url(input);
            let twice = canonical_url(&once);
            assert_eq!(once, twice, "not idempotent for {input}");
        }
    }

    #[test]
    fn canonical_url_output_has_no_scheme_fragment_or_userinfo() {
        let inputs = [
            "https://user:pw@www.Example.com:8443/a/b/?utm_source=x#frag",
            "http://example.com",
            "https://example.com?a=1",
            "https://example.com#frag",
        ];
        for input in inputs {
            let output = canonical_url(input);
            assert!(!output.contains("://"), "scheme leaked for {input}: {output}");
            assert!(!output.contains('#'), "fragment leaked for {input}: {output}");
            assert!(!output.contains('@'), "userinfo leaked for {input}: {output}");
        }
    }

    #[test]
    fn equivalent_urls_share_one_key() {
        let left = canonical_url("https://example.com/a?b=2&a=1#f");
        let right = canonical_url("http://www.example.com/a?a=1&b=2");
        assert_eq!(left, right);
        assert_eq!(left, "example.com/a?a=1&b=2");
    }
}