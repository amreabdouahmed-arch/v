use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use mysql_async::{OptsBuilder, Pool, SslOpts, Value};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use url::Url;

pub const DOCUMENTS_TABLE: &str = "search_documents";
pub const QUEUE_TABLE: &str = "crawl_queue";

#[derive(Debug, Clone, Default)]
pub struct Page {
    pub url: String,
    pub title: String,
    pub content: String,
    pub description: String,
    pub language: String,
    pub status_code: i16,
    pub crawled_at: Option<f64>,
    pub source_metadata_json: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct LegacyPage {
    pub url: String,
    pub title: String,
    pub content: String,
    pub description: String,
    pub language: String,
    pub status_code: i16,
    pub crawled_at: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct TiDbSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl TiDbSettings {
    pub fn from_env() -> Result<Self> {
        let host = required_env("TIDB_HOST")?;
        let user = required_env("TIDB_USER")?;
        let password = required_env("TIDB_PASSWORD")?;
        let port = env::var("TIDB_PORT")
            .unwrap_or_else(|_| "4000".into())
            .parse::<u16>()
            .context("TIDB_PORT must be a valid port")?;
        let database = env::var("TIDB_DATABASE")
            .or_else(|_| env::var("VELO_TIDB_DATABASE"))
            .unwrap_or_else(|_| "velo_search".into());
        if !valid_identifier(&database) {
            bail!("TIDB_DATABASE may contain only letters, numbers, and underscores");
        }
        Ok(Self {
            host,
            port,
            user,
            password,
            database,
        })
    }

    pub fn pool(&self) -> Pool {
        let mut ssl = SslOpts::default();
        if let Ok(ca) = env::var("TIDB_SSL_CA") {
            ssl = ssl.with_root_certs(vec![std::path::PathBuf::from(ca).into()]);
        }
        Pool::new(
            OptsBuilder::default()
                .ip_or_hostname(self.host.clone())
                .tcp_port(self.port)
                .user(Some(self.user.clone()))
                .pass(Some(self.password.clone()))
                .db_name(Some(self.database.clone()))
                .ssl_opts(Some(ssl)),
        )
    }
}

pub fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("Set {name} in the environment"))?;
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

pub fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn canonicalize_url(raw: &str) -> Result<String> {
    let mut url = Url::parse(raw.trim()).context("invalid URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("only absolute HTTP(S) URLs are supported");
    }
    url.set_fragment(None);
    if (url.scheme() == "http" && url.port() == Some(80))
        || (url.scheme() == "https" && url.port() == Some(443))
    {
        url.set_port(None)
            .map_err(|_| anyhow::anyhow!("invalid URL port"))?;
    }
    if url.path().is_empty() {
        url.set_path("/");
    }
    let mut pairs = url
        .query_pairs()
        .filter(|(key, _)| {
            !key.to_ascii_lowercase().starts_with("utm_")
                && key.to_ascii_lowercase() != "fbclid"
                && key.to_ascii_lowercase() != "gclid"
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    pairs.sort();
    url.set_query(None);
    if !pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(pairs);
    }
    Ok(url.to_string())
}

pub fn normalize_text(input: &str) -> String {
    input
        .nfkc()
        .flat_map(char::to_lowercase)
        .filter_map(|ch| match ch {
            '\u{064b}'..='\u{065f}' | '\u{0670}' | '\u{0640}' => None,
            'إ' | 'أ' | 'آ' | 'ٱ' => Some('ا'),
            'ى' => Some('ي'),
            'ؤ' => Some('و'),
            'ئ' => Some('ي'),
            c if c.is_alphanumeric() || c == '_' || c.is_whitespace() => Some(c),
            _ => Some(' '),
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn page_hash(canonical_url: &str) -> [u8; 32] {
    Sha256::digest(canonical_url.as_bytes()).into()
}

pub fn text_value(value: &str) -> Value {
    Value::Bytes(value.as_bytes().to_vec())
}

pub fn page_values(page: &Page) -> Result<Vec<Value>> {
    let canonical = canonicalize_url(&page.url)?;
    let hash = page_hash(&canonical);
    let host = Url::parse(&canonical)?.host_str().unwrap_or("").to_owned();
    let title = page.title.chars().take(1000).collect::<String>();
    let description = page.description.chars().take(5000).collect::<String>();
    let content = page.content.chars().take(1_000_000).collect::<String>();
    let normalized = normalize_text(&format!("{title} {description} {content}"));
    let digest = hex_string(&Sha256::digest(normalized.as_bytes()));
    let search_text = normalize_text(&format!("{title} {title} {title} {description} {content}"))
        .chars()
        .take(1_010_000)
        .collect::<String>();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    Ok(vec![
        Value::Bytes(hash.to_vec()),
        text_value(&canonical),
        text_value(&canonical),
        text_value(&host),
        text_value(&title),
        text_value(&description),
        text_value(&content),
        text_value(&search_text),
        text_value(&page.language.chars().take(20).collect::<String>()),
        Value::Int(if page.status_code == 0 {
            200
        } else {
            page.status_code as i64
        }),
        Value::Double(page.crawled_at.unwrap_or(now)),
        text_value(&digest),
        page.source_metadata_json
            .as_ref()
            .map(|s| text_value(s))
            .unwrap_or(Value::NULL),
    ])
}

pub fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub async fn upsert_tidb_pages(pool: &Pool, pages: &[Page]) -> Result<usize> {
    use mysql_async::prelude::Queryable;
    let statement = format!(
        "INSERT INTO `{DOCUMENTS_TABLE}` \
         (url_hash,url,canonical_url,host,title,description,content,search_text,language,status_code,crawled_at,content_hash,source_metadata_json) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE \
         url=VALUES(url),canonical_url=VALUES(canonical_url),host=VALUES(host),title=VALUES(title),\
         description=VALUES(description),content=VALUES(content),search_text=VALUES(search_text),\
         language=VALUES(language),status_code=VALUES(status_code),crawled_at=VALUES(crawled_at),\
         content_hash=VALUES(content_hash),source_metadata_json=COALESCE(VALUES(source_metadata_json),source_metadata_json)"
    );
    let mut connection = pool.get_conn().await.context("connecting to TiDB Cloud")?;
    let mut written = 0;
    for page in pages {
        let values = page_values(page)?;
        connection
            .exec_drop(&statement, mysql_async::Params::Positional(values))
            .await?;
        written += 1;
    }
    connection.query_drop("COMMIT").await?;
    Ok(written)
}

pub async fn insert_ignore_tidb_pages(pool: &Pool, pages: &[Page]) -> Result<(usize, usize)> {
    use mysql_async::prelude::Queryable;
    let statement = format!(
        "INSERT IGNORE INTO `{DOCUMENTS_TABLE}` \
         (url_hash,url,canonical_url,host,title,description,content,search_text,language,status_code,crawled_at,content_hash,source_metadata_json) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    );
    let mut connection = pool.get_conn().await?;
    let mut inserted = 0;
    let mut skipped = 0;
    for page in pages {
        let Ok(values) = page_values(page) else {
            skipped += 1;
            continue;
        };
        connection
            .exec_drop(&statement, mysql_async::Params::Positional(values))
            .await?;
        let changed: Option<i64> = connection.query_first("SELECT ROW_COUNT()").await?;
        if changed.unwrap_or_default() > 0 {
            inserted += 1;
        } else {
            skipped += 1;
        }
    }
    connection.query_drop("COMMIT").await?;
    Ok((inserted, skipped))
}

pub async fn initialize_search_schema(pool: &Pool) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let mut connection = pool.get_conn().await?;
    connection.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS `{DOCUMENTS_TABLE}` (\
         url_hash BINARY(32) NOT NULL,url TEXT NOT NULL,canonical_url TEXT NOT NULL,\
         host VARCHAR(255) NOT NULL DEFAULT '',title TEXT NOT NULL,description TEXT NOT NULL,\
         content MEDIUMTEXT NOT NULL,search_text MEDIUMTEXT NOT NULL,language VARCHAR(20) NOT NULL DEFAULT '',\
         status_code SMALLINT NOT NULL DEFAULT 200,crawled_at DOUBLE NOT NULL,\
         content_hash CHAR(64) NOT NULL DEFAULT '',source_metadata_json MEDIUMTEXT NULL,\
         pagerank DOUBLE NOT NULL DEFAULT 0,\
         PRIMARY KEY (url_hash),KEY search_host_idx (host)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    )).await?;
    let columns: Vec<mysql_async::Row> = connection
        .query(format!("SHOW COLUMNS FROM `{DOCUMENTS_TABLE}`"))
        .await?;
    if !columns
        .iter()
        .any(|row| row.get::<String, _>(0).as_deref() == Some("source_metadata_json"))
    {
        connection
            .query_drop(format!(
                "ALTER TABLE `{DOCUMENTS_TABLE}` ADD COLUMN source_metadata_json MEDIUMTEXT NULL"
            ))
            .await?;
    }
    if !columns
        .iter()
        .any(|row| row.get::<String, _>(0).as_deref() == Some("pagerank"))
    {
        connection
            .query_drop(format!(
                "ALTER TABLE `{DOCUMENTS_TABLE}` ADD COLUMN pagerank DOUBLE NOT NULL DEFAULT 0"
            ))
            .await?;
    }
    connection
        .query_drop(format!(
            "CREATE TABLE IF NOT EXISTS `{QUEUE_TABLE}` (\
         url_hash BINARY(32) NOT NULL,url TEXT NOT NULL,depth INT NOT NULL DEFAULT 0,\
         status VARCHAR(20) NOT NULL DEFAULT 'queued',attempts INT NOT NULL DEFAULT 0,\
         next_attempt DOUBLE NOT NULL DEFAULT 0,locked_at DOUBLE NOT NULL DEFAULT 0,\
         last_error VARCHAR(1000) NOT NULL DEFAULT '',PRIMARY KEY (url_hash),\
         KEY queue_ready_idx (status,next_attempt,depth),KEY queue_lease_idx (status,locked_at))\
         ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        ))
        .await?;
    connection.query_drop("COMMIT").await?;
    Ok(())
}

pub async fn ensure_text_index(pool: &Pool) -> Result<bool> {
    use mysql_async::prelude::Queryable;
    let mut connection = pool.get_conn().await?;
    let indexes: Vec<mysql_async::Row> = connection
        .query(format!("SHOW INDEX FROM `{DOCUMENTS_TABLE}`"))
        .await?;
    if indexes
        .iter()
        .any(|row| row.get::<String, _>(2).as_deref() == Some("ft_search_text"))
    {
        return Ok(true);
    }
    match connection
        .query_drop(format!(
            "ALTER TABLE `{DOCUMENTS_TABLE}` ADD FULLTEXT INDEX ft_search_text \
         (search_text) WITH PARSER MULTILINGUAL ADD_COLUMNAR_REPLICA_ON_DEMAND"
        ))
        .await
    {
        Ok(()) => {
            connection.query_drop("COMMIT").await?;
            Ok(true)
        }
        Err(error) => {
            connection.query_drop("ROLLBACK").await?;
            eprintln!(
                "TiDB FULLTEXT index is unavailable ({error}); search will use the fallback query"
            );
            Ok(false)
        }
    }
}

pub async fn missing_page_hashes(pool: &Pool, pages: &[Page]) -> Result<Vec<Page>> {
    use mysql_async::prelude::Queryable;
    let mut keys = Vec::new();
    let mut candidates = Vec::new();
    for page in pages {
        if let Ok(canonical) = canonicalize_url(&page.url) {
            let hash = page_hash(&canonical);
            keys.push(Value::Bytes(hash.to_vec()));
            candidates.push((page.clone(), hash));
        }
    }
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut connection = pool.get_conn().await?;
    let mut found = std::collections::HashSet::<Vec<u8>>::new();
    for batch in keys.chunks(400) {
        let placeholders = vec!["?"; batch.len()].join(",");
        let query =
            format!("SELECT url_hash FROM `{DOCUMENTS_TABLE}` WHERE url_hash IN ({placeholders})");
        let rows: Vec<Vec<u8>> = connection
            .exec(query, mysql_async::Params::Positional(batch.to_vec()))
            .await?;
        found.extend(rows);
    }
    Ok(candidates
        .into_iter()
        .filter(|(_, key)| !found.contains(key.as_slice()))
        .map(|(page, _)| page)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_database_names() {
        assert!(valid_identifier("velo_search_2"));
        assert!(!valid_identifier("velo-search"));
        assert!(!valid_identifier("x; DROP TABLE"));
        assert!(!valid_identifier(""));
    }

    #[test]
    fn canonicalizes_and_hashes_stable_http_urls() {
        let first = canonicalize_url("HTTPS://Example.COM:443/a#fragment").unwrap();
        assert_eq!(first, "https://example.com/a");
        assert_eq!(page_hash(&first), page_hash(&first));
    }
}
