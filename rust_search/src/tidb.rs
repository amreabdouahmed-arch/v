use std::{env, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use mysql_async::{prelude::Queryable, OptsBuilder, Pool, Row, SslOpts, Value};

use crate::ported::crawler_ingestion::CrawlPage;
use crate::state::{required_env, DOCUMENTS_TABLE, SEARCH_FIELDS, SearchResult};

pub(crate) async fn create_tidb_pool() -> Result<(Pool, bool)> {
    let host = required_env("TIDB_HOST")?;
    let user = required_env("TIDB_USER")?;
    let password = required_env("TIDB_PASSWORD")?;
    let port = env::var("TIDB_PORT")
        .unwrap_or_else(|_| "4000".to_owned())
        .parse::<u16>()
        .context("TIDB_PORT must be a valid port number")?;
    let database = env::var("TIDB_DATABASE")
        .or_else(|_| env::var("VELO_TIDB_DATABASE"))
        .unwrap_or_else(|_| "velo_search".to_owned());
    if !database
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(anyhow!(
            "TIDB_DATABASE may contain only letters, numbers, and underscores"
        ));
    }

    let mut ssl = SslOpts::default();
    if let Ok(ca_path) = env::var("TIDB_SSL_CA") {
        ssl = ssl.with_root_certs(vec![PathBuf::from(ca_path).into()]);
    }
    let bootstrap_pool = Pool::new(
        OptsBuilder::default()
            .ip_or_hostname(host.clone())
            .tcp_port(port)
            .user(Some(user.clone()))
            .pass(Some(password.clone()))
            .db_name(Some("sys"))
            .ssl_opts(Some(ssl.clone())),
    );
    let mut bootstrap = bootstrap_pool
        .get_conn()
        .await
        .context("Could not connect to TiDB Cloud system database")?;
    bootstrap
        .query_drop(format!(
            "CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        ))
        .await
        .context("Could not initialize the TiDB Cloud database")?;
    drop(bootstrap);
    drop(bootstrap_pool);

    let options = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some(database))
        .ssl_opts(Some(ssl));
    let pool = Pool::new(options);
    let mut connection = pool
        .get_conn()
        .await
        .context("Could not connect to TiDB Cloud")?;
    connection
        .query_drop("SELECT 1")
        .await
        .context("Could not verify the TiDB Cloud connection")?;
    drop(connection);
    let fulltext_enabled = ensure_tidb_schema(&pool).await?;
    if !fulltext_enabled {
        eprintln!("TiDB full-text index unavailable; using keyword matching");
    }
    Ok((pool, fulltext_enabled))
}

async fn ensure_tidb_schema(pool: &Pool) -> Result<bool> {
    let mut connection = pool.get_conn().await?;
    connection
        .query_drop(format!(
            "CREATE TABLE IF NOT EXISTS `{DOCUMENTS_TABLE}` (
                url_hash BINARY(32) NOT NULL,
                url TEXT NOT NULL,
                canonical_url TEXT NOT NULL,
                host VARCHAR(255) NOT NULL DEFAULT '',
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                content MEDIUMTEXT NOT NULL,
                search_text MEDIUMTEXT NOT NULL,
                language VARCHAR(20) NOT NULL DEFAULT '',
                status_code SMALLINT NOT NULL DEFAULT 200,
                crawled_at DOUBLE NOT NULL,
                content_hash CHAR(64) NOT NULL DEFAULT '',
                source_metadata_json MEDIUMTEXT NULL,
                pagerank DOUBLE NOT NULL DEFAULT 0,
                PRIMARY KEY (url_hash),
                KEY search_host_idx (host)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        ))
        .await
        .context("Could not initialize TiDB search-document table")?;
    connection
        .query_drop(
            "CREATE TABLE IF NOT EXISTS `crawl_queue` (
                url_hash BINARY(32) NOT NULL,
                url TEXT NOT NULL,
                depth INT NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL DEFAULT 'queued',
                attempts INT NOT NULL DEFAULT 0,
                next_attempt DOUBLE NOT NULL DEFAULT 0,
                locked_at DOUBLE NOT NULL DEFAULT 0,
                last_error VARCHAR(1000) NOT NULL DEFAULT '',
                PRIMARY KEY (url_hash),
                KEY queue_ready_idx (status, next_attempt, depth),
                KEY queue_lease_idx (status, locked_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        )
        .await
        .context("Could not initialize TiDB crawl-queue table")?;
    let columns = connection
        .query::<Row, _>(format!("SHOW COLUMNS FROM `{DOCUMENTS_TABLE}`"))
        .await?;
    if !columns.iter().any(|row| {
        row.get::<Option<String>, _>(0)
            .flatten()
            .is_some_and(|name| name == "source_metadata_json")
    }) {
        connection
            .query_drop(format!(
                "ALTER TABLE `{DOCUMENTS_TABLE}` ADD COLUMN source_metadata_json MEDIUMTEXT NULL"
            ))
            .await
            .context("Could not add TiDB source-metadata column")?;
    }
    if !columns.iter().any(|row| {
        row.get::<Option<String>, _>(0)
            .flatten()
            .is_some_and(|name| name == "pagerank")
    }) {
        connection
            .query_drop(format!(
                "ALTER TABLE `{DOCUMENTS_TABLE}` ADD COLUMN pagerank DOUBLE NOT NULL DEFAULT 0"
            ))
            .await
            .context("Could not add TiDB pagerank column")?;
    }
    let indexes = connection
        .query::<Row, _>(format!("SHOW INDEX FROM `{DOCUMENTS_TABLE}`"))
        .await?;
    if indexes.iter().any(|row| {
        row.get::<Option<String>, _>(2)
            .flatten()
            .is_some_and(|name| name == "ft_search_text")
    }) {
        return Ok(true);
    }
    match connection
        .query_drop(format!(
            "ALTER TABLE `{DOCUMENTS_TABLE}` ADD FULLTEXT INDEX ft_search_text \
             (search_text) WITH PARSER MULTILINGUAL ADD_COLUMNAR_REPLICA_ON_DEMAND"
        ))
        .await
    {
        Ok(()) => Ok(true),
        Err(error) => {
            eprintln!("Could not create TiDB full-text index: {error}");
            Ok(false)
        }
    }
}

fn row_string(row: &Row, field: &str) -> String {
    row.get::<Option<String>, _>(field)
        .flatten()
        .unwrap_or_default()
}

fn row_score(row: &Row) -> f64 {
    row.get::<Option<f64>, _>("score")
        .flatten()
        .unwrap_or_default()
}

fn row_favicon(row: &Row) -> Option<String> {
    let metadata = row_string(row, "source_metadata_json");
    if metadata.is_empty() {
        return None;
    }
    serde_json::from_str::<CrawlPage>(&metadata)
        .ok()
        .and_then(|page| page.favicon)
}

pub(crate) fn mysql_pattern(term: &str) -> String {
    format!(
        "%{}%",
        term.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

/// LIKE pattern anchored at the start, for prefix lookups such as title
/// suggestions. Escapes the LIKE wildcards; the caller binds the result as a
/// parameter, so no SQL injection is possible.
pub(crate) fn mysql_prefix_pattern(term: &str) -> String {
    format!(
        "{}%",
        term.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

fn score_expression(terms: &[&str]) -> (String, Vec<Value>) {
    let mut parts = Vec::with_capacity(terms.len());
    let mut params = Vec::with_capacity(terms.len() * SEARCH_FIELDS.len());
    for term in terms {
        let pattern = mysql_pattern(term);
        parts.push(
            "(CASE WHEN title LIKE ? THEN 5 ELSE 0 END + \
             CASE WHEN description LIKE ? THEN 2 ELSE 0 END + \
             CASE WHEN content LIKE ? THEN 1 ELSE 0 END)",
        );
        for _ in SEARCH_FIELDS {
            params.push(Value::Bytes(pattern.as_bytes().to_vec()));
        }
    }
    (parts.join(" + "), params)
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn tidb_count(pool: Pool, expression: String) -> Result<u64> {
    let mut connection = pool.get_conn().await?;
    let statement = format!("SELECT COUNT(*) FROM `{DOCUMENTS_TABLE}` WHERE {expression}");
    let total = connection
        .exec_first::<u64, _, _>(statement, mysql_async::Params::Empty)
        .await?
        .unwrap_or_default();
    Ok(total)
}

async fn tidb_rows(
    pool: Pool,
    expression: String,
    mut where_params: Vec<Value>,
    terms: Vec<String>,
    limit: usize,
    offset: usize,
) -> Result<Vec<SearchResult>> {
    let term_refs: Vec<&str> = terms.iter().map(String::as_str).collect();
    let (score, mut params) = score_expression(&term_refs);
    params.append(&mut where_params);
    params.push(Value::UInt(limit as u64));
    params.push(Value::UInt(offset as u64));
    let statement = format!(
        "SELECT url,title,description,content,source_metadata_json,CAST(({score}) AS DOUBLE) AS score \
         FROM `{DOCUMENTS_TABLE}` WHERE {expression} \
         ORDER BY score DESC LIMIT ? OFFSET ?"
    );
    let mut connection = pool.get_conn().await?;
    let rows = connection
        .exec::<Row, _, _>(statement, mysql_async::Params::Positional(params))
        .await?;
    Ok(rows
        .iter()
        .map(|row| SearchResult {
            url: row_string(row, "url"),
            title: row_string(row, "title"),
            description: row_string(row, "description"),
            content: row_string(row, "content"),
            favicon: row_favicon(row),
            score: row_score(row),
            source: "TiDB Cloud",
        })
        .collect())
}

pub(crate) async fn search_tidb(
    pool: Pool,
    normalized: &str,
    limit: usize,
    offset: usize,
    use_fulltext: bool,
) -> Result<(Vec<SearchResult>, u64)> {
    let terms: Vec<String> = normalized.split_whitespace().map(str::to_owned).collect();
    if terms.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let fulltext_allowed = use_fulltext
        && !normalized
            .chars()
            .any(|character| ('\u{0600}'..='\u{06ff}').contains(&character));

    if fulltext_allowed {
        let expression = format!("fts_match_word({}, search_text)", sql_literal(normalized));
        match tidb_count(pool.clone(), expression.clone()).await {
            Ok(count) => match tidb_rows(
                pool.clone(),
                expression,
                Vec::new(),
                terms.clone(),
                limit,
                offset,
            )
            .await
            {
                Ok(rows) => return Ok((rows, count)),
                Err(error) => {
                    eprintln!(
                        "TiDB full-text result query failed; retrying with keyword matching: {error}"
                    );
                }
            },
            Err(error) => {
                eprintln!("TiDB full-text count failed; retrying with keyword matching: {error}");
            }
        }
    }

    let expression = terms
        .iter()
        .map(|_| "search_text LIKE ?")
        .collect::<Vec<_>>()
        .join(" AND ");
    let patterns = terms
        .iter()
        .map(|term| mysql_pattern(term))
        .collect::<Vec<_>>();
    let mut connection = pool.get_conn().await?;
    let count_statement = format!("SELECT COUNT(*) FROM `{DOCUMENTS_TABLE}` WHERE {expression}");
    let count_params = patterns
        .iter()
        .map(|pattern| Value::Bytes(pattern.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    let count = connection
        .exec_first::<u64, _, _>(
            count_statement,
            mysql_async::Params::Positional(count_params),
        )
        .await?
        .unwrap_or_default();
    drop(connection);
    let rows = tidb_rows(
        pool,
        expression,
        patterns
            .into_iter()
            .map(|pattern| Value::Bytes(pattern.into_bytes()))
            .collect(),
        terms,
        limit,
        offset,
    )
    .await?;
    Ok((rows, count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_patterns_escape_wildcards() {
        assert_eq!(mysql_pattern(r"a%b_c\d"), r"%a\%b\_c\\d%");
    }

    #[test]
    fn mysql_prefix_pattern_escapes_and_anchors() {
        // Escapes LIKE wildcards and anchors at the start — a leading wildcard
        // would defeat any title index.
        assert_eq!(mysql_prefix_pattern(r"a%b_c\d"), r"a\%b\_c\\d%");
        assert_eq!(mysql_prefix_pattern("rust"), "rust%");
    }

    #[test]
    fn sql_literals_escape_single_quotes() {
        assert_eq!(sql_literal("O'Reilly"), "'O''Reilly'");
    }

    #[test]
    fn score_expression_has_three_parameters_per_query_term() {
        let (expression, parameters) = score_expression(&["alpha", "beta"]);
        assert_eq!(parameters.len(), 6);
        assert_eq!(expression.matches("LIKE ?").count(), 6);
    }
}
