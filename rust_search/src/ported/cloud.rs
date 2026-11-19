use std::path::Path;

use anyhow::{bail, Context, Result};
use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client, Collection, Database,
};
use rusqlite::{Connection, OpenFlags};

use crate::ported::common::{LegacyPage, Page, TiDbSettings};

pub(crate) async fn mongo_collection(
    db_env: &[&str],
    collection_env: &[&str],
    default_collection: &str,
) -> Result<(Client, Database, Collection<Document>)> {
    let uri = std::env::var("MONGO_URI")
        .or_else(|_| std::env::var("MONGODB_URI"))
        .context("Set MONGO_URI before using MongoDB")?;
    let options = ClientOptions::parse(&uri).await?;
    let client = Client::with_options(options)?;
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await?;
    let database_name = db_env
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .filter(|value| !value.trim().is_empty());
    let database = if let Some(name) = database_name {
        client.database(&name)
    } else {
        client
            .default_database()
            .context("set MONGO_DATABASE or include a database in MONGO_URI")?
    };
    let collection_name = collection_env
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default_collection.to_owned());
    let collection = database.collection(&collection_name);
    Ok((client, database, collection))
}

pub async fn sync_pages_to_mongodb(
    pages: &[LegacyPage],
    collection_name: &str,
) -> Result<(usize, usize)> {
    let (_client, _database, collection) = mongo_collection(
        &["MONGO_DATABASE", "MONGO_DB", "MONGODB_DB"],
        &[],
        collection_name,
    )
    .await?;
    let mut synced = 0;
    let mut errors = 0;
    for page in pages {
        if page.url.trim().is_empty() {
            errors += 1;
            continue;
        }
        let update = doc! { "$set": {
            "url": &page.url,
            "title": &page.title,
            "content": &page.content,
            "synced_at": mongodb::bson::DateTime::now(),
            "source": "SQLite-Local",
        }};
        match collection
            .update_one(doc! { "url": &page.url }, update)
            .upsert(true)
            .await
        {
            Ok(_) => synced += 1,
            Err(_) => errors += 1,
        }
    }
    Ok((synced, errors))
}

/// Read old SQLite input read-only, preferring pages and falling back to universities.
pub async fn sync_legacy_pages_to_mongodb(path: impl AsRef<Path>) -> Result<(usize, usize)> {
    let (pages, is_universities) = read_sync_source(path)?;
    let collection = if is_universities {
        "Universities_Pages"
    } else {
        "Crawled_Pages"
    };
    sync_pages_to_mongodb(&pages, collection).await
}

pub(crate) fn read_sync_source(path: impl AsRef<Path>) -> Result<(Vec<LegacyPage>, bool)> {
    let connection = Connection::open_with_flags(
        path.as_ref(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {} read-only", path.as_ref().display()))?;
    let pages_exist: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='pages')",
        [],
        |row| row.get(0),
    )?;
    let pages_usable = if pages_exist {
        let columns = connection
            .prepare("PRAGMA table_info(pages)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;
        ["url", "title", "content"]
            .iter()
            .all(|column| columns.contains(*column))
    } else {
        false
    };
    if pages_usable {
        let mut statement = connection.prepare("SELECT url,title,content FROM pages")?;
        let rows = statement.query_map([], |row| {
            Ok(LegacyPage {
                url: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                title: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                content: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                ..LegacyPage::default()
            })
        })?;
        return Ok((rows.collect::<rusqlite::Result<Vec<_>>>()?, false));
    }
    let universities_exist: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='universities')",
        [],
        |row| row.get(0),
    )?;
    if !universities_exist {
        bail!("neither pages nor universities table exists");
    }
    let mut statement = connection.prepare("SELECT url,name,description FROM universities")?;
    let rows = statement.query_map([], |row| {
        Ok(LegacyPage {
            url: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            title: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            content: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            language: "__universities__".to_owned(),
            ..LegacyPage::default()
        })
    })?;
    Ok((rows.collect::<rusqlite::Result<Vec<_>>>()?, true))
}

/// Preserve the original migration's old TiDB table shape and INSERT IGNORE behavior.
pub async fn migrate_legacy_pages_to_velo(path: impl AsRef<Path>) -> Result<usize> {
    let legacy_pages = read_pages_for_old_tidb(path)?;
    let pages = legacy_pages
        .into_iter()
        .map(|page| Page {
            url: page.url,
            title: page.title,
            content: page.content,
            description: String::new(),
            language: page.language,
            status_code: 200,
            crawled_at: page.crawled_at,
            source_metadata_json: None,
        })
        .collect::<Vec<_>>();
    let settings = TiDbSettings::from_env()?;
    let pool = settings.pool();
    crate::ported::common::initialize_search_schema(&pool).await?;
    crate::ported::common::upsert_tidb_pages(&pool, &pages).await
}

fn read_pages_for_old_tidb(path: impl AsRef<Path>) -> Result<Vec<LegacyPage>> {
    let connection = Connection::open_with_flags(
        path.as_ref(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening {} read-only", path.as_ref().display()))?;
    let mut statement = connection.prepare("SELECT url,title,content FROM pages")?;
    let rows = statement.query_map([], |row| {
        Ok(LegacyPage {
            url: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            title: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            content: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            ..LegacyPage::default()
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(crate) fn from_mongo_document(doc: &Document) -> Option<Page> {
    let url = doc.get_str("url").ok()?.to_owned();
    Some(Page {
        url,
        title: doc.get_str("title").unwrap_or_default().to_owned(),
        content: doc.get_str("content").unwrap_or_default().to_owned(),
        description: doc.get_str("description").unwrap_or_default().to_owned(),
        language: doc
            .get_str("language")
            .or_else(|_| doc.get_str("html_lang"))
            .unwrap_or_default()
            .to_owned(),
        status_code: doc.get_i32("status_code").unwrap_or(200) as i16,
        crawled_at: doc.get_f64("crawled_at").ok().or_else(|| {
            doc.get_datetime("crawled_at")
                .ok()
                .map(|value| value.timestamp_millis() as f64 / 1000.0)
        }),
        source_metadata_json: None,
    })
}
