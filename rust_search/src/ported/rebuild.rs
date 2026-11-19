//! Self-healing Tantivy index: rebuilds it from the cloud stores when the
//! local index directory is empty, so a fresh deploy or a wiped `velo_index`
//! recovers automatically. The cloud stores stay the source of truth; the
//! fast index is derived data — no external volume or service needed.

use anyhow::Result;
use mongodb::{bson::Document, Collection};
use mysql_async::Pool;

use crate::ported::semantic::{load_mongo_pages, load_tidb_pages, SemanticSourcePage};
use crate::ported::tantivy_engine::TantivyEngine;

/// Rebuilds the Tantivy index from TiDB and MongoDB when it has no
/// documents, indexing up to `max_documents` most-recent pages. Returns the
/// number of pages indexed. A non-empty index is left untouched — the
/// startup rebuild is only a self-heal for empty indexes.
pub async fn rebuild_tantivy_index_if_empty(
    engine: &TantivyEngine,
    pool: Option<&Pool>,
    mongo: Option<&Collection<Document>>,
    max_documents: usize,
) -> Result<usize> {
    if max_documents == 0 || engine.document_count() > 0 {
        return Ok(0);
    }
    // Both sources are loaded before anything is staged: a failure in the
    // second leg must not leave uncommitted documents behind, because a later
    // runtime commit would flush them and make the index non-empty, which
    // permanently disables every future startup rebuild.
    let tidb_pages = match pool {
        Some(pool) => load_tidb_pages(pool, max_documents).await?,
        None => Vec::new(),
    };
    let remaining = max_documents.saturating_sub(tidb_pages.len());
    let mongo_pages = match mongo {
        Some(collection) if remaining > 0 => load_mongo_pages(collection, remaining).await?,
        _ => Vec::new(),
    };
    let added = add_pages(engine, &tidb_pages) + add_pages(engine, &mongo_pages);
    if added > 0 {
        engine.commit()?;
    }
    Ok(added)
}

fn add_pages(engine: &TantivyEngine, pages: &[SemanticSourcePage]) -> usize {
    let mut added = 0;
    for page in pages {
        if page.url.is_empty() {
            continue;
        }
        let body = if page.description.is_empty() {
            page.content.clone()
        } else {
            format!("{} {}", page.description, page.content)
        };
        match engine.add_document(&page.url, &page.title, &page.url, &body) {
            Ok(()) => added += 1,
            Err(error) => eprintln!("Skipping page {}: {error}", page.url),
        }
    }
    added
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_engine(tag: &str) -> TantivyEngine {
        let dir = std::env::temp_dir().join(format!("velo_rebuild_{tag}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        TantivyEngine::open_or_create(&dir).expect("engine opens")
    }

    fn page(url: &str, title: &str, description: &str, content: &str) -> SemanticSourcePage {
        SemanticSourcePage {
            url: url.to_owned(),
            title: title.to_owned(),
            description: description.to_owned(),
            content: content.to_owned(),
        }
    }

    #[test]
    fn empty_index_is_rebuilt_from_pages_and_stays_searchable() {
        let engine = test_engine("rebuild");
        let pages = [
            page(
                "https://a.test",
                "دليل الجامعات",
                "قائمة الجامعات المصرية",
                "القبول والامتحانات",
            ),
            page("https://b.test", "Rust guide", "", "Tantivy full-text search"),
        ];
        assert_eq!(add_pages(&engine, &pages), 2);
        engine.commit().expect("commit");
        assert_eq!(engine.document_count(), 2);
        let hits = engine.search("الجامعات", 5, 0).expect("search");
        assert_eq!(hits.hits[0].url, "https://a.test");
        let _ = fs::remove_dir_all(engine.index_dir);
    }

    #[test]
    fn pages_without_urls_are_skipped() {
        let engine = test_engine("skip");
        let pages = [
            page("", "no url", "d", "c"),
            page("https://ok.test", "ok", "", "content here"),
        ];
        assert_eq!(add_pages(&engine, &pages), 1);
        let _ = fs::remove_dir_all(engine.index_dir);
    }

    #[test]
    fn staging_the_same_pages_twice_does_not_duplicate() {
        let engine = test_engine("dedupe");
        let pages = [
            page("https://a.test", "one", "", "shared content"),
            page("https://b.test", "two", "", "shared content"),
        ];
        assert_eq!(add_pages(&engine, &pages), 2);
        assert_eq!(add_pages(&engine, &pages), 2);
        engine.commit().expect("commit");
        assert_eq!(engine.document_count(), 2);
        let _ = fs::remove_dir_all(engine.index_dir);
    }

    #[tokio::test]
    async fn populated_index_is_left_untouched() {
        let engine = test_engine("guarded");
        engine
            .add_document("1", "Existing", "https://x.test", "some content")
            .expect("add");
        engine.commit().expect("commit");
        let added = rebuild_tantivy_index_if_empty(&engine, None, None, 100)
            .await
            .expect("rebuild");
        assert_eq!(added, 0);
        assert_eq!(engine.document_count(), 1);
        let _ = fs::remove_dir_all(engine.index_dir);
    }

    #[tokio::test]
    async fn empty_index_without_backends_rebuilds_nothing() {
        let engine = test_engine("nobackend");
        let added = rebuild_tantivy_index_if_empty(&engine, None, None, 100)
            .await
            .expect("rebuild");
        assert_eq!(added, 0);
        assert_eq!(engine.document_count(), 0);
        let _ = fs::remove_dir_all(engine.index_dir);
    }
}
