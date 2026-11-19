use super::lemmatizer::Lemmatizer;
use super::query_parser::{ParsedQuery, QueryParser};
use super::ranking_engine::RankingEngine;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub struct CloudDocument {
    pub id: String,
    pub title: String,
    pub content: String,
    pub url: String,
}

#[derive(Debug, Clone, Default)]
pub struct CloudStats {
    pub total_universities: u64,
    pub total_pages: u64,
    pub indexed_documents: u64,
}

/// Required cloud-only data boundary for the orchestrator. Implementations must query the
/// configured cloud stores; this module deliberately has no local database fallback.
pub type CloudFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

pub trait CloudSearchBackend: Send + Sync {
    fn search<'a>(&'a self, query: &'a str, limit: usize) -> CloudFuture<'a, Vec<CloudDocument>>;
    fn stats(&self) -> CloudFuture<'_, CloudStats>;
}

pub struct SearchEngine {
    backend: Arc<dyn CloudSearchBackend>,
    query_parser: QueryParser,
    _lemmatizer: Lemmatizer,
    ranking_engine: RankingEngine,
}

impl SearchEngine {
    pub fn new(backend: Arc<dyn CloudSearchBackend>) -> Self {
        Self {
            backend,
            query_parser: QueryParser::new(),
            _lemmatizer: Lemmatizer,
            ranking_engine: RankingEngine::new(),
        }
    }

    /// Search the cloud backend using parsed and expanded terms, then apply the local
    /// ranking features to only the cloud-returned documents.
    pub async fn search(&mut self, query: &str, limit: usize) -> Result<Value, String> {
        let started = Instant::now();
        let parsed = self.query_parser.parse(query);
        let expanded_terms = self.query_parser.expand(&parsed);
        let expanded_query = expanded_terms.join(" ");
        let raw_results = self.backend.search(&expanded_query, 50).await?;
        let docs: Vec<Value> = raw_results
            .into_iter()
            .map(|doc| {
                json!({
                    "id": doc.id, "title": doc.title, "content": doc.content, "url": doc.url
                })
            })
            .collect();
        let ranked = self.ranking_engine.rank_documents(query, &docs, None);
        let results: Vec<Value> = ranked
            .into_iter()
            .take(limit)
            .map(|(doc, score)| {
                let id = string_field(&doc, "id");
                let title = string_field(&doc, "title");
                let url = string_field(&doc, "url");
                let content = string_field(&doc, "content");
                json!({
                    "id": id,
                    "title": if title.is_empty() { "Untitled".to_owned() } else { title },
                    "url": if url.is_empty() { "#".to_owned() } else { url },
                    "description": content.chars().take(200).collect::<String>(),
                    "score": round_to(score, 4),
                    "type": if id.starts_with("uni_") { "university" } else { "page" }
                })
            })
            .collect();
        let featured_snippet = featured_snippet(query);
        let knowledge_panel = knowledge_panel(query);
        let ai_overview = ai_overview(query);
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        Ok(json!({
            "query": query,
            "parsed": parsed_value(&parsed),
            "total_results": results.len(),
            "results": results,
            "featured_snippet": featured_snippet,
            "knowledge_panel": knowledge_panel,
            "ai_overview": ai_overview,
            "search_time_ms": round_to(elapsed_ms, 2)
        }))
    }

    pub fn get_suggestions(&self, query: &str, limit: usize) -> Vec<String> {
        if query.trim().is_empty() {
            return Vec::new();
        }
        let lower = query.to_lowercase();
        let tech = [
            "html css javascript tutorial",
            "web development best practices",
            "python programming guide",
            "machine learning basics",
            "database design principles",
        ];
        let universities = [
            "universities in usa",
            "best engineering universities",
            "computer science programs",
            "university rankings 2024",
            "scholarship opportunities",
        ];
        let source: &[&str] = if ["html", "css", "js", "web", "programming"]
            .iter()
            .any(|t| lower.contains(t))
        {
            &tech
        } else {
            &universities
        };
        source
            .iter()
            .filter(|suggestion| suggestion.contains(&lower))
            .take(limit)
            .map(|suggestion| (*suggestion).to_owned())
            .collect()
    }

    pub async fn get_stats(&self) -> Result<Value, String> {
        let stats = self.backend.stats().await?;
        Ok(json!({
            "total_universities": stats.total_universities,
            "total_pages": stats.total_pages,
            "indexed_documents": stats.indexed_documents,
            "engine_version": "2.0.0"
        }))
    }

    pub fn featured_snippet(query: &str) -> Option<Value> {
        featured_snippet(query)
    }
    pub fn knowledge_panel(query: &str) -> Option<Value> {
        knowledge_panel(query)
    }
    pub fn ai_overview(query: &str) -> Option<Value> {
        ai_overview(query)
    }
}

fn parsed_value(parsed: &ParsedQuery) -> Value {
    json!({
        "original": parsed.original,
        "language": parsed.language,
        "tokens": parsed.tokens,
        "keywords": parsed.keywords,
        "stemmed": parsed.stemmed,
        "intent": parsed.intent,
        "word_count": parsed.word_count,
        "unique_words": parsed.unique_words
    })
}

fn string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn round_to(value: f64, places: i32) -> f64 {
    let scale = 10_f64.powi(places);
    (value * scale).round_ties_even() / scale
}

fn featured_snippet(query: &str) -> Option<Value> {
    let lower = query.to_lowercase();
    if query.contains("ما هي") || lower.contains("what is") {
        if lower.contains("html") {
            return Some(json!({
                "type":"definition",
                "title":"ما هي HTML؟",
                "content":"HTML (HyperText Markup Language) هي لغة ترميز النصوص الفائقة، وهي اللغة الأساسية لإنشاء صفحات الويب.",
                "source":"W3Schools"
            }));
        }
        if lower.contains("css") {
            return Some(json!({
                "type":"definition",
                "title":"ما هي CSS؟",
                "content":"CSS (Cascading Style Sheets) هي لغة تنسيق تستخدم لوصف مظهر صفحات الويب المكتوبة بلغة HTML.",
                "source":"MDN Web Docs"
            }));
        }
    }
    None
}

fn knowledge_panel(query: &str) -> Option<Value> {
    let lower = query.to_lowercase();
    if ["html", "css", "javascript", "web"]
        .iter()
        .any(|term| lower.contains(term))
    {
        Some(json!({
            "title":"تقنيات تطوير الويب",
            "content":[
                "HTML: لبناء الهيكل والمحتوى الأساسي لصفحات الويب",
                "CSS: لتصميم المظهر الخارجي والألوان وتنسيق العناصر",
                "JavaScript: المسؤولة عن التفاعل والحركات داخل الواجهة"
            ],
            "tags":["HTML","CSS","JavaScript","TypeScript","Angular"],
            "source":"Wikipedia"
        }))
    } else {
        None
    }
}

fn ai_overview(query: &str) -> Option<Value> {
    let lower = query.to_lowercase();
    if ["تقنيات", "technologies", "ويب", "web"]
        .iter()
        .any(|term| lower.contains(term))
    {
        Some(json!({
            "title":"ملخص ذكي",
            "content":[
                "الواجهة الأمامية: HTML, CSS, JavaScript لتصميم واجهة المستخدم",
                "الخادم الخلفي: Python, Node.js, Go لمعالجة البيانات والمنطق",
                "قواعد البيانات: MySQL, MongoDB, PostgreSQL لتخزين المعلومات"
            ],
            "sources":["W3Schools","MDN Web Docs","freeCodeCamp"]
        }))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeCloud {
        seen_query: Mutex<String>,
    }
    impl CloudSearchBackend for FakeCloud {
        fn search<'a>(
            &'a self,
            query: &'a str,
            _limit: usize,
        ) -> CloudFuture<'a, Vec<CloudDocument>> {
            *self.seen_query.lock().unwrap() = query.to_owned();
            Box::pin(async {
                Ok(vec![
                    CloudDocument {
                        id: "uni_1".into(),
                        title: "HTML University Guide".into(),
                        content: "HTML guide".into(),
                        url: "https://example.edu".into(),
                    },
                    CloudDocument {
                        id: "page_2".into(),
                        title: "Other".into(),
                        content: "unrelated".into(),
                        url: "https://example.org".into(),
                    },
                ])
            })
        }
        fn stats(&self) -> CloudFuture<'_, CloudStats> {
            Box::pin(async {
                Ok(CloudStats {
                    total_universities: 1,
                    total_pages: 2,
                    indexed_documents: 3,
                })
            })
        }
    }

    #[tokio::test]
    async fn search_uses_expanded_cloud_query_and_preserves_enriched_outputs() {
        let cloud = Arc::new(FakeCloud {
            seen_query: Mutex::new(String::new()),
        });
        let mut engine = SearchEngine::new(cloud.clone());
        let response = engine.search("what is HTML", 5).await.unwrap();
        let expanded = cloud.seen_query.lock().unwrap().clone();
        assert!(expanded.contains("html"));
        assert_eq!(response["total_results"], 2);
        assert_eq!(response["results"][0]["type"], "university");
        assert_eq!(response["featured_snippet"]["source"], "W3Schools");
        assert!(response["knowledge_panel"].is_object());
    }

    #[tokio::test]
    async fn stats_and_suggestion_behavior() {
        let engine = SearchEngine::new(Arc::new(FakeCloud {
            seen_query: Mutex::new(String::new()),
        }));
        assert_eq!(engine.get_stats().await.unwrap()["total_pages"], 2);
        assert_eq!(
            engine.get_suggestions("html", 10),
            ["html css javascript tutorial"]
        );
        assert!(engine.get_suggestions("  ", 10).is_empty());
        assert!(SearchEngine::ai_overview("web technologies").is_some());
    }
}
