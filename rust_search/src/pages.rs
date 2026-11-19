use axum::http::header;
use axum::response::{Html, IntoResponse, Response};
use axum::http::HeaderValue;

fn html_no_store(html: Html<&'static str>) -> Response {
    let mut response = html.into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(crate) fn featured_snippet(query: &str) -> Option<serde_json::Value> {
    let query_lower = query.to_lowercase();
    if (query.contains("ما هي") || query_lower.contains("what is")) && query_lower.contains("html")
    {
        return Some(serde_json::json!({
            "type": "definition",
            "title": "ما هي HTML؟",
            "content": "HTML (HyperText Markup Language) هي لغة ترميز النصوص الفائقة، وهي اللغة الأساسية لإنشاء صفحات الويب.",
            "source": "W3Schools"
        }));
    }
    if (query.contains("ما هي") || query_lower.contains("what is")) && query_lower.contains("css")
    {
        return Some(serde_json::json!({
            "type": "definition",
            "title": "ما هي CSS؟",
            "content": "CSS (Cascading Style Sheets) هي لغة تنسيق تستخدم لوصف مظهر صفحات الويب المكتوبة بلغة HTML.",
            "source": "MDN Web Docs"
        }));
    }
    None
}

pub(crate) fn knowledge_panel(query: &str) -> Option<serde_json::Value> {
    let query_lower = query.to_lowercase();
    if ["html", "css", "javascript", "web"]
        .iter()
        .any(|term| query_lower.contains(term))
    {
        Some(serde_json::json!({
            "title": "تقنيات تطوير الويب",
            "content": [
                "HTML: لبناء الهيكل والمحتوى الأساسي لصفحات الويب",
                "CSS: لتصميم المظهر الخارجي والألوان وتنسيق العناصر",
                "JavaScript: المسؤولة عن التفاعل والحركات داخل الواجهة"
            ],
            "tags": ["HTML", "CSS", "JavaScript", "TypeScript", "Angular"],
            "source": "Wikipedia"
        }))
    } else {
        None
    }
}

pub(crate) fn ai_overview(query: &str) -> Option<serde_json::Value> {
    if ["تقنيات", "technologies", "ويب", "web"]
        .iter()
        .any(|term| query.to_lowercase().contains(term))
    {
        Some(serde_json::json!({
            "title": "ملخص ذكي",
            "content": [
                "الواجهة الأمامية: HTML, CSS, JavaScript لتصميم واجهة المستخدم",
                "الخادم الخلفي: Python, Node.js, Go لمعالجة البيانات والمنطق",
                "قواعد البيانات: MySQL, MongoDB, PostgreSQL لتخزين المعلومات"
            ],
            "sources": ["W3Schools", "MDN Web Docs", "freeCodeCamp"]
        }))
    } else {
        None
    }
}

pub(crate) async fn home() -> Response {
    html_no_store(Html(include_str!("../../index.html")))
}

pub(crate) async fn results_page() -> Response {
    html_no_store(Html(include_str!("../../results.html")))
}

pub(crate) async fn dashboard_page() -> Response {
    html_no_store(Html(include_str!("../../dashboard.html")))
}
