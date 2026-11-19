use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

use crate::ported::common::LegacyPage;

const TEMPLATES: [&str; 5] = [
    "Summarize the following content",
    "Extract key information from this text",
    "Explain the main points of this article",
    "Provide a brief overview of this content",
    "What are the key takeaways from this text?",
];

/// Legacy SQLite is opened strictly read-only; it is never created or modified.
pub fn read_legacy_pages(path: impl AsRef<Path>, limit: Option<usize>) -> Result<Vec<LegacyPage>> {
    let path = path.as_ref();
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening legacy SQLite input {} read-only", path.display()))?;
    let mut statement = connection.prepare(
        "SELECT url,title,content FROM pages WHERE title IS NOT NULL AND content IS NOT NULL ORDER BY rowid"
    )?;
    let rows = statement.query_map([], |row| {
        Ok(LegacyPage {
            url: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
            title: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            content: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            ..LegacyPage::default()
        })
    })?;
    let mut pages = Vec::new();
    for row in rows {
        let page = row?;
        if page.content.chars().count() > 100 && page.content.trim().chars().count() >= 50 {
            pages.push(page);
            if limit.is_some_and(|max| pages.len() >= max) {
                break;
            }
        }
    }
    Ok(pages)
}

pub fn export_llama3(path: impl AsRef<Path>, pages: &[LegacyPage]) -> Result<usize> {
    let mut output = BufWriter::new(
        File::create(path.as_ref())
            .with_context(|| format!("creating {}", path.as_ref().display()))?,
    );
    for (i, page) in pages.iter().enumerate() {
        let content = page.content.trim().chars().take(2000).collect::<String>();
        let instruction = TEMPLATES[(i + 1) % TEMPLATES.len()];
        let text = format!(
            "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\nYou are a helpful assistant that can analyze and summarize content accurately.<|eot_id|><|start_header_id|>user<|end_header_id|>\n{instruction}:\n\nTitle: {}\n\nContent: {content}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\nBased on the content titled \"{}\", here are the key points:\n\n1. {} discusses important information about the topic.\n2. The content provides detailed insights and explanations.\n3. Key concepts are thoroughly covered with relevant details.\n\nFor more specific information, please refer to the full content.<|eot_id|>",
            page.title, page.title, page.title
        );
        writeln!(
            output,
            "{{\"text\":\"{}\",\"metadata\":{{\"title\":\"{}\",\"content_length\":{},\"instruction\":\"{}\"}}}}",
            json_escape(&text), json_escape(&page.title), page.content.trim().chars().count(),
            json_escape(instruction)
        )?;
    }
    output.flush()?;
    Ok(pages.len())
}

pub fn export_alpaca(path: impl AsRef<Path>, pages: &[LegacyPage]) -> Result<usize> {
    let mut output = BufWriter::new(
        File::create(path.as_ref())
            .with_context(|| format!("creating {}", path.as_ref().display()))?,
    );
    write!(output, "[")?;
    for (i, page) in pages.iter().enumerate() {
        if i > 0 {
            write!(output, ",")?;
        }
        let content = page.content.trim().chars().take(1500).collect::<String>();
        let instruction = TEMPLATES[i % TEMPLATES.len()];
        write!(
            output,
            "{{\"instruction\":\"{}\",\"input\":\"Title: {}\\n\\nContent: {}\",\"output\":\"This content titled '{}' provides comprehensive information on the subject matter, including detailed explanations and key insights.\"}}",
            json_escape(instruction), json_escape(&page.title), json_escape(&content), json_escape(&page.title)
        )?;
    }
    writeln!(output, "]")?;
    output.flush()?;
    Ok(pages.len())
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c <= '\u{1f}' => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::json_escape;

    #[test]
    fn json_escaping_handles_control_characters_and_quotes() {
        assert_eq!(json_escape("a\"\\\n\t"), "a\\\"\\\\\\n\\t");
    }
}
