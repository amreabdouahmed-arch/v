use anyhow::{Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use tantivy::collector::{Count, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    Field, FieldType, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, STORED,
    STRING,
};
use tantivy::tokenizer::{
    LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Token, TokenFilter, TokenStream,
    Tokenizer,
};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Term, TantivyDocument};
use unicode_normalization::UnicodeNormalization;

use super::common::{hex_string, normalize_text};

/// Tokenizer name used by the title and body fields on freshly created
/// indexes: tantivy's default pipeline plus Arabic folding identical to
/// `normalize_text`. Existing indexes keep whatever schema they were built
/// with, so their behavior never changes on upgrade.
const ARABIC_NORM: &str = "arabic_norm";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TantivySearchHit {
    pub id: String,
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TantivySearchResult {
    pub hits: Vec<TantivySearchHit>,
    pub total_hits: usize,
    pub query: String,
}

#[derive(Clone)]
pub struct TantivyEngine {
    index: Index,
    reader: IndexReader,
    writer: Arc<Mutex<IndexWriter>>,
    pub id_field: tantivy::schema::Field,
    pub title_field: tantivy::schema::Field,
    pub url_field: tantivy::schema::Field,
    pub body_field: tantivy::schema::Field,
    pub index_dir: PathBuf,
    /// Whether the indexed token stream is Arabic-normalized. Read back from
    /// the on-disk schema: only fresh/rebuilt indexes opt in, pre-existing
    /// ones keep the legacy raw-tokenizer behavior.
    normalized: bool,
}

static RE_TAGS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<\|[^>]*\|>").expect("static tag regex compiles"));
static RE_SPACES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s+").expect("static space regex compiles"));
/// Navigation/boilerplate blocks are dropped entirely, including their text.
static RE_HTML_BLOCKS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    ["script", "style", "nav", "header", "footer", "aside"]
        .iter()
        .map(|tag| {
            Regex::new(&format!(r"(?is)<{tag}\b[^>]*>.*?</{tag}\s*>"))
                .expect("static block regex compiles")
        })
        .collect()
});
/// Remaining tags become spaces, so `<p>one</p><p>two</p>` never turns into
/// `onetwo`.
static RE_HTML_TAGS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)<[^>]*>").expect("static html tag regex compiles"));
/// Navigation labels that some older crawls stored as plain text, without the
/// surrounding `<nav>`/`<header>` tags. They are only removed from documents
/// that clearly start with a skip link or jump link, so a page that legitimately
/// talks about a "menu" keeps its wording.
static RE_NAV_PHRASES: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)(?:skip to (?:main )?content|jump to content|explore websites, people, and locations|look up people by [“”"']?last name sounds like[“”"']?|what are you looking for\?|see more results|suggestions or feedback\?|\bmenu\b)\s*[↓↑→←]?"#,
    )
    .expect("static navigation regex compiles")
});

fn starts_with_navigation(text: &str) -> bool {
    let head: String = text
        .trim_start()
        .chars()
        .take(160)
        .collect::<String>()
        .to_ascii_lowercase();
    head.contains("skip to content")
        || head.contains("skip to main content")
        || head.contains("jump to content")
}

/// Drops plain-text navigation blocks that older crawls saved before their
/// HTML tags were removed. Real page text that follows the navigation is kept.
fn strip_navigation_noise(text: &str) -> String {
    if !starts_with_navigation(text) {
        return text.to_owned();
    }
    RE_NAV_PHRASES.replace_all(text, " ").into_owned()
}

/// Clean raw LLM tags, HTML boilerplate, and system instructions to produce
/// clean human snippets. The legacy `Content:` / `Title:` / `Based on the
/// content` markers are only stripped from texts that actually carry LLM tags,
/// so a plain document that legitimately contains "Content:" keeps its text.
pub fn clean_raw_text(text: &str) -> String {
    let tagged = RE_TAGS.is_match(text);
    let mut stripped = RE_TAGS.replace_all(text, "").into_owned();
    for block in RE_HTML_BLOCKS.iter() {
        stripped = block.replace_all(&stripped, " ").into_owned();
    }
    let stripped = RE_HTML_TAGS.replace_all(&stripped, " ");
    let stripped = strip_navigation_noise(&stripped);

    let content_text = if tagged {
        if let Some(idx) = stripped.find("Content:") {
            &stripped[idx + 8..]
        } else if let Some(idx) = stripped.find("Title:") {
            &stripped[idx..]
        } else {
            &stripped
        }
    } else {
        &stripped
    };

    let clean = if tagged {
        if let Some(idx) = content_text.find("Based on the content") {
            &content_text[..idx]
        } else {
            content_text
        }
    } else {
        content_text
    };

    let collapsed = RE_SPACES.replace_all(clean.trim(), " ");
    collapsed.into_owned()
}

/// Picks a window around the first query term instead of always taking the
/// page header. That keeps menus and cookie banners out of the snippet when
/// the actual match sits further down the page.
fn query_centered_snippet(text: &str, query: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        return text.to_owned();
    }

    let lower = text.to_lowercase();
    let mut first_match: Option<usize> = None;
    for term in query.split_whitespace() {
        let term = term
            .trim_matches(|ch: char| !ch.is_alphanumeric())
            .to_lowercase();
        if term.is_empty() {
            continue;
        }
        if let Some(byte_pos) = lower.find(&term) {
            let char_pos = lower[..byte_pos].chars().count();
            first_match = Some(first_match.map_or(char_pos, |best| best.min(char_pos)));
        }
    }

    let start = match first_match {
        Some(pos) => {
            let window_start = pos.saturating_sub(75);
            let mut start = window_start;
            for index in (window_start..pos).rev() {
                if is_boundary_at(&chars, index) {
                    start = index + 1;
                    break;
                }
            }
            if start == window_start && window_start > 0 {
                // Never begin in the middle of a word: when no punctuation is
                // nearby, start exactly at the match instead.
                let cuts_word = chars
                    .get(window_start - 1)
                    .is_some_and(|ch| ch.is_alphanumeric())
                    && chars
                        .get(window_start)
                        .is_some_and(|ch| ch.is_alphanumeric());
                if cuts_word {
                    start = pos;
                }
            }
            while start < pos && is_leading_noise(chars[start]) {
                start += 1;
            }
            start
        }
        None => 0,
    };
    let end = (start + max_chars).min(chars.len());
    let snippet: String = chars[start..end].iter().collect();
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        snippet,
        if end < chars.len() { "…" } else { "" }
    )
}

fn is_boundary_at(chars: &[char], index: usize) -> bool {
    let Some(&ch) = chars.get(index) else {
        return false;
    };
    if index > 0
        && index + 1 < chars.len()
        && chars[index - 1].is_ascii_digit()
        && chars[index + 1].is_ascii_digit()
    {
        return false;
    }
    let next = chars.get(index + 1).copied();
    match ch {
        '\n' => true,
        '.' | '!' | '?' | ',' | '—' | '–' | '•' | '،' | '؛' | '؟' => next.is_none_or(|next| {
            next.is_whitespace() || next.is_uppercase() || matches!(next, '"' | '“' | '‘' | '(')
        }),
        ':' | ';' => next.is_none_or(char::is_whitespace),
        _ => false,
    }
}

fn is_leading_noise(ch: char) -> bool {
    ch.is_whitespace()
        || matches!(
            ch,
            '"' | '\'' | '“' | '”' | '‘' | '’' | '(' | ')' | '[' | ']' | '-' | '–' | '—' | '•' | '·'
        )
}

fn stable_title_hash(title: &str) -> String {
    let mut hasher = DefaultHasher::new();
    title.to_lowercase().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Derive realistic, clean web links instead of raw local IDs. The suffix is
/// a stable hash of the title, so the same document maps to the same URL on
/// every run (position-independent). The "mit" keyword gate matches whole
/// words only: a title containing "limit" or "submitting" must not fabricate
/// an hr.mit.edu link.
pub fn derive_clean_url(title: &str) -> String {
    let stable = stable_title_hash(title);
    let lower = title.to_lowercase();
    let has_word =
        |word: &str| lower.split(|c: char| !c.is_ascii_alphanumeric()).any(|part| part == word);
    if lower.contains("theunijobs") || lower.contains("academic jobs") {
        format!("https://www.timeshighereducation.com/unijobs/listings/{stable}")
    } else if lower.contains("times higher education") || lower.contains("rankings") {
        format!("https://www.timeshighereducation.com/world-university-rankings/{stable}")
    } else if has_word("mit") {
        format!("https://hr.mit.edu/careers/jobs/{stable}")
    } else if lower.contains("harvard") {
        format!("https://www.harvard.edu/programs/{stable}")
    } else if lower.contains("stanford") {
        format!("https://www.stanford.edu/academics/{stable}")
    } else if lower.contains("cairo") {
        format!("https://cu.edu.eg/page/{stable}")
    } else if let Some(domain_part) = title.split('|').next_back() {
        let domain_clean: String = domain_part
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '-')
            .collect();
        let domain_lower = domain_clean.to_lowercase();
        if domain_lower.contains('.') {
            format!("https://www.{domain_lower}/articles/{stable}")
        } else {
            format!("https://www.academic-portal.org/records/{stable}")
        }
    } else {
        format!("https://www.academic-portal.org/records/{stable}")
    }
}

fn build_schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("id", STRING | STORED);
    // Same indexing options as the legacy TEXT constant (fieldnorms, with
    // frequencies and positions) except for the tokenizer, so BM25 scoring
    // behavior is unchanged; only the indexed token stream is Arabic-folded.
    let normalized_text = TextOptions::default().set_stored().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(ARABIC_NORM)
            .set_fieldnorms(true)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    builder.add_text_field("title", normalized_text.clone());
    builder.add_text_field("url", STRING | STORED);
    builder.add_text_field("body", normalized_text);
    builder.build()
}

/// Registers the Arabic-normalizing pipeline on the index. Registration is
/// additive to tantivy's built-in tokenizers, so legacy schemas that point at
/// "default" keep working untouched.
fn register_arabic_tokenizer(index: &Index) {
    index.tokenizers().register(
        ARABIC_NORM,
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(4096))
            .filter(LowerCaser)
            .filter(ArabicFoldFilter)
            .build(),
    );
}

/// Token filter that folds Arabic text exactly like `normalize_text`: NFKC
/// normalization, lowercasing, diacritic/tatweel removal, and hamza/alef
/// unification. It only rewrites the indexed token stream; stored text (and
/// therefore snippets) keeps its original spelling.
#[derive(Clone)]
struct ArabicFoldFilter;

impl TokenFilter for ArabicFoldFilter {
    type Tokenizer<T: Tokenizer> = ArabicFoldTokenizer<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> ArabicFoldTokenizer<T> {
        ArabicFoldTokenizer { inner: tokenizer }
    }
}

#[derive(Clone)]
struct ArabicFoldTokenizer<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for ArabicFoldTokenizer<T> {
    type TokenStream<'a> = ArabicFoldTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        ArabicFoldTokenStream {
            tail: self.inner.token_stream(text),
        }
    }
}

struct ArabicFoldTokenStream<T> {
    tail: T,
}

impl<T: TokenStream> TokenStream for ArabicFoldTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let token = self.tail.token_mut();
        token.text = fold_arabic_text(&token.text);
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

/// Folds one token's text with the exact `normalize_text` mapping. Characters
/// that `normalize_text` would turn into spaces are dropped, because a token
/// cannot be split any further. The mapping is idempotent, so query text that
/// was already normalized by `normalize_text` folds to itself.
fn fold_arabic_text(text: &str) -> String {
    text.chars()
        .nfkc()
        .flat_map(char::to_lowercase)
        .filter_map(|ch| match ch {
            '\u{064b}'..='\u{065f}' | '\u{0670}' | '\u{0640}' => None,
            'إ' | 'أ' | 'آ' | 'ٱ' => Some('ا'),
            'ى' => Some('ي'),
            'ؤ' => Some('و'),
            'ئ' => Some('ي'),
            c if c.is_alphanumeric() => Some(c),
            _ => None,
        })
        .collect()
}

/// True when the query uses tantivy query syntax that `normalize_text` would
/// destroy (operators, field prefixes, ranges, boolean keywords) — such
/// queries pass through raw even on normalized indexes. Synonym-expanded
/// queries are already normalized term by term upstream, so they are safe
/// here precisely because the gate keeps them raw.
fn query_has_syntax(query: &str) -> bool {
    query
        .chars()
        .any(|c| {
            matches!(
                c,
                '"' | ':' | '(' | ')' | '[' | ']' | '{' | '}' | '+' | '-' | '^' | '*' | '~' | '!'
            )
        })
        || query
            .split_whitespace()
            .any(|token| matches!(token, "AND" | "OR" | "NOT"))
}

fn field_tokenizer(schema: &Schema, field: tantivy::schema::Field) -> Option<String> {
    match schema.get_field_entry(field).field_type() {
        FieldType::Str(options) => options
            .get_indexing_options()
            .map(|indexing| indexing.tokenizer().to_owned()),
        _ => None,
    }
}

fn schema_is_normalized(schema: &Schema) -> bool {
    schema
        .get_field("title")
        .ok()
        .map(|title| field_tokenizer(schema, title).as_deref() == Some(ARABIC_NORM))
        .unwrap_or(false)
}

/// Last-resort query for input the strict parser rejects and the lenient parser
/// throws away entirely. `10:30` parses as a field prefix (`Field does not
/// exist: '10'`) and lenient mode yields an `EmptyQuery`, so tokenizing the
/// text with each field's own analyzer and OR-ing the terms keeps such input
/// searchable instead of silently returning nothing.
fn plain_text_fallback_query(
    index: &Index,
    fields: &[Field],
    text: &str,
) -> Option<Box<dyn Query>> {
    let schema = index.schema();
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for field in fields {
        let Some(tokenizer_name) = field_tokenizer(&schema, *field) else {
            continue;
        };
        let Some(mut analyzer) = index.tokenizers().get(&tokenizer_name) else {
            continue;
        };
        analyzer.token_stream(text).process(&mut |token| {
            if token.text.is_empty() {
                return;
            }
            clauses.push((
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(*field, &token.text),
                    IndexRecordOption::WithFreqs,
                )) as Box<dyn Query>,
            ));
        });
    }
    if clauses.is_empty() {
        None
    } else {
        Some(Box::new(BooleanQuery::new(clauses)))
    }
}

/// Opens the index, creating it with the Arabic-normalized schema when new.
/// A pre-existing index built with the legacy raw-tokenizer schema keeps
/// today's exact raw-query behavior (the `normalized` flag is read back from
/// the on-disk schema), so existing data is never silently rewritten. Only an
/// unusable directory surfaces an error, which the caller may heal by wiping.
fn open_index(dir: &Path) -> Result<(Index, bool)> {
    let mmap_dir = MmapDirectory::open(dir)
        .with_context(|| format!("failed to open mmap directory {:?}", dir))?;
    match Index::open_or_create(mmap_dir, build_schema()) {
        Ok(index) => Ok((index, true)),
        Err(create_error) => {
            let mmap_dir = MmapDirectory::open(dir)
                .with_context(|| format!("failed to open mmap directory {:?}", dir))?;
            match Index::open(mmap_dir) {
                Ok(index) => {
                    eprintln!(
                        "Keeping the existing velo_index schema: {create_error}; Arabic normalization stays off for this index until it is rebuilt"
                    );
                    let normalized = schema_is_normalized(&index.schema());
                    Ok((index, normalized))
                }
                Err(open_error) => Err(anyhow::Error::from(open_error).context(format!(
                    "velo_index is unusable ({create_error})"
                ))),
            }
        }
    }
}

impl TantivyEngine {
    pub fn open_or_create(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("failed to create dir {:?}", dir))?;

        let (index, normalized) = open_index(dir)?;
        register_arabic_tokenizer(&index);

        let schema = index.schema();
        let id_field = schema
            .get_field("id")
            .context("index schema is missing the id field")?;
        let title_field = schema
            .get_field("title")
            .context("index schema is missing the title field")?;
        let url_field = schema
            .get_field("url")
            .context("index schema is missing the url field")?;
        let body_field = schema
            .get_field("body")
            .context("index schema is missing the body field")?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index,
            reader,
            writer: Arc::new(Mutex::new(writer)),
            id_field,
            title_field,
            url_field,
            body_field,
            index_dir: dir.to_path_buf(),
            normalized,
        })
    }

    pub fn add_document(&self, id: &str, title: &str, url: &str, body: &str) -> Result<()> {
        let clean_content = clean_raw_text(body);
        let mut doc = TantivyDocument::default();
        doc.add_text(self.id_field, id);
        doc.add_text(self.title_field, title);
        doc.add_text(self.url_field, url);
        doc.add_text(self.body_field, &clean_content);

        let writer = self
            .writer
            .lock()
            .map_err(|e| anyhow::anyhow!("writer lock poisoned: {}", e))?;
        // Upsert semantics: the id field is a keyed STRING field, so deleting
        // the exact id term first keeps re-indexed documents from duplicating.
        writer.delete_term(Term::from_field_text(self.id_field, id));
        writer.add_document(doc)?;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|e| anyhow::anyhow!("writer lock poisoned: {}", e))?;
        writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    pub fn search(&self, query_str: &str, limit: usize, offset: usize) -> Result<TantivySearchResult> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.title_field, self.body_field]);
        let effective = if self.normalized && !query_has_syntax(query_str) {
            normalize_text(query_str)
        } else {
            query_str.to_owned()
        };
        let (query, parse_failed) = match query_parser.parse_query(&effective) {
            Ok(query) => (query, false),
            Err(error) => {
                eprintln!("Query parse fell back to lenient mode: {error}");
                let (lenient, _warnings) = query_parser.parse_query_lenient(&effective);
                (lenient, true)
            }
        };

        let (mut top_docs, mut match_count) = searcher.search(
            &query,
            &(
                TopDocs::with_limit(limit)
                    .and_offset(offset)
                    .order_by_score(),
                Count,
            ),
        )?;

        // A lenient parse can discard every clause (e.g. `10:30` becomes an
        // `EmptyQuery`). Retry the raw text as plain terms so a document that
        // literally contains it is still found instead of a silent zero.
        if parse_failed && match_count == 0 {
            if let Some(fallback) = plain_text_fallback_query(
                &self.index,
                &[self.title_field, self.body_field],
                &effective,
            ) {
                (top_docs, match_count) = searcher.search(
                    &fallback,
                    &(
                        TopDocs::with_limit(limit)
                            .and_offset(offset)
                            .order_by_score(),
                        Count,
                    ),
                )?;
            }
        }
        let total_hits = match_count;

        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;

            let id = retrieved_doc
                .get_first(self.id_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let title = retrieved_doc
                .get_first(self.title_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let mut url = retrieved_doc
                .get_first(self.url_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            if url.starts_with("local://") {
                url = derive_clean_url(&title);
            }

            let body = retrieved_doc
                .get_first(self.body_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let raw_snippet = clean_raw_text(body);
            // The stored body often repeats the title; drop it only when it
            // is a separate leading token, never gluing two words together.
            let body_text = match raw_snippet.strip_prefix(&title) {
                Some(rest)
                    if rest.chars().next().is_some_and(|ch| ch.is_whitespace()) =>
                {
                    rest.trim_start()
                }
                _ => raw_snippet.as_str(),
            };
            // Cut by characters, never by bytes: byte-offset slicing panics
            // on multi-byte Arabic/CJK text.
            let snippet = if body_text.is_empty() {
                title.clone()
            } else {
                query_centered_snippet(body_text, query_str, 240)
            };

            hits.push(TantivySearchHit {
                id,
                title,
                url,
                snippet,
                score,
            });
        }

        Ok(TantivySearchResult {
            hits,
            total_hits,
            query: query_str.to_string(),
        })
    }

    /// Collects the corpus vocabulary with summed document frequencies over
    /// the title and body fields. Feeds the spell checker's candidate words.
    pub fn vocabulary(&self) -> Result<Vec<(String, u64)>> {
        let searcher = self.reader.searcher();
        let mut vocabulary: HashMap<String, u64> = HashMap::new();
        for segment_reader in searcher.segment_readers() {
            for field in [self.title_field, self.body_field] {
                let inverted_index = segment_reader.inverted_index(field)?;
                let mut stream = inverted_index.terms().stream()?;
                while stream.advance() {
                    let Ok(word) = std::str::from_utf8(stream.key()) else {
                        continue;
                    };
                    *vocabulary.entry(word.to_owned()).or_default() +=
                        u64::from(stream.value().doc_freq);
                }
            }
        }
        Ok(vocabulary.into_iter().collect())
    }

    /// Number of documents currently in the index.
    pub fn document_count(&self) -> usize {
        usize::try_from(self.reader.searcher().num_docs()).unwrap_or_default()
    }

    /// High performance batch ingestion of large JSONL files (like training_data.jsonl)
    pub fn index_jsonl(&self, path: &Path, max_records: Option<usize>) -> Result<usize> {
        let file = fs::File::open(path)?;
        let reader = BufReader::with_capacity(1024 * 1024, file);
        let mut count = 0;

        let writer = self
            .writer
            .lock()
            .map_err(|e| anyhow::anyhow!("writer lock poisoned: {}", e))?;

        for line_res in reader.lines() {
            let line = line_res?;
            if line.trim().is_empty() {
                continue;
            }

            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&line) {
                let title = json_val
                    .get("metadata")
                    .and_then(|m| m.get("title"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("Untitled");

                let text = json_val
                    .get("text")
                    .and_then(|t| t.as_str())
                    .unwrap_or_default();

                let clean_content = clean_raw_text(text);
                if clean_content.is_empty() {
                    continue;
                }

                // Content-hash ids make re-imports idempotent: the same
                // record replaces itself instead of duplicating.
                let id = hex_string(&Sha256::digest(clean_content.as_bytes()));
                let url = derive_clean_url(title);

                let mut doc = TantivyDocument::default();
                doc.add_text(self.id_field, &id);
                doc.add_text(self.title_field, title);
                doc.add_text(self.url_field, &url);
                doc.add_text(self.body_field, &clean_content);

                writer.delete_term(Term::from_field_text(self.id_field, &id));
                writer.add_document(doc)?;
                count += 1;

                if let Some(limit) = max_records {
                    if count >= limit {
                        break;
                    }
                }
            }
        }

        drop(writer);
        self.commit()?;
        Ok(count)
    }

    /// Index clean universities dataset
    pub fn index_universities_json(&self, path: &Path, max_records: Option<usize>) -> Result<usize> {
        let file_content = fs::read_to_string(path)?;
        let items: Vec<serde_json::Value> = serde_json::from_str(&file_content)?;
        let mut count = 0;

        let writer = self
            .writer
            .lock()
            .map_err(|e| anyhow::anyhow!("writer lock poisoned: {}", e))?;

        for item in &items {
            let name = item.get("name").and_then(|v| v.as_str()).unwrap_or_default();
            let country = item.get("country").and_then(|v| v.as_str()).unwrap_or_default();
            let main_url = item.get("main_url").and_then(|v| v.as_str()).unwrap_or_default();
            let domain = item.get("domains").and_then(|v| v.as_str()).unwrap_or_default();
            if name.is_empty() || main_url.is_empty() {
                continue;
            }

            let title = format!("{} - Official University Portal ({})", name, country);
            let body = format!(
                "{} is a premier university located in {}. Official website: {}. Academic degree programs, admissions, faculty research, campus life, and student resources.",
                name, country, domain
            );

            let mut doc = TantivyDocument::default();
            doc.add_text(self.id_field, main_url);
            doc.add_text(self.title_field, &title);
            doc.add_text(self.url_field, main_url);
            doc.add_text(self.body_field, &body);

            writer.delete_term(Term::from_field_text(self.id_field, main_url));
            writer.add_document(doc)?;
            count += 1;

            if let Some(limit) = max_records {
                if count >= limit {
                    break;
                }
            }
        }

        drop(writer);
        self.commit()?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each test gets its own directory: two engines on one directory would
    /// contend for the writer lock.
    fn test_engine(tag: &str) -> (TantivyEngine, PathBuf) {
        let dir = std::env::temp_dir().join(format!("tantivy_engine_{tag}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        (TantivyEngine::open_or_create(&dir).expect("engine"), dir)
    }

    #[test]
    fn test_tantivy_index_and_search() {
        let (engine, dir) = test_engine("basic");

        engine
            .add_document(
                "doc_1",
                "Rust Big Data Search Engine",
                "https://example.com/rust",
                "Tantivy is a full-text search engine library written in Rust inspired by Apache Lucene.",
            )
            .expect("add doc 1");

        engine
            .add_document(
                "doc_2",
                "Google Architecture and C++",
                "https://example.com/google",
                "Large search engines use C++ and Rust for distributed crawling, inverted index, and Big Data.",
            )
            .expect("add doc 2");

        engine.commit().expect("commit");

        let res = engine.search("Tantivy", 10, 0).expect("search");
        assert_eq!(res.total_hits, 1);
        assert_eq!(res.hits[0].id, "doc_1");

        let res2 = engine.search("search engines", 10, 0).expect("search 2");
        assert!(res2.total_hits >= 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snippet_truncation_never_splits_utf8_characters() {
        let (engine, dir) = test_engine("snippet");

        let body = "كلمة ".repeat(200);
        engine
            .add_document("u1", "عنوان عربي", "https://a.test", &body)
            .expect("add");
        engine.commit().expect("commit");

        let res = engine
            .search("كلمة", 5, 0)
            .expect("search must not panic on a long Arabic body");
        assert!(!res.hits.is_empty());
        for hit in &res.hits {
            assert!(hit.snippet.chars().count() <= 243);
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snippet_centers_on_the_query_instead_of_the_navigation_menu() {
        let (engine, dir) = test_engine("snippet_context");
        let title = "Collaborating Institutions | MIT - Massachusetts Institute of Technology";
        let body = format!(
            "{title} Skip to content ↓ Menu Explore websites, people, and locations {} Harvard research partners",
            "filler ".repeat(80)
        );
        engine
            .add_document("u1", title, "https://mit.test", &body)
            .expect("add");
        engine.commit().expect("commit");

        let res = engine.search("harvard", 5, 0).expect("search");
        assert!(!res.hits.is_empty());
        let snippet = &res.hits[0].snippet;
        assert!(snippet.contains("Harvard"));
        assert!(!snippet.contains("Skip to content"));
        assert!(!snippet.starts_with("Collaborating Institutions"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snippet_starts_at_a_natural_clause_boundary() {
        let text = format!(
            "Best Movies on Paramount+Find your next movie. FiltersRelease YearReset {} On a fall night in 2003, Harvard undergrad and computer programming genius",
            "filler ".repeat(30)
        );
        let snippet = query_centered_snippet(&text, "harvard", 240);
        assert!(
            snippet.trim_start_matches('…').starts_with("Harvard undergrad"),
            "{snippet}"
        );
    }

    #[test]
    fn snippet_never_starts_in_the_middle_of_a_word() {
        let text = format!("{}Harvard research partners", "x".repeat(300));
        let snippet = query_centered_snippet(&text, "harvard", 240);
        assert!(
            snippet
                .trim_start_matches('…')
                .starts_with("Harvard research partners"),
            "{snippet}"
        );
    }

    #[test]
    fn clean_raw_text_drops_html_blocks_and_keeps_word_spacing() {
        let cleaned = clean_raw_text(
            "<header>Menu About</header><p>Hello <b>world</b></p><script>alert(1)</script>",
        );
        assert_eq!(cleaned, "Hello world");
        assert_eq!(clean_raw_text("one<br>two"), "one two");
    }

    #[test]
    fn clean_raw_text_drops_plain_text_navigation_after_skip_link() {
        let cleaned = clean_raw_text(
            "MIT Values | MIT - Massachusetts Institute of Technology Skip to content ↓ Menu Explore websites, people, and locations Look up people by “last name sounds like” What are you looking for? See More Results Suggestions or feedback?",
        );
        assert_eq!(
            cleaned,
            "MIT Values | MIT - Massachusetts Institute of Technology"
        );
    }

    #[test]
    fn clean_raw_text_keeps_menu_wording_in_normal_articles() {
        assert_eq!(
            clean_raw_text("Restaurant menu with seasonal prices"),
            "Restaurant menu with seasonal prices"
        );
    }

    #[test]
    fn readding_the_same_document_updates_instead_of_duplicating() {
        let (engine, dir) = test_engine("upsert");

        engine
            .add_document("u1", "First title", "https://a.test", "first body about rust")
            .expect("add 1");
        engine.commit().expect("commit");
        engine
            .add_document(
                "u1",
                "Second title",
                "https://a.test",
                "second body about rust indexing",
            )
            .expect("add 2");
        engine.commit().expect("commit");

        assert_eq!(engine.document_count(), 1);
        let res = engine.search("second", 5, 0).expect("search");
        assert_eq!(res.total_hits, 1);
        assert_eq!(res.hits[0].title, "Second title");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn strict_parse_failure_falls_back_to_lenient() {
        let (engine, dir) = test_engine("lenient");

        engine
            .add_document("u1", "Schedule", "https://a.test", "class starts at 10:30 sharp")
            .expect("add");
        engine.commit().expect("commit");

        let res = engine.search("10:30", 10, 0).expect("lenient fallback");
        assert_eq!(res.total_hits, 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn diacritic_queries_match_after_normalization_and_stored_text_stays_raw() {
        let (engine, dir) = test_engine("diacritics");

        engine
            .add_document("u1", "عنوان", "https://a.test", "مُحَرِّك البحث السريع")
            .expect("add");
        engine.commit().expect("commit");

        let res = engine.search("محرك", 5, 0).expect("search");
        assert_eq!(res.total_hits, 1);
        // The stored body keeps the original spelling (snippets are raw).
        assert_eq!(res.hits[0].snippet, "مُحَرِّك البحث السريع");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn plain_bodies_keep_literal_content_marker() {
        let (engine, dir) = test_engine("content_marker");

        engine
            .add_document(
                "u1",
                "Notes",
                "https://a.test",
                "Intro text. Content: details here. More follows.",
            )
            .expect("add");
        engine.commit().expect("commit");

        let res = engine.search("details", 5, 0).expect("search");
        assert_eq!(res.total_hits, 1);
        assert!(res.hits[0].snippet.contains("Content: details here"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn total_hits_counts_all_matches_not_window() {
        let (engine, dir) = test_engine("count");

        for index in 0..3 {
            engine
                .add_document(&format!("u{index}"), "Rust doc", "https://a.test", "rust searching body")
                .expect("add");
        }
        engine.commit().expect("commit");

        let res = engine.search("rust", 1, 0).expect("search");
        assert_eq!(res.total_hits, 3);
        assert_eq!(res.hits.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn local_url_derivation_gates_words_and_is_stable() {
        // "limits" must not trip the whole-word "mit" gate.
        let limit_url = derive_clean_url("Admission limits for programs");
        assert!(!limit_url.contains("hr.mit.edu"));

        // Same title always maps to the same URL, independent of position.
        assert_eq!(derive_clean_url("MIT careers"), derive_clean_url("MIT careers"));
        assert!(derive_clean_url("MIT careers").contains("hr.mit.edu"));
    }

    #[test]
    fn index_jsonl_reimport_is_idempotent() {
        let (engine, dir) = test_engine("jsonl");

        let path = dir.join("training.jsonl");
        fs::write(
            &path,
            r#"{"metadata": {"title": "Rust doc"}, "text": "body one about rust"}"#,
        )
        .expect("write jsonl");

        engine.index_jsonl(&path, None).expect("first import");
        let first = engine.document_count();
        engine.index_jsonl(&path, None).expect("second import");

        assert_eq!(first, 1);
        assert_eq!(engine.document_count(), 1);

        let _ = fs::remove_dir_all(&dir);
    }
}
