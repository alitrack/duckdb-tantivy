//! Pure full-text search and text-analysis logic (no Arrow): the tantivy
//! language registry, tokenization/stemming, and an ephemeral in-memory BM25
//! index over a small document corpus.
//!
//! Everything here operates on plain Rust `&str`/`String` values; the
//! binding layer in `lib.rs` is a thin adapter over these functions.
//!
//! ## Ephemeral-index semantics
//! Every search/scoring call builds a brand-new tantivy index in a **RAM
//! directory** ([`Index::create_in_ram`]), loads the supplied documents, runs the
//! query, and drops the index when the call returns. Nothing is ever written to
//! disk or persisted across calls — there is no shared/incremental index. This
//! keeps the extension stateless and the corpus fully caller-controlled, at the cost
//! of rebuilding the index per call (fine for the modest corpora these functions
//! are designed for; see [`MAX_DOCS`] / [`MAX_TEXT_BYTES`]).
//!
//! ## Robustness
//! All input is bounded ([`MAX_DOCS`], [`MAX_TEXT_BYTES`]) and treated purely as
//! text (never executed). Malformed JSON, unknown languages and malformed queries
//! surface as clear errors; empty corpora / empty queries yield no results rather
//! than panicking. The extension never panics on caller input.
//!
//! ## Persistent indexes (v0.3)
//! On top of the ephemeral surface, this module also builds **named on-disk
//! (mmap-backed) indexes** ([`PersistentIndex`]): `create_persistent` builds
//! one from a corpus in a directory the caller owns, `open_persistent`
//! reopens it read-only, and `persistent_search` queries it with EXACTLY the
//! same semantics and tie-break contract as [`bm25_search`] — so a persistent
//! index and a fresh ephemeral search over the same corpus return identical
//! `(doc_id, score)` rows (the v0.3 cross-check guarantee).

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use jieba_rs::Jieba;
use tantivy::collector::TopDocs;
#[cfg(not(target_arch = "wasm32"))]
use tantivy::directory::MmapDirectory;
#[cfg(target_arch = "wasm32")]
use tantivy::directory::RamDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, STORED};
use tantivy::Term;
use tantivy::tokenizer::{
    Language as TvLanguage, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer,
    Token, TokenStream, Tokenizer,
};
use tantivy::{doc, Index, IndexReader, Searcher, TantivyDocument};
use unicode_normalization::UnicodeNormalization;

/// NFC-normalize `s`. Both documents and queries are normalized with this before
/// they reach the tokenizer, so canonically-equivalent text (precomposed `café`
/// vs decomposed `cafe\u{0301}`) yields identical tokens and can match. Without
/// this, the same visible word could fail to match purely on its Unicode
/// normalization form.
fn nfc(s: &str) -> String {
    s.nfc().collect()
}

/// Upper bound on the number of documents we will index in a single call. Far
/// larger than the per-call corpora these functions target, but small enough to
/// keep the ephemeral RAM index cheap and guard against pathological payloads.
pub const MAX_DOCS: usize = 1_000_000;

/// Upper bound (bytes) on any single text value (a document body or a query),
/// guarding against pathological input. 16 MiB dwarfs any realistic document.
pub const MAX_TEXT_BYTES: usize = 16 * 1024 * 1024;

/// Heap budget handed to the tantivy index writer (bytes). The minimum tantivy
/// accepts comfortably; our corpora are small and committed in one shot.
const WRITER_HEAP_BYTES: usize = 15_000_000;

/// The name under which our per-language analyzer is registered on each index.
const ANALYZER: &str = "vgi_stem";

/// Errors from the analysis / search surface. All map to a clear DuckDB error.
/// `PartialEq` is derived so tests (and callers) can compare `Result`s directly;
/// every variant wraps a `String`, so structural equality is well-defined.
#[derive(Debug, PartialEq)]
pub enum SearchError {
    /// The language id is not a supported Snowball stemmer language.
    UnknownLanguage(String),
    /// The `docs_json` payload was not a JSON array of strings / {id,text}.
    BadDocsJson(String),
    /// The query string could not be parsed by tantivy's query parser.
    BadQuery(String),
    /// Input exceeded a size/count bound.
    TooLarge(String),
    /// An unexpected tantivy/internal error.
    Internal(String),
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchError::UnknownLanguage(l) => write!(
                f,
                "unknown stemmer language '{l}'; see supported_languages()"
            ),
            SearchError::BadDocsJson(e) => write!(f, "invalid docs_json: {e}"),
            SearchError::BadQuery(e) => write!(f, "invalid query: {e}"),
            SearchError::TooLarge(e) => write!(f, "input too large: {e}"),
            SearchError::Internal(e) => write!(f, "internal search error: {e}"),
        }
    }
}

impl std::error::Error for SearchError {}

type Result<T> = std::result::Result<T, SearchError>;

/// The canonical supported language identifiers, in a stable order. Drives
/// `supported_languages()` and the README table. The 18 Snowball stemmer
/// languages plus `chinese` (jieba — no stemmer, see [`Lang::Chinese`]).
pub const SUPPORTED_LANGUAGES: &[&str] = &[
    "arabic",
    "chinese",
    "danish",
    "dutch",
    "english",
    "finnish",
    "french",
    "german",
    "greek",
    "hungarian",
    "italian",
    "norwegian",
    "portuguese",
    "romanian",
    "russian",
    "spanish",
    "swedish",
    "tamil",
    "turkish",
];

/// A resolved search language. Snowball languages carry a tantivy stemmer;
/// Chinese is a tokenization-only language (jieba word segmentation +
/// lowercase, no stemmer — Chinese has no word stems).
#[derive(Debug, Clone, Copy)]
pub enum Lang {
    Snowball(TvLanguage),
    Chinese,
}

/// Resolve a (case-insensitive) language id to a [`Lang`].
/// `None`/empty selects English by convention; unknown ids are an error.
fn resolve_language(name: &str) -> Result<Lang> {
    let n = name.trim().to_ascii_lowercase();
    let lang = match n.as_str() {
        "arabic" | "ar" => Lang::Snowball(TvLanguage::Arabic),
        "danish" | "da" => Lang::Snowball(TvLanguage::Danish),
        "dutch" | "nl" => Lang::Snowball(TvLanguage::Dutch),
        "english" | "en" => Lang::Snowball(TvLanguage::English),
        "finnish" | "fi" => Lang::Snowball(TvLanguage::Finnish),
        "french" | "fr" => Lang::Snowball(TvLanguage::French),
        "german" | "de" => Lang::Snowball(TvLanguage::German),
        "greek" | "el" => Lang::Snowball(TvLanguage::Greek),
        "hungarian" | "hu" => Lang::Snowball(TvLanguage::Hungarian),
        "italian" | "it" => Lang::Snowball(TvLanguage::Italian),
        "norwegian" | "no" | "nb" => Lang::Snowball(TvLanguage::Norwegian),
        "portuguese" | "pt" => Lang::Snowball(TvLanguage::Portuguese),
        "romanian" | "ro" => Lang::Snowball(TvLanguage::Romanian),
        "russian" | "ru" => Lang::Snowball(TvLanguage::Russian),
        "spanish" | "es" => Lang::Snowball(TvLanguage::Spanish),
        "swedish" | "sv" => Lang::Snowball(TvLanguage::Swedish),
        "tamil" | "ta" => Lang::Snowball(TvLanguage::Tamil),
        "turkish" | "tr" => Lang::Snowball(TvLanguage::Turkish),
        // Chinese: no Snowball stemmer exists for it; jieba segments words and
        // the LowerCaser filter handles the Latin parts of mixed text.
        "chinese" | "zh" | "\u{4e2d}\u{6587}" => Lang::Chinese,
        _ => return Err(SearchError::UnknownLanguage(name.to_string())),
    };
    Ok(lang)
}

/// The `lang` column value `supported_languages()` reports for each language:
/// plain id for Snowball languages, annotated with the segmentation engine
/// for tokenization-only languages (e.g. `chinese (jieba)`).
pub fn language_display(lang: &str) -> &str {
    if lang == "chinese" {
        "chinese (jieba)"
    } else {
        lang
    }
}

/// Build the default analyzer: a simple unicode-word tokenizer, lowercased, with
/// over-long tokens removed (no stemming). Mirrors tantivy's `default` pipeline.
fn default_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build()
}

/// Build a stemming analyzer for `lang`: tokenize, lowercase, drop long tokens,
/// then Snowball-stem. Mirrors tantivy's `{lang}_stem` pipeline.
fn stem_analyzer(lang: TvLanguage) -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(Stemmer::new(lang))
        .build()
}

/// Process-wide jieba instance. The embedded dictionary (~349k entries) is
/// decompressed and loaded into the cedar trie exactly ONCE per process; every
/// [`JiebaTokenizer`] — and thus every analyzer clone tantivy builds for its
/// writer/segment/query-parser paths — shares this single instance behind an
/// `Arc`. This mirrors upstream `tantivy-jieba`, which holds a process-global
/// `Jieba` in a `lazy_static!`. Without the `OnceLock` each `chinese`-language
/// call would rebuild the whole dictionary (measured: +20 MB RSS, tens of ms
/// per call), and the scalar paths (`tokenize`/`stem`/`bm25_score`) pay it
/// per row.
static JIEBA: OnceLock<Arc<Jieba>> = OnceLock::new();

/// Fetch the process-wide [`Jieba`], loading the embedded dictionary on first
/// use only.
fn shared_jieba() -> Arc<Jieba> {
    JIEBA.get_or_init(|| Arc::new(Jieba::new())).clone()
}

/// A tantivy [`Tokenizer`] backed by jieba-rs: Chinese word segmentation with
/// the HMM path for out-of-dictionary words. `cut` returns slices of the
/// input; whitespace/punctuation-only fragments are dropped (jieba passes
/// them through) and the kept slices carry real byte offsets into the input,
/// computed by pointer difference in [`JiebaTokenStream::advance`]. The
/// dictionary itself is loaded once per process (see [`JIEBA`]).
struct JiebaTokenizer {
    jieba: Arc<Jieba>,
}

impl Clone for JiebaTokenizer {
    fn clone(&self) -> Self {
        JiebaTokenizer {
            jieba: Arc::clone(&self.jieba),
        }
    }
}

impl JiebaTokenizer {
    fn new() -> Self {
        JiebaTokenizer {
            jieba: shared_jieba(),
        }
    }
}

struct JiebaTokenStream<'a> {
    text: &'a str,
    words: std::vec::IntoIter<&'a str>,
    position: usize,
    token: Token,
}

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        // Keep slices carrying at least one alphanumeric character; drops the
        // whitespace/punctuation fragments jieba emits between words. `cut`
        // returns slices of `text`, so the kept items stay borrows of the
        // input — pointer difference in `advance` below is a real offset.
        let words: Vec<&'a str> = self
            .jieba
            .cut(text, true)
            .into_iter()
            .filter(|w| w.chars().any(|c| c.is_alphanumeric()))
            .collect();
        JiebaTokenStream {
            text,
            words: words.into_iter(),
            position: 0,
            token: Token {
                offset_from: 0,
                offset_to: 0,
                position: 0,
                text: String::new(),
                position_length: 1,
            },
        }
    }
}

impl<'a> TokenStream for JiebaTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.reset();
        let Some(word) = self.words.next() else {
            return false;
        };
        // Real offsets: jieba's `cut` returns slices of the input text, so the
        // pointer difference is the token's true byte position in the original
        // input (same technique as tantivy-jieba). Offsets therefore always
        // fall on char boundaries and `&text[offset_from..offset_to]` slices
        // back to exactly the token text — the tantivy Tokenizer contract.
        self.token.offset_from = word.as_ptr() as usize - self.text.as_ptr() as usize;
        self.token.offset_to = self.token.offset_from + word.len();
        self.token.position = self.position;
        self.token.position_length = 1;
        self.position += 1;
        self.token.text.push_str(word);
        true
    }
    fn token(&self) -> &Token {
        &self.token
    }
    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

/// Build the Chinese analyzer: jieba word segmentation, drop over-long tokens,
/// lowercase (harmless for Han characters, lowercases the Latin/digit parts of
/// mixed text). No stemmer — Chinese has no word stems.
fn chinese_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(JiebaTokenizer::new())
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .build()
}

/// The analyzer pipeline for a resolved [`Lang`]: Snowball stemmer for the 18
/// stemming languages, jieba segmentation for Chinese.
fn analyzer_for(lang: &Lang) -> TextAnalyzer {
    match lang {
        Lang::Snowball(l) => stem_analyzer(*l),
        Lang::Chinese => chinese_analyzer(),
    }
}

/// Collect the tokens produced by an analyzer over `text`. The text is
/// NFC-normalized first (and the same normalization is applied to indexed
/// documents in `bm25_search`), so tokenization of canonically-equivalent
/// Unicode spellings agrees between documents and queries.
fn run_analyzer(mut analyzer: TextAnalyzer, text: &str) -> Vec<String> {
    let text = nfc(text);
    let mut out = Vec::new();
    let mut stream = analyzer.token_stream(&text);
    stream.process(&mut |t: &Token| out.push(t.text.clone()));
    out
}

/// Tokenize `text` with the default (non-stemming) analyzer: unicode word
/// splitting + lowercasing. Empty text → empty vector.
pub fn tokenize_default(text: &str) -> Result<Vec<String>> {
    check_text_len(text)?;
    Ok(run_analyzer(default_analyzer(), text))
}

/// Tokenize `text` with the stemming analyzer for `lang`. Empty text → empty
/// vector; unknown language → error.
pub fn tokenize_lang(text: &str, lang: &str) -> Result<Vec<String>> {
    check_text_len(text)?;
    let l = resolve_language(lang)?;
    Ok(run_analyzer(analyzer_for(&l), text))
}

/// Reduce a single `word` for `lang`: Snowball-stemmed for the 18 stemmer
/// languages. `chinese` has no word stems, so the input is returned
/// UNCHANGED (identity) — the same "one word in, one word out" contract the
/// 18 stemmer languages honor: `stem` never splits a multi-word input and
/// never space-joins (segmenting a phrase is `tokenize`'s job, not `stem`'s).
/// Empty word → empty string; unknown language → error. E.g.
/// `stem("running","english") == "run"`, `stem("苹果手机","chinese") == "苹果手机"`.
pub fn stem(word: &str, lang: &str) -> Result<String> {
    check_text_len(word)?;
    let l = resolve_language(lang)?;
    if matches!(l, Lang::Chinese) {
        // Chinese has no word stems: identity preserves the single-word contract.
        return Ok(word.to_string());
    }
    let Lang::Snowball(tl) = l else { unreachable!("chinese handled above"); };
    let toks = run_analyzer(stem_analyzer(tl), word);
    // A lone word yields a single token; join defensively if the tokenizer split.
    Ok(toks.join(" "))
}

/// A single search hit: the caller's document id and its BM25 score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hit {
    pub doc_id: i64,
    pub score: f64,
}

/// One document to index: a caller id and its text body.
#[derive(Debug, Clone)]
pub struct Document {
    pub id: i64,
    pub text: String,
}

/// Parse the `docs_json` payload into a document list. Accepts either:
///
///   * a JSON array of strings — `["the cat sat","dogs bark"]` — where the doc id
///     is the array index (0-based); or
///   * a JSON array of objects — `[{"id":7,"text":"…"}, …]` — with an explicit
///     integer `id` and string `text`.
///
/// An empty array yields an empty corpus. A `null`/empty payload also yields an
/// empty corpus. Anything else is a [`SearchError::BadDocsJson`].
pub fn parse_docs(docs_json: &str) -> Result<Vec<Document>> {
    let trimmed = docs_json.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let value: serde_json::Value =
        serde_json::from_str(trimmed).map_err(|e| SearchError::BadDocsJson(e.to_string()))?;
    let arr = match value {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Null => return Ok(Vec::new()),
        _ => {
            return Err(SearchError::BadDocsJson(
                "expected a JSON array of strings or {id,text} objects".into(),
            ))
        }
    };
    if arr.len() > MAX_DOCS {
        return Err(SearchError::TooLarge(format!(
            "{} documents exceeds the limit of {MAX_DOCS}",
            arr.len()
        )));
    }
    let mut docs = Vec::with_capacity(arr.len());
    for (idx, item) in arr.into_iter().enumerate() {
        match item {
            serde_json::Value::String(s) => {
                check_text_len(&s)?;
                docs.push(Document {
                    id: idx as i64,
                    text: s,
                });
            }
            serde_json::Value::Object(map) => {
                let id = match map.get("id") {
                    Some(serde_json::Value::Number(n)) if n.is_i64() => n.as_i64().unwrap(),
                    Some(serde_json::Value::Number(n)) if n.is_u64() => n.as_u64().unwrap() as i64,
                    None => idx as i64,
                    _ => {
                        return Err(SearchError::BadDocsJson(format!(
                            "doc at index {idx}: 'id' must be an integer"
                        )))
                    }
                };
                let text = match map.get("text") {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    _ => {
                        return Err(SearchError::BadDocsJson(format!(
                            "doc at index {idx}: missing string 'text'"
                        )))
                    }
                };
                check_text_len(&text)?;
                docs.push(Document { id, text });
            }
            _ => {
                return Err(SearchError::BadDocsJson(format!(
                    "doc at index {idx}: expected a string or an {{id,text}} object"
                )))
            }
        }
    }
    Ok(docs)
}

/// Build an ephemeral RAM index over `docs` (analyzed with the `lang` stemmer),
/// run the BM25 `query`, and return up to `limit` hits ranked by descending
/// score (ties broken by ascending doc id for determinism). Empty corpus or
/// empty/blank query → no hits. Unknown language or unparseable query → error.
///
/// The index is created in RAM, used once, and dropped on return (see the
/// module-level *ephemeral-index semantics*).
///
/// **Tie-break contract, honored at the cutoff.** tantivy's `TopDocs` selects
/// the top-K by score but makes no stability promise for *equal* scores, so
/// `TopDocs::with_limit(K)` alone would let a small-id document lose a
/// tie at the cutoff to an arbitrary heap ordering. We therefore collect with
/// a limit large enough to keep **every** matching document
/// (`limit.max(docs.len())` — a single committed RAM segment, so one collector
/// merge), sort the full match set by `(score desc, doc_id asc)`, and only
/// then truncate to `limit`. The documented tie-break thus holds exactly,
/// and the extra work is bounded by the corpus size we already indexed.
pub fn bm25_search(docs: &[Document], query: &str, lang: &str, limit: usize) -> Result<Vec<Hit>> {
    check_text_len(query)?;
    let l = resolve_language(lang)?;    if docs.is_empty() || query.trim().is_empty() {
        return Ok(Vec::new());
    }

    // Schema: a stored i64 id + a stemmed, positional-indexed text body.
    let mut sb = Schema::builder();
    let id_field = sb.add_i64_field("id", STORED);
    let text_opts = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(ANALYZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let body_field = sb.add_text_field("body", text_opts);
    let schema = sb.build();

    let index = Index::create_in_ram(schema);
    index.tokenizers().register(ANALYZER, analyzer_for(&l));

    let mut writer = index
        .writer(WRITER_HEAP_BYTES)
        .map_err(|e| SearchError::Internal(e.to_string()))?;
    for d in docs {
        // NFC before indexing: a precomposed and a decomposed spelling of the
        // same word must tokenize identically (same for the query below).
        let text = nfc(&d.text);
        writer
            .add_document(doc!(id_field => d.id, body_field => text.as_str()))
            .map_err(|e| SearchError::Internal(e.to_string()))?;
    }
    writer
        .commit()
        .map_err(|e| SearchError::Internal(e.to_string()))?;

    let reader = index
        .reader()
        .map_err(|e| SearchError::Internal(e.to_string()))?;
    let searcher = reader.searcher();

    let mut parser = QueryParser::for_index(&index, vec![body_field]);
    parser.set_conjunction_by_default(); // AND semantics: all terms must match.
    let parsed = parser
        .parse_query(&sanitize_query(&nfc(query)))
        .map_err(|e| SearchError::BadQuery(e.to_string()))?;

    // Collect enough to keep the full match set (see the fn doc for why a
    // bare `TopDocs::with_limit(limit)` is not enough for the tie-break).
    let collect_limit = limit.max(1).max(docs.len());
    let top = searcher
        .search(&parsed, &TopDocs::with_limit(collect_limit))
        .map_err(|e| SearchError::Internal(e.to_string()))?;

    let mut hits = Vec::with_capacity(top.len().min(limit.max(1)));
    for (score, addr) in top {
        let stored: TantivyDocument = searcher
            .doc(addr)
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        let doc_id = stored
            .get_first(id_field)
            .and_then(|v| v.as_i64())
            .ok_or_else(|| SearchError::Internal("stored doc missing id".into()))?;
        hits.push(Hit {
            doc_id,
            score: score as f64,
        });
    }
    // Deterministic tie-break over the FULL match set before truncating:
    // score desc, then doc_id asc. Truncating afterwards preserves the exact
    // (score desc, doc_id asc) top-`limit` the API promises.
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

/// Score a *single* document against `query` (`lang` stemmer) by building a
/// one-document ephemeral index — an ad-hoc relevance probe. Returns the BM25
/// score, or `0.0` if the document does not match (or the corpus/query is empty).
/// This is intentionally a 1-doc index, so scores are **not** comparable across
/// calls (BM25 statistics depend on the corpus); use `bm25_search` to rank a real
/// corpus. Unknown language / bad query → error.
pub fn bm25_score(doc_text: &str, query: &str, lang: &str) -> Result<f64> {
    check_text_len(doc_text)?;
    let docs = [Document {
        id: 0,
        text: doc_text.to_string(),
    }];
    let hits = bm25_search(&docs, query, lang, 1)?;
    Ok(hits.first().map(|h| h.score).unwrap_or(0.0))
}

/// Escape tantivy query-parser metacharacters so that a caller-supplied
/// string is searched as **plain text**: every word is a term, with no
/// field syntax (`:`), grouping (`(` `)`), operators (`+` `-` `!` `*`),
/// ranges (`>` `<` `=`), boosts (`~`), or quoting (`"`).
///
/// News headlines and user input routinely contain these (`"Enter your
/// e-mail: ..."`, `"Q2: strong quarter (40%)"`); without sanitizing they
/// turn into `BadQuery` errors (a `:` with no field before it is a syntax
/// error in the tantivy parser). The escape is character-for-character,
/// so tokenization (unicode word-split + stemmer) is untouched and only
/// the parser's special meaning is neutralized.
///
/// One subtlety: a `"` that is itself preceded by a surviving `\` is
/// *escaped* (`\"` in the output) rather than left bare — a bare `"` in
/// the query would otherwise start an unterminated phrase.
pub fn sanitize_query(q: &str) -> String {
    let mut out = String::with_capacity(q.len() + 8);
    for c in q.chars() {
        match c {
            '\\' | '"' | ':' | '(' | ')' | '+' | '-' | '!' | '=' | '>' | '<' | '~' | '*' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// The tantivy version string, e.g. `"tantivy v0.24.2, index_format v7"`.
pub fn tantivy_version() -> String {
    tantivy::version_string().to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// dryrun-style pre-execution estimate (duckdb-dryrun inspired)
// ─────────────────────────────────────────────────────────────────────────────

/// A pre-execution cost estimate for a persistent-index search. No term
/// dictionary is opened, no postings are decoded, no scoring runs: the query
/// is analyzed with the SAME chain the index was built with, and per-segment
/// term metadata is taken from `searcher.segment_readers()` ONLY where the
/// public API exposes it. Numbers are honest lower/upper bounds, with the
/// caveats spelled out in `notes` (dryrun's philosophy: degrade, never lie).
#[derive(Debug, Clone)]
pub struct Estimate {
    pub num_docs: u64,
    pub segments: u64,
    pub query_terms: Vec<String>,
    /// Estimated postings bytes this query would decode across segments.
    /// `None` when the tantivy version exposes no per-term byte sizes — the
    /// estimate then falls back to doc-frequency bounds and says so in notes.
    pub estimated_postings_bytes: Option<u64>,
    /// Total inverted-index bytes on disk (meta + store + postings), an
    /// upper bound for any single query.
    pub total_index_bytes: u64,
    /// Upper bound on candidate documents (sum of per-term doc frequencies).
    pub estimated_max_candidates: u64,
    pub confidence: Confidence,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    High,
    Medium,
    Low,
}

impl Confidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Confidence::High => "high",
            Confidence::Medium => "medium",
            Confidence::Low => "low",
        }
    }
}

/// Estimate the cost of searching `idx` with `query` WITHOUT executing the
/// search. The query is normalized exactly like `persistent_search` (NFC +
/// sanitization) and tokenized with the index's language chain, so the term
/// list is bit-for-bit what the query parser would look up.
pub fn estimate_search(idx: &PersistentIndex, query: &str) -> Result<Estimate> {
    check_text_len(query)?;
    if query.trim().is_empty() {
        return Err(SearchError::BadQuery("empty query".into()));
    }

    let searcher = &idx.searcher_handle;
    let num_docs = searcher.num_docs();
    let mut notes = Vec::new();
    let mut confidence = Confidence::High;

    // Same normalization + analysis chain as persistent_search.
    let sanitized = sanitize_query(&nfc(query));
    let terms = tokenize_lang(&sanitized, &idx.language)?;
    if terms.is_empty() {
        notes.push("query tokenizes to zero terms; no postings would be read".into());
        return Ok(Estimate {
            num_docs,
            segments: searcher.segment_readers().len() as u64,
            query_terms: terms,
            estimated_postings_bytes: Some(0),
            total_index_bytes: index_bytes_on_disk(idx),
            estimated_max_candidates: 0,
            confidence: Confidence::High,
            notes,
        });
    }
    let unique_terms: Vec<String> = {
        let mut t = terms.clone();
        t.sort();
        t.dedup();
        t
    };

    let mut postings_bytes: u64 = 0;
    let mut positions_bytes: u64 = 0;
    let mut max_candidates: u64 = 0;
    let mut missing_terms: usize = 0;

    for seg in searcher.segment_readers() {
        let inv = match seg.inverted_index(idx.body_field) {
            Ok(i) => i,
            Err(e) => return Err(SearchError::Internal(e.to_string())),
        };
        let dict = inv.terms();
        for t in &unique_terms {
            if let Ok(None) | Err(_) = dict.term_ord(t.as_bytes()) {
                // Term absent from this segment: conjunction (AND) would
                // short-circuit here — zero cost. Disjunction (OR) semantics
                // are not what persistent_search uses.
                missing_terms += 1;
                continue;
            }
            let term = Term::from_field_text(idx.body_field, t);
            if let Ok(Some(info)) = inv.get_term_info(&term) {
                max_candidates += info.doc_freq as u64;
                // EXACT bytes: TermInfo exposes the posting-list byte range in
                // the .idx file AND the positions byte range in the .pos file
                // (the index was built WithFreqsAndPositions, so a search
                // decodes both). No approximation — confidence stays high.
                postings_bytes += info.postings_range.len() as u64;
                positions_bytes += info.positions_range.len() as u64;
            }
        }
    }

    if missing_terms > 0 {
        notes.push(format!(
            "{missing_terms} term-segment pairs absent (AND short-circuits; estimate assumes present pairs)"
        ));
    }
    // postings + positions are both decoded by a WithFreqsAndPositions search;
    // report the exact combined byte cost.
    postings_bytes += positions_bytes;
    if max_candidates > num_docs {
        notes.push("max candidates exceeds num_docs (terms co-occur); conjunction prunes further".into());
    }

    Ok(Estimate {
        num_docs,
        segments: searcher.segment_readers().len() as u64,
        query_terms: unique_terms,
        estimated_postings_bytes: Some(postings_bytes),
        total_index_bytes: index_bytes_on_disk(idx),
        estimated_max_candidates: max_candidates,
        confidence,
        notes,
    })
}

/// Total bytes of the index directory on disk (meta + store + postings).
/// Read-only walk; mmap pages are not read.
fn index_bytes_on_disk(idx: &PersistentIndex) -> u64 {
    fn dir_size(p: &Path) -> u64 {
        let mut n = 0u64;
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let Ok(md) = e.metadata() else { continue };
                if md.is_file() {
                    n += md.len();
                } else if md.is_dir() {
                    n += dir_size(&e.path());
                }
            }
        }
        n
    }
    dir_size(Path::new(&idx.path))
}

/// A term from the raw-term dictionary: the lowercased, unstemmed surface form
/// and how many documents it occurs in.
#[derive(Debug, Clone, PartialEq)]
pub struct RawTerm {
    pub term: String,
    pub df: i64,
}

/// Build the **raw-term dictionary** for a corpus of text bodies: every
/// lowercased, unicode-word token (no stemming — the `default` analyzer),
/// with its document frequency (how many distinct texts contain it).
///
/// This is the correction-space counterpart of a stemmed BM25 index: typo
/// correction compares edit distances against *surface* spellings (`cruide`
/// is one edit from `crude`, but both stem to `crud`, so a stemmed dictionary
/// cannot distinguish them). Search still runs against the stemmed index —
/// the query-side stemmer maps a corrected surface form back to the indexed
/// term, so the two spaces are bridged at query time.
///
/// `df` counts documents, not occurrences (the same word three times in one
/// text still counts once) — it ranks equally-close correction candidates by
/// corpus frequency, exactly like a `raw_dict(df)` table would.
///
/// Terms are returned sorted by `(df DESC, term ASC)` so the most frequent
/// first is the one a `LIMIT 1` correction candidate picks.
///
/// Bounds: [`MAX_DOCS`] texts and [`MAX_TEXT_BYTES`] per text, same as
/// `bm25_search`; a corpus that violates them is an error, never a silent
/// truncation.
pub fn raw_term_dictionary(texts: &[&str]) -> Result<Vec<RawTerm>> {
    if texts.len() > MAX_DOCS {
        return Err(SearchError::TooLarge(format!(
            "{} texts exceeds the limit of {MAX_DOCS}",
            texts.len()
        )));
    }
    let mut df: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for t in texts {
        check_text_len(t)?;
        // Per-text seen set: df counts documents, not token occurrences.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for tok in run_analyzer(default_analyzer(), t) {
            if seen.insert(tok.clone()) {
                *df.entry(tok).or_insert(0) += 1;
            }
        }
    }
    let mut out: Vec<RawTerm> = df
        .into_iter()
        .map(|(term, df)| RawTerm { term, df })
        .collect();
    out.sort_by(|a, b| b.df.cmp(&a.df).then_with(|| a.term.cmp(&b.term)));
    Ok(out)
}

/// Enforce the per-text-value size bound.
fn check_text_len(text: &str) -> Result<()> {
    if text.len() > MAX_TEXT_BYTES {
        return Err(SearchError::TooLarge(format!(
            "{} bytes exceeds the limit of {MAX_TEXT_BYTES}",
            text.len()
        )));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Persistent (on-disk, mmap-backed) index — the v0.3 counterpart of the
// ephemeral index above.
//
// A named tantivy index that lives in a directory on disk: built ONCE from a
// corpus, then searched repeatedly without rebuilding. The binding layer
// (lib.rs) keeps one of these per name in a process-global registry and
// exposes them through the `tantivy_create_index` / `tantivy_search_index`
// / `tantivy_list` / `tantivy_save_index` / `tantivy_load_index` table
// functions.
//
//   * `create_persistent` — create or REBUILD the index in a directory the
//     caller owns (the directory is wiped first, like CREATE OR REPLACE);
//   * `open_persistent`   — reopen an existing index directory read-only;
//   * `persistent_search` — search with EXACTLY the query semantics and
//     tie-break contract of [`bm25_search`] (full match set kept, sorted
//     score-desc / doc-id-asc, then truncated at the cutoff).
//
// Provenance: every index directory carries a [`INDEX_META_FILE`] sidecar
// recording the name/language it was built with. [`open_persistent`] refuses
// directories without that sidecar (a foreign or corrupt index is a clear
// error, never a guess).
// ─────────────────────────────────────────────────────────────────────────────

/// The sidecar file (inside an index directory) recording which name/language
/// the index was built with.
pub const INDEX_META_FILE: &str = "__tantivy_meta.json";

/// A persistent, named tantivy index on disk. Cheap to share behind an `Arc`:
/// `reader` and `searcher_handle` are `Arc`-based tantivy handles, and the
/// index is IMMUTABLE after creation (a rebuild is a fresh `create_persistent`)
/// — so the stored `searcher_handle` snapshot never goes stale.
pub struct PersistentIndex {
    /// The registry key chosen by the caller (also written into the meta file).
    pub name: String,
    /// The on-disk directory this index was created in / opened from.
    pub path: String,
    /// The tantivy reader. Intentionally never read directly (all searches go
    /// through `searcher_handle`); it is retained so the index's directory
    /// handle stays alive for the handle's lifetime. (`Debug` is implemented
    /// manually below because tantivy's reader/searcher types don't derive it.
    ///)
    #[allow(dead_code)]
    pub reader: IndexReader,
    /// A searcher pinned to the committed snapshot; used for all searches.
    pub searcher_handle: Searcher,
    /// The stored i64 document-id field.
    pub id_field: Field,
    /// The analyzed text-body field.
    pub body_field: Field,
    /// The resolved (alias-normalized) language id the index was built with,
    /// e.g. `"chinese"`.
    pub language: String,
    /// Number of documents in the index — the match-set bound for the
    /// tie-break collector, exactly as in [`bm25_search`].
    pub num_docs: usize,
}

impl fmt::Debug for PersistentIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistentIndex")
            .field("name", &self.name)
            .field("path", &self.path)
            .field("language", &self.language)
            .field("num_docs", &self.num_docs)
            .finish()
    }
}

/// Conservative index-name check: a single path segment —
/// `[A-Za-z_][A-Za-z0-9_-]*`, at most 128 chars. Names double as directory
/// names, so separators, dots and traversal characters are rejected.
pub fn is_safe_index_name(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() || s.len() > 128 {
        return false;
    }
    let bytes = s.as_bytes();
    if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
        return false;
    }
    bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
}

/// The default on-disk location of a persistent index: the `TANTIVY_INDEX_DIR`
/// environment variable (if set and non-empty) plus the index name, else
/// `.tantivy_index/<name>` under the current working directory.
pub fn persistent_index_dir(name: &str) -> PathBuf {
    let base = std::env::var("TANTIVY_INDEX_DIR")
        .ok()
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(".tantivy_index").to_path_buf());
    base.join(name)
}

/// The canonical language id for a resolved [`Lang`] (alias-normalized: both
/// `"zh"` and `"CHINESE"` report as `"chinese"`).
pub fn lang_id(l: &Lang) -> &'static str {
    match l {
        Lang::Chinese => "chinese",
        Lang::Snowball(t) => match t {
            TvLanguage::Arabic => "arabic",
            TvLanguage::Danish => "danish",
            TvLanguage::Dutch => "dutch",
            TvLanguage::English => "english",
            TvLanguage::Finnish => "finnish",
            TvLanguage::French => "french",
            TvLanguage::German => "german",
            TvLanguage::Greek => "greek",
            TvLanguage::Hungarian => "hungarian",
            TvLanguage::Italian => "italian",
            TvLanguage::Norwegian => "norwegian",
            TvLanguage::Portuguese => "portuguese",
            TvLanguage::Romanian => "romanian",
            TvLanguage::Russian => "russian",
            TvLanguage::Spanish => "spanish",
            TvLanguage::Swedish => "swedish",
            TvLanguage::Tamil => "tamil",
            TvLanguage::Turkish => "turkish",
        },
    }
}

/// Recursively remove everything inside `dir` (the directory itself is kept).
fn wipe_dir(dir: &Path) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let p = entry?.path();
        if p.is_dir() {
            std::fs::remove_dir_all(&p)?;
        } else {
            std::fs::remove_file(&p)?;
        }
    }
    Ok(())
}

/// Create (or REBUILD) the persistent index `name` in `dir`, indexing all of
/// `docs` with the `lang` analyzer, and return an open handle.
///
/// `dir` is caller-owned: it is created if missing and WIPED if non-empty
/// (rebuild semantics — like `CREATE OR REPLACE`, never a silent append).
/// A [`INDEX_META_FILE`] sidecar recording name/language is written into `dir`
/// for provenance (see [`open_persistent`]).
pub fn create_persistent(
    name: &str,
    dir: &Path,
    lang: &str,
    docs: &[Document],
) -> Result<PersistentIndex> {
    if !is_safe_index_name(name) {
        return Err(SearchError::Internal(format!(
            "invalid index name '{name}' (expected a single segment like 'my_index')"
        )));
    }
    let l = resolve_language(lang)?;
    if docs.len() > MAX_DOCS {
        return Err(SearchError::TooLarge(format!(
            "{} documents exceeds the limit of {MAX_DOCS}",
            docs.len()
        )));
    }

    wipe_dir(dir).map_err(|e| {
        SearchError::Internal(format!(
            "cannot prepare index directory '{}': {e}",
            dir.display()
        ))
    })?;
    std::fs::create_dir_all(dir).map_err(|e| {
        SearchError::Internal(format!(
            "cannot create index directory '{}': {e}",
            dir.display()
        ))
    })?;

    // Same schema shape as `bm25_search`: a stored i64 id + an analyzed body.
    // Built unconditionally (both targets read id_field/body_field below).
    let mut sb = Schema::builder();
    let id_field = sb.add_i64_field("id", STORED);
    let text_opts = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(ANALYZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let body_field = sb.add_text_field("body", text_opts);
    let schema = sb.build();

    #[cfg(target_arch = "wasm32")]
    {
        // No threads on wasm32 (single-threaded emscripten side module): the
        // regular IndexWriter needs a segment-updater thread pool and the
        // default reader policy needs a watcher thread. Use the threadless
        // single-segment writer and a manual-reload reader instead.
        let index = Index::create_in_ram(schema);
        index.tokenizers().register(ANALYZER, analyzer_for(&l));
        let mut writer =
            tantivy::SingleSegmentIndexWriter::new(index, WRITER_HEAP_BYTES)
                .map_err(|e| SearchError::Internal(format!("[wasm-step writer-new] {e}")))?;
        for d in docs {
            let text = nfc(&d.text);
            writer
                .add_document(doc!(id_field => d.id, body_field => text.as_str()))
                .map_err(|e| SearchError::Internal(format!("[wasm-step add-doc] {e}")))?;
        }
        let index = writer
            .finalize()
            .map_err(|e| SearchError::Internal(format!("[wasm-step finalize] {e}")))?;

        let reader: IndexReader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .map_err(|e: tantivy::TantivyError| SearchError::Internal(format!("[wasm-step reader] {e}")))?;

        // Provenance sidecar lives in the RAM directory (no fs on wasm32).
        let meta = format!(
            "{{\"name\":{},\"language\":{}}}",
            serde_json::to_string(name).map_err(|e| SearchError::Internal(e.to_string()))?,
            serde_json::to_string(lang_id(&l)).map_err(|e| SearchError::Internal(e.to_string()))?
        );
        use tantivy::directory::Directory;
        index
            .directory()
            .atomic_write(std::path::Path::new(INDEX_META_FILE), meta.as_bytes())
            .map_err(|e| SearchError::Internal(e.to_string()))?;

        let searcher = reader.searcher();
        let num_docs = searcher.num_docs() as usize;
        return Ok(PersistentIndex {
            name: name.to_string(),
            path: dir.to_string_lossy().into_owned(),
            reader,
            searcher_handle: searcher,
            id_field,
            body_field,
            language: lang_id(&l).to_string(),
            num_docs,
        });
    }
    #[cfg(not(target_arch = "wasm32"))]
    let mmap = MmapDirectory::open(dir).map_err(|e| {
        SearchError::Internal(format!(
            "cannot mmap index directory '{}': {e}",
            dir.display()
        ))
    })?;

    #[cfg(not(target_arch = "wasm32"))]
    let index = Index::open_or_create(mmap, schema).map_err(|e| {
        SearchError::Internal(format!(
            "cannot open index in '{}': {e}",
            dir.display()
        ))
    })?;
    #[cfg(not(target_arch = "wasm32"))]
    {
    index.tokenizers().register(ANALYZER, analyzer_for(&l));

    let mut writer = index
        .writer(WRITER_HEAP_BYTES)
        .map_err(|e| SearchError::Internal(e.to_string()))?;
    for d in docs {
        // NFC before indexing (same contract as bm25_search).
        let text = nfc(&d.text);
        writer
            .add_document(doc!(id_field => d.id, body_field => text.as_str()))
            .map_err(|e| SearchError::Internal(e.to_string()))?;
    }
    writer
        .commit()
        .map_err(|e| SearchError::Internal(e.to_string()))?;
    drop(writer); // release the directory lockfile before reopening read-only

    let reader = index
        .reader()
        .map_err(|e| SearchError::Internal(e.to_string()))?;
    let searcher = reader.searcher();

    let num_docs = searcher.num_docs() as usize;

    // Provenance sidecar: a failure here fails the whole create (kept
    // symmetric with `open_persistent`, which requires the file).
    let meta = format!(
        "{{\"name\":{},\"language\":{}}}",
        serde_json::to_string(name).map_err(|e| SearchError::Internal(e.to_string()))?,
        serde_json::to_string(lang_id(&l)).map_err(|e| SearchError::Internal(e.to_string()))?
    );
    std::fs::write(dir.join(INDEX_META_FILE), meta).map_err(|e| {
        SearchError::Internal(format!(
            "cannot write {INDEX_META_FILE} in '{}': {e}",
            dir.display()
        ))
    })?;

    Ok(PersistentIndex {
        name: name.to_string(),
        path: dir.to_string_lossy().into_owned(),
        reader,
        searcher_handle: searcher,
        id_field,
        body_field,
        language: lang_id(&l).to_string(),
        num_docs,
    })
    }
}

/// Reopen an existing persistent index from `dir` (read-only) and return a
/// handle registered under `name`. Errors clearly when `dir` holds no
/// tantivy index, when the schema lacks the `id`/`body` fields, or when the
/// provenance sidecar is missing (foreign/corrupt directory).
pub fn open_persistent(name: &str, dir: &Path) -> Result<PersistentIndex> {
    if !is_safe_index_name(name) {
        return Err(SearchError::Internal(format!(
            "invalid index name '{name}' (expected a single segment like 'my_index')"
        )));
    }
    // No filesystem on wasm32: persistent named indexes are a native-only
    // feature; wasm callers get a clear error instead of a panic.
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (name, dir);
        return Err(SearchError::Internal(
            "persistent on-disk indexes are not available in wasm builds; \
             create an in-RAM index with tantivy_create_index (no path) instead"
                .to_string(),
        ));
    }
    #[cfg(target_arch = "wasm32")]
    let index: Index = {
        unreachable!("rejected above on wasm32")
    };
    #[cfg(not(target_arch = "wasm32"))]
    let index = Index::open_in_dir(dir).map_err(|e| {
        SearchError::Internal(format!(
            "no tantivy index found at '{}': {e}",
            dir.display()
        ))
    })?;
    // Provenance first: a directory without our sidecar is a foreign/corrupt
    // index, and that is the most diagnostic error to surface.
    let meta_text = std::fs::read_to_string(dir.join(INDEX_META_FILE)).map_err(|_| {
        SearchError::Internal(format!(
            "index at '{}' is missing its {INDEX_META_FILE} provenance file; only \n             indexes created by tantivy_create_index can be loaded",
            dir.display()
        ))
    })?;
    let meta: serde_json::Value = serde_json::from_str(&meta_text).map_err(|e| {
        SearchError::Internal(format!(
            "corrupt {INDEX_META_FILE} at '{}': {e}",
            dir.display()
        ))
    })?;
    let language = meta
        .get("language")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SearchError::Internal(format!(
                "corrupt {INDEX_META_FILE} at '{}' (no language)",
                dir.display()
            ))
        })?
        .to_string();
    // A reopened Index carries an EMPTY tokenizer manager (managers are
    // per-process and never persisted) — re-register the build-time analyzer
    // or the QueryParser cannot resolve the body field's tokenizer.
    let l = resolve_language(&language)?;
    index.tokenizers().register(ANALYZER, analyzer_for(&l));

    let schema = index.schema();
    let id_field = schema.get_field("id").map_err(|e| {
        SearchError::Internal(format!(
            "index at '{}' has no 'id' field ({e}); only indexes created by \n             tantivy_create_index can be loaded",
            dir.display()
        ))
    })?;
    let body_field = schema.get_field("body").map_err(|e| {
        SearchError::Internal(format!(
            "index at '{}' has no 'body' field ({e}); only indexes created by \n             tantivy_create_index can be loaded",
            dir.display()
        ))
    })?;

    let reader = index
        .reader()
        .map_err(|e| SearchError::Internal(e.to_string()))?;
    let searcher = reader.searcher();
    let num_docs = searcher.num_docs() as usize;

    Ok(PersistentIndex {
        name: name.to_string(),
        path: dir.to_string_lossy().into_owned(),
        reader,
        searcher_handle: searcher,
        id_field,
        body_field,
        language,
        num_docs,
    })
}

/// Search a persistent index. Same query semantics as [`bm25_search`]: NFC +
/// metacharacter-sanitized, conjunction (AND) by default, and the documented
/// tie-break honored at the cutoff (the full match set is kept, sorted score
/// desc / doc id asc, then truncated to `limit`) — so results are
/// bit-identical to an ephemeral `bm25_search` over the same corpus with the
/// same parameters. Empty/blank query → no hits; malformed query →
/// [`SearchError::BadQuery`].
pub fn persistent_search(idx: &PersistentIndex, query: &str, limit: usize) -> Result<Vec<Hit>> {
    check_text_len(query)?;
    if query.trim().is_empty() {
        return Ok(Vec::new());
    }

    let searcher = &idx.searcher_handle;
    let mut parser = QueryParser::for_index(searcher.index(), vec![idx.body_field]);
    parser.set_conjunction_by_default(); // AND semantics: all terms must match.
    let parsed = parser
        .parse_query(&sanitize_query(&nfc(query)))
        .map_err(|e| SearchError::BadQuery(e.to_string()))?;

    // Keep the FULL match set before the cutoff (same contract as bm25_search:
    // TopDocs makes no stability promise for equal scores at the limit).
    let collect_limit = limit.max(1).max(idx.num_docs);
    let top = searcher
        .search(&parsed, &TopDocs::with_limit(collect_limit))
        .map_err(|e| SearchError::Internal(e.to_string()))?;

    let mut hits = Vec::with_capacity(top.len().min(limit.max(1)));
    for (score, addr) in top {
        let stored: TantivyDocument = searcher
            .doc(addr)
            .map_err(|e| SearchError::Internal(e.to_string()))?;
        let doc_id = stored
            .get_first(idx.id_field)
            .and_then(|v| v.as_i64())
            .ok_or_else(|| SearchError::Internal("stored doc missing id".into()))?;
        hits.push(Hit {
            doc_id,
            score: score as f64,
        });
    }
    // Deterministic tie-break over the FULL match set before truncating.
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus() -> Vec<Document> {
        vec![
            Document {
                id: 0,
                text: "the cat sat on the warm mat".into(),
            },
            Document {
                id: 1,
                text: "a curious cat chased the laser dot".into(),
            },
            Document {
                id: 2,
                text: "the loyal dog barked at the mailman".into(),
            },
            Document {
                id: 3,
                text: "the stock market crashed and finance suffered".into(),
            },
        ]
    }

    #[test]
    fn raw_term_dictionary_counts_documents_not_occurrences() {
        let texts = [
            "the cat sat on the mat",       // "the" x2 in one doc → counts once
            "a cat and a dog",
            "the dog barks at the cat",     // "the" x2 again, "cat", "dog"
        ];
        let dict = raw_term_dictionary(&texts).unwrap();
        let get = |t: &str| -> i64 { dict.iter().find(|r| r.term == t).unwrap().df };
        // "cat" appears in docs 0,1,2 → df 3; "dog" in 1,2 → df 2; "the" in 0,2 → df 2.
        assert_eq!(get("cat"), 3, "cat in all three docs");
        assert_eq!(get("dog"), 2, "dog in docs 1 and 2");
        assert_eq!(get("the"), 2, "the counts per-document despite x2 in one doc");
        assert_eq!(get("barks"), 1);
        // Sorted by df desc, then term asc: cat(3) first, then dog/the(2) in order.
        assert_eq!(dict[0].term, "cat");
        assert_eq!(dict[1].term, "dog");
        assert_eq!(dict[2].term, "the");
    }

    #[test]
    fn raw_term_dictionary_lowercases_and_strips_punct() {
        let texts = ["Cat, CAT! cat-sitter", "crude oil"];
        let dict = raw_term_dictionary(&texts).unwrap();
        let get = |t: &str| -> Option<i64> { dict.iter().find(|r| r.term == t).map(|r| r.df) };
        // All "Cat"/"CAT"/"cat" lowercase to one term; punctuation splits words.
        assert_eq!(get("cat"), Some(1));
        assert_eq!(get("cat-sitter"), None, "hyphen is a token boundary");
        assert_eq!(get("sitter"), Some(1));
        assert_eq!(get("crude"), Some(1));
        assert!(get("Cat").is_none(), "dictionary is lowercase-only");
    }

    #[test]
    fn raw_term_dictionary_empty_corpus_is_empty() {
        assert!(raw_term_dictionary(&[]).unwrap().is_empty());
    }

    #[test]
    fn sanitize_query_escapes_metacharacters_but_keeps_words() {
        // The real-world trigger: a headline with a colon in it.
        assert_eq!(sanitize_query("enter your e-mail: archos"),
                   "enter your e\\-mail\\: archos");
        assert_eq!(sanitize_query("q2: strong quarter (40%)"),
                   "q2\\: strong quarter \\(40%\\)");
        assert_eq!(sanitize_query("a-b +c \"d\" e*f"),
                   "a\\-b \\+c \\\"d\\\" e\\*f");
        // Plain words, digits and unicode pass through untouched.
        assert_eq!(sanitize_query("crude oil refinery"), "crude oil refinery");
        assert_eq!(sanitize_query("café 40% done"), "café 40% done");
    }

    #[test]
    fn colon_headline_searches_instead_of_erroring() {
        // Before sanitization, "e-mail:" is a field reference and the parser
        // raises BadQuery; now the words search as plain text (AND semantics:
        // the doc must contain both "e-mail" and "archos" — it does).
        let docs = vec![Document {
            id: 0,
            text: "Enter your e-mail: Archos has turned".into(),
        }];
        let hits = bm25_search(&docs, "Enter your e-mail: Archos", "english", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, 0);
    }

    #[test]
    fn cat_query_ranks_cat_docs_first() {
        let hits = bm25_search(&corpus(), "cat", "english", 10).unwrap();
        assert_eq!(hits.len(), 2, "only the two cat docs match");
        // Both cat docs (0 and 1) outrank the non-matching finance/dog docs.
        let ids: Vec<i64> = hits.iter().map(|h| h.doc_id).collect();
        assert!(ids.contains(&0) && ids.contains(&1));
        assert!(!ids.contains(&2) && !ids.contains(&3));
        // Scores are descending.
        for w in hits.windows(2) {
            assert!(w[0].score >= w[1].score, "scores must be descending");
        }
    }

    #[test]
    fn finance_query_ranks_finance_doc() {
        let hits = bm25_search(&corpus(), "finance market", "english", 10).unwrap();
        assert_eq!(hits[0].doc_id, 3, "the finance doc ranks first");
    }

    #[test]
    fn explicit_ids_are_preserved() {
        let docs =
            parse_docs(r#"[{"id":42,"text":"a happy cat"},{"id":7,"text":"a sad dog"}]"#).unwrap();
        let hits = bm25_search(&docs, "cat", "english", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, 42, "explicit doc id is surfaced");
    }

    #[test]
    fn empty_corpus_and_query_yield_no_hits() {
        assert!(bm25_search(&[], "cat", "english", 10).unwrap().is_empty());
        assert!(bm25_search(&corpus(), "   ", "english", 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn bm25_score_relevant_beats_irrelevant() {
        let relevant = bm25_score("the cat sat on the mat", "cat", "english").unwrap();
        let irrelevant = bm25_score("the stock market crashed", "cat", "english").unwrap();
        assert!(relevant > 0.0);
        assert_eq!(irrelevant, 0.0, "non-matching doc scores 0");
        assert!(relevant > irrelevant);
    }

    #[test]
    fn tokenize_default_splits_and_lowercases() {
        let toks = tokenize_default("Running quickly, CATS!").unwrap();
        assert_eq!(toks, vec!["running", "quickly", "cats"]);
    }

    #[test]
    fn tokenize_english_stems() {
        let toks = tokenize_lang("Running quickly", "english").unwrap();
        assert_eq!(toks, vec!["run", "quick"]);
    }

    #[test]
    fn stem_english_running_is_run() {
        assert_eq!(stem("running", "english").unwrap(), "run");
        assert_eq!(stem("cats", "english").unwrap(), "cat");
    }

    #[test]
    fn empty_text_is_empty_not_error() {
        assert!(tokenize_default("").unwrap().is_empty());
        assert_eq!(stem("", "english").unwrap(), "");
    }

    #[test]
    fn unknown_language_is_error() {
        assert!(matches!(
            stem("x", "klingon"),
            Err(SearchError::UnknownLanguage(_))
        ));
        assert!(matches!(
            tokenize_lang("x", "klingon"),
            Err(SearchError::UnknownLanguage(_))
        ));
    }

    #[test]
    fn parse_docs_forms() {
        assert!(parse_docs("").unwrap().is_empty());
        assert!(parse_docs("[]").unwrap().is_empty());
        assert!(parse_docs("null").unwrap().is_empty());
        let strs = parse_docs(r#"["a","b"]"#).unwrap();
        assert_eq!(strs.len(), 2);
        assert_eq!(strs[0].id, 0);
        assert_eq!(strs[1].id, 1);
        assert!(parse_docs("not json").is_err());
        assert!(
            parse_docs(r#"{"id":1}"#).is_err(),
            "top-level must be array"
        );
        assert!(
            parse_docs(r#"[{"text":1}]"#).is_err(),
            "text must be string"
        );
    }

    #[test]
    fn supported_languages_includes_english() {
        assert!(SUPPORTED_LANGUAGES.contains(&"english"));
        assert_eq!(SUPPORTED_LANGUAGES.len(), 19);
        for l in SUPPORTED_LANGUAGES {
            assert!(resolve_language(l).is_ok(), "{l} must resolve");
        }
    }

    #[test]
    fn version_string_mentions_tantivy() {
        assert!(tantivy_version().contains("tantivy"));
    }

    /// P1 tie-break regression: three documents with *identical* content
    /// (hence identical BM25 scores) plus one non-matching doc. With
    /// `top_k := 1` the old `TopDocs::with_limit(1)` kept an arbitrary
    /// one of the ties (the collector makes no stability promise); the
    /// contract is that the SMALLEST doc id wins.
    #[test]
    fn top_k_tie_at_cutoff_returns_smallest_doc_id() {
        let docs = vec![
            Document { id: 9, text: "a cat".into() },
            Document { id: 2, text: "a cat".into() },
            Document { id: 5, text: "a cat".into() },
            Document { id: 1, text: "a dog".into() },
        ];
        let hits = bm25_search(&docs, "cat", "english", 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].doc_id, 2,
            "equal-score tie at the cutoff must return the smallest doc id"
        );
        // And top_k := 3 returns all three, in ascending-id order.
        let hits3 = bm25_search(&docs, "cat", "english", 3).unwrap();
        assert_eq!(
            hits3.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
            vec![2, 5, 9]
        );
    }

    /// P1 Unicode regression: a precomposed `é` (U+00E9) and the
    /// decomposed `e` + combining acute (U+0301) spell the same visible
    /// word and must match each other after NFC normalization.
    #[test]
    fn precomposed_and_decomposed_unicode_match() {
        let docs = vec![Document { id: 0, text: "caf\u{00E9} latte".into() }];
        // Query with the decomposed form must still hit the precomposed doc.
        let hits = bm25_search(&docs, "caf\u{0065}\u{0301}", "english", 10).unwrap();
        assert_eq!(hits.len(), 1, "NFC must make both spellings match");
        assert_eq!(hits[0].doc_id, 0);
        // Tokenization agrees across normalization forms too.
        assert_eq!(
            tokenize_default("caf\u{00E9}"),
            tokenize_default("caf\u{0065}\u{0301}")
        );
    }

    #[test]
    fn resolve_language_chinese_aliases() {
        assert!(matches!(resolve_language("chinese").unwrap(), Lang::Chinese));
        assert!(matches!(resolve_language("ZH").unwrap(), Lang::Chinese));
        assert!(matches!(resolve_language("  中文 ").unwrap(), Lang::Chinese));
        assert!(matches!(
            resolve_language("english").unwrap(),
            Lang::Snowball(TvLanguage::English)
        ));
    }

    #[test]
    fn tokenize_chinese_segments_words_not_chars() {
        let toks = tokenize_lang("浙江大学邱建 crater 星系团", "chinese").unwrap();
        assert!(
            toks.iter().any(|t| t == "浙江大学"),
            "full word 浙江大学 expected, got: {toks:?}"
        );
        assert!(
            toks.iter().any(|t| t == "crater"),
            "latin word must survive, got: {toks:?}"
        );
        // Whitespace/punctuation-only fragments must not appear.
        assert!(
            toks.iter().all(|t| t.chars().any(|c| c.is_alphanumeric())),
            "no punctuation/whitespace tokens, got: {toks:?}"
        );
    }

    #[test]
    fn stem_chinese_is_identity() {
        // No stemmer for Chinese: identity — even multi-word input comes back
        // unchanged (no splitting, no space-joined output; that would violate
        // the single-word contract the 18 Snowball languages honor).
        assert_eq!(stem("苹果", "chinese").unwrap(), "苹果");
        // F1 regression pin: multi-word input must NOT come back space-joined.
        assert_eq!(stem("苹果手机", "chinese").unwrap(), "苹果手机");
        assert_eq!(stem("iPhone", "chinese").unwrap(), "iPhone");
    }

    #[test]
    fn bm25_search_chinese_corpus() {
        let docs = vec![
            Document { id: 0, text: "苹果发布了新款手机".into() },
            Document { id: 1, text: "华为的麒麟芯片很强大".into() },
            Document { id: 2, text: "北京大学的计算机系招生".into() },
            Document { id: 3, text: "今天天气很好适合出门运动".into() },
            Document { id: 4, text: "苹果 苹果 股票 苹果".into() },
        ];
        let hits = bm25_search(&docs, "苹果", "chinese", 10).unwrap();
        let ids: Vec<i64> = hits.iter().map(|h| h.doc_id).collect();
        assert_eq!(ids, vec![4, 0], "doc 4 (3x 苹果) first, then doc 0; got {ids:?}");
        // Conjunction (AND): no doc contains both 苹果 and 华为.
        let conj = bm25_search(&docs, "苹果 华为", "chinese", 10).unwrap();
        assert!(conj.is_empty(), "AND semantics must yield no hits");
        // Absent word → no hits.
        assert!(bm25_search(&docs, "企鹅", "chinese", 10).unwrap().is_empty());
    }

    #[test]
    fn supported_languages_marks_chinese_jieba() {
        assert!(SUPPORTED_LANGUAGES.contains(&"chinese"));
        assert_eq!(language_display("chinese"), "chinese (jieba)");
        assert_eq!(language_display("english"), "english");
    }

    /// F3 regression: JiebaTokenStream offsets must be REAL byte ranges into
    /// the original text — on char boundaries, with
    /// &text[offset_from..offset_to] == token text. Includes a mixed
    /// Chinese/latin line where the old accumulated offsets pointed at the
    /// wrong words (and could panic mid-character on &text[a..b]).
    #[test]
    fn jieba_token_offsets_slice_original_text() {
        let cases: [(&str, usize); 4] = [
            ("苹果 iPhone 15 发布", 4),
            ("苹果 发布了 新款手机，crater 星系团。", 6),
            ("浙江大学邱建 crater 星系团", 4),
            ("今天天气很好适合运动", 1),
        ];
        for (text, min) in cases {
            let mut tokenizer = JiebaTokenizer::new();
            let mut stream = tokenizer.token_stream(text);
            let mut n = 0;
            stream.process(&mut |tok| {
                let slice = &text[tok.offset_from..tok.offset_to];
                assert_eq!(
                    slice, tok.text,
                    "token {n} of {text:?}: offsets must slice the original text"
                );
                n += 1;
            });
            assert!(n >= min, "expected >= {min} tokens for {text:?}, got {n}");
        }
    }

    // ── persistent (on-disk) index ───────────────────────────────────────────

    fn tmp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "duckdb_tantivy_test_{}_{}",
            std::process::id(),
            tag
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn persistent_index_roundtrip_matches_bm25_search() {
        let docs = corpus();
        let dir = tmp_dir("roundtrip");
        let idx = create_persistent("idx_en", &dir, "english", &docs).unwrap();
        assert_eq!(idx.language, "english");
        assert_eq!(idx.num_docs, docs.len());
        for q in ["cat", "finance market", "no_such_word_zzz", "   "] {
            let eph = bm25_search(&docs, q, "english", 10).unwrap();
            let pers = persistent_search(&idx, q, 10).unwrap();
            assert_eq!(eph, pers, "query {q:?}: persistent must equal ephemeral");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistent_search_tie_break_matches_contract() {
        let docs = vec![
            Document { id: 9, text: "a cat".into() },
            Document { id: 2, text: "a cat".into() },
            Document { id: 5, text: "a cat".into() },
            Document { id: 1, text: "a dog".into() },
        ];
        let dir = tmp_dir("tie");
        let idx = create_persistent("tie_idx", &dir, "english", &docs).unwrap();
        let one = persistent_search(&idx, "cat", 1).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(
            one[0].doc_id, 2,
            "equal-score tie at the cutoff must return the smallest doc id"
        );
        let three = persistent_search(&idx, "cat", 3).unwrap();
        assert_eq!(
            three.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
            vec![2, 5, 9]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistent_search_chinese_corpus() {
        let docs = vec![
            Document { id: 0, text: "苹果发布了新款手机".into() },
            Document { id: 1, text: "华为的麒麟芯片很强大".into() },
            Document { id: 4, text: "苹果 苹果 股票 苹果".into() },
        ];
        let dir = tmp_dir("zh");
        let idx = create_persistent("zh_idx", &dir, "chinese", &docs).unwrap();
        assert_eq!(idx.language, "chinese");
        let hits = persistent_search(&idx, "苹果", 10).unwrap();
        assert_eq!(
            hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
            vec![4, 0],
            "doc 4 (3x 苹果) first, then doc 0"
        );
        // Must equal the ephemeral chinese search on the same corpus.
        assert_eq!(bm25_search(&docs, "苹果", "chinese", 10).unwrap(), hits);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_persistent_rebuilds_wiped_directory() {
        let dir = tmp_dir("rebuild");
        let docs1 = vec![Document { id: 1, text: "zebra".into() }];
        create_persistent("rb", &dir, "english", &docs1).unwrap();
        // Second build with different content replaces (not appends) the first.
        let docs2 = vec![Document { id: 2, text: "giraffe".into() }];
        let idx = create_persistent("rb", &dir, "english", &docs2).unwrap();
        assert_eq!(idx.num_docs, 1, "rebuild must not append to the old segment");
        assert!(persistent_search(&idx, "zebra", 10).unwrap().is_empty());
        assert!(!persistent_search(&idx, "giraffe", 10).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_persistent_recovers_language_and_searches() {
        let docs = corpus();
        let dir = tmp_dir("reopen");
        create_persistent("idx_zh", &dir, "chinese", &docs).unwrap();
        let opened = open_persistent("idx_zh", &dir).unwrap();
        assert_eq!(opened.language, "chinese");
        assert_eq!(opened.num_docs, docs.len());
        assert!(!persistent_search(&opened, "cat", 10).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_persistent_rejects_foreign_directory_without_meta() {
        // A genuine tantivy index on disk, but NOT created by us: no meta
        // sidecar. `open_persistent` must refuse it with a clear error.
        let dir = tmp_dir("foreign");
        let mut sb = Schema::builder();
        let f = sb.add_text_field("body", TextOptions::default());
        #[cfg_attr(target_arch = "wasm32", allow(unused))]
        let index =
            Index::open_or_create(MmapDirectory::open(&dir).unwrap(), sb.build()).unwrap();
        let mut writer = index.writer(WRITER_HEAP_BYTES).unwrap();
        writer.add_document(doc!(f => "hello")).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let e = open_persistent("foreign_idx", &dir).unwrap_err();
        assert!(
            e.to_string().contains(INDEX_META_FILE),
            "must cite the missing provenance file: {e}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_persistent_missing_directory_is_error() {
        let e =
            open_persistent("ghost", Path::new("/nonexistent/definitely_not_here_12345"))
                .unwrap_err();
        assert!(
            e.to_string().contains("no tantivy index"),
            "clear missing-index error: {e}"
        );
    }

    #[test]
    fn create_persistent_unknown_language_is_error() {
        let docs = corpus();
        let dir = tmp_dir("badlang");
        let e = create_persistent("bad", &dir, "klingon", &docs).unwrap_err();
        assert!(matches!(e, SearchError::UnknownLanguage(_)), "got: {e:?}");
    }

    #[test]
    fn is_safe_index_name_rules() {
        assert!(is_safe_index_name("my_index"));
        assert!(is_safe_index_name("idx-2"));
        assert!(is_safe_index_name("_x"));
        assert!(!is_safe_index_name(""));
        assert!(!is_safe_index_name("a/b"));
        assert!(!is_safe_index_name(".."));
        assert!(!is_safe_index_name("a b"));
        assert!(!is_safe_index_name("9lives"));
        assert!(!is_safe_index_name("中文索引"));
        assert!(!is_safe_index_name(&"x".repeat(129)));
    }

    #[test]
    fn persistent_index_dir_default_location() {
        // No TANTIVY_INDEX_DIR in the test environment → .tantivy_index/<name>.
        std::env::remove_var("TANTIVY_INDEX_DIR");
        let p = persistent_index_dir("foo");
        assert!(
            p.to_string_lossy().contains(".tantivy_index"),
            "default base, got: {p:?}"
        );
        assert!(p.to_string_lossy().ends_with("foo"));
    }

    #[test]
    fn lang_id_normalizes_aliases() {
        assert_eq!(lang_id(&resolve_language("zh").unwrap()), "chinese");
        assert_eq!(lang_id(&resolve_language("CHINESE").unwrap()), "chinese");
        assert_eq!(lang_id(&resolve_language("ENGLISH").unwrap()), "english");
        assert_eq!(lang_id(&resolve_language("fr").unwrap()), "french");
    }
}

