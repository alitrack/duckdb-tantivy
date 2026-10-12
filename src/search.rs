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

use std::fmt;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, STORED};
use tantivy::tokenizer::{
    Language as TvLanguage, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer,
    Token, TokenStream,
};
use tantivy::{doc, Index, TantivyDocument};
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

/// The canonical Snowball/stemmer language identifiers tantivy supports, in a
/// stable order. Drives `supported_languages()` and the README table.
pub const SUPPORTED_LANGUAGES: &[&str] = &[
    "arabic",
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

/// Resolve a (case-insensitive) language id to a tantivy [`TvLanguage`].
/// `None`/empty selects English by convention; unknown ids are an error.
fn resolve_language(name: &str) -> Result<TvLanguage> {
    let n = name.trim().to_ascii_lowercase();
    let lang = match n.as_str() {
        "arabic" | "ar" => TvLanguage::Arabic,
        "danish" | "da" => TvLanguage::Danish,
        "dutch" | "nl" => TvLanguage::Dutch,
        "english" | "en" => TvLanguage::English,
        "finnish" | "fi" => TvLanguage::Finnish,
        "french" | "fr" => TvLanguage::French,
        "german" | "de" => TvLanguage::German,
        "greek" | "el" => TvLanguage::Greek,
        "hungarian" | "hu" => TvLanguage::Hungarian,
        "italian" | "it" => TvLanguage::Italian,
        "norwegian" | "no" | "nb" => TvLanguage::Norwegian,
        "portuguese" | "pt" => TvLanguage::Portuguese,
        "romanian" | "ro" => TvLanguage::Romanian,
        "russian" | "ru" => TvLanguage::Russian,
        "spanish" | "es" => TvLanguage::Spanish,
        "swedish" | "sv" => TvLanguage::Swedish,
        "tamil" | "ta" => TvLanguage::Tamil,
        "turkish" | "tr" => TvLanguage::Turkish,
        _ => return Err(SearchError::UnknownLanguage(name.to_string())),
    };
    Ok(lang)
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
    Ok(run_analyzer(stem_analyzer(l), text))
}

/// Snowball-stem a single `word` for `lang` (the whole input is treated as one
/// token, lowercased then stemmed). Empty word → empty string; unknown language
/// → error. E.g. `stem("running", "english") == "run"`.
pub fn stem(word: &str, lang: &str) -> Result<String> {
    check_text_len(word)?;
    let l = resolve_language(lang)?;
    let toks = run_analyzer(stem_analyzer(l), word);
    // A lone word yields a single token; join defensively if the tokenizer split.
    Ok(toks.join(" "))
}

/// A single search hit: the caller's document id and its BM25 score.
#[derive(Debug, Clone, Copy)]
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
    let l = resolve_language(lang)?;
    if docs.is_empty() || query.trim().is_empty() {
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
    index.tokenizers().register(ANALYZER, stem_analyzer(l));

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
        .parse_query(&nfc(query))
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

/// The tantivy version string, e.g. `"tantivy v0.24.2, index_format v7"`.
pub fn tantivy_version() -> String {
    tantivy::version_string().to_string()
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
        assert_eq!(SUPPORTED_LANGUAGES.len(), 18);
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
}
