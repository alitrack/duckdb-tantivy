//! SQL-level integration tests: LOAD the built `.duckdb_extension` into a real
//! in-memory DuckDB and exercise the full function surface.
//!
//! Run: `cargo test --release` (after `cargo build --release` + `append_metadata`).
//!
//! Harness note: the extension keeps ONE session connection (a `OnceLock` in
//! the process, bound to the first in-memory database that loads it). The
//! quack-rs `InMemoryDb` test helper opens a *fresh* in-memory database per
//! instance, so all tests here share a single DB instance (behind a `Mutex`)
//! to keep the session connection valid. This mirrors real usage, where one
//! DuckDB process loads the extension once and many tables/queries run on it.

use std::sync::{Mutex, MutexGuard, OnceLock};

use quack_rs::testing::InMemoryDb;

static SHARED: OnceLock<Mutex<InMemoryDb>> = OnceLock::new();

fn db() -> MutexGuard<'static, InMemoryDb> {
    let m = SHARED.get_or_init(|| {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/release/tantivy.duckdb_extension");
        assert!(path.exists(), "build first: cargo build --release && append_metadata -> {path:?}");
        let d = InMemoryDb::open_unsigned().expect("open in-memory db");
        d.execute_batch(&format!("LOAD '{}'", path.display())).expect("load extension");
        Mutex::new(d)
    });
    m.lock().unwrap_or_else(|p| p.into_inner())
}

const CORPUS: &str = r#"
CREATE OR REPLACE TABLE docs(doc_id BIGINT, body VARCHAR);
INSERT INTO docs VALUES
  (0, 'the cat sat on the warm mat'),
  (1, 'a curious cat chased the laser dot'),
  (2, 'the loyal dog barked at the mailman'),
  (3, 'the stock market crashed and finance suffered'),
  (4, 'cats fish and cat birds');
"#;

#[test]
fn bm25_search_ranks_cat_docs_first() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    // Only the three cat docs match (0, 1, 4); the dog/finance docs do not.
    let n: i64 = d
        .query_one("SELECT count(*) FROM bm25_search('docs','cat', id_column:='doc_id', text_column:='body')")
        .unwrap();
    assert_eq!(n, 3, "expected 3 cat docs, got {n}");
    let all: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR, ',' ORDER BY doc_id) FROM bm25_search('docs','cat', id_column:='doc_id', text_column:='body')")
        .unwrap();
    let got: std::collections::HashSet<i64> = all
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect();
    assert!(
        got.contains(&0) && got.contains(&1) && got.contains(&4),
        "got {all}"
    );
    assert!(!got.contains(&2) && !got.contains(&3), "got {all}");
    // Scores descending as emitted (search.rs sorts score desc, doc_id asc).
    let scores: String = d
        .query_one("SELECT string_agg(score::VARCHAR, '|') FROM (SELECT score FROM bm25_search('docs','cat', id_column:='doc_id', text_column:='body') ORDER BY score DESC, doc_id)")
        .unwrap();
    let nums: Vec<f64> = scores.split('|').map(|s| s.parse().unwrap()).collect();
    for w in nums.windows(2) {
        assert!(w[0] >= w[1], "scores must be descending: {scores}");
    }
}

#[test]
fn bm25_search_defaults_id_text_language_limit() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    // 2-arg form: id_column omitted → 0-based sequence over selected rows;
    // text_column defaults to 'body', language to 'english', limit to 100.
    let top: i64 = d
        .query_one("SELECT doc_id FROM bm25_search('docs','cat') ORDER BY score DESC LIMIT 1")
        .unwrap();
    // With auto sequence ids, the cat docs are rows 0, 1, 4 of the table.
    assert!((0..=4).contains(&top), "top auto-id must be a cat row, got {top}");
    let n: i64 = d.query_one("SELECT count(*) FROM bm25_search('docs','cat')").unwrap();
    assert_eq!(n, 3);
}

#[test]
fn bm25_search_limit_bounds_results() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let n: i64 = d
        .query_one("SELECT count(*) FROM bm25_search('docs','cat', id_column:='doc_id', text_column:='body', top_k:=1)")
        .unwrap();
    assert_eq!(n, 1, "top_k=1 must return exactly one row");
}

#[test]
fn tokenize_default_overload() {
    let d = db();
    // InMemoryDb only exposes query_one<T: FromSql> and duckdb-rs has no
    // FromSql impl for Vec<T>, so assert list content via scalar access.
    // `len()` is core in both the bundled test build and the 1.5 CLI.
    let n: i64 = d.query_one("SELECT len(tokenize('Running quickly'))").unwrap();
    assert_eq!(n, 2, "expected 2 tokens");
    let t1: String = d.query_one("SELECT tokenize('Running quickly')[1]").unwrap();
    let t2: String = d.query_one("SELECT tokenize('Running quickly')[2]").unwrap();
    assert_eq!(t1, "running");
    assert_eq!(t2, "quickly");
}

#[test]
fn tokenize_lang_overload_stems() {
    let d = db();
    let n: i64 = d
        .query_one("SELECT len(tokenize('Running quickly','english'))")
        .unwrap();
    assert_eq!(n, 2, "expected 2 tokens");
    let t1: String = d.query_one("SELECT tokenize('Running quickly','english')[1]").unwrap();
    let t2: String = d.query_one("SELECT tokenize('Running quickly','english')[2]").unwrap();
    assert_eq!(t1, "run");
    assert_eq!(t2, "quick");
}

#[test]
fn tokenize_null_input_is_null() {
    let d = db();
    let v: Option<String> = d.query_one("SELECT tokenize(NULL)[1]").unwrap();
    assert!(v.is_none(), "NULL input must yield NULL output");
}

#[test]
fn stem_english() {
    let d = db();
    assert_eq!(
        d.query_one::<String>("SELECT stem('running','english')").unwrap(),
        "run"
    );
    assert_eq!(
        d.query_one::<String>("SELECT stem('cats','english')").unwrap(),
        "cat"
    );
}

#[test]
fn bm25_score_relevant_positive_irrelevant_zero() {
    let d = db();
    let relevant: f64 = d
        .query_one("SELECT bm25_score('the cat sat','cat','english')")
        .unwrap();
    let irrelevant: f64 = d
        .query_one("SELECT bm25_score('stock crash','cat','english')")
        .unwrap();
    assert!(relevant > 0.0, "relevant doc must score > 0, got {relevant}");
    assert_eq!(irrelevant, 0.0, "non-matching doc must score 0.0");
    // 2-arg overload defaults language to english.
    let relevant2: f64 = d
        .query_one("SELECT bm25_score('the cat sat','cat')")
        .unwrap();
    assert!(relevant2 > 0.0);
}

#[test]
fn supported_languages_has_18_rows() {
    let d = db();
    let n: i64 = d
        .query_one("SELECT count(*) FROM supported_languages()")
        .unwrap();
    assert_eq!(n, 18, "SUPPORTED_LANGUAGES has 18 entries");
    let has_english: i64 = d
        .query_one("SELECT count(*) FROM supported_languages() WHERE lang='english'")
        .unwrap();
    assert_eq!(has_english, 1);
}

#[test]
fn tantivy_version_mentions_tantivy() {
    let d = db();
    let v: String = d.query_one("SELECT tantivy_version()").unwrap();
    assert!(
        v.contains("tantivy"),
        "version string must mention tantivy: {v}"
    );
}

#[test]
fn bm25_search_missing_table_is_clear_error() {
    let d = db();
    let err = d
        .query_one::<i64>(
            "SELECT count(*) FROM bm25_search('no_such_table','cat')",
        )
        .expect_err("must fail on missing table");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("bm25_search"),
        "error should name the function: {msg}"
    );
}

#[test]
fn bm25_search_missing_column_is_clear_error() {
    let d = db();
    d.execute_batch("CREATE TABLE no_body(id BIGINT); INSERT INTO no_body VALUES (1)")
        .unwrap();
    let err = d
        .query_one::<i64>(
            "SELECT count(*) FROM bm25_search('no_body','cat', text_column:='no_such_col')",
        )
        .expect_err("must fail on missing text column");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("bm25_search"),
        "error should name the function: {msg}"
    );
}

#[test]
fn tokenize_unknown_language_is_error() {
    let d = db();
    let err = d
        .query_one::<String>("SELECT tokenize('x','klingon')[1]")
        .expect_err("must fail on unknown language");
    let msg = err.to_string();
    assert!(
        msg.contains("klingon") || msg.to_lowercase().contains("language"),
        "clear language error: {msg}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Hardening (added after adversarial review): top_k bounds, memory guard,
// tokenize overflow.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn bm25_search_top_k_below_one_is_error() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    // 0 must be rejected (it used to silently fall back to 100).
    let err = d
        .query_one::<i64>("SELECT count(*) FROM bm25_search('docs','cat', top_k:=0)")
        .expect_err("top_k:=0 must be an error");
    assert!(
        err.to_string().to_lowercase().contains("top_k"),
        "error must name top_k: {err}"
    );
    // Negative values are rejected too.
    d.query_one::<i64>("SELECT count(*) FROM bm25_search('docs','cat', top_k:=-3)")
        .expect_err("top_k:=-3 must be an error");
}

#[test]
fn bm25_search_top_k_above_cap_is_error() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    // i64::MAX used to overflow tantivy's TopDocs allocation and abort the
    // whole host process (a user-triggerable DoS). It must now be rejected
    // cleanly at bind time.
    let err = d
        .query_one::<i64>(
            "SELECT count(*) FROM bm25_search('docs','cat', top_k:=9223372036854775807)",
        )
        .expect_err("top_k:=i64::MAX must be an error, not an abort");
    assert!(
        err.to_string().to_lowercase().contains("top_k"),
        "error must name top_k: {err}"
    );
}

#[test]
fn bm25_search_honors_top_k_boundary() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    // The cap itself (100_000) is allowed and simply returns all 3 hits.
    let n: i64 = d
        .query_one(
            "SELECT count(*) FROM bm25_search('docs','cat', top_k:=100000)",
        )
        .unwrap();
    assert_eq!(n, 3, "top_k at the cap returns every hit");
}

#[test]
fn tokenize_overflow_is_error_not_silent_null() {
    let d = db();
    // A single input whose tokenization exceeds the 1,000,000-element per-call
    // cap must error out rather than emit silent NULL rows (P1-2).
    let huge = "a ".repeat(1_000_001);
    let err = d
        .query_one::<i64>(&format!("SELECT len(tokenize('{}'))", huge))
        .expect_err("tokenize overflow must be an error");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("exceeds") || msg.contains("tokenize"),
        "clear overflow error: {err}"
    );
}
