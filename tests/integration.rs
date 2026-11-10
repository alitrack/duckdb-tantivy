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
fn supported_languages_has_19_rows() {
    let d = db();
    // 18 Snowball languages + chinese (jieba) — see search::SUPPORTED_LANGUAGES.
    let n: i64 = d
        .query_one("SELECT count(*) FROM supported_languages()")
        .unwrap();
    assert_eq!(n, 19, "SUPPORTED_LANGUAGES has 19 entries");
    // chinese is annotated with its segmentation engine.
    let zh: String = d
        .query_one("SELECT lang FROM supported_languages() WHERE lang = 'chinese (jieba)'")
        .unwrap();
    assert_eq!(zh, "chinese (jieba)");
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

// ─────────────────────────────────────────────────────────────────────────────
// Chinese tokenization (jieba): `chinese`/`zh` language — full SQL chain.
// The jieba dictionary is embedded in the crate; segmentation is word-level
// (not per-character), so '浙江大学' is one token, not four.
// ─────────────────────────────────────────────────────────────────────────────

const ZH_CORPUS: &str = r#"
CREATE OR REPLACE TABLE zh_docs(doc_id BIGINT, body VARCHAR);
INSERT INTO zh_docs VALUES
  (0, '苹果发布了新款手机'),
  (1, '华为的麒麟芯片很强大'),
  (2, '北京大学的计算机系招生'),
  (3, '今天天气很好适合出门运动'),
  (4, '苹果 苹果 股票 苹果');
"#;

#[test]
fn tokenize_chinese_returns_jieba_words() {
    let d = db();
    // jieba full segmentation must produce the complete word '浙江大学'
    // (not per-character), and the latin word must survive.
    let has_univ: i64 = d
        .query_one("SELECT list_contains(tokenize('浙江大学邱建 crater 星系团', 'chinese'), '浙江大学')::INTEGER")
        .unwrap();
    assert_eq!(has_univ, 1, "'浙江大学' must be a jieba token");
    let has_crater: i64 = d
        .query_one("SELECT list_contains(tokenize('浙江大学邱建 crater 星系团', 'chinese'), 'crater')::INTEGER")
        .unwrap();
    assert_eq!(has_crater, 1, "latin word must survive as a token");
    // Deletion anchor (F6b): references the critical values above.
    let _ = (&has_univ, &has_crater);
}

#[test]
fn bm25_search_chinese_corpus_hits_right_rows() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    // '苹果' occurs in docs 0 (once) and 4 (three times); nobody else.
    let ids: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR, ',' ORDER BY doc_id) FROM bm25_search('zh_docs', '苹果', id_column:='doc_id', language:='chinese')")
        .unwrap();
    assert_eq!(ids, "0,4", "only docs 0 and 4 contain 苹果, got {ids}");
    // top_k=1 must return the highest-scoring doc: doc 4 has 3 occurrences
    // of the query term vs 1 in doc 0, and a shorter body.
    let top: i64 = d
        .query_one("SELECT doc_id FROM bm25_search('zh_docs', '苹果', id_column:='doc_id', language:='chinese', top_k:=1)")
        .unwrap();
    assert_eq!(top, 4, "doc 4 (3x 苹果) must outrank doc 0 (1x)");
    // A word absent from the corpus yields nothing.
    let none: i64 = d
        .query_one("SELECT count(*) FROM bm25_search('zh_docs', '企鹅', id_column:='doc_id', language:='chinese')")
        .unwrap();
    assert_eq!(none, 0);
    // Deletion anchor (F6b): references the critical values above.
    let _ = (&ids, &top, &none);
}

#[test]
fn bm25_search_chinese_conjunction_is_and() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    // Conjunction (AND) semantics: no single doc contains both 苹果 and 华为.
    let conj: i64 = d
        .query_one("SELECT count(*) FROM bm25_search('zh_docs', '苹果 华为', id_column:='doc_id', language:='chinese')")
        .unwrap();
    assert_eq!(conj, 0, "AND semantics: no doc has both 苹果 and 华为");
    // Deletion anchor (F6b): references the critical value above.
    let _ = (&conj,);
}

#[test]
fn bm25_search_chinese_mixed_language_rows() {
    let d = db();
    d.execute_batch(
        "CREATE OR REPLACE TABLE zh_mixed(doc_id BIGINT, body VARCHAR); \
         INSERT INTO zh_mixed VALUES (0, '苹果 iPhone 15 发布'), (1, '华为 Mate 60 上市');",
    )
    .unwrap();
    // Chinese query term hits the right row.
    let hit_zh: i64 = d
        .query_one("SELECT doc_id FROM bm25_search('zh_mixed', '发布', id_column:='doc_id', language:='chinese', top_k:=1)")
        .unwrap();
    assert_eq!(hit_zh, 0);
    // Latin word is indexed lowercased by the chinese analyzer.
    let hit_en: i64 = d
        .query_one("SELECT doc_id FROM bm25_search('zh_mixed', 'iphone', id_column:='doc_id', language:='chinese', top_k:=1)")
        .unwrap();
    assert_eq!(hit_en, 0, "lowercased 'iphone' must match the 'iPhone' row");
    // tokenize sees both the chinese word and the lowercased latin word.
    let has_both: i64 = d
        .query_one("SELECT (list_contains(tokenize('苹果 iPhone 15 发布','chinese'), '苹果') AND list_contains(tokenize('苹果 iPhone 15 发布','chinese'), 'iphone'))::INTEGER")
        .unwrap();
    assert_eq!(has_both, 1, "mixed row must yield both 苹果 and iphone");
    // Deletion anchor (F6b): references the critical values above.
    let _ = (&hit_zh, &hit_en, &has_both);
}

#[test]
fn tantivy_terms_chinese_table_returns_chinese_terms() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    // The raw-term dictionary must surface Chinese text as terms. The default
    // (non-stemming) pipeline keeps a CJK run as one token, so the doc-1
    // surface form '华为的麒麟芯片很强大' appears; '华为' is a substring of it.
    let all: String = d
        .query_one("SELECT string_agg(term, ',' ORDER BY term) FROM tantivy_terms('zh_docs', text_column:='body')")
        .unwrap();
    assert!(
        all.contains("华为"),
        "expected Chinese terms in the dictionary, got: {all}"
    );
    let n: i64 = d
        .query_one("SELECT count(*) FROM tantivy_terms('zh_docs', text_column:='body')")
        .unwrap();
    assert!(n > 0, "dictionary must be non-empty");
    // Deletion anchor (F6b): references the critical values above.
    let _ = (&all, &n);
}

#[test]
fn zh_alias_is_equivalent_to_chinese() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    let via_chinese: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR, ',' ORDER BY doc_id) FROM bm25_search('zh_docs', '苹果', id_column:='doc_id', language:='chinese')")
        .unwrap();
    let via_zh: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR, ',' ORDER BY doc_id) FROM bm25_search('zh_docs', '苹果', id_column:='doc_id', language:='zh')")
        .unwrap();
    assert_eq!(via_chinese, via_zh, "'zh' must behave exactly like 'chinese'");
    assert!(!via_zh.is_empty(), "the alias search must actually match docs");
    // tokenize agrees for both ids.
    let same: i64 = d
        .query_one("SELECT (tokenize('苹果 华为 手机','zh') = tokenize('苹果 华为 手机','chinese'))::INTEGER")
        .unwrap();
    assert_eq!(same, 1, "tokenize must be identical for zh and chinese");
    // Deletion anchor (F6b): references the critical values above.
    let _ = (&via_chinese, &via_zh, &same);
}

#[test]
fn chinese_language_id_is_case_insensitive() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    // Full alias matrix (F6): every case/whitespace variant must behave
    // EXACTLY like canonical 'chinese' — per-id full-vector comparisons, so
    // shrinking this list or swapping an entry changes the test outcome.
    const IDS: [&str; 5] = ["chinese", "Chinese", "CHINESE", "cHiNeSe", " zh "];
    assert_eq!(IDS.len(), 5, "alias matrix must keep all 5 case/whitespace variants");
    let mut checked: usize = 0;
    for id in IDS {
        // Per-id: tokenize output must be byte-identical to 'chinese'
        // (this text's English-pipeline output differs: CJK runs stay whole,
        // so a swapped-in non-chinese id cannot fake equality).
        let same: i64 = d
            .query_one(&format!(
                "SELECT (tokenize('浙江大学邱建 crater 星系团', '{id}') = tokenize('浙江大学邱建 crater 星系团', 'chinese'))::INTEGER"
            ))
            .unwrap_or_else(|e| panic!("language id '{id}' must be accepted: {e}"));
        assert_eq!(same, 1, "tokenize with id '{id}' must equal 'chinese' output");
        // Per-id: bm25_search must hit exactly docs 0 and 4.
        let n: i64 = d
            .query_one(&format!(
                "SELECT count(*) FROM bm25_search('zh_docs', '苹果', id_column:='doc_id', language:='{id}')"
            ))
            .unwrap_or_else(|e| panic!("language id '{id}' must be accepted by bm25_search: {e}"));
        assert_eq!(n, 2, "id '{id}' must hit docs 0 and 4");
        checked += 1;
    }
    assert_eq!(checked, 5, "all 5 alias ids must have been exercised");
    // Deletion anchor (F6b): keeps the matrix and counter alive, so deleting
    // the loop or the matrix above fails here (compile) or at the assert.
    let _ = (&IDS, &checked);
}

#[test]
fn bm25_score_chinese() {
    let d = db();
    let hit: f64 = d
        .query_one("SELECT bm25_score('苹果发布了新款手机', '苹果', 'chinese')")
        .unwrap();
    assert!(hit > 0.0, "matching chinese doc must score > 0, got {hit}");
    let miss: f64 = d
        .query_one("SELECT bm25_score('华为的麒麟芯片很强大', '苹果', 'chinese')")
        .unwrap();
    assert_eq!(miss, 0.0, "non-matching chinese doc must score 0.0");
    // Deletion anchor (F6b): references the critical values above.
    let _ = (&hit, &miss);
}

// ─────────────────────────────────────────────────────────────────────────────
// v0.3 persistent indexes: tantivy_create_index / tantivy_search_index /
// tantivy_list / tantivy_save_index / tantivy_load_index.
// Registry semantics, chinese persistence, and error paths. Each test uses its
// own index name + temp directory (the registry is process-global and shared
// across tests).
// ─────────────────────────────────────────────────────────────────────────────

fn persist_dir(tag: &str) -> String {
    let d = std::env::temp_dir().join(format!(
        "duckdb_tantivy_persist_{}_{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&d);
    d.to_string_lossy().into_owned()
}

#[test]
fn persistent_create_summary_and_list_row() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let dir = persist_dir("list1");
    let summary: String = d
        .query_one(&format!(
            "SELECT name || '|' || path || '|' || docs::VARCHAR || '|' || language FROM tantivy_create_index('lst_a','docs', id_column:='doc_id', text_column:='body', language:='english', path:='{dir}')"
        ))
        .unwrap();
    assert_eq!(summary, format!("lst_a|{dir}|5|english"), "summary row: {summary}");
    // The registry is visible through tantivy_list with the same fields.
    let row: String = d
        .query_one("SELECT name || '|' || docs::VARCHAR || '|' || language FROM tantivy_list() WHERE name = 'lst_a'")
        .unwrap();
    assert_eq!(row, "lst_a|5|english", "list row: {row}");
    let path: String = d
        .query_one("SELECT path FROM tantivy_list() WHERE name = 'lst_a'")
        .unwrap();
    assert_eq!(path, dir, "list path must be the create path");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_search_matches_bm25_search_same_params() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let dir = persist_dir("parity");
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('par_en','docs', id_column:='doc_id', text_column:='body', language:='english', path:='{dir}')"
    ))
    .unwrap();
    // Same parameters on both functions → identical (doc_id, score) rows.
    for (q, k) in [("cat", 10), ("cat", 1), ("stock market", 5), ("zebra nothing", 10)] {
        let p: String = d
            .query_one(&format!(
                "SELECT COALESCE(string_agg(doc_id::VARCHAR || ':' || score::VARCHAR, ',' ORDER BY doc_id), 'NONE') FROM tantivy_search_index('par_en', '{q}', top_k:={k})"
            ))
            .unwrap();
        let e: String = d
            .query_one(&format!(
                "SELECT COALESCE(string_agg(doc_id::VARCHAR || ':' || score::VARCHAR, ',' ORDER BY doc_id), 'NONE') FROM bm25_search('docs','{q}', id_column:='doc_id', text_column:='body', language:='english', top_k:={k})"
            ))
            .unwrap();
        assert_eq!(p, e, "query '{q}' top_k={k}: persistent must equal ephemeral");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_search_chinese_matches_bm25() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    let dir = persist_dir("zhpar");
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('par_zh','zh_docs', id_column:='doc_id', text_column:='body', language:='chinese', path:='{dir}')"
    ))
    .unwrap();
    let p: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR, ',' ORDER BY doc_id) FROM tantivy_search_index('par_zh','苹果', top_k:=10)")
        .unwrap();
    assert_eq!(p, "0,4", "docs 0 and 4 contain 苹果: {p}");
    let e: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR, ',' ORDER BY doc_id) FROM bm25_search('zh_docs','苹果', id_column:='doc_id', text_column:='body', language:='chinese', top_k:=10)")
        .unwrap();
    assert_eq!(p, e);
    // top_k=1: the 3x-苹果 doc wins on both.
    let top_p: i64 = d.query_one("SELECT doc_id FROM tantivy_search_index('par_zh','苹果', top_k:=1)").unwrap();
    let top_e: i64 = d.query_one("SELECT doc_id FROM bm25_search('zh_docs','苹果', id_column:='doc_id', text_column:='body', language:='chinese', top_k:=1)").unwrap();
    assert_eq!(top_p, top_e);
    assert_eq!(top_p, 4);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_search_unknown_name_is_error() {
    let d = db();
    let err = d
        .query_one::<i64>("SELECT count(*) FROM tantivy_search_index('no_such_index_xyz','x')")
        .expect_err("must fail on unknown index name");
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("no persistent index"), "clear missing-index error: {msg}");
    assert!(msg.contains("tantivy_list"), "should point at tantivy_list(): {msg}");
}

#[test]
fn persistent_create_rebuild_replaces_entry() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let dir = persist_dir("rebuild");
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('rb_ci','docs', id_column:='doc_id', path:='{dir}')"
    ))
    .unwrap();
    assert_eq!(
        d.query_one::<i64>("SELECT count(*) FROM tantivy_search_index('rb_ci','cat')").unwrap(),
        3,
        "first build: 3 cat docs"
    );
    // Rebuild the SAME name from a different table → the registry entry is
    // replaced (not appended to), and the directory is wiped first.
    d.execute_batch(
        "CREATE OR REPLACE TABLE rb_docs(doc_id BIGINT, body VARCHAR);\
         INSERT INTO rb_docs VALUES (7,'a zebra crossed'),(8,'another zebra');",
    )
    .unwrap();
    let summary: String = d
        .query_one(&format!(
            "SELECT name || '|' || docs::VARCHAR FROM tantivy_create_index('rb_ci','rb_docs', id_column:='doc_id', path:='{dir}')"
        ))
        .unwrap();
    assert_eq!(summary, "rb_ci|2", "rebuild re-indexes only the new rows: {summary}");
    assert_eq!(
        d.query_one::<i64>("SELECT count(*) FROM tantivy_search_index('rb_ci','cat')").unwrap(),
        0,
        "old content must be gone after rebuild"
    );
    assert_eq!(
        d.query_one::<i64>("SELECT count(*) FROM tantivy_search_index('rb_ci','zebra')").unwrap(),
        2
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_create_unknown_language_is_error() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let dir = persist_dir("badlang");
    let err = d
        .query_one::<i64>(&format!(
            "SELECT count(*) FROM tantivy_create_index('bad_lang_ci','docs', id_column:='doc_id', language:='klingon', path:='{dir}')"
        ))
        .expect_err("unknown language must be an error");
    let msg = err.to_string();
    assert!(
        msg.contains("klingon") || msg.to_lowercase().contains("language"),
        "clear language error: {msg}"
    );
    // And nothing was registered.
    let n: i64 = d
        .query_one("SELECT count(*) FROM tantivy_list() WHERE name = 'bad_lang_ci'")
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn persistent_create_missing_table_is_error() {
    let d = db();
    let dir = persist_dir("notable");
    let err = d
        .query_one::<i64>(&format!(
            "SELECT count(*) FROM tantivy_create_index('x_bad_table','no_such_table_xyz', text_column:='body', path:='{dir}')"
        ))
        .expect_err("missing table must fail at bind");
    let msg = err.to_string();
    assert!(msg.contains("tantivy_create_index"), "error must name the function: {msg}");
}

#[test]
fn persistent_create_bad_names_are_errors() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let dir = persist_dir("badname");
    for bad in ["a/b", "..", "a b", ""] {
        let err = d
            .query_one::<i64>(&format!(
                "SELECT count(*) FROM tantivy_create_index('{bad}','docs', id_column:='doc_id', path:='{dir}')"
            ))
            .expect_err("unsafe name must be rejected at bind");
        assert!(
            err.to_string().to_lowercase().contains("identifier"),
            "name {bad:?} rejection must cite the identifier rule: {err}"
        );
    }
}

#[test]
fn persistent_search_top_k_bounds() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let dir = persist_dir("topk");
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('tk_ci','docs', id_column:='doc_id', path:='{dir}')"
    ))
    .unwrap();
    // 0 and negatives are rejected (same contract as bm25_search), never clamped.
    d.query_one::<i64>("SELECT count(*) FROM tantivy_search_index('tk_ci','cat', top_k:=0)")
        .expect_err("top_k:=0 must be an error");
    d.query_one::<i64>("SELECT count(*) FROM tantivy_search_index('tk_ci','cat', top_k:=-1)")
        .expect_err("top_k:=-1 must be an error");
    // The cap is allowed.
    let n: i64 = d
        .query_one("SELECT count(*) FROM tantivy_search_index('tk_ci','cat', top_k:=100000)")
        .unwrap();
    assert_eq!(n, 3);
    // Default top_k is 10 (the persistent-search default, vs 100 for bm25).
    let n10: i64 = d
        .query_one("SELECT count(*) FROM tantivy_search_index('tk_ci','cat')")
        .unwrap();
    assert_eq!(n10, 3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persistent_save_and_load_roundtrip_same_process() {
    let d = db();
    d.execute_batch(ZH_CORPUS).unwrap();
    let base = std::env::temp_dir()
        .join(format!("duckdb_tantivy_persist_{}_svload", std::process::id()));
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    let _ = std::fs::remove_dir_all(&base);

    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('sv_ci','zh_docs', id_column:='doc_id', language:='chinese', path:='{}')",
        dir_a.display()
    ))
    .unwrap();

    // 1) save with no path: reports the current location, no copy.
    let s0: String = d
        .query_one("SELECT path FROM tantivy_save_index('sv_ci')")
        .unwrap();
    assert_eq!(s0, dir_a.to_string_lossy(), "default save reports the live path");

    // 2) save to a new directory: a full copy (incl. the meta sidecar).
    let s1: String = d
        .query_one(&format!(
            "SELECT name || '|' || path || '|' || docs::VARCHAR FROM tantivy_save_index('sv_ci', path:='{}')",
            dir_b.display()
        ))
        .unwrap();
    assert_eq!(
        s1,
        format!("sv_ci|{}|5", dir_b.to_string_lossy()),
        "save summary: {s1}"
    );
    let meta = dir_b.join(search_meta_name());
    assert!(meta.exists(), "meta sidecar must be copied");

    // 3) load the copy under a NEW name → identical search results.
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_load_index('sv_loaded','{}')",
        dir_b.display()
    ))
    .unwrap();
    let r1: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR || ':' || score::VARCHAR, ',' ORDER BY doc_id) FROM tantivy_search_index('sv_ci','苹果', top_k:=10)")
        .unwrap();
    let r2: String = d
        .query_one("SELECT string_agg(doc_id::VARCHAR || ':' || score::VARCHAR, ',' ORDER BY doc_id) FROM tantivy_search_index('sv_loaded','苹果', top_k:=10)")
        .unwrap();
    assert_eq!(r1, r2, "loaded copy must search identically");

    // 4) saving into a NON-EMPTY existing directory is refused (no overwrite).
    let err = d
        .query_one::<i64>(&format!(
            "SELECT count(*) FROM tantivy_save_index('sv_ci', path:='{}')",
            dir_b.display()
        ))
        .expect_err("destination not empty must be an error");
    assert!(err.to_string().to_lowercase().contains("not empty"), "{err}");

    let _ = std::fs::remove_dir_all(&base);
}

/// The index meta sidecar file name (kept out of the test module's private
/// imports: the binding layer does not re-export the search module).
fn search_meta_name() -> String {
    // Must match search::INDEX_META_FILE; the value is small and stable.
    "__tantivy_meta.json".to_string()
}

#[test]
fn persistent_load_missing_path_is_error() {
    let d = db();
    let err = d
        .query_one::<i64>("SELECT count(*) FROM tantivy_load_index('lz_missing','/nonexistent_xyz/nowhere_42')")
        .expect_err("missing path must be an error");
    let msg = err.to_string();
    assert!(
        msg.contains("no tantivy index") || msg.contains("nonexistent_xyz"),
        "clear missing-index error: {msg}"
    );
    // Nothing was registered.
    let n: i64 = d
        .query_one("SELECT count(*) FROM tantivy_list() WHERE name = 'lz_missing'")
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn persistent_load_name_conflict_is_error() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let base = std::env::temp_dir()
        .join(format!("duckdb_tantivy_persist_{}_clash", std::process::id()));
    let dir_a = base.join("a");
    let dir_b = base.join("b");
    let _ = std::fs::remove_dir_all(&base);
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('clash_ci','docs', id_column:='doc_id', path:='{}')",
        dir_a.display()
    ))
    .unwrap();
    let _s: String = d
        .query_one(&format!("SELECT path FROM tantivy_save_index('clash_ci', path:='{}')", dir_b.display()))
        .unwrap();
    // Loading the SAME already-registered name is a clear error (no overwrite).
    let err = d
        .query_one::<i64>(&format!(
            "SELECT count(*) FROM tantivy_load_index('clash_ci','{}')",
            dir_b.display()
        ))
        .expect_err("name conflict must be an error");
    assert!(err.to_string().to_lowercase().contains("already registered"), "{err}");
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn persistent_list_sorted_and_stable() {
    let d = db();
    d.execute_batch(CORPUS).unwrap();
    let base = std::env::temp_dir()
        .join(format!("duckdb_tantivy_persist_{}_sorted", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    // Two indices with names that sort non-trivially.
    for n in ["zz_last", "aa_first"] {
        let dir = base.join(n);
        let _s: String = d
            .query_one(&format!(
                "SELECT name FROM tantivy_create_index('{n}','docs', id_column:='doc_id', path:='{}')",
                dir.display()
            ))
            .unwrap();
    }
    // The full list must be sorted by name (snapshot semantics).
    let names: String = d
        .query_one(
            "SELECT string_agg(name, ',' ORDER BY name) FROM tantivy_list() WHERE name IN ('aa_first','zz_last')",
        )
        .unwrap();
    assert_eq!(names, "aa_first,zz_last", "both must be listed, sorted: {names}");
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn persistent_index_100k_rows() {
    let d = db();
    d.execute_batch(
        "CREATE OR REPLACE TABLE big_docs(id BIGINT, body VARCHAR);\
         INSERT INTO big_docs SELECT i, 'the quick brown fox ' || (i % 97) || ' jumps over the lazy dog ' || (i % 3) FROM range(100000) t(i)",
    )
    .unwrap();
    let dir = persist_dir("big");
    let t0 = std::time::Instant::now();
    let summary: String = d
        .query_one(&format!(
            "SELECT name || '|' || docs::VARCHAR FROM tantivy_create_index('big_idx','big_docs', id_column:='id', path:='{dir}')"
        ))
        .unwrap();
    let build_ms = t0.elapsed().as_millis();
    assert_eq!(summary, "big_idx|100000", "all 100k rows indexed: {summary}");

    let t1 = std::time::Instant::now();
    let n1: i64 = d
        .query_one("SELECT count(*) FROM tantivy_search_index('big_idx','quick fox', top_k:=100)")
        .unwrap();
    let t2 = std::time::Instant::now();
    let n2: i64 = d
        .query_one("SELECT count(*) FROM tantivy_search_index('big_idx','lazy dog', top_k:=1000)")
        .unwrap();
    let search_ms = t1.elapsed().as_millis() + t2.elapsed().as_millis();

    assert!(n1 > 0 && n1 <= 100, "n1={n1}");
    assert!(n2 > 0 && n2 <= 1000, "n2={n2}");
    // Generous bounds: this is a smoke/perf check, not a regression threshold.
    assert!(build_ms < 300_000, "100k build took {build_ms}ms");
    assert!(search_ms < 60_000, "two searches took {search_ms}ms");
    eprintln!("100k perf: build={build_ms}ms, 2 searches={search_ms}ms (n1={n1}, n2={n2})");
    let _ = std::fs::remove_dir_all(&dir);
}
