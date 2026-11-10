//! Cross-process persistent-index e2e (v0.3) — driven by
//! `scripts/run_persistent_e2e.sh` (`E2E_OUT=<dir>`).
//!
//! Each `cargo test` invocation is a FRESH OS process (fresh registry):
//!
//!   `fresh_process_save`  — create two tables (english + chinese), build two
//!                           persistent indexes, `tantivy_save_index` both to
//!                           copies, and dump:
//!                             * persistent search results (both languages)
//!                             * ephemeral `bm25_search` results, same params
//!                           to CSVs in $E2E_OUT.
//!
//!   `fresh_process_load`  — in a DIFFERENT fresh process, `tantivy_load_index`
//!                           both saved copies under new names and dump the
//!                           SAME searches to CSVs.
//!
//!   `fresh_process_perf_100k` — 100k-row build + searches with timings.
//!
//! The script then `diff`s the CSVs:
//!   * `search*_create` vs `search*_load`   → bit-identical across processes
//!   * `search*_ephemeral` vs `search*_create` → bm25_search ==
//!                                              tantivy_search_index, same
//!                                              params, same results
//!
//! These tests are `#[ignore]`d from the regular suite (they need $E2E_OUT);
//! the in-process coverage lives in `tests/integration.rs`.

use quack_rs::testing::InMemoryDb;

const EN_CORPUS: &str = r#"
CREATE OR REPLACE TABLE docs(doc_id BIGINT, body VARCHAR);
INSERT INTO docs VALUES
  (0, 'the cat sat on the warm mat'),
  (1, 'a curious cat chased the laser dot'),
  (2, 'the loyal dog barked at the mailman'),
  (3, 'the stock market crashed and finance suffered'),
  (4, 'cats fish and cat birds');
"#;

const ZH_CORPUS: &str = r#"
CREATE OR REPLACE TABLE zh_docs(doc_id BIGINT, body VARCHAR);
INSERT INTO zh_docs VALUES
  (0, '苹果发布了新款手机'),
  (1, '华为的麒麟芯片很强大'),
  (2, '北京大学的计算机系招生'),
  (3, '今天天气很好适合出门运动'),
  (4, '苹果 苹果 股票 苹果');
"#;

fn load_db() -> InMemoryDb {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/release/tantivy.duckdb_extension");
    assert!(
        path.exists(),
        "build first: scripts/run_persistent_e2e.sh (or make release)"
    );
    let d = InMemoryDb::open_unsigned().expect("open in-memory db");
    d.execute_batch(&format!("LOAD '{}'", path.display()))
        .expect("load extension");
    d
}

fn out_dir() -> String {
    std::env::var("E2E_OUT")
        .expect("E2E_OUT not set — run via scripts/run_persistent_e2e.sh")
}

fn dump(d: &InMemoryDb, out: &str, file: &str, query: &str) {
    d.execute_batch(&format!("COPY ({query}) TO '{out}/{file}.csv' (HEADER)"))
        .expect("copy out");
}

#[test]
#[ignore]
fn fresh_process_save() {
    let out = out_dir();
    let d = load_db();
    d.execute_batch(EN_CORPUS).unwrap();
    d.execute_batch(ZH_CORPUS).unwrap();

    let en_dir = format!("{out}/en_index");
    let en_copy = format!("{out}/en_index_copy");
    let zh_dir = format!("{out}/zh_index");
    let zh_copy = format!("{out}/zh_index_copy");

    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('e2e_en','docs', id_column:='doc_id', text_column:='body', language:='english', path:='{en_dir}')"
    ))
    .unwrap();
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_save_index('e2e_en', path:='{en_copy}')"
    ))
    .unwrap();
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('e2e_zh','zh_docs', id_column:='doc_id', text_column:='body', language:='chinese', path:='{zh_dir}')"
    ))
    .unwrap();
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_save_index('e2e_zh', path:='{zh_copy}')"
    ))
    .unwrap();

    // Persistent searches (this process, on the freshly built indices).
    dump(&d, &out, "search_create",
        "SELECT doc_id, score FROM tantivy_search_index('e2e_en','cat', top_k:=10) ORDER BY doc_id");
    dump(&d, &out, "search2_create",
        "SELECT doc_id, score FROM tantivy_search_index('e2e_en','stock market', top_k:=10) ORDER BY doc_id");
    dump(&d, &out, "search_zh_create",
        "SELECT doc_id, score FROM tantivy_search_index('e2e_zh','苹果', top_k:=10) ORDER BY doc_id");

    // Ephemeral references: identical parameters to bm25_search (default top_k
    // there is 100 — pass the same top_k:=10 as above for an exact cross-check).
    dump(&d, &out, "search_ephemeral",
        "SELECT doc_id, score FROM bm25_search('docs','cat', id_column:='doc_id', text_column:='body', language:='english', top_k:=10) ORDER BY doc_id");
    dump(&d, &out, "search2_ephemeral",
        "SELECT doc_id, score FROM bm25_search('docs','stock market', id_column:='doc_id', text_column:='body', language:='english', top_k:=10) ORDER BY doc_id");
    dump(&d, &out, "search_zh_ephemeral",
        "SELECT doc_id, score FROM bm25_search('zh_docs','苹果', id_column:='doc_id', text_column:='body', language:='chinese', top_k:=10) ORDER BY doc_id");

    eprintln!("SAVE phase: 3 persistent + 3 ephemeral CSVs written to {out}");
}

#[test]
#[ignore]
fn fresh_process_load() {
    let out = out_dir();
    let d = load_db();
    // A DIFFERENT fresh process: the registry starts empty. Load both saved
    // copies under new names — no table access needed at all.
    let en_copy = format!("{out}/en_index_copy");
    let zh_copy = format!("{out}/zh_index_copy");

    let summary: String = d
        .query_one(&format!(
            "SELECT name || '|' || docs::VARCHAR || '|' || language FROM tantivy_load_index('e2e_en_loaded', path:='{en_copy}')"
        ))
        .unwrap();
    assert_eq!(summary, "e2e_en_loaded|5|english", "loaded summary: {summary}");

    let summary_zh: String = d
        .query_one(&format!(
            "SELECT name || '|' || docs::VARCHAR || '|' || language FROM tantivy_load_index('e2e_zh_loaded', path:='{zh_copy}')"
        ))
        .unwrap();
    assert_eq!(summary_zh, "e2e_zh_loaded|5|chinese", "loaded summary: {summary_zh}");

    // The SAME searches as the save phase — the script diffs these files.
    dump(&d, &out, "search_load",
        "SELECT doc_id, score FROM tantivy_search_index('e2e_en_loaded','cat', top_k:=10) ORDER BY doc_id");
    dump(&d, &out, "search2_load",
        "SELECT doc_id, score FROM tantivy_search_index('e2e_en_loaded','stock market', top_k:=10) ORDER BY doc_id");
    dump(&d, &out, "search_zh_load",
        "SELECT doc_id, score FROM tantivy_search_index('e2e_zh_loaded','苹果', top_k:=10) ORDER BY doc_id");

    eprintln!("LOAD phase: 3 CSVs written to {out} (fresh process)");
}

/// M9 coordinator repro, cross-process: 100k-row CHINESE index (their exact
/// template) — process A builds + saves, process B loads and searches.
///
/// The coordinator's acceptance query is '苹果 股票' top_k 3, but on THIS
/// corpus every row contains both terms exactly once (identical token count:
/// each digit run is one jieba token), so all 100k documents tie exactly on
/// score (idf ≈ 1e-5, strictly positive); the documented (score desc, doc_id
/// asc) contract therefore returns 0,1,2 — and round(score,3) shows 0.0 for
/// a ~1e-5 score. The M9 phase pins that full-precision output is
/// bit-identical across the fresh-process load AND equal to ephemeral
/// bm25_search, plus discriminative queries ('9699' rare, '96' mid-freq)
/// that prove correct big-index term matching / stored-id / scoring.
const M9_CORPUS: &str = r#"
CREATE OR REPLACE TABLE big(doc_id BIGINT, body VARCHAR);
INSERT INTO big SELECT i, '文档第 '||i||' 号 苹果 股票 涨跌 '||(i%97)||' 科技公司营收 '||(i%31) FROM range(100000) t(i);
"#;

#[test]
#[ignore]
fn fresh_process_m9_chinese_100k_save() {
    let out = out_dir();
    let d = load_db();
    d.execute_batch(M9_CORPUS).unwrap();
    let dir = format!("{out}/m9_index");
    let copy = format!("{out}/m9_index_copy");
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_create_index('m9ci','big', id_column:='doc_id', text_column:='body', language:='chinese', path:='{dir}')"
    )).unwrap();
    d.execute_batch(&format!("SELECT * FROM tantivy_save_index('m9ci', path:='{copy}')")).unwrap();

    // Persistent (fresh process A) — full precision, unrounded.
    dump(&d, &out, "m9_p_create",
        "SELECT doc_id, score FROM tantivy_search_index('m9ci','苹果 股票', top_k:=3) ORDER BY doc_id");
    dump(&d, &out, "m9_r_create",
        "SELECT doc_id, score FROM tantivy_search_index('m9ci','9699', top_k:=3) ORDER BY doc_id");
    dump(&d, &out, "m9_m_create",
        "SELECT count(*) FROM tantivy_search_index('m9ci','96', top_k:=100000)");
    // The coordinator's literal acceptance view (rounded).
    dump(&d, &out, "m9_p_create_rounded",
        "SELECT doc_id, round(score,3) FROM tantivy_search_index('m9ci','苹果 股票', top_k:=3) ORDER BY doc_id");
    // Ephemeral ground truth, identical parameters.
    dump(&d, &out, "m9_p_ephemeral",
        "SELECT doc_id, score FROM bm25_search('big','苹果 股票', id_column:='doc_id', text_column:='body', language:='chinese', top_k:=3) ORDER BY doc_id");
    dump(&d, &out, "m9_r_ephemeral",
        "SELECT doc_id, score FROM bm25_search('big','9699', id_column:='doc_id', text_column:='body', language:='chinese', top_k:=3) ORDER BY doc_id");
    dump(&d, &out, "m9_m_ephemeral",
        "SELECT count(*) FROM bm25_search('big','96', id_column:='doc_id', text_column:='body', language:='chinese', top_k:=100000)");
    eprintln!("M9 SAVE phase: 7 CSVs (fresh process A)");
}

#[test]
#[ignore]
fn fresh_process_m9_chinese_100k_load() {
    let out = out_dir();
    let d = load_db();
    let copy = format!("{out}/m9_index_copy");
    d.execute_batch(&format!(
        "SELECT * FROM tantivy_load_index('m9ci_loaded', path:='{copy}')"
    )).unwrap();

    // Persistent (FRESH process B) — must be bit-identical to process A.
    dump(&d, &out, "m9_p_load",
        "SELECT doc_id, score FROM tantivy_search_index('m9ci_loaded','苹果 股票', top_k:=3) ORDER BY doc_id");
    dump(&d, &out, "m9_r_load",
        "SELECT doc_id, score FROM tantivy_search_index('m9ci_loaded','9699', top_k:=3) ORDER BY doc_id");
    dump(&d, &out, "m9_m_load",
        "SELECT count(*) FROM tantivy_search_index('m9ci_loaded','96', top_k:=100000)");
    dump(&d, &out, "m9_p_load_rounded",
        "SELECT doc_id, round(score,3) FROM tantivy_search_index('m9ci_loaded','苹果 股票', top_k:=3) ORDER BY doc_id");
    eprintln!("M9 LOAD phase: 4 CSVs (fresh process B)");
}

#[test]
#[ignore]
fn fresh_process_perf_100k() {
    let out = out_dir();
    let d = load_db();
    d.execute_batch(
        "CREATE OR REPLACE TABLE big_docs(id BIGINT, body VARCHAR);
         INSERT INTO big_docs SELECT i, 'the quick brown fox ' || (i % 97) || ' jumps over the lazy dog ' || (i % 3) FROM range(100000) t(i)",
    )
    .unwrap();
    let dir = format!("{out}/big_index");

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

    std::fs::write(
        format!("{out}/perf.txt"),
        format!("build_ms={build_ms}\nsearch_ms={search_ms}\nn1={n1}\nn2={n2}\n"),
    )
    .unwrap();
    eprintln!(
        "PERF 100k: build={build_ms}ms, 2 searches={search_ms}ms (n1={n1}, n2={n2})"
    );
}
