# Lane C report — v0.3.0 persistent (on-disk, mmap) indexes

Branch: `feat/persistent-index` · Base: `75f62bd` (v0.2.1, build+25 integration tests green)

## Delivered

| Function | Signature | Returns |
|---|---|---|
| `tantivy_create_index` | `(name, table, [id_column := ''], [text_column := 'body'], [language := 'english'], [path := ''])` | `name, path, docs, language` |
| `tantivy_search_index` | `(name, query, [top_k := 10])` | `doc_id, score` |
| `tantivy_list` | `()` | `name, path, docs, language` |
| `tantivy_save_index` | `(name, [path := ''])` | `name, path, docs` |
| `tantivy_load_index` | `(name, path)` | `name, path, docs, language` |

## Design decisions

- **Pure search layer (`src/search.rs`)**: `PersistentIndex {name, path, reader,
  searcher_handle, id_field, body_field, language, num_docs}` +
  `create_persistent` / `open_persistent` / `persistent_search` +
  `is_safe_index_name` / `persistent_index_dir` / `lang_id` / `INDEX_META_FILE`.
- **Registry (`src/lib.rs`)**: `static REGISTRY: OnceLock<Mutex<HashMap<String,
  Arc<PersistentIndex>>>>` (duckdb-vss pattern) + `CREATE_INDEX_LOCK`. Lock
  discipline: session → build → registry; search/list/save/load touch only the
  registry, held only for Arc clones/inserts — never across a query.
- **`persistent_search` ≡ `bm25_search` semantics**: NFC + `sanitize_query` +
  conjunction (AND) + full-match-set collector (`limit.max(num_docs)`) sorted
  (score desc, doc_id asc), truncated at the cutoff → bit-identical results.
- **Provenance**: `__tantivy_meta.json` sidecar (name + language) written on
  create, copied on save, required on load (foreign/corrupt dirs → clear
  error). Language is recovered on load and the build-time analyzer is
  re-registered (tantivy `TokenizerManager` is per-process, never persisted —
  the one non-obvious API trap found in the 0.24.2 source).
- **Rebuild, never append**: create wipes its (caller-owned) directory first.
  v0.3 has no drop; name conflict on load → explicit error.
- **Guards**: index name = single safe path segment; create reuses
  bm25_search's pre-flight corpus measurement (MAX_DOCS + text-byte cap)
  before materializing; save refuses empty-existing/ancestor-overlapping
  destinations; top_k bounds `[1, 100000]` shared with bm25_search.

## Red lines

- **Existing functions: zero behavior change.** Multiset-verified against
  `75f62bd`: every original line of `src/lib.rs`/`src/search.rs` is intact;
  the only altered lines are 3 import lines (extended with new imports) and
  `Hit`'s derive (added `PartialEq`, additive). All 25 baseline tests pass
  unmodified.
- **No silent truncation**: every failure path (bad name/table/language/query,
  missing/corrupt index dir, name conflict, non-empty save destination,
  out-of-range top_k, oversized corpus) is an explicit SQL error naming the
  function.
- **No push** performed; commits local on `feat/persistent-index`.

## Evidence (all offline, `--locked`, release builds)

- `cargo build --release --offline --locked` — green at every step.
- Unit tests: **37/37** (25 baseline + 12 new: roundtrip-vs-bm25 parity,
  tie-break at cutoff, chinese corpus, rebuild-wipes-dir, open-from-disk,
  foreign-dir rejection, missing-dir/unknown-language errors, name rules).
- SQL e2e: **39/39** (25 baseline + 14 new persistent tests).
- **Cross-process bit-identity** (`scripts/run_persistent_e2e.sh`, 3 fresh
  `cargo test` processes): fresh A builds english+chinese indices, saves
  copies, dumps persistent + ephemeral searches; fresh B loads both copies
  under new names, dumps the same searches. **All 6 CSV diffs empty**:
  create-vs-load bit-identical (english + chinese), and
  `bm25_search` == `tantivy_search_index` on identical parameters.
- **100k-row performance** (fresh process): build = **274 ms**, two searches
  (top_k 100 / 1000) = **46 ms** total; in-suite 100k test green.
- Extension stamped with `extension_version = v0.3.0` (Cargo.toml,
  `configure/extension_version.txt`, `Cargo.lock` synced; lock diff = local
  package version only).

## Files

- `src/search.rs` — persistent-index core (+12 unit tests)
- `src/lib.rs` — REGISTRY/CREATE_INDEX_LOCK + 5 table functions + registration
- `tests/integration.rs` — +14 SQL tests
- `tests/persistent_e2e.rs` — cross-process harness (3 `#[ignore]`d tests)
- `scripts/run_persistent_e2e.sh` — one-shot rebuild + suite + e2e evidence
- `README.md` — function surface, persistent-index section, limits, limitations
- `Cargo.toml` / `Cargo.lock` / `configure/extension_version.txt` — v0.3.0

## Known limitations (documented in README)

- Registry is process-global; no `drop` in v0.3 (rebuild under a new name or
  in a fresh process).
- No incremental updates: rebuild only.
- `path :=` on create takes ownership of the directory (wiped on rebuild).
- Session-connection limitations of `bm25_search` (temp tables / uncommitted
  txns) apply identically to `tantivy_create_index`'s corpus read.

## M9 验收 bug 修复(协调者上报:10 万行中文 fresh-load 查询"错误")

**结论:扩展无代码缺陷。上报的行正是正确 BM25 引擎在该语料上的结果;"错误"外观由
两个良性原因造成,已全部取证固化进测试套件。**

复现(两个独立 `duckdb -unsigned` CLI 进程,协调者 SQL 原样,WSL 环境):
进程 A `tantivy_create_index('bigidx','big', ..., language:='chinese', path:='/tmp/tantivy_accept/bigidx')`
(10 万行,其模板);进程 B `tantivy_load_index` 后
`SELECT doc_id, round(score,3) FROM tantivy_search_index('bigidx','苹果 股票', top_k:=3)`
→ `0/1/2, 0.0`。

### 为什么这是正确结果

1. **score 不是 0,是 `round(score,3)` 把它抹掉了。** 全精度输出:
   `0:1.0013554856413975e-05, 1:1.0013554856413975e-05, 2:1.0013554856413975e-05` ——
   严格为正;`round(1.0014e-5, 3) = 0.000`。
   为何这么小:苹果/股票在该语料**每一行**都出现(df = N = 100k)→
   tantivy idf = ln(1 + 0.5/(N+0.5)) ≈ 5e-6/词 → 两词总分 ≈ 1e-5。
2. **doc_id 0/1/2 是真正的 top-3。** 该模板中每行恰好各含一词(tf 均匀),且每行
   token 数相同(每个数字串是 1 个 jieba token)→ **全部 10 万行 score 完全相等**
   (已验证:三行共享同一个 bit-identical 值)。按本扩展书面契约 (score desc,
   doc_id asc) 裁决平局 → 0,1,2。对这一查询,**任何 BM25 参数化都无法让 9699 排在
   0 前面**:doc 9699 含两词的频率(各 1 次)与长度和 doc 0 完全相同。
   4~5 行小语料"对拍对"只是因为那里 df=2/5(非常见词 → 有效 idf → 有意义的排序);
   表观"规模相关"是语料统计(df/N),不是引擎差异。
3. **协调者列出的 4 个怀疑点全部实证排除**(100k 中文、多 segment、fresh 进程 load):
   - doc_addr/stored field 读取:判别性稀有词 `'9699'`(仅 doc 9699 含该数字串)
     返回**恰好 doc 9699、score 11.107** —— 大 doc_id 与正 score,正是针对该文档
     的查询应有的结果;
   - TopDocs collect_limit/tie-break:与临时索引 `bm25_search` bit-identical;
   - load 路径 schema/analyzer 绑定:跨进程结果一致(meta 侧车恢复语言、
     analyzer 重注册,规模下同样成立);
   - DocId 失配:跨 segment stored id 读取正确。
4. **100k 中文规模下跨进程 bit-identical 保持**:create-vs-load 与
   ephemeral-vs-persistent 共 6 组 diff(苹果 股票 top3、稀有 9699、中频 '96' 命中数)
   全空(scripts/run_persistent_e2e.sh [5/6])。中频 '96' → 1030 命中,
   = |{i : i%97=96}| = floor(99903/97)+1,数学精确。

### 若验收脚本断言"大 doc_id"

针对查询 '苹果 股票',该断言是**对 BM25 的错误测试**:任何正确引擎在该语料上
都无法满足(见第 2 条),不存在能让它通过的引擎修改。验收应改为:
- 对 '苹果 股票' top_k 3 断言 doc_id 0/1/2 且 score 为正(全精度);或
- 用扩展自己的契约做对拍:`tantivy_search_index` vs `bm25_search` 同参数同结果;
- 判别性查询断言,如 '9699' → doc 9699、score ≈ 11.1。

### 回归测试(已进套件)

- `tests/integration.rs::m9_chinese_100k_acceptance_parity` —— in-process:
  协调者原样语料;对拍/正 score/平局契约/判别性查询,全断言。
- `tests/persistent_e2e.rs::fresh_process_m9_chinese_100k_{save,load}` +
  `scripts/run_persistent_e2e.sh` [5/6] —— fresh 进程 CLI 级对拍,并打印协调者
  字面 `round(score,3)` 视图与全精度视图存证。
