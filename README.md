# duckdb-tantivy

A native Rust [DuckDB](https://duckdb.org) loadable extension that adds **BM25
full-text search** (via [tantivy](https://github.com/quickwit-oss/tantivy))
plus **tokenization** and **Snowball stemming** — written against the C
Extension API (ABI v1.2.0) through
[quack-rs](https://github.com/tomtom215/quack-rs). No VGI, no Arrow IPC, no
JSON shims: table functions bind to real tables and stream real columns.

**Current release: v0.4.0** — wasm support; persistent named indexes (v0.3.0)
(`tantivy_create_index` / `tantivy_search_index` / `tantivy_save_index` /
`tantivy_load_index` / `tantivy_list`) on top of the v0.2 BM25 surface.
DuckDB C Extension API v1.2.0; artifacts are **unsigned** (load with
`duckdb -unsigned`).

## Function surface

| Function | Description |
|---|---|
| `bm25_search(table, query, [id_column := ''], [text_column := 'body'], [language := 'english'], [top_k := 100])` | BM25 search over a table → `doc_id BIGINT, score DOUBLE` |
| `tantivy_create_index(name, table, [id_column := ''], [text_column := 'body'], [language := 'english'], [path := ''], [extra_fields := ''])` | Build a **persistent** on-disk (mmap-backed) index `name` from a table and register it in the process → `name, path, docs, language`. V3: `extra_fields := 'title, abstract'` indexes extra text columns as their own fields (v0.4.0) |
| `SET tantivy_default_language = 'chinese'` / `SET tantivy_default_top_k = 50` | Session-scope SQL knobs: defaults for the `language` / `top_k` parameters when omitted (explicit args win; `RESET` restores) |
| `tantivy_search_index(name, query, [top_k := 10])` | BM25 search against a registered persistent index → `doc_id BIGINT, score DOUBLE`. Same query semantics and tie-break as `bm25_search` → identical results (v0.3.0) |
| `tantivy_rrf_hybrid(fts_name, fts_query, vec_dir, vec_query_json, [rrf_k := 60], [top_k := 10], [vec_fetch := 50])` | **Hybrid search**: Reciprocal Rank Fusion of BM25 with a turbovec IVF index (`vec_dir` = `turboquant_build_ivf` output dir; loads its `index.tv` via the same crates.io turbovec crate duckdb_turbovec uses — no format glue). Both sides index the SAME ids. → `doc_id, rrf_score, fts_rank, vec_rank, fts_score, vec_score` (full explainability). Dual-hit docs outrank one-sided champions. |
| `tantivy_stats(name, query)` | **Pre-execution cost estimate** for `tantivy_search_index` (duckdb-dryrun style): tokenize the query with the index's own chain, read per-segment term doc-frequencies, and report what a search WOULD touch — `num_docs, segments, query_terms, estimated_postings_bytes, total_index_bytes, estimated_max_candidates, confidence, notes`. Never decodes postings, never scores. Absent terms report 0 bytes with `confidence = high` (AND short-circuits); postings bytes are a doc_freq × 8 approximation flagged in `notes` with `confidence = medium` (tantivy 0.24 exposes no per-term byte size). ~2 ms on a 20k-doc index. |
| `tantivy_list()` | The registered persistent indexes → `name, path, docs, language` (v0.3.0) |
| `tantivy_save_index(name, [path := ''])` | Copy a registered index's directory to `path` (or report where it lives without `path`) → `name, path, docs` (v0.3.0) |
| `tantivy_load_index(name, path := '...')` | Open a previously saved index directory (from any process) read-only and register it under `name` — `path` is a named parameter (style-consistent with `save`) but **required** → `name, path, docs, language` (v0.3.0) |
| `tantivy_terms(table, [text_column := 'body'])` | Raw-term (unstemmed, lowercased) dictionary of the table → `term VARCHAR, df BIGINT`. Document-frequency per term; the correction-space dictionary for typo-tolerance macro layers. |
| `supported_languages()` | The 18 Snowball languages tantivy can stem, plus `chinese (jieba)` |
| `tokenize(text)` / `tokenize(text, language)` | Tokenize (+ stem if language given) → `VARCHAR[]`. `language := 'chinese'` (alias `zh`) uses jieba word segmentation instead of Snowball |
| `stem(word, language)` | Snowball-stem a single word (`chinese`/`zh`: no stemmer, returns the original word unchanged — 中文无词干，恒等返回原词，不拆分、不插入空格) |
| `bm25_score(doc, query)` / `bm25_score(doc, query, language)` | BM25 score of one doc against a query |
| `tantivy_version()` | Version string of the embedded tantivy |
| `tantivy_snippet(name, query, text)` | Highlighted ~150-char fragment of `text` for `query`, using the persistent index `name`'s corpus statistics and its own tokenizer (Snowball/jieba). Markers `[HIGHLIGHT]`/`[/HIGHLIGHT]`; no-match → leading fragment unhighlighted; unknown index / bad query / over-length → NULL (COALESCE if needed). `SELECT tantivy_snippet('idx','quantum computing', body) FROM docs JOIN tantivy_search_index('idx','quantum computing') USING (doc_id)` is the RAG pattern. |

`top_k` is named `top_k` (not `limit`): `limit` is a reserved SQL keyword and
cannot appear as a named-argument name.

## Example

```sql
CREATE TABLE docs(doc_id BIGINT, body VARCHAR);
INSERT INTO docs VALUES (0, 'the cat sat'), (1, 'a cat here'), (2, 'the dog barked');

LOAD 'tantivy.duckdb_extension';        -- add -unsigned to the duckdb CLI

SELECT * FROM bm25_search('docs', 'cat');
-- doc_id | score
--       1 | 0.92...
--       0 | 0.92...

SELECT * FROM bm25_search('docs', 'cat', top_k := 1);
SELECT tokenize('Running quickly');          -- [running, quickly]  (no stem)
SELECT tokenize('Running quickly','english');-- [run, quick]
SELECT stem('cats','english');               -- cat
SELECT bm25_score('the cat sat','cat');      -- 0.28...
```

## Chinese tokenization (jieba)

`language := 'chinese'` (aliases `zh`, `中文`; case-insensitive) routes
indexing and queries through **jieba** word segmentation instead of the
Snowball stemmer — there are no Chinese word stems, so the pipeline is
*segment + lowercase only*. Mixed Chinese/English text works: Han runs are
segmented into words, Latin/digit runs pass through lowercased (`iPhone` →
`iphone`), and the normal conjunctive (AND) query semantics apply to the
segmented terms.

```sql
CREATE TABLE zh_docs(doc_id BIGINT, body VARCHAR);
INSERT INTO zh_docs VALUES
  (0, '苹果发布了新款手机'),
  (4, '苹果 苹果 股票 苹果');

SELECT * FROM bm25_search('zh_docs', '苹果', language := 'chinese');
-- doc_id | score
--      4 | ...   -- 3 occurrences rank above doc 0's single occurrence

SELECT tokenize('浙江大学邱建 crater 星系团', 'chinese');
-- [浙江大学, 邱建, crater, 星系团]   (jieba words, not per-character)

SELECT bm25_score('苹果发布了新款手机', '苹果', 'chinese');  -- > 0
SELECT language := 'zh'  -- alias, identical behaviour to 'chinese'
```

`supported_languages()` lists it as `chinese (jieba)`. Note that
`tantivy_terms` is language-agnostic (it uses the non-stemming default
analyzer), so for CJK it surfaces whole contiguous Han runs as surface terms —
use `bm25_search`/`tokenize` with `language := 'chinese'` when you need
word-level Chinese terms.

To rebuild and re-verify the whole chain from scratch (offline, rebuild + stamp + full test suite + Chinese assertions, exit 0 = all green):

```sh
bash scripts/run_chinese_e2e.sh
```

## Persistent named indexes (v0.3.0)

The v0.2 surface is *ephemeral*: every `bm25_search` call rebuilds a RAM index
from the table, searches once, and throws the index away. v0.3.0 adds **named
persistent indexes** — built once from a table, stored on disk in a directory
(mmap-backed, no background server), and searched any number of times, with
results **bit-identical** to `bm25_search` over the same rows.

### Functions

| Function | Returns |
|---|---|
| `tantivy_create_index(name, table, [id_column := ''], [text_column := 'body'], [language := 'english'], [path := ''], [extra_fields := ''])` | `name, path, docs, language` |
| `tantivy_search_index(name, query, [top_k := 10], [boosts := ''])` with `boosts := 'title:2.0'` | per-query field boosts (V3) |
| `tantivy_search_index(name, query, [top_k := 10])` | `doc_id BIGINT, score DOUBLE` |
| `tantivy_stats(name, query)` | `num_docs, segments, query_terms, estimated_postings_bytes, total_index_bytes, estimated_max_candidates, confidence, notes` |
| `tantivy_save_index(name, [path := ''])` | `name, path, docs` |
| `tantivy_load_index(name, path := '...')` | `name, path, docs, language` |
| `tantivy_list()` | `name, path, docs, language` |

- `tantivy_create_index` builds (or **rebuilds** — the index directory is
  wiped first, `CREATE OR REPLACE` semantics, never a silent append) the
  on-disk index `name` from the table's current rows and registers it in the
  process-global registry. Default location: `$TANTIVY_INDEX_DIR/<name>`,
  else `.tantivy_index/<name>` in the working directory.
- `tantivy_search_index` runs a BM25 query against a registered index: the
  same query pipeline (NFC, metacharacter sanitization, conjunctive/AND
  matching, score-desc/doc-id-asc tie-break at the cutoff) as `bm25_search`,
  so both return **bit-identical** `(doc_id, score)` rows for the same corpus
  and parameters. Search touches no table.
- `tantivy_save_index` copies the index directory to `path` — a full,
  loadable copy including the provenance sidecar — or, with no `path`, simply
  reports where the index lives. An existing, non-empty destination is
  refused (never an overwrite).
- `tantivy_load_index` opens a previously saved directory (created in any
  process) read-only and registers it under `name`. `path` is a named
  parameter (style-matched to `save`) but **required**: omitting it is a
  bind-time error.
- `tantivy_list` snapshots the process's registry, one row per registered
  index, sorted by name.

### Example: build → search → save → load in a fresh process

```sql
-- build once (default location: $TANTIVY_INDEX_DIR/<name>, else .tantivy_index/<name>)
SELECT * FROM tantivy_create_index('news', 'articles', id_column := 'id');

-- search any number of times — no rebuild, no table access
SELECT * FROM tantivy_search_index('news', 'crude oil', top_k := 20);
SELECT * FROM tantivy_list();

-- back it up / move it to another machine (or report where it lives)
SELECT * FROM tantivy_save_index('news', path := '/backup/news_index');

-- in a FRESH process (or under a new name): open it and search
SELECT * FROM tantivy_load_index('news_copy', path := '/backup/news_index');
SELECT * FROM tantivy_search_index('news_copy', 'crude oil', top_k := 20);
```

### Chinese (jieba)

```sql
CREATE TABLE zh_docs(doc_id BIGINT, body VARCHAR);
INSERT INTO zh_docs VALUES
  (0, '苹果发布了新款手机'),
  (4, '苹果 苹果 股票 苹果');

SELECT * FROM tantivy_create_index('zh', 'zh_docs', id_column := 'doc_id',
                                   language := 'chinese');
SELECT * FROM tantivy_search_index('zh', '苹果 股票');
-- doc_id | score
--      4 | ...   -- conjunctive (AND): only doc 4 contains both terms

-- in a fresh process (default dir: .tantivy_index/zh):
SELECT * FROM tantivy_load_index('zh2', path := '.tantivy_index/zh');
SELECT * FROM tantivy_search_index('zh2', '苹果 股票');  -- identical rows
```

### Persistent vs ephemeral — which to use

- **Use `bm25_search` (ephemeral)** for one-off or ad-hoc searches, for
  tables that change between queries (it always sees the current rows), or
  when you don't want to manage index names and files.
- **Use a persistent index** when you search the same corpus repeatedly (no
  per-query table scan or rebuild), when you need reproducible results across
  processes/machines (`save` → `load`), or when per-call rebuilds would hurt.
  Trade-off: an index is a *snapshot* — it does not see table changes made
  after the build; refresh it by rebuilding (v0.3.0 has no incremental
  update).

### Semantics and guarantees

- **Same ranking as `bm25_search`.** The e2e harness diffs persistent vs
  ephemeral results across fresh processes (english and chinese) and enforces
  bit-identity on every run.
- **The index is immutable.** There is no incremental update and no `drop`
  in v0.3.0: replace an index by rebuilding it under a fresh name, or in a
  fresh process.
- **The registry is process-global.** Registered indexes live for the life of
  the DuckDB process, in a process-wide registry (like duckdb-vss's). Loading
  a name that is already registered is a clear error.
- **Provenance.** Every index directory carries a `__tantivy_meta.json`
  sidecar (name + language); `tantivy_load_index` refuses directories without
  it (foreign or corrupt index → clear error, never a guess).

### Known limitations (v0.3.0)

- **The registry is process-global and has no drop.** Indexes registered in a
  DuckDB process stay registered until the process exits; there is no
  `tantivy_drop_index`. To replace an index, rebuild it under a different
  name (or in a fresh process); on-disk directories outlive the process and
  can be moved, backed up, or deleted by hand.
- **`path` on `tantivy_create_index` takes ownership of that directory**
  (wiped on every rebuild) — never point it at a directory whose contents
  you need.
- **Same corpus rules as `bm25_search`:** `id_column` must be castable to
  `BIGINT`; temporary tables and uncommitted transactions are not visible to
  the corpus read (session-connection limitation, see *Known limitations*
  below); 1,000,000-row + proportional text-bytes pre-flight cap; 16 MiB per
  text value.
- **No incremental updates** — changes to the source table after the build
  are not reflected; rebuild the index.

### Performance (synthetic corpus, measured)

Fresh-process, release-build measurements on a 100k-row synthetic corpus
(`scripts/run_persistent_e2e.sh` step 6; in-suite smoke test
`persistent_index_100k_rows`):

- **Index build: ≈ 0.3–0.7 s** (0.3 s in our CI-class run, 0.7 s on the
  coordinator's WSL machine — machine-dependent).
- **Search: tens of ms per query** — two searches (top_k 100 / 1000) totaled
  ≈ 46 ms in our run, including the SQL round-trip; no table access, no
  rebuild.
- **Cross-process bit-identical:** after `save` → fresh-process `load`,
  persistent results diff empty against both the pre-save index and an
  ephemeral `bm25_search` with identical parameters — six CSV diffs (english
  + chinese) in the standard cross-process step, plus six more on the 100k-row
  Chinese acceptance corpus.

These are order-of-magnitude smoke numbers on synthetic corpora, not a
benchmark; the test suite asserts generous bounds, not thresholds.

To rebuild and re-verify the persistent chain (offline, full suite +
cross-process save→load bit-identity diffs + 100k-row performance, exit 0 = all
green):

```sh
bash scripts/run_persistent_e2e.sh
```

## Hard limits (by design — they become clean SQL errors, never crashes or silent data loss)

| Limit | Value | Rationale |
|---|---|---|
| `top_k` range | `[1, 100000]` | above the cap, tantivy's top-N collector allocation (`2 × limit`) becomes a memory bomb; below 1 is a semantic error. Out-of-range values are rejected at bind time. |
| Rows indexed per `bm25_search` call | 1,000,000 (plus a proportional text-bytes cap) | the extension builds an in-RAM index per call; add a `WHERE` clause for larger tables |
| Per-text length | 16 MiB | one row cannot blow up tokenization |
| `tokenize` output per call | 1,000,000 elements | overflow is reported as an error, never as silent NULL rows |
| `tantivy_create_index` corpus | 1,000,000 rows (+ proportional text-bytes cap), same as `bm25_search` | enforced by the same pre-flight measurement before any row is copied; the index directory is wiped before a rebuild, never appended to |
| Index names | single identifier segment, `[A-Za-z_][A-Za-z0-9_-]*` ≤ 128 chars | names double as directory names — no separators, dots, or traversal |

## Known limitations

- **wasm builds: persistent named indexes are native-only.** The extension
  compiles for wasm32 (tantivy runs on its in-RAM directory), so `bm25_search`,
  `tantivy_terms`, the `tokenize`/`stem`/`bm25_score` scalars, and `tantivy_stats`
  work in DuckDB-WASM — but `tantivy_create_index` with a `path`, and
  `tantivy_load_index`, return a clear error there (no filesystem, no mmap).
  wasm callers get an in-RAM ephemeral index instead.
- **Temporary tables and uncommitted transactions are not visible to
  `bm25_search`.** DuckDB's C-API table-function callbacks do not receive the
  caller's connection, so the extension queries the table through a
  connection it opens itself (on the same database instance). Temporary
  catalogs are per-connection, so `CREATE TEMP TABLE` results in
  "table does not exist". Permanent and attached tables work fine.
- **Auto row ids** (`id_column := ''`) are 0-based sequence numbers over the
  indexed rows *in table scan order* — they are not stable across table
  mutations or filters. Pass a real `id_column` when you need stable ids.
- **Query semantics are conjunctive (AND)**: every word in the query must
  match (tantivy `set_conjunction_by_default`). A misspelled word or a word
  absent from the corpus yields *zero* results for the whole query. For
  typo tolerance this is why the companion macro layer (duckdb-vss
  `test/tantivy_typo.sql`) fuses the original and corrected queries with
  two-arm RRF instead of merging them into one search.
- **Query text is escaped before parsing.** The query string is fed through
  `sanitize_query`, which backslash-escapes the tantivy query-parser
  metacharacters (`\ : ( ) + - ! = > < ~ * "`), so `bm25_search('docs',
  'e-mail: archos (40%)')` searches as plain words instead of raising a
  `BadQuery` syntax error. Field syntax and phrase quoting in queries are
  therefore not available by design.
- `id_column` must be castable to `BIGINT`; non-integer id columns are not
  supported.
- **Persistent indexes are process-scoped with no drop (v0.3.0).** The registry
  is process-global: indexes registered in a DuckDB process stay registered
  until the process exits, and there is no `tantivy_drop_index` — to replace
  an index, rebuild it under a different name or in a fresh process. On-disk
  directories outlive the process; `tantivy_load_index` re-registers them. A
  `tantivy_create_index` with an explicit `path` takes ownership of that
  directory (wipes it on rebuild) — never point it at a directory you need.
- The in-repo CI/release workflows build for `linux_amd64`, `osx_arm64`,
  `osx_amd64`, and `windows_amd64` (one native build per platform runner).
  Artifacts are **unsigned** (load with `duckdb -unsigned`); code-signing and
  community-registry publication are not set up yet.

## Building

The build delegates to [extension-ci-tools](https://github.com/duckdb/extension-ci-tools)
(vendored as the `extension-ci-tools` submodule), the same pipeline the other
DuckDB extensions use:

```sh
git submodule update --init --recursive   # first time only
make                # == make configure && make release
make test           # cargo unit tests + SQL-level e2e (LOADs the stamped extension)
make clean          # clean build/ + cargo clean
make clean_all      # clean + drop the configure venv
```

`make release` builds the cdylib with cargo and appends the 512-byte DuckDB
metadata footer, producing `build/release/tantivy.duckdb_extension`
(C_STRUCT, C-API floor `v1.2.0`, platform auto-detected by `make configure`).
`make test` mirrors it to `target/release/` and runs the cargo suite, which
compiles a bundled in-memory DuckDB and `LOAD`s the stamped artifact.

CI: `.github/workflows/ci.yml` builds and tests natively on `linux_amd64`,
`osx_arm64`, `osx_amd64`, and `windows_amd64` (the cargo e2e suite is skipped
on Windows, which compiles a bundled in-memory DuckDB from source).
`.github/workflows/release.yml` (on `v*` tags) builds the same matrix and
uploads the stamped `tantivy-<platform>.duckdb_extension` artifacts straight
to the GitHub Release. Neither workflow uses `upload-artifact`, so the shared
Actions artifact-storage pool is never touched. Artifacts are **unsigned**
(load with `duckdb -unsigned`).

### Manual (no make)

```sh
cargo build --release --bin append_metadata
cargo build --release --lib
./target/release/append_metadata \
  target/release/libtantivy.dylib \
  target/release/tantivy.duckdb_extension \
  --abi-type C_STRUCT --extension-version v0.1.0 \
  --duckdb-version v1.2.0 --platform osx_arm64
```

## License

MIT
