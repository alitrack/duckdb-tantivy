# duckdb-tantivy

A native Rust [DuckDB](https://duckdb.org) loadable extension that adds **BM25
full-text search** (via [tantivy](https://github.com/quickwit-oss/tantivy))
plus **tokenization** and **Snowball stemming** — written against the C
Extension API (ABI v1.2.0) through
[quack-rs](https://github.com/tomtom215/quack-rs). No VGI, no Arrow IPC, no
JSON shims: table functions bind to real tables and stream real columns.

## Function surface

| Function | Description |
|---|---|
| `bm25_search(table, query, [id_column := ''], [text_column := 'body'], [language := 'english'], [top_k := 100])` | BM25 search over a table → `doc_id BIGINT, score DOUBLE` |
| `supported_languages()` | The 18 Snowball languages tantivy can stem |
| `tokenize(text)` / `tokenize(text, language)` | Tokenize (+ stem if language given) → `VARCHAR[]` |
| `stem(word, language)` | Snowball-stem a single word |
| `bm25_score(doc, query)` / `bm25_score(doc, query, language)` | BM25 score of one doc against a query |
| `tantivy_version()` | Version string of the embedded tantivy |

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

## Hard limits (by design — they become clean SQL errors, never crashes or silent data loss)

| Limit | Value | Rationale |
|---|---|---|
| `top_k` range | `[1, 100000]` | above the cap, tantivy's top-N collector allocation (`2 × limit`) becomes a memory bomb; below 1 is a semantic error. Out-of-range values are rejected at bind time. |
| Rows indexed per `bm25_search` call | 1,000,000 (plus a proportional text-bytes cap) | the extension builds an in-RAM index per call; add a `WHERE` clause for larger tables |
| Per-text length | 16 MiB | one row cannot blow up tokenization |
| `tokenize` output per call | 1,000,000 elements | overflow is reported as an error, never as silent NULL rows |

## Known limitations

- **Temporary tables and uncommitted transactions are not visible to
  `bm25_search`.** DuckDB's C-API table-function callbacks do not receive the
  caller's connection, so the extension queries the table through a
  connection it opens itself (on the same database instance). Temporary
  catalogs are per-connection, so `CREATE TEMP TABLE` results in
  "table does not exist". Permanent and attached tables work fine.
- **Auto row ids** (`id_column := ''`) are 0-based sequence numbers over the
  indexed rows *in table scan order* — they are not stable across table
  mutations or filters. Pass a real `id_column` when you need stable ids.
- `id_column` must be castable to `BIGINT`; non-integer id columns are not
  supported.
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
