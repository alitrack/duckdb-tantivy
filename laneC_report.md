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
